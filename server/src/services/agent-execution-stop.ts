import { readProcessStartedAt } from "./hot-restart.js";
import { runUsedConversationAdapter } from "./conversation-continuation.js";
import { and, eq, inArray, isNull, or, sql } from "drizzle-orm";
import { environmentLeases, heartbeatRuns, nativeRunFinalizations, type Db } from "@paperclipai/db";
import { hasNativeLocalProcessStop } from "./native-local-process-stop.js";
import { hasRemoteTerminationReceipt } from "./remote-execution-termination.js";
import { isCancelledNativeStartup } from "./cancelled-native-startup.js";

/** Cancellation changes admission. Process and lease evidence controls cleanup. */
export async function agentExecutionsHaveStopped(db: Db, agentIds: string[]) {
  if (!agentIds.length) return true;
  // Lifecycle stop retries poll this for every stopped agent. Read only the
  // columns the process and lease checks need: the full rows (context snapshot,
  // result, output excerpts) of an agent's whole run history are megabytes of
  // TOAST to decompress and ship on each attempt. The few cancelled runs that
  // reach the startup and acknowledgement proofs load their full row by id.
  const runs = await db.select({
    id: heartbeatRuns.id,
    companyId: heartbeatRuns.companyId,
    status: heartbeatRuns.status,
    runtimeMode: heartbeatRuns.runtimeMode,
    startedAt: heartbeatRuns.startedAt,
    processPid: heartbeatRuns.processPid,
    processGroupId: heartbeatRuns.processGroupId,
    processStartedAt: heartbeatRuns.processStartedAt,
    controllerLeaseExpiresAt: heartbeatRuns.controllerLeaseExpiresAt,
  }).from(heartbeatRuns).where(inArray(heartbeatRuns.agentId, agentIds));
  const fullRun = async (runId: string) => {
    const [row] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    return row;
  };
  const runIds = db.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(inArray(heartbeatRuns.agentId, agentIds));
  const leases = await db.select().from(environmentLeases).where(or(
    inArray(environmentLeases.heartbeatRunId, runIds),
    inArray(sql`${environmentLeases.metadata}->>'agentId'`, agentIds),
  ));
  if (leases.some(lease => (!lease.releasedAt && !(lease.status === "retained" && lease.cleanupStatus === "success")) || lease.status === "pending_cleanup" || lease.cleanupStatus === "failed")) return false;
  const coordinators = await db.select().from(nativeRunFinalizations)
    .where(inArray(nativeRunFinalizations.runId, runIds));
  const now = new Date();
  const abandoned = coordinators.filter(row => row.leaseOwner);
  if (abandoned.some(row => !row.leaseExpiresAt || row.leaseExpiresAt > now || row.resultId ||
      runs.find(run => run.id === row.runId)?.status !== "cancelled")) return false;
  const absent = (pid: number) => {
    try { process.kill(pid, 0); return false; }
    catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
  };
  for (const coordinator of abandoned) {
    if (!coordinator.controllerPid || absent(coordinator.controllerPid)) continue;
    const observed = await readProcessStartedAt(coordinator.controllerPid).catch(() => null);
    if (!observed || !coordinator.controllerProcessStartedAt ||
        new Date(observed).getTime() === coordinator.controllerProcessStartedAt.getTime()) return false;
  }
  for (const run of runs) {
    if (!["succeeded", "failed", "cancelled", "timed_out", "interrupted"].includes(run.status)) return false;
    if (run.controllerLeaseExpiresAt && run.controllerLeaseExpiresAt > new Date()) return false;
    const runLeases = leases.filter(lease => lease.heartbeatRunId === run.id);
    if (runLeases.some(lease => lease.provider && lease.provider !== "local")) {
      if (run.status === "cancelled" && !runLeases.every(hasRemoteTerminationReceipt)) return false;
      continue;
    }
    let processAlive = Boolean(run.processPid && !absent(run.processPid));
    let groupAlive = Boolean(run.processGroupId && !absent(-run.processGroupId));
    if (processAlive && run.processStartedAt) {
      const observed = await readProcessStartedAt(run.processPid!).catch(() => null);
      if (observed && new Date(observed).getTime() !== run.processStartedAt.getTime()) {
        processAlive = false;
        if (run.processGroupId === run.processPid) groupAlive = false;
      }
    }
    if (processAlive || groupAlive) return false;
    if (run.status !== "cancelled" || !run.startedAt) continue;
    if (run.processPid || run.processGroupId) continue;
    if (run.runtimeMode === "native" && !await hasNativeLocalProcessStop(db, run.companyId, run.id)) {
      const full = await fullRun(run.id);
      if (!full || !await isCancelledNativeStartup(db, full, coordinators.find(row => row.runId === run.id))) return false;
    }
    if (run.runtimeMode === "legacy") {
      const full = await fullRun(run.id);
      if (!full) return false;
      if ((full.resultJson?.executionCancellation as Record<string, unknown> | undefined)?.state !== "acknowledged" &&
          !(full.executionStage === "settled" && !await runUsedConversationAdapter(db, full)) &&
          !await isCancelledNativeStartup(db, full, coordinators.find(row => row.runId === run.id))) return false;
    }
  }
  // Release an expired owner only after its cancelled execution has stop proof.
  for (const coordinator of abandoned) {
    const [cleared] = await db.update(nativeRunFinalizations).set({ leaseOwner: null, leaseExpiresAt: null, updatedAt: now })
      .where(and(eq(nativeRunFinalizations.runId, coordinator.runId),
        eq(nativeRunFinalizations.leaseOwner, coordinator.leaseOwner!),
        eq(nativeRunFinalizations.leaseExpiresAt, coordinator.leaseExpiresAt!),
        eq(nativeRunFinalizations.controllerGeneration, coordinator.controllerGeneration),
        eq(nativeRunFinalizations.phase, coordinator.phase), isNull(nativeRunFinalizations.resultId)))
      .returning({ runId: nativeRunFinalizations.runId });
    if (!cleared) return false;
  }
  return true;
}
