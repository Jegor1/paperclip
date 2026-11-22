import type { Agent } from "@paperclipai/shared";
import { getAdapterLabel } from "../adapters/adapter-display-registry";

/** Provider label for an agent: "AI LOCALE" when metadata.aiProvider is "locale", else the adapter label. */
export function getAgentProviderLabel(agent: Pick<Agent, "adapterType" | "metadata">): string {
  const provider = agent.metadata?.aiProvider;
  if (typeof provider === "string" && provider.trim().toLowerCase() === "locale") return "AI LOCALE";
  return getAdapterLabel(agent.adapterType);
}
