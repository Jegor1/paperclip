import { describe, expect, it } from "vitest";
import { getAgentProviderLabel } from "./agent-ai-provider";

describe("getAgentProviderLabel", () => {
  it("shows AI LOCALE for agents on the local engine", () => {
    expect(getAgentProviderLabel({ adapterType: "claude_local", metadata: { aiProvider: "locale" } })).toBe("AI LOCALE");
  });
  it("falls back to the adapter label", () => {
    expect(getAgentProviderLabel({ adapterType: "claude_local", metadata: null })).not.toBe("AI LOCALE");
  });
});
