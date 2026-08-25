import { describe, expect, it } from "vite-plus/test";

import {
  cursorCloudResumeCursor,
  cursorCloudToolItemType,
  decodeCursorCloudStreamEvent,
  formatCloudGitSummary,
  normalizeGitHubHttpsUrl,
  parseCursorCloudResume,
  pickGitHubRemoteUrl,
  resolveCursorCloudApiOrigin,
  resultTurnState,
} from "./CursorCloudProtocol.ts";

describe("CursorCloudProtocol", () => {
  it("normalizes github ssh, https, and git remote urls", () => {
    expect(normalizeGitHubHttpsUrl("git@github.com:org/repo.git")).toBe(
      "https://github.com/org/repo",
    );
    expect(normalizeGitHubHttpsUrl("ssh://git@github.com/org/repo")).toBe(
      "https://github.com/org/repo",
    );
    expect(normalizeGitHubHttpsUrl("https://github.com/org/repo.git")).toBe(
      "https://github.com/org/repo",
    );
    expect(normalizeGitHubHttpsUrl("github.com/org/repo")).toBe("https://github.com/org/repo");
    expect(normalizeGitHubHttpsUrl("https://gitlab.com/org/repo.git")).toBeNull();
    expect(normalizeGitHubHttpsUrl("")).toBeNull();
  });

  it("prefers origin when it is a github remote", () => {
    expect(
      pickGitHubRemoteUrl([
        { name: "upstream", url: "git@github.com:other/repo.git" },
        { name: "origin", url: "git@github.com:org/repo.git" },
      ]),
    ).toBe("https://github.com/org/repo");
  });

  it("falls back to the first github remote when origin is not github", () => {
    expect(
      pickGitHubRemoteUrl([
        { name: "origin", url: "git@gitlab.com:org/repo.git" },
        { name: "github", url: "https://github.com/org/repo.git" },
      ]),
    ).toBe("https://github.com/org/repo");
  });

  it("parses and writes a versioned resume cursor", () => {
    expect(parseCursorCloudResume({ schemaVersion: 1, agentId: "bc-123" })).toEqual({
      agentId: "bc-123",
    });
    expect(parseCursorCloudResume({ schemaVersion: 2, agentId: "bc-123" })).toBeUndefined();
    expect(parseCursorCloudResume({ schemaVersion: 1, agentId: "" })).toBeUndefined();
    expect(cursorCloudResumeCursor("bc-123")).toEqual({ schemaVersion: 1, agentId: "bc-123" });
  });

  it("decodes simplified SSE events and ignores empty assistant deltas", () => {
    expect(decodeCursorCloudStreamEvent("assistant", { text: "Hello" }, "1")).toEqual({
      event: "assistant",
      id: "1",
      data: { text: "Hello" },
    });
    expect(decodeCursorCloudStreamEvent("assistant", { text: "" }, "1")).toBeNull();
    expect(
      decodeCursorCloudStreamEvent("tool_call", {
        callId: "tool-1",
        name: "read_file",
        status: "running",
        args: { path: "README.md" },
      }),
    ).toEqual({
      event: "tool_call",
      data: {
        callId: "tool-1",
        name: "read_file",
        status: "running",
        args: { path: "README.md" },
      },
    });
    expect(cursorCloudToolItemType("read_file")).toBe("dynamic_tool_call");
    expect(cursorCloudToolItemType("run_terminal_cmd")).toBe("command_execution");
  });

  it("formats pull request urls for the thread", () => {
    expect(
      formatCloudGitSummary({
        branches: [
          {
            repoUrl: "https://github.com/org/repo",
            branch: "cursor/cloud-agents-1",
            prUrl: "https://github.com/org/repo/pull/12",
          },
        ],
      }),
    ).toBe("Opened pull request: https://github.com/org/repo/pull/12\nBranch: cursor/cloud-agents-1");
  });

  it("maps run status onto turn state", () => {
    expect(resultTurnState("FINISHED")).toBe("completed");
    expect(resultTurnState("CANCELLED")).toBe("cancelled");
    expect(resultTurnState("EXPIRED")).toBe("interrupted");
    expect(resultTurnState("ERROR")).toBe("failed");
  });

  it("defaults and trims the public API origin", () => {
    expect(resolveCursorCloudApiOrigin(undefined)).toBe("https://api.cursor.com");
    expect(resolveCursorCloudApiOrigin(" https://example.test/v1/ ")).toBe(
      "https://example.test/v1",
    );
  });
});
