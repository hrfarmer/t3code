import type { CanonicalItemType, ProviderDriverKind } from "@t3tools/contracts";
import { ProviderDriverKind as ProviderDriverKindSchema } from "@t3tools/contracts";

export const CURSOR_CLOUD_DRIVER_KIND: ProviderDriverKind =
  ProviderDriverKindSchema.make("cursorCloud");

export const CURSOR_CLOUD_RESUME_VERSION = 1 as const;
export const DEFAULT_CURSOR_CLOUD_API_ORIGIN = "https://api.cursor.com";

export interface CursorCloudResume {
  readonly schemaVersion: typeof CURSOR_CLOUD_RESUME_VERSION;
  readonly agentId: string;
}

export interface CursorCloudGitBranch {
  readonly repoUrl: string;
  readonly branch?: string;
  readonly prUrl?: string;
}

export interface CursorCloudGitSnapshot {
  readonly branches?: ReadonlyArray<CursorCloudGitBranch>;
}

export interface CursorCloudToolCallEvent {
  readonly callId: string;
  readonly name: string;
  readonly status: "running" | "completed";
  readonly args?: unknown;
  readonly result?: unknown;
  readonly truncated?: { readonly args?: true; readonly result?: true };
}

export type CursorCloudStreamEvent =
  | { readonly event: "status"; readonly id?: string; readonly data: { readonly runId?: string; readonly status?: string } }
  | { readonly event: "assistant"; readonly id?: string; readonly data: { readonly text: string } }
  | { readonly event: "thinking"; readonly id?: string; readonly data: { readonly text: string } }
  | { readonly event: "tool_call"; readonly id?: string; readonly data: CursorCloudToolCallEvent }
  | {
      readonly event: "result";
      readonly id?: string;
      readonly data: {
        readonly runId?: string;
        readonly status?: string;
        readonly text?: string;
        readonly durationMs?: number;
        readonly git?: CursorCloudGitSnapshot;
      };
    }
  | { readonly event: "error"; readonly id?: string; readonly data: { readonly code?: string; readonly message?: string } }
  | { readonly event: "done"; readonly id?: string; readonly data: unknown }
  | { readonly event: "heartbeat"; readonly id?: string; readonly data: unknown }
  | { readonly event: "interaction_update"; readonly id?: string; readonly data: unknown };

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

export function parseCursorCloudResume(raw: unknown): { readonly agentId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== CURSOR_CLOUD_RESUME_VERSION) return undefined;
  if (typeof raw.agentId !== "string" || raw.agentId.trim().length === 0) return undefined;
  return { agentId: raw.agentId.trim() };
}

export function cursorCloudResumeCursor(agentId: string): CursorCloudResume {
  return { schemaVersion: CURSOR_CLOUD_RESUME_VERSION, agentId };
}

export function resolveCursorCloudApiOrigin(apiEndpoint: string | undefined): string {
  const trimmed = apiEndpoint?.trim();
  if (!trimmed) return DEFAULT_CURSOR_CLOUD_API_ORIGIN;
  return trimmed.replace(/\/+$/u, "");
}

export function normalizeGitHubHttpsUrl(remoteUrl: string): string | null {
  const trimmed = remoteUrl.trim();
  if (trimmed.length === 0) return null;

  const sshMatch = trimmed.match(/^(?:ssh:\/\/)?git@github\.com[:/]([^/]+)\/(.+?)(?:\.git)?$/iu);
  if (sshMatch?.[1] && sshMatch[2]) {
    return `https://github.com/${sshMatch[1]}/${sshMatch[2].replace(/\.git$/iu, "")}`;
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmed.includes("://") ? trimmed : `https://${trimmed}`);
  } catch {
    return null;
  }

  if (!/(^|\.)github\.com$/iu.test(parsed.hostname)) {
    return null;
  }

  const parts = parsed.pathname.replace(/^\/+|\/+$/gu, "").replace(/\.git$/iu, "").split("/");
  if (parts.length < 2 || parts[0] === undefined || parts[1] === undefined) {
    return null;
  }
  return `https://github.com/${parts[0]}/${parts[1]}`;
}

export function pickGitHubRemoteUrl(
  remotes: ReadonlyArray<{ readonly name: string; readonly url: string }>,
): string | null {
  const origin = remotes.find((remote) => remote.name === "origin");
  if (origin) {
    const normalized = normalizeGitHubHttpsUrl(origin.url);
    if (normalized) return normalized;
  }
  for (const remote of remotes) {
    const normalized = normalizeGitHubHttpsUrl(remote.url);
    if (normalized) return normalized;
  }
  return null;
}

export function formatCloudGitSummary(git: CursorCloudGitSnapshot | undefined): string | undefined {
  const branches = git?.branches ?? [];
  if (branches.length === 0) return undefined;
  const lines: string[] = [];
  for (const branch of branches) {
    if (typeof branch.prUrl === "string" && branch.prUrl.trim().length > 0) {
      lines.push(`Opened pull request: ${branch.prUrl.trim()}`);
    }
    if (typeof branch.branch === "string" && branch.branch.trim().length > 0) {
      lines.push(`Branch: ${branch.branch.trim()}`);
    }
  }
  return lines.length > 0 ? lines.join("\n") : undefined;
}

const TOOL_ITEM_TYPES: Record<string, CanonicalItemType> = {
  run_terminal_cmd: "command_execution",
  shell: "command_execution",
  bash: "command_execution",
  grep: "command_execution",
  read_file: "dynamic_tool_call",
  read: "dynamic_tool_call",
  edit: "file_change",
  write: "file_change",
  apply_patch: "file_change",
  search_replace: "file_change",
  web_search: "web_search",
  websearch: "web_search",
  mcp: "mcp_tool_call",
  image_view: "image_view",
  read_image: "image_view",
};

export function cursorCloudToolItemType(name: string): CanonicalItemType {
  const key = name.trim().toLowerCase().replace(/[\s-]+/gu, "_");
  return TOOL_ITEM_TYPES[key] ?? "dynamic_tool_call";
}

export function cursorCloudToolTitle(name: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) return "Tool";
  return trimmed.replace(/[_-]+/gu, " ");
}

export function decodeCursorCloudStreamEvent(
  eventName: string,
  data: unknown,
  id?: string,
): CursorCloudStreamEvent | null {
  const event = eventName.trim() || "message";
  switch (event) {
    case "status": {
      if (!isRecord(data)) return { event: "status", id, data: {} };
      return {
        event: "status",
        id,
        data: {
          ...(typeof data.runId === "string" ? { runId: data.runId } : {}),
          ...(typeof data.status === "string" ? { status: data.status } : {}),
        },
      };
    }
    case "assistant": {
      if (!isRecord(data) || typeof data.text !== "string" || data.text.length === 0) {
        return null;
      }
      return { event: "assistant", id, data: { text: data.text } };
    }
    case "thinking": {
      if (!isRecord(data) || typeof data.text !== "string" || data.text.length === 0) {
        return null;
      }
      return { event: "thinking", id, data: { text: data.text } };
    }
    case "tool_call": {
      if (!isRecord(data) || typeof data.callId !== "string" || typeof data.name !== "string") {
        return null;
      }
      const status = data.status === "completed" ? "completed" : "running";
      return {
        event: "tool_call",
        id,
        data: {
          callId: data.callId,
          name: data.name,
          status,
          ...(data.args !== undefined ? { args: data.args } : {}),
          ...(data.result !== undefined ? { result: data.result } : {}),
          ...(isRecord(data.truncated)
            ? {
                truncated: {
                  ...(data.truncated.args === true ? { args: true as const } : {}),
                  ...(data.truncated.result === true ? { result: true as const } : {}),
                },
              }
            : {}),
        },
      };
    }
    case "result": {
      if (!isRecord(data)) return { event: "result", id, data: {} };
      return {
        event: "result",
        id,
        data: {
          ...(typeof data.runId === "string" ? { runId: data.runId } : {}),
          ...(typeof data.status === "string" ? { status: data.status } : {}),
          ...(typeof data.text === "string" ? { text: data.text } : {}),
          ...(typeof data.durationMs === "number" ? { durationMs: data.durationMs } : {}),
          ...(isRecord(data.git) ? { git: data.git as CursorCloudGitSnapshot } : {}),
        },
      };
    }
    case "error": {
      if (!isRecord(data)) return { event: "error", id, data: {} };
      return {
        event: "error",
        id,
        data: {
          ...(typeof data.code === "string" ? { code: data.code } : {}),
          ...(typeof data.message === "string" ? { message: data.message } : {}),
        },
      };
    }
    case "done":
      return { event: "done", id, data };
    case "heartbeat":
      return { event: "heartbeat", id, data };
    case "interaction_update":
      return { event: "interaction_update", id, data };
    default:
      return null;
  }
}

export function resultTurnState(status: string | undefined): "completed" | "failed" | "interrupted" | "cancelled" {
  switch ((status ?? "").toUpperCase()) {
    case "FINISHED":
      return "completed";
    case "CANCELLED":
      return "cancelled";
    case "EXPIRED":
      return "interrupted";
    case "ERROR":
    default:
      return "failed";
  }
}
