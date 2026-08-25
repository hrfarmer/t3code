import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sse from "effect/unstable/encoding/Sse";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  decodeCursorCloudStreamEvent,
  resolveCursorCloudApiOrigin,
  type CursorCloudGitSnapshot,
  type CursorCloudStreamEvent,
} from "../cursorCloud/CursorCloudProtocol.ts";

export class CursorCloudApiError extends Schema.TaggedErrorClass<CursorCloudApiError>()(
  "CursorCloudApiError",
  {
    method: Schema.String,
    status: Schema.optional(Schema.Number),
    code: Schema.optional(Schema.String),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface CursorCloudPromptImage {
  readonly data: string;
  readonly mimeType: string;
}

export interface CursorCloudPrompt {
  readonly text: string;
  readonly images?: ReadonlyArray<CursorCloudPromptImage>;
}

export interface CursorCloudModelParam {
  readonly id: string;
  readonly value: string;
}

export interface CursorCloudModelSelection {
  readonly id: string;
  readonly params?: ReadonlyArray<CursorCloudModelParam>;
}

export interface CursorCloudCreateAgentInput {
  readonly prompt: CursorCloudPrompt;
  readonly model?: CursorCloudModelSelection;
  readonly repos: ReadonlyArray<{ readonly url: string; readonly startingRef?: string }>;
  readonly autoCreatePR?: boolean;
  readonly workOnCurrentBranch?: boolean;
}

export interface CursorCloudCreateRunInput {
  readonly prompt: CursorCloudPrompt;
}

export interface CursorCloudMe {
  readonly apiKeyName?: string;
  readonly userEmail?: string;
  readonly userFirstName?: string;
  readonly userLastName?: string;
}

export interface CursorCloudModelInfo {
  readonly id: string;
  readonly displayName: string;
  readonly description?: string;
  readonly parameters?: ReadonlyArray<{
    readonly id: string;
    readonly displayName?: string;
    readonly values: ReadonlyArray<{ readonly value: string; readonly displayName?: string }>;
  }>;
  readonly variants?: ReadonlyArray<{
    readonly params: ReadonlyArray<CursorCloudModelParam>;
    readonly displayName: string;
    readonly isDefault?: boolean;
  }>;
}

export interface CursorCloudAgent {
  readonly id: string;
  readonly name?: string;
  readonly status?: string;
  readonly url?: string;
  readonly latestRunId?: string;
}

export interface CursorCloudRun {
  readonly id: string;
  readonly agentId?: string;
  readonly status?: string;
  readonly result?: string;
  readonly durationMs?: number;
  readonly git?: CursorCloudGitSnapshot;
}

export interface CursorCloudApi {
  readonly getMe: () => Effect.Effect<CursorCloudMe, CursorCloudApiError>;
  readonly listModels: () => Effect.Effect<ReadonlyArray<CursorCloudModelInfo>, CursorCloudApiError>;
  readonly createAgent: (
    input: CursorCloudCreateAgentInput,
  ) => Effect.Effect<{ readonly agent: CursorCloudAgent; readonly run: CursorCloudRun }, CursorCloudApiError>;
  readonly createRun: (
    agentId: string,
    input: CursorCloudCreateRunInput,
  ) => Effect.Effect<{ readonly run: CursorCloudRun }, CursorCloudApiError>;
  readonly getRun: (
    agentId: string,
    runId: string,
  ) => Effect.Effect<CursorCloudRun, CursorCloudApiError>;
  readonly getAgent: (agentId: string) => Effect.Effect<CursorCloudAgent, CursorCloudApiError>;
  readonly streamRun: (
    agentId: string,
    runId: string,
    lastEventId?: string,
  ) => Stream.Stream<CursorCloudStreamEvent, CursorCloudApiError>;
  readonly cancelRun: (agentId: string, runId: string) => Effect.Effect<void, CursorCloudApiError>;
  readonly archiveAgent: (agentId: string) => Effect.Effect<void, CursorCloudApiError>;
  readonly unarchiveAgent: (agentId: string) => Effect.Effect<void, CursorCloudApiError>;
  readonly deleteAgent: (agentId: string) => Effect.Effect<void, CursorCloudApiError>;
}

const readJson = (response: HttpClientResponse.HttpClientResponse) =>
  HttpClientResponse.schemaBodyJson(Schema.Unknown)(response);

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const asString = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;

const asNumber = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;

const parseAgent = (value: unknown): CursorCloudAgent => {
  const record = asRecord(value);
  return {
    id: asString(record.id) ?? "",
    ...(asString(record.name) ? { name: asString(record.name) } : {}),
    ...(asString(record.status) ? { status: asString(record.status) } : {}),
    ...(asString(record.url) ? { url: asString(record.url) } : {}),
    ...(asString(record.latestRunId) ? { latestRunId: asString(record.latestRunId) } : {}),
  };
};

const parseRun = (value: unknown): CursorCloudRun => {
  const record = asRecord(value);
  return {
    id: asString(record.id) ?? "",
    ...(asString(record.agentId) ? { agentId: asString(record.agentId) } : {}),
    ...(asString(record.status) ? { status: asString(record.status) } : {}),
    ...(asString(record.result) ? { result: asString(record.result) } : {}),
    ...(asNumber(record.durationMs) !== undefined ? { durationMs: asNumber(record.durationMs) } : {}),
    ...(record.git !== undefined ? { git: record.git as CursorCloudGitSnapshot } : {}),
  };
};

const failStatus = (
  method: string,
  status: number,
  body: unknown,
): CursorCloudApiError => {
  const record = asRecord(body);
  const code = asString(record.code) ?? asString(record.error);
  const detail =
    asString(record.message) ??
    asString(record.detail) ??
    `Cursor Cloud API ${method} failed (${status}).`;
  return new CursorCloudApiError({ method, status, detail, ...(code ? { code } : {}) });
};

export const makeCursorCloudApi = (input: {
  readonly httpClient: HttpClient.HttpClient;
  readonly apiKey: string;
  readonly apiEndpoint?: string;
}): CursorCloudApi => {
  const origin = resolveCursorCloudApiOrigin(input.apiEndpoint);
  const authorize = (request: HttpClientRequest.HttpClientRequest) =>
    request.pipe(
      HttpClientRequest.bearerToken(input.apiKey),
      HttpClientRequest.setHeader("Accept", "application/json"),
    );

  const url = (path: string) => `${origin}${path}`;

  const executeJson = (method: string, request: HttpClientRequest.HttpClientRequest) =>
    input.httpClient.execute(authorize(request)).pipe(
      Effect.mapError(
        (cause) =>
          new CursorCloudApiError({
            method,
            detail: `Cursor Cloud API ${method} request failed.`,
            cause,
          }),
      ),
      Effect.flatMap((response) =>
        readJson(response).pipe(
          Effect.orElseSucceed(() => ({})),
          Effect.flatMap((body) =>
            response.status >= 400
              ? Effect.fail(failStatus(method, response.status, body))
              : Effect.succeed(body),
          ),
        ),
      ),
    );

  const executeEmpty = (method: string, request: HttpClientRequest.HttpClientRequest) =>
    input.httpClient.execute(authorize(request)).pipe(
      Effect.mapError(
        (cause) =>
          new CursorCloudApiError({
            method,
            detail: `Cursor Cloud API ${method} request failed.`,
            cause,
          }),
      ),
      Effect.flatMap((response) =>
        response.status >= 400
          ? readJson(response).pipe(
              Effect.orElseSucceed(() => ({})),
              Effect.flatMap((body) => Effect.fail(failStatus(method, response.status, body))),
            )
          : Effect.void,
      ),
    );

  return {
    getMe: () =>
      executeJson("GET /v1/me", HttpClientRequest.get(url("/v1/me"))).pipe(
        Effect.map((body) => {
          const record = asRecord(body);
          return {
            ...(asString(record.apiKeyName) ? { apiKeyName: asString(record.apiKeyName) } : {}),
            ...(asString(record.userEmail) ? { userEmail: asString(record.userEmail) } : {}),
            ...(asString(record.userFirstName)
              ? { userFirstName: asString(record.userFirstName) }
              : {}),
            ...(asString(record.userLastName)
              ? { userLastName: asString(record.userLastName) }
              : {}),
          };
        }),
      ),

    listModels: () =>
      executeJson("GET /v1/models", HttpClientRequest.get(url("/v1/models"))).pipe(
        Effect.map((body) => {
          const items = asRecord(body).items;
          if (!Array.isArray(items)) return [];
          return items.flatMap((item) => {
            const record = asRecord(item);
            const id = asString(record.id);
            const displayName = asString(record.displayName) ?? id;
            if (!id || !displayName) return [];
            return [
              {
                id,
                displayName,
                ...(asString(record.description) ? { description: asString(record.description) } : {}),
                ...(Array.isArray(record.parameters)
                  ? { parameters: record.parameters as CursorCloudModelInfo["parameters"] }
                  : {}),
                ...(Array.isArray(record.variants)
                  ? { variants: record.variants as CursorCloudModelInfo["variants"] }
                  : {}),
              } satisfies CursorCloudModelInfo,
            ];
          });
        }),
      ),

    createAgent: (createInput) =>
      HttpClientRequest.post(url("/v1/agents")).pipe(
        HttpClientRequest.bodyJson({
          prompt: createInput.prompt,
          ...(createInput.model ? { model: createInput.model } : {}),
          repos: createInput.repos,
          ...(createInput.autoCreatePR !== undefined
            ? { autoCreatePR: createInput.autoCreatePR }
            : {}),
          ...(createInput.workOnCurrentBranch !== undefined
            ? { workOnCurrentBranch: createInput.workOnCurrentBranch }
            : {}),
        }),
        Effect.mapError(
          (cause) =>
            new CursorCloudApiError({
              method: "POST /v1/agents",
              detail: "Failed to encode Cursor Cloud create-agent body.",
              cause,
            }),
        ),
        Effect.flatMap((request) => executeJson("POST /v1/agents", request)),
        Effect.flatMap((body) => {
          const record = asRecord(body);
          const agent = parseAgent(record.agent ?? record);
          const run = parseRun(record.run ?? {});
          if (!agent.id) {
            return Effect.fail(
              new CursorCloudApiError({
                method: "POST /v1/agents",
                detail: "Cursor Cloud create-agent response did not include an agent id.",
              }),
            );
          }
          return Effect.succeed({ agent, run });
        }),
      ),

    createRun: (agentId, createInput) =>
      HttpClientRequest.post(url(`/v1/agents/${encodeURIComponent(agentId)}/runs`)).pipe(
        HttpClientRequest.bodyJson({ prompt: createInput.prompt }),
        Effect.mapError(
          (cause) =>
            new CursorCloudApiError({
              method: "POST /v1/agents/{id}/runs",
              detail: "Failed to encode Cursor Cloud create-run body.",
              cause,
            }),
        ),
        Effect.flatMap((request) => executeJson("POST /v1/agents/{id}/runs", request)),
        Effect.flatMap((body) => {
          const record = asRecord(body);
          const run = parseRun(record.run ?? record);
          if (!run.id) {
            return Effect.fail(
              new CursorCloudApiError({
                method: "POST /v1/agents/{id}/runs",
                detail: "Cursor Cloud create-run response did not include a run id.",
              }),
            );
          }
          return Effect.succeed({ run });
        }),
      ),

    getRun: (agentId, runId) =>
      executeJson(
        "GET /v1/agents/{id}/runs/{runId}",
        HttpClientRequest.get(
          url(`/v1/agents/${encodeURIComponent(agentId)}/runs/${encodeURIComponent(runId)}`),
        ),
      ).pipe(Effect.map(parseRun)),

    getAgent: (agentId) =>
      executeJson(
        "GET /v1/agents/{id}",
        HttpClientRequest.get(url(`/v1/agents/${encodeURIComponent(agentId)}`)),
      ).pipe(
        Effect.flatMap((body) => {
          const agent = parseAgent(asRecord(body).agent ?? body);
          if (!agent.id) {
            return Effect.fail(
              new CursorCloudApiError({
                method: "GET /v1/agents/{id}",
                detail: "Cursor Cloud get-agent response did not include an agent id.",
              }),
            );
          }
          return Effect.succeed(agent);
        }),
      ),

    streamRun: (agentId, runId, lastEventId) => {
      const method = "GET /v1/agents/{id}/runs/{runId}/stream";
      let request = HttpClientRequest.get(
        url(`/v1/agents/${encodeURIComponent(agentId)}/runs/${encodeURIComponent(runId)}/stream`),
      ).pipe(
        HttpClientRequest.bearerToken(input.apiKey),
        HttpClientRequest.setHeader("Accept", "text/event-stream"),
      );
      if (lastEventId && lastEventId.length > 0) {
        request = request.pipe(HttpClientRequest.setHeader("Last-Event-ID", lastEventId));
      }
      return input.httpClient.execute(request).pipe(
        Effect.mapError(
          (cause) =>
            new CursorCloudApiError({
              method,
              detail: "Cursor Cloud run stream request failed.",
              cause,
            }),
        ),
        Effect.flatMap((response) => {
          if (response.status >= 400) {
            return readJson(response).pipe(
              Effect.orElseSucceed(() => ({})),
              Effect.flatMap((body) => Effect.fail(failStatus(method, response.status, body))),
            );
          }
          return Effect.succeed(response.stream);
        }),
        Stream.unwrap,
        Stream.decodeText,
        Stream.pipeThroughChannel(Sse.decode()),
        Stream.map((sseEvent) => {
          const parsed =
            sseEvent.data.length > 0 ? parseJsonOrString(sseEvent.data) : sseEvent.data;
          return (
            decodeCursorCloudStreamEvent(sseEvent.event, parsed, sseEvent.id) ?? {
              event: "heartbeat" as const,
              id: sseEvent.id,
              data: parsed,
            }
          );
        }),
        Stream.filter((event) => event.event !== "heartbeat" && event.event !== "interaction_update"),
      );
    },

    cancelRun: (agentId, runId) =>
      executeEmpty(
        "POST /v1/agents/{id}/runs/{runId}/cancel",
        HttpClientRequest.post(
          url(
            `/v1/agents/${encodeURIComponent(agentId)}/runs/${encodeURIComponent(runId)}/cancel`,
          ),
        ),
      ),

    archiveAgent: (agentId) =>
      executeEmpty(
        "POST /v1/agents/{id}/archive",
        HttpClientRequest.post(url(`/v1/agents/${encodeURIComponent(agentId)}/archive`)),
      ),

    unarchiveAgent: (agentId) =>
      executeEmpty(
        "POST /v1/agents/{id}/unarchive",
        HttpClientRequest.post(url(`/v1/agents/${encodeURIComponent(agentId)}/unarchive`)),
      ),

    deleteAgent: (agentId) =>
      executeEmpty(
        "DELETE /v1/agents/{id}",
        HttpClientRequest.delete(url(`/v1/agents/${encodeURIComponent(agentId)}`)),
      ),
  };
};

const parseJsonOrString = (data: string): unknown => {
  try {
    return JSON.parse(data) as unknown;
  } catch {
    return data;
  }
};
