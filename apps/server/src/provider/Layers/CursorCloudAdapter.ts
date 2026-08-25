import {
  EventId,
  type CursorCloudSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderInstanceId,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HttpClient } from "effect/unstable/http";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { CursorCloudAdapterShape } from "../Services/CursorCloudAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  makeCursorCloudApi,
  CursorCloudApiError,
  type CursorCloudApi,
  type CursorCloudModelParam,
  type CursorCloudPrompt,
  type CursorCloudPromptImage,
} from "./CursorCloudApi.ts";
import { resolveCursorCloudGitTarget, CursorCloudGitError } from "../cursorCloud/CursorCloudGit.ts";
import {
  CURSOR_CLOUD_DRIVER_KIND,
  cursorCloudResumeCursor,
  cursorCloudToolItemType,
  cursorCloudToolTitle,
  formatCloudGitSummary,
  parseCursorCloudResume,
  resultTurnState,
  type CursorCloudStreamEvent,
} from "../cursorCloud/CursorCloudProtocol.ts";
import type { CursorCloudGitTarget } from "../cursorCloud/CursorCloudGit.ts";

const PROVIDER = CURSOR_CLOUD_DRIVER_KIND;
const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

interface CursorCloudSessionContext {
  session: ProviderSession;
  agentId: string | undefined;
  readonly cwd: string | undefined;
  readonly model: string | undefined;
  readonly modelParams: ReadonlyArray<CursorCloudModelParam>;
  activeTurnId: TurnId | undefined;
  activeRunId: string | undefined;
  lastEventId: string | undefined;
  streamFiber: Fiber.Fiber<void, unknown> | undefined;
  readonly stopped: Ref.Ref<boolean>;
  readonly toolItemIds: Map<string, string>;
}

export interface MakeCursorCloudAdapterOptions {
  readonly instanceId: ProviderInstanceId;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly api?: CursorCloudApi;
  readonly resolveGitTarget?: (
    cwd: string,
  ) => Effect.Effect<CursorCloudGitTarget, CursorCloudGitError>;
}

const toAdapterRequestError = (error: CursorCloudApiError, method: string) =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: error.detail,
    cause: error,
  });

const modelParamsFromSelection = (
  options: ReadonlyArray<{ readonly id: string; readonly value: string | boolean }> | undefined,
): CursorCloudModelParam[] => {
  if (!options) return [];
  return options.flatMap((option) => {
    if (typeof option.value === "boolean") {
      return [{ id: option.id, value: option.value ? "true" : "false" }];
    }
    const trimmed = option.value.trim();
    return trimmed.length > 0 ? [{ id: option.id, value: trimmed }] : [];
  });
};

export const makeCursorCloudAdapter = Effect.fn("makeCursorCloudAdapter")(function* (
  settings: CursorCloudSettings,
  options: MakeCursorCloudAdapterOptions,
) {
  const crypto = yield* Crypto.Crypto;
  const httpClient = yield* HttpClient.HttpClient;
  const fileSystem = yield* FileSystem.FileSystem;
  const serverConfig = yield* ServerConfig;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runtimeContext = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(runtimeContext);
  const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const sessions = new Map<ThreadId, CursorCloudSessionContext>();
  const api =
    options.api ??
    makeCursorCloudApi({
      httpClient,
      apiKey: settings.apiKey,
      apiEndpoint: settings.apiEndpoint,
    });
  const resolveGitTarget =
    options.resolveGitTarget ??
    ((cwd: string) =>
      resolveCursorCloudGitTarget(cwd).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
      ));
  const randomUUIDv4 = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate Cursor Cloud runtime identifier.",
          cause,
        }),
    ),
  );

  yield* Effect.addFinalizer(() =>
    Effect.forEach([...sessions.keys()], (threadId) => stopLocalSession(threadId), {
      concurrency: "unbounded",
      discard: true,
    }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents)), Effect.ignoreCause),
  );

  const emit = (event: ProviderRuntimeEvent) => Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

  const stamp = (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly itemId?: string;
  }) =>
    Effect.all({
      eventId: randomUUIDv4.pipe(Effect.map(EventId.make)),
      createdAt: nowIso,
    }).pipe(
      Effect.map(({ eventId, createdAt }) => ({
        eventId,
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        threadId: input.threadId,
        createdAt,
        ...(input.turnId ? { turnId: input.turnId } : {}),
        ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
      })),
    );

  const ensureSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }
      if (yield* Ref.get(context.stopped)) {
        return yield* new ProviderAdapterSessionClosedError({
          provider: PROVIDER,
          threadId,
        });
      }
      return context;
    });

  const interruptStream = (context: CursorCloudSessionContext) =>
    Effect.gen(function* () {
      const fiber = context.streamFiber;
      context.streamFiber = undefined;
      if (fiber) {
        yield* Fiber.interrupt(fiber).pipe(Effect.ignore);
      }
    });

  const stopLocalSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) return;
      const alreadyStopped = yield* Ref.getAndSet(context.stopped, true);
      yield* interruptStream(context);
      sessions.delete(threadId);
      if (alreadyStopped) return;
      const base = yield* stamp({ threadId, turnId: context.activeTurnId });
      yield* emit({
        ...base,
        type: "session.exited",
        payload: { reason: "stopped", recoverable: true },
      });
    });

  const loadPrompt = (input: {
    readonly threadId: ThreadId;
    readonly text: string;
    readonly attachments?: ReadonlyArray<{
      readonly type: "image";
      readonly id: string;
      readonly name: string;
      readonly mimeType: string;
      readonly sizeBytes: number;
    }>;
  }): Effect.Effect<CursorCloudPrompt, ProviderAdapterRequestError | ProviderAdapterValidationError> =>
    Effect.gen(function* () {
      const images: CursorCloudPromptImage[] = [];
      for (const attachment of input.attachments ?? []) {
        const path = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!path) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `Could not resolve attachment ${attachment.name}.`,
          });
        }
        const bytes = yield* fileSystem.readFile(path).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "readAttachment",
                detail: `Failed to read attachment ${attachment.name}.`,
                cause,
              }),
          ),
        );
        images.push({
          data: Buffer.from(bytes).toString("base64"),
          mimeType: attachment.mimeType,
        });
      }
      return {
        text: input.text,
        ...(images.length > 0 ? { images } : {}),
      };
    });

  const consumeRunStream = (
    context: CursorCloudSessionContext,
    threadId: ThreadId,
    turnId: TurnId,
    agentId: string,
    runId: string,
  ) =>
    Effect.gen(function* () {
      const finalize = (state: "completed" | "failed" | "interrupted" | "cancelled", errorMessage?: string) =>
        Effect.gen(function* () {
          if (context.activeTurnId !== turnId) return;
          context.activeTurnId = undefined;
          context.activeRunId = undefined;
          const base = yield* stamp({ threadId, turnId });
          yield* emit({
            ...base,
            type: "turn.completed",
            payload: {
              state,
              ...(errorMessage ? { errorMessage } : {}),
            },
          });
        });

      yield* api.streamRun(agentId, runId, context.lastEventId).pipe(
        Stream.runForEach((event: CursorCloudStreamEvent) =>
          Effect.gen(function* () {
            if (event.id) context.lastEventId = event.id;
            if (options.nativeEventLogger) {
              yield* options.nativeEventLogger.write(
                {
                  source: "cursor.cloud.sse",
                  event: event.event,
                  ...(event.id ? { id: event.id } : {}),
                  data: event.data,
                },
                threadId,
              );
            }
            if (event.event === "assistant") {
              const base = yield* stamp({ threadId, turnId });
              yield* emit({
                ...base,
                type: "content.delta",
                payload: { streamKind: "assistant_text", delta: event.data.text },
              });
              return;
            }
            if (event.event === "thinking") {
              const base = yield* stamp({ threadId, turnId });
              yield* emit({
                ...base,
                type: "content.delta",
                payload: { streamKind: "reasoning_text", delta: event.data.text },
              });
              return;
            }
            if (event.event === "tool_call") {
              const itemId = context.toolItemIds.get(event.data.callId) ?? event.data.callId;
              context.toolItemIds.set(event.data.callId, itemId);
              const itemType = cursorCloudToolItemType(event.data.name);
              const title = cursorCloudToolTitle(event.data.name);
              const base = yield* stamp({ threadId, turnId, itemId });
              if (event.data.status === "running") {
                yield* emit({
                  ...base,
                  type: "item.started",
                  payload: {
                    itemType,
                    status: "inProgress",
                    title,
                    data: event.data.args,
                  },
                });
                return;
              }
              yield* emit({
                ...base,
                type: "item.completed",
                payload: {
                  itemType,
                  status: "completed",
                  title,
                  data: event.data.result ?? event.data.args,
                },
              });
              return;
            }
            if (event.event === "result") {
              const gitSummary = formatCloudGitSummary(event.data.git);
              const text = event.data.text?.trim();
              const extra = [text, gitSummary].filter((part): part is string => Boolean(part));
              if (extra.length > 0) {
                const base = yield* stamp({ threadId, turnId });
                yield* emit({
                  ...base,
                  type: "content.delta",
                  payload: {
                    streamKind: "assistant_text",
                    delta: extra.join("\n\n"),
                  },
                });
              }
              yield* finalize(resultTurnState(event.data.status), event.data.status === "ERROR" ? event.data.text : undefined);
              return;
            }
            if (event.event === "error") {
              yield* finalize("failed", event.data.message ?? event.data.code ?? "Cursor Cloud run failed.");
              return;
            }
            if (event.event === "done") {
              if (context.activeTurnId === turnId) {
                yield* finalize("completed");
              }
            }
          }),
        ),
        Effect.catch((error: CursorCloudApiError) =>
          Effect.gen(function* () {
            if (error.status === 410) {
              const run = yield* api.getRun(agentId, runId).pipe(
                Effect.catch(() =>
                  Effect.succeed({
                    id: runId,
                    status: "ERROR",
                    result: error.detail,
                  }),
                ),
              );
              const gitSummary = formatCloudGitSummary(run.git);
              const extra = [run.result, gitSummary].filter((part): part is string => Boolean(part));
              if (extra.length > 0) {
                const base = yield* stamp({ threadId, turnId });
                yield* emit({
                  ...base,
                  type: "content.delta",
                  payload: { streamKind: "assistant_text", delta: extra.join("\n\n") },
                });
              }
              yield* finalize(resultTurnState(run.status), run.status === "ERROR" ? run.result : undefined);
              return;
            }
            yield* finalize("failed", error.detail);
          }),
        ),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : finalize("failed", Cause.pretty(cause)),
        ),
      );
      if (context.activeTurnId === turnId) {
        yield* finalize("completed");
      }
    });

  const startSession: CursorCloudAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      if (settings.apiKey.trim().length === 0 && options.api === undefined) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: "Add a Cursor API key in Settings before starting a Cursor Cloud thread.",
        });
      }
      yield* stopLocalSession(input.threadId);
      const resumed = parseCursorCloudResume(input.resumeCursor);
      const model = input.modelSelection?.model;
      const modelParams = modelParamsFromSelection(input.modelSelection?.options);
      const createdAt = yield* nowIso;
      const stopped = yield* Ref.make(false);
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        createdAt,
        updatedAt: createdAt,
        ...(input.cwd ? { cwd: input.cwd } : {}),
        ...(model ? { model } : {}),
        ...(resumed ? { resumeCursor: cursorCloudResumeCursor(resumed.agentId) } : {}),
      };
      const context: CursorCloudSessionContext = {
        session,
        agentId: resumed?.agentId,
        cwd: input.cwd,
        model,
        modelParams,
        activeTurnId: undefined,
        activeRunId: undefined,
        lastEventId: undefined,
        streamFiber: undefined,
        stopped,
        toolItemIds: new Map(),
      };
      sessions.set(input.threadId, context);
      const base = yield* stamp({ threadId: input.threadId });
      yield* emit({
        ...base,
        type: "session.started",
        payload: {
          message: "Cursor Cloud session attached.",
          ...(resumed ? { resume: cursorCloudResumeCursor(resumed.agentId) } : {}),
        },
      });
      yield* emit({
        ...base,
        type: "thread.started",
        payload: {
          ...(resumed ? { providerThreadId: resumed.agentId } : {}),
        },
      });
      return session;
    });

  const sendTurn: CursorCloudAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* ensureSession(input.threadId);
      if (context.activeTurnId !== undefined) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue:
            "Cursor Cloud cannot steer an in-flight run. Wait for it to finish or interrupt it, then send a follow-up.",
        });
      }
      const text = input.input?.trim() ?? "";
      if (text.length === 0 && (input.attachments?.length ?? 0) === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Cursor Cloud needs a prompt.",
        });
      }

      const prompt = yield* loadPrompt({
        threadId: input.threadId,
        text,
        attachments: input.attachments,
      });
      const turnId = TurnId.make(yield* randomUUIDv4);
      context.activeTurnId = turnId;
      context.session = { ...context.session, status: "running", activeTurnId: turnId };
      const turnStart = yield* stamp({ threadId: input.threadId, turnId });
      yield* emit({
        ...turnStart,
        type: "turn.started",
        payload: { ...(context.model ? { model: context.model } : {}) },
      });

      const modelSelection =
        context.model && context.model.length > 0
          ? {
              id: context.model,
              ...(context.modelParams.length > 0 ? { params: context.modelParams } : {}),
            }
          : undefined;

      yield* Effect.gen(function* () {
        if (!context.agentId) {
          if (!context.cwd) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Cursor Cloud needs a project directory so it can find the GitHub remote.",
            });
          }
          const git = yield* resolveGitTarget(context.cwd).pipe(
            Effect.mapError(
              (error) =>
                new ProviderAdapterValidationError({
                  provider: PROVIDER,
                  operation: "sendTurn",
                  issue: error.detail,
                  cause: error,
                }),
            ),
          );
          const created = yield* api
            .createAgent({
              prompt,
              ...(modelSelection ? { model: modelSelection } : {}),
              repos: [{ url: git.repoUrl, startingRef: git.startingRef }],
              autoCreatePR: settings.autoCreatePR,
              workOnCurrentBranch: false,
            })
            .pipe(Effect.mapError((error) => toAdapterRequestError(error, "POST /v1/agents")));
          context.agentId = created.agent.id;
          context.activeRunId = created.run.id || created.agent.latestRunId;
          if (created.agent.name && created.agent.name.trim().length > 0) {
            const meta = yield* stamp({ threadId: input.threadId, turnId });
            yield* emit({
              ...meta,
              type: "thread.metadata.updated",
              payload: { name: created.agent.name.trim() },
            });
          }
          return;
        }
        const created = yield* api.createRun(context.agentId, { prompt }).pipe(
          Effect.catch((error: CursorCloudApiError) => {
            if (error.status !== 409) {
              return Effect.fail(toAdapterRequestError(error, "POST /v1/agents/{id}/runs"));
            }
            const agentId = context.agentId;
            if (!agentId) {
              return Effect.fail(toAdapterRequestError(error, "POST /v1/agents/{id}/runs"));
            }
            return api.getAgent(agentId).pipe(
              Effect.catch(() => Effect.succeed({ id: agentId, latestRunId: undefined })),
              Effect.flatMap((agent) =>
                agent.latestRunId
                  ? api.streamRun(agentId, agent.latestRunId).pipe(Stream.runDrain, Effect.ignore)
                  : Effect.void,
              ),
              Effect.flatMap(() =>
                api.createRun(agentId, { prompt }).pipe(
                  Effect.mapError((retryError) =>
                    toAdapterRequestError(retryError, "POST /v1/agents/{id}/runs"),
                  ),
                ),
              ),
            );
          }),
        );
        context.activeRunId = created.run.id;
      }).pipe(
        Effect.tapError((error) =>
          Effect.gen(function* () {
            context.activeTurnId = undefined;
            context.activeRunId = undefined;
            const base = yield* stamp({ threadId: input.threadId, turnId });
            yield* emit({
              ...base,
              type: "turn.completed",
              payload: {
                state: "failed",
                errorMessage: error.message,
              },
            });
          }),
        ),
      );

      const agentId = context.agentId;
      const runId = context.activeRunId;
      if (!agentId || !runId) {
        context.activeTurnId = undefined;
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "sendTurn",
          detail: "Cursor Cloud did not return a run id.",
        });
      }
      const resumeCursor = cursorCloudResumeCursor(agentId);
      context.session = { ...context.session, resumeCursor };
      // Adapter runtime, not sendTurn: that fiber finishes before SSE ends.
      context.streamFiber = runFork(
        consumeRunStream(context, input.threadId, turnId, agentId, runId),
      );
      return { threadId: input.threadId, turnId, resumeCursor };
    });

  const interruptTurn: CursorCloudAdapterShape["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = yield* ensureSession(threadId);
      const runId = context.activeRunId;
      const agentId = context.agentId;
      if (agentId && runId) {
        yield* api.cancelRun(agentId, runId).pipe(
          Effect.catch((error: CursorCloudApiError) =>
            error.status === 409 ? Effect.void : Effect.fail(toAdapterRequestError(error, "cancel")),
          ),
        );
      }
      const turnId = context.activeTurnId;
      yield* interruptStream(context);
      if (turnId) {
        context.activeTurnId = undefined;
        context.activeRunId = undefined;
        const base = yield* stamp({ threadId, turnId });
        yield* emit({
          ...base,
          type: "turn.aborted",
          payload: { reason: "interrupted" },
        });
      }
    });

  const applyThreadLifecycle: NonNullable<CursorCloudAdapterShape["applyThreadLifecycle"]> = (
    threadId,
    action,
    resumeCursor,
  ) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      const agentId =
        context?.agentId ?? parseCursorCloudResume(resumeCursor)?.agentId ?? parseCursorCloudResume(context?.session.resumeCursor)?.agentId;
      if (!agentId) return;
      if (action === "archive") {
        yield* api.archiveAgent(agentId).pipe(
          Effect.mapError((error) => toAdapterRequestError(error, "archive")),
        );
        return;
      }
      if (action === "unarchive") {
        yield* api.unarchiveAgent(agentId).pipe(
          Effect.mapError((error) => toAdapterRequestError(error, "unarchive")),
        );
        return;
      }
      yield* api.deleteAgent(agentId).pipe(
        Effect.mapError((error) => toAdapterRequestError(error, "delete")),
      );
    });

  const adapter: CursorCloudAdapterShape = {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: () =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToRequest",
          issue: "Cursor Cloud agents do not pause for T3 approvals.",
        }),
      ),
    respondToUserInput: () =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToUserInput",
          issue: "Cursor Cloud agents do not request T3 user input.",
        }),
      ),
    stopSession: (threadId) => stopLocalSession(threadId),
    applyThreadLifecycle,
    listSessions: () => Effect.succeed([...sessions.values()].map((context) => context.session)),
    hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
    readThread: (threadId) =>
      Effect.succeed({
        threadId,
        turns: [],
      }),
    rollbackThread: () =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Cursor Cloud does not support local turn rollback. Revert on the pull request.",
        }),
      ),
    stopAll: () =>
      Effect.forEach([...sessions.keys()], (threadId) => stopLocalSession(threadId), {
        concurrency: "unbounded",
        discard: true,
      }),
    streamEvents: Stream.fromQueue(runtimeEvents),
  };

  return adapter;
});
