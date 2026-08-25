import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CursorCloudSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { ServerConfig } from "../../config.ts";
import { makeCursorCloudAdapter } from "./CursorCloudAdapter.ts";
import type { CursorCloudApi, CursorCloudRun } from "./CursorCloudApi.ts";
import { CursorCloudApiError } from "./CursorCloudApi.ts";
import type { CursorCloudStreamEvent } from "../cursorCloud/CursorCloudProtocol.ts";

const decodeSettings = Schema.decodeSync(CursorCloudSettings);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);

const TestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "cursor-cloud-adapter-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, new Response("{}", { status: 200 }))),
      ),
    ),
  ),
);

const settings = decodeSettings({
  enabled: true,
  apiKey: "crsr_test",
  autoCreatePR: true,
});

const finishedRun = (id: string): CursorCloudRun => ({
  id,
  status: "FINISHED",
  result: "All done.",
});

const makeMockApi = () => {
  const state = {
    createAgentCalls: 0,
    createRunCalls: 0,
    archiveCalls: [] as string[],
    unarchiveCalls: [] as string[],
    deleteCalls: [] as string[],
    cancelCalls: [] as string[],
    lastCreatePrompt: "",
    lastFollowUpPrompt: "",
    createRunError: null as CursorCloudApiError | null,
    hangRuns: new Set<string>(),
    streamEventsByRun: new Map<string, ReadonlyArray<CursorCloudStreamEvent>>(),
    getRunById: new Map<string, CursorCloudRun>(),
  };

  const api: CursorCloudApi = {
    getMe: () => Effect.succeed({ userEmail: "dev@example.com" }),
    listModels: () => Effect.succeed([]),
    createAgent: (input) =>
      Effect.sync(() => {
        state.createAgentCalls += 1;
        state.lastCreatePrompt = input.prompt.text;
        state.streamEventsByRun.set("run-1", [
          { event: "assistant", data: { text: "Working on it." } },
          {
            event: "result",
            data: {
              status: "FINISHED",
              text: "Opened the PR.",
              git: {
                branches: [
                  {
                    repoUrl: "https://github.com/org/repo",
                    prUrl: "https://github.com/org/repo/pull/12",
                  },
                ],
              },
            },
          },
        ]);
        return {
          agent: { id: "bc-1", name: "Fix the flaky test", latestRunId: "run-1" },
          run: { id: "run-1", status: "CREATING" },
        };
      }),
    createRun: (_agentId, input) =>
      Effect.gen(function* () {
        if (state.createRunError) {
          const error = state.createRunError;
          state.createRunError = null;
          return yield* error;
        }
        state.createRunCalls += 1;
        state.lastFollowUpPrompt = input.prompt.text;
        state.streamEventsByRun.set("run-2", [
          { event: "assistant", data: { text: "Follow-up done." } },
          { event: "result", data: { status: "FINISHED", text: "Updated." } },
        ]);
        return { run: { id: "run-2", status: "CREATING" } };
      }),
    getAgent: (agentId) =>
      Effect.succeed({ id: agentId, latestRunId: "run-busy" }),
    getRun: (_agentId, runId) =>
      Effect.succeed(state.getRunById.get(runId) ?? finishedRun(runId)),
    streamRun: (_agentId, runId) => {
      if (state.hangRuns.has(runId)) {
        const prefix = state.streamEventsByRun.get(runId) ?? [];
        return Stream.fromIterable(prefix.slice(0, 1)).pipe(Stream.concat(Stream.never));
      }
      const events = state.streamEventsByRun.get(runId);
      if (events) {
        return Stream.fromIterable(events);
      }
      return Stream.fail(
        new CursorCloudApiError({
          method: "GET /v1/agents/{id}/runs/{runId}/stream",
          status: 410,
          code: "stream_expired",
          detail: "Stream expired.",
        }),
      );
    },
    cancelRun: (agentId, runId) =>
      Effect.sync(() => {
        state.cancelCalls.push(`${agentId}:${runId}`);
      }),
    archiveAgent: (agentId) =>
      Effect.sync(() => {
        state.archiveCalls.push(agentId);
      }),
    unarchiveAgent: (agentId) =>
      Effect.sync(() => {
        state.unarchiveCalls.push(agentId);
      }),
    deleteAgent: (agentId) =>
      Effect.sync(() => {
        state.deleteCalls.push(agentId);
      }),
  };

  return { api, state };
};

const collectUntil = <A extends { readonly type: string }>(
  stream: Stream.Stream<A>,
  predicate: (event: A) => boolean,
) =>
  Effect.gen(function* () {
    const events = yield* Ref.make<A[]>([]);
    const done = yield* Deferred.make<void>();
    yield* stream.pipe(
      Stream.runForEach((event) =>
        Ref.update(events, (current) => [...current, event]).pipe(
          Effect.zipRight(
            predicate(event)
              ? Deferred.succeed(done, undefined).pipe(Effect.ignore)
              : Effect.void,
          ),
        ),
      ),
      Effect.forkScoped({ startImmediately: true }),
    );
    return {
      wait: Deferred.await(done),
      snapshot: Ref.get(events),
    };
  });

it.layer(TestLayer)("CursorCloudAdapter", (it) => {
  it.effect("creates an agent on the first turn and stores a resume cursor", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-create");
      const collected = yield* collectUntil(
        adapter.streamEvents,
        (event) => event.type === "turn.completed",
      );

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
      });
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "Fix the flaky test",
      });
      yield* collected.wait;

      assert.equal(state.createAgentCalls, 1);
      assert.equal(state.createRunCalls, 0);
      assert.deepEqual(turn.resumeCursor, { schemaVersion: 1, agentId: "bc-1" });
      const events = yield* collected.snapshot;
      assert.isTrue(
        events.some(
          (event) =>
            event.type === "content.delta" &&
            event.payload.delta.includes("https://github.com/org/repo/pull/12"),
        ),
      );
    }),
  );

  it.effect("uses /runs for a follow-up on the stored agent", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-follow-up");
      const first = yield* collectUntil(
        adapter.streamEvents,
        (event) => event.type === "turn.completed",
      );

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        resumeCursor: { schemaVersion: 1, agentId: "bc-persisted" },
      });
      yield* adapter.sendTurn({ threadId, input: "Keep going" });
      yield* first.wait;

      assert.equal(state.createAgentCalls, 0);
      assert.equal(state.createRunCalls, 1);
      assert.equal(state.lastFollowUpPrompt, "Keep going");
    }),
  );

  it.effect("does not archive the cloud agent on local stopSession", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-stop");
      const collected = yield* collectUntil(
        adapter.streamEvents,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
      });
      yield* adapter.sendTurn({ threadId, input: "Ship it" });
      yield* collected.wait;
      yield* adapter.stopSession(threadId);

      assert.deepEqual(state.archiveCalls, []);
      assert.deepEqual(state.deleteCalls, []);

      yield* adapter.applyThreadLifecycle?.(threadId, "archive", {
        schemaVersion: 1,
        agentId: "bc-1",
      });
      assert.deepEqual(state.archiveCalls, ["bc-1"]);
    }),
  );

  it.effect("falls back to the run result when the SSE stream has expired", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      state.getRunById.set("run-2", {
        id: "run-2",
        status: "FINISHED",
        result: "Recovered from GET run.",
      });
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-expired");
      const collected = yield* collectUntil(
        adapter.streamEvents,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        resumeCursor: { schemaVersion: 1, agentId: "bc-1" },
      });
      state.streamEventsByRun.delete("run-2");
      yield* adapter.sendTurn({ threadId, input: "Recover" });
      yield* collected.wait;
      const events = yield* collected.snapshot;
      assert.isTrue(
        events.some(
          (event) =>
            event.type === "content.delta" && event.payload.delta.includes("Recovered from GET run."),
        ),
      );
    }),
  );

  it.effect("waits out 409 agent_busy then retries the follow-up run", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      state.createRunError = new CursorCloudApiError({
        method: "POST /v1/agents/{id}/runs",
        status: 409,
        code: "agent_busy",
        detail: "Agent is already running a turn.",
      });
      state.streamEventsByRun.set("run-busy", [
        { event: "result", data: { status: "FINISHED" } },
      ]);
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-busy");
      const collected = yield* collectUntil(
        adapter.streamEvents,
        (event) => event.type === "turn.completed",
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
        resumeCursor: { schemaVersion: 1, agentId: "bc-persisted" },
      });
      yield* adapter.sendTurn({ threadId, input: "Retry after busy" });
      yield* collected.wait;
      assert.equal(state.createAgentCalls, 0);
      assert.equal(state.createRunCalls, 1);
      assert.equal(state.lastFollowUpPrompt, "Retry after busy");
    }),
  );

  it.effect("cancels the in-flight cloud run on interrupt", () =>
    Effect.gen(function* () {
      const { api, state } = makeMockApi();
      state.hangRuns.add("run-1");
      const adapter = yield* makeCursorCloudAdapter(settings, {
        instanceId: ProviderInstanceId.make("cursorCloud"),
        api,
        resolveGitTarget: () =>
          Effect.succeed({ repoUrl: "https://github.com/org/repo", startingRef: "main" }),
      });
      const threadId = asThreadId("thread-cloud-cancel");
      const events = yield* Ref.make<Array<{ readonly type: string }>>([]);
      const sawDelta = yield* Deferred.make<void>();
      const sawAbort = yield* Deferred.make<void>();
      yield* adapter.streamEvents.pipe(
        Stream.runForEach((event) =>
          Ref.update(events, (current) => [...current, event]).pipe(
            Effect.zipRight(
              event.type === "content.delta"
                ? Deferred.succeed(sawDelta, undefined).pipe(Effect.ignore)
                : event.type === "turn.aborted"
                  ? Deferred.succeed(sawAbort, undefined).pipe(Effect.ignore)
                  : Effect.void,
            ),
          ),
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* adapter.startSession({
        provider: ProviderDriverKind.make("cursorCloud"),
        threadId,
        runtimeMode: "full-access",
        cwd: "/tmp/project",
      });
      yield* adapter.sendTurn({ threadId, input: "Long running" });
      yield* Deferred.await(sawDelta);
      yield* adapter.interruptTurn(threadId);
      yield* Deferred.await(sawAbort);
      assert.deepEqual(state.cancelCalls, ["bc-1:run-1"]);
    }),
  );
});
