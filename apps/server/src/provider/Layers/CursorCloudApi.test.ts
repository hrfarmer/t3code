import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { CursorCloudApiError, makeCursorCloudApi } from "./CursorCloudApi.ts";

const jsonResponse = (
  request: HttpClientRequest.HttpClientRequest,
  body: unknown,
  status = 200,
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    }),
  );

const sseResponse = (
  request: HttpClientRequest.HttpClientRequest,
  body: string,
  status = 200,
) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(body, {
      status,
      headers: { "content-type": "text/event-stream" },
    }),
  );

it.effect("creates an agent, follows up, cancels, and archives", () =>
  Effect.gen(function* () {
    const seen: string[] = [];
    const client = HttpClient.make((request) => {
      seen.push(`${request.method} ${request.url}`);
      if (request.method === "POST" && request.url.endsWith("/v1/agents")) {
        return Effect.succeed(
          jsonResponse(request, {
            agent: { id: "bc-1", name: "Fix the bug", latestRunId: "run-1" },
            run: { id: "run-1", status: "CREATING" },
          }),
        );
      }
      if (request.method === "POST" && request.url.endsWith("/v1/agents/bc-1/runs")) {
        return Effect.succeed(jsonResponse(request, { id: "run-2", status: "CREATING" }));
      }
      if (request.method === "POST" && request.url.endsWith("/v1/agents/bc-1/runs/run-2/cancel")) {
        return Effect.succeed(jsonResponse(request, {}));
      }
      if (request.method === "POST" && request.url.endsWith("/v1/agents/bc-1/archive")) {
        return Effect.succeed(jsonResponse(request, {}));
      }
      if (request.method === "POST" && request.url.endsWith("/v1/agents/bc-1/unarchive")) {
        return Effect.succeed(jsonResponse(request, {}));
      }
      return Effect.succeed(jsonResponse(request, { error: "not found" }, 404));
    });

    const api = makeCursorCloudApi({
      httpClient: client,
      apiKey: "crsr_test",
    });

    const created = yield* api.createAgent({
      prompt: { text: "Fix the flaky test" },
      repos: [{ url: "https://github.com/org/repo", startingRef: "main" }],
      autoCreatePR: true,
      workOnCurrentBranch: false,
    });
    assert.equal(created.agent.id, "bc-1");
    assert.equal(created.run.id, "run-1");

    const followUp = yield* api.createRun("bc-1", { prompt: { text: "Also add a test" } });
    assert.equal(followUp.run.id, "run-2");

    yield* api.cancelRun("bc-1", "run-2");
    yield* api.archiveAgent("bc-1");
    yield* api.unarchiveAgent("bc-1");

    assert.deepEqual(seen, [
      "POST https://api.cursor.com/v1/agents",
      "POST https://api.cursor.com/v1/agents/bc-1/runs",
      "POST https://api.cursor.com/v1/agents/bc-1/runs/run-2/cancel",
      "POST https://api.cursor.com/v1/agents/bc-1/archive",
      "POST https://api.cursor.com/v1/agents/bc-1/unarchive",
    ]);
  }),
);

it.effect("surfaces 409 agent_busy on follow-up", () =>
  Effect.gen(function* () {
    const client = HttpClient.make((request) =>
      Effect.succeed(
        jsonResponse(
          request,
          { code: "agent_busy", message: "Agent is already running a turn." },
          409,
        ),
      ),
    );
    const api = makeCursorCloudApi({ httpClient: client, apiKey: "crsr_test" });
    const error = yield* api.createRun("bc-1", { prompt: { text: "follow up" } }).pipe(Effect.flip);
    assert.equal(error._tag, "CursorCloudApiError");
    assert.equal(error.status, 409);
    assert.equal(error.code, "agent_busy");
  }),
);

it.effect("parses the run SSE stream and sends Last-Event-ID on resume", () =>
  Effect.gen(function* () {
    let lastEventId: string | undefined;
    const client = HttpClient.make((request) => {
      lastEventId = request.headers["last-event-id"];
      return Effect.succeed(
        sseResponse(
          request,
          [
            "id: 1",
            "event: assistant",
            'data: {"text":"Hello"}',
            "",
            "id: 2",
            "event: thinking",
            'data: {"text":"..."}',
            "",
            "event: heartbeat",
            "data: {}",
            "",
            "id: 3",
            "event: result",
            'data: {"status":"FINISHED","text":"Done","git":{"branches":[{"prUrl":"https://github.com/org/repo/pull/1"}]}}',
            "",
          ].join("\n"),
        ),
      );
    });
    const api = makeCursorCloudApi({ httpClient: client, apiKey: "crsr_test" });
    const events = yield* Stream.runCollect(api.streamRun("bc-1", "run-1", "evt-prev"));
    assert.equal(lastEventId, "evt-prev");
    assert.equal(events.length, 3);
    assert.equal(events[0]?.event, "assistant");
    assert.equal(events[1]?.event, "thinking");
    assert.equal(events[2]?.event, "result");
  }),
);

it.effect("fails a 410 expired stream with stream_expired", () =>
  Effect.gen(function* () {
    const client = HttpClient.make((request) =>
      Effect.succeed(
        jsonResponse(request, { code: "stream_expired", message: "Stream expired." }, 410),
      ),
    );
    const api = makeCursorCloudApi({ httpClient: client, apiKey: "crsr_test" });
    const error = yield* Stream.runDrain(api.streamRun("bc-1", "run-1")).pipe(Effect.flip);
    assert.instanceOf(error, CursorCloudApiError);
    assert.equal(error.status, 410);
    assert.equal(error.code, "stream_expired");
  }),
);
