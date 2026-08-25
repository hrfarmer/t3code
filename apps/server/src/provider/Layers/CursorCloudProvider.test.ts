import { assert, it } from "@effect/vitest";
import { CursorCloudSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { checkCursorCloudProviderStatus } from "./CursorCloudProvider.ts";

const decodeSettings = Schema.decodeSync(CursorCloudSettings);

it.effect("reports unauthenticated when no API key is configured", () =>
  Effect.gen(function* () {
    const snapshot = yield* checkCursorCloudProviderStatus(
      decodeSettings({ enabled: true, apiKey: "" }),
    ).pipe(
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response("{}", { status: 500 }))),
        ),
      ),
    );

    assert.equal(snapshot.auth.status, "unauthenticated");
    assert.match(snapshot.message ?? "", /API key/);
  }),
);

it.effect("builds an authenticated status card from GET /v1/me", () =>
  Effect.gen(function* () {
    const snapshot = yield* checkCursorCloudProviderStatus(
      decodeSettings({ enabled: true, apiKey: "crsr_test" }),
    ).pipe(
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) => {
          if (request.url.endsWith("/v1/me")) {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({
                  userEmail: "dev@example.com",
                  apiKeyName: "t3-dev",
                }),
              ),
            );
          }
          if (request.url.endsWith("/v1/models")) {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                Response.json({
                  items: [{ id: "composer-2", displayName: "Composer 2" }],
                }),
              ),
            );
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response("{}", { status: 404 })),
          );
        }),
      ),
    );

    assert.equal(snapshot.status, "ready");
    assert.equal(snapshot.auth.status, "authenticated");
    assert.equal(snapshot.auth.email, "dev@example.com");
    assert.equal(snapshot.models[0]?.slug, "composer-2");
    assert.match(snapshot.message ?? "", /Cursor VM/);
  }),
);
