import type {
  CursorCloudSettings,
  ModelCapabilities,
  ServerProviderAuth,
  ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { HttpClient } from "effect/unstable/http";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { makeCursorCloudApi, type CursorCloudModelInfo } from "./CursorCloudApi.ts";

const PRESENTATION = {
  displayName: "Cursor Cloud",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "composer-2",
    name: "Composer 2",
    isCustom: false,
    isDefault: true,
    capabilities: EMPTY_CAPABILITIES,
  },
];

export function buildInitialCursorCloudProviderSnapshot(
  settings: CursorCloudSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: false,
        checkedAt,
        models: FALLBACK_MODELS,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Cursor Cloud is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models: FALLBACK_MODELS,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Cursor Cloud API access...",
      },
    });
  });
}

const toServerModels = (models: ReadonlyArray<CursorCloudModelInfo>): ServerProviderModel[] => {
  if (models.length === 0) return [...FALLBACK_MODELS];
  return models.map((model, index) => {
    const optionDescriptors = (model.parameters ?? []).map((parameter) =>
      buildSelectOptionDescriptor({
        id: parameter.id,
        label: parameter.displayName ?? parameter.id,
        options: parameter.values.map((value) => ({
          value: value.value,
          label: value.displayName ?? value.value,
        })),
      }),
    );
    const isDefault = model.variants?.some((variant) => variant.isDefault === true) ?? index === 0;
    return {
      slug: model.id,
      name: model.displayName,
      isCustom: false,
      isDefault,
      capabilities:
        optionDescriptors.length > 0
          ? createModelCapabilities({ optionDescriptors })
          : EMPTY_CAPABILITIES,
    };
  });
};

export const checkCursorCloudProviderStatus = (
  settings: CursorCloudSettings,
): Effect.Effect<ServerProviderDraft, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    if (!settings.enabled) {
      return yield* buildInitialCursorCloudProviderSnapshot(settings);
    }

    const apiKey = settings.apiKey.trim();
    if (apiKey.length === 0) {
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: true,
        checkedAt,
        models: FALLBACK_MODELS,
        probe: {
          installed: true,
          version: null,
          status: "warning",
          auth: { status: "unauthenticated" },
          message:
            "Add a Cursor API key in Settings. Create one at cursor.com/dashboard/api, then paste it here.",
        },
      });
    }

    const httpClient = yield* HttpClient.HttpClient;
    const api = makeCursorCloudApi({
      httpClient,
      apiKey,
      apiEndpoint: settings.apiEndpoint,
    });

    const meResult = yield* api.getMe().pipe(Effect.result);
    if (meResult._tag === "Failure") {
      return buildServerProvider({
        presentation: PRESENTATION,
        enabled: true,
        checkedAt,
        models: FALLBACK_MODELS,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unauthenticated" },
          message: meResult.failure.detail,
        },
      });
    }

    const email = meResult.success.userEmail;
    const label = meResult.success.apiKeyName;
    const auth: ServerProviderAuth = {
      status: "authenticated",
      type: "apiKey",
      ...(email ? { email } : {}),
      ...(label ? { label } : {}),
    };

    const modelsResult = yield* api.listModels().pipe(Effect.result);
    const models = modelsResult._tag === "Success" ? toServerModels(modelsResult.success) : FALLBACK_MODELS;

    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "ready",
        auth,
        message: email
          ? `Signed in to Cursor Cloud as ${email}. Work runs on a Cursor VM, not this machine.`
          : "Signed in to Cursor Cloud. Work runs on a Cursor VM, not this machine.",
      },
    });
  });
