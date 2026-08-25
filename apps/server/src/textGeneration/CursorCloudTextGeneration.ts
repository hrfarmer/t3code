import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { TextGeneration } from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  new TextGenerationError({
    operation,
    detail:
      "Cursor Cloud does not generate local git text. Review the pull request the cloud agent opens.",
  });

export const makeCursorCloudTextGeneration = (): TextGeneration["Service"] => ({
  generateCommitMessage: () => Effect.fail(unsupported("generateCommitMessage")),
  generatePrContent: () => Effect.fail(unsupported("generatePrContent")),
  generateBranchName: () => Effect.fail(unsupported("generateBranchName")),
  generateThreadTitle: () => Effect.fail(unsupported("generateThreadTitle")),
});
