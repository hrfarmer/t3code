import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { isGitRepository } from "../../git/Utils.ts";
import { collectStreamAsString } from "../providerSnapshot.ts";
import { pickGitHubRemoteUrl } from "./CursorCloudProtocol.ts";

export class CursorCloudGitError extends Schema.TaggedErrorClass<CursorCloudGitError>()(
  "CursorCloudGitError",
  {
    cwd: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface CursorCloudGitTarget {
  readonly repoUrl: string;
  readonly startingRef: string;
}

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(ChildProcess.make("git", [...args], { cwd }));
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        collectStreamAsString(child.stdout),
        collectStreamAsString(child.stderr),
        child.exitCode.pipe(Effect.map(Number)),
      ],
      { concurrency: "unbounded" },
    );
    return { stdout, stderr, code: exitCode };
  }).pipe(Effect.scoped);

const parseRemoteLines = (
  stdout: string,
): Array<{ readonly name: string; readonly url: string }> => {
  const remotes = new Map<string, string>();
  for (const line of stdout.split("\n")) {
    const match = line.trim().match(/^(\S+)\s+(\S+)\s+\(fetch\)$/u);
    if (!match?.[1] || !match[2]) continue;
    remotes.set(match[1], match[2]);
  }
  return [...remotes.entries()].map(([name, url]) => ({ name, url }));
};

export const resolveCursorCloudGitTarget = (
  cwd: string,
): Effect.Effect<CursorCloudGitTarget, CursorCloudGitError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    if (!isGitRepository(cwd)) {
      return yield* new CursorCloudGitError({
        cwd,
        detail:
          "Cursor Cloud needs a GitHub repository. Open a project that is a Git clone of a GitHub repo.",
      });
    }

    const remotesResult = yield* runGit(cwd, ["remote", "-v"]).pipe(
      Effect.mapError(
        () =>
          new CursorCloudGitError({
            cwd,
            detail: "Could not read git remotes for this project.",
          }),
      ),
    );
    if (remotesResult.code !== 0) {
      return yield* new CursorCloudGitError({
        cwd,
        detail: remotesResult.stderr.trim() || "Could not read git remotes for this project.",
      });
    }

    const repoUrl = pickGitHubRemoteUrl(parseRemoteLines(remotesResult.stdout));
    if (!repoUrl) {
      return yield* new CursorCloudGitError({
        cwd,
        detail:
          "Cursor Cloud only works with GitHub remotes in v1. Add a github.com origin to this project.",
      });
    }

    const branchResult = yield* runGit(cwd, ["rev-parse", "--abbrev-ref", "HEAD"]).pipe(
      Effect.mapError(
        () =>
          new CursorCloudGitError({
            cwd,
            detail: "Could not read the current git branch.",
          }),
      ),
    );
    const branch = branchResult.stdout.trim();
    const startingRef = branch.length > 0 && branch !== "HEAD" ? branch : "main";

    return { repoUrl, startingRef };
  });
