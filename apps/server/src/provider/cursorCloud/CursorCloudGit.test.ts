import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { collectStreamAsString } from "../providerSnapshot.ts";
import { CursorCloudGitError, resolveCursorCloudGitTarget } from "./CursorCloudGit.ts";

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

it.layer(NodeServices.layer)("resolveCursorCloudGitTarget", (it) => {
  it.effect("resolves a github origin to https://github.com/org/repo", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "cursor-cloud-git-ok-" });
      const init = yield* runGit(cwd, ["init"]);
      assert.equal(init.code, 0);
      const remote = yield* runGit(cwd, [
        "remote",
        "add",
        "origin",
        "git@github.com:org/repo.git",
      ]);
      assert.equal(remote.code, 0, remote.stderr);
      yield* runGit(cwd, ["checkout", "-b", "feature"]);
      const target = yield* resolveCursorCloudGitTarget(cwd);
      assert.equal(target.repoUrl, "https://github.com/org/repo");
      assert.equal(target.startingRef, "feature");
    }),
  );

  it.effect("fails clearly when there is no github remote", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "cursor-cloud-git-missing-" });
      yield* runGit(cwd, ["init"]);
      yield* runGit(cwd, ["remote", "add", "origin", "git@gitlab.com:org/repo.git"]);
      const error = yield* resolveCursorCloudGitTarget(cwd).pipe(Effect.flip);
      assert.instanceOf(error, CursorCloudGitError);
      assert.match(error.detail, /GitHub/);
    }),
  );

  it.effect("fails clearly when the directory is not a git repo", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "cursor-cloud-git-plain-" });
      const error = yield* resolveCursorCloudGitTarget(cwd).pipe(Effect.flip);
      assert.instanceOf(error, CursorCloudGitError);
      assert.match(error.detail, /Git clone/i);
    }),
  );
});
