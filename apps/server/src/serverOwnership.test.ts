import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { SERVER_EXIT_CODE_STATE_DIR_OWNED } from "@t3tools/contracts";
import {
  HostProcessEnvironment,
  HostProcessPlatform,
  HostProcessUserId,
} from "@t3tools/shared/hostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProcessRunner from "./processRunner.ts";
import * as ServerOwnership from "./serverOwnership.ts";
import { acquireServerOwnershipLock } from "./serverOwnershipLock.ts";
import { renderBootServicePlist, renderBootServiceUnit } from "./cloud/bootService.ts";
import {
  BOOT_SERVICE_LAUNCHD_LABEL,
  BOOT_SERVICE_PLIST_FILE,
  BOOT_SERVICE_UNIT_ENV,
  BOOT_SERVICE_UNIT_FILE,
} from "./cloud/bootServiceConfig.ts";
import {
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_LAUNCHER_PROTOCOL,
} from "./cloud/serviceProtocol.ts";

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isLegacyBootServiceError = Schema.is(ServerOwnership.LegacyBootServiceError);

const makeHarness = Effect.fn("test.makeOwnershipHarness")(function* (
  platform: NodeJS.Platform = "darwin",
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* fs.makeTempDirectoryScoped({ prefix: "t3-legacy-boot-owner-" });
  const baseDir = path.join(home, "T3 Data & Home");
  const stateDir = path.join(baseDir, "userdata");
  const statePath = path.join(stateDir, "server-runtime.json");
  const version = "0.0.42";
  const runtime = path.join(baseDir, "runtime", "versions", version, "t3");
  const unitPath =
    platform === "darwin"
      ? path.join(home, "Library", "LaunchAgents", BOOT_SERVICE_PLIST_FILE)
      : path.join(home, ".config", "systemd", "user", BOOT_SERVICE_UNIT_FILE);
  yield* fs.makeDirectory(stateDir, { recursive: true });
  yield* fs.makeDirectory(path.join(baseDir, "runtime"));
  yield* fs.makeDirectory(path.dirname(unitPath), { recursive: true });
  yield* fs.writeFileString(
    path.join(baseDir, "runtime", "service-state.json"),
    yield* encodeJson({
      protocol: 1,
      activeVersion: version,
    }),
  );
  let servedBaseDir = baseDir;
  const writeUnit = (servedHome = baseDir) => {
    servedBaseDir = servedHome;
    const plan = {
      program: [path.join(servedHome, "runtime", "versions", version, "t3"), "__service-launcher"],
      baseDir: servedHome,
      unitPath,
      logPath: path.join(stateDir, "logs", "boot-service.log"),
    };
    return fs.writeFileString(
      unitPath,
      platform === "darwin"
        ? renderBootServicePlist(plan, { homeDir: home, environmentPath: "/usr/bin" })
        : renderBootServiceUnit(plan),
    );
  };
  yield* writeUnit();
  const calls: ProcessRunner.ProcessRunInput[] = [];
  const control = {
    loaded: true,
    disabled: false,
    booleanDisabledOutput: false,
    active: true,
    enabled: true,
    ownership: false,
    preflightCode: 0,
    failCommand: "",
    failure: "spawn",
  };
  const runner = ProcessRunner.ProcessRunner.of({
    run: (input) =>
      Effect.gen(function* () {
        calls.push(input);
        assert.isDefined(input.timeout);
        if (input.command === control.failCommand) {
          if (control.failure === "timeout")
            return yield* new ProcessRunner.ProcessTimeoutError({
              command: input.command,
              argumentCount: input.args.length,
              timeoutMs: Duration.toMillis(Duration.fromInputUnsafe(input.timeout ?? 0)),
            });
          return yield* new ProcessRunner.ProcessSpawnError({
            command: input.command,
            argumentCount: input.args.length,
            cause: new Error("command missing"),
          });
        }
        let code = 0;
        let stdout = "";
        if (input.command === "launchctl") {
          if (input.args[0] === "print") code = control.loaded ? 0 : 1;
          else if (input.args[0] === "print-disabled") {
            const state = control.booleanDisabledOutput
              ? String(control.disabled)
              : control.disabled
                ? "disabled"
                : "enabled";
            stdout = `disabled services = {\n "${BOOT_SERVICE_LAUNCHD_LABEL}" => ${state}\n}`;
          } else return yield* Effect.die("Unexpected launchctl operation");
        } else if (input.command === "systemctl") {
          if (input.args[1] === "is-active") {
            code = control.active ? 0 : 3;
            stdout = control.active ? "active\n" : "inactive\n";
          } else if (input.args[1] === "is-enabled") {
            code = control.enabled ? 0 : 1;
            stdout = control.enabled ? "enabled\n" : "disabled\n";
          } else return yield* Effect.die("Unexpected systemctl operation");
        } else if (
          input.command === path.join(servedBaseDir, "runtime", "versions", version, "t3")
        ) {
          assert.deepEqual(input.args, [
            "__service-preflight",
            "--database-path",
            path.join(servedBaseDir, "userdata", "statev2.sqlite"),
            "--launcher-protocol",
            String(SERVICE_LAUNCHER_PROTOCOL),
          ]);
          code = control.preflightCode;
          stdout = yield* encodeJson({
            status: "ready",
            version,
            launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
            ...(control.ownership ? { ownershipProtocol: 1 } : {}),
          }).pipe(Effect.orDie);
        } else return yield* Effect.die("Unexpected process invocation");
        return {
          stdout,
          stderr: "",
          code: ChildProcessSpawner.ExitCode(code),
          timedOut: false,
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
          stderrInvalidUtf8: false,
        };
      }),
  });
  const acquire = (
    env: NodeJS.ProcessEnv = {},
    trial?: { readonly previousOwnerId: string | null; readonly ownerId: string },
    target = statePath,
  ) =>
    Effect.scoped(ServerOwnership.acquireServerOwnership(target, trial)).pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provideService(HostProcessPlatform, platform),
      Effect.provideService(HostProcessUserId, 501),
      Effect.provideService(HostProcessEnvironment, env),
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: { HOME: home } }))),
    );
  return {
    fs,
    path,
    home,
    baseDir,
    stateDir,
    statePath,
    version,
    runtime,
    unitPath,
    control,
    calls,
    acquire,
    writeUnit,
  };
});

describe("ownership beside a legacy boot service", () => {
  it.effect.each(["darwin", "linux"] as const)(
    "refuses a loaded pre-lock service for this home on %s",
    (platform) =>
      Effect.gen(function* () {
        const h = yield* makeHarness(platform);
        const error = yield* h.acquire().pipe(Effect.flip);
        assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
        if (isLegacyBootServiceError(error)) {
          assert.equal(error[Runtime.errorExitCode], SERVER_EXIT_CODE_STATE_DIR_OWNED);
          assert.include(error.message, h.version);
          assert.include(error.message, "t3 update");
          assert.include(error.message, "t3 service uninstall");
          assert.include(error.message, "No server was stopped.");
        }
        assert.isFalse(yield* h.fs.exists(h.statePath));
        const lock = yield* Effect.acquireRelease(
          Effect.promise(() => acquireServerOwnershipLock(h.stateDir)),
          (value) => Effect.sync(() => value.close()),
        );
        assert.isNull(lock.readOwnerId());
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["darwin", "linux"] as const)(
    "allows a service runtime that supports ownership on %s",
    (platform) =>
      Effect.gen(function* () {
        const h = yield* makeHarness(platform);
        h.control.ownership = true;
        yield* h.acquire();
        assert.isTrue(h.calls.some((call) => call.command === h.runtime));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each(["darwin", "linux"] as const)(
    "allows a unit serving a different home on %s",
    (platform) =>
      Effect.gen(function* () {
        const h = yield* makeHarness(platform);
        const otherHome = h.path.join(h.home, "other-home");
        yield* h.fs.makeDirectory(otherHome);
        yield* h.writeUnit(otherHome);
        yield* h.acquire();
        assert.deepEqual(h.calls, []);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([false, true])(
    "allows a booted-out and disabled launch agent (boolean output: %s)",
    (booleanOutput) =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        h.control.loaded = false;
        h.control.disabled = true;
        h.control.booleanDisabledOutput = booleanOutput;
        yield* h.acquire();
        assert.isFalse(h.calls.some((call) => call.command === h.runtime));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([false, true])(
    "refuses a booted-out launch agent that will load at login (boolean output: %s)",
    (booleanOutput) =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        h.control.loaded = false;
        h.control.booleanDisabledOutput = booleanOutput;
        const error = yield* h.acquire().pipe(Effect.flip);
        assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses a loaded launch agent even when it is disabled", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.control.disabled = true;
      const error = yield* h.acquire().pipe(Effect.flip);
      assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("allows an inactive and disabled systemd service", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness("linux");
      h.control.active = false;
      h.control.enabled = false;
      yield* h.acquire();
      assert.isFalse(h.calls.some((call) => call.command === h.runtime));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses an enabled legacy systemd service that will run at login", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness("linux");
      h.control.active = false;
      const error = yield* h.acquire().pipe(Effect.flip);
      assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("compares canonical home paths through symlinks", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const alias = h.path.join(h.home, "alias-home");
      yield* h.fs.symlink(h.baseDir, alias);
      yield* h.writeUnit(alias);
      const error = yield* h.acquire().pipe(Effect.flip);
      assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([SERVICE_LAUNCHER_CONTEXT_ENV, BOOT_SERVICE_UNIT_ENV])(
    "skips the service's own processes marked by %s",
    (key) =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        yield* h.acquire({ [key]: "service-context" });
        assert.deepEqual(h.calls, []);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("skips update trials", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* h.acquire({}, { previousOwnerId: null, ownerId: "trial-owner" });
      assert.deepEqual(h.calls, []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.each([
    { stage: "manager", failure: "spawn" },
    { stage: "manager", failure: "timeout" },
    { stage: "runtime", failure: "spawn" },
    { stage: "runtime", failure: "timeout" },
  ])("allows startup when the $stage probe fails to $failure", ({ stage, failure }) =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.control.failCommand = stage === "manager" ? "launchctl" : h.runtime;
      h.control.failure = failure;
      yield* h.acquire();
      assert.isTrue(h.calls.some((call) => call.command === h.control.failCommand));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("treats an unsupported preflight command as a pre-lock runtime", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      h.control.preflightCode = 1;
      const error = yield* h.acquire().pipe(Effect.flip);
      assert.instanceOf(error, ServerOwnership.LegacyBootServiceError);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("skips unsupported platforms", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness("win32");
      yield* h.acquire();
      assert.deepEqual(h.calls, []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not probe services when the SQLite lock is already held", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      yield* Effect.acquireRelease(
        Effect.promise(() => acquireServerOwnershipLock(h.stateDir)),
        (value) => Effect.sync(() => value.close()),
      );
      const error = yield* h.acquire().pipe(Effect.flip);
      assert.instanceOf(error, ServerOwnership.ServerAlreadyRunningError);
      assert.deepEqual(h.calls, []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
