// @effect-diagnostics nodeBuiltinImport:off - Publication must finish synchronously while the scope holds ownership.
// @effect-diagnostics schemaSyncInEffect:off - Descriptor validation and publication are synchronous under the ownership lock.
import {
  HostProcessEnvironment,
  HostProcessPlatform,
  HostProcessUserId,
} from "@t3tools/shared/hostProcess";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { SERVER_EXIT_CODE_STATE_DIR_OWNED } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";

import * as ProcessRunner from "./processRunner.ts";
import { acquireServerOwnershipLock, SERVER_UPDATE_RECOVERY_FILE } from "./serverOwnershipLock.ts";
import {
  serviceStateHasPendingUpdate,
  serviceStatePendingUpdateId,
  serviceStateActiveVersion,
  SERVICE_LAUNCHER_CONTEXT_ENV,
  SERVICE_STATE_FILE,
} from "./cloud/serviceProtocol.ts";
import {
  BOOT_SERVICE_LAUNCHD_LABEL,
  BOOT_SERVICE_PLIST_FILE,
  BOOT_SERVICE_UNIT_FILE,
  BOOT_SERVICE_UNIT_ENV,
  bootServiceBaseDirOf,
} from "./cloud/bootServiceConfig.ts";
import { runtimeOwnershipProbe, runtimeSupportsOwnership } from "./cloud/servicePreflight.ts";

import {
  isProcessAlive,
  readPersistedServerRuntimeState,
  PersistedServerRuntimeState,
} from "./serverRuntimeState.ts";

export class ServerAlreadyRunningError extends Schema.TaggedError<ServerAlreadyRunningError>()(
  "ServerAlreadyRunningError",
  { stateDir: Schema.String },
) {
  // Distinct process exit code so a supervisor can tell "owned by another
  // server" apart from a crash and stop restarting.
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return `A T3 Code server already owns ${this.stateDir}. Finish active agent work, stop that server through the app or terminal that started it, then retry this command with the same home directory. No server was stopped.`;
  }
}

export class ServerOwnershipError extends Schema.TaggedError<ServerOwnershipError>()(
  "ServerOwnershipError",
  { statePath: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Could not acquire or update server ownership at ${this.statePath}.`;
  }
}

export class LegacyBootServiceError extends Schema.TaggedError<LegacyBootServiceError>()(
  "LegacyBootServiceError",
  { stateDir: Schema.String, version: Schema.String },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return `A background service for this T3 home (${this.stateDir}) runs an older T3 Code version (${this.version}) that cannot share it safely. Run t3 update to update the service or t3 service uninstall to remove it, then retry. No server was stopped.`;
  }
}

export class ServerOwnershipReleasedError extends Schema.TaggedError<ServerOwnershipReleasedError>()(
  "ServerOwnershipReleasedError",
  { statePath: Schema.String },
) {
  override get message(): string {
    return `Cannot publish server runtime state after ownership was released at ${this.statePath}.`;
  }
}

export class ServerUpdateRecoveryRequiredError extends Schema.TaggedError<ServerUpdateRecoveryRequiredError>()(
  "ServerUpdateRecoveryRequiredError",
  { statePath: Schema.String },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;
  override get message(): string {
    return `An interrupted server update requires recovery at ${this.statePath}. Recover the database and service state before restarting. No database files were restored.`;
  }
}

const isServerUpdateRecoveryRequiredError = Schema.is(ServerUpdateRecoveryRequiredError);

const requireNoInterruptedRestore = (statePath: string) =>
  Effect.try({
    try: () => {
      const marker = NodePath.join(NodePath.dirname(statePath), SERVER_UPDATE_RECOVERY_FILE);
      if (NodeFS.existsSync(marker)) {
        throw new ServerUpdateRecoveryRequiredError({ statePath: marker });
      }
      const runtimeDir = NodePath.join(NodePath.dirname(NodePath.dirname(statePath)), "runtime");
      let contents: string;
      try {
        contents = NodeFS.readFileSync(NodePath.join(runtimeDir, "service-state.json"), "utf8");
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return;
        throw cause;
      }
      const updateId = serviceStatePendingUpdateId(contents);
      if (
        serviceStateHasPendingUpdate(contents) &&
        (updateId === undefined ||
          NodeFS.existsSync(NodePath.join(runtimeDir, "db-backup", updateId)))
      ) {
        throw new ServerUpdateRecoveryRequiredError({ statePath: runtimeDir });
      }
    },
    catch: (cause) =>
      isServerUpdateRecoveryRequiredError(cause)
        ? cause
        : new ServerOwnershipError({ statePath, cause }),
  });

const encodeRuntimeState = Schema.encodeSync(Schema.fromJsonString(PersistedServerRuntimeState));
const decodeRuntimeState = Schema.decodeUnknownSync(
  Schema.fromJsonString(PersistedServerRuntimeState),
);

/** Treat a legacy record as stale only when process start time proves PID reuse. */
const legacyOwnerIsLive = Effect.fn("legacyOwnerIsLive")(function* (
  state: PersistedServerRuntimeState,
) {
  if (!isProcessAlive(state.pid)) return false;
  const recordedAt = Date.parse(state.startedAt);
  if (!Number.isFinite(recordedAt)) return true;
  const platform = yield* HostProcessPlatform;
  const windows = platform === "win32";
  const runner = yield* ProcessRunner.ProcessRunner;
  const result = yield* runner
    .run({
      command: windows ? "powershell.exe" : "ps",
      args: windows
        ? [
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            `(Get-Process -Id ${state.pid} -ErrorAction Stop).StartTime.ToUniversalTime().ToString('o')`,
          ]
        : ["-p", String(state.pid), "-o", "lstart="],
      env: { LC_ALL: "C", TZ: "UTC" },
      timeout: Duration.seconds(2),
      maxOutputBytes: 16_384,
    })
    .pipe(Effect.option);
  if (Option.isNone(result) || result.value.code !== 0) return true;
  const output = result.value.stdout.trim();
  const startedAt = Date.parse(windows ? output : `${output} UTC`);
  // ps reports whole seconds. Unknown identity stays conservative, and no
  // process is ever signalled based on this comparison.
  return !Number.isFinite(startedAt) || startedAt <= recordedAt + 1_000;
});

const legacyBootServiceVersion = Effect.fn("legacyBootServiceVersion")(
  function* (statePath: string) {
    const env = yield* HostProcessEnvironment;
    if (env[SERVICE_LAUNCHER_CONTEXT_ENV] !== undefined || env[BOOT_SERVICE_UNIT_ENV] !== undefined)
      return;
    const platform = yield* HostProcessPlatform;
    if (platform !== "darwin" && platform !== "linux") return;
    const home = yield* Config.String("HOME").pipe(Config.withDefault(""));
    if (home === "") return;
    const uid = yield* HostProcessUserId;
    if (platform === "darwin" && uid === undefined) return;
    const fs = yield* FileSystem.FileSystem;
    const unit = yield* fs.readFileString(
      platform === "darwin"
        ? NodePath.join(home, "Library", "LaunchAgents", BOOT_SERVICE_PLIST_FILE)
        : NodePath.join(home, ".config", "systemd", "user", BOOT_SERVICE_UNIT_FILE),
    );
    const baseDir = bootServiceBaseDirOf(unit);
    if (baseDir === undefined) return;
    if (
      (yield* fs.realPath(baseDir)) !==
      (yield* fs.realPath(NodePath.dirname(NodePath.dirname(statePath))))
    )
      return;
    const runner = yield* ProcessRunner.ProcessRunner;
    const probe = (command: string, args: ReadonlyArray<string>) =>
      runner.run({ command, args, timeout: Duration.seconds(5), maxOutputBytes: 16_384 });
    if (platform === "darwin") {
      const loaded = yield* probe("launchctl", [
        "print",
        `gui/${uid}/${BOOT_SERVICE_LAUNCHD_LABEL}`,
      ]);
      if (loaded.code !== 0) {
        const disabled = yield* probe("launchctl", ["print-disabled", `gui/${uid}`]);
        // macOS 13+ prints `=> disabled`; earlier releases print `=> true`.
        if (
          disabled.code !== 0 ||
          [
            `"${BOOT_SERVICE_LAUNCHD_LABEL}" => disabled`,
            `"${BOOT_SERVICE_LAUNCHD_LABEL}" => true`,
          ].some((line) => disabled.stdout.includes(line))
        )
          return;
      }
    } else {
      const active = yield* probe("systemctl", ["--user", "is-active", BOOT_SERVICE_UNIT_FILE]);
      if (active.code !== 0) {
        const enabled = yield* probe("systemctl", ["--user", "is-enabled", BOOT_SERVICE_UNIT_FILE]);
        if (enabled.code !== 0 || enabled.stdout.trim() !== "enabled") return;
      }
    }
    // The state file and `__service-preflight` shipped in the same release, so
    // every runtime named here answers the probe instead of starting a server.
    const version = serviceStateActiveVersion(
      yield* fs.readFileString(NodePath.join(baseDir, "runtime", SERVICE_STATE_FILE)),
    );
    if (version === undefined) return;
    const capability = runtimeOwnershipProbe(
      NodePath.join(baseDir, "runtime", "versions", version, "t3"),
      NodePath.join(baseDir, "userdata", "statev2.sqlite"),
    );
    const result = yield* runner.run({
      command: capability.command,
      args: capability.args,
      timeout: Duration.millis(capability.timeoutMs),
      maxOutputBytes: capability.maxOutputBytes,
    });
    if (result.timedOut) return;
    if (result.code !== 0 || !runtimeSupportsOwnership(result.stdout, version)) return version;
  },
  Effect.orElseSucceed(() => undefined),
);

/**
 * Hold an OS file lock until the server and its finalizers stop. This separate
 * SQLite file never contains application data and must never be unlinked.
 * SQLite releases the lock on process exit, including SIGKILL. No PID is killed
 * and no heartbeat can expire while a live server is paused.
 */
export const acquireServerOwnership = Effect.fn("acquireServerOwnership")(function* (
  statePath: string,
  trial?: { readonly previousOwnerId: string | null; readonly ownerId: string },
) {
  const crypto = yield* Crypto.Crypto;
  const ownerId =
    trial?.ownerId ??
    (yield* crypto.randomUUIDv4.pipe(
      Effect.mapError((cause) => new ServerOwnershipError({ statePath, cause })),
    ));
  const resource = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const lock = await acquireServerOwnershipLock(NodePath.dirname(statePath));
        return {
          lock,
          path: NodePath.join(lock.stateDir, NodePath.basename(statePath)),
          active: true,
        };
      },
      catch: (cause) =>
        cause instanceof Error &&
        (("errcode" in cause && cause.errcode === 5) ||
          ("code" in cause && cause.code === "SQLITE_BUSY"))
          ? new ServerAlreadyRunningError({ stateDir: NodePath.dirname(statePath) })
          : new ServerOwnershipError({ statePath, cause }),
    }),
    (resource) =>
      Effect.gen(function* () {
        resource.active = false;
        const state = yield* readPersistedServerRuntimeState(resource.path);
        yield* Effect.try({
          try: () => {
            if (Option.isSome(state) && state.value.ownerId === ownerId) {
              NodeFS.rmSync(resource.path, { force: true });
            }
          },
          catch: (cause) => new ServerOwnershipError({ statePath, cause }),
        }).pipe(Effect.ignore({ log: true }));
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            resource.active = false;
            resource.lock.close();
          }),
        ),
      ),
  );

  if (trial === undefined) yield* requireNoInterruptedRestore(resource.path);
  if (trial === undefined && statePath !== resource.path)
    yield* requireNoInterruptedRestore(statePath);

  // Older releases have no lock. Do not replace their record while their PID
  // still identifies that process. New records with a free lock belong to a
  // crashed or stopped owner.
  const previous = yield* Effect.try({
    try: () => {
      try {
        const contents = NodeFS.readFileSync(resource.path, "utf8");
        return Option.some(decodeRuntimeState(contents));
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") {
          return Option.none<PersistedServerRuntimeState>();
        }
        throw cause;
      }
    },
    catch: (cause) => new ServerOwnershipError({ statePath, cause }),
  });
  if (
    Option.isSome(previous) &&
    previous.value.ownerId === undefined &&
    (yield* legacyOwnerIsLive(previous.value))
  ) {
    return yield* new ServerAlreadyRunningError({ stateDir: resource.lock.stateDir });
  }

  if (trial === undefined) {
    const version = yield* legacyBootServiceVersion(statePath);
    if (version !== undefined)
      return yield* new LegacyBootServiceError({ stateDir: resource.lock.stateDir, version });
  }

  yield* Effect.try({
    try: () => {
      if (trial !== undefined && resource.lock.readOwnerId() !== trial.previousOwnerId) {
        throw Object.assign(
          new Error("Another owner used the database after the update snapshot."),
          { code: "T3_STATE_DIR_OWNED" },
        );
      }
      resource.lock.claim(ownerId);
    },
    catch: (cause) =>
      cause instanceof Error && "code" in cause && cause.code === "T3_STATE_DIR_OWNED"
        ? new ServerAlreadyRunningError({ stateDir: resource.lock.stateDir })
        : new ServerOwnershipError({ statePath, cause }),
  });

  return {
    publish: (state: PersistedServerRuntimeState) =>
      Effect.suspend<void, ServerOwnershipError | ServerOwnershipReleasedError, never>(() => {
        if (!resource.active) return Effect.fail(new ServerOwnershipReleasedError({ statePath }));
        return Effect.try({
          try: () => {
            const temporaryPath = `${resource.path}.${ownerId}.tmp`;
            try {
              NodeFS.writeFileSync(
                temporaryPath,
                `${encodeRuntimeState({ ...state, ownerId })}\n`,
                {
                  mode: 0o600,
                },
              );
              NodeFS.renameSync(temporaryPath, resource.path);
            } finally {
              NodeFS.rmSync(temporaryPath, { force: true });
            }
          },
          catch: (cause) => new ServerOwnershipError({ statePath, cause }),
        });
      }),
  };
});

/** Check before service setup. The server still acquires its own lifetime lock. */
export const requireServerStopped = (statePath: string) =>
  Effect.scoped(acquireServerOwnership(statePath)).pipe(Effect.asVoid);
