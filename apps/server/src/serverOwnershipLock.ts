// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off - The standalone launcher cannot depend on Effect.
// Shared with the standalone service launcher. Keep imports limited to native modules.
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import {
  serviceStateHasPendingUpdate,
  serviceStatePendingUpdateId,
} from "./cloud/serviceProtocol.ts";

export const SERVER_UPDATE_RECOVERY_FILE = "server-update-pending";

const syncStateDirectory = (stateDir: string) => {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone launcher lock cannot depend on the Effect runtime.
  if (process.platform === "win32") return;
  const directory = NodeFS.openSync(stateDir, "r");
  try {
    NodeFS.fsyncSync(directory);
  } finally {
    NodeFS.closeSync(directory);
  }
};

/** Never unlink this file. SQLite releases its OS lock when the holder exits. */
export async function acquireServerOwnershipLock(
  directory: string,
  options?: {
    readonly guardLegacyOwner?: boolean;
    readonly cli?: boolean;
    readonly launcher?: boolean;
  },
) {
  await NodeFSP.mkdir(directory, { recursive: true });
  const stateDir = await NodeFSP.realpath(directory);
  const lockPath = NodePath.join(
    stateDir,
    options?.cli
      ? "server-cli.sqlite"
      : options?.launcher
        ? "server-launcher.sqlite"
        : "server-owner.sqlite",
  );
  let db: { exec: (sql: string) => unknown; close: () => void };
  if (process.versions.bun) {
    // Keep Bun's runtime-only module out of the Node build's module resolver.
    const sqlite = (await import("bun:sqlite".toString())) as {
      Database: new (filename: string) => typeof db;
    };
    db = new sqlite.Database(lockPath);
  } else {
    db = new (await import("node:sqlite")).DatabaseSync(lockPath);
  }
  try {
    db.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE;");
    if (options?.cli) {
      if (NodeFS.existsSync(NodePath.join(stateDir, SERVER_UPDATE_RECOVERY_FILE))) {
        throw new Error("An interrupted update requires manual recovery before CLI access.");
      }
      for (const parent of new Set([
        NodePath.dirname(stateDir),
        NodePath.dirname(NodePath.resolve(directory)),
      ])) {
        let contents: string | undefined;
        const runtimeDir = NodePath.join(parent, "runtime");
        try {
          contents = await NodeFSP.readFile(
            NodePath.join(runtimeDir, "service-state.json"),
            "utf8",
          );
        } catch (cause) {
          if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
        }
        const updateId = contents === undefined ? undefined : serviceStatePendingUpdateId(contents);
        if (
          contents !== undefined &&
          serviceStateHasPendingUpdate(contents) &&
          (updateId === undefined ||
            NodeFS.existsSync(NodePath.join(runtimeDir, "db-backup", updateId)))
        ) {
          throw new Error(
            "An interrupted update has an existing database backup. Manual recovery is required; no database files were restored.",
          );
        }
      }
    }
    if (options?.guardLegacyOwner) {
      let runtimeState: unknown;
      try {
        runtimeState = JSON.parse(
          await NodeFSP.readFile(NodePath.join(stateDir, "server-runtime.json"), "utf8"),
        );
      } catch (cause) {
        if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
      }
      if (runtimeState !== undefined) {
        // Unknown descriptors and live legacy PIDs must fail closed before a
        // launcher copies data. PID reuse can cause a conservative refusal;
        // no process is ever stopped based on this record.
        if (
          typeof runtimeState !== "object" ||
          runtimeState === null ||
          !("version" in runtimeState) ||
          runtimeState.version !== 1 ||
          !("pid" in runtimeState) ||
          typeof runtimeState.pid !== "number" ||
          !Number.isInteger(runtimeState.pid) ||
          ("ownerId" in runtimeState && typeof runtimeState.ownerId !== "string")
        ) {
          throw new Error(`Cannot inspect server ownership at ${stateDir}.`);
        }
        if (!("ownerId" in runtimeState) && runtimeState.pid > 0) {
          let alive = true;
          try {
            process.kill(runtimeState.pid, 0);
          } catch (cause) {
            if (cause instanceof Error && "code" in cause && cause.code === "ESRCH") {
              alive = false;
            } else if (!(cause instanceof Error && "code" in cause && cause.code === "EPERM")) {
              throw cause;
            }
          }
          if (alive) {
            throw Object.assign(
              new Error(`A legacy server may still own ${stateDir}. Stop it before updating.`),
              { code: "T3_STATE_DIR_OWNED" },
            );
          }
        }
      }
    }
  } catch (cause) {
    db.close();
    throw cause;
  }
  return {
    stateDir,
    close: () => db.close(),
    markUpdatePending: (updateId: string) => {
      const fd = NodeFS.openSync(NodePath.join(stateDir, SERVER_UPDATE_RECOVERY_FILE), "wx", 0o600);
      try {
        NodeFS.writeFileSync(fd, `${updateId}\n`);
        NodeFS.fsyncSync(fd);
      } finally {
        NodeFS.closeSync(fd);
      }
      syncStateDirectory(stateDir);
    },
    clearUpdatePending: (updateId: string) => {
      const marker = NodePath.join(stateDir, SERVER_UPDATE_RECOVERY_FILE);
      let contents: string;
      try {
        contents = NodeFS.readFileSync(marker, "utf8");
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return;
        throw cause;
      }
      if (contents.trim() !== updateId) throw new Error("Another update requires recovery.");
      NodeFS.rmSync(marker);
      syncStateDirectory(stateDir);
    },
    readOwnerId: () => {
      try {
        const value: unknown = JSON.parse(
          NodeFS.readFileSync(NodePath.join(stateDir, "server-owner.json"), "utf8"),
        );
        if (typeof value !== "string" || value.length === 0)
          throw new Error("Invalid ownership history.");
        return value;
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return null;
        throw cause;
      }
    },
    claim: (ownerId: string) => {
      // This record survives graceful shutdown. Backup/rollback can detect an
      // intervening writer even when its discovery record has been removed.
      const target = NodePath.join(stateDir, "server-owner.json");
      const temporary = `${target}.${ownerId}.tmp`;
      try {
        const fd = NodeFS.openSync(temporary, "wx", 0o600);
        try {
          NodeFS.writeFileSync(fd, `${JSON.stringify(ownerId)}\n`);
          NodeFS.fsyncSync(fd);
        } finally {
          NodeFS.closeSync(fd);
        }
        NodeFS.renameSync(temporary, target);
        syncStateDirectory(stateDir);
      } finally {
        NodeFS.rmSync(temporary, { force: true });
      }
    },
  };
}
