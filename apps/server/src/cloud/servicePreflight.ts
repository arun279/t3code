import packageJson from "../../package.json" with { type: "json" };
import { SERVICE_LAUNCHER_PROTOCOL } from "./serviceProtocol.ts";

export type ServicePreflightResult =
  | {
      readonly status: "ready";
      readonly version: string;
      readonly launcherProtocol: typeof SERVICE_LAUNCHER_PROTOCOL;
      readonly ownershipProtocol?: 1;
    }
  | {
      readonly status: "blocked";
      readonly version: string;
      readonly reason: string;
    };

export function runServicePreflight(input: {
  /** Older servers always pass this flag when invoking a staged preflight. */
  readonly databasePath: string;
  readonly launcherProtocol: number;
  readonly version?: string;
}): ServicePreflightResult {
  const version = input.version ?? packageJson.version;
  if (input.launcherProtocol !== SERVICE_LAUNCHER_PROTOCOL) {
    return {
      status: "blocked",
      version,
      reason:
        "This release requires a newer T3 Code service launcher. Update it on the server machine.",
    };
  }

  return {
    status: "ready",
    version,
    launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
    ownershipProtocol: 1,
  };
}

export function decodeServicePreflightResult(value: unknown): ServicePreflightResult | undefined {
  if (typeof value !== "object" || value === null) {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  if (
    record.status === "ready" &&
    record.launcherProtocol === SERVICE_LAUNCHER_PROTOCOL &&
    typeof record.version === "string"
  ) {
    return {
      status: "ready",
      version: record.version,
      launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
      ...(record.ownershipProtocol === 1 ? { ownershipProtocol: 1 as const } : {}),
    };
  }
  if (
    record.status === "blocked" &&
    typeof record.version === "string" &&
    typeof record.reason === "string"
  ) {
    return { status: "blocked", version: record.version, reason: record.reason };
  }
  return undefined;
}

/** This command reports capabilities without opening application persistence. */
export function runtimeOwnershipProbe(command: string, databasePath: string) {
  return {
    command,
    args: [
      "__service-preflight",
      "--database-path",
      databasePath,
      "--launcher-protocol",
      String(SERVICE_LAUNCHER_PROTOCOL),
    ],
    timeoutMs: 15_000,
    maxOutputBytes: 16_384,
  };
}

export function runtimeSupportsOwnership(stdout: string, version: string): boolean {
  try {
    const result = decodeServicePreflightResult(JSON.parse(stdout));
    return (
      result?.status === "ready" && result.version === version && result.ownershipProtocol === 1
    );
  } catch {
    return false;
  }
}
