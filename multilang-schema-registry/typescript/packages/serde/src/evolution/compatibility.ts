/**
 * Glue schema-compatibility mode: enum, default, and case-exact validator.
 *
 * The client's responsibility for compatibility is **acceptance + validation
 * + carry**: it recognises the eight mode strings the Glue service accepts,
 * rejects anything else case-exactly, and passes the chosen mode through to
 * the register call. The client itself never computes whether two schemas
 * are compatible — that check is performed by the Glue service at
 * `RegisterSchemaVersion` time. This module makes no Glue call.
 *
 * The accepted set and the case-exact match rule mirror the Java canonical
 * (`software.amazon.awssdk.services.glue.model.Compatibility` — the enum
 * `valueOf` is case-sensitive over the `knownValues()` list) and the Go
 * reference (`core/config.go` — `validCompatibilities` + `validateCompatibility`,
 * with `DefaultCompatibility = "BACKWARD"`).
 *
 * `GsrInvalidCompatibilityModeError` is a config-validation error and is
 * deliberately **distinct from the wire-format decode taxonomy** exported by
 * `@gsr/core`: an unrecognised mode string is a caller configuration fault
 * detected before any decode happens, not malformed wire data. It mirrors
 * the Go `ErrInvalidCompatibility` sentinel.
 */

/**
 * The exact case-sensitive set of compatibility modes the Glue service
 * accepts. Enum values are string-valued and identical to their key so
 * that `CompatibilityMode.BACKWARD === "BACKWARD"` at runtime — this is
 * the shape the eventual register call passes on the wire.
 */
export enum CompatibilityMode {
  NONE = "NONE",
  DISABLED = "DISABLED",
  BACKWARD = "BACKWARD",
  BACKWARD_ALL = "BACKWARD_ALL",
  FORWARD = "FORWARD",
  FORWARD_ALL = "FORWARD_ALL",
  FULL = "FULL",
  FULL_ALL = "FULL_ALL",
}

/**
 * Default compatibility mode when the caller supplies none. Matches the
 * Java canonical default and the Go reference (`DefaultCompatibility`).
 */
export const DEFAULT_COMPATIBILITY_MODE: CompatibilityMode =
  CompatibilityMode.BACKWARD;

/**
 * The eight accepted mode strings, indexed as a set for O(1) case-exact
 * membership. Populated from the enum so a future addition to the enum
 * flows through without a second source of truth.
 */
const VALID_COMPATIBILITY_MODES: ReadonlySet<string> = new Set(
  Object.values(CompatibilityMode),
);

/**
 * True when `value` is one of the eight compatibility modes, matched
 * case-exactly. Lowercase variants (e.g. `"backward"`) and typos
 * (e.g. `"FORWARDS"`) return `false`.
 */
export function isCompatibilityMode(value: string): value is CompatibilityMode {
  return VALID_COMPATIBILITY_MODES.has(value);
}

/**
 * Parses `value` as a `CompatibilityMode`. Returns the matching enum
 * member on success; throws {@link GsrInvalidCompatibilityModeError}
 * otherwise. Matching is case-exact — `"backward"` and the empty string
 * are both rejected, mirroring the Java `Compatibility.valueOf` behavior
 * on the enum's `knownValues()` list.
 */
export function parseCompatibilityMode(value: string): CompatibilityMode {
  if (!isCompatibilityMode(value)) {
    throw new GsrInvalidCompatibilityModeError(
      `invalid compatibility mode: ${JSON.stringify(value)}`,
    );
  }
  return value;
}

/**
 * Thrown when a compatibility-mode string does not match the accepted
 * case-exact set. This is a caller-configuration fault, distinct from
 * the wire-format decode-error taxonomy (`GsrIncompatibleDataError` and
 * its siblings) exported by `@gsr/core`, and mirrors the Go reference's
 * `ErrInvalidCompatibility` sentinel.
 */
export class GsrInvalidCompatibilityModeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GsrInvalidCompatibilityModeError";
  }
}
