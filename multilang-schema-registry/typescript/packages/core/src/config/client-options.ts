/**
 * `buildGlueClientConfig` — map a validated {@link GsrConfig} into the
 * `@aws-sdk/client-glue` v3 client-config object.
 *
 * Mirrors Go's `LoadConfigFromMap` (`core/config.go`), which builds one
 * `aws.Config` carrying region, endpoint, credentials, retryer, and the
 * user-agent middleware. SDK v3 takes the same set of concerns as fields
 * of one `GlueClientConfig`, so all four client-config capabilities converge here:
 *
 *   1. AssumeRole      — `fromTemporaryCredentials` when `assumeRoleArn`
 *                        is set; otherwise leave `credentials` unset so
 *                        the SDK's default credential chain applies.
 *   2. Region+endpoint — `region` (default `us-east-2`, applied by
 *                        `parseConfig` per A-REGION); `endpoint` is a
 *                        custom override applied regardless of region.
 *   3. Retry           — SDK v3 native `retryMode` + `maxAttempts`
 *                        (default `standard` / `3` matches Go's
 *                        standard-retryer `MaxAttempts=3`).
 *   4. User-agent      — `customUserAgent = [["glue-schema-registry-js",
 *                        effectiveUserAgentApp]]`. The `key/value` pair
 *                        form is used (not key-only) so the emitted
 *                        header token preserves the `/` separator that
 *                        Go's `AddUserAgentKeyValue("glue-schema-registry-go", app)`
 *                        produces (`config.go:159-161`).
 *
 * Pure — no network I/O, no SDK client construction. The concrete Glue
 * client is built by the Glue seam module.
 */
import { fromTemporaryCredentials } from "@aws-sdk/credential-providers";
import type { GlueClientConfig } from "@aws-sdk/client-glue";

import type { GsrConfig } from "./config.js";

/**
 * Language-suffix token stamped on every outbound Glue request under the
 * `customUserAgent` middleware. Mirrors Go's `glue-schema-registry-go` and
 * Java's `aws-glue-schema-registry-java` pattern.
 */
export const USER_AGENT_LANGUAGE_TOKEN = "glue-schema-registry-js";

/**
 * Build the `GlueClientConfig` object consumed by `new GlueClient(...)`.
 *
 * The returned object is a plain data value — every field is a literal,
 * a caller-supplied string, or (for AssumeRole) the opaque provider
 * function returned by `fromTemporaryCredentials`. Callers introspect the
 * returned config directly in unit tests; no live AWS call is made here.
 */
export function buildGlueClientConfig(cfg: GsrConfig): GlueClientConfig {
  const out: GlueClientConfig = {
    region: cfg.region,
    retryMode: cfg.retryMode,
    maxAttempts: cfg.maxAttempts,
    customUserAgent: [[USER_AGENT_LANGUAGE_TOKEN, cfg.effectiveUserAgentApp]],
  };

  // Custom endpoint override applies regardless of region — mirrors Go's
  // unconditional `WithEndpointResolver` when the config value is set
  // (`config.go:196-198`). Only surface when explicitly provided so the
  // SDK's default endpoint resolution is preserved on the empty path.
  if (cfg.endpoint !== undefined) {
    out.endpoint = cfg.endpoint;
  }

  // AssumeRole is active iff the ARN is set. When absent, leave
  // `credentials` unset so the SDK's default credential-provider chain
  // applies (Go's dormant path — `stscreds.NewAssumeRoleProvider` is
  // only wired when the ARN is non-empty, `config.go:189-194`).
  if (cfg.assumeRoleArn !== undefined) {
    out.credentials = fromTemporaryCredentials({
      params: {
        RoleArn: cfg.assumeRoleArn,
        RoleSessionName: cfg.assumeRoleSessionName,
      },
    });
  }

  return out;
}
