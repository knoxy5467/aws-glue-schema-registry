# Publishing `@gsr/core` and `@gsr/serde`

This runbook describes how a release operator publishes the TypeScript
AWS Glue Schema Registry (GSR) client packages. It covers two paths:

1. **Private npm registry** — publishing to an operator-supplied private
   npm registry (for internal distribution inside an organization).
2. **Public npm registry** — publishing to the public
   `https://registry.npmjs.org` under a scope you control.

The two paths are complementary, not exclusive: a version can be
published to a private registry for early adopters, then re-released to
the public npm registry once any organization-specific release-approval
process has cleared.

> **All credentialed and executable steps in this runbook are
> OPERATOR-GATED.** The repository itself performs no automated
> publish. There is no `postinstall` hook, no CI job, and no npm script
> that runs `npm publish` or logs into a registry. An operator must run
> each publish step by hand, from a workstation with the right
> credentials, after confirming the entry criteria for that path.

## What the repository ships

The two published workspaces under `packages/` are `@gsr/core` and
`@gsr/serde`. Their `package.json` files carry the publish metadata
directly:

| Field | Value | Purpose |
|---|---|---|
| `version` | `1.0.0` | The frozen public API. Bumps follow semver. |
| `license` | `Apache-2.0` | The stated license. Operator-confirmable — see [License](#license) below. |
| `files` | `["dist", "README.md", "LICENSE"]` | The tarball allowlist. Only these paths ship. `src/**`, `*.test.ts`, `tsconfig*`, `tsup.config.*`, `vitest*`, examples, and bench are excluded. |
| `exports` | dual ESM + CJS map | The `import` condition resolves to `dist/index.mjs` + `dist/index.d.ts`; the `require` condition resolves to `dist/index.cjs` + `dist/index.d.cts`. |
| `engines.node` | `>=20` | Consumers must run Node.js 20 or newer. |
| `publishConfig.registry` | operator-supplied private-registry endpoint | The default target. Overridden per-path — see below. |
| `publishConfig.access` | `restricted` | Restricted access. The public path sets `--access public` on the publish command instead. |

The private workspace `@gsr/integration-tests` carries `"private": true`
and no `files`/`exports` map, so npm refuses to publish it. It is a
test harness and never ships.

The dependency `@gsr/serde` → `@gsr/core` is expressed as
`"@gsr/core": "~1.0.0"` in the published `peerDependencies` map (with a
matching `devDependencies` entry that makes the workspace build resolve
locally). Peer-only guarantees a **single shared `@gsr/core` instance**
at consumer install time — under strict resolvers (pnpm,
`npm install --strict-peer-deps`) a dual `dependencies` +
`peerDependencies` declaration can dedupe to two copies, at which point
cross-package `instanceof` on error classes silently breaks. Consumers
therefore install both packages together
(`npm install @gsr/serde @gsr/core`); the two packages are versioned in
lockstep, so the tilde range keeps them locked to the same minor
version.

## Prerequisites (both paths)

Before running any publish step:

- Node.js **20 or newer** and a current npm client (`npm --version`
  should report the npm shipped with your Node 20+ install).
- A clean checkout of the tag or commit being released. Run
  `git status` — the working tree must be clean.
- A local build:
  ```
  npm ci
  npm run build --workspaces
  ```
  This is offline: it emits `dist/index.mjs`, `dist/index.cjs`,
  `dist/index.d.ts`, `dist/index.d.cts` (plus source maps) into each of
  `packages/core/dist/` and `packages/serde/dist/`.
- Offline verification:
  ```
  npm run verify:pack
  ```
  This asserts each published tarball's file manifest against the
  `files` allowlist without touching the network or any registry.
  **Do not proceed** if this step fails.
- Confirmation that `packages/core/LICENSE`, `packages/serde/LICENSE`,
  and the root `LICENSE` all exist. The `files` allowlist requires the
  per-package `LICENSE`.

## Path 1 — Private npm registry

This path publishes the current `1.0.0` tarball of each package to an
operator-supplied private npm registry (for example, an AWS
CodeArtifact repository, a JFrog Artifactory npm repo, a Verdaccio
instance, or any other npm-protocol-compatible registry).

### Operator entry criteria

- The operator holds credentials for the target private registry, with
  publish permission on the `@gsr` scope (or whichever scope is being
  used in the fork).
- The registry endpoint URL is known. It is operator-supplied — this
  runbook does not embed it.
- The operator has agreed the released version number with the package
  maintainers.

### Steps (OPERATOR-GATED — run each step by hand)

1. **[OPERATOR-GATED]** Configure the local npm client to authenticate
   against the private registry. The exact command depends on the
   registry — consult the registry's own documentation for the
   login flow (typically `npm login --registry=<url>` or a
   registry-specific CLI that writes credentials into `~/.npmrc`).
   Confirm authentication with:
   ```
   npm whoami --registry=<private-registry-url>
   ```

2. **[OPERATOR-GATED]** Publish `@gsr/core` first (because `@gsr/serde`
   declares it as a runtime peer dependency):
   ```
   npm publish --workspace @gsr/core --registry=<private-registry-url>
   ```
   Expect npm to report the tarball size + shasum. If the version
   already exists in the registry, npm refuses the publish and the run
   stops — this is intentional (versions are immutable).

3. **[OPERATOR-GATED]** Publish `@gsr/serde`:
   ```
   npm publish --workspace @gsr/serde --registry=<private-registry-url>
   ```

4. **[OPERATOR-GATED]** Verify the two versions are visible in the
   registry (command depends on the registry — typically `npm view` or
   the registry's own list-versions API):
   ```
   npm view @gsr/core@1.0.0 --registry=<private-registry-url>
   npm view @gsr/serde@1.0.0 --registry=<private-registry-url>
   ```

5. **[OPERATOR-GATED]** Optional smoke install from a scratch directory
   to confirm the registry endpoint resolves as expected:
   ```
   mkdir /tmp/gsr-smoke && cd /tmp/gsr-smoke
   npm init -y >/dev/null
   npm install @gsr/serde @gsr/core --registry=<private-registry-url>
   node -e "console.log(Object.keys(require('@gsr/serde')))"
   ```

### Owners

- **Package maintainers** — cut the release commit, agree the version,
  and drive the publish.
- **Release operator** — an engineer with credentials for the target
  private registry. May be the same person as a maintainer.
- **Consumers** — teams that install from the private registry.

## Path 2 — Public npm registry

This path releases `@gsr/core` and `@gsr/serde` to the **public** npm
registry (`https://registry.npmjs.org`) under a scope you control. Any
organization-specific release-approval process (security review,
open-source-compliance review, license approval) sits UPSTREAM of this
runbook — clear it before proceeding.

### Operator entry criteria

- The version has been running against internal consumers long enough
  to demonstrate stability. Path 1 (or an equivalent internal-usage
  gate) is a prerequisite; Path 2 is not a shortcut around it.
- Any organization-specific release approvals have cleared and been
  recorded.
- The `Apache-2.0` license text in `LICENSE` is operator-confirmable —
  verify with the maintainers before publishing.
- The operator has credentials for the public-npm publish account or a
  comparable mechanism.

### Recommended gates before publish

Every gate below produces an artifact (approval, sign-off, or filed
exception) that the maintainers keep on file. If a gate cannot clear,
the release pauses; the operator does not skip ahead.

1. **[OPERATOR-GATED] Security review.** A security review against the
   intended public tarballs and their transitive dependency tree.
   Entry criteria: the tagged release commit is stable and matches
   what will be published; the review targets the exact tarballs
   produced by `npm pack --workspace @gsr/core` and
   `npm pack --workspace @gsr/serde`.

2. **[OPERATOR-GATED] Open-source-compliance review.** Confirms the
   declared license (`Apache-2.0`), NOTICE requirements for third-party
   dependencies, and copyright headers. Entry criteria: security
   review complete.

3. **[OPERATOR-GATED] Integration testing.** A final integration test
   in a clean, publicly-reachable environment against the exact
   tarballs that will be published, exercising both the `import` (ESM)
   and `require` (CJS) resolution paths on Node.js 20 and the current
   LTS. Entry criteria: OSS-compliance review complete.

4. **[OPERATOR-GATED] Public npm release.** With all prior gates
   cleared, the release operator publishes to the public npm registry:
   ```
   npm publish --workspace @gsr/core --access public --registry https://registry.npmjs.org
   npm publish --workspace @gsr/serde --access public --registry https://registry.npmjs.org
   ```
   Note the explicit `--registry` flag — it overrides the
   operator-supplied `publishConfig.registry` set for internal
   distribution. Ownership: the release operator, paired with a
   maintainer.

5. **[OPERATOR-GATED] Post-release verification.** Confirm the two
   versions resolve from the public registry:
   ```
   npm view @gsr/core@1.0.0 --registry https://registry.npmjs.org
   npm view @gsr/serde@1.0.0 --registry https://registry.npmjs.org
   ```
   Both should report the just-published tarball's `shasum` and
   `dist.tarball` URL.

### Owners

- **Package maintainers** — cut the release, drive the approval
  sequence, own the public documentation.
- **Security reviewer** — owns Gate 1.
- **OSS-compliance reviewer** — owns Gate 2.
- **Release operator** — owns Gates 3, 4, and 5 alongside a maintainer.

## License

Both packages declare `Apache-2.0` via the `license` field in each
`package.json` and ship a per-package `LICENSE` file inside the
tarball. The `LICENSE` text is operator-confirmable at release time:
if the maintainers or the OSS-compliance reviewer determine a
different license is required for a specific public release, that
change lands in the repository (root `LICENSE`, per-package `LICENSE`,
and each `package.json`'s `license` field) as a normal source change
before the release is cut. This runbook does not modify the license
out-of-band.

## What this runbook does *not* do

- **It does not run `npm publish`.** There is no CI job wired to any
  of these commands; no npm script performs a live publish; no
  automation triggers on a merge or a tag. Every publish is an
  operator running a command by hand.
- **It does not embed real credentials, domain names, or repository
  URLs.** The publish role, the registry endpoint, and any
  organization-specific case identifiers are operator-supplied at run
  time.
- **It does not decide the version.** The `version` field in each
  `package.json` is set by the maintainers as part of preparing the
  release commit; the operator publishes whatever version the
  checked-out tree declares.
- **It does not replace any organization-specific release-approval
  process.** The gated sequence above summarises the reviews a
  well-run public release should clear; where an organization runs
  its own process, the authoritative checklist lives in that
  process, not here.
