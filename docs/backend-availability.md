# Fixed-source availability and release acceptance

Implementation: v0.12.11, based on upstream v0.12.10 (`91baaf0`).
Acceptance record: 2026-10-05, Asia/Shanghai.

## Behavior

The existing model choice fixes the account, endpoint and upstream model before
admission. HTTP, SSE, individual WebSocket turns and Compact use that decision.
Ordinary requests do not select a different account. The existing auto-review
selection policy remains separate.

Structured `usage_limit_reached`, `insufficient_quota`, `quota_exhausted` and
`billing_hard_limit_reached` errors establish exhaustion for the selected source
and model. A 429 status or quota-related prose alone does not establish it.
Explicit rate limiting with `Retry-After` creates a distinct cooldown. Failure
feedback is recorded before another attempt; final usage and activity still
settle once through `RequestOutcome`. Ambiguous native connection failures and
gateway timeouts are not automatically replayed.

After a request-derived block expires, one same-source request can probe for
recovery. Cancellation releases the slot without declaring the source healthy.
Only that probe's successful completion can clear its block; an older concurrent
success cannot. Availability facts are bounded, process-local observations,
not persisted budget reservations.

Quota reads are keyed by the actual account owner and checked against credential
identity when completed. Imported aliases can share the quota result while
keeping their own public account identity. An observation started before a newer
failure cannot overwrite its evidence. A displayed percentage of 100 is not
sufficient for rejection: quota-based admission requires an explicit model
`rate_limit_id` and an explicit exhausted/denied window. Unknown pool mappings
remain unknown. A mapped exhausted window needs a newer quota observation;
elapsed reset time alone does not certify recovery.

The existing management snapshots distinguish unknown, observed, stale, low
quota, exhausted and rate-limited states, with scoped model blocks and recent
failure reasons. Network errors do not turn into exhaustion. A snapshot does not
promise that an entire Codex task can finish.

## Refresh and helper bounds

- Concurrent readers of an owner join one refresh, waiting at most two seconds.
  There are at most four ordinary quota refresh owners running at once.
- Successful results are cached for two seconds; repeated failures back off
  from 30 seconds to 15 minutes. Failed credential saves bypass the cache.
  Explicit quota reset invalidates the cached result.
- Stale request-time observations wake the existing lifecycle-managed sampler,
  at most once per owner per minute, through a bounded queue. They do not spawn
  a helper per generating request or wait on a slow query. Unknown stays usable.
- The existing credential refresh rules, per-account locks and shutdown save
  gate still own rotations. Each helper query has its existing 45-second bound;
  the existing imported-account recovery may perform a second query.
- Helpers retain `[features] plugins = false`, use a private temporary
  `CODEX_HOME`, `HOME` and `USERPROFILE`, cap stdout at 4 MiB, and enforce the
  two-second process-exit limit before joining readers.
- Shutdown cancels quota reads, including a check before launching a prepared
  helper. Credentials already rotated in its temporary home are still persisted
  before returning cancellation. Content-free worker wait/completion events
  identify which shutdown stage is still draining.

The import API keeps its existing account response and adds an operation receipt.
Credential saving and local catalog publication share an idempotent workflow.
The receipt explicitly leaves client catalog and inference verification false.
It does not enable integration, restart Codex, consume quota resets or bypass
integration leases. The existing page can repeat its catalog-refresh step.

## Reproducible acceptance

Use a canonical temporary path as the CI workflows do; macOS `/var` symlink
paths intentionally fail the managed-file write policy. Rust 1.93.1 is the
release toolchain.

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets --no-fail-fast
cargo build --locked -p emp-app --bin EMP
python3 scripts/availability_acceptance.py --codex-bin /absolute/codex --emp-bin target/debug/EMP
cargo build --locked -p emp-codex --example quota_probe
python3 scripts/quota_probe_acceptance.py --runner target/debug/examples/quota_probe --codex-bin /absolute/codex
```

The Rust contracts exercise structured versus unstructured errors, model and
credential isolation, feedback from stream terminals, the next turn on an open
WebSocket, single-flight recovery and cancellation, stale observations, shared
quota helpers, output/EOF limits, credential-rotation persistence, existing
auto-review behavior and integration ownership.

The continuation script uses the installed Codex CLI 0.156.1, the real EMP
binary, temporary homes, fake credentials and two local upstreams. It switches
the selected EMP model within the same Codex provider using the existing
`exec resume --last -m recovery/model` path. Its three assertions cover early
quota rejection, interrupted output, and quota rejection after a read-only tool.
The latter requires preserved tool output and zero repeated tool executions.
Changing Codex's provider configuration is a different operation and is not
claimed to preserve the same continuation behavior.

Observed results: early exhaustion sent one upstream request; interrupted output
caused six bounded client attempts; tool-then-exhaustion sent two requests. Each
explicit model switch resumed with one request to the second service. The tool
result was preserved, with zero repeated tool executions.

## Verification limits

A real quota-runner probe with fake credentials returned `quota_auth_required`
in approximately 0.6 seconds, left no temporary credential directory, and made
zero connections observed by its refusing proxy. The authenticated helper path
was not exercised: **authenticated zero-plugin-download acceptance is UNKNOWN**.
Proxy observations alone are not a complete network audit. No real account or
paid generation is used by these scripts.

The exit journey checks the `stopping` receipt separately from actual process
exit, verifies native configuration restoration, and starts EMP once with
automatic activation disabled to verify restoration persists. These checks do
not establish that a running Codex desktop backend has reloaded its settings.
The reported need for two desktop restarts has not been reproduced or attributed
to a specific cause. A `stopping` receipt must not be interpreted as verified
desktop runtime recovery; the existing live-verification boundary remains.
Cold local runtime discovery can also delay process exit after the receipt; the
process test allows 30 seconds for that drain, while separately checking the
restored file and first restart. No claim of instant exit is made.

Release packages embed the publishing GitHub repository as their update source.
Local builds without `EMP_RELEASE_REPOSITORY` retain the upstream default.
The release workflow still validates versions, checksums, platform packages,
installation/rollback, TLS and disconnect handling before publishing.
