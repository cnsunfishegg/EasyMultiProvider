//! Fixture credentials only. Run behind an instrumented rejecting proxy.
use std::time::{Duration, Instant};
fn main() {
    let binary = std::env::args().nth(1).expect("absolute Codex binary path");
    assert!(std::path::Path::new(&binary).is_absolute());
    let auth = serde_json::json!({"tokens":{"access_token":"fixture-invalid-token",
        "account_id":"fixture-account","refresh_token":"fixture-invalid-refresh"}});
    let started = Instant::now();
    let result = emp_codex::quota::run_quota_query(&auth, &binary, Duration::from_secs(5), false);
    println!(
        "{}",
        serde_json::json!({"elapsed_ms":started.elapsed().as_millis(),
        "status":"unknown", "error_code":result.err().map(|error| error.code()),
        "credentials":"fixture_only"})
    );
}
