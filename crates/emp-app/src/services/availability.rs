//! Evidence for the already selected source. Never selects another route.
//!
//! Request failures are scoped to the exact upstream model. Quota reads only
//! govern an explicitly identified limit bucket, never an arbitrary first
//! bucket. Display percentages are not proof that a provider rejects work.
use crate::app::ServerState;
use emp_core::{OpaqueJson, ResolvedRoute};
use emp_router::native_http::NativeHttpError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const TICKET: &str = "_emp_admission";
const RECHECK_SECONDS: u64 = 60;
const OBSERVATION_SECONDS: u64 = 120;
const MAX_FACTS: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Subject {
    owner: String,
    version: String,
    source: String,
    model: String,
    bucket: Option<String>,
    #[serde(default)]
    probe: Option<u64>,
}

#[derive(Clone)]
struct Block {
    reason: &'static str,
    recheck_at: u64,
    probe: Option<u64>,
}

#[derive(Default)]
struct Facts {
    revision: u64,
    next_probe: u64,
    blocks: BTreeMap<(String, String), Block>,
    quotas: BTreeMap<String, QuotaObservation>,
    last_failure: BTreeMap<String, Value>,
}

struct QuotaObservation {
    observed_at: u64,
    quota: Value,
}

#[derive(Default)]
pub(crate) struct Availability(Mutex<Facts>);

/// Owned by the HTTP request or individual WS turn, including early returns.
/// Cancellation releases the recovery slot without declaring the source healthy.
pub(crate) struct Admission {
    state: Arc<Availability>,
    subject: Option<Subject>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(subject) = &self.subject
            && let Ok(mut facts) = self.state.0.lock()
            && let Some(block) = facts.blocks.get_mut(&key(subject))
            && subject.probe.is_some()
            && block.probe == subject.probe
        {
            block.probe = None;
            block.recheck_at = now().saturating_add(RECHECK_SECONDS);
        }
    }
}

pub(crate) fn now() -> u64 {
    crate::util::system_now().max(0.0) as u64
}

fn digest(value: &Value) -> String {
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}

fn key(subject: &Subject) -> (String, String) {
    (subject.source.clone(), subject.model.clone())
}

pub(crate) fn account_identity(state: &ServerState, id: &str) -> Option<(String, String)> {
    let auth = if id == "@native" {
        super::accounts::native_auth_document(&state.backend.accounts.native_auth_path)?
    } else {
        let config = state.backend.configuration.read().ok()?.clone();
        let account = config["accounts"]
            .as_array()?
            .iter()
            .find(|a| a["id"] == id)?;
        let path = account["auth_file"].as_str()?;
        state
            .backend
            .accounts
            .pending_rotations
            .lock()
            .ok()?
            .get(path)
            .cloned()
            .or_else(|| {
                state
                    .backend
                    .configuration
                    .vault
                    .read_encrypted_json(std::path::Path::new(path))
                    .ok()
            })?
    };
    let headers = emp_codex::account_auth_headers(&auth)?;
    let owner = emp_state::usage::account_owner(&headers);
    (!owner.is_empty()).then(|| (owner, digest(&auth)))
}

fn subject(
    state: &ServerState,
    route: &ResolvedRoute,
    incoming: &BTreeMap<String, String>,
) -> Option<Subject> {
    let provider = route.provider.value();
    let account = provider.get("account").and_then(|a| a["id"].as_str());
    let native = provider.get("implicit_native") == Some(&Value::Bool(true));
    let (owner, version) = if account.is_some() || native {
        account_identity(state, account.unwrap_or("@native"))?
    } else if provider.get("auth_mode").and_then(Value::as_str) == Some("forward") {
        // Bind forwarded evidence to the caller, never the native login.
        let owner = emp_state::usage::account_owner(incoming);
        if owner.is_empty() {
            return None;
        }
        let credentials = incoming
            .iter()
            .filter(|(name, _)| matches!(name.as_str(), "authorization" | "chatgpt-account-id"))
            .collect::<BTreeMap<_, _>>();
        (owner, digest(&json!(credentials)))
    } else {
        let identity = digest(&json!([
            provider.get("base_url"),
            provider.get("api_key"),
            provider.get("api_key_file"),
            provider.get("auth_mode"),
            route.deployment_identity
        ]));
        (identity.clone(), identity)
    };
    Some(Subject {
        source: format!(
            "{}:{}:{}",
            owner, route.endpoint_fingerprint, route.deployment_identity
        ),
        owner,
        version,
        model: route.upstream_model.clone(),
        // Unknown model/bucket mappings remain unknown; e.g. Spark and review
        // must not inherit another model pool's window.
        bucket: route
            .model
            .value()
            .get("rate_limit_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        probe: None,
    })
}

/// Native credentials are read after routing. Refuse an identity change in
/// that gap rather than attributing a new login's result to the old source.
pub(crate) fn validate_owner(
    route: &ResolvedRoute,
    headers: &BTreeMap<String, String>,
) -> Result<(), NativeHttpError> {
    if let Some(subject) = ticket(route)
        && emp_state::usage::account_owner(headers) != subject.owner
    {
        return Err(NativeHttpError::router(
            409,
            "The selected account changed while preparing the request; retry with the selected model.",
        ));
    }
    Ok(())
}

pub(crate) fn ticket(route: &ResolvedRoute) -> Option<Subject> {
    serde_json::from_value(route.provider.value().get(TICKET)?.clone()).ok()
}

#[derive(Debug)]
pub(crate) struct Rejection {
    reason: &'static str,
    retry_at: u64,
}

impl Rejection {
    pub(crate) fn native(&self) -> NativeHttpError {
        let rate_limited = self.reason == "rate_limited";
        let code = if rate_limited {
            "rate_limit_exceeded"
        } else {
            "usage_limit_reached"
        };
        NativeHttpError {
            status: 429,
            body: json!({"error": {"type":code, "code":code,
                "message":"The selected source is unavailable for this model. Wait for a quota recheck or choose a source explicitly.",
                "failure_reason":self.reason, "resets_at":self.retry_at}}),
            headers: BTreeMap::from([(
                "retry-after".to_owned(),
                self.retry_at.saturating_sub(now()).max(1).to_string(),
            )]),
        }
    }
    pub(crate) fn websocket(&self) -> Value {
        let error = self.native();
        json!({"type":"error", "status":error.status, "error":error.body["error"]})
    }
}

pub(crate) fn admit(
    state: &ServerState,
    route: &mut ResolvedRoute,
    incoming: &BTreeMap<String, String>,
) -> Result<Admission, Rejection> {
    let availability = Arc::clone(&state.backend.availability);
    let mut subject = subject(state, route, incoming);
    if let Some(subject) = &mut subject {
        if availability.needs_refresh(&subject.owner) {
            let provider = route.provider.value();
            let account = provider
                .get("account")
                .and_then(|a| a["id"].as_str())
                .or_else(|| {
                    (provider.get("implicit_native") == Some(&Value::Bool(true)))
                        .then_some("@native")
                });
            if let Some(account) = account {
                super::quota::request_refresh(state, account, &subject.owner);
            }
        }
        let result = availability.check(subject, now(), true);
        let journal = &state.backend.diagnostics.journal;
        journal.event("info", "request_admission", &json!({
            "source":journal.pseudonym(&subject.source), "model":journal.pseudonym(&subject.model),
            "decision": if result.is_err() { "reject" } else if subject.probe.is_some() { "recheck" } else { "proceed" },
            "reason":result.as_ref().err().map(|e| e.reason)
        }));
        result?;
        let mut provider = route.provider.value().clone();
        provider.insert(TICKET.to_owned(), json!(subject));
        // The small internal ticket adds no user/provider fields to the wire.
        if let Ok(snapshot) = OpaqueJson::new(provider) {
            route.provider = snapshot;
        }
    }
    Ok(Admission {
        state: availability,
        subject,
    })
}

impl Availability {
    fn needs_refresh(&self, owner: &str) -> bool {
        self.0.lock().is_ok_and(|facts| {
            facts
                .quotas
                .get(owner)
                .is_none_or(|q| now().saturating_sub(q.observed_at) >= 60)
        })
    }
    fn check(&self, subject: &mut Subject, time: u64, claim: bool) -> Result<(), Rejection> {
        let Ok(mut facts) = self.0.lock() else {
            return Ok(());
        };
        if let Some(observation) = facts.quotas.get(&subject.owner)
            && let Some(bucket) = subject.bucket.as_deref()
            && let Some(retry_at) = exhausted_bucket(&observation.quota, bucket, time)
        {
            return Err(Rejection {
                reason: "quota_exhausted",
                retry_at,
            });
        }
        facts.next_probe = facts.next_probe.wrapping_add(1);
        let probe = facts.next_probe;
        if let Some(block) = facts.blocks.get_mut(&key(subject)) {
            if subject.probe.is_some() && subject.probe == block.probe {
                return Ok(());
            }
            if time < block.recheck_at || block.probe.is_some() || !claim {
                return Err(Rejection {
                    reason: block.reason,
                    retry_at: block.recheck_at,
                });
            }
            // Expiry permits one same-source recovery request, not health.
            // The guard releases it on all cancellation and early-return paths.
            block.probe = Some(probe);
            subject.probe = block.probe;
        }
        Ok(())
    }

    pub(crate) fn before_attempt(&self, route: &ResolvedRoute) -> Result<(), Rejection> {
        if let Some(mut subject) = ticket(route) {
            self.check(&mut subject, now(), false)?;
        }
        Ok(())
    }

    pub(crate) fn failure(&self, subject: &Subject, reason: &str, retry: Option<u64>) -> bool {
        let Ok(mut facts) = self.0.lock() else {
            return false;
        };
        if !reason.is_empty()
            && (facts.last_failure.len() < MAX_FACTS
                || facts.last_failure.contains_key(&subject.owner))
        {
            facts.last_failure.insert(
                subject.owner.clone(),
                json!({"reason":reason,"model":subject.model,"observed_at":now()}),
            );
        }
        let reason = match (reason, retry) {
            ("quota_exhausted_confirmed", _) => "quota_exhausted",
            ("rate_limited", Some(seconds)) if seconds > 0 => "rate_limited",
            _ => return false,
        };
        if facts.blocks.len() >= MAX_FACTS && !facts.blocks.contains_key(&key(subject)) {
            if let Some(oldest) = facts
                .blocks
                .iter()
                .filter(|(_, block)| block.probe.is_none())
                .min_by_key(|(_, block)| block.recheck_at)
                .map(|(key, _)| key.clone())
            {
                facts.blocks.remove(&oldest);
            } else {
                return false;
            }
        }
        if let Some(observation) = facts.quotas.get_mut(&subject.owner) {
            observation.observed_at = 0;
        }
        facts.revision = facts.revision.wrapping_add(1);
        facts.blocks.insert(
            key(subject),
            Block {
                reason,
                recheck_at: now().saturating_add(retry.unwrap_or(RECHECK_SECONDS).clamp(1, 86400)),
                probe: None,
            },
        );
        true
    }

    pub(crate) fn event(&self, subject: &Subject, event: &Value) {
        let response = event.get("response").unwrap_or(event);
        let error = response.get("error").unwrap_or(&Value::Null);
        if confirmed_quota(error) {
            let retry = error["resets_at"]
                .as_u64()
                .map(|at| at.saturating_sub(now()));
            self.failure(subject, "quota_exhausted_confirmed", retry);
        } else if (event["type"] == "response.completed" || response["status"] == "completed")
            && subject.probe.is_some()
            && let Ok(mut facts) = self.0.lock()
            && facts
                .blocks
                .get(&key(subject))
                .is_some_and(|b| b.probe == subject.probe)
        {
            facts.blocks.remove(&key(subject));
        }
    }

    pub(crate) fn snapshot(&self, owner: &str) -> Value {
        let Ok(facts) = self.0.lock() else {
            return json!({"state":"unknown"});
        };
        let blocked = facts
            .blocks
            .iter()
            .filter(|((source, _), _)| source.starts_with(&format!("{owner}:")))
            .map(|((_, model), block)| {
                json!({"model":model, "reason":block.reason, "retry_at":block.recheck_at,
                "state":if block.probe.is_some() { "rechecking" } else if block.reason == "rate_limited" { "rate_limited" } else { "exhausted" }})
            })
            .collect::<Vec<_>>();
        let observation = facts.quotas.get(owner);
        let age = observation.map(|q| now().saturating_sub(q.observed_at));
        let low = observation.is_some_and(|q| {
            std::iter::once(&q.quota["rate_limits"])
                .chain(
                    q.quota["rate_limits_by_limit_id"]
                        .as_object()
                        .into_iter()
                        .flat_map(|buckets| buckets.values()),
                )
                .any(|bucket| {
                    ["primary", "secondary"].iter().any(|window| {
                        bucket[*window]["usedPercent"]
                            .as_f64()
                            .is_some_and(|used| used >= 90.0)
                    })
                })
        });
        json!({"state": if blocked.iter().any(|b| b["reason"] == "quota_exhausted") { "exhausted" }
            else if !blocked.is_empty() { "rate_limited" } else if age.is_none() { "unknown" }
            else if age.is_some_and(|a| a > OBSERVATION_SECONDS) { "stale" } else if low { "low_quota" } else { "observed" },
            "observation_age_seconds":age, "model_blocks":blocked,
            "last_failure":facts.last_failure.get(owner), "admission_guaranteed":false})
    }

    pub(crate) fn revision(&self) -> u64 {
        self.0.lock().map_or(0, |facts| facts.revision)
    }

    /// A quota read started before a request failure cannot supersede it.
    /// Identity/credential validation is performed by the refresh owner.
    pub(crate) fn quota(&self, owner: &str, revision: u64, quota: &Value) {
        if let Ok(mut facts) = self.0.lock()
            && facts.revision == revision
            && (facts.quotas.len() < MAX_FACTS || facts.quotas.contains_key(owner))
        {
            facts.quotas.insert(
                owner.to_owned(),
                QuotaObservation {
                    observed_at: now(),
                    quota: quota.clone(),
                },
            );
        }
    }
}

fn confirmed_quota(error: &Value) -> bool {
    ["code", "type"].iter().any(|field| {
        matches!(
            error[*field].as_str(),
            Some(
                "usage_limit_reached"
                    | "insufficient_quota"
                    | "quota_exhausted"
                    | "billing_hard_limit_reached"
            )
        )
    })
}

fn exhausted_bucket(quota: &Value, id: &str, time: u64) -> Option<u64> {
    let bucket = quota["rate_limits_by_limit_id"]
        .get(id)
        .or_else(|| (quota["rate_limits"]["limitId"] == id).then_some(&quota["rate_limits"]))?;
    ["primary", "secondary"]
        .iter()
        .filter_map(|name| {
            let window = &bucket[*name];
            // usedPercent is presentation data. An explicit allowance/exhaustion
            // flag is required until this upstream's percentage semantics are verified.
            (window["limitReached"] == true || window["allowed"] == false)
                .then(|| {
                    window["resetsAt"]
                        .as_u64()
                        .unwrap_or(time + RECHECK_SECONDS)
                })
                .map(|reset| reset.max(time.saturating_add(RECHECK_SECONDS)))
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn subject() -> Subject {
        Subject {
            owner: "owner".into(),
            version: "v1".into(),
            source: "endpoint-owner".into(),
            model: "model".into(),
            bucket: Some("codex".into()),
            probe: None,
        }
    }
    #[test]
    fn failure_invalidates_inflight_read_and_old_success_cannot_clear_it() {
        let state = Availability::default();
        let mut s = subject();
        let revision = state.revision();
        assert!(!state.failure(&s, "quota_exhausted", None));
        assert!(state.failure(&s, "quota_exhausted_confirmed", None));
        state.quota("owner", revision, &json!({}));
        state.event(&s, &json!({"type":"response.completed"}));
        assert!(state.check(&mut s, now(), true).is_err());
        assert!(state.0.lock().unwrap().quotas.is_empty());
        let mut other = s.clone();
        other.model = "other-model".into();
        assert!(state.check(&mut other, now(), true).is_ok());
    }
    #[test]
    fn recovery_is_single_flight_and_guard_releases_without_claiming_health() {
        let state = Arc::new(Availability::default());
        let mut s = subject();
        state.failure(&s, "quota_exhausted_confirmed", None);
        let time = now() + RECHECK_SECONDS + 1;
        state.check(&mut s, time, true).unwrap();
        assert!(state.check(&mut subject(), time, true).is_err());
        let guard = Admission {
            state: state.clone(),
            subject: Some(s.clone()),
        };
        drop(guard);
        assert!(state.check(&mut subject(), now(), true).is_err());
        let mut next = subject();
        state
            .check(&mut next, time + RECHECK_SECONDS, true)
            .unwrap();
        state.event(&next, &json!({"type":"response.completed"}));
        assert!(state.check(&mut subject(), time, true).is_ok());
    }
    #[test]
    fn percentages_unknown_buckets_and_multiple_windows_are_not_conflated() {
        let quota = json!({"rate_limits_by_limit_id":{"codex":{"primary":{"usedPercent":100},"secondary":{"usedPercent":99}},
            "spark":{"primary":{"limitReached":true,"resetsAt":200},"secondary":{"allowed":false,"resetsAt":400}}}});
        assert_eq!(exhausted_bucket(&quota, "codex", 100), None);
        assert_eq!(exhausted_bucket(&quota, "missing", 100), None);
        assert_eq!(exhausted_bucket(&quota, "spark", 250), Some(400));
    }

    #[test]
    fn retry_after_is_a_separate_block_and_network_failure_keeps_quota_unknown() {
        let state = Availability::default();
        let mut s = subject();
        s.source = "owner:endpoint:default".into();
        assert!(!state.failure(&s, "network", None));
        assert_eq!(state.snapshot("owner")["state"], "unknown");
        assert!(state.check(&mut s, now(), true).is_ok());
        assert!(state.failure(&s, "rate_limited", Some(10)));
        let error = state.check(&mut s, now(), true).unwrap_err();
        assert_eq!(error.native().body["error"]["code"], "rate_limit_exceeded");
        assert_eq!(state.snapshot("owner")["state"], "rate_limited");
        state.check(&mut s, now() + 11, true).unwrap();
        assert!(state.check(&mut subject(), now(), true).is_ok());
        state.event(&s, &json!({"type":"response.completed"}));
        assert!(state.check(&mut s, now(), true).is_ok());
    }
}
