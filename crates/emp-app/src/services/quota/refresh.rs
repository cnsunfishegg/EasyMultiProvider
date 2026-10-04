//! One query per real quota owner. Waiting callers share its result instead
//! of lining up more helper processes. Credential locks still own rotations.
use super::*;
use std::collections::BTreeMap;
use std::sync::{Condvar, atomic::AtomicUsize};
use std::time::Instant;

type ResultSnapshot = Result<Value, QuotaError>;
#[derive(Default)]
pub(crate) struct RefreshCoordinator {
    flights: Mutex<BTreeMap<String, Arc<Flight>>>,
    active: AtomicUsize,
}

#[derive(Default)]
struct Flight {
    state: Mutex<FlightState>,
    finished: Condvar,
}
#[derive(Default)]
struct FlightState {
    running: bool,
    failures: u32,
    result: Option<(Instant, String, ResultSnapshot)>,
    requested: Option<Instant>,
    generation: u64,
}

impl RefreshCoordinator {
    fn flight(&self, owner: &str) -> Option<Arc<Flight>> {
        let mut flights = self.flights.lock().ok()?;
        if flights.len() >= 1024 && !flights.contains_key(owner) {
            flights.retain(|_, flight| {
                Arc::strong_count(flight) > 1
                    || flight.state.lock().is_ok_and(|current| {
                        current.running
                            || current
                                .result
                                .as_ref()
                                .map(|(at, _, _)| *at)
                                .into_iter()
                                .chain(current.requested)
                                .any(|at| at.elapsed() < Duration::from_secs(900))
                    })
            });
            if flights.len() >= 1024 {
                return None;
            }
        }
        Some(Arc::clone(flights.entry(owner.to_owned()).or_default()))
    }

    pub(super) fn schedule(&self, owner: &str) -> bool {
        let Some(flight) = self.flight(owner) else {
            return false;
        };
        let Ok(mut current) = flight.state.lock() else {
            return false;
        };
        if current.running
            || current
                .requested
                .is_some_and(|at| at.elapsed() < Duration::from_secs(60))
        {
            return false;
        }
        current.requested = Some(Instant::now());
        true
    }

    pub(super) fn invalidate(&self, owner: &str) {
        if let Ok(flights) = self.flights.lock()
            && let Some(flight) = flights.get(owner)
            && let Ok(mut current) = flight.state.lock()
        {
            current.generation = current.generation.wrapping_add(1);
            current.result = None;
        }
    }
}

struct Running<'a> {
    coordinator: &'a RefreshCoordinator,
    flight: Arc<Flight>,
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.coordinator.active.fetch_sub(1, Ordering::AcqRel);
        if let Ok(mut state) = self.flight.state.lock() {
            state.running = false;
        }
        self.flight.finished.notify_all();
    }
}

pub(super) fn refresh(state: &ServerState, account_id: &str, force: bool) -> ResultSnapshot {
    let identity = super::super::availability::account_identity(state, account_id);
    let (owner, version) = identity
        .clone()
        .unwrap_or_else(|| (format!("unknown:{account_id}"), String::new()));
    let coordinator = &state.backend.accounts.quota_refreshes;
    let flight = coordinator.flight(&owner).ok_or_else(unavailable)?;
    let mut current = flight.state.lock().map_err(|_| unavailable())?;
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut shared = false;
    while current.running {
        shared = true;
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || state.shutdown.load(Ordering::Acquire) {
            return Err(QuotaError::new(
                "A shared quota check is still in progress",
                "quota_refresh_pending",
            ));
        }
        current = flight
            .finished
            .wait_timeout(current, left)
            .map_err(|_| unavailable())?
            .0;
    }
    if let Some((finished, credential, result)) = &current.result {
        let ttl = if result.is_ok() {
            2
        } else {
            (30u64 << current.failures.saturating_sub(1).min(5)).min(900)
        };
        let save_failure = result
            .as_ref()
            .err()
            .is_some_and(|e| e.code() == "quota_credentials_save_failed");
        let pending_rotation = state
            .backend
            .accounts
            .pending_rotations
            .lock()
            .is_ok_and(|p| !p.is_empty());
        if !save_failure
            && !pending_rotation
            && (shared
                || ((!force || result.is_err())
                    && credential == &version
                    && finished.elapsed() < Duration::from_secs(ttl)))
        {
            // A different import alias shares only the quota, not its display
            // identity or credential status. Identity replacements never join.
            let current_identity = super::super::availability::account_identity(state, account_id);
            if !current_identity
                .as_ref()
                .is_some_and(|(current_owner, current_version)| {
                    current_owner == &owner
                        && (current_identity == identity || current_version == credential)
                })
            {
                return Err(QuotaError::new(
                    "account changed during quota refresh",
                    "quota_error",
                ));
            }
            let mut snapshot = if account_id == "@native" {
                let config = state
                    .backend
                    .configuration
                    .read()
                    .map_err(|_| unavailable())?;
                native_account_snapshot(state, &config)
            } else {
                account_public_snapshot(state, account_id).ok_or_else(unavailable)?
            };
            snapshot["quota"] = result.as_ref().map_err(Clone::clone)?["quota"].clone();
            return Ok(snapshot);
        }
    }
    if coordinator.active.fetch_add(1, Ordering::AcqRel) >= 4 {
        coordinator.active.fetch_sub(1, Ordering::AcqRel);
        return Err(QuotaError::new(
            "Quota helper capacity is busy",
            "quota_refresh_pending",
        ));
    }
    current.running = true;
    let generation = current.generation;
    drop(current);
    let running = Running {
        coordinator,
        flight: Arc::clone(&flight),
    };
    let result = (|| {
        let lock = quota_refresh_lock(state, account_id).ok_or_else(unavailable)?;
        let _guard = lock.lock().map_err(|_| unavailable())?;
        if super::super::availability::account_identity(state, account_id) != identity {
            return Err(QuotaError::new(
                "account changed during quota refresh",
                "quota_error",
            ));
        }
        refresh_account_by_id(state, account_id)
    })();
    let current_version = super::super::availability::account_identity(state, account_id)
        .map(|(_, v)| v)
        .unwrap_or(version);
    if let Ok(mut current) = flight.state.lock()
        && current.generation == generation
    {
        current.failures = if result.is_ok() {
            0
        } else {
            current.failures.saturating_add(1)
        };
        current.result = Some((Instant::now(), current_version, result.clone()));
    }
    drop(running);
    result
}

fn unavailable() -> QuotaError {
    QuotaError::new("Codex account quota check failed", "quota_error")
}
