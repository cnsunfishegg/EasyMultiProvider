//! Existing management endpoints share the credential -> local catalog stages.
//! Publishing is idempotent; activation and live verification remain explicit
//! existing integration operations under the integration manager's lease.
use crate::app::ServerState;
use serde_json::{Value, json};
use std::path::PathBuf;

pub(crate) fn import(state: &ServerState, body: &Value) -> Result<Value, String> {
    let account = super::accounts::import_account_state(state, body)?;
    let publication = publish(state);
    Ok(json!({"account":account, "operation": receipt(publication.is_ok())}))
}

pub(crate) fn publish(state: &ServerState) -> Result<(PathBuf, usize), ()> {
    let result = super::catalog::refresh_catalog(state)?;
    super::account_catalog::request_refresh(state, false);
    Ok(result)
}

pub(crate) fn receipt(published: bool) -> Value {
    json!({
        "scope":"credentials_and_local_catalog",
        "state":if published { "completed" } else { "publishing_pending" },
        "stages":{"credentials_saved":true,"local_catalog_published":published,"client_catalog_verified":false,"inference_verified":false},
        "next_action":if published { "use_existing_integration_status" } else { "retry_existing_catalog_refresh" }
    })
}
