use candid::Principal;
use ic_cdk::api::{
    management_canister::main::{
        CanisterInfoRequest, CanisterInfoResponse, CanisterInstallMode, ClearChunkStoreArgument,
        InstallChunkedCodeArgument, InstallCodeArgument, UploadChunkArgument,
    },
};
use ic_cdk::api::time;
use ic_cdk::call::{Call, CallErrorExt};

pub mod retry;
use retry::{
    retry_idempotent_bounded_wait_call_until, retry_nonidempotent_best_effort_response_call_until,
    RetryError, Deadline,
};
use crate::retry::ErrorCause;

/// Represents a canister's principal ID on the IC.
pub type CanisterId = Principal;

/// Timeout options for `upgrade_canister_until`.
#[derive(Debug, Clone)]
pub enum Timeout {
    /// Retry the upgrade steps until the given number of seconds has elapsed (based on IC time).
    Seconds(u64),

    /// Retry until the *caller* canister starts stopping, with no other time limit.
    UntilCallerStopping,

    /// Retry until either the caller canister is stopping or `seconds` have elapsed, whichever comes first.
    SecondsOrUntilCallerStopping(u64),
}

/// Describes the stage of the upgrade during which an error occurred
/// or after which we could not confirm status.
#[derive(Debug, Clone, Copy)]
pub enum UpgradeStage {
    Stopping,
    ObtainingInfo,
    Installing,
    Starting,
}

/// Errors returned by `upgrade_canister_until`.
#[derive(Debug)]
pub struct UpgradeError {
    pub stage: UpgradeStage,
    pub error: RetryError,
}

/// Holds the meta-information needed for a chunked WASM install.
#[derive(Debug, Clone)]
pub struct ChunkedModule {
    /// SHA-256 hash of the entire WASM to be installed.
    pub wasm_module_hash: Vec<u8>,

    /// The canister storing the chunks (must be on the same subnet).
    pub store_canister_id: CanisterId,

    /// The list of chunk hashes that compose the WASM.
    pub chunk_hashes_list: Vec<Vec<u8>>,
}

/// The WASM to be installed.
#[derive(Debug, Clone)]
pub enum WasmModule {
    /// A module < 2MB that can be installed in a single message
    Bytes(Vec<u8>),
    /// A module > 2MB that must be installed in chunks. Chunks are assumed to already have been uploaded.
    ChunkedModule(ChunkedModule),
}

/// Safely upgrade a canister to a new version, without blocking the caller from
/// upgrading itself.
///
/// Stops, installs, and then restarts the target canister.
/// Uses best-effort responses under the hood, ensuring that the caller isn't blocked
/// from upgrading itself due to open call contexts.
/// It retries any failed best-effort response calls until the specified
/// timeout is reached or the caller canister starts stopping.
/// In corner cases, it may be unknown whether the upgrade
/// succeeded (as indicated by the `StatusUnknown` return variant).
///
/// # Procedure
///
/// 1. **Stop** the canister C via a best-effort call (`SysUnknown` => retry).
///    - Because `stop_canister` is idempotent, we can safely retry until definite success.
/// 2. **Obtain** the current version (`canister_info`) to record the old WASM hash.
/// 3. **Upgrade** the canister. If `SysUnknown` is returned, call `canister_info` again:
///    - If the canister’s hash changed, we know the upgrade went through.
///    - If not, we retry or eventually give up as `StatusUnknown`.
/// 4. **Start** the canister again, also with best-effort calls.
///
/// # Returns
/// * `Ok(())` if we can confirm a successful upgrade.
/// * `Err(UpgradeError::UpgradeFailed(...))` if the upgrade failed definitively.
/// * `Err(UpgradeError::StatusUnknown(...))` if we cannot confirm success or failure.
pub async fn upgrade_canister_until(
    target_id: CanisterId,
    wasm_module: WasmModule,
    arg: Vec<u8>,
    timeout: Timeout,
) -> Result<(), UpgradeError> {
    // Converts a `BestEffortError` into an `UpgradeError` at a given stage.
    let add_stage =
        |stage: UpgradeStage| move |error: RetryError| UpgradeError { stage, error };

    let deadline = timeout_to_deadline(&timeout);
    // 1) Stop the canister (best-effort).
    best_effort_stop(target_id, &deadline)
        .await
        .map_err(add_stage(UpgradeStage::Stopping))?;

    // 2) Query the current canister version for reference.
    let version = best_effort_canister_info(target_id, &deadline)
        .await
        .map(|info| info.total_num_changes)
        .map_err(add_stage(UpgradeStage::ObtainingInfo))?;

    // 3) Install (upgrade) the new WASM. Loop until success or timeout. We can't retry directly
    // here if we don't know what happened, since installation isn't idempotent. Instead, use the
    // version number to determine if the upgrade went through.
    loop {
        let install_result = match wasm_module {
            WasmModule::Bytes(ref wasm_bytes) => {
                best_effort_install_single_chunk(target_id, wasm_bytes, &arg, &deadline).await
            }
            WasmModule::ChunkedModule(ref chunked) => {
                best_effort_install_chunked(target_id, chunked, &arg, &deadline).await
            }
        };

        match install_result {
            Ok(()) => {}
            // Note that for installation, unretriable errors include `SysUnknown`
            // Try to figure out what happened using the version and retry if the version
            // hasn't moved
            Err(RetryError::StatusUnknown(ErrorCause::CallFailed(rejection)))
            if !rejection.is_clean_reject() =>
                {
                    let new_version = best_effort_canister_info(target_id, &deadline)
                        .await
                        .map(|info| info.total_num_changes)
                        .map_err(add_stage(UpgradeStage::Installing))?;
                    if new_version <= version {
                        continue;
                    } else {
                        break;
                    }
                }
            Err(error) => {
                return Err(UpgradeError {
                    stage: UpgradeStage::Installing,
                    error,
                });
            }
        };
    }

    best_effort_start(target_id, &deadline)
        .await
        .map_err(add_stage(UpgradeStage::Starting))
}

/// Stop a canister with best-effort calls until success or timeout.
async fn best_effort_stop(
    target_id: Principal,
    deadline: &Deadline,
) -> Result<(), RetryError> {
    Ok(retry_idempotent_bounded_wait_call_until(
        Call::bounded_wait(Principal::management_canister(), "stop_canister").with_arg(target_id.clone()),
        deadline,
    )
        // TODO: don't unwrap here
        .await?.candid().unwrap())
}

/// Start a canister with best-effort calls until success or timeout.
async fn best_effort_start(
    target_id: CanisterId,
    deadline: &Deadline,
) -> Result<(), RetryError> {
    Ok(retry_idempotent_bounded_wait_call_until(
        Call::bounded_wait(Principal::management_canister(), "start_canister").with_arg(target_id),
        deadline,
    )
        // TODO: don't unwrap here
        .await?.candid().unwrap())
}

/// Retrieve canister info (including module hash) with best-effort calls.
async fn best_effort_canister_info(
    target_id: CanisterId,
    deadline: &Deadline,
) -> Result<CanisterInfoResponse, RetryError> {
    let arg = CanisterInfoRequest {
        canister_id: target_id,
        num_requested_changes: None,
    };

    Ok(retry_idempotent_bounded_wait_call_until(
        Call::bounded_wait(Principal::management_canister(), "canister_info").with_arg(arg),
        deadline,
    )
        // TODO: don't unwrap here
        .await?.candid().unwrap())
}

/// Install a small (<2MB) WASM in a single call via `install_code`.
/// Since code installation isn't idempotent, we don't just retry on `SysUnknown`.
/// Rather, we leave it up to the caller to handle.
async fn best_effort_install_single_chunk(
    target_id: CanisterId,
    wasm_bytes: &[u8],
    arg: &[u8],
    deadline: &Deadline,
) -> Result<(), RetryError> {
    // We use the extended argument to include `sender_canister_version`:
    let install_args = InstallCodeArgument {
        mode: CanisterInstallMode::Upgrade(None),
        canister_id: target_id,
        wasm_module: wasm_bytes.to_vec(),
        arg: arg.to_vec(),
    };

    Ok(retry_nonidempotent_best_effort_response_call_until(
        Call::bounded_wait(Principal::management_canister(), "install_code").with_arg(&install_args),
        deadline,
    )
        // TODO: don't unwrap here
        .await?.candid().unwrap())
}

#[allow(dead_code)]
async fn upload_chunks(
    store_canister_id: CanisterId,
    chunks: Vec<Vec<u8>>,
    deadline: &Deadline,
) -> Result<(), RetryError> {
    // First, clear the chunk store. This is idempotent, so we can retry.
    let call = Call::bounded_wait(Principal::management_canister(), "clear_chunk_store").with_arg(
        ClearChunkStoreArgument {
            canister_id: store_canister_id,
        },
    );

    // TODO: don't unwrap here?
    let _: () = retry_idempotent_bounded_wait_call_until(call, deadline).await?.candid().unwrap();

    for chunk in chunks {
        let chunk_install_args = UploadChunkArgument {
            canister_id: store_canister_id,
            chunk,
        };

        // Uploading chunks is also idempotent, so we can retry.
        let call = Call::bounded_wait(Principal::management_canister(), "upload_chunk")
            .with_arg(chunk_install_args);
        // TODO: don't unwrap here
        let _: () = retry_idempotent_bounded_wait_call_until(call, deadline).await?.candid().unwrap();
    }
    Ok(())
}

/// Install a large (>2MB) WASM by referencing pre-uploaded chunks, via `install_chunked_code`.
/// Chunks are assumed to already have been uploaded
async fn best_effort_install_chunked(
    target_id: CanisterId,
    chunked: &ChunkedModule,
    arg: &[u8],
    deadline: &Deadline,
) -> Result<(), RetryError> {
    let install_args = InstallChunkedCodeArgument {
        mode: CanisterInstallMode::Upgrade(None),
        target_canister: target_id,
        store_canister: Some(chunked.store_canister_id),
        chunk_hashes_list: chunked
            .chunk_hashes_list
            .iter()
            .map(|hash| ic_cdk::api::management_canister::main::ChunkHash { hash: hash.clone() })
            .collect(),
        wasm_module_hash: chunked.wasm_module_hash.clone(),
        arg: arg.to_vec(),
    };

    let install_call =
        Call::bounded_wait(Principal::management_canister(), "install_chunked_code").with_arg(&install_args);
    let res = retry_nonidempotent_best_effort_response_call_until(install_call, deadline).await?;
    Ok(res.candid().unwrap())
}

/// Returns the “deadline” as a nanosecond timestamp (IC time), or `None` if the timeout is only “UntilCallerStopping”.
fn timeout_to_deadline(timeout: &Timeout) -> Deadline {
    match timeout {
        Timeout::Seconds(secs) => Deadline::Time(time() + *secs),
        Timeout::UntilCallerStopping => Deadline::Stopping,
        Timeout::SecondsOrUntilCallerStopping(secs) => Deadline::TimeOrStopping(time() + *secs),
    }
}
