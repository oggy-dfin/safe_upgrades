use candid::CandidType;
use ic_cdk::api::{canister_status, time, CanisterStatusCode};
use ic_cdk::call::{Call, CallError, CallRejected, RejectCode};

#[derive(Debug, Clone)]
pub enum Deadline {
    Time(u64),
    Stopping,
    TimeOrStopping(u64),
}

#[derive(Clone, Debug)]
pub enum Unretriable {
    Reject(CallRejected),
    DecodingFailed(String),
}

/// An error type for retried best-effort response calls.
#[derive(Clone, Debug)]
pub enum RetryError {
    /// We know that the call failed, and there is no point in retrying
    CallFailed(Unretriable),
    /// A fatal error. We don't know whether the call failed, but there is no point in retrying.
    Fatal(Unretriable),
    /// We don't know whether the call failed or succeeded, but the caller instructed us to not
    /// retry any more.
    GaveUp,
}

fn is_retryable(rejection: &CallRejected) -> bool {
    !rejection.is_sync()
        && (rejection.reject_code() == RejectCode::SysUnknown
        || rejection.reject_code() == RejectCode::SysTransient)
}

pub async fn retry_idempotent_best_effort_response_call<'a, 'm, P, R>(
    call: Call<'a, 'm>,
    should_retry: P,
) -> Result<R, RetryError>
where
    R: CandidType + for<'de> candid::Deserialize<'de>,
    P: Fn() -> bool,
{
    let mut no_unknown_results = true;

    loop {
        if !should_retry() {
            return Err(RetryError::GaveUp);
        }

        match call.call().await {
            Ok(result) => return Ok(result),
            // Unretriable error due to a result decoding error
            Err(CallError::CandidDecodeFailed(msg)) => {
                if no_unknown_results {
                    return Err(RetryError::CallFailed(Unretriable::DecodingFailed(msg)));
                } else {
                    return Err(RetryError::Fatal(Unretriable::DecodingFailed(msg)));
                }
            }
            // Unretriable rejection errors
            Err(CallError::CallRejected(rejection)) if !is_retryable(&rejection) => {
                if no_unknown_results {
                    return Err(RetryError::CallFailed(Unretriable::Reject(rejection)));
                } else {
                    return Err(RetryError::Fatal(Unretriable::Reject(rejection)));
                }
            }
            // Retry a non-sync SysUnknown
            Err(CallError::CallRejected(rejection))
            if rejection.reject_code() == RejectCode::SysUnknown =>
                {
                    no_unknown_results = false;
                    continue;
                }
            // The only remaining option is a non-sync SysTransient => retry
            Err(CallError::CallRejected(_rejection)) => continue,
        }
    }
}

pub async fn retry_idempotent_best_effort_response_call_until<'a, 'm, R>(
    call: Call<'a, 'm>,
    deadline: &Deadline,
) -> Result<R, RetryError>
where
    R: CandidType + for<'de> candid::Deserialize<'de>,
{
    retry_idempotent_best_effort_response_call(call, || !out_of_time_or_stopping(deadline)).await
}

/// Retries a non-idempotent call best-effort response call until the deadline is reached,
/// the caller canister is stopping, or a non-retryable error occurs.
pub async fn retry_nonidempotent_best_effort_response_call_until<'a, 'm, R>(
    call: Call<'a, 'm>,
    deadline: &Deadline,
) -> Result<R, RetryError>
where
    R: CandidType + for<'de> candid::Deserialize<'de>,
{
    loop {
        if out_of_time_or_stopping(deadline) {
            return Err(RetryError::GaveUp);
        }

        match call.call().await {
            Ok(res) => return Ok(res),
            Err(CallError::CandidDecodeFailed(msg)) => {
                return Err(RetryError::Fatal(Unretriable::DecodingFailed(msg)))
            }
            Err(CallError::CallRejected(rejection)) if !is_retryable(&rejection) => {
                return Err(RetryError::CallFailed(Unretriable::Reject(rejection)))
            }
            Err(CallError::CallRejected(rejection))
            if rejection.reject_code() == RejectCode::SysUnknown =>
                {
                    return Err(RetryError::Fatal(Unretriable::Reject(rejection)))
                }
            // Non-sync SysTransient => retry
            Err(CallError::CallRejected(_rejection)) => continue,
        }
    }
}

/// Checks if we have exceeded our deadline or if the caller canister is stopping (if that’s part of the config).
fn out_of_time_or_stopping(deadline: &Deadline) -> bool {
    match deadline {
        Deadline::Time(dl) => time() >= *dl,
        Deadline::Stopping => canister_status() == CanisterStatusCode::Stopping,
        Deadline::TimeOrStopping(dl) => {
            canister_status() == CanisterStatusCode::Stopping || time() >= *dl
        }
    }
}
