use ic_cdk::api::{canister_status, time, CanisterStatusCode};
use ic_cdk::call::{Call, CallErrorExt, CallFailed, Response};

#[derive(Debug, Clone)]
pub enum Deadline {
    Time(u64),
    Stopping,
    TimeOrStopping(u64),
}

#[derive(Debug, Clone)]
pub enum ErrorCause {
    CallFailed(CallFailed),
    GaveUpRetrying,
}


/// An error type for retried best-effort response calls.
#[derive(Clone, Debug)]
pub enum RetryError {
    /// We know that the call failed, and there is no point in retrying
    CallFailed(ErrorCause),
    /// A fatal error. We don't know whether the call failed, but there is no point in retrying.
    StatusUnknown(ErrorCause),

}

pub async fn retry_idempotent_bounded_wait_call<'a, 'm, P>(
    call: Call<'a, 'm>,
    should_retry: &P,
) -> Result<Response, RetryError>
where
    P: Fn() -> bool,
{
    let mut no_unknown_results = true;

    loop {
        if !should_retry() {
            return Err(if no_unknown_results {
                RetryError::CallFailed(ErrorCause::GaveUpRetrying)
            } else {
                RetryError::StatusUnknown(ErrorCause::GaveUpRetrying)
            });
        }

        match call.clone().await {
            Ok(result) => return Ok(result),
            // Unretriable error due to a result decoding error
            Err(e) if !e.is_immediately_retryable() => {
                if no_unknown_results {
                    return Err(RetryError::CallFailed(ErrorCause::CallFailed(e)));
                } else {
                    return Err(RetryError::StatusUnknown(ErrorCause::CallFailed(e)));
                }
            }
            // Retry a non-sync SysUnknown
            Err(e) if !e.is_clean_reject() => {
                no_unknown_results = false;
                continue;
            }
            // The only remaining option is a non-sync SysTransient => retry
            Err(_e) => continue,
        }
    }
}

pub async fn retry_idempotent_bounded_wait_call_until<'a, 'm>(
    call: Call<'a, 'm>,
    deadline: &Deadline,
) -> Result<Response, RetryError>
where
{
    retry_idempotent_bounded_wait_call(call, &|| !out_of_time_or_stopping(deadline)).await
}

/// Retries a non-idempotent call best-effort response call until the deadline is reached,
/// the caller canister is stopping, or a non-retryable error occurs.
pub async fn retry_nonidempotent_best_effort_response_call_until<'m, 'a>(
    call: Call<'m, 'a>,
    deadline: &Deadline,
) -> Result<Response, RetryError>
where
{
    loop {
        if out_of_time_or_stopping(deadline) {
            return Err(
                RetryError::CallFailed(ErrorCause::GaveUpRetrying)
            );
        }

        match call.clone().await {
            Ok(res) => return Ok(res),
            Err(e) if !e.is_immediately_retryable() => {
                return Err(RetryError::CallFailed(ErrorCause::CallFailed(e)))
            }
            Err(e)
            if !e.is_clean_reject() =>
                {
                    return Err(RetryError::StatusUnknown(ErrorCause::CallFailed(e)))
                }
            // Non-sync SysTransient => retry
            Err(_e) => continue,
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
