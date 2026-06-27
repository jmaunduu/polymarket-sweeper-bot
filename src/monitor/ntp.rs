//! NTP clock verification.
//!
//! Checks at startup that the system clock is within max_drift_ms of real time.
//! Re-checks periodically during operation.
//! Refuses to start if drift exceeds threshold because a drifted clock produces
//! HMAC auth failures that look like credential problems.

use anyhow::{bail, Result};
use rsntp::SntpClient;
use std::time::Duration;
use tracing::{error, info, warn};

/// Result of a single NTP check.
#[derive(Debug)]
pub struct NtpStatus {
    pub drift_ms: f64,
    pub synchronized: bool,
}

/// Query the configured NTP server and return clock drift in milliseconds.
/// Positive drift means the local clock is ahead. Negative means it is behind.
pub async fn check_drift(server: &str) -> Result<NtpStatus> {
    let client = SntpClient::new();
    let server = server.to_string();

    let result = tokio::task::spawn_blocking(move || client.synchronize(&server)).await??;
    let offset = result.clock_offset();
    let drift_ms = offset.as_secs_f64() * 1000.0;

    Ok(NtpStatus {
        drift_ms,
        synchronized: true,
    })
}

/// Startup check: fails hard if drift exceeds threshold.
/// Call this before any API connections are opened.
pub async fn enforce_sync(server: &str, max_drift_ms: f64) -> Result<()> {
    info!("Checking NTP clock drift against {}...", server);

    match check_drift(server).await {
        Ok(status) => {
            let abs_drift = status.drift_ms.abs();
            if abs_drift > max_drift_ms {
                error!(
                    "Clock drift {:.1}ms exceeds limit {:.0}ms. Fix with: sudo systemctl restart ntp OR sudo chronyc makestep",
                    abs_drift, max_drift_ms
                );
                bail!(
                    "Clock drift {:.1}ms too large (limit: {:.0}ms). HMAC auth will fail. Run: sudo timedatectl set-ntp true",
                    abs_drift, max_drift_ms
                );
            }

            info!(
                "NTP OK: clock drift {:.1}ms (limit: {:.0}ms)",
                abs_drift, max_drift_ms
            );
            Ok(())
        }
        Err(err) => {
            // NTP reachability is best-effort here. The OS NTP daemon is still
            // the primary sync mechanism.
            warn!(
                "NTP check failed ({}). Falling back to OS sync status.",
                err
            );
            check_os_ntp_status()
        }
    }
}

/// Best-effort fallback that checks common OS-level NTP status markers.
fn check_os_ntp_status() -> Result<()> {
    if std::path::Path::new("/run/systemd/timesync/synchronized").exists() {
        info!("OS NTP sync confirmed via systemd-timesyncd.");
        return Ok(());
    }

    if std::path::Path::new("/var/run/chrony/chronyd.pid").exists() {
        info!("chrony running; assuming NTP sync.");
        return Ok(());
    }

    warn!(
        "Could not confirm NTP sync. Proceeding, but watch for auth failures. Fix: sudo apt install ntp && sudo systemctl enable ntp"
    );
    Ok(())
}

/// Spawn a background task that re-checks NTP drift every `interval_mins`.
/// Logs warnings but does not halt the bot if drift grows during operation.
pub fn spawn_periodic_check(server: String, max_drift_ms: f64, interval_mins: u64) {
    if interval_mins == 0 {
        return;
    }

    tokio::spawn(async move {
        let interval = Duration::from_secs(interval_mins * 60);
        loop {
            tokio::time::sleep(interval).await;
            match check_drift(&server).await {
                Ok(status) => {
                    if status.drift_ms.abs() > max_drift_ms {
                        warn!(
                            "NTP drift growing: {:.1}ms. Auth may start failing.",
                            status.drift_ms
                        );
                    } else {
                        info!("NTP periodic check OK: drift {:.1}ms", status.drift_ms);
                    }
                }
                Err(err) => warn!("Periodic NTP check failed: {}", err),
            }
        }
    });
}
