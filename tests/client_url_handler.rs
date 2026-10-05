#![cfg(unix)]
pub mod support;

use serde_json::json;
use std::time::Duration;
use support::url_handler::HandlerFixture;

#[test]
fn client_url_handler_runs_real_process_and_drains_stderr() {
    let f = HandlerFixture::start(json!({"stderr_flood":true}));
    f.submit("success", 20_000);
    let result = f.completion();
    assert_eq!(result["request_id"], "success");
    assert_eq!(result["result"]["outcome"], "opened");
    assert_eq!(f.invocations().len(), 1);
    assert_eq!(f.invocations()[0]["action"]["url"], "https://example.com/");
}
#[test]
fn client_url_handler_serializes_deduplicates_and_expires_queued_work() {
    let f = HandlerFixture::start(json!({"hold":true}));
    f.submit("first", 20_000);
    f.wait_started();
    f.submit("first", 20_000);
    f.submit("expired", 80);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(f.invocations().len(), 1);
    f.release();
    assert_eq!(f.completion()["request_id"], "first");
    let expired = f.completion();
    assert_eq!(expired["request_id"], "expired");
    assert_eq!(expired["result"]["error"], "handler_timeout");
    assert_eq!(f.invocations().len(), 1);
}
#[test]
fn client_url_handler_snapshots_argv_but_reloads_future_requests() {
    let f = HandlerFixture::start(json!({"hold":true}));
    f.submit("first", 20_000);
    f.wait_started();
    f.submit("queued", 20_000);
    std::thread::sleep(Duration::from_millis(100));
    std::fs::write(
        &f.config,
        "onboarding=false\n[client]\nopen_url_command=[]\n",
    )
    .unwrap();
    f.release();
    assert_eq!(f.completion()["result"]["ok"], true);
    assert_eq!(f.completion()["result"]["ok"], true);
    f.submit("disabled", 20_000);
    assert_eq!(f.completion()["result"]["error"], "handler_failed");
    assert_eq!(f.invocations().len(), 2);
}
#[test]
fn client_url_handler_rejects_invalid_output_and_exit_combinations() {
    for scenario in [
        json!({"mode":"malformed"}),
        json!({"mode":"pretty"}),
        json!({"mode":"unterminated"}),
        json!({"mode":"multiple"}),
        json!({"mode":"oversized"}),
        json!({"exit_code":1}),
        json!({"mode":"failure"}),
    ] {
        let f = HandlerFixture::start(scenario);
        f.submit("invalid", 20_000);
        let result = f.completion();
        assert_eq!(result["result"]["ok"], false);
    }
}
#[test]
fn client_url_handler_timeout_kills_only_owned_child_and_client_stays_responsive() {
    let f = HandlerFixture::start(json!({"delay_ms":5000}));
    f.submit("timeout", 180);
    f.wait_started();
    let result = f.completion();
    assert_eq!(result["result"]["error"], "handler_timeout");
    assert!(f.server.call("client.list", json!({}))["result"]["clients"]
        .as_array()
        .is_some_and(|c| !c.is_empty()));
    assert!(
        f.messages.recv_timeout(Duration::from_millis(50)).is_err() || !f.invocations().is_empty()
    );
}
