#![cfg(unix)]
pub mod support;
use serde_json::json;
use std::time::Duration;
use support::client_url::UrlFixture;

#[test]
fn client_url_bridge_cli_lists_clients_and_explicit_foreground_overrides_stale_origin() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    f.focus(&a);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["client", "list", "--json"])
        .env("HERDR_SOCKET_PATH", &f.api)
        .env_remove("HERDR_SESSION")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let list: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(list["result"]["clients"][0]["client_id"], a.public_id);
    let socket = f.api.clone();
    let pending = std::thread::spawn(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args([
                "client",
                "open-url",
                "https://example.com/",
                "--client",
                "foreground",
                "--json",
            ])
            .env("HERDR_SOCKET_PATH", socket)
            .env("HERDR_ORIGIN_CLIENT_ID", "stale")
            .env_remove("HERDR_SESSION")
            .output()
            .unwrap()
    });
    let request = f.next_open(&a);
    f.complete(&a, &request, "opened");
    let output = pending.join().unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["result"]["client_id"],
        a.public_id
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["client", "open-url", "https://example.com/", "--json"])
        .env("HERDR_SOCKET_PATH", &f.api)
        .env("HERDR_ORIGIN_CLIENT_ID", "stale")
        .env_remove("HERDR_SESSION")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["error"]["code"],
        "client_unavailable"
    );
}

#[test]
fn client_url_bridge_cli_help_does_not_start_internal_client() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["client", "--help"])
        .env_remove("HERDR_SOCKET_PATH")
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("open-url"));
}

#[test]
fn client_url_bridge_cli_partial_reply_has_absolute_deadline_and_no_retry() {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::time::{Duration, Instant};
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("herdr-url-cli-stalled-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("api.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let started = Instant::now();
    let child = std::thread::spawn(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["client", "open-url", "https://example.com/", "--json"])
            .env("HERDR_SOCKET_PATH", path)
            .env_remove("HERDR_SESSION")
            .env_remove("HERDR_ORIGIN_CLIENT_ID")
            .output()
            .unwrap()
    });
    let (mut connection, _) = listener.accept().unwrap();
    let mut request = String::new();
    BufReader::new(connection.try_clone().unwrap())
        .read_line(&mut request)
        .unwrap();
    connection.write_all(b"{\"id\":").unwrap();
    let output = child.join().unwrap();
    assert!(started.elapsed() < Duration::from_secs(28));
    assert!(!output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error"]["code"], "indeterminate_delivery");
    listener.set_nonblocking(true).unwrap();
    assert!(
        listener.accept().is_err(),
        "CLI retried an uncertain delivery"
    );
    std::fs::write(
        root.join("observed.json"),
        serde_json::to_vec(
            &json!({"request":request,"response":value,"elapsed_ms":started.elapsed().as_millis()}),
        )
        .unwrap(),
    )
    .unwrap();
    std::fs::write(root.join("replay.txt"), "cargo nextest run --test client_url_bridge -E 'test(client_url_bridge_cli_partial_reply)'\n").unwrap();
}

#[test]
fn client_url_bridge_pins_default_client_across_focus_change() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    let b = f.attach("fixture-b", true);
    f.focus(&a);
    let pending = f.open_async(None, "https://example.com/");
    let request = f.next_open(&a);
    f.focus(&b);
    f.complete(&a, &request, "opened");
    let response = pending.join().unwrap();
    assert_eq!(response["result"]["client_id"], a.public_id);
    assert_eq!(response["result"]["outcome"], "opened");
    assert_eq!(f.open_count(&b), 0);
}

#[test]
fn client_url_bridge_explicit_client_ignores_foreground_and_spoofed_reply() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    let b = f.attach("fixture-b", true);
    f.focus(&b);
    let pending = f.open_async(Some(&a.public_id), "https://example.com/");
    let request = f.next_open(&a);
    f.complete(&b, &request, "opened");
    f.complete(&a, &request, "focused");
    let response = pending.join().unwrap();
    assert_eq!(response["result"]["client_id"], a.public_id);
    assert_eq!(response["result"]["outcome"], "focused");
    assert_eq!(f.open_count(&b), 0);
}

#[test]
fn client_url_bridge_disconnect_does_not_reroute_delivered_request() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    let b = f.attach("fixture-b", true);
    let pending = f.open_async(Some(&a.public_id), "https://example.com/");
    let _request = f.next_open(&a);
    f.disconnect(&a);
    f.focus(&b);
    assert_eq!(
        pending.join().unwrap()["error"]["code"],
        "indeterminate_delivery"
    );
    assert_eq!(f.open_count(&b), 0);
    assert_eq!(
        f.call(
            "client.open_url",
            json!({"url":"https://example.com/","client":a.public_id})
        )["error"]["code"],
        "client_unavailable"
    );
}

#[test]
fn client_url_bridge_rejects_invalid_or_unsupported_selection_before_delivery() {
    let mut f = UrlFixture::start();
    assert_eq!(
        f.call("client.open_url", json!({"url":"https://example.com/"}))["error"]["code"],
        "no_foreground_client"
    );
    let a = f.attach("fixture-a", true);
    let old = f.attach("fixture-old", false);
    f.focus(&old);
    assert_eq!(
        f.call("client.open_url", json!({"url":"https://example.com/"}))["error"]["code"],
        "client_unsupported"
    );
    assert_eq!(
        f.call(
            "client.open_url",
            json!({"url":"https://example.com/","client":"stale"})
        )["error"]["code"],
        "client_unavailable"
    );
    for url in [
        "file:///tmp/test",
        "https://name:secret@example.com/",
        "https://example.com/\u{1}",
    ] {
        assert_eq!(
            f.call("client.open_url", json!({"url":url,"client":a.public_id}))["error"]["code"],
            "invalid_url"
        );
    }
    assert_eq!(
        f.call(
            "client.open_url",
            json!({"url":format!("https://example.com/{}", "é".repeat(2100)),"client":a.public_id})
        )["error"]["code"],
        "invalid_url"
    );
    assert_eq!(f.open_count(&a), 0);
    assert_eq!(f.open_count(&old), 0);
}

#[test]
fn client_url_bridge_per_client_overflow_is_bounded_and_releases_on_completion() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    let mut pending = vec![];
    let mut controls = vec![];
    for _ in 0..4 {
        pending.push(f.open_async(Some(&a.public_id), "https://example.com/"));
        controls.push(f.next_open(&a));
    }
    assert_eq!(
        f.call(
            "client.open_url",
            json!({"url":"https://example.com/","client":a.public_id})
        )["error"]["code"],
        "overloaded"
    );
    for request in controls {
        f.complete(&a, &request, "opened");
    }
    for response in pending {
        assert!(response.join().unwrap().get("result").is_some());
    }
    let pending = f.open_async(Some(&a.public_id), "https://example.com/");
    let request = f.next_open(&a);
    f.complete(&a, &request, "opened");
    assert!(pending.join().unwrap().get("result").is_some());
}

#[test]
fn client_url_bridge_global_overflow_cannot_retarget_another_client() {
    let mut f = UrlFixture::start();
    let mut clients = vec![];
    for n in 0..9 {
        clients.push(f.attach(&format!("fixture-{n}"), true));
    }
    let mut pending = vec![];
    let mut requests = vec![];
    for client in &clients[..8] {
        for _ in 0..4 {
            pending.push(f.open_async(Some(&client.public_id), "https://example.com/"));
            requests.push((client, f.next_open(client)));
        }
    }
    assert_eq!(
        f.call(
            "client.open_url",
            json!({"url":"https://example.com/","client":clients[8].public_id})
        )["error"]["code"],
        "overloaded"
    );
    assert_eq!(f.open_count(&clients[8]), 0);
    for (client, request) in requests {
        f.complete(client, &request, "opened");
    }
    for p in pending {
        assert!(p.join().unwrap().get("result").is_some());
    }
}

#[test]
fn client_url_bridge_lost_result_expires_without_replay_and_releases_capacity() {
    let mut f = UrlFixture::start();
    let a = f.attach("fixture-a", true);
    let b = f.attach("fixture-b", true);
    let pending = f.open_async(Some(&a.public_id), "https://example.com/");
    let request = f.next_open(&a);
    let mut wrong_boot = request.clone();
    wrong_boot["boot_id"] = json!("stale-server-boot");
    f.complete(&a, &wrong_boot, "opened");
    f.focus(&b);
    assert_eq!(
        pending.join().unwrap()["error"]["code"],
        "indeterminate_delivery"
    );
    assert_eq!(f.open_count(&a), 1);
    assert_eq!(f.open_count(&b), 0);
    let pending = f.open_async(Some(&a.public_id), "https://example.com/");
    let request = f.next_open(&a);
    f.complete(&a, &request, "focused");
    assert_eq!(pending.join().unwrap()["result"]["outcome"], "focused");
}
#[test]
fn client_url_bridge_delayed_plugin_and_popup_keep_invoking_client() {
    use std::fs;
    for popup in [false, true] {
        let root =
            std::env::temp_dir().join(format!("herdr-origin-{}-{}", std::process::id(), popup));
        fs::create_dir_all(&root).unwrap();
        let script = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/client-url/delayed-plugin.py"
        );
        fs::write(root.join("herdr-plugin.toml"),format!("id = \"test.origin\"\nname = \"Origin\"\nversion = \"0.1.0\"\nmin_herdr_version = \"0.6.10\"\n[[actions]]\nid = \"open\"\ntitle = \"Open\"\ncommand = {}\n",serde_json::json!(["/usr/bin/python3",script,root]))).unwrap();
        let command = if popup {
            format!("/usr/bin/python3 {script} {}", root.display())
        } else {
            "test.origin.open".into()
        };
        let config = format!(
            "onboarding=false\n[[keys.command]]\nkey=\"alt+w\"\ntype=\"{}\"\ncommand={}\n",
            if popup { "popup" } else { "plugin_action" },
            serde_json::to_string(&command).unwrap()
        );
        let mut f = UrlFixture::start_with_config(&config);
        assert!(f
            .call("plugin.link", json!({"path":root,"enabled":true}))
            .get("result")
            .is_some());
        let a = f.attach("origin-a", true);
        let b = f.attach("origin-b", true);
        f.focus(&a);
        f.invoke_command(&a);
        assert!(
            support::wait_until(Duration::from_secs(5), Duration::from_millis(10), || root
                .join("origin.txt")
                .exists()),
            "plugin did not start: {}",
            f.artifact_dir.display()
        );
        f.focus(&b);
        fs::write(root.join("release"), "").unwrap();
        assert_eq!(
            fs::read_to_string(root.join("origin.txt")).unwrap(),
            a.public_id,
            "origin must come from actual invoking connection"
        );
        let request = f.next_open(&a);
        f.complete(&a, &request, "opened");
        assert!(support::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            || root.join("result.json").exists()
        ));
        assert_eq!(f.open_count(&b), 0);
    }
}

#[test]
fn client_url_bridge_disconnected_plugin_origin_never_retargets() {
    use std::fs;
    let root = std::env::temp_dir().join(format!("herdr-origin-disconnect-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/client-url/delayed-plugin.py"
    );
    let command = format!("/usr/bin/python3 {script} {}", root.display());
    let config = format!(
        "onboarding=false\n[[keys.command]]\nkey=\"alt+w\"\ntype=\"popup\"\ncommand={}\n",
        serde_json::to_string(&command).unwrap()
    );
    let mut f = UrlFixture::start_with_config(&config);
    let a = f.attach("disconnect-a", true);
    let b = f.attach("disconnect-b", true);
    f.focus(&a);
    f.invoke_command(&a);
    assert!(support::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || root.join("origin.txt").exists()
    ));
    f.disconnect(&a);
    f.focus(&b);
    fs::write(root.join("release"), "").unwrap();
    assert!(support::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || root.join("result.json").exists()
    ));
    let result: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(root.join("result.json")).unwrap()).unwrap();
    assert_eq!(result["error"]["code"], "client_unavailable");
    assert_eq!(f.open_count(&b), 0);
}
#[test]
fn client_url_bridge_cli_rejects_wrong_id_and_bad_outcome() {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    for wrong_id in [true, false] {
        let root = std::path::PathBuf::from("/tmp").join(format!(
            "herdr-url-invalid-result-{}-{wrong_id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("replay.txt"),
            "just test-one client_url_bridge_cli_rejects_wrong_id_and_bad_outcome\n",
        )
        .unwrap();
        let socket = root.join("api.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let responder = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            let response = json!({"id":if wrong_id {json!("wrong")} else {request["id"].clone()},"result":{"type":"client_open_url","client_id":"a","outcome":if wrong_id {"opened"} else {"invented"}}});
            writeln!(stream, "{response}").unwrap();
        });
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["client", "open-url", "https://example.com/", "--json"])
            .env("HERDR_SOCKET_PATH", &socket)
            .env_remove("HERDR_SESSION")
            .env_remove("HERDR_ORIGIN_CLIENT_ID")
            .output()
            .unwrap();
        responder.join().unwrap();
        std::fs::write(root.join("stdout.json"), &result.stdout).unwrap();
        assert_eq!(
            result.status.code(),
            Some(1),
            "untrusted result must not report success"
        );
        let response: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(response["error"]["code"], "indeterminate_delivery");
    }
}

#[test]
fn client_url_bridge_untrusted_context_and_agent_terminal_do_not_pin_origin() {
    use std::fs;
    let mut f = UrlFixture::start_with_origin("onboarding=false\n", Some("inherited-desktop"));
    let root = f.artifact_dir.join("plugin");
    fs::create_dir_all(&root).unwrap();
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/client-url/delayed-plugin.py"
    );
    fs::write(root.join("herdr-plugin.toml"), format!("id=\"test.untrusted\"\nname=\"Untrusted\"\nversion=\"0.1.0\"\nmin_herdr_version=\"0.6.10\"\n[[actions]]\nid=\"open\"\ntitle=\"Open\"\ncommand={}\n", json!(["/usr/bin/python3",script,root]))).unwrap();
    assert!(f
        .call("plugin.link", json!({"path":root,"enabled":true}))
        .get("result")
        .is_some());
    let a = f.attach("context-a", true);
    let b = f.attach("context-b", true);
    f.focus(&a);
    assert!(f
        .call(
            "plugin.action.invoke",
            json!({"action_id":"test.untrusted.open","context":{"origin_client_id":a.public_id}})
        )
        .get("result")
        .is_some());
    assert!(support::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || root.join("origin.txt").exists()
    ));
    assert_eq!(fs::read_to_string(root.join("origin.txt")).unwrap(), "");
    let context: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(root.join("context.json")).unwrap()).unwrap();
    assert!(context.get("origin_client_id").is_none());
    f.focus(&b);
    fs::write(root.join("release"), "").unwrap();
    let request = f.next_open(&b);
    f.complete(&b, &request, "opened");
    assert_eq!(f.open_count(&a), 0);
    let created = f.call(
        "workspace.create",
        json!({"cwd":f.artifact_dir,"focus":true}),
    );
    let pane = created["result"]["root_pane"]["pane_id"].as_str().unwrap();
    let target = f.artifact_dir.join("terminal-origin.txt");
    let command = format!(
        "printf '%s' \"${{HERDR_ORIGIN_CLIENT_ID-unset}}\" > '{}'\n",
        target.display()
    );
    assert!(f
        .call("pane.send_text", json!({"pane_id":pane,"text":command}))
        .get("result")
        .is_some());
    assert!(support::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || target.exists()
    ));
    assert_eq!(fs::read_to_string(target).unwrap(), "unset");
    fs::write(
        f.artifact_dir.join("provenance-observed.json"),
        json!({"context":context,"terminal_origin":"unset","delivered_to":b.public_id}).to_string(),
    )
    .unwrap();
}
