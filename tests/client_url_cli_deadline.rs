//! Run this same CLI/IPC acceptance on Unix sockets and Windows named pipes.
#![cfg(any(unix, windows))]
use interprocess::local_socket::{prelude::*, ListenerOptions};
use std::io::{BufRead, BufReader, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[test]
fn client_url_bridge_cli_native_ipc_partial_reply_deadline() {
    let root = std::env::temp_dir().join(format!(
        "herdr-url-ipc-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("api.sock");
    #[cfg(unix)]
    let name = path
        .as_path()
        .to_fs_name::<interprocess::local_socket::GenericFilePath>()
        .unwrap();
    #[cfg(windows)]
    let name = path
        .to_str()
        .unwrap()
        .to_ns_name::<interprocess::local_socket::GenericNamespaced>()
        .unwrap();
    let listener = ListenerOptions::new().name(name).create_sync().unwrap();
    let started = Instant::now();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["client", "open-url", "https://example.com/", "--json"])
        .env("HERDR_SOCKET_PATH", &path)
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ORIGIN_CLIENT_ID")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut connection = listener.accept().unwrap();
    let mut request = String::new();
    BufReader::new(&mut connection)
        .read_line(&mut request)
        .unwrap();
    connection.write_all(b"{\"id\":").unwrap();
    connection.flush().unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(started.elapsed() < Duration::from_secs(28));
    assert!(!output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["error"]["code"], "indeterminate_delivery");
    std::fs::write(root.join("observed.json"), serde_json::json!({"request":request,"response":response,"elapsed_ms":started.elapsed().as_millis()}).to_string()).unwrap();
    std::fs::write(
        root.join("replay.txt"),
        "just test-one client_url_bridge_cli_native_ipc_partial_reply_deadline\n",
    )
    .unwrap();
}
