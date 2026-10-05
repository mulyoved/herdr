use super::*;
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct FixtureClient {
    pub public_id: String,
}
pub struct UrlFixture {
    pub artifact_dir: PathBuf,
    pub api: PathBuf,
    client_socket: PathBuf,
    _master: Box<dyn MasterPty + Send>,
    server: Box<dyn Child + Send + Sync>,
    connections: HashMap<String, UnixStream>,
    controls: HashMap<String, usize>,
    snapshots: HashMap<String, Value>,
}
impl UrlFixture {
    pub fn start() -> Self {
        Self::start_with_config("onboarding = false\n")
    }
    pub fn start_with_config(contents: &str) -> Self {
        Self::start_with_origin(contents, None)
    }
    pub fn start_with_origin(contents: &str, origin: Option<&str>) -> Self {
        let artifact_dir = std::env::temp_dir().join(format!(
            "herdr-client-url-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = artifact_dir.join("config");
        let runtime = artifact_dir.join("runtime");
        fs::create_dir_all(config.join("herdr-dev")).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        fs::write(config.join("herdr-dev/config.toml"), contents).unwrap();
        fs::write(artifact_dir.join("replay.txt"), "ZIG=/home/muly/.local/share/herdr-build-tools/zig-x86_64-linux-0.16.0/zig just test-one client_url_bridge\n").unwrap();
        register_runtime_dir(&runtime);
        let api = runtime.join("herdr.sock");
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
        isolate_herdr_test_process(&mut cmd);
        cmd.arg("server");
        cmd.env("XDG_CONFIG_HOME", &config);
        cmd.env("HERDR_CONFIG_PATH", config.join("herdr-dev/config.toml"));
        cmd.env("XDG_STATE_HOME", artifact_dir.join("state"));
        cmd.env("XDG_RUNTIME_DIR", &runtime);
        cmd.env("HERDR_SOCKET_PATH", &api);
        cmd.env("HERDR_DISABLE_SOUND", "1");
        cmd.env("SHELL", "/bin/sh");
        if let Some(origin) = origin {
            cmd.env("HERDR_ORIGIN_CLIENT_ID", origin);
        } else {
            cmd.env_remove("HERDR_ORIGIN_CLIENT_ID");
        }
        cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
        cmd.env_remove("HERDR_ENV");
        let server = pair.slave.spawn_command(cmd).unwrap();
        register_spawned_herdr_pid(server.process_id());
        drop(pair.slave);
        let client_socket = runtime.join("herdr-client.sock");
        let mut reader = pair.master.try_clone_reader().unwrap();
        let log_path = artifact_dir.join("server.log");
        thread::spawn(move || {
            let mut log = fs::File::create(log_path).unwrap();
            let _ = std::io::copy(&mut reader, &mut log);
        });
        let fixture = Self {
            artifact_dir,
            api,
            client_socket,
            _master: pair.master,
            server,
            connections: HashMap::new(),
            controls: HashMap::new(),
            snapshots: HashMap::new(),
        };
        assert!(
            wait_until(
                Duration::from_secs(20),
                Duration::from_millis(25),
                || fixture.api.exists() && fixture.client_socket.exists()
            ),
            "server sockets missing: {}",
            fixture.artifact_dir.display()
        );
        let response = fixture.call("workspace.create", json!({"label":"URL fixture"}));
        assert!(
            response.get("result").is_some(),
            "workspace fixture failed: {response}"
        );
        fixture
    }
    fn record(&self, kind: &str, data: &Value) {
        let mut out = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.artifact_dir.join("events.jsonl"))
            .unwrap();
        writeln!(out, "{}", json!({"kind":kind,"data":data})).unwrap();
    }
    pub fn call(&self, method: &str, params: Value) -> Value {
        let request = json!({"id":"fixture", "method":method,"params":params});
        self.record("api.request", &request);
        let response = request_api(&self.api, &request);
        self.record("api.response", &response);
        response
    }
    pub fn attach(&mut self, platform: &str, capable: bool) -> FixtureClient {
        let mut stream = UnixStream::connect(&self.client_socket).unwrap();
        let actions: &[&str] = if capable { &["open_url.v1"] } else { &[] };
        let (_, error) = client_shell_handshake_with_actions(
            &mut stream,
            CURRENT_ENDPOINT_PROTOCOL_GENERATION,
            80,
            24,
            actions,
            platform,
        )
        .unwrap();
        assert!(error.is_none(), "handshake: {error:?}");
        let snapshot = loop {
            let (kind, payload) = read_server_message(&mut stream).unwrap();
            if kind == SERVER_MESSAGE_ENDPOINT_CONTROL {
                let (name, data) = decode_named_control(&payload).unwrap();
                if name == "shell.snapshot.v1" {
                    break data;
                }
            }
        };
        while read_server_message(&mut stream).unwrap().0 != SERVER_MESSAGE_PANE_SURFACE {}
        let list = self.call("client.list", json!({}));
        let public_id = list["result"]["clients"]
            .as_array()
            .expect("client.list must return clients")
            .iter()
            .find(|c| c["platform"] == platform)
            .expect("attached client missing")["client_id"]
            .as_str()
            .unwrap()
            .to_owned();
        self.snapshots.insert(public_id.clone(), snapshot);
        self.connections.insert(public_id.clone(), stream);
        FixtureClient { public_id }
    }
    pub fn invoke_command(&mut self, client: &FixtureClient) {
        let snapshot = &self.snapshots[&client.public_id];
        let command_id = snapshot["commands"][0]["command_id"]
            .as_str()
            .expect("projected command missing");
        let request = json!({"id":"origin-command","method":"command.invoke","params":{"command_id":command_id}});
        let payload = encode_varint_enum(
            15,
            &[
                &encode_string(snapshot["boot_id"].as_str().unwrap()),
                &encode_string(&request.to_string()),
            ],
        );
        self.connections
            .get_mut(&client.public_id)
            .unwrap()
            .write_all(&frame_message(&payload))
            .unwrap();
    }
    pub fn focus(&mut self, client: &FixtureClient) {
        send_client_shell_focus(self.connections.get_mut(&client.public_id).unwrap(), true)
            .unwrap();
        assert!(
            wait_until(Duration::from_secs(3), Duration::from_millis(10), || {
                self.call("client.list", json!({}))["result"]["clients"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|c| c["client_id"] == client.public_id && c["foreground"] == true)
            }),
            "focus not reflected"
        );
    }
    pub fn open_async(&self, client: Option<&str>, url: &str) -> JoinHandle<Value> {
        let socket = self.api.clone();
        let mut params = json!({"url":url});
        if let Some(c) = client {
            params["client"] = json!(c);
        }
        thread::spawn(move || {
            request_api(
                &socket,
                &json!({"id":"open", "method":"client.open_url", "params":params}),
            )
        })
    }
    pub fn next_open(&mut self, client: &FixtureClient) -> Value {
        let stream = self.connections.get_mut(&client.public_id).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let (kind, payload) = read_server_message(stream).expect("URL control missing");
            if kind == SERVER_MESSAGE_ENDPOINT_CONTROL {
                let (name, data) = decode_named_control(&payload).unwrap();
                if name == "client.open_url.request.v1" {
                    *self.controls.entry(client.public_id.clone()).or_default() += 1;
                    self.record("control.open", &data);
                    return data;
                }
            }
        }
        panic!("URL control not found");
    }
    pub fn complete(&mut self, client: &FixtureClient, request: &Value, outcome: &str) {
        send_named_control(
            self.connections.get_mut(&client.public_id).unwrap(),
            "client.open_url.result.v1",
            &json!({
                "request_id":request["request_id"], "boot_id":request["boot_id"],
                "result":{"schemaVersion":1,"ok":true,"outcome":outcome}
            }),
        );
    }
    pub fn disconnect(&mut self, client: &FixtureClient) {
        self.connections.remove(&client.public_id);
    }
    pub fn open_count(&mut self, client: &FixtureClient) -> usize {
        let Some(stream) = self.connections.get_mut(&client.public_id) else {
            return *self.controls.get(&client.public_id).unwrap_or(&0);
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        while let Ok((kind, payload)) = read_server_message(stream) {
            if kind == SERVER_MESSAGE_ENDPOINT_CONTROL
                && decode_named_control(&payload).unwrap().0 == "client.open_url.request.v1"
            {
                *self.controls.entry(client.public_id.clone()).or_default() += 1;
            }
        }
        *self.controls.get(&client.public_id).unwrap_or(&0)
    }
}
pub fn request_api(socket: &Path, request: &Value) -> Value {
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(25)))
        .unwrap();
    writeln!(stream, "{request}").unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}
impl Drop for UrlFixture {
    fn drop(&mut self) {
        self.connections.clear();
        let pid = self.server.process_id();
        let _ = self.server.kill();
        let _ = self.server.wait();
        unregister_spawned_herdr_pid(pid);
        // Artifacts deliberately survive cleanup; no live user resources are touched.
    }
}
