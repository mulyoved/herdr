//! Real client PTY with a frame-observing, test-owned server proxy.
use super::client_url::UrlFixture;
use super::*;
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use serde_json::{json, Value};
use std::os::unix::net::UnixListener;
use std::sync::{mpsc, Arc};

pub struct HandlerFixture {
    pub server: UrlFixture,
    pub config: PathBuf,
    peer: Arc<Mutex<UnixStream>>,
    pub messages: mpsc::Receiver<(String, Value)>,
    child: Box<dyn Child + Send + Sync>,
    _master: Box<dyn MasterPty + Send>,
}
impl HandlerFixture {
    pub fn start(scenario: Value) -> Self {
        let server = UrlFixture::start();
        let root = &server.artifact_dir;
        fs::write(root.join("scenario.json"), scenario.to_string()).unwrap();
        fs::write(
            root.join("replay.txt"),
            "just test-one client_url_handler\n",
        )
        .unwrap();
        let config = root.join("client.toml");
        let argv = json!([
            "/usr/bin/python3",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/client-url/handler.py"
            ),
            root
        ]);
        fs::write(
            &config,
            format!("onboarding = false\n[client]\nopen_url_command = {argv}\n"),
        )
        .unwrap();
        let proxy = root.join("proxy.sock");
        let listener = UnixListener::bind(&proxy).unwrap();
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
        cmd.arg("client");
        cmd.env("HERDR_CLIENT_SOCKET_PATH", &proxy);
        cmd.env_remove("HERDR_SOCKET_PATH");
        cmd.env("HERDR_CONFIG_PATH", &config);
        cmd.env("XDG_CONFIG_HOME", root.join("client-config"));
        cmd.env("XDG_STATE_HOME", root.join("client-state"));
        cmd.env("XDG_RUNTIME_DIR", root.join("client-runtime"));
        cmd.env("HERDR_DISABLE_SOUND", "1");
        cmd.env_remove("HERDR_ENV");
        let child = pair.slave.spawn_command(cmd).unwrap();
        register_spawned_herdr_pid(child.process_id());
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let log = root.join("client.log");
        thread::spawn(move || {
            let mut out = fs::File::create(log).unwrap();
            let _ = std::io::copy(&mut reader, &mut out);
        });
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut downstream = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Err(e) => panic!("client attach: {e}"),
            }
        };
        let mut upstream =
            UnixStream::connect(server.api.with_file_name("herdr-client.sock")).unwrap();
        let peer = Arc::new(Mutex::new(downstream.try_clone().unwrap()));
        let write_peer = peer.clone();
        let (tx, messages) = mpsc::channel();
        let mut upstream_writer = upstream.try_clone().unwrap();
        let observed = root.join("client-controls.jsonl");
        thread::spawn(move || {
            while let Ok((kind, payload)) = read_server_message(&mut downstream) {
                if kind == 20 {
                    let (name, value) = decode_named_control(&payload).unwrap();
                    let mut out = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&observed)
                        .unwrap();
                    writeln!(out, "{}", json!({"kind":name,"data":value})).unwrap();
                    let _ = tx.send((name, value));
                }
                let mut raw = encode_varint_u32(kind);
                raw.extend(payload);
                if upstream_writer.write_all(&frame_message(&raw)).is_err() {
                    break;
                }
            }
        });
        thread::spawn(move || {
            while let Ok((kind, payload)) = read_server_message(&mut upstream) {
                let mut raw = encode_varint_u32(kind);
                raw.extend(payload);
                if write_peer
                    .lock()
                    .unwrap()
                    .write_all(&frame_message(&raw))
                    .is_err()
                {
                    break;
                }
            }
        });
        let fixture = Self {
            server,
            config,
            peer,
            messages,
            child,
            _master: pair.master,
        };
        assert!(
            wait_until(Duration::from_secs(10), Duration::from_millis(10), || {
                fixture.server.call("client.list", json!({}))["result"]["clients"]
                    .as_array()
                    .is_some_and(|c| !c.is_empty())
            }),
            "client bootstrap missing: {}; server={}; client={}; controls={}",
            fixture.server.artifact_dir.display(),
            fs::read_to_string(fixture.server.artifact_dir.join("server.log")).unwrap_or_default(),
            fs::read_to_string(fixture.server.artifact_dir.join("client.log")).unwrap_or_default(),
            fs::read_to_string(fixture.server.artifact_dir.join("client-controls.jsonl"))
                .unwrap_or_default()
        );
        // Snapshot publication precedes URL injection; let the client process the bootstrap.
        thread::sleep(Duration::from_millis(150));
        fixture
    }
    pub fn submit(&self, id: &str, remaining_ms: u64) {
        let boot = self.server.call("client.list", json!({}))["result"]["clients"][0]["client_id"]
            .as_str()
            .unwrap()
            .rsplit_once(':')
            .unwrap()
            .0
            .to_owned();
        let value = json!({"request_id":id,"boot_id":boot,"remaining_ms":remaining_ms,"action":{"schemaVersion":1,"action":"open-url","url":"https://example.com/"}});
        let raw = encode_varint_enum(
            SERVER_MESSAGE_ENDPOINT_CONTROL,
            &[
                &encode_string("client.open_url.request.v1"),
                &encode_string(&value.to_string()),
            ],
        );
        self.peer
            .lock()
            .unwrap()
            .write_all(&frame_message(&raw))
            .unwrap();
    }
    pub fn invocations(&self) -> Vec<Value> {
        fs::read_to_string(self.server.artifact_dir.join("handler-events.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    pub fn wait_started(&self) {
        assert!(
            wait_until(Duration::from_secs(4), Duration::from_millis(10), || !self
                .invocations()
                .is_empty()),
            "handler never started: {}",
            self.server.artifact_dir.display()
        );
    }
    pub fn release(&self) {
        fs::write(self.server.artifact_dir.join("release"), "").unwrap();
    }
    pub fn completion(&self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(18);
        while let Ok((kind, value)) = self
            .messages
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            if kind == "client.open_url.result.v1" {
                return value;
            }
        }
        panic!("completion missing: {}", self.server.artifact_dir.display());
    }
}
impl Drop for HandlerFixture {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        let _ = self.child.wait();
        unregister_spawned_herdr_pid(pid);
        let _ = self.peer.lock().unwrap().shutdown(std::net::Shutdown::Both);
    }
}
