//! Local command ownership and bounded process lifetime for optional URL actions.
use super::{endpoint::ClientEndpointId, events::ClientLoopEvent};
use crate::client_url::{self, HostUrlErrorCode, HostUrlResult, OpenUrlCompletion, OpenUrlControl};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

type Identity = (ClientEndpointId, u64, String, String);
struct Accepted {
    endpoint_id: ClientEndpointId,
    generation: u64,
    request: OpenUrlControl,
    argv: Vec<String>,
    received_at: Instant,
}
impl Accepted {
    fn identity(&self) -> Identity {
        (
            self.endpoint_id.clone(),
            self.generation,
            self.request.boot_id.clone(),
            self.request.request_id.clone(),
        )
    }
    fn event(self, result: HostUrlResult) -> ClientLoopEvent {
        ClientLoopEvent::UrlActionFinished {
            endpoint_id: self.endpoint_id,
            generation: self.generation,
            completion: OpenUrlCompletion {
                request_id: self.request.request_id,
                boot_id: self.request.boot_id,
                result,
            },
        }
    }
}
fn failure(error: HostUrlErrorCode) -> HostUrlResult {
    HostUrlResult {
        schema_version: 1,
        ok: false,
        outcome: None,
        error: Some(error),
    }
}
pub(super) fn local_command() -> Vec<String> {
    crate::config::load_live_config()
        .ok()
        .filter(|loaded| !loaded.invalid_sections.iter().any(|s| s == "client"))
        .map(|loaded| loaded.config.client.open_url_command)
        .unwrap_or_default()
}
pub(super) fn command_available(argv: &[String]) -> bool {
    argv.first().is_some_and(|exe| {
        let path = std::path::Path::new(exe);
        path.is_absolute() && path.is_file() && crate::platform::command_is_executable(path)
    })
}
pub(super) struct UrlActionDispatcher {
    queue: mpsc::SyncSender<Accepted>,
    inflight: Arc<Mutex<HashSet<Identity>>>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
    events: tokio::sync::mpsc::Sender<ClientLoopEvent>,
}
impl UrlActionDispatcher {
    pub(super) fn new(events: tokio::sync::mpsc::Sender<ClientLoopEvent>) -> std::io::Result<Self> {
        let (queue, received) = mpsc::sync_channel::<Accepted>(32);
        let inflight = Arc::new(Mutex::new(HashSet::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let cancelled = stop.clone();
        let tx = events.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let worker = std::thread::Builder::new()
            .name("client-url-handler".into())
            .spawn(move || {
                while !cancelled.load(Ordering::Acquire) {
                    let accepted = match received.recv_timeout(Duration::from_millis(20)) {
                        Ok(a) => a,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(_) => break,
                    };
                    let result = runtime.block_on(run_handler(&accepted, &cancelled));
                    // The client loop releases identity after processing completion; queued duplicates remain suppressed.
                    runtime.block_on(async {
                        let event = accepted.event(result);
                        tokio::select! { _=tx.send(event)=>{}, _=wait_cancelled(&cancelled)=>{} }
                    });
                }
            })?;
        Ok(Self {
            queue,
            inflight,
            stop,
            worker: Some(worker),
            events,
        })
    }
    pub(super) fn enqueue(
        &self,
        endpoint_id: ClientEndpointId,
        generation: u64,
        mut request: OpenUrlControl,
        argv: Vec<String>,
        now: Instant,
    ) {
        let validation = client_url::validate_open_url_action(request.action.clone());
        if let Ok(action) = &validation {
            request.action = action.clone();
        }
        let accepted = Accepted {
            endpoint_id,
            generation,
            request,
            argv,
            received_at: now,
        };
        let identity = accepted.identity();
        let mut pending = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        if pending.contains(&identity) {
            return;
        }
        if validation.is_err() || pending.len() >= 32 {
            let _ = self
                .events
                .try_send(accepted.event(failure(HostUrlErrorCode::InvalidPayload)));
            return;
        }
        pending.insert(identity.clone());
        if let Err(error) = self.queue.try_send(accepted) {
            pending.remove(&identity);
            let accepted = match error {
                mpsc::TrySendError::Full(a) | mpsc::TrySendError::Disconnected(a) => a,
            };
            let _ = self
                .events
                .try_send(accepted.event(failure(HostUrlErrorCode::HandlerFailed)));
        }
    }
    pub(super) fn completed(
        &self,
        endpoint_id: &ClientEndpointId,
        generation: u64,
        completion: &OpenUrlCompletion,
    ) {
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(
                endpoint_id.clone(),
                generation,
                completion.boot_id.clone(),
                completion.request_id.clone(),
            ));
    }
}
impl Drop for UrlActionDispatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
async fn wait_cancelled(stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn drain(
    mut stream: impl AsyncRead + Unpin,
    limit: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut result = Vec::new();
    let mut oversized = false;
    let mut buffer = [0; 2048];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Ok((result, oversized));
        }
        let keep = n.min(limit.saturating_sub(result.len()));
        result.extend_from_slice(&buffer[..keep]);
        oversized |= keep < n;
    }
}
async fn run_handler(accepted: &Accepted, stop: &AtomicBool) -> HostUrlResult {
    let remaining = Duration::from_millis(accepted.request.remaining_ms.min(20_000))
        .saturating_sub(accepted.received_at.elapsed());
    if remaining.is_zero() {
        return failure(HostUrlErrorCode::HandlerTimeout);
    }
    if !command_available(&accepted.argv) {
        return failure(HostUrlErrorCode::HandlerFailed);
    }
    let budget = remaining.min(Duration::from_secs(15));
    let mut command = tokio::process::Command::new(&accepted.argv[0]);
    command
        .args(&accepted.argv[1..])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    // Invocation identity is conveyed only through structured stdin, never inherited origin metadata.
    command.env_remove("HERDR_ORIGIN_CLIENT_ID");
    let Ok(mut child) = command.spawn() else {
        return failure(HostUrlErrorCode::HandlerFailed);
    };
    let (Some(mut stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        return failure(HostUrlErrorCode::HandlerFailed);
    };
    let Ok(mut action) = serde_json::to_vec(&accepted.request.action) else {
        return failure(HostUrlErrorCode::InvalidPayload);
    };
    action.push(b'\n');
    let operation = async {
        let input = async {
            stdin.write_all(&action).await?;
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        tokio::try_join!(
            input,
            drain(stdout, 4096),
            drain(stderr, 8192),
            child.wait()
        )
    };
    let result = tokio::select! {
        result=tokio::time::timeout(budget,operation)=>result.ok().and_then(Result::ok),
        _=wait_cancelled(stop)=>None,
    };
    let Some((_, (output, oversized), _, status)) = result else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return failure(HostUrlErrorCode::HandlerTimeout);
    };
    if oversized || !output.ends_with(b"\n") || output[..output.len() - 1].contains(&b'\n') {
        return failure(HostUrlErrorCode::InvalidResult);
    }
    let Ok(result) = serde_json::from_slice::<HostUrlResult>(&output) else {
        return failure(HostUrlErrorCode::InvalidResult);
    };
    if !result.valid() || status.success() != result.ok {
        return failure(HostUrlErrorCode::InvalidResult);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    // Real process with a paused completion consumer: PTY E2Es cannot deterministically
    // order a duplicate already queued in the UI before the worker's completion.
    #[cfg(unix)]
    #[tokio::test]
    async fn queued_completion_retains_inflight_duplicate_protection() {
        let root = std::env::temp_dir().join(format!("herdr-url-dedup-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let events = root.join("invocations.txt");
        std::fs::write(&events, "").unwrap();
        let script = root.join("handler.py");
        std::fs::write(&script, "import sys,json\njson.loads(sys.stdin.readline())\nwith open(sys.argv[1],'a') as f:f.write('called\\n')\nprint(json.dumps({'schemaVersion':1,'ok':True,'outcome':'opened'}))\n").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let dispatcher = UrlActionDispatcher::new(tx).unwrap();
        let request = OpenUrlControl {
            request_id: "same".into(),
            boot_id: "boot".into(),
            remaining_ms: 20_000,
            action: crate::client_url::OpenUrlAction {
                schema_version: 1,
                action: "open-url".into(),
                url: "https://example.com/".into(),
                key: None,
            },
        };
        let argv = vec![
            "/usr/bin/python3".into(),
            script.to_string_lossy().into_owned(),
            events.to_string_lossy().into_owned(),
        ];
        dispatcher.enqueue(
            ClientEndpointId::Local,
            1,
            request.clone(),
            argv.clone(),
            Instant::now(),
        );
        let _completion = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        // Hold this event unprocessed while dispatching the duplicate that preceded it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        dispatcher.enqueue(ClientEndpointId::Local, 1, request, argv, Instant::now());
        assert!(
            tokio::time::timeout(Duration::from_millis(250), rx.recv())
                .await
                .is_err(),
            "duplicate launched before completion was processed"
        );
        assert_eq!(std::fs::read_to_string(&events).unwrap().lines().count(), 1);
        std::fs::write(
            root.join("replay.txt"),
            "just test-one queued_completion_retains_inflight_duplicate_protection\n",
        )
        .unwrap();
    }
}
