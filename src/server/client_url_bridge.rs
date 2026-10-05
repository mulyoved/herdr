//! Pinned pending response ownership lives in server runtime, outside AppState.
use crate::client_url::{ClientUrlError, OpenUrlAction, OpenUrlCompletion, OpenUrlControl};
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

struct Pending {
    connection: u64,
    public_id: String,
    api_id: String,
    boot_id: String,
    deadline: Instant,
    delivered: bool,
    respond_to: Sender<String>,
}
#[derive(Default)]
pub(crate) struct ClientUrlBridge {
    pending: HashMap<String, Pending>,
    next_id: u64,
}
pub(crate) fn error_response(api_id: &str, error: ClientUrlError, client: Option<&str>) -> String {
    serde_json::json!({"id":api_id,"error":{"code":error.code(),"message":match client {
        Some(id) if id.len() <= 256 && !id.chars().any(char::is_control) => format!("{} (client {id})", error.code()), _ => error.code().to_owned()
    }}}).to_string()
}
impl ClientUrlBridge {
    pub(crate) fn begin(
        &mut self,
        connection: u64,
        public_id: String,
        boot_id: &str,
        action: OpenUrlAction,
        now: Instant,
        api_id: String,
        respond_to: Sender<String>,
    ) -> Result<OpenUrlControl, ClientUrlError> {
        if self.pending.len() >= 32
            || self
                .pending
                .values()
                .filter(|p| p.connection == connection)
                .count()
                >= 4
        {
            return Err(ClientUrlError::Overloaded);
        }
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(ClientUrlError::Overloaded)?;
        let request_id = format!("url-{:x}", self.next_id);
        self.pending.insert(
            request_id.clone(),
            Pending {
                connection,
                public_id,
                api_id,
                boot_id: boot_id.to_owned(),
                deadline: now + Duration::from_secs(20),
                delivered: false,
                respond_to,
            },
        );
        Ok(OpenUrlControl {
            request_id,
            boot_id: boot_id.to_owned(),
            action,
            remaining_ms: 20_000,
        })
    }
    pub(crate) fn mark_delivered(&mut self, request_id: &str) {
        if let Some(p) = self.pending.get_mut(request_id) {
            p.delivered = true;
        }
    }
    pub(crate) fn complete(&mut self, connection: u64, completion: OpenUrlCompletion) {
        let Some(p) = self.pending.get(&completion.request_id) else {
            return;
        };
        if p.connection != connection || p.boot_id != completion.boot_id {
            return;
        }
        let Some(p) = self.pending.remove(&completion.request_id) else {
            return;
        };
        let response = if !completion.result.valid() {
            error_response(&p.api_id, ClientUrlError::HandlerFailed, Some(&p.public_id))
        } else if let Some(outcome) = completion.result.outcome {
            serde_json::json!({"id":p.api_id,"result":{"type":"client_open_url","client_id":p.public_id,"outcome":outcome}}).to_string()
        } else {
            error_response(&p.api_id, ClientUrlError::HandlerFailed, Some(&p.public_id))
        };
        let _ = p.respond_to.send(response);
    }
    pub(crate) fn fail(&mut self, request_id: &str, error: ClientUrlError) {
        if let Some(p) = self.pending.remove(request_id) {
            let _ = p
                .respond_to
                .send(error_response(&p.api_id, error, Some(&p.public_id)));
        }
    }
    pub(crate) fn disconnect(&mut self, connection: u64) {
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.connection == connection)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let error = if self.pending.get(&id).is_some_and(|p| p.delivered) {
                ClientUrlError::IndeterminateDelivery
            } else {
                ClientUrlError::ClientUnavailable
            };
            self.fail(&id, error);
        }
    }
    pub(crate) fn expire(&mut self, now: Instant) {
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.fail(&id, ClientUrlError::IndeterminateDelivery);
        }
    }
}
impl Drop for ClientUrlBridge {
    fn drop(&mut self) {
        let ids: Vec<_> = self.pending.keys().cloned().collect();
        for id in ids {
            self.fail(&id, ClientUrlError::IndeterminateDelivery);
        }
    }
}
