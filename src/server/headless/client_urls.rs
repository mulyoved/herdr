use super::HeadlessServer;
use crate::api::{
    schema::{ClientInfo, ClientOpenUrlParams, Method},
    ApiRequestMessage,
};
use crate::client_url::{self, ClientUrlError, OpenUrlAction};
use crate::protocol::ServerMessage;
use crate::server::client_url_bridge::error_response;
use std::time::Instant;

impl HeadlessServer {
    pub(super) fn public_client_id(&self, id: u64) -> String {
        format!("{}:{id}", self.client_shell_boot_id)
    }
    fn client_info(&self) -> Vec<ClientInfo> {
        let mut rows: Vec<_> = self
            .clients
            .iter()
            .filter(|(_, c)| c.is_shell_client())
            .map(|(&id, c)| ClientInfo {
                client_id: self.public_client_id(id),
                foreground: self.foreground_client_id == Some(id),
                platform: c.client_platform.clone(),
                open_url: c.client_actions.iter().any(|a| a == client_url::CAPABILITY),
            })
            .collect();
        rows.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        rows
    }
    fn find_client_by_public_id(&self, public_id: &str) -> Option<u64> {
        self.clients.iter().find_map(|(&id, c)| {
            (c.is_shell_client() && self.public_client_id(id) == public_id).then_some(id)
        })
    }
    pub(super) fn handle_client_url_api(&mut self, msg: ApiRequestMessage) {
        match msg.request.method {
            Method::ClientList(_) => {
                let _ = msg.respond_to.send(serde_json::json!({"id":msg.request.id,"result":{"type":"client_list","clients":self.client_info()}}).to_string());
            }
            Method::ClientOpenUrl(params) => {
                let selected = self.resolve_url_client(&params);
                let action = client_url::validate_open_url_action(OpenUrlAction {
                    schema_version: 1,
                    action: "open-url".into(),
                    url: params.url,
                    key: params.key,
                });
                let resolved = action.and_then(|action| selected.map(|id| (id, action)));
                let result = resolved.and_then(|(id, action)| {
                    let public_id = self.public_client_id(id);
                    self.client_url_bridge
                        .begin(
                            id,
                            public_id,
                            &self.client_shell_boot_id,
                            action,
                            Instant::now(),
                            msg.request.id.clone(),
                            msg.respond_to.clone(),
                        )
                        .map(|control| (id, control))
                });
                match result {
                    Ok((id, control)) => {
                        let data = match serde_json::to_string(&control) {
                            Ok(data) => data,
                            Err(_) => {
                                self.client_url_bridge
                                    .fail(&control.request_id, ClientUrlError::ClientUnavailable);
                                return;
                            }
                        };
                        if !self.send_to_client(
                            id,
                            ServerMessage::EndpointControl {
                                kind: client_url::REQUEST_KIND.into(),
                                data,
                            },
                        ) {
                            self.client_url_bridge
                                .fail(&control.request_id, ClientUrlError::ClientUnavailable);
                        } else {
                            self.client_url_bridge.mark_delivered(&control.request_id);
                        }
                    }
                    Err(error) => {
                        let _ = msg.respond_to.send(error_response(
                            &msg.request.id,
                            error,
                            params.client.as_deref(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    fn resolve_url_client(&self, params: &ClientOpenUrlParams) -> Result<u64, ClientUrlError> {
        let selected = match params.client.as_deref() {
            None | Some("foreground") => self
                .foreground_client_id
                .ok_or(ClientUrlError::NoForegroundClient)?,
            Some(id) => self
                .find_client_by_public_id(id)
                .ok_or(ClientUrlError::ClientUnavailable)?,
        };
        let client = self
            .clients
            .get(&selected)
            .ok_or(ClientUrlError::ClientUnavailable)?;
        if !client.is_shell_client()
            || !client
                .client_actions
                .iter()
                .any(|a| a == client_url::CAPABILITY)
        {
            return Err(ClientUrlError::ClientUnsupported);
        }
        Ok(selected)
    }
}
