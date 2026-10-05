//! Optional client action data; never carries a server-selected executable.
use serde::{Deserialize, Serialize};

pub const CAPABILITY: &str = "open_url.v1";
pub const REQUEST_KIND: &str = "client.open_url.request.v1";
pub const RESULT_KIND: &str = "client.open_url.result.v1";
pub const MAX_ACTION_BYTES: usize = 4096;
pub const MAX_CONTROL_BYTES: usize = 8192;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenUrlAction {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u8,
    pub action: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

pub fn validate_open_url_action(
    mut action: OpenUrlAction,
) -> Result<OpenUrlAction, ClientUrlError> {
    if action.schema_version != 1
        || action.action != "open-url"
        || action.url.chars().any(char::is_control)
    {
        return Err(ClientUrlError::InvalidUrl);
    }
    let parsed = url::Url::parse(&action.url).map_err(|_| ClientUrlError::InvalidUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || action
            .url
            .split("://")
            .nth(1)
            .and_then(|v| v.split(['/', '?', '#']).next())
            .is_some_and(|v| v.contains('@'))
    {
        return Err(ClientUrlError::InvalidUrl);
    }
    if let Some(key) = &action.key {
        if key.is_empty()
            || key.len() > 128
            || !key
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        {
            return Err(ClientUrlError::InvalidUrl);
        }
    }
    action.url = parsed.to_string();
    let bytes = serde_json::to_vec(&action).map_err(|_| ClientUrlError::InvalidUrl)?;
    if bytes.len() + 1 > MAX_ACTION_BYTES {
        return Err(ClientUrlError::InvalidUrl);
    }
    Ok(action)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UrlOutcome {
    Opened,
    Focused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostUrlErrorCode {
    InvalidPayload,
    ChromeUnavailable,
    CdpUnavailable,
    HandlerFailed,
    HandlerTimeout,
    InvalidResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostUrlResult {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u8,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<UrlOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<HostUrlErrorCode>,
}
impl HostUrlResult {
    pub fn valid(&self) -> bool {
        self.schema_version == 1
            && if self.ok {
                self.outcome.is_some() && self.error.is_none()
            } else {
                self.outcome.is_none() && self.error.is_some()
            }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenUrlControl {
    pub request_id: String,
    pub boot_id: String,
    pub action: OpenUrlAction,
    pub remaining_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenUrlCompletion {
    pub request_id: String,
    pub boot_id: String,
    pub result: HostUrlResult,
}

#[derive(Debug, Clone, Copy)]
pub enum ClientUrlError {
    InvalidUrl,
    NoForegroundClient,
    ClientUnavailable,
    ClientUnsupported,
    Overloaded,
    HandlerFailed,
    IndeterminateDelivery,
}
impl ClientUrlError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidUrl => "invalid_url",
            Self::NoForegroundClient => "no_foreground_client",
            Self::ClientUnavailable => "client_unavailable",
            Self::ClientUnsupported => "client_unsupported",
            Self::Overloaded => "overloaded",
            Self::HandlerFailed => "handler_failed",
            Self::IndeterminateDelivery => "indeterminate_delivery",
        }
    }
}
