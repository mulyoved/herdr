use crate::api::schema::{ClientOpenUrlParams, EmptyParams, Method, Request};
use crate::client_url::{validate_open_url_action, OpenUrlAction};
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) fn is_utility(args: &[String]) -> bool {
    matches!(
        args.first().map(String::as_str),
        Some("list" | "open-url" | "--help" | "-h")
    )
}
pub(super) fn run_client_command(args: &[String]) -> io::Result<i32> {
    let mut json = false;
    let mut selector = None;
    let mut key = None;
    let mut url = None;
    let mut index = 1;
    let subcommand = args.first().map(String::as_str).unwrap_or_default();
    while let Some(arg) = args.get(index) {
        match arg.as_str() {
            "--json" if !json => json = true,
            "--client" | "--key" if subcommand == "open-url" => {
                index += 1;
                let Some(value) = args
                    .get(index)
                    .filter(|v| !v.is_empty() && !v.starts_with('-'))
                else {
                    return usage_error();
                };
                let slot = if arg == "--client" {
                    &mut selector
                } else {
                    &mut key
                };
                if slot.replace(value.clone()).is_some() {
                    return usage_error();
                }
            }
            value if subcommand == "open-url" && !value.starts_with('-') && url.is_none() => {
                url = Some(value.to_owned())
            }
            _ => return usage_error(),
        }
        index += 1;
    }
    let method = match subcommand {
        "list" => Method::ClientList(EmptyParams::default()),
        "open-url" => {
            let Some(url) = url else {
                return usage_error();
            };
            let action = match validate_open_url_action(OpenUrlAction {
                schema_version: 1,
                action: "open-url".into(),
                url,
                key,
            }) {
                Ok(action) => action,
                Err(_) => {
                    eprintln!("invalid_url");
                    return Ok(2);
                }
            };
            let client = selector.or_else(|| std::env::var("HERDR_ORIGIN_CLIENT_ID").ok());
            if client.as_ref().is_some_and(|id| {
                id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            }) {
                return usage_error();
            }
            Method::ClientOpenUrl(ClientOpenUrlParams {
                url: action.url,
                key: action.key,
                client,
            })
        }
        _ => return usage_error(),
    };
    let id = format!(
        "client-url:{}:{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos()
    );
    let request = Request {
        id: id.clone(),
        method,
    };
    let api = super::target::api_client()?;
    let response = match api.request_value_with_deadline(&request, Duration::from_secs(25)) {
        Ok(response) => response,
        Err(error) => {
            let code = match &error {
                crate::api::client::ApiClientError::Io(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    "server_unavailable"
                }
                _ => "indeterminate_delivery",
            };
            if json {
                println!(
                    "{}",
                    serde_json::json!({"id":id,"error":{"code":code,"message":"URL response unavailable; delivery may be indeterminate"}})
                );
            } else {
                eprintln!("{code}: URL response unavailable; delivery may be indeterminate");
            }
            return Ok(1);
        }
    };
    let valid = response.get("id").and_then(serde_json::Value::as_str) == Some(&id)
        && match crate::api::client::parse_response_value(response.clone()) {
            Ok(success) => match success.result {
                crate::api::schema::ResponseResult::ClientList { .. } => subcommand == "list",
                crate::api::schema::ResponseResult::ClientOpenUrl { client_id, .. } => {
                    subcommand == "open-url"
                        && !client_id.trim().is_empty()
                        && client_id.len() <= 256
                        && !client_id.chars().any(char::is_control)
                }
                _ => false,
            },
            Err(crate::api::client::ApiClientError::ErrorResponse(_)) => true,
            Err(_) => false,
        };
    if !valid {
        if json {
            println!(
                "{}",
                serde_json::json!({"id":id,"error":{"code":"indeterminate_delivery","message":"Invalid URL response; delivery may be indeterminate"}})
            );
        } else {
            eprintln!(
                "indeterminate_delivery: Invalid URL response; delivery may be indeterminate"
            );
        }
        return Ok(1);
    }
    if json {
        println!("{response}");
    } else if let Some(error) = response.get("error") {
        eprintln!(
            "{}",
            error["message"]
                .as_str()
                .unwrap_or("Client URL operation failed")
        );
    } else if subcommand == "list" {
        if let Some(clients) = response["result"]["clients"].as_array() {
            for client in clients {
                println!(
                    "{} {} {} open-url={}",
                    client["client_id"].as_str().unwrap_or_default(),
                    if client["foreground"] == true {
                        "foreground"
                    } else {
                        "background"
                    },
                    client["platform"].as_str().unwrap_or("unknown"),
                    client["open_url"]
                );
            }
        }
    } else {
        println!(
            "{} {}",
            response["result"]["client_id"].as_str().unwrap_or_default(),
            response["result"]["outcome"].as_str().unwrap_or_default()
        );
    }
    Ok(if response.get("error").is_some() {
        1
    } else {
        0
    })
}
fn usage_error() -> io::Result<i32> {
    eprintln!("usage: herdr client list [--json] | open-url <url> [--key KEY] [--client ID|foreground] [--json]");
    Ok(2)
}
