use crate::config::AuthConfig;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, sync::Mutex};

pub const STORAGE_RESOURCE: &str = "https://storage.azure.com/";
pub const SQL_RESOURCE: &str = "https://database.windows.net/";

struct Cached {
    token: Arc<str>,
    expires: u64,
}
pub struct Tokens {
    config: AuthConfig,
    http: reqwest::Client,
    cache: Mutex<HashMap<String, Cached>>,
}

#[derive(Deserialize)]
struct OAuthToken {
    access_token: String,
    expires_in: Option<u64>,
    expires_on: Option<String>,
}

impl Tokens {
    pub fn new(config: AuthConfig) -> Result<Self> {
        Ok(Self {
            config,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn get(&self, resource: &str) -> Result<Arc<str>> {
        let mut cache = self.cache.lock().await;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if let Some(token) = cache.get(resource).filter(|t| t.expires > now + 300) {
            return Ok(token.token.clone());
        }
        let (token, expires) = match self.config.mode.as_str() {
            "cli" => {
                // Execute az directly. Never interpolate credentials into shell command text.
                #[cfg(windows)]
                let mut command = {
                    let mut cmd = Command::new("cmd.exe");
                    cmd.args(["/d", "/c", "az", "account", "get-access-token"]);
                    cmd
                };
                #[cfg(not(windows))]
                let mut command = {
                    let mut cmd = Command::new("az");
                    cmd.args(["account", "get-access-token"]);
                    cmd
                };
                let output = tokio::time::timeout(
                    Duration::from_secs(60),
                    command
                        .args(["--resource", resource, "--output", "json"])
                        .kill_on_drop(true)
                        .output(),
                )
                .await
                .context("Azure CLI token request timed out")??;
                ensure!(
                    output.status.success(),
                    "Azure CLI could not obtain a token; run az login and check permissions"
                );
                let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
                let value = json["accessToken"]
                    .as_str()
                    .context("Azure CLI returned no accessToken")?
                    .to_owned();
                let expires = json["expires_on"]
                    .as_u64()
                    .or_else(|| json["expires_on"].as_str().and_then(|s| s.parse().ok()))
                    .context("Azure CLI must return expires_on (update Azure CLI if missing)")?;
                (value, expires)
            }
            "service_principal" => {
                let tenant =
                    std::env::var("AZURE_TENANT_ID").context("AZURE_TENANT_ID is required")?;
                ensure!(
                    tenant
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
                    "Invalid tenant ID"
                );
                let client =
                    std::env::var("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID is required")?;
                let secret = std::env::var("AZURE_CLIENT_SECRET")
                    .context("AZURE_CLIENT_SECRET is required")?;
                let scope = format!("{}/.default", resource.trim_end_matches('/'));
                let response = self
                    .http
                    .post(format!(
                        "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
                    ))
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", client.as_str()),
                        ("client_secret", secret.as_str()),
                        ("scope", scope.as_str()),
                    ])
                    .send()
                    .await?;
                ensure!(
                    response.status().is_success(),
                    "Entra token endpoint returned {}",
                    response.status()
                );
                let result: OAuthToken = response.json().await?;
                (
                    result.access_token,
                    now + result.expires_in.context("Missing expires_in")?,
                )
            }
            "managed_identity" => {
                let mut request = self
                    .http
                    .get("http://169.254.169.254/metadata/identity/oauth2/token")
                    .header("Metadata", "true")
                    .query(&[("api-version", "2018-02-01"), ("resource", resource)]);
                if let Some(id) = &self.config.managed_identity_client_id {
                    request = request.query(&[("client_id", id)]);
                }
                let response = request.send().await?;
                ensure!(
                    response.status().is_success(),
                    "Managed identity endpoint returned {}",
                    response.status()
                );
                let result: OAuthToken = response.json().await?;
                (
                    result.access_token,
                    result.expires_on.context("Missing expires_on")?.parse()?,
                )
            }
            mode => bail!("Unknown authentication mode: {mode}"),
        };
        ensure!(
            !token.is_empty() && expires > now + 300,
            "Token is empty or expires too soon"
        );
        let token: Arc<str> = token.into();
        cache.insert(
            resource.into(),
            Cached {
                token: token.clone(),
                expires,
            },
        );
        Ok(token)
    }
}
