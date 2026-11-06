use crate::{
    auth::{STORAGE_RESOURCE, Tokens},
    config::{StorageConfig, validate_path},
};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Method, Response, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputFile {
    /// Container-relative path. Worker CLI also accepts this as `path`.
    pub path: String,
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub etag: Option<String>,
}

#[derive(Deserialize)]
struct Listing {
    #[serde(default)]
    paths: Vec<ListedPath>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedPath {
    name: String,
    is_directory: Option<String>,
    content_length: Option<String>,
    etag: Option<String>,
}

pub struct Download {
    pub file: NamedTempFile,
    pub checksum: String,
    pub etag: String,
}
#[derive(Clone)]
pub struct Storage {
    config: StorageConfig,
    http: reqwest::Client,
    tokens: Arc<Tokens>,
}

fn canonical_etag(value: &str) -> String {
    format!("\"{}\"", value.trim().trim_matches('"'))
}

fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|part| utf8_percent_encode(part, NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

impl Storage {
    pub fn new(config: StorageConfig, tokens: Arc<Tokens>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
            .read_timeout(Duration::from_secs(config.read_timeout_seconds))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            config,
            http,
            tokens,
        })
    }

    fn url(&self, path: &str) -> String {
        format!(
            "https://{}.dfs.core.windows.net/{}/{}",
            self.config.account,
            self.config.container,
            encode_path(path)
        )
    }

    async fn request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        let token = self.tokens.get(STORAGE_RESOURCE).await?;
        Ok(self
            .http
            .request(method, self.url(path))
            .bearer_auth(token.as_ref())
            .header("x-ms-version", "2023-11-03"))
    }

    async fn checked(response: Response) -> Result<Response> {
        if !response.status().is_success() {
            let status = response.status();
            let code = response
                .headers()
                .get("x-ms-error-code")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("unknown")
                .to_owned();
            anyhow::bail!("Azure storage request failed: {status}, code={code}");
        }
        Ok(response)
    }

    pub fn normalise_file(&self, mut file: InputFile) -> Result<InputFile> {
        file.etag = file.etag.map(|value| canonical_etag(&value));
        if let Some(path) = file.path.strip_prefix("abfss://") {
            let (authority, relative) = path
                .split_once('/')
                .context("ABFSS path has no file path")?;
            ensure!(
                authority
                    == format!(
                        "{}@{}.dfs.core.windows.net",
                        self.config.container, self.config.account
                    ),
                "Worker file belongs to a different account or container"
            );
            file.path = relative.into();
        }
        validate_path(&file.path)?;
        ensure!(
            file.path.rsplit('/').next() == Some(file.name.as_str()),
            "File name does not match its path"
        );
        Ok(file)
    }

    async fn entries(&self, prefix: &str) -> Result<Vec<ListedPath>> {
        validate_path(prefix)?;
        let mut files = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut request = self.request(Method::GET, "").await?.query(&[
                ("resource", "filesystem"),
                ("directory", prefix),
                ("recursive", "true"),
                ("maxResults", "5000"),
            ]);
            if let Some(token) = &continuation {
                request = request.query(&[("continuation", token)]);
            }
            let response = request.send().await?;
            if response.status() == StatusCode::NOT_FOUND {
                break;
            }
            let response = Self::checked(response).await?;
            continuation = response
                .headers()
                .get("x-ms-continuation")
                .and_then(|h| h.to_str().ok())
                .filter(|s| !s.is_empty())
                .map(str::to_owned);
            for item in response.json::<Listing>().await?.paths {
                validate_path(&item.name)?;
                ensure!(
                    item.name.starts_with(&format!("{prefix}/")),
                    "Listing returned a path outside its prefix"
                );
                files.push(item);
            }
            if continuation.is_none() {
                break;
            }
        }
        Ok(files)
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<InputFile>> {
        let mut files = self
            .entries(prefix)
            .await?
            .into_iter()
            .filter(|item| item.is_directory.as_deref() != Some("true"))
            .map(|item| {
                Ok(InputFile {
                    name: item
                        .name
                        .rsplit('/')
                        .next()
                        .context("Missing file name")?
                        .into(),
                    path: item.name,
                    size: item
                        .content_length
                        .context("Missing content length")?
                        .parse()?,
                    etag: item.etag.map(|value| canonical_etag(&value)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(files)
    }

    pub async fn remove_empty_directories(&self, prefix: &str) -> Result<()> {
        let mut directories: Vec<String> = self
            .entries(prefix)
            .await?
            .into_iter()
            .filter(|p| p.is_directory.as_deref() == Some("true"))
            .map(|p| p.name)
            .collect();
        directories.sort_by_key(|path| std::cmp::Reverse(path.matches('/').count()));
        for path in directories {
            let response = self
                .request(Method::DELETE, &path)
                .await?
                .query(&[("recursive", "false")])
                .send()
                .await?;
            let code = response
                .headers()
                .get("x-ms-error-code")
                .and_then(|h| h.to_str().ok());
            if response.status() == StatusCode::NOT_FOUND || code == Some("DirectoryNotEmpty") {
                continue;
            }
            Self::checked(response).await?;
        }
        Ok(())
    }

    pub async fn download(&self, file: &InputFile) -> Result<Download> {
        let mut request = self.request(Method::GET, &file.path).await?;
        if let Some(etag) = &file.etag {
            request = request.header("If-Match", etag);
        }
        let response = Self::checked(request.send().await?).await?;
        let etag = response
            .headers()
            .get("etag")
            .context("Storage returned no ETag")?
            .to_str()?
            .into();
        let suffix = file
            .name
            .rsplit_once('.')
            .map(|(_, extension)| format!(".{extension}"));
        let temp = tempfile::Builder::new()
            .suffix(suffix.as_deref().unwrap_or(".tmp"))
            .tempfile()
            .context("Creating download spool")?;
        let mut writer = tokio::fs::OpenOptions::new()
            .write(true)
            .open(temp.path())
            .await?;
        let mut stream = response.bytes_stream();
        let mut checksum = Sha256::new();
        let mut size = 0_u64;
        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            size = size
                .checked_add(bytes.len() as u64)
                .context("File size overflow")?;
            checksum.update(&bytes);
            writer.write_all(&bytes).await?;
        }
        writer.flush().await?;
        drop(writer);
        ensure!(
            size == file.size,
            "File size changed during transfer (expected {}, read {size})",
            file.size
        );
        Ok(Download {
            file: temp,
            checksum: format!("{:x}", checksum.finalize()),
            etag,
        })
    }

    async fn properties(&self, path: &str) -> Result<Option<(u64, String)>> {
        let response = self.request(Method::HEAD, path).await?.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = Self::checked(response).await?;
        let size = response
            .headers()
            .get("content-length")
            .context("No content length")?
            .to_str()?
            .parse()?;
        let etag = response
            .headers()
            .get("etag")
            .context("No ETag")?
            .to_str()?
            .into();
        Ok(Some((size, etag)))
    }

    pub async fn move_loaded(
        &self,
        file: &InputFile,
        destination: &str,
        checksum: &str,
    ) -> Result<()> {
        if self.properties(&file.path).await?.is_some() {
            let download = self.download(file).await?;
            ensure!(
                download.checksum == checksum,
                "Previously loaded path now contains different bytes"
            );
            self.move_file(&file.path, destination, checksum, Some(&download.etag))
                .await
        } else {
            self.move_file(&file.path, destination, checksum, None)
                .await
        }
    }

    pub async fn create_parents(&self, destination: &str) -> Result<()> {
        let pieces: Vec<_> = destination.split('/').collect();
        for count in 1..pieces.len() {
            let parent = pieces[..count].join("/");
            let response = self
                .request(Method::PUT, &parent)
                .await?
                .query(&[("resource", "directory")])
                .header("If-None-Match", "*")
                .header("Content-Length", "0")
                .send()
                .await?;
            // Only an existing directory is acceptable; a conflicting file must fail.
            if [StatusCode::CONFLICT, StatusCode::PRECONDITION_FAILED].contains(&response.status())
            {
                let head =
                    Self::checked(self.request(Method::HEAD, &parent).await?.send().await?).await?;
                ensure!(
                    head.headers()
                        .get("x-ms-resource-type")
                        .and_then(|h| h.to_str().ok())
                        == Some("directory"),
                    "Destination parent is not a directory: {parent}"
                );
            } else {
                Self::checked(response).await?;
            }
        }
        Ok(())
    }

    /// ADLS Gen2 rename is atomic within this filesystem. Never overwrite a destination.
    pub async fn move_file(
        &self,
        source: &str,
        destination: &str,
        checksum: &str,
        expected_etag: Option<&str>,
    ) -> Result<()> {
        validate_path(source)?;
        validate_path(destination)?;
        let Some((_, etag)) = self.properties(source).await? else {
            // A response may be lost after a successful rename. Verify the destination bytes.
            let (size, destination_etag) = self
                .properties(destination)
                .await?
                .context("Neither source nor destination exists")?;
            let download = self
                .download(&InputFile {
                    path: destination.into(),
                    name: destination.rsplit('/').next().unwrap().into(),
                    size,
                    etag: Some(destination_etag),
                })
                .await?;
            ensure!(
                download.checksum == checksum,
                "Existing destination has different bytes"
            );
            return Ok(());
        };
        if let Some(expected) = expected_etag {
            ensure!(
                etag == expected,
                "Source changed after it was loaded: {source}"
            );
        }
        self.create_parents(destination).await?;
        let rename_source = format!("/{}/{}", self.config.container, encode_path(source));
        let response = self
            .request(Method::PUT, destination)
            .await?
            .header("x-ms-rename-source", rename_source)
            .header("x-ms-source-if-match", etag)
            .header("If-None-Match", "*")
            .header("Content-Length", "0")
            .send()
            .await?;
        Self::checked(response).await?;
        Ok(())
    }
}

pub fn relative_file(path: &str, input: &str) -> Result<String> {
    let prefix = format!("{}/", input.trim_end_matches('/'));
    let relative = path
        .strip_prefix(&prefix)
        .context("File is outside the configured input folder")?;
    validate_path(relative)?;
    Ok(relative.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_preserve_folders_and_encode_special_characters() {
        assert_eq!(canonical_etag("0xAB"), canonical_etag("\"0xAB\""));
        assert_eq!(
            relative_file("base/Test/In/sub/a.csv", "base/Test/In").unwrap(),
            "sub/a.csv"
        );
        assert!(relative_file("base/Test/In-other/a.csv", "base/Test/In").is_err());
        assert_eq!(
            encode_path("In/May-26 #1.xlsx"),
            "In/May%2D26%20%231%2Exlsx"
        );
    }
}
