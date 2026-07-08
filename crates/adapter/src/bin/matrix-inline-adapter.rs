use std::{error::Error, net::SocketAddr, path::PathBuf, process::ExitCode, str::FromStr};

use inline_client::{ClientIdentity, InlineClient, SdkBackend, SqliteStore};
use matrix_inline_adapter::bind_adapter_http;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:29342";
const DEFAULT_STORE_PATH: &str = "inline-client.sqlite3";
const DEFAULT_API_BASE_URL: &str = "https://api.inline.chat";
const DEFAULT_REALTIME_URL: &str = "wss://api.inline.chat/realtime";

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("matrix-inline-adapter: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let options = AdapterOptions::parse(std::env::args().skip(1))?;
    if options.help {
        print_help();
        return Ok(());
    }

    let store = SqliteStore::open(&options.store_path)?;
    let mut backend = SdkBackend::builder()
        .api_base_url(options.api_base_url)
        .realtime_url(options.realtime_url)
        .identity(ClientIdentity::new(
            "matrix-inline-adapter",
            inline_client::VERSION,
        ))
        .store(store);
    if options.realtime_handshake {
        backend = backend.enable_realtime_handshake();
    }
    let backend = backend.build()?;

    let client = InlineClient::builder().backend(backend).build().spawn();
    resume_stored_session(&client).await?;
    bind_adapter_http(options.bind_addr, client).await?;
    Ok(())
}

async fn resume_stored_session(client: &InlineClient) -> Result<(), Box<dyn Error>> {
    match client.resume_session().await {
        Ok(status) => {
            log::info!("matrix-inline-adapter startup status: {:?}", status.status);
            Ok(())
        }
        Err(error) => {
            log::warn!("matrix-inline-adapter startup resume failed: {error}");
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AdapterOptions {
    bind_addr: SocketAddr,
    store_path: PathBuf,
    api_base_url: String,
    realtime_url: String,
    realtime_handshake: bool,
    help: bool,
}

impl AdapterOptions {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut options = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bind" => {
                    options.bind_addr = parse_value(&mut args, "--bind")?;
                }
                "--store" => {
                    options.store_path = PathBuf::from(parse_string(&mut args, "--store")?);
                }
                "--api-base-url" => {
                    options.api_base_url = parse_string(&mut args, "--api-base-url")?;
                }
                "--realtime-url" => {
                    options.realtime_url = parse_string(&mut args, "--realtime-url")?;
                }
                "--realtime-handshake" => {
                    options.realtime_handshake = true;
                }
                "--no-realtime-handshake" => {
                    options.realtime_handshake = false;
                }
                "-h" | "--help" => {
                    options.help = true;
                }
                other => return Err(format!("unknown option {other:?}")),
            }
        }
        Ok(options)
    }
}

impl Default for AdapterOptions {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND_ADDR
                .parse()
                .expect("default bind address should parse"),
            store_path: PathBuf::from(DEFAULT_STORE_PATH),
            api_base_url: DEFAULT_API_BASE_URL.to_owned(),
            realtime_url: DEFAULT_REALTIME_URL.to_owned(),
            realtime_handshake: true,
            help: false,
        }
    }
}

fn parse_value<T>(
    args: &mut impl Iterator<Item = String>,
    option: &'static str,
) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = parse_string(args, option)?;
    value
        .parse::<T>()
        .map_err(|error| format!("invalid {option} value {value:?}: {error}"))
}

fn parse_string(
    args: &mut impl Iterator<Item = String>,
    option: &'static str,
) -> Result<String, String> {
    args.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{option} requires a value"))
}

fn print_help() {
    println!(
        "\
matrix-inline-adapter

Usage:
  matrix-inline-adapter [options]

Options:
  --bind <addr>              Loopback bind address [default: {DEFAULT_BIND_ADDR}]
  --store <path>             SQLite store path [default: {DEFAULT_STORE_PATH}]
  --api-base-url <url>       Inline API base URL [default: {DEFAULT_API_BASE_URL}]
  --realtime-url <url>       Inline realtime URL [default: {DEFAULT_REALTIME_URL}]
  --realtime-handshake       Validate credentials by opening realtime on connect [default]
  --no-realtime-handshake    Persist session without realtime handshake
  -h, --help                 Show this help
"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults() {
        let options = AdapterOptions::parse([]).unwrap();

        assert_eq!(options.bind_addr.to_string(), DEFAULT_BIND_ADDR);
        assert_eq!(options.store_path, PathBuf::from(DEFAULT_STORE_PATH));
        assert!(options.realtime_handshake);
    }

    #[test]
    fn parse_overrides() {
        let options = AdapterOptions::parse([
            "--bind".to_owned(),
            "127.0.0.1:30000".to_owned(),
            "--store".to_owned(),
            "/tmp/inline-client.sqlite3".to_owned(),
            "--api-base-url".to_owned(),
            "http://127.0.0.1:8080".to_owned(),
            "--realtime-url".to_owned(),
            "ws://127.0.0.1:8080/realtime".to_owned(),
            "--no-realtime-handshake".to_owned(),
        ])
        .unwrap();

        assert_eq!(options.bind_addr.to_string(), "127.0.0.1:30000");
        assert_eq!(
            options.store_path,
            PathBuf::from("/tmp/inline-client.sqlite3")
        );
        assert_eq!(options.api_base_url, "http://127.0.0.1:8080");
        assert_eq!(options.realtime_url, "ws://127.0.0.1:8080/realtime");
        assert!(!options.realtime_handshake);
    }

    #[test]
    fn parse_rejects_unknown_option() {
        let err = AdapterOptions::parse(["--wat".to_owned()]).unwrap_err();

        assert!(err.contains("unknown option"));
    }
}
