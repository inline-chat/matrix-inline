use std::{
    error::Error,
    fs::OpenOptions,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
    sync::Arc,
};

use inline_client::{ClientIdentity, ClientStore, InlineClient, SdkBackend, SqliteStore};
use matrix_inline_adapter::{
    AdapterClientFactory, AdapterClientRegistration, AdapterEventStore, AdapterHttpState,
    bind_adapter_http_state,
};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:29342";
const DEFAULT_STORE_PATH: &str = "inline-client.sqlite3";
const DEFAULT_API_BASE_URL: &str = "https://api.inline.chat/v1";
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

    let account_store_dir = options
        .account_store_dir
        .clone()
        .unwrap_or_else(|| default_account_store_dir(&options.store_path));
    std::fs::create_dir_all(&account_store_dir)?;
    set_private_directory_permissions(&account_store_dir)?;
    prepare_private_sqlite_path(&options.store_path)?;
    migrate_legacy_session(&options.store_path, &account_store_dir, None).await?;
    let event_store = AdapterEventStore::open(&options.store_path)?;
    let legacy_store_path = options.store_path.clone();
    let api_base_url = options.api_base_url.clone();
    let realtime_url = options.realtime_url.clone();
    let realtime_handshake = options.realtime_handshake;
    let factory: AdapterClientFactory = Arc::new(move |namespace| {
        let account_store_dir = account_store_dir.clone();
        let legacy_store_path = legacy_store_path.clone();
        let api_base_url = api_base_url.clone();
        let realtime_url = realtime_url.clone();
        Box::pin(async move {
            migrate_legacy_session(&legacy_store_path, &account_store_dir, Some(&namespace))
                .await
                .map_err(|error| error.to_string())?;
            build_account_client(
                &account_store_dir,
                &namespace,
                api_base_url,
                realtime_url,
                realtime_handshake,
            )
            .await
            .map_err(|error| error.to_string())
        })
    });
    let http_state = AdapterHttpState::with_client_factory(event_store, factory);
    bind_adapter_http_state(options.bind_addr, http_state).await?;
    Ok(())
}

async fn build_account_client(
    account_store_dir: &Path,
    namespace: &str,
    api_base_url: String,
    realtime_url: String,
    realtime_handshake: bool,
) -> Result<AdapterClientRegistration, Box<dyn Error>> {
    let store_path = account_store_dir.join(format!("{namespace}.sqlite3"));
    prepare_private_sqlite_path(&store_path)?;
    let store = SqliteStore::open(store_path)?;
    let has_session = store.load_session().await?.is_some();
    let mut backend = SdkBackend::builder()
        .api_base_url(api_base_url)
        .realtime_url(realtime_url)
        .identity(ClientIdentity::new(
            "matrix-inline-adapter",
            inline_client::VERSION,
        ))
        .store(store);
    if realtime_handshake {
        backend = backend.enable_realtime_handshake();
    }
    let backend = backend.build()?;
    let client = InlineClient::builder().backend(backend).build().spawn();
    Ok(AdapterClientRegistration {
        client,
        resume_stored_session: has_session,
    })
}

fn default_account_store_dir(store_path: &Path) -> PathBuf {
    let file_name = store_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("inline-client");
    store_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{file_name}.accounts"))
}

async fn migrate_legacy_session(
    legacy_path: &Path,
    account_store_dir: &Path,
    requested_namespace: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    if !legacy_path.exists() {
        return Ok(());
    }
    let legacy = SqliteStore::open(legacy_path)?;
    let Some(mut session) = legacy.load_session().await? else {
        return Ok(());
    };
    let stored_namespace = session
        .account_namespace
        .as_deref()
        .map(str::trim)
        .filter(|namespace| valid_store_namespace(namespace));
    let requested_namespace = requested_namespace
        .map(str::trim)
        .filter(|namespace| valid_store_namespace(namespace));
    let namespace = match (stored_namespace, requested_namespace) {
        (Some(namespace), _) => namespace.to_owned(),
        (None, Some(namespace)) => {
            session.account_namespace = Some(namespace.to_owned());
            legacy.save_session(session.clone()).await?;
            log::info!(
                "claimed unnamespaced legacy Inline session for the requesting account store"
            );
            namespace.to_owned()
        }
        (None, None) => {
            log::info!(
                "legacy Inline session has no account namespace; it will be claimed by the first existing login"
            );
            return Ok(());
        }
    };
    let target_path = account_store_dir.join(format!("{namespace}.sqlite3"));
    prepare_private_sqlite_path(&target_path)?;
    let target = SqliteStore::open(target_path)?;
    if target.load_session().await?.is_none() {
        target.save_session(session).await?;
        log::info!("imported legacy Inline session into isolated account store");
    }
    Ok(())
}

fn prepare_private_sqlite_path(path: &Path) -> io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let _file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    set_private_file_permissions(path)
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn valid_store_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 128
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AdapterOptions {
    bind_addr: SocketAddr,
    store_path: PathBuf,
    account_store_dir: Option<PathBuf>,
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
                "--account-store-dir" => {
                    options.account_store_dir = Some(PathBuf::from(parse_string(
                        &mut args,
                        "--account-store-dir",
                    )?));
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
        if !options.help && !options.bind_addr.ip().is_loopback() {
            return Err(format!(
                "--bind must use a loopback address because the adapter has no network authentication (got {})",
                options.bind_addr
            ));
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
            account_store_dir: None,
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
  --account-store-dir <path> Directory for isolated per-account client stores
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
    use inline_client::{AuthCredential, AuthToken, StoredSession};

    use super::*;

    fn temp_store_root(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "matrix-inline-adapter-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn stored_session(namespace: Option<&str>) -> StoredSession {
        StoredSession {
            auth: AuthCredential::AccessToken {
                token: AuthToken::try_new("legacy-token").unwrap(),
            },
            account_namespace: namespace.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn parse_defaults() {
        let options = AdapterOptions::parse([]).unwrap();

        assert_eq!(options.bind_addr.to_string(), DEFAULT_BIND_ADDR);
        assert_eq!(options.store_path, PathBuf::from(DEFAULT_STORE_PATH));
        assert_eq!(options.api_base_url, DEFAULT_API_BASE_URL);
        assert_eq!(options.realtime_url, DEFAULT_REALTIME_URL);
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

    #[test]
    fn parse_rejects_non_loopback_bind() {
        let err =
            AdapterOptions::parse(["--bind".to_owned(), "0.0.0.0:29342".to_owned()]).unwrap_err();

        assert!(err.contains("loopback"));
    }

    #[tokio::test]
    async fn migrates_initial_shared_session_to_the_matching_account_store() {
        let root = temp_store_root("named-migration");
        let legacy_path = root.join("inline-client.sqlite3");
        let account_store_dir = root.join("accounts");
        let legacy = SqliteStore::open(&legacy_path).unwrap();
        legacy
            .save_session(stored_session(Some("42")))
            .await
            .unwrap();
        drop(legacy);

        migrate_legacy_session(&legacy_path, &account_store_dir, None)
            .await
            .unwrap();

        let migrated = SqliteStore::open(account_store_dir.join("42.sqlite3")).unwrap();
        assert_eq!(
            migrated
                .load_session()
                .await
                .unwrap()
                .unwrap()
                .account_namespace
                .as_deref(),
            Some("42")
        );
        drop(migrated);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn first_existing_login_claims_an_unnamespaced_initial_session() {
        let root = temp_store_root("unnamed-migration");
        let legacy_path = root.join("inline-client.sqlite3");
        let account_store_dir = root.join("accounts");
        let legacy = SqliteStore::open(&legacy_path).unwrap();
        legacy.save_session(stored_session(None)).await.unwrap();
        drop(legacy);

        migrate_legacy_session(&legacy_path, &account_store_dir, None)
            .await
            .unwrap();
        assert!(!account_store_dir.join("42.sqlite3").exists());

        migrate_legacy_session(&legacy_path, &account_store_dir, Some("42"))
            .await
            .unwrap();

        let legacy = SqliteStore::open(&legacy_path).unwrap();
        let migrated = SqliteStore::open(account_store_dir.join("42.sqlite3")).unwrap();
        for store in [legacy, migrated] {
            assert_eq!(
                store
                    .load_session()
                    .await
                    .unwrap()
                    .unwrap()
                    .account_namespace
                    .as_deref(),
                Some("42")
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
