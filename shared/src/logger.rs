pub use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_logger(service_name: &str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);

    tracing::info!("--- [{}] Logger Initialized ---", service_name);
}
