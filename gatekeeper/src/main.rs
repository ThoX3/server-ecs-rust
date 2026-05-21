use axum::{routing::{get, post}, Router};
use deadpool_redis::{Config, Runtime};
use std::sync::Arc;
use std::net::SocketAddr;

mod handlers;
mod redis_pool;

pub struct AppState {
    pub redis_pool: deadpool_redis::Pool,
}

#[tokio::main]
async fn main() {
    // Start the HTTP gateway.
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let cfg = Config::from_url(redis_url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let state = Arc::new(AppState { redis_pool: pool });

    let app = Router::new()
        .route("/health", get(handlers::health_handler))
        .route("/login", post(handlers::login_handler))
        .with_state(state);

    let port: u16 = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string()).parse().unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    println!("Gatekeeper listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}
