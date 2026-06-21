use crate::AppState;
use axum::http::StatusCode;
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use shared::ServerInfo;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub player_id: String,
    pub broker: ServerInfo,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

pub async fn health_handler() -> Json<HealthResponse> {
    // Return a simple health status.
    Json(HealthResponse { status: "ok".to_string() })
}

fn get_zone_for_ip(ip: &str) -> String {
    // Choose a zone based on IP prefix.
    if ip.starts_with("192.") || ip.starts_with("10.") || ip.starts_with("127.") {
        "zone_A".to_string()
    } else {
        "zone_B".to_string()
    }
}

pub async fn login_handler(
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate credentials and route to a server.
    if payload.username.is_empty() || payload.password.as_deref() != Some("1234") {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { error: "Invalid credentials".to_string() })
        ));
    }

    let broker_host = std::env::var("BROKER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let broker_port: u16 = std::env::var("BROKER_PORT").unwrap_or_else(|_| "9000".to_string()).parse().unwrap_or(9000);

    let response = LoginResponse {
        player_id: Uuid::new_v4().to_string(),
        broker: ServerInfo {
            ip: broker_host,
            port: broker_port,
            zone: "global".to_string(),
        },
    };
    Ok(Json(response))
}
