use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::sync::Arc;
use shared::ServerInfo;
use crate::AppState;
use axum::http::StatusCode;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub player_id: String,
    pub server: ServerInfo,
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
    Json(HealthResponse { status: "ok".to_string() })
}

fn get_zone_for_ip(ip: &str) -> String {
    // Dummy GeoIP implementation
    // In a real application, you would query a MaxMind GeoIP database here.
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
    if payload.username.is_empty() || payload.password.as_deref() != Some("1234") {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { error: "Invalid credentials".to_string() })
        ));
    }

    let ip_str = addr.ip().to_string();
    let target_zone = get_zone_for_ip(&ip_str);

    if let Some(server) = crate::redis_pool::find_available_server(&state.redis_pool, &target_zone).await {
        let response = LoginResponse {
            player_id: Uuid::new_v4().to_string(),
            server,
        };
        Ok(Json(response))
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse { error: "No server available".to_string() })
        ))
    }
}
