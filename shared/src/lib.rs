use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinRequest {
    pub username: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WelcomeMessage {
    pub player_id: String,
}
