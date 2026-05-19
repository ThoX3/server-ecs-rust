use bevy::prelude::*;
use game_sockets::{GameConnection, GamePeer};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Resource, Clone)]
pub struct ServerConfig {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub max_players: usize,
    pub orchestrator_addr: SocketAddr,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let port = std::env::var("DS_PORT")
            .unwrap_or_else(|_| "7001".to_string())
            .parse()
            .unwrap_or(7001);
        let max_players = std::env::var("MAX_PLAYERS")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100);
        let orch_port = std::env::var("ORCH_PORT")
            .unwrap_or_else(|_| "8000".to_string())
            .parse()
            .unwrap_or(8000);

        Self {
            id: Uuid::new_v4().to_string(),
            ip: "127.0.0.1".to_string(),
            port,
            zone: std::env::var("ZONE").unwrap_or_else(|_| "zone_A".to_string()),
            max_players,
            orchestrator_addr: format!("127.0.0.1:{}", orch_port).parse().unwrap(),
        }
    }
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<GameConnection, Entity>,
}

#[derive(Resource)]
pub struct ServerNetwork(pub GamePeer);
