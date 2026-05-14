use bevy::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Resource)]
pub struct ServerConfig {
    pub id: String,
    pub port: u16,
    pub zone: String,
    pub max_players: usize,
    pub orchestrator_addr: SocketAddr,
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<SocketAddr, PlayerInfo>,
}

pub struct PlayerInfo {
    pub username: String,
}
