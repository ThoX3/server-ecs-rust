use bevy::prelude::*;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
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
    pub players: HashMap<SocketAddr, String>,
}

#[derive(Resource)]
pub struct ChannelRecv(pub mpsc::Receiver<(SocketAddr, String)>);

#[derive(Component)]
pub struct Player {
    pub id: String,
}

pub fn main() {
    let config = ServerConfig::from_env();

    let (tx, rx) = mpsc::channel(100);
    let config_clone = config.clone();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            run_server_network(config_clone, tx).await;
        });
    });

    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(config)
        .insert_resource(PlayerRegistry::default())
        .insert_resource(ChannelRecv(rx))
        .add_systems(Update, handle_networks)
        .run();
}

fn handle_networks(
    mut commands: Commands,
    mut channel: ResMut<ChannelRecv>,
    mut registry: ResMut<PlayerRegistry>,
) {
    while let Ok((addr, username)) = channel.0.try_recv() {
        if !registry.players.contains_key(&addr) {
            let player_id = Uuid::new_v4().to_string();
            registry.players.insert(addr, player_id.clone());

            commands.spawn((Player { id: player_id }, Transform::default()));

            println!("Player {} joined from {}", username, addr);
        }
    }
}

async fn run_server_network(config: ServerConfig, tx: mpsc::Sender<(SocketAddr, String)>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.cert.der().clone();
    let key_der = cert.signing_key.serialize_der();

    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert_der)],
            PrivateKeyDer::try_from(key_der).unwrap(),
        )
        .unwrap();

    let server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
    ));
    let endpoint = quinn::Endpoint::server(
        server_config,
        format!("0.0.0.0:{}", config.port).parse().unwrap(),
    )
    .unwrap();

    let config_clone = config.clone();
    tokio::spawn(async move {
        heartbeat_task(config_clone).await;
    });

    while let Some(conn) = endpoint.accept().await {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            if let Ok(connection) = conn.await {
                if let Ok((mut send, mut recv)) = connection.accept_bi().await {
                    let mut buf = [0u8; 1024];
                    if let Ok(Some(len)) = recv.read(&mut buf).await {
                        if let Ok(req) = serde_json::from_slice::<JoinRequest>(&buf[..len]) {
                            let _ = tx_clone
                                .send((connection.remote_address(), req.username))
                                .await;

                            let resp = WelcomeMessage {
                                player_id: Uuid::new_v4().to_string(),
                            };
                            if let Ok(bytes) = serde_json::to_vec(&resp) {
                                let _ = send.write_all(&bytes).await;
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn heartbeat_task(config: ServerConfig) {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let mut sys = System::new_all();
    let mut ticker = interval(Duration::from_secs(5));

    loop {
        ticker.tick().await;
        sys.refresh_all();

        let cpu_usage = sys.global_cpu_usage();
        let ram_usage = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

        let hb = Heartbeat {
            id: config.id.clone(),
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            player_count: 0, // todo: use real count
            max_players: config.max_players,
            cpu_usage,
            ram_usage,
        };

        if let Ok(bytes) = serde_json::to_vec(&hb) {
            let _ = socket.send_to(&bytes, config.orchestrator_addr);
        }
    }
}
