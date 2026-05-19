use bevy::prelude::*;
use game_sockets::protocols::UdpBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::net::UdpSocket;
use sysinfo::System;
use tokio::time::{interval, Duration};
use uuid::Uuid;

mod resources;
use resources::{PlayerRegistry, ServerConfig, ServerNetwork};

#[derive(Component)]
pub struct Player {
    pub id: String,
}

pub fn main() {
    let config = ServerConfig::from_env();

    let peer = GamePeer::new(UdpBackend::new());
    peer.listen("0.0.0.0", config.port).unwrap();

    let config_clone = config.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            heartbeat_task(config_clone).await;
        });
    });

    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(config)
        .insert_resource(PlayerRegistry::default())
        .insert_resource(ServerNetwork(peer))
        .add_systems(Update, handle_networks)
        .run();
}

fn handle_networks(
    mut commands: Commands,
    mut network: ResMut<ServerNetwork>,
    mut registry: ResMut<PlayerRegistry>,
) {
    while let Ok(Some(event)) = network.0.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                println!("New connection: {:?}", conn);
            }
            GameNetworkEvent::Message {
                connection,
                stream,
                data,
            } => {
                if let Ok(req) = serde_json::from_slice::<JoinRequest>(&data) {
                    if !registry.players.contains_key(&connection) {
                        let player_id = Uuid::new_v4().to_string();
                        registry.players.insert(connection, player_id.clone());

                        commands.spawn((
                            Player {
                                id: player_id.clone(),
                            },
                            Transform::default(),
                        ));

                        println!("Player {} joined", req.username);

                        let resp = WelcomeMessage { player_id };
                        if let Ok(bytes) = serde_json::to_vec(&resp) {
                            let _ = network
                                .0
                                .send(&connection, &stream, bytes::Bytes::from(bytes));
                        }
                    }
                }
            }
            GameNetworkEvent::Disconnected(conn) => {
                registry.players.remove(&conn);
                println!("Disconnected: {:?}", conn);
            }
            _ => {}
        }
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
