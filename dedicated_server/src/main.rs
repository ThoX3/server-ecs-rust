use bevy::prelude::*;
use game_sockets::protocols::UdpBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use serde::{Deserialize, Serialize};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{Duration, interval};
use uuid::Uuid;

mod resources;
use resources::{PlayerRegistry, ServerConfig, ServerNetwork};

#[derive(Component)]
pub struct Player {
    pub id: String,
}

#[derive(Component, Serialize, Deserialize, Clone, Debug)]
pub struct PlayerInput {
    pub movement_x: f32,
    pub movement_y: f32,
}

#[derive(Resource)]
pub struct PlayerCount(pub Arc<AtomicUsize>);

pub fn main() {
    // Start the dedicated server.
    let config = ServerConfig::from_env();

    let peer = GamePeer::new(UdpBackend::new());
    peer.listen("0.0.0.0", config.port).unwrap();

    let config_clone = config.clone();

    let player_count = Arc::new(AtomicUsize::new(0));
    let player_count_tokio = player_count.clone();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            heartbeat_task(config_clone, player_count_tokio).await;
        });
    });

    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(config)
        .insert_resource(PlayerRegistry::default())
        .insert_resource(ServerNetwork(peer))
        .insert_resource(PlayerCount(player_count))
        .add_systems(Update, (handle_networks, move_players))
        .run();
}

fn handle_networks(
    mut commands: Commands,
    mut network: ResMut<ServerNetwork>,
    mut registry: ResMut<PlayerRegistry>,
    count: Res<PlayerCount>,
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
                // Cas 1 : Le joueur est déjà connecté, on traite ses inputs de gameplay
                if let Some(&player_entity) = registry.players.get(&connection) {
                    let input_payload = &data[4..];
                    if let Ok(input) = serde_json::from_slice::<PlayerInput>(&input_payload) {
                        if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                            entity_commands.insert(input);
                        }
                    }
                }
                // Cas 2 : Le joueur n'est pas encore enregistré, on traite sa demande de connexion
                else if let Ok(req) = serde_json::from_slice::<JoinRequest>(&data) {
                    if !registry.players.contains_key(&connection) {
                        let player_id = Uuid::new_v4().to_string();
                        let entity = commands
                            .spawn((
                                Player {
                                    id: player_id.clone(),
                                },
                                Transform::default(),
                                PlayerInput {
                                    movement_x: 0.0,
                                    movement_y: 0.0,
                                },
                            ))
                            .id();

                        registry.players.insert(connection, entity);
                        count.0.fetch_add(1, Ordering::Relaxed);

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
                if let Some(entity) = registry.players.remove(&conn) {
                    commands.entity(entity).despawn(); // Despawn the player entity.
                    count.0.fetch_sub(1, Ordering::Relaxed); // Decrement player count.
                }
                println!("Deconnexion : {:?}", conn);
            }
            _ => {}
        }
    }
}

async fn heartbeat_task(config: ServerConfig, current_players: Arc<AtomicUsize>) {
    // Send periodic heartbeats to the orchestrator.
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let mut ticker = interval(Duration::from_secs(5));

    loop {
        ticker.tick().await;

        let players = current_players.load(Ordering::Relaxed);

        let status = if players >= config.max_players {
            "full".to_string()
        } else {
            "available".to_string()
        };

        let hb = Heartbeat {
            id: config.id.clone(),
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            player_count: players,
            max_players: config.max_players,
            status,
        };

        if let Ok(bytes) = serde_json::to_vec(&hb) {
            let _ = socket.send_to(&bytes, config.orchestrator_addr);
        }
    }
}

fn move_players(time: Res<Time>, mut query: Query<(&PlayerInput, &mut Transform), With<Player>>) {
    for (input, mut transform) in query.iter_mut() {
        let speed = 5.0;

        transform.translation.x += input.movement_x * speed * time.delta_secs();
        transform.translation.y += input.movement_y * speed * time.delta_secs();
    }
}
