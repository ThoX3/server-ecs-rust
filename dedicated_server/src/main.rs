use bevy::prelude::*;
use serde::{Deserialize, Serialize};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{Duration, interval};
use uuid::Uuid;

mod resources;
use resources::{PlayerRegistry, ServerConfig, ServerNetwork};
use shared::logger::{info, warn, error};

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

#[derive(Component, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u32);

#[derive(Component, PartialEq, Eq, Debug)]
pub enum Authority {
    Owned,
    Ghost,
}

pub fn main() {
    shared::logger::init_logger("DedicatedServer");
    // Start the dedicated server.
    let config = ServerConfig::from_env();

    let socket = StdUdpSocket::bind(format!("0.0.0.0:{}", config.port)).unwrap();
    socket.set_nonblocking(true).unwrap();

    // Register all shards with the Broker
    for shard in &config.shards {
        let mut reg_msg = [0u8; 33];
        reg_msg[0] = 0x06;
        let topic_bytes = shard.as_bytes();
        reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
        let _ = socket.send_to(&reg_msg, config.broker_addr);
        info!("Registered shard authority for: {}", shard);
        
        // Notify Spatial Server that we are ready
        if let Some(id_str) = shard.split(':').nth(1) {
            if let Ok(shard_id) = id_str.parse::<u32>() {
                let mut ready_msg = [0u8; 5];
                ready_msg[0] = 0x14;
                ready_msg[1..5].copy_from_slice(&shard_id.to_le_bytes());
                let _ = socket.send_to(&ready_msg, config.broker_addr);
                info!("Sent Ready signal for shard {}", shard_id);
            }
        }
    }

    let inter_shard_port = config.port + 1000;
    let inter_socket = StdUdpSocket::bind(format!("0.0.0.0:{}", inter_shard_port)).unwrap();
    inter_socket.set_nonblocking(true).unwrap();

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
        .insert_resource(ServerNetwork(socket))
        .insert_resource(PlayerCount(player_count))
        .add_systems(
            Update,
            (
                handle_networks,
                move_players,
            )
                .chain(),
        )
        .run();
}

fn handle_networks(
    mut commands: Commands,
    network: Res<ServerNetwork>,
    mut registry: ResMut<PlayerRegistry>,
    count: Res<PlayerCount>,
    mut transforms: Query<(&mut Transform, &Authority)>,
    config: Res<ServerConfig>,
) {
    let mut buf = [0u8; 2048];
    while let Ok((amt, _src)) = network.0.recv_from(&mut buf) {
        if amt < 1 { continue; }

        let tag = buf[0];
        if tag == 0x05 {
            // Client Input routed by broker: 0x05 | client_id(4) | payload
            if amt < 5 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let payload = &buf[5..amt];

            // If it is a 0x10 Position Update, sync it if the entity is a Ghost
            if payload.len() > 0 && payload[0] == 0x10 {
                if payload.len() >= 9 {
                    let x = f32::from_le_bytes(payload[1..5].try_into().unwrap());
                    let y = f32::from_le_bytes(payload[5..9].try_into().unwrap());

                    if let Some(&player_entity) = registry.players.get(&client_id) {
                        if let Ok((mut transform, authority)) = transforms.get_mut(player_entity) {
                            if *authority == Authority::Ghost {
                                transform.translation.x = x;
                                transform.translation.y = y;
                            }
                        }
                    } else {
                        // Unknown client routing us a position update? They must have entered our ghost margin!
                        let string_id = Uuid::new_v4().to_string();
                        let entity = commands
                            .spawn((
                                Player { id: string_id.clone() },
                                Transform::from_xyz(x, y, 0.0),
                                PlayerInput { movement_x: 0.0, movement_y: 0.0 },
                                NetworkId(client_id),
                                Authority::Ghost,
                            ))
                            .id();
                        registry.players.insert(client_id, entity);
                        count.0.fetch_add(1, Ordering::Relaxed);
                        // println!("Spawned Ghost for entering client {}", client_id);
                    }
                }
                continue; // Position Update handled
            }

            if let Some(&player_entity) = registry.players.get(&client_id) {
                if let Ok(input) = serde_json::from_slice::<PlayerInput>(payload) {
                    if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                        entity_commands.insert(input);
                    }
                }
            } else if let Ok(req) = serde_json::from_slice::<JoinRequest>(payload) {
                // New player joining!
                let string_id = Uuid::new_v4().to_string();
                let entity = commands
                    .spawn((
                        Player { id: string_id.clone() },
                        Transform::default(),
                        PlayerInput { movement_x: 0.0, movement_y: 0.0 },
                        NetworkId(client_id),
                        Authority::Owned,
                    ))
                    .id();

                registry.players.insert(client_id, entity);
                count.0.fetch_add(1, Ordering::Relaxed);
                info!("Player {} joined shard", req.username);
            }
        } else if tag == 0x07 {
            // Disconnect packet from Broker
            if amt < 5 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            if let Some(entity) = registry.players.remove(&client_id) {
                commands.entity(entity).despawn();
                count.0.fetch_sub(1, Ordering::Relaxed);
                info!("Client {} disconnected from shard", client_id);
            }
        } else if tag == 0x12 {
            // AuthorityChange: 0x12 | client_id(4) | old_shard(4) | new_shard(4)
            if amt < 13 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let old_shard = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let new_shard = u32::from_le_bytes(buf[9..13].try_into().unwrap());

            let hosts_old = config.shards.contains(&format!("shard:{}", old_shard));
            let hosts_new = config.shards.contains(&format!("shard:{}", new_shard));

            if let Some(&player_entity) = registry.players.get(&client_id) {
                if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                    if hosts_old && !hosts_new {
                        entity_commands.insert(Authority::Ghost);
                        info!("Client {} demoted to Ghost on Shard {}", client_id, old_shard);
                    } else if hosts_new {
                        entity_commands.insert(Authority::Owned);
                        info!("Client {} promoted to Owned on Shard {}", client_id, new_shard);
                    }
                }
            }
        }
    }
}

async fn heartbeat_task(config: ServerConfig, current_players: Arc<AtomicUsize>) {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
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

fn move_players(
    time: Res<Time>,
    mut query: Query<(&PlayerInput, &mut Transform, &Authority), With<Player>>,
) {
    for (input, mut transform, authority) in query.iter_mut() {
        if !matches!(authority, Authority::Ghost { .. }) {
            let speed = 5.0;
            transform.translation.x += input.movement_x * speed * time.delta_secs();
            transform.translation.y += input.movement_y * speed * time.delta_secs();
        }
    }
}
