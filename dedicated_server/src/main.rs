use bevy::prelude::*;
use game_sockets::protocols::UdpBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use serde::{Deserialize, Serialize};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
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

#[derive(Resource)]
pub struct InterShardSocket(pub StdUdpSocket);

#[derive(Component, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u32);

#[derive(Component, PartialEq, Eq, Debug)]
pub enum Authority {
    Owned,
    PendingHandoff { target_shard_addr: SocketAddr },
    Ghost { source_shard_addr: SocketAddr },
}

#[derive(Component, Default)]
pub struct HandoffTickCount(pub u32);

pub const HANDOFF_STABILIZE_TICKS: u32 = 5;

pub struct CrossingAlert {
    pub entity_id: u32,
    pub target_shard: SocketAddr,
}

#[derive(Resource, Default)]
pub struct CrossingAlertQueue(pub Vec<CrossingAlert>);

pub struct HandoffCompleteTrigger {
    pub entity_id: u32,
}

#[derive(Resource, Default)]
pub struct HandoffCompleteQueue(pub Vec<HandoffCompleteTrigger>);

pub fn main() {
    // Start the dedicated server.
    let config = ServerConfig::from_env();

    let peer = GamePeer::new(UdpBackend::new());
    peer.listen("0.0.0.0", config.port).unwrap();

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
        .insert_resource(ServerNetwork(peer))
        .insert_resource(PlayerCount(player_count))
        .insert_resource(InterShardSocket(inter_socket))
        .init_resource::<CrossingAlertQueue>()
        .init_resource::<HandoffCompleteQueue>()
        .add_systems(
            Update,
            (
                handle_networks,
                move_players,
                initiate_handoff_system,
                handle_inter_shard_messages,
                sync_ghosts_system,
                check_handoff_stable_system,
                finalize_handoff_system,
            )
                .chain(),
        )
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
                        let string_id = Uuid::new_v4().to_string();
                        let numeric_id = rand::random::<u32>();
                        let entity = commands
                            .spawn((
                                Player {
                                    id: string_id.clone(),
                                },
                                Transform::default(),
                                PlayerInput {
                                    movement_x: 0.0,
                                    movement_y: 0.0,
                                },
                                NetworkId(numeric_id),
                                Authority::Owned,
                                HandoffTickCount::default(),
                            ))
                            .id();

                        registry.players.insert(connection, entity);
                        count.0.fetch_add(1, Ordering::Relaxed);

                        println!("Player {} joined", req.username);

                        let resp = WelcomeMessage {
                            player_id: string_id,
                        };
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
                    commands.entity(entity).despawn();
                    count.0.fetch_sub(1, Ordering::Relaxed);
                }
                println!("Deconnexion : {:?}", conn);
            }
            _ => {}
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

pub fn initiate_handoff_system(
    mut queue: ResMut<CrossingAlertQueue>,
    mut query: Query<(&mut Authority, &Transform, &PlayerInput, &NetworkId)>,
    socket: Res<InterShardSocket>,
) {
    for alert in queue.0.drain(..) {
        for (mut authority, transform, input, net_id) in query.iter_mut() {
            if net_id.0 == alert.entity_id && *authority == Authority::Owned {
                println!("Initiation du Handoff pour {}", net_id.0);

                *authority = Authority::PendingHandoff {
                    target_shard_addr: alert.target_shard,
                };

                let mut buf = [0u8; 85];
                buf[0] = 0x20;
                buf[1..5].copy_from_slice(&net_id.0.to_le_bytes());
                buf[5..9].copy_from_slice(&transform.translation.x.to_le_bytes());
                buf[9..13].copy_from_slice(&transform.translation.y.to_le_bytes());
                buf[13..17].copy_from_slice(&input.movement_x.to_le_bytes());
                buf[17..21].copy_from_slice(&input.movement_y.to_le_bytes());

                let _ = socket.0.send_to(&buf, alert.target_shard);
            }
        }
    }
}

pub fn sync_ghosts_system(
    query: Query<(&Authority, &Transform, &PlayerInput, &NetworkId)>,
    socket: Res<InterShardSocket>,
) {
    for (authority, transform, input, net_id) in query.iter() {
        if let Authority::PendingHandoff { target_shard_addr } = authority {
            // Tag 0x23 : GhostUpdate
            // entity_id: u32 (4) | pos: Vec2 (8) | vel: Vec2 (8) = 1 + 4 + 8 + 8 = 21 octets
            let mut buf = [0u8; 21];
            buf[0] = 0x23;
            buf[1..5].copy_from_slice(&net_id.0.to_le_bytes());
            buf[5..9].copy_from_slice(&transform.translation.x.to_le_bytes());
            buf[9..13].copy_from_slice(&transform.translation.y.to_le_bytes());
            buf[13..17].copy_from_slice(&input.movement_x.to_le_bytes());
            buf[17..21].copy_from_slice(&input.movement_y.to_le_bytes());

            let _ = socket.0.send_to(&buf, target_shard_addr);
        }
    }
}

pub fn handle_inter_shard_messages(
    mut commands: Commands,
    socket: Res<InterShardSocket>,
    mut query: Query<(
        Entity,
        &NetworkId,
        &mut Authority,
        &mut Transform,
        &mut PlayerInput,
        Option<&mut HandoffTickCount>,
    )>,
) {
    let mut buf = [0u8; 512];

    while let Ok((amt, src)) = socket.0.recv_from(&mut buf) {
        if amt < 5 {
            continue;
        }

        let tag = buf[0];
        let entity_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());

        match tag {
            0x20 => {
                // HandoffRequest reçu
                if amt >= 85 {
                    let pos_x = f32::from_le_bytes(buf[5..9].try_into().unwrap());
                    let pos_y = f32::from_le_bytes(buf[9..13].try_into().unwrap());
                    let vel_x = f32::from_le_bytes(buf[13..17].try_into().unwrap());
                    let vel_y = f32::from_le_bytes(buf[17..21].try_into().unwrap());

                    let already_exists = query
                        .iter()
                        .any(|(_, net_id, _, _, _, _)| net_id.0 == entity_id);

                    if already_exists {
                        // Tag 0x22 : HandoffReject
                        let mut reject_buf = [0u8; 5];
                        reject_buf[0] = 0x22;
                        reject_buf[1..5].copy_from_slice(&entity_id.to_le_bytes());
                        let _ = socket.0.send_to(&reject_buf, src);
                    } else {
                        commands.spawn((
                            Player {
                                id: entity_id.to_string(),
                            },
                            PlayerInput {
                                movement_x: vel_x,
                                movement_y: vel_y,
                            },
                            NetworkId(entity_id),
                            Transform::from_xyz(pos_x, pos_y, 0.0),
                            Authority::Ghost {
                                source_shard_addr: src,
                            },
                            HandoffTickCount::default(),
                        ));

                        // Tag 0x21 : HandoffAccept
                        let mut accept_buf = [0u8; 5];
                        accept_buf[0] = 0x21;
                        accept_buf[1..5].copy_from_slice(&entity_id.to_le_bytes());
                        let _ = socket.0.send_to(&accept_buf, src);

                        println!("Ghost spawn pour {} (handoff accepté)", entity_id);
                    }
                }
            }
            0x21 => {
                // HandoffAccept reçu côté source : le shard destination a bien
                // spawn le Ghost.
                println!("HandoffAccept reçu pour {}", entity_id);
            }
            0x22 => {
                // HandoffReject : le shard destination a refusé le transfert.
                // L'entité reprend l'autorité complète et rebondit sur la frontière.
                for (_, net_id, mut authority, _, mut input, _) in query.iter_mut() {
                    if net_id.0 == entity_id {
                        *authority = Authority::Owned;
                        input.movement_x *= -1.0;
                        input.movement_y *= -1.0;
                        println!("HandoffReject pour {} : rebond sur la frontière", entity_id);
                    }
                }
            }
            0x23 => {
                // GhostUpdate reçu côté destination : on met à jour la copie locale
                // du Ghost et on incrémente son compteur de synchronisation.
                if amt >= 21 {
                    let pos_x = f32::from_le_bytes(buf[5..9].try_into().unwrap());
                    let pos_y = f32::from_le_bytes(buf[9..13].try_into().unwrap());
                    let vel_x = f32::from_le_bytes(buf[13..17].try_into().unwrap());
                    let vel_y = f32::from_le_bytes(buf[17..21].try_into().unwrap());

                    for (_, net_id, authority, mut transform, mut input, sync_count) in
                        query.iter_mut()
                    {
                        if net_id.0 == entity_id && matches!(*authority, Authority::Ghost { .. }) {
                            transform.translation.x = pos_x;
                            transform.translation.y = pos_y;
                            input.movement_x = vel_x;
                            input.movement_y = vel_y;

                            if let Some(mut count) = sync_count {
                                count.0 += 1;
                            }
                        }
                    }
                }
            }
            0x24 => {
                // HandoffComplete reçu côté destination : on prend l'autorité
                // complète sur le Ghost, qui devient Owned.
                for (_, net_id, mut authority, _, _, sync_count) in query.iter_mut() {
                    if net_id.0 == entity_id && matches!(*authority, Authority::Ghost { .. }) {
                        *authority = Authority::Owned;
                        if let Some(mut count) = sync_count {
                            count.0 = 0;
                        }
                        println!("HandoffComplete pour {} : autorité prise", entity_id);
                    }
                }
            }
            _ => {}
        }
    }
}

pub fn check_handoff_stable_system(
    mut complete_queue: ResMut<HandoffCompleteQueue>,
    mut query: Query<(&mut Authority, &NetworkId, &mut HandoffTickCount)>,
) {
    for (authority, net_id, mut tick_count) in query.iter_mut() {
        if let Authority::PendingHandoff { .. } = *authority {
            tick_count.0 += 1;
            if tick_count.0 >= HANDOFF_STABILIZE_TICKS {
                complete_queue.0.push(HandoffCompleteTrigger {
                    entity_id: net_id.0,
                });
            }
        }
    }
}

pub fn finalize_handoff_system(
    mut queue: ResMut<HandoffCompleteQueue>,
    mut commands: Commands,
    query: Query<(Entity, &Authority, &NetworkId)>,
    socket: Res<InterShardSocket>,
) {
    for trigger in queue.0.drain(..) {
        for (entity, authority, net_id) in query.iter() {
            if net_id.0 == trigger.entity_id {
                if let Authority::PendingHandoff { target_shard_addr } = authority {
                    // Tag 0x24 : HandoffComplete
                    let mut buf = [0u8; 5];
                    buf[0] = 0x24;
                    buf[1..5].copy_from_slice(&net_id.0.to_le_bytes());
                    let _ = socket.0.send_to(&buf, *target_shard_addr);

                    println!("HandoffComplete envoyé pour {} -> despawn local", net_id.0);

                    commands.entity(entity).despawn();
                }
            }
        }
    }
}
