use bevy::prelude::*;
use game_sockets::protocols::UdpBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
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
        .add_event::<CrossingAlert>()
        .add_systems(
            Update,
            (
                handle_networks,
                system_movement,
                system_publish_to_broker,
                handle_crossing_alerts,
            ),
        )
        .run();
}

fn handle_networks(
    mut commands: Commands,
    mut network: ResMut<ServerNetwork>,
    mut registry: ResMut<PlayerRegistry>,
    count: Res<PlayerCount>,
    mut query: Query<(&Player, &mut Transform, &mut EntityAuthority)>,
) {
    // Handle incoming network events.
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
                if data.is_empty() {
                    continue;
                }
                let tag = data[0];

                // -------------------------------------------------------------
                // PROTOCOLE INTER-SHARDS (Partie 3)
                // -------------------------------------------------------------
                match tag {
                    0x20 => {
                        if data.len() >= 1 + 36 + 8 + 8 + 64 {
                            let mut offset = 1;

                            let id_bytes = &data[offset..offset + 36];
                            let entity_id = String::from_utf8_lossy(id_bytes).into_owned();
                            offset += 36;

                            let x =
                                f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                            let y = f32::from_le_bytes(
                                data[offset + 4..offset + 8].try_into().unwrap(),
                            );
                            let pos = Vec2::new(x, y);
                            offset += 8;

                            let _vel_x =
                                f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                            let _vel_y = f32::from_le_bytes(
                                data[offset + 4..offset + 8].try_into().unwrap(),
                            );
                            offset += 8;

                            let mut state = [0u8; 64];
                            state.copy_from_slice(&data[offset..offset + 64]);

                            let player_exists = query.iter().any(|(p, _, _)| p.id == entity_id);

                            if !player_exists {
                                commands.spawn((
                                    Player {
                                        id: entity_id.clone(),
                                    },
                                    Transform::from_translation(pos.extend(0.0)),
                                    EntityAuthority {
                                        state: AuthorityState::Ghost, // Il commence comme GHOST !
                                        target_shard_id: None,
                                    },
                                ));
                                println!(
                                    "Entité {} spawnée en mode GHOST suite à HandoffRequest",
                                    entity_id
                                );
                            }

                            let mut response = Vec::new();
                            response.push(0x21); // Tag
                            response.extend_from_slice(entity_id.as_bytes());

                            let _ =
                                network
                                    .0
                                    .send(&connection, &stream, bytes::Bytes::from(response));
                        }
                    }
                    0x21 => {
                        if data.len() >= 37 {
                            let entity_id = String::from_utf8_lossy(&data[1..37]).into_owned();

                            for (player, _, mut authority) in query.iter_mut() {
                                if player.id == entity_id
                                    && authority.state == AuthorityState::Owned
                                {
                                    authority.state = AuthorityState::PendingHandoff;
                                    println!(
                                        "Le shard voisin a accepté le transfert. {} est maintenant PendingHandoff",
                                        entity_id
                                    );
                                }
                            }
                        }
                    }
                    0x22 => {
                        if data.len() >= 37 {
                            let entity_id = String::from_utf8_lossy(&data[1..37]).into_owned();

                            for (player, mut transform, mut authority) in query.iter_mut() {
                                if player.id == entity_id {
                                    authority.state = AuthorityState::Owned;
                                    authority.target_shard_id = None;

                                    println!(
                                        "Handoff REJETÉ pour {}. L'entité rebondit.",
                                        entity_id
                                    );
                                }
                            }
                        }
                    }
                    0x23 => {
                        if data.len() >= 1 + 36 + 8 {
                            let entity_id = String::from_utf8_lossy(&data[1..37]).into_owned();

                            let x = f32::from_le_bytes(data[37..41].try_into().unwrap());
                            let y = f32::from_le_bytes(data[41..45].try_into().unwrap());
                            let new_pos = Vec2::new(x, y);

                            for (player, mut transform, authority) in query.iter_mut() {
                                if player.id == entity_id
                                    && authority.state == AuthorityState::Ghost
                                {
                                    transform.translation = new_pos.extend(0.0);
                                }
                            }
                        }
                    }
                    0x24 => {
                        if data.len() >= 37 {
                            let entity_id = String::from_utf8_lossy(&data[1..37]).into_owned();

                            for (player, _, mut authority) in query.iter_mut() {
                                if player.id == entity_id
                                    && authority.state == AuthorityState::Ghost
                                {
                                    authority.state = AuthorityState::Owned;
                                    authority.target_shard_id = None;
                                    println!(
                                        "Handoff terminé ! L'entité {} passe de GHOST à OWNED.",
                                        player.id
                                    );
                                }
                            }
                        }
                    }

                    // -------------------------------------------------------------
                    // PROTOCOLE CLIENT (Fallback ou format par défaut)
                    // -------------------------------------------------------------
                    _ => {
                        if let Ok(req) = serde_json::from_slice::<JoinRequest>(&data) {
                            if !registry.players.contains_key(&connection) {
                                let player_id = Uuid::new_v4().to_string();
                                let entity = commands
                                    .spawn((
                                        Player {
                                            id: player_id.clone(),
                                        },
                                        Transform::default(),
                                        EntityAuthority {
                                            state: AuthorityState::Owned,
                                            target_shard: None,
                                        },
                                    ))
                                    .id();
                                registry.players.insert(connection, entity);

                                count.0.fetch_add(1, Ordering::Relaxed);
                                println!("Player {} joined", req.username);

                                let resp = WelcomeMessage { player_id };
                                if let Ok(bytes) = serde_json::to_vec(&resp) {
                                    let _ = network.0.send(
                                        &connection,
                                        &stream,
                                        bytes::Bytes::from(bytes),
                                    );
                                }
                            }
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

fn system_movement(
    _time: Res<Time>,
    mut query: Query<(&Player, &mut Transform, &EntityAuthority)>,
) {
    for (player, transform, authority) in query.iter_mut() {
        if authority.state == AuthorityState::Ghost {
            continue;
        }

        // --- Logique de mouvement / physique normale ---
        if authority.state == AuthorityState::PendingHandoff {
            let mut update_packet = Vec::new();
            update_packet.push(0x23); // Tag GhostUpdate
            update_packet.extend_from_slice(player.id.as_bytes());
            update_packet.extend_from_slice(&transform.translation.x.to_le_bytes());
            update_packet.extend_from_slice(&transform.translation.y.to_le_bytes());

            // TODO: Récupérer la connexion réseau du shard cible via authority.target_shard
        }
    }
}

fn system_publish_to_broker(
    network: Res<ServerNetwork>,
    config: Res<ServerConfig>,
    query: Query<(&Player, &Transform, &EntityAuthority)>,
) {
    let mut payload = Vec::new();

    for (player, transform, _authority) in query.iter() {
        let entity_data = (player.id.clone(), transform.translation.truncate());
        if let Ok(data) = serde_json::to_vec(&entity_data) {
            payload.extend(data);
        }
    }

    if payload.is_empty() {
        return;
    }

    let mut packet = Vec::new();
    packet.push(0x03);

    let mut topic_bytes = [0u8; 32];
    let shard_topic = format!("shard:{}", config.id);
    let src = shard_topic.as_bytes();
    topic_bytes[..src.len()].copy_from_slice(src);
    packet.extend_from_slice(&topic_bytes);

    let len = payload.len() as u16;
    packet.extend_from_slice(&len.to_le_bytes());

    packet.extend(payload);

    // Envoi au Broker
    // network.0.send(&broker_connection, &stream, bytes::Bytes::from(packet));
}

fn handle_crossing_alerts(
    mut alerts: EventReader<CrossingAlert>,
    mut query: Query<(&Player, &Transform, &mut EntityAuthority)>,
    network: Res<ServerNetwork>, // Pour envoyer le message 0x20 au shard voisin
) {
    for alert in alerts.read() {
        for (player, transform, mut authority) in query.iter_mut() {
            if player.id == alert.entity_id && authority.state == AuthorityState::Owned {
                authority.state = AuthorityState::PendingHandoff;
                authority.target_shard_id = Some(alert.target_shard_id);

                let _packet_0x20 =
                    build_handoff_request(player.id.clone(), transform.translation.truncate());

                println!(
                    "Handoff initié pour le joueur {} vers Shard {}",
                    player.id, alert.target_shard_id
                );
            }
        }
    }
}

fn build_handoff_request(entity_id: String, pos: Vec2) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x20); // Tag HandoffRequest
    packet.extend_from_slice(entity_id.as_bytes()); // 36 bytes (UUID)
    packet.extend_from_slice(&pos.x.to_le_bytes()); // 4 bytes
    packet.extend_from_slice(&pos.y.to_le_bytes()); // 4 bytes

    packet.extend_from_slice(&0.0f32.to_le_bytes()); // 4 bytes
    packet.extend_from_slice(&0.0f32.to_le_bytes()); // 4 bytes

    let state = [0u8; 64];
    packet.extend_from_slice(&state);

    packet
}
