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

/// Compteur de ticks, à double usage selon le rôle de l'entité :
/// - Côté shard SOURCE, sur une entité `PendingHandoff` : nombre de ticks écoulés
///   depuis le début du transfert (proxy du nombre de `GhostUpdate` envoyés au
///   shard destination). Utilisé par `check_handoff_stable_system` pour décider
///   quand envoyer `HandoffComplete`.
/// - Côté shard DESTINATION, sur une entité `Ghost` : nombre de `GhostUpdate`
///   effectivement reçus depuis le shard source. Remis à zéro à `HandoffComplete`.
///
/// Ce composant est ajouté dès le spawn initial de TOUTE entité `Player` afin que
/// `check_handoff_stable_system` (dont la query le requiert) capture aussi
/// l'entité côté source pendant `PendingHandoff`.
#[derive(Component, Default)]
pub struct HandoffTickCount(pub u32);

/// Nombre de ticks consécutifs en PendingHandoff avant de considérer le transfert
/// terminé et d'envoyer HandoffComplete.
pub const HANDOFF_STABILIZE_TICKS: u32 = 5;

/// Émis par le service spatial quand une entité entre dans la marge d'une frontière
/// inter-shard et doit être transférée.
pub struct CrossingAlert {
    pub entity_id: u32,
    pub target_shard: SocketAddr,
}

#[derive(Resource, Default)]
pub struct CrossingAlertQueue(pub Vec<CrossingAlert>);

/// Émis (côté shard source) quand un ghost destination est jugé stable et que
/// l'autorité complète doit lui être transférée.
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
                initiate_handoff_system, // Étape 1 : envoie HandoffRequest (0x20)
                handle_inter_shard_messages, // Étapes 0x20-0x24 : accept/reject/ghost/complete
                sync_ghosts_system,      // Étape continue : GhostUpdate (0x23)
                check_handoff_stable_system, // Détecte un ghost stable -> remplit HandoffCompleteQueue
                finalize_handoff_system, // Étape finale : HandoffComplete (0x24) + despawn source
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
        // Un Ghost ne simule pas localement : sa position vient des GhostUpdate du voisin.
        // Owned et PendingHandoff continuent d'être simulés normalement par ce shard
        // jusqu'à ce que l'autorité soit effectivement transférée (HandoffComplete).
        if !matches!(authority, Authority::Ghost { .. }) {
            let speed = 5.0;
            transform.translation.x += input.movement_x * speed * time.delta_secs();
            transform.translation.y += input.movement_y * speed * time.delta_secs();
        }
    }
}

/// Étape 1 : le service spatial a détecté une entité approchant d'une frontière
/// (`CrossingAlert`). On envoie un `HandoffRequest` (0x20) au shard voisin et on
/// passe l'entité en `PendingHandoff`. Elle continue d'être simulée normalement
/// par ce shard jusqu'à `HandoffComplete`.
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

                // Tag 0x20 : HandoffRequest
                // entity_id: u32 (4) | pos: Vec2 (8) | vel: Vec2 (8) | state: [u8; 64]
                // = 1 + 4 + 8 + 8 + 64 = 85 octets
                let mut buf = [0u8; 85];
                buf[0] = 0x20;
                buf[1..5].copy_from_slice(&net_id.0.to_le_bytes());
                buf[5..9].copy_from_slice(&transform.translation.x.to_le_bytes());
                buf[9..13].copy_from_slice(&transform.translation.y.to_le_bytes());
                buf[13..17].copy_from_slice(&input.movement_x.to_le_bytes());
                buf[17..21].copy_from_slice(&input.movement_y.to_le_bytes());
                // buf[21..85] = state, laissé à 0 (non utilisé pour ce TP)

                let _ = socket.0.send_to(&buf, alert.target_shard);
            }
        }
    }
}

/// Étape continue : tant qu'une entité est `PendingHandoff` côté shard source,
/// on publie son état au shard destination via `GhostUpdate` (0x23) à chaque tick,
/// pour que sa copie `Ghost` reste synchronisée.
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
                // HandoffRequest reçu : ce shard devient la destination potentielle.
                // On décide accept/reject (ici : toujours accepter, sauf si l'entité
                // existe déjà localement sous une autre forme).
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
                        // Spawn de l'entité en état Ghost : copie lecture seule,
                        // simulée par le shard source (`src`) jusqu'à HandoffComplete.
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
                // spawn le Ghost. L'entité reste PendingHandoff ; on continue
                // d'envoyer des GhostUpdate jusqu'à ce qu'elle soit jugée stable
                // (voir check_handoff_stable_system), puis HandoffComplete.
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

/// Détecte, côté shard SOURCE, qu'une entité `PendingHandoff` a été transférée
/// depuis assez de ticks pour être considérée stable côté destination, et
/// programme l'envoi de `HandoffComplete` via `HandoffCompleteQueue`.
///
/// `HandoffTickCount` est ajouté dès le spawn initial de toute entité `Player`
/// (voir `handle_networks`), ce qui garantit que cette query capture bien
/// l'entité source pendant sa phase `PendingHandoff` — contrairement à une
/// version où ce composant n'existerait que sur les `Ghost` (côté destination),
/// qui exclurait systématiquement l'entité source de cette query.
///
/// Le nombre de ticks écoulés en `PendingHandoff` sert de proxy simple au nombre
/// de `GhostUpdate` envoyés au shard destination.
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

/// Étape finale : envoie `HandoffComplete` (0x24) au shard destination et
/// despawn la copie locale (le shard destination devient désormais Owned).
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
