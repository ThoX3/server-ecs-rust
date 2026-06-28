use bevy::prelude::*;
use serde::{Deserialize, Serialize};
use shared::{Heartbeat, JoinRequest, WelcomeMessage};
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use uuid::Uuid;

mod resources;
use resources::{PlayerRegistry, ServerConfig, ServerNetwork};
use shared::logger::{error, info, warn};

// ==========================================
// 1. BEVY ECS COMPONENTS & RESOURCES
// ==========================================

/// Identifies an entity as a Player in the ECS world.
#[derive(Component)]
pub struct Player {
    pub id: String, // String UUID for database/persistence
}

/// The current movement inputs requested by the player (e.g. from pressing W A S D)
#[derive(Component, Serialize, Deserialize, Clone, Debug)]
pub struct PlayerInput {
    pub movement_x: f32,
    pub movement_y: f32,
}

/// A global shared resource holding the player count.
/// Wrapped in an AtomicUsize so the async Tokio heartbeat thread can read it without locking Bevy.
#[derive(Resource)]
pub struct PlayerCount(pub Arc<AtomicUsize>);

/// The Network ID (4 bytes) assigned to this client by the Gatekeeper.
#[derive(Component, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkId(pub u32);

/// Defines the server's authority over an entity.
/// `Owned`: The server calculates physics and logic for this entity.
/// `Ghost`: The server just watches this entity (from a neighboring shard's broadcast) and draws it.
#[derive(Component, PartialEq, Eq, Debug)]
pub enum Authority {
    Owned,
    Ghost,
}

/// A timer used during the "Warm-up" phase when a server first boots up.
#[derive(Resource)]
pub struct WarmupTimer(pub Timer);

// ==========================================
// 2. MAIN STARTUP LOGIC
// ==========================================

pub fn main() {
    shared::logger::init_logger("DedicatedServer");
    // Load config from environment variables (PORT, SHARDS, PARENT_SHARD, etc.)
    let config = ServerConfig::from_env();

    // Bind the primary UDP socket for Bevy to read/write packets
    let socket = StdUdpSocket::bind(format!("0.0.0.0:{}", config.port)).unwrap();
    socket.set_nonblocking(true).unwrap();

    // ----------------------------------------------------
    // WARM-UP PHASE (ScaleUp Synchronization)
    // ----------------------------------------------------
    // If the Orchestrator booted us with a PARENT_SHARD env var, we are part of a ScaleUp!
    // We cannot immediately take over. We must first act as a "Ghost" client to sync the parent's state.
    let warming_up = config.parent_shard.is_some();
    if warming_up {
        let parent = config.parent_shard.as_ref().unwrap();
        
        // Construct a `0x01 Subscribe` packet to the Broker for the parent's topic
        let mut sub_msg = [0u8; 37];
        sub_msg[0] = 0x01;
        sub_msg[1..5].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes()); // Dummy client ID so we don't conflict with real players
        let topic_bytes = parent.as_bytes();
        sub_msg[5..5 + topic_bytes.len()].copy_from_slice(topic_bytes);
        let _ = socket.send_to(&sub_msg, config.broker_addr);
        info!("Warming up: Subscribed to parent shard {}", parent);
    }

    // ----------------------------------------------------
    // REGISTER TOPIC AUTHORITY
    // ----------------------------------------------------
    let initial_shards = config.shards.read().unwrap().clone();
    for shard in &initial_shards {
        // Construct a `0x06 Claim Authority` packet.
        // This tells the Broker: "Route all player inputs for this topic to MY ip address!"
        let mut reg_msg = [0u8; 33];
        reg_msg[0] = 0x06;
        let topic_bytes = shard.as_bytes();
        reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
        let _ = socket.send_to(&reg_msg, config.broker_addr);
        info!("Registered shard authority for: {}", shard);

        // If we are NOT warming up, tell the Spatial Server we are ready immediately.
        if !warming_up {
            if let Some(id_str) = shard.split(':').nth(1) {
                if let Ok(shard_id) = id_str.parse::<u32>() {
                    let mut ready_msg = [0u8; 5];
                    ready_msg[0] = 0x14; // 0x14 Ready protocol
                    ready_msg[1..5].copy_from_slice(&shard_id.to_le_bytes());
                    let _ = socket.send_to(&ready_msg, config.broker_addr);
                    info!("Sent Ready signal for shard {}", shard_id);
                }
            }
        }
    }

    // Register our specific Server UUID as a topic as well, so the Orchestrator can route direct commands to us.
    let mut reg_id_msg = [0u8; 33];
    reg_id_msg[0] = 0x06;
    let id_bytes = config.id.as_bytes();
    reg_id_msg[1..1 + id_bytes.len()].copy_from_slice(id_bytes);
    let _ = socket.send_to(&reg_id_msg, config.broker_addr);
    info!("Registered direct authority for server ID: {}", config.id);

    // ----------------------------------------------------
    // HEARTBEAT BACKGROUND THREAD
    // ----------------------------------------------------
    let config_clone = config.clone();
    let player_count = Arc::new(AtomicUsize::new(0));
    let player_count_tokio = player_count.clone();

    // Spawn an isolated Tokio thread purely for sending heartbeats.
    // We do this so a lag spike in the Bevy ECS loop doesn't cause the Orchestrator to think we crashed!
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            heartbeat_task(config_clone, player_count_tokio).await;
        });
    });

    // Initialize the Warmup timer (2 seconds if warming up, 0 seconds if normal boot)
    let warmup_timer = if warming_up {
        WarmupTimer(Timer::new(Duration::from_millis(2000), TimerMode::Once))
    } else {
        WarmupTimer(Timer::new(Duration::from_millis(0), TimerMode::Once))
    };

    // ----------------------------------------------------
    // BEVY ECS APP BOOT
    // ----------------------------------------------------
    App::new()
        .add_plugins(MinimalPlugins.set(bevy::app::ScheduleRunnerPlugin::run_loop(
            Duration::from_secs_f64(1.0 / 60.0),
        ))) // Minimal because we have no graphics/renderer, cap at 60 tick/s
        .insert_resource(config)
        .insert_resource(PlayerRegistry::default()) // Maps network IDs to ECS Entities
        .insert_resource(ServerNetwork(socket))     // Wraps our UDP socket for Bevy systems
        .insert_resource(PlayerCount(player_count))
        .insert_resource(warmup_timer)
        // Add our systems in sequential order
        .add_systems(
            Update,
            (
                warmup_system,   // Checks if warm-up is complete
                handle_networks, // Reads incoming UDP packets
                move_players,    // Calculates game physics/logic
                broadcast_state, // Publishes ECS state to the Broker
            )
                .chain(),
        )
        .run();
}

// ==========================================
// 3. BEVY ECS SYSTEMS
// ==========================================

/// Monitors the warm-up phase. Once the timer finishes, it sends the `0x14 Ready` signal.
fn warmup_system(
    mut timer: ResMut<WarmupTimer>,
    time: Res<Time>,
    config: Res<ServerConfig>,
    network: Res<ServerNetwork>,
) {
    timer.0.tick(time.delta());
    
    if timer.0.just_finished() {
        info!("Warm-up complete. Emitting Ready signal for shards.");
        let current_shards = config.shards.read().unwrap().clone();
        for shard in &current_shards {
            if let Some(id_str) = shard.split(':').nth(1) {
                if let Ok(shard_id) = id_str.parse::<u32>() {
                    let mut ready_msg = [0u8; 5];
                    ready_msg[0] = 0x14; // 0x14 Ready
                    ready_msg[1..5].copy_from_slice(&shard_id.to_le_bytes());
                    let _ = network.0.send_to(&ready_msg, config.broker_addr);
                    info!("Sent Ready signal for shard {}", shard_id);
                }
            }
        }
    }
}

/// The main networking ingestion loop. 
/// Reads from the UDP socket and parses incoming protocols.
fn handle_networks(
    mut commands: Commands,
    network: Res<ServerNetwork>,
    mut registry: ResMut<PlayerRegistry>,
    player_count_res: Res<PlayerCount>,
    mut transforms: Query<(&mut Transform, &Authority)>,
    config: Res<ServerConfig>,
) {
    let mut buf = [0u8; 2048];
    // Drain the socket of all queued packets
    while let Ok((amt, _src)) = network.0.recv_from(&mut buf) {
        if amt < 1 { continue; }

        let tag = buf[0];
        
        // ----------------------------------------
        // INFRASTRUCTURE COMMANDS
        // ----------------------------------------
        if tag == 0x17 {
            // Graceful Shutdown (from Orchestrator)
            // The Orchestrator decided to merge this shard. It sends this to cleanly shut down the OS process.
            info!("Received 0x17 Shutdown command from Orchestrator. Shutting down gracefully...");
            std::process::exit(0);
        } else if tag == 0x18 {
            // AssignShards (Capacity Packing)
            // The Orchestrator noticed we are under-capacity (<128 players) and assigned us new shards!
            if amt < 5 { continue; }
            let count = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let mut offset = 5;
            for _ in 0..count {
                if offset + 4 > amt { break; }
                let shard_id = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                offset += 4;

                let shard_str = format!("shard:{}", shard_id);
                // Add the new shard to our tracked list
                config.shards.write().unwrap().push(shard_str.clone());

                // Immediately claim authority for the new topic
                let mut reg_msg = [0u8; 33];
                reg_msg[0] = 0x06;
                let topic_bytes = shard_str.as_bytes();
                reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
                let _ = network.0.send_to(&reg_msg, config.broker_addr);
                info!("Dynamically claimed authority for: {}", shard_str);

                // Tell the Spatial Server we are ready to take players
                let mut ready_msg = [0u8; 5];
                ready_msg[0] = 0x14; // 0x14 Ready
                ready_msg[1..5].copy_from_slice(&shard_id.to_le_bytes());
                let _ = network.0.send_to(&ready_msg, config.broker_addr);
                info!("Sent Ready signal for new shard {}", shard_id);
            }
            continue;
        } 
        
        // ----------------------------------------
        // GAMEPLAY SYNC & INPUT
        // ----------------------------------------
        else if tag == 0x04 {
            // 0x04 Broadcast (State sync from a parent shard or neighbor)
            // We only receive these if we subscribed to another shard (e.g. during warm-up)
            if amt < 5 { continue; }
            let count = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let mut offset = 5;
            for _ in 0..count {
                if offset + 12 > amt { break; }
                let client_id = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                let x = f32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
                let y = f32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap());
                offset += 12;

                if let Some(&player_entity) = registry.players.get(&client_id) {
                    // Update the ghost entity's transform
                    if let Ok((mut transform, _)) = transforms.get_mut(player_entity) {
                        transform.translation.x = x;
                        transform.translation.y = y;
                    }
                } else {
                    // We don't know this player yet! Spawn them as a Ghost entity.
                    let string_id = Uuid::new_v4().to_string();
                    let entity = commands
                        .spawn((
                            Player { id: string_id.clone() },
                            Transform::from_xyz(x, y, 0.0),
                            PlayerInput { movement_x: 0.0, movement_y: 0.0 },
                            NetworkId(client_id),
                            Authority::Ghost, // Mark as Ghost so we don't calculate physics for them!
                        ))
                        .id();
                    registry.players.insert(client_id, entity);
                    player_count_res.0.fetch_add(1, Ordering::Relaxed);
                }
            }
            continue;
        } else if tag == 0x05 {
            // 0x05 Routed Client Input (from Broker)
            if amt < 5 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let payload = &buf[5..amt];

            // If the payload is a 0x10 Position Update, it means this client is sending us Ghost data directly
            // (e.g., they crossed a border and are telling us where they are while we prepare to take over)
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
                        // Spawn them as a Ghost if we don't know them
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
                        player_count_res.0.fetch_add(1, Ordering::Relaxed);
                    }
                }
                continue; // Position Update handled
            }

            // Normal JSON PlayerInput payload parsing
            if let Some(&player_entity) = registry.players.get(&client_id) {
                if let Ok(input) = serde_json::from_slice::<PlayerInput>(payload) {
                    // Update the ECS component with the new requested movement vector
                    info!("Received PlayerInput from {}: {:?}", client_id, input);
                    if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                        entity_commands.insert(input);
                    }
                } else if let Ok(req) = serde_json::from_slice::<JoinRequest>(payload) {
                    // They sent a JoinRequest but are already in the registry (probably as a Ghost).
                    // Promote them to Owned!
                    info!("Player {} (already Ghost) sent a JoinRequest. Promoting to Owned!", req.username);
                    if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                        entity_commands.insert(Authority::Owned);
                    }
                }
            } else if let Ok(req) = serde_json::from_slice::<JoinRequest>(payload) {
                // New player joining the game! Spawn them with Authority::Owned.
                let string_id = Uuid::new_v4().to_string();
                let entity = commands
                    .spawn((
                        Player { id: string_id.clone() },
                        Transform::from_xyz(0.0, 0.0, 0.0),
                        PlayerInput { movement_x: 0.0, movement_y: 0.0 },
                        NetworkId(client_id),
                        Authority::Owned, // We own their physics!
                    ))
                    .id();

                registry.players.insert(client_id, entity);
                player_count_res.0.fetch_add(1, Ordering::Relaxed);
                info!("[{}] Player {} joined shard", config.id, req.username);
            }
        } else if tag == 0x07 {
            // 0x07 Disconnect
            if amt < 5 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            if let Some(entity) = registry.players.remove(&client_id) {
                commands.entity(entity).despawn();
                player_count_res.0.fetch_sub(1, Ordering::Relaxed);
                info!("[{}] Client {} disconnected from shard", config.id, client_id);
            }
        } else if tag == 0x12 {
            // 0x12 AuthorityChange (from Spatial Server)
            // A player crossed a border into or out of our shard!
            if amt < 13 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let old_shard = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let new_shard = u32::from_le_bytes(buf[9..13].try_into().unwrap());

            // Check if we host the old shard and/or the new shard
            let hosts_old = config.shards.read().unwrap().contains(&format!("shard:{}", old_shard));
            let hosts_new = config.shards.read().unwrap().contains(&format!("shard:{}", new_shard));

            if let Some(&player_entity) = registry.players.get(&client_id) {
                if let Ok(mut entity_commands) = commands.get_entity(player_entity) {
                    if hosts_old && !hosts_new {
                        // The player left our shard. Demote them from Owned to Ghost!
                        // (They might still be near the border, so we keep tracking them but stop moving them).
                        entity_commands.insert(Authority::Ghost);
                        info!("[{}] Client {} demoted to Ghost on Shard {}", config.id, client_id, old_shard);
                    } else if hosts_new {
                        // The player entered our shard. Promote them from Ghost to Owned!
                        // We take over physics calculations immediately.
                        entity_commands.insert(Authority::Owned);
                        info!("[{}] Client {} promoted to Owned on Shard {}", config.id, client_id, new_shard);
                    }
                }
            }
        }
    }
}

/// Applies movement vectors to entity positions.
/// Notice how it explicitly skips entities with `Authority::Ghost`!
fn move_players(
    time: Res<Time>,
    config: Res<ServerConfig>,
    mut query: Query<(&PlayerInput, &mut Transform, &Authority, &NetworkId), With<Player>>,
) {
    for (input, mut transform, authority, net_id) in query.iter_mut() {
        if !matches!(authority, Authority::Ghost { .. }) {
            let speed = 5.0;
            // Standard frame-independent movement logic
            transform.translation.x += input.movement_x * speed * time.delta_secs();
            transform.translation.y += input.movement_y * speed * time.delta_secs();
            
            if input.movement_x != 0.0 || input.movement_y != 0.0 {
                info!("[{}] Moving player {} to ({}, {})", config.id, net_id.0, transform.translation.x, transform.translation.y);
            }
        }
    }
}

/// Packages the entire ECS world state and Publishes it to the Broker.
fn broadcast_state(
    query: Query<(&NetworkId, &Transform), With<Player>>,
    network: Res<ServerNetwork>,
    config: Res<ServerConfig>,
) {
    if query.is_empty() {
        return;
    }

    // Prepare a packed binary game data payload
    // Format: Number of players (1 byte) | [client_id (4 bytes) | x (4 bytes) | y (4 bytes)] * N
    let mut payload = vec![];
    let mut count = 0u8;
    for (net_id, transform) in query.iter() {
        if count >= 255 { break; }
        count += 1;
        payload.extend_from_slice(&net_id.0.to_le_bytes());
        payload.extend_from_slice(&transform.translation.x.to_le_bytes());
        payload.extend_from_slice(&transform.translation.y.to_le_bytes());
    }

    let mut game_data = vec![count];
    game_data.extend(payload);

    // If this server manages 3 different shards, it must broadcast 3 times
    let current_shards = config.shards.read().unwrap().clone();
    for shard in &current_shards {
        let mut msg = vec![0x03]; // 0x03 Publish
        let mut topic = [0u8; 32];
        let bytes = shard.as_bytes();
        topic[..bytes.len()].copy_from_slice(bytes);

        msg.extend_from_slice(&topic);
        let payload_len = game_data.len() as u16;
        msg.extend_from_slice(&payload_len.to_le_bytes());
        msg.extend_from_slice(&game_data);

        // Send payload to the PubSub Broker
        let _ = network.0.send_to(&msg, config.broker_addr);
    }
}

// ==========================================
// 4. BACKGROUND TASKS
// ==========================================

/// Loops infinitely to tell the Orchestrator "I am alive and here is my capacity!"
async fn heartbeat_task(config: ServerConfig, current_players: Arc<AtomicUsize>) {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let mut ticker = interval(Duration::from_secs(2));

    loop {
        ticker.tick().await;
        
        // Read the atomic player count thread-safely
        let players = current_players.load(Ordering::Relaxed);
        let status = if players >= config.max_players {
            "full".to_string()
        } else {
            "available".to_string()
        };

        // Extract raw integer shard IDs from the string format ("shard:5" -> 5)
        let shards: Vec<u32> = config.shards.read().unwrap().iter().filter_map(|s| {
            s.split(':').nth(1).and_then(|id| id.parse::<u32>().ok())
        }).collect();

        let hb = Heartbeat {
            id: config.id.clone(),
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            player_count: players,
            max_players: config.max_players,
            shards,
            status,
        };

        // Serialize the JSON struct and fire it to the Orchestrator
        if let Ok(bytes) = serde_json::to_vec(&hb) {
            let _ = socket.send_to(&bytes, config.orchestrator_addr).await;
        }
    }
}
