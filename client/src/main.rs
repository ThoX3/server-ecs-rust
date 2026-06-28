use bevy::app::AppExit;
use bevy::prelude::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use shared::JoinRequest;
use std::net::UdpSocket;

#[derive(Component)]
struct Player {
    id: u32,
    last_seen: f32,
}

#[derive(Component)]
struct TargetPosition(Vec2);

#[derive(Component)]
struct LocalPlayer;

#[derive(Component)]
struct ServerAuthGhost;

#[derive(Serialize)]
struct LoginRequest {
    username: String,
    password: Option<String>,
}

#[derive(Deserialize)]
struct LoginResponse {
    player_id: String,
    broker: shared::ServerInfo,
}

#[derive(Resource)]
struct NetworkConfig {
    socket: UdpSocket,
    client_id: u32,
    broker_addr: String,
}

#[derive(Resource)]
struct ClientState {
    shard_authority: Option<u32>,
    joined: bool,
    spawned_on_server: bool,
    known_players: std::collections::HashSet<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PlayerInput {
    pub x: f32,
    pub y: f32,
}

fn main() {
    let client_id: u32 = rand::rng().random_range(1000..9999);
    let username = format!("Player_{}", client_id);
    
    info!("Starting client '{}'", username);
    
    // Login to Gatekeeper
    info!("Logging in via Gatekeeper at 127.0.0.1:3000...");
    let http_client = reqwest::blocking::Client::new();
    let login_req = LoginRequest {
        username: username.clone(),
        password: Some("1234".to_string()),
    };
    
    let res = http_client.post("http://127.0.0.1:3000/login")
        .json(&login_req)
        .send()
        .expect("Failed to connect to Gatekeeper");
        
    let login_res: LoginResponse = res.json().expect("Invalid Gatekeeper response");
    let broker_addr = format!("{}:{}", login_res.broker.ip, login_res.broker.port);
    info!("Gatekeeper assigned Broker at {}", broker_addr);

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();

    // Subscribe to spatial_updates
    let mut sub_msg = [0u8; 37];
    sub_msg[0] = 0x01;
    sub_msg[1..5].copy_from_slice(&client_id.to_le_bytes());
    let topic_bytes = b"spatial_updates";
    sub_msg[5..5 + topic_bytes.len()].copy_from_slice(topic_bytes);
    socket.send_to(&sub_msg, &broker_addr).unwrap();

    // Trigger Spatial Server to see us and assign a shard
    let mut pos_msg = [0u8; 14];
    pos_msg[0] = 0x05;
    pos_msg[1..5].copy_from_slice(&client_id.to_le_bytes());
    pos_msg[5] = 0x10;
    pos_msg[6..10].copy_from_slice(&0.0f32.to_le_bytes());
    pos_msg[10..14].copy_from_slice(&0.0f32.to_le_bytes());
    socket.send_to(&pos_msg, &broker_addr).unwrap();

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: format!("Client {}", client_id),
                ..default()
            }),
            ..default()
        }))
        .insert_resource(NetworkConfig { socket, client_id, broker_addr })
        .insert_resource(ClientState {
            shard_authority: None,
            joined: false,
            spawned_on_server: false,
            known_players: std::collections::HashSet::new(),
        })
        .add_systems(Startup, setup)
        .add_systems(Update, (handle_network, handle_input, draw_grid, camera_follow, cleanup_disconnected_players, interpolate_positions))
        .run();
}

fn interpolate_positions(
    time: Res<Time>,
    mut query: Query<(&mut Transform, &TargetPosition), Without<LocalPlayer>>,
) {
    let dt = time.delta_secs();
    let lerp_factor = (dt * 15.0).min(1.0);
    for (mut transform, target) in query.iter_mut() {
        let current = transform.translation.truncate();
        let next = current.lerp(target.0, lerp_factor);
        transform.translation.x = next.x;
        transform.translation.y = next.y;
    }
}

fn cleanup_disconnected_players(
    mut players: Query<(&Player, &mut MeshMaterial2d<ColorMaterial>)>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    time: Res<Time>,
) {
    let now = time.elapsed_secs();
    for (player, mut material_handle) in players.iter_mut() {
        if now - player.last_seen > 2.0 {
            // Player hasn't been seen for 2 seconds, turn them grey!
            material_handle.0 = materials.add(Color::srgb(0.5, 0.5, 0.5));
        }
    }
}

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
    
    // UI Legend
    commands.spawn((
        Node {
            position_type: PositionType::Absolute,
            top: Val::Px(10.0),
            left: Val::Px(10.0),
            ..default()
        },
    )).with_children(|parent| {
        parent.spawn((
            Text::new("Green: Local Player\nBlue: Server Auth\nRed: Others\nGrey: Disconnected"),
            TextFont {
                font_size: 20.0,
                ..default()
            },
            TextColor(Color::WHITE),
        ));
    });
}

impl Drop for NetworkConfig {
    fn drop(&mut self) {
        info!("Client closing, sending 0x07 Disconnect to Broker...");
        let mut msg = [0u8; 5];
        msg[0] = 0x07;
        msg[1..5].copy_from_slice(&self.client_id.to_le_bytes());
        let _ = self.socket.send_to(&msg, &self.broker_addr);
    }
}

fn handle_network(
    mut commands: Commands,
    network: Res<NetworkConfig>,
    mut state: ResMut<ClientState>,
    mut players: Query<(Entity, &mut Player, &mut TargetPosition)>,
    mut local_transforms: Query<&mut Transform, With<LocalPlayer>>,
    mut ghosts: Query<&mut TargetPosition, (With<ServerAuthGhost>, Without<Player>)>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    time: Res<Time>,
) {
    let mut buf = [0u8; 1024];
    while let Ok((amt, _src)) = network.socket.recv_from(&mut buf) {
        if amt < 1 {
            continue;
        }

        let tag = buf[0];

        if tag == 0x04 {
            // Broadcast
            if amt < 4 {
                warn!("0x04 packet too small");
                continue;
            }
            let _payload_len = u16::from_le_bytes(buf[1..3].try_into().unwrap());
            let count = buf[3];
            let mut offset = 4;
            
            // info!("Received 0x04 Broadcast with player count: {}", count);

            for i in 0..count {
                if offset + 12 > amt {
                    warn!("0x04 packet cut off before player {}", i);
                    break;
                }
                let p_id = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
                let x = f32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
                let y = f32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap());
                offset += 12;
                
                // info!("Parsed player {} at ({}, {})", p_id, x, y);

                let mut found_in_ecs = false;
                for (_, mut player, mut target_pos) in players.iter_mut() {
                    if player.id == p_id {
                        if p_id != network.client_id {
                            target_pos.0 = Vec2::new(x, y);
                        } else {
                            for mut ghost_target in ghosts.iter_mut() {
                                ghost_target.0 = Vec2::new(x, y);
                            }
                            // Client-side prediction correction!
                            // If the server's authoritative position diverges from our local prediction
                            // by more than a threshold, snap back to the server's position.
                            for mut local_transform in local_transforms.iter_mut() {
                                let server_pos = Vec2::new(x, y);
                                let local_pos = local_transform.translation.truncate();
                                if server_pos.distance(local_pos) > 100.0 {
                                    local_transform.translation.x = server_pos.x;
                                    local_transform.translation.y = server_pos.y;
                                    warn!("Server correction applied! Snapped back to {:?}", server_pos);
                                }
                            }
                        }
                        player.last_seen = time.elapsed_secs();
                        found_in_ecs = true;
                    }
                }

                // If we didn't find them in ECS AND we haven't already queued a spawn for them
                if !found_in_ecs && !state.known_players.contains(&p_id) {
                    state.known_players.insert(p_id);
                    info!("Player {} not found in ECS, spawning them now!", p_id);
                    // Spawn new player sprite
                    let color = if p_id == network.client_id {
                        state.spawned_on_server = true;
                        info!("It's us! We spawned on the server successfully.");
                        Color::srgb(0.0, 1.0, 0.0) // Green for us
                    } else {
                        Color::srgb(1.0, 0.0, 0.0) // Red for others
                    };

                    let mut cmd = commands.spawn((
                        Mesh2d(meshes.add(Rectangle::new(30.0, 30.0))),
                        MeshMaterial2d(materials.add(color)),
                        Transform::from_xyz(x, y, 0.0),
                        TargetPosition(Vec2::new(x, y)),
                        Player { id: p_id, last_seen: time.elapsed_secs() },
                    ));

                    if p_id == network.client_id {
                        cmd.insert(LocalPlayer);

                        commands.spawn((
                            Mesh2d(meshes.add(Rectangle::new(30.0, 30.0))),
                            MeshMaterial2d(materials.add(Color::srgba(0.0, 0.0, 1.0, 0.5))),
                            Transform::from_xyz(x, y, 1.0),
                            TargetPosition(Vec2::new(x, y)),
                            ServerAuthGhost,
                        ));
                    }
                }
                
                // Echo our position back to spatial server so it knows where we are
                if p_id == network.client_id && state.spawned_on_server {
                    let mut pos_msg = [0u8; 14];
                    pos_msg[0] = 0x05;
                    pos_msg[1..5].copy_from_slice(&network.client_id.to_le_bytes());
                    pos_msg[5] = 0x10;
                    pos_msg[6..10].copy_from_slice(&x.to_le_bytes());
                    pos_msg[10..14].copy_from_slice(&y.to_le_bytes());
                    let _ = network.socket.send_to(&pos_msg, &network.broker_addr);
                }
            }
        } else if tag == 0x11 {
            let count = buf[5];
            info!("Entered ghost zone! Can see {} shards.", count);
        } else if tag == 0x12 {
            if amt < 13 {
                continue;
            }
            let target_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            if target_client != network.client_id {
                continue;
            }

            let old_shard = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let new_shard = u32::from_le_bytes(buf[9..13].try_into().unwrap());
            info!(
                "Authority changed from Shard {} to Shard {}",
                old_shard, new_shard
            );

            state.shard_authority = Some(new_shard);
            state.joined = true;
        }
    }
}

fn handle_input(
    keyboard: Res<ButtonInput<KeyCode>>,
    network: Res<NetworkConfig>,
    state: Res<ClientState>,
    mut last_log_time: Local<f32>,
    mut last_input_was_zero: Local<bool>,
    time: Res<Time>,
    mut player_query: Query<&mut Transform, With<LocalPlayer>>,
) {
    let now = time.elapsed_secs();
    let should_log = now - *last_log_time > 1.0;
    if should_log {
        *last_log_time = now;
        info!("handle_input: joined={}, spawned={}", state.joined, state.spawned_on_server);
    }

    if !state.joined {
        if should_log {
            info!("Not joined! Sending dummy position ping to Spatial Server...");
        }
        // We haven't joined yet. Continuously send the dummy position to Spatial Server
        // so it realizes we exist and assigns us a shard!
        let mut pos_msg = [0u8; 14];
        pos_msg[0] = 0x05;
        pos_msg[1..5].copy_from_slice(&network.client_id.to_le_bytes());
        pos_msg[5] = 0x10;
        pos_msg[6..10].copy_from_slice(&0.0f32.to_le_bytes());
        pos_msg[10..14].copy_from_slice(&0.0f32.to_le_bytes());
        let _ = network.socket.send_to(&pos_msg, &network.broker_addr);
        return;
    }

    if !state.spawned_on_server {
        if should_log {
            info!("Joined but not spawned! Sending JoinRequest to Broker...");
        }
        // We have a shard, but haven't appeared in its Broadcasts yet.
        // Resend JoinRequest!
        let req = JoinRequest {
            username: format!("Player_{}", network.client_id),
        };
        let payload = serde_json::to_vec(&req).unwrap();
        let mut join_msg = vec![0x05];
        join_msg.extend_from_slice(&network.client_id.to_le_bytes());
        join_msg.extend_from_slice(&payload);
        let _ = network.socket.send_to(&join_msg, &network.broker_addr);
        return;
    }

    let mut dir_x: f32 = 0.0;
    let mut dir_y: f32 = 0.0;
    let speed = 300.0; // 300 units per second

    if keyboard.pressed(KeyCode::KeyW) { dir_y += 1.0; }
    if keyboard.pressed(KeyCode::KeyS) { dir_y -= 1.0; }
    if keyboard.pressed(KeyCode::KeyA) { dir_x -= 1.0; }
    if keyboard.pressed(KeyCode::KeyD) { dir_x += 1.0; }

    // Normalize direction so diagonal movement isn't faster
    if dir_x != 0.0 || dir_y != 0.0 {
        let len = (dir_x * dir_x + dir_y * dir_y).sqrt();
        dir_x /= len;
        dir_y /= len;
    }
    
    for mut local_transform in player_query.iter_mut() {
        local_transform.translation.x += dir_x * speed * time.delta_secs();
        local_transform.translation.y += dir_y * speed * time.delta_secs();

        let is_zero = dir_x == 0.0 && dir_y == 0.0;

        if is_zero && *last_input_was_zero {
            continue; // Don't waste bandwidth when idle
        }

        *last_input_was_zero = is_zero;

        let input = PlayerInput {
            x: local_transform.translation.x,
            y: local_transform.translation.y,
        };
        let payload = serde_json::to_vec(&input).unwrap();

        let mut msg = vec![0x05];
        msg.extend_from_slice(&network.client_id.to_le_bytes());
        msg.extend_from_slice(&payload);
        let _ = network.socket.send_to(&msg, &network.broker_addr);
    }
}

fn draw_grid(mut gizmos: Gizmos) {
    // Draw a simple 2D grid
    for x in -10..=10 {
        let pos_x = x as f32 * 50.0;
        gizmos.line_2d(Vec2::new(pos_x, -500.0), Vec2::new(pos_x, 500.0), Color::srgb(0.3, 0.3, 0.3));
    }
    for y in -10..=10 {
        let pos_y = y as f32 * 50.0;
        gizmos.line_2d(Vec2::new(-500.0, pos_y), Vec2::new(500.0, pos_y), Color::srgb(0.3, 0.3, 0.3));
    }
    
    // Draw the quadrant boundaries
    gizmos.line_2d(Vec2::new(0.0, -500.0), Vec2::new(0.0, 500.0), Color::srgb(0.8, 0.8, 0.0));
    gizmos.line_2d(Vec2::new(-500.0, 0.0), Vec2::new(500.0, 0.0), Color::srgb(0.8, 0.8, 0.0));
}

fn camera_follow(
    player_q: Query<&Transform, With<LocalPlayer>>,
    mut camera_q: Query<&mut Transform, (With<Camera>, Without<LocalPlayer>)>,
) {
    for player_transform in player_q.iter() {
        for mut camera_transform in camera_q.iter_mut() {
            let target = player_transform.translation;
            camera_transform.translation.x += (target.x - camera_transform.translation.x) * 0.1;
            camera_transform.translation.y += (target.y - camera_transform.translation.y) * 0.1;
        }
    }
}
