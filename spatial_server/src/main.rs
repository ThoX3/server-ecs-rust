use bevy::math::{Rect, Vec2};
use shared::logger::info;
use shared::quadtree::{PositionUpdate, QuadTree, SpatialAction, SpatialService};
use std::env;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::logger::init_logger("SpatialServer");
    info!("Starting...");

    let port = env::var("PORT").unwrap_or_else(|_| "9001".to_string());
    let broker_addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    let orch_addr = env::var("ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", port)).await?);
    info!("Listening on port {}", port);
    info!("Broker address: {}", broker_addr);
    info!("Orchestrator address: {}", orch_addr);
    // Initialisation du QuadTree
    // Monde de -1000 à 1000
    let bounds = Rect::from_center_size(Vec2::ZERO, Vec2::new(2000.0, 2000.0));
    let mut quadtree = QuadTree::new(bounds, 0, 4);
    let mut next_id = 1; // Since root is 0, next is 1

    let mut spatial_service = SpatialService::new(quadtree, 50.0, next_id); // Margin de 50.0

    // pending_ready: child_shard_id -> parent_shard_id
    let mut pending_ready = std::collections::HashMap::<u32, u32>::new();
    // parent_to_children: parent_shard_id -> [child_shard_ids that are not yet ready]
    let mut parent_to_children = std::collections::HashMap::<u32, Vec<u32>>::new();

    // Register as the authority for "spatial_updates"
    let mut reg_msg = [0u8; 33];
    reg_msg[0] = 0x06;
    let topic_bytes = b"spatial_updates";
    reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
    socket.send_to(&reg_msg, &broker_addr).await?;
    info!("Registered spatial_updates authority with Broker.");

    let mut buf = [0u8; 1024];

    loop {
        let (len, _src) = socket.recv_from(&mut buf).await?;
        if len < 1 {
            continue;
        }

        let tag = buf[0];
        if tag == 0x05 {
            // Routed client input: 0x05 | client_id(4) | sub_tag(1) | payload...
            if len < 6 {
                continue;
            }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let sub_tag = buf[5];

            if sub_tag == 0x10 {
                // PositionUpdate: 0x10 | x(4) | y(4)
                if len < 14 {
                    continue;
                }
                let x = f32::from_le_bytes(buf[6..10].try_into().unwrap());
                let y = f32::from_le_bytes(buf[10..14].try_into().unwrap());

                let update = PositionUpdate { client_id, x, y };
                let actions = spatial_service.handle_position_update(&update, &pending_ready);

                for action in actions {
                    match action {
                        SpatialAction::Subscribe { client_id, topic } => {
                            let mut msg = [0u8; 37];
                            msg[0] = 0x01;
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..37].copy_from_slice(&topic);
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                        SpatialAction::Unsubscribe { client_id, topic } => {
                            let mut msg = [0u8; 37];
                            msg[0] = 0x02;
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..37].copy_from_slice(&topic);
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                        SpatialAction::CrossingAlert { client_id, shards } => {
                            // CrossingAlert: 0x11 | client_id(4) | num_shards(1) | shard_ids...
                            let mut msg = vec![0x11];
                            msg.extend_from_slice(&client_id.to_le_bytes());
                            msg.push(shards.len() as u8);
                            for shard_id in shards {
                                msg.extend_from_slice(&shard_id.to_le_bytes());
                            }
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                        SpatialAction::AuthorityChange {
                            client_id,
                            old_shard,
                            new_shard,
                        } => {
                            // AuthorityChange: 0x12 | client_id(4) | old_shard(4) | new_shard(4)
                            let mut msg = [0u8; 13];
                            msg[0] = 0x12;
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..9].copy_from_slice(&old_shard.to_le_bytes());
                            msg[9..13].copy_from_slice(&new_shard.to_le_bytes());
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                        SpatialAction::ScaleUp { parent_shard, new_shards } => {
                            // ScaleUp to Orchestrator via Broker: 0x08 | orchestrator_commands(32) | 0x13 | parent(4) | s1(4) | s2(4) | s3(4) | s4(4)
                            info!("Shard {} overpopulated! Requesting Orchestrator to ScaleUp new shards: {:?}", parent_shard, new_shards);
                            let mut msg = vec![0x08];
                            let topic = b"orchestrator_commands";
                            let mut t = [0u8; 32];
                            t[..topic.len()].copy_from_slice(topic);
                            msg.extend_from_slice(&t);
                            msg.push(0x13);
                            msg.extend_from_slice(&parent_shard.to_le_bytes());
                            for ns in &new_shards {
                                msg.extend_from_slice(&ns.to_le_bytes());
                                pending_ready.insert(*ns, parent_shard);
                            }
                            parent_to_children.insert(parent_shard, new_shards);
                            
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                        SpatialAction::ScaleDown { parent_shard, old_shards } => {
                            // ScaleDown to Orchestrator via Broker: 0x08 | orchestrator_commands(32) | 0x15 | parent(4) | s1(4) | s2(4) | s3(4) | s4(4)
                            info!("Shards {:?} underpopulated! Requesting Orchestrator to ScaleDown to parent: {}", old_shards, parent_shard);
                            let mut msg = vec![0x08];
                            let topic = b"orchestrator_commands";
                            let mut t = [0u8; 32];
                            t[..topic.len()].copy_from_slice(topic);
                            msg.extend_from_slice(&t);
                            msg.push(0x15);
                            msg.extend_from_slice(&parent_shard.to_le_bytes());
                            for os in &old_shards {
                                msg.extend_from_slice(&os.to_le_bytes());
                            }
                            
                            // We wait for the parent_shard to send Ready before we confirm handoff
                            pending_ready.insert(parent_shard, parent_shard);
                            parent_to_children.insert(parent_shard, old_shards);
                            
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                    }
                }
            } else if sub_tag == 0x14 {
                // Server Ready: 0x14 | shard_id(4)
                if len >= 10 {
                    let shard_id = u32::from_le_bytes(buf[6..10].try_into().unwrap());
                    info!("Spatial Server received Ready signal from new shard {}!", shard_id);
                    
                    if let Some(&parent_shard) = pending_ready.get(&shard_id) {
                        pending_ready.remove(&shard_id);
                        if let Some(children) = parent_to_children.get_mut(&parent_shard) {
                            children.retain(|&x| x != shard_id);
                            if children.is_empty() {
                                // But for ScaleDown, old_shards are children, new_shard is parent!
                                // Check if this was a merge (parent_shard was in pending_merges)
                                if spatial_service.pending_merges.contains(&parent_shard) {
                                    info!("Parent shard {} is ready after ScaleDown! Emitting AuthorityChange to merge clients.", parent_shard);
                                    spatial_service.pending_merges.remove(&parent_shard);
                                    
                                    // Move clients from old_shards to parent_shard
                                    // But wait, the QuadTree ALREADY updated its structure during `try_merge`!
                                    // So the clients are just waiting for `AuthorityChange` on their next PositionUpdate, just like split!
                                    info!("Merge handoff completed. Clients will transition on next update.");
                                } else {
                                    info!("All children for parent shard {} are ready! Executing Handoff.", parent_shard);
                                    spatial_service.pending_splits.remove(&parent_shard);
                                    info!("Split handoff completed for parent {}. Clients will transition seamlessly on their next update.", parent_shard);
                                }
                                parent_to_children.remove(&parent_shard);
                            }
                        }
                    }
                }
            }
        } else if tag == 0x07 {
            // Disconnect: 0x07 | client_id(4)
            if len < 5 {
                continue;
            }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            spatial_service.client_primary_shard.remove(&client_id);
            spatial_service.client_subscribed_shards.remove(&client_id);
            info!(
                "Client {} disconnected, removed from spatial tracking.",
                client_id
            );
            
            // Check for potential merges
            let actions = spatial_service.check_merges();
            for action in actions {
                if let SpatialAction::ScaleDown { parent_shard, old_shards } = action {
                    info!("Shards {:?} underpopulated! Requesting Orchestrator to ScaleDown to parent: {}", old_shards, parent_shard);
                    let mut msg = vec![0x15];
                    msg.extend_from_slice(&parent_shard.to_le_bytes());
                    for os in &old_shards {
                        msg.extend_from_slice(&os.to_le_bytes());
                    }
                    
                    // We wait for the parent_shard to send Ready before we confirm handoff
                    // Use pending_ready but map parent_shard to itself
                    pending_ready.insert(parent_shard, parent_shard);
                    parent_to_children.insert(parent_shard, vec![parent_shard]); // Just wait for parent
                    
                    let _ = socket.send_to(&msg, &orch_addr).await;
                }
            }
        }
    }
}
