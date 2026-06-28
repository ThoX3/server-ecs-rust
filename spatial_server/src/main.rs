use bevy::math::{Rect, Vec2};
use shared::logger::info;
use shared::quadtree::{PositionUpdate, QuadTree, SpatialAction, SpatialService};
use std::env;
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ==========================================
    // 1. INITIALIZATION & SETUP
    // ==========================================
    // Initialize the logger so we can see what the SpatialServer is doing.
    shared::logger::init_logger("SpatialServer");
    info!("Starting...");

    // Read network configuration from environment variables.
    // The SpatialServer needs to know its own port, where to reach the Broker,
    // and where the Orchestrator lives (although mostly it sends to Orchestrator via the Broker now).
    let port = env::var("PORT").unwrap_or_else(|_| "9001".to_string());
    let broker_addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    let orch_addr = env::var("ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

    // Bind the UDP socket for the server. We wrap it in an Arc (Atomically Reference Counted)
    // so we could potentially share it across async tasks if needed, though here it's mostly single-threaded loop.
    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", port)).await?);
    info!("Listening on port {}", port);
    info!("Broker address: {}", broker_addr);
    info!("Orchestrator address: {}", orch_addr);

    // ==========================================
    // 2. QUADTREE & SPATIAL SERVICE CREATION
    // ==========================================
    // Create the base geographical bounds for the entire game world.
    // Here we define a 2000x2000 square centered at (0, 0).
    // This is the "root" shard from which all other shards will split.
    let bounds = Rect::from_center_size(Vec2::ZERO, Vec2::new(2000.0, 2000.0));

    // Initialize the QuadTree. We start at depth 0, with a maximum depth of 4 splits.
    // This means the world can split into 4^4 = 256 smallest possible shards.
    let mut quadtree = QuadTree::new(bounds, 0, 4);

    // The root shard gets an implicit ID of 0, so the next generated shard will start at ID 1.
    let mut next_id = 1;

    // Create the central `SpatialService` which encapsulates the logic for routing,
    // bounds checking, and deciding when to split or merge shards.
    // A margin of 50.0 means players will subscribe to neighboring shards if they are
    // within 50 units of the border.
    let mut spatial_service = SpatialService::new(quadtree, 50.0, next_id);

    // ==========================================
    // 3. STATE SYNCHRONIZATION LOCKS
    // ==========================================
    // Because splitting and merging requires spinning up/down OS processes via the Orchestrator,
    // these actions are asynchronous. We need to track what we're waiting for.

    // `pending_ready` maps a `child_shard_id` to its `parent_shard_id`.
    // It tells us: "We are waiting for child shard X to send a Ready signal before we complete a split for parent Y."
    let mut pending_ready = std::collections::HashMap::<u32, u32>::new();

    // `parent_to_children` maps a `parent_shard_id` to a list of `child_shard_ids`.
    // It helps us track if ALL children for a specific parent have booted up.
    // Once the vector is empty, the handoff is complete!
    let mut parent_to_children = std::collections::HashMap::<u32, Vec<u32>>::new();

    // ==========================================
    // 4. BROKER REGISTRATION
    // ==========================================
    // The SpatialServer needs to receive updates (like 0x14 Ready or 0x19 ServerCrash) from other services.
    // It registers itself as the authority for the "spatial_updates" topic on the PubSub Broker.
    let mut reg_msg = [0u8; 33];
    reg_msg[0] = 0x06; // 0x06 is the Claim Authority protocol tag
    let topic_bytes = b"spatial_updates";
    reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
    socket.send_to(&reg_msg, &broker_addr).await?;
    info!("Registered spatial_updates authority with Broker.");

    // The main network receive buffer
    let mut buf = [0u8; 1024];

    // ==========================================
    // 5. MAIN EVENT LOOP
    // ==========================================
    loop {
        // Wait for incoming UDP packets
        let (len, _src) = socket.recv_from(&mut buf).await?;
        if len < 1 {
            continue;
        }

        let tag = buf[0];

        // ----------------------------------------------------
        // PROTOCOL: 0x05 (ROUTED CLIENT INPUT / INTERNAL CMD)
        // ----------------------------------------------------
        // Almost all messages arriving here are routed by the Broker and thus start with 0x05.
        // Format: 0x05 | client_id (4 bytes) | sub_tag (1 byte) | payload...
        if tag == 0x05 {
            if len < 6 {
                continue;
            }

            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let sub_tag = buf[5];

            // ------------------------------------------------
            // SUB-PROTOCOL: 0x10 (POSITION UPDATE)
            // ------------------------------------------------
            // This is the lifeblood of the SpatialServer. Clients send their X/Y coordinates
            // frequently. We use this to figure out where they are and if they crossed any borders.
            if sub_tag == 0x10 {
                if len < 14 {
                    continue;
                }

                // Extract the X and Y floating point coordinates from the payload
                let x = f32::from_le_bytes(buf[6..10].try_into().unwrap());
                let y = f32::from_le_bytes(buf[10..14].try_into().unwrap());

                let update = PositionUpdate { client_id, x, y };

                // Pass the coordinates into our core logic engine.
                // It will calculate what actions need to be taken (e.g., subscribing to a new ghost zone,
                // swapping authority to a new shard, or triggering a full scale up).
                let actions = spatial_service.handle_position_update(&update, &pending_ready);

                // Execute the actions dictated by the core logic
                for action in actions {
                    match action {
                        // Action: The player moved near the border of a new shard.
                        // Tell the Broker to Subscribe this player to the new shard's topic.
                        SpatialAction::Subscribe { client_id, topic } => {
                            let mut msg = [0u8; 37];
                            msg[0] = 0x01; // 0x01 Subscribe tag
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..37].copy_from_slice(&topic);
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }

                        // Action: The player moved away from the border of a ghost shard.
                        // Tell the Broker to Unsubscribe this player from that topic to save bandwidth.
                        SpatialAction::Unsubscribe { client_id, topic } => {
                            let mut msg = [0u8; 37];
                            msg[0] = 0x02; // 0x02 Unsubscribe tag
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..37].copy_from_slice(&topic);
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }

                        // Action: (Legacy/Debug) Tells the client exactly which shards they are near.
                        SpatialAction::CrossingAlert { client_id, shards } => {
                            let mut msg = vec![0x11];
                            msg.extend_from_slice(&client_id.to_le_bytes());
                            msg.push(shards.len() as u8);
                            for shard_id in shards {
                                msg.extend_from_slice(&shard_id.to_le_bytes());
                            }
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }

                        // Action: The player completely crossed the border into a different shard!
                        // Tell the client to change its primary authority so its inputs go to the new server.
                        SpatialAction::AuthorityChange {
                            client_id,
                            old_shard,
                            new_shard,
                        } => {
                            let mut msg = [0u8; 13];
                            msg[0] = 0x12; // 0x12 AuthorityChange tag
                            msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                            msg[5..9].copy_from_slice(&old_shard.to_le_bytes());
                            msg[9..13].copy_from_slice(&new_shard.to_le_bytes());
                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }

                        // Action: The shard is overpopulated! We need to dynamically boot up more servers.
                        SpatialAction::ScaleUp {
                            parent_shard,
                            new_shards,
                        } => {
                            info!(
                                "Shard {} overpopulated! Requesting Orchestrator to ScaleUp new shards: {:?}",
                                parent_shard, new_shards
                            );

                            // We route a message to the Orchestrator using the Broker's `0x08 RouteToTopic` command.
                            let mut msg = vec![0x08];
                            let topic = b"orchestrator_commands";
                            let mut t = [0u8; 32];
                            t[..topic.len()].copy_from_slice(topic);
                            msg.extend_from_slice(&t);

                            // 0x13 is the internal tag the Orchestrator reads to start a ScaleUp
                            msg.push(0x13);
                            msg.extend_from_slice(&parent_shard.to_le_bytes());

                            // Append the 4 new child shard IDs so the Orchestrator knows what to assign
                            for ns in &new_shards {
                                msg.extend_from_slice(&ns.to_le_bytes());
                                // Lock the state: we are now waiting for this child shard to boot
                                pending_ready.insert(*ns, parent_shard);
                            }
                            // Store the parent -> children mapping for the readiness check
                            parent_to_children.insert(parent_shard, new_shards);

                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }

                        // Action: The shard is a ghost town. Tell the Orchestrator to merge it with its siblings.
                        SpatialAction::ScaleDown {
                            parent_shard,
                            old_shards,
                        } => {
                            info!(
                                "Shards {:?} underpopulated! Requesting Orchestrator to ScaleDown to parent: {}",
                                old_shards, parent_shard
                            );

                            // We route a message to the Orchestrator using the Broker's `0x08 RouteToTopic` command.
                            let mut msg = vec![0x08];
                            let topic = b"orchestrator_commands";
                            let mut t = [0u8; 32];
                            t[..topic.len()].copy_from_slice(topic);
                            msg.extend_from_slice(&t);

                            // 0x15 is the internal tag the Orchestrator reads to start a ScaleDown
                            msg.push(0x15);
                            msg.extend_from_slice(&parent_shard.to_le_bytes());
                            for os in &old_shards {
                                msg.extend_from_slice(&os.to_le_bytes());
                            }

                            // We place the parent_shard into the pending_ready map.
                            // The target server will run a warm-up phase and then send 0x14 Ready when done.
                            pending_ready.insert(parent_shard, parent_shard);
                            parent_to_children.insert(parent_shard, old_shards);

                            let _ = socket.send_to(&msg, &broker_addr).await;
                        }
                    }
                }
            }
            // ------------------------------------------------
            // SUB-PROTOCOL: 0x14 (SERVER READY SIGNAL)
            // ------------------------------------------------
            // Emitted by DedicatedServers when they finish booting/warming up
            else if sub_tag == 0x14 {
                if len >= 10 {
                    let shard_id = u32::from_le_bytes(buf[6..10].try_into().unwrap());
                    info!(
                        "Spatial Server received Ready signal from new shard {}!",
                        shard_id
                    );

                    // Did we expect this shard to become ready?
                    if let Some(&parent_shard) = pending_ready.get(&shard_id) {
                        // Yes! Remove it from the waiting list.
                        pending_ready.remove(&shard_id);

                        // Check if all siblings are also ready
                        if let Some(children) = parent_to_children.get_mut(&parent_shard) {
                            children.retain(|&x| x != shard_id);

                            // If the children vector is empty, ALL new servers have successfully booted up!
                            if children.is_empty() {
                                // Was this a merge (ScaleDown) or a split (ScaleUp)?
                                if spatial_service.pending_merges.contains(&parent_shard) {
                                    info!(
                                        "Parent shard {} is ready after ScaleDown! Emitting AuthorityChange to merge clients.",
                                        parent_shard
                                    );

                                    // Remove the state lock
                                    spatial_service.pending_merges.remove(&parent_shard);

                                    // The clients will automatically be moved on their next 0x10 PositionUpdate
                                    info!(
                                        "Merge handoff completed. Clients will transition on next update."
                                    );
                                } else {
                                    info!(
                                        "All children for parent shard {} are ready! Executing Handoff.",
                                        parent_shard
                                    );

                                    // Remove the state lock
                                    spatial_service.pending_splits.remove(&parent_shard);

                                    info!(
                                        "Split handoff completed for parent {}. Clients will transition seamlessly on their next update.",
                                        parent_shard
                                    );
                                }
                                // Clean up the parent tracking map
                                parent_to_children.remove(&parent_shard);
                            }
                        }
                    }
                }
            }
            // ------------------------------------------------
            // SUB-PROTOCOL: 0x19 (CATASTROPHIC SERVER CRASH)
            // ------------------------------------------------
            // Emitted by the Orchestrator if a server stops sending heartbeats
            else if sub_tag == 0x19 {
                if len < 10 {
                    continue;
                }
                let count = u32::from_le_bytes(buf[6..10].try_into().unwrap());
                let mut offset = 10;

                // Read the list of shards that were managed by the dead server
                let mut crashed_shards = Vec::new();
                for _ in 0..count {
                    if offset + 4 > len {
                        break;
                    }
                    crashed_shards.push(u32::from_le_bytes(
                        buf[offset..offset + 4].try_into().unwrap(),
                    ));
                    offset += 4;
                }

                info!(
                    "Emergency: Received ServerCrash for shards {:?}",
                    crashed_shards
                );

                // The SpatialService will forcefully collapse the QuadTree nodes for these shards
                // and return a list of emergency AuthorityChange commands to rescue clients.
                let actions = spatial_service.handle_server_crash(&crashed_shards);

                for action in actions {
                    // Send out the mass-rescue commands to all affected clients
                    if let SpatialAction::AuthorityChange {
                        client_id,
                        old_shard,
                        new_shard,
                    } = action
                    {
                        let mut msg = [0u8; 13];
                        msg[0] = 0x12;
                        msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                        msg[5..9].copy_from_slice(&old_shard.to_le_bytes());
                        msg[9..13].copy_from_slice(&new_shard.to_le_bytes());
                        let _ = socket.send_to(&msg, &broker_addr).await;
                        info!("Rescued client {} back to shard {}", client_id, new_shard);
                    }
                }
            }
        }
        // ----------------------------------------------------
        // PROTOCOL: 0x07 (CLIENT DISCONNECT)
        // ----------------------------------------------------
        // Emitted by the Gatekeeper when a player's TCP connection drops
        else if tag == 0x07 {
            if len < 5 {
                continue;
            }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());

            // Clean up the player from memory so they don't count towards population limits
            spatial_service.client_primary_shard.remove(&client_id);
            spatial_service.client_subscribed_shards.remove(&client_id);
            info!(
                "Client {} disconnected, removed from spatial tracking.",
                client_id
            );

            // Because a player left, we might have dropped below the MIN_POPULATION threshold!
            // We ask the QuadTree to check if any siblings can be merged now.
            let actions = spatial_service.check_merges();
            for action in actions {
                if let SpatialAction::ScaleDown {
                    parent_shard,
                    old_shards,
                } = action
                {
                    info!(
                        "Shards {:?} underpopulated! Requesting Orchestrator to ScaleDown to parent: {}",
                        old_shards, parent_shard
                    );

                    // Route the 0x15 ScaleDown command to the Orchestrator
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
                    parent_to_children.insert(parent_shard, vec![parent_shard]);

                    let _ = socket.send_to(&msg, &broker_addr).await;
                }
            }
        }
    }
}
