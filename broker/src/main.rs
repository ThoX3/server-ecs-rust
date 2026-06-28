use shared::logger::{info, warn};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};

// ==========================================
// 1. DATA STRUCTURES
// ==========================================

/// A Topic is a fixed 32-byte array. Everything in our system (shards, orchestrator commands)
/// is just a Topic string converted into 32 bytes.
type Topic = [u8; 32];
type ClientId = u32;

/// A TopicRoute holds the definitive network address of the Server that currently "owns" this topic.
/// It also keeps a HashSet of all ClientIds that have subscribed to it.
struct TopicRoute {
    authority_shard: SocketAddr, // The Dedicated Server / Orchestrator / Spatial Server managing this topic
    clients: HashSet<ClientId>,  // Players listening to this topic
}

/// The entire state of the Broker.
struct BrokerState {
    // Maps a ClientId to their direct UDP connection via the Gatekeeper
    client_addresses: HashMap<ClientId, SocketAddr>,
    // Maps a ClientId to a set of all Topics they are currently subscribed to
    client_to_topics: HashMap<ClientId, HashSet<Topic>>,
    // Maps a Topic string to its routing information (Owner address + Subscribers)
    routes: HashMap<Topic, TopicRoute>,
}

impl BrokerState {
    fn new() -> Self {
        Self {
            client_addresses: HashMap::new(),
            client_to_topics: HashMap::new(),
            routes: HashMap::new(),
        }
    }
}

// ==========================================
// 2. MAIN LOOP
// ==========================================
fn main() -> std::io::Result<()> {
    shared::logger::init_logger("Broker");
    
    // The Broker is the absolute core of the network, listening on port 9000.
    let socket = UdpSocket::bind("0.0.0.0:9000")?;
    let mut state = BrokerState::new();
    let mut buf = [0u8; 2048];

    info!("Broker PubSub démarré sur le port 9000...");

    loop {
        // Read raw UDP packets from ANY source (Clients, Gatekeeper, Dedicated Servers, Orchestrator)
        let (amt, src) = socket.recv_from(&mut buf)?;

        if amt < 1 { continue; }

        // The first byte determines the protocol!
        let tag = buf[0];
        let payload = &buf[1..amt];

        match tag {
            0x01 => handle_subscribe(&mut state, payload),
            0x02 => handle_unsubscribe(&mut state, payload),
            0x03 => handle_publish(&mut state, &socket, payload, src),
            0x05 => handle_client_input(&mut state, &socket, payload, src),
            0x06 => handle_register_shard(&mut state, payload, src),
            0x07 => handle_client_disconnect(&mut state, &socket, payload),
            0x08 => handle_route_to_topic(&mut state, &socket, payload),
            0x11 => handle_crossing_alert(&mut state, &socket, payload),
            0x12 => handle_authority_change(&mut state, &socket, payload),
            0x14 => handle_server_ready(&mut state, &socket, payload),
            _ => warn!("Tag inconnu: 0x{:02X}", tag),
        }
    }
}

// ==========================================
// 3. PROTOCOL HANDLERS
// ==========================================

/// Handles `0x08 RouteToTopic`
/// A highly useful command. A service can use this to send a raw payload directly to the Authority of a Topic.
/// Ex: SpatialServer sends 0x13 ScaleUp to the `orchestrator_commands` topic. The Orchestrator receives it.
fn handle_route_to_topic(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    // 0x08 | topic(32) | payload
    if data.len() < 32 { return; }

    let mut topic = [0u8; 32];
    topic.copy_from_slice(&data[0..32]);
    let payload = &data[32..];

    if let Some(route) = state.routes.get(&topic) {
        let _ = socket.send_to(payload, route.authority_shard);
    }
}

/// Handles `0x14 ServerReady`
/// A Dedicated Server has finished its warm-up phase and is ready to accept players.
/// The Broker intercepts this and artificially routes it to the `spatial_updates` topic.
fn handle_server_ready(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    // 0x14 | shard_id(4)
    if data.len() < 4 { return; }

    let mut topic = [0u8; 32];
    let bytes = b"spatial_updates";
    topic[..bytes.len()].copy_from_slice(bytes);

    if let Some(route) = state.routes.get(&topic) {
        // We prepend 0x05 (Routed Client Input) with a dummy Client ID 0.
        // This is a neat trick so the SpatialServer's 0x05 handler parses it automatically!
        let mut msg = vec![0x05];
        msg.extend_from_slice(&0u32.to_le_bytes()); 
        msg.push(0x14); // Inner tag 0x14
        msg.extend_from_slice(data);
        let _ = socket.send_to(&msg, route.authority_shard);
    }
}

/// Handles `0x11 CrossingAlert`
/// SpatialServer sends this to notify a client they are near a border.
fn handle_crossing_alert(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 4 { return; }
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    if let Some(client_addr) = state.client_addresses.get(&client_id) {
        let mut msg = vec![0x11];
        msg.extend_from_slice(data);
        let _ = socket.send_to(&msg, client_addr);
    }
}

/// Handles `0x12 AuthorityChange`
/// SpatialServer sends this to force a client to switch its inputs to a new Shard.
fn handle_authority_change(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 12 { return; }
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);
    
    let old_shard = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let new_shard = u32::from_le_bytes(data[8..12].try_into().unwrap());

    let mut msg = vec![0x12];
    msg.extend_from_slice(data);

    // 1. Send to the Client
    if let Some(client_addr) = state.client_addresses.get(&client_id) {
        let _ = socket.send_to(&msg, client_addr);
    }
    
    // 2. Send to the Old Shard Server (so it can demote to Ghost)
    let old_topic = shared::quadtree::string_to_topic(&format!("shard:{}", old_shard));
    if let Some(route) = state.routes.get(&old_topic) {
        let _ = socket.send_to(&msg, route.authority_shard);
    }
    
    // 3. Send to the New Shard Server (so it can promote to Owned)
    let new_topic = shared::quadtree::string_to_topic(&format!("shard:{}", new_shard));
    if let Some(route) = state.routes.get(&new_topic) {
        let _ = socket.send_to(&msg, route.authority_shard);
    }
}

/// Handles `0x07 ClientDisconnect`
/// Gatekeeper sends this when a TCP connection drops.
fn handle_client_disconnect(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 4 { return; }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);

        // Notify all the Authority Shards this client was talking to
        if let Some(topics) = state.client_to_topics.get(&client_id) {
            for topic in topics {
                if let Some(route) = state.routes.get_mut(topic) {
                    let mut msg = [0u8; 5];
                    msg[0] = 0x07;
                    msg[1..5].copy_from_slice(&client_id_bytes);
                    // Route the disconnect to the server so it despawns the player's ECS Entity
                    let _ = socket.send_to(&msg, route.authority_shard);

                    route.clients.remove(&client_id);
                }
            }
        }

        // Complete cleanup from memory
        state.client_to_topics.remove(&client_id);
        state.client_addresses.remove(&client_id);
    }
}

/// Handles `0x01 Subscribe`
/// Adds a ClientId to the HashSet of subscribers for a given Topic.
fn handle_subscribe(state: &mut BrokerState, data: &[u8]) {
    if data.len() < 36 { return; }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&data[4..36]);

        let new_client = !state.client_to_topics.contains_key(&client_id);
        state.client_to_topics.entry(client_id).or_default().insert(topic);

        if let Some(route) = state.routes.get_mut(&topic) {
            route.clients.insert(client_id);
        }
        
        if new_client {
            info!("New client connected to Broker: ID {}", client_id);
        }
        
        // Find string representation of topic
        let topic_str = String::from_utf8_lossy(&topic).trim_end_matches('\0').to_string();
        info!("Client {} subscribed to topic '{}'", client_id, topic_str);
    }
}

/// Handles `0x02 Unsubscribe`
/// Removes a ClientId from the HashSet of subscribers for a given Topic.
fn handle_unsubscribe(state: &mut BrokerState, data: &[u8]) {
    if data.len() < 36 { return; }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&data[4..36]);

        if let Some(topics) = state.client_to_topics.get_mut(&client_id) {
            topics.remove(&topic);
        }

        if let Some(route) = state.routes.get_mut(&topic) {
            route.clients.remove(&client_id);
        }
    }
}

/// Handles `0x03 Publish`
/// A Dedicated Server uses this to broadcast game state (health, position, etc).
/// The Broker takes the payload and duplicates it to EVERY client in the `clients` HashSet.
fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    // 0x03 | topic(32) | payload_len(2) | game_data
    if data.len() < 34 { return; }
    
    let mut topic = [0u8; 32];
    topic.copy_from_slice(&data[0..32]);

    if let Ok(payload_len_bytes) = data[32..34].try_into() {
        let payload_len = u16::from_le_bytes(payload_len_bytes) as usize;

        if data.len() < 34 + payload_len { return; }
        
        let game_data = &data[34..34 + payload_len];

        if let Some(route) = state.routes.get(&topic) {
            // Security: Only the server that OWNS the topic can publish data to it!
            if src != route.authority_shard { return; }

            // Loop through all subscribed players and send them the data as `0x04 Broadcast`
            for client_id in &route.clients {
                if let Some(client_addr) = state.client_addresses.get(client_id) {
                    let mut broadcast_msg = [0u8; 1500];
                    broadcast_msg[0] = 0x04;
                    broadcast_msg[1..3].copy_from_slice(&(payload_len as u16).to_le_bytes());
                    let total_len = 3 + game_data.len();

                    if total_len <= 1500 {
                        broadcast_msg[3..total_len].copy_from_slice(game_data);
                        let _ = socket.send_to(&broadcast_msg[..total_len], client_addr);
                    }
                }
            }
        }
    }
}

/// Handles `0x05 Routed Client Input`
/// Clients send commands (move up, attack, jump). 
/// The Broker finds EVERY Topic the client is subscribed to and forwards the command to the Authority servers!
fn handle_client_input(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 4 { return; }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let input_payload = &data[4..];

        // Ensure we know the client's return address so we can send data back
        if !state.client_addresses.contains_key(&client_id) {
            info!("Registered return address for Client {}", client_id);
        }
        state.client_addresses.insert(client_id, src);

        if let Some(topics) = state.client_to_topics.get(&client_id) {
            for topic in topics {
                if let Some(route) = state.routes.get(topic) {
                    let mut shard_msg = [0u8; 512];
                    shard_msg[0] = 0x05;
                    shard_msg[1..5].copy_from_slice(&client_id_bytes);
                    let total_len = 5 + input_payload.len();

                    if total_len <= 512 {
                        shard_msg[5..total_len].copy_from_slice(input_payload);
                        let _ = socket.send_to(&shard_msg[..total_len], route.authority_shard);
                    }
                }
            }
        }
    }
}

/// Handles `0x06 Claim Authority`
/// Called when a DedicatedServer boots up or is assigned a Shard via `0x18 AssignShards`.
/// It binds the Topic string to the Sender's UDP IP/Port.
fn handle_register_shard(state: &mut BrokerState, data: &[u8], src: SocketAddr) {
    if data.len() < 32 { return; }
    
    let mut topic = [0u8; 32];
    topic.copy_from_slice(&data[0..32]);

    if let Some(route) = state.routes.get_mut(&topic) {
        route.authority_shard = src;
    } else {
        state.routes.insert(
            topic,
            TopicRoute {
                authority_shard: src,
                clients: HashSet::new(),
            },
        );
    }
    
    let topic_str = String::from_utf8_lossy(&topic).trim_end_matches('\0').to_string();
    info!("Server {} claimed authority for topic '{}'", src, topic_str);
}
