use shared::logger::{info, warn};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};

type Topic = [u8; 32];
type ClientId = u32;

struct TopicRoute {
    authority_shard: SocketAddr,
    clients: HashSet<ClientId>,
}

struct BrokerState {
    client_addresses: HashMap<ClientId, SocketAddr>,
    client_to_topics: HashMap<ClientId, HashSet<Topic>>,
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

fn main() -> std::io::Result<()> {
    shared::logger::init_logger("Broker");
    let socket = UdpSocket::bind("0.0.0.0:9000")?;
    let mut state = BrokerState::new();
    let mut buf = [0u8; 2048];

    info!("Broker PubSub démarré sur le port 9000...");

    loop {
        let (amt, src) = socket.recv_from(&mut buf)?;

        if amt < 1 {
            continue;
        }

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

fn handle_route_to_topic(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    // 0x08 | topic(32) | payload
    if data.len() < 32 {
        return;
    }

    let mut topic = [0u8; 32];
    topic.copy_from_slice(&data[0..32]);
    let payload = &data[32..];

    if let Some(route) = state.routes.get(&topic) {
        let _ = socket.send_to(payload, route.authority_shard);
    }
}

fn handle_server_ready(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    // 0x14 | shard_id(4)
    if data.len() < 4 {
        return;
    }

    // Route it to the spatial_updates authority
    let mut topic = [0u8; 32];
    let bytes = b"spatial_updates";
    topic[..bytes.len()].copy_from_slice(bytes);

    if let Some(route) = state.routes.get(&topic) {
        let mut msg = vec![0x05];
        msg.extend_from_slice(&0u32.to_le_bytes()); // Dummy client ID 0
        msg.push(0x14);
        msg.extend_from_slice(data);
        let _ = socket.send_to(&msg, route.authority_shard);
    }
}

fn handle_crossing_alert(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 4 {
        return;
    }
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    if let Some(client_addr) = state.client_addresses.get(&client_id) {
        let mut msg = vec![0x11];
        msg.extend_from_slice(data);
        let _ = socket.send_to(&msg, client_addr);
    }
}

fn handle_authority_change(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 12 {
        return;
    }
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    if let Some(client_addr) = state.client_addresses.get(&client_id) {
        let mut msg = vec![0x12];
        msg.extend_from_slice(data);
        let _ = socket.send_to(&msg, client_addr);
    }
}

fn handle_client_disconnect(state: &mut BrokerState, socket: &UdpSocket, data: &[u8]) {
    if data.len() < 4 {
        return;
    }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);

        // Notify all authority shards before cleaning up
        if let Some(topics) = state.client_to_topics.get(&client_id) {
            for topic in topics {
                if let Some(route) = state.routes.get_mut(topic) {
                    // Forward disconnect to shard
                    let mut msg = [0u8; 5];
                    msg[0] = 0x07;
                    msg[1..5].copy_from_slice(&client_id_bytes);
                    let _ = socket.send_to(&msg, route.authority_shard);

                    // Clean up route client list
                    route.clients.remove(&client_id);
                }
            }
        }

        // Complete cleanup
        state.client_to_topics.remove(&client_id);
        state.client_addresses.remove(&client_id);
    }
}


fn handle_subscribe(state: &mut BrokerState, data: &[u8]) {
    if data.len() < 36 {
        return;
    }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let mut topic = [0u8; 32];
        topic.copy_from_slice(&data[4..36]);

        state
            .client_to_topics
            .entry(client_id)
            .or_default()
            .insert(topic);

        if let Some(route) = state.routes.get_mut(&topic) {
            route.clients.insert(client_id);
        }
    }
}

fn handle_unsubscribe(state: &mut BrokerState, data: &[u8]) {
    if data.len() < 36 {
        return;
    }

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

fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 34 {
        return;
    }
    let mut topic = [0u8; 32];
    topic.copy_from_slice(&data[0..32]);

    if let Ok(payload_len_bytes) = data[32..34].try_into() {
        let payload_len = u16::from_le_bytes(payload_len_bytes) as usize;

        if data.len() < 34 + payload_len {
            return;
        }
        let game_data = &data[34..34 + payload_len];

        if let Some(route) = state.routes.get(&topic) {
            if src != route.authority_shard {
                return;
            }

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

fn handle_client_input(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 4 {
        return;
    }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let input_payload = &data[4..];

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

fn handle_register_shard(state: &mut BrokerState, data: &[u8], src: SocketAddr) {
    if data.len() < 32 {
        return;
    }
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
}
