use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};

type Topic = u8;
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
    let socket = UdpSocket::bind("0.0.0.0:9000")?;
    let mut state = BrokerState::new();
    let mut buf = [0u8; 2048];

    println!("Broker PubSub démarré sur le port 9000...");

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
            _ => eprintln!("Tag inconnu: 0x{:02X}", tag),
        }
    }
}

fn handle_subscribe(state: &mut BrokerState, data: &[u8]) {
    if data.len() < 5 {
        return;
    }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let topic: Topic = data[4];

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
    if data.len() < 5 {
        return;
    }

    if let Ok(client_id_bytes) = data[0..4].try_into() {
        let client_id = u32::from_le_bytes(client_id_bytes);
        let topic: Topic = data[4];

        if let Some(topics) = state.client_to_topics.get_mut(&client_id) {
            topics.remove(&topic);
        }

        if let Some(route) = state.routes.get_mut(&topic) {
            route.clients.remove(&client_id);
        }
    }
}

fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 3 {
        return;
    }
    let topic: Topic = data[0];

    if let Ok(payload_len_bytes) = data[1..3].try_into() {
        let payload_len = u16::from_le_bytes(payload_len_bytes) as usize;

        if data.len() < 3 + payload_len {
            return;
        }
        let game_data = &data[3..3 + payload_len];

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
                    shard_msg[0..4].copy_from_slice(&client_id_bytes);
                    let total_len = 4 + input_payload.len();

                    if total_len <= 512 {
                        shard_msg[4..total_len].copy_from_slice(input_payload);
                        let _ = socket.send_to(&shard_msg[..total_len], route.authority_shard);
                    }
                }
            }
        }
    }
}

fn handle_register_shard(state: &mut BrokerState, data: &[u8], src: SocketAddr) {
    if data.len() < 1 {
        return;
    }
    let topic: Topic = data[0];

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
