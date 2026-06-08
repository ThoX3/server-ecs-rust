use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};

type Topic = [u8; 32];
type ClientId = u32;

struct TopicRoute {
    authority_shard: SocketAddr,
    client_id: Option<ClientId>,
}

struct BrokerState {
    client_addresses: HashMap<ClientId, SocketAddr>,
    client_to_topic: HashMap<ClientId, Topic>,
    routes: HashMap<Topic, TopicRoute>,
}

impl BrokerState {
    fn new() -> Self {
        Self {
            client_addresses: HashMap::new(),
            client_to_topic: HashMap::new(),
            routes: HashMap::new(),
        }
    }
}

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:9000")?;
    let mut state = BrokerState::new();
    let mut buf = [0u8; 65535];

    // TODO : INITIALISATION DES PREMIERS SHARDS
    // let mut shard0_topic = [0u8; 32];
    // shard0_topic[0..7].copy_from_slice(b"shard:0");
    // state.shard_addresses.insert(shard0_topic, "127.0.0.1:8001".parse().unwrap());

    println!("Broker PubSub (Option B - Multi-Shards) démarré sur le port 9000...");

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
    if data.len() < 36 {
        return;
    }
    let client_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let topic: Topic = data[4..36].try_into().unwrap();

    state.client_to_topic.insert(client_id, topic);

    if let Some(route) = state.routes.get_mut(&topic) {
        route.client_id = Some(client_id);
    }
}

fn handle_unsubscribe(state: &mut BrokerState, data: &[u8]) {
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    let topic: Topic = data[4..36].try_into().unwrap();

    state.client_to_topic.remove(&client_id);
    if let Some(route) = state.routes.get_mut(&topic) {
        if route.client_id == Some(client_id) {
            route.client_id = None;
        }
    }
}

fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 34 {
        return;
    }
    let topic: Topic = data[0..32].try_into().unwrap();
    let payload_len = u16::from_le_bytes(data[32..34].try_into().unwrap()) as usize;

    if data.len() < 34 + payload_len {
        return;
    }
    let game_data = &data[34..34 + payload_len];

    if let Some(route) = state.routes.get(&topic) {
        if src != route.authority_shard {
            return;
        }

        if let Some(client_id) = route.client_id {
            if let Some(client_addr) = state.client_addresses.get(&client_id) {
                let mut broadcast_msg = Vec::with_capacity(1 + 2 + game_data.len());
                broadcast_msg.push(0x04); // Tag Broadcast
                broadcast_msg.extend_from_slice(&(payload_len as u16).to_le_bytes());
                broadcast_msg.extend_from_slice(game_data);

                let _ = socket.send_to(&broadcast_msg, client_addr);
            }
        }
    }
}

fn handle_client_input(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    if data.len() < 4 {
        return;
    }

    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);
    let input_payload = &data[4..];

    state.client_addresses.insert(client_id, src);

    if let Some(topic) = state.client_to_topic.get(&client_id) {
        if let Some(route) = state.routes.get(topic) {
            let mut shard_msg = Vec::with_capacity(1 + 4 + 16);
            shard_msg.push(0x05); // Tag Client Input
            shard_msg.extend_from_slice(&client_id_bytes);
            shard_msg.extend_from_slice(input_payload);

            let _ = socket.send_to(&shard_msg, route.authority_shard);
        }
    }
}

fn handle_register_shard(state: &mut BrokerState, data: &[u8], src: SocketAddr) {
    if data.len() < 32 {
        return;
    }
    let topic: Topic = data[0..32].try_into().unwrap();

    if let Some(route) = state.routes.get_mut(&topic) {
        route.authority_shard = src;
    } else {
        state.routes.insert(
            topic,
            TopicRoute {
                authority_shard: src,
                client_id: None,
            },
        );
    }
}
