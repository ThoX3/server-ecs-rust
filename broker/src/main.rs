use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};

type Topic = [u8; 32];
type ClientId = u32;

struct BrokerState {
    client_addresses: HashMap<ClientId, SocketAddr>,
    client_topics: HashMap<ClientId, Topic>,
    topic_subscribers: HashMap<Topic, HashSet<ClientId>>,
    shard_addresses: HashMap<Topic, SocketAddr>,
}

impl BrokerState {
    fn new() -> Self {
        Self {
            client_addresses: HashMap::new(),
            client_topics: HashMap::new(),
            topic_subscribers: HashMap::new(),
            shard_addresses: HashMap::new(),
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
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    let topic = data[4..36].try_into().unwrap();

    state.client_topics.insert(client_id, topic);
    state
        .topic_subscribers
        .entry(topic)
        .or_insert_with(std::collections::HashSet::new)
        .insert(client_id);
}

fn handle_unsubscribe(state: &mut BrokerState, data: &[u8]) {
    let client_id_bytes = data[0..4].try_into().unwrap();
    let client_id = u32::from_le_bytes(client_id_bytes);

    let topic = data[4..36].try_into().unwrap();

    state.client_topics.remove(&client_id);
    if let Some(subscribers) = state.topic_subscribers.get_mut(&topic) {
        subscribers.remove(&client_id);
    }
}

fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    let topic: [u8; 32] = data[0..32].try_into().unwrap();
    let game_data = &data[32..];

    // Securité
    if let Some(official_shard_addr) = state.shard_addresses.get(&topic) {
        if src != *official_shard_addr {
            eprintln!(
                "Refus de publication : l'adresse {} n'est pas le Shard officiel pour ce topic.",
                src
            );
            return;
        }
    } else {
        eprintln!("Refus de publication : aucun Shard enregistré pour ce topic.");
        return;
    }

    let mut broadcast_msg = Vec::new();
    broadcast_msg.push(0x04);
    broadcast_msg.extend_from_slice(&topic);
    broadcast_msg.extend_from_slice(game_data);

    // Envoi à tous les abonnés du shard
    if let Some(subscribers) = state.topic_subscribers.get(&topic) {
        for client_id in subscribers {
            if let Some(client_addr) = state.client_addresses.get(client_id) {
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

    if let Some(topic) = state.client_topics.get(&client_id) {
        if let Some(shard_addr) = state.shard_addresses.get(topic) {
            let mut shard_msg = Vec::with_capacity(1 + 4 + input_payload.len());
            shard_msg.push(0x05);
            shard_msg.extend_from_slice(&client_id_bytes);
            shard_msg.extend_from_slice(input_payload);

            let _ = socket.send_to(&shard_msg, shard_addr);
        }
    }
}

fn handle_register_shard(state: &mut BrokerState, data: &[u8], src: SocketAddr) {
    if data.len() < 32 {
        return;
    }
    let topic: [u8; 32] = data[0..32].try_into().unwrap();
    state.shard_addresses.insert(topic, src);
}
