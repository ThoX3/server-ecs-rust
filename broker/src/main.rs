use std::collections::{HashMap, HashSet};
use std::net::{UdpSocket, SocketAddr};

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

    println!("Broker PubSub démarré sur le port 9000...");

    loop {
        let (amt, src) = socket.recv_from(&mut buf)?;

        if amt < 1 { continue; }

        let tag = buf[0];
        let payload = &buf[1..amt];

        match tag {
            0x01 => handle_subscribe(&mut state, payload),
            0x02 => handle_unsubscribe(&mut state, payload),
            0x03 => handle_publish(&mut state, &socket, payload, src),
            0x05 => handle_client_input(&mut state, &socket, payload, src),
            _ => eprintln!("Tag inconnu: 0x{:02X}", tag),
        }
    }
}


fn handle_subscribe(state: &mut BrokerState, data: &[u8]) {
    // TODO
}

fn handle_unsubscribe(state: &mut BrokerState, data: &[u8]) {
    // TODO
}

fn handle_publish(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    // TODO
}

fn handle_client_input(state: &mut BrokerState, socket: &UdpSocket, data: &[u8], src: SocketAddr) {
    // TODO
}
