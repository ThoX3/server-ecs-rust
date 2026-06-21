use shared::logger::{error, info, warn};
use shared::Heartbeat;
use std::collections::HashMap;
use std::env;
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

const TTL_SECONDS: u64 = 15; // Temps avant qu'un serveur soit considéré comme mort
const SCALER_INTERVAL: u64 = 5; // Fréquence de vérification de la flotte (en secondes)
const BASE_DS_PORT: u16 = 7001; // Port de départ pour attribuer aux nouveaux serveurs

struct ServerState {
    heartbeat: Heartbeat,
    last_seen: Instant,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::logger::init_logger("Orchestrator");
    // Boot the orchestrator and its tasks.
    info!("Démarrage de l'orchestrateur...");

    let orch_port = std::env::var("ORCH_PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse::<u16>()
        .unwrap_or(8000);

    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", orch_port)).await?);
    info!("Orchestrateur à l'écoute sur le port UDP {}", orch_port);

    // Register the orchestrator_commands topic with the broker
    let broker_addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".to_string());
    let mut reg_msg = [0u8; 33];
    reg_msg[0] = 0x06;
    let topic_bytes = b"orchestrator_commands";
    reg_msg[1..1 + topic_bytes.len()].copy_from_slice(topic_bytes);
    let _ = socket.send_to(&reg_msg, broker_addr).await?;
    info!("Registered orchestrator_commands authority with Broker.");

    // Fleet state
    let fleet: Arc<Mutex<HashMap<String, ServerState>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener_fleet = fleet.clone();

    let listener_socket = socket.clone();

    // Store server processes to allow killing them on ScaleDown
    let processes: Arc<Mutex<HashMap<u32, std::process::Child>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener_processes = processes.clone();

    // Listen for heartbeats.
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_listener(listener_socket, listener_fleet, listener_processes).await {
            error!("Erreur dans la tâche heartbeat_listener: {:?}", e);
        }
    });

    // Monitor and scale the fleet.
    let scaler_handle = tokio::spawn(async move {
        if let Err(e) = scaler_loop(fleet).await {
            error!("Erreur dans la tâche scaler_loop: {:?}", e);
        }
    });

    tokio::try_join!(heartbeat_handle, scaler_handle)?;

    Ok(())
}

async fn heartbeat_listener(
    socket: Arc<UdpSocket>,
    fleet: Arc<Mutex<HashMap<String, ServerState>>>,
    processes: Arc<Mutex<HashMap<u32, std::process::Child>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Receive heartbeats and update in-memory fleet state.
    let mut buf = [0u8; 2048];

    loop {
        let (len, _sender_addr) = socket.recv_from(&mut buf).await?;
        if len == 0 { continue; }

        if buf[0] == 0x13 && len >= 21 {
            // ScaleUp
            let parent = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
            let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
            let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
            info!("Received ScaleUp instruction for shards {}, {}, {}, {}", s1, s2, s3, s4);

            let pack_server_id = {
                let fleet_lock = fleet.lock().await;
                fleet_lock.iter().find(|(_, state)| state.heartbeat.player_count < 128).map(|(id, _)| id.clone())
            };

            if let Some(target_server_id) = pack_server_id {
                info!("Packing new shards into existing server {}", target_server_id);
                let mut topic = [0u8; 32];
                topic[..target_server_id.len()].copy_from_slice(target_server_id.as_bytes());
                let mut msg = vec![0x08];
                msg.extend_from_slice(&topic);
                msg.push(0x18);
                msg.extend_from_slice(&4u32.to_le_bytes());
                msg.extend_from_slice(&s1.to_le_bytes());
                msg.extend_from_slice(&s2.to_le_bytes());
                msg.extend_from_slice(&s3.to_le_bytes());
                msg.extend_from_slice(&s4.to_le_bytes());
                let broker_addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
                let _ = socket.send_to(&msg, broker_addr).await;
            } else {
                let port = find_free_port().unwrap_or(7100);
                match spawn_server(port, vec![s1, s2, s3, s4], Some(parent)) {
                    Ok(child) => {
                        let mut procs = processes.lock().await;
                        procs.insert(s1, child);
                    }
                    Err(e) => error!("Failed to spawn server on port {}: {:?}", port, e),
                }
            }
            continue;
        } else if buf[0] == 0x15 && len >= 21 {
            // ScaleDown
            let parent = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
            let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
            let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
            info!("Received ScaleDown instruction. Merging shards {}, {}, {}, {} into parent {}", s1, s2, s3, s4, parent);

            {
                let mut procs = processes.lock().await;
                if let Some(mut child) = procs.remove(&s1) {
                    let mut topic = [0u8; 32];
                    let shard_str = format!("shard:{}", s1);
                    topic[..shard_str.len()].copy_from_slice(shard_str.as_bytes());
                    let mut msg = vec![0x08];
                    msg.extend_from_slice(&topic);
                    msg.push(0x17);
                    let broker_addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
                    let _ = socket.send_to(&msg, broker_addr).await;
                    info!("Sent Graceful Shutdown command to {} child process.", shard_str);

                    // Spawn a task to wait for the child process so it doesn't become a zombie
                    tokio::spawn(async move {
                        let _ = child.wait();
                    });
                }
            }

            let port = find_free_port().unwrap_or(7100);
            match spawn_server(port, vec![parent], None) {
                Ok(child) => {
                    let mut procs = processes.lock().await;
                    procs.insert(parent, child);
                }
                Err(e) => error!("Failed to spawn parent server on port {}: {:?}", port, e),
            }
            continue;
        }

        if let Ok(hb) = serde_json::from_slice::<Heartbeat>(&buf[..len]) {
            let status = if hb.player_count >= hb.max_players {
                "full"
            } else {
                "available"
            };

            info!("Heartbeat traité pour le serveur {} (Statut: {})", hb.id, status);
            let mut fleet_lock = fleet.lock().await;
            fleet_lock.insert(hb.id.clone(), ServerState {
                heartbeat: hb,
                last_seen: Instant::now(),
            });
        }
    }
}

async fn scaler_loop(fleet: Arc<Mutex<HashMap<String, ServerState>>>) -> Result<(), Box<dyn std::error::Error>> {
    let mut interval = interval(Duration::from_secs(SCALER_INTERVAL));
    let mut next_port_to_use = BASE_DS_PORT;

    loop {
        interval.tick().await;

        let available_count = {
            let mut fleet_lock = fleet.lock().await;
            let now = Instant::now();

            let mut orphaned_shards = Vec::new();

            // Prune dead servers
            fleet_lock.retain(|_, state| {
                if now.duration_since(state.last_seen).as_secs() < TTL_SECONDS {
                    true
                } else {
                    orphaned_shards.extend(state.heartbeat.shards.clone());
                    false
                }
            });

            if !orphaned_shards.is_empty() {
                let mut topic = [0u8; 32];
                let bytes = b"spatial_updates";
                topic[..bytes.len()].copy_from_slice(bytes);
                let mut msg = vec![0x08];
                msg.extend_from_slice(&topic);
                msg.push(0x19);
                let count = orphaned_shards.len() as u32;
                msg.extend_from_slice(&count.to_le_bytes());
                for s in &orphaned_shards {
                    msg.extend_from_slice(&s.to_le_bytes());
                }
                let local_socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
                let broker_addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
                let _ = local_socket.send_to(&msg, broker_addr).await;
                error!("Detected Server Crash! Sent 0x19 ServerCrash for shards: {:?}", orphaned_shards);
            }

            fleet_lock.values()
                .filter(|state| state.heartbeat.player_count < state.heartbeat.max_players)
                .count()
        };

        let hot_min: usize = std::env::var("HOT_SERVERS_MIN")
            .unwrap_or_else(|_| "2".to_string())
            .parse()
            .unwrap_or(2);

        info!("Flotte actuelle : {} serveurs disponibles (Requis minimum : {})", available_count, hot_min);

        if available_count < hot_min {
            let needed = hot_min - available_count;
            warn!("Alerte sous-effectif ! Lancement de {} serveur(s) dédié(s)...", needed);

            for _ in 0..needed {
                let port = next_port_to_use;
                next_port_to_use += 1;

                if let Err(e) = spawn_server(port, vec![0], None) {
                    error!("Impossible de spawner le serveur sur le port {}: {:?}", port, e);
                }
            }
        }
    }
}

fn find_free_port() -> Option<u16> {
    for port in 7100..7200 {
        if std::net::UdpSocket::bind(format!("127.0.0.1:{}", port)).is_ok() {
            return Some(port);
        }
    }
    None
}

fn spawn_server(port: u16, shards: Vec<u32>, parent: Option<u32>) -> io::Result<std::process::Child> {
    let ds_path = env::var("DS_BINARY_PATH").unwrap_or_else(|_| "cargo".to_string());

    let mut cmd = std::process::Command::new(&ds_path);

    if ds_path == "cargo" {
        cmd.arg("run").arg("-p").arg("dedicated_server").arg("--");
    }

    let shards_str = shards.iter().map(|s| format!("shard:{}", s)).collect::<Vec<_>>().join(",");
    info!("Spawning server on port {} managing shards: {}", port, shards_str);

    cmd.env("DS_PORT", port.to_string())
        .env("SHARDS", shards_str);

    if let Some(p) = parent {
        cmd.env("PARENT_SHARD", format!("shard:{}", p));
    }

    Ok(cmd.spawn()?)
}
