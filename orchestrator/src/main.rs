use shared::Heartbeat;
use std::env;
use std::io;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use std::collections::HashMap;
use shared::logger::{info, warn, error};
use tokio::time::{interval, Duration};
use std::time::Instant;

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
            
            let port = find_free_port().unwrap_or(7100);
            match spawn_server(port, vec![s1, s2, s3, s4]) {
                Ok(child) => {
                    let mut procs = processes.lock().await;
                    procs.insert(s1, child);
                }
                Err(e) => error!("Failed to spawn server on port {}: {:?}", port, e),
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
                    info!("Killing dedicated server process for child shards.");
                    let _ = child.kill();
                }
            }

            let port = find_free_port().unwrap_or(7100);
            match spawn_server(port, vec![parent]) {
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
            // Prune dead servers
            fleet_lock.retain(|_, state| now.duration_since(state.last_seen).as_secs() < TTL_SECONDS);
            
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

                if let Err(e) = spawn_server(port, vec![0]) {
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

fn spawn_server(port: u16, shards: Vec<u32>) -> io::Result<std::process::Child> {
    let ds_path = env::var("DS_BINARY_PATH").unwrap_or_else(|_| "cargo".to_string());

    let mut cmd = std::process::Command::new(&ds_path);

    if ds_path == "cargo" {
        cmd.arg("run").arg("-p").arg("dedicated_server").arg("--");
    }

    let shards_str = shards.iter().map(|s| format!("shard:{}", s)).collect::<Vec<_>>().join(",");
    info!("Spawning server on port {} managing shards: {}", port, shards_str);

    let child = cmd.env("DS_PORT", port.to_string())
       .env("SHARDS", shards_str)
       .spawn()?;

    Ok(child)
}
