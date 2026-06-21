use redis::AsyncCommands;
use shared::Heartbeat;
use std::env;
use std::io;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use std::collections::HashMap;
use shared::logger::{info, warn, error};
use tokio::time::{interval, Duration};

const TTL_SECONDS: u64 = 15; // Temps avant qu'un serveur soit considéré comme mort
const SCALER_INTERVAL: u64 = 5; // Fréquence de vérification de la flotte (en secondes)
const HOT_SERVERS_MIN: usize = 2; // Nombre minimal de serveurs vides requis
const BASE_DS_PORT: u16 = 7001; // Port de départ pour attribuer aux nouveaux serveurs

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

    // Client Redis partagé entre les tâches
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let redis_client_raw = redis::Client::open(redis_url)?;
    let redis_client_shared = Arc::new(redis_client_raw);

    let listener_socket = socket.clone();

    let listener_redis = redis_client_shared.clone();

    // Store server processes to allow killing them on ScaleDown
    let processes: Arc<Mutex<HashMap<u32, std::process::Child>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener_processes = processes.clone();

    // Listen for heartbeats.
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_listener(listener_socket, listener_redis, listener_processes).await {
            error!("Erreur dans la tâche heartbeat_listener: {:?}", e);
        }
    });

    // Monitor and scale the fleet.
    let scaler_handle = tokio::spawn(async move {
        if let Err(e) = scaler_loop(redis_client_shared).await {
            error!("Erreur dans la tâche scaler_loop: {:?}", e);
        }
    });

    tokio::try_join!(heartbeat_handle, scaler_handle)?;

    Ok(())
}

async fn heartbeat_listener(
    socket: Arc<UdpSocket>,
    redis_client: Arc<redis::Client>,
    processes: Arc<Mutex<HashMap<u32, std::process::Child>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Receive heartbeats and update Redis.
    let mut buf = [0u8; 2048];
    let mut con = redis_client.get_multiplexed_async_connection().await?;

    loop {
        let (len, _sender_addr) = socket.recv_from(&mut buf).await?;
        if len == 0 { continue; }
        
        if buf[0] == 0x13 && len >= 21 {
            // ScaleUp: 0x13 | parent(4) | s1(4) | s2(4) | s3(4) | s4(4)
            let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
            let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
            let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
            info!("Received ScaleUp instruction for shards {}, {}, {}, {}", s1, s2, s3, s4);
            
            // Find a port
            let port = find_free_port().unwrap_or(7100);
            match spawn_server(port, vec![s1, s2, s3, s4]) {
                Ok(child) => {
                    let mut procs = processes.lock().await;
                    procs.insert(s1, child);
                    // Only need to map one of the shards to kill the process later
                }
                Err(e) => error!("Failed to spawn server on port {}: {:?}", port, e),
            }
            continue;
        }

        if buf[0] == 0x15 && len >= 21 {
            // ScaleDown: 0x15 | parent(4) | s1(4) | s2(4) | s3(4) | s4(4)
            let parent = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
            let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
            let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
            info!("Received ScaleDown instruction. Merging shards {}, {}, {}, {} into parent {}", s1, s2, s3, s4, parent);
            
            // Kill the old server that was managing these shards
            {
                let mut procs = processes.lock().await;
                if let Some(mut child) = procs.remove(&s1) {
                    info!("Killing dedicated server process for child shards.");
                    let _ = child.kill();
                }
            }

            // Spawn the parent server
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
            let redis_key = format!("server:{}", hb.id);

            let status = if hb.player_count >= hb.max_players {
                "full"
            } else {
                "available"
            };

            let _: () = redis::pipe()
                .atomic()
                .hset(&redis_key, "id", &hb.id)
                .hset(&redis_key, "ip", &hb.ip)
                .hset(&redis_key, "port", hb.port)
                .hset(&redis_key, "zone", &hb.zone)
                .hset(&redis_key, "status", status)
                .hset(&redis_key, "players", hb.player_count)
                .expire(&redis_key, TTL_SECONDS as i64)
                .query_async(&mut con)
                .await?;

            info!(
                "Heartbeat traité pour le serveur {} (Statut: {})",
                hb.id, status
            );
        }
    }
}

async fn scaler_loop(redis_client: Arc<redis::Client>) -> Result<(), Box<dyn std::error::Error>> {
    // Periodically ensure the fleet size.
    let mut interval = interval(Duration::from_secs(SCALER_INTERVAL));
    let mut next_port_to_use = BASE_DS_PORT;

    loop {
        interval.tick().await;

        match count_available_servers(redis_client.clone()).await {
            Ok(available_count) => {
                info!(
                    "Flotte actuelle : {} serveurs disponibles (Requis minimum : {})",
                    available_count, HOT_SERVERS_MIN
                );

                if available_count < HOT_SERVERS_MIN {
                    let needed = HOT_SERVERS_MIN - available_count;
                    warn!(
                        "Alerte sous-effectif ! Lancement de {} serveur(s) dédié(s)...",
                        needed
                    );

                    for _ in 0..needed {
                        let port = next_port_to_use;
                        next_port_to_use += 1;

                        // Spawn a basic server for shard:0 as fallback
                        if let Err(e) = spawn_server(port, vec![0]) {
                            error!(
                                "Impossible de spawner le serveur sur le port {}: {:?}",
                                port, e
                            );
                        }
                    }
                }
            }
            Err(e) => error!("Erreur lors du calcul de la flotte dans Redis : {:?}", e),
        }
    }
}

async fn count_available_servers(
    redis_client: Arc<redis::Client>,
) -> Result<usize, redis::RedisError> {
    // Count available servers in Redis.
    let mut con = redis_client.get_multiplexed_async_connection().await?;
    let mut available_count = 0;

    let mut cmd = redis::cmd("SCAN");
    cmd.arg(0)
        .arg("MATCH")
        .arg("server:*")
        .arg("COUNT")
        .arg(100);

    let (mut cursor, keys): (u64, Vec<String>) = cmd.query_async(&mut con).await?;

    for key in &keys {
        let status: Option<String> = con.hget(key, "status").await?;
        if let Some(s) = status {
            if s == "available" {
                available_count += 1;
            }
        }
    }

    while cursor != 0 {
        let mut next_cmd = redis::cmd("SCAN");
        next_cmd
            .arg(cursor)
            .arg("MATCH")
            .arg("server:*")
            .arg("COUNT")
            .arg(100);
        let (next_cursor, next_keys): (u64, Vec<String>) = next_cmd.query_async(&mut con).await?;
        cursor = next_cursor;

        for key in &next_keys {
            let status: Option<String> = con.hget(key, "status").await?;
            if let Some(s) = status {
                if s == "available" {
                    available_count += 1;
                }
            }
        }
    }

    Ok(available_count)
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
