use redis::AsyncCommands;
use shared::Heartbeat;
use std::env;
use std::io;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};

const TTL_SECONDS: u64 = 15;        // Temps avant qu'un serveur soit considéré comme mort
const SCALER_INTERVAL: u64 = 5;     // Fréquence de vérification de la flotte (en secondes)
const HOT_SERVERS_MIN: usize = 2;   // Nombre minimal de serveurs vides requis
const BASE_DS_PORT: u16 = 7001;     // Port de départ pour attribuer aux nouveaux serveurs

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Boot the orchestrator and its tasks.
    println!("Démarrage de l'orchestrateur...");

    let orch_port = std::env::var("ORCH_PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse::<u16>()
        .unwrap_or(8000);

    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", orch_port)).await?);
    println!("Orchestrateur à l'écoute sur le port UDP {}", orch_port);

    // Client Redis partagé entre les tâches
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let redis_client_raw = redis::Client::open(redis_url)?;
    let redis_client_shared = Arc::new(redis_client_raw);

    let listener_socket = socket.clone();

    let listener_redis = redis_client_shared.clone();

    // Listen for heartbeats.
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_listener(listener_socket, listener_redis).await {
            eprintln!("Erreur dans la tâche heartbeat_listener: {:?}", e);
        }
    });

    // Monitor and scale the fleet.
    let scaler_handle = tokio::spawn(async move {
        if let Err(e) = scaler_loop(redis_client_shared).await {
            eprintln!("Erreur dans la tâche scaler_loop: {:?}", e);
        }
    });

    tokio::try_join!(heartbeat_handle, scaler_handle)?;

    Ok(())
}

async fn heartbeat_listener(
    socket: Arc<UdpSocket>,
    redis_client: Arc<redis::Client>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Receive heartbeats and update Redis.
    let mut buf = [0u8; 2048];
    let mut con = redis_client.get_multiplexed_async_connection().await?;

    loop {
        let (len, _sender_addr) = socket.recv_from(&mut buf).await?;

        if let Ok(hb) = serde_json::from_slice::<Heartbeat>(&buf[..len]) {
            let redis_key = format!("server:{}", hb.id);

            let status = if hb.player_count >= hb.max_players { "full" } else { "available" };

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

            println!("Heartbeat traité pour le serveur {} (Statut: {})", hb.id, status);
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
                println!("Flotte actuelle : {} serveurs disponibles (Requis minimum : {})", available_count, HOT_SERVERS_MIN);

                if available_count < HOT_SERVERS_MIN {
                    let needed = HOT_SERVERS_MIN - available_count;
                    println!("Alerte sous-effectif ! Lancement de {} serveur(s) dédié(s)...", needed);

                    for _ in 0..needed {
                        let port = next_port_to_use;
                        next_port_to_use += 1;

                        if let Err(e) = spawn_server(port) {
                            eprintln!("Impossible de spawner le serveur sur le port {}: {:?}", port, e);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Erreur lors du calcul de la flotte dans Redis : {:?}", e),
        }
    }
}

async fn count_available_servers(redis_client: Arc<redis::Client>) -> Result<usize, redis::RedisError> {
    // Count available servers in Redis.
    let mut con = redis_client.get_multiplexed_async_connection().await?;
    let mut available_count = 0;

    let mut cmd = redis::cmd("SCAN");
    cmd.arg(0).arg("MATCH").arg("server:*").arg("COUNT").arg(100);

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
        next_cmd.arg(cursor).arg("MATCH").arg("server:*").arg("COUNT").arg(100);
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

fn spawn_server(port: u16) -> io::Result<()> {
    let ds_path = env::var("DS_BINARY_PATH").unwrap_or_else(|_| "cargo".to_string());

    let mut cmd = std::process::Command::new(&ds_path);

    if ds_path == "cargo" {
        cmd.arg("run").arg("-p").arg("dedicated_server").arg("--");
    }

    cmd.env("DS_PORT", port.to_string()).spawn()?;

    Ok(())
}
