use redis::AsyncCommands;
use shared::Heartbeat;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::Arc;
use std::path::Path;
use tokio::net::UdpSocket;
use tokio::time::{interval, Duration};

const TTL_SECONDS: u64 = 15;        // Temps avant qu'un serveur soit considéré comme mort
const SCALER_INTERVAL: u64 = 5;     // Fréquence de vérification de la flotte (en secondes)
const HOT_SERVERS_MIN: usize = 2;   // Nombre minimal de serveurs vides requis
const BASE_DS_PORT: u16 = 7001;     // Port de départ pour attribuer aux nouveaux serveurs

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Démarrage de l'orchestrateur...");

    let orch_port = std::env::var("ORCH_PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse::<u16>()
        .unwrap_or(8000);

    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", orch_port)).await?);
    println!("Orchestrateur à l'écoute sur le port UDP {}", orch_port);

    // Client Redis partagé entre les tâches
    let redis_client_raw = redis::Client::open("redis://127.0.0.1:6379/")?;
    let redis_client_shared = Arc::new(redis_client_raw);
    // -----------------------------------------------

    let listener_socket = socket.clone();

    let listener_redis = redis_client_shared.clone();

    // Écoute des Heartbeats
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_listener(listener_socket, listener_redis).await {
            eprintln!("Erreur dans la tâche heartbeat_listener: {:?}", e);
        }
    });

    // Surveillance et Scaling de la flotte
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

fn spawn_server(port: u16) -> std::io::Result<()> {
    let exe_name = if std::env::consts::EXE_EXTENSION.is_empty() {
        "dedicated_server".to_string()
    } else {
        format!("dedicated_server.{}", std::env::consts::EXE_EXTENSION)
    };

    let bin_path = Path::new(".").join("target").join("debug").join(exe_name);

    if !bin_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Le binaire est introuvable au chemin : {:?}. As-tu fait un 'cargo build' ?", bin_path)
        ));
    }

    println!("Création d'un sous-processus pour le serveur dédié sur le port {}...", port);

    Command::new(&bin_path)
        .env("DS_PORT", port.to_string())
        .spawn()?;

    println!("Processus serveur instancié avec succès en tâche de fond !");
    Ok(())
}
