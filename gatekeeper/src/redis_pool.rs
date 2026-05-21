use deadpool_redis::{redis::AsyncCommands, Pool};
use shared::ServerInfo;

pub async fn find_available_server(pool: &Pool, target_zone: &str) -> Option<ServerInfo> {
    // Find an available server, preferring the target zone.
    let mut conn = pool.get().await.ok()?;

    // Get all keys
    let keys: Vec<String> = conn.keys("server:*").await.ok()?;

    let mut fallback_server = None;

    for key in keys {
        let (ip, port, zone, status): (Option<String>, Option<u16>, Option<String>, Option<String>) =
            redis::pipe()
                .hget(&key, "ip")
                .hget(&key, "port")
                .hget(&key, "zone")
                .hget(&key, "status")
                .query_async(&mut conn)
                .await.ok()?;

        if let (Some(ip), Some(port), Some(zone)) = (ip, port, zone) {
            let status = status.unwrap_or_else(|| "available".to_string());
            if status == "available" {
                let info = ServerInfo { ip, port, zone: zone.clone() };
                if zone == target_zone {
                    return Some(info); // found optimal server
                } else if fallback_server.is_none() {
                    fallback_server = Some(info);
                }
            }
        }
    }

    // Return a server in another zone if target zone has no available server
    fallback_server
}
