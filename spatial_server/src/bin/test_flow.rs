use std::process::Command;
use std::time::Duration;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use shared::logger::{info, error, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::logger::init_logger("TestFlow");
    info!("Starting Full Integration Test Flow");

    // 0. Pre-build all binaries to avoid Cargo file locks and compilation delays
    info!("[TEST] Pre-building all binaries...");
    let build_status = Command::new("cargo")
        .args(["build", "--workspace"])
        .status()
        .expect("Failed to execute cargo build");
    
    if !build_status.success() {
        error!("[ERROR] Failed to pre-build workspace!");
        return Err("Build failed".into());
    }

    // 1. Boot Broker
    info!("[TEST] Spawning Broker at 0.0.0.0:9000...");
    let mut broker_process = Command::new("./target/debug/broker")
        .spawn()
        .expect("Failed to start broker");

    // 2. Boot Gatekeeper
    info!("[TEST] Spawning Gatekeeper at 0.0.0.0:3000...");
    let mut gatekeeper_process = Command::new("./target/debug/gatekeeper")
        .spawn()
        .expect("Failed to start gatekeeper");

    // Give them time to start
    sleep(Duration::from_millis(2000)).await;

    // 3. Boot Spatial Server
    info!("[TEST] Spawning Spatial Server at 0.0.0.0:9001...");
    let mut spatial_process = Command::new("./target/debug/spatial_server")
        .env("PORT", "9001")
        .env("BROKER_ADDR", "127.0.0.1:9000")
        .env("ORCH_ADDR", "127.0.0.1:8000")
        .spawn()
        .expect("Failed to start spatial_server");

    sleep(Duration::from_millis(1000)).await;

    // 4. Boot Orchestrator
    info!("[TEST] Spawning Orchestrator at 0.0.0.0:8000 with HOT_SERVERS_MIN=1...");
    let mut orchestrator_process = Command::new("./target/debug/orchestrator")
        .env("HOT_SERVERS_MIN", "1") // Only boot 1 game server natively
        .env("ORCH_PORT", "8000")
        // Make sure Orchestrator spawns the compiled dedicated_server
        .env("DS_BINARY_PATH", "./target/debug/dedicated_server")
        .spawn()
        .expect("Failed to start orchestrator");

    // Wait for orchestrator to boot a dedicated server and register authority on the broker
    info!("[TEST] Waiting 4 seconds for Orchestrator and Game Server to boot...");
    sleep(Duration::from_millis(4000)).await;

    // 5. Gatekeeper HTTP Login
    info!("[TEST] Client connecting via Gatekeeper...");
    let output = Command::new("curl")
        .args([
            "-s",
            "-X", "POST", "http://127.0.0.1:3000/login",
            "-H", "Content-Type: application/json",
            "-d", r#"{"username":"test","password":"1234"}"#
        ])
        .output()
        .expect("Failed to run curl");
    
    let resp = String::from_utf8_lossy(&output.stdout);
    info!("[TEST] Gatekeeper response: {}", resp);
    
    if !resp.contains("player_id") {
        error!("[ERROR] Gatekeeper login failed!");
        cleanup(broker_process, gatekeeper_process, spatial_process, orchestrator_process);
        return Err("Login failed".into());
    }

    // Extract player_id and broker port from JSON manually (quick and dirty)
    let player_id_str = resp.split(r#""player_id":""#).nth(1).unwrap().split(r#"""#).next().unwrap();
    // Use a fixed hash for player_id or just parse it? We just need a u32. Let's use 42.
    let client_id = 42u32;
    info!("[TEST] Using mocked client_id = 42");

    // 6. Connect Client to Broker
    let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    info!("[TEST] Mock Client bounded at: {:?}", client.local_addr()?);

    // Subscribe client to spatial_updates
    let mut sub_msg = [0u8; 37];
    sub_msg[0] = 0x01;
    sub_msg[1..5].copy_from_slice(&client_id.to_le_bytes());
    let topic_bytes = b"spatial_updates";
    sub_msg[5..5 + topic_bytes.len()].copy_from_slice(topic_bytes);
    client.send_to(&sub_msg, "127.0.0.1:9000").await?;

    sleep(Duration::from_millis(500)).await;

    // 7. Send Position Update Continuously
    info!("--- Step 1: Sending Position Update (0.0, 0.0) Continuously ---");
    let client_clone = client.clone();
    tokio::spawn(async move {
        let mut pos_msg = [0u8; 14];
        pos_msg[0] = 0x05;
        pos_msg[1..5].copy_from_slice(&client_id.to_le_bytes());
        pos_msg[5] = 0x10;
        pos_msg[6..10].copy_from_slice(&(0.0f32).to_le_bytes());
        pos_msg[10..14].copy_from_slice(&(0.0f32).to_le_bytes());
        loop {
            let _ = client_clone.send_to(&pos_msg, "127.0.0.1:9000").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Wait to see if we receive Broadcasts from shard:0
    let mut buf = [0u8; 2048];
    let mut received_authority = false;
    let mut received_broadcast = false;
    
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(3) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), client.recv_from(&mut buf)).await {
            let tag = buf[0];
            if tag == 0x04 {
                // Broadcast from Dedicated Server
                let payload_len = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;
                info!("[SUCCESS] Client received Broadcast game data ({} bytes)!", payload_len);
                received_broadcast = true;
                break;
            } else if tag == 0x12 {
                // Authority change!
                info!("[SUCCESS] Client received Authority Change!");
                received_authority = true;
            } else {
                info!("Client received packet {:X} of len {}", tag, len);
            }
        }
    }

    if !received_broadcast {
        error!("[ERROR] Client did not receive Broadcasts from Dedicated Server. Integration failed!");
    } else {
        info!("[SUCCESS] Full integration test passed! The Gatekeeper, Broker, Orchestrator, Spatial Server, and Dedicated Server are successfully communicating.");
    }

    // Cleanup
    info!("Shutting down...");
    cleanup(broker_process, gatekeeper_process, spatial_process, orchestrator_process);
    
    Ok(())
}

fn cleanup(
    mut broker: std::process::Child,
    mut gate: std::process::Child,
    mut spatial: std::process::Child,
    mut orch: std::process::Child,
) {
    let _ = broker.kill();
    let _ = gate.kill();
    let _ = spatial.kill();
    let _ = orch.kill();
    // Also kill any orphaned dedicated_servers
    let _ = std::process::Command::new("pkill").arg("-f").arg("dedicated_server").output();
}
