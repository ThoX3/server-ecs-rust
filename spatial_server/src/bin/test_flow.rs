use shared::logger::{error, info};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::sleep;

struct MockClient {
    id: u32,
    socket: Arc<UdpSocket>,
}

impl MockClient {
    async fn new(id: u32) -> Self {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        // Subscribe to spatial_updates
        let mut sub_msg = [0u8; 37];
        sub_msg[0] = 0x01;
        sub_msg[1..5].copy_from_slice(&id.to_le_bytes());
        let topic_bytes = b"spatial_updates";
        sub_msg[5..5 + topic_bytes.len()].copy_from_slice(topic_bytes);
        socket.send_to(&sub_msg, "127.0.0.1:9000").await.unwrap();

        Self { id, socket }
    }

    async fn move_to(&self, x: f32, y: f32) {
        let mut pos_msg = [0u8; 14];
        pos_msg[0] = 0x05;
        pos_msg[1..5].copy_from_slice(&self.id.to_le_bytes());
        pos_msg[5] = 0x10;
        pos_msg[6..10].copy_from_slice(&x.to_le_bytes());
        pos_msg[10..14].copy_from_slice(&y.to_le_bytes());
        self.socket.send_to(&pos_msg, "127.0.0.1:9000").await.unwrap();
    }

    async fn disconnect(&self) {
        let mut msg = [0u8; 5];
        msg[0] = 0x07;
        msg[1..5].copy_from_slice(&self.id.to_le_bytes());
        self.socket.send_to(&msg, "127.0.0.1:9000").await.unwrap();
    }

    async fn wait_for_tag(&self, target_tag: u8, timeout_secs: u64) -> Option<Vec<u8>> {
        let mut buf = [0u8; 2048];
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(50), self.socket.recv_from(&mut buf)).await {
                if buf[0] == target_tag {
                    return Some(buf[..len].to_vec());
                }
            }
            // Keep sending position so server doesn't think we are idle/ghosting (if applicable)
        }
        None
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::logger::init_logger("TestFlow");
    info!("Starting Full Integration Test Flow");

    // 0. Pre-build
    info!("[TEST] Pre-building all binaries...");
    let build_status = Command::new("cargo")
        .args(["build", "--workspace"])
        .status()
        .expect("Failed to execute cargo build");

    if !build_status.success() {
        error!("[ERROR] Failed to pre-build workspace!");
        return Err("Build failed".into());
    }

    // 1. Boot Services
    info!("[TEST] Spawning Services...");
    let broker_process = Command::new("./target/debug/broker").spawn().unwrap();
    let gatekeeper_process = Command::new("./target/debug/gatekeeper").spawn().unwrap();
    sleep(Duration::from_millis(1000)).await;

    let spatial_process = Command::new("./target/debug/spatial_server")
        .env("PORT", "9001")
        .env("BROKER_ADDR", "127.0.0.1:9000")
        .env("ORCH_ADDR", "127.0.0.1:8000")
        .spawn().unwrap();
    sleep(Duration::from_millis(500)).await;

    let orchestrator_process = Command::new("./target/debug/orchestrator")
        .env("HOT_SERVERS_MIN", "1")
        .env("ORCH_PORT", "8000")
        .env("DS_BINARY_PATH", "./target/debug/dedicated_server")
        .spawn().unwrap();

    info!("[TEST] Waiting 4 seconds for Orchestrator to boot shard:0...");
    sleep(Duration::from_millis(4000)).await;

    // We skip actual Gatekeeper HTTP login since we know it just assigns random UUIDs
    // and returns the broker IP. We just mock client IDs to control them easily.

    let client_a = MockClient::new(101).await;
    let client_b = MockClient::new(102).await;
    let client_c = MockClient::new(103).await;

    sleep(Duration::from_millis(500)).await;

    info!("=== TEST 1: Initial Spawn & Receiving Broadcast ===");
    let mut joined = false;
    for _ in 0..100 {
        client_a.move_to(0.0, 0.0).await;
        if let Some(_data) = client_a.wait_for_tag(0x04, 1).await {
            info!("[SUCCESS] Client A received initial Broadcast! Joined shard 0.");
            joined = true;
            break;
        }
    }
    if !joined {
        panic!("Client A failed to receive initial Broadcast");
    }

    info!("=== TEST 2: Overpopulating & ScaleUp ===");
    // Move all clients to NW quadrant (-250, 250) to trigger ScaleUp
    // SpatialServer splits when pop > 2. So 3 clients will trigger it!
    client_a.move_to(-250.0, 250.0).await;
    client_b.move_to(-250.0, 250.0).await;
    client_c.move_to(-250.0, 250.0).await;

    info!("Waiting for ScaleUp and Authority migration...");
    // When ScaleUp happens, all 3 clients should receive a new AuthorityChange moving them to shard:1
    let mut scaleup_success = false;
    for _ in 0..100 { // wait up to 10 seconds for orchestrator to boot 4 shards!
        client_a.move_to(-250.0, 250.0).await;
        if let Some(data) = client_a.wait_for_tag(0x12, 1).await {
            let new_shard = u32::from_le_bytes(data[9..13].try_into().unwrap());
            info!("[SUCCESS] Client A migrated to new shard {} after ScaleUp!", new_shard);
            scaleup_success = true;
            break;
        }
    }
    if !scaleup_success {
        panic!("ScaleUp failed or AuthorityChange not received");
    }

    info!("=== TEST 3: Going near Ghost Zones (Margins) ===");
    // Move Client A towards the boundary between NW and NE (X=0). Margin is 50.
    client_a.move_to(-40.0, 250.0).await;
    if let Some(data) = client_a.wait_for_tag(0x11, 3).await { // 0x11 CrossingAlert
        let count = data[5];
        info!("[SUCCESS] Client A received CrossingAlert for {} shards!", count);
    } else {
        panic!("CrossingAlert not received when entering margin");
    }

    info!("=== TEST 4: Going Over ===");
    // Move completely into NE shard (X=10)
    client_a.move_to(10.0, 250.0).await;
    if let Some(data) = client_a.wait_for_tag(0x12, 3).await {
        let new_shard = u32::from_le_bytes(data[9..13].try_into().unwrap());
        info!("[SUCCESS] Client A migrated to shard {} upon crossing boundary!", new_shard);
    } else {
        panic!("AuthorityChange not received when crossing boundary");
    }

    info!("=== TEST 5: Going Back ===");
    client_a.move_to(-10.0, 250.0).await;
    if let Some(data) = client_a.wait_for_tag(0x12, 3).await {
        let new_shard = u32::from_le_bytes(data[9..13].try_into().unwrap());
        info!("[SUCCESS] Client A migrated back to shard {}!", new_shard);
    } else {
        panic!("AuthorityChange not received when going back");
    }

    info!("=== TEST 6: Underpopulating & ScaleDown ===");
    // Clients B and C disconnect
    client_b.disconnect().await;
    client_c.disconnect().await;

    info!("Waiting for ScaleDown and Authority migration to parent...");
    let mut scaledown_success = false;
    for _ in 0..100 { // wait up to 10 seconds for orchestrator to kill children and boot parent
        client_a.move_to(-10.0, 250.0).await;
        if let Some(data) = client_a.wait_for_tag(0x12, 1).await {
            let new_shard = u32::from_le_bytes(data[9..13].try_into().unwrap());
            info!("[SUCCESS] Client A migrated to parent shard {} after ScaleDown!", new_shard);
            scaledown_success = true;
            break;
        }
    }
    if !scaledown_success {
        panic!("ScaleDown failed or AuthorityChange not received");
    }

    info!("[SUCCESS] All Spatial Test Cases Passed Natively!");

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
    let _ = std::process::Command::new("pkill").arg("-f").arg("dedicated_server").output();
}
