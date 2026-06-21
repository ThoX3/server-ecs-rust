use std::process::Command;
use std::time::Duration;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::sleep;
use shared::logger::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    shared::logger::init_logger("TestFlow");
    info!("Starting End-to-End Sharding Test Flow");
    // 1. Start Broker
    info!("[TEST] Spawning Broker at 0.0.0.0:9000...");
    let mut broker_process = Command::new("cargo")
        .args(["run", "-p", "broker"])
        .spawn()
        .expect("Failed to start broker via cargo");
    
    // Give broker time to start (cargo run takes a bit longer)
    sleep(Duration::from_millis(2000)).await;

    // 2. Start Spatial Server
    info!("[TEST] Spawning Spatial Server at 0.0.0.0:9001...");
    let mut spatial_process = Command::new("cargo")
        .args(["run", "-p", "spatial_server", "--bin", "spatial_server"])
        .env("PORT", "9001")
        .env("BROKER_ADDR", "127.0.0.1:9000")
        .spawn()
        .expect("Failed to start spatial_server via cargo");

    sleep(Duration::from_millis(2000)).await;

    // 3. Create mock Shard sockets
    let shard0 = UdpSocket::bind("127.0.0.1:0").await?; // Shard 0 (NW)
    let shard1 = UdpSocket::bind("127.0.0.1:0").await?; // Shard 1 (NE)
    
    info!("[TEST] Mock Shard 0 bounded at: {:?}", shard0.local_addr()?);
    info!("[TEST] Mock Shard 1 bounded at: {:?}", shard1.local_addr()?);

    // Register shards
    let register_shard = |topic_name: &str| -> [u8; 33] {
        let mut msg = [0u8; 33];
        msg[0] = 0x06; // Tag
        let bytes = topic_name.as_bytes();
        let len = bytes.len().min(32);
        msg[1..1+len].copy_from_slice(&bytes[..len]);
        msg
    };

    shard0.send_to(&register_shard("shard:0"), "127.0.0.1:9000").await?;
    shard1.send_to(&register_shard("shard:1"), "127.0.0.1:9000").await?;
    info!("[TEST] Shards registered with the Broker.");

    // 3.5 Mock Orchestrator UDP Port
    let mock_orch = UdpSocket::bind("127.0.0.1:8000").await?;
    info!("[TEST] Mock Orchestrator listening on 127.0.0.1:8000");

    shard0.send_to(&register_shard("shard:0"), "127.0.0.1:9000").await?;
    shard1.send_to(&register_shard("shard:1"), "127.0.0.1:9000").await?;
    info!("[TEST] Shards registered with the Broker.");

    sleep(Duration::from_millis(500)).await;

    // 4. Create mock Client
    let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    info!("[TEST] Mock Client 42 bounded at: {:?}", client.local_addr()?);

    // Subscribe client 42 to spatial_updates
    let mut sub_msg = [0u8; 37];
    sub_msg[0] = 0x01;
    sub_msg[1..5].copy_from_slice(&42u32.to_le_bytes());
    let topic_bytes = b"spatial_updates";
    sub_msg[5..5 + topic_bytes.len()].copy_from_slice(topic_bytes);
    client.send_to(&sub_msg, "127.0.0.1:9000").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- STEP 1: Position Update -> Subscribing to Shard 0 ---
    info!("--- Step 1: Sending Position Update (-500.0, 500.0) ---");
    let mut pos_msg = [0u8; 14];
    pos_msg[0] = 0x05;
    pos_msg[1..5].copy_from_slice(&42u32.to_le_bytes());
    pos_msg[5] = 0x10;
    pos_msg[6..10].copy_from_slice(&(-500.0f32).to_le_bytes());
    pos_msg[10..14].copy_from_slice(&(500.0f32).to_le_bytes());
    client.send_to(&pos_msg, "127.0.0.1:9000").await?;
    
    // Give it time to route the Subscribe
    sleep(Duration::from_millis(500)).await;

    // --- STEP 2: Client Input routing ---
    info!("--- Step 2: Sending Client Input ---");
    let mut input_msg = vec![0x05];
    input_msg.extend_from_slice(&42u32.to_le_bytes());
    input_msg.extend_from_slice(b"Hello Shard 0!");
    
    client.send_to(&input_msg, "127.0.0.1:9000").await?; // send to broker

    // Listen on Shard 0
    let mut buf = [0u8; 1024];
    if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_secs(1), shard0.recv_from(&mut buf)).await {
        if buf[0] == 0x05 {
            let rec_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let payload = String::from_utf8_lossy(&buf[5..len]);
            info!("[SUCCESS] Shard 0 received input from client {}: {}", rec_client, payload);
        } else {
            error!("[ERROR] Shard 0 received unexpected packet instead of input: {:X}", buf[0]);
        }
    } else {
        error!("[ERROR] Shard 0 did not receive the client input!");
    }

    // --- STEP 3: Crossing Alert ---
    info!("--- Step 3: Moving near borders (-10.0, 10.0) ---");
    let mut pos_msg2 = [0u8; 14];
    pos_msg2[0] = 0x05;
    pos_msg2[1..5].copy_from_slice(&42u32.to_le_bytes());
    pos_msg2[5] = 0x10;
    pos_msg2[6..10].copy_from_slice(&(-10.0f32).to_le_bytes());
    pos_msg2[10..14].copy_from_slice(&(10.0f32).to_le_bytes());
    client.send_to(&pos_msg2, "127.0.0.1:9000").await?;

    // Listen on Shard 0 for CrossingAlert (forwarded by broker)
    let mut found_alert = false;
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), shard0.recv_from(&mut buf)).await {
            if buf[0] == 0x11 {
                let rec_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let num_shards = buf[5];
                let mut near = vec![];
                for i in 0..num_shards {
                    let offset = 6 + (i as usize) * 4;
                    near.push(u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap()));
                }
                info!("[SUCCESS] Shard 0 received Crossing Alert for client {}! Near shards: {:?}", rec_client, near);
                found_alert = true;
                break;
            } else if buf[0] == 0x05 {
                // Ignore routed PositionUpdate
                continue;
            } else {
                error!("[ERROR] Shard 0 received unexpected packet: {:X}", buf[0]);
            }
        }
    }
    if !found_alert {
        error!("[ERROR] Shard 0 did not receive the Crossing Alert!");
    }

    // --- STEP 4: Overpopulation & Splitting (Phase 3) ---
    info!("--- Step 4: Simulating Overpopulation on Shard 0 ---");
    let client2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let client3 = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    
    // Subscribe client2 and client3 to spatial_updates
    let mut sub_msg2 = sub_msg.clone();
    sub_msg2[1..5].copy_from_slice(&43u32.to_le_bytes());
    client2.send_to(&sub_msg2, "127.0.0.1:9000").await?;
    
    let mut sub_msg3 = sub_msg.clone();
    sub_msg3[1..5].copy_from_slice(&44u32.to_le_bytes());
    client3.send_to(&sub_msg3, "127.0.0.1:9000").await?;
    
    sleep(Duration::from_millis(100)).await;

    // Send position updates for client2 and client3 in Shard 0's core
    let mut p2 = pos_msg.clone();
    p2[1..5].copy_from_slice(&43u32.to_le_bytes());
    p2[6..10].copy_from_slice(&(-500.0f32).to_le_bytes());
    p2[10..14].copy_from_slice(&(500.0f32).to_le_bytes());
    client2.send_to(&p2, "127.0.0.1:9000").await?;

    let mut p3 = pos_msg.clone();
    p3[1..5].copy_from_slice(&44u32.to_le_bytes());
    p3[6..10].copy_from_slice(&(-500.0f32).to_le_bytes());
    p3[10..14].copy_from_slice(&(500.0f32).to_le_bytes());
    client3.send_to(&p3, "127.0.0.1:9000").await?;
    
    // This should trigger the ScaleUp packet to our mock Orchestrator!
    let mut found_scaleup = false;
    let mut new_shards_to_boot = vec![];
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), mock_orch.recv_from(&mut buf)).await {
            info!("mock_orch received a packet of {} bytes, tag: {:X}", len, buf[0]);
            if buf[0] == 0x13 && len >= 21 {
                let parent = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
                let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
                info!("[SUCCESS] Orchestrator received ScaleUp from Shard {} -> [{}, {}, {}, {}]", parent, s1, s2, s3, s4);
                found_scaleup = true;
                new_shards_to_boot = vec![s1, s2, s3, s4];
                break;
            }
        }
    }
    if !found_scaleup {
        error!("[ERROR] Orchestrator did not receive ScaleUp packet!");
    }

    // --- STEP 5: New Shards Booting (Phase 3) ---
    info!("--- Step 5: New Dedicated Server boots and sends Ready signals ---");
    for s in &new_shards_to_boot {
        // First register them
        shard0.send_to(&register_shard(&format!("shard:{}", s)), "127.0.0.1:9000").await?;
        // Then send Ready
        let mut ready_msg = [0u8; 5];
        ready_msg[0] = 0x14;
        ready_msg[1..5].copy_from_slice(&s.to_le_bytes());
        shard0.send_to(&ready_msg, "127.0.0.1:9000").await?;
    }
    sleep(Duration::from_millis(500)).await;

    // --- STEP 6: Handoff Verification ---
    info!("--- Step 6: Verifying Handoff routing ---");
    // Client 43 sends another position update
    client2.send_to(&p2, "127.0.0.1:9000").await?;
    // Client 42 sends another position update to transition to Shard 4
    client.send_to(&pos_msg, "127.0.0.1:9000").await?;
    
    // Listen on Shard 0 (which was the parent) to verify it received the AuthorityChange to drop them
    // Wait, SpatialServer sends the AuthorityChange to Broker, Broker forwards it to the *client's subscribed topics' authorities*.
    // The clients were in Shard 0, so Shard 0 will receive `0x12`.
    let mut found_auth_split = false;
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), shard0.recv_from(&mut buf)).await {
            info!("shard0 received a packet of {} bytes, tag: {:X}", len, buf[0]);
            if buf[0] == 0x12 {
                let rec_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let old_shard = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let new_shard = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                info!("[SUCCESS] Shard 0 received Authority Change after Split for client {}: {} -> {}", rec_client, old_shard, new_shard);
                found_auth_split = true;
                break;
            }
        }
    }
    
    if !found_auth_split {
        error!("[ERROR] AuthorityChange was not broadcasted after the Split and Ready handoff!");
    }

    // --- STEP 7: Merging (Phase 4) ---
    info!("--- Step 7: Disconnecting clients to trigger Merge ---");
    // Send Disconnect (0x07) directly to Spatial Server for client2 and client3
    let mut dis_msg = [0u8; 5];
    dis_msg[0] = 0x07;
    
    dis_msg[1..5].copy_from_slice(&43u32.to_le_bytes());
    client2.send_to(&dis_msg, "127.0.0.1:9001").await?;
    
    dis_msg[1..5].copy_from_slice(&44u32.to_le_bytes());
    client3.send_to(&dis_msg, "127.0.0.1:9001").await?;
    
    // This leaves only client 42, total population = 1 (which matches MIN_POPULATION = 1)
    // Wait for Orchestrator to receive ScaleDown (0x15)
    let mut found_scaledown = false;
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), mock_orch.recv_from(&mut buf)).await {
            if buf[0] == 0x15 && len >= 21 {
                let parent = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let s1 = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let s2 = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                let s3 = u32::from_le_bytes(buf[13..17].try_into().unwrap());
                let s4 = u32::from_le_bytes(buf[17..21].try_into().unwrap());
                info!("[SUCCESS] Orchestrator received ScaleDown! Merging [{}, {}, {}, {}] into {}", s1, s2, s3, s4, parent);
                found_scaledown = true;
                break;
            }
        }
    }
    
    if !found_scaledown {
        error!("[ERROR] Orchestrator did not receive ScaleDown packet!");
    }

    info!("--- Step 8: Parent Server Boots and takes Authority ---");
    // Send Ready for Shard 0
    let mut ready_msg = [0u8; 5];
    ready_msg[0] = 0x14;
    ready_msg[1..5].copy_from_slice(&0u32.to_le_bytes()); // parent shard 0
    shard0.send_to(&ready_msg, "127.0.0.1:9000").await?; // to broker
    
    sleep(Duration::from_millis(500)).await;
    
    // Client 42 sends a position update, which should trigger the AuthorityChange back to Shard 0!
    client.send_to(&pos_msg, "127.0.0.1:9000").await?;
    
    let mut found_auth_merge = false;
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_millis(100), shard0.recv_from(&mut buf)).await {
            if buf[0] == 0x12 {
                let rec_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
                let old_shard = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let new_shard = u32::from_le_bytes(buf[9..13].try_into().unwrap());
                if new_shard == 0 {
                    info!("[SUCCESS] Shard 0 received Authority Change after Merge for client {}: {} -> {}", rec_client, old_shard, new_shard);
                    found_auth_merge = true;
                    break;
                }
            }
        }
    }
    
    if !found_auth_merge {
        error!("[ERROR] AuthorityChange was not broadcasted after the Merge and Ready handoff!");
    }

    info!("Test flow completed. Shutting down...");
    let _ = broker_process.kill();
    let _ = spatial_process.kill();

    Ok(())
}
