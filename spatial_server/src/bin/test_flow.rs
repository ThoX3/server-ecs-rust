use std::process::Command;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Starting End-to-End Sharding Test Flow ===\n");

    // 1. Start Broker
    println!("[TEST] Spawning Broker at 0.0.0.0:9000...");
    let mut broker_process = Command::new("cargo")
        .args(["run", "-p", "broker"])
        .spawn()
        .expect("Failed to start broker via cargo");
    
    // Give broker time to start (cargo run takes a bit longer)
    sleep(Duration::from_millis(2000)).await;

    // 2. Start Spatial Server
    println!("[TEST] Spawning Spatial Server at 0.0.0.0:9001...");
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
    
    println!("[TEST] Mock Shard 0 bounded at: {:?}", shard0.local_addr()?);
    println!("[TEST] Mock Shard 1 bounded at: {:?}", shard1.local_addr()?);

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
    println!("[TEST] Shards registered with the Broker.");

    sleep(Duration::from_millis(500)).await;

    // 4. Create mock Client
    let client = UdpSocket::bind("127.0.0.1:0").await?;
    let client_id: u32 = 42;
    println!("[TEST] Mock Client {} bounded at: {:?}", client_id, client.local_addr()?);

    // --- STEP 1: Position Update -> Subscribing to Shard 0 ---
    println!("\n--- Step 1: Sending Position Update (-500.0, 500.0) ---");
    let mut pos_msg = vec![0x10];
    pos_msg.extend_from_slice(&client_id.to_le_bytes());
    pos_msg.extend_from_slice(&(-500.0f32).to_le_bytes());
    pos_msg.extend_from_slice(&(500.0f32).to_le_bytes());
    
    client.send_to(&pos_msg, "127.0.0.1:9001").await?;
    
    // Give it time to route the Subscribe
    sleep(Duration::from_millis(500)).await;

    // --- STEP 2: Client Input routing ---
    println!("\n--- Step 2: Sending Client Input ---");
    let mut input_msg = vec![0x05];
    input_msg.extend_from_slice(&client_id.to_le_bytes());
    input_msg.extend_from_slice(b"Hello Shard 0!");
    
    client.send_to(&input_msg, "127.0.0.1:9000").await?; // send to broker

    // Listen on Shard 0
    let mut buf = [0u8; 1024];
    if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_secs(1), shard0.recv_from(&mut buf)).await {
        let rec_client = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let payload = String::from_utf8_lossy(&buf[4..len]);
        println!("[SUCCESS] Shard 0 received input from client {}: {}", rec_client, payload);
    } else {
        println!("[ERROR] Shard 0 did not receive the client input!");
    }

    // --- STEP 3: Crossing Alert ---
    println!("\n--- Step 3: Moving near borders (-10.0, 10.0) ---");
    let mut pos_msg2 = vec![0x10];
    pos_msg2.extend_from_slice(&client_id.to_le_bytes());
    pos_msg2.extend_from_slice(&(-10.0f32).to_le_bytes());
    pos_msg2.extend_from_slice(&(10.0f32).to_le_bytes());
    
    client.send_to(&pos_msg2, "127.0.0.1:9001").await?;

    // Listen on Shard 0 for CrossingAlert (forwarded by broker)
    if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_secs(1), shard0.recv_from(&mut buf)).await {
        if buf[0] == 0x11 {
            let rec_client = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let num_shards = buf[5];
            let mut near_shards = Vec::new();
            for i in 0..num_shards as usize {
                let start = 6 + i*4;
                let s_id = u32::from_le_bytes(buf[start..start+4].try_into().unwrap());
                near_shards.push(s_id);
            }
            println!("[SUCCESS] Shard 0 received Crossing Alert for client {}! Near shards: {:?}", rec_client, near_shards);
        } else {
            println!("[ERROR] Shard 0 received unexpected packet: {:X}", buf[0]);
        }
    } else {
        println!("[ERROR] Shard 0 did not receive the crossing alert!");
    }

    // --- STEP 4: Handoff to Shard 1 ---
    println!("\n--- Step 4: Handoff to Shard 1 (500.0, 500.0) ---");
    let mut pos_msg3 = vec![0x10];
    pos_msg3.extend_from_slice(&client_id.to_le_bytes());
    pos_msg3.extend_from_slice(&(500.0f32).to_le_bytes());
    pos_msg3.extend_from_slice(&(500.0f32).to_le_bytes());
    
    client.send_to(&pos_msg3, "127.0.0.1:9001").await?;
    
    sleep(Duration::from_millis(500)).await;

    // --- STEP 5: Verify new routing ---
    println!("\n--- Step 5: Sending Client Input after handoff ---");
    let mut input_msg2 = vec![0x05];
    input_msg2.extend_from_slice(&client_id.to_le_bytes());
    input_msg2.extend_from_slice(b"Hello Shard 1!");
    
    client.send_to(&input_msg2, "127.0.0.1:9000").await?;

    if let Ok(Ok((len, _))) = tokio::time::timeout(Duration::from_secs(1), shard1.recv_from(&mut buf)).await {
        let rec_client = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let payload = String::from_utf8_lossy(&buf[4..len]);
        println!("[SUCCESS] Shard 1 received input from client {}: {}", rec_client, payload);
    } else {
        println!("[ERROR] Shard 1 did not receive the client input!");
    }

    println!("\n=== Test flow completed. Shutting down... ===");
    let _ = broker_process.kill();
    let _ = spatial_process.kill();

    Ok(())
}
