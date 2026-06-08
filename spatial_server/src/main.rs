use std::env;
use std::sync::Arc;
use tokio::net::UdpSocket;
use bevy::math::{Rect, Vec2};
use shared::quadtree::{QuadTree, SpatialService, SpatialAction, PositionUpdate};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Spatial Server starting...");

    let port = env::var("PORT").unwrap_or_else(|_| "9001".to_string());
    let broker_addr = env::var("BROKER_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".to_string());

    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{}", port)).await?);
    println!("Listening on port {}", port);
    println!("Broker address: {}", broker_addr);

    // Initialisation du QuadTree
    // Monde de -1000 à 1000
    let bounds = Rect::from_center_size(Vec2::ZERO, Vec2::new(2000.0, 2000.0));
    let mut quadtree = QuadTree::new(bounds, 0, 2);
    quadtree.split(); // split au moins une fois pour avoir 4 shards (0, 1, 2, 3)
    let mut next_id = 0;
    quadtree.assign_shards(&mut next_id);

    let mut spatial_service = SpatialService::new(quadtree, 50.0); // Margin de 50.0

    let mut buf = [0u8; 1024];

    loop {
        let (len, _src) = socket.recv_from(&mut buf).await?;
        if len < 1 { continue; }

        let tag = buf[0];
        if tag == 0x10 {
            // PositionUpdate: 0x10 | client_id(4) | x(4) | y(4)
            if len < 13 { continue; }
            let client_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
            let x = f32::from_le_bytes(buf[5..9].try_into().unwrap());
            let y = f32::from_le_bytes(buf[9..13].try_into().unwrap());

            let update = PositionUpdate { client_id, x, y };
            let actions = spatial_service.handle_position_update(&update);

            for action in actions {
                match action {
                    SpatialAction::Subscribe { client_id, topic } => {
                        let mut msg = [0u8; 37];
                        msg[0] = 0x01;
                        msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                        msg[5..37].copy_from_slice(&topic);
                        let _ = socket.send_to(&msg, &broker_addr).await;
                    }
                    SpatialAction::Unsubscribe { client_id, topic } => {
                        let mut msg = [0u8; 37];
                        msg[0] = 0x02;
                        msg[1..5].copy_from_slice(&client_id.to_le_bytes());
                        msg[5..37].copy_from_slice(&topic);
                        let _ = socket.send_to(&msg, &broker_addr).await;
                    }
                    SpatialAction::CrossingAlert { client_id, shards } => {
                        // CrossingAlert: 0x11 | client_id(4) | num_shards(1) | shard_ids...
                        let mut msg = vec![0x11];
                        msg.extend_from_slice(&client_id.to_le_bytes());
                        msg.push(shards.len() as u8);
                        for shard_id in shards {
                            msg.extend_from_slice(&shard_id.to_le_bytes());
                        }
                        let _ = socket.send_to(&msg, &broker_addr).await;
                    }
                }
            }
        }
    }
}
