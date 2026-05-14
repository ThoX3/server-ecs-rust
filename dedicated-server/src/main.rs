use bevy::prelude::*;
use bevy::app::MinimalPlugins;

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        // .insert_resource(ServerConfig::from_env()) // À implémenter
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat).chain())
        .run();
}

fn bind_socket() { /* À implémenter */ }
fn receive_packets() { /* À implémenter */ }
fn send_heartbeat() { /* À implémenter - toutes les 5s */ }
