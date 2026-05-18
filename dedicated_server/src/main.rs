use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        //.insert_resource(ServerConfig::from_env())
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat).chain())
        .run();
}

fn bind_socket() {
    println!("Startup: Liaison du socket UDP...");
}

fn receive_packets() {
    // Logique de réception des paquets
}

fn send_heartbeat() {
    // Logique d'envoi du heartbeat toutes les 5 secondes
}
