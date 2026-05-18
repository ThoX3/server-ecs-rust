# MMORPG Server Architecture - Lab 01

Ce projet implémente une architecture de serveurs de jeu simplifiée pour un MMORPG, répartie en quatre composants distincts qui communiquent entre eux.

## 🏗️ Architecture du Projet

* **`shared`** : Bibliothèque contenant les structures de données communes et sérialisables (`Heartbeat`, `ServerInfo`, etc.).
* **`dedicated_server`** : Serveur de jeu minimaliste propulsé par Bevy 0.18 et `game_sockets`.
* **`orchestrator`** : Gestionnaire de la flotte asynchrone (Tokio) chargé de surveiller et de spawner les serveurs dédiés via Redis.
* **`gatekeeper`** : API REST (Axum) qui sert de point d'entrée unique pour l'authentification et l'aiguillage des joueurs.

---

## 🚀 Instructions de Lancement

Pour démarrer l'ensemble de l'infrastructure de bout en bout, suivez ces étapes dans l'ordre au sein de terminaux distincts :

### 1. Démarrer le Registre Partagé (Redis)
Lancez l'instance Redis officielle en tâche de fond à l'aide de Docker sur le port par défaut :
```bash
docker run -d --name redis-mmorpg -p 6379:6379 redis:7-alpine
```

### 2. Lancer l'Orchestrateur
Pour démarrer l'orchestrateur pour qu'il initialise la flotte minimale de serveurs et commence à écouter les heartbeats UDP :
```bash
cargo run -p orchestrator
```

### 3. Lancer le Gatekeeper (API REST)
Pour démarrer le point d'entrée unique pour permettre aux clients de s'authentifier :
```bash
cargo run -p gatekeeper
```

### 4. Lancer le Dedicated Game Server (Bevy + game_sockets)
Pour lancer un serveur de jeu minimaliste capable d'accepter des connexions de joueurs et d'envoyer un heartbeat périodique à l'orchestrateur :
```bash
cargo run -p dedicated_server
```
