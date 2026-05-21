# MMORPG Server Architecture - Lab 01

Ce projet implémente une architecture de serveurs de jeu simplifiée pour un MMORPG, répartie en quatre composants distincts qui communiquent entre eux.

## Architecture du Projet

* **`shared`** : Bibliothèque contenant les structures de données communes et sérialisables (`Heartbeat`, `ServerInfo`, etc.).
* **`dedicated_server`** : Serveur de jeu minimaliste propulsé par Bevy 0.18 et `game_sockets`.
* **`orchestrator`** : Gestionnaire de la flotte asynchrone (Tokio) chargé de surveiller et de spawner les serveurs dédiés via Redis.
* **`gatekeeper`** : API REST (Axum) qui sert de point d'entrée unique pour l'authentification et l'aiguillage des joueurs.

---

## Instructions de Lancement

### Démarrer l'infrastructure complète
Lancez l'instance de notre projet complet grâce au fichier `docker-compose.yml` : 
```bash
docker-compose up --build -d
```
