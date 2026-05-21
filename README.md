# MMORPG Server Architecture - Lab 01

Ce projet implémente une architecture de serveurs de jeu simplifiée pour un MMORPG, répartie en quatre composants distincts qui communiquent entre eux.

## Architecture du Projet

* **`shared`** : Bibliothèque contenant les structures de données communes et sérialisables (`Heartbeat`, `ServerInfo`, etc.).
* **`dedicated_server`** : Serveur de jeu minimaliste propulsé par Bevy 0.18 et `game_sockets`.
* **`orchestrator`** : Gestionnaire de la flotte asynchrone (Tokio) chargé de surveiller et de spawner les serveurs dédiés via Redis.
* **`gatekeeper`** : API REST (Axum) qui sert de point d'entrée unique pour l'authentification et l'aiguillage des joueurs.

# Architecture Cible TP2 — MMO Distribué
## Programmation Réseau Avancée pour Jeux

Ce document décrit la topologie réseau et le flux d'orchestration pour le **TP2**, mettant en œuvre un pattern **PubSub avec Broker**, un **Service Spatial (QuadTree)**, et un mécanisme d'**Autorité Flexible (Ghost Zones)**.

---

## 1. Diagramme d'Architecture Globale

Voici comment s'organisent les composants de l'infrastructure. Contrairement au TP1, le client n'a plus de contact direct avec les serveurs dédiés (*Shards*), tout passe par le **Broker PubSub**.

    ┌────────────────────────────────────────────────────────┐
    │                      CLIENT (fictif)                   │
    │   1. POST /login  ────────────────────────────────┐    │
    │   2. Reçoit { ip_broker, port_broker }            │    │
    │   3. Connexion UDP unique au Broker               │    │
    │      (Envoie ClientInput, Reçoit Broadcast)       │    │
    └───────────────────────────────────────────────────┼────┘
                                                        │
              ┌─────────────────────────────────────────▼───────────┐
              │         GATEKEEPER (Axum/Rocket REST API)           │
              │  POST /login   → vérifie crédentials (dummy)        │
              │                → retourne IP/Port du BROKER         │
              └───────────────────────────┬─────────────────────────┘
                                          │ (Optionnel : vérifie statut global)
                              ┌───────────▼───────────┐
                              │        REDIS          │
                              │  (Orchestration &     │
                              │   Surveillance Flotte)│
                              └───────────▲───────────┘
                                          │ HSET / EXPIRE
                              ┌───────────┴───────────┐
                              │     ORCHESTRATOR      │
                              │ - Spawn les Shards    │
                              │ - Gère la capacité    │
                              │ - Heartbeat polling   │
                              └───────────▲───────────┘
                                          │ heartbeat
        ┌─────────────────────────────────┴─────────────────────────────────┐
        │                                                                   │
    ┌───▼───────────────────────────┐                   ┌───────────────────▼──┐
    │     BROKER (PubSub Router)    │◄── Abonnement ───►│    SERVICE SPATIAL   │
    │ - Seul point d'accès UDP      │   (Sub/Unsub)     │ - Maintient QuadTree │
    │ - Route les ClientInput       │                   │ - Reçoit positions   │
    │ - Dispatche les Broadcasts    │                   │ - Gère les Topics    │
    │ - Table de routage en RAM     │                   │ - Émet CrossingAlert │
    └────▲─────────────────────▲────┘                   └───────────▲──────────┘
         │ Publish/Input       │ Publish/Input                      │ PositionUpdate
         │                     │                                    │
    ┌────▼──────┐         ┌────▼──────┐                             │
    │  SHARD 0  │         │  SHARD 1  │◄────────────────────────────┘
    │ (Zone A)  │◄───────►│ (Zone B)  │
    │ Autorité  │ Handoff │ Ghost     │
    └───────────┘ Request └───────────┘

---

## 2. Diagramme de Séquence : Le protocole de Handoff

Ce diagramme illustre le flux exact des messages binaires lorsqu'un joueur traverse la ligne médiane à l'intérieur d'une zone de transition (*Ghost Zone*) :

      CLIENT             BROKER           SERVICE SPATIAL        SHARD 0             SHARD 1
     (Joueur)          (Routeur UDP)        (QuadTree)        (Autorité actuelle)  (Futur Autorité)
        │                    │                   │                   │                   │
        │ 1. Mouvement (Input)                   │                   │                   │
        ├───────────────────►│                   │                   │                   │
        │                    │ 2. Route l'Input vers l'Autorité      │                   │
        │                    ├──────────────────────────────────────►│                   │
        │                    │                   │                   │ 3. Calcule la physique
        │                    │                   │                   │ (Le joueur franchit 
        │                    │                   │                   │  la ligne médiane)
        │                    │ 4. Publish (Nouvelle Position)        │                   │
        │                    │◄──────────────────────────────────────┤                   │
        │ 5. Broadcast (Vue) │ 6. PositionUpdate │                   │                   │
        │◄───────────────────┼──────────────────►│                   │                   │
        │                    │                   │ 7. Vérifie le QuadTree
        │                    │                   │ (Détecte le changement de feuille)
        │                    │                   │                   │                   │
        │                    │ 8. CrossingAlert  │                   │                   │
        │                    │◄──────────────────┤                   │                   │
        │                    │                   │                   │                   │
        │                    │ 9. Relay HandoffRequest               │                   │
        │                    ├───────────────────────────────────────┼──────────────────►│
        │                    │                   │                   │                   │ 10. Passe de GHOST
        │                    │                   │                   │                   │     à OWNED
        │                    │ 11. HandoffAccept │                   │                   │
        │                    │◄──────────────────────────────────────┼───────────────────┤
        │                    │ 12. Relay Accept  │                   │                   │
        │                    ├──────────────────────────────────────►│                   │
        │                    │                   │                   │ 13. Passe de OWNED
        │                    │                   │                   │     à GHOST

---

## Instructions de Lancement

### Démarrer l'infrastructure complète
Lancez l'instance de notre projet complet grâce au fichier `docker-compose.yml` : 
```bash
docker-compose up --build -d
```
