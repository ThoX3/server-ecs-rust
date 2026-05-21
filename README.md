# MMORPG Server Architecture - Lab 01

Ce projet implémente une architecture de serveurs de jeu simplifiée pour un MMORPG, répartie en quatre composants distincts qui communiquent entre eux.

## Architecture du Projet

### `shared`
* **Rôle :** Bibliothèque centrale de types et structures de données communes.

### `dedicated_server` (Le Shard)
* **Rôle :** Serveur de jeu minimaliste propulsé par Bevy 0.18.

### `orchestrator`
* **Rôle :** Gestionnaire asynchrone de la flotte (Tokio).

### `gatekeeper`
* **Rôle :** API REST (Axum) servant de point d'entrée unique.

### `broker`
* **Rôle :** Courtier réseau PubSub binaire utilisant `game_sockets`.

### `spatial_server`
* **Rôle :** Gestionnaire de l'Area of Interest (AoI) et du partitionnement.

---

1. **Connexion :** Client ➔ Gatekeeper (HTTP) ➔ Reçoit l'IP et le Port du Broker.
2. **Boucle de Jeu :** Client ➔ Broker (UDP `ClientInput`) ➔ Shard Autoritaire (`Owned`).
3. **Réplication :** Shard ➔ Broker (UDP `Publish`) ➔ Clients Abonnés (UDP `Broadcast`).
4. **Abonnement Spatial :** Shard ➔ Spatial Server (UDP `PositionUpdate`) ➔ QuadTree ➔ Broker (Commandes `Subscribe` / `Unsubscribe`).

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

## 2. Diagramme de Séquence : Authentification et Protocole de Handoff

Ce diagramme illustre le cycle de vie complet : de la connexion initiale du joueur via l'API REST jusqu'au flux des messages binaires UDP lors du franchissement de la ligne médiane dans la zone de transition (*Ghost Zone*).

  CLIENT          GATEKEEPER         BROKER       SERVICE SPATIAL      SHARD 0          SHARD 1
 (Joueur)        (API REST)       (Routeur UDP)      (QuadTree)     (Autorité act.) (Futur Autorité)
    │                 │                 │                 │                 │                 │
    │─── PHASE 1 : AUTHENTIFICATION (HTTP REST) ──────────────────────────────────────────────────│
    │                 │                 │                 │                 │                 │
    │ 1. POST /login  │                 │                 │                 │                 │
    ├────────────────►│                 │                 │                 │                 │
    │                 │ 2. Vérifie      │                 │                 │                 │
    │                 │    credentials  │                 │                 │                 │
    │                 │    (pass:"1234")│                 │                 │                 │
    │ 3. Retourne JSON│                 │                 │                 │                 │
    │    {token, ip_broker, port_broker}│                 │                 │                 │
    │◄────────────────┤                 │                 │                 │                 │
    │                 │                 │                 │                 │                 │
    │─── PHASE 2 : BOUCLE DE JEU STANDARD & HANDOFF (UDP VIA GAME_SOCKETS) ───────────────────────│
    │                 │                 │                 │                 │                 │
    │ 4. ClientInput (0x05)             │                 │                 │                 │
    ├──────────────────────────────────►│                 │                 │                 │
    │                 │                 │ 5. Route l'Input vers l'Autorité  │                 │
    │                 │                 ├──────────────────────────────────►│                 │
    │                 │                 │                 │                 │ 6. Calcule la   │
    │                 │                 │                 │                 │    physique     │
    │                 │                 │                 │                 │ (Franchit la    │
    │                 │                 │                 │                 │  ligne médiane) │
    │                 │                 │ 7. Publish Position (0x03)        │                 │
    │                 │                 │◄──────────────────────────────────┤                 │
    │ 8. Broadcast (0x04)               │ 9. PositionUpdate (0x10)          │                 │
    │◄──────────────────────────────────┼────────────────►│                │                 │
    │                 │                 │                 │ 10. Vérifie     │                 │
    │                 │                 │                 │     le QuadTree │                 │
    │                 │                 │                 │                 │                 │
    │─── PHASE 3 : LE HANDOFF (Le joueur dépasse la ligne médiane de la Ghost Zone) ──────────│
    │                 │                 │                 │                 │                 │
    │                 │                 │ 11. CrossingAlert                 │                 │
    │                 │                 │◄────────────────┤                 │                 │
    │                 │                 │                 │                 │                 │
    │                 │                 │ 12. Relay HandoffRequest (0x20)   │                 │
    │                 │                 ├───────────────────────────────────┼────────────────►│
    │                 │                 │                 │                 │                 │ 13. Passe de    
    │                 │                 │                 │                 │                 │     GHOST       
    │                 │                 │                 │                 │                 │     à OWNED     
    │                 │                 │ 14. HandoffAccept (0x21)          │                 │
    │                 │                 │◄──────────────────────────────────┼─────────────────┤
    │                 │                 │ 15. Relay Accept                  │                 │
    │                 │                 ├──────────────────────────────────►│                 │
    │                 │                 │                 │                 │ 16. Passe de    │
    │                 │                 │                 │                 │     OWNED       │
    │                 │                 │                 │                 │     à GHOST     │
---

## Instructions de Lancement

### Démarrer l'infrastructure complète
Lancez l'instance de notre projet complet grâce au fichier `docker-compose.yml` : 
```bash
docker-compose up --build -d
```
