use bevy::math::{Rect, Vec2};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionUpdate {
    pub client_id: u32,
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SpatialAction {
    Subscribe { client_id: u32, topic: [u8; 32] },
    Unsubscribe { client_id: u32, topic: [u8; 32] },
    CrossingAlert { client_id: u32, shards: Vec<u32> },
    AuthorityChange { client_id: u32, old_shard: u32, new_shard: u32 },
    ScaleUp { parent_shard: u32, new_shards: Vec<u32> },
    ScaleDown { parent_shard: u32, old_shards: Vec<u32> },
}

pub fn string_to_topic(s: &str) -> [u8; 32] {
    let mut topic = [0u8; 32];
    let bytes = s.as_bytes();
    let len = bytes.len().min(32);
    topic[..len].copy_from_slice(&bytes[..len]);
    topic
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuadTree {
    pub bounds: Rect,
    pub depth: u8,
    pub max_depth: u8,
    pub children: Option<Box<[QuadTree; 4]>>,
    pub shard_id: Option<u32>,
}

impl QuadTree {
    pub fn new(bounds: Rect, depth: u8, max_depth: u8) -> Self {
        Self {
            bounds,
            depth,
            max_depth,
            children: None,
            shard_id: None,
        }
    }

    /// Split this node into 4 children.
    pub fn split(&mut self) {
        if self.depth >= self.max_depth || self.children.is_some() {
            return;
        }

        let half_w = self.bounds.width() / 2.0;
        let half_h = self.bounds.height() / 2.0;

        let center = self.bounds.center();

        let nw_bounds = Rect::from_center_size(
            Vec2::new(center.x - half_w / 2.0, center.y + half_h / 2.0),
            Vec2::new(half_w, half_h),
        );
        let ne_bounds = Rect::from_center_size(
            Vec2::new(center.x + half_w / 2.0, center.y + half_h / 2.0),
            Vec2::new(half_w, half_h),
        );
        let sw_bounds = Rect::from_center_size(
            Vec2::new(center.x - half_w / 2.0, center.y - half_h / 2.0),
            Vec2::new(half_w, half_h),
        );
        let se_bounds = Rect::from_center_size(
            Vec2::new(center.x + half_w / 2.0, center.y - half_h / 2.0),
            Vec2::new(half_w, half_h),
        );

        let next_depth = self.depth + 1;

        self.children = Some(Box::new([
            QuadTree::new(nw_bounds, next_depth, self.max_depth),
            QuadTree::new(ne_bounds, next_depth, self.max_depth),
            QuadTree::new(sw_bounds, next_depth, self.max_depth),
            QuadTree::new(se_bounds, next_depth, self.max_depth),
        ]));
        // Keep shard_id intact so we can restore it on merge
    }

    /// Recursively define leaf shards in sequence
    pub fn assign_shards(&mut self, next_id: &mut u32) {
        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                child.assign_shards(next_id);
            }
        } else {
            self.shard_id = Some(*next_id);
            *next_id += 1;
        }
    }

    /// Recursively find a specific leaf shard and split it.
    /// Returns the 4 new shard IDs if successful.
    pub fn split_leaf(&mut self, target_shard_id: u32, next_id: &mut u32) -> Option<[u32; 4]> {
        if let Some(children) = &mut self.children {
            for child in children.iter_mut() {
                if let Some(new_ids) = child.split_leaf(target_shard_id, next_id) {
                    return Some(new_ids);
                }
            }
            None
        } else if self.shard_id == Some(target_shard_id) {
            self.split();
            // After split, we have 4 children. Assign them new shards.
            if let Some(children) = &mut self.children {
                let mut ids = [0u32; 4];
                for (i, child) in children.iter_mut().enumerate() {
                    child.shard_id = Some(*next_id);
                    ids[i] = *next_id;
                    *next_id += 1;
                }
                Some(ids)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Return the shard_id of the leaf containing `pos`.
    pub fn shard_for(&self, pos: Vec2) -> Option<u32> {
        if !self.bounds.contains(pos) {
            return None;
        }

        if let Some(children) = &self.children {
            for child in children.iter() {
                if let Some(id) = child.shard_for(pos) {
                    return Some(id);
                }
            }
        } else {
            return self.shard_id;
        }

        None
    }

    /// Return distinct shard_ids in a radius `margin` around `pos`.
    pub fn shards_near(&self, pos: Vec2, margin: f32) -> Vec<u32> {
        let mut result = Vec::new();
        self.shards_near_impl(pos, margin, &mut result);
        result.sort_unstable();
        result.dedup();
        result
    }

    fn shards_near_impl(&self, pos: Vec2, margin: f32, result: &mut Vec<u32>) {
        let margin_rect = Rect::from_center_size(pos, Vec2::new(margin * 2.0, margin * 2.0));

        if !rects_overlap(self.bounds, margin_rect) {
            return;
        }

        if let Some(children) = &self.children {
            for child in children.iter() {
                child.shards_near_impl(pos, margin, result);
            }
        } else if let Some(id) = self.shard_id {
            result.push(id);
        }
    }

    /// Try to find a node where all 4 children are leaves and combined population <= max_pop.
    /// Returns (parent_shard, [child_1, child_2, child_3, child_4]) if found and merged.
    pub fn try_merge(&mut self, pop_map: &std::collections::HashMap<u32, usize>, max_pop: usize) -> Option<(u32, [u32; 4])> {
        if let Some(children) = &mut self.children {
            // Check if any child can be merged recursively first
            for child in children.iter_mut() {
                if let Some(res) = child.try_merge(pop_map, max_pop) {
                    return Some(res);
                }
            }

            // All children must be leaves and the current node must have a shard_id (root does not)
            if let Some(parent_id) = self.shard_id {
                let all_leaves = children.iter().all(|c| c.children.is_none());
                if all_leaves {
                    let mut total_pop = 0;
                    let mut child_ids = [0u32; 4];
                    for (i, child) in children.iter().enumerate() {
                        let cid = child.shard_id.unwrap();
                        total_pop += pop_map.get(&cid).copied().unwrap_or(0);
                        child_ids[i] = cid;
                    }

                    if total_pop <= max_pop {
                        // Merge!
                        self.children = None;
                        return Some((parent_id, child_ids));
                    }
                }
            }
        }
        None
    }
}

fn rects_overlap(r1: Rect, r2: Rect) -> bool {
    r1.min.x <= r2.max.x && r1.max.x >= r2.min.x && r1.min.y <= r2.max.y && r1.max.y >= r2.min.y
}

pub struct SpatialService {
    pub quadtree: QuadTree,
    pub margin: f32,
    pub client_primary_shard: std::collections::HashMap<u32, u32>,
    pub client_subscribed_shards: std::collections::HashMap<u32, Vec<u32>>,
    pub next_shard_id: u32,
    pub pending_splits: std::collections::HashSet<u32>,
    pub pending_merges: std::collections::HashSet<u32>, // parent shards waiting for ScaleDown completion
}

impl SpatialService {
    pub const MAX_POPULATION: usize = 2; // Very low for testing dynamic scaling
    pub const MIN_POPULATION: usize = 1; // Merge when 1 for testing

    pub fn new(quadtree: QuadTree, margin: f32, next_shard_id: u32) -> Self {
        Self {
            quadtree,
            margin,
            client_primary_shard: std::collections::HashMap::new(),
            client_subscribed_shards: std::collections::HashMap::new(),
            next_shard_id,
            pending_splits: std::collections::HashSet::new(),
            pending_merges: std::collections::HashSet::new(),
        }
    }

    pub fn handle_position_update(
        &mut self,
        update: &PositionUpdate,
        pending_ready: &std::collections::HashMap<u32, u32>
    ) -> Vec<SpatialAction> {
        let mut actions = Vec::new();
        let pos = Vec2::new(update.x, update.y);

        let mut new_shard_opt = self.quadtree.shard_for(pos);
        // Route back to parent if the child shard is not ready yet
        if let Some(s) = new_shard_opt {
            if let Some(&parent) = pending_ready.get(&s) {
                new_shard_opt = Some(parent);
            }
        }
        if let Some(new_primary) = new_shard_opt {
            let current_primary = self.client_primary_shard.get(&update.client_id).copied();

            if current_primary != Some(new_primary) {
                if let Some(old) = current_primary {
                    actions.push(SpatialAction::AuthorityChange {
                        client_id: update.client_id,
                        old_shard: old,
                        new_shard: new_primary,
                    });
                }
                self.client_primary_shard.insert(update.client_id, new_primary);
            }
        }

        // Check overlapping shards in the margin
        let mut near = self.quadtree.shards_near(pos, self.margin);
        // Map any unready near shards back to their parents
        for s in near.iter_mut() {
            if let Some(&parent) = pending_ready.get(s) {
                *s = parent;
            }
        }
        if let Some(primary) = new_shard_opt {
            if !near.contains(&primary) {
                near.push(primary);
            }
        }
        near.sort_unstable();
        near.dedup();

        let currently_subscribed = self
            .client_subscribed_shards
            .get(&update.client_id)
            .cloned()
            .unwrap_or_default();

        // New subscriptions
        for &shard in &near {
            if !currently_subscribed.contains(&shard) {
                actions.push(SpatialAction::Subscribe {
                    client_id: update.client_id,
                    topic: string_to_topic(&format!("shard:{}", shard)),
                });
            }
        }

        // Unsubscriptions
        for &shard in &currently_subscribed {
            if !near.contains(&shard) {
                actions.push(SpatialAction::Unsubscribe {
                    client_id: update.client_id,
                    topic: string_to_topic(&format!("shard:{}", shard)),
                });
            }
        }

        self.client_subscribed_shards.insert(update.client_id, near.clone());

        // Emit CrossingAlert if player is in the margin of multiple shards.
        // We always emit it so the servers are aware of the proximity.
        if near.len() > 1 {
            actions.push(SpatialAction::CrossingAlert {
                client_id: update.client_id,
                shards: near,
            });
        }

        // Check Population for splits
        if let Some(primary) = new_shard_opt {
            if !self.pending_splits.contains(&primary) && !self.pending_merges.contains(&primary) {
                let pop = self.client_primary_shard.values().filter(|&&s| s == primary).count();
                if pop > Self::MAX_POPULATION {
                    self.pending_splits.insert(primary);
                    if let Some(new_shards_arr) = self.quadtree.split_leaf(primary, &mut self.next_shard_id) {
                        let new_shards = new_shards_arr.to_vec();
                        actions.push(SpatialAction::ScaleUp {
                            parent_shard: primary,
                            new_shards,
                        });
                    }
                }
            }
        }

        actions
    }

    pub fn check_merges(&mut self) -> Vec<SpatialAction> {
        let mut actions = Vec::new();
        let mut pop_map = std::collections::HashMap::new();
        for &shard in self.client_primary_shard.values() {
            *pop_map.entry(shard).or_insert(0) += 1;
        }

        // We can loop to find multiple merges, but 1 is fine for now
        if let Some((parent_shard, old_shards_arr)) = self.quadtree.try_merge(&pop_map, Self::MIN_POPULATION) {
            let old_shards = old_shards_arr.to_vec();
            self.pending_merges.insert(parent_shard);
            actions.push(SpatialAction::ScaleDown {
                parent_shard,
                old_shards,
            });
        }
        
        actions
    }
}
