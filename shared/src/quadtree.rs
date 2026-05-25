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
    Subscribe { client_id: u32, topic: String },
    Unsubscribe { client_id: u32, topic: String },
    CrossingAlert { client_id: u32, shards: Vec<u32> },
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
        self.shard_id = None; // inner node has no shard
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
        }

        self.shard_id
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
}

fn rects_overlap(r1: Rect, r2: Rect) -> bool {
    r1.min.x <= r2.max.x && r1.max.x >= r2.min.x && r1.min.y <= r2.max.y && r1.max.y >= r2.min.y
}

pub struct SpatialService {
    pub quadtree: QuadTree,
    pub margin: f32,
    pub client_shards: std::collections::HashMap<u32, u32>,
}

impl SpatialService {
    pub fn new(quadtree: QuadTree, margin: f32) -> Self {
        Self {
            quadtree,
            margin,
            client_shards: std::collections::HashMap::new(),
        }
    }

    pub fn handle_position_update(&mut self, update: &PositionUpdate) -> Vec<SpatialAction> {
        let mut actions = Vec::new();
        let pos = Vec2::new(update.x, update.y);

        let new_shard_opt = self.quadtree.shard_for(pos);
        if let Some(new_shard) = new_shard_opt {
            let current_shard = self.client_shards.get(&update.client_id).copied();

            if current_shard != Some(new_shard) {
                // Shard changed or new client
                if let Some(old) = current_shard {
                    actions.push(SpatialAction::Unsubscribe {
                        client_id: update.client_id,
                        topic: format!("shard:{}", old),
                    });
                }

                actions.push(SpatialAction::Subscribe {
                    client_id: update.client_id,
                    topic: format!("shard:{}", new_shard),
                });

                self.client_shards.insert(update.client_id, new_shard);
            }
        }

        // Check if near multiple shards
        let near = self.quadtree.shards_near(pos, self.margin);
        if near.len() > 1 {
            actions.push(SpatialAction::CrossingAlert {
                client_id: update.client_id,
                shards: near,
            });
        }

        actions
    }
}
