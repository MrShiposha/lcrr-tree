pub mod mbr;
mod node;
mod obj_space;
pub mod tree_builder;
pub mod visitor;

#[cfg(test)]
mod test;

#[cfg(test)]
mod proptest;

use {
    crate::tree::mbr::{CoordTrait, MBR},
    std::{
        cmp::Ordering,
        env,
        fmt::Debug,
        ops::Deref,
        sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    },
};

pub use crate::tree::visitor::Visitor;
pub use node::{Node, NodeId, RecordId, RecordIdKind};
pub use obj_space::ObjSpace;
pub use tree_builder::LRTreeBuilder;

pub type InternalNode<CoordT> = Node<CoordT, NodeChildren>;
pub type DataNode<CoordT, ObjectT> = Node<CoordT, ObjectT>;

type NodeChildren = Vec<RecordId>;

macro_rules! obj_space {
    () => {
        RwLockWriteGuard<ObjSpace<CoordT, ObjectT>>
    };
}

macro_rules! filter_intersections {
    ($area:ident in $obj_space:ident) => {
        |&&child_id| mbr::intersects($obj_space.get_mbr(child_id), $area)
    };
}

#[macro_export]
macro_rules! debug_log {
    ($($tt:tt)*) => {
        log::debug! {
            target: env!("CARGO_PKG_NAME"),
            $($tt)*
        }
    };
}

/// Bind nodes: [obj_space]: parent node ID => child node ID
#[macro_export]
macro_rules! bind {
    ([$obj_space:expr] $parent_node_id:expr => set($child_ids:expr)) => {
        $crate::debug_log!("bind set into Parent({:?})", $parent_node_id);

        $obj_space
            .get_node_mut($parent_node_id)
            .payload
            .reserve($child_ids.len());
        while let Some(child_id) = $child_ids.pop() {
            bind!([$obj_space] $parent_node_id => child_id);
        }

        $crate::debug_log!("[COMPLETED] bind set into Parent({:?})", $parent_node_id);
    };

    ([$obj_space:expr] $parent_node_id:expr => $child_id:expr) => {{
        $obj_space.add_child($parent_node_id, $child_id);
        $obj_space.set_parent_info($child_id, $parent_node_id);

        $crate::debug_log!(
            "bind: Parent({:?}) -> Child({:?})",
            $parent_node_id,
            $child_id,
        );
    }};
}

pub trait InsertHandler<ResultT = Self> {
    fn before_insert(&mut self, new_data_id: NodeId);

    fn after_insert(&mut self, new_data_id: NodeId);
}

#[derive(Debug)]
pub struct LRTree<CoordT: CoordTrait, ObjectT: Debug + Clone> {
    obj_space: RwLock<ObjSpace<CoordT, ObjectT>>,
}

impl<CoordT, ObjectT> LRTree<CoordT, ObjectT>
where
    CoordT: CoordTrait,
    ObjectT: Debug + Clone,
{
    pub fn with_obj_space(mut obj_space: ObjSpace<CoordT, ObjectT>) -> Self {
        debug_log!(
            "create new tree: dimension = {}, min_records = {}, max_records = {}",
            obj_space.dimension,
            obj_space.min_records,
            obj_space.max_records
        );

        if obj_space.is_unordered() {
            let mut builder = LRTreeBuilder::with_obj_space(obj_space);
            builder.build();

            obj_space = builder.obj_space;
        }

        let obj_space = RwLock::new(obj_space);

        Self { obj_space }
    }

    pub fn set_build(&self, mut builder: LRTreeBuilder<CoordT, ObjectT>) {
        debug_log!("set new build");

        builder.build();

        *self.obj_space.write().unwrap() = builder.obj_space;

        debug_log!("set new build -- success");
    }

    pub fn lock_obj_space(&self) -> RwLockReadGuard<ObjSpace<CoordT, ObjectT>> {
        self.obj_space.read().unwrap()
    }

    pub fn access_object<H, R>(&self, record_id: NodeId, mut handler: H) -> R
    where
        H: FnMut(&ObjectT, &MBR<CoordT>) -> R,
    {
        let obj_space = self.obj_space.read().unwrap();
        let node = obj_space.get_data(record_id);

        debug_log!("access object #{}: {:?}", record_id, node.payload);

        handler(&node.payload, &node.mbr)
    }

    // pub fn access_object_mut<H>(&self, record_id: RecordId, mut handler: H)
    // where
    //     H: FnMut(&mut MBR<CoordT>, &mut ObjectT)
    // {
    //     let mut obj_space = self.obj_space.write().unwrap();
    //     let node = obj_space.get_data_mut(record_id);

    //     handler(&mut node.mbr, &mut node.payload)
    // }

    pub fn visit<V: Visitor<CoordT, ObjectT>>(&self, visitor: &mut V) {
        if self.obj_space.read().unwrap().is_empty() {
            return;
        }

        self.visit_helper(visitor, self.obj_space.read().unwrap().root_id);
    }

    pub fn search(&self, area: &MBR<CoordT>) -> Vec<NodeId> {
        let obj_space = self.obj_space.read().unwrap();
        let mut result = vec![];

        debug_log!("search in area {}", area);

        let root_id = obj_space.root_id;
        Self::search_helper(&obj_space, root_id, area, &mut |&rec_id| {
            result.push(rec_id.as_node_id())
        });

        debug_log!("search result in area {} -- {:?}", area, result);

        result
    }

    pub fn insert(&self, object: ObjectT, mbr: MBR<CoordT>) -> NodeId {
        struct DefaultHelper;

        impl InsertHandler for DefaultHelper {
            fn before_insert(&mut self, _: NodeId) {}

            fn after_insert(&mut self, _: NodeId) {}
        }

        self.insert_transaction(object, mbr, &mut DefaultHelper)
    }

    pub fn insert_transaction(
        &self,
        object: ObjectT,
        mbr: MBR<CoordT>,
        helper: &mut impl InsertHandler,
    ) -> NodeId {
        let mut obj_space = self.obj_space.write().unwrap();
        assert_eq!(mbr.dimension(), obj_space.dimension, "unexpected dimension");

        let new_object_id = obj_space.make_data_node(object, mbr);
        let new_object_node_id = new_object_id.as_node_id();

        helper.before_insert(new_object_node_id);

        Self::insert_helper(&mut obj_space, new_object_id, |node_id, _| {
            matches![node_id, RecordId::Leaf(_)]
        });

        helper.after_insert(new_object_node_id);

        new_object_node_id
    }

    pub fn mark_as_removed<I: Iterator<Item = NodeId>>(&self, data_ids: I) {
        self.obj_space.write().unwrap().mark_as_removed(data_ids);
    }

    fn insert_helper<P>(obj_space: &mut obj_space![], insert_node_id: RecordId, predicate: P)
    where
        P: FnMut(RecordId, usize) -> bool,
    {
        let mbr = obj_space.get_mbr(insert_node_id).clone();
        debug_log!("insert {:?} with {}", insert_node_id, mbr);

        let max_records = obj_space.max_records;

        let node_id = Self::select_node(obj_space, &mbr, predicate);

        let leaf = obj_space.get_node_mut(node_id);
        let extra_leaf_id = if leaf.payload.len() < max_records {
            bind!([obj_space] node_id => insert_node_id);
            None
        } else {
            let extra_leaf_id = Self::split_node(obj_space, node_id, insert_node_id);
            Some(extra_leaf_id)
        };

        Self::fix_tree(obj_space, node_id, extra_leaf_id);

        let obj_node_id = insert_node_id.as_node_id();
        debug_log!(
            "[COMPLETED] inserted object #{} with {} into {:?}",
            obj_node_id,
            mbr,
            node_id
        );
    }

    fn select_node<P>(obj_space: &mut obj_space![], mbr: &MBR<CoordT>, mut predicate: P) -> RecordId
    where
        P: FnMut(RecordId, usize) -> bool,
    {
        let mut height = 0;
        let mut node_id = obj_space.root_id;

        debug_log!("select node for {}", mbr);

        if obj_space.is_empty() {
            return node_id;
        }

        loop {
            if predicate(node_id, height) {
                debug_log!("node for {} -- {:?}", mbr, node_id);
                return node_id;
            } else {
                node_id = *obj_space
                    .get_node(node_id)
                    .payload
                    .iter()
                    .map(|child_id| {
                        let delta = mbr::mbr_delta(obj_space.get_mbr(*child_id), mbr);

                        debug_log!("{}, delta for {:?} = {:?}", mbr, child_id, delta);

                        (child_id, delta)
                    })
                    .min_by(|lhs, rhs| {
                        let (&lhs_id, lhs_delta) = lhs;
                        let (&rhs_id, rhs_delta) = rhs;

                        let ord = lhs_delta
                            .partial_cmp(rhs_delta)
                            .expect("cmp result is expected");

                        match ord {
                            Ordering::Equal => obj_space
                                .get_mbr(lhs_id)
                                .volume()
                                .partial_cmp(&obj_space.get_mbr(rhs_id).volume())
                                .expect("cmp result is expected"),
                            _ => ord,
                        }
                    })
                    .map(|(id, _)| id)
                    .unwrap()
            }

            height += 1;
        }
    }

    fn fix_tree(
        obj_space: &mut obj_space![],
        mut node_id: RecordId,
        mut extra_node_id: Option<RecordId>,
    ) {
        debug_log!("fix tree");

        let max_records = obj_space.max_records;
        let mut parent_node_id = obj_space.get_node(node_id).parent_id;
        while !matches![parent_node_id, RecordId::Root] {
            debug_log!("fix {:?}", node_id);

            let parent_mbr = obj_space.get_mbr(parent_node_id);
            let node_mbr = obj_space.get_mbr(node_id);
            let fixed_parent_mbr = mbr::common_mbr(parent_mbr, node_mbr);
            obj_space.set_mbr(parent_node_id, fixed_parent_mbr);

            if let Some(new_node_id) = extra_node_id {
                let parent = obj_space.get_node_mut(parent_node_id);

                if parent.payload.len() < max_records {
                    bind!([obj_space] parent_node_id => new_node_id);
                    extra_node_id = None;
                } else {
                    extra_node_id = Some(Self::split_node(obj_space, parent_node_id, new_node_id));
                }
            }

            node_id = parent_node_id;
            parent_node_id = obj_space.get_node(node_id).parent_id;
        }

        if let Some(extra_node_id) = extra_node_id {
            debug_log!("fix root {:?}", node_id);

            let new_root_id = obj_space.make_node(RecordIdKind::Internal);
            bind!([obj_space] new_root_id => node_id);
            bind!([obj_space] new_root_id => extra_node_id);

            obj_space.root_id = new_root_id;
        }

        debug_log!("[COMPLETED] fix tree");
    }

    fn search_helper<Storage, Handler>(
        obj_space: &Storage,
        node_id: RecordId,
        area: &MBR<CoordT>,
        handler: &mut Handler,
    ) where
        Storage: Deref<Target = ObjSpace<CoordT, ObjectT>>,
        Handler: FnMut(&RecordId),
    {
        if obj_space.is_empty() {
            return;
        }

        let node = obj_space.get_node(node_id);
        match node_id {
            RecordId::Leaf(_) => node
                .payload
                .iter()
                .filter(filter_intersections!(area in obj_space))
                .for_each(|child_id| handler(child_id)),
            _ => node
                .payload
                .iter()
                .filter(filter_intersections!(area in obj_space))
                .for_each(|&child_id| {
                    Self::search_helper(obj_space, child_id, area, handler);
                }),
        }
    }

    fn split_node(
        obj_space: &mut obj_space![],
        node_id: RecordId,
        extra_child_id: RecordId,
    ) -> RecordId {
        debug_log!("split {:?}", node_id);

        let dimension = obj_space.dimension;

        let mut children = obj_space.get_node_mut(node_id).abort_children();
        children.push(extra_child_id);

        let children_len = children.len();

        let (lhs, rhs) = Self::select_first_pair(obj_space, &mut children, dimension);
        debug_log!("select first pair = ({:?}, {:?})", lhs, rhs);

        bind!([obj_space] node_id => lhs);

        let new_node_id = obj_space.make_node(node_id.kind());

        bind!([obj_space] new_node_id => rhs);

        let mut node_num = 1;
        let mut new_node_num = 1;
        while !children.is_empty() {
            let num = children.len();
            if obj_space.min_records.saturating_sub(node_num) >= num {
                bind!([obj_space] node_id => set(children));
                break;
            }

            if obj_space.min_records.saturating_sub(new_node_num) >= num {
                bind!([obj_space] new_node_id => set(children));
                break;
            }

            let rec_id = children.pop().unwrap();
            let rec_mbr = obj_space.get_mbr(rec_id);
            let mbr = obj_space.get_mbr(node_id);
            let new_mbr = obj_space.get_mbr(new_node_id);

            let mbr_volume = mbr.volume();
            let new_mbr_volume = new_mbr.volume();

            let delta = mbr::common_mbr(mbr, rec_mbr).volume() - mbr_volume;
            let new_delta = mbr::common_mbr(new_mbr, rec_mbr).volume() - new_mbr_volume;

            if delta < new_delta || delta == new_delta && node_num < new_node_num {
                bind!([obj_space] node_id => rec_id);
                node_num += 1;
            } else {
                bind!([obj_space] new_node_id => rec_id);
                new_node_num += 1;
            }
        }

        debug_assert_eq!(
            obj_space.get_node(node_id).payload.len()
                + obj_space.get_node(new_node_id).payload.len(),
            children_len,
            "Two nodes after split must contain all old nodes + the new one"
        );

        debug_log!("[COMPLETED] split {:?}", node_id);
        new_node_id
    }

    fn select_first_pair(
        obj_space: &mut obj_space![],
        records: &mut Vec<RecordId>,
        dimension: usize,
    ) -> (RecordId, RecordId) {
        let params = (0..dimension)
            .map(|dim| (dim, records.iter()))
            .map(|(dim, mut records)| {
                let first_id = records.next().unwrap();
                let bounds = obj_space.get_mbr(*first_id).bounds(dim);

                let mut min = bounds.min.clone();
                let mut max = bounds.min.clone();

                let mut max_low_idx = 0;
                let mut max_low_id = first_id;
                let mut max_low = min.clone();

                let mut min_high_idx = 0;
                let mut min_high_id = first_id;
                let mut min_high = max.clone();

                records
                    .enumerate()
                    .map(|(index, id)| {
                        // We skipped one element, but we need an index for a whole vector
                        (index + 1, id)
                    })
                    .for_each(|(index, id)| {
                        let bounds = obj_space.get_mbr(*id).bounds(dim);

                        if bounds.min > max_low {
                            max_low_idx = index;
                            max_low_id = id;
                            max_low = bounds.min.clone();
                        } else if bounds.max < min_high {
                            min_high_idx = index;
                            min_high_id = id;
                            min_high = bounds.max.clone();
                        }

                        if bounds.max > max {
                            max = bounds.max.clone();
                        }

                        if bounds.min < min {
                            min = bounds.min.clone();
                        }
                    });

                let length = max - min;
                let d = (min_high - max_low) / length;

                (d, *max_low_id, *min_high_id, max_low_idx, min_high_idx)
            })
            .min_by(|(d_lhs, ..), (d_rhs, ..)| {
                d_lhs.partial_cmp(d_rhs).expect("cmp result expected")
            })
            .unwrap();

        let (_, mut lhs, mut rhs, mut lhs_idx, mut rhs_idx) = params;

        match rhs_idx.cmp(&lhs_idx) {
            Ordering::Greater => std::mem::swap(&mut lhs_idx, &mut rhs_idx),
            Ordering::Equal => {
                // they are not separated - arbitrarily choose the first and the last
                lhs_idx = records.len() - 1;
                rhs_idx = 0;

                lhs = records[lhs_idx];
                rhs = records[rhs_idx];
            }
            _ => {}
        }

        records.swap_remove(lhs_idx);
        records.swap_remove(rhs_idx);

        (lhs, rhs)
    }

    fn visit_helper<V: Visitor<CoordT, ObjectT>>(&self, visitor: &mut V, id: RecordId) {
        match id {
            RecordId::Data(data_id) => {
                visitor.visit_data(id, self.obj_space.read().unwrap().get_data(data_id))
            }
            _ => {
                let obj_space = self.obj_space.read().unwrap();
                let node = obj_space.get_node(id);
                visitor.enter_node(id, node);
                node.payload.iter().for_each(|&child_id| {
                    self.visit_helper(visitor, child_id);
                });
                visitor.leave_node(id, node);
            }
        }
    }
}

#[cfg(feature = "with-dbg-vis")]
use dbg_vis::{DebugVis, DebugVisJSON};

#[cfg(feature = "with-dbg-vis")]
impl<CoordT: CoordTrait, ObjectT: Clone + Debug> DebugVis for LRTree<CoordT, ObjectT> {
    fn debug_visualize(&self) -> DebugVisJSON {
        let mut visitor = visitor::dbg_vis::LRTreeDbgVis::new();

        self.visit(&mut visitor);

        visitor.debug_visualize()
    }
}

pub trait InternalNodeTrait<CoordT> {
    fn new(capacity: usize) -> Self;

    fn with_mbr(capacity: usize, mbr: MBR<CoordT>) -> Self;

    fn abort_children(&mut self) -> NodeChildren;
}

impl<CoordT: CoordTrait> InternalNodeTrait<CoordT> for InternalNode<CoordT> {
    fn new(capacity: usize) -> Self {
        Self::with_mbr(capacity, unsafe { MBR::undefined() })
    }

    fn with_mbr(capacity: usize, mbr: MBR<CoordT>) -> Self {
        Self {
            parent_id: RecordId::Root,
            mbr,
            payload: NodeChildren::with_capacity(capacity),
        }
    }

    fn abort_children(&mut self) -> NodeChildren {
        self.mbr = unsafe { MBR::undefined() };

        let capacity = self.payload.capacity();
        std::mem::replace(&mut self.payload, NodeChildren::with_capacity(capacity))
    }
}
