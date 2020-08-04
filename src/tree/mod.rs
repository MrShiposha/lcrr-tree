pub mod mbr;
mod node;

#[cfg(test)]
mod test;

use {
    crate::tree::{
        mbr::{CoordTrait, MBR},
        node::{Node, NodeId, RecordId},
    },
    id_cache::Storage,
    petgraph::graphmap::UnGraphMap,
    std::{
        cmp::Ordering,
        iter::Extend,
        ops::Deref,
        sync::{RwLock, RwLockWriteGuard},
    },
};

type ChildIdStorage = Vec<RecordId>;

type RecordsNum = i16;

macro_rules! node {
    (internal) => {
        Node<CoordT, ChildIdStorage>
    };

    (data) => {
        Node<CoordT, ObjectT>
    };
}

macro_rules! storage {
    () => {
        RwLockWriteGuard<TreeStorage<CoordT, ObjectT>>
    };
}

macro_rules! filter_intersections {
    ($area:ident in $storage:ident) => {
        |&&child_id| mbr::intersects($storage.get_mbr(child_id), $area)
    };
}

/// Bind nodes: [storage]: parent node ID => child node ID
macro_rules! bind {
    ([$storage:expr] $parent_node_id:expr => set($child_ids:expr)) => {
        $storage
            .get_node_mut($parent_node_id)
            .payload
            .reserve($child_ids.len());
        while let Some(child_id) = $child_ids.pop() {
            $storage.set_parent_id(child_id, $parent_node_id);
            $storage.add_child($parent_node_id, child_id);
        }
    };

    ([$storage:expr] $parent_node_id:expr => $child_id:expr) => {
        $storage.set_parent_id($child_id, $parent_node_id);
        $storage.add_child($parent_node_id, $child_id);
    };
}

pub struct LCRRTree<CoordT, ObjectT> {
    storage: RwLock<TreeStorage<CoordT, ObjectT>>,
}

impl<CoordT, ObjectT> LCRRTree<CoordT, ObjectT>
where
    CoordT: CoordTrait,
{
    pub fn new(dimension: usize, min_records: RecordsNum, max_records: RecordsNum) -> Self {
        assert!(0 < min_records && min_records < max_records);

        let storage = RwLock::new(TreeStorage::new(dimension, min_records, max_records));

        Self { storage }
    }

    pub fn access_object<H>(&self, record_id: NodeId, mut handler: H)
    where
        H: FnMut(&MBR<CoordT>, &ObjectT),
    {
        let storage = self.storage.read().unwrap();
        let node = storage.get_data(record_id);

        handler(&node.mbr, &node.payload)
    }

    // pub fn access_object_mut<H>(&self, record_id: RecordId, mut handler: H)
    // where
    //     H: FnMut(&mut MBR<CoordT>, &mut ObjectT)
    // {
    //     let mut storage = self.storage.write().unwrap();
    //     let node = storage.get_data_mut(record_id);

    //     handler(&mut node.mbr, &mut node.payload)
    // }

    pub fn search(&self, area: &MBR<CoordT>) -> Vec<NodeId> {
        let storage = self.storage.read().unwrap();
        let mut result = vec![];

        let root_id = storage.root_id;
        Self::search_helper(&storage, root_id, area, &mut |&rec_id| {
            result.push(rec_id.as_node_id())
        });

        result
    }

    pub fn insert(&self, object: ObjectT, mbr: MBR<CoordT>) -> NodeId {
        let mut storage = self.storage.write().unwrap();
        assert_eq!(mbr.dimension(), storage.dimension, "unexpected dimension");

        let max_records = storage.max_records;

        let leaf_id = Self::select_leaf(&mut storage, &mbr);
        let new_object_id = Self::make_data_node(&mut storage, leaf_id, object, mbr.clone());

        Self::find_new_collisions(&mut storage, new_object_id, &mbr);

        let leaf = storage.get_node_mut(leaf_id);
        let extra_leaf_id = if (leaf.payload.len() as RecordsNum) < max_records {
            bind!([storage] leaf_id => new_object_id);
            None
        } else {
            let extra_leaf_id = Self::split_node(&mut storage, leaf_id, new_object_id);
            Some(extra_leaf_id)
        };

        Self::fix_tree(&mut storage, leaf_id, extra_leaf_id);
        new_object_id.as_node_id()
    }

    fn select_leaf(storage: &mut storage![], new_mbr: &MBR<CoordT>) -> RecordId {
        let mut node_id = storage.root_id;

        loop {
            match node_id {
                RecordId::Leaf(_) => return node_id,
                _ => {
                    node_id = *storage
                        .get_node(node_id)
                        .payload
                        .iter()
                        .map(|child_id| {
                            let delta = Self::mbr_delta(storage.get_mbr(*child_id), new_mbr);

                            (child_id, delta)
                        })
                        .min_by(|lhs, rhs| {
                            let (&lhs_id, lhs_delta) = lhs;
                            let (&rhs_id, rhs_delta) = rhs;

                            let ord = lhs_delta.cmp(rhs_delta);

                            match ord {
                                Ordering::Equal => storage
                                    .get_mbr(lhs_id)
                                    .volume()
                                    .cmp(&storage.get_mbr(rhs_id).volume()),
                                _ => ord,
                            }
                        })
                        .map(|(id, _)| id)
                        .unwrap()
                }
            }
        }
    }

    fn find_new_collisions(storage: &mut storage![], new_object_id: RecordId, mbr: &MBR<CoordT>) {
        let mut edges = vec![];
        let root_id = storage.root_id;

        Self::search_helper(storage, root_id, mbr, &mut |&rec_id| {
            edges.push((new_object_id.as_node_id(), rec_id.as_node_id()))
        });

        storage.collisions.extend(edges);
    }

    fn mbr_delta(src: &MBR<CoordT>, addition: &MBR<CoordT>) -> CoordT {
        let common = mbr::common_mbr(src, addition);

        common.volume() - src.volume()
    }

    fn fix_tree(
        storage: &mut storage![],
        mut node_id: RecordId,
        mut extra_node_id: Option<RecordId>,
    ) {
        let max_records = storage.max_records;
        let mut parent_node_id = storage.get_node(node_id).parent_id;
        while !matches![parent_node_id, RecordId::Root] {
            if let Some(new_node_id) = extra_node_id {
                let parent = storage.get_node_mut(parent_node_id);

                if (parent.payload.len() as RecordsNum) < max_records {
                    bind!([storage] parent_node_id => new_node_id);
                    extra_node_id = None;
                } else {
                    extra_node_id = Some(Self::split_node(storage, parent_node_id, new_node_id));
                }
            }

            node_id = parent_node_id;
            parent_node_id = storage.get_node(node_id).parent_id;
        }

        if let Some(extra_node_id) = extra_node_id {
            let new_root_id = RecordId::Internal(Self::make_node(storage, RecordId::Root));

            storage.get_node_mut(node_id).parent_id = new_root_id;
            storage.get_node_mut(extra_node_id).parent_id = new_root_id;

            bind!([storage] new_root_id => node_id);
            bind!([storage] new_root_id => extra_node_id);
            storage.root_id = new_root_id;
        }
    }

    fn search_helper<Storage, Handler>(
        storage: &Storage,
        node_id: RecordId,
        area: &MBR<CoordT>,
        handler: &mut Handler,
    ) where
        Storage: Deref<Target = TreeStorage<CoordT, ObjectT>>,
        Handler: FnMut(&RecordId),
    {
        let node = storage.get_node(node_id);
        match node_id {
            RecordId::Leaf(_) => node
                .payload
                .iter()
                .filter(filter_intersections!(area in storage))
                .for_each(|child_id| handler(child_id)),
            _ => node
                .payload
                .iter()
                .filter(filter_intersections!(area in storage))
                .for_each(|&child_id| {
                    Self::search_helper(storage, child_id, area, handler);
                }),
        }
    }

    fn split_node(
        storage: &mut storage![],
        node_id: RecordId,
        extra_child_id: RecordId,
    ) -> RecordId {
        let max_records = storage.max_records;
        let dimension = storage.dimension;
        let mut children = std::mem::replace(
            &mut storage.get_node_mut(node_id).payload,
            Self::make_children_storage(max_records),
        );

        children.push(extra_child_id);

        let (lhs, rhs) = Self::select_first_pair(storage, &mut children, dimension);

        bind!([storage] node_id => lhs);

        let new_node_id = Self::make_node(storage, storage.get_node(node_id).parent_id);

        let new_node_id = node_id.make_sibling_id(new_node_id);

        bind!([storage] new_node_id => rhs);

        let mut node_num = 1;
        let mut new_node_num = 1;
        while !children.is_empty() {
            let num = children.len() as RecordsNum;
            if storage.min_records - node_num >= num {
                bind!([storage] node_id => set(children));
                break;
            }

            if storage.min_records - new_node_num >= num {
                bind!([storage] new_node_id => set(children));
                break;
            }

            let rec_id = children.pop().unwrap();
            let rec_mbr = storage.get_mbr(rec_id);
            let mbr = storage.get_mbr(node_id);
            let new_mbr = storage.get_mbr(new_node_id);

            let mbr_volume = mbr.volume();
            let new_mbr_volume = new_mbr.volume();

            let delta = mbr::common_mbr(mbr, rec_mbr).volume() - mbr_volume;
            let new_delta = mbr::common_mbr(new_mbr, rec_mbr).volume() - new_mbr_volume;

            if delta < new_delta || delta == new_delta && node_num < new_node_num {
                bind!([storage] node_id => rec_id);
                node_num += 1;
            } else {
                bind!([storage] new_node_id => rec_id);
                new_node_num += 1;
            }
        }

        new_node_id
    }

    fn select_first_pair(
        storage: &mut storage![],
        records: &mut Vec<RecordId>,
        dimension: usize,
    ) -> (RecordId, RecordId) {
        let params = (0..dimension)
            .map(|dim| (dim, records.iter()))
            .map(|(dim, mut records)| {
                let first_id = records.next().unwrap();
                let bounds = storage.get_mbr(*first_id).bounds(dim);

                let mut max = bounds.max.clone();
                let mut min = bounds.min.clone();

                let mut hi_idx = 0;
                let mut hi_id = first_id;
                let mut hi_min = min.clone();

                let mut lo_idx = 0;
                let mut lo_id = first_id;
                let mut lo_max = max.clone();

                records
                    .enumerate()
                    .map(|(index, id)| {
                        // We skipped one element, but we need an index for a whole vector
                        (index + 1, id)
                    })
                    .for_each(|(index, id)| {
                        let bounds = storage.get_mbr(*id).bounds(dim);

                        if bounds.max > max {
                            max = bounds.max.clone();
                        } else if bounds.max < lo_max {
                            lo_idx = index;
                            lo_id = id;
                            lo_max = bounds.max.clone();
                        }

                        if bounds.min < min {
                            min = bounds.min.clone();
                        } else if bounds.min > hi_min {
                            hi_idx = index;
                            hi_id = id;
                            hi_min = bounds.min.clone();
                        }
                    });

                let length = max - min;
                let d = (lo_max - hi_min) / length;

                (d, *hi_id, *lo_id, hi_idx, lo_idx)
            })
            .min_by_key(|(d, ..)| d.clone())
            .unwrap();

        let (_, lhs, rhs, lhs_idx, rhs_idx) = params;
        records.swap_remove(lhs_idx);
        records.swap_remove(rhs_idx);

        (lhs, rhs)
    }

    fn make_node(storage: &mut storage![], parent_id: RecordId) -> NodeId {
        let node = Node {
            parent_id,
            mbr: unsafe { MBR::new_singularity(storage.dimension) },
            payload: Self::make_children_storage(storage.max_records),
        };

        storage.nodes.insert(node)
    }

    fn make_data_node(
        storage: &mut storage![],
        parent_id: RecordId,
        object: ObjectT,
        mbr: MBR<CoordT>,
    ) -> RecordId {
        let node = Node {
            parent_id,
            mbr,
            payload: object,
        };

        RecordId::Data(storage.data_nodes.insert(node))
    }

    fn make_children_storage(max_records: RecordsNum) -> ChildIdStorage {
        Vec::with_capacity(max_records as usize)
    }
}

struct TreeStorage<CoordT, ObjectT> {
    nodes: Storage<node![internal]>,
    data_nodes: Storage<node![data]>,
    dimension: usize,
    min_records: RecordsNum,
    max_records: RecordsNum,
    root_id: RecordId,
    collisions: UnGraphMap<NodeId, ()>,
}

impl<CoordT: CoordTrait, ObjectT> TreeStorage<CoordT, ObjectT> {
    fn new(dimension: usize, min_records: RecordsNum, max_records: RecordsNum) -> Self {
        let mut nodes = Storage::new();

        let root_node = Node {
            parent_id: RecordId::Root,
            mbr: unsafe { MBR::new_singularity(dimension) },
            payload: LCRRTree::<CoordT, ObjectT>::make_children_storage(max_records),
        };

        let root_id = RecordId::Leaf(nodes.insert(root_node));

        Self {
            nodes,
            data_nodes: Storage::new(),
            dimension,
            min_records,
            max_records,
            root_id,
            collisions: UnGraphMap::new(),
        }
    }

    // fn get_parent_id(&self, id: RecordId) -> RecordId {
    //     match id {
    //         RecordId::Data(id) => self.data_nodes.get(id).parent_id,
    //         _ => self.nodes.get(id.as_node_id()).parent_id
    //     }
    // }

    fn set_parent_id(&mut self, id: RecordId, parent_id: RecordId) {
        match id {
            RecordId::Data(id) => self.data_nodes.get_mut(id).parent_id = parent_id,
            _ => self.nodes.get_mut(id.as_node_id()).parent_id = parent_id,
        }
    }

    fn add_child(&mut self, id: RecordId, child_id: RecordId) {
        let node = self.get_node(id);
        let child_mbr = self.get_mbr(child_id);

        let new_parent_mbr = if node.payload.is_empty() {
            child_mbr.clone()
        } else {
            mbr::common_mbr(&node.mbr, child_mbr)
        };

        let node = self.get_node_mut(id);
        node.payload.push(child_id);
        node.mbr = new_parent_mbr;
    }

    fn get_mbr(&self, id: RecordId) -> &MBR<CoordT> {
        match id {
            RecordId::Data(id) => &self.data_nodes.get(id).mbr,
            _ => &self.nodes.get(id.as_node_id()).mbr,
        }
    }

    // fn get_mbr_mut(&mut self, id: RecordId) -> &mut MBR<CoordT> {
    //     match id {
    //         RecordId::Data(id) => &mut self.data_nodes.get_mut(id).mbr,
    //         _ => &mut self.nodes.get_mut(id.as_node_id()).mbr
    //     }
    // }

    fn get_node(&self, id: RecordId) -> &node![internal] {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        self.nodes.get(id.as_node_id())
    }

    fn get_node_mut(&mut self, id: RecordId) -> &mut node![internal] {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        self.nodes.get_mut(id.as_node_id())
    }

    fn get_data(&self, id: NodeId) -> &node![data] {
        self.data_nodes.get(id)
    }

    // fn get_data_mut(&mut self, id: RecordId) -> &mut node![data] {
    //     debug_assert! {
    //         matches!(id, RecordId::Data(_)),
    //         "expected data id"
    //     };

    //     self.data_nodes.get_mut(id.as_node_id())
    // }
}
