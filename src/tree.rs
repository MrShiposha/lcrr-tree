use {
    std::{
        sync::{RwLock, RwLockWriteGuard},
        ops::{Sub, Div},
    },
    id_cache::Storage,
    crate::{
        node::{Node, NodeId, RecordId},
        mbr::MBR
    }
};

type ChildIdStorage = Storage<RecordId>;

macro_rules! node {
    (struct) => {
        Node<CoordT, ChildIdStorage>
    };

    (data) => {
        Node<CoordT, ObjectT>
    };
}

pub struct CRTree<CoordT, ObjectT> {
    storage: RwLock<TreeStorage<CoordT, ObjectT>>
}

impl<CoordT, ObjectT> CRTree<CoordT, ObjectT>
where
    CoordT: Sub<Output=CoordT> + Div<Output=CoordT> + Ord + Clone
{
    pub fn new(dimension: usize, min_records: usize, max_records: usize) -> Self {
        // TODO assert min/max records != 0 and each other
        Self {
            storage: RwLock::new(TreeStorage::new(dimension, min_records, max_records)),
        }
    }

    pub fn access_object<H>(&self, record_id: RecordId, mut handler: H)
    where
        H: FnMut(&MBR<CoordT>, &ObjectT)
    {
        let storage = self.storage.read().unwrap();
        let node = storage.get_data(record_id);

        handler(&node.mbr, &node.payload)
    }

    pub fn access_object_mut<H>(&mut self, record_id: RecordId, mut handler: H)
    where
        H: FnMut(&mut MBR<CoordT>, &mut ObjectT)
    {
        let mut storage = self.storage.write().unwrap();
        let node = storage.get_data_mut(record_id);

        handler(&mut node.mbr, &mut node.payload)
    }

    fn split_node(
        storage: &mut RwLockWriteGuard<TreeStorage<CoordT, ObjectT>>,
        node_id: RecordId,
        extra_child_id: RecordId
    ) -> node![struct] {
        let max_records = storage.max_records;
        let dimension = storage.dimension;
        let children = std::mem::replace(
            &mut storage.get_node_mut(node_id).payload,
            Self::make_children_storage(max_records)
        );

        // It is assumed that `node` is full. So it is safe to use `Storage::into_vec`
        let children = unsafe {
            children.into_vec()
        };

        let (lhs, rhs) = match node_id {
            RecordId::Leaf(_) => Self::select_first_pair(&mut storage.data_nodes, children, dimension),
            RecordId::Internal(_) => Self::select_first_pair(&mut storage.nodes, children, dimension),
            _ => unreachable!()
        };

        unimplemented!()
    }

    fn select_first_pair<PayloadT>(
        storage: &mut Storage<Node<CoordT, PayloadT>>,
        records: Vec<RecordId>,
        dimension: usize
    ) -> (RecordId, RecordId) {
        (0..dimension).map(|dim|
            (dim, records.iter())
        ).map(|(dim, mut records)| {
            let first_id = records.next().unwrap();
            let first_rec = storage.get(first_id.as_node_id());
            let bounds = first_rec.mbr.bounds(dim);

            let mut max = bounds.max.clone();
            let mut min = bounds.min.clone();

            let mut hi_id = first_id;
            let mut hi_min = min.clone();

            let mut lo_id = first_id;
            let mut lo_max = max.clone();

            records.for_each(|id| {
                let rec = storage.get(id.as_node_id());
                let bounds = rec.mbr.bounds(dim);

                // TODO CHECKME
                if bounds.max > max {
                    max = bounds.max.clone();
                } else if bounds.max < lo_max {
                    lo_id = id;
                    lo_max = bounds.max.clone();
                }

                if bounds.min < min {
                    min = bounds.min.clone();
                } else if bounds.min > hi_min {
                    hi_id = id;
                    hi_min = bounds.min.clone();
                }
            });

            let length = max - min;
            let d = (lo_max - hi_min) / length;

            (d, (hi_id, lo_id))
        }).min_by_key(|(d, _)| d.clone())
        .map(|(_, (lhs, rhs))| (lhs.clone(), rhs.clone())).unwrap()
    }

    fn make_node(
        parent_id: RecordId,
        mbr: MBR<CoordT>,
        max_records: usize
    ) -> node![struct] {
        Node {
            parent_id,
            mbr,
            payload: Self::make_children_storage(max_records)
        }
    }

    fn make_children_storage(max_records: usize) -> ChildIdStorage {
        Storage::with_capacity(max_records)
    }
}

struct TreeStorage<CoordT, ObjectT> {
    nodes: Storage<node![struct]>,
    data_nodes: Storage<node![data]>,
    dimension: usize,
    min_records: usize,
    max_records: usize
}

impl<CoordT, ObjectT> TreeStorage<CoordT, ObjectT> {
    pub fn new(dimension: usize, min_records: usize, max_records: usize) -> Self {
        Self {
            nodes: Storage::new(),
            data_nodes: Storage::new(),
            dimension,
            min_records,
            max_records
        }
    }

    pub fn get_node(&self, id: RecordId) -> &node![struct] {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        self.nodes.get(id.as_node_id())
    }

    pub fn get_node_mut(&mut self, id: RecordId) -> &mut node![struct] {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        self.nodes.get_mut(id.as_node_id())
    }

    pub fn get_data(&self, id: RecordId) -> &node![data] {
        debug_assert! {
            matches!(id, RecordId::Data(_)),
            "expected data id"
        };

        self.data_nodes.get(id.as_node_id())
    }

    pub fn get_data_mut(&mut self, id: RecordId) -> &mut node![data] {
        debug_assert! {
            matches!(id, RecordId::Data(_)),
            "expected data id"
        };

        self.data_nodes.get_mut(id.as_node_id())
    }
}
