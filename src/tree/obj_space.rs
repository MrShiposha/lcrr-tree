use {
    super::{
        mbr, CoordTrait, DataNode, InternalNode, InternalNodeTrait, Node, NodeId, RecordId,
        RecordIdKind, MBR,
    },
    id_storage::ShrinkableStorage,
    std::{fmt::Debug, iter::Extend},
};

#[derive(Debug)]
pub struct ObjSpace<CoordT: CoordTrait, ObjectT: Clone> {
    nodes: Vec<InternalNode<CoordT>>,
    data_nodes: ShrinkableStorage<DataNode<CoordT, ObjectT>>,
    pub(crate) dimension: usize,
    pub(crate) min_records: usize,
    pub(crate) max_records: usize,
    pub(crate) root_id: RecordId,
}

impl<CoordT: CoordTrait, ObjectT: Debug + Clone> ObjSpace<CoordT, ObjectT> {
    pub fn new(dimension: usize, min_records: usize, max_records: usize) -> Self {
        Self::with_data_nodes(
            dimension,
            min_records,
            max_records,
            ShrinkableStorage::new(),
        )
    }

    pub fn with_data(
        dimension: usize,
        min_records: usize,
        max_records: usize,
        data: impl IntoIterator<Item = (ObjectT, MBR<CoordT>)>,
    ) -> Self {
        let mut data_nodes = ShrinkableStorage::new();
        data_nodes.extend(
            data.into_iter()
                .map(|(object, mbr)| Self::make_data_node_raw(object, mbr)),
        );

        Self::with_data_nodes(dimension, min_records, max_records, data_nodes)
    }

    pub fn clone_shrinked(&self) -> Self {
        Self::with_data_nodes(
            self.dimension,
            self.min_records,
            self.max_records,
            self.data_nodes.shrink(),
        )
    }

    pub(crate) fn with_data_nodes(
        dimension: usize,
        min_records: usize,
        max_records: usize,
        data_nodes: ShrinkableStorage<DataNode<CoordT, ObjectT>>,
    ) -> Self {
        assert!(dimension > 0);
        assert!(min_records >= 2);
        assert!(min_records <= (max_records as f64 / 2.0).ceil() as usize);

        let mut storage = Self {
            nodes: vec![],
            data_nodes,
            dimension,
            min_records,
            max_records,
            root_id: RecordId::Root,
        };

        storage.root_id = storage.make_node(RecordIdKind::Leaf);

        storage
    }

    pub(crate) fn clear_tree_structure(&mut self) {
        self.nodes.clear();

        self.root_id = self.make_node(RecordIdKind::Leaf);
    }

    pub(crate) fn make_node(&mut self, node_id_kind: RecordIdKind) -> RecordId {
        self.make_node_with_mbr(node_id_kind, unsafe { MBR::undefined() })
    }

    pub(crate) fn make_node_with_mbr(
        &mut self,
        node_id_kind: RecordIdKind,
        mbr: MBR<CoordT>,
    ) -> RecordId {
        let node = InternalNode::with_mbr(self.max_records as usize, mbr);

        let id = self.nodes.len();
        self.nodes.push(node);

        RecordId::from_node_id(id, node_id_kind)
    }

    pub fn make_data_node(&mut self, object: ObjectT, mbr: MBR<CoordT>) -> NodeId {
        let node = Self::make_data_node_raw(object, mbr);

        self.data_nodes.insert(node)
    }

    fn make_data_node_raw(object: ObjectT, mbr: MBR<CoordT>) -> DataNode<CoordT, ObjectT> {
        Node {
            parent_id: RecordId::Root,
            mbr,
            payload: object,
        }
    }

    pub fn data_num(&self) -> usize {
        self.data_nodes.volume()
    }

    pub fn is_empty(&self) -> bool {
        self.data_nodes.is_empty()
    }

    pub fn is_unordered(&self) -> bool {
        // Data is exists, but the root node has no children.
        !self.is_empty() && self.nodes[0].payload.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &ObjectT, &MBR<CoordT>)> {
        self.data_nodes
            .iter()
            .map(|(id, node)| (id, &node.payload, &node.mbr))
    }

    pub fn get_data_mbr(&self, id: NodeId) -> &MBR<CoordT> {
        &self.data_nodes.get(id).mbr
    }

    pub fn get_data_payload(&self, id: NodeId) -> &ObjectT {
        &self.get_data(id).payload
    }

    pub fn get_root_mbr(&self) -> &MBR<CoordT> {
        self.get_mbr(self.root_id)
    }

    pub(crate) fn iter_data_ids(&self) -> impl Iterator<Item = RecordId> {
        self.data_nodes.iter_ids().map(RecordId::Data)
    }

    pub(crate) fn mark_as_removed<I: Iterator<Item = NodeId>>(&mut self, data_ids: I) {
        unsafe {
            self.data_nodes.free_ids(data_ids);
        }
    }

    pub(crate) fn restore_removed(&mut self) {
        self.data_nodes.restore_freed();
    }

    pub(crate) fn set_parent_info(&mut self, id: RecordId, parent_id: RecordId) {
        match id {
            RecordId::Data(id) => {
                let node = self.get_data_mut(id);
                node.parent_id = parent_id;
            }
            _ => {
                let node = &mut self.nodes[id.as_node_id()];
                node.parent_id = parent_id;
            }
        }
    }

    pub(crate) fn add_child(&mut self, id: RecordId, child_id: RecordId) {
        let child_mbr = self.get_mbr(child_id).clone();
        let node = self.get_node_mut(id);

        node.payload.push(child_id);

        let new_parent_mbr = if node.payload.len() == 1 {
            child_mbr
        } else {
            mbr::common_mbr(&node.mbr, &child_mbr)
        };

        node.mbr = new_parent_mbr;
    }

    /// # Safety
    /// This fn will not adjust parent's MBR,
    /// the caller has to do this himself.
    pub(crate) unsafe fn add_child_raw(&mut self, id: RecordId, child_id: RecordId) {
        self.get_node_mut(id).payload.push(child_id);
    }

    pub(crate) fn get_mbr(&self, id: RecordId) -> &MBR<CoordT> {
        match id {
            RecordId::Data(id) => self.get_data_mbr(id),
            _ => &self.nodes[id.as_node_id()].mbr,
        }
    }

    pub(crate) fn set_mbr(&mut self, id: RecordId, mbr: MBR<CoordT>) {
        match id {
            RecordId::Data(id) => self.data_nodes.get_mut(id).mbr = mbr,
            _ => self.nodes[id.as_node_id()].mbr = mbr,
        }
    }

    pub(crate) fn get_node(&self, id: RecordId) -> &InternalNode<CoordT> {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        &self.nodes[id.as_node_id()]
    }

    pub(crate) fn get_node_mut(&mut self, id: RecordId) -> &mut InternalNode<CoordT> {
        debug_assert! {
            !matches!(id, RecordId::Data(_)),
            "data id is not allowed here"
        };

        &mut self.nodes[id.as_node_id()]
    }

    pub(crate) fn get_data(&self, id: NodeId) -> &DataNode<CoordT, ObjectT> {
        self.data_nodes.get(id)
    }

    pub(crate) fn get_data_mut(&mut self, id: NodeId) -> &mut DataNode<CoordT, ObjectT> {
        self.data_nodes.get_mut(id)
    }
}

impl<CoordT: CoordTrait, ObjectT: Clone + Debug> Extend<(ObjectT, MBR<CoordT>)>
    for ObjSpace<CoordT, ObjectT>
{
    fn extend<T: IntoIterator<Item = (ObjectT, MBR<CoordT>)>>(&mut self, iter: T) {
        self.data_nodes.extend(
            iter.into_iter()
                .map(|(object, mbr)| Self::make_data_node_raw(object, mbr)),
        );
    }
}
