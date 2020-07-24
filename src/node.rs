use {
    id_cache::Id,
    crate::mbr::MBR
};

pub type NodeId = Id;

#[derive(Debug, Clone, Copy)]
pub enum RecordId {
    RootParent,
    Root(NodeId),
    Internal(NodeId),
    Leaf(NodeId),
    Data(NodeId)
}

impl RecordId {
    pub fn as_node_id(&self) -> NodeId {
        match self {
            RecordId::Root(id) => *id,
            RecordId::Internal(id) => *id,
            RecordId::Leaf(id) => *id,
            RecordId::Data(id) => *id,
            RootParent => panic!("root node has no parent id")
        }
    }
}

pub struct Node<CoordT, PayloadT> {
    pub(crate) parent_id: RecordId,
    pub(crate) mbr: MBR<CoordT>,
    pub(crate) payload: PayloadT
}