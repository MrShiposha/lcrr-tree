use {crate::mbr::MBR, id_cache::Id};

pub type NodeId = Id;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordId {
    Root,
    Internal(NodeId),
    Leaf(NodeId),
    Data(NodeId),
}

impl RecordId {
    pub fn make_sibling_id(&self, id: NodeId) -> RecordId {
        match self {
            RecordId::Root => panic!("root can't have siblings"),
            RecordId::Internal(_) => RecordId::Internal(id),
            RecordId::Leaf(_) => RecordId::Leaf(id),
            RecordId::Data(_) => RecordId::Data(id),
        }
    }

    pub fn as_node_id(&self) -> NodeId {
        match self {
            RecordId::Root => panic!("unable to get root id"),
            RecordId::Internal(id) => *id,
            RecordId::Leaf(id) => *id,
            RecordId::Data(id) => *id,
        }
    }
}

pub struct Node<CoordT, PayloadT> {
    pub(crate) parent_id: RecordId,
    pub(crate) mbr: MBR<CoordT>,
    pub(crate) payload: PayloadT,
}

impl<CoordT, PayloadT> Node<CoordT, PayloadT> {
    pub fn parent_id(&self) -> RecordId {
        self.parent_id
    }

    pub fn mbr(&self) -> &MBR<CoordT> {
        &self.mbr
    }

    pub fn payload(&self) -> &PayloadT {
        &self.payload
    }
}
