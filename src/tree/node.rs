use {
    crate::mbr::{CoordTrait, MBR},
    id_storage::Id,
    std::string::ToString,
};

pub type NodeId = Id;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordId {
    Root,
    Internal(NodeId),
    Leaf(NodeId),
    Data(NodeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecordIdKind {
    Internal,
    Leaf,
}

impl RecordId {
    pub fn from_node_id(id: NodeId, kind: RecordIdKind) -> Self {
        match kind {
            RecordIdKind::Internal => RecordId::Internal(id),
            RecordIdKind::Leaf => RecordId::Leaf(id),
        }
    }

    pub fn as_node_id(&self) -> NodeId {
        match self {
            RecordId::Root => panic!("unable to get root node id (actual root is internal node)"),
            RecordId::Internal(id) => *id,
            RecordId::Leaf(id) => *id,
            RecordId::Data(id) => *id,
        }
    }

    pub fn kind(&self) -> RecordIdKind {
        match self {
            RecordId::Internal(_) => RecordIdKind::Internal,
            RecordId::Leaf(_) => RecordIdKind::Leaf,
            _ => panic!("invalid node kind"),
        }
    }

    pub fn set_kind(&mut self, kind: RecordIdKind) {
        *self = match kind {
            RecordIdKind::Internal => RecordId::Internal(self.as_node_id()),
            RecordIdKind::Leaf => RecordId::Leaf(self.as_node_id()),
        };
    }
}

impl ToString for RecordId {
    fn to_string(&self) -> String {
        match self {
            RecordId::Data(id) => format!["Data({})", id],
            RecordId::Leaf(id) => format!["Leaf({})", id],
            RecordId::Internal(id) => format!["Internal({})", id],
            RecordId::Root => "Root".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node<CoordT: CoordTrait, PayloadT: Clone> {
    pub(crate) parent_id: RecordId,
    pub(crate) mbr: MBR<CoordT>,
    pub(crate) payload: PayloadT,
}

impl<CoordT: CoordTrait, PayloadT: Clone> Node<CoordT, PayloadT> {
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
