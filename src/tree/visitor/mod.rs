use super::{CoordTrait, DataNode, InternalNode, RecordId};

#[cfg(feature = "with-dbg-vis")]
pub mod dbg_vis;

pub trait Visitor<CoordT: CoordTrait, ObjectT: Clone> {
    fn enter_node(&mut self, record_id: RecordId, node: &InternalNode<CoordT>);

    fn leave_node(&mut self, record_id: RecordId, node: &InternalNode<CoordT>);

    fn visit_data(&mut self, record_id: RecordId, node: &DataNode<CoordT, ObjectT>);
}
