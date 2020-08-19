use super::{InternalNode, Node, RecordId};

pub trait Visitor<CoordT, ObjectT> {
    fn enter_node(&mut self, record_id: RecordId, node: &InternalNode<CoordT>);

    fn leave_node(&mut self, record_id: RecordId, node: &InternalNode<CoordT>);

    fn visit_data(&mut self, record_id: RecordId, node: &Node<CoordT, ObjectT>);
}
