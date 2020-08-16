use super::{InternalNode, Node};

pub trait Visitor<CoordT, ObjectT> {
    fn enter_node(&mut self, node: &InternalNode<CoordT>);

    fn leave_node(&mut self, node: &InternalNode<CoordT>);

    fn visit_data(&mut self, node: &Node<CoordT, ObjectT>);
}
