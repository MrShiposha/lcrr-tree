use {
    crate::{
        tree::{visitor::Visitor, CoordTrait, DataNode, InternalNode},
        RecordId,
    },
    dbg_vis::{DebugVis, DebugVisJSON},
    petgraph::graphmap::UnGraphMap,
};

pub struct LRTreeDbgVis {
    graph: UnGraphMap<RecordId, ()>,
}

impl LRTreeDbgVis {
    pub fn new() -> Self {
        Self {
            graph: UnGraphMap::new(),
        }
    }
}

impl<CoordT: CoordTrait, ObjectT: Clone> Visitor<CoordT, ObjectT> for LRTreeDbgVis {
    fn enter_node(&mut self, record_id: RecordId, node: &InternalNode<CoordT>) {
        let parent_id = node.parent_id;

        self.graph.add_edge(record_id, parent_id, ());
    }

    fn leave_node(&mut self, _: RecordId, _: &InternalNode<CoordT>) {
        // do nothing
    }

    fn visit_data(&mut self, record_id: RecordId, node: &DataNode<CoordT, ObjectT>) {
        let parent_id = node.parent_id;

        self.graph.add_edge(record_id, parent_id, ());
    }
}

impl DebugVis for LRTreeDbgVis {
    fn debug_visualize(&self) -> DebugVisJSON {
        self.graph.debug_visualize()
    }
}
