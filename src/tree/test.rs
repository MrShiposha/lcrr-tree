use {
    crate::{
        mbr,
        tree::node::{Node, NodeId, RecordId},
        InternalNode, LCRRTree, Visitor,
    },
    std::collections::hash_set::HashSet,
};

include!("../../tests/res/test_logger.rs");

#[test]
fn test_tree_leaf() {
    init_logger();

    let tree = LCRRTree::new(2, 2, 5);
    let root_id = tree.storage.read().unwrap().root_id;

    let first_id = tree.insert(
        "First",
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    {
        let storage = tree.storage.read().unwrap();
        let tree_mbr = storage.get_mbr(root_id);

        tree.access_object(first_id, |mbr, object| {
            assert_eq!(*object, "First");
            assert_eq!(*tree_mbr, *mbr);
        });

        assert_eq!(storage.collisions.edge_count(), 0);
    }

    let second_id = tree.insert(
        "Second",
        mbr! {
            X = [-5; -3],
            Y = [-5;  5]
        },
    );

    tree.access_object(second_id, |_, object| {
        assert_eq!(*object, "Second");
    });

    {
        let storage = tree.storage.read().unwrap();
        let tree_mbr = storage.get_mbr(root_id);

        assert_eq!(
            *tree_mbr,
            mbr! {
                X = [-5; 10],
                Y = [-5; 10]
            }
        );

        assert_eq!(storage.collisions.edge_count(), 0);
    }

    let set = tree.search(&mbr! {
        X = [-2; -1],
        Y = [-3;  3]
    });

    assert!(set.is_empty());

    let set = tree.search(&mbr! {
        X = [7; 15],
        Y = [2;  3]
    });

    assert_eq!(set, vec![first_id]);

    let set = tree.search(&mbr! {
        X = [-7; -4],
        Y = [ 2;  3]
    });

    assert_eq!(set, vec![second_id]);

    let set = tree.search(&mbr! {
        X = [-4; 4],
        Y = [ 2; 3]
    });

    assert_eq!(set, vec![first_id, second_id]);

    let third_id = tree.insert(
        "Third",
        mbr! {
            X = [-4; 4],
            Y = [ 2; 3]
        },
    );

    tree.access_object(third_id, |_, object| {
        assert_eq!(*object, "Third");
    });

    {
        let storage = tree.storage.read().unwrap();
        let tree_mbr = storage.get_mbr(root_id);

        assert_eq!(
            *tree_mbr,
            mbr! {
                X = [-5; 10],
                Y = [-5; 10]
            }
        );

        assert_eq!(storage.collisions.edge_count(), 2);
        assert!(storage.collisions.contains_edge(first_id, third_id));
        assert!(storage.collisions.contains_edge(second_id, third_id));
    }
}

#[test]
fn test_tree_split() {
    init_logger();

    let tree = LCRRTree::new(2, 2, 5);
    tree.insert(
        1,
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    tree.insert(
        2,
        mbr! {
            X = [11; 21],
            Y = [ 0; 10]
        },
    );

    tree.insert(
        3,
        mbr! {
            X = [22; 32],
            Y = [ 0; 10]
        },
    );

    tree.insert(
        4,
        mbr! {
            X = [ 0; 10],
            Y = [11; 21]
        },
    );

    tree.insert(
        5,
        mbr! {
            X = [11; 21],
            Y = [11; 21]
        },
    );

    tree.insert(
        6,
        mbr! {
            X = [22; 32],
            Y = [11; 21]
        },
    );

    let set: HashSet<NodeId> = tree
        .search(&mbr! {
            X = [3; 25],
            Y = [3; 15]
        })
        .iter()
        .cloned()
        .collect();

    let expected: HashSet<NodeId> = [0, 1, 2, 3, 4, 5].iter().cloned().collect();

    assert_eq!(set, expected);
}

#[test]
fn test_tree_same_delta() {
    init_logger();

    let tree = LCRRTree::new(2, 2, 5);
    let mut storage = tree.storage.write().unwrap();

    let first_mbr = mbr! {
        X = [0; 5],
        Y = [0; 5]
    };
    let first_node_id = RecordId::Leaf(storage.nodes.insert(Node {
        parent_id: RecordId::Root,
        mbr: first_mbr.clone(),
        payload: vec![],
    }));

    let second_mbr = mbr! {
        X = [12; 13],
        Y = [-1; 4]
    };
    let second_node_id = RecordId::Leaf(storage.nodes.insert(Node {
        parent_id: RecordId::Root,
        mbr: second_mbr.clone(),
        payload: vec![],
    }));

    let root_id = storage.root_id;
    let root = storage.nodes.get_mut(root_id.as_node_id());
    root.payload.push(first_node_id);
    root.payload.push(second_node_id);
    root.mbr = mbr::common_mbr(&first_mbr, &second_mbr);
    storage.root_id = RecordId::Internal(root_id.as_node_id());

    let node_0_data_0 = RecordId::Data(storage.data_nodes.insert(Node {
        parent_id: first_node_id,
        mbr: mbr! {
            X = [0; 3],
            Y = [0; 3]
        },
        payload: 0,
    }));

    let node_0_data_1 = RecordId::Data(storage.data_nodes.insert(Node {
        parent_id: first_node_id,
        mbr: mbr! {
            X = [4; 5],
            Y = [4; 5]
        },
        payload: 1,
    }));

    let node_1_data_0 = RecordId::Data(storage.data_nodes.insert(Node {
        parent_id: second_node_id,
        mbr: mbr! {
            X = [13; 14],
            Y = [-1; 0]
        },
        payload: 0,
    }));

    let node_1_data_1 = RecordId::Data(storage.data_nodes.insert(Node {
        parent_id: second_node_id,
        mbr: mbr! {
            X = [13; 14],
            Y = [3; 4]
        },
        payload: 1,
    }));

    let first = storage.get_node_mut(first_node_id);
    first.payload.push(node_0_data_0);
    first.payload.push(node_0_data_1);

    let second = storage.get_node_mut(second_node_id);
    second.payload.push(node_1_data_0);
    second.payload.push(node_1_data_1);
    std::mem::drop(storage);

    let test_record_id = tree.insert(
        2,
        mbr! {
            X = [8; 10],
            Y = [3; 5]
        },
    );

    let storage = tree.storage.read().unwrap();
    let test_leaf_id = storage.get_data(test_record_id).parent_id;
    assert_eq!(test_leaf_id, second_node_id);
}

#[test]
fn test_tree_visitor() {
    struct TestVisitor {
        lvl: usize,
    }

    impl TestVisitor {
        fn new() -> Self {
            Self { lvl: 0 }
        }
    }

    impl Visitor<i32, i32> for TestVisitor {
        fn enter_node(&mut self, node: &InternalNode<i32>) {
            match node.parent_id {
                RecordId::Root => assert_eq!(self.lvl, 0),
                RecordId::Internal(_) => assert_eq!(self.lvl, 1),
                RecordId::Leaf(_) => assert_eq!(self.lvl, 2),
                _ => unreachable!(),
            }

            self.lvl += 1;
        }

        fn leave_node(&mut self, _: &InternalNode<i32>) {
            self.lvl -= 1;
        }

        fn visit_data(&mut self, node: &Node<i32, i32>) {
            assert!(matches!(node.payload, 1..=12));
            assert_eq!(self.lvl, 2);
        }
    }

    init_logger();

    let tree = LCRRTree::new(2, 2, 5);
    tree.insert(
        1,
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    tree.insert(
        2,
        mbr! {
            X = [11; 21],
            Y = [ 0; 10]
        },
    );

    tree.insert(
        3,
        mbr! {
            X = [22; 32],
            Y = [ 0; 10]
        },
    );

    tree.insert(
        4,
        mbr! {
            X = [ 0; 10],
            Y = [11; 21]
        },
    );

    tree.insert(
        5,
        mbr! {
            X = [11; 21],
            Y = [11; 21]
        },
    );

    tree.insert(
        6,
        mbr! {
            X = [22; 32],
            Y = [11; 21]
        },
    );

    tree.insert(
        7,
        mbr! {
            X = [32; 42],
            Y = [11; 21]
        },
    );

    tree.insert(
        8,
        mbr! {
            X = [42; 52],
            Y = [11; 21]
        },
    );

    tree.insert(
        9,
        mbr! {
            X = [52; 62],
            Y = [11; 21]
        },
    );

    tree.insert(
        10,
        mbr! {
            X = [62; 72],
            Y = [11; 21]
        },
    );

    tree.insert(
        11,
        mbr! {
            X = [82; 92],
            Y = [11; 21]
        },
    );

    tree.insert(
        12,
        mbr! {
            X = [92; 102],
            Y = [11; 21]
        },
    );

    let mut visitor = TestVisitor::new();
    tree.visit(&mut visitor);
}
