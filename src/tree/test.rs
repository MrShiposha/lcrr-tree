use {
    crate::{
        bind, mbr,
        tree::{
            node::{NodeId, RecordId, RecordIdKind},
            DataNode,
        },
        CoordTrait, InsertHandler, InternalNode, LRTree, ObjSpace, Visitor,
    },
    std::collections::hash_set::HashSet,
};

use {
    log::{LevelFilter, Metadata, Record},
    std::sync::Once,
};

struct Logger;

impl log::Log for Logger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        println!(
            "|{}| {} - {}",
            record.target(),
            record.level(),
            record.args()
        );
    }

    fn flush(&self) {}
}

static LOGGER: Logger = Logger;
static INIT: Once = Once::new();

pub fn init_logger() {
    INIT.call_once(|| {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .unwrap();
    })
}

#[test]
fn test_tree_leaf() {
    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
    let root_id = tree.obj_space.read().unwrap().root_id;

    let first_id = tree.insert(
        "First",
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    {
        let obj_space = tree.obj_space.read().unwrap();
        let tree_mbr = obj_space.get_mbr(root_id);

        tree.access_object(first_id, |object, mbr| {
            assert_eq!(*object, "First");
            assert_eq!(*tree_mbr, *mbr);
        });
    }

    let second_id = tree.insert(
        "Second",
        mbr! {
            X = [-5; -3],
            Y = [-5;  5]
        },
    );

    tree.access_object(second_id, |object, _| {
        assert_eq!(*object, "Second");
    });

    {
        let obj_space = tree.obj_space.read().unwrap();
        let tree_mbr = obj_space.get_mbr(root_id);

        assert_eq!(
            *tree_mbr,
            mbr! {
                X = [-5; 10],
                Y = [-5; 10]
            }
        );
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

    tree.access_object(third_id, |object, _| {
        assert_eq!(*object, "Third");
    });

    {
        let obj_space = tree.obj_space.read().unwrap();
        let tree_mbr = obj_space.get_mbr(root_id);

        assert_eq!(
            *tree_mbr,
            mbr! {
                X = [-5; 10],
                Y = [-5; 10]
            }
        );
    }
}

#[test]
fn test_tree_search_access() {
    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
    let root_id = tree.obj_space.read().unwrap().root_id;

    let first_id = tree.insert(
        "First",
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    {
        let obj_space = tree.obj_space.read().unwrap();
        let tree_mbr = obj_space.get_mbr(root_id);

        tree.access_object(first_id, |object, mbr| {
            assert_eq!(*object, "First");
            assert_eq!(*tree_mbr, *mbr);
        });
    }

    let second_id = tree.insert(
        "Second",
        mbr! {
            X = [-5; -3],
            Y = [-5;  5]
        },
    );

    tree.access_object(second_id, |object, _| {
        assert_eq!(*object, "Second");
    });

    {
        let obj_space = tree.obj_space.read().unwrap();
        let tree_mbr = obj_space.get_mbr(root_id);

        assert_eq!(
            *tree_mbr,
            mbr! {
                X = [-5; 10],
                Y = [-5; 10]
            }
        );
    }
    tree.search_access(
        &mbr! {
            X = [7; 15],
            Y = [2;  3]
        },
        |_, id| assert_eq!(id, first_id),
    );

    tree.search_access(
        &mbr! {
            X = [-7; -4],
            Y = [ 2;  3]
        },
        |_, id| assert_eq!(id, second_id),
    );

    tree.search_access(
        &mbr! {
            X = [-4; 4],
            Y = [ 2; 3]
        },
        |_, id| assert!([first_id, second_id].contains(&id)),
    );
}

#[test]
fn test_tree_builder_leaf() {
    init_logger();

    let data = vec![
        (
            "First",
            mbr! {
                X = [0; 10],
                Y = [0; 10]
            },
        ),
        (
            "Second",
            mbr! {
                X = [-5; -3],
                Y = [-5;  5]
            },
        ),
        (
            "Third",
            mbr! {
                X = [-4; 4],
                Y = [ 2; 3]
            },
        ),
    ];

    let tree = LRTree::with_obj_space(ObjSpace::with_data(2, 2, 5, data));

    let ids = tree.search(&mbr! {
        X = [-4; 7],
        Y = [-3; 5]
    });

    ids.iter()
        .map(|&id| tree.access_object(id, |&obj, _| obj))
        .for_each(|obj| {
            assert!(matches!(obj, "First" | "Second" | "Third"));
        });
}

#[test]
fn test_tree_split() {
    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
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
fn test_tree_insert_transaction() {
    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
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

    macro_rules! new_value {
        () => {
            7
        };
    }

    struct Handler;

    impl InsertHandler<i32, i32> for Handler {
        fn before_insert(&mut self, obj_space: &ObjSpace<i32, i32>, new_id: usize) {
            assert_eq!(*obj_space.get_data_payload(new_id), new_value![]);

            let expected: HashSet<NodeId> = [0, 1, 2, 3, 4, 5].iter().cloned().collect();
            let mut intersections = HashSet::new();

            LRTree::search_access_obj_space(obj_space, obj_space.get_data_mbr(new_id), |_, id| {
                intersections.insert(id);
            });

            assert_eq!(intersections, expected);
        }
    }

    tree.insert_transaction(
        new_value![],
        mbr! {
            X = [3; 25],
            Y = [3; 15]
        },
        &mut Handler,
    );
}

#[test]
fn test_tree_retain() {
    init_logger();

    let mut remove_values = vec![];
    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
    tree.insert(
        1,
        mbr! {
            X = [0; 10],
            Y = [0; 10]
        },
    );

    let value = tree.insert(
        2,
        mbr! {
            X = [11; 21],
            Y = [ 0; 10]
        },
    );
    remove_values.push(value);

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

    let value = tree.insert(
        5,
        mbr! {
            X = [11; 21],
            Y = [11; 21]
        },
    );
    remove_values.push(value);

    tree.insert(
        6,
        mbr! {
            X = [22; 32],
            Y = [11; 21]
        },
    );

    tree.retain(
        &mbr! {
            X = [3; 25],
            Y = [3; 15]
        },
        |obj_space, id| !remove_values.contains(&obj_space.get_data_payload(id)),
    );

    let root_mbr = tree.lock_obj_space().get_root_mbr().clone();
    let new_tree = LRTree::with_obj_space(tree.lock_obj_space().clone_shrinked());

    new_tree.search_access(&root_mbr, |obj_space, id| {
        assert!(!remove_values.contains(&obj_space.get_data_payload(id)))
    });
}

#[test]
fn test_tree_same_delta() {
    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
    let mut obj_space = tree.obj_space.write().unwrap();

    let first_node_id = obj_space.make_node(RecordIdKind::Leaf);
    let node = obj_space.get_node_mut(first_node_id);
    node.mbr = mbr! {
        X = [0; 5],
        Y = [0; 5]
    };

    let second_node_id = obj_space.make_node(RecordIdKind::Leaf);
    let node = obj_space.get_node_mut(second_node_id);
    node.mbr = mbr! {
        X = [12; 13],
        Y = [-1; 4]
    };

    let root_id = RecordId::from_node_id(obj_space.root_id.as_node_id(), RecordIdKind::Internal);
    obj_space.root_id = root_id;
    bind!([obj_space] root_id => first_node_id);
    bind!([obj_space] root_id => second_node_id);

    let node_0_data_0 = obj_space.make_data_node(
        0,
        mbr! {
            X = [0; 3],
            Y = [0; 3]
        },
    );
    obj_space.get_data_mut(node_0_data_0.as_node_id()).parent_id = first_node_id;

    let node_0_data_1 = obj_space.make_data_node(
        1,
        mbr! {
            X = [4; 5],
            Y = [4; 5]
        },
    );
    obj_space.get_data_mut(node_0_data_1.as_node_id()).parent_id = first_node_id;

    let node_1_data_0 = obj_space.make_data_node(
        0,
        mbr! {
            X = [12; 13],
            Y = [-1; 0]
        },
    );
    obj_space.get_data_mut(node_1_data_0.as_node_id()).parent_id = second_node_id;

    let node_1_data_1 = obj_space.make_data_node(
        1,
        mbr! {
            X = [12; 13],
            Y = [3; 4]
        },
    );
    obj_space.get_data_mut(node_1_data_1.as_node_id()).parent_id = second_node_id;

    let first = obj_space.get_node_mut(first_node_id);
    first.payload.push(node_0_data_0);
    first.payload.push(node_0_data_1);

    let second = obj_space.get_node_mut(second_node_id);
    second.payload.push(node_1_data_0);
    second.payload.push(node_1_data_1);
    std::mem::drop(obj_space);

    let test_record_id = tree.insert(
        2,
        mbr! {
            X = [8; 10],
            Y = [3; 5]
        },
    );

    let obj_space = tree.obj_space.read().unwrap();
    let test_leaf_id = obj_space.get_data(test_record_id).parent_id;
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
        fn enter_node(&mut self, record_id: RecordId, node: &InternalNode<i32>) {
            match node.parent_id {
                RecordId::Root => {
                    assert_eq!(self.lvl, 0);
                    assert_eq!(record_id, RecordId::Internal(2));
                }
                RecordId::Internal(_) => assert_eq!(self.lvl, 1),
                RecordId::Leaf(_) => assert_eq!(self.lvl, 2),
                _ => unreachable!(),
            }

            self.lvl += 1;
        }

        fn leave_node(&mut self, _: RecordId, _: &InternalNode<i32>) {
            self.lvl -= 1;
        }

        fn visit_data(&mut self, record_id: RecordId, node: &DataNode<i32, i32>) {
            assert!(matches!(node.payload, 1..=12));
            assert!(matches!(record_id, RecordId::Data(_)));
            assert_eq!(self.lvl, 2);
        }
    }

    init_logger();

    let tree = LRTree::with_obj_space(ObjSpace::new(2, 2, 5));
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
