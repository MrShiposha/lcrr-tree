use {
    crate::{mbr, tree::node::NodeId, LCRRTree},
    std::collections::hash_set::HashSet,
};

#[test]
fn test_tree_leaf() {
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
