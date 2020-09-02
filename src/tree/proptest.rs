use {
    crate::{
        mbr,
        tree::{self, test::init_logger},
        LRTree,
    },
    proptest::{
        collection::{self, size_range},
        prelude::*,
    },
    std::{
        collections::HashSet,
        sync::atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(feature = "with-dbg-vis")]
use dbg_vis::DebugVis;

use dbg_vis::dbg_vis;

type Coord = i64;
type Bounds = mbr::Bounds<Coord>;
type MBR = mbr::MBR<Coord>;
type Object = usize;
type Tree = LRTree<Coord, Object>;

const MIN_COORD: Coord = 50;
const MAX_COORD: Coord = 550;

const MIN_RECORDS: usize = 2;
const MAX_RECORDS: usize = 20;

const MIN_MBR_NUM: usize = 0;
const MAX_MBR_NUM: usize = 200;

static TEST_2D_NUM: AtomicUsize = AtomicUsize::new(0);

// include!("../../tests/res/test_dumper.rs");

#[derive(Debug)]
struct TestParams {
    dim: usize,
    min_records: usize,
    max_records: usize,
    mbrs: Vec<MBR>,
    additional_mbrs: Vec<MBR>,
    search_mbr: MBR,
    mark_removed: HashSet<usize>,
    alpha: f32,
}

impl Arbitrary for TestParams {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        dim()
            .prop_flat_map(|dim| {
                let additional_mbrs = mbrs(dim, MIN_MBR_NUM, MAX_MBR_NUM / 2);
                let mbrs = mbrs(dim, MIN_MBR_NUM, MAX_MBR_NUM);

                (Just(dim), mbrs, additional_mbrs)
            })
            .prop_flat_map(|(dim, mbrs, additional_mbrs)| {
                let mark_removed = any_mark_removed(mbrs.len());
                let alpha = (0..=50).prop_map(|v| v as f32 * 0.01); // [0.0, 0.5]

                (
                    Just(dim),
                    record_nums(),
                    Just(mbrs),
                    Just(additional_mbrs),
                    any_with::<MBR>(dim),
                    mark_removed,
                    alpha,
                )
            })
            .prop_map(
                |(dim, record_nums, mbrs, additional_mbrs, search_mbr, mark_removed, alpha)| {
                    let (min_records, max_records) = record_nums;

                    TestParams {
                        dim,
                        min_records,
                        max_records,
                        mbrs,
                        additional_mbrs,
                        search_mbr,
                        mark_removed,
                        alpha,
                    }
                },
            )
            .boxed()
    }
}

impl Arbitrary for Bounds {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (MIN_COORD + 1..MAX_COORD)
            .prop_flat_map(|max| {
                let min = MIN_COORD..max;
                (min, Just(max))
            })
            .prop_map(|(min, max)| Self::new(min, max))
            .boxed()
    }
}

impl Arbitrary for MBR {
    type Parameters = usize;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(dim: Self::Parameters) -> Self::Strategy {
        any_with::<Vec<Bounds>>(size_range(dim).lift())
            .prop_map(MBR::new)
            .boxed()
    }
}

fn dim() -> impl Strategy<Value = usize> {
    1..=7usize
}

fn record_nums() -> impl Strategy<Value = (usize, usize)> {
    (MIN_RECORDS + 1..MAX_RECORDS).prop_flat_map(|max_records| {
        let min_records_limit = (max_records as f32 / 2.0).ceil() as usize;

        let min_records = MIN_RECORDS..=min_records_limit;

        (min_records, Just(max_records))
    })
}

fn mbrs(dim: usize, min: usize, max: usize) -> impl Strategy<Value = Vec<MBR>> {
    (min..max).prop_flat_map(move |num| any_with::<Vec<MBR>>(size_range(num).with(dim)))
}

fn any_mark_removed(obj_len: usize) -> impl Strategy<Value = HashSet<usize>> {
    collection::hash_set(0..obj_len, 0..=obj_len)
}

fn as_objects<'i>(mbrs: impl Iterator<Item = &'i MBR>) -> impl Iterator<Item = (usize, MBR)> {
    mbrs.cloned().enumerate()
}

fn dynamic_build_tree(test_params: &TestParams) -> Tree {
    dbg_vis!(let watch: { tree });

    let tree = Tree::with_obj_space(tree::ObjSpace::new(
        test_params.dim,
        test_params.min_records,
        test_params.max_records,
    ));

    for (i, mbr) in as_objects(test_params.mbrs.iter()) {
        tree.insert(i, mbr);
        dbg_vis!(watch.tree);
    }

    tree
}

fn static_build_tree(test_params: &TestParams) -> Tree {
    dbg_vis!(let watch: { tree });

    let obj_space = tree::ObjSpace::with_data(
        test_params.dim,
        test_params.min_records,
        test_params.max_records,
        as_objects(test_params.mbrs.iter()),
    );

    let tree = Tree::with_obj_space(tree::ObjSpace::new(
        test_params.dim,
        test_params.min_records,
        test_params.max_records,
    ));

    dbg_vis!(watch.tree);

    let mut builder = tree::LRTreeBuilder::with_obj_space(obj_space);
    builder.build_with_alpha(test_params.alpha);

    tree.set_build(builder);

    dbg_vis!(watch.tree);

    tree
}

fn hybrid_build_tree(test_params: &TestParams) -> Tree {
    let tree = static_build_tree(test_params);

    dbg_vis!(let watch: { tree });
    dbg_vis!(watch.tree);

    let base_i = test_params.mbrs.len();
    for (i, mbr) in as_objects(test_params.additional_mbrs.iter()) {
        tree.insert(base_i + i, mbr);
        dbg_vis!(watch.tree);
    }

    tree
}

fn remake_static_tree(old_tree: &Tree) -> Tree {
    let obj_space = old_tree.lock_obj_space().clone_shrinked();

    Tree::with_obj_space(obj_space)
}

fn search_intersections<'a>(
    search_mbr: &MBR,
    data_mbrs: impl Iterator<Item = (Object, MBR)>,
) -> HashSet<Object> {
    let mut found = HashSet::new();

    for (object, mbr) in data_mbrs {
        if mbr::intersects(search_mbr, &mbr) {
            found.insert(object);
        }
    }

    found
}

fn check_tree(
    tree: &Tree,
    search_mbr: &MBR,
    expected: &HashSet<usize>,
    tree_name: &'static str,
) -> Result<(), TestCaseError> {
    let found: HashSet<_> = tree
        .search(&search_mbr)
        .iter()
        .map(|&id| tree.access_object(id, |&object, _| object))
        .collect();

    prop_assert_eq!(found, expected.clone(), "{} tree failure", tree_name);
    Ok(())
}

proptest! {
    #[test]
    fn tree_property_test(test_params in any::<TestParams>()) {
        init_logger();

        dbg_vis! {
            let watch: {
                dyn_tree,
                static_tree,
                removed_tree,
                hybrid_tree
            }
        }

        let expected_found = search_intersections(
            &test_params.search_mbr,
            as_objects(test_params.mbrs.iter())
        );

        let dyn_tree = dynamic_build_tree(&test_params);
        dbg_vis!(watch.dyn_tree);

        check_tree(
            &dyn_tree,
            &test_params.search_mbr,
            &expected_found,
            "dynamic"
        )?;

        let static_tree = static_build_tree(&test_params);
        dbg_vis!(watch.static_tree);

        check_tree(
            &static_tree,
            &test_params.search_mbr,
            &expected_found,
            "static"
        )?;

        static_tree.mark_as_removed(test_params.mark_removed.iter().cloned());

        let static_tree_removed = remake_static_tree(&static_tree);
        dbg_vis!(watch.removed_tree = static_tree_removed);

        let expected_found_removed = expected_found.difference(&test_params.mark_removed)
            .cloned()
            .collect();

        check_tree(
            &static_tree_removed,
            &test_params.search_mbr,
            &expected_found_removed,
            "static-removed"
        )?;

        let hybrid_expected_found = search_intersections(
            &test_params.search_mbr,
            as_objects(
                test_params.mbrs.iter()
                    .chain(test_params.additional_mbrs.iter())
            )
        );

        let hybrid_tree = hybrid_build_tree(&test_params);
        dbg_vis!(watch.hybrid_tree);

        check_tree(
            &hybrid_tree,
            &test_params.search_mbr,
            &hybrid_expected_found,
            "hybrid"
        )?;

        if test_params.dim == 2 {
            TEST_2D_NUM.fetch_add(1, Ordering::SeqCst);
        }
    }
}
