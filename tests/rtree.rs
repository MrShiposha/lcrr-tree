use {
    lcrr_tree::{mbr, LCRRTree, MBR},
    regex::Regex,
    rtree_test::{Coord, Rect, TestCase},
    std::{collections::HashSet, env, fs::read_dir, path::Path},
};

include!("../tests/res/test_logger.rs");

const TEST_CASES_DIR: &'static str = "tests/res/cases";

#[test]
fn rtree_cases() {
    init_logger();

    let cases_dir = Path::new(TEST_CASES_DIR);
    assert!(
        cases_dir.is_dir(),
        "`{}` is expected to be a directory",
        TEST_CASES_DIR
    );

    let regex = match env::var("RTT_REGEX") {
        Ok(var) => Regex::new(&var),
        _ => Regex::new(".*"),
    };

    assert!(regex.is_ok(), "$RTT_REGEX contains invalid regex");
    let regex = regex.unwrap();

    let dir_iter = read_dir(cases_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| regex.is_match(&path.to_string_lossy()));

    for path in dir_iter {
        handle_test_case(&path);
    }
}

fn handle_test_case(path: &Path) {
    assert!(
        path.is_file(),
        "`{}` is expected to be a file",
        path.display()
    );
    println!("case: `{}`", path.file_name().unwrap().to_string_lossy());

    let case = TestCase::load(path);
    test_case_helper(case);
}

fn test_case_helper(case: TestCase) {
    let rtree = LCRRTree::new(2, 2, 5);

    for (i, rect) in case.data_rects.iter().enumerate() {
        rtree.insert(i, as_mbr(rect));
    }

    let founded = rtree
        .search(&as_mbr(&case.search_rect))
        .iter()
        .map(|&id| rtree.access_object(id, |_, &i| i))
        .collect::<HashSet<_>>();

    assert_eq!(founded, case.founded);
}

fn as_mbr(rect: &Rect) -> MBR<Coord> {
    mbr! {
        X = [rect.left; rect.right],
        Y = [rect.top; rect.bottom]
    }
}
