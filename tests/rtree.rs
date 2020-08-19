use {
    lcrr_tree::{mbr, InternalNode, LCRRTree, Node, RecordId, Visitor, MBR},
    regex::Regex,
    rtree_test::{painter::*, Coord, Rect, TestCase},
    std::{
        collections::HashSet,
        env,
        fs::{create_dir, read_dir},
        path::Path,
    },
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
    let (tree, founded) = test_case_helper(&case);

    if let Ok(var) = env::var("RTT_DUMP_DIR") {
        let dump_dir = Path::new(&var);
        if !dump_dir.exists() {
            create_dir(dump_dir).unwrap();
        }

        assert!(dump_dir.is_dir());

        let mut dump_file_path = dump_dir.join(path.file_name().unwrap());
        dump_file_path.set_extension("dump.png");

        dump_tree(dump_file_path, tree, case, founded);
    }
}

fn test_case_helper(case: &TestCase) -> (LCRRTree<Coord, usize>, HashSet<usize>) {
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

    (rtree, founded)
}

fn as_mbr(rect: &Rect) -> MBR<Coord> {
    mbr! {
        X = [rect.left; rect.right],
        Y = [rect.top; rect.bottom]
    }
}

fn as_rect(mbr: &MBR<Coord>) -> Rect {
    Rect {
        left: mbr.bounds(0).min,
        right: mbr.bounds(0).max,
        top: mbr.bounds(1).min,
        bottom: mbr.bounds(1).max,
    }
}

fn dump_tree<P: AsRef<Path>>(
    path: P,
    tree: LCRRTree<Coord, usize>,
    mut test_case: TestCase,
    actually_founded: HashSet<usize>,
) {
    let mut painter = Painter::new(800, 600);

    test_case.founded = actually_founded;
    painter.draw_test_case(&test_case);

    let mut dumper = Dumper::new(painter);
    tree.visit(&mut dumper);

    dumper.dump(path);
}

struct Dumper {
    painter: Painter,
    lvl: u16,
}

impl Dumper {
    fn new(painter: Painter) -> Self {
        Self { painter, lvl: 0 }
    }

    fn dump<P: AsRef<Path>>(self, path: P) {
        self.painter.save_image(path);
    }
}

impl Visitor<Coord, usize> for Dumper {
    fn enter_node(&mut self, id: RecordId, node: &InternalNode<Coord>) {
        let h = 360 - 40 * (self.lvl % (360 / 40));
        let s = 100;
        let v = 100;

        self.painter.draw_indexed_rect(
            &as_rect(&node.mbr()),
            ColorHSV(h, s, v).into_rgb(),
            id.as_node_id(),
        );

        self.lvl += 1;
    }

    fn leave_node(&mut self, _: RecordId, _: &InternalNode<Coord>) {
        self.lvl -= 1;
    }

    fn visit_data(&mut self, _: RecordId, _: &Node<Coord, usize>) {
        // do nothing
    }
}
