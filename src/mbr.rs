use std::ops::{Sub, Mul};

pub struct Bounds<CoordT> {
    min: CoordT,
    max: CoordT
}

impl<CoordT: Ord> Bounds<CoordT> {
    pub fn new(min: CoordT, max: CoordT) -> Self {
        debug_assert!(min < max, "a min bound mast be less than a max bound");

        Self {
            min,
            max
        }
    }

    pub fn is_in_bound(&self, value: &CoordT) -> bool {
        self.min <= *value && *value <= self.max
    }
}

impl<CoordT: Sub<Output=CoordT> + Clone> Bounds<CoordT> {
    pub fn length(&self) -> CoordT {
        self.max.clone() - self.min.clone()
    }
}

impl<CoordT: Clone> Clone for Bounds<CoordT> {
    fn clone(&self) -> Self {
        Self {
            min: self.min.clone(),
            max: self.max.clone()
        }
    }
}

/// Minimum bounding rectangle
pub struct MBR<CoordT> {
    bounds: Vec<Bounds<CoordT>>
}

impl<CoordT> MBR<CoordT> {
    pub fn new(bounds: Vec<Bounds<CoordT>>) -> Self {
        debug_assert!(!bounds.is_empty(), "MBR can't be zero-dimension");

        Self {
            bounds
        }
    }

    pub fn dimension(&self) -> usize {
        self.bounds.len()
    }
}

impl<CoordT: Sub<Output=CoordT> + Mul<Output=CoordT> + Clone> MBR<CoordT> {
    pub fn volume(&self) -> CoordT {
        let init_volume = self.bounds.first().unwrap().length();
        self.bounds.iter().skip(1).fold(
            init_volume,
            |acc, bounds| acc * bounds.length()
        )
    }
}

impl<CoordT: Clone> Clone for MBR<CoordT> {
    fn clone(&self) -> Self {
        Self {
            bounds: self.bounds.clone()
        }
    }
}

pub fn intersects<CoordT: Ord>(lhs: &MBR<CoordT>, rhs: &MBR<CoordT>) -> bool {
    if lhs as *const _ == rhs as *const _ {
        return true;
    }

    debug_assert_eq!(
        lhs.dimension(),
        rhs.dimension(),
        "unable to compare MBRs with different dimensions"
    );

    let mut intersected_axis = 0usize;
    for (self_bound, other_bound) in lhs.bounds.iter().zip(rhs.bounds.iter()) {
        if self_bound.is_in_bound(&other_bound.min)
        || self_bound.is_in_bound(&other_bound.max)
        || other_bound.is_in_bound(&self_bound.min) {
            intersected_axis += 1;
        }
    }

    intersected_axis == lhs.dimension()
}

pub fn common_mbr<CoordT: Ord + Clone>(lhs: &MBR<CoordT>, rhs: &MBR<CoordT>) -> MBR<CoordT> {
    if lhs as *const _ == rhs as *const _ {
        return lhs.clone();
    }

    debug_assert_eq!(
        lhs.dimension(),
        rhs.dimension(),
        "unable to make common MBR for MBRs with different dimensions"
    );

    let bounds = lhs.bounds.iter().zip(rhs.bounds.iter())
        .map(|(lhs, rhs)| {
            let min = if lhs.min < rhs.min {
                lhs.min.clone()
            } else {
                rhs.min.clone()
            };

            let max = if lhs.max > rhs.max {
                lhs.max.clone()
            } else {
                rhs.max.clone()
            };

            Bounds::new(min, max)
        }).collect::<Vec<_>>();

    MBR::new(bounds)
}

#[cfg(test)]
mod test {
    use crate::mbr;

    #[test]
    fn test_new_mbr() {
        let mbr = mbr! {
            X = [0; 10]
        };

        assert_eq!(mbr.dimension(), 1);
        assert_eq!(mbr.bounds[0].min, 0);
        assert_eq!(mbr.bounds[0].max, 10);

        let mbr = mbr! {
            X = [  0; 10],
            Y = [-10; -1]
        };

        assert_eq!(mbr.dimension(), 2);
        assert_eq!(mbr.bounds[0].min, 0);
        assert_eq!(mbr.bounds[0].max, 10);
        assert_eq!(mbr.bounds[1].min, -10);
        assert_eq!(mbr.bounds[1].max, -1);
    }

    #[test]
    fn test_bounds_length() {
        let bounds = mbr::Bounds::new(-4, 4);
        assert_eq!(bounds.length(), 8);
    }

    #[test]
    fn test_mbr_volume() {
        let mbr = mbr! {
            X = [-4; 4]
        };

        assert_eq!(mbr.volume(), 8);

        let mbr = mbr! {
            X = [0; 8],
            Y = [3; 7]
        };

        assert_eq!(mbr.volume(), 32);
    }

    #[test]
    fn test_1d_mbr_intersects() {
        let mbr_0 = mbr! {
            X = [0; 4]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_0));

        let mbr_1 = mbr! {
            X = [5; 9]
        };

        assert!(!mbr::intersects(&mbr_0, &mbr_1));
        assert!(!mbr::intersects(&mbr_1, &mbr_0));

        let mbr_1 = mbr! {
            X = [4; 9]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));

        let mbr_1 = mbr! {
            X = [-4; 0]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));

        let mbr_1 = mbr! {
            X = [-4; 1]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));

        let mbr_1 = mbr! {
            X = [1; 5]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));

        let mbr_1 = mbr! {
            X = [-10; 10]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));
    }

    #[test]
    fn test_multidimensional_mbr_intersects_with() {
        let test_min_bound = 0;
        let test_max_bound = 10;

        let max_test_dim = 4;

        for dims in 1..=max_test_dim {
            let src_mbr = make_n_dim_mbr(dims, test_min_bound, test_max_bound);
            let test_mbr = make_n_dim_mbr(dims, test_min_bound, test_max_bound);

            for test_dim_index in 0..dims {
                test_mbr_dimension_intersects_with(&src_mbr, test_mbr.clone(), test_dim_index);
            }
        }
    }

    #[test]
    fn test_mbr_cross_intersects_with() {
        let mbr_0 = mbr! {
            X = [0; 10],
            Y = [-3; 8]
        };

        let mbr_1 = mbr! {
            X = [-5; 4],
            Y = [-7; -1]
        };

        assert!(mbr::intersects(&mbr_0, &mbr_1));
        assert!(mbr::intersects(&mbr_1, &mbr_0));
    }

    #[test]
    fn test_common_mbr() {
        let mbr_0 = mbr! {
            X = [0; 10],
            Y = [-3; 8]
        };

        let mbr_1 = mbr! {
            X = [-5; 4],
            Y = [-7; -1]
        };

        let common = mbr::common_mbr(&mbr_0, &mbr_1);
        assert_eq!(common.bounds[0].min, -5);
        assert_eq!(common.bounds[0].max, 10);
        assert_eq!(common.bounds[1].min, -7);
        assert_eq!(common.bounds[1].max, 8);
    }

    fn test_mbr_dimension_intersects_with(src_mbr: &mbr::MBR<i32>, mut test_mbr: mbr::MBR<i32>, test_dim_index: usize) {
        let src_bounds = src_mbr.bounds.first().unwrap();
        let length = src_bounds.length();

        let intersect_set = vec![
            mbr::Bounds::new(src_bounds.min - length, src_bounds.max - length),
            mbr::Bounds::new(src_bounds.min - length / 2, src_bounds.max - length / 2),
            mbr::Bounds::new(src_bounds.min + length / 2, src_bounds.max + length / 2),
            mbr::Bounds::new(src_bounds.min + length, src_bounds.max + length),
            mbr::Bounds::new(src_bounds.min + length / 4, src_bounds.max - length / 4),
            mbr::Bounds::new(src_bounds.min - length / 4, src_bounds.max + length / 4),
        ];

        let non_intersect_set = vec![
            mbr::Bounds::new(src_bounds.min - length - 1, src_bounds.max - length - 1),
            mbr::Bounds::new(src_bounds.min + length + 1, src_bounds.max + length + 1),
        ];

        for bounds in intersect_set {
            test_mbr.bounds[test_dim_index] = bounds.clone();

            assert!(mbr::intersects(&src_mbr, &test_mbr));
            assert!(mbr::intersects(&src_mbr, &test_mbr));
        }

        for bounds in non_intersect_set {
            test_mbr.bounds[test_dim_index] = bounds.clone();

            assert!(!mbr::intersects(&src_mbr, &test_mbr));
            assert!(!mbr::intersects(&src_mbr, &test_mbr));
        }
    }

    fn make_n_dim_mbr(n: usize, min: i32, max: i32) -> mbr::MBR<i32> {
        let bounds = vec![mbr::Bounds::new(min, max); n];
        mbr::MBR::new(bounds)
    }
}
