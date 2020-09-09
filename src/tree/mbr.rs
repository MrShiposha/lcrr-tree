use {
    num::{Num, NumCast},
    std::{
        cmp::{Ordering, PartialOrd},
        fmt::{self, Debug, Display},
        mem::MaybeUninit,
    },
};

pub trait CoordTrait: Default + Debug + Num + NumCast + PartialOrd<Self> + Clone {}

impl<T> CoordTrait for T where T: Default + Debug + Num + NumCast + PartialOrd<Self> + Clone {}

#[derive(Debug)]
pub struct Bounds<CoordT> {
    pub min: CoordT,
    pub max: CoordT,
}

impl<CoordT: CoordTrait> Bounds<CoordT> {
    pub fn new(min: CoordT, max: CoordT) -> Self {
        debug_assert!(min.le(&max), "a min bound must be less than a max bound");

        unsafe { Self::new_unchecked(min, max) }
    }

    /// # Safety
    ///
    /// `min` must be less than `max`
    ///
    /// If `min > max` -- it is NOT UB, so it is possible, but not desirable.
    pub unsafe fn new_unchecked(min: CoordT, max: CoordT) -> Self {
        Self { min, max }
    }

    pub fn is_in_bound(&self, value: &CoordT) -> bool {
        self.min <= *value && *value <= self.max
    }

    pub fn length(&self) -> CoordT {
        self.max.clone() - self.min.clone()
    }
}

impl<CoordT: CoordTrait> Clone for Bounds<CoordT> {
    fn clone(&self) -> Self {
        Self {
            min: self.min.clone(),
            max: self.max.clone(),
        }
    }
}

impl<CoordT: PartialEq> PartialEq for Bounds<CoordT> {
    fn eq(&self, rhs: &Self) -> bool {
        self.min.eq(&rhs.min) && self.max.eq(&rhs.max)
    }
}

impl<CoordT: PartialEq + Eq> Eq for Bounds<CoordT> {}

/// Minimum bounding rectangle
#[derive(Debug)]
pub struct MBR<CoordT> {
    bounds: Vec<Bounds<CoordT>>,
}

impl<CoordT: CoordTrait> MBR<CoordT> {
    pub fn new(bounds: Vec<Bounds<CoordT>>) -> Self {
        debug_assert!(!bounds.is_empty(), "MBR can't be zero-dimension");

        unsafe { Self::new_unchecked(bounds) }
    }

    /// # Safety
    ///
    /// `bounds` must be not empty.
    ///
    /// If `bounds` is empty -- it is NOT UB, so it is possible, but not desirable.
    pub unsafe fn new_unchecked(bounds: Vec<Bounds<CoordT>>) -> Self {
        Self { bounds }
    }

    /// # Safety
    ///
    /// Use it only as "uninit" state
    /// # Notes
    /// * `common_mbr`: for undefined MBR and any other MBR returns the other one.
    /// * `intersects`: undefined MBR intersects with any other MBR.
    /// * `dimension`: returns `0`.
    /// * `bounds`: panics.
    /// * `volume`: returns `0`.
    pub unsafe fn undefined() -> Self {
        Self::new_unchecked(vec![])
    }

    pub fn is_undefined(&self) -> bool {
        self.bounds.is_empty()
    }

    pub fn dimension(&self) -> usize {
        self.bounds.len()
    }

    pub fn bounds(&self, axis_index: usize) -> &Bounds<CoordT> {
        &self.bounds[axis_index]
    }

    pub fn volume(&self) -> CoordT {
        let init_volume = self
            .bounds
            .first()
            .map(|bounds| bounds.length())
            .unwrap_or_else(CoordT::zero);

        self.bounds
            .iter()
            .skip(1)
            .fold(init_volume, |acc, bounds| acc * bounds.length())
    }
}

impl<CoordT: CoordTrait> Clone for MBR<CoordT> {
    fn clone(&self) -> Self {
        Self {
            bounds: self.bounds.clone(),
        }
    }
}

impl<CoordT: PartialEq> PartialEq for MBR<CoordT> {
    fn eq(&self, rhs: &Self) -> bool {
        self.bounds == rhs.bounds
    }
}

impl<CoordT: Eq> Eq for MBR<CoordT> {}

impl<CoordT: CoordTrait> Display for MBR<CoordT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        if self.is_undefined() {
            return write!(f, "MBR {{ /undefined/ }}");
        }

        write!(f, "MBR {{ ")?;
        for (i, bound) in self.bounds.iter().enumerate() {
            write!(f, "x{}: [{:?}; {:?}] ", i + 1, bound.min, bound.max)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

pub fn intersects<CoordT: CoordTrait>(lhs: &MBR<CoordT>, rhs: &MBR<CoordT>) -> bool {
    if lhs as *const _ == rhs as *const _ {
        return true;
    }

    let min_dim = std::cmp::min(lhs.dimension(), rhs.dimension());

    let mut intersected_axis = 0usize;
    for (self_bound, other_bound) in lhs.bounds.iter().zip(rhs.bounds.iter()) {
        if self_bound.is_in_bound(&other_bound.min)
            || self_bound.is_in_bound(&other_bound.max)
            || other_bound.is_in_bound(&self_bound.min)
        {
            intersected_axis += 1;
        }
    }

    intersected_axis == min_dim
}

pub fn common_mbr<CoordT: CoordTrait>(lhs: &MBR<CoordT>, rhs: &MBR<CoordT>) -> MBR<CoordT> {
    if lhs as *const _ == rhs as *const _ {
        return lhs.clone();
    }

    let lhs_dim = lhs.dimension();
    let rhs_dim = rhs.dimension();

    let lhs_bounds;
    let rhs_bounds;

    let mut bounds_ext = MaybeUninit::<Vec<Bounds<CoordT>>>::uninit();

    match lhs_dim.cmp(&rhs_dim) {
        Ordering::Equal => {
            lhs_bounds = &lhs.bounds;
            rhs_bounds = &rhs.bounds;
        }
        Ordering::Less => {
            unsafe {
                bounds_ext
                    .as_mut_ptr()
                    .write(extend_bounds(&lhs.bounds, &rhs.bounds))
            }

            lhs_bounds = unsafe { &*bounds_ext.as_ptr() };
            rhs_bounds = &rhs.bounds;
        }
        Ordering::Greater => {
            unsafe {
                bounds_ext
                    .as_mut_ptr()
                    .write(extend_bounds(&rhs.bounds, &lhs.bounds))
            }

            lhs_bounds = &lhs.bounds;
            rhs_bounds = unsafe { &*bounds_ext.as_ptr() };
        }
    }

    let bounds = lhs_bounds
        .iter()
        .zip(rhs_bounds)
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
        })
        .collect::<Vec<_>>();

    unsafe { MBR::new_unchecked(bounds) }
}

fn extend_bounds<CoordT: CoordTrait>(
    src_bounds: &[Bounds<CoordT>],
    target_bounds: &[Bounds<CoordT>],
) -> Vec<Bounds<CoordT>> {
    debug_assert!(!target_bounds.is_empty());
    debug_assert!(src_bounds.len() < target_bounds.len());

    let bounds_diff = target_bounds.len() - src_bounds.len();
    let mut bounds = src_bounds.to_vec();

    let Bounds { min, max } = target_bounds[0].clone();

    let (min, max) = target_bounds
        .iter()
        .fold((min, max), |(mut min, mut max), bounds| {
            if bounds.min.lt(&min) {
                min = bounds.min.clone();
            }

            if bounds.max.gt(&max) {
                max = bounds.max.clone();
            }

            (min, max)
        });

    // This bounds are invalid and will be replaced by common_mbr fn.
    let bounds_ext = unsafe { Bounds::new_unchecked(max, min) };
    for _ in 0..bounds_diff {
        bounds.push(bounds_ext.clone());
    }

    bounds
}

pub fn common_mbr_from_iter<'a, I, CoordT>(iter: I) -> MBR<CoordT>
where
    I: Iterator<Item = &'a MBR<CoordT>>,
    CoordT: CoordTrait + 'a,
{
    iter.fold(unsafe { MBR::undefined() }, |common, mbr| {
        common_mbr(&common, &mbr)
    })
}

pub fn mbr_delta<CoordT: CoordTrait>(src: &MBR<CoordT>, addition: &MBR<CoordT>) -> CoordT {
    let common = common_mbr(src, addition);

    common.volume() - src.volume()
}

#[cfg(test)]
mod test {
    use crate::{mbr, mbr::MBR};

    #[test]
    fn test_new_mbr() {
        let mbr = mbr! {
            X = [0; 10]
        };

        assert!(!mbr.is_undefined());
        assert_eq!(mbr.dimension(), 1);
        assert_eq!(mbr.bounds[0].min, 0);
        assert_eq!(mbr.bounds[0].max, 10);

        let mbr = mbr! {
            X = [  0; 10],
            Y = [-10; -1]
        };

        assert!(!mbr.is_undefined());
        assert_eq!(mbr.dimension(), 2);
        assert_eq!(mbr.bounds[0].min, 0);
        assert_eq!(mbr.bounds[0].max, 10);
        assert_eq!(mbr.bounds[1].min, -10);
        assert_eq!(mbr.bounds[1].max, -1);
    }

    #[test]
    fn test_undefined() {
        let undefined = unsafe { MBR::<u32>::undefined() };

        assert!(undefined.is_undefined());
    }

    #[test]
    fn test_mbr_bounds() {
        let mbr = mbr! {
            X = [0; 10]
        };

        assert_eq!(mbr.dimension(), 1);
        assert_eq!(mbr.bounds(0).min, 0);
        assert_eq!(mbr.bounds(0).max, 10);

        let mbr = mbr! {
            X = [  0; 10],
            Y = [-10; -1]
        };

        assert_eq!(mbr.dimension(), 2);
        assert_eq!(mbr.bounds(0).min, 0);
        assert_eq!(mbr.bounds(0).max, 10);
        assert_eq!(mbr.bounds(1).min, -10);
        assert_eq!(mbr.bounds(1).max, -1);

        let undefined = unsafe { MBR::<u32>::undefined() };

        assert_eq!(undefined.dimension(), 0);
    }

    #[test]
    #[should_panic]
    fn test_panic_mbr_bounds() {
        let mbr = mbr! {
            X = [0; 10]
        };

        mbr.bounds(mbr.dimension());
    }

    #[test]
    #[should_panic]
    fn test_panic_undefined_bounds() {
        let undefined = unsafe { MBR::<u32>::undefined() };

        undefined.bounds(0);
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

        let undefined = unsafe { MBR::<u32>::undefined() };

        assert_eq!(undefined.volume(), 0);
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
    fn test_mbr_intersects_undefined() {
        let undefined = unsafe { MBR::undefined() };

        let undefined_1 = unsafe { MBR::undefined() };

        let mbr = mbr! {
            X = [0; 10],
            Y = [-3; 8]
        };

        assert!(mbr::intersects(&mbr, &undefined));
        assert!(mbr::intersects(&undefined, &mbr));
        assert!(mbr::intersects(&undefined, &undefined_1));
        assert!(mbr::intersects(&undefined_1, &undefined));
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

    #[test]
    fn test_common_mbr_undefined() {
        let undefined = unsafe { MBR::undefined() };

        let mbr = mbr! {
            X = [0; 10],
            Y = [-3; 8]
        };

        let common = mbr::common_mbr(&mbr, &undefined);
        assert_eq!(common, mbr);

        let undefined_1 = unsafe { MBR::undefined() };

        let common = mbr::common_mbr(&undefined, &undefined_1);
        assert_eq!(common, undefined);
        assert_eq!(common, undefined_1);
        assert_eq!(common, unsafe { MBR::undefined() });
    }

    #[test]
    fn test_1d_point_intersects() {
        let mbr = mbr![X = [0; 10]];

        for x in 0..=10 {
            assert!(mbr::intersects(&mbr, &mbr![X = [x; x]]));
        }

        assert!(!mbr::intersects(&mbr, &mbr![X = [-1; -1]]));
        assert!(!mbr::intersects(&mbr, &mbr![X = [11; 11]]));
    }

    #[test]
    fn test_2d_line_intersects() {
        let mbr = mbr! {
            X = [0; 10],
            Y = [0; 10]
        };

        for x in 0..=10 {
            assert!(mbr::intersects(&mbr, &mbr![X = [x; x]]));
        }

        assert!(!mbr::intersects(&mbr, &mbr![X = [-1; -1]]));
        assert!(!mbr::intersects(&mbr, &mbr![X = [11; 11]]));
    }

    #[test]
    fn test_common_mbr_iter() {
        let mbr_0 = mbr! {
            X = [0; 10],
            Y = [-3; 8]
        };

        let mbr_1 = mbr! {
            X = [-5; 4],
            Y = [-7; -1]
        };

        let mbr_2 = mbr! {
            X = [5; 19],
            Y = [2;  9]
        };

        let mbrs = vec![mbr_0, mbr_1, mbr_2];

        let common = mbr::common_mbr_from_iter(mbrs.iter());
        assert_eq!(common.bounds[0].min, -5);
        assert_eq!(common.bounds[0].max, 19);
        assert_eq!(common.bounds[1].min, -7);
        assert_eq!(common.bounds[1].max, 9);
    }

    fn test_mbr_dimension_intersects_with(
        src_mbr: &mbr::MBR<i32>,
        mut test_mbr: mbr::MBR<i32>,
        test_dim_index: usize,
    ) {
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
