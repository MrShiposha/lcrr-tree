pub mod tree;

pub use tree::*;
pub use mbr::MBR;

#[macro_export]
macro_rules! mbr {
    (
        $($_axis_name:ident = [$min_bound:expr; $max_bound:expr]),+
    ) => {
        $crate::mbr::MBR::new(
            vec![$(
                $crate::mbr::Bounds::new(
                    $min_bound,
                    $max_bound
                )
            ),+]
        )
    };
}