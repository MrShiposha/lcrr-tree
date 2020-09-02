use {
    crate::tree::{mbr, mbr::Bounds, obj_space::ObjSpace, CoordTrait, RecordId, RecordIdKind, MBR},
    std::{fmt::Debug, iter},
};

type NodeGroup<'ids, CoordT> = (&'ids mut [RecordId], MBR<CoordT>);

pub struct LRTreeBuilder<CoordT: CoordTrait, ObjectT: Clone> {
    pub(crate) obj_space: ObjSpace<CoordT, ObjectT>,
    alpha: f32,
    is_built: bool,
}

impl<CoordT, ObjectT> LRTreeBuilder<CoordT, ObjectT>
where
    CoordT: CoordTrait,
    ObjectT: Debug + Clone,
{
    pub fn with_obj_space(obj_space: ObjSpace<CoordT, ObjectT>) -> Self {
        Self {
            obj_space,
            alpha: 0.45,
            is_built: false,
        }
    }

    pub fn build_with_alpha(&mut self, alpha: f32) {
        assert!(0.0 <= alpha && alpha <= 0.5);

        self.alpha = alpha;

        self.build();
    }

    pub fn build(&mut self) {
        if self.is_built {
            return;
        }

        if self.obj_space.is_empty() {
            self.is_built = true;
            return;
        }

        let data_num = self.obj_space.data_num();
        let level;

        if data_num <= self.obj_space.max_records as usize {
            level = 1;
            self.obj_space.root_id.set_kind(RecordIdKind::Leaf);
        } else {
            level = (data_num as f64)
                .log(self.obj_space.max_records as f64)
                .ceil() as usize;
            self.obj_space.root_id.set_kind(RecordIdKind::Internal);
        }

        let mut unbinded_ids = self.obj_space.iter_data_ids().collect::<Vec<_>>();
        let unbinded_ids = unbinded_ids.as_mut_slice();

        let root_id = self.obj_space.root_id;

        self.build_node(root_id, level - 1, unbinded_ids);

        let root_mbr = mbr::common_mbr_from_iter(
            self.obj_space
                .get_node(root_id)
                .payload
                .iter()
                .map(|&id| self.obj_space.get_mbr(id)),
        );

        self.obj_space.set_mbr(root_id, root_mbr);

        self.is_built = true;
    }

    pub fn is_built(&self) -> bool {
        self.is_built
    }

    fn build_node(&mut self, node_id: RecordId, level: usize, unbinded_ids: &mut [RecordId]) {
        let new_node_id_kind;
        match level {
            0 => {
                self.obj_space
                    .get_node_mut(node_id)
                    .payload
                    .extend_from_slice(unbinded_ids);

                unbinded_ids.iter().for_each(|&id| {
                    self.obj_space.set_parent_info(id, node_id);
                });

                return;
            }
            1 => new_node_id_kind = RecordIdKind::Leaf,
            _ => new_node_id_kind = RecordIdKind::Internal,
        }

        let ids_num = unbinded_ids.len();
        let node_child_num = (ids_num as f64).powf(1.0 / (level + 1) as f64).ceil() as usize;
        let groups = self.split_groups(node_child_num, level, unbinded_ids);

        for (group, mbr) in groups {
            let new_node_id = self.obj_space.make_node_with_mbr(new_node_id_kind, mbr);

            unsafe {
                self.obj_space.add_child_raw(node_id, new_node_id);
            }

            self.obj_space.set_parent_info(new_node_id, node_id);
            self.build_node(new_node_id, level - 1, group);
        }
    }

    fn split_groups<'ids>(
        &mut self,
        node_child_num: usize,
        level: usize,
        unbinded_ids: &'ids mut [RecordId],
    ) -> Vec<NodeGroup<'ids, CoordT>> {
        let mut groups = vec![];
        let mut sub_group_1;
        let mut sub_group_2;

        let first_group_coeff = node_child_num / 2;
        let second_group_coeff = node_child_num - first_group_coeff;

        let (group_1, group_2) =
            self.split_into_2_groups(first_group_coeff, second_group_coeff, level, unbinded_ids);

        let (_, ref mbr_1) = group_1;
        if !mbr_1.is_undefined() {
            if first_group_coeff > 1 {
                sub_group_1 = self.split_groups(first_group_coeff, level, group_1.0);
            } else {
                sub_group_1 = vec![group_1]
            }
        } else {
            sub_group_1 = vec![];
        }

        let (_, ref mbr_2) = group_2;
        if !mbr_2.is_undefined() {
            if second_group_coeff > 1 {
                sub_group_2 = self.split_groups(second_group_coeff, level, group_2.0);
            } else {
                sub_group_2 = vec![group_2];
            }
        } else {
            sub_group_2 = vec![];
        }

        groups.append(&mut sub_group_1);
        groups.append(&mut sub_group_2);
        groups
    }

    fn split_into_2_groups<'ids>(
        &mut self,
        first_group_coeff: usize,
        second_group_coeff: usize,
        level: usize,
        unbinded_ids: &'ids mut [RecordId],
    ) -> (NodeGroup<'ids, CoordT>, NodeGroup<'ids, CoordT>) {
        macro_rules! mbrs {
            ($($indices:tt)*) => {
                unbinded_ids[$($indices)*].iter().map(|&id| self.obj_space.get_mbr(id))
            };
        }

        let min_records = (self.obj_space.min_records as usize).pow(level as u32);
        let max_records = (self.obj_space.max_records as usize).pow(level as u32);

        let sort_axis_idx = self.find_sort_axis_index(unbinded_ids);

        unbinded_ids.sort_unstable_by(|&lhs_id, &rhs_id| {
            let sort_value = |bounds: &Bounds<CoordT>| {
                let sum = (bounds.min.clone() + bounds.max.clone())
                    .to_f32()
                    .expect("CoordT is expected to be convertible to f32");

                sum / 2.0
            };

            let lhs = self.obj_space.get_mbr(lhs_id).bounds(sort_axis_idx);
            let rhs = self.obj_space.get_mbr(rhs_id).bounds(sort_axis_idx);

            let lhs = sort_value(lhs);
            let rhs = sort_value(rhs);

            lhs.partial_cmp(&rhs).expect("cmp result is expected")
        });

        let ids_num = unbinded_ids.len() as f32;

        let first_quantile = (self.alpha * ids_num) as usize;
        let second_quantile = ((1.0 - self.alpha) * ids_num) as usize;

        let mut left_part_idx = first_quantile;
        let mut right_part_idx = (unbinded_ids.len() - second_quantile).saturating_sub(1);

        let mut first_group_len = first_quantile;
        let mut second_group_len = second_quantile;

        let mut first_mbr;
        let mut second_mbr;

        macro_rules! return_groups {
            () => {{
                let (first_group, second_group) = unbinded_ids.split_at_mut(left_part_idx);

                return ((first_group, first_mbr), (second_group, second_mbr))
            }};

            (@move rest_mbrs => $mbr:ident) => {
                $mbr = mbr::common_mbr_from_iter(
                    mbrs![left_part_idx..=right_part_idx].chain(iter::once(&$mbr))
                );
            };

            (rest => first_group) => {{
                return_groups![@move rest_mbrs => first_mbr];

                left_part_idx = right_part_idx + 1;
                return_groups![];
            }};

            (rest => second_group) => {{
                return_groups![@move rest_mbrs => second_mbr];
                return_groups![];
            }};
        }

        first_mbr = mbr::common_mbr_from_iter(mbrs![..left_part_idx]);

        second_mbr = mbr::common_mbr_from_iter(mbrs![right_part_idx + 1..]);

        loop {
            if right_part_idx < left_part_idx {
                return_groups![];
            }

            if first_group_len < first_group_coeff * min_records {
                return_groups![rest => first_group];
            }

            if second_group_len < second_group_coeff * min_records {
                return_groups![rest => second_group];
            }

            if first_group_len > first_group_coeff * max_records {
                return_groups![rest => second_group];
            }

            if second_group_len > second_group_coeff * max_records {
                return_groups![rest => first_group];
            }

            let obj_mbr = self.obj_space.get_mbr(unbinded_ids[left_part_idx]);
            let common_first_mbr = mbr::common_mbr(&first_mbr, obj_mbr);
            let common_second_mbr = mbr::common_mbr(&second_mbr, obj_mbr);

            let first_delta = common_first_mbr.volume() - first_mbr.volume();
            let second_delta = common_second_mbr.volume() - second_mbr.volume();

            if first_delta >= second_delta {
                unbinded_ids.swap(left_part_idx, right_part_idx);

                right_part_idx -= 1;
                second_group_len += 1;
                second_mbr = common_second_mbr;
            } else {
                left_part_idx += 1;
                first_group_len += 1;
                first_mbr = common_first_mbr;
            }
        }
    }

    fn find_sort_axis_index<'ids>(&self, unbinded_ids: &'ids [RecordId]) -> usize {
        (0..self.obj_space.dimension)
            .map(|dim| (dim, unbinded_ids.iter()))
            .map(|(dim, mut ids)| {
                let first_id = ids.next().unwrap();
                let bounds = self.obj_space.get_mbr(*first_id).bounds(dim);

                let mut max_low = bounds.min.clone();
                let mut max_high = bounds.max.clone();
                let mut min_low = bounds.min.clone();
                let mut min_high = bounds.max.clone();

                ids.for_each(|id| {
                    let bounds = self.obj_space.get_mbr(*id).bounds(dim);

                    if bounds.min > max_low {
                        max_low = bounds.min.clone();
                    } else if bounds.min < min_low {
                        min_low = bounds.min.clone();
                    }

                    if bounds.max > max_high {
                        max_high = bounds.max.clone();
                    } else if bounds.max < min_high {
                        min_high = bounds.max.clone();
                    }
                });
                (dim, (max_low - min_high) / (max_high - min_low))
            })
            .max_by(|(_, lhs_key), (_, rhs_key)| {
                lhs_key.partial_cmp(rhs_key).expect("cmp result expected")
            })
            .map(|(dim, _)| dim)
            .unwrap()
    }
}
