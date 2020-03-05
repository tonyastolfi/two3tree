pub trait SubtreeMerge {
    fn merge_left(
        &mut self, //
        config: &TreeConfig,
        subtree: Subtree,
        left_min: i32,
    ) -> MergeResult;

    fn merge_right(
        &mut self, //
        config: &TreeConfig,
        subtree_min: i32,
        subtree: Subtree,
    ) -> MergeResult;
}
