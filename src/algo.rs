pub fn lower_bound_by_key<'a, B, F, T: 'a, V: std::ops::Deref<Target = [T]>>(
    v: &'a V,
    b: &B,
    f: F,
) -> usize
where
    B: Ord,
    F: FnMut(&'a T) -> B,
{
    match v.binary_search_by_key(b, f) {
        Result::Ok(i) => i,
        Result::Err(i) => i,
    }
}

pub fn upper_bound_by_key<'a, B, F, T: 'a, V: std::ops::Deref<Target = [T]>>(
    v: &'a V,
    b: &B,
    mut f: F,
) -> usize
where
    B: Ord + Eq,
    F: FnMut(&'a T) -> B,
{
    match v.binary_search_by_key(b, |x| f(x)) {
        Result::Ok(mut i) => {
            while i < v.len() && &f(&v[i]) == b {
                i += 1
            }
            i
        }
        Result::Err(i) => i,
    }
}
