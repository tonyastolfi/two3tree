use std::ops::Deref;

pub fn lower_bound_by_key<'seq, Seq, Elem, Bound, KeyFn>(
    seq: &'seq Seq,
    bound: &Bound,
    key_fn: KeyFn,
) -> usize
where
    Seq: Deref<Target = [Elem]>,
    Elem: 'seq,
    Bound: Ord,
    KeyFn: FnMut(&'seq Elem) -> Bound,
{
    match seq.binary_search_by_key(bound, key_fn) {
        Result::Ok(index) => index,
        Result::Err(index) => index,
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
