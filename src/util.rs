// Yes, we have a "utilities" file...
// NOTE: Let's reorganize this code later.


use std::collections::Bound;
use std;

pub type Result<T> = std::result::Result<T, Box<std::error::Error>>;

#[derive(Clone)]
pub struct Interval<T> {
    pub lower: Bound<T>,
    pub upper: Bound<T>,
}

pub fn below_upper_bound(x: &str, bound: &Bound<String>) -> bool {
    return match bound {
        &Bound::Excluded(ref s) => x < &s,
        &Bound::Included(ref s) => x <= &s,
        &Bound::Unbounded => true,
    };
}

pub fn above_lower_bound(x: &str, bound: &Bound<String>) -> bool {
    return match bound {
        &Bound::Excluded(ref s) => x > &s,
        &Bound::Included(ref s) => x >= &s,
        &Bound::Unbounded => true,
    }
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Set(String),
    Delete,
}

// NOTE: Avoid copying out whole String for key
pub trait MutationIterator {
    fn current_key(&self) -> Result<Option<String>>;
    // NOTE: Collapse this Option into the Result, since None is caused by bad
    // API usage.
    fn current_value(&self) -> Result<Option<Mutation>>;
    fn step(&mut self) -> Result<()>;
}

