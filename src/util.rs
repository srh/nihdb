// Yes, we have a "utilities" file...
// NOTE: Let's reorganize this code later.

use error::*;

use std;
use std::collections::Bound;

pub type Result<T> = std::result::Result<T, Box<std::error::Error>>;
// The type of keys and values.
pub type Buf = Vec<u8>;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

#[derive(Clone)]
pub struct Interval<T> {
    pub lower: Bound<T>,
    pub upper: Bound<T>,
}

pub fn below_upper_bound(x: &[u8], bound: &Bound<Buf>) -> bool {
    return match bound {
        &Bound::Excluded(ref s) => x < &s,
        &Bound::Included(ref s) => x <= &s,
        &Bound::Unbounded => true,
    };
}

pub fn above_lower_bound(x: &[u8], bound: &Bound<Buf>) -> bool {
    return match bound {
        &Bound::Excluded(ref s) => x > &s,
        &Bound::Included(ref s) => x >= &s,
        &Bound::Unbounded => true,
    }
}

pub fn table_filename(table_id: TableId) -> String { format!("{}.tab", table_id.0) }
pub fn table_filepath(dir: &str, table_id: TableId) -> String { format!("{}/{}.tab", dir, table_id.0) }

pub fn mk_err<T>(msg: &str) -> Result<T> {
    return Err(Box::new(Error::new(msg)));
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Set(Buf),
    Delete,
}
