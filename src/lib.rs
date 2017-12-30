#[allow(dead_code)]

use std::collections::Bound;
use std::collections::btree_map::*;

mod memstore;
use memstore::MemStore;

pub struct Store {
    memstore: MemStore,
}

#[derive(Clone)]
pub struct Interval<T> {
    pub lower: Bound<T>,
    pub upper: Bound<T>,
}

pub struct StoreIter {
    interval: Interval<String>,
    reverse: bool,
}

impl Store {
    pub fn new() -> Store {
        return Store{memstore: MemStore::new()};
    }

    pub fn insert(&mut self, key: &str, val: &str) -> bool {
        if let Some(_) = self.memstore.entries.get(key) {
            return false;
        }
        self.put(key, val);
        return true;
    }

    pub fn replace(&mut self, key: &str, val: &str) -> bool {
        if let Some(_) = self.memstore.entries.get(key) {
            self.put(key, val);
            return true;
        }
        return false;
    }

    pub fn put(&mut self, key: &str, val: &str) {
        self.memstore.set(key, val)
    }

    pub fn remove(&mut self, key: &str) -> bool {
        return self.memstore.remove(key);
    }

    pub fn get(&mut self, key: &str) -> String {
        if let Some(x) = self.memstore.entries.get(key) {
            return x.clone();
        }
        return String::new();
    }

    pub fn directional_range(&mut self, interval: Interval<String>, reverse: bool) -> StoreIter {
        return StoreIter{interval: interval, reverse: reverse};
    }

    pub fn range(&mut self, interval: Interval<String>) -> StoreIter {
        return self.directional_range(interval, false);
    }

    pub fn next(&mut self, iter: &mut StoreIter) -> Option<(String, String)> {
        // NOTE: Avoid having to clone the bounds.
        let mut range: Range<String, String> = self.memstore.entries.range((iter.interval.lower.clone(), iter.interval.upper.clone()));
        let res = if !iter.reverse { range.next() } else { range.next_back() };
        if let Some((key, value)) = res {
            // NOTE: It'd be nice not to have to copy the key...
            if !iter.reverse {
                iter.interval.lower = Bound::Excluded(key.clone());
            } else {
                iter.interval.upper = Bound::Excluded(key.clone());
            }
            return Some((key.clone(), value.clone()));
        } else {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;
    use super::*;
    #[test]
    fn putget() {
        let mut kv = Store::new();
        kv.put("foo", "Hey");
        let x: String = kv.get("foo");
        assert_eq!("Hey", x);
    }

    #[test]
    fn range() {
        let mut kv = Store::new();
        kv.put("a", "alpha");
        kv.put("b", "beta");
        kv.put("c", "charlie");
        kv.put("d", "delta");
        let interval = Interval::<String>{lower: Bound::Unbounded, upper: Bound::Excluded("d".to_string())};
        let mut it: StoreIter = kv.range(interval.clone());
        assert_eq!(Some(("a".to_string(), "alpha".to_string())), kv.next(&mut it));
        assert_eq!(Some(("b".to_string(), "beta".to_string())), kv.next(&mut it));
        assert_eq!(Some(("c".to_string(), "charlie".to_string())), kv.next(&mut it));
        assert_eq!(None, kv.next(&mut it));
        it = kv.directional_range(interval, true);

        assert_eq!(Some(("c".to_string(), "charlie".to_string())), kv.next(&mut it));
        assert_eq!(Some(("b".to_string(), "beta".to_string())), kv.next(&mut it));
        assert_eq!(Some(("a".to_string(), "alpha".to_string())), kv.next(&mut it));
        assert_eq!(None, kv.next(&mut it)); 
    }

    #[test]
    fn overwrite() {
        let mut kv = Store::new();
        kv.put("a", "alpha");
        kv.put("a", "alpha-2");
        assert_eq!("alpha-2", kv.get("a"));
        let inserted: bool = kv.insert("a", "alpha-3");
        assert!(!inserted);
        let overwrote: bool = kv.replace("a", "alpha-4");
        assert!(overwrote);
        assert_eq!("alpha-4", kv.get("a"));
    }
}
