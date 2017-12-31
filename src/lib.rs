#[allow(dead_code)]

use std::collections::Bound;

mod memstore;
use memstore::*;

pub struct Store {
    // Never empty
    memstores: Vec<MemStore>,
    threshold: usize,
}

#[derive(Clone)]
pub struct Interval<T> {
    pub lower: Bound<T>,
    pub upper: Bound<T>,
}

pub struct StoreIter {
    interval: Interval<String>,
}

impl Store {
    pub fn new() -> Store {
        const MEMSTORE_DEFAULT_THRESHOLD: usize = 1000000;
        return Store::make(MEMSTORE_DEFAULT_THRESHOLD);
    }

    pub fn make(threshold: usize) -> Store {
        return Store{
            memstores: vec![MemStore::new()],
            threshold: threshold,
        }
    }

    pub fn insert(&mut self, key: &str, val: &str) -> bool {
        if !self.exists(key) {
            self.put(key, val);
            return true;
        }
        return false;
    }

    pub fn replace(&mut self, key: &str, val: &str) -> bool {
        if self.exists(key) {
            self.put(key, val);
            return true;
        }
        return false;
    }

    pub fn put(&mut self, key: &str, val: &str) {
        self.memstores[0].apply(key.to_string(), Mutation::Set(val.to_string()));
        self.consider_split();
    }

    pub fn remove(&mut self, key: &str) -> bool {
        if self.exists(key) {
            self.memstores[0].apply(key.to_string(), Mutation::Delete);
            self.consider_split();
            return true;
        }
        return false;
    }

    fn consider_split(&mut self) {
        if self.memstores[0].mem_usage >= self.threshold {
            self.memstores.insert(0, MemStore::new());
        }
    }

    pub fn exists(&mut self, key: &str) -> bool {
        for store in self.memstores.iter() {
            if let Some(m) = store.lookup(key) {
                return match m {
                    &Mutation::Set(_) => true,
                    &Mutation::Delete => false,
                };
            }
        }
        return false;
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        for store in self.memstores.iter() {
            if let Some(m) = store.lookup(key) {
                return match m {
                    &Mutation::Set(ref x) => Some(x.clone()),
                    &Mutation::Delete => None,
                }
            }
        }
        return None;
    }

    // NOTE: Add directional ranges (i.e. backwards range iteration).
    pub fn range(&mut self, interval: Interval<String>) -> StoreIter {
        return StoreIter{interval: interval}
    }

    pub fn next(&mut self, iter: &mut StoreIter) -> Option<(String, String)> {
        loop {
            let mut i: usize = self.memstores.len();
            let mut min_key: Option<&str> = None;
            let mut min_key_index: usize = 0;
            while i > 0 {
                i -= 1;
                if let Some(k_i) = self.memstores[i].lookup_after(&iter.interval.lower) {
                    if let Some(mk) = min_key {
                        if k_i <= mk {
                            min_key = Some(k_i);
                            min_key_index = i;
                        }
                    } else {
                        min_key = Some(k_i);
                        min_key_index = i;
                    }
                }
            }

            if let Some(k) = min_key {
                iter.interval.lower = Bound::Excluded(k.to_string());
                if !below_upper_bound(k, &iter.interval.upper) {
                    return None;
                }
                let mutation = self.memstores[min_key_index].lookup(k).unwrap();
                match mutation {
                    &Mutation::Set(ref value) => {
                        return Some((k.to_string(), value.clone()));
                    },
                    &Mutation::Delete => {
                        continue;
                    }
                };
            }
            return None;
        }
    }
}

fn below_upper_bound(x: &str, bound: &Bound<String>) -> bool {
    return match bound {
        &Bound::Excluded(ref s) => x < &s,
        &Bound::Included(ref s) => x <= &s,
        &Bound::Unbounded => true,
    };
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;
    use super::*;
    #[test]
    fn putget() {
        let mut kv = Store::new();
        kv.put("foo", "Hey");
        let x: Option<String> = kv.get("foo");
        assert_eq!(Some("Hey".to_string()), x);
        assert!(kv.exists("foo"));
        assert_eq!(None, kv.get("bar"));
        assert!(!kv.exists("bar"));
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
    }

    #[test]
    fn overwrite() {
        let mut kv = Store::new();
        kv.put("a", "alpha");
        kv.put("a", "alpha-2");
        assert_eq!(Some("alpha-2".to_string()), kv.get("a"));
        let inserted: bool = kv.insert("a", "alpha-3");
        assert!(!inserted);
        let overwrote: bool = kv.replace("a", "alpha-4");
        assert!(overwrote);
        assert_eq!(Some("alpha-4".to_string()), kv.get("a"));
    }

    #[test]
    fn many() {
        // Adds enough stuff to the store to use multiple MemStores, to test
        // that we iterate through multiple ones properly.
        let mut kv = Store::make(100);
        for i in 0..102 {
            kv.put(&i.to_string(), &format!("value-{}", i.to_string()));
        }
        // Remove one, so that we test Delete entries really do override Set entries.
        let removed: bool = kv.remove("11");
        assert!(removed);
        assert!(1 < kv.memstores.len());
        let interval = Interval::<String>{lower: Bound::Excluded("1".to_string()), upper: Bound::Unbounded};
        let mut it: StoreIter = kv.range(interval.clone());
        assert_eq!(Some(("10".to_string(), "value-10".to_string())), kv.next(&mut it));
        assert_eq!(Some(("100".to_string(), "value-100".to_string())), kv.next(&mut it));
        assert_eq!(Some(("101".to_string(), "value-101".to_string())), kv.next(&mut it));
        assert_eq!(Some(("12".to_string(), "value-12".to_string())), kv.next(&mut it));
        assert_eq!(Some(("13".to_string(), "value-13".to_string())), kv.next(&mut it));
    }
}
