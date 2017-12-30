#[allow(dead_code)]
mod rihdb {
    use std::collections::Bound;
    use std::collections::btree_map::*;

    pub struct Store {
        memstore: MemStore,
    }

    pub struct MemStore {
        entries: BTreeMap<String, String>,
        mem_usage: usize,
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

    fn key_usage(key: &str) -> usize {
        // NOTE: Really compute overhead.
        return key.len() + 8;
    }

    fn value_usage(val: &str) -> usize {
        // NOTE: Really compute overhead.
        return val.len() + 8;
    }

    impl MemStore {
        fn set(&mut self, key: &str, val: &str) {
           let k_usage: usize = key_usage(key);
            let old_usage: usize;
            if let Some(old_value) = self.entries.get(key) {
                old_usage = k_usage + value_usage(&old_value);
            } else {
                old_usage = 0;
            }

            let new_usage: usize = k_usage + value_usage(val);
            // Temporary overflow is OK because it's a usize.
            // NOTE: Wait, is unsigned overflow OK in Rust, in debug mode?
            self.mem_usage = (self.mem_usage + new_usage) - old_usage;
            self.entries.insert(key.to_string(), val.to_string());
        }

        fn remove(&mut self, key: &str) -> bool {
            if let Some(value) = self.entries.remove(key) {
                let usage: usize = key_usage(key) + value_usage(&value);
                self.mem_usage -= usage;
                return true;
            }
            return false;
        }

        pub fn new() -> MemStore {
            return MemStore{entries: BTreeMap::<String, String>::new(), mem_usage: 0};
        }
    }

    impl Store {
        pub fn new() -> Store {
            return Store{memstore:MemStore::new()};
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
}

#[cfg(test)]
mod tests {
    use rihdb::*;
    use std::collections::Bound;
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
