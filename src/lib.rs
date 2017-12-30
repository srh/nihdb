mod rihdb {
    use std::collections::Bound as StdBound;
    use std::collections::btree_map::*;

    pub struct Store {
        entries: BTreeMap<String, String>,
    }

    pub enum Bound<T> {
        Unbounded,
        Inclusive(T),
        Exclusive(T),
    }

    pub struct Interval<T> {
        pub lower: Bound<T>,
        pub upper: Bound<T>,
    }

    pub struct StoreIter {
        interval: Interval<String>,
        reverse: bool,
        // TODO What info to have here?
    }

    impl Store {
        pub fn new() -> Store {
            return Store{entries: BTreeMap::<String, String>::new()};
        }

        pub fn put(&mut self, key: &str, val: &str) {
            self.entries.insert(key.to_string(), val.to_string());
        }

        pub fn get(&mut self, key: &str) -> String {
            if let Some(x) = self.entries.get(key) {
                return x.clone();
            } else {
                return String::new();
            }
        }

        pub fn directional_range(&mut self, interval: Interval<String>, reverse: bool) -> StoreIter {
            return StoreIter{interval: interval, reverse: reverse};
        }

        pub fn range(&mut self, interval: Interval<String>) -> StoreIter {
            return self.directional_range(interval, false);
        }

        pub fn next(&mut self, iter: &mut StoreIter) -> Option<(String, String)> {
            if !iter.reverse {
                let mut range: Range<String, String> = self.entries.range((convert_bound(&iter.interval.lower), convert_bound(&iter.interval.upper)));
                if let Some((key, value)) = range.next() {
                    // TODO: It'd be nice not to have to copy the key...
                    iter.interval.lower = Bound::Exclusive(key.clone());
                    return Some((key.clone(), value.clone()));
                } else {
                    return None;
                }
            } else {
                // TODO implement
                return None;
            }
        }
    }

    // TODO: Avoid having to recopy the String values.
    fn convert_bound(b: &Bound<String>) -> StdBound<String> {
        return match b {
            &Bound::Inclusive(ref x) => StdBound::Included(x.clone()),
            &Bound::Exclusive(ref x) => StdBound::Excluded(x.clone()),
            &Bound::Unbounded => StdBound::Unbounded,
        }
    }
}

#[cfg(test)]
mod tests {
    use rihdb::*;
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
        let mut it: StoreIter = kv.range(Interval::<String>{lower: Bound::Unbounded, upper: Bound::Exclusive("d".to_string())});
        assert_eq!(Some(("a".to_string(), "alpha".to_string())), kv.next(&mut it));
        assert_eq!(Some(("b".to_string(), "beta".to_string())), kv.next(&mut it));
        assert_eq!(Some(("c".to_string(), "charlie".to_string())), kv.next(&mut it));
        assert_eq!(None, kv.next(&mut it));
    }
}
