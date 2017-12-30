mod rihdb {
    use std::collections::btree_map::*;

    pub struct Store {
        entries: BTreeMap<String, String>,
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
}
