mod rihdb {
    pub struct Store {

    }

    impl Store {
        pub fn new() -> Store {
            return Store{};
        }

        pub fn put(&mut self, key: &str, val: &str) {
            println!("put '{}', '{}'", key, val);
        }

        pub fn get(&mut self, key: &str) -> String {
            return "Hey".to_string();
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
