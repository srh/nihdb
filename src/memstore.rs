use std::collections::*;

pub struct MemStore {
    pub entries: BTreeMap<String, String>,
    pub mem_usage: usize,
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
    pub fn set(&mut self, key: &str, val: &str) {
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

    pub fn remove(&mut self, key: &str) -> bool {
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