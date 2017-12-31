use std::collections::*;
use std::collections::btree_map::*;

pub enum Mutation {
    Set(String),
    Delete,
}

pub struct MemStore {
    pub entries: BTreeMap<String, Mutation>,
    pub mem_usage: usize,
}

// NOTE: Really compute overhead.
fn key_usage(key: &str) -> usize { return 8 + key.len(); }
fn set_value_usage(val: &str) -> usize { return 1 + 8 + val.len(); }
fn value_usage(val: &Mutation) -> usize {
    return match val {
        &Mutation::Set(ref x) => set_value_usage(&x),
        &Mutation::Delete => 1,
    };
}

impl MemStore {
    pub fn apply(&mut self, key: String, val: Mutation) {
        let k_usage: usize = key_usage(&key);
        let old_usage: usize;
        if let Some(old_value) = self.entries.get(&key) {
            old_usage = k_usage + value_usage(&old_value);
        } else {
            old_usage = 0;
        }

        let new_usage: usize = k_usage + value_usage(&val);
        // Temporary overflow is OK because it's a usize.
        // NOTE: Wait, is unsigned overflow OK in Rust, in debug mode?
        self.mem_usage = (self.mem_usage + new_usage) - old_usage;
        self.entries.insert(key, val);
    }

    pub fn lookup(&self, key: &str) -> Option<&Mutation> {
        return self.entries.get(key);
    }

    pub fn lookup_after(&self, bound: &Bound<String>) -> Option<&str> {
        // NOTE: Avoid having to clone the bound.
        let mut range: Range<String, Mutation> = self.entries.range((bound.clone(), Bound::Unbounded));
        if let Some((key, _)) = range.next() {
            return Some(key);
        }
        return None;
    }

    pub fn new() -> MemStore {
        return MemStore{entries: BTreeMap::<String, Mutation>::new(), mem_usage: 0};
    }
}
