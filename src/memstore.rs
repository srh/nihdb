use util::*;
use error::*;
use std::collections::*;
use std::collections::btree_map::*;

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

    pub fn lookup_after(&self, lower_bound: &Bound<String>) -> Option<&str> {
        // NOTE: Avoid having to clone the bound.
        let mut range: Range<String, Mutation> = self.entries.range((lower_bound.clone(), Bound::Unbounded));
        if let Some((key, _)) = range.next() {
            return Some(key);
        }
        return None;
    }

    pub fn first_in_range(&self, interval: &Interval<String>) -> Option<&str> {
        let mut range: Range<String, Mutation> = self.entries.range((interval.lower.clone(), interval.upper.clone()));
        return range.next().map(|(key, _)| key.as_str());
    }

    pub fn new() -> MemStore {
        return MemStore{entries: BTreeMap::<String, Mutation>::new(), mem_usage: 0};
    }
}

pub struct MemStoreIterator<'a> {
    memstore: &'a MemStore,
    // (Why not use a BTreeMap iterator?  Because in the future we'll
    // have other stuff modifying... I guess.  Pre-architecting.)
    current: Option<&'a str>,
    upper_bound: Bound<String>,
}

impl<'a> MemStoreIterator<'a> {
    pub fn make(ms: &'a MemStore, interval: &Interval<String>) -> MemStoreIterator<'a> {
        return MemStoreIterator{
            memstore: ms,
            current: ms.first_in_range(interval),
            upper_bound: interval.upper.clone(),
        };
    }
}

impl<'a> MutationIterator for MemStoreIterator<'a> {
    fn current_key(&self) -> Result<Option<String>> {
        return Ok(self.current.map(|x| x.to_string()));
    }

    fn current_value(&self) -> Result<Option<Mutation>> {
        if let Some(key) = self.current {
            return Ok(self.memstore.lookup(key).map(|x| x.clone()));
        }
        return Ok(None);
    }

    fn step(&mut self) -> Result<()> {
        // NOTE: Avoid having to clone the upper bound.
        let lower_bound = Bound::Excluded(self.current.or_err("step past end")?.to_string());
        let mut range: Range<String, Mutation> = self.memstore.entries.range((lower_bound, self.upper_bound.clone()));
        if let Some((key, _)) = range.next() {
            self.current = Some(&key);
        } else {
            self.current = None;
        }
        return Ok(());
    }
}