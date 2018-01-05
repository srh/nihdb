use error::*;
use iter::*;
use util::*;

use std::collections::*;
use std::collections::btree_map::*;

pub struct MemStore {
    pub entries: BTreeMap<Buf, Mutation>,
    pub mem_usage: usize,
}

// NOTE: Really compute overhead.
fn key_usage(key: &[u8]) -> usize { return 8 + key.len(); }
fn set_value_usage(val: &[u8]) -> usize { return 1 + 8 + val.len(); }
fn value_usage(val: &Mutation) -> usize {
    return match val {
        &Mutation::Set(ref x) => set_value_usage(&x),
        &Mutation::Delete => 1,
    };
}

impl MemStore {
    pub fn apply(&mut self, key: Buf, val: Mutation) {
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

    pub fn lookup(&self, key: &[u8]) -> Option<&Mutation> {
        return self.entries.get(key);
    }

    pub fn first_in_range(&self, interval: &Interval<Buf>) -> Option<&[u8]> {
        let mut range: Range<Buf, Mutation> = self.entries.range((interval.lower.clone(), interval.upper.clone()));
        return range.next().map(|(key, _)| key as &[u8]);
    }

    pub fn new() -> MemStore {
        return MemStore{entries: BTreeMap::<Buf, Mutation>::new(), mem_usage: 0};
    }
}

pub struct MemStoreIterator<'a> {
    memstore: &'a MemStore,
    // (Why not use a BTreeMap iterator?  Because in the future we'll
    // have other stuff modifying... I guess.  Pre-architecting.)
    current: Option<&'a [u8]>,
    upper_bound: Bound<Buf>,
}

impl<'a> MemStoreIterator<'a> {
    pub fn make(ms: &'a MemStore, interval: &Interval<Buf>) -> MemStoreIterator<'a> {
        return MemStoreIterator{
            memstore: ms,
            current: ms.first_in_range(interval),
            upper_bound: interval.upper.clone(),
        };
    }
}

impl<'a> MutationIterator for MemStoreIterator<'a> {
    fn current_key(&self) -> Result<Option<Buf>> {
        return Ok(self.current.map(|x| x.to_vec()));
    }

    fn current_value(&self) -> Result<Option<Mutation>> {
        if let Some(key) = self.current {
            return Ok(self.memstore.lookup(key).map(|x| x.clone()));
        }
        return Ok(None);
    }

    fn step(&mut self) -> Result<()> {
        // NOTE: Avoid having to clone the upper bound.
        let lower_bound = Bound::Excluded(self.current.or_err("step past end")?.to_vec());
        let mut range: Range<Buf, Mutation> = self.memstore.entries.range((lower_bound, self.upper_bound.clone()));
        if let Some((key, _)) = range.next() {
            self.current = Some(&key);
        } else {
            self.current = None;
        }
        return Ok(());
    }
}