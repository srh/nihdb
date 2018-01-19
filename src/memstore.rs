use error::*;
use iter::*;
use util::*;
use disk;

use std::collections::*;
use std::collections::btree_map::*;

pub struct MemStore {
    pub entries: BTreeMap<Buf, Mutation>,
    pub mem_usage: usize,
}

impl MemStore {
    pub fn apply(&mut self, key: Buf, val: Mutation) {
        let k_usage: usize = disk::approx_key_usage(&key);
        let old_usage: usize;
        if let Some(old_value) = self.entries.get(&key) {
            old_usage = k_usage + disk::approx_value_usage(&old_value);
        } else {
            old_usage = 0;
        }

        let new_usage: usize = k_usage + disk::approx_value_usage(&val);
        // Temporary overflow is OK because it's a usize.
        // NOTE: Wait, is unsigned overflow OK in Rust, in debug mode?
        self.mem_usage = (self.mem_usage + new_usage) - old_usage;
        self.entries.insert(key, val);
    }

    pub fn lookup(&self, key: &[u8]) -> Option<&Mutation> {
        return self.entries.get(key);
    }

    pub fn first_in_range(&self, interval: &Interval<Buf>) -> Option<&[u8]> {
        // NOTE: no need for bounds cloning
        let mut range: Range<Buf, Mutation> = self.entries.range((interval.lower.clone(), interval.upper.clone()));
        return range.next().map(|(key, _)| key as &[u8]);
    }

    pub fn last_in_range(&self, interval: &Interval<Buf>) -> Option<&[u8]> {
        // NOTE: no need for bounds cloning
        let mut range: Range<Buf, Mutation> = self.entries.range((interval.lower.clone(), interval.upper.clone()));
        return range.next_back().map(|(key, _)| key as &[u8]);
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
    bound: Bound<Buf>,
    direction: Direction,
}

impl<'a> MemStoreIterator<'a> {
    pub fn make(ms: &'a MemStore, interval: &Interval<Buf>, direction: Direction) -> MemStoreIterator<'a> {
        return match direction {
            Direction::Forward => MemStoreIterator{
                memstore: ms,
                current: ms.first_in_range(interval),
                bound: interval.upper.clone(),
                direction: direction,
            },
            Direction::Backward => MemStoreIterator{
                memstore: ms,
                current: ms.last_in_range(interval),
                bound: interval.lower.clone(),
                direction: direction,
            }
        }
    }
}

fn ref_bound(x: &Bound<Buf>) -> Bound<&[u8]> {
    match x {
        &Bound::Excluded(ref b) => Bound::Excluded(b),
        &Bound::Included(ref b) => Bound::Included(b),
        &Bound::Unbounded => Bound::Unbounded,
    }
}

impl<'a> MutationIterator for MemStoreIterator<'a> {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        return Ok(self.current);
    }

    fn current_value(&mut self) -> Result<Mutation> {
        if let Some(key) = self.current {
            return Ok(self.memstore.lookup(key).expect("invalid MemStoreIterator").clone());
        }
        return mk_err("current_value called on empty MemStoreIterator");
    }

    fn step(&mut self) -> Result<()> {
        let current_bound = Bound::Excluded(self.current.or_err("step past end")?);
        match self.direction {
            Direction::Forward => {
                let mut range: Range<Buf, Mutation> = self.memstore.entries.range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>(
                    (current_bound, ref_bound(&self.bound))
                );
                if let Some((key, _)) = range.next() {
                    self.current = Some(&key);
                } else {
                    self.current = None;
                }
                return Ok(());
            }
            Direction::Backward => {
                let mut range: Range<Buf, Mutation> = self.memstore.entries.range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>(
                    (ref_bound(&self.bound), current_bound)
                );
                if let Some((key, _)) = range.next_back() {
                    self.current = Some(&key);
                } else {
                    self.current = None;
                }
                return Ok(());
            }
        }
    }
}