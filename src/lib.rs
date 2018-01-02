#[allow(dead_code)]

use std::collections::Bound;

extern crate rand;

mod memstore;
use memstore::*;
mod disk;
use disk::*;
mod toc;
use toc::*;
mod error;
use error::*;
mod encoding;

pub struct Store {
    // Never empty
    memstores: Vec<MemStore>,
    threshold: usize,
    directory: String,
    toc_file: std::fs::File,
    toc: TOC,
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
    pub fn create(dir: &str) -> Result<()> {
        // NOTE: We'll want directory locking and such.
        // NOTE: Pass errors up
        std::fs::create_dir(dir).expect("create_dir");  // NOTE: Never use expect
        create_toc(dir).expect("create_toc");
        return Ok(());
    }

    pub fn open(dir: &str, threshold: usize) -> Result<Store> {
        // NOTE: Review if we should error upon some of these error cases.
        // NOTE: Better error handling
        // NOTE: Clean up optional chaining.

        let (toc_file, toc) = read_toc(dir).expect("read_toc");
        let mut ms = MemStore::new();
        for fileno in 0..toc.next_table_id {
            if let Some(table_info) = toc.table_infos.get(&fileno) {
                iterate_table(dir, table_info, &mut |key: String, value: Mutation| {
                    ms.apply(key, value);
                })?;
            }
        }

        return Ok(Store::make_existing(threshold, dir.to_string(), toc_file, toc, ms));
    }

    pub fn make(threshold: usize, directory: String, toc_file: std::fs::File, toc: TOC) -> Store {
        return Store::make_existing(threshold, directory, toc_file, toc, MemStore::new());
    }

    fn make_existing(threshold: usize, directory: String, toc_file: std::fs::File, toc: TOC, ms: MemStore) -> Store {
        return Store{
            memstores: vec![MemStore::new(), ms],
            threshold: threshold,

            directory: directory,
            toc_file: toc_file,
            toc: toc,
        }
    }

    pub fn insert(&mut self, key: &str, val: &str) -> Result<bool> {
        if !self.exists(key) {
            self.put(key, val)?;
            return Ok(true);
        }
        return Ok(false);
    }

    pub fn replace(&mut self, key: &str, val: &str) -> Result<bool> {
        if self.exists(key) {
            self.put(key, val)?;
            return Ok(true);
        }
        return Ok(false);
    }

    pub fn put(&mut self, key: &str, val: &str) -> Result<()> {
        self.memstores[0].apply(key.to_string(), Mutation::Set(val.to_string()));
        return self.consider_split();
    }

    pub fn remove(&mut self, key: &str) -> Result<bool> {
        if self.exists(key) {
            self.memstores[0].apply(key.to_string(), Mutation::Delete);
            self.consider_split()?;
            return Ok(true);
        }
        return Ok(false);
    }

    pub fn flush(&mut self) -> Result<()> {
        self.do_flush()?;
        if self.memstores.len() > 1 {
            // We move all the entries to the combined memstore's map.
            // NOTE: Its mem_usage field is inaccurate (albeit unused).
            let mut ms = self.memstores.remove(0);
            self.memstores[0].entries.append(&mut ms.entries);
        }
        self.memstores.insert(0, MemStore::new());
        return Ok(());
    }

    fn consider_split(&mut self) -> Result<()> {
        if self.memstores[0].mem_usage >= self.threshold {
            self.flush()?;
        }
        return Ok(());
    }

    fn do_flush(&mut self) -> Result<()> {
        let ms: &MemStore = &self.memstores[0];
        if ms.entries.is_empty() {
            return Ok(());
        }
        let table_id = self.toc.next_table_id;
        self.toc.next_table_id += 1;
        let (keys_offset, file_size, smallest, biggest) = flush_to_disk(&self.directory, table_id, &ms)?;
        let ti = TableInfo{
            id: table_id,
            level: 0,
            keys_offset: keys_offset,
            file_size: file_size,
            smallest_key: smallest,
            biggest_key: biggest,
        };
        append_toc(&mut self.toc_file, Entry{additions: vec![ti], removals: vec![]})?;
        return Ok(());
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

    use rand::*;

    struct TestStore {
        // In an option so we can drop it before deleting directory.
        store: Option<Store>,
        directory: String,
    }

    fn random_testdir() -> String {
        let mut rng = rand::thread_rng();
        let mut x: u32 = rng.gen();
        let mut ret = "testdir-".to_string();
        for _ in 0..6 {
            ret.push(std::char::from_u32(97 + (x % 26)).unwrap());
            x /= 26;
        }
        return ret;
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            // Cleanup the Store before we delete the directory.
            self.close();
            std::fs::remove_dir_all(&self.directory).expect("remove_dir_all");
        }
    }

    impl TestStore {
        fn create() -> TestStore {
            let dir: String = random_testdir();
            Store::create(&dir).unwrap();
            let mut ts = TestStore{store: None, directory: dir};
            ts.open();
            return ts;
        }
        fn open(&mut self) {
            assert!(self.store.is_none());
            let store: Store = Store::open(&self.directory, 100).unwrap();
            self.store = Some(store);
        }
        fn close(&mut self) -> Option<()> {
            return self.store.take().map(|_| ());
        }
        fn kv(&mut self) -> &mut Store {
            return self.store.as_mut().unwrap();
        }
    }

    #[test]
    fn putget() {
        let mut ts = TestStore::create();
        let kv = ts.kv();
        kv.put("foo", "Hey").unwrap();
        let x: Option<String> = kv.get("foo");
        assert_eq!(Some("Hey".to_string()), x);
        assert!(kv.exists("foo"));
        assert_eq!(None, kv.get("bar"));
        assert!(!kv.exists("bar"));
    }

    #[test]
    fn range() {
        let mut ts = TestStore::create();
        let kv = ts.kv();
        kv.put("a", "alpha").unwrap();
        kv.put("b", "beta").unwrap();
        kv.put("c", "charlie").unwrap();
        kv.put("d", "delta").unwrap();
        let interval = Interval::<String>{lower: Bound::Unbounded, upper: Bound::Excluded("d".to_string())};
        let mut it: StoreIter = kv.range(interval.clone());
        assert_eq!(Some(("a".to_string(), "alpha".to_string())), kv.next(&mut it));
        assert_eq!(Some(("b".to_string(), "beta".to_string())), kv.next(&mut it));
        assert_eq!(Some(("c".to_string(), "charlie".to_string())), kv.next(&mut it));
        assert_eq!(None, kv.next(&mut it));
    }

    #[test]
    fn overwrite() {
        let mut ts = TestStore::create();
        let kv = ts.kv();

        kv.put("a", "alpha").unwrap();
        kv.put("a", "alpha-2").unwrap();
        assert_eq!(Some("alpha-2".to_string()), kv.get("a"));
        let inserted: bool = kv.insert("a", "alpha-3").unwrap();
        assert!(!inserted);
        let overwrote: bool = kv.replace("a", "alpha-4").unwrap();
        assert!(overwrote);
        assert_eq!(Some("alpha-4".to_string()), kv.get("a"));
    }

    fn write_basic_kv(ts: &mut TestStore) {
        let kv = ts.kv();
        for i in 0..102 {
            kv.put(&i.to_string(), &format!("value-{}", i.to_string())).unwrap();
        }
        // Remove one, so that we test Delete entries really do override Set entries.
        let removed: bool = kv.remove("11").unwrap();
        assert!(removed);
        assert!(1 < kv.memstores.len());
    }

    fn verify_basic_kv(ts: &mut TestStore) {
        let kv = ts.kv();
        let interval = Interval::<String>{lower: Bound::Excluded("1".to_string()), upper: Bound::Unbounded};
        let mut it: StoreIter = kv.range(interval.clone());
        assert_eq!(Some(("10".to_string(), "value-10".to_string())), kv.next(&mut it));
        assert_eq!(Some(("100".to_string(), "value-100".to_string())), kv.next(&mut it));
        assert_eq!(Some(("101".to_string(), "value-101".to_string())), kv.next(&mut it));
        assert_eq!(Some(("12".to_string(), "value-12".to_string())), kv.next(&mut it));
        assert_eq!(Some(("13".to_string(), "value-13".to_string())), kv.next(&mut it));
    }

    #[test]
    fn many() {
        let mut ts = TestStore::create();
        write_basic_kv(&mut ts);
        verify_basic_kv(&mut ts);
    }

    #[test]
    fn disk() {
        let mut ts = TestStore::create();
        write_basic_kv(&mut ts);
        ts.kv().flush().unwrap();
        // Remove (and drop) existing store.
        assert!(ts.close().is_some());
        ts.open();
        verify_basic_kv(&mut ts);
    }
}
