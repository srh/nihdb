extern crate crc;

use encoding::*;
use util::*;

use fnv;
use std;
use std::collections::*;
use std::io::Read;
use std::io::Write;

/* toc file format:

    [entry][entry]...[entry]
    
[entry] format:

    [u64][u32][varint]
    L     C    N

    where L is the length of the entry, C is its checksum, and N is the entry.

*/

// NOTE: Make these newtypes.
pub type TableId = u64;
pub type LevelNumber = u64;

// NOTE: We should track size of garbage data in TOC and occasionally rewrite from scratch.
pub struct Toc {
    pub table_infos: fnv::FnvHashMap<TableId, TableInfo>,
    // NOTE: We'll want levels (besides zero) to be organized by key order.
    pub level_infos: BTreeMap<LevelNumber, BTreeSet<TableId>>,
    pub next_table_id: u64,
}

#[derive(Debug)]
pub struct Entry {
    pub removals: Vec<TableId>,
    pub additions: Vec<TableInfo>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub id: TableId,
    pub level: LevelNumber,
    // Offset of the keys in the table file
    pub keys_offset: u64,
    pub file_size: u64,
    // The smallest and biggest keys (defining a closed interval) in the file.
    // (The file must always have at least one key.)
    pub smallest_key: Buf,
    pub biggest_key: Buf,
}

fn toc_filename(dir: &str) -> String {
    return format!("{}/toc", dir);
}

pub fn create_toc(dir: &str) -> Result<std::fs::File> {
    let f = std::fs::File::create(toc_filename(dir))?;
    // Nothing to write yet.
    return Ok(f);
}

fn remove_table(toc: &mut Toc, table_id: TableId) {
    let ti: TableInfo = toc.table_infos.remove(&table_id).expect("TOC table removal");
    let v: &mut BTreeSet<TableId> = toc.level_infos.get_mut(&ti.level).expect("TOC table removal level");
    let removed: bool = v.remove(&ti.id);
    assert!(removed);
}

fn add_table(toc: &mut Toc, table_info: TableInfo) {
    let table_id = table_info.id;
    let level = table_info.level;
    let inserted: bool = toc.table_infos.insert(table_id, table_info).is_none();
    assert!(inserted);
    let set: &mut BTreeSet<u64> = toc.level_infos.entry(level).or_insert_with(|| BTreeSet::<u64>::new());
    let inserted: bool = set.insert(table_id);
    assert!(inserted);
    toc.next_table_id = toc.next_table_id.max(table_id + 1);
}

fn encode_table_info(v: &mut Vec<u8>, ti: &TableInfo) {
    encode_uvarint(v, ti.id);
    encode_uvarint(v, ti.level);
    encode_uvarint(v, ti.keys_offset);
    encode_uvarint(v, ti.file_size);
    encode_str(v, &ti.smallest_key);
    encode_str(v, &ti.biggest_key);
}

fn decode_table_info(buf: &[u8], pos: &mut usize) -> Option<TableInfo> {
    let id: u64 = decode_uvarint(&buf, pos)?;
    let level: u64 = decode_uvarint(&buf, pos)?;
    let keys_offset: u64 = decode_uvarint(&buf, pos)?;
    let file_size: u64 = decode_uvarint(&buf, pos)?;
    let smallest_key: Buf = decode_str(&buf, pos)?;
    let biggest_key: Buf = decode_str(&buf, pos)?;
    return Some(TableInfo{
        id: id,
        level: level,
        keys_offset: keys_offset,
        file_size: file_size,
        smallest_key: smallest_key,
        biggest_key: biggest_key,
    });
}

fn encode_entry(ent: &Entry) -> Vec<u8> {
    let mut v = Vec::<u8>::new();

    encode_uvarint(&mut v, ent.removals.len() as u64);
    for &table in &ent.removals {
        encode_uvarint(&mut v, table);
    }

    encode_uvarint(&mut v, ent.additions.len() as u64);
    for ref table_info in &ent.additions {
        encode_table_info(&mut v, &table_info);
    }

    let length: usize = v.len();
    let checksum: u32 = crc::crc32::checksum_castagnoli(&v);
    let mut ret = Vec::<u8>::new();
    encode_u64(&mut ret, length as u64);
    encode_u32(&mut ret, checksum);
    ret.extend(v);
    return ret;
}

fn decode_entry(buf: &[u8], pos: &mut usize) -> Option<Entry> {
    let length: usize = try_into_size(decode_u64(&buf, pos)?)?;
    let checksum: u32 = decode_u32(&buf, pos)?;

    let front = *pos;
    if length > buf.len() - front {
        return None;
    }

    let entry_slice = &buf[front..front+length];
    let computed_checksum: u32 = crc::crc32::checksum_castagnoli(entry_slice);
    if checksum != computed_checksum {
        return None;
    }

    let num_removals: usize = try_into_size(decode_uvarint(&buf, pos)?)?;
    let mut removals = Vec::<TableId>::new();
    for _ in 0..num_removals {
        let table: TableId = decode_uvarint(&buf, pos)?;
        removals.push(table);
    }

    let num_additions: usize = try_into_size(decode_uvarint(&buf, pos)?)?;
    let mut additions = Vec::<TableInfo>::new();
    for _ in 0..num_additions {
        additions.push(decode_table_info(&buf, pos)?);
    }

    if *pos - front != length {
        return None;
    }
    return Some(Entry{removals, additions});
}

fn process_entry(toc: &mut Toc, entry: Entry) {
    // Process removals first -- maybe we'll remove+add for level-changing logic
    for table_id in entry.removals {
        remove_table(toc, table_id);
    }
    for addition in entry.additions {
        add_table(toc, addition);
    }
}

fn parse_tablefile_name(name: &str) -> Option<TableId> {
    if !name.ends_with(".tab") {
        return None;
    }
    let frontpart: &str = name.split_at(name.len() - 4).0;
    if let Some(x) = frontpart.parse::<u64>().ok() {
        // Multiple strings ("1", "01", "001", ...) can parse to the same
        // integer, so double-check that this is truly the right table file.
        if table_filename(x) == name {
            return Some(x);
        }
    }
    return None;
}

// Returns map of table id to file size.
fn read_dir_tables(dir: &str) -> Result<fnv::FnvHashMap<TableId, u64>> {
    let mut ret = fnv::FnvHashMap::default();
    for entry_result in std::fs::read_dir(dir)? {
        let ent = entry_result?;
        // Just looking for valid tab files, so we merely ignore non-unicode file names.
        if let Some(filename) = ent.file_name().to_str() {
            if let Some(table_id) = parse_tablefile_name(filename) {
                let m = ent.metadata()?;
                if !m.is_file() {
                    return rih_err("non-file table file name");
                }
                let result = ret.insert(table_id, m.len());
                assert!(result.is_none());
            }
        }
    }
    return Ok(ret);
}

fn validate_toc(toc: &Toc, dirent_tables: &fnv::FnvHashMap<TableId, u64>) -> bool {
    return toc.table_infos.iter().all(|(id, info)|
        dirent_tables.get(id).map(|x| *x) == Some(info.file_size)
    );
}

pub fn read_toc(dir: &str) -> Result<(std::fs::File, Toc)> {
    let mut f = std::fs::OpenOptions::new().read(true).append(true)
        .open(toc_filename(dir))?;
    let mut buf = Vec::<u8>::new();
    f.read_to_end(&mut buf)?;

    let mut toc = Toc{
        table_infos: fnv::FnvHashMap::default(),
        level_infos: BTreeMap::new(),
        next_table_id: 0,
    };

    let mut pos: usize = 0;
    while pos < buf.len() {
        let savepos = pos;
        if let Some(entry) = decode_entry(&buf, &mut pos) {
            process_entry(&mut toc, entry);
        } else {
            f.set_len(savepos as u64)?;
            // NOTE: It would be decent to seek to end (instead of past end),
            // even though not strictly necessary because we opened using
            // append(true).
            return Ok((f, toc));
        }
    }

    let dirent_tables: fnv::FnvHashMap<TableId, u64> = read_dir_tables(dir)?;
    if !validate_toc(&toc, &dirent_tables) {
        return rih_err("invalid toc");
    }
    return Ok((f, toc));
}

pub fn append_toc(toc: &mut Toc, f: &mut std::fs::File, entry: Entry) -> Result<()> {
    let data: Vec<u8> = encode_entry(&entry);
    f.write_all(&data)?;
    process_entry(toc, entry);
    return Ok(());
}
