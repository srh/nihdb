use encoding::*;
use error::*;
use iter::*;
use memstore::*;
use util::*;
use toc::*;

use owning_ref::*;
use std;
use std::collections::Bound;
use std::cmp::*;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::rc::Rc;


/* .tab file format:

    [values...][keys...][8-byte KEY_OFFSET]
               ^
               KEY_OFFSET

[values...] format:

    [value][value]...[value]

    where each value is either [u8 = 0][str] or [u8 = 1].

[str] format:

    [unsigned varint][bytes...]

[keys...] format:

    NOTE: This doc is not yet implemented.

    [entry][entry]...[entry][len][u8 length of len]

    with the entries in ascending order by key, the last [len] holding the byte length of the last
    [entry], the last [u8 length of len] holding the byte length of the last [len].

[len] format:

    a varint

[key] format:

    [unsigned varint][unsigned varint][unsigned varint][str]

    with the unsigned varints being the previous entry length, the offset of the value,
    and length of the value.  The str is the key.
*/

const TAB_BACK_PADDING: usize = 8;

fn encode_mutation(v: &mut Vec<u8>, m: &Mutation) {
    match m {
        &Mutation::Set(ref s) => {
            v.push(0);
            encode_str(v, s);
        },
        &Mutation::Delete => {
            v.push(1);
        }
    }
}

fn decode_mutation(v: &[u8], pos: &mut usize) -> Option<Mutation> {
    let b: u8 = *v.get(*pos)?;
    *pos += 1;
    if b == 0 {
        let s: Buf = decode_str(&v, pos)?;
        return Some(Mutation::Set(s));
    } else if b == 1 {
        return Some(Mutation::Delete);
    } else {
        return None;
    }
}

// NOTE: Could compress key entry lens with key len, remove key len

// NOTE: Should represent mutation with set/delete bit in keys by adding 1 to value len.

// NOTE: Could compress value offsets, because values are in order.

// NOTE: Compaction could judiciously avoid rewriting value portion of .tab
// files, by truncating keys off of old .tab files and having keys reference offsets
// in different tab files.  Compression of value offset would be relative to the
// neighboring value offset referencing the same .tab file.

// Approximate estimates of disk overhead (within 1% since lengths are varint-encoded).
pub fn approx_key_usage(key: &[u8]) -> usize {
    return 1 // prev key entry len
        + 1 // value len
        + 3 // value offset
        + 1 // key len
        + key.len();
}
fn set_value_usage(val: &[u8]) -> usize {
    return 1 // Set/delete byte
        + 1 // val len
        + val.len();
}
pub fn approx_value_usage(val: &Mutation) -> usize {
    return match val {
        &Mutation::Set(ref x) => set_value_usage(&x),
        &Mutation::Delete => 1,
    };
}


pub struct TableBuilder {
    values_buf: Vec<u8>,
    keys_buf: Vec<u8>,
    // NOTE: Instead of copying/allocating these, we could (a) reuse the same
    // buffer, or (b) decode out of keys_buf when we need the value.
    first_key: Option<Buf>,
    last_key: Option<Buf>,
    last_entry_len: u64,
}

impl TableBuilder {
    pub fn new() -> TableBuilder {
        return TableBuilder{
            values_buf: Vec::new(),
            keys_buf: Vec::new(),
            first_key: None,
            last_key: None,
            last_entry_len: 0,
        };
    }

    // Returns true if nothing has been written to the table builder.
    pub fn is_empty(&self) -> bool {
        return self.first_key.is_none();
    }

    pub fn lowerbound_file_size(&self) -> usize {
        return self.values_buf.len() + self.keys_buf.len() + TAB_BACK_PADDING;
    }

    // This method has to be called in increasing order.
    // NOTE: Possibly could take key by value.
    pub fn add_mutation(&mut self, key: &[u8], value: &Mutation) {
        self.last_key = Some(key.to_vec());
        if self.first_key.is_none() {
            self.first_key = self.last_key.clone();
        }
        let value_offset = self.values_buf.len() as u64;
        encode_mutation(&mut self.values_buf, value);
        let value_length = self.values_buf.len() as u64 - value_offset;
        let pre_pos: usize = self.keys_buf.len();
        encode_uvarint(&mut self.keys_buf, self.last_entry_len);
        encode_uvarint(&mut self.keys_buf, value_offset);
        encode_uvarint(&mut self.keys_buf, value_length);
        encode_str(&mut self.keys_buf, key);
        self.last_entry_len = (self.keys_buf.len() - pre_pos) as u64;
    }

    // Returns keys_offset, file_size, smallest key, biggest key.
    // NOTE: Take self by value.
    pub fn finish(&mut self, writer: &mut Write) -> Result<(u64, u64, Buf, Buf)> {
        assert!(!self.first_key.is_none());
        let keys_offset = self.values_buf.len() as u64;
        let pre_offset = self.keys_buf.len();
        // Encode last value of pre_pos.
        encode_uvarint(&mut self.keys_buf, self.last_entry_len);
        // Encode length of last uvarint, so we can step backwards.
        let step_back = (self.keys_buf.len() - pre_offset) as u8;
        self.keys_buf.push(step_back);
        encode_u64(&mut self.keys_buf, keys_offset);  // NOTE: Not necessary now that it's in TOC.
        writer.write_all(&self.values_buf)?;
        writer.write_all(&self.keys_buf)?;
        writer.flush()?;
        return Ok((
            keys_offset,
            keys_offset + self.keys_buf.len() as u64,
            self.first_key.as_ref().unwrap().clone(),
            self.last_key.as_ref().unwrap().clone(),
        ));
    }
}

// Returns keys_offset, file_size, smallest key, biggest key.
pub fn flush_to_disk<'a>(dir: &str, table_id: TableId, m: &'a MemStore) -> Result<(u64, u64, Buf, Buf)> {
    assert!(!m.entries.is_empty());
    let mut builder = TableBuilder::new();
    
    for (key, value) in m.entries.iter() {
        builder.add_mutation(key, value);
    }
    let mut f = std::fs::File::create(table_filepath(dir, table_id))?;
    return builder.finish(&mut f);
}

fn open_table_file(dir: &str, table_id: TableId) -> Result<std::fs::File> {
    let f = std::fs::File::open(table_filepath(dir, table_id))?;
    return Ok(f);
}

// NOTE: We'll want to use pread.
fn read_exact(f: &mut std::fs::File, offset: u64, length: usize) -> Result<Vec<u8>> {
    // NOTE: Can we use unsafe to get uninitialized buf
    f.seek(std::io::SeekFrom::Start(offset))?;
    let mut buf = Vec::<u8>::new();
    buf.resize(length, 0u8);
    f.read_exact(&mut buf)?;
    return Ok(buf);
}

pub fn lookup_table(dir: &str, ti: &TableInfo, key: &[u8]) -> Result<Option<Mutation>> {
    let (mut f, keys_buf) = load_table_keys_buf(dir, ti)?;
    
    // NOTE: Give file better random access structure
    let mut iter = TableKeysIterator::whole_table(RcRef::new(Rc::new(keys_buf)).map(|v: &Vec<u8>| v as &[u8]))?;
    while let Some((iter_key, value_offset, value_length)) = iter.next_key()? {
        match key.cmp(iter_key) {
            Ordering::Less => {
                break;
            },
            Ordering::Equal => {
                let value_length = try_into_size(value_length).or_err("value length too big")?;
                let value_buf: Vec<u8> = read_exact(&mut f, value_offset, value_length)?;
                let mut pos: usize = 0;
                let value: Mutation = decode_mutation(&value_buf, &mut pos).or_err("cannot decode mutation")?;
                if pos != value_buf.len() {
                    return mk_err("mutation decoded too small");
                }
                return Ok(Some(value));
            },
            Ordering::Greater => (),
        };
    }

    return Ok(None);
}

struct TableKeysIterator {
    keys: RcRef<Vec<u8>, [u8]>,
    // Position after the last entry, but before the last entry length or its 1-byte length
    keys_pos: usize,
    keys_end_pos: usize,
}

struct TableKeysInterval {
    keys_pos: usize,
    keys_end_pos: usize,
}

impl TableKeysIterator {
    fn whole_table(keys: RcRef<Vec<u8>, [u8]>) -> Result<TableKeysIterator> {
        let step_back = *keys.get(keys.len() - 1).or_err("table keys buffer too small")? as usize;
        if keys.len() < 1 + step_back {
            return mk_err("table keys step_back too small");
        }
        let keys_end_pos: usize = keys.len() - 1 - step_back;
        return Ok(TableKeysIterator{keys: keys, keys_pos: 0, keys_end_pos: keys_end_pos});
    }

    fn save_pos(&self) -> TableKeysInterval {
        return TableKeysInterval{keys_pos: self.keys_pos, keys_end_pos: self.keys_end_pos};
    }

    fn decode_key<'a>(keys: &'a RcRef<Vec<u8>, [u8]>, pos: &mut usize) -> Result<(&'a [u8], u64, u64)> {
        let _prev_entry_length: u64 = decode_uvarint(keys, pos)
            .or_err("could not decode prev entry length")?;
        let value_offset: u64 = decode_uvarint(keys, pos)
            .or_err("could not decode value_offset")?;
        let value_length: u64 = decode_uvarint(keys, pos)
            .or_err("could not decode value_length")?;
        let key: &[u8] = observe_str(&*keys, pos).or_err("cannot decode key")?;
        return Ok((key, value_offset, value_length));
    }

    fn help_current_key(keys: &RcRef<Vec<u8>, [u8]>, keys_pos: usize, keys_end_pos: usize) -> Result<Option<(&[u8], u64, u64)>> {
        if keys_pos == keys_end_pos {
            return Ok(None);
        }
        // NOTE: It might be nice if this came pre-decoded.
        let mut pos = keys_pos;
        let tup = TableKeysIterator::decode_key(keys, &mut pos)?;
        return Ok(Some(tup));
    }

    fn current_key(&self) -> Result<Option<(&[u8], u64, u64)>> {
        return TableKeysIterator::help_current_key(&self.keys, self.keys_pos, self.keys_end_pos);
    }

    // Helper separates the mutability of kesy from keys_pos for use with
    // help_current_key.
    fn help_step_key(keys: &RcRef<Vec<u8>, [u8]>, keys_pos: &mut usize) -> Result<()> {
        let mut pos = *keys_pos;
        let _ = TableKeysIterator::decode_key(&keys, &mut pos)?;
        *keys_pos = pos;
        return Ok(());
    }

    fn step_key(&mut self) -> Result<()> {
        return TableKeysIterator::help_step_key(&self.keys, &mut self.keys_pos)
    }

    fn next_key(&mut self) -> Result<Option<(&[u8], u64, u64)>> {
        if let Some(ret) = TableKeysIterator::help_current_key(&self.keys, self.keys_pos, self.keys_end_pos)? {
            TableKeysIterator::help_step_key(&self.keys, &mut self.keys_pos)?;
            return Ok(Some(ret));
        }
        return Ok(None);
    }

    fn current_back_key(&self) -> Result<Option<(&[u8], u64, u64)>> {
        if self.keys_pos == self.keys_end_pos {
            return Ok(None);
        }
        let mut pos = self.keys_end_pos;
        let entry_length = decode_uvarint(&self.keys, &mut pos)
            .or_err("cannot decode prev length")?;
        assert!(entry_length > 0);  // NOTE: error handling
        // NOTE: usize conversion
        let ret = TableKeysIterator::help_current_key(
            &self.keys, self.keys_end_pos - entry_length as usize, self.keys_end_pos);
        return ret;
    }

    // This works like a DoubleEndedIterator -- steps backwards.
    fn step_back_key(&mut self) -> Result<bool> {
        if self.keys_pos == self.keys_end_pos {
            return Ok(false);
        }
        let mut pos = self.keys_end_pos;
        let entry_length = decode_uvarint(&self.keys, &mut pos).or_err("cannot decode prev length")?;
        assert!(entry_length > 0);  // NOTE: Handle error better
        self.keys_end_pos -= entry_length as usize;  // NOTE: Handle conversion
        assert!(self.keys_pos <= self.keys_end_pos);
        return Ok(true);
    }
}

pub fn load_table_keys_buf(dir: &str, ti: &TableInfo) -> Result<(std::fs::File, Vec<u8>)> {
    let mut f: std::fs::File = open_table_file(dir, ti.id)?;
    // NOTE: Make these guarantees of TableInfo.
    let keys_offset: usize = try_into_size(ti.keys_offset).or_err("lookup_table keys_offset")?;
    let file_size: usize = try_into_size(ti.file_size).or_err("lookup_table file_size")?;
    assert!(file_size >= TAB_BACK_PADDING && file_size - TAB_BACK_PADDING >= keys_offset);
    let keys_buf = read_exact(&mut f, ti.keys_offset, file_size - TAB_BACK_PADDING - keys_offset)?;
    return Ok((f, keys_buf));
}

fn advance_past_lower_bound(iter: &mut TableKeysIterator, lower: &Bound<Buf>) -> Result<()> {
    // NOTE: Double-decodes keys.
    while let Some((key, _, _)) = TableKeysIterator::help_current_key(&iter.keys, iter.keys_pos, iter.keys_end_pos)? {
        if above_lower_bound(key, lower) {
            return Ok(());
        }
        TableKeysIterator::help_step_key(&iter.keys, &mut iter.keys_pos)?;
    }
    return Ok(());
}

fn advance_before_upper_bound(iter: &mut TableKeysIterator, upper: &Bound<Buf>) -> Result<()> {
    loop {
        let pos = iter.save_pos();
        if !iter.step_back_key()? {
            return Ok(());
        }
        let (key, _, _) = TableKeysIterator::help_current_key(&iter.keys, iter.keys_end_pos, pos.keys_end_pos)?
            .or_err("current_key after step_back_key")?;
        if below_upper_bound(key, upper) {
            iter.keys_pos = pos.keys_pos;
            iter.keys_end_pos = pos.keys_end_pos;
            return Ok(());
        }
    }
}

pub struct TableIterator {
    keys_iter: TableKeysIterator,
    // values_buf is just a slice of the table file that we're going to iterate,
    // pre-computed based on key range.  So any offsets into it need to have
    // offset_of_values_buf subtracted.
    values_buf: Vec<u8>,
    offset_of_values_buf: u64,
    direction: Direction,
}

impl TableIterator {
    pub fn make(dir: &str, ti: &TableInfo, interval: &Interval<Buf>, direction: Direction
    ) -> Result<TableIterator> {
        let (mut f, keys_buf) = load_table_keys_buf(dir, ti)?;
        let mut keys_iter = TableKeysIterator::whole_table(RcRef::new(Rc::new(keys_buf)).map(|v| v as &[u8]))?;
        advance_past_lower_bound(&mut keys_iter, &interval.lower)?;
        advance_before_upper_bound(&mut keys_iter, &interval.upper)?;
        // NOTE: We could use the upper bound to read fewer values.
        if let Some((_, value_offset, _)) = TableIterator::help_current_entry(&keys_iter, Direction::Forward)? {
            let length: usize = try_into_size(ti.keys_offset - value_offset).or_err("bad value_offset")?;
            let values_buf: Vec<u8> = read_exact(&mut f, value_offset, length)?;
            return Ok(TableIterator{
                keys_iter: keys_iter,
                values_buf: values_buf,
                offset_of_values_buf: value_offset,
                direction: direction,
            });
        } else {
            return Ok(TableIterator{
                keys_iter: keys_iter,
                // keys_iter is empty, so these will never get used.
                values_buf: Vec::<u8>::new(),
                offset_of_values_buf: 0,
                direction: direction,
            });
        }
    }

    fn help_current_entry(keys_iter: &TableKeysIterator, direction: Direction
    ) -> Result<Option<(&[u8], u64, u64)>> {
        return match direction {
            Direction::Forward => keys_iter.current_key(),
            Direction::Backward => keys_iter.current_back_key()
        };
    }

    fn current_entry(&self) -> Result<Option<(&[u8], u64, u64)>> {
        return TableIterator::help_current_entry(&self.keys_iter, self.direction);
    }
}

impl MutationIterator for TableIterator {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        let ret = self.current_entry().map(|x| x.map(|(k, _, _)| k));
        return ret;
    }

    fn current_value(&mut self) -> Result<Mutation> {
        if let Some((_, value_offset, value_length)) = self.current_entry()? {
            let value_rel_offset: u64 = value_offset - self.offset_of_values_buf;
            let value_rel_offset = try_into_size(value_rel_offset).or_err("value_rel_offset not size")?;
            let value_length = try_into_size(value_length).or_err("value_length not size")?;

            let sl: &[u8] = self.values_buf.get(value_rel_offset..value_rel_offset + value_length)
                .or_err("bad value offset/length")?;

            let mut pos: usize = 0;
            let value: Mutation = decode_mutation(sl, &mut pos).or_err("cannot decode mutation")?;
            if pos != value_length {
                return mk_err("mutation decoded too small");
            }
            return Ok(value);
        }
        return mk_err("current_value called on empty TableIterator");
    }

    fn step(&mut self) -> Result<()> {
        match self.direction {
            Direction::Forward => {
                return self.keys_iter.step_key();
            },
            Direction::Backward => {
                if !self.keys_iter.step_back_key()? {
                    return mk_err("cannot step backward in TableIterator");
                }
                return Ok(());
            }
        }
    }
}
