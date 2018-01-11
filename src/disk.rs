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

    [key][key]...[key]

    with the keys in ascending order

[key] format:

    [unsigned varint][unsigned varint][str]

    with the unsigned varints being the offset, length of the value.
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

pub struct TableBuilder {
    values_buf: Vec<u8>,
    keys_buf: Vec<u8>,
    // NOTE: Instead of copying/allocating these, we could (a) reuse the same
    // buffer, or (b) decode out of keys_buf when we need the value.
    first_key: Option<Buf>,
    last_key: Option<Buf>,
}

impl TableBuilder {
    pub fn new() -> TableBuilder {
        return TableBuilder{
            values_buf: Vec::new(),
            keys_buf: Vec::new(),
            first_key: None,
            last_key: None,
        };
    }

    // Returns true if nothing has been written to the table builder.
    pub fn is_empty(&self) -> bool {
        return self.first_key.is_none();
    }

    pub fn file_size(&self) -> usize {
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
        encode_uvarint(&mut self.keys_buf, value_offset);
        encode_uvarint(&mut self.keys_buf, value_length);
        encode_str(&mut self.keys_buf, key);
    }

    // Returns keys_offset, file_size, smallest key, biggest key.
    pub fn finish(&mut self, writer: &mut Write) -> Result<(u64, u64, Buf, Buf)> {
        assert!(!self.first_key.is_none());
        let keys_offset = self.values_buf.len() as u64;
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
pub fn flush_to_disk<'a>(dir: &str, table_id: u64, m: &'a MemStore) -> Result<(u64, u64, Buf, Buf)> {
    assert!(!m.entries.is_empty());
    let mut builder = TableBuilder::new();
    
    for (key, value) in m.entries.iter() {
        builder.add_mutation(key, value);
    }
    let mut f = std::fs::File::create(table_filepath(dir, table_id))?;
    return builder.finish(&mut f);
}

fn open_table_file(dir: &str, table_id: u64) -> Result<std::fs::File> {
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
    let mut iter = TableKeysIterator::whole_table(RcRef::new(Rc::new(keys_buf)).map(|v: &Vec<u8>| v as &[u8]));
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
                    return rih_err("mutation decoded too small");
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
    keys_pos: usize,
}

impl TableKeysIterator {
    fn whole_table(keys: RcRef<Vec<u8>, [u8]>) -> TableKeysIterator {
        return TableKeysIterator{keys: keys, keys_pos: 0};
    }

    fn decode_key<'a>(keys: &'a RcRef<Vec<u8>, [u8]>, pos: &mut usize) -> Result<(&'a [u8], u64, u64)> {
        let value_offset: u64 = decode_uvarint(keys, pos)
            .or_err("could not decode value_offset")?;
        let value_length: u64 = decode_uvarint(keys, pos)
            .or_err("could not decode value_length")?;
        let key: &[u8] = observe_str(&*keys, pos).or_err("cannot decode key")?;
        return Ok((key, value_offset, value_length));
    }

    fn help_current_key(keys: &RcRef<Vec<u8>, [u8]>, keys_pos: usize) -> Result<Option<(&[u8], u64, u64)>> {
        if keys_pos == keys.len() {
            return Ok(None);
        }
        // NOTE: It might be nice if this came pre-decoded.
        let mut pos = keys_pos;
        let tup = TableKeysIterator::decode_key(keys, &mut pos)?;
        return Ok(Some(tup));
    }

    fn current_key(&self) -> Result<Option<(&[u8], u64, u64)>> {
        return TableKeysIterator::help_current_key(&self.keys, self.keys_pos);
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
        if let Some(ret) = TableKeysIterator::help_current_key(&self.keys, self.keys_pos)? {
            TableKeysIterator::help_step_key(&self.keys, &mut self.keys_pos)?;
            return Ok(Some(ret));
        }
        return Ok(None);
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
    while let Some((key, _, _)) = TableKeysIterator::help_current_key(&iter.keys, iter.keys_pos)? {
        if above_lower_bound(key, lower) {
            return Ok(());
        }
        TableKeysIterator::help_step_key(&iter.keys, &mut iter.keys_pos)?;
    }
    return Ok(());
}

pub struct TableIterator {
    keys_iter: TableKeysIterator,
    // values_buf is just a slice of the table file that we're going to iterate,
    // pre-computed based on key range.  So any offsets into it need to have
    // offset_of_values_buf subtracted.
    values_buf: Vec<u8>,
    offset_of_values_buf: u64,
}

impl TableIterator {
    pub fn make(dir: &str, ti: &TableInfo, interval: &Interval<Buf>) -> Result<TableIterator> {
        let (mut f, keys_buf) = load_table_keys_buf(dir, ti)?;
        let mut keys_iter = TableKeysIterator::whole_table(RcRef::new(Rc::new(keys_buf)).map(|v| v as &[u8]));
        advance_past_lower_bound(&mut keys_iter, &interval.lower)?;
        // NOTE: We could also advance past the upper bound (don't bother if
        // greater than ti.biggest_key) and read a smaller range of values.
        if let Some((_, value_offset, _)) = keys_iter.current_key()? {
            let length: usize = try_into_size(ti.keys_offset - value_offset).or_err("bad value_offset")?;
            let values_buf: Vec<u8> = read_exact(&mut f, value_offset, length)?;
            return Ok(TableIterator{
                keys_iter: keys_iter,
                values_buf: values_buf,
                offset_of_values_buf: value_offset,
            });
        } else {
            return Ok(TableIterator{
                keys_iter: keys_iter,
                // keys_iter is empty, so these will never get used.
                values_buf: Vec::<u8>::new(),
                offset_of_values_buf: 0,
            });
        }
    }
}

impl MutationIterator for TableIterator {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        return self.keys_iter.current_key().map(|x| x.map(|(k, _, _)| k));
    }

    fn current_value(&mut self) -> Result<Mutation> {
        if let Some((_, value_offset, value_length)) = self.keys_iter.current_key()? {
            let value_rel_offset: u64 = value_offset - self.offset_of_values_buf;
            let value_rel_offset = try_into_size(value_rel_offset).or_err("value_rel_offset not size")?;
            let value_length = try_into_size(value_length).or_err("value_length not size")?;

            let sl: &[u8] = self.values_buf.get(value_rel_offset..value_rel_offset + value_length)
                .or_err("bad value offset/length")?;

            let mut pos: usize = 0;
            let value: Mutation = decode_mutation(sl, &mut pos).or_err("cannot decode mutation")?;
            if pos != value_length {
                return rih_err("mutation decoded too small");
            }
            return Ok(value);
        }
        return rih_err("current_value called on empty TableIterator");
    }

    fn step(&mut self) -> Result<()> {
        return self.keys_iter.step_key();
    }
}
