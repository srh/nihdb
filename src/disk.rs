use memstore::*;
use encoding::*;
use error::*;
use toc::*;
use std;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::cmp::*;

/* .tab file format:

    [values...][keys...][64-bit KEY_OFFSET]
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
        let s: String = decode_str(&v, pos)?;
        return Some(Mutation::Set(s));
    } else if b == 1 {
        return Some(Mutation::Delete);
    } else {
        return None;
    }
}

struct TableBuilder<'a> {
    values_buf: Vec<u8>,
    keys_buf: Vec<u8>,
    first_key: Option<&'a str>,
    last_key: Option<&'a str>,
}

impl<'a> TableBuilder<'a> {
    fn new() -> TableBuilder<'a> {
        return TableBuilder{
            values_buf: Vec::new(),
            keys_buf: Vec::new(),
            first_key: None,
            last_key: None,
        };
    }

    // This method has to be called in increasing order.
    fn add_mutation(&mut self, key: &'a str, value: &Mutation) {
        self.last_key = Some(key);
        if self.first_key.is_none() {
            self.first_key = self.last_key;
        }
        let value_offset = self.values_buf.len() as u64;
        encode_mutation(&mut self.values_buf, value);
        let value_length = self.values_buf.len() as u64 - value_offset;
        encode_uint(&mut self.keys_buf, value_offset);
        encode_uint(&mut self.keys_buf, value_length);
        encode_str(&mut self.keys_buf, key);
    }

    // Returns keys_offset, file_size, smallest key, biggest key.
    fn finish(&mut self, writer: &mut Write) -> Result<(u64, u64, String, String)> {
        assert!(!self.first_key.is_none());
        let keys_offset = self.values_buf.len() as u64;
        encode_u64(&mut self.keys_buf, keys_offset);  // NOTE: Not necessary now that it's in TOC.
        writer.write_all(&self.values_buf)?;
        writer.write_all(&self.keys_buf)?;
        writer.flush()?;
        return Ok((
            keys_offset,
            keys_offset + self.keys_buf.len() as u64,
            self.first_key.unwrap().to_string(),
            self.last_key.unwrap().to_string()
        ));
    }
}

fn table_filepath(dir: &str, table_id: u64) -> String {
    return format!("{}/{}.tab", dir, table_id);
}

// Returns keys_offset, file_size, smallest key, biggest key.
pub fn flush_to_disk<'a>(dir: &str, table_id: u64, m: &'a MemStore) -> Result<(u64, u64, String, String)> {
    assert!(!m.entries.is_empty());
    let mut builder = TableBuilder::<'a>::new();
    
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

// Iterates until buf exhausted or func returns false.
pub fn iterate_keys<T: FnMut(String, u64, u64) -> Result<bool>>(keys_buf: &[u8], mut func: T) -> Result<()> {
    let mut pos: usize = 0;
    while pos < keys_buf.len() {
        let value_offset: u64 = decode_uint(keys_buf, &mut pos)
            .or_err("could not decode value offset")?;
        let value_length: u64 = decode_uint(keys_buf, &mut pos)
            .or_err("could not decode value length")?;
        let key: String = decode_str(keys_buf, &mut pos).or_err("cannot decode key")?;
        if !func(key, value_offset, value_length)? {
            return Ok(());
        }
    }
    return Ok(());
}

pub fn iterate_table(dir: &str, ti: &TableInfo, func: &mut FnMut(String, Mutation) -> ()) -> Result<()> {
    let mut buf = Vec::<u8>::new();
    {
        let mut f: std::fs::File = open_table_file(dir, ti.id)?;
        f.read_to_end(&mut buf)?;
    }

    if buf.len() < 8 {
        // NOTE: Should be impossible in TableInfo.
        Err(RihError::new("table too short"))?;
    }

    let keys_end: usize = buf.len() - 8;
    let keys_offset: usize = try_into_size(ti.keys_offset).or_err("keys_offset invalid")?;

    let keys_buf: &[u8] = buf.get(keys_offset..keys_end).or_err("bad keys interval")?;
    let value_buf: &[u8] = buf.get(0..keys_offset).or_err("bad values interval")?;
    iterate_keys(keys_buf, |key: String, value_offset64: u64, value_length64: u64| {
        let value_offset = try_into_size(value_offset64).or_err("value_offset not size")?;
        let value_length = try_into_size(value_length64).or_err("value_length not size")?;
        let value_slice = value_buf.get(value_offset..value_offset + value_length)
            .or_err("value has improper slice")?;
        
        let mut pos: usize = 0;
        let value: Mutation = decode_mutation(value_slice, &mut pos).or_err("cannot decode mutation")?;
        if pos != value_length {
            Err(RihError::new("mutation decoded to small"))?;
        }
        func(key, value);
        return Ok(true);
    })?;

    return Ok(());
}

fn read_exact(f: &mut std::fs::File, offset: u64, length: usize) -> Result<Vec<u8>> {
    // NOTE: Can we use unsafe to get uninitialized buf
    f.seek(std::io::SeekFrom::Start(offset))?;
    let mut buf = Vec::<u8>::new();
    buf.resize(length, 0u8);
    f.read_exact(&mut buf)?;
    return Ok(buf);
}

fn lookup_table(dir: &str, ti: &TableInfo, key: &str) -> Result<Option<Mutation>> {
    let mut f: std::fs::File = open_table_file(dir, ti.id)?;
    let keys_offset: usize = try_into_size(ti.keys_offset).or_err("lookup_table keys_offset")?;
    assert!(ti.file_size - 8 >= ti.keys_offset);  // NOTE: Make a guarantee of TableInfo.
    // NOTE: Hard-coded 8's everywhere.
    let keys_buf = read_exact(&mut f, ti.keys_offset, (ti.file_size - 8 - ti.keys_offset) as usize)?;
    
    // NOTE: Give file better random access structure
    // NOTE: Don't copy the key out in this case
    let mut ret: Option<Mutation> = None;
    iterate_keys(&keys_buf, |iter_key: String, value_offset: u64, value_length: u64| {
        return Ok(match key.cmp(&iter_key) {
            Ordering::Less => false,
            Ordering::Equal => {
                let value_length = try_into_size(value_length).or_err("value length too big")?;
                let value_buf: Vec<u8> = read_exact(&mut f, value_offset, value_length)?;
                let mut pos: usize = 0;
                let value: Mutation = decode_mutation(&value_buf, &mut pos).or_err("cannot decode mutation")?;
                if pos != value_buf.len() {
                    Err(RihError::new("mutation decoded too small"))?;
                }
                ret = Some(value);
                false  // stop iterating keys
            },
            Ordering::Greater => true,
        });
    })?;

    return Ok(ret);
}
