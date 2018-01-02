use memstore::*;
use encoding::*;
use error::*;
use std;
use std::io::Read;
use std::io::Write;

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

    [unsigned varint][str]

    with the unsigned varint being the offset of the value.
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
        let s: String = decode_str(v, pos)?;
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
        encode_uint(&mut self.keys_buf, self.values_buf.len() as u64);
        encode_str(&mut self.keys_buf, key);
        encode_mutation(&mut self.values_buf, value);
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

// Returns keys_offset, file_size, smallest key, biggest key.
pub fn flush_to_disk<'a>(dir: &str, table_id: u64, m: &'a MemStore) -> Result<(u64, u64, String, String)> {
    assert!(!m.entries.is_empty());
    let mut builder = TableBuilder::<'a>::new();
    
    for (key, value) in m.entries.iter() {
        builder.add_mutation(key, value);
    }
    let mut f = std::fs::File::create(format!("{}/{}.tab", dir, table_id))?;
    return builder.finish(&mut f);
}

pub fn iterate_table(dir: &str, table_id: u64, func: &mut FnMut(String, Mutation) -> ()) -> Result<()> {
    let mut f: std::fs::File = std::fs::File::open(format!("{}/{}.tab", dir, table_id))?;
    let mut buf = Vec::<u8>::new();
    f.read_to_end(&mut buf)?;

    if buf.len() < 8 {
        Err(RihError::new("table too short"))?;
    }

    // NOTE: We don't really need to read the keys_offset from the table file,
    // because it's in the TOC.
    let keys_end: usize = buf.len() - 8;
    let keys_offset: usize = {
        let mut pos = keys_end;
        try_into_size(decode_u64(&buf, &mut pos)
            .or_err("could not decode keys_offset")?)
            .or_err("keys_offset invalid")?
    };

    let key_buf: &[u8] = buf.get(keys_offset..keys_end).or_err("bad keys interval")?;
    let value_buf: &[u8] = buf.get(0..keys_offset).or_err("bad values interval")?;
    let mut i: usize = 0;
    while i < key_buf.len() {
        let value_offset: usize = try_into_size(decode_uint(key_buf, &mut i)
            .or_err("could not decode uint")?)
            .or_err("value offset out of range")?;
        if value_offset >= keys_offset {
            Err(RihError::new("key has improper value offset"))?;
        }
        let key: String = decode_str(key_buf, &mut i).or_err("cannot decode key")?;
        let value: Mutation = {
            let mut pos = value_offset;
            decode_mutation(value_buf, &mut pos).or_err("cannot decode value")?
        };
        func(key, value);
    }

    return Ok(());
}
