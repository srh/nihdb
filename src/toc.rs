use std;
use std::io::*;
use encoding::*;
use std::result::Result;

extern crate crc;

/* toc file format:

    [entry][entry]...[entry]
    
[entry] format:

    [u64][u32][varint]
    L     C    N

    where L is the length of the entry, C is its checksum, and N is the entry.

*/

pub struct TOC {
    pub next_table_number: u64,
}

pub struct Entry {
    pub next_table_number: u64,
}

fn toc_filename(dir: &str) -> String {
    return format!("{}/toc", dir);
}

pub fn create_toc(dir: &str) -> Result<std::fs::File, std::io::Error> {
    let f = std::fs::File::create(toc_filename(dir));
    // Nothing to write yet.
    return f;
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

    let next_table_number: u64 = decode_uint(entry_slice, pos)?;
    if *pos - front != length {
        return None;
    }
    return Some(Entry{next_table_number: next_table_number});
}

fn encode_entry(ent: &Entry) -> Vec<u8> {
    let mut v = Vec::<u8>::new();
    encode_uint(&mut v, ent.next_table_number);
    let length: usize = v.len();
    let checksum: u32 = crc::crc32::checksum_castagnoli(&v);
    let mut ret = Vec::<u8>::new();
    encode_u64(&mut ret, length as u64);
    encode_u32(&mut ret, checksum);
    ret.extend(v);
    return ret;
}

pub fn read_toc(dir: &str) -> Option<(std::fs::File, TOC)> {
    let mut f = std::fs::OpenOptions::new().read(true).append(true)
        .open(toc_filename(dir)).expect("open toc");  // NOTE error handling
    let mut buf = Vec::<u8>::new();
    f.read_to_end(&mut buf).expect("read_to_end toc");  // NOTE error handling

    let mut toc = TOC{next_table_number: 0};

    let mut pos: usize = 0;
    while pos < buf.len() {
        let savepos = pos;
        if let Some(entry) = decode_entry(&buf, &mut pos) {
            toc.next_table_number = entry.next_table_number;
        } else {
            f.set_len(savepos as u64).expect("read_toc set len");  // NOTE error handling
            return Some((f, toc));
        }
    }

    return Some((f, toc));
}

pub fn append_toc(f: &mut std::fs::File, entry: Entry) -> Result<(), std::io::Error> {
    let data: Vec<u8> = encode_entry(&entry);
    f.write_all(&data)?;
    return Ok(());
}