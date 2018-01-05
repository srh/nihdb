use util::*;

pub fn encode_uvarint(v: &mut Vec<u8>, mut n: u64) {
    while n >= 128 {
        v.push((128 | (n & 127)) as u8);
        n >>= 7;
    }
    v.push(n as u8);
}

pub fn decode_uvarint(v: &[u8], pos: &mut usize) -> Option<u64> {
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    while *pos < v.len() {
        let b: u8 = v[*pos];
        println!("decoded byte {}", b);
        *pos += 1;
        n |= ((b & 127) as u64) << shift;
        shift += 7;
        if 0 == (b & 128) {
            return Some(n);
        }
    }
    return None;
}

pub fn try_into_size(x: u64) -> Option<usize> {
    if x > (usize::max_value() as u64) {
        return None;
    }
    return Some(x as usize);
}

pub fn encode_u64(v: &mut Vec<u8>, mut n: u64) {
    // NOTE: Find some stdlib function to do this, maybe.
    for _ in 0..8 {
        v.push((n & 255) as u8);
        n = n >> 8;
    }
}

pub fn decode_u64(v: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos + 8 > v.len() {
        return None;
    }
    let mut n: u64 = 0;
    for i in (0..8).rev() {
        n <<= 8;
        n |= v[*pos + i] as u64;
    }
    *pos += 8;
    return Some(n);
}

pub fn encode_u32(v: &mut Vec<u8>, mut n: u32) {
    // NOTE: Find some stdlib function to do this, maybe.
    for _ in 0..4 {
        v.push((n & 255) as u8);
        n = n >> 8;
    }
}

pub fn decode_u32(v: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > v.len() {
        return None;
    }
    let mut n: u32 = 0;
    for i in (0..4).rev() {
        n <<= 8;
        n |= v[*pos + i] as u32;
    }
    *pos += 4;
    return Some(n);
}

pub fn encode_str(v: &mut Vec<u8>, b: &[u8]) {
    encode_uvarint(v, b.len() as u64);
    v.extend_from_slice(b);
}

pub fn observe_str<'a>(v: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let length: usize = try_into_size(decode_uvarint(v, pos)?)?;
    if v.len() - *pos < length {
        return None;
    }
    let end_pos = *pos + length;
    let slice = &v[*pos..end_pos];
    *pos = end_pos;
    return Some(slice);
}

pub fn decode_str(v: &[u8], pos: &mut usize) -> Option<Buf> {
    let s: &[u8] = observe_str(v, pos)?;
    return Some(s.to_vec());
}

#[cfg(test)]
mod tests {
    #[test]
    fn str() {
        let mut v = Vec::<u8>::new();
        let text: &[u8] = "this is a test".as_bytes();
        super::encode_str(&mut v, text);
        let mut pos: usize = 0;
        assert_eq!(Some(text.to_vec()), super::decode_str(&v, &mut pos));
        assert_eq!(v.len(), pos);
    }

    fn help_test_uvarint(num: u64) {
        let mut v = Vec::<u8>::new();
        super::encode_uvarint(&mut v, num);
        let mut pos: usize = 0;
        assert_eq!(Some(num), super::decode_uvarint(&v, &mut pos));
        assert_eq!(v.len(), pos);
    }

    #[test]
    fn uint() {
        help_test_uvarint(0);
        help_test_uvarint(37);
        help_test_uvarint(127);
        help_test_uvarint(128);
        help_test_uvarint(137);
        help_test_uvarint(12345678);
    }
}
