// NOTE: Handle overflow and be generic w.r.t. int types.
pub fn encode_uint(v: &mut Vec<u8>, mut n: u64) {
    while n >= 128 {
        v.push((128 | (n & 127)) as u8);
        n >>= 7;
    }
    v.push(n as u8);
}

pub fn decode_uint(v: &[u8], pos: &mut usize) -> Option<u64> {
    let mut n: u64 = 0;
    while *pos < v.len() {
        let b = v[*pos];
        *pos += 1;
        if b < 128 {
            return Some(n | (b as u64));
        } else {
            n <<= 7;
            n |= (b & 127) as u64;
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

pub fn encode_str(v: &mut Vec<u8>, n: &str) {
    let b: &[u8] = n.as_bytes();
    encode_uint(v, b.len() as u64);
    v.extend_from_slice(b);
}

pub fn decode_str(v: &[u8], pos: &mut usize) -> Option<String> {
    let length: usize = try_into_size(decode_uint(v, pos)?)?;
    if v.len() - *pos < length {
        return None;
    }
    let end_pos = *pos + length;
    let mut buf = Vec::<u8>::new();
    buf.extend_from_slice(&v[*pos..end_pos]);
    *pos = end_pos;
    return String::from_utf8(buf).ok();
}

#[cfg(test)]
mod tests {
    #[test]
    fn str() {
        let mut v = Vec::<u8>::new();
        let text: &str = "this is a test";
        super::encode_str(&mut v, text);
        let mut pos: usize = 0;
        assert_eq!(Some(text.to_string()), super::decode_str(&v, &mut pos));
        assert_eq!(v.len(), pos);
    }

    #[test]
    fn uint() {
        let mut v = Vec::<u8>::new();
        let num: u64 = 37;
        super::encode_uint(&mut v, num);
        let mut pos: usize = 0;
        assert_eq!(Some(num), super::decode_uint(&v, &mut pos));
        assert_eq!(v.len(), pos);
    }
}
