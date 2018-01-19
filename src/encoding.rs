use util::*;

// Format for varints:  Base-128, little-endian, [1][7bit] ... [1][7bit] [0][7bit]

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
    loop {
        if *pos >= v.len() {
            return None;
        }
        if shift == 63 {
            // Overflow protection
            break;
        }

        let b: u8 = v[*pos];
        *pos += 1;
        n |= ((b & 127) as u64) << shift;
        shift += 7;
        if 0 == (b & 128) {
            return Some(n);
        }
    }

    // Any 64-bit int we encode will have this byte be 1.  We'll also tolerate
    // 0.  For a valid 64-bit varint, could also be 128 or 129, followed by a
    // bunch of leading zeros, but we prohibit that.
    let b: u8 = v[*pos];
    *pos += 1;
    if b > 1 {
        return None;
    }
    n |= (b as u64) << 63;
    return Some(n);
}

pub fn try_into_size(x: u64) -> Option<usize> {
    if x > (usize::max_value() as u64) {
        return None;
    }
    return Some(x as usize);
}

// We could use a byteorder function for these... if we wanted to.
pub fn encode_u64(v: &mut Vec<u8>, mut n: u64) {
    let mut bytes = [0u8; 8];
    for i in 0..8 {
        bytes[i] = (n & 255) as u8;
        n >>= 8;
    }
    v.extend_from_slice(&bytes);
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
    let mut bytes = [0u8; 4];
    for i in 0..4 {
        bytes[i] = (n & 255) as u8;
        n >>= 8;
    }
    v.extend_from_slice(&bytes);
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

    fn help_test_enc64(num: u64) {
        let mut v = Vec::<u8>::new();
        super::encode_uvarint(&mut v, num);
        let mut pos: usize = 0;
        assert_eq!(Some(num), super::decode_uvarint(&v, &mut pos));
        assert_eq!(v.len(), pos);

        v.clear();
        super::encode_u64(&mut v, num);
        pos = 0;
        assert_eq!(Some(num), super::decode_u64(&v, &mut pos));
        assert_eq!(v.len(), pos);
        assert_eq!(8, v.len());

        if num <= u32::max_value() as u64 {
            let num = num as u32;
            v.clear();
            super::encode_u32(&mut v, num);
            pos = 0;
            assert_eq!(Some(num), super::decode_u32(&v, &mut pos));
            assert_eq!(v.len(), pos);
            assert_eq!(4, v.len());
        }
    }

    #[test]
    fn uint() {
        help_test_enc64(0);
        help_test_enc64(37);
        help_test_enc64(127);
        help_test_enc64(128);
        help_test_enc64(137);
        help_test_enc64(12345678);
        // Doesn't hit overflow-checking case
        help_test_enc64(((1u64) << 62) | 12345678);
        // Does hit overflow-checking case
        help_test_enc64(((1u64) << 63) | 12345678);
    }
}
