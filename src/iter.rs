use error::*;
use util::*;

// NOTE: Avoid copying out whole String for key
pub trait MutationIterator {
    fn current_key(&self) -> Result<Option<&[u8]>>;
    fn current_value(&mut self) -> Result<Mutation>;
    fn step(&mut self) -> Result<()>;
}

pub struct MergeIterator<'a> {
    // iters and iters_front are parallel arrays.
    iters: Vec<Box<MutationIterator + 'a>>,
    // NOTE: This could be a priority queue.
    iters_front: Vec<Option<Buf>>,
}

fn smallest_front<'a>(iter: &'a MergeIterator) -> Option<(usize, &'a [u8])> {
    return iter.iters_front.iter().enumerate()
        .filter_map(|(i, opt_key)| opt_key.as_ref().map(|k: &'a Vec<u8>| (i, k.as_ref())))
        .min_by_key(|&(_, k)| k);
}

impl<'a> MergeIterator<'a> {
    pub fn make(mut iters: Vec<Box<MutationIterator + 'a>>) -> Result<MergeIterator<'a>> {
        let mut iters_front = Vec::<Option<Buf>>::new();
        for it in iters.iter_mut() {
            iters_front.push(it.current_key()?.map(|x| x.to_vec()));
        }
        return Ok(MergeIterator{
            iters: iters,
            iters_front: iters_front,
        });
    }
}

impl<'a> MutationIterator for MergeIterator<'a> {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        return Ok(smallest_front(&self).map(|(_, k)| k));
    }
    fn current_value(&mut self) -> Result<Mutation> {
        if let Some((i, _)) = smallest_front(&self) {
            return self.iters[i].current_value();
        } else {
            return Err(Box::new(RihError::new("current_value called on empty MutationIterator")));
        }
    }
    fn step(&mut self) -> Result<()> {
        let smallest: Buf = {
            let (_, key) = smallest_front(&self).or_err("step MergeIterator too far")?;
            key.to_vec()  // NOTE: Sigh on the copying.  _Move_ it out of iters_front.
        };
        for i in 0..self.iters.len() {
            if self.iters_front[i].as_ref() == Some(&smallest) {
                self.iters[i].step()?;
                self.iters_front[i] = self.iters[i].current_key()?.map(|x| x.to_vec());
            }
        }
        return Ok(());
    }
}

// NOTE: Hard-code table iterator here?
pub struct ConcatIterator<'a> {
    // (Current key, current iterator)
    current: Option<(Buf, Box<MutationIterator>)>,
    next_gen: Box<FnMut() -> Option<Box<MutationIterator>> + 'a>,
}

impl<'a> ConcatIterator<'a> {
    pub fn make(mut next_gen: Box<FnMut() -> Option<Box<MutationIterator>> + 'a>) -> Result<ConcatIterator<'a>> {
        loop {
            if let Some(current) = (*next_gen)() {
                if let Some(key) = current.current_key()?.map(|x| x.to_vec()) {
                    return Ok(ConcatIterator{current: Some((key, current)), next_gen: next_gen});
                }
                continue;
            } else {
                return Ok(ConcatIterator{
                    current: None,
                    next_gen: next_gen,
                });
            }
        }
    }
}

impl<'a> MutationIterator for ConcatIterator<'a> {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        return Ok(self.current.as_ref().map(|&(ref key, _)| key as &[u8]));
    }
    fn current_value(&mut self) -> Result<Mutation> {
        if let Some(&mut (_, ref mut iter)) = self.current.as_mut() {
            return iter.current_value();
        } else {
            return Err(Box::new(RihError::new("current_value called on empty ConcatIterator")));
        }
    }
    fn step(&mut self) -> Result<()> {
        if let Some(tup) = self.current.as_mut() {
            tup.1.step()?;
            loop {
                if let Some(k) = tup.1.current_key()?.map(|x| x.to_vec()) {
                    tup.0 = k;
                    return Ok(());
                } else {
                    if let Some(iter) = (*self.next_gen)() {
                        tup.1 = iter;
                    } else {
                        break;
                    }
                }
            }
        } else {
            return Err(Box::new(RihError::new("step called on empty ConcatIterator")));
        }
        self.current = None;
        return Ok(());
    }
}
