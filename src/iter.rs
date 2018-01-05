use error::*;
use util::*;

// NOTE: Avoid copying out whole String for key
pub trait MutationIterator {
    fn current_key(&mut self) -> Result<Option<Buf>>;
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
            iters_front.push(it.current_key()?);
        }
        return Ok(MergeIterator{
            iters: iters,
            iters_front: iters_front,
        });
    }
}

impl<'a> MutationIterator for MergeIterator<'a> {
    fn current_key(&mut self) -> Result<Option<Buf>> {
        return Ok(smallest_front(&self).map(|(_, k)| k.to_vec()));
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
                self.iters_front[i] = self.iters[i].current_key()?;
            }
        }
        return Ok(());
    }
}

// NOTE: Hard-code table iterator here?
pub struct ConcatIterator<'a> {
    current: Box<MutationIterator + 'a>,
    next_gen: Box<FnMut() -> Option<Box<MutationIterator + 'a>> + 'a>,
}

impl<'a> ConcatIterator<'a> {
    pub fn make(mut next_gen: Box<FnMut() -> Option<Box<MutationIterator + 'a>> + 'a>) -> ConcatIterator<'a> {
        if let Some(current) = (*next_gen)() {
            return ConcatIterator{current: current, next_gen: next_gen};
        }

        // Just use some convenient empty iterator.
        return ConcatIterator{
            current: Box::new(EmptyIterator{}),
            next_gen: Box::new(|| None),
        };
    }
}

impl<'a> MutationIterator for ConcatIterator<'a> {
    fn current_key(&mut self) -> Result<Option<Buf>> {
        loop {
            if let Some(key) = self.current.current_key()? {
                return Ok(Some(key));
            }
            if let Some(iter) = (*self.next_gen)() {
                self.current = iter;
            } else {
                return Ok(None);
            }
        }
    }
    fn current_value(&mut self) -> Result<Mutation> {
        loop {
            // NOTE: Another gross cloning of the key
            if let Some(_) = self.current.current_key()? {
                return self.current.current_value();
            }
            if let Some(iter) = (*self.next_gen)() {
                self.current = iter;
            } else {
                return Err(Box::new(RihError::new("current_value called on empty ConcatIterator")));
            }
        }
    }
    fn step(&mut self) -> Result<()> {
        loop {
            if let Some(_) = self.current.current_key()? {
                return self.current.step();
            }
            if let Some(iter) = (*self.next_gen)() {
                self.current = iter;
            } else {
                return Err(Box::new(RihError::new("step called on empty ConcatIterator")));
            }
        }
    }
}

struct EmptyIterator { }

impl MutationIterator for EmptyIterator {
    fn current_key(&mut self) -> Result<Option<Buf>> { Ok(None) }
    fn current_value(&mut self) -> Result<Mutation> {
        return Err(Box::new(RihError::new("EmptyIterator current_value")));
    }
    fn step(&mut self) -> Result<()> {
        return Err(Box::new(RihError::new("EmptyIterator step")));
    }
}