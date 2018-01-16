use error::*;
use util::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward, Backward
}

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
    direction: Direction,
}

fn frontmost_front<'a>(iter: &'a MergeIterator) -> Option<(usize, &'a [u8])> {
    let ixkeys = iter.iters_front.iter().enumerate()
        .filter_map(|(i, opt_key)| opt_key.as_ref().map(|k: &'a Vec<u8>| (i, k.as_ref())));
    if let Direction::Forward = iter.direction {
        return ixkeys.min_by_key(|&(_, k)| k);
    } else {
        // We want the first maximal element to be returned, not the last.  So we add a tie breaker.
        // (min_by_key returns the first, so we didn't need a tie breaker for that case).
        let n: usize = iter.iters_front.len();
        return ixkeys.max_by_key(|&(i, k)| (k, n - i));
    }
}

impl<'a> MergeIterator<'a> {
    pub fn make(mut iters: Vec<Box<MutationIterator + 'a>>, direction: Direction) -> Result<MergeIterator<'a>> {
        let mut iters_front = Vec::<Option<Buf>>::new();
        for it in iters.iter_mut() {
            iters_front.push(it.current_key()?.map(|x| {
                x.to_vec()
            }));
        }
        return Ok(MergeIterator{
            iters: iters,
            iters_front: iters_front,
            direction: direction,
        });
    }
}

impl<'a> MutationIterator for MergeIterator<'a> {
    fn current_key(&self) -> Result<Option<&[u8]>> {
        let ret = Ok(frontmost_front(&self).map(|(_, k)| k));
        return ret;
    }
    fn current_value(&mut self) -> Result<Mutation> {
        if let Some((i, _)) = frontmost_front(&self) {
            return self.iters[i].current_value();
        } else {
            return mk_err("current_value called on empty MutationIterator");
        }
    }
    fn step(&mut self) -> Result<()> {
        let frontmost: Buf = {
            let (_, key) = frontmost_front(&self).or_err("step MergeIterator too far")?;
            key.to_vec()  // NOTE: Sigh on the copying.  _Move_ it out of iters_front.
        };
        for i in 0..self.iters.len() {
            if self.iters_front[i].as_ref() == Some(&frontmost) {
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
        let ret = Ok(self.current.as_ref().map(|&(ref key, _)| key as &[u8]));
        return ret;
    }
    fn current_value(&mut self) -> Result<Mutation> {
        if let Some(&mut (_, ref mut iter)) = self.current.as_mut() {
            return iter.current_value();
        } else {
            return mk_err("current_value called on empty ConcatIterator");
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
            return mk_err("step called on empty ConcatIterator");
        }
        self.current = None;
        return Ok(());
    }
}
