use util::*;
use error::*;

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
    pub fn make(iters: Vec<Box<MutationIterator + 'a>>) -> Result<MergeIterator<'a>> {
        let mut iters_front = Vec::<Option<Buf>>::new();
        for it in iters.iter() {
            iters_front.push(it.current_key()?);
        }
        return Ok(MergeIterator{
            iters: iters,
            iters_front: iters_front,
        });
    }
}

impl<'a> MutationIterator for MergeIterator<'a> {
    fn current_key(&self) -> Result<Option<Buf>> {
        return Ok(smallest_front(&self).map(|(_, k)| k.to_vec()));
    }
    fn current_value(&self) -> Result<Option<Mutation>> {
        if let Some((i, _)) = smallest_front(&self) {
            return self.iters[i].current_value();
        } else {
            return Ok(None);
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
