use std::collections::VecDeque;
use std::fmt;

#[derive(Debug, Clone)]
pub enum KnowledgeKind<D: Clone + fmt::Debug> {
    Free,
    Occupied(D),
    End,
}

impl<D: Clone + fmt::Debug> KnowledgeKind<D> {
    fn is_end(&self) -> bool {
        if let KnowledgeKind::End = self {
            true
        } else {
            false
        }
    }
}

impl Copy for KnowledgeKind<()> {}

#[derive(Debug, Clone)]
pub struct Knowledge<D: fmt::Debug + Clone> {
    pub from: u64,
    pub len: u64,
    pub kind: KnowledgeKind<D>,
}

impl<D: Clone + fmt::Debug> KnowledgeKind<D> {
    fn without_data(&self) -> KnowledgeKind<()> {
        match self {
            KnowledgeKind::End => KnowledgeKind::End,
            KnowledgeKind::Free => KnowledgeKind::Free,
            KnowledgeKind::Occupied(_) => KnowledgeKind::Occupied(()),
        }
    }
}

impl Copy for Knowledge<()> {}

impl<D: Clone + fmt::Debug> Knowledge<D> {
    fn without_data(&self) -> Knowledge<()> {
        Knowledge {
            from: self.from,
            len: self.len,
            kind: self.kind.without_data(),
        }
    }
}

impl PartialEq for KnowledgeKind<()> {
    fn eq(&self, other: &Self) -> bool {
        use KnowledgeKind::*;
        match (self, other) {
            (Free, Free) | (End, End) | (Occupied(()), Occupied(())) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
enum TrimResult<D: fmt::Debug> {
    Noop,
    Trimmed(D),
}

impl<D: fmt::Debug> TrimResult<D> {
    fn into_trimmed(self) -> Option<D> {
        match self {
            TrimResult::Noop => None,
            TrimResult::Trimmed(d) => Some(d),
        }
    }
}

impl<D: Clone + Default + fmt::Debug> Knowledge<D> {
    // Returns what was trimmed-off knowledge if the kind of knowledge can be trimmed (=split)
    // or an error if it cannot be trimmed.
    fn try_trim_front(&mut self, new_front: u64) -> Result<TrimResult<Knowledge<D>>, ()> {
        if new_front <= self.from {
            return Ok(TrimResult::Noop);
        }
        assert!(self.from <= new_front);
        assert!((new_front - self.from) <= self.len);
        match &mut self.kind {
            KnowledgeKind::Free | KnowledgeKind::End => {
                let orig_front = self.from;
                let trimmed_off_len = new_front - self.from;
                self.len -= trimmed_off_len;
                self.from = new_front;
                Ok(TrimResult::Trimmed(Knowledge {
                    from: orig_front,
                    len: trimmed_off_len,
                    kind: self.kind.clone(), // cheap because this kind doesn't have data
                }))
            }
            KnowledgeKind::Occupied(d) => {
                if new_front == self.from + self.len {
                    let mut orig_d = D::default();
                    std::mem::swap(d, &mut orig_d);
                    let k = Knowledge {
                        from: self.from,
                        len: self.len,
                        kind: KnowledgeKind::Occupied(orig_d),
                    };
                    self.len = 0;
                    self.from = new_front;
                    Ok(TrimResult::Trimmed(k))
                } else {
                    Err(())
                }
            }
        }

    }
}

pub struct ObjectMergeHelper<D: fmt::Debug + Clone> {
    emit_from: u64,
    level_last_insert: Vec<Option<Knowledge<()>>>,
    knowledge: Vec<VecDeque<Knowledge<D>>>,
}

impl<D> ObjectMergeHelper<D>
where
    D: Clone + Default + fmt::Debug,
{
    pub fn new(num_levels: usize) -> Self {
        ObjectMergeHelper {
            emit_from: 0,
            level_last_insert: vec![None; num_levels],
            knowledge: vec![VecDeque::new(); num_levels],
        }
    }

    fn find_action(&mut self, out: &mut VecDeque<Knowledge<D>>) -> Option<()> {
        for deq in &self.knowledge {
            if deq.is_empty() {
                return None;
            }
        }
        // all deqs have at least one element

        dbg!(&self.knowledge);
        use itertools::Itertools;
        let (min_offset, min_end) = {
            let offsets = self
                .knowledge
                .iter()
                .filter_map(|deq| deq.front())
                .flat_map(|k| vec![k.from, k.from + k.len])
                .sorted()
                .dedup()
                .take(2)
                .collect::<Vec<_>>();
            assert!(
                offsets.len() == 2 || (offsets.len() == 1 && offsets[0] == std::u64::MAX),
                "{:?}",
                offsets
            );
            if offsets.len() == 1 {
                return None;
            }
            (offsets[0], offsets[1])
        };

        if min_end - min_offset == 0 {
            return None;
        }

        // determine what to emit for this range
        // and trim all unrequired knowledge
        // note that data cannot be trimmed, but FREE can
        let emit_range = dbg!((min_offset, min_end));

        let knowledge_len = self.knowledge.len();
        let mut knowledge_in_emit_range = self
            .knowledge
            .iter_mut()
            .flat_map(|(deq)| {
                deq.iter_mut().filter_map(move |k: &mut Knowledge<_>| {
                    // range contains begin, but not necessarily end
                    if k.from >= min_offset {
                        Some(k)
                    } else {
                        None
                    }
                })
            })
            .collect::<Vec<_>>();
        assert!(knowledge_in_emit_range.len() > 0);

        // trim all to the end of emit range, and determine the winner
        let winner_candidates = dbg!(knowledge_in_emit_range)
            .iter_mut()
            // NOTE: this map has a side-effect
            .map(|k| {
                dbg!(dbg!(k).try_trim_front(min_end).unwrap()) // TODO nicer errors
            })
            .filter_map(|trim_res| trim_res.into_trimmed())
            .collect::<Vec<_>>();

        // the winner is the highest level that is not a Knowledge::End sentinel

        let winner = winner_candidates
            .into_iter()
            .filter(|k| !k.kind.is_end())
            .next();
        let winner = match winner {
            Some(w) => w,
            None => return Some(()),
        };

        // cleanup deq
        self.knowledge.iter_mut().for_each(|deq| {
            if deq.len() > 0 {
                if deq[0].len == 0 {
                    deq.pop_front();
                } else if deq[0].from <= min_end {
                    assert!(deq[0].from >= min_end);
                }
            }
        });

        // check invariant: we have dropped all knowledge before the end of emit_range
        self.knowledge.iter().enumerate().for_each(|(level, deq)| {
            debug_assert!(
                deq.iter().all(|k| (k.from + k.len) >= min_end),
                "{:?}",
                level
            );
            debug_assert!(
                deq.len() <= 1,
                "level = {:?} deq.len() = {:?}\ndeq = {:#?}",
                level,
                deq.len(),
                deq
            );
        });

        // emit winner
        assert!(
            self.emit_from <= winner.from,
            "impl error: winner must have offset >= already emitted\n{:?}\n{:?}",
            self.emit_from,
            winner
        );
        self.emit_from = winner.from + winner.len;
        out.push_back(dbg!(winner).clone());

        Some(())

    }

    fn repeat_find_action(&mut self, out: &mut VecDeque<Knowledge<D>>) {
        loop {
            if self.find_action(out).is_none() {
                break;
            }
        }
    }

    fn check_track_and_normalize_insert_at_level(
        &mut self,
        level: usize,
        candidate: &mut Knowledge<D>,
    ) {
        let mut this_insert = candidate.without_data();

        assert!(
            this_insert.from.checked_add(this_insert.len).is_some(),
            "level=({:?}) + offset({:?}) exceed u64 space",
            level,
            this_insert.from
        );
        assert!(level < self.knowledge.len(), "level number out of bounds");

        let last_insert = &self.level_last_insert[level];
        let last_insert = match last_insert {
            Some(i) => i,
            None => {
                self.level_last_insert[level] = Some(this_insert);
                return;
            }
        };

        if last_insert.from + last_insert.len > this_insert.from {
            // Due to the traversal algorithm used by ZFS (dmu_traverse.c), it is possible that overlapping FREE
            // records are emitted (the following excerpt from zstreamdump has omissions, but remains in original order)
            //
            //   OBJECT object = 1 type = 21 bonustype = 0 blksz = 512 bonuslen = 0 dn_slots = 1 raw_bonuslen = 0 flags = 0 maxblkid = 0 indblkshift = 17 nlevels = 1 nblkptr = 3
            //   FREE object = 1 offset = 512 length = -1
            //   WRITE object = 1 type = 21 checksum type = 7 compression type = 2
            //       flags = 0 offset = 0 logical_size = 512 compressed_size = 512 payload_size = 512 props = 8200000000 salt = 0000000000000000 iv = 000000000000000000000000 mac = c3d24b5f100f4c236567115ccdfcc17a
            //   FREE object = 1 offset = 512 length = 1024
            //
            // That second FREE is the effect of only the first of 3 blktpr_t being used to represent the contents of object 1
            // i.e. object 1 consists only of 1 x blksz = 1 x 512 byte in this case
            // the dmu_traverse.c code visits the other 2 blkptr_t, though, and issues FREE
            //
            // LSMKey discriminates FREE records first by offset, then by their length, ascending.
            // Hence, we will observe the second FREE before the first FREE when merging.
            let is_free_to_end_overlapping_only_with_last_free = last_insert.kind
                == KnowledgeKind::Free
                && this_insert.kind == KnowledgeKind::Free
                && this_insert.from == last_insert.from;
            if is_free_to_end_overlapping_only_with_last_free {
                // ok, see comment above
                // just patch up its range
                let new_from = last_insert.from + last_insert.len;
                assert!(new_from > this_insert.from);
                let subtracted_len = new_from - this_insert.from;
                candidate.from = new_from;
                candidate.len -= subtracted_len;
                this_insert = candidate.without_data();
            } else {
                panic!(
                    "level already observed insert {:?}, cannot insert before it {:?}",
                    last_insert, this_insert
                );
            }
        }

        self.level_last_insert[level] = Some(this_insert);
    }

    pub fn insert_free(
        &mut self,
        out: &mut VecDeque<Knowledge<D>>,
        level: usize,
        from: u64,
        len: u64,
    ) {
        let mut k = Knowledge {
            from,
            len,
            kind: KnowledgeKind::Free,
        };
        self.check_track_and_normalize_insert_at_level(level, &mut k);
        self.knowledge[level].push_back(k);
        self.repeat_find_action(out)
    }

    pub fn insert_write(
        &mut self,
        out: &mut VecDeque<Knowledge<D>>,
        level: usize,
        from: u64,
        len: u64,
        data: D,
    ) {
        let mut k = Knowledge {
            from,
            len,
            kind: KnowledgeKind::Occupied(data),
        };
        self.check_track_and_normalize_insert_at_level(level, &mut k);
        self.knowledge[level].push_back(k);
        self.repeat_find_action(out)
    }

    pub fn insert_end(&mut self, out: &mut VecDeque<Knowledge<D>>, level: usize) {
        let from = std::cmp::max(
            self.level_last_insert[level]
                .map(|i| i.from + i.len)
                .unwrap_or(0),
            self.knowledge[level]
                .iter()
                .map(|k| k.from + k.len)
                .max()
                .unwrap_or(0),
        );
        let len = std::u64::MAX - from;
        let mut k = Knowledge {
            from,
            len,
            kind: KnowledgeKind::End,
        };
        self.check_track_and_normalize_insert_at_level(level, &mut k);
        self.knowledge[level].push_back(k);
        self.repeat_find_action(out)
    }
}

#[cfg(test)]
mod helper_tests {

    use super::*;
    #[test]
    fn basics_2_levels() {
        let mut h: ObjectMergeHelper<usize> = ObjectMergeHelper::new(2);
        let mut out: VecDeque<Knowledge<usize>> = VecDeque::new();
        h.insert_write(&mut out, 0, 2, 3, 23);
        h.insert_free(&mut out, 1, 0, 10);
        h.insert_end(&mut out, 0);
        h.insert_end(&mut out, 1);
        let out = Vec::from(out);
        assert_eq!(
            out,
            vec![
                Knowledge {
                    from: 0,
                    len: 2,
                    kind: KnowledgeKind::Free
                },
                Knowledge {
                    from: 2,
                    len: 3,
                    kind: KnowledgeKind::Occupied(23 as usize),
                },
                Knowledge {
                    from: 5,
                    len: 5,
                    kind: KnowledgeKind::Free
                },
            ]
        );
    }

    #[test]
    fn gaps() {
        let mut h = ObjectMergeHelper::new(2);
        let mut out = VecDeque::new();

        h.insert_write(&mut out, 0, 0, 2, 23);
        h.insert_free(&mut out, 1, 3, 1); // gap at [2,3)
        h.insert_write(&mut out, 0, 5, 2, 42); // gap at [4,5)

        h.insert_end(&mut out, 0);
        h.insert_end(&mut out, 1);
        let out = Vec::from(out);

        assert_eq!(
            out,
            vec![
                Knowledge {
                    from: 0,
                    len: 2,
                    kind: KnowledgeKind::Occupied(23 as usize),
                },
                Knowledge {
                    from: 3,
                    len: 1,
                    kind: KnowledgeKind::Free,
                },
                Knowledge {
                    from: 5,
                    len: 2,
                    kind: KnowledgeKind::Occupied(42),
                }
            ]
        );
    }

}