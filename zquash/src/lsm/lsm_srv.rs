use std::io;

use failure::{self, format_err, Error, ResultExt};
use std::fs;
use std::io::prelude::*;
use std::iter::Peekable;
use std::path::PathBuf;

use crate::dmu_stream;
use crate::dmu_stream::RecordWithPayload;

use super::lsm;
use super::object_merge::ObjectMergeHelper;

use crate::split_tree::{self, SplitTree};

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

pub struct LSMSrvConfig {
    pub root_dir: PathBuf,
}

fn stream_dir(config: &LSMSrvConfig, name: String) -> PathBuf {
    config.root_dir.join(name)
}

fn sorted_stream_path(config: &LSMSrvConfig, name: String) -> PathBuf {
    config.root_dir.join(name).join("sorted.bin")
}

pub fn read_stream(
    config: &LSMSrvConfig,
    stream: &mut io::Read,
    name: String,
) -> Result<(), failure::Error> {
    let stream_dir = stream_dir(config, name.clone());
    if stream_dir.exists() {
        return Err(format_err!(
            "stream dir {:?} exists, db inconsistent or duplicate name",
            stream_dir
        ))?;
    }
    fs::create_dir_all(&stream_dir).context("create stream dir")?;

    let writer_tmp = stream_dir.join("writer");
    fs::create_dir(&writer_tmp).context("create writer tmp dir")?;

    let stream_sorted_out = sorted_stream_path(config, name.clone());

    use super::lsm::LSMWriter;

    let mut writer = LSMWriter::new(writer_tmp.clone(), 1 << 10, 10); // FIXME

    dmu_stream::read_with_callback(stream, |mut record| -> Result<(), Error> {
        let dmu_stream::Record {
            header: drr,
            payload_len,
            mut payload_reader,
        } = record;

        let lsm_k = LSMKey(drr);
        let mut payload = Vec::new();
        payload_reader
            .read_to_end(&mut payload)
            .context("read payload")?;
        let lsm_v = payload;
        writer.insert(lsm_k, lsm_v);

        Ok(())
    })
    .unwrap();

    writer.merge_completely(&stream_sorted_out); // FIXME

    Ok(())
}

trait LSMReaderIterTrait: Iterator<Item = (dmu_replay_record, Vec<u8>)> {}

impl<I: Iterator<Item = (dmu_replay_record, Vec<u8>)>> LSMReaderIterTrait for I {}

type LSMReaderIter = Rc<RefCell<Peekable<Box<LSMReaderIterTrait>>>>;

struct ObjectRangeIter {
    stream_idx: usize,
    stream: LSMReaderIter,
    current_objects_within_range_drained: Rc<Cell<bool>>,
}

use std::fmt;

impl fmt::Debug for ObjectRangeIter {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ObjectRangeIter")
            .field("stream_idx", &self.stream_idx)
            .finish()
    }
}

#[derive(Debug)]
struct ObjectRangeInfo {
    object_range_drr: RecordWithPayload,
    objid_space: SplitTree<RecordWithPayload>,
}

impl ObjectRangeInfo {
    fn firstobj(&self) -> u64 {
        assert!(self.object_range_drr.drr.drr_type == dmu_replay_record_DRR_OBJECT_RANGE);
        unsafe {
            self.object_range_drr
                .drr
                .drr_u
                .drr_object_range
                .drr_firstobj
        }
    }
    fn contains_id(&self, obj_id: u64) -> bool {
        assert!(self.object_range_drr.drr.drr_type == dmu_replay_record_DRR_OBJECT_RANGE);
        unsafe {
            let drr_o = &self.object_range_drr.drr.drr_u.drr_object_range;
            drr_o.drr_firstobj <= obj_id && obj_id < (drr_o.drr_firstobj + drr_o.drr_numslots)
        }
    }
}

struct ObjectsWithinObjectRangeIterator {
    stream_idx: usize,
    stream: LSMReaderIter,
    drained: Rc<Cell<bool>>,
    object_iter_drained: Rc<Cell<bool>>,
}

impl fmt::Debug for ObjectsWithinObjectRangeIterator {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ObjectsWithinObjectRangeIterator")
            .field("stream_idx", &self.stream_idx)
            .finish()
    }
}

struct ObjectIter {
    obj_id: u64,
    stream: LSMReaderIter,
    drained: Rc<Cell<bool>>,
}

impl Iterator for ObjectRangeIter {
    type Item = (ObjectRangeInfo, ObjectsWithinObjectRangeIterator);
    fn next(&mut self) -> Option<Self::Item> {
        if !self.current_objects_within_range_drained.get() {
            panic!("must drain previously returned iterator")
        }

        let mut stream = self.stream.borrow_mut();
        let (end_record, _) = stream.peek().unwrap();
        assert!(
            ObjectsWithinObjectRangeIterator::is_follow(&end_record),
            "{:?}",
            drr_debug(end_record)
        );
        assert!(
            !ObjectsWithinObjectRangeIterator::should_consume(end_record),
            "{:?}",
            drr_debug(end_record)
        );
        if Self::is_follow(end_record) {
            return None;
        }
        // INVARIANT: all follows of ObjectsWithinObjectRangeIterator::is_follow covered

        let (or_record, payload) = stream.next().expect("expecting a record");

        use itertools::Itertools;
        let object_range_drr = RecordWithPayload {
            drr: or_record,
            payload,
        };

        let object_range_info_records = stream.peeking_take_while(|(drr, _)| {
            drr.drr_type == dmu_replay_record_DRR_FREEOBJECTS
                || drr.drr_type == dmu_replay_record_DRR_OBJECT
        });

        let mut objid_space = SplitTree::new();
        for (drr, payload) in object_range_info_records {
            let (offset, length) = unsafe {
                match drr.drr_type {
                    dmu_replay_record_DRR_FREEOBJECTS => (
                        drr.drr_u.drr_freeobjects.drr_firstobj,
                        drr.drr_u.drr_freeobjects.drr_numobjs,
                    ),
                    dmu_replay_record_DRR_OBJECT => (drr.drr_u.drr_object.drr_object, 1),
                    _ => panic!("unexpected drr_type {:?}", drr_debug(drr)), // FIXME
                }
            };
            objid_space.insert(offset, length, RecordWithPayload { drr, payload });
        }

        let or_info = ObjectRangeInfo {
            object_range_drr,
            objid_space,
        };

        drop(stream);

        self.current_objects_within_range_drained = Rc::new(Cell::new(false));

        let iter = ObjectsWithinObjectRangeIterator {
            stream_idx: self.stream_idx,
            stream: self.stream.clone(),
            drained: self.current_objects_within_range_drained.clone(),
            object_iter_drained: Rc::new(Cell::new(true)),
        };
        return Some((or_info, iter));
    }
}

impl Iterator for ObjectsWithinObjectRangeIterator {
    type Item = (u64, ObjectIter);
    fn next(&mut self) -> Option<Self::Item> {
        if !self.object_iter_drained.get() {
            panic!("must drain previously returned iterator")
        }

        if self.drained.get() {
            return None;
        }

        let mut stream = self.stream.borrow_mut();

        let (next_record, _) = stream.peek().unwrap();
        if Self::is_follow(next_record) {
            self.drained.set(true);
            return None;
        }

        assert!(
            Self::should_consume(next_record)

            " unexpected record type {:?}",
            drr_debug(next_record)
        );
        let obj_id = unsafe { LSMKey(*next_record).lower_obj_id() }.unwrap(); // FIXME

        drop(stream);

        self.object_iter_drained = Rc::new(Cell::new(false));

        let object_iter = ObjectIter {
            obj_id,
            stream: self.stream.clone(),
            drained: self.object_iter_drained.clone(),
        };

        Some((obj_id, object_iter))
    }
}

impl ObjectRangeIter {
    fn is_follow(drr: &dmu_replay_record) -> bool {
        // END RECORD
        if drr.drr_type == dmu_replay_record_DRR_END {
            return true;
        }

        // Trailing FREEOBJECTS
        if LSMKey::is_trailing_freeobjects(drr) {
            return true;
        }

        return false;
    }
}

impl ObjectsWithinObjectRangeIterator {
    // returns `true` iff drr is a follow-element of an object range stream,
    // i.e. the a stream element not consumed by this iterator
    fn is_follow(drr: &dmu_replay_record) -> bool {
        // next OBJECT_RANGE
        if drr.drr_type == dmu_replay_record_DRR_OBJECT_RANGE {
            return true;
        }

        // we are nested within ObjectRangeIter
        if ObjectRangeIter::is_follow(drr) {
            return true;
        }

        return false;
    }

    fn should_consume(drr: &dmu_replay_record) -> bool {
        if drr.drr_type == dmu_replay_record_DRR_WRITE || drr.drr_type == dmu_replay_record_DRR_FREE
        {
            let _obj_id = unsafe { LSMKey(*drr).lower_obj_id() }.unwrap();
            // TODO validate that obj_id is within this iterator's object range
            return true;
        }
        return false;
    }

    fn peek_objid(&self) -> Option<u64> {
        if self.drained.get() {
            return None;
        }
        match self.stream.borrow_mut().peek() {
            Some((drr, _)) => {
                if Self::is_follow(drr) {
                    None
                } else {
                    assert!(
                        Self::should_consume(drr)
                        " unexpected record type {:?}",
                        drr_debug(drr)
                    ); // FIXME
                    Some(unsafe { LSMKey(*drr).lower_obj_id() }.unwrap())
                }
            }
            None => None,
        }
    }
}

impl Iterator for ObjectIter {
    type Item = RecordWithPayload;
    fn next(&mut self) -> Option<Self::Item> {
        if self.drained.get() {
            return None;
        }

        let mut stream = self.stream.borrow_mut();

        let (next_record, _) = stream.peek().unwrap();
        let same_objid =
            unsafe { LSMKey(*next_record).lower_obj_id() }.map_or(false, |id| self.obj_id == id);
        if !same_objid {
            // TODO need check for invalid record type here?
            self.drained.set(true);
            return None;
        }

        unsafe {
            match next_record.drr_type {
                dmu_replay_record_DRR_WRITE | dmu_replay_record_DRR_FREE => {
                    let (drr, payload) = stream.next().unwrap(); // checked above
                    Some(RecordWithPayload { drr, payload })
                }
                dmu_replay_record_DRR_OBJECT_RANGE => None,
                _ => panic!("unexpected drr_type {:?}", drr_debug(next_record)),
            }
        }
    }
}

unsafe fn consume_until_object_range_return_begin(
    stream: Rc<RefCell<Peekable<Box<dyn LSMReaderIterTrait>>>>,
) -> Option<RecordWithPayload> {
    let s = &mut *stream.borrow_mut();
    let mut begin = None;
    loop {
        let (r, pay) = s.peek().unwrap();
        if r.drr_type == dmu_replay_record_DRR_OBJECT_RANGE {
            break;
        } else if r.drr_type == dmu_replay_record_DRR_BEGIN {
            assert!(begin.is_none()); // TODO error handling
            begin = Some(RecordWithPayload {
                drr: *r,
                payload: pay.clone(),
            });
            s.next();
        } else {
            dbg!(drr_debug(r));
            s.next();
        }
    }
    return begin;
}

unsafe fn symbolic_dump_consume_lsm_reader(
    stream: Rc<RefCell<Peekable<Box<dyn LSMReaderIterTrait>>>>,
) {
    println!("suck up until OBJECT_RANGE begins");
    let begin = consume_until_object_range_return_begin(stream.clone());
    dbg!(begin);
    println!("dump ObjectRangeIter and child iterators");
    let mut iter = ObjectRangeIter {
        stream_idx: 0,
        stream: stream.clone(),
        current_objects_within_range_drained: Rc::default(),
    };
    for (or, it) in iter {
        dbg!(or);
        for (o, rec_it) in it {
            dbg!(o);
            for rec in rec_it {
                dbg!(rec);
            }
        }
    }
    println!("dump remainder of the stream");
    {
        // scope for Drop
        let s = &mut *stream.borrow_mut();
        loop {
            if let Some((r, _)) = s.next() {
                dbg!(drr_debug(r));
            } else {
                println!("end of stream");
                break;
            }
        }
    }
}

pub unsafe fn show(config: &LSMSrvConfig, loaded_stream: &str) {
    let mut r = lsm::LSMReader::<LSMKey, Vec<u8>>::open(&sorted_stream_path(
        config,
        (*loaded_stream).to_owned(),
    ));
    for (k, _) in r {
        println!("{:?}", drr_debug(&k.0));
    }
}

pub unsafe fn merge_streams(
    config: &LSMSrvConfig,
    streams_newest_to_oldest: &[&str],
    target: String,
) -> Result<(), failure::Error> {
    let mut streams: Vec<Peekable<Box<dyn LSMReaderIterTrait>>> = vec![];
    for (stream, stream_path) in streams_newest_to_oldest.iter().enumerate() {
        let x: Box<dyn Iterator<Item = (LSMKey, Vec<u8>)>> = Box::new(lsm::LSMReader::open(
            &sorted_stream_path(config, (*stream_path).to_owned()),
        ));
        // let x: Box<dyn Iterator<Item=(dmu_replay_record, Vec<u8>)>> =
        let x: Box<dyn LSMReaderIterTrait> =
            Box::new(x.map(|(LSMKey(drr), payload): (LSMKey, Vec<u8>)| (drr, payload)));
        let x = x.peekable();
        streams.push(x);
    }
    let mut streams: Vec<Rc<RefCell<Peekable<Box<dyn LSMReaderIterTrait>>>>> = streams
        .into_iter()
        .map(|b| Rc::new(RefCell::new(b)))
        .collect();

    let writer_out = sorted_stream_path(config, target);
    fs::create_dir_all(writer_out.parent().unwrap())?;
    let mut target: lsm::SortedLSMWriter<LSMKey, Vec<u8>> = lsm::SortedLSMWriter::new(&writer_out); // FIXME

    // merge BEGIN record, drop noop FREEOBJECTS(0, 0) record
    use std::collections::VecDeque;
    let mut begins_oldest_to_newest = streams
        .iter()
        .rev()
        .map(|s| consume_until_object_range_return_begin(s.clone()).unwrap())
        .collect::<VecDeque<_>>();
    assert!(streams.len() > 0);
    let begin = begins_oldest_to_newest.pop_front().unwrap();
    let begin = begins_oldest_to_newest.into_iter().fold(begin, |mut b, r| {
        unsafe {
            use const_cstr::const_cstr;
            assert_eq!(r.drr.drr_type, dmu_replay_record_DRR_BEGIN);
            assert_eq!(
                dbg!(&b).drr.drr_u.drr_begin.drr_toguid,
                dbg!(&r).drr.drr_u.drr_begin.drr_fromguid
            ); // TODO error
            let fromguid = b.drr.drr_u.drr_begin.drr_fromguid;
            let from_ivset_guid = {
                let mut from_ivset_guid: u64 = 0;
                b.drr_begin_mutate_crypt_keydata(&mut |crypt_keydata| -> Result<(), ()> {
                    nvpair_sys::nvlist_lookup_uint64(
                        crypt_keydata,
                        const_cstr!("from_ivset_guid").as_ptr(),
                        &mut from_ivset_guid,
                    );
                    Err(()) // abort mutation
                });
                from_ivset_guid
            };
            b = r;
            b.drr.drr_u.drr_begin.drr_fromguid = fromguid;
            b.drr_begin_mutate_crypt_keydata(&mut |crypt_keydata| -> Result<(), ()> {
                nvpair_sys::nvlist_add_uint64(
                    crypt_keydata,
                    const_cstr!("from_ivset_guid").as_ptr(),
                    from_ivset_guid,
                );
                Ok(())
            })
            .expect("mutate ivset guid");
            b
        }
    });
    target.insert(LSMKey(begin.drr.clone()), begin.payload);
    let drr_toguid = unsafe { begin.drr.drr_u.drr_begin.drr_toguid };

    let mut streams: Vec<_> = streams
        .into_iter()
        .enumerate()
        .map(|(stream_idx, stream)| {
            ObjectRangeIter {
                stream_idx,
                stream: stream.clone(),
                current_objects_within_range_drained: Rc::new(Cell::new(true)),
            }
            .peekable()
        })
        .collect();

    let mut next_object_range = 0; // we know every stream touches object range [0, 32)
    loop {
        assert_eq!(next_object_range % 32, 0);

        let this_range_streams = streams
            .iter_mut()
            .enumerate()
            .filter_map(|(i, s)| {
                let (ori, or_iter) = s.peek()?; // TODO
                if ori.firstobj() == next_object_range {
                    Some(i)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        println!("this_range_streams = {:?}", this_range_streams);

        if this_range_streams.len() == 0 {
            // check if there is any next range
            let min = streams
                .iter_mut()
                .enumerate()
                .filter_map(|(i, s)| Some((i, s.peek()?)))
                .min_by_key(|(i, (ori, _))| ori.firstobj());
            if let Some((min_idx, (ori, _))) = min {
                next_object_range = ori.firstobj();
                continue;
            } else {
                break;
            }
        } else {
            // advance next_object_range
            next_object_range = streams[this_range_streams[0]].peek().unwrap().0.firstobj();
        }
        assert!(is_sorted::IsSorted::is_sorted(
            &mut this_range_streams.iter()
        ));
        assert!(this_range_streams.len() > 0);

        // the highest-level (= first) OBJECT_RANGE record wins
        // and with it, all FREEOBJECTS and OBJECT records (encoded in objid_space)
        //
        // However, we still gotta merge the READ and WRITE records per object

        let this_range_streams = this_range_streams
            .into_iter()
            // consume from those streams that have the current object range
            .map(|stream_idx| streams[stream_idx].next().unwrap())
            .collect::<Vec<_>>();

        // split the (ObjectRangeInfo, ObjectsWithinObjectRange) iterators into two separate iterators
        let (oris, mut or_iters) = this_range_streams.into_iter().fold(
            (vec![], vec![]),
            |(mut oris, mut or_iters), (ori, or_iter)| {
                oris.push(ori);
                or_iters.push(or_iter);
                (oris, or_iters)
            },
        );

        let ori: &ObjectRangeInfo = &oris[0]; // most recent OBJECT_RANGE wins
        target.insert(LSMKey(ori.object_range_drr.drr), Vec::<u8>::new());
        // insert FREEOBJECTS and OBJECT records
        for (obj_id, len, occ_obj) in ori.objid_space.iter() {
            match occ_obj {
                split_tree::Occ::Unknown => (),
                split_tree::Occ::Free => {
                    let freeobjects = unsafe {
                        let mut r: dmu_replay_record = std::mem::zeroed();
                        r.drr_type = dmu_replay_record_DRR_FREEOBJECTS;
                        r.drr_payloadlen = 0;
                        r.drr_u.drr_freeobjects = dmu_replay_record__bindgen_ty_2_drr_freeobjects {
                            drr_firstobj: obj_id,
                            drr_numobjs: len,
                            drr_toguid,
                        };
                        r
                    };
                    // sw.write_record_and_payload_reader(or.unwrap(), &mut io::empty())?;
                    target.insert(LSMKey(freeobjects), vec![]);
                }
                split_tree::Occ::Occupied(obj) => {
                    target.insert(LSMKey(obj.drr), obj.payload.clone());
                }
            }
        }
        let mut next_object_id = ori.firstobj();

        while ori.contains_id(next_object_id) {
            println!("next_object_id = {:?}", next_object_id);

            let mut this_object_streams = or_iters
                .iter_mut()
                .filter_map(|or_iter| {
                    if let Some(obj_id) = or_iter.peek_objid() {
                        if obj_id == next_object_id {
                            Some(or_iter)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            println!("this_object_streams = {:?}", this_object_streams);

            let this_object_id = next_object_id;
            next_object_id += 1;
            if this_object_streams.len() == 0 {
                continue;
            }

            // merge (= consume) iterators for this object
            let mut merger: ObjectMergeHelper<DrrWrapper> =
                ObjectMergeHelper::new(this_object_streams.len());
            let object_iters =
                this_object_streams
                    .iter_mut()
                    .enumerate()
                    .map(|(level, oior_iter)| {
                        let (obj_id, obj_iter) = oior_iter.next().unwrap(); // consume object()
                        assert_eq!(obj_id, this_object_id);
                        obj_iter.map(move |drr| (drr, level))
                    });
            let unified = itertools::kmerge_by(
                object_iters,
                |(drr_a, level_a): &(RecordWithPayload, usize),
                 (drr_b, level_b): &(RecordWithPayload, usize)| {
                    let offset_ord = LSMKey(drr_a.drr)
                        .offset_in_object_if_any()
                        .cmp(&LSMKey(drr_b.drr).offset_in_object_if_any());
                    if offset_ord == Ordering::Equal {
                        level_a < level_b
                    } else {
                        offset_ord == Ordering::Less
                    }
                },
            );

            #[derive(Clone, Debug, From, Into)]
            struct DrrWrapper(RecordWithPayload);

            impl Default for DrrWrapper {
                fn default() -> Self {
                    let drr = unsafe {
                        let drr: dmu_replay_record = std::mem::zeroed();
                        drr
                    };
                    DrrWrapper(RecordWithPayload {
                        drr,
                        payload: vec![],
                    })
                }
            }
            let mut out = VecDeque::new();
            for (record, level) in unified {
                assert_eq!(
                    Some(this_object_id),
                    unsafe { LSMKey(record.drr).lower_obj_id() },
                    "unexpected object id {:?}",
                    record
                );
                if record.drr.drr_type == dmu_replay_record_DRR_WRITE {
                    let o = &record.drr.drr_u.drr_write;
                    merger.insert_write(
                        &mut out,
                        level,
                        o.drr_offset,
                        o.drr_logical_size,
                        record.into(),
                    );
                } else if record.drr.drr_type == dmu_replay_record_DRR_FREE {
                    assert_eq!(record.payload.len(), 0);
                    let o = &dbg!(record).drr.drr_u.drr_free;
                    let len = if o.drr_length == std::u64::MAX {
                        std::u64::MAX - o.drr_offset // free to end
                    } else {
                        o.drr_length
                    };
                    merger.insert_free(&mut out, level, o.drr_offset, len);
                } else {
                    panic!(" unexpected record type {:?}", record);
                }
            }
            // for each level, end it
            for level in 0..this_object_streams.len() {
                merger.insert_end(&mut out, level)
            }

            // write out object_info
            for knowledge in out {
                use super::object_merge::KnowledgeKind::{End, Free, Occupied};
                match knowledge.kind {
                    Occupied(DrrWrapper(RecordWithPayload { drr, payload })) => {
                        target.insert(LSMKey(drr), payload);
                    }
                    Free => {
                        let free = unsafe {
                            let mut r: dmu_replay_record = std::mem::zeroed();
                            r.drr_type = dmu_replay_record_DRR_FREE;
                            r.drr_payloadlen = 0;
                            r.drr_u.drr_free = dmu_replay_record__bindgen_ty_2_drr_free {
                                drr_object: this_object_id,
                                drr_offset: knowledge.from,
                                drr_length: knowledge.len,
                                drr_toguid,
                            };
                            r
                        };
                        dbg!(drr_debug(&free));
                        target.insert(LSMKey(free), Vec::new());
                    }
                    End => panic!("merger emitted End record, unsupported"),
                }
            }
        }
        // After while ori.contains_id(next_object_id)
        for stream in or_iters {
            println!("draining stream {:?}", stream);
            let stream_dbg = format!("{:?}", stream);
            for (obj_id, obj_iter) in stream {
                println!("draining object {:?}", obj_id);
                obj_iter.for_each(|x| {
                    println!(
                        "drain leftovers of object range {:?} {:?} {:?} {:?}",
                        ori,
                        stream_dbg,
                        obj_id,
                        drr_debug(&x.drr)
                    )
                });
            }
        }
    }

    // synthesize END record
    let end_drr = unsafe {
        let mut drr: dmu_replay_record = std::mem::zeroed();
        drr.drr_type = dmu_replay_record_DRR_END;
        drr.drr_payloadlen = 0;
        drr.drr_u.drr_end.drr_toguid = drr_toguid;
        drr
    };
    target.insert(LSMKey(end_drr), vec![]);

    Ok(())
}

pub unsafe fn write_stream(
    config: &LSMSrvConfig,
    name: String,
    out: &mut std::io::Write,
) -> Result<(), failure::Error> {
    let mut out = dmu_stream::StreamWriter::new(out);

    let reader: lsm::LSMReader<LSMKey, Vec<u8>> =
        lsm::LSMReader::open(&sorted_stream_path(config, name).to_owned());

    for (LSMKey(drr), payload) in reader {
        out.write_record_and_payload_reader(&drr, &mut io::Cursor::new(&payload))
            .context(format!("drr {:?}", drr_debug(drr)))?;
    }

    Ok(())
}

use bindings::*;

use serde::{Deserialize, Serialize, Serializer};

/// sort such that
/// BEGIN
/// FREEOBJECTS (firstobj=0, numobjs=0)
/// OBJECT_RANGE by firstobj / 32
///   FREEOBJECTS, OBJECT by object_id
///   FREE, WRITE by (object_id, offset_in_object)
/// FREEOBJECTS (firstobj=X, numobjs=0)
/// FREEOBJECTS (firstobj=X, numobjs=u64max-X)
/// END
#[derive(Serialize, Deserialize)]
struct LSMKey(dmu_replay_record);
use std::cmp::Ordering;

impl LSMKey {
    unsafe fn lower_obj_id(&self) -> Option<u64> {
        let u = &self.0.drr_u;
        let obj_id = match self.0.drr_type {
            dmu_replay_record_DRR_BEGIN => return None, // TOOD
            dmu_replay_record_DRR_END => return None,   // TODO
            dmu_replay_record_DRR_OBJECT_RANGE => u.drr_object_range.drr_firstobj,
            dmu_replay_record_DRR_OBJECT => u.drr_object.drr_object,
            dmu_replay_record_DRR_FREEOBJECTS => u.drr_freeobjects.drr_firstobj,
            dmu_replay_record_DRR_WRITE => u.drr_write.drr_object,
            dmu_replay_record_DRR_FREE => u.drr_free.drr_object,
            _ => unimplemented!(), // TODO
        };
        Some(obj_id)
    }

    unsafe fn offset_in_object_if_any(&self) -> Option<u64> {
        let u = &self.0.drr_u;
        match self.0.drr_type {
            dmu_replay_record_DRR_BEGIN => None,
            dmu_replay_record_DRR_END => None,
            dmu_replay_record_DRR_OBJECT_RANGE => None,
            dmu_replay_record_DRR_OBJECT => None,
            dmu_replay_record_DRR_FREEOBJECTS => None,
            dmu_replay_record_DRR_WRITE => Some(u.drr_write.drr_offset),
            dmu_replay_record_DRR_FREE => Some(u.drr_free.drr_offset),
            _ => unimplemented!(), // TODO
        }
    }

    unsafe fn length_in_object_if_any(&self) -> Option<u64> {
        let u = &self.0.drr_u;
        match self.0.drr_type {
            dmu_replay_record_DRR_BEGIN => None,
            dmu_replay_record_DRR_END => None,
            dmu_replay_record_DRR_OBJECT_RANGE => None,
            dmu_replay_record_DRR_OBJECT => None,
            dmu_replay_record_DRR_FREEOBJECTS => None,
            dmu_replay_record_DRR_WRITE => Some(u.drr_write.drr_logical_size),
            dmu_replay_record_DRR_FREE => Some(u.drr_free.drr_length),
            _ => unimplemented!(), // TODO
        }
    }

    fn is_trailing_freeobjects(drr: &dmu_replay_record) -> bool {
        if !drr.drr_type == dmu_replay_record_DRR_FREEOBJECTS {
            return false;
        }
        unsafe {
            let (firstobj, numobjs) = unsafe {
                let fos = drr.drr_u.drr_freeobjects;
                (fos.drr_firstobj, fos.drr_numobjs)
            };
            if firstobj == 0 && numobjs == 0 {
                return true;
            }
            if numobjs == std::u64::MAX - firstobj {
                return true;
            }
        }
        return false;
    }
}

impl PartialEq for LSMKey {
    fn eq(&self, o: &LSMKey) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}

impl Eq for LSMKey {}

impl PartialOrd for LSMKey {
    fn partial_cmp(&self, other: &LSMKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LSMKey {
    fn cmp(&self, o: &LSMKey) -> Ordering {
        unsafe {
            let diff_it = || {
                format!(
                    "{}",
                    difference::Changeset::new(
                        &format!("{:#?}", drr_debug(&self.0)),
                        &format!("{:#?}", drr_debug(&o.0)),
                        "\n"
                    )
                )
            };

            const begin_end_type_ordering: &'_ [(usize, dmu_replay_record__bindgen_ty_1)] = &[
                (0, dmu_replay_record_DRR_BEGIN),
                (1, dmu_replay_record_DRR_OBJECT_RANGE),
                (1, dmu_replay_record_DRR_FREEOBJECTS),
                (1, dmu_replay_record_DRR_OBJECT),
                (1, dmu_replay_record_DRR_FREE),
                (1, dmu_replay_record_DRR_WRITE),
                (2, dmu_replay_record_DRR_END),
            ];
            let type_ordering_find_prio =
                |req_ty, type_ordering: &[(usize, dmu_replay_record__bindgen_ty_1)]| {
                    for (prio, ty) in type_ordering {
                        if *ty == req_ty {
                            return Some(*prio);
                        }
                    }
                    return None;
                };

            let s_ty_prio =
                type_ordering_find_prio(self.0.drr_type, begin_end_type_ordering).unwrap(); // FIXME
            let o_ty_prio = type_ordering_find_prio(o.0.drr_type, begin_end_type_ordering).unwrap(); // FIXME
            if s_ty_prio.cmp(&o_ty_prio) != Ordering::Equal {
                return s_ty_prio.cmp(&o_ty_prio);
            }

            // handle all special cases that do not fit into OBJECT_RANGE
            let order_for_records_after_last_object_range_and_its_frees_and_writes =
                |drr: &dmu_replay_record| {
                    if drr.drr_type == dmu_replay_record_DRR_FREEOBJECTS {
                        let (firstobj, numobj) = {
                            let fos = drr.drr_u.drr_freeobjects;
                            (fos.drr_firstobj, fos.drr_numobjs)
                        };
                        // FREEOBJECTS(firstobj=X, numobjs=0)
                        //  <
                        // FREEOBJECTS (firstobj=X, numobjs=u64max-X)
                        if (numobj == 0) {
                            (1, 0)
                        } else if (numobj == std::u64::MAX - firstobj) {
                            (1, numobj)
                        } else {
                            (0, 0) // indeterminate
                        }
                    } else if drr.drr_type == dmu_replay_record_DRR_END {
                        (2, 0)
                    } else {
                        (0, 0) // indeterminate
                    }
                };
            {
                let cmp =
                    order_for_records_after_last_object_range_and_its_frees_and_writes(&self.0)
                        .cmp(
                            &order_for_records_after_last_object_range_and_its_frees_and_writes(
                                &o.0,
                            ),
                        );
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }

            let self_lobjid = self
                .lower_obj_id()
                .expect("expected record type to have object id");
            let other_lobjid = o
                .lower_obj_id()
                .expect("expected record type to have object id");

            // group all records by OBJECT_RANGE
            let self_or = (self_lobjid / 32);
            let othr_or = (other_lobjid / 32);
            if self_or.cmp(&othr_or) != Ordering::Equal {
                return self_or.cmp(&othr_or);
            }

            // within an OBJECT_RANGE order by the following cohorts:
            //       1                  2                 3
            // OBJECT_RANGE < (FREEOBJECTS,OBJECT) < (WRITE,FREE)
            const by_object_range_ordering: &'_ [(usize, dmu_replay_record__bindgen_ty_1)] = &[
                (1, dmu_replay_record_DRR_OBJECT_RANGE),
                (2, dmu_replay_record_DRR_FREEOBJECTS),
                (2, dmu_replay_record_DRR_OBJECT),
                (3, dmu_replay_record_DRR_FREE),
                (3, dmu_replay_record_DRR_WRITE),
            ];
            let s_ty_prio =
                type_ordering_find_prio(self.0.drr_type, by_object_range_ordering).unwrap(); // FIXME
            let o_ty_prio =
                type_ordering_find_prio(o.0.drr_type, by_object_range_ordering).unwrap(); // FIXME
            if s_ty_prio.cmp(&o_ty_prio) != Ordering::Equal {
                return s_ty_prio.cmp(&o_ty_prio);
            }

            // within each cohort, it is sufficient to order by firstobj
            if self_lobjid.cmp(&other_lobjid) != Ordering::Equal {
                return self_lobjid.cmp(&other_lobjid);
            }

            // within cohort 3, order by offset, then FREE < WRITE, then length
            assert_eq!(s_ty_prio, o_ty_prio, "equality, see above");
            if s_ty_prio == 3 {
                let s_offset = self
                    .offset_in_object_if_any()
                    .ok_or_else(|| {
                        format_err!("record type must have offset in object:\n{}", diff_it())
                    })
                    .unwrap();
                let o_offset = o
                    .offset_in_object_if_any()
                    .ok_or_else(|| {
                        format_err!("record type must have offset in object:\n{}", diff_it())
                    })
                    .unwrap();

                if s_offset.cmp(&o_offset) != Ordering::Equal {
                    return s_offset.cmp(&o_offset);
                }

                // within cohort 3: if offset is same, FREE < WRITE
                const cohort_3_type_ordering: &'_ [(usize, dmu_replay_record__bindgen_ty_1)] = &[
                    (1, dmu_replay_record_DRR_FREE),
                    (2, dmu_replay_record_DRR_WRITE),
                ];
                let s_ty_prio =
                    type_ordering_find_prio(self.0.drr_type, cohort_3_type_ordering).unwrap(); // FIXME
                let o_ty_prio =
                    type_ordering_find_prio(o.0.drr_type, cohort_3_type_ordering).unwrap(); // FIXME
                if s_ty_prio.cmp(&o_ty_prio) != Ordering::Equal {
                    return s_ty_prio.cmp(&o_ty_prio);
                }

                // same offset and same type (FREE / WRITE): let the length win
                let s_length = self
                    .length_in_object_if_any()
                    .ok_or_else(|| {
                        format_err!("record type must have length in object:\n{}", diff_it())
                    })
                    .unwrap();
                let o_length = o
                    .length_in_object_if_any()
                    .ok_or_else(|| {
                        format_err!("record type must have length in object:\n{}", diff_it())
                    })
                    .unwrap();

                if s_length.cmp(&o_length) != Ordering::Equal {
                    return s_length.cmp(&o_length);
                }

                // FALLTHROUGH
            }

            // we have no more discriminators (that we know of at the time of writing)
            // => if the records are not equal, the stream is considered invalid
            if format!("{:?}", drr_debug(&self.0)) == format!("{:?}", drr_debug(&o.0)) {
                // FIXME 111!!!!
                return Ordering::Equal;
            } else {
                panic!("unexpected record:\n{}", diff_it());
            }
        }
    }
}
