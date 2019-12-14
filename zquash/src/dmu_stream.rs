use bindings::*;
use failure::{Error, ResultExt};
use std::io;
use std::io::{BufReader, Read};

use crate::fletcher4::Fletcher4;

pub trait ReplayRecordExt {
    fn bytes(&self) -> &[u8];
    fn bytes_until_checksum(&self) -> &[u8];
    fn checksum(&self) -> &[u64; 4];
    fn checksum_bytes(&self) -> &[u8];
}

impl ReplayRecordExt for dmu_replay_record {
    #[inline]
    fn bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const _ as *const u8, std::mem::size_of_val(&*self))
        }
    }
    #[inline]
    fn bytes_until_checksum(&self) -> &[u8] {
        // checksum is at the end of DRR
        unsafe {
            std::slice::from_raw_parts(
                self as *const _ as *const u8,
                std::mem::size_of_val(&*self)
                    - std::mem::size_of_val(&self.drr_u.drr_checksum.drr_checksum),
            )
        }
    }
    #[inline]
    fn checksum(&self) -> &[u64; 4] {
        assert!(self.drr_type != dmu_replay_record_DRR_BEGIN);
        unsafe { &self.drr_u.drr_checksum.drr_checksum.zc_word }
    }
    #[inline]
    fn checksum_bytes(&self) -> &[u8] {
        assert!(self.drr_type != dmu_replay_record_DRR_BEGIN);
        unsafe {
            std::slice::from_raw_parts(
                &self.drr_u.drr_checksum.drr_checksum.zc_word as *const _ as *const u8,
                std::mem::size_of_val(&self.drr_u.drr_checksum.drr_checksum.zc_word),
            )
        }
    }
}


#[derive(Clone)]
pub struct RecordWithPayload {
    pub drr: dmu_replay_record,
    pub payload: Vec<u8>,
}

use std::fmt;

impl fmt::Debug for RecordWithPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordWithPayload")
            .field("drr", &drr_debug(&self.drr))
            .finish()
    }
}

pub struct Record<'r> {
    pub header: dmu_replay_record,
    pub payload_len: u64,
    pub payload_reader: &'r mut io::Read,
}

fn drain(buf: &mut [u8], r: &mut io::Read) -> Result<(), Error> {
    loop {
        match r.read(buf) {
            Ok(0) => return Ok(()),
            Ok(_) => (),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => failure::bail!(e),
        }
    }
}

pub fn read_with_callback<C>(input: &mut io::Read, mut cb: C) -> Result<(), Error>
where
    C: FnMut(Record<'_>) -> Result<(), Error>,
{
    let mut input = BufReader::new(input);
    let mut drain_buf = vec![0 as u8; 1 << 15];
    let mut header: dmu_replay_record = unsafe { std::mem::zeroed() };
    loop {
        unsafe {
            let header_slice = std::slice::from_raw_parts_mut(
                &mut header as *mut dmu_replay_record as *mut u8,
                std::mem::size_of::<dmu_replay_record>(),
            );
            input
                .read_exact(&mut header_slice[..])
                .context("read header")?;
        }
        let payload_len = unsafe { drr_effective_payload_len(&header as *const _) } as u64;
        let mut payload_reader = input.by_ref().take(payload_len);
        let r = Record {
            header,
            payload_len,
            payload_reader: &mut payload_reader,
        };
        cb(r)?;
        // always drain the reader
        drain(&mut drain_buf, &mut payload_reader).context("drain remaining payload")?;
        if header.drr_type == dmu_replay_record_DRR_END {
            break;
        }
    }
    Ok(())
}

struct RecordPayloadWriter<'f, W> {
    fletcher: &'f mut Fletcher4,
    w: W,
    written: usize,
}

impl<A> Drop for RecordPayloadWriter<'_, A> {
    fn drop(&mut self) {
        assert!(self.written % 4 == 0);
    }
}

impl<W> io::Write for RecordPayloadWriter<'_, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.w.write(buf)?;
        self.written += written;
        self.fletcher.feed_bytes(&buf[0..written]);
        Ok((written))
    }
    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

pub struct StreamWriter<W> {
    checksum: Fletcher4,
    expect_begin: bool,
    seen_end: bool,
    out: W,
}

use std::io::Write;

impl<W: Write> StreamWriter<W> {
    pub fn new(out: W) -> StreamWriter<W> {
        StreamWriter {
            checksum: Fletcher4::new(),
            expect_begin: true,
            seen_end: false,
            out,
        }
    }

    pub fn write_record_and_payload_reader<P: Read>(
        &mut self,
        header: &dmu_replay_record,
        payload: &mut P,
    ) -> Result<(), Error> {
        let mut header: dmu_replay_record = *header; // copy
        if header.drr_type == dmu_replay_record_DRR_END {
            self.seen_end = true;
            unsafe {
                header.drr_u.drr_end.drr_checksum.zc_word = self.checksum.checksum();
            }
        }
        if self.expect_begin {
            assert!(!self.seen_end);
            assert_eq!(header.drr_type, dmu_replay_record_DRR_BEGIN);
            self.expect_begin = false;
            self.checksum.feed_bytes(header.bytes());
        } else {
            self.checksum.feed_bytes(header.bytes_until_checksum());
            unsafe {
                header.drr_u.drr_checksum.drr_checksum.zc_word = self.checksum.checksum();
            }
            self.checksum.feed_bytes(header.checksum_bytes());
        }
        self.out.write_all(header.bytes()).context("write header")?;
        let mut w = RecordPayloadWriter {
            fletcher: &mut self.checksum,
            w: &mut self.out,
            written: 0,
        };
        let n = io::copy(payload, &mut w)?;
        assert!(n % 4 == 0);
        Ok(())
    }
}
