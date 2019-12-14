use std::io;
use std::io::prelude::*;

#[derive(Debug)]
pub struct Fletcher4 {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
}

impl Fletcher4 {
    pub fn new() -> Fletcher4 {
        Fletcher4 {
            a: 0,
            b: 0,
            c: 0,
            d: 0,
        }
    }

    // data.len() % 4 == 0
    #[inline]
    pub fn feed_bytes(&mut self, data: &[u8]) {
        assert!(data.len() % 4 == 0);
        self.feed_iter(data.chunks_exact(4).map(|s: &[u8]| {
            debug_assert!(s.len() == 4);
            let s: [u8; 4] = [s[0], s[1], s[2], s[3]];
            // let s = std::mem::transmute::<_, [u8; 4]>(s[0..4]);
            u32::from_ne_bytes(s)
        }))
    }

    pub fn feed_iter<I>(&mut self, data: I)
    where
        I: IntoIterator<Item = u32>,
    {
        data.into_iter().for_each(|f| self.feed(f))
    }

    #[inline]
    pub fn feed(&mut self, f: u32) {
        /* from zfs_fletcher.c:
         *	a  = a    + f
         *	 i    i-1    i-1
         *
         *	b  = b    + a
         *	 i    i-1    i
         *
         *	c  = c    + b		(fletcher-4 only)
         *	 i    i-1    i
         *
         *	d  = d    + c		(fletcher-4 only)
         *	 i    i-1    i
         */
        self.a = self.a.wrapping_add(u64::from(f));
        self.b = self.b.wrapping_add(self.a);
        self.c = self.c.wrapping_add(self.b);
        self.d = self.d.wrapping_add(self.c);
    }

    #[inline]
    pub fn checksum(&self) -> [u64; 4] {
        [self.a, self.b, self.c, self.d]
    }
}
