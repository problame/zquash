mod generated;
pub use generated::*;

use std::fmt;

pub type drr_begin = dmu_replay_record__bindgen_ty_2_drr_begin;
pub type drr_end = dmu_replay_record__bindgen_ty_2_drr_end;

impl fmt::Debug for drr_begin {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
        pub struct drr_begin {
          pub drr_magic: u64,
          pub drr_versioninfo: u64,
          pub drr_creation_time: u64,
          pub drr_type: dmu_objset_type_t,
          pub drr_flags: u32,
          pub drr_toguid: u64,
          pub drr_fromguid: u64,
          pub drr_toname: [::std::os::raw::c_char; 256usize],
        }
        */
        let drr_toname = unsafe {
            std::slice::from_raw_parts(
                self.drr_toname[..].as_ptr() as *const u8,
                self.drr_toname.len(),
            )
        };
        let term = drr_toname
            .iter()
            .enumerate()
            .filter_map(|(i, c)| if *c == 0 { Some(i) } else { None })
            .next()
            .expect("drr_toname should be null terminated");
        use std::ffi::CStr;
        let drr_toname = unsafe { CStr::from_bytes_with_nul_unchecked(&drr_toname[0..=term]) };
        out.debug_struct("drr_begin")
            .field("drr_magic", &self.drr_magic)
            .field("drr_versioninfo", &self.drr_versioninfo)
            .field("drr_creation_time", &self.drr_creation_time)
            .field("drr_type", &self.drr_type)
            .field("drr_flags", &self.drr_flags)
            .field("drr_toguid", &self.drr_toguid)
            .field("drr_fromguid", &self.drr_fromguid)
            .field("drr_toname", &drr_toname)
            .finish()
    }
}

use std::borrow::Borrow;

pub struct drr_debug<R: Borrow<dmu_replay_record>>(pub R);

impl<R: Borrow<dmu_replay_record>> fmt::Debug for drr_debug<R> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let drr = self.0.borrow();
        let dbg: &dyn fmt::Debug = unsafe {
            if drr.drr_type == dmu_replay_record_DRR_OBJECT {
                &drr.drr_u.drr_object
            } else if drr.drr_type == dmu_replay_record_DRR_FREEOBJECTS {
                &drr.drr_u.drr_freeobjects
            } else if drr.drr_type == dmu_replay_record_DRR_WRITE {
                &drr.drr_u.drr_write
            } else if drr.drr_type == dmu_replay_record_DRR_FREE {
                &drr.drr_u.drr_free
            } else if drr.drr_type == dmu_replay_record_DRR_BEGIN {
                &drr.drr_u.drr_begin
            } else if drr.drr_type == dmu_replay_record_DRR_END {
                &drr.drr_u.drr_end
            } else if drr.drr_type == dmu_replay_record_DRR_SPILL {
                &drr.drr_u.drr_spill
            } else if drr.drr_type == dmu_replay_record_DRR_OBJECT_RANGE {
                &drr.drr_u.drr_object_range
            } else {
                unreachable!()
            }
        };
        if !out.alternate() {
            write!(out, "{:?}", dbg)
        } else {
            write!(out, "{:#?}", dbg)
        }
    }
}

pub mod byte_serialize_impls {

    use super::dmu_replay_record;
    use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    struct dmu_replay_record_visitor;

    impl<'de> Visitor<'de> for dmu_replay_record_visitor {
        type Value = dmu_replay_record;
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(
                formatter,
                "expecting a byte array of size {}",
                std::mem::size_of::<Self::Value>()
            )
        }
        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            assert!(v.len() == std::mem::size_of::<dmu_replay_record>());
            let mut record: dmu_replay_record;
            unsafe {
                record = std::mem::zeroed();
                let record_aligned_ptr = &mut record as *mut _ as *mut u8;
                std::ptr::copy(v.as_ptr(), record_aligned_ptr, v.len());
            }
            Ok(record)
        }
    }

    impl Serialize for dmu_replay_record {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let as_bytes = unsafe {
                std::slice::from_raw_parts(
                    self as *const _ as *const u8,
                    std::mem::size_of_val(&*self),
                )
            };
            serializer.serialize_bytes(as_bytes)
        }
    }

    impl<'de> Deserialize<'de> for dmu_replay_record {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_bytes(dmu_replay_record_visitor)
        }
    }
}
