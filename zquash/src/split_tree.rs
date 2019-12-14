use std::collections::BTreeMap;
use Occ::*;

#[derive(Clone, PartialEq, Eq)]
pub enum Occ<T> {
    Unknown,
    Free,
    Occupied(T),
}

impl<T> Occ<T> {
    pub fn is_unknown(&self) -> bool {
        match self {
            Unknown => true,
            _ => false,
        }
    }

    pub fn is_free(&self) -> bool {
        match self {
            Free => true,
            _ => false,
        }
    }

    pub fn is_occupied(&self) -> bool {
        match self {
            Occupied(_) => true,
            _ => false,
        }
    }

    pub fn occupied(&self) -> Option<&T> {
        match self {
            Occupied(o) => Some(o),
            _ => None,
        }
    }

    pub fn occupied_mut(&mut self) -> Option<&mut T> {
        match self {
            Occupied(o) => Some(o),
            _ => None,
        }
    }

    pub fn to_empty_t(&self) -> Occ<()> {
        match self {
            Unknown => Unknown,
            Free => Free,
            Occupied(_) => Occupied(()),
        }
    }
}

use std::fmt;

impl<T> fmt::Debug for Occ<T> {
    fn fmt(&self, o: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Occupied(_) => "Occupied",
            Free => "Free",
            Unknown => "Unknown",
        };
        write!(o, "{}", s)
    }
}

type Offset = u64;

#[derive(Debug)]
pub struct SplitTree<T> {
    m: BTreeMap<Offset, Occ<T>>,
}

impl<T: fmt::Debug> SplitTree<T> {
    pub fn new() -> Self {
        let mut m = BTreeMap::new();
        m.insert(0, Unknown);
        SplitTree { m }
    }

    pub fn insert(&mut self, offset: u64, length: u64, content: T) {
        //Get state behind insertion
        let behind_insertion_offset = offset + length;
        let (behind_insertion_definition_offset_ref, behind_insertion_ref) = self
            .m
            .range(0..=behind_insertion_offset)
            .last()
            .expect("always at least one split");

        let (behind_insertion_definition_offset, behind_insertion) = (
            *behind_insertion_definition_offset_ref,
            behind_insertion_ref.to_empty_t(),
        );

        //Get state at start of insert
        let (start_insert_definition_offset_ref, start_insert_ref) = self
            .m
            .range(0..=offset)
            .last()
            .expect("always at least one split");
        //Insert may not start in the middle of occupied space
        match start_insert_ref {
            Occupied(_) => {
                assert!(*start_insert_definition_offset_ref == offset);
            }
            _ => {}
        }

        //Remove all entries in insertion range
        let indices_to_remove = self
            .m
            .range(offset..behind_insertion_offset)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for k in indices_to_remove {
            self.m
                .remove(&k)
                .expect("This index was just queried, it should be there");
        }

        //Insert write
        self.m.insert(offset, Occupied(content));

        //Write state at end
        if behind_insertion_definition_offset != behind_insertion_offset {
            let with_t = match behind_insertion {
                Occupied(_) => {
                    panic!("may not insert inside an existing occupied block");
                }
                Free => Free,
                Unknown => Unknown,
            };
            self.m.insert(behind_insertion_offset, with_t);
        }
    }

    pub fn free(&mut self, offset: u64, length: u64) {
        assert!(length > 0);
        //Get state behind free
        let behind_free_offset = offset
            .checked_add(length)
            .expect("offset + length overflows address space");
        let (behind_free_definition_offset_ref, behind_free_ref) = self
            .m
            .range(0..=behind_free_offset)
            .last()
            .expect("always at least one split");

        let (behind_free_definition_offset, behind_free) = (
            *behind_free_definition_offset_ref,
            behind_free_ref.to_empty_t(),
        );

        //Get state at start of free
        let (start_free_definition_offset_ref, start_free_ref) = self
            .m
            .range(0..=offset)
            .last()
            .expect("always at least one split");
        //Free may not start in the middle of occupied space
        match start_free_ref {
            Occupied(_) => {
                assert!(*start_free_definition_offset_ref == offset);
            }
            _ => {}
        }

        //Remove all entries in free range
        let indices_to_remove = self
            .m
            .range(offset..behind_free_offset)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for k in indices_to_remove {
            self.m
                .remove(&k)
                .expect("this index was just queried, it should be there");
        }

        //Insert free
        self.m.insert(offset, Free);

        //Write state at end
        if behind_free_definition_offset != behind_free_offset {
            let with_t = match behind_free {
                Occupied(_) => {
                    panic!("may not free part of occupied block");
                }
                Free => Free,
                Unknown => Unknown,
            };
            let replaced = self.m.insert(behind_free_offset, with_t);
            if let Some(_) = replaced {
                panic!("it was checked that this does not replace anything:\noffset={}\nlength={}\nbehind_free_offset={}\nbehind_free_definition_offset={}\n{:#?}", offset, length, behind_free_offset, behind_free_definition_offset, self);
            }
        }
    }

    pub fn free_to_end(&mut self, offset: u64) {
        use std::ops::Bound::*;
        //Get state at start of free
        let (start_free_definition_offset_ref, start_free_ref) = self
            .m
            .range(0..=offset)
            .last()
            .expect("always at least one split");
        //Free may not start in the middle of occupied space
        match start_free_ref {
            Occupied(_) => {
                assert!(*start_free_definition_offset_ref == offset);
            }
            _ => {}
        }

        //Remove all entries in free range
        let indices_to_remove = self
            .m
            .range((Included(offset), Unbounded))
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for k in indices_to_remove {
            self.m
                .remove(&k)
                .expect("this index was just queried, it should be there");
        }

        //Insert free
        self.m.insert(offset, Free);
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u64, u64, &'a Occ<T>)> + 'a {
        let sentinel = Some((&std::u64::MAX, &Occ::Unknown)).into_iter();
        let shifted = self.m.iter().skip(1).chain(sentinel);
        self.m
            .iter()
            .zip(shifted)
            .map(|((offset, occ), (next_offset, _))| {
                assert!(offset <= next_offset);
                (*offset, (*next_offset - *offset), occ)
            })
    }

    pub fn iter_occupied<'a>(&'a self) -> impl Iterator<Item = (u64, u64, &'a T)> + 'a {
        self.iter().filter_map(|(o, l, occ)| {
            if let Some(occ) = occ.occupied() {
                Some((o, l, occ))
            } else {
                None
            }
        })
    }

    pub fn get_mut(&mut self, addr: u64) -> (&u64, &mut Occ<T>) {
        self.m
            .range_mut(0..=addr)
            .last()
            .expect("always at least one split")
    }

    pub fn get(&self, addr: u64) -> (&u64, &Occ<T>) {
        self.m
            .range(0..=addr)
            .last()
            .expect("always at least one split")
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    impl<T> SplitTree<T> {
        fn get_occ_at_index(&self, offset: u64) -> &Occ<T> {
            let (_, occ_ref) = self
                .m
                .range(0..=offset)
                .last()
                .expect("always at least one split");

            occ_ref
        }
    }

    fn check_unecessary_splits<T: Clone>(t: &SplitTree<T>) {
        use std::ops::Bound::*;
        let mut last_entry: Option<Occ<T>> = None;
        for (_, v) in t.m.range((Included(&0), Unbounded)) {
            if let Some(last_entry) = last_entry {
                match (last_entry, v) {
                    (Unknown, Unknown) => panic!("unnecessary Unknown"),
                    (Free, Free) => panic!("unneccecary Free"),
                    _ => {}
                }
            }
            last_entry = Some(v.clone());
        }
    }
    #[test]
    fn insert() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.insert(20, 10, ());
        t.insert(35, 10, ());
        let t = dbg!(t);
        assert!(t.get_occ_at_index(9).is_unknown());
        assert!(t.get_occ_at_index(10).is_occupied());
        assert!(t.get_occ_at_index(20).is_occupied());
        assert!(t.get_occ_at_index(30).is_unknown());
        assert!(t.get_occ_at_index(34).is_unknown());
        assert!(t.get_occ_at_index(35).is_occupied());
        assert!(t.get_occ_at_index(44).is_occupied());
        assert!(t.get_occ_at_index(45).is_unknown());
        check_unecessary_splits(&t);
    }

    #[test]
    fn free() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.insert(20, 10, ());
        t.insert(35, 10, ());
        t.free(10, 10);
        t.free(33, 12);
        let t = dbg!(t);
        assert!(t.get_occ_at_index(9).is_unknown());
        assert!(t.get_occ_at_index(10).is_free());
        assert!(t.get_occ_at_index(19).is_free());
        assert!(t.get_occ_at_index(20).is_occupied());
        assert!(t.get_occ_at_index(30).is_unknown());
        assert!(t.get_occ_at_index(32).is_unknown());
        assert!(t.get_occ_at_index(33).is_free());
        assert!(t.get_occ_at_index(44).is_free());
        assert!(t.get_occ_at_index(45).is_unknown());
        check_unecessary_splits(&t);
    }

    #[test]
    fn overwrite() {
        let mut t = SplitTree::new();
        t.insert(10, 10, 1);
        t.insert(10, 10, 2);
        let occ = t.get_occ_at_index(15);
        match occ {
            Occupied(2) => {}
            _ => panic!("Overwrite did not change data"),
        }
    }

    #[test]
    fn iterator() {
        let mut t = SplitTree::new();
        t.insert(10, 10, 1);
        t.insert(20, 10, 1);
        t.free(30, 10);
        let mut it = t.iter();
        assert!(it.next().unwrap().2.is_unknown());
        assert!(it.next().unwrap().2.is_occupied());
        assert!(it.next().unwrap().2.is_occupied());
        assert!(it.next().unwrap().2.is_free());
        assert!(it.next().unwrap().2.is_unknown());
        assert!(it.next().is_none());
    }

    #[should_panic]
    #[test]
    fn double_insert_panics() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.insert(5, 10, ());
    }

    #[should_panic]
    #[test]
    fn double_insert_panics2() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.insert(15, 10, ());
    }

    #[should_panic]
    #[test]
    fn free_part_of_occupied_panics() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.free(5, 10);
    }

    #[should_panic]
    #[test]
    fn free_part_of_occupied_panics2() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.free(15, 10);
    }

    #[should_panic]
    #[test]
    fn free_part_of_occupied_panics3() {
        let mut t = SplitTree::new();
        t.insert(10, 10, ());
        t.free(15, 20);
    }

    #[should_panic]
    #[test]
    fn free_part_of_occupied_panics4() {
        let mut t = SplitTree::new();
        t.insert(10, 20, ());
        t.free(15, 5);
    }

    #[test]
    #[should_panic]
    fn free_u64_max_panics() {
        let mut t = SplitTree::new();
        t.insert(10, 20, ());
        t.insert(30, 20, ());
        t.free(30, std::u64::MAX);
    }

    #[test]
    fn free_to_end() {
        let mut t = SplitTree::new();
        t.insert(10, 20, ());
        t.insert(30, 20, ());
        t.free_to_end(30);
        assert!(t.get_occ_at_index(29).is_occupied());
        assert!(t.get_occ_at_index(30).is_free());
        assert!(t.get_occ_at_index(std::u64::MAX).is_free());
    }

    #[test]
    fn free_everything() {
        let mut t = SplitTree::new();
        t.insert(10, 20, ());
        t.insert(30, 20, ());
        t.free_to_end(0);
        assert!(t.get_occ_at_index(0).is_free());
        assert!(t.get_occ_at_index(30).is_free());
        assert!(t.get_occ_at_index(std::u64::MAX).is_free());
    }
}
