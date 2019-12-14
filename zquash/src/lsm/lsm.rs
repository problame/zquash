extern crate bincode;
extern crate serde;

use serde::de::DeserializeOwned;
use serde::Serialize;

struct FileIdentifier {
    level: usize,
    num_in_level: usize,
}

impl FileIdentifier {
    fn to_path(&self, directory: &std::path::Path) -> std::path::PathBuf {
        directory.join(format!("{}_{}", self.level, self.num_in_level))
    }
}

fn read_to_opt<R: std::io::Read, T: serde::de::DeserializeOwned>(reader: R) -> Option<T> {
    let obj = bincode::deserialize_from(reader);
    if let Ok(obj) = obj {
        return Some(obj);
    }
    //TODO check if at end of stream some better way
    match obj {
        Err(err) => match err.as_ref() {
            bincode::ErrorKind::Io(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => panic!(format!("IO Error: {:?}", err)),
            },
            _ => panic!(format!("Deserialize Error")),
        },
        _ => unreachable!(),
    }
}

pub struct LSMWriter<K: Ord, V> {
    btree: std::collections::BTreeMap<K, V>,
    directory: std::path::PathBuf,
    level_counts: Vec<usize>,
    max_entries_in_ram: usize,
    max_files_per_level: usize,
}

impl<K: Ord + Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> LSMWriter<K, V> {
    pub fn new(
        directory: std::path::PathBuf,
        max_entries_in_ram: usize,
        max_files_per_level: usize,
    ) -> LSMWriter<K, V> {
        std::fs::create_dir_all(&directory).unwrap();
        LSMWriter {
            btree: std::collections::BTreeMap::new(),
            directory,
            level_counts: Vec::new(),
            max_entries_in_ram,
            max_files_per_level,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.btree.insert(key, value);
        if self.btree.len() > self.max_entries_in_ram {
            self.write_btree_to_file();
            self.btree.clear();
            self.merge_full_levels();
        }
    }

    fn new_file_at_level(&mut self, level: usize) -> FileIdentifier {
        if level > 20 {
            panic!("Suspiciously high level")
        }
        while self.level_counts.len() <= level {
            self.level_counts.push(0);
        }
        let id = FileIdentifier {
            level: level,
            num_in_level: self.level_counts[level],
        };
        self.level_counts[level] += 1;
        id
    }

    fn merge_level_up(&mut self, level: usize) {
        assert!(self.level_counts[level] > 1); //Merging just one file would work but should not be called
        let merge_paths: Vec<_> = (0..self.level_counts[level])
            .map(|num| {
                FileIdentifier {
                    level: level,
                    num_in_level: num,
                }
                .to_path(&self.directory)
            })
            .collect();
        let out_path = self.new_file_at_level(level + 1).to_path(&self.directory);
        self.merge_files(&merge_paths, &out_path);
        self.level_counts[level] = 0;
    }

    fn merge_full_levels(&mut self) {
        let mut level = 0;
        while level < self.level_counts.len() {
            if self.level_counts[level] >= self.max_files_per_level {
                self.merge_level_up(level);
                level += 1;
            } else {
                //Levels can only become full if a new file from a lower level propagates up
                break;
            }
        }
    }

    pub fn merge_completely(mut self, out_path: &std::path::Path) {
        self.write_btree_to_file();

        let mut all_paths = Vec::new();

        for (level, count) in self.level_counts.iter().enumerate() {
            (0..*count)
                .map(|num| {
                    FileIdentifier {
                        level: level,
                        num_in_level: num,
                    }
                    .to_path(&self.directory)
                })
                .for_each(|path| all_paths.push(path));
        }

        //Maybe it is not most efficient to merge all files at the same time
        //as there is probably a sweet spot somewhere for how many files one should stream from at once
        //But easier for now
        self.merge_files(&all_paths, &out_path);
        std::fs::remove_dir(self.directory).unwrap();
    }

    fn write_btree_to_file(&mut self) {
        let path = self.new_file_at_level(0).to_path(&self.directory);
        let file = std::fs::File::create(path).unwrap();
        let mut stream = std::io::BufWriter::new(file);
        for (key, value) in self.btree.iter() {
            bincode::serialize_into(&mut stream, &key).unwrap();
            bincode::serialize_into(&mut stream, &value).unwrap();
        }
    }

    fn merge_files(&self, paths: &Vec<std::path::PathBuf>, out_path: &std::path::Path) {
        let mut streams: Vec<_> = paths
            .iter()
            .map(|path| std::fs::File::open(path).unwrap())
            .map(|file| std::io::BufReader::new(file))
            .collect();
        let mut out_stream = std::io::BufWriter::new(std::fs::File::create(out_path).unwrap());

        fn cmp_opt<T: Ord>(x: &Option<T>, y: &Option<T>) -> std::cmp::Ordering {
            use std::cmp::Ordering;
            match (x, y) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            }
        }

        //Vec of lowest key of each stream, is None if stream is empty
        let mut keys: Vec<Option<K>> = streams
            .iter_mut()
            .map(|stream| read_to_opt(stream))
            .collect();

        loop {
            let (i, min_k) = keys
                .iter()
                .enumerate()
                .min_by(|(_, k1), (_, k2)| cmp_opt(k1, k2))
                .unwrap();
            if let Some(min_k) = min_k {
                bincode::serialize_into(&mut out_stream, &min_k).unwrap();
                let min_v: V = read_to_opt(&mut streams[i])
                    .expect("Key was read from file but value is missing");
                bincode::serialize_into(&mut out_stream, &min_v).unwrap();
                keys[i] = read_to_opt(&mut streams[i]);
            } else {
                break;
            }
        }

        for p in paths {
            std::fs::remove_file(p).unwrap();
        }
    }
}

pub struct LSMReader<K, V> {
    file: std::io::BufReader<std::fs::File>,
    oh: std::marker::PhantomData<K>,
    why: std::marker::PhantomData<V>,
}

impl<K, V> LSMReader<K, V> {
    pub fn open(path: &std::path::Path) -> LSMReader<K, V> {
        LSMReader {
            file: std::io::BufReader::new(std::fs::File::open(path).unwrap()),
            oh: std::marker::PhantomData,
            why: std::marker::PhantomData,
        }
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for LSMReader<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let key: Option<K> = read_to_opt(&mut self.file);
        if let Some(key) = key {
            let value: V =
                read_to_opt(&mut self.file).expect("Key was read from file but value is missing");
            Some((key, value))
        } else {
            None
        }
    }
}

pub struct SortedLSMWriter<K: Ord, V> {
    file: std::io::BufWriter<std::fs::File>,
    last_entry: Option<K>,
    phantom: std::marker::PhantomData<V>,
}

impl<K: Ord + Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> SortedLSMWriter<K, V> {
    pub fn new(path: &std::path::Path) -> SortedLSMWriter<K, V> {
        SortedLSMWriter {
            file: std::io::BufWriter::new(std::fs::File::create(path).unwrap()),
            last_entry: None,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if let Some(last_key) = &self.last_entry {
            assert!(last_key.cmp(&key) == std::cmp::Ordering::Less);
        }
        bincode::serialize_into(&mut self.file, &key).unwrap();
        bincode::serialize_into(&mut self.file, &value).unwrap();
        self.last_entry = Some(key);
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use super::*;

    impl<K: Ord + Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> LSMWriter<K, V> {
        pub fn into_reader(mut self, out_path: &std::path::Path) -> LSMReader<K, V> {
            self.merge_completely(out_path);
            LSMReader::open(out_path)
        }
    }

    #[test]
    fn sort_reversed_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let dir2 = tempfile::tempdir().unwrap();
        let pathout = dir2.path().join("outfile");
        std::fs::remove_dir_all(&path);
        std::fs::remove_file(&pathout);
        let mut lsm = LSMWriter::new(path, 2, 3);
        for i in (0..100).rev() {
            lsm.insert(i, i + 10);
        }
        let reader = lsm.into_reader(&pathout);
        assert!(reader.eq((0..100).map(|x| (x, x + 10))));
        std::fs::remove_file(&pathout);
    }
}
