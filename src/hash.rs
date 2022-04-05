use std::collections::hash_map::DefaultHasher;
use std::default::Default;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use serde::Serialize;
use serde::Deserialize;
use farmhash;
use t1ha;
use mur3;

/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    IntField(i32),
    StringField(String),
}

impl Field {
    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::IntField(x) => x.to_le_bytes().to_vec(),
            Field::StringField(s) => {
                let s_len: usize = s.len();
                let mut result = s_len.to_le_bytes().to_vec();
                let mut s_bytes = s.clone().into_bytes();
                let padding_len: usize = 128 - s_bytes.len();
                let pad = vec![0; padding_len];
                s_bytes.extend(&pad);
                result.extend(s_bytes);
                result
            }
        }
    }

    /// Unwraps integer fields.
    pub fn unwrap_int_field(&self) -> i32 {
        match self {
            Field::IntField(i) => *i,
            _ => panic!("Expected i32"),
        }
    }

    /// Unwraps string fields.
    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::StringField(s) => &s,
            _ => panic!("Expected String"),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::IntField(x) => write!(f, "{}", x),
            Field::StringField(x) => write!(f, "{}", x),
        }
    }
}

/// Hashable trait has three hash functions
pub trait Hashable {
    fn farm_hash(&self) -> usize;
    fn murmur_hash3(&self) -> usize;
    fn t1ha_hash(&self) -> usize;
    fn std_hash(&self) -> usize;
}

/// Implementation for Field's Hashable trait
impl Hashable for Field {
    // using FarmHash 64-bit hash functions to get hash value
    fn farm_hash(&self) -> usize {
        let result= match self {
            Field::IntField(i) => {
                farmhash::hash64(&i.to_be_bytes()) as usize
            }
            Field::StringField(s) => {
                farmhash::hash64(s.as_bytes()) as usize
            }
        };
        result
    }

    // using MurmurHash3 32-bit hash functions to get hash value
    fn murmur_hash3(&self) -> usize {
        let result= match self {
            Field::IntField(i) => {
                mur3::murmurhash3_x86_32(&i.to_be_bytes(), 0) as usize
            }
            Field::StringField(s) => {
                mur3::murmurhash3_x86_32(s.as_bytes(), 0) as usize
            }
        };
        result
    }

    // using t1ha 64-bit hash functions to get hash value
    fn t1ha_hash(&self) -> usize {
        let result= match self {
            Field::IntField(x) => {
                t1ha::t1ha0(&x.to_be_bytes(), 0) as usize
            },
            Field::StringField(x) => {
                t1ha::t1ha0(x.as_bytes(), 0) as usize
            },
        };
        result
    }

    // using std::hash 64-bit functions to get hash value
    fn std_hash(&self) -> usize {
        let mut hasher = DefaultHasher::new();
        let result= match self {
            Field::IntField(i) => {
                i.hash(&mut hasher);
                hasher.finish() as usize
            },
            Field::StringField(s) => {
                s.hash(&mut hasher);
                hasher.finish() as usize
            },
        };
        result
    }
}

/// Implementation for Field's default trait
impl Default for Field {
    fn default() -> Self { Field::IntField(0) }
}

/// Different types of hash functions
#[derive(Clone, Copy, PartialEq)]
pub enum HashFunction {
    FarmHash,
    MurmurHash3,
    T1haHash,
    StdHash,
}

/// Different types of hash schemes
#[derive(Clone, Copy, PartialEq)]
pub enum HashScheme {
    LinearProbe,
    RobinHood,
    Hopscotch,
}

/// Different types of extend hash table methods
#[derive(Clone, Copy)]
pub enum ExtendOption {
    ExtendBucketSize,
    ExtendBucketNumber,
}

/// Data structure for hash nodes, contains key, value, and taken attributes
#[derive(Debug, Clone)]
pub struct HashNode {
    pub(crate) key: (Field, Field),
    pub(crate) value: usize,
    taken: bool,
    dis: usize,
}

/// Implementation for HashNode's default trait
impl Default for HashNode {
    fn default() -> HashNode {
        HashNode {
            key: (Field::default(), Field::default()),
            value: 0,
            taken: false,
            dis: usize::MAX,
        }
    }
}

/// HashTable contains vec of hash buckets
pub struct HashTable {
    pub(crate) buckets: Vec<Vec<HashNode>>,
    pub(crate) taken_count: Vec<usize>,
    pub(crate) BUCKET_NUMBER: usize,
    pub(crate) BUCKET_SIZE: usize,
    pub(crate) function: HashFunction,
    pub(crate) scheme: HashScheme,
    pub(crate) H: usize,
    pub(crate) extend_op: ExtendOption,
    pub(crate) hop_info: Vec<Vec<usize>>,
    pub(crate) load_factor: f64,
}

/// Implementation for HashTable's default trait
impl Default for HashTable {
    fn default() -> HashTable {
        HashTable {
            buckets: vec![],
            taken_count: vec![],
            BUCKET_NUMBER: 0,
            BUCKET_SIZE: 0,
            function: HashFunction::StdHash,
            scheme: HashScheme::LinearProbe,
            H: 4,
            extend_op: ExtendOption::ExtendBucketSize,
            hop_info: vec![],
            load_factor: 0.9,
        }
    }
}

impl HashTable {
    // initialize a new hash table with certain BUCKET_SIZE and BUCKET_NUMBER, HashFunction and HashScheme
    pub fn new(
        b_size: usize,
        b_num: usize,
        func: HashFunction,
        sche: HashScheme,
        h: usize,
        op: ExtendOption,
        load_f: f64,
    ) -> Self {
        Self {
            buckets: vec![vec![HashNode::default(); b_size]; b_num],
            taken_count: vec![0; b_num],
            BUCKET_NUMBER: b_num,
            BUCKET_SIZE: b_size,
            function: func,
            scheme: sche,
            H: h,
            extend_op: op,
            hop_info: vec![vec![0; b_size]; b_num],
            load_factor: load_f,
        }
    }

    // method to get the specific bucket base on the key
    fn get_bucket_index(&self, key: (&Field, &Field)) -> Option<usize> {
        // using different hash functions to get the index for bucket
        let bucket_index = match self.function {
            // using mod 10 to prevent overflow
            HashFunction::FarmHash => {
                (key.0.farm_hash() % 10 + key.1.farm_hash() % 10) % self.BUCKET_NUMBER
            },
            HashFunction::MurmurHash3 => {
                (key.0.murmur_hash3() % 10 + key.1.murmur_hash3() % 10) % self.BUCKET_NUMBER
            },
            HashFunction::T1haHash => {
                (key.0.t1ha_hash() % 10 + key.1.t1ha_hash() % 10) % self.BUCKET_NUMBER
            },
            HashFunction::StdHash => {
                (key.0.std_hash() % 10 + key.1.std_hash() % 10) % self.BUCKET_NUMBER
            },
        };
        // check if the bucket is full and return bucket_index
        if self.taken_count[bucket_index] >= self.BUCKET_SIZE {
            println!("Couldn't get bucket_index!");
            None
        } else {
            Some(bucket_index)
        }
    }

    // method to use linear probe hashing to resolve collision
    fn linear_probe(
        &self,
        key: (&Field, &Field),
        target_bucket_index: usize,
        index: usize
    ) -> Option<usize> {
        let mut i = index;
        // check the empty slot in the bucket
        for _ in 0..self.BUCKET_SIZE {
            // if slot haven't been taken, find it
            if !self.buckets[target_bucket_index][i].taken {
                break;
            }
            // if the key is the same then find it
            if (&self.buckets[target_bucket_index][i].key.0,
                &self.buckets[target_bucket_index][i].key.1) == key {
                break;
            }
            i = (i + 1) % self.BUCKET_SIZE;
        }
        Some(i)
    }

    // method to use robin hood hashing to resolve collision
    fn robin_hood(
        &self,
        key: (&Field, &Field),
        bucket_index: usize,
        ori_index: usize
    ) -> Option<(usize, usize)> {
        let mut index = ori_index;
        let mut distance = 0;
        // check the empty slot in the bucket
        for _ in 0..self.BUCKET_SIZE {
            // if slot haven't been taken, find it
            if !self.buckets[bucket_index][index].taken {
                break;
            }
            // if the key is the same then find it
            if (&self.buckets[bucket_index][index].key.0,
                &self.buckets[bucket_index][index].key.1) == key {
                break;
            }
            // if the distance is larger than origin HashNode then find it
            if distance > self.buckets[bucket_index][index].dis {
                break;
            }
            distance += 1;
            index = (index + 1) % self.BUCKET_SIZE;
        }
        return Some((index, distance));
    }

    // method to get a tuple of (bucket_index, index, distance)
    fn get_indexes(&mut self, key: (&Field, &Field)) -> Option<(usize, usize, usize)> {
        // get target bucket index
        let bucket_index = self.get_bucket_index(key)?;

        // using different hash functions to get the index in one bucket
        let mut index = match self.function {
            HashFunction::FarmHash => {
                (key.0.farm_hash() / 10 + key.1.farm_hash() / 100) % self.BUCKET_SIZE
            },
            HashFunction::MurmurHash3 => {
                (key.0.murmur_hash3() / 10 + key.1.murmur_hash3() / 100) % self.BUCKET_SIZE
            },
            HashFunction::T1haHash => {
                (key.0.t1ha_hash() / 10 + key.1.t1ha_hash() / 100) % self.BUCKET_SIZE
            },
            HashFunction::StdHash => {
                (key.0.std_hash() / 10 + key.1.std_hash() / 100) % self.BUCKET_SIZE
            },
        };

        let mut dis = 0;
        // check if the index has been taken
        if self.buckets[bucket_index][index].taken {
            // using different hashing scheme to solve duplicate
            match self.scheme {
                HashScheme::LinearProbe => {
                    index = self.linear_probe(key, bucket_index, index).unwrap();
                },
                HashScheme::Hopscotch => {
                    // println!("{}", index);
                    return Some((bucket_index, index, dis));
                },
                HashScheme::RobinHood => {
                    let res = self.robin_hood(key, bucket_index, index).unwrap();
                    index = res.0;
                    dis = res.1;
                    return Some((bucket_index, index, dis));
                },
            };
        }

        // check again and return
        if self.buckets[bucket_index][index].taken &&
            (&self.buckets[bucket_index][index].key.0 != key.0 ||
            &self.buckets[bucket_index][index].key.1 != key.1) {
            // return None if couldn't find a available slot
            println!("Couldn't get indexes.");
            None
        } else {
            // return the bucket_index, index, and distance
            Some((bucket_index, index, dis))
        }
    }

    // method to get the mutable value
    pub fn get_mut_value(&mut self, key: (&Field, &Field)) -> Option<&mut usize> {
        if let Some(indexes) = self.get_indexes(key) {
            Some(&mut self.buckets[indexes.0][indexes.1].value)
        } else {
            println!("Couldn't get mut_value");
            None
        }
    }

    // method to get the value
    pub fn get_value(&mut self, key: (&Field, &Field)) -> Option<&usize> {
        if let Some(indexes) = self.get_indexes(key) {
            if self.scheme == HashScheme::Hopscotch {
                // check th hop info
                for n in (0..self.H).rev() {
                    // loop through the slots base on the hop
                    if (self.hop_info[indexes.0][indexes.1] & (1 << n as usize)) != 0 {
                        // compare the key
                        if &self.buckets[indexes.0][indexes.1 + (self.H - 1 - n)].key.0 != key.0 &&
                            &self.buckets[indexes.0][indexes.1 + (self.H - 1 - n)].key.1 != key.1 {
                            return Some(&self.buckets[indexes.0][indexes.1  + (self.H - 1 - n)].value);
                        }
                    }
                }
                return None;
            } else {
                return Some(&self.buckets[indexes.0][indexes.1].value);
            }
        } else {
            println!("Couldn't get value");
            return None;
        }
    }

    // method to use hopscotch hashing to insert
    // return 0 if ok, 1 if need to resize
    fn hopscotch_insert(&mut self, new_key: (Field, Field), new_value: usize, indexes: (usize, usize)){
        let bucket_index = indexes.0;
        let index = indexes.1;
        let mut empty = false;
        // hop is full
        if self.hop_info[bucket_index][index] >= self.H.pow(2) {
            println!("No available swaps");
            self.extend();
            self.insert(new_key.clone(), new_value);
            return
        }

        // look through neighborhood for empty space or same key
        let end_of_H = std::cmp::min(index + self.H, self.BUCKET_SIZE);
        for i in index..end_of_H {
            if self.buckets[bucket_index][i].taken == false {  // slot is empty, insert the node
                // put entry in empty space
                self.buckets[bucket_index][i] = HashNode { key: new_key.clone(), value: new_value, taken: true, dis: 0};
                self.hop_info[bucket_index][index] |= 0b_1 << (self.H - 1 - (i - index));
                self.taken_count[bucket_index] += 1;
                return
            } else if self.buckets[bucket_index][i].key == new_key { // same key, then update value
                self.buckets[bucket_index][i].value += new_value;
                return
            }
        }

        // if no room in neighborhood, look through the rest of the table for an empty space to swap with
        // empty_index -> potentially empty index, start_index -> interval starting index, candidate_index -> swap candidate index
        for mut empty_index in end_of_H..self.BUCKET_SIZE {
            if self.buckets[bucket_index][empty_index].taken == false {  // find empty slot
                let mut start_index = empty_index - (self.H - 1);
                'inner: loop {
                    for candidate_index in start_index..(start_index + self.H) {
                        if self.hop_info[bucket_index][candidate_index] > 0 {
                            // check every digit in H
                            for n in (0..self.H).rev() {
                                if (self.hop_info[bucket_index][candidate_index] & (1 << n as usize)) != 0 {
                                    // no available slot before the empty
                                    if candidate_index + (self.H - 1 - n) >= empty_index {
                                        println!("No available swaps");
                                        self.extend();
                                        self.insert(new_key.clone(), new_value);
                                        return
                                    }
                                    // swap the target with empty slot
                                    self.buckets[bucket_index][empty_index] = self.buckets[bucket_index][candidate_index + (self.H - 1 - n)].clone();
                                    self.buckets[bucket_index][empty_index].taken = true;
                                    self.buckets[bucket_index][candidate_index + (self.H - 1 - n)] = HashNode::default();
                                    self.hop_info[bucket_index][candidate_index] -= usize::pow(2, n as u32);
                                    // if empty_index - candidate_index > 3 {
                                    //     println!("????????");
                                    //     panic!();
                                    // }
                                    self.hop_info[bucket_index][candidate_index] += usize::pow(2,  (self.H - 1 - (empty_index - candidate_index)) as u32);
                                    empty_index = candidate_index + (self.H - 1 - n);
                                    break;
                                }
                            }
                            start_index = empty_index.checked_sub((self.H - 1)).unwrap_or(0);

                            if empty_index - index < self.H {
                                // we are now within the neighborhood, so put new entry in empty space
                                self.buckets[bucket_index][empty_index] = HashNode { key: new_key.clone(), value: new_value, taken: true, dis: 0};
                                self.hop_info[bucket_index][index] |= 1 << (self.H - 1 - (empty_index - index) as usize);
                                self.taken_count[bucket_index] += 1;
                                return
                            } else {
                                // look for another swap to move empty closer (or into) neighborhood
                                continue 'inner
                            }
                        }
                    }
                    // can't swap anything with empty space, need to resize
                    println!("Can't swap it into the neighborhood! Extended!");
                    self.extend();
                    self.insert(new_key.clone(), new_value);
                    return
                }
            }
        }
        println!("No empty space!");
        self.extend();
        self.insert(new_key.clone(), new_value);
        return
    }

    // method to insert a new HashNode
    pub fn insert(&mut self, new_key: (Field, Field), new_value: usize) {
        // extent the hash table once reach the load limit
        for i in 0..self.BUCKET_NUMBER {
            if (self.BUCKET_SIZE as f64 * self.load_factor).floor() as usize <= self.taken_count[i] {
                println!("Rehash b/c load factor");
                self.extend();
                println!("Rehash finished");
                self.insert(new_key.clone(), new_value);
            }
        }

        // get the tuple of (bucket_index, index)
        if let Some(indexes) =
        self.get_indexes((&new_key.0, &new_key.1)){
            if self.scheme == HashScheme::Hopscotch { // using helper method to insert w/ hopscotch
                self.hopscotch_insert(new_key.clone(), new_value, (indexes.0, indexes.1));
            } else if self.buckets[indexes.0][indexes.1].key == new_key { // check if the the key is already existed in the table
                // add new value to the old one
                self.buckets[indexes.0][indexes.1].value += new_value;
            } else if self.buckets[indexes.0][indexes.1].taken == false { // if not been taken
                // directly insert the new value
                self.buckets[indexes.0][indexes.1] = HashNode {key: new_key, value: new_value, taken: true, dis: indexes.2};
                self.taken_count[indexes.0] += 1;
            } else { // robin hood situation
                // insert the new node and then original node
                let ori_node = self.buckets[indexes.0][indexes.1].clone();
                self.buckets[indexes.0][indexes.1] = HashNode {key: new_key, value: new_value, taken: true, dis: indexes.2};
                self.insert(ori_node.key, ori_node.value);
            }
        } else {
            println!("Rehash b/c can't get index");
            self.extend();
            println!("Rehash finished");
            self.insert(new_key.clone(), new_value);
        };
    }

    // method to extend the bucket number / bucket size and then rehash the table
    fn extend(&mut self) {
        assert!(self.buckets.len() > 0);
        let mut new_self = match self.extend_op {
            // extend the bucket size to twice of the original bucket size
            ExtendOption::ExtendBucketSize => {
                Self {
                    buckets: vec![vec![HashNode::default(); self.BUCKET_SIZE * 2]; self.BUCKET_NUMBER],
                    taken_count: vec![0; self.BUCKET_NUMBER],
                    BUCKET_SIZE: self.BUCKET_SIZE * 2,
                    BUCKET_NUMBER: self.BUCKET_NUMBER,
                    function: self.function,
                    scheme: self.scheme,
                    H: self.H,
                    extend_op: self.extend_op,
                    hop_info: vec![vec![0; self.BUCKET_SIZE * 2]; self.BUCKET_NUMBER],
                    load_factor: self.load_factor,
                }
            },
            // extend the bucket number to twice of than original bucket number
            ExtendOption::ExtendBucketNumber => {
                Self {
                    buckets: vec![vec![HashNode::default(); self.BUCKET_SIZE]; self.BUCKET_NUMBER * 2],
                    taken_count: vec![0; self.BUCKET_NUMBER * 2],
                    BUCKET_SIZE: self.BUCKET_SIZE,
                    BUCKET_NUMBER: self.BUCKET_NUMBER * 2,
                    function: self.function,
                    scheme: self.scheme,
                    H: self.H,
                    extend_op: self.extend_op,
                    hop_info: vec![vec![0; self.BUCKET_SIZE]; self.BUCKET_NUMBER * 2],
                    load_factor: self.load_factor,
                }
            }
        };

        // insert the <key, value> to new hash table
        for bucket in self.buckets.iter() {
            for node in bucket.iter() {
                if node.taken {
                    new_self.insert(node.key.clone(), node.value.clone());
                }
            }
        }
        *self = new_self;
    }
}

#[cfg(test)]
mod test_hash {
    use super::*;

    // function to test extend
    pub fn test_extend() {
        let mut table = HashTable::new(
            5,
            1,
            HashFunction::FarmHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.75,
        );

        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(0);
        table.insert((name, course_taken), 1);

        let name = Field::StringField(String::from("Ben"));
        let course_taken = Field::IntField(1);
        table.insert((name, course_taken), 1);

        let name = Field::StringField(String::from("Chris"));
        let course_taken = Field::IntField(1);
        table.insert((name, course_taken), 1);

        // Before first rehash: 5 * 0.75 = 3.75
        assert_eq!(3, table.taken_count[0]);
        assert_eq!(5, table.BUCKET_SIZE);

        let name = Field::StringField(String::from("David"));
        let course_taken = Field::IntField(1);
        table.insert((name, course_taken), 1);

        // After first rehash: 10 * 0.75 = 7.5
        assert_eq!(4, table.taken_count[0]);
        assert_eq!(10, table.BUCKET_SIZE);

        let name = Field::StringField(String::from("Eva"));
        let course_taken = Field::IntField(85);
        table.insert((name, course_taken), 1);

        let name = Field::StringField(String::from("Frank"));
        let course_taken = Field::IntField(16);
        table.insert((name, course_taken), 1);

        let name = Field::StringField(String::from("Grant"));
        let course_taken = Field::IntField(63);
        table.insert((name, course_taken), 1);

        // before second rehash
        assert_eq!(7, table.taken_count[0]);
        assert_eq!(10, table.BUCKET_SIZE);

        let name = Field::StringField(String::from("Hilton"));
        let course_taken = Field::IntField(11);
        table.insert((name, course_taken), 1);

        // after second rehash
        assert_eq!(8, table.taken_count[0]);
        assert_eq!(20, table.BUCKET_SIZE);
    }

    // function to test hopscotch
    pub fn test_hopscotch() {
        let mut table = HashTable::new(
            13,
            1,
            HashFunction::FarmHash,
            HashScheme::Hopscotch,
            4,
            ExtendOption::ExtendBucketSize,
            1.0,
        );
        table.buckets[0][0].taken = true;
        table.buckets[0][0].key = (Field::StringField(String::from("M")), Field::IntField(0));
        table.buckets[0][1].taken = true;
        table.buckets[0][1].key = (Field::StringField(String::from("M")), Field::IntField(1));
        table.buckets[0][3].taken = true;
        table.buckets[0][3].key = (Field::StringField(String::from("M")), Field::IntField(3));
        table.hop_info[0][3] = 4; // 0100
        table.buckets[0][4].taken = true;
        table.buckets[0][4].key = (Field::StringField(String::from("M")), Field::IntField(4));
        table.buckets[0][5].taken = true;
        table.hop_info[0][5] = 10; // 1010
        table.buckets[0][6].taken = true;
        table.buckets[0][6].key = (Field::StringField(String::from("M")), Field::IntField(6));
        table.buckets[0][7].taken = true;
        table.buckets[0][7].key = (Field::StringField(String::from("M")), Field::IntField(7));
        table.hop_info[0][7] = 4; // 0100
        table.buckets[0][8].taken = true;
        table.buckets[0][8].key = (Field::StringField(String::from("M")), Field::IntField(8));
        table.buckets[0][9].taken = true;
        table.buckets[0][9].key = (Field::StringField(String::from("M")), Field::IntField(9));
        table.hop_info[0][9] = 4; // 0100
        table.buckets[0][10].taken = true;
        table.buckets[0][10].key = (Field::StringField(String::from("M")), Field::IntField(10));
        table.buckets[0][11].taken = true;
        table.buckets[0][11].key = (Field::StringField(String::from("M")), Field::IntField(11));
        table.taken_count[0] = 11;

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(8);
        // assert_eq!(table.get_indexes((&name, &course_taken)).unwrap().1, 3);
        table.insert((name, course_taken), 1);
        assert_eq!(table.hop_info[0][9], 1);
        assert_eq!(table.hop_info[0][7], 1);
        assert_eq!(table.hop_info[0][5], 3);
        assert_eq!(table.hop_info[0][3], 6);
        assert_eq!(table.buckets[0][12].taken, true);
        assert_eq!(table.taken_count[0], 12);
    }

    // function to test hopscotch
    pub fn test_hopscotch2() {
        let mut table = HashTable::new(
            100,
            1,
            HashFunction::FarmHash,
            HashScheme::Hopscotch,
            10,
            ExtendOption::ExtendBucketSize,
            0.75,
        );

        let name1 = Field::StringField(String::from("Adamdsf"));
        let course_taken1 = Field::IntField(0);
        table.insert((name1, course_taken1), 1);

        let name2 = Field::StringField(String::from("Bensdfsdfds"));
        let course_taken2 = Field::IntField(1);
        table.insert((name2, course_taken2), 1);

        let name3 = Field::StringField(String::from("Chrissdfds"));
        let course_taken3 = Field::IntField(1);
        table.insert((name3, course_taken3), 1);

        let name4 = Field::StringField(String::from("Daviddf"));
        let course_taken4 = Field::IntField(1);
        table.insert((name4, course_taken4), 1);

        let name5 = Field::StringField(String::from("Evadsfsdfsdfsdfsd"));
        let course_taken5 = Field::IntField(85);
        table.insert((name5, course_taken5), 1);

        let name6 = Field::StringField(String::from("Franksdf"));
        let course_taken6 = Field::IntField(16);
        table.insert((name6, course_taken6), 1);

        let name7 = Field::StringField(String::from("Grantsdf"));
        let course_taken7 = Field::IntField(63);
        table.insert((name7, course_taken7), 1);

        let name8 = Field::StringField(String::from("Hilton"));
        let course_taken8 = Field::IntField(11);
        table.insert((name8, course_taken8), 1);

        let name9 = Field::StringField(String::from("Idamsdfsdf"));
        let course_taken9 = Field::IntField(23);
        table.insert((name9, course_taken9), 1);

        let name10 = Field::StringField(String::from("Jendf"));
        let course_taken10 = Field::IntField(656);
        table.insert((name10, course_taken10), 1);

        let name11 = Field::StringField(String::from("Khrissdfs"));
        let course_taken11 = Field::IntField(989);
        table.insert((name11, course_taken11), 1);

        let name12 = Field::StringField(String::from("Lavid"));
        let course_taken12 = Field::IntField(45);
        // assert_eq!(table.get_indexes((&name12, &course_taken12)).unwrap().1, 8);
        table.insert((name12, course_taken12), 1);

        let name13 = Field::StringField(String::from("Mva"));
        let course_taken13 = Field::IntField(9879);
        table.insert((name13, course_taken13), 1);

        let name14 = Field::StringField(String::from("Nrank"));
        let course_taken14 = Field::IntField(454);
        table.insert((name14, course_taken14), 1);

        let name15 = Field::StringField(String::from("Osdafhj"));
        let course_taken15 = Field::StringField(String::from("Ohajd"));
        table.insert((name15, course_taken15), 1);

        let name16 = Field::StringField(String::from("Podfh"));
        let course_taken16 = Field::StringField(String::from("Pdfki"));
        table.insert((name16, course_taken16), 1);

        let name17 = Field::StringField(String::from("Qkdsfai"));
        let course_taken17 = Field::StringField(String::from("Qjidif"));
        table.insert((name17, course_taken17), 1);

        let name18 = Field::StringField(String::from("Rjksdf"));
        let course_taken18 = Field::StringField(String::from("Rkdsfi"));
        table.insert((name18, course_taken18), 1);

        let name19 = Field::StringField(String::from("Sjkdfi"));
        let course_taken19 = Field::StringField(String::from("Sjkdfi"));
        table.insert((name19, course_taken19), 1);

        let name20 = Field::StringField(String::from("Thsdud"));
        let course_taken20 = Field::StringField(String::from("Thjksdfi"));
        table.insert((name20, course_taken20), 1);
    }

    // function to test insert with robin hood scheme
    pub fn test_insert_robin_hood() {
        let mut table = HashTable::new(
            4,
            1,
            HashFunction::StdHash,
            HashScheme::RobinHood,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        // HN1 -> 0
        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(1);
        // let indexes = table.get_indexes((&name, &course_taken)).unwrap();
        // assert_eq!(indexes.1, 0);
        // assert_eq!(indexes.2, 0);
        table.insert((name, course_taken), 1);
        assert_eq!(table.buckets[0][0].key, (Field::StringField(String::from("Adam")), Field::IntField(1)));
        assert_eq!(table.buckets[0][0].dis, 0);

        // HN2 -> 1
        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(2);
        // let indexes = table.get_indexes((&name, &course_taken)).unwrap();
        // assert_eq!(indexes.1, 1);
        // assert_eq!(indexes.2, 0);
        table.insert((name, course_taken), 1);
        assert_eq!(table.buckets[0][1].key, (Field::StringField(String::from("Adam")), Field::IntField(2)));
        assert_eq!(table.buckets[0][1].dis, 0);
        assert_eq!(table.buckets[0][1].taken, true);

        // HN3 -> 1 -> 2
        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(6);
        let indexes3 = table.get_indexes((&name, &course_taken)).unwrap();
        assert_eq!(indexes3.1, 2);
        assert_eq!(indexes3.2, 1);
        table.insert((name, course_taken), 1);
        assert_eq!(table.buckets[0][2].key, (Field::StringField(String::from("Adam")), Field::IntField(6)));
        assert_eq!(table.buckets[0][2].dis, 1);
        assert_eq!(table.buckets[0][2].taken, true);

        // HN4 -> 0 -> 2
        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(0);
        let indexes3 = table.get_indexes((&name, &course_taken)).unwrap();
        assert_eq!(indexes3.1, 1);
        assert_eq!(indexes3.2, 1);
        table.insert((name, course_taken), 1);
        assert_eq!(table.buckets[0][1].key, (Field::StringField(String::from("Adam")), Field::IntField(0)));
        assert_eq!(table.buckets[0][1].dis, 1);
        assert_eq!(table.buckets[0][1].taken, true);

        // HN2 -> 1 -> 3
        assert_eq!(table.buckets[0][3].key, (Field::StringField(String::from("Adam")), Field::IntField(2)));
        assert_eq!(table.buckets[0][3].dis, 2);
        assert_eq!(table.buckets[0][3].taken, true);
    }

    // function to test basic functionality of Field
    pub fn test_field() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(1, f_int.unwrap_int_field());
        assert_eq!("Hello", f_str.unwrap_string_field());
    }

    // function to test basic functionality of user defined enum
    pub fn test_my_enum() {
        let s = HashFunction::FarmHash;
        match s{
            HashFunction::MurmurHash3 => { println!("Murmur3") },
            HashFunction::T1haHash => { println!("T1") },
            HashFunction::FarmHash => { println!("Farm") },
            HashFunction::StdHash => { println!("Std") },
        };
    }

    // function to test std hash function for Field
    pub fn test_std_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(1742378985846435984 as usize, f_int.std_hash());
        assert_eq!(12991522711919756218 as usize, f_str.std_hash());
    }

    // function to test farm hash function for Field
    pub fn test_farm_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        let f_str2 = Field::StringField(String::from("There"));
        let sum = f_int.farm_hash() + f_str2.farm_hash();
        assert_eq!(538479481099171624 as usize, f_int.farm_hash());
        assert_eq!(15404698994557526151 as usize, f_str.farm_hash());
    }

    // function to test murmur3 hash function for Field
    pub fn test_murmur3_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(854115492 as usize, f_int.murmur_hash3());
        assert_eq!(316307400 as usize, f_str.murmur_hash3());
    }

    // function to test t1ha function for Field
    pub fn test_t1ha_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(4348539232621042483 as usize, f_int.t1ha_hash());
        assert_eq!(3284986864571460951 as usize, f_str.t1ha_hash());
    }

    // function to test initialization and modification of HashNode
    pub fn test_hash_node() {
        // init a node object with default
        let mut node = HashNode::default();
        assert_eq!((Field::IntField(0), Field::IntField(0)), node.key);
        assert_eq!(0, node.value);
        assert_eq!(false, node.taken);
        assert_eq!(usize::MAX, node.dis);

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let hash_key = (name, course_taken);

        // modify the node object
        node = HashNode {key: hash_key, value: 1, taken: true, dis: 0};
        assert_eq!((Field::StringField(String::from("Mark")), Field::IntField(6)), node.key);
        assert_eq!(1, node.value);
        assert_eq!(true, node.taken);
        assert_eq!(0, node.dis);
    }

    // function to test initialization of HashTable
    pub fn test_table_new() {
        let table = HashTable::new(
            10,
            2,
            HashFunction::StdHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );
        assert_eq!(2, table.BUCKET_NUMBER);
        assert_eq!(10, table.BUCKET_SIZE);
        assert_eq!(vec![0; 2],table.taken_count);
        assert_eq!(2, table.buckets.len());
        assert_eq!(10, table.buckets[0].len());
        assert_eq!(false, table.buckets[0][0].taken);
        assert_eq!((Field::IntField(0), Field::IntField(0)), table.buckets[0][0].key);
        assert_eq!(0, table.buckets[0][0].value);
        assert_eq!(4, table.H);
    }

    // function to test get_bucket_index
    pub fn test_get_bucket_index() {
        let table = HashTable::new(
            10,
            2,
            HashFunction::MurmurHash3,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let hash_key = (&name, &course_taken);
        assert_eq!(table.get_bucket_index(hash_key).unwrap(), 1);
    }

    // function to test linear_probe
    pub fn test_linear_probe() {
        let mut table = HashTable::new(
            10,
            1,
            HashFunction::StdHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );
        table.buckets[0][0].taken = true;

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        assert_eq!(
            table.linear_probe((&name, &course_taken), 0, 0).unwrap(),
            1);

        table.buckets[0][1].key = (name, course_taken);
        table.buckets[0][1].taken = true;
        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        assert_eq!(
            table.linear_probe((&name, &course_taken), 0, 0).unwrap(),
            1);

        let name2 = Field::StringField(String::from("Jack"));
        let course_taken2 = Field::IntField(3);
        table.buckets[0][1].key = (name2, course_taken2);
        table.buckets[0][1].taken = true;
        assert_eq!(
            table.linear_probe((&name, &course_taken), 0, 0).unwrap(),
            2);
    }

    // function to test get_index
    pub fn test_get_indexes() {
        let mut table = HashTable::new(
            10,
            1,
            HashFunction::FarmHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );
        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);

        // table.buckets[0][9].taken = true;
        // table.buckets[0][0].taken = true;

        let indexes = table.get_indexes((&name, &course_taken));
        assert_eq!(0, indexes.unwrap().0);
        assert_eq!(9, indexes.unwrap().1);
        assert_eq!(0, indexes.unwrap().2);
    }

    // function to test get_mut_value
    pub fn test_get_mut_value() {
        let mut table = HashTable::new(
            10,
            1,
            HashFunction::FarmHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let indexes = table.get_indexes((&name, &course_taken)).unwrap();
        table.buckets[indexes.0][indexes.1].key = (name, course_taken);
        table.buckets[indexes.0][indexes.1].value = 1;

        let v = table.get_mut_value(
            (&Field::StringField(String::from("Mark")), &Field::IntField(6))).unwrap();
        let expected_v = 1 as usize;
        assert_eq!(&expected_v, v);
    }

    // function to test get_value
    pub fn test_get_value() {
        let mut table = HashTable::new(
            10,
            1,
            HashFunction::FarmHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let indexes = table.get_indexes((&name, &course_taken)).unwrap();
        table.buckets[indexes.0][indexes.1].key = (name, course_taken);
        table.buckets[indexes.0][indexes.1].value = 1;

        let v = table.get_mut_value(
            (&Field::StringField(String::from("Mark")), &Field::IntField(6))).unwrap();
        let expected_v = 1 as usize;
        assert_eq!(&expected_v, v);
    }

    // function to test insert
    pub fn test_insert() {
        let mut table = HashTable::new(
            10,
            2,
            HashFunction::T1haHash,
            HashScheme::LinearProbe,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        let name1 = Field::StringField(String::from("Mark"));
        let course_taken1 = Field::IntField(6);
        let indexes1 = table.get_indexes((&name1, &course_taken1)).unwrap();

        table.insert((name1, course_taken1), 1);
        assert_eq!(Field::StringField(String::from("Mark")), table.buckets[indexes1.0][indexes1.1].key.0);
        assert_eq!(Field::IntField(6), table.buckets[indexes1.0][indexes1.1].key.1);
        assert_eq!(1, table.buckets[indexes1.0][indexes1.1].value);
        assert_eq!(true, table.buckets[indexes1.0][indexes1.1].taken);
        assert_eq!(1, table.taken_count[indexes1.0]);

        let name1_2 = Field::StringField(String::from("Mark"));
        let course_taken1_2 = Field::IntField(6);
        table.insert((name1_2, course_taken1_2), 1);
        assert_eq!(Field::StringField(String::from("Mark")), table.buckets[indexes1.0][indexes1.1].key.0);
        assert_eq!(Field::IntField(6), table.buckets[indexes1.0][indexes1.1].key.1);
        assert_eq!(2, table.buckets[indexes1.0][indexes1.1].value);
        assert_eq!(true, table.buckets[indexes1.0][indexes1.1].taken);
        assert_eq!(1, table.taken_count[indexes1.0]);
    }

    // function to test robin_hood
    pub fn test_robin_hood() {
        let mut table = HashTable::new(
            10,
            1,
            HashFunction::StdHash,
            HashScheme::RobinHood,
            4,
            ExtendOption::ExtendBucketSize,
            0.9,
        );

        // HN1 -> 0
        let name = Field::StringField(String::from("Adam"));
        let course_taken = Field::IntField(6);
        let node = HashNode {key: (name, course_taken), value: 1, taken: true, dis: 0};
        table.buckets[0][0] = node;

        // HN2 -> 0 -> 1
        let name = Field::StringField(String::from("Ben"));
        let course_taken = Field::IntField(12);
        assert_eq!(
            table.robin_hood((&name, &course_taken), 0, 0).unwrap(),
            (1 as usize, 1 as usize));
        let node = HashNode {key: (name, course_taken), value: 1, taken: true, dis: 1};
        table.buckets[0][1] = node;

        // HN3 -> 1 -> 2
        let name = Field::StringField(String::from("Chris"));
        let course_taken = Field::IntField(1);
        assert_eq!(
            table.robin_hood((&name, &course_taken), 0, 1).unwrap(),
            (2 as usize, 1 as usize));
        let node = HashNode {key: (name, course_taken), value: 1, taken: true, dis: 1};
        table.buckets[0][2] = node;

        // HN4 -> 0 -> 2
        let name = Field::StringField(String::from("David"));
        let course_taken = Field::IntField(3);
        assert_eq!(
            table.robin_hood((&name, &course_taken), 0, 0).unwrap(),
            (2 as usize, 2 as usize));
        let node = HashNode {key: (name, course_taken), value: 1, taken: true, dis: 2};
        table.buckets[0][2] = node;

        // HN3 -> 1 -> 3
        let name = Field::StringField(String::from("Chris"));
        let course_taken = Field::IntField(1);
        assert_eq!(
            table.robin_hood((&name, &course_taken), 0, 1).unwrap(),
            (3 as usize, 2 as usize));
        let node = HashNode {key: (name, course_taken), value: 1, taken: true, dis: 2};
        table.buckets[0][3] = node;
    }

    mod hash {
        use super::*;

        #[test]
        fn t_extend() {
            test_extend();
        }

        #[test]
        fn t_hopscotch2() {
            test_hopscotch2();
        }

        #[test]
        fn t_hopscotch() {
            test_hopscotch();
        }

        #[test]
        fn t_insert_robin_hood() {
            test_insert_robin_hood();
        }

        #[test]
        fn t_robin_hood() {
            test_robin_hood();
        }

        #[test]
        fn t_insert() {
            test_insert();
        }

        #[test]
        fn t_get_value() {
            test_get_value();
        }

        #[test]
        fn t_get_mut_value() {
            test_get_mut_value();
        }

        #[test]
        fn t_get_indexes() {
            test_get_indexes();
        }

        #[test]
        fn t_linear_probe() {
            test_linear_probe();
        }

        #[test]
        fn t_field() {
            test_field();
        }

        #[test]
        fn t_my_enum() {
            test_my_enum();
        }

        #[test]
        fn t_std_hash() {
            test_std_hash();
        }

        #[test]
        fn t_farm_hash() {
            test_farm_hash();
        }

        #[test]
        fn t_murmur3_hash() {
            test_murmur3_hash();
        }

        #[test]
        fn t_t1ha_hash() {
            test_t1ha_hash();
        }

        #[test]
        fn t_hash_node() {
            test_hash_node();
        }

        #[test]
        fn t_table_new() {
            test_table_new();
        }

        #[test]
        fn t_get_bucket_index() {
            test_get_bucket_index();
        }

    }
}
