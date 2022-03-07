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
#[derive(Clone, Copy)]
pub enum HashFunction {
    FarmHash,
    MurmurHash3,
    T1haHash,
    StdHash,
}

/// Different types of hash schemes
#[derive(Clone, Copy)]
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

/// TODO: load_factor
/// HashTable contains vec of hash buckets
pub struct HashTable {
    pub(crate) buckets: Vec<Vec<HashNode>>,
    pub(crate) taken_count: Vec<usize>,
    pub(crate) BUCKET_NUMBER: usize,
    pub(crate) BUCKET_SIZE: usize,
    pub(crate) function: HashFunction,
    pub(crate) scheme: HashScheme,
    // load_factor: usize,
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
        }
    }
}

impl HashTable {
    // initialize a new hash table with certain BUCKET_SIZE and BUCKET_NUMBER, HashFunction and HashScheme
    pub fn new(b_size: usize, b_num: usize, func: HashFunction, sche: HashScheme) -> Self {
        Self {
            buckets: vec![vec![HashNode::default(); b_size]; b_num],
            taken_count: vec![0; b_num],
            BUCKET_NUMBER: b_num,
            BUCKET_SIZE: b_size,
            function: func,
            scheme: sche,
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
    fn linear_probe(&self, key: (&Field, &Field), target_bucket_index: usize, index: usize) -> Option<usize> {
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

    // TODO: method to use hopscotch hashing to resolve collision
    fn hopscotch(&self, key: (&Field, &Field), target_bucket_index: usize, index: usize) -> Option<usize> {
        None
    }

    // method to use robin hood hashing to resolve collision
    fn robin_hood(&self, key: (&Field, &Field), bucket_index: usize, ori_index: usize) -> Option<(usize, usize)> {
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

    // method to get a tuple of (bucket_index, index)
    fn get_indexes(&self, key: (&Field, &Field)) -> Option<(usize, usize, usize)> {
        // get target bucket index
        let bucket_index = self.get_bucket_index(key).unwrap();

        // using different hash functions to get the index in one bucket
        let mut index = match self.function {
            HashFunction::FarmHash => {
                (key.0.farm_hash() % 10 + key.1.farm_hash() % 10) % self.BUCKET_SIZE
            },
            HashFunction::MurmurHash3 => {
                (key.0.murmur_hash3() % 10 + key.1.murmur_hash3() % 10) % self.BUCKET_SIZE
            },
            HashFunction::T1haHash => {
                (key.0.t1ha_hash() % 10 + key.1.t1ha_hash() % 10) % self.BUCKET_SIZE
            },
            HashFunction::StdHash => {
                (key.0.std_hash() % 10 + key.1.std_hash() % 10) % self.BUCKET_SIZE
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
                    index = self.hopscotch(key, bucket_index, index).unwrap();
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
    pub fn get_value(&self, key: (&Field, &Field)) -> Option<&usize> {
        if let Some(indexes) = self.get_indexes(key) {
            Some(&self.buckets[indexes.0][indexes.1].value)
        } else {
            println!("Couldn't get value");
            None
        }
    }

    // method to insert a new HashNode
    pub fn insert(&mut self, new_key: (Field, Field), new_value: usize) {
        // get the tuple of (bucket_index, index)
        if let Some(indexes) =
        self.get_indexes((&new_key.0, &new_key.1)){
            // check if the the key is already existed in the table
            if self.buckets[indexes.0][indexes.1].key == new_key {
                // add new value to the old one
                self.buckets[indexes.0][indexes.1].value += new_value;
            } else if self.buckets[indexes.0][indexes.1].taken == false { // if not been taken
                // directly insert the new value
                self.buckets[indexes.0][indexes.1] = HashNode {key: new_key, value: new_value, taken: true, dis: indexes.2 };
                self.taken_count[indexes.0] += 1;
            } else { // if the slot already been taken
                // insert the new value and insert the origin value
                let ori_node = self.buckets[indexes.0][indexes.1].clone();
                self.buckets[indexes.0][indexes.1] = HashNode {key: new_key, value: new_value, taken: true, dis: indexes.2 };
                self.insert(ori_node.key, ori_node.value);
            }
        };
    }

    // method to extend the bucket number / bucket size and then rehash the table
    pub fn extend(&mut self, op: ExtendOption) {
        assert!(self.buckets.len() > 0);
        let mut new_self = match op {
            // extend the bucket size to twice of the original bucket size
            ExtendOption::ExtendBucketSize => {
                Self {
                    buckets: vec![vec![HashNode::default(); self.BUCKET_SIZE * 2]; self.BUCKET_NUMBER],
                    taken_count: vec![0; self.BUCKET_NUMBER],
                    BUCKET_SIZE: self.BUCKET_SIZE * 2,
                    BUCKET_NUMBER: self.BUCKET_NUMBER,
                    function: self.function,
                    scheme: self.scheme,
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

    // function to test hopscotch
    pub fn test_hopscotch() {

    }

    // function to test insert with robin hood scheme
    pub fn test_insert_robin_hood() {
        let mut table = HashTable::new(4, 1, HashFunction::StdHash, HashScheme::RobinHood);

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
        let table = HashTable::new(10, 2, HashFunction::StdHash, HashScheme::LinearProbe);
        assert_eq!(2, table.BUCKET_NUMBER);
        assert_eq!(10, table.BUCKET_SIZE);
        assert_eq!(vec![0; 2],table.taken_count);
        assert_eq!(2, table.buckets.len());
        assert_eq!(10, table.buckets[0].len());
        assert_eq!(false, table.buckets[0][0].taken);
        assert_eq!((Field::IntField(0), Field::IntField(0)), table.buckets[0][0].key);
        assert_eq!(0, table.buckets[0][0].value);
    }

    // function to test get_bucket_index
    pub fn test_get_bucket_index() {
        let table = HashTable::new(10, 2, HashFunction::MurmurHash3, HashScheme::LinearProbe);

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let hash_key = (&name, &course_taken);
        assert_eq!(table.get_bucket_index(hash_key).unwrap(), 1);
    }

    // function to test linear_probe
    pub fn test_linear_probe() {
        let mut table = HashTable::new(10, 1, HashFunction::StdHash, HashScheme::LinearProbe);
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
        let mut table = HashTable::new(10, 1, HashFunction::FarmHash, HashScheme::LinearProbe);
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
        let mut table = HashTable::new(10, 1, HashFunction::FarmHash, HashScheme::LinearProbe);

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
        let mut table = HashTable::new(10, 1, HashFunction::FarmHash, HashScheme::LinearProbe);

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
        let mut table = HashTable::new(10, 2, HashFunction::T1haHash, HashScheme::LinearProbe);

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

    // function to test extend
    pub fn test_extend() {
        let mut table = HashTable::new(10, 1, HashFunction::FarmHash, HashScheme::LinearProbe);
        let name1 = Field::StringField(String::from("Mark"));
        let course_taken1 = Field::IntField(6);
        table.insert((name1, course_taken1), 1);
        assert_eq!(1, table.taken_count[0]);

        table.extend(ExtendOption::ExtendBucketNumber);
        assert_eq!(2, table.buckets.len());
        assert_eq!(2, table.BUCKET_NUMBER);
        assert_eq!(1, table.taken_count[1]);

        let name1 = Field::StringField(String::from("Jenny"));
        let course_taken1 = Field::IntField(12);
        table.insert((name1, course_taken1), 1);

        table.extend(ExtendOption::ExtendBucketSize);
        assert_eq!(20, table.buckets[0].len());
        assert_eq!(20, table.buckets[1].len());
        // assert_eq!(20, table.buckets[2].len());
        // assert_eq!(20, table.buckets[3].len());
        assert_eq!(20, table.BUCKET_SIZE);
    }

    // function to test robin_hood
    pub fn test_robin_hood() {
        let mut table = HashTable::new(10, 1, HashFunction::StdHash, HashScheme::RobinHood);

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
        fn t_extend() {
            test_extend();
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
