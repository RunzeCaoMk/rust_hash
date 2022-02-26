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
struct HashNode {
    key: (Field, Field),
    value: usize,
    taken: bool,
}

/// Implementation for HashNode's default trait
impl Default for HashNode {
    fn default() -> HashNode {
        HashNode {
            key: (Field::default(), Field::default()),
            value: 0,
            taken: false,
        }
    }
}

/// TODO: load_factor
/// HashTable contains vec of hash buckets
pub struct HashTable {
    buckets: Vec<Vec<HashNode>>,
    taken_count: Vec<usize>,
    BUCKET_NUMBER: usize,
    BUCKET_SIZE: usize,
    // load_factor: usize,
}

impl HashTable {
    // initialize a new hash table with certain BUCKET_SIZE and BUCKET_NUMBER
    pub fn new(b_size: usize, b_num: usize) -> Self {
        Self {
            buckets: vec![vec![HashNode::default(); b_size]; b_num],
            taken_count: vec![0; b_num],
            BUCKET_NUMBER: b_num,
            BUCKET_SIZE: b_size,
        }
    }

    // method to get the specific bucket base on the key
    fn get_bucket_index(&self, key: (&Field, &Field), function: HashFunction) -> Option<usize> {
        // using different hash functions to get the index for bucket
        let bucket_index = match function {
            HashFunction::MurmurHash3 => {
                (key.0.murmur_hash3() + key.1.murmur_hash3()) % self.BUCKET_NUMBER
            },
            HashFunction::FarmHash => {
                (key.0.farm_hash() + key.1.farm_hash()) % self.BUCKET_NUMBER
            },
            HashFunction::T1haHash => {
                (key.0.t1ha_hash() + key.1.t1ha_hash()) % self.BUCKET_NUMBER
            },
            HashFunction::StdHash => {
                (key.0.std_hash() + key.1.std_hash()) % self.BUCKET_NUMBER
            },
        };
        // check if the bucket is full and return bucket_index
        if self.taken_count[bucket_index] >= self.BUCKET_SIZE {
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
            if !self.buckets[target_bucket_index][i].taken {
                break;
            }
            if (&self.buckets[target_bucket_index][i].key.0,
                &self.buckets[target_bucket_index][i].key.1) == key {
                break;
            }
            i = (i + 1) % self.BUCKET_SIZE;
        }
        Some(i)
    }

    // method to get the hash index
    fn get_index(&self, key: &Key, function: HashFunction, scheme: HashScheme) -> Option<Vec<usize>> {
        // get target bucket index
        let Some(bucket_index) = get_bucket_index(key, function)?;
        // get the target bucket
        let target_bucket = self.buckets[bucket_index];

        // using different hash functions to get the index in one bucket
        let mut index = match function {
            HashFunction::MurmurHash3 => {
                key.murmurhash() % self.BUCKET_SIZE
            },
            HashFunction::FarmHash => {
                key.farmhash() % self.BUCKET_SIZE
            },
            HashFunction::XXHash => {
                key.xxhash() % self.BUCKET_SIZE
            },
        };

        // check if the index has been taken
        if target_bucket[index].taken {
            // using different hashing scheme to solve duplicate
            index = match scheme {
                HashScheme::LinearProbe => {
                    linear_probe(target_bucket, index)
                },
                HashScheme::Hopscotch => {
                    hopscotch(target_bucket, index)
                },
                HashScheme::RobinHood => {
                    robin_hood(target_bucket, index)
                },
            };
        }

        // return the bucket_index and index
        Some(vec![bucket_index, index])

        // if target_bucket[index].taken && target_bucket[index].key == *key {
        //     Some(vec![bucket_index, index])
        // } else {
        //     None
        // }
    }
}

fn main() {
    // function to test basic functionality of Field
    fn test_field() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(1, f_int.unwrap_int_field());
        assert_eq!("Hello", f_str.unwrap_string_field());
    }

    // function to test basic functionality of user defined enum
    fn test_my_enum() {
        let s = HashFunction::FarmHash;
        match s{
            HashFunction::MurmurHash3 => { println!("Murmur3") },
            HashFunction::T1haHash => { println!("T1") },
            HashFunction::FarmHash => { println!("Farm") },
            HashFunction::StdHash => { println!("Std") },
        };
    }

    // function to test std hash function for Field
    fn test_std_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(1742378985846435984 as usize, f_int.std_hash());
        assert_eq!(12991522711919756218 as usize, f_str.std_hash());
    }

    // function to test farm hash function for Field
    fn test_farm_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(538479481099171624 as usize, f_int.farm_hash());
        assert_eq!(15404698994557526151 as usize, f_str.farm_hash());
    }

    // function to test murmur3 hash function for Field
    fn test_murmur3_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(854115492 as usize, f_int.murmur_hash3());
        assert_eq!(316307400 as usize, f_str.murmur_hash3());
    }

    // function to test t1ha function for Field
    fn test_t1ha_hash() {
        let f_int = Field::IntField(1);
        let f_str = Field::StringField(String::from("Hello"));
        assert_eq!(4348539232621042483 as usize, f_int.t1ha_hash());
        assert_eq!(3284986864571460951 as usize, f_str.t1ha_hash());
    }

    // function to test initialization and modification of HashNode
    fn test_hash_node() {
        // init a node object with default
        let mut node = HashNode::default();
        assert_eq!(false, node.taken);
        assert_eq!((Field::IntField(0), Field::IntField(0)), node.key);
        assert_eq!(0, node.value);

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let hash_value = name.std_hash() + course_taken.std_hash();
        let hash_key = (name, course_taken);

        // modify the node object
        node = HashNode {key: hash_key, value: hash_value, taken: true};
        assert_eq!((Field::StringField(String::from("Mark")), Field::IntField(6)), node.key);
        assert_eq!(6821831411456522797, node.value);
        assert_eq!(true, node.taken);
    }

    // function to test initialization of HashTable
    fn test_table_new() {
        let table = HashTable::new(10, 2);
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
    fn test_get_bucket_index() {
        let table = HashTable::new(10, 2);

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        let hash_key = (&name, &course_taken);
        assert_eq!(table.get_bucket_index(hash_key, HashFunction::MurmurHash3).unwrap(), 1);
    }

    // function to test linear_probe
    fn test_linear_probe() {
        let mut table = HashTable::new(10, 1);
        table.buckets[0][0].taken = true;

        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        assert_eq!(table.linear_probe((&name, &course_taken), 0, 0).unwrap(), 1);

        table.buckets[0][1].key = (name, course_taken);
        table.buckets[0][1].taken = true;
        let name = Field::StringField(String::from("Mark"));
        let course_taken = Field::IntField(6);
        assert_eq!(table.linear_probe((&name, &course_taken), 0, 0).unwrap(), 1);

        let name2 = Field::StringField(String::from("Jack"));
        let course_taken2 = Field::IntField(3);
        table.buckets[0][1].key = (name2, course_taken2);
        table.buckets[0][1].taken = true;
        assert_eq!(table.linear_probe((&name, &course_taken), 0, 0).unwrap(), 2);
    }

    // function to
    fn test_get_index() {

    }
    // Testing!
    test_get_index();
    // test_linear_probe();
    // test_get_bucket_index();
    // test_table_new();
    // test_hash_node();
    // test_t1ha_hash();
    // test_murmur3_hash();
    // test_farm_hash();
    // test_std_hash();
    // test_func_enum();
    // test_field();
}
