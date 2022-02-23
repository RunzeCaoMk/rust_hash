use std::fmt;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use fasthash::{farm, murmur3, xx};

/// For each of the dtypes, make sure that there is a corresponding field type.
// #[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
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

/// Different types of hash functions
#[derive(Clone, Copy)]
pub enum HashFunction {
    FarmHash,
    MurmurHash3,
    XXHash,
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

/// Hashable trait has three hash functions
pub trait Hashable {
    fn farmhash(&self) -> usize;
    fn murmurhash(&self) -> usize;
    fn xxhash(&self) -> usize;
}

/// Implementation for Field's Hashable trait
impl Hashable for Field {
    // using FarmHash 32-bit hash functions to get hash value
    fn farmhash(&self) -> usize {
        let mut result: usize = 0;
        match self {
            Field::IntField(x) => {
                result = farm::hash32(x.to_be_bytes()) as usize;
            },
            Field::StringField(x) => {
                result = farm::hash32(x.as_bytes()) as usize;
            },
        }
        result
    }

    // using MurmurHash3 32-bit hash functions to get hash value
    fn murmurhash(&self) -> usize {
        let mut result: usize = 0;
        match self {
            Field::IntField(x) => {
                result = murmur3::hash32(x.to_be_bytes()) as usize;
            },
            Field::StringField(x) => {
                result = murmur3::hash32(x.as_bytes()) as usize;
            },
        }
        result
    }

    // using xxHash 32-bit hash functions to get hash value
    fn xxhash(&self) -> usize {
        let mut result: usize = 0;
        match self {
            Field::IntField(x) => {
                result = xx::hash32(x.to_be_bytes()) as usize;
            },
            Field::StringField(x) => {
                result = xx::hash32(x.as_bytes()) as usize;
            },
        }
        result
    }
}

fn main() {
    println!("Hello, world!");
}
