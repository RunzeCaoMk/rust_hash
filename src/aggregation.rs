use std::ptr::swap;
use crate::common::{CrustyError, OpIterator, PredicateOp};
use crate::hash::{Field, HashTable, HashNode, HashFunction, HashScheme};

// Compares the fields of two tuples using a predicate.
pub struct JoinPredicate {
    left_index: usize,
    right_index: usize,
    op: PredicateOp
}

impl JoinPredicate {
    pub fn new(op: PredicateOp, left_index: usize, right_index: usize) -> Self {
        Self {
            left_index,
            right_index,
            op
        }
    }
}

/// Hash equi-join implementation.
pub struct HashEqJoin {
    open: bool,
    left_child: Vec<(Field,Field)>,
    right_child: Vec<(Field,Field)>,
    join_hash_table: HashTable,
    current_node: Option<HashNode>,
    current_bucket: Option<Vec<HashNode>>,
}

impl HashEqJoin {
    #[allow(dead_code)]
    pub fn new(
        l_child: Vec<(Field,Field)>,
        r_child: Vec<(Field,Field)>,
        bucket_number: usize,
        bucket_size: usize,
    ) -> Self {
        Self {
            open: false,
            left_child: l_child,
            right_child: r_child,
            join_hash_table: HashTable::new(bucket_size, bucket_number),
            current_node: None,
            current_bucket: None,
        }
    }

    pub fn join(&mut self, function: HashFunction, scheme: HashScheme) -> Vec<(Field, Field)> {
        let mut res = Vec::default();
        for tuple in self.left_child.clone() {
            self.join_hash_table.insert(tuple, 1, function, scheme);
        }
        for tuple in self.right_child.clone() {
            if self.join_hash_table.get_value((&tuple.0, &tuple.1), function, scheme) == Some(&(1 as usize)) {
                res.push(tuple);
            }
        }
        res
    }
}

#[cfg(test)]
mod test_agg {
    use super::*;

    /// Creates a Vec of (StringField, StringField) given a Vec of (&str, &str) 's
    pub fn create_vec_tuple(tuple_data: Vec<(&str, &str)>) -> Vec<(Field, Field)> {
        let mut tuples = Vec::new();
        for item in &tuple_data {
            let fields = (Field::StringField((*item.0).parse().unwrap()),
                          Field::StringField((*item.1).parse().unwrap()));
            tuples.push(fields);
        }
        tuples
    }

    // function to test initialize a HashEqJoin
    pub fn test_new() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );
        assert_eq!(h_e_join.open, false);
        assert_eq!(h_e_join.left_child.len(), 4);
        assert_eq!(h_e_join.right_child.len(), 4);
    }

    // function to test join a HashEqJoin
    pub fn test_join() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let mut h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );
        let res_farm = h_e_join.join(HashFunction::FarmHash, HashScheme::LinearProbe);
        let res_murmur = h_e_join.join(HashFunction::MurmurHash3, HashScheme::LinearProbe);
        let res_std = h_e_join.join(HashFunction::StdHash, HashScheme::LinearProbe);
        let res_t1ha = h_e_join.join(HashFunction::T1haHash, HashScheme::LinearProbe);
        // assert_eq!(res[0], (Field::StringField(String::from("CS")), Field::StringField(String::from("Adam"))));
    }

    mod join {
        use super::*;

        #[test]
        fn t_new() {
            test_new();
        }

        #[test]
        fn t_join() {
            test_join();
        }
    }
}