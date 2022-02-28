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

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        // self.left_child.open()?;
        // let field_num = self.predicate.left_index();
        // while let Some(t) = self.left_child.next()? {
        //     let field = t.get_field(field_num).unwrap();
        //     let entry = self.join_map.entry(field.clone()).or_insert_with(Vec::new);
        //     entry.push(t);
        // }
        // self.left_child.close()?;
        // self.right_child.open()?;
        // self.current_tuple = self.right_child.next()?;
        // self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<HashNode>, CrustyError> {
//         if !self.open {
//             panic!("Operator has not been opened")
//         }
//
        let mut res = None;
//         while let Some(t2) = &self.current_tuple {
//             let field = t2.get_field(self.predicate.right_index()).unwrap();
//             if let Some(tuples) = self.join_map.get(field) {
//                 if self.current_bucket.is_none() {
//                     let mut ti =
//                         TupleIterator::new(tuples.to_vec(), self.left_child.get_schema().clone());
//                     ti.open()?;
//                     self.current_bucket = Some(ti);
//                 }
//                 if let Some(t1) = self.current_bucket.as_mut().unwrap().next()? {
//                     res = Some(t1.merge(t2));
//                     break;
//                 }
//                 self.current_bucket = None;
//             }
//             self.current_tuple = self.right_child.next()?;
//         }
        Ok(res)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.right_child = Vec::default();
        self.join_hash_table = HashTable::default();
        self.current_node = None;
        self.current_bucket = None;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.close()?;
        self.open()
    }

}

#[cfg(test)]
mod test_join {
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

    // function to test join a HashEqJoin using FarmHash
    pub fn test_join_farm() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let mut h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );
        let res_farm = h_e_join.join(HashFunction::FarmHash, HashScheme::LinearProbe);

        let dep = Field::StringField(String::from("CS"));

        assert_eq!(res_farm.len(), 3);
        assert_eq!(res_farm[0], (dep.clone(), Field::StringField(String::from("Adam"))));
        assert_eq!(res_farm[1], (dep.clone(), Field::StringField(String::from("Ben"))));
        assert_eq!(res_farm[2], (dep.clone(), Field::StringField(String::from("Chris"))));

    }

    // function to test join a HashEqJoin using MurmurHash3
    pub fn test_join_murmur3() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let mut h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );

        let res_murmur = h_e_join.join(HashFunction::MurmurHash3, HashScheme::LinearProbe);

        let dep = Field::StringField(String::from("CS"));

        assert_eq!(res_murmur.len(), 3);
        assert_eq!(res_murmur[0], (dep.clone(), Field::StringField(String::from("Adam"))));
        assert_eq!(res_murmur[1], (dep.clone(), Field::StringField(String::from("Ben"))));
        assert_eq!(res_murmur[2], (dep.clone(), Field::StringField(String::from("Chris"))));
    }

    // function to test join a HashEqJoin using std::hash
    pub fn test_join_std() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let mut h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );

        let res_std = h_e_join.join(HashFunction::StdHash, HashScheme::LinearProbe);

        let dep = Field::StringField(String::from("CS"));

        assert_eq!(res_std.len(), 3);
        assert_eq!(res_std[0], (dep.clone(), Field::StringField(String::from("Adam"))));
        assert_eq!(res_std[1], (dep.clone(), Field::StringField(String::from("Ben"))));
        assert_eq!(res_std[2], (dep.clone(), Field::StringField(String::from("Chris"))));

    }

    // function to test join a HashEqJoin using T1haHash
    pub fn test_join_t1ha() {
        let l_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "David")]);
        let r_child = create_vec_tuple(
            vec![("CS", "Adam"), ("CS", "Ben"), ("CS", "Chris"), ("CS", "Eva"), ("CS", "Fordham")]);
        let b_number = 2 as usize;
        let b_size = 10 as usize;
        let mut h_e_join = HashEqJoin::new(
            l_child,
            r_child,
            b_number,
            b_size
        );

        let res_t1ha = h_e_join.join(HashFunction::T1haHash, HashScheme::LinearProbe);

        let dep = Field::StringField(String::from("CS"));

        assert_eq!(res_t1ha.len(), 3);
        assert_eq!(res_t1ha[0], (dep.clone(), Field::StringField(String::from("Adam"))));
        assert_eq!(res_t1ha[1], (dep.clone(), Field::StringField(String::from("Ben"))));
        assert_eq!(res_t1ha[2], (dep.clone(), Field::StringField(String::from("Chris"))));
    }

    mod join {
        use super::*;

        #[test]
        fn t_new() {
            test_new();
        }

        #[test]
        fn t_join_farm() {
            test_join_farm();
        }

        #[test]
        fn t_join_murmur3() {
            test_join_murmur3();
        }

        #[test]
        fn t_join_std() {
            test_join_std();
        }

        #[test]
        fn t_join_t1ha() {
            test_join_t1ha();
        }
    }
}