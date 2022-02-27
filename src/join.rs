use crate::common::{CrustyError, OpIterator, PredicateOp};
use crate::hash::{Field, HashTable, HashNode};

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
    predicate: JoinPredicate,
    open: bool,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    join_hash_table: HashTable,
    current_node: Option<HashNode>,
    current_bucket: Option<Vec<HashNode>>,
}

impl HashEqJoin {
    #[allow(dead_code)]
    pub fn new(
        op: PredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
        bucket_number: usize,
        bucket_size: usize,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            open: false,
            left_child,
            right_child,
            join_hash_table: HashTable::new(bucket_size, bucket_number),
            current_node: None,
            current_bucket: None,
        }
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
//         self.left_child.open()?;
//         let field_num = self.predicate.left_index();
//         while let Some(t) = self.left_child.next()? {
//             let field = t.get_field(field_num).unwrap();
//             let entry = self.join_map.entry(field.clone()).or_insert_with(Vec::new);
//             entry.push(t);
//         }
//         self.left_child.close()?;
//         self.right_child.open()?;
//         self.current_tuple = self.right_child.next()?;
//         self.open = true;
        Ok(())
    }
//
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
        self.right_child.close()?;
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