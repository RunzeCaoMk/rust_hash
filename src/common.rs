use serde::{Deserialize, Serialize};
use crate::hash::HashNode;
use std::{fmt, io};
use std::error::Error;

/// Predicate operators.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum PredicateOp {
    Equals,
    GreaterThan,
    LessThan,
    LessThanOrEq,
    GreaterThanOrEq,
    NotEq,
    All,
}

impl PredicateOp {
    /// Do predicate comparison.
    ///
    /// # Arguments
    ///
    /// * `left_field` - Left field of the predicate.
    /// * `right_field` - Right field of the predicate.
    pub fn compare<T: Ord>(&self, left_field: &T, right_field: &T) -> bool {
        match self {
            PredicateOp::Equals => left_field == right_field,
            PredicateOp::GreaterThan => left_field > right_field,
            PredicateOp::LessThan => left_field < right_field,
            PredicateOp::LessThanOrEq => left_field <= right_field,
            PredicateOp::GreaterThanOrEq => left_field >= right_field,
            PredicateOp::NotEq => left_field != right_field,
            PredicateOp::All => true,
        }
    }

    /// Flip the operator.
    pub fn flip(&self) -> Self {
        match self {
            PredicateOp::GreaterThan => PredicateOp::LessThan,
            PredicateOp::LessThan => PredicateOp::GreaterThan,
            PredicateOp::LessThanOrEq => PredicateOp::GreaterThanOrEq,
            PredicateOp::GreaterThanOrEq => PredicateOp::LessThanOrEq,
            op => *op,
        }
    }
}

pub trait OpIterator {
    /// Opens the iterator. This must be called before any of the other methods.
    fn open(&mut self) -> Result<(), CrustyError>;

    /// Advances the iterator and returns the next tuple from the operator.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn next(&mut self) -> Result<Option<HashNode>, CrustyError>;

    /// Closes the iterator.
    fn close(&mut self) -> Result<(), CrustyError>;

    /// Returns the iterator to the start.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn rewind(&mut self) -> Result<(), CrustyError>;
}

/// Custom error type.
#[derive(Debug, Clone, PartialEq)]
pub enum CrustyError {
    /// IO Errors.
    IOError(String),
    /// Custom errors.
    CrustyError(String),
    /// Validation errors.
    ValidationError(String),
    /// Execution errors.
    ExecutionError(String),
    /// Transaction aborted.
    TransactionAbortedError,
}

impl fmt::Display for CrustyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                CrustyError::IOError(s) => s.to_string(),
                CrustyError::TransactionAbortedError => String::from("Transaction Aborted Error"),
            }
        )
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for CrustyError {
    fn from(error: io::Error) -> Self {
        CrustyError::IOError(error.to_string())
    }
}

impl Error for CrustyError {}