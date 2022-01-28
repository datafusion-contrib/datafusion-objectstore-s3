use std::error::Error;
use std::fmt::{Display, Formatter};

/// Enum with all errors in this crate.
/// PartialEq is to enable testing for specific error types
#[derive(Debug, PartialEq)]
pub enum S3Error {
    /// Returned when functionaly is not yet available.
    NotImplemented(String),
    /// Wrapper for AWS errors
    AWS(String),
}

impl Display for S3Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::NotImplemented(desc) => write!(f, "Not yet implemented: {}", desc),
            S3Error::AWS(desc) => write!(f, "AWS error: {}", desc),
        }
    }
}

impl Error for S3Error {}
