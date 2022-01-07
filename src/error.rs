use std::error::Error;

/// Enum with all errors in this crate.
pub enum S3Error {
    /// Returned when functionaly is not yet available.
    NotImplemented(String),
    /// Wrapper for IO errors
    Io(std::io::Error),
    /// Wrapper for AWS errors
    AWS(std::io::Error),
}

impl From<io::Error> for S3Error {
    fn from(err: io::Error) -> Self {
        S3Error::IoError(err)
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            S3Error::NotImplemented(desc) => write!(f, "Not yet implemented: {}", desc),
            S3Error::Io(desc) => {
                write!(f, "IO error: {}", desc)
            }
        }
    }
}

impl error::Error for S3Error {}

/// Typedef for a [`std::result::Result`] of an [`S3Error`].
pub type Result<T> = std::result::Result<T, S3Error>;