use std::fmt;

use crate::query::DecodeError;

/// Error type for query execution and connection handling.
#[derive(Debug, Clone)]
pub enum Error {
    /// The database driver reported an error.
    Backend(String),
    /// The connection worker thread is no longer running.
    WorkerGone,
    /// A row value could not be decoded into the requested type.
    Decode(DecodeError),
    /// The query was rejected before reaching the database.
    InvalidQuery(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Backend(message) => write!(f, "{message}"),
            Error::WorkerGone => write!(f, "database worker is unavailable"),
            Error::Decode(error) => write!(f, "{error}"),
            Error::InvalidQuery(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Self {
        Error::Decode(error)
    }
}
