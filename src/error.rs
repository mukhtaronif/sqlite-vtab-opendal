//! Error types for the library
//!
//! This module defines all error types that can occur when using the virtual table.
//! We use `thiserror` for ergonomic error handling and automatic Display/Error implementations.

use thiserror::Error;

/// The main error type for this library
///
/// This enum covers all error cases that can occur during virtual table operations,
/// from configuration issues to storage backend errors.
#[derive(Error, Debug)]
pub enum VTableError {
    /// Error from the underlying OpenDAL storage layer (boxed to reduce enum size)
    #[error("Storage backend error: {0}")]
    OpenDal(Box<opendal::Error>),

    /// Error from SQLite operations
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Invalid configuration provided
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Missing required credential or parameter
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),

    /// Invalid path format
    #[error("Invalid path: {0}")]
    InvalidPath(String),

    /// Error during async operation
    #[error("Async operation failed: {0}")]
    AsyncError(String),

    /// Generic error with custom message
    #[error("{0}")]
    Custom(String),
}

/// Convenience Result type for this library
pub type Result<T> = std::result::Result<T, VTableError>;

impl From<opendal::Error> for VTableError {
    /// Convert OpenDAL error to VTableError (boxing it to reduce size)
    fn from(err: opendal::Error) -> Self {
        VTableError::OpenDal(Box::new(err))
    }
}

impl From<VTableError> for rusqlite::Error {
    /// Convert our error type to rusqlite::Error for use in virtual table callbacks
    fn from(err: VTableError) -> Self {
        rusqlite::Error::ModuleError(err.to_string())
    }
}
