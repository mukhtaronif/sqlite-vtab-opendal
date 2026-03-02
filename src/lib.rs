//! # sqlite-vtable-opendal
//!
//! Federated SQLite Virtual Tables for Cloud Object Stores using OpenDAL
//!
//! This library provides a lightweight way to query metadata from cloud object stores
//! (Dropbox, S3, etc.) using SQL without ingesting the data. It uses OpenDAL as the
//! storage abstraction layer and SQLite's virtual table interface.
//!
//! ## Features
//!
//! - **Metadata-only queries**: Query file metadata without downloading content
//! - **Multiple backends**: Dropbox, S3, Google Drive, Local FS, and more
//! - **Standard SQL**: Use familiar SQL syntax for cloud storage queries
//! - **Pagination support**: Handle large directories efficiently
//! - **Zero data ingestion**: Query directly from cloud without materialization
//!
//! ## Example
//!
//! ```rust,ignore
//! use rusqlite::Connection;
//!
//! let conn = Connection::open_in_memory()?;
//!
//! // Backend implementations will provide their own registration functions
//! // For example with Dropbox backend:
//! // dropbox_vtab::register(&conn)?;
//!
//! // Query cloud storage metadata using standard SQL
//! let mut stmt = conn.prepare("
//!     SELECT path, size FROM dropbox_files
//!     WHERE size > 10000000
//!     ORDER BY size DESC
//! ")?;
//! # Ok::<(), rusqlite::Error>(())
//! ```

pub mod types;
pub mod error;
pub mod backends;
pub mod vtab;

// Re-export commonly used types
pub use types::{FileMetadata, QueryConfig};
pub use error::{VTableError, Result};
