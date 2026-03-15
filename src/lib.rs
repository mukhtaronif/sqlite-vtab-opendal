//! # sqlite-vtable-opendal
//!
//! Federated SQLite Virtual Tables for Cloud Object Stores using OpenDAL
//!
//! This library provides a lightweight way to query metadata from cloud object stores
//! (Dropbox, S3, Google Drive, PostgreSQL, HTTP) using SQL without ingesting the data.
//! It uses OpenDAL as the storage abstraction layer and SQLite's virtual table interface.
//!
//! ## Features
//!
//! - **Metadata-only queries**: Query file metadata without downloading content
//! - **6 storage backends**: Local FS, S3, Dropbox, Google Drive, PostgreSQL, HTTP
//! - **Standard SQL**: Use familiar SQL syntax for cloud storage queries
//! - **Pagination support**: Handle large directories efficiently with limit/offset
//! - **Zero data ingestion**: Query directly from storage without materialization
//! - **Async support**: Non-blocking operations via Tokio runtime
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rusqlite::Connection;
//! use sqlite_vtable_opendal::backends::local_fs;
//!
//! # fn main() -> rusqlite::Result<()> {
//! let conn = Connection::open_in_memory()?;
//!
//! // Register virtual table for local filesystem
//! local_fs::register(&conn, "my_files", "/tmp")?;
//!
//! // Query using standard SQL
//! let mut stmt = conn.prepare(
//!     "SELECT path, size FROM my_files
//!      WHERE size > 1000000
//!      ORDER BY size DESC"
//! )?;
//!
//! let files = stmt.query_map([], |row| {
//!     Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
//! })?;
//!
//! for file in files {
//!     let (path, size) = file?;
//!     println!("{}: {} bytes", path, size);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Available Backends
//!
//! - [`backends::local_fs`] - Local filesystem
//! - [`backends::s3`] - AWS S3 (and compatible services)
//! - [`backends::dropbox`] - Dropbox
//! - [`backends::gdrive`] - Google Drive
//! - [`backends::postgresql`] - PostgreSQL databases
//! - [`backends::http`] - HTTP/HTTPS endpoints

pub mod types;
pub mod error;
pub mod backends;
pub mod vtab;

// Re-export commonly used types
pub use types::{FileMetadata, QueryConfig};
pub use error::{VTableError, Result};
