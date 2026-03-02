//! Core types and data structures for the virtual table
//!
//! This module defines the fundamental types used throughout the library:
//! - `FileMetadata`: Represents a file or directory in cloud storage
//! - `QueryConfig`: Configuration for how queries should be executed

use serde::{Deserialize, Serialize};

/// Represents metadata for a file or directory in cloud storage
///
/// This struct contains all the information that can be queried through
/// the virtual table SQL interface. It's designed to be lightweight by default,
/// only fetching file contents when explicitly requested.
///
/// # Examples
///
/// ```
/// use sqlite_vtable_opendal::types::FileMetadata;
///
/// let file = FileMetadata {
///     name: "document.pdf".to_string(),
///     path: "/docs/document.pdf".to_string(),
///     size: 1024000,
///     last_modified: Some("2024-01-15T10:30:00Z".to_string()),
///     etag: Some("abc123".to_string()),
///     is_dir: false,
///     content_type: Some("application/pdf".to_string()),
///     content: None, // Not fetched by default
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// The name of the file or directory (without path)
    pub name: String,

    /// The full path of the file or directory
    pub path: String,

    /// The size of the file in bytes (0 for directories)
    pub size: u64,

    /// ISO 8601 formatted timestamp of last modification
    pub last_modified: Option<String>,

    /// ETag or content hash (MD5, SHA256, etc.)
    pub etag: Option<String>,

    /// Whether this entry is a directory
    pub is_dir: bool,

    /// MIME type or file extension
    pub content_type: Option<String>,

    /// Actual file content (only populated when explicitly requested)
    /// This is None by default to avoid unnecessary data transfer
    pub content: Option<Vec<u8>>,
}

/// Configuration for querying file metadata
///
/// This struct controls how the virtual table fetches data from cloud storage.
/// Users can configure whether to fetch content, recurse into directories,
/// and implement pagination.
///
/// # Examples
///
/// ```
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// // Metadata-only query (default)
/// let config = QueryConfig::default();
///
/// // Fetch file contents as well
/// let config = QueryConfig {
///     fetch_content: true,
///     ..Default::default()
/// };
///
/// // Recursive listing with pagination
/// let config = QueryConfig {
///     root_path: "/documents".to_string(),
///     recursive: true,
///     limit: Some(100),
///     offset: 0,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// The root path to start listing from
    pub root_path: String,

    /// Whether to fetch file contents (default: false for metadata-only queries)
    pub fetch_content: bool,

    /// Whether to recursively list subdirectories
    pub recursive: bool,

    /// Maximum number of results to return (for pagination)
    pub limit: Option<usize>,

    /// Offset for pagination
    pub offset: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            root_path: "/".to_string(),
            fetch_content: false,
            recursive: false,
            limit: None,
            offset: 0,
        }
    }
}

/// Column indices for the virtual table schema
///
/// These constants make it easier to reference columns by name
/// rather than magic numbers in the code.
pub mod columns {
    pub const PATH: i32 = 0;
    pub const SIZE: i32 = 1;
    pub const LAST_MODIFIED: i32 = 2;
    pub const ETAG: i32 = 3;
    pub const IS_DIR: i32 = 4;
    pub const CONTENT_TYPE: i32 = 5;
    pub const NAME: i32 = 6;
    pub const CONTENT: i32 = 7;
}
