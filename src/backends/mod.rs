//! Storage backend implementations
//!
//! This module defines the trait that all storage backends must implement,
//! and provides implementations for various cloud storage providers.

use crate::error::Result;
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;

/// Trait that all storage backends must implement
///
/// This trait provides a common interface for listing files from different
/// storage backends (Dropbox, S3, local filesystem, etc.).
///
/// Backend implementations handle the specifics of connecting to storage
/// and converting OpenDAL entries to our FileMetadata structure.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// List files from the storage backend according to the query configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration controlling what to fetch and how
    ///
    /// # Returns
    ///
    /// A vector of FileMetadata entries
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails to list files or if credentials are invalid
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>>;

    /// Get the name of this backend (for logging/debugging)
    fn backend_name(&self) -> &'static str;
}

// Backend implementations
pub mod local_fs;
pub mod dropbox;
pub mod s3;
pub mod gdrive;
pub mod http;
