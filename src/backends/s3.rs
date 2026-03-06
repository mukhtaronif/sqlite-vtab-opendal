//! AWS S3 storage backend implementation
//!
//! This backend allows querying S3 buckets and objects using SQL.
//! Requires AWS credentials and bucket configuration.

use crate::backends::StorageBackend;
use crate::error::{Result, VTableError};
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use opendal::{services::S3, EntryMode, Metakey, Operator};
use std::path::Path;

/// AWS S3 storage backend
///
/// This backend uses OpenDAL's S3 service to list objects from S3 buckets.
///
/// # Authentication
///
/// Requires AWS credentials. Can use:
/// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
/// - IAM roles (when running on EC2/ECS)
/// - Explicit credentials passed to constructor
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_vtable_opendal::backends::s3::S3Backend;
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// let backend = S3Backend::new("my-bucket", "us-east-1")
///     .with_credentials("access_key", "secret_key");
/// let config = QueryConfig::default();
/// let files = backend.list_files(&config).await?;
/// ```
pub struct S3Backend {
    /// S3 bucket name
    bucket: String,
    /// AWS region
    region: String,
    /// AWS access key ID (optional, can use env vars or IAM)
    access_key_id: Option<String>,
    /// AWS secret access key (optional)
    secret_access_key: Option<String>,
    /// Base path/prefix in bucket
    base_path: String,
}

impl S3Backend {
    /// Create a new S3 backend
    ///
    /// # Arguments
    ///
    /// * `bucket` - S3 bucket name
    /// * `region` - AWS region (e.g., "us-east-1")
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::s3::S3Backend;
    ///
    /// let backend = S3Backend::new("my-bucket", "us-east-1");
    /// ```
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            access_key_id: None,
            secret_access_key: None,
            base_path: "/".to_string(),
        }
    }

    /// Set AWS credentials explicitly
    ///
    /// # Arguments
    ///
    /// * `access_key_id` - AWS access key ID
    /// * `secret_access_key` - AWS secret access key
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::s3::S3Backend;
    ///
    /// let backend = S3Backend::new("my-bucket", "us-east-1")
    ///     .with_credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    /// ```
    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Set base path/prefix in bucket
    ///
    /// # Arguments
    ///
    /// * `path` - Base path (e.g., "data/2024/")
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::s3::S3Backend;
    ///
    /// let backend = S3Backend::new("my-bucket", "us-east-1")
    ///     .with_base_path("data/2024/");
    /// ```
    pub fn with_base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Create an OpenDAL operator for S3
    fn create_operator(&self) -> Result<Operator> {
        let mut builder = S3::default()
            .bucket(&self.bucket)
            .region(&self.region)
            .root(&self.base_path);

        // Add credentials if provided
        if let (Some(key_id), Some(secret)) = (&self.access_key_id, &self.secret_access_key) {
            builder = builder.access_key_id(key_id).secret_access_key(secret);
        }

        Operator::new(builder)
            .map(|op| op.finish())
            .map_err(|e| VTableError::OpenDal(e))
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>> {
        let operator = self.create_operator()?;
        let mut results = Vec::new();

        // Normalize the path
        let normalized_path = if config.root_path.is_empty() || config.root_path == "/" {
            "".to_string()
        } else {
            config.root_path.trim_matches('/').to_string()
        };

        // Create lister with metadata keys
        let lister_builder = operator.lister_with(&normalized_path);

        let mut lister = lister_builder
            .recursive(config.recursive)
            .metakey(
                Metakey::ContentLength
                    | Metakey::ContentMd5
                    | Metakey::ContentType
                    | Metakey::Mode
                    | Metakey::LastModified
                    | Metakey::Etag,
            )
            .await
            .map_err(|e| VTableError::OpenDal(e))?;

        // Iterate through entries
        while let Some(entry) = lister.try_next().await.map_err(|e| VTableError::OpenDal(e))? {
            let entry_path = entry.path();
            let entry_mode = entry.metadata().mode();

            // Skip the root directory itself
            if entry_path.is_empty() || entry_path == "/" || entry_path == "." {
                continue;
            }

            let full_path = if entry_path.starts_with('/') {
                entry_path.to_string()
            } else {
                format!("/{}", entry_path)
            };

            // Extract object name from path
            let name = Path::new(&full_path)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| {
                    let clean_path = entry_path.trim_end_matches('/');
                    Path::new(clean_path)
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default()
                });

            if entry_mode == EntryMode::FILE {
                // Fetch detailed metadata for files
                let metadata = operator
                    .stat(&full_path)
                    .await
                    .map_err(|e| VTableError::OpenDal(e))?;

                // Optionally fetch content
                let content = if config.fetch_content {
                    operator
                        .read(&full_path)
                        .await
                        .ok()
                        .map(|bytes| bytes.to_vec())
                } else {
                    None
                };

                results.push(FileMetadata {
                    name,
                    path: full_path.clone(),
                    size: metadata.content_length(),
                    last_modified: metadata.last_modified().map(|dt| dt.to_string()),
                    etag: metadata
                        .etag()
                        .or_else(|| metadata.content_md5())
                        .map(|e| e.to_string()),
                    is_dir: false,
                    content_type: metadata
                        .content_type()
                        .map(|ct| ct.to_string())
                        .or_else(|| {
                            Path::new(&full_path)
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .map(|ext| ext.to_string())
                        }),
                    content,
                });

                // Apply limit if specified
                if let Some(limit) = config.limit {
                    if results.len() >= limit + config.offset {
                        break;
                    }
                }
            } else if entry_mode == EntryMode::DIR {
                // Add directory entry (S3 "folders")
                results.push(FileMetadata {
                    name,
                    path: full_path,
                    size: 0,
                    last_modified: None,
                    etag: None,
                    is_dir: true,
                    content_type: Some("directory".to_string()),
                    content: None,
                });

                // Apply limit if specified
                if let Some(limit) = config.limit {
                    if results.len() >= limit + config.offset {
                        break;
                    }
                }
            }
        }

        // Apply offset
        if config.offset > 0 && config.offset < results.len() {
            results = results.into_iter().skip(config.offset).collect();
        }

        Ok(results)
    }

    fn backend_name(&self) -> &'static str {
        "s3"
    }
}

/// Register the S3 virtual table with SQLite
///
/// This function creates a virtual table module that allows querying
/// S3 objects using SQL.
///
/// # Arguments
///
/// * `conn` - SQLite connection
/// * `module_name` - Name for the virtual table (e.g., "s3_files")
/// * `bucket` - S3 bucket name
/// * `region` - AWS region
/// * `access_key_id` - AWS access key ID (optional, use "" for IAM/env)
/// * `secret_access_key` - AWS secret access key (optional)
///
/// # Example
///
/// ```rust,ignore
/// use rusqlite::Connection;
/// use sqlite_vtable_opendal::backends::s3;
///
/// let conn = Connection::open_in_memory()?;
/// s3::register(&conn, "s3_files", "my-bucket", "us-east-1", "KEY_ID", "SECRET")?;
///
/// // Now you can query: SELECT * FROM s3_files
/// ```
pub fn register(
    conn: &rusqlite::Connection,
    module_name: &str,
    bucket: impl Into<String>,
    region: impl Into<String>,
    access_key_id: impl Into<String>,
    secret_access_key: impl Into<String>,
) -> rusqlite::Result<()> {
    use crate::types::{columns, QueryConfig};
    use rusqlite::{
        ffi,
        vtab::{self, eponymous_only_module, IndexInfo, VTab, VTabCursor, VTabKind},
    };
    use std::os::raw::c_int;

    let bucket_name = bucket.into();
    let region_name = region.into();
    let key_id = access_key_id.into();
    let secret = secret_access_key.into();

    // Create a specific table type for S3
    #[repr(C)]
    struct S3Table {
        base: ffi::sqlite3_vtab,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
    }

    // Create a specific cursor type for S3
    #[repr(C)]
    struct S3Cursor {
        base: ffi::sqlite3_vtab_cursor,
        files: Vec<crate::types::FileMetadata>,
        current_row: usize,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
    }

    impl S3Cursor {
        fn new(
            bucket: String,
            region: String,
            access_key_id: String,
            secret_access_key: String,
        ) -> Self {
            Self {
                base: ffi::sqlite3_vtab_cursor::default(),
                files: Vec::new(),
                current_row: 0,
                bucket,
                region,
                access_key_id,
                secret_access_key,
            }
        }
    }

    unsafe impl VTabCursor for S3Cursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &vtab::Values<'_>,
        ) -> rusqlite::Result<()> {
            // Create backend and fetch files
            let mut backend = S3Backend::new(&self.bucket, &self.region);
            if !self.access_key_id.is_empty() {
                backend = backend.with_credentials(&self.access_key_id, &self.secret_access_key);
            }
            let config = QueryConfig::default();

            // Fetch files from the backend (blocking the async call)
            let files = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    backend.list_files(&config).await
                })
            })
            .map_err(|e| rusqlite::Error::ModuleError(e.to_string()))?;

            self.files = files;
            self.current_row = 0;
            Ok(())
        }

        fn next(&mut self) -> rusqlite::Result<()> {
            self.current_row += 1;
            Ok(())
        }

        fn eof(&self) -> bool {
            self.current_row >= self.files.len()
        }

        fn column(&self, ctx: &mut vtab::Context, col_index: c_int) -> rusqlite::Result<()> {
            if self.current_row >= self.files.len() {
                return Ok(());
            }

            let file = &self.files[self.current_row];

            match col_index {
                columns::PATH => ctx.set_result(&file.path),
                columns::SIZE => ctx.set_result(&(file.size as i64)),
                columns::LAST_MODIFIED => ctx.set_result(&file.last_modified),
                columns::ETAG => ctx.set_result(&file.etag),
                columns::IS_DIR => ctx.set_result(&file.is_dir),
                columns::CONTENT_TYPE => ctx.set_result(&file.content_type),
                columns::NAME => ctx.set_result(&file.name),
                columns::CONTENT => {
                    if let Some(ref content) = file.content {
                        ctx.set_result(&content.as_slice())
                    } else {
                        ctx.set_result::<Option<&[u8]>>(&None)
                    }
                }
                _ => Ok(()),
            }
        }

        fn rowid(&self) -> rusqlite::Result<i64> {
            Ok(self.current_row as i64)
        }
    }

    impl vtab::CreateVTab<'_> for S3Table {
        const KIND: VTabKind = VTabKind::EponymousOnly;
    }

    unsafe impl VTab<'_> for S3Table {
        type Aux = (String, String, String, String);
        type Cursor = S3Cursor;

        fn connect(
            _db: &mut vtab::VTabConnection,
            aux: Option<&Self::Aux>,
            _args: &[&[u8]],
        ) -> rusqlite::Result<(String, Self)> {
            let schema = "
                CREATE TABLE x(
                    path TEXT,
                    size INTEGER,
                    last_modified TEXT,
                    etag TEXT,
                    is_dir INTEGER,
                    content_type TEXT,
                    name TEXT,
                    content BLOB
                )
            ";

            let (bucket, region, access_key_id, secret_access_key) =
                if let Some((b, r, k, s)) = aux {
                    (b.clone(), r.clone(), k.clone(), s.clone())
                } else {
                    (
                        "".to_string(),
                        "us-east-1".to_string(),
                        "".to_string(),
                        "".to_string(),
                    )
                };

            Ok((
                schema.to_owned(),
                S3Table {
                    base: ffi::sqlite3_vtab::default(),
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                },
            ))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(1000.0);
            Ok(())
        }

        fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
            Ok(S3Cursor::new(
                self.bucket.clone(),
                self.region.clone(),
                self.access_key_id.clone(),
                self.secret_access_key.clone(),
            ))
        }
    }

    conn.create_module(
        module_name,
        eponymous_only_module::<S3Table>(),
        Some((bucket_name, region_name, key_id, secret)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_creation() {
        let backend = S3Backend::new("my-bucket", "us-east-1");
        assert_eq!(backend.bucket, "my-bucket");
        assert_eq!(backend.region, "us-east-1");
        assert_eq!(backend.backend_name(), "s3");
        assert!(backend.access_key_id.is_none());
        assert!(backend.secret_access_key.is_none());
    }

    #[test]
    fn test_backend_with_credentials() {
        let backend = S3Backend::new("my-bucket", "us-west-2")
            .with_credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI");
        assert_eq!(backend.bucket, "my-bucket");
        assert_eq!(backend.region, "us-west-2");
        assert_eq!(
            backend.access_key_id,
            Some("AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(backend.secret_access_key, Some("wJalrXUtnFEMI".to_string()));
    }

    #[test]
    fn test_backend_with_base_path() {
        let backend = S3Backend::new("my-bucket", "eu-west-1").with_base_path("data/2024/");
        assert_eq!(backend.base_path, "data/2024/");
    }

    #[test]
    fn test_backend_builder_pattern() {
        let backend = S3Backend::new("test-bucket", "ap-south-1")
            .with_credentials("key", "secret")
            .with_base_path("logs/");
        assert_eq!(backend.bucket, "test-bucket");
        assert_eq!(backend.region, "ap-south-1");
        assert_eq!(backend.access_key_id, Some("key".to_string()));
        assert_eq!(backend.base_path, "logs/");
    }

    // Note: Integration tests with actual S3 would require credentials
    // and are better suited for manual testing or CI with secrets
}
