//! Google Drive storage backend implementation
//!
//! This backend allows querying Google Drive files and folders using SQL.
//! Requires a Google Drive access token for authentication.

use crate::backends::StorageBackend;
use crate::error::Result;
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use opendal::{services::Gdrive, EntryMode, Metakey, Operator};
use std::path::Path;

/// Google Drive storage backend
///
/// This backend uses OpenDAL's Gdrive service to list files from Google Drive.
///
/// # Authentication
///
/// Requires a Google Drive access token. You can obtain one from:
/// https://console.cloud.google.com/apis/credentials
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_vtable_opendal::backends::gdrive::GdriveBackend;
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// let backend = GdriveBackend::new("YOUR_ACCESS_TOKEN", "/");
/// let config = QueryConfig::default();
/// let files = backend.list_files(&config).await?;
/// ```
pub struct GdriveBackend {
    /// Google Drive access token
    access_token: String,
    /// Base path in Google Drive (e.g., "/" for root)
    base_path: String,
}

impl GdriveBackend {
    /// Create a new Google Drive backend
    ///
    /// # Arguments
    ///
    /// * `access_token` - Google Drive API access token
    /// * `base_path` - Base path to query from (e.g., "/" for root)
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::gdrive::GdriveBackend;
    ///
    /// let backend = GdriveBackend::new("token", "/My Documents");
    /// ```
    pub fn new(access_token: impl Into<String>, base_path: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            base_path: base_path.into(),
        }
    }

    /// Create an OpenDAL operator for Google Drive
    fn create_operator(&self) -> Result<Operator> {
        let builder = Gdrive::default()
            .access_token(&self.access_token)
            .root(&self.base_path);

        Ok(Operator::new(builder)?
            .finish())
    }
}

#[async_trait]
impl StorageBackend for GdriveBackend {
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>> {
        let operator = self.create_operator()?;
        let mut results = Vec::new();

        // Normalize the path
        let normalized_path = if config.root_path.is_empty() || config.root_path == "/" {
            "".to_string()
        } else {
            let clean_path = config.root_path.trim_matches('/');
            if clean_path.is_empty() {
                "".to_string()
            } else {
                format!("/{}", clean_path)
            }
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
                    | Metakey::LastModified,
            )
            .await
            ?;

        // Iterate through entries
        while let Some(entry) = lister.try_next().await? {
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

            // Extract file name from path
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
                    ?;

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
                    etag: metadata.content_md5().map(|md5| md5.to_string()),
                    is_dir: false,
                    content_type: Path::new(&full_path)
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| ext.to_string()),
                    content,
                });

                // Apply limit if specified
                if let Some(limit) = config.limit {
                    if results.len() >= limit + config.offset {
                        break;
                    }
                }
            } else if entry_mode == EntryMode::DIR {
                // Add directory entry
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
        "gdrive"
    }
}

/// Register the gdrive virtual table with SQLite
///
/// This function creates a virtual table module that allows querying
/// Google Drive files using SQL.
///
/// # Arguments
///
/// * `conn` - SQLite connection
/// * `module_name` - Name for the virtual table (e.g., "gdrive_files")
/// * `access_token` - Google Drive access token
/// * `base_path` - Base path in Google Drive (e.g., "/" for root)
///
/// # Example
///
/// ```rust,ignore
/// use rusqlite::Connection;
/// use sqlite_vtable_opendal::backends::gdrive;
///
/// let conn = Connection::open_in_memory()?;
/// gdrive::register(&conn, "gdrive_files", "YOUR_TOKEN", "/")?;
///
/// // Now you can query: SELECT * FROM gdrive_files
/// ```
pub fn register(
    conn: &rusqlite::Connection,
    module_name: &str,
    access_token: impl Into<String>,
    base_path: impl Into<String>,
) -> rusqlite::Result<()> {
    use crate::types::{columns, QueryConfig};
    use rusqlite::{
        ffi,
        vtab::{self, eponymous_only_module, IndexInfo, VTab, VTabCursor, VTabKind},
    };
    use std::os::raw::c_int;

    let token = access_token.into();
    let path = base_path.into();

    // Create a specific table type for Google Drive
    #[repr(C)]
    struct GdriveTable {
        base: ffi::sqlite3_vtab,
        access_token: String,
        base_path: String,
    }

    // Create a specific cursor type for Google Drive
    #[repr(C)]
    struct GdriveCursor {
        base: ffi::sqlite3_vtab_cursor,
        files: Vec<crate::types::FileMetadata>,
        current_row: usize,
        access_token: String,
        base_path: String,
    }

    impl GdriveCursor {
        fn new(access_token: String, base_path: String) -> Self {
            Self {
                base: ffi::sqlite3_vtab_cursor::default(),
                files: Vec::new(),
                current_row: 0,
                access_token,
                base_path,
            }
        }
    }

    unsafe impl VTabCursor for GdriveCursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &vtab::Values<'_>,
        ) -> rusqlite::Result<()> {
            // Create backend and fetch files
            let backend = GdriveBackend::new(&self.access_token, &self.base_path);
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

    impl vtab::CreateVTab<'_> for GdriveTable {
        const KIND: VTabKind = VTabKind::EponymousOnly;
    }

    unsafe impl VTab<'_> for GdriveTable {
        type Aux = (String, String);
        type Cursor = GdriveCursor;

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

            let (access_token, base_path) = if let Some((token, path)) = aux {
                (token.clone(), path.clone())
            } else {
                ("/".to_string(), "/".to_string())
            };

            Ok((
                schema.to_owned(),
                GdriveTable {
                    base: ffi::sqlite3_vtab::default(),
                    access_token,
                    base_path,
                },
            ))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(1000.0);
            Ok(())
        }

        fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
            Ok(GdriveCursor::new(
                self.access_token.clone(),
                self.base_path.clone(),
            ))
        }
    }

    conn.create_module(
        module_name,
        eponymous_only_module::<GdriveTable>(),
        Some((token, path)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_creation() {
        let backend = GdriveBackend::new("test_token", "/My Documents");
        assert_eq!(backend.access_token, "test_token");
        assert_eq!(backend.base_path, "/My Documents");
        assert_eq!(backend.backend_name(), "gdrive");
    }

    #[test]
    fn test_backend_with_root_path() {
        let backend = GdriveBackend::new("token", "/");
        assert_eq!(backend.base_path, "/");
    }

    // Note: Integration tests with actual Google Drive API would require credentials
    // and are better suited for manual testing or CI with secrets
}
