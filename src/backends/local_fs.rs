//! Local filesystem backend implementation
//!
//! This backend allows querying local directories using SQL.
//! It's useful for file discovery, auditing, and testing.

use crate::backends::StorageBackend;
use crate::error::{Result, VTableError};
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use opendal::{services::Fs, EntryMode, Metakey, Operator};
use std::path::Path;

/// Local filesystem storage backend
///
/// This backend uses OpenDAL's Fs service to list files from local directories.
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_vtable_opendal::backends::local_fs::LocalFsBackend;
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// let backend = LocalFsBackend::new("/path/to/directory");
/// let config = QueryConfig::default();
/// let files = backend.list_files(&config).await?;
/// ```
pub struct LocalFsBackend {
    /// Root directory path
    root_path: String,
}

impl LocalFsBackend {
    /// Create a new local filesystem backend
    ///
    /// # Arguments
    ///
    /// * `root_path` - The root directory to query from
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::local_fs::LocalFsBackend;
    ///
    /// let backend = LocalFsBackend::new("/tmp");
    /// ```
    pub fn new(root_path: impl Into<String>) -> Self {
        Self {
            root_path: root_path.into(),
        }
    }

    /// Create an OpenDAL operator for the local filesystem
    fn create_operator(&self) -> Result<Operator> {
        let builder = Fs::default().root(&self.root_path);

        Operator::new(builder)
            .map(|op| op.finish())
            .map_err(|e| VTableError::OpenDal(e))
    }
}

#[async_trait]
impl StorageBackend for LocalFsBackend {
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>> {
        let operator = self.create_operator()?;
        let mut results = Vec::new();

        // Normalize the path
        let normalized_path = if config.root_path == "/" || config.root_path.is_empty() {
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
                    | Metakey::ContentType
                    | Metakey::Mode
                    | Metakey::LastModified,
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
                    etag: metadata.etag().map(|e| e.to_string()),
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
        "local_fs"
    }
}

/// Register the local_fs virtual table with SQLite
///
/// This function creates a virtual table module that allows querying
/// local directories using SQL.
///
/// # Arguments
///
/// * `conn` - SQLite connection
/// * `module_name` - Name for the virtual table (e.g., "local_files")
/// * `root_path` - Root directory to query
///
/// # Example
///
/// ```rust,ignore
/// use rusqlite::Connection;
/// use sqlite_vtable_opendal::backends::local_fs;
///
/// let conn = Connection::open_in_memory()?;
/// local_fs::register(&conn, "local_files", "/tmp")?;
///
/// // Now you can query: SELECT * FROM local_files
/// ```
pub fn register(
    conn: &rusqlite::Connection,
    module_name: &str,
    root_path: impl Into<String>,
) -> rusqlite::Result<()> {
    use crate::types::{columns, QueryConfig};
    use rusqlite::{
        ffi,
        vtab::{self, eponymous_only_module, IndexInfo, VTab, VTabCursor, VTabKind},
    };
    use std::os::raw::c_int;

    let root = root_path.into();

    // Create a specific table type for local_fs
    #[repr(C)]
    struct LocalFsTable {
        base: ffi::sqlite3_vtab,
        root_path: String,
    }

    // Create a specific cursor type for local_fs
    #[repr(C)]
    struct LocalFsCursor {
        base: ffi::sqlite3_vtab_cursor,
        files: Vec<crate::types::FileMetadata>,
        current_row: usize,
        root_path: String,
    }

    impl LocalFsCursor {
        fn new(root_path: String) -> Self {
            Self {
                base: ffi::sqlite3_vtab_cursor::default(),
                files: Vec::new(),
                current_row: 0,
                root_path,
            }
        }
    }

    unsafe impl VTabCursor for LocalFsCursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &vtab::Values<'_>,
        ) -> rusqlite::Result<()> {
            // Create backend and fetch files
            let backend = LocalFsBackend::new(&self.root_path);
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

    impl vtab::CreateVTab<'_> for LocalFsTable {
        const KIND: VTabKind = VTabKind::EponymousOnly;
    }

    unsafe impl VTab<'_> for LocalFsTable {
        type Aux = String;
        type Cursor = LocalFsCursor;

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

            let root_path = aux.cloned().unwrap_or_else(|| "/".to_string());

            Ok((
                schema.to_owned(),
                LocalFsTable {
                    base: ffi::sqlite3_vtab::default(),
                    root_path,
                },
            ))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(100.0);
            Ok(())
        }

        fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
            Ok(LocalFsCursor::new(self.root_path.clone()))
        }
    }

    conn.create_module(module_name, eponymous_only_module::<LocalFsTable>(), Some(root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_list_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(temp_dir.path().to_str().unwrap());
        let config = QueryConfig::default();

        let files = backend.list_files(&config).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_list_files() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create test files
        fs::write(temp_path.join("file1.txt"), "content1").unwrap();
        fs::write(temp_path.join("file2.txt"), "content2").unwrap();

        let backend = LocalFsBackend::new(temp_path.to_str().unwrap());
        let config = QueryConfig::default();

        let files = backend.list_files(&config).await.unwrap();
        assert_eq!(files.len(), 2);

        // Check that files are listed
        assert!(files.iter().any(|f| f.name == "file1.txt"));
        assert!(files.iter().any(|f| f.name == "file2.txt"));
    }

    #[tokio::test]
    async fn test_file_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        fs::write(temp_path.join("test.txt"), "hello world").unwrap();

        let backend = LocalFsBackend::new(temp_path.to_str().unwrap());
        let config = QueryConfig::default();

        let files = backend.list_files(&config).await.unwrap();
        assert_eq!(files.len(), 1);

        let file = &files[0];
        assert_eq!(file.name, "test.txt");
        assert_eq!(file.size, 11); // "hello world" is 11 bytes
        assert!(!file.is_dir);
        assert_eq!(file.content_type, Some("txt".to_string()));
        assert!(file.content.is_none()); // Not fetched by default
    }

    #[tokio::test]
    async fn test_fetch_content() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        fs::write(temp_path.join("test.txt"), "hello world").unwrap();

        let backend = LocalFsBackend::new(temp_path.to_str().unwrap());
        let config = QueryConfig {
            fetch_content: true,
            ..Default::default()
        };

        let files = backend.list_files(&config).await.unwrap();
        assert_eq!(files.len(), 1);

        let file = &files[0];
        assert!(file.content.is_some());
        assert_eq!(file.content.as_ref().unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn test_recursive_listing() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create nested structure
        fs::create_dir(temp_path.join("subdir")).unwrap();
        fs::write(temp_path.join("file1.txt"), "content1").unwrap();
        fs::write(temp_path.join("subdir/file2.txt"), "content2").unwrap();

        let backend = LocalFsBackend::new(temp_path.to_str().unwrap());
        let config = QueryConfig {
            recursive: true,
            ..Default::default()
        };

        let files = backend.list_files(&config).await.unwrap();

        // Should have: subdir (directory), file1.txt, subdir/file2.txt
        assert!(files.len() >= 2); // At least the files, maybe the directory too

        assert!(files.iter().any(|f| f.name == "file1.txt"));
        assert!(files.iter().any(|f| f.name == "file2.txt"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sqlite_integration() {
        use rusqlite::Connection;

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create test files
        fs::write(temp_path.join("large.txt"), "x".repeat(10000)).unwrap();
        fs::write(temp_path.join("small.txt"), "tiny").unwrap();
        fs::write(temp_path.join("medium.txt"), "medium content").unwrap();

        // Open SQLite connection and register virtual table
        let conn = Connection::open_in_memory().unwrap();
        register(&conn, "local_files", temp_path.to_str().unwrap()).unwrap();

        // Query all files
        let mut stmt = conn
            .prepare("SELECT name, size, is_dir FROM local_files ORDER BY name")
            .unwrap();

        let files: Vec<(String, i64, bool)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(files.len(), 3);
        assert!(files.iter().any(|(name, _, _)| name == "large.txt"));
        assert!(files.iter().any(|(name, _, _)| name == "small.txt"));
        assert!(files.iter().any(|(name, _, _)| name == "medium.txt"));

        // Query with WHERE clause
        let mut stmt = conn
            .prepare("SELECT name FROM local_files WHERE size > 100")
            .unwrap();

        let large_files: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        // Only large.txt should be returned
        assert_eq!(large_files.len(), 1);
        assert_eq!(large_files[0], "large.txt");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sqlite_count_and_aggregate() {
        use rusqlite::Connection;

        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create multiple files
        for i in 1..=5 {
            fs::write(temp_path.join(format!("file{}.txt", i)), format!("content{}", i))
                .unwrap();
        }

        let conn = Connection::open_in_memory().unwrap();
        register(&conn, "local_files", temp_path.to_str().unwrap()).unwrap();

        // Test COUNT
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM local_files", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 5);

        // Test SUM of sizes
        let total_size: i64 = conn
            .query_row("SELECT SUM(size) FROM local_files", [], |row| row.get(0))
            .unwrap();
        assert!(total_size > 0);

        // Test ORDER BY
        let first_file: String = conn
            .query_row(
                "SELECT name FROM local_files ORDER BY name LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_file, "file1.txt");
    }
}
