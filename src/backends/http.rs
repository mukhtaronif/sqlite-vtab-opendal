//! HTTP storage backend implementation
//!
//! This backend allows fetching files from HTTP/HTTPS endpoints.
//! It treats HTTP resources as "files" that can be queried.

use crate::backends::StorageBackend;
use crate::error::{Result, VTableError};
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;
use opendal::{services::Http, Operator};

/// HTTP storage backend
///
/// This backend uses OpenDAL's HTTP service to fetch files from HTTP endpoints.
/// Each HTTP resource is treated as a file.
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_vtable_opendal::backends::http::HttpBackend;
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// let backend = HttpBackend::new("https://api.example.com");
/// let config = QueryConfig::default();
/// let files = backend.list_files(&config).await?;
/// ```
pub struct HttpBackend {
    /// Base HTTP endpoint URL
    endpoint: String,
}

impl HttpBackend {
    /// Create a new HTTP backend
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Base HTTP endpoint URL (e.g., "https://api.example.com")
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::http::HttpBackend;
    ///
    /// let backend = HttpBackend::new("https://api.example.com/data");
    /// ```
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// Create an OpenDAL operator for HTTP
    fn create_operator(&self) -> Result<Operator> {
        let builder = Http::default().endpoint(&self.endpoint);

        Operator::new(builder)
            .map(|op| op.finish())
            .map_err(|e| VTableError::OpenDal(e))
    }
}

#[async_trait]
impl StorageBackend for HttpBackend {
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>> {
        let operator = self.create_operator()?;
        let mut results = Vec::new();

        // Normalize the path
        let normalized_path = if config.root_path.is_empty() || config.root_path == "/" {
            "".to_string()
        } else {
            config.root_path.trim_matches('/').to_string()
        };

        // For HTTP, we typically fetch a single resource
        // Try to get metadata first
        let path = if normalized_path.is_empty() {
            "".to_string()
        } else {
            normalized_path.clone()
        };

        match operator.stat(&path).await {
            Ok(metadata) => {
                // Optionally fetch content
                let content = if config.fetch_content {
                    operator.read(&path).await.ok().map(|bytes| bytes.to_vec())
                } else {
                    None
                };

                // Extract file name from path
                let name = if path.is_empty() {
                    "index".to_string()
                } else {
                    path.split('/').last().unwrap_or(&path).to_string()
                };

                results.push(FileMetadata {
                    name,
                    path: if path.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}", path)
                    },
                    size: metadata.content_length(),
                    last_modified: metadata.last_modified().map(|dt| dt.to_string()),
                    etag: metadata.etag().map(|e| e.to_string()),
                    is_dir: false,
                    content_type: metadata.content_type().map(|ct| ct.to_string()),
                    content,
                });
            }
            Err(_) => {
                // If stat fails, try to read the content directly
                if let Ok(bytes) = operator.read(&path).await {
                    let name = if path.is_empty() {
                        "index".to_string()
                    } else {
                        path.split('/').last().unwrap_or(&path).to_string()
                    };

                    let content_data = bytes.to_vec();
                    let size = content_data.len() as u64;

                    results.push(FileMetadata {
                        name,
                        path: if path.is_empty() {
                            "/".to_string()
                        } else {
                            format!("/{}", path)
                        },
                        size,
                        last_modified: None,
                        etag: None,
                        is_dir: false,
                        content_type: Some("application/octet-stream".to_string()),
                        content: if config.fetch_content {
                            Some(content_data)
                        } else {
                            None
                        },
                    });
                }
            }
        }

        Ok(results)
    }

    fn backend_name(&self) -> &'static str {
        "http"
    }
}

/// Register the http virtual table with SQLite
///
/// This function creates a virtual table module that allows querying
/// HTTP resources using SQL.
///
/// # Arguments
///
/// * `conn` - SQLite connection
/// * `module_name` - Name for the virtual table (e.g., "http_data")
/// * `endpoint` - Base HTTP endpoint URL
///
/// # Example
///
/// ```rust,ignore
/// use rusqlite::Connection;
/// use sqlite_vtable_opendal::backends::http;
///
/// let conn = Connection::open_in_memory()?;
/// http::register(&conn, "http_data", "https://api.example.com")?;
///
/// // Now you can query: SELECT * FROM http_data
/// ```
pub fn register(
    conn: &rusqlite::Connection,
    module_name: &str,
    endpoint: impl Into<String>,
) -> rusqlite::Result<()> {
    use crate::types::{columns, QueryConfig};
    use rusqlite::{
        ffi,
        vtab::{self, eponymous_only_module, IndexInfo, VTab, VTabCursor, VTabKind},
    };
    use std::os::raw::c_int;

    let endpoint_str = endpoint.into();

    // Create a specific table type for HTTP
    #[repr(C)]
    struct HttpTable {
        base: ffi::sqlite3_vtab,
        endpoint: String,
    }

    // Create a specific cursor type for HTTP
    #[repr(C)]
    struct HttpCursor {
        base: ffi::sqlite3_vtab_cursor,
        files: Vec<crate::types::FileMetadata>,
        current_row: usize,
        endpoint: String,
    }

    impl HttpCursor {
        fn new(endpoint: String) -> Self {
            Self {
                base: ffi::sqlite3_vtab_cursor::default(),
                files: Vec::new(),
                current_row: 0,
                endpoint,
            }
        }
    }

    unsafe impl VTabCursor for HttpCursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &vtab::Values<'_>,
        ) -> rusqlite::Result<()> {
            // Create backend and fetch files
            let backend = HttpBackend::new(&self.endpoint);
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

    impl vtab::CreateVTab<'_> for HttpTable {
        const KIND: VTabKind = VTabKind::EponymousOnly;
    }

    unsafe impl VTab<'_> for HttpTable {
        type Aux = String;
        type Cursor = HttpCursor;

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

            let endpoint = if let Some(ep) = aux {
                ep.clone()
            } else {
                "".to_string()
            };

            Ok((
                schema.to_owned(),
                HttpTable {
                    base: ffi::sqlite3_vtab::default(),
                    endpoint,
                },
            ))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(100.0);
            Ok(())
        }

        fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
            Ok(HttpCursor::new(self.endpoint.clone()))
        }
    }

    conn.create_module(
        module_name,
        eponymous_only_module::<HttpTable>(),
        Some(endpoint_str),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_creation() {
        let backend = HttpBackend::new("https://api.example.com/data");
        assert_eq!(backend.endpoint, "https://api.example.com/data");
        assert_eq!(backend.backend_name(), "http");
    }

    #[test]
    fn test_backend_with_different_endpoints() {
        let backend = HttpBackend::new("http://localhost:8080");
        assert_eq!(backend.endpoint, "http://localhost:8080");
    }

    // Note: Integration tests with actual HTTP endpoints would require a test server
    // and are better suited for manual testing or CI with mock servers
}
