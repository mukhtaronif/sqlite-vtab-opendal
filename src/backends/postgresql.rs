//! PostgreSQL storage backend implementation
//!
//! This backend allows querying PostgreSQL database tables using SQL.
//! It treats database records as "files" where the key column is the path
//! and the value column is the content.

use crate::backends::StorageBackend;
use crate::error::{Result, VTableError};
use crate::types::{FileMetadata, QueryConfig};
use async_trait::async_trait;
use opendal::{services::Postgresql, Operator};
use tokio_postgres::NoTls;

/// PostgreSQL storage backend
///
/// This backend uses OpenDAL's Postgresql service to query database tables.
/// Each row is treated as a "file" where:
/// - The key_field column becomes the file path
/// - The value_field column becomes the file content
///
/// # Example
///
/// ```rust,ignore
/// use sqlite_vtable_opendal::backends::postgresql::PostgresqlBackend;
/// use sqlite_vtable_opendal::types::QueryConfig;
///
/// let backend = PostgresqlBackend::new(
///     "postgresql://user:pass@localhost/db",
///     "my_table",
///     "id",
///     "data"
/// );
/// let config = QueryConfig::default();
/// let files = backend.list_files(&config).await?;
/// ```
pub struct PostgresqlBackend {
    /// PostgreSQL connection string
    connection_string: String,
    /// Table name to query
    table: String,
    /// Column name for the key (becomes file path)
    key_field: String,
    /// Column name for the value (becomes file content)
    value_field: String,
}

impl PostgresqlBackend {
    /// Create a new PostgreSQL backend
    ///
    /// # Arguments
    ///
    /// * `connection_string` - PostgreSQL connection string
    /// * `table` - Table name to query
    /// * `key_field` - Column name for keys (default: "key")
    /// * `value_field` - Column name for values (default: "value")
    ///
    /// # Example
    ///
    /// ```
    /// use sqlite_vtable_opendal::backends::postgresql::PostgresqlBackend;
    ///
    /// let backend = PostgresqlBackend::new(
    ///     "postgresql://localhost/mydb",
    ///     "documents",
    ///     "id",
    ///     "content"
    /// );
    /// ```
    pub fn new(
        connection_string: impl Into<String>,
        table: impl Into<String>,
        key_field: impl Into<String>,
        value_field: impl Into<String>,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            table: table.into(),
            key_field: key_field.into(),
            value_field: value_field.into(),
        }
    }

    /// Create an OpenDAL operator for PostgreSQL
    fn create_operator(&self) -> Result<Operator> {
        let builder = Postgresql::default()
            .connection_string(&self.connection_string)
            .table(&self.table)
            .key_field(&self.key_field)
            .value_field(&self.value_field);

        Ok(Operator::new(builder)?
            .finish())
    }
}

#[async_trait]
impl StorageBackend for PostgresqlBackend {
    async fn list_files(&self, config: &QueryConfig) -> Result<Vec<FileMetadata>> {
        let operator = self.create_operator()?;
        let mut results = Vec::new();

        // Also query PostgreSQL directly to get all keys
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .map_err(|e| VTableError::Custom(format!("PostgreSQL connection error: {}", e)))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        // Query all keys from the table
        let query = format!(
            "SELECT {}::text FROM {}",
            self.key_field, self.table
        );

        let rows = client
            .query(&query, &[])
            .await
            .map_err(|e| VTableError::Custom(format!("PostgreSQL query error: {}", e)))?;

        // Fetch metadata for each key
        for row in rows {
            let key: String = row.get(0);
            let path = format!("/{}", key.trim_matches('/'));

            // Get metadata using OpenDAL
            let metadata_result = operator.stat(&path).await;

            if let Ok(metadata) = metadata_result {
                // Optionally fetch content
                let content = if config.fetch_content {
                    operator.read(&path).await.ok().map(|bytes| bytes.to_vec())
                } else {
                    None
                };

                results.push(FileMetadata {
                    name: key.clone(),
                    path: path.clone(),
                    size: metadata.content_length(),
                    last_modified: metadata.last_modified().map(|dt| dt.to_string()),
                    etag: metadata
                        .content_md5()
                        .map(|md5| md5.to_string()),
                    is_dir: false,
                    content_type: Some("application/octet-stream".to_string()),
                    content,
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
        "postgresql"
    }
}

/// Register the postgresql virtual table with SQLite
///
/// This function creates a virtual table module that allows querying
/// PostgreSQL database tables using SQL.
///
/// # Arguments
///
/// * `conn` - SQLite connection
/// * `module_name` - Name for the virtual table (e.g., "pg_data")
/// * `connection_string` - PostgreSQL connection string
/// * `table` - Table name to query
/// * `key_field` - Column name for keys
/// * `value_field` - Column name for values
///
/// # Example
///
/// ```rust,ignore
/// use rusqlite::Connection;
/// use sqlite_vtable_opendal::backends::postgresql;
///
/// let conn = Connection::open_in_memory()?;
/// postgresql::register(
///     &conn,
///     "pg_data",
///     "postgresql://localhost/mydb",
///     "documents",
///     "id",
///     "content"
/// )?;
///
/// // Now you can query: SELECT * FROM pg_data
/// ```
pub fn register(
    conn: &rusqlite::Connection,
    module_name: &str,
    connection_string: impl Into<String>,
    table: impl Into<String>,
    key_field: impl Into<String>,
    value_field: impl Into<String>,
) -> rusqlite::Result<()> {
    use crate::types::{columns, QueryConfig};
    use rusqlite::{
        ffi,
        vtab::{self, eponymous_only_module, IndexInfo, VTab, VTabCursor, VTabKind},
    };
    use std::os::raw::c_int;

    let conn_str = connection_string.into();
    let table_name = table.into();
    let key_col = key_field.into();
    let value_col = value_field.into();

    // Create a specific table type for PostgreSQL
    #[repr(C)]
    struct PostgresqlTable {
        base: ffi::sqlite3_vtab,
        connection_string: String,
        table: String,
        key_field: String,
        value_field: String,
    }

    // Create a specific cursor type for PostgreSQL
    #[repr(C)]
    struct PostgresqlCursor {
        base: ffi::sqlite3_vtab_cursor,
        files: Vec<crate::types::FileMetadata>,
        current_row: usize,
        connection_string: String,
        table: String,
        key_field: String,
        value_field: String,
    }

    impl PostgresqlCursor {
        fn new(
            connection_string: String,
            table: String,
            key_field: String,
            value_field: String,
        ) -> Self {
            Self {
                base: ffi::sqlite3_vtab_cursor::default(),
                files: Vec::new(),
                current_row: 0,
                connection_string,
                table,
                key_field,
                value_field,
            }
        }
    }

    unsafe impl VTabCursor for PostgresqlCursor {
        fn filter(
            &mut self,
            _idx_num: c_int,
            _idx_str: Option<&str>,
            _args: &vtab::Values<'_>,
        ) -> rusqlite::Result<()> {
            // Create backend and fetch files
            let backend = PostgresqlBackend::new(
                &self.connection_string,
                &self.table,
                &self.key_field,
                &self.value_field,
            );
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

    impl vtab::CreateVTab<'_> for PostgresqlTable {
        const KIND: VTabKind = VTabKind::EponymousOnly;
    }

    unsafe impl VTab<'_> for PostgresqlTable {
        type Aux = (String, String, String, String);
        type Cursor = PostgresqlCursor;

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

            let (connection_string, table, key_field, value_field) =
                if let Some((conn, tbl, key, val)) = aux {
                    (
                        conn.clone(),
                        tbl.clone(),
                        key.clone(),
                        val.clone(),
                    )
                } else {
                    (
                        "".to_string(),
                        "".to_string(),
                        "key".to_string(),
                        "value".to_string(),
                    )
                };

            Ok((
                schema.to_owned(),
                PostgresqlTable {
                    base: ffi::sqlite3_vtab::default(),
                    connection_string,
                    table,
                    key_field,
                    value_field,
                },
            ))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(1000.0);
            Ok(())
        }

        fn open(&mut self) -> rusqlite::Result<Self::Cursor> {
            Ok(PostgresqlCursor::new(
                self.connection_string.clone(),
                self.table.clone(),
                self.key_field.clone(),
                self.value_field.clone(),
            ))
        }
    }

    conn.create_module(
        module_name,
        eponymous_only_module::<PostgresqlTable>(),
        Some((conn_str, table_name, key_col, value_col)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_creation() {
        let backend = PostgresqlBackend::new(
            "postgresql://localhost/test",
            "documents",
            "id",
            "data",
        );
        assert_eq!(backend.connection_string, "postgresql://localhost/test");
        assert_eq!(backend.table, "documents");
        assert_eq!(backend.key_field, "id");
        assert_eq!(backend.value_field, "data");
        assert_eq!(backend.backend_name(), "postgresql");
    }

    #[test]
    fn test_backend_with_different_fields() {
        let backend = PostgresqlBackend::new(
            "postgresql://user:pass@db.example.com:5432/mydb",
            "my_table",
            "key_column",
            "value_column",
        );
        assert_eq!(backend.key_field, "key_column");
        assert_eq!(backend.value_field, "value_column");
    }

    // Note: Integration tests with actual PostgreSQL would require a running database
    // and are better suited for manual testing or CI with docker-compose
}
