# sqlite-vtable-opendal

> **Federated SQLite Virtual Tables for Cloud Object Stores using OpenDAL**

Query cloud storage metadata using SQL — without ingestion.

[![Crates.io](https://img.shields.io/crates/v/sqlite-vtable-opendal.svg)](https://crates.io/crates/sqlite-vtable-opendal)
[![Documentation](https://docs.rs/sqlite-vtable-opendal/badge.svg)](https://docs.rs/sqlite-vtable-opendal)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

---

## 🎯 Problem Statement

Modern data workflows require querying metadata from cloud object stores (Dropbox, S3, etc.). Today this requires:

- ✗ Custom scripts
- ✗ Full data ingestion
- ✗ Non-composable APIs

**There is no lightweight way to query remote object metadata using SQL without ingestion.**

---

## 💡 Solution

`sqlite-vtable-opendal` provides SQLite virtual tables that expose cloud storage as queryable tables:

```sql
SELECT path, size
FROM local_files
WHERE size > 10000000
ORDER BY size DESC;
```

**No data ingestion. No materialization. Just pure SQL.**

---

## ✨ Features

- 🚀 **Zero Data Ingestion** - Query directly from storage without downloading
- 📊 **Standard SQL** - Use familiar SQL syntax for cloud storage queries
- ⚡ **Metadata-Only Queries** - Fetch only what you need (size, dates, etags)
- 🔌 **Multiple Backends** - Local FS, Dropbox, S3, Google Drive, HTTP (via OpenDAL)
- 🎯 **Composable** - Combine with SQLite's powerful query engine
- 🧪 **Well-Tested** - Comprehensive test coverage across all backends

---

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
sqlite-vtable-opendal = "0.1.0"
rusqlite = { version = "0.32", features = ["bundled-full"] }
```

---

## 🚀 Quick Start

### Local Filesystem Example

```rust
use rusqlite::Connection;
use sqlite_vtable_opendal::backends::local_fs;

fn main() -> rusqlite::Result<()> {
    // Open SQLite connection
    let conn = Connection::open_in_memory()?;

    // Register virtual table for /tmp directory
    local_fs::register(&conn, "local_files", "/tmp")?;

    // Query files using SQL!
    let mut stmt = conn.prepare(
        "SELECT name, size FROM local_files
         WHERE size > 1000
         ORDER BY size DESC"
    )?;

    let files = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    for file in files {
        let (name, size) = file?;
        println!("{}: {} bytes", name, size);
    }

    Ok(())
}
```

### Available Columns

All virtual tables expose these columns:

| Column | Type | Description |
|--------|------|-------------|
| `path` | TEXT | Full path to file/directory |
| `size` | INTEGER | File size in bytes |
| `last_modified` | TEXT | ISO 8601 timestamp |
| `etag` | TEXT | Content hash (MD5, SHA256, etc.) |
| `is_dir` | INTEGER | 1 if directory, 0 if file |
| `content_type` | TEXT | MIME type or file extension |
| `name` | TEXT | File/directory name (without path) |
| `content` | BLOB | Actual file content (NULL by default) |

---

## 📚 Backend Usage

### Local Filesystem

```rust
use sqlite_vtable_opendal::backends::local_fs;

local_fs::register(&conn, "my_files", "/path/to/directory")?;
```

Query:
```sql
SELECT * FROM my_files WHERE name LIKE '%.txt';
```

### AWS S3

```rust
use sqlite_vtable_opendal::backends::s3;

s3::register(
    &conn,
    "s3_files",
    "my-bucket",           // bucket name
    "us-east-1",           // region
    "AKIAIOSFODNN7EXAMPLE", // access_key_id
    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" // secret_access_key
)?;
```

Query:
```sql
SELECT path, size FROM s3_files WHERE path LIKE '%.csv' LIMIT 100;
```

### Dropbox

```rust
use sqlite_vtable_opendal::backends::dropbox;

dropbox::register(
    &conn,
    "dropbox_files",
    "your_access_token",  // Get from Dropbox App Console
    "/"                   // root path
)?;
```

Query:
```sql
SELECT name, last_modified FROM dropbox_files ORDER BY last_modified DESC;
```

### Google Drive

```rust
use sqlite_vtable_opendal::backends::gdrive;

gdrive::register(
    &conn,
    "gdrive_files",
    "your_access_token",  // OAuth2 access token
    "/"                   // root path
)?;
```

Query:
```sql
SELECT path, size FROM gdrive_files WHERE is_dir = 0;
```

### HTTP

```rust
use sqlite_vtable_opendal::backends::http;

http::register(
    &conn,
    "http_data",
    "https://api.example.com/data"  // endpoint URL
)?;
```

Query:
```sql
SELECT path, content_type FROM http_data;
```

---

## 🔍 SQL Query Examples

### Find Large Files

```sql
SELECT path, size FROM local_files
WHERE size > 100000000
ORDER BY size DESC
LIMIT 10;
```

### Count Files by Extension

```sql
SELECT
    content_type,
    COUNT(*) as count,
    SUM(size) as total_size
FROM local_files
WHERE is_dir = 0
GROUP BY content_type
ORDER BY count DESC;
```

### Find Recently Modified Files

```sql
SELECT path, last_modified
FROM local_files
WHERE last_modified > '2024-01-01'
ORDER BY last_modified DESC;
```

### Calculate Directory Statistics

```sql
SELECT
    COUNT(*) as file_count,
    SUM(size) as total_bytes,
    AVG(size) as avg_size,
    MAX(size) as largest_file
FROM local_files
WHERE is_dir = 0;
```

---

## 🏗️ Architecture

### System Flow

```
SQLite Query → Virtual Table → OpenDAL → Storage Backend → Metadata
```

### Design Principles

1. **Lazy Loading** - Only fetch what's requested
2. **Metadata-First** - Content fetching is opt-in
3. **Async-Ready** - Non-blocking operations via Tokio
4. **Extensible** - Easy to add new storage backends

### Key Components

- **`types`** - Core data structures (`FileMetadata`, `QueryConfig`)
- **`error`** - Comprehensive error handling with `thiserror`
- **`vtab`** - SQLite virtual table infrastructure
- **`backends`** - Storage backend implementations

---

## 🧪 Testing

Run all tests:

```bash
cargo test
```

Run specific backend tests:

```bash
cargo test local_fs
```

Run with output:

```bash
cargo test -- --nocapture
```

### Test Coverage

- **Unit Tests**: 21 tests covering all backend functionality
- **Doc Tests**: 10 tests ensuring examples work
- **Integration Tests**: Full end-to-end SQLite query validation

---

## 🎯 Use Cases

### Data Engineering

```sql
-- Discover datasets without downloading
SELECT path FROM s3_files WHERE path LIKE '%/data/2024/%';
```

### Backup Auditing

```sql
-- Find backups older than 30 days
SELECT path, last_modified FROM dropbox_files
WHERE path LIKE '%backup%'
AND last_modified < date('now', '-30 days');
```

### Large File Detection

```sql
-- Identify files consuming most space
SELECT path, size FROM local_files
WHERE size > 1000000000
ORDER BY size DESC;
```

### Compliance Scanning

```sql
-- Find files modified in specific timeframe
SELECT path, last_modified FROM gdrive_files
WHERE last_modified BETWEEN '2024-01-01' AND '2024-12-31';
```

---

## 🛠️ Development

### Project Structure

```
src/
├── lib.rs                  # Library entry point
├── types.rs                # Core data structures
├── error.rs                # Error types
├── backends/
│   ├── mod.rs              # Backend trait
│   ├── local_fs.rs         # Local filesystem backend
│   ├── s3.rs               # AWS S3 backend
│   ├── dropbox.rs          # Dropbox backend
│   ├── gdrive.rs           # Google Drive backend
│   └── http.rs             # HTTP/HTTPS backend
└── vtab/
    └── mod.rs              # SQLite virtual table implementation
```

### Adding a New Backend

1. Create a new file in `src/backends/`
2. Implement the `StorageBackend` trait
3. Implement a `register()` function for SQLite
4. Add comprehensive tests
5. Update documentation

See `src/backends/local_fs.rs` for reference implementation.

---

## 🤝 Contributing

Contributions are welcome! Areas we'd love help with:

- 🌐 Additional storage backends (Azure Blob, Google Cloud Storage, MinIO)
- 📊 Query optimization (predicate pushdown, index hints)
- 💾 Metadata caching layer
- 📖 More usage examples and tutorials
- 🧪 Additional tests and benchmarks
- 🐛 Bug fixes and performance improvements

### Development Setup

```bash
git clone https://github.com/mukhtaronif/sqlite-vtab-opendal.git
cd sqlite-vtab-opendal
cargo build
cargo test
```

---

## 🔗 Related Projects

- [OpenDAL](https://github.com/apache/opendal) - Unified data access layer
- [rusqlite](https://github.com/rusqlite/rusqlite) - SQLite bindings for Rust
- [Surveilr](https://github.com/surveilr/surveilr) - Uses this library for federated queries


---

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

--- 

## 🙏 Acknowledgments

- Built with [OpenDAL](https://github.com/apache/opendal) for storage abstraction
- Inspired by SQLite's virtual table flexibility
- Developed for [Surveilr](https://github.com/surveilr/surveilr) federated queries

---

<div align="center">

**[Documentation](https://docs.rs/sqlite-vtable-opendal)** |
**[Crates.io](https://crates.io/crates/sqlite-vtable-opendal)** |
**[Repository](https://github.com/mukhtaronif/sqlite-vtab-opendal)** |
**[Issues](https://github.com/mukhtaronif/sqlite-vtab-opendal/issues)**

Made with ❤️ for the Rust community

</div>
