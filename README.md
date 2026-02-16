# Content-Addressed Block Store

A Python content-addressed storage system with content-defined chunking and automatic deduplication. Efficiently stores large files by splitting them into content-defined chunks and deduplicating at the chunk level.

## Features

- **Content-Defined Chunking**: Rabin fingerprinting approximation for intelligent file splitting
- **Automatic Deduplication**: Chunk-level deduplication with reference counting
- **Optional Compression**: zlib compression for space efficiency
- **SHA256 Content Addressing**: Immutable, verifiable content identifiers
- **Garbage Collection**: Reference-counted cleanup of unreferenced chunks
- **SQLite Metadata**: Robust metadata storage with proper indexing
- **Thread-Safe**: Reentrant lock protection for concurrent access
- **Integrity Verification**: Hash-based blob verification

## Installation

```bash
pip install content-addressed-store
```

## Usage

```python
from content_addressed_store import ContentAddressedStore

store = ContentAddressedStore("./my_store", enable_compression=True)

# Store data (returns content hash)
blob_hash = store.put(b"Hello, World!" * 1000, metadata={"type": "text"})

# Retrieve data
data = store.get(blob_hash)

# Verify integrity
assert store.verify(blob_hash)

# Get statistics
print(store.stats())
```

## License

MIT
