"""
Content-Addressed Block Store with Chunking and Deduplication
Alternative implementation focused on efficient large file storage
"""

import hashlib
import json
import os
import sqlite3
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Dict, Tuple, BinaryIO
from datetime import datetime
import threading


class ChunkAlgorithm:
    """Content-defined chunking using Rabin fingerprinting approximation"""
    
    def __init__(self, target_size: int = 4096, min_size: int = 2048, max_size: int = 8192):
        self.target_size = target_size
        self.min_size = min_size
        self.max_size = max_size
        self.window_size = 64
        self.mask = (1 << 13) - 1  # Rolling hash mask
    
    def chunk(self, data: bytes) -> List[bytes]:
        """Split data into content-defined chunks"""
        if len(data) <= self.min_size:
            return [data]
        
        chunks = []
        start = 0
        
        while start < len(data):
            end = self._find_chunk_boundary(data, start)
            chunks.append(data[start:end])
            start = end
        
        return chunks
    
    def _find_chunk_boundary(self, data: bytes, start: int) -> int:
        """Find next chunk boundary using rolling hash"""
        pos = start + self.min_size
        end = min(start + self.max_size, len(data))
        
        if pos >= end:
            return end
        
        # Simple rolling hash
        window = data[pos:min(pos + self.window_size, end)]
        hash_val = sum(b * (i + 1) for i, b in enumerate(window))
        
        while pos < end - self.window_size:
            if (hash_val & self.mask) == 0:
                return pos
            
            # Roll the window
            if pos + self.window_size < len(data):
                hash_val = hash_val - data[pos] + data[pos + self.window_size] * self.window_size
            
            pos += 1
        
        return end


@dataclass
class Chunk:
    """Represents a content chunk"""
    hash: str
    size: int
    data: Optional[bytes] = None
    compressed: bool = False
    ref_count: int = 0


@dataclass
class Blob:
    """Represents a stored blob composed of chunks"""
    hash: str
    size: int
    chunks: List[str]  # List of chunk hashes
    metadata: Dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class ContentAddressedStore:
    """
    Content-addressed storage with:
    - Content-defined chunking for large files
    - Automatic deduplication at chunk level
    - Optional compression
    - Reference counting for garbage collection
    """
    
    def __init__(
        self,
        storage_path: str,
        enable_compression: bool = True,
        chunk_target_size: int = 4096
    ):
        self.storage_path = Path(storage_path)
        self.enable_compression = enable_compression
        self.chunker = ChunkAlgorithm(target_size=chunk_target_size)
        self.lock = threading.RLock()
        
        # Initialize storage
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.chunks_path = self.storage_path / "chunks"
        self.chunks_path.mkdir(exist_ok=True)
        
        # Initialize database
        self.db_path = self.storage_path / "store.db"
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database for metadata"""
        with sqlite3.connect(str(self.db_path)) as conn:
            # Chunks table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    hash TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    compressed INTEGER NOT NULL,
                    ref_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                )
            """)
            
            # Blobs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blobs (
                    hash TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    chunk_list TEXT NOT NULL,
                    metadata TEXT,
                    created_at TEXT NOT NULL,
                    last_accessed TEXT
                )
            """)
            
            # Blob-chunk relationship
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blob_chunks (
                    blob_hash TEXT NOT NULL,
                    chunk_hash TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    PRIMARY KEY (blob_hash, chunk_hash, chunk_index),
                    FOREIGN KEY (blob_hash) REFERENCES blobs(hash) ON DELETE CASCADE,
                    FOREIGN KEY (chunk_hash) REFERENCES chunks(hash) ON DELETE CASCADE
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_ref ON chunks(ref_count)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_blob_chunks ON blob_chunks(blob_hash)")
            conn.commit()
    
    def _chunk_file_path(self, chunk_hash: str) -> Path:
        """Get file path for chunk storage with sharding"""
        prefix = chunk_hash[:4]
        shard_dir = self.chunks_path / prefix
        shard_dir.mkdir(exist_ok=True)
        return shard_dir / chunk_hash
    
    def _hash_data(self, data: bytes) -> str:
        """Calculate SHA256 hash of data"""
        return hashlib.sha256(data).hexdigest()
    
    def _store_chunk(self, data: bytes) -> str:
        """Store a single chunk and return its hash"""
        chunk_hash = self._hash_data(data)
        
        with self.lock:
            # Check if chunk already exists
            if self._chunk_exists(chunk_hash):
                self._increment_chunk_ref(chunk_hash)
                return chunk_hash
            
            # Compress if enabled
            stored_data = data
            compressed = False
            if self.enable_compression and len(data) > 100:
                compressed_data = zlib.compress(data, level=6)
                if len(compressed_data) < len(data):
                    stored_data = compressed_data
                    compressed = True
            
            # Write chunk to disk
            chunk_path = self._chunk_file_path(chunk_hash)
            with open(chunk_path, 'wb') as f:
                f.write(stored_data)
                os.fsync(f.fileno())
            
            # Update database
            now = datetime.utcnow().isoformat()
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO chunks (hash, size, compressed, ref_count, created_at)
                    VALUES (?, ?, ?, 1, ?)
                """, (chunk_hash, len(data), int(compressed), now))
                conn.commit()
            
            return chunk_hash
    
    def _load_chunk(self, chunk_hash: str) -> Optional[bytes]:
        """Load a chunk from storage"""
        with self.lock:
            # Get chunk metadata
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    SELECT compressed FROM chunks WHERE hash = ?
                """, (chunk_hash,))
                row = cursor.fetchone()
                if not row:
                    return None
                compressed = bool(row[0])
            
            # Read chunk data
            chunk_path = self._chunk_file_path(chunk_hash)
            if not chunk_path.exists():
                return None
            
            with open(chunk_path, 'rb') as f:
                data = f.read()
            
            # Decompress if needed
            if compressed:
                data = zlib.decompress(data)
            
            return data
    
    def put(self, data: bytes, metadata: Optional[Dict] = None) -> str:
        """
        Store data and return its content hash.
        Automatically chunks large data and deduplicates.
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")
        
        blob_hash = self._hash_data(data)
        
        with self.lock:
            # Check if blob already exists
            if self._blob_exists(blob_hash):
                return blob_hash
            
            # Chunk the data
            chunks = self.chunker.chunk(data)
            chunk_hashes = []
            
            # Store each chunk
            for chunk_data in chunks:
                chunk_hash = self._store_chunk(chunk_data)
                chunk_hashes.append(chunk_hash)
            
            # Store blob metadata
            now = datetime.utcnow().isoformat()
            metadata_json = json.dumps(metadata or {})
            chunk_list_json = json.dumps(chunk_hashes)
            
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO blobs (hash, size, chunk_list, metadata, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (blob_hash, len(data), chunk_list_json, metadata_json, now))
                
                # Link blob to chunks
                for idx, chunk_hash in enumerate(chunk_hashes):
                    conn.execute("""
                        INSERT INTO blob_chunks (blob_hash, chunk_hash, chunk_index)
                        VALUES (?, ?, ?)
                    """, (blob_hash, chunk_hash, idx))
                
                conn.commit()
            
            return blob_hash
    
    def get(self, blob_hash: str) -> Optional[bytes]:
        """Retrieve blob data by its hash"""
        with self.lock:
            # Get chunk list
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    SELECT chunk_list FROM blobs WHERE hash = ?
                """, (blob_hash,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                chunk_hashes = json.loads(row[0])
                
                # Update access time
                now = datetime.utcnow().isoformat()
                conn.execute("""
                    UPDATE blobs SET last_accessed = ? WHERE hash = ?
                """, (now, blob_hash))
                conn.commit()
            
            # Reconstruct data from chunks
            data_parts = []
            for chunk_hash in chunk_hashes:
                chunk_data = self._load_chunk(chunk_hash)
                if chunk_data is None:
                    return None
                data_parts.append(chunk_data)
            
            return b''.join(data_parts)
    
    def get_metadata(self, blob_hash: str) -> Optional[Dict]:
        """Get blob metadata"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT metadata, size, created_at, last_accessed
                FROM blobs WHERE hash = ?
            """, (blob_hash,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                'metadata': json.loads(row[0]),
                'size': row[1],
                'created_at': row[2],
                'last_accessed': row[3]
            }
    
    def exists(self, blob_hash: str) -> bool:
        """Check if a blob exists"""
        return self._blob_exists(blob_hash)
    
    def delete(self, blob_hash: str) -> bool:
        """Delete a blob and decrement chunk references"""
        with self.lock:
            if not self._blob_exists(blob_hash):
                return False
            
            # Get chunk list
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    SELECT chunk_list FROM blobs WHERE hash = ?
                """, (blob_hash,))
                row = cursor.fetchone()
                
                if not row:
                    return False
                
                chunk_hashes = json.loads(row[0])
                
                # Delete blob
                conn.execute("DELETE FROM blobs WHERE hash = ?", (blob_hash,))
                conn.execute("DELETE FROM blob_chunks WHERE blob_hash = ?", (blob_hash,))
                conn.commit()
            
            # Decrement chunk references
            for chunk_hash in chunk_hashes:
                self._decrement_chunk_ref(chunk_hash)
            
            return True
    
    def list_blobs(self, limit: int = 100) -> List[Dict]:
        """List recent blobs"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT hash, size, created_at, last_accessed
                FROM blobs
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            
            return [
                {
                    'hash': row[0],
                    'size': row[1],
                    'created_at': row[2],
                    'last_accessed': row[3]
                }
                for row in cursor.fetchall()
            ]
    
    def _blob_exists(self, blob_hash: str) -> bool:
        """Check if blob exists"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("SELECT 1 FROM blobs WHERE hash = ?", (blob_hash,))
            return cursor.fetchone() is not None
    
    def _chunk_exists(self, chunk_hash: str) -> bool:
        """Check if chunk exists"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("SELECT 1 FROM chunks WHERE hash = ?", (chunk_hash,))
            return cursor.fetchone() is not None
    
    def _increment_chunk_ref(self, chunk_hash: str):
        """Increment chunk reference count"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?
            """, (chunk_hash,))
            conn.commit()
    
    def _decrement_chunk_ref(self, chunk_hash: str):
        """Decrement chunk reference count"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?
            """, (chunk_hash,))
            conn.commit()
    
    def garbage_collect(self) -> int:
        """Remove unreferenced chunks"""
        with self.lock:
            # Find chunks with zero references
            with sqlite3.connect(str(self.db_path)) as conn:
                cursor = conn.execute("""
                    SELECT hash FROM chunks WHERE ref_count = 0
                """)
                unreferenced = [row[0] for row in cursor.fetchall()]
            
            # Delete chunks
            removed = 0
            for chunk_hash in unreferenced:
                chunk_path = self._chunk_file_path(chunk_hash)
                if chunk_path.exists():
                    chunk_path.unlink()
                    removed += 1
                
                with sqlite3.connect(str(self.db_path)) as conn:
                    conn.execute("DELETE FROM chunks WHERE hash = ?", (chunk_hash,))
                    conn.commit()
            
            return removed
    
    def stats(self) -> Dict:
        """Get storage statistics"""
        with sqlite3.connect(str(self.db_path)) as conn:
            # Blob stats
            cursor = conn.execute("""
                SELECT COUNT(*), SUM(size), AVG(size) FROM blobs
            """)
            blob_stats = cursor.fetchone()
            
            # Chunk stats
            cursor = conn.execute("""
                SELECT COUNT(*), SUM(size), AVG(ref_count) FROM chunks
            """)
            chunk_stats = cursor.fetchone()
            
            # Deduplication ratio
            cursor = conn.execute("SELECT SUM(size) FROM chunks")
            unique_size = cursor.fetchone()[0] or 0
            total_size = blob_stats[1] or 0
            
            dedup_ratio = (1 - unique_size / total_size) * 100 if total_size > 0 else 0
        
        return {
            'blob_count': blob_stats[0] or 0,
            'total_blob_size': total_size,
            'avg_blob_size': blob_stats[2] or 0,
            'chunk_count': chunk_stats[0] or 0,
            'unique_chunk_size': unique_size,
            'avg_chunk_refs': chunk_stats[2] or 0,
            'deduplication_ratio': f"{dedup_ratio:.2f}%",
            'space_saved': total_size - unique_size
        }
    
    def verify(self, blob_hash: str) -> bool:
        """Verify blob integrity"""
        data = self.get(blob_hash)
        if data is None:
            return False
        
        calculated_hash = self._hash_data(data)
        return calculated_hash == blob_hash


# Example usage
if __name__ == "__main__":
    store = ContentAddressedStore("./cas_store", enable_compression=True)
    
    # Store some data
    data1 = b"Hello, World! " * 1000
    hash1 = store.put(data1, metadata={"type": "text", "version": 1})
    print(f"Stored blob: {hash1}")
    
    # Store duplicate data (will deduplicate)
    data2 = b"Hello, World! " * 1000
    hash2 = store.put(data2, metadata={"type": "text", "version": 2})
    print(f"Stored duplicate: {hash2}")
    print(f"Hashes match: {hash1 == hash2}")
    
    # Store data with partial overlap
    data3 = b"Hello, World! " * 500 + b"New content " * 500
    hash3 = store.put(data3)
    print(f"Stored overlapping: {hash3}")
    
    # Retrieve data
    retrieved = store.get(hash1)
    print(f"Retrieved matches original: {retrieved == data1}")
    
    # Get statistics
    stats = store.stats()
    print(f"\nStorage stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Verify integrity
    is_valid = store.verify(hash1)
    print(f"\nIntegrity check: {is_valid}")
    
    # List blobs
    blobs = store.list_blobs()
    print(f"\nStored blobs: {len(blobs)}")
