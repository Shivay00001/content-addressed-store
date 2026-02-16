"""Tests for Content-Addressed Block Store"""
import os
import shutil
import tempfile
import unittest

from content_addressed_store import ContentAddressedStore, ChunkAlgorithm


class TestChunkAlgorithm(unittest.TestCase):
    def test_small_data_single_chunk(self):
        chunker = ChunkAlgorithm()
        data = b"small"
        chunks = chunker.chunk(data)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], data)

    def test_large_data_multiple_chunks(self):
        chunker = ChunkAlgorithm(target_size=1024, min_size=512, max_size=2048)
        data = os.urandom(10000)
        chunks = chunker.chunk(data)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(b''.join(chunks), data)

    def test_empty_data(self):
        chunker = ChunkAlgorithm()
        chunks = chunker.chunk(b"")
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], b"")


class TestContentAddressedStore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.store = ContentAddressedStore(
            os.path.join(self.test_dir, "cas"),
            enable_compression=True
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_put_and_get(self):
        data = b"Hello, World!" * 100
        blob_hash = self.store.put(data)
        retrieved = self.store.get(blob_hash)
        self.assertEqual(data, retrieved)

    def test_deduplication(self):
        data = b"duplicate data" * 500
        hash1 = self.store.put(data, metadata={"v": 1})
        hash2 = self.store.put(data, metadata={"v": 2})
        self.assertEqual(hash1, hash2)

    def test_different_data_different_hashes(self):
        hash1 = self.store.put(b"data1" * 100)
        hash2 = self.store.put(b"data2" * 100)
        self.assertNotEqual(hash1, hash2)

    def test_delete(self):
        blob_hash = self.store.put(b"to be deleted" * 100)
        self.assertTrue(self.store.exists(blob_hash))
        self.assertTrue(self.store.delete(blob_hash))
        self.assertFalse(self.store.exists(blob_hash))

    def test_delete_nonexistent(self):
        self.assertFalse(self.store.delete("nonexistent_hash"))

    def test_get_nonexistent(self):
        self.assertIsNone(self.store.get("nonexistent_hash"))

    def test_metadata(self):
        data = b"metadata test" * 100
        meta = {"type": "text", "version": 1}
        blob_hash = self.store.put(data, metadata=meta)
        info = self.store.get_metadata(blob_hash)
        self.assertIsNotNone(info)
        self.assertEqual(info['metadata']['type'], 'text')

    def test_verify_integrity(self):
        data = b"integrity test" * 500
        blob_hash = self.store.put(data)
        self.assertTrue(self.store.verify(blob_hash))

    def test_list_blobs(self):
        self.store.put(b"blob1" * 100)
        self.store.put(b"blob2" * 100)
        blobs = self.store.list_blobs()
        self.assertEqual(len(blobs), 2)

    def test_stats(self):
        self.store.put(b"stats test" * 500)
        stats = self.store.stats()
        self.assertEqual(stats['blob_count'], 1)
        self.assertGreater(stats['total_blob_size'], 0)

    def test_garbage_collect(self):
        blob_hash = self.store.put(b"gc test" * 100)
        self.store.delete(blob_hash)
        removed = self.store.garbage_collect()
        self.assertGreaterEqual(removed, 0)

    def test_type_error(self):
        with self.assertRaises(TypeError):
            self.store.put("not bytes")

    def test_compression(self):
        # Highly compressible data
        data = b"a" * 10000
        blob_hash = self.store.put(data)
        retrieved = self.store.get(blob_hash)
        self.assertEqual(data, retrieved)


if __name__ == "__main__":
    unittest.main()
