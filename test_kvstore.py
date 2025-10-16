import pytest
import os
from pathlib import Path
from kvstore import KVStore


@pytest.fixture
def temp_store(tmp_path):
    """Create a temporary KVStore for testing."""
    storage_file = tmp_path / "test_kvstore.log"
    store = KVStore(str(storage_file))
    yield store
    if storage_file.exists():
        os.remove(storage_file)


def test_set_and_get(temp_store):
    """Test basic SET and GET operations."""
    temp_store.set("name", "Alice")
    assert temp_store.get("name") == "Alice"
    
    temp_store.set("age", "30")
    assert temp_store.get("age") == "30"


def test_get_nonexistent_key(temp_store):
    """Test getting a key that doesn't exist."""
    assert temp_store.get("nonexistent") is None


def test_overwrite_key(temp_store):
    """Test overwriting an existing key."""
    temp_store.set("color", "red")
    assert temp_store.get("color") == "red"
    
    temp_store.set("color", "blue")
    assert temp_store.get("color") == "blue"


def test_empty_key_raises_error(temp_store):
    """Test that empty keys raise an error."""
    with pytest.raises(ValueError):
        temp_store.set("", "value")


def test_persistence_and_rebuild(tmp_path):
    """Test that data persists and index rebuilds correctly."""
    storage_file = tmp_path / "persist_test.log"
    
    store1 = KVStore(str(storage_file))
    store1.set("key1", "value1")
    store1.set("key2", "value2")
    store1.set("key3", "value3")
    
    store2 = KVStore(str(storage_file))
    assert store2.get("key1") == "value1"
    assert store2.get("key2") == "value2"
    assert store2.get("key3") == "value3"
    
    if storage_file.exists():
        os.remove(storage_file)


def test_rebuild_with_updates(tmp_path):
    """Test that rebuilding handles updates correctly."""
    storage_file = tmp_path / "update_test.log"
    
    store1 = KVStore(str(storage_file))
    store1.set("counter", "1")
    store1.set("counter", "2")
    store1.set("counter", "3")
    
    store2 = KVStore(str(storage_file))
    assert store2.get("counter") == "3"
    
    if storage_file.exists():
        os.remove(storage_file)


def test_delete_key(temp_store):
    """Test deleting a key."""
    temp_store.set("temp", "value")
    assert temp_store.get("temp") == "value"
    
    result = temp_store.delete("temp")
    assert result is True
    assert temp_store.get("temp") is None


def test_delete_nonexistent_key(temp_store):
    """Test deleting a key that doesn't exist."""
    result = temp_store.delete("nonexistent")
    assert result is False


def test_delete_persistence(tmp_path):
    """Test that deletes persist across restarts."""
    storage_file = tmp_path / "delete_test.log"
    
    store1 = KVStore(str(storage_file))
    store1.set("key1", "value1")
    store1.set("key2", "value2")
    store1.delete("key1")
    
    store2 = KVStore(str(storage_file))
    assert store2.get("key1") is None
    assert store2.get("key2") == "value2"
    
    if storage_file.exists():
        os.remove(storage_file)


def test_keys_list(temp_store):
    """Test listing all keys."""
    assert temp_store.keys() == []
    
    temp_store.set("a", "1")
    temp_store.set("b", "2")
    temp_store.set("c", "3")
    
    keys = temp_store.keys()
    assert len(keys) == 3
    assert "a" in keys
    assert "b" in keys
    assert "c" in keys


def test_append_only_format(tmp_path):
    """Test that the storage format is append-only."""
    storage_file = tmp_path / "format_test.log"
    
    store = KVStore(str(storage_file))
    store.set("key", "value1")
    store.set("key", "value2")
    
    with open(storage_file, 'r') as f:
        lines = f.readlines()
    
    assert len(lines) == 2
    assert '"key"' in lines[0]
    assert '"value1"' in lines[0]
    assert '"key"' in lines[1]
    assert '"value2"' in lines[1]
    
    if storage_file.exists():
        os.remove(storage_file)


def test_complex_values(temp_store):
    """Test storing complex string values."""
    temp_store.set("json", '{"name": "Alice", "age": 30}')
    assert temp_store.get("json") == '{"name": "Alice", "age": 30}'
    
    temp_store.set("multiword", "This is a longer value with spaces")
    assert temp_store.get("multiword") == "This is a longer value with spaces"
