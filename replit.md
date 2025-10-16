# Command-Line Key-Value Store

## Overview
A simple command-line key-value store with append-only disk storage. The store persists data to disk and rebuilds its index on startup, ensuring durability across sessions.

## Features
- **SET/GET Commands**: Store and retrieve key-value pairs
- **Append-Only Storage**: All operations are logged to disk in append-only format
- **Index Rebuilding**: Automatically rebuilds in-memory index from disk on startup
- **DELETE Support**: Delete keys with tombstone markers
- **Persistence**: Data survives program restarts
- **Automated Testing**: Comprehensive test suite with 12 tests

## Project Structure
- `kvstore.py` - Core key-value store implementation with append-only storage
- `main.py` - Interactive command-line interface
- `test_kvstore.py` - Automated test suite using pytest
- `kvstore.log` - Default storage file (created on first use)

## Usage

### Interactive Mode
Run the CLI to use the key-value store interactively:
```bash
python main.py
```

Available commands:
- `SET <key> <value>` - Set a key-value pair
- `GET <key>` - Get the value for a key
- `DELETE <key>` - Delete a key
- `KEYS` - List all keys
- `HELP` - Show help message
- `EXIT` - Exit the program

### Custom Storage File
Specify a custom storage file:
```bash
python main.py --storage custom_store.log
```

### Running Tests
Run the automated test suite:
```bash
pytest test_kvstore.py -v
```

## How It Works

### Append-Only Storage
All SET and DELETE operations are appended to a log file as JSON entries:
```json
{"key": "name", "value": "Alice"}
{"key": "age", "value": "30"}
{"key": "name", "value": null}  // tombstone for deletion
```

### Index Rebuilding
On startup, the store reads the entire log file and rebuilds the in-memory index:
- SET operations add/update keys in the index
- DELETE operations (null values) remove keys from the index
- Later entries for the same key override earlier ones

This ensures the index always reflects the current state of the store.

## Architecture

### KVStore Class
- `set(key, value)`: Appends a SET operation to log and updates index
- `get(key)`: Returns value from in-memory index
- `delete(key)`: Appends a tombstone to log and removes from index
- `_rebuild_index()`: Reconstructs index from log file on startup
- `keys()`: Returns list of all current keys

### Data Persistence
- All writes are immediately flushed to disk
- No data is lost on unexpected shutdown
- Index is deterministically rebuilt from log

## Recent Changes
- 2025-10-16: Initial implementation complete
  - Core KVStore class with append-only storage
  - Interactive CLI with SET, GET, DELETE, KEYS commands
  - Comprehensive test suite (12 tests, all passing)
  - Workflow configured for easy running

## Future Enhancements
- Log compaction to reclaim disk space
- Batch operations for multiple keys
- Snapshot functionality for faster rebuilding
- Performance optimization for large datasets
- Binary storage format for efficiency
