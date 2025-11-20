import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict


class KVStore:
    def __init__(self, storage_path: str = "kvstore.log"):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.index: Dict[str, str] = {}
        self._rebuild_index()
    
    def _rebuild_index(self) -> None:
        """Rebuild the in-memory index from the append-only log file."""
        if not self.storage_path.exists():
            return
        
        with open(self.storage_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    key = entry.get('key')
                    value = entry.get('value')
                    if key is not None:
                        if value is not None:
                            self.index[key] = value
                        else:
                            self.index.pop(key, None)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping corrupt log line: {e}", file=sys.stderr)
                    continue
    
    def set(self, key: str, value: str) -> None:
        """Set a key-value pair and append to the log file."""
        if not key:
            raise ValueError("Key cannot be empty")
        
        entry = {"key": key, "value": value}
        
        with open(self.storage_path, 'a') as f:
            f.write(json.dumps(entry) + '\n')
        
        self.index[key] = value
    
    def get(self, key: str) -> Optional[str]:
        """Get the value for a given key."""
        return self.index.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete a key by writing a tombstone to the log."""
        if key not in self.index:
            return False
        
        entry = {"key": key, "value": None}
        
        with open(self.storage_path, 'a') as f:
            f.write(json.dumps(entry) + '\n')
        
        del self.index[key]
        return True
    
    def keys(self) -> list:
        """Return all keys currently in the store."""
        return list(self.index.keys())
    
    def clear_storage(self) -> None:
        """Clear the storage file (for testing purposes)."""
        if self.storage_path.exists():
            os.remove(self.storage_path)
        self.index.clear()
