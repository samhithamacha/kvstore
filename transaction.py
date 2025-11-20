import time

class TransactionManager:
    def __init__(self, index, ttl_mgr, storage):
        self.index = index
        self.ttl = ttl_mgr
        self.storage = storage

        self.active = False
        self.buffer = {}

    def begin(self):
        self.active = True
        self.buffer = {}

    def read(self, key):
        if self.active and key in self.buffer:
            return self.buffer[key]
        return self.index.get(key)

    def write(self, key, value):
        if self.active:
            self.buffer[key] = value
        else:
            self.index.set(key, value)
            self.storage.append(f"SET {key} {value}")

    def delete(self, key):
        exists = self.read(key) is not None
        if not exists:
            return 0
        if self.active:
            self.buffer[key] = None
        else:
            self.index.delete(key)
            self.ttl.delete(key)
            self.storage.append(f"DEL {key}")
        return 1

    def commit(self):
        if not self.active:
            return
        for k, v in self.buffer.items():
            if v is None:
                self.index.delete(k)
                self.ttl.delete(k)
                self.storage.append(f"DEL {k}")
            else:
                self.index.set(k, v)
                self.storage.append(f"SET {k} {v}")
        self.active = False
        self.buffer = {}

    def abort(self):
        self.active = False
        self.buffer = {}
