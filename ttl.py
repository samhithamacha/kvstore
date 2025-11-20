import time

class TTLManager:
    def __init__(self):
        self.expiry = {}  # key → timestamp

    def set(self, key, ms):
        if ms <= 0:
           expiry_ts = time.time() * 1000 - 1
        else:
           expiry_ts = time.time() * 1000 + ms
        self.expiry[key] = expiry_ts
        return expiry_ts


    def replay_set(self, key, timestamp):
        self.expiry[key] = timestamp

    def is_expired(self, key):
        if key not in self.expiry:
            return False
        return time.time() * 1000 >= self.expiry[key]

    def delete(self, key):
        self.expiry.pop(key, None)

    def persist(self, key):
        if key in self.expiry:
            del self.expiry[key]
            return 1
        return 0

    def remaining(self, key):
        if key not in self.expiry:
            return None
        diff = int(self.expiry[key] - time.time() * 1000)
        return max(diff, 0)
