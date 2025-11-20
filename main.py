import sys
import time
from index import Index
from storage import Storage
from transaction import TransactionManager
from ttl import TTLManager

# Initialize components
index = Index()
ttl_mgr = TTLManager()
storage = Storage("data.db")
txn = TransactionManager(index, ttl_mgr, storage)

# Replay existing log on startup
storage.replay(index, ttl_mgr)

def output(msg):
    """Print output immediately."""
    sys.stdout.write(str(msg) + "\n")
    sys.stdout.flush()

def get_key_visible(key):
    """Return value if key exists and not expired."""
    val = index.get(key)
    if val is None:
        return None
    if ttl_mgr.is_expired(key):
        index.delete(key)
        ttl_mgr.delete(key)
        return None
    return val

def process_command(parts):
    if not parts:
        return

    cmd = parts[0].upper()

    if cmd == "EXIT":
        sys.exit(0)

    elif cmd == "GET":
        if len(parts) != 2:
            output("ERR bad command")
            return
        key = parts[1]
        val = txn.read(key)
        val = None if (val is None or ttl_mgr.is_expired(parts[1])) else val
        output(val if val is not None else "nil")

    elif cmd == "SET":
        if len(parts) < 3:
            output("ERR bad command")
            return
        key = parts[1]
        value = " ".join(parts[2:])
        txn.write(key, value)
        output("OK")

    elif cmd == "DEL":
        if len(parts) != 2:
            output("ERR bad command")
            return
        key = parts[1]
        res = txn.delete(key)
        output(res)

    elif cmd == "EXISTS":
        if len(parts) != 2:
            output("ERR bad command")
            return
        key = parts[1]
        val = txn.read(key)
        val = None if (val is None or ttl_mgr.is_expired(key)) else val
        output(1 if val is not None else 0)

    elif cmd == "MSET":
        if (len(parts)-1) % 2 != 0:
            output("ERR bad command")
            return
        for i in range(1, len(parts), 2):
            k, v = parts[i], parts[i+1]
            txn.write(k, v)
        output("OK")

    elif cmd == "MGET":
        keys = parts[1:]
        for k in keys:
            val = txn.read(k)
            val = None if (val is None or ttl_mgr.is_expired(k)) else val
            output(val if val is not None else "nil")

    elif cmd == "BEGIN":
        txn.begin()
        output("OK")

    elif cmd == "COMMIT":
        txn.commit()
        output("OK")

    elif cmd == "ABORT":
        txn.abort()
        output("OK")

    elif cmd == "EXPIRE":
        if len(parts) != 3:
            output("ERR bad command")
            return
        key = parts[1]
        try:
            ms = int(parts[2])
        except:
            output("ERR bad value")
            return
        val = txn.read(key)
        if val is None:
            output(0)
        else:
            ttl_mgr.set(key, ms)
            output(1)

    elif cmd == "TTL":
        if len(parts) != 2:
            output("ERR bad command")
            return
        key = parts[1]
        if index.get(key) is None or ttl_mgr.is_expired(key):
            output(-2)
            return
        ttl = ttl_mgr.remaining(key)
        output(ttl if ttl is not None else -1)

    elif cmd == "PERSIST":
        if len(parts) != 2:
            output("ERR bad command")
            return
        key = parts[1]
        res = ttl_mgr.persist(key)
        output(res)

    elif cmd == "RANGE":
        if len(parts) != 3:
            output("ERR bad command")
            return
        start, end = parts[1], parts[2]
        keys = index.range_scan(start, end)
        for k in keys:
            if not ttl_mgr.is_expired(k):
                output(k)
        output("END")

    else:
        output("ERR unknown command")

def main():
    for line in sys.stdin:
        process_command(line.strip().split())

if __name__ == "__main__":
    main()
