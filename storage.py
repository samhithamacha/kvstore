import os
import time

class Storage:
    def __init__(self, filename):
        self.filename = filename
        # ensure file exists
        if not os.path.exists(self.filename):
            with open(self.filename, "w", encoding="utf-8") as f:
                pass

    def append(self, record):
        """Durably append a single record (one-line) to the WAL."""
        # write, flush, fsync for durability
        with open(self.filename, "a", encoding="utf-8") as f:
            f.write(record + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                # on some platforms fsync may fail; ignore but ideally keep it
                pass

    def replay(self, index, ttl_mgr):
        """Replay data.db, applying lines in order. Be resilient to partial/malformed lines."""
        try:
            with open(self.filename, "r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.rstrip("\n")
                    if not line:
                        continue

                    # try to parse the line robustly
                    try:
                        # split into at most 3 parts: OP, key, rest
                        parts = line.split(" ", 2)
                        op = parts[0]
                    except Exception:
                        # malformed — skip this line (likely partial write)
                        continue

                    try:
                        if op == "SET":
                            # SET key value (value may have spaces)
                            if len(parts) < 3:
                                # malformed SET line
                                continue
                            _, k, v = parts
                            index.set(k, v)

                        elif op == "DEL":
                            # DEL key
                            if len(parts) < 2:
                                continue
                            _, k = parts[:2]
                            index.delete(k)
                            ttl_mgr.delete(k)

                        elif op == "EXPIRE":
                            # EXPIRE key expiry_ts (expiry_ts is absolute ms)
                            if len(parts) < 3:
                                continue
                            _, k, ts = parts
                            try:
                                expiry_ts = float(ts)
                            except:
                                continue
                            ttl_mgr.replay_set(k, expiry_ts)

                        elif op == "PERSIST":
                            # PERSIST key -- remove TTL
                            if len(parts) < 2:
                                continue
                            _, k = parts[:2]
                            ttl_mgr.delete(k)

                        else:
                            # unknown op: ignore
                            continue

                    except Exception:
                        # guard against any unexpected runtime error on this line
                        continue

        except FileNotFoundError:
            # no log yet — that's fine
            return
