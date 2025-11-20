class Index:
    def __init__(self):
        self.keys = []
        self.values = []

    # binary search
    def _find(self, key):
        lo, hi = 0, len(self.keys)-1
        while lo <= hi:
            mid = (lo+hi)//2
            if self.keys[mid] == key:
                return mid
            elif self.keys[mid] < key:
                lo = mid+1
            else:
                hi = mid-1
        return ~lo

    def set(self, key, value):
        pos = self._find(key)
        if pos >= 0:
            self.values[pos] = value
        else:
            ins = ~pos
            self.keys.insert(ins, key)
            self.values.insert(ins, value)

    def get(self, key):
        pos = self._find(key)
        if pos >= 0:
            return self.values[pos]
        return None

    def delete(self, key):
        pos = self._find(key)
        if pos >= 0:
            self.keys.pop(pos)
            self.values.pop(pos)
            return True
        return False

    # lexicographic range scan
    def range_scan(self, start, end):
        result = []
        for k in self.keys:
            if (start == "" or k >= start) and (end == "" or k <= end):
                result.append(k)
        return result

