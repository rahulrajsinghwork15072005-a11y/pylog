# pylog — Append-Only Log Line-by-Line

> Deep reading of `pylog/log.py:27` — `recover:112` + `append:155` + sparse index `103`. Plain English for non-coders, .

Commit log = append-only file like a ship's logbook — only add to end. Solves (1) crash mid-write and (2) fast lookup via sparse index.

## Segment = .log + .index

```py
class Segment:
 def __init__(self, dir_path, base_offset, index_interval_bytes):
 self.log_path = f"{base_offset:020d}.log" # zero-padded so sort = order
 self.index_path = f"{base_offset:020d}.index"
 self.index = [] # (rel_offset, byte_pos) sparse
 self.size = 0
 self.last_indexed_position = -1
```

## recover — crash-safe star

```py
def recover(self):
 # reset
 if not exists(log_path): create empty; return base_offset
 next_offset = base_offset; good_size = 0
 with open(log_path, "rb") as f:
 while True:
 position = f.tell
 try: rec = read_record(f) # CRC check inside
 except CorruptRecord: break # torn → stop, discard tail
 if rec is None: break # clean EOF
 self._maybe_index(rec.offset, position)
 good_size = f.tell; next_offset = rec.offset + 1
 if good_size < file_size: truncate(good_size) # cut torn tail
 return next_offset
```

Plain: replay, checksum fails → stop + truncate at first bad record — worst case lose last unfinished record.

## append + sparse index

```py
def append(self, offset, record_bytes, timestamp, fsync=False):
 position = self.size
 fh.write(record_bytes); fh.flush
 if fsync: os.fsync(fh.fileno)
 self.size += len(record_bytes)
 if not index or position - last_indexed >= interval:
 rel = offset - base_offset
 index.append((rel, position)); idx_fh.write(pack(rel, position))
```

Every `index_interval_bytes` drops a signpost — read binary-searches nearest ≤ target, seeks, scans forward O(log segments + short scan).

## read payoff

To find #12345: binary search `index` for signpost ≤12345 → `seek(start_pos)` `read:174` → scan forward until `rec.offset==12345` or `>`. O(log segments).

One sentence: **segments + CRC + truncate at first failure + sparse signposts**.
