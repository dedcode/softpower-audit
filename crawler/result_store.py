"""Compact run accounting and bounded serialization; original evidence stays in GCS/BQ."""
import json
from collections import Counter, defaultdict
from typing import NamedTuple


class Entry(NamedTuple):
    updated_at: str
    status: str
    outlet: str
    attempts: int
    retries: int
    response_bytes: int
    stored_bytes: int

    @property
    def terminal(self):
        return self.status not in ('deferred', 'retrying')


class ResultIndex:
    """Keep only the latest compact metadata, never article text or attempt histories.

    Pending entries are retained for timestamp ordering, but do not count as done.
    This matters when a newer deferred checkpoint follows an older final result.
    """
    def __init__(self):
        self.entries = {}
        self.counts = Counter()
        self.domains = defaultdict(Counter)
        self.metrics = Counter()
        self.completed = 0

    def __contains__(self, article_id):
        entry = self.entries.get(article_id)
        return entry is not None and entry.terminal

    def __len__(self):
        return self.completed

    def record(self, result):
        previous = self.entries.get(result['article_id'])
        if previous is not None and result['updated_at'] <= previous.updated_at:
            return False
        attempts = [len(event.get('http_attempts', ())) for event in result.get('attempts', ())]
        entry = Entry(result['updated_at'], result['status'], result['outlet'],
                      sum(attempts), sum(max(0, count - 1) for count in attempts),
                      result.get('response_bytes', 0), result.get('stored_bytes', 0))
        if previous is not None:
            self._count(previous, -1)
        self.entries[result['article_id']] = entry
        self._count(entry, 1)
        return True

    def _count(self, entry, sign):
        if not entry.terminal:
            return
        self.completed += sign
        self.counts[entry.status] += sign
        self.domains[entry.outlet][entry.status] += sign
        for name in ('attempts', 'retries', 'response_bytes', 'stored_bytes'):
            self.metrics[name] += sign * getattr(entry, name)

    def snapshot(self):
        return {
            'counts': +self.counts,
            'domains': {outlet: +counts for outlet, counts in self.domains.items()},
            **{name: self.metrics[name] for name in ('attempts', 'retries', 'response_bytes', 'stored_bytes')},
        }


class JsonlBatch:
    """Byte- and row-bounded records, allowing one oversized record by itself."""
    def __init__(self, max_rows, max_bytes):
        self.max_rows = max_rows
        self.max_bytes = max_bytes
        self.rows = []
        self.byte_count = 0

    def __len__(self):
        return len(self.rows)

    def fits(self, encoded):
        return not self.rows or (len(self.rows) < self.max_rows and self.byte_count + len(encoded) <= self.max_bytes)

    @property
    def full(self):
        return len(self.rows) >= self.max_rows or self.byte_count >= self.max_bytes

    def append(self, encoded):
        if not self.fits(encoded):
            raise ValueError('Flush the batch before appending this record')
        self.rows.append(encoded)
        self.byte_count += len(encoded)

    def data(self):
        return b''.join(self.rows)

    def clear(self):
        self.rows.clear()
        self.byte_count = 0


def encode_jsonl(record):
    return (json.dumps(record, separators=(',', ':')) + '\n').encode()


def iter_input_rows(stream, chunk_size=65536):
    """Read an existing JSON array manifest incrementally, with no new dependency.

    The manifest's elements must be objects. Only an individual object and a
    small read buffer are decoded at once; caller decides which rows to retain.
    """
    decoder = json.JSONDecoder()
    buffer = ''
    position = 0
    eof = False

    def refill():
        nonlocal buffer, position, eof
        buffer = buffer[position:]
        position = 0
        part = stream.read(chunk_size)
        eof = not part
        buffer += part

    def next_character():
        nonlocal position
        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position < len(buffer):
                return buffer[position]
            if eof:
                return ''
            refill()

    if next_character() != '[':
        raise ValueError('Input manifest must be a JSON array')
    position += 1
    first = True
    while True:
        char = next_character()
        if char == ']':
            position += 1
            break
        if not first:
            if char != ',':
                raise ValueError('Expected a comma in input manifest')
            position += 1
            char = next_character()
        if char != '{':
            raise ValueError('Input manifest elements must be objects')
        while True:
            try:
                row, end = decoder.raw_decode(buffer, position)
                position = end
                break
            except json.JSONDecodeError:
                if eof:
                    raise ValueError('Incomplete input manifest') from None
                refill()
        yield row
        first = False
    if next_character():
        raise ValueError('Unexpected data after input manifest')


def memory_usage():
    """Linux cgroup usage includes child browsers and the memory-backed filesystem."""
    from pathlib import Path
    base = Path('/sys/fs/cgroup')
    result = {}
    for key, paths in {
        'current_bytes': ('memory.current', 'memory/memory.usage_in_bytes'),
        'peak_bytes': ('memory.peak', 'memory/memory.max_usage_in_bytes'),
        'limit_bytes': ('memory.max', 'memory/memory.limit_in_bytes'),
    }.items():
        for path in paths:
            try:
                value = int((base / path).read_text().strip())
                if 0 <= value < 2**60:
                    result[key] = value
                    break
            except (OSError, ValueError):
                continue
    return result
