"""Opt-in private operation journal. No networking, credentials or background work."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat

MAX_OPERATIONS = 128
MAX_EVENTS = 5000
MAX_VALUE = 262144


def encode(value):
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if len(text.encode()) > MAX_VALUE:
        raise ValueError("Journal value exceeds its bounded size; no automatic retry")
    return text


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def private_file(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
        os.close(fd)
        raise ValueError("Journal files must be private, singly linked regular files owned by this user")
    return fd


class Journal:
    """One exclusive workflow owner; FULL synchronous SQLite transactions.

    Intents are committed before a send can begin. They are never erased or
    automatically retried, including when the writer dies before dispatch.
    Quotas fail explicitly instead of dropping replay or deduplication state.
    """

    def __init__(self, directory):
        self.db = None
        self.lock = None
        path = Path(directory).expanduser().resolve(strict=True)
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("Use an existing, user-owned 0700 state directory")
        try:
            self.lock = private_file(path / "owner.lock")
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ValueError("Another workflow owns this journal; do not assign a second sender") from exc
            fd = private_file(path / "operations.sqlite3")
            os.close(fd)
            previous = os.umask(0o077)
            try:
                self.db = sqlite3.connect(path / "operations.sqlite3")
                self.db.execute("PRAGMA journal_mode=DELETE")
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                row = self.db.execute("SELECT value FROM metadata WHERE key='version'").fetchone()
                if row and row[0] != "1":
                    raise ValueError("Unsupported journal version")
                self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('version', '1')")
                self.db.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, binding TEXT NOT NULL, state TEXT NOT NULL)")
                self.db.execute("CREATE TABLE IF NOT EXISTS events (ordinal INTEGER PRIMARY KEY, operation TEXT NOT NULL, key TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(operation,key))")
                self.db.commit()
            finally:
                os.umask(previous)
        except BaseException:
            self.close()
            raise

    def get(self, request_id):
        row = self.db.execute("SELECT binding,state FROM operations WHERE id=?", (request_id,)).fetchone()
        return {"binding": json.loads(row[0]), "state": json.loads(row[1])} if row else None

    def begin(self, request_id, binding, state):
        if self.get(request_id):
            raise ValueError("Operation already exists; observe it, never resend")
        if self.db.execute("SELECT count(*) FROM operations").fetchone()[0] >= MAX_OPERATIONS:
            raise ValueError("Journal operation capacity reached; retain it for deduplication")
        with self.db:
            self.db.execute("INSERT INTO operations VALUES (?,?,?)", (request_id, encode(binding), encode(state)))

    def checkpoint(self, request_id, state, events=()):
        # Evidence and the cursor that advances beyond it commit atomically.
        if not self.get(request_id):
            raise ValueError("Unknown operation")
        with self.db:
            count = self.db.execute("SELECT count(*) FROM events").fetchone()[0]
            for event in events:
                payload = encode(event)
                key = hashlib.sha256(payload.encode()).hexdigest()
                if self.db.execute("SELECT 1 FROM events WHERE operation=? AND key=?", (request_id, key)).fetchone():
                    continue
                if count >= MAX_EVENTS:
                    raise ValueError("Journal event capacity reached; cursor was not advanced")
                self.db.execute("INSERT INTO events(operation,key,payload) VALUES (?,?,?)", (request_id, key, payload))
                count += 1
            self.db.execute("UPDATE operations SET state=? WHERE id=?", (encode(state), request_id))

    def replay(self, request_id, after=0, limit=100):
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid journal replay cursor/limit")
        rows = self.db.execute("SELECT ordinal,payload FROM events WHERE operation=? AND ordinal>? ORDER BY ordinal LIMIT ?", (request_id, after, limit)).fetchall()
        return [{"ordinal": ordinal, "event": json.loads(payload)} for ordinal, payload in rows]

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.lock is not None:
            os.close(self.lock)
            self.lock = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
