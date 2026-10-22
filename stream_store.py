"""Private bounded event journal with an independent read-only status lane."""
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from events import canonical, fingerprint, packed
from journal import private_file

MAX_EVENTS = 5000
MAX_BYTES = 8 * 1024 * 1024
MAX_EVENT_BYTES = 8192
MAX_STATE_BYTES = 32768
MAX_CONSUMERS = 16


class CapacityError(ValueError):
    pass


class RecoveryRequired(RuntimeError):
    pass


class Store:
    def __init__(self, directory, mode="writer"):
        if mode not in ("writer", "reader", "consumer"):
            raise ValueError("Invalid journal mode")
        self.db = self.lock = None
        self.recovery_required = False
        self.mode = mode
        directory = Path(directory).expanduser()
        if directory.is_symlink():
            raise ValueError("State directory must not be a symlink")
        path = directory.resolve(strict=True)
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("Use an existing user-owned 0700 state directory")
        database = path / "stream.sqlite3"
        self.database = database
        existing = database.exists() and database.stat().st_size > 0
        try:
            if mode == "writer":
                self.lock = private_file(path / "collector.lock")
                try:
                    fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise ValueError("Another collector owns this journal") from error
                os.close(private_file(database))
            else:
                info = database.lstat()  # Readers never create missing state.
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
                    raise ValueError("Unsafe journal file")
            for suffix in ("-journal", "-wal", "-shm"):
                sidecar = Path(str(database) + suffix)
                if sidecar.exists() or sidecar.is_symlink():
                    info = sidecar.lstat()
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
                        raise ValueError("Unsafe journal sidecar")
            previous = os.umask(0o077)
            try:
                self.db = sqlite3.connect(database.as_uri() + ("?mode=ro" if mode == "reader" else "?mode=rw"), uri=True, timeout=1)
                if mode == "writer":
                    if existing and self.db.execute("SELECT value FROM metadata WHERE key='version'").fetchone() != ("1",):
                        raise ValueError("Unsupported collector journal version")
                    self.db.execute("PRAGMA journal_mode=DELETE")
                    self.db.execute("PRAGMA synchronous=FULL")
                    self.db.execute("PRAGMA max_page_count=8192")
                    self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    self.db.execute("INSERT OR IGNORE INTO metadata VALUES ('version','1')")
                    self.db.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, hash TEXT NOT NULL, payload TEXT NOT NULL, bytes INTEGER NOT NULL)")
                    self.db.execute("CREATE TABLE IF NOT EXISTS consumers (name TEXT PRIMARY KEY, cursor INTEGER NOT NULL)")
                    self.db.execute("CREATE TABLE IF NOT EXISTS publications (seq INTEGER PRIMARY KEY REFERENCES events(seq), status TEXT NOT NULL)")
                    self.db.commit()
                try:
                    if self.db.execute("SELECT value FROM metadata WHERE key='version'").fetchone() != ("1",):
                        raise ValueError("Unsupported collector journal version")
                except sqlite3.OperationalError as error:
                    # SQLITE_READONLY_ROLLBACK is the extended code on Python
                    # versions exposing it. Older Python uses the exact message
                    # plus an existing rollback journal as a conservative hint.
                    if self._needs_recovery(error):
                        self.recovery_required = True
                    else:
                        raise
                if mode == "consumer":
                    self.db.execute("PRAGMA synchronous=FULL")
            finally:
                os.umask(previous)
        except BaseException:
            self.close()
            raise

    def _needs_recovery(self, error):
        code = getattr(error, "sqlite_errorcode", None)
        hot = Path(str(self.database) + "-journal").exists()
        return self.mode == "reader" and hot and (code == 776 or (code is None and str(error) == "attempt to write a readonly database"))

    def _rows(self, sql, args=()):
        if self.recovery_required:
            raise RecoveryRequired("Offline journal recovery required")
        try:
            return self.db.execute(sql, args).fetchall()
        except sqlite3.OperationalError as error:
            if self._needs_recovery(error):
                self.recovery_required = True
                raise RecoveryRequired("Offline journal recovery required") from error
            raise

    def state(self):
        rows = self._rows("SELECT value FROM metadata WHERE key='state'")
        row = rows[0] if rows else None
        return json.loads(row[0]) if row else {}

    def bind(self, binding, identity):
        state = self.state()
        if state.get("binding") and (state["binding"] != binding or state["identity"] != identity):
            raise ValueError("Journal scope or producer identity changed; explicit reconciliation required")
        state.update(binding=binding, identity=identity)
        return state

    def commit(self, state, events=()):
        if self.mode != "writer":
            raise ValueError("Only the collector may advance observation")
        inserted = []
        self.db.execute("BEGIN IMMEDIATE")
        try:
            old = self.state()
            count, size = self.db.execute("SELECT count(*), coalesce(sum(bytes),0) FROM events").fetchone()
            for event in events:
                payload = packed(event)
                length = len(payload.encode())
                if length > MAX_EVENT_BYTES:
                    raise CapacityError("Event too large; cursor unchanged")
                existing = self.db.execute("SELECT hash FROM events WHERE id=?", (event["event_id"],)).fetchone()
                signature = canonical(event)
                if existing:
                    if existing[0] != signature:
                        raise ValueError("Event ID collision; cursor unchanged")
                    continue
                if count >= MAX_EVENTS or size + length > MAX_BYTES:
                    raise CapacityError("Journal capacity reached; cursor unchanged; retain deduplication state")
                row = self.db.execute("INSERT INTO events(id,hash,payload,bytes) VALUES (?,?,?,?)", (event["event_id"], signature, payload, length))
                inserted.append({"seq": row.lastrowid, "event": event})
                count += 1
                size += length
                if event["kind"] == "producer_status":
                    state["reported_state"] = event["content"]["reported_state"]
                    state["reported_event_id"] = event["event_id"]
            material = ("collection", "error", "reported_state", "reported_event_id", "publication", "endpoint")
            changed = any(old.get(k) != state.get(k) for k in material)
            state["status_version"] = old.get("status_version", 0) + int(changed or not old)
            raw = packed(state)
            if len(raw.encode()) > MAX_STATE_BYTES:
                raise CapacityError("State too large; cursor unchanged")
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('state',?)", (raw,))
            self.db.commit()
            return inserted
        except BaseException:
            self.db.rollback()
            raise

    def status(self, *, now=None):
        try:
            return self._status(now=now)
        except RecoveryRequired:
            return {"state": {"collection": "recovery_required"}, "producer_observation": "unverified",
                    "last_seq": None, "consumer_cursors": {}, "parent_delivery": "unavailable_unqualified",
                    "send_calls": 0, "presentation_id": fingerprint("sqlite_recovery_required")}

    def _status(self, *, now=None):
        state = self.state()
        now = time.time() if now is None else now
        last = state.get("last_verified")
        age = max(0, now - last) if last is not None else None
        if state.get("collection") == "collecting":
            liveness = "fresh" if age is not None and age <= state["lease_seconds"] else "stale"
        else:
            liveness = "observer_stopped" if last is not None else "unverified"
        maximum = self._rows("SELECT coalesce(max(seq),0) FROM events")[0][0]
        consumers = dict(self._rows("SELECT name,cursor FROM consumers"))
        return {"state": state, "producer_observation": liveness, "verification_age_seconds": age,
                "last_seq": maximum, "parent_delivery": "unavailable_unqualified", "send_calls": 0,
                "consumer_cursors": consumers,
                "presentation_id": fingerprint([state.get("binding"), state.get("status_version", 0), liveness])}

    def replay(self, after=0, limit=100):
        if self.recovery_required:
            raise RecoveryRequired("Offline journal recovery required")
        maximum = self._rows("SELECT coalesce(max(seq),0) FROM events")[0][0]
        if type(after) is not int or not 0 <= after <= maximum or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid replay cursor or limit")
        rows = self._rows("SELECT e.seq,e.payload,coalesce(p.status,'not_attempted') FROM events e LEFT JOIN publications p ON e.seq=p.seq WHERE e.seq>? ORDER BY e.seq LIMIT ?", (after, limit))
        return [{"seq": seq, "event": json.loads(payload), "publication": publication} for seq, payload, publication in rows]

    def publication(self, seq, outcome):
        if self.mode != "writer" or outcome not in ("uncertain", "pipe_written_no_parent_ack"):
            raise ValueError("Invalid publication update")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            if not self.db.execute("SELECT 1 FROM events WHERE seq=?", (seq,)).fetchone():
                raise ValueError("Unknown event")
            if outcome == "uncertain":
                # Intent before pipe I/O; a crash can never look like no attempt.
                self.db.execute("INSERT INTO publications VALUES (?,?)", (seq, outcome))
            elif self.db.execute("UPDATE publications SET status=? WHERE seq=? AND status='uncertain'", (outcome, seq)).rowcount != 1:
                raise ValueError("No pending publication")

    @classmethod
    def recover(cls, directory):
        path = Path(directory).expanduser() / "stream.sqlite3"
        if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
            raise ValueError("Recovery requires an existing journal")
        with cls(directory) as store:
            state = store.state()
            if not state:
                raise ValueError("Recovery requires existing collector state")
            state.update(collection="recovered_observer_stopped", endpoint=None)
            store.commit(state)
            return store.status()

    def acknowledge(self, consumer, cursor):
        if self.mode != "consumer":
            raise ValueError("Use the separate consumer connection")
        if not isinstance(consumer, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", consumer):
            raise ValueError("Invalid consumer name")
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            maximum = self.db.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0]
            if type(cursor) is not int or not 0 <= cursor <= maximum:
                raise ValueError("Invalid consumer cursor")
            old = self.db.execute("SELECT cursor FROM consumers WHERE name=?", (consumer,)).fetchone()
            if old and cursor < old[0]:
                raise ValueError("Consumer cursor cannot move backwards")
            if not old and self.db.execute("SELECT count(*) FROM consumers").fetchone()[0] >= MAX_CONSUMERS:
                raise CapacityError("Consumer capacity reached")
            self.db.execute("INSERT OR REPLACE INTO consumers VALUES (?,?)", (consumer, cursor))
        return {"consumer": consumer, "cursor": cursor, "parent_presentation_proven": False}

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
