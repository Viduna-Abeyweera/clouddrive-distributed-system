# =============================================================================
# consensus/log_manager.py
# CloudDrive — Member 4: Consensus & Agreement
#
# LogManager: maintains the Raft append-only operation log.
# Every file write in CloudDrive is first committed to this log
# before being applied to storage.
# =============================================================================

import threading
import time


class LogEntry:
    """A single entry in the Raft log."""
    def __init__(self, term: int, index: int, command: dict):
        self.term    = term      # the election term when this was created
        self.index   = index     # position in the log (1-indexed)
        self.command = command   # the operation: {"type": "WRITE", "filename": ..., ...}
        self.committed = False

    def to_dict(self) -> dict:
        return {
            "term":      self.term,
            "index":     self.index,
            "command":   self.command,
            "committed": self.committed,
        }


class LogManager:
    """
    Manages the Raft operation log for one node.

    The log is an ordered list of LogEntry objects.
    The leader appends new entries and replicates them to followers.
    An entry is "committed" once a majority of nodes have it.

    For CloudDrive, commands in the log represent file operations:
        {"type": "WRITE", "filename": "report.pdf", "version": 3, ...}
    """

    def __init__(self, node):
        self.node        = node
        self.log         = []           # list of LogEntry
        self.commit_index = 0           # highest committed index
        self.last_applied = 0           # highest applied to storage
        self._lock        = threading.Lock()

        # Track how many followers have acknowledged each index
        # index → set of node_ids that have acknowledged
        self._ack_counts  = {}

    # ------------------------------------------------------------------
    # Leader operations
    # ------------------------------------------------------------------

    def append(self, command: dict) -> LogEntry:
        """
        Append a new command to the leader's log.
        Called by the API when a write comes in.

        Returns the new LogEntry (with index and term).
        """
        with self._lock:
            raft  = self.node.raft_node
            term  = raft.current_term if raft else 0
            index = len(self.log) + 1

            entry = LogEntry(term=term, index=index, command=command)
            self.log.append(entry)
            self._ack_counts[index] = {self.node.node_id}   # leader ACKs itself

        print(f"[LOG] {self.node.node_id} appended entry #{index}: {command.get('type')}")
        return entry

    def replicate_to_followers(self, entry: LogEntry):
        """
        Send APPEND_ENTRIES to all alive peers with the new log entry.
        Called after append() to distribute the entry.

        IMPORTANT: Only the leader should call this. If a follower sends
        APPEND_ENTRIES, the real leader will see it as a valid leader
        message (same term) and step down — causing a split-brain bug.
        """
        # Safety check: only the leader should replicate log entries
        raft = self.node.raft_node
        if raft:
            from consensus.raft import Role
            if raft.role != Role.LEADER:
                print(f"[LOG] {self.node.node_id} is not leader — skipping log replication")
                return

        alive = self.node.failure_detector.get_alive_nodes() if self.node.failure_detector else []

        for peer in alive:
            if peer["id"] == self.node.node_id:
                continue

            threading.Thread(
                target=self._send_append_entries,
                args=(peer, entry),
                daemon=True
            ).start()

    def _send_append_entries(self, peer: dict, entry: LogEntry):
        """Send one AppendEntries RPC to a single peer."""
        raft = self.node.raft_node

        # Double-check we're still the leader before sending.
        # Prevents race condition where node loses leadership between
        # the check in replicate_to_followers() and this point.
        if raft:
            from consensus.raft import Role
            if raft.role != Role.LEADER:
                return

        response = self.node.send_message(
            peer["host"], peer["port"],
            {
                "type":      "APPEND_ENTRIES",
                "term":      raft.current_term if raft else 0,
                "leader_id": self.node.node_id,
                "entries":   [entry.to_dict()],
                "commit_index": self.commit_index,
            }
        )

        if "error" not in response and response.get("success"):
            self._record_ack(entry.index, peer["id"])

    def _record_ack(self, index: int, node_id: str):
        """
        Record that node_id has acknowledged log index.
        Commit the entry if a majority have acknowledged.
        """
        with self._lock:
            if index not in self._ack_counts:
                self._ack_counts[index] = set()
            self._ack_counts[index].add(node_id)

            total    = len(self.node.peers) + 1
            majority = (total // 2) + 1
            acks     = len(self._ack_counts[index])

            if acks >= majority and index > self.commit_index:
                self.commit_index = index
                self._apply_committed(index)

    def _apply_committed(self, index: int):
        """Apply a committed log entry to actual storage."""
        if index < 1 or index > len(self.log):
            return

        entry   = self.log[index - 1]
        command = entry.command

        if command.get("type") == "WRITE":
            filename  = command.get("filename")
            file_data = command.get("file_data")
            if filename and file_data:
                self.node.store_file(filename, file_data)

        entry.committed = True
        self.last_applied = index
        print(f"[LOG] Entry #{index} committed and applied")

    # ------------------------------------------------------------------
    # Follower operations
    # ------------------------------------------------------------------

    def handle_append_entries(self, message: dict):
        """
        Process an AppendEntries RPC received from the leader.
        Appends new entries to the local log and updates commit index.
        """
        entries = message.get("entries", [])
        leader_commit = message.get("commit_index", 0)

        with self._lock:
            for entry_dict in entries:
                index = entry_dict.get("index", 0)
                # Only append if this is a new entry
                if index > len(self.log):
                    entry = LogEntry(
                        term    = entry_dict["term"],
                        index   = index,
                        command = entry_dict["command"]
                    )
                    self.log.append(entry)
                    print(f"[LOG] {self.node.node_id} appended entry "
                          f"#{index} from leader")

            # Advance commit index
            if leader_commit > self.commit_index:
                new_commit = min(leader_commit, len(self.log))
                for i in range(self.commit_index + 1, new_commit + 1):
                    self._apply_committed(i)
                self.commit_index = new_commit

    def handle_append_response(self, message: dict):
        """Leader handles ACK from follower for AppendEntries."""
        if message.get("success"):
            node_id   = message.get("node_id")
            log_index = message.get("log_index", 0)
            if node_id and log_index:
                self._record_ack(log_index, node_id)

    # ------------------------------------------------------------------
    # Queries (used by leader election for log up-to-date check)
    # ------------------------------------------------------------------

    def get_last_index(self) -> int:
        """Return the index of the last log entry (0 if empty)."""
        with self._lock:
            return len(self.log)

    def get_last_term(self) -> int:
        """Return the term of the last log entry (0 if empty)."""
        with self._lock:
            if not self.log:
                return 0
            return self.log[-1].term

    def get_log_summary(self) -> list:
        """Return a list of log entry summaries for the /status endpoint."""
        with self._lock:
            return [
                {
                    "index":     e.index,
                    "term":      e.term,
                    "command":   e.command.get("type", "?"),
                    "committed": e.committed,
                }
                for e in self.log
            ]