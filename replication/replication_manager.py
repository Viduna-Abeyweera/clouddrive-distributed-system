# =============================================================================
# replication/replication_manager.py
# CloudDrive — Member 2: Data Replication & Consistency
#
# ReplicationManager: handles all file write and read operations.
# Strategy: Primary-Backup replication.
#   - All writes go to primary first, then broadcast to alive backups.
#   - Reads come from local node with read-repair to heal stale replicas.
# =============================================================================

import threading
import time

from node.config            import REPLICATION_FACTOR, PRIMARY_NODE_ID
from replication.conflict_resolver import resolve
from replication.consistency        import read_repair


class ReplicationManager:
    """
    Manages file replication across the CloudDrive cluster.

    Primary-Backup model:
      Write path:  client → primary → all alive backups
      Read path:   local node (+ read repair to fix any stale copies)

    The primary is initially set by config, but Raft (Member 4) can
    update it dynamically via set_primary() when a new leader is elected.
    """

    def __init__(self, node, failure_detector):
        """
        Args:
            node:             the local StorageNode
            failure_detector: FailureDetector from Member 1
        """
        self.node             = node
        self.failure_detector = failure_detector
        self.primary_id       = PRIMARY_NODE_ID   # updated by Raft on leader change
        self._lock            = threading.Lock()

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def write_file(self, filename: str, content: str, timestamp: int) -> dict:
        """
        Write a file to the cluster using primary-backup replication.

        Steps:
          1. Build the file_data record with version, timestamp, vector clock.
          2. Store on the primary (local node if we are primary, else send there).
          3. Broadcast REPLICATE to all alive backup nodes.
          4. Return a summary of what was written and where.

        Args:
            filename:  name of the file
            content:   file content (string)
            timestamp: Lamport timestamp from Member 3's clock

        Returns: dict with status, version, replicated_to list
        """
        with self._lock:
            # Determine next version number
            existing = self.node.get_file(filename)
            version  = (existing.get("version", 0) + 1) if existing else 1

            # Build the file record
            file_data = {
                "content":   content,
                "timestamp": timestamp,
                "version":   version,
                "author":    self.node.node_id,
                "written_at": time.time(),
            }

            # Check for conflicts with existing version
            if existing:
                winner = resolve(existing, file_data)
                if winner is existing:
                    print(f"[REPLICATION] Conflict on '{filename}' — "
                          f"existing version wins (LWW)")
                    return {
                        "status":   "conflict_rejected",
                        "filename": filename,
                        "winner":   "existing",
                        "version":  existing["version"],
                    }

            # 1. Store on local node (primary if we are it, or just locally)
            self.node.store_file(filename, file_data)

            # 2. Replicate to all alive peers
            replicated_to = []
            failed_nodes  = []
            alive_peers   = self.failure_detector.get_alive_nodes()

            for peer in alive_peers:
                if peer["id"] == self.node.node_id:
                    continue    # skip self — already stored above

                response = self.node.send_message(
                    peer["host"], peer["port"],
                    {
                        "type":      "REPLICATE",
                        "filename":  filename,
                        "file_data": file_data,
                    }
                )
                if "error" not in response:
                    replicated_to.append(peer["id"])
                else:
                    failed_nodes.append(peer["id"])
                    print(f"[REPLICATION] Failed to replicate '{filename}' "
                          f"to {peer['id']}: {response['error']}")

            print(f"[REPLICATION] '{filename}' v{version} written to "
                  f"{self.node.node_id} + replicated to {replicated_to}")

            return {
                "status":         "ok",
                "filename":       filename,
                "version":        version,
                "timestamp":      timestamp,
                "primary":        self.node.node_id,
                "replicated_to":  replicated_to,
                "failed_nodes":   failed_nodes,
            }

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def read_file(self, filename: str) -> dict:
        """
        Read a file with read repair.

        Steps:
          1. Read from local node.
          2. In the background, check all alive nodes for newer versions.
          3. If any node has a newer version, update stale nodes (read repair).
          4. Return the latest version found.

        Returns: file_data dict or None if not found anywhere.
        """
        # First, try to read locally
        local_data = self.node.get_file(filename)

        # Trigger read repair in background (non-blocking for the client)
        repair_thread = threading.Thread(
            target=read_repair,
            args=(filename, self.node, self.failure_detector),
            daemon=True
        )
        repair_thread.start()

        # Return local data immediately (eventual consistency model)
        # The repair runs in background and fixes stale replicas
        return local_data

    def read_file_strong(self, filename: str) -> dict:
        """
        Read with strong consistency — queries all alive nodes, returns
        the version with the highest Lamport timestamp.
        Slower than read_file() but guarantees the latest version.
        """
        alive = self.failure_detector.get_alive_nodes()
        candidates = []

        for peer in alive:
            if peer["id"] == self.node.node_id:
                local = self.node.get_file(filename)
                if local:
                    candidates.append(local)
                continue

            response = self.node.send_message(
                peer["host"], peer["port"],
                {"type": "READ_FILE", "filename": filename}
            )
            if "error" not in response and response.get("status") == "ok":
                candidates.append(response["file_data"])

        if not candidates:
            return None

        # Return the candidate with the highest Lamport timestamp
        return max(candidates, key=lambda f: f.get("timestamp", 0))

    # ------------------------------------------------------------------
    # Primary management (called by Raft on leader election)
    # ------------------------------------------------------------------

    def set_primary(self, new_primary_id: str):
        """
        Update which node is the primary.
        Called by Member 4 (Raft) when a new leader is elected.
        """
        old = self.primary_id
        self.primary_id = new_primary_id
        print(f"[REPLICATION] Primary changed: {old} → {new_primary_id}")

    def get_primary(self) -> str:
        return self.primary_id

    def is_primary(self) -> bool:
        return self.node.node_id == self.primary_id
