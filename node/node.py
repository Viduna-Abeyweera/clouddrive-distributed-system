# =============================================================================
# node/node.py
# CloudDrive — Distributed File Storage System
# Base StorageNode class used by ALL 4 members.
# Handles: socket server, message routing, file storage, Lamport clock integration.
# =============================================================================

import socket
import threading
import json
import time
import os

from node.config import NODES, get_peers


class StorageNode:
    """
    Base class representing one node in the CloudDrive cluster.
    Each node:
      - Stores files in memory (dict) and on disk (data/ folder)
      - Listens for incoming TCP messages from peers
      - Sends messages to peers via TCP
      - Maintains a Lamport clock (injected by Member 3)
      - Routes incoming messages to the correct handler module
    """

    def __init__(self, node_id: str, host: str, port: int):
        self.node_id  = node_id
        self.host     = host
        self.port     = port
        self.peers    = get_peers(node_id)   # list of peer config dicts

        # In-memory file store: filename -> {"content": ..., "timestamp": ..., "version": ...}
        self.files    = {}
        self.files_lock = threading.Lock()

        # Node liveness flag — set to False to simulate a crash in tests
        self.alive    = True

        # Disk storage directory
        self.data_dir = os.path.join("data", node_id)
        os.makedirs(self.data_dir, exist_ok=True)

        # ---------------------------------------------------------------------------
        # Lamport clock — injected after construction by run.py
        # Member 3 owns this object; we just hold a reference here so that
        # every send_message() and every received message can update it.
        # ---------------------------------------------------------------------------
        self.lamport_clock = None   # set by run.py: node.lamport_clock = LamportClock()

        # ---------------------------------------------------------------------------
        # Component references — set by run.py after construction
        # ---------------------------------------------------------------------------
        self.heartbeat_service  = None   # Member 1
        self.failure_detector   = None   # Member 1
        self.replication_mgr    = None   # Member 2
        self.raft_node          = None   # Member 4

        # Message handler registry — each module registers its own handler
        # key: message type string  value: callable(node, message) -> dict
        self._handlers = {}

        # Start the TCP listener in a background thread
        self._server_thread = threading.Thread(target=self._listen, daemon=True)
        self._server_thread.start()

        print(f"[NODE] {self.node_id} started on {self.host}:{self.port}")

    # -------------------------------------------------------------------------
    # File storage API
    # -------------------------------------------------------------------------

    def store_file(self, filename: str, file_data: dict):
        """
        Store a file in memory and persist to disk.
        file_data format: {"content": str, "timestamp": int, "version": int}
        """
        with self.files_lock:
            self.files[filename] = file_data

        # Persist to disk so data survives a process restart
        filepath = os.path.join(self.data_dir, filename + ".json")
        with open(filepath, "w") as f:
            json.dump(file_data, f)

        print(f"[STORAGE] {self.node_id} stored '{filename}' "
              f"(v{file_data.get('version', 1)}, ts={file_data.get('timestamp', 0)})")

    def get_file(self, filename: str) -> dict:
        """Return file data dict or None if not found."""
        with self.files_lock:
            return self.files.get(filename, None)

    def get_all_files(self) -> dict:
        """Return a copy of the entire file store (used by recovery sync)."""
        with self.files_lock:
            return dict(self.files)

    def load_from_disk(self):
        """
        On startup, reload any files previously persisted to disk.
        Called by run.py before joining the cluster.
        """
        if not os.path.exists(self.data_dir):
            return
        for fname in os.listdir(self.data_dir):
            if fname.endswith(".json"):
                filepath = os.path.join(self.data_dir, fname)
                try:
                    with open(filepath, "r") as f:
                        data = json.load(f)
                    original_name = fname[:-5]   # strip .json
                    with self.files_lock:
                        self.files[original_name] = data
                except Exception as e:
                    print(f"[STORAGE] Failed to load {fname}: {e}")
        print(f"[STORAGE] {self.node_id} loaded {len(self.files)} files from disk")

    # -------------------------------------------------------------------------
    # Networking — send
    # -------------------------------------------------------------------------

    def send_message(self, host: str, port: int, message: dict) -> dict:
        """
        Send a JSON message to a peer over TCP and return the response.
        Automatically attaches this node's current Lamport timestamp.
        If this node is simulating a crash, drop all outgoing messages.
        """
        # If simulating a crash, do not send anything outward
        if not self.alive:
            return {"error": "node is simulating a crash"}
        # Attach Lamport timestamp (Member 3)
        if self.lamport_clock:
            message["lamport_ts"] = self.lamport_clock.tick()

        message["from_node"] = self.node_id

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)                        # 3-second connection timeout
            s.connect((host, port))
            s.send(json.dumps(message).encode("utf-8"))

            # Signal end of data so the server stops waiting for more
            s.shutdown(socket.SHUT_WR)

            # Read response — may arrive in chunks
            chunks = []
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            s.close()

            raw = b"".join(chunks).decode("utf-8")
            return json.loads(raw) if raw else {"status": "ok"}

        except (ConnectionRefusedError, socket.timeout, OSError):
            return {"error": f"cannot reach {host}:{port}"}
        except json.JSONDecodeError as e:
            return {"error": f"bad JSON response: {e}"}

    # -------------------------------------------------------------------------
    # Networking — receive (TCP server)
    # -------------------------------------------------------------------------

    def _listen(self):
        """
        Background thread: accepts incoming TCP connections and dispatches
        each message to the appropriate registered handler.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(10)

        while True:
            try:
                conn, addr = server.accept()
                # Handle each connection in its own thread so we never block
                t = threading.Thread(
                    target=self._handle_connection,
                    args=(conn,),
                    daemon=True
                )
                t.start()
            except Exception as e:
                print(f"[NODE] {self.node_id} listener error: {e}")

    def _handle_connection(self, conn):
        """
        Read a complete JSON message from a connection, update the Lamport
        clock, dispatch to the registered handler, and send back the response.
        """
        try:
            chunks = []
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)

            raw = b"".join(chunks).decode("utf-8")
            if not raw:
                conn.close()
                return

            message = json.loads(raw)

            # Update Lamport clock on every receive (Member 3)
            if self.lamport_clock and "lamport_ts" in message:
                self.lamport_clock.update(message["lamport_ts"])

            # Dispatch to the correct handler
            response = self._dispatch(message)

            conn.send(json.dumps(response).encode("utf-8"))
            conn.shutdown(socket.SHUT_WR)   # signal end of response

        except Exception as e:
            try:
                conn.send(json.dumps({"error": str(e)}).encode("utf-8"))
            except Exception:
                pass
        finally:
            conn.close()

    def _dispatch(self, message: dict) -> dict:
        """
        Route an incoming message to the correct registered handler.
        Falls back to a default handler for unknown message types.
        """
        # ── NEW: If simulating a crash, reject ALL incoming messages ──
        # Without this, a "dead" node still accepts REPLICATE messages
        # from peers who haven't detected the failure yet, causing the
        # node to receive files it should have missed.
        if not self.alive:
            return {"error": "node is simulating a crash"}

        msg_type = message.get("type", "unknown")

        if msg_type in self._handlers:
            try:
                return self._handlers[msg_type](self, message)
            except Exception as e:
                return {"error": f"handler error for {msg_type}: {e}"}

        # Built-in handlers for core node operations
        if msg_type == "heartbeat":
            return self._handle_heartbeat(message)
        if msg_type == "REPLICATE":
            return self._handle_replicate(message)
        if msg_type == "SYNC_REQUEST":
            return self._handle_sync_request(message)
        if msg_type == "READ_FILE":
            return self._handle_read_file(message)
        if msg_type == "GET_VERSION":
            return self._handle_get_version(message)

        return {"error": f"unknown message type: {msg_type}"}

    def register_handler(self, msg_type: str, handler_fn):
        """
        Register a custom message handler function.
        handler_fn signature: fn(node: StorageNode, message: dict) -> dict
        Used by Raft (Member 4) to register VOTE_REQUEST and APPEND_ENTRIES handlers.
        """
        self._handlers[msg_type] = handler_fn
        print(f"[NODE] {self.node_id} registered handler for '{msg_type}'")

    # -------------------------------------------------------------------------
    # Built-in message handlers
    # -------------------------------------------------------------------------

    def _handle_heartbeat(self, message: dict) -> dict:
        """Respond to a heartbeat ping."""
        return {"status": "alive", "node_id": self.node_id,
                "lamport_ts": self.lamport_clock.get_time() if self.lamport_clock else 0}

    def _handle_replicate(self, message: dict) -> dict:
        """
        Receive a file replication request from the primary node.
        Stores the file locally without triggering further replication.
        """
        filename  = message.get("filename")
        file_data = message.get("file_data")
        if filename and file_data:
            self.store_file(filename, file_data)
            return {"status": "ok", "stored": filename}
        return {"error": "missing filename or file_data"}

    def _handle_sync_request(self, message: dict) -> dict:
        """
        A recovering node is requesting all files to resync.
        Return the entire file store.
        """
        print(f"[SYNC] {self.node_id} sending all files to recovering node "
              f"{message.get('from_node')}")
        return {"status": "ok", "files": self.get_all_files()}

    def _handle_read_file(self, message: dict) -> dict:
        """Return a specific file's data (used by read repair)."""
        filename = message.get("filename")
        file_data = self.get_file(filename)
        if file_data:
            return {"status": "ok", "filename": filename, "file_data": file_data}
        return {"status": "not_found", "filename": filename}

    def _handle_get_version(self, message: dict) -> dict:
        """Return version + timestamp of a file (used by consistency check)."""
        filename  = message.get("filename")
        file_data = self.get_file(filename)
        if file_data:
            return {
                "status":    "ok",
                "filename":  filename,
                "version":   file_data.get("version", 0),
                "timestamp": file_data.get("timestamp", 0),
                "node_id":   self.node_id
            }
        return {"status": "not_found", "filename": filename, "version": 0, "timestamp": 0}

    # -------------------------------------------------------------------------
    # Status / debug
    # -------------------------------------------------------------------------

    def get_status(self) -> dict:
        """Return a summary of this node's current state."""
        return {
            "node_id":     self.node_id,
            "host":        self.host,
            "port":        self.port,
            "alive":       self.alive,
            "file_count":  len(self.files),
            "files":       list(self.files.keys()),
            "lamport_ts":  self.lamport_clock.get_time() if self.lamport_clock else 0,
            "leader":      self.raft_node.leader_id if self.raft_node else None,
            "role":        self.raft_node.role.value if self.raft_node else "unknown",
        }

    def __repr__(self):
        return f"StorageNode({self.node_id}, {self.host}:{self.port})"
