# =============================================================================
# fault_tolerance/heartbeat.py
# HeartbeatService: pings every peer every 2 seconds.
# Tracks last-seen timestamps and marks nodes as dead after 6s of silence.
# =============================================================================

import threading
import time

from node.config import HEARTBEAT_INTERVAL_SEC, HEARTBEAT_TIMEOUT_SEC


class HeartbeatService:
    """
    Sends periodic heartbeat pings to all peer nodes.
    Tracks which nodes are alive and which are dead.

    Design decision: 2-second interval with 6-second timeout (3 missed pings).
    Trade-off: faster detection (e.g. 0.5s) means more network traffic;
    slower detection (e.g. 10s) means longer unavailability window.
    2s / 6s is a good balance for a LAN prototype.
    """

    def __init__(self, node,
                 interval: float = HEARTBEAT_INTERVAL_SEC,
                 timeout:  float = HEARTBEAT_TIMEOUT_SEC):
        """
        Args:
            node:     the StorageNode this service belongs to
            interval: seconds between heartbeat pings
            timeout:  seconds of silence before declaring a peer dead
        """
        self.node     = node
        self.interval = interval
        self.timeout  = timeout

        # Per-peer state — keyed by peer node_id
        self.last_seen:    dict[str, float] = {}   # node_id → unix timestamp
        self.alive:        dict[str, bool]  = {}   # node_id → True/False
        self.failure_times: dict[str, float] = {}  # node_id → when it went down
        self.recovery_times: dict[str, float] = {} # node_id → when it came back

        # Initialise all peers as "unknown" (not yet pinged)
        for peer in self.node.peers:
            pid = peer["id"]
            self.last_seen[pid]     = time.time()  # assume alive at start
            self.alive[pid]         = True

        self._lock   = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        """Start the background heartbeat thread."""
        self._thread.start()
        print(f"[HEARTBEAT] Service started for {self.node.node_id} "
              f"(interval={self.interval}s, timeout={self.timeout}s)")


    # Background loop
    def _run(self):
        """Ping all peers in a loop, forever."""
        while True:
            for peer in self.node.peers:
                self._ping_peer(peer)
            time.sleep(self.interval)

    def _ping_peer(self, peer: dict):
        """
        Send a heartbeat message to one peer.
        Update last_seen on success; mark as dead on timeout.
        """
        pid  = peer["id"]
        host = peer["host"]
        port = peer["port"]

        response = self.node.send_message(host, port, {
            "type": "heartbeat",
            "from": self.node.node_id,
        })

        with self._lock:
            if "error" not in response:
                # Peer responded — it's alive
                was_dead = not self.alive.get(pid, True)
                self.last_seen[pid] = time.time()
                self.alive[pid]     = True

                if was_dead:
                    # Node just came back online
                    self.recovery_times[pid] = time.time()
                    elapsed = self.recovery_times[pid] - self.failure_times.get(pid, 0)
                    print(f"[HEARTBEAT] {pid} is back ONLINE "
                          f"(was down for {elapsed:.1f}s)")
            else:
                # No response — check if timeout exceeded
                elapsed = time.time() - self.last_seen.get(pid, time.time())

                if elapsed > self.timeout and self.alive.get(pid, True):
                    # Just went down
                    self.alive[pid]           = False
                    self.failure_times[pid]   = time.time()
                    print(f"[HEARTBEAT] *** {pid} is DOWN "
                          f"(no response for {elapsed:.1f}s) ***")


    # Public accessors
    def is_alive(self, node_id: str) -> bool:
        """Return True if the given peer is currently considered alive."""
        with self._lock:
            return self.alive.get(node_id, False)

    def get_alive_peers(self) -> list:
        """Return list of peer config dicts for all currently alive peers."""
        with self._lock:
            return [p for p in self.node.peers if self.alive.get(p["id"], False)]

    def get_dead_peers(self) -> list:
        """Return list of peer config dicts for all currently dead peers."""
        with self._lock:
            return [p for p in self.node.peers if not self.alive.get(p["id"], True)]

    def get_status_table(self) -> dict:
        """Return a summary dict for the /status API endpoint and reports."""
        with self._lock:
            return {
                pid: {
                    "alive":        self.alive.get(pid, False),
                    "last_seen_ago": round(time.time() - self.last_seen.get(pid, 0), 2),
                }
                for pid in [p["id"] for p in self.node.peers]
            }
