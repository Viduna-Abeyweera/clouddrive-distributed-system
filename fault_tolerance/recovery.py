# =============================================================================
# fault_tolerance/recovery.py
# RecoveryService: handles data resynchronization when a crashed node
# comes back online. Detects node rejoin events via HeartbeatService and
# automatically pulls all missed files from a healthy peer.
# =============================================================================

import threading
import time


class RecoveryService:

    def __init__(self, node, failure_detector, check_interval: float = 3.0):
        """
        Args:
            node:             the local StorageNode
            failure_detector: FailureDetector instance
            check_interval:   seconds between recovery checks
        """
        self.node             = node
        self.failure_detector = failure_detector
        self.check_interval   = check_interval

        # Track which nodes we have already recovered (avoid duplicate syncs)
        self._recovered: set = set()
        # Track which nodes were dead on the last check
        self._previously_dead: set = set()

        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        """Start the background recovery monitor thread."""
        self._thread.start()
        print(f"[RECOVERY] Recovery service started for {self.node.node_id}")


    # Background monitor
    def _run(self):
        """
        Periodically check if any previously-dead node has come back online.
        When detected, trigger a recovery sync.
        """
        while True:
            time.sleep(self.check_interval)
            self._check_for_recoveries()

    def _check_for_recoveries(self):
        """
        Compare current alive-set against previously-dead-set.
        Any node that was dead and is now alive needs recovery.
        """
        dead_now  = {n["id"] for n in self.failure_detector.get_dead_nodes()}
        alive_now = {n["id"] for n in self.failure_detector.get_alive_nodes()}

        # Nodes that just came back: were dead before, alive now
        came_back = self._previously_dead & alive_now

        for node_id in came_back:
            if node_id == self.node.node_id:
                # This node itself is recovering — trigger self-recovery
                self._self_recover()
            elif node_id not in self._recovered:
                # A peer came back — they will self-recover, but log it
                print(f"[RECOVERY] Peer {node_id} detected back online")

        self._previously_dead = dead_now

    def _self_recover(self):
        """
        This node itself has just rejoined the cluster.
        Pull all missed files from the first available healthy peer.
        """
        healthy_peers = self.failure_detector.get_alive_nodes()
        # Exclude self
        remote_peers  = [p for p in healthy_peers
                         if p["id"] != self.node.node_id]

        if not remote_peers:
            print(f"[RECOVERY] {self.node.node_id} has no healthy peers to sync from")
            return

        peer = remote_peers[0]
        recover_node(self.node, peer)

    # Force-trigger recovery (used by API /simulate/recover)
    def trigger_recovery(self, peer: dict = None):
        """
        Manually trigger a recovery sync.
        If peer is None, picks the first available healthy peer.
        """
        if peer is None:
            alive = self.failure_detector.get_alive_nodes()
            peers = [p for p in alive if p["id"] != self.node.node_id]
            if not peers:
                print(f"[RECOVERY] No healthy peers available for {self.node.node_id}")
                return
            peer = peers[0]

        recover_node(self.node, peer)


# =================================
# Module-level recovery function
# ==================================

def recover_node(recovering_node, healthy_peer: dict):

    start_time = time.time()
    print(f"\n[RECOVERY] {recovering_node.node_id} requesting sync from "
          f"{healthy_peer['id']}...")

    response = recovering_node.send_message(
        healthy_peer["host"],
        healthy_peer["port"],
        {"type": "SYNC_REQUEST", "from": recovering_node.node_id}
    )

    if "error" in response:
        print(f"[RECOVERY] Sync failed: {response['error']}")
        return

    received_files = response.get("files", {})
    synced_count   = 0
    skipped_count  = 0

    for filename, file_data in received_files.items():
        existing = recovering_node.get_file(filename)

        if existing is None:
            # We don't have this file at all — store it
            recovering_node.store_file(filename, file_data)
            synced_count += 1

        elif file_data.get("version", 0) > existing.get("version", 0):
            # Peer has a newer version — update ours
            recovering_node.store_file(filename, file_data)
            synced_count += 1

        else:
            # We already have an equal or newer version — skip
            skipped_count += 1

    elapsed = round(time.time() - start_time, 2)
    print(f"[RECOVERY] Sync complete in {elapsed}s — "
          f"synced: {synced_count}, skipped: {skipped_count}, "
          f"total peer files: {len(received_files)}")
    return {
        "synced":  synced_count,
        "skipped": skipped_count,
        "elapsed": elapsed,
    }
