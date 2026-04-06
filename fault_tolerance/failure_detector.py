# =============================================================================
# fault_tolerance/failure_detector.py
# FailureDetector: clean API over HeartbeatService.
# to know which nodes are currently reachable.
# =============================================================================

from node.config import NODES


class FailureDetector:
    """
    Provides a clean, module-independent interface for querying node liveness.
    Wraps HeartbeatService so other members don't need to know its internals.

    Usage (from ReplicationManager, Member 2):
        alive = failure_detector.get_alive_nodes()
        for node in alive:
            # replicate to this node
    """

    def __init__(self, heartbeat_service):
        """
        Args:
            heartbeat_service: a running HeartbeatService instance.
        """
        self.heartbeat = heartbeat_service

    # Core queries

    def get_alive_nodes(self) -> list:
        """
        Return a list of peer config dicts for all nodes that are alive.
        Includes the local node (always considered alive).

        Returns list of dicts: [{"id": ..., "host": ..., "port": ...}, ...]
        """
        alive_peers = self.heartbeat.get_alive_peers()

        # Include the local node itself
        local = {
            "id":   self.heartbeat.node.node_id,
            "host": self.heartbeat.node.host,
            "port": self.heartbeat.node.port,
        }

        # Avoid duplicates
        ids = {p["id"] for p in alive_peers}
        if local["id"] not in ids:
            alive_peers = [local] + alive_peers

        return alive_peers

    def get_dead_nodes(self) -> list:
        """Return a list of peer config dicts for all nodes that are dead."""
        return self.heartbeat.get_dead_peers()

    def is_node_alive(self, node_id: str) -> bool:
        """
        Check if a specific node is alive.
        The local node is always considered alive.
        """
        if node_id == self.heartbeat.node.node_id:
            return True
        return self.heartbeat.is_alive(node_id)

    def get_alive_count(self) -> int:
        """Return the total number of currently alive nodes (including self)."""
        return len(self.get_alive_nodes())

    def has_quorum(self) -> bool:
        """
        Returns True if a majority of nodes are alive.
        Quorum = floor(N/2) + 1 where N = total nodes.
        For 4 nodes: quorum = 3.
        Used by Raft to decide if operations can proceed.
        """
        total  = len(NODES)
        quorum = (total // 2) + 1
        return self.get_alive_count() >= quorum


    # Reporting
    def get_full_status(self) -> dict:
        """
        Return a complete status dict for the /status endpoint and reports.
        """
        return {
            "alive_nodes": [n["id"] for n in self.get_alive_nodes()],
            "dead_nodes":  [n["id"] for n in self.get_dead_nodes()],
            "alive_count": self.get_alive_count(),
            "has_quorum":  self.has_quorum(),
            "peer_details": self.heartbeat.get_status_table(),
        }

    def print_status(self):
        """Print a formatted liveness table — useful during demos."""
        status = self.get_full_status()
        print("\n" + "=" * 45)
        print("  Cluster Node Status")
        print("=" * 45)
        print(f"  {'Node':<12} {'Status':<10} {'Last Seen'}")
        print(f"  {'-'*12} {'-'*10} {'-'*15}")
        for node_id, detail in status["peer_details"].items():
            s = "ALIVE" if detail["alive"] else "DEAD"
            print(f"  {node_id:<12} {s:<10} {detail['last_seen_ago']:.1f}s ago")
        print(f"\n  Quorum: {'YES' if status['has_quorum'] else 'NO'} "
              f"({status['alive_count']}/{len(NODES)} alive)")
        print("=" * 45 + "\n")
