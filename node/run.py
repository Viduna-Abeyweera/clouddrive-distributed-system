# =============================================================================
# node/run.py
# CloudDrive — Distributed File Storage System
# Entry point: starts a single storage node with all 4 components wired together.
#
# Usage:
#   python -m node.run --id node_A --port 5001
#   python -m node.run --id node_B --port 5002
#   python -m node.run --id node_C --port 5003
#   python -m node.run --id node_D --port 5004
#
# Open 4 separate terminal windows and run one command in each.
# =============================================================================

import argparse
import time
import sys

from node.node   import StorageNode
from node.config import get_api_port, NODES

from time_sync.lamport_clock        import LamportClock           # Member 3
from fault_tolerance.heartbeat      import HeartbeatService       # Member 1
from fault_tolerance.failure_detector import FailureDetector      # Member 1
from fault_tolerance.recovery       import RecoveryService        # Member 1
from replication.replication_manager import ReplicationManager    # Member 2
from consensus.raft                 import RaftNode               # Member 4
from consensus.leader_election      import LeaderElection         # Member 4
from consensus.log_manager          import LogManager             # Member 4
from api.server                     import create_app             # Shared


def start_node(node_id: str, port: int):
    """
    Boot sequence for a single CloudDrive storage node.
    Wires all 4 components together in the correct dependency order.
    """

    print(f"\n{'='*60}")
    print(f"  CloudDrive — Starting {node_id} on port {port}")
    print(f"{'='*60}\n")

    # ------------------------------------------------------------------
    # 1. Create the base node
    # ------------------------------------------------------------------
    node = StorageNode(node_id=node_id, host="127.0.0.1", port=port)

    # Reload any files that were persisted to disk before a crash
    node.load_from_disk()

    # ------------------------------------------------------------------
    # 2. Attach Lamport clock (Member 3 — must be first, everyone depends on it)
    # ------------------------------------------------------------------
    clock = LamportClock()
    node.lamport_clock = clock
    print(f"[BOOT] Lamport clock attached to {node_id}")

    # ------------------------------------------------------------------
    # 3. Start heartbeat and failure detection (Member 1)
    # ------------------------------------------------------------------
    heartbeat = HeartbeatService(node=node)
    heartbeat.start()
    node.heartbeat_service = heartbeat

    failure_detector = FailureDetector(heartbeat_service=heartbeat)
    node.failure_detector = failure_detector
    print(f"[BOOT] Heartbeat service started for {node_id}")

    # ------------------------------------------------------------------
    # 4. Start replication manager (Member 2 — needs failure detector)
    # ------------------------------------------------------------------
    replication_mgr = ReplicationManager(
        node=node,
        failure_detector=failure_detector
    )
    node.replication_mgr = replication_mgr
    print(f"[BOOT] Replication manager started for {node_id}")

    # ------------------------------------------------------------------
    # 5. Start Raft consensus (Member 4 — needs replication manager)
    # ------------------------------------------------------------------
    log_manager  = LogManager(node=node)
    raft         = RaftNode(node=node, log_manager=log_manager)
    leader_elect = LeaderElection(raft_node=raft, replication_mgr=replication_mgr)

    raft.leader_election = leader_elect
    node.raft_node       = raft
    raft.start()
    print(f"[BOOT] Raft consensus started for {node_id}")

    # ------------------------------------------------------------------
    # 6. Start recovery service (Member 1 — needs all other services ready)
    # ------------------------------------------------------------------
    recovery = RecoveryService(node=node, failure_detector=failure_detector)
    recovery.start()
    print(f"[BOOT] Recovery service started for {node_id}")

    # ------------------------------------------------------------------
    # 7. Start Flask API server (Shared — must be last)
    # ------------------------------------------------------------------
    api_port = get_api_port(port)
    app = create_app(node=node,
                     replication_mgr=replication_mgr,
                     failure_detector=failure_detector,
                     raft=raft,
                     clock=clock)

    print(f"\n[BOOT] {node_id} fully started.")
    print(f"       Socket listener : 127.0.0.1:{port}")
    print(f"       HTTP API        : http://127.0.0.1:{api_port}")
    print(f"       Upload  : POST http://127.0.0.1:{api_port}/upload")
    print(f"       Download: GET  http://127.0.0.1:{api_port}/download/<filename>")
    print(f"       Status  : GET  http://127.0.0.1:{api_port}/status\n")

    # Run Flask (blocks forever)
    app.run(host="0.0.0.0", port=api_port, debug=False, use_reloader=False)


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a CloudDrive storage node")
    parser.add_argument("--id",   required=True,
                        choices=[n["id"] for n in NODES],
                        help="Node ID (node_A, node_B, node_C, or node_D)")
    parser.add_argument("--port", required=True, type=int,
                        help="TCP port for inter-node communication (5001-5004)")
    args = parser.parse_args()

    try:
        start_node(node_id=args.id, port=args.port)
    except KeyboardInterrupt:
        print(f"\n[BOOT] {args.id} shutting down.")
        sys.exit(0)
