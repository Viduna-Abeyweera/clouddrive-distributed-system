# =============================================================================
# node/config.py
# CloudDrive — Distributed File Storage System
# Cluster configuration: node IDs, hosts, and ports for all 4 simulated nodes.
# All members import this file — do NOT modify without team agreement.
# =============================================================================

# ---------------------------------------------------------------------------
# Node definitions
# Each node runs as a separate Python process on your local machine.
# To simulate a server crash, simply close that node's terminal window.
# ---------------------------------------------------------------------------
NODES = [
    {"id": "node_A", "host": "127.0.0.1", "port": 5001},
    {"id": "node_B", "host": "127.0.0.1", "port": 5002},
    {"id": "node_C", "host": "127.0.0.1", "port": 5003},
    {"id": "node_D", "host": "127.0.0.1", "port": 5004},
]

# ---------------------------------------------------------------------------
# Replication settings
# ---------------------------------------------------------------------------
REPLICATION_FACTOR = 3          # every file is stored on 3 of the 4 nodes
PRIMARY_NODE_ID    = "node_A"   # default primary; Raft will override this

# ---------------------------------------------------------------------------
# Heartbeat / failure detection settings (Member 1)
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL_SEC  = 2     # ping every peer every 2 seconds
HEARTBEAT_TIMEOUT_SEC   = 6     # declare dead after 6 s (3 missed pings)

# ---------------------------------------------------------------------------
# Raft consensus settings (Member 4)
# ---------------------------------------------------------------------------
ELECTION_TIMEOUT_MIN_SEC = 5    # minimum election timeout
ELECTION_TIMEOUT_MAX_SEC = 10   # maximum election timeout (randomised)
LEADER_HEARTBEAT_SEC     = 1    # leader sends heartbeat every 1 second

# ---------------------------------------------------------------------------
# API settings
# ---------------------------------------------------------------------------
API_PORT_OFFSET = 1000          # Flask API port = node port + 1000
                                # e.g. node_A listens on 5001, API on 6001

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def get_node_by_id(node_id: str) -> dict:
    """Return the node config dict for a given node_id, or None."""
    for node in NODES:
        if node["id"] == node_id:
            return node
    return None


def get_peers(node_id: str) -> list:
    """Return all nodes except the one with the given node_id."""
    return [n for n in NODES if n["id"] != node_id]


def get_api_port(node_port: int) -> int:
    """Return the Flask API port for a given node port."""
    return node_port + API_PORT_OFFSET
