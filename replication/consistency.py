# =============================================================================
# replication/consistency.py
# CloudDrive — Member 2: Data Replication & Consistency
#
# Read Repair: passively heals inconsistencies when files are read.
# Called by ReplicationManager.read_file() in a background thread.
# =============================================================================

import threading


def read_repair(filename: str, local_node, failure_detector):
    """
    Query all alive nodes for their version of filename.
    Find the latest version. Push it to any node that has an older version.

    This implements the 'eventual consistency' model:
    data becomes consistent passively as files are read, without requiring
    a synchronous multi-node commit on every write.

    Args:
        filename:         the file to check across the cluster
        local_node:       the StorageNode initiating the repair
        failure_detector: FailureDetector to find alive nodes
    """
    alive_nodes = failure_detector.get_alive_nodes()

    if len(alive_nodes) <= 1:
        return  # nothing to repair with only one node

    # --- Step 1: collect version info from all alive nodes ---
    versions = []

    for peer in alive_nodes:
        if peer["id"] == local_node.node_id:
            # Check local storage directly
            local_data = local_node.get_file(filename)
            if local_data:
                versions.append({
                    "node":      peer,
                    "file_data": local_data,
                    "timestamp": local_data.get("timestamp", 0),
                    "version":   local_data.get("version",   0),
                })
            else:
                versions.append({
                    "node":      peer,
                    "file_data": None,
                    "timestamp": 0,
                    "version":   0,
                })
        else:
            response = local_node.send_message(
                peer["host"], peer["port"],
                {"type": "GET_VERSION", "filename": filename}
            )
            if "error" not in response and response.get("status") == "ok":
                versions.append({
                    "node":      peer,
                    "file_data": None,   # don't fetch full content yet
                    "timestamp": response.get("timestamp", 0),
                    "version":   response.get("version",   0),
                })

    # --- Step 2: find the latest version ---
    if not versions:
        return

    latest = max(versions, key=lambda v: (v["version"], v["timestamp"]))

    if latest["version"] == 0:
        return  # file doesn't exist anywhere

    # --- Step 3: fetch full content of latest version if we don't have it ---
    if latest["file_data"] is None:
        peer     = latest["node"]
        response = local_node.send_message(
            peer["host"], peer["port"],
            {"type": "READ_FILE", "filename": filename}
        )
        if "error" in response or response.get("status") != "ok":
            return
        latest["file_data"] = response["file_data"]

    latest_data = latest["file_data"]

    # --- Step 4: push latest version to any stale nodes ---
    repaired = []
    for v in versions:
        if v["version"] < latest["version"]:
            peer = v["node"]

            if peer["id"] == local_node.node_id:
                # Repair locally
                local_node.store_file(filename, latest_data)
            else:
                local_node.send_message(
                    peer["host"], peer["port"],
                    {
                        "type":      "REPLICATE",
                        "filename":  filename,
                        "file_data": latest_data,
                    }
                )
            repaired.append(peer["id"])

    if repaired:
        print(f"[CONSISTENCY] Read repair on '{filename}': "
              f"updated {repaired} to v{latest['version']}")
