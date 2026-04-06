# =============================================================================
# api/server.py
# CloudDrive — Shared: HTTP API
#
# Flask application that exposes CloudDrive operations over HTTP.
# This is the single entry point for all client interactions.
# Wires together all 4 components.
# =============================================================================

import time
from flask import Flask, request, jsonify
from flask_cors import CORS


def create_app(node, replication_mgr, failure_detector, raft, clock):
    """
    Create and configure the Flask application.

    Args:
        node:             StorageNode instance
        replication_mgr:  ReplicationManager (Member 2)
        failure_detector: FailureDetector (Member 1)
        raft:             RaftNode (Member 4)
        clock:            LamportClock (Member 3)

    Returns: configured Flask app
    """
    app = Flask(__name__)
    CORS(app)

    # -------------------------------------------------------------------------
    # POST /upload  — upload a file to CloudDrive
    # -------------------------------------------------------------------------
    @app.route("/upload", methods=["POST"])
    def upload():
        """
        Upload a file and replicate it across the cluster.

        Request body (JSON):
            {"filename": "report.pdf", "content": "file contents here"}

        Response:
            {"status": "ok", "filename": ..., "version": ..., "timestamp": ...,
             "replicated_to": [...]}
        """
        data = request.get_json()
        if not data or "filename" not in data or "content" not in data:
            return jsonify({"error": "request must include 'filename' and 'content'"}), 400

        filename = data["filename"]
        content  = data["content"]

        # Get a Lamport timestamp for this write (Member 3)
        ts = clock.tick()

        # Write + replicate (Member 2)
        result = replication_mgr.write_file(filename, content, ts)

        # Append to Raft log (Member 4)
        # IMPORTANT: Only the LEADER should append to the Raft log and
        # replicate log entries to followers. If a follower does this,
        # it sends APPEND_ENTRIES with its own node_id as leader_id,
        # causing the real leader to step down — a split-brain bug.
        if raft and raft.log_manager:
            from consensus.raft import Role
            if raft.role == Role.LEADER:
                log_entry = raft.log_manager.append({
                    "type":      "WRITE",
                    "filename":  filename,
                    "file_data": node.get_file(filename),
                })
                raft.log_manager.replicate_to_followers(log_entry)

        if result.get("status") in ("ok", "conflict_rejected"):
            return jsonify(result), 200
        return jsonify(result), 500

    # -------------------------------------------------------------------------
    # GET /download/<filename>  — download a file
    # -------------------------------------------------------------------------
    @app.route("/download/<filename>", methods=["GET"])
    def download(filename):
        """
        Download a file. Triggers read repair in the background.

        Query params:
            ?strong=true  — use strong consistency (slower but guaranteed latest)

        Response:
            {"filename": ..., "content": ..., "version": ..., "timestamp": ...}
        """
        strong = request.args.get("strong", "false").lower() == "true"

        if strong:
            file_data = replication_mgr.read_file_strong(filename)
        else:
            file_data = replication_mgr.read_file(filename)

        if not file_data:
            return jsonify({"error": f"file '{filename}' not found"}), 404

        return jsonify({
            "filename":  filename,
            "content":   file_data.get("content"),
            "version":   file_data.get("version",   1),
            "timestamp": file_data.get("timestamp", 0),
            "author":    file_data.get("author",    "unknown"),
        }), 200

    # -------------------------------------------------------------------------
    # GET /files  — list all files stored on this node
    # -------------------------------------------------------------------------
    @app.route("/files", methods=["GET"])
    def list_files():
        """Return a list of all filenames stored on this node."""
        all_files = node.get_all_files()
        return jsonify({
            "node_id": node.node_id,
            "count":   len(all_files),
            "files":   [
                {
                    "filename":  fn,
                    "version":   data.get("version",   1),
                    "timestamp": data.get("timestamp", 0),
                }
                for fn, data in all_files.items()
            ]
        }), 200

    # -------------------------------------------------------------------------
    # GET /status  — cluster health dashboard
    # -------------------------------------------------------------------------
    @app.route("/status", methods=["GET"])
    def status():
        """
        Return complete cluster status. Great for demos — shows everything
        happening in the system in one JSON response.
        """
        fd_status = failure_detector.get_full_status() if failure_detector else {}
        raft_state = raft.get_state() if raft else {}

        return jsonify({
            "this_node":   node.node_id,
            "role":        raft_state.get("role",    "unknown"),
            "leader":      raft_state.get("leader_id", None),
            "term":        raft_state.get("current_term", 0),
            "lamport_ts":  clock.get_time(),
            "file_count":  len(node.get_all_files()),
            "alive_nodes": fd_status.get("alive_nodes", []),
            "dead_nodes":  fd_status.get("dead_nodes",  []),
            "has_quorum":  fd_status.get("has_quorum",  False),
            "peer_details": fd_status.get("peer_details", {}),
            "log_entries": raft.log_manager.get_log_summary() if raft else [],
        }), 200

    # -------------------------------------------------------------------------
    # POST /simulate/kill  — simulate this node crashing (for tests/demo)
    # -------------------------------------------------------------------------
    @app.route("/simulate/kill", methods=["POST"])
    def simulate_kill():
        """
        Simulate a node crash by setting alive = False.
        Also stops Raft heartbeats if this node was the leader,
        preventing stale heartbeat threads from running.
        """
        node.alive = False

        # Stop leader heartbeats immediately so the dead node
        # doesn't keep trying to send heartbeats (which all fail)
        if raft and raft._heartbeat_timer:
            raft._heartbeat_timer.cancel()
            raft._heartbeat_timer = None

        print(f"\n[DEMO] {node.node_id} simulating CRASH\n")
        return jsonify({
            "status":  "killed",
            "node_id": node.node_id,
            "message": f"{node.node_id} is now simulating a crash"
        }), 200

    # -------------------------------------------------------------------------
    # POST /simulate/recover  — bring a killed node back online
    # -------------------------------------------------------------------------
    @app.route("/simulate/recover", methods=["POST"])
    def simulate_recover():
        """
        Bring a simulated-dead node back online and trigger recovery sync.

        Resets Raft state to FOLLOWER so the recovered node doesn't
        think it's still the leader. The actual leader's heartbeats
        will reach this node within 1 second and confirm the correct state.

        Uses node.peers (static config) instead of failure_detector.get_alive_nodes()
        because the failure detector hasn't had time to re-ping peers yet after
        the simulated crash — it still thinks all peers are dead.
        """
        node.alive = True
        print(f"\n[DEMO] {node.node_id} recovering...\n")

        # Reset Raft state to FOLLOWER on recovery.
        # Without this, a node that was leader before being killed
        # might still think it's leader and conflict with the new
        # legitimately elected leader.
        if raft:
            from consensus.raft import Role
            with raft._state_lock:
                if raft.role in (Role.LEADER, Role.CANDIDATE):
                    print(f"[DEMO] Resetting {node.node_id} from {raft.role.value} to FOLLOWER")
                    raft.role = Role.FOLLOWER
                    raft.voted_for = None
                    raft.leader_id = None
            # Restart the election timer so this node participates
            # in future elections if needed
            raft._reset_election_timer()

        # Trigger file sync from a healthy peer
        result = {}
        from fault_tolerance.recovery import recover_node

        # Try each known peer from config until one responds successfully.
        for peer in node.peers:
            sync_result = recover_node(node, peer)
            if sync_result is not None:
                result = sync_result
                break   # successfully synced from one peer, that's enough

        return jsonify({
            "status":      "recovered",
            "node_id":     node.node_id,
            "sync_result": result,
        }), 200

    # -------------------------------------------------------------------------
    # GET /node/<node_id>/files  — query files on a specific node (for demos)
    # -------------------------------------------------------------------------
    @app.route("/node/<target_node_id>/files", methods=["GET"])
    def node_files(target_node_id):
        """
        Ask a specific peer node for its file list.
        Useful in demos to show all 4 nodes have the same files.
        """
        from node.config import get_node_by_id, get_api_port
        import requests as req

        target = get_node_by_id(target_node_id)
        if not target:
            return jsonify({"error": f"unknown node: {target_node_id}"}), 404

        if target_node_id == node.node_id:
            return list_files()

        api_port = get_api_port(target["port"])
        try:
            r = req.get(f"http://{target['host']}:{api_port}/files", timeout=3)
            return jsonify(r.json()), r.status_code
        except Exception as e:
            return jsonify({"error": str(e)}), 503

    # -------------------------------------------------------------------------
    # GET /cluster/sync-check/<filename>  — verify replication consistency
    # -------------------------------------------------------------------------
    @app.route("/cluster/sync-check/<filename>", methods=["GET"])
    def sync_check(filename):
        """
        Query all alive nodes for their version of a file.
        Returns a table showing whether all nodes are in sync.
        Perfect for demos — proves replication is working.
        """
        alive = failure_detector.get_alive_nodes()
        versions = []

        for peer in alive:
            if peer["id"] == node.node_id:
                fd = node.get_file(filename)
                versions.append({
                    "node_id":  node.node_id,
                    "version":  fd.get("version",   0) if fd else 0,
                    "timestamp": fd.get("timestamp", 0) if fd else 0,
                    "has_file": fd is not None,
                })
            else:
                response = node.send_message(
                    peer["host"], peer["port"],
                    {"type": "GET_VERSION", "filename": filename}
                )
                versions.append({
                    "node_id":   peer["id"],
                    "version":   response.get("version",   0),
                    "timestamp": response.get("timestamp", 0),
                    "has_file":  response.get("status") == "ok",
                })

        all_versions = [v["version"] for v in versions if v["has_file"]]
        in_sync = len(set(all_versions)) <= 1 if all_versions else False

        return jsonify({
            "filename":  filename,
            "in_sync":   in_sync,
            "versions":  versions,
        }), 200

    return app