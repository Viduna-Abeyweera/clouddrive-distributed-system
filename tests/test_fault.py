# =============================================================================
# tests/test_fault.py
# Fault Tolerance Tests
# =============================================================================

import time
import pytest
import threading
from unittest.mock import MagicMock

from fault_tolerance.heartbeat        import HeartbeatService
from fault_tolerance.failure_detector import FailureDetector
from fault_tolerance.recovery         import recover_node


def make_mock_node(node_id="node_A"):
    node         = MagicMock()
    node.node_id = node_id
    node.host    = "127.0.0.1"
    node.port    = 5001
    node.peers   = [
        {"id": "node_B", "host": "127.0.0.1", "port": 5002},
        {"id": "node_C", "host": "127.0.0.1", "port": 5003},
    ]
    node.files             = {}
    node.lamport_clock     = MagicMock()
    node.lamport_clock.tick.return_value = 1
    return node


class TestHeartbeat:

    def test_alive_node_is_detected(self):
        node = make_mock_node()
        node.send_message.return_value = {"status": "alive", "node_id": "node_B"}
        hb = HeartbeatService(node=node, interval=0.1, timeout=0.5)
        hb.start()
        time.sleep(0.3)
        assert hb.is_alive("node_B") is True

    def test_dead_node_detected_after_timeout(self):
        node = make_mock_node()
        node.send_message.return_value = {"error": "connection refused"}
        hb = HeartbeatService(node=node, interval=0.1, timeout=0.3)
        hb.last_seen["node_B"] = time.time() - 1.0
        hb.alive["node_B"]     = True
        hb.start()
        time.sleep(0.5)
        assert hb.is_alive("node_B") is False

    def test_recovered_node_marked_alive_again(self):
        node = make_mock_node()
        hb   = HeartbeatService(node=node, interval=0.1, timeout=0.3)
        hb.alive["node_B"]     = False
        hb.last_seen["node_B"] = time.time() - 1.0
        node.send_message.return_value = {"status": "alive", "node_id": "node_B"}
        hb.start()
        time.sleep(0.4)
        assert hb.is_alive("node_B") is True

    def test_get_alive_peers_excludes_dead(self):
        node = make_mock_node()
        hb   = HeartbeatService(node=node, interval=10, timeout=30)
        hb.alive["node_B"] = True
        hb.alive["node_C"] = False
        alive = hb.get_alive_peers()
        ids   = [p["id"] for p in alive]
        assert "node_B" in ids
        assert "node_C" not in ids


class TestFailureDetector:

    def test_quorum_with_three_alive(self):
        node = make_mock_node()
        hb   = MagicMock()
        hb.node = node
        hb.get_alive_peers.return_value = [
            {"id": "node_B", "host": "127.0.0.1", "port": 5002},
            {"id": "node_C", "host": "127.0.0.1", "port": 5003},
        ]
        hb.get_dead_peers.return_value = [
            {"id": "node_D", "host": "127.0.0.1", "port": 5004},
        ]
        hb.is_alive.return_value = True
        fd = FailureDetector(heartbeat_service=hb)
        assert fd.has_quorum() is True

    def test_no_quorum_with_one_alive(self):
        node = make_mock_node()
        hb   = MagicMock()
        hb.node = node
        hb.get_alive_peers.return_value = []
        hb.get_dead_peers.return_value  = [
            {"id": "node_B", "host": "127.0.0.1", "port": 5002},
            {"id": "node_C", "host": "127.0.0.1", "port": 5003},
            {"id": "node_D", "host": "127.0.0.1", "port": 5004},
        ]
        fd = FailureDetector(heartbeat_service=hb)
        assert fd.has_quorum() is False

    def test_local_node_always_alive(self):
        node = make_mock_node("node_A")
        hb   = MagicMock()
        hb.node = node
        hb.get_alive_peers.return_value = []
        hb.get_dead_peers.return_value  = []
        fd = FailureDetector(heartbeat_service=hb)
        assert fd.is_node_alive("node_A") is True

    def test_get_full_status_structure(self):
        node = make_mock_node()
        hb   = MagicMock()
        hb.node = node
        hb.get_alive_peers.return_value  = []
        hb.get_dead_peers.return_value   = []
        hb.get_status_table.return_value = {}
        fd     = FailureDetector(heartbeat_service=hb)
        status = fd.get_full_status()
        assert "alive_nodes" in status
        assert "dead_nodes"  in status
        assert "has_quorum"  in status
        assert "alive_count" in status


class TestRecovery:

    def test_recover_syncs_all_missing_files(self):
        recovering       = make_mock_node("node_A")
        recovering.files = {}
        recovering.get_file.return_value = None
        recovering.send_message.return_value = {
            "status": "ok",
            "files": {
                "report.pdf": {"content": "data1", "version": 1, "timestamp": 5},
                "notes.txt":  {"content": "data2", "version": 1, "timestamp": 6},
            }
        }
        healthy_peer = {"id": "node_B", "host": "127.0.0.1", "port": 5002}
        result       = recover_node(recovering, healthy_peer)
        assert result["synced"]  == 2
        assert result["skipped"] == 0

    def test_recover_skips_files_with_older_version(self):
        recovering = make_mock_node("node_A")
        recovering.get_file.side_effect = lambda fn: (
            {"content": "local_newer", "version": 5, "timestamp": 100}
            if fn == "report.pdf" else None
        )
        recovering.send_message.return_value = {
            "status": "ok",
            "files": {
                "report.pdf": {"content": "peer_older", "version": 2, "timestamp": 40},
            }
        }
        healthy_peer = {"id": "node_B", "host": "127.0.0.1", "port": 5002}
        result       = recover_node(recovering, healthy_peer)
        assert result["skipped"] == 1
        assert result["synced"]  == 0

    def test_recover_handles_connection_failure(self):
        recovering = make_mock_node("node_A")
        recovering.send_message.return_value = {"error": "connection refused"}
        healthy_peer = {"id": "node_B", "host": "127.0.0.1", "port": 5002}
        result = recover_node(recovering, healthy_peer)
        assert result is None

    def test_elapsed_time_recorded(self):
        recovering = make_mock_node("node_A")
        recovering.get_file.return_value = None
        recovering.send_message.return_value = {
            "status": "ok",
            "files": {
                "file.txt": {"content": "hello", "version": 1, "timestamp": 1},
            }
        }
        healthy_peer = {"id": "node_B", "host": "127.0.0.1", "port": 5002}
        result       = recover_node(recovering, healthy_peer)
        assert "elapsed" in result
        assert result["elapsed"] >= 0
