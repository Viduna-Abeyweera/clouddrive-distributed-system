# =============================================================================
# tests/test_replication.py
# CloudDrive — Member 2: Replication & Consistency Tests
# Run with: pytest tests/test_replication.py -v
# =============================================================================

import pytest
from unittest.mock import MagicMock
from replication.conflict_resolver import resolve, detect_conflict, resolve_list
from replication.consistency        import read_repair


class TestConflictResolver:
    def test_higher_timestamp_wins(self):
        """Last-Write-Wins: higher Lamport timestamp should win."""
        v_old = {"content": "old content", "timestamp": 3, "author": "node_A", "version": 1}
        v_new = {"content": "new content", "timestamp": 9, "author": "node_B", "version": 2}
        winner = resolve(v_old, v_new)
        assert winner["content"] == "new content"

    def test_tie_broken_by_node_id(self):
        """Equal timestamps should use alphabetical node_id as tiebreaker."""
        v_a = {"content": "from A", "timestamp": 5, "author": "node_A", "version": 1}
        v_b = {"content": "from B", "timestamp": 5, "author": "node_B", "version": 1}
        winner = resolve(v_a, v_b)
        # node_A < node_B alphabetically, so node_A wins
        assert winner["author"] == "node_A"

    def test_resolve_list_finds_latest(self):
        """resolve_list should return the version with the highest timestamp."""
        versions = [
            {"content": "v1", "timestamp": 1, "author": "node_A", "version": 1},
            {"content": "v3", "timestamp": 9, "author": "node_C", "version": 3},
            {"content": "v2", "timestamp": 5, "author": "node_B", "version": 2},
        ]
        winner = resolve_list(versions)
        assert winner["content"] == "v3"

    def test_detect_concurrent_without_vector_clocks(self):
        """Without vector clocks, equal timestamps are classified as equal."""
        v1 = {"timestamp": 7, "version": 1}
        v2 = {"timestamp": 7, "version": 1}
        result = detect_conflict(v1, v2)
        assert result == "equal"

    def test_detect_no_conflict_sequential(self):
        """Different timestamps should be classified as no_conflict."""
        v1 = {"timestamp": 3, "version": 1}
        v2 = {"timestamp": 8, "version": 2}
        result = detect_conflict(v1, v2)
        assert result == "no_conflict"


class TestReadRepair:
    def test_read_repair_fixes_stale_node(self):
        """Read repair should push the latest version to a stale node."""
        local_node = MagicMock()
        local_node.node_id = "node_A"

        # Local has latest version (v3)
        local_node.get_file.return_value = {
            "content": "latest", "version": 3, "timestamp": 10
        }

        # Peer has stale version (v1)
        local_node.send_message.side_effect = [
            # GET_VERSION from node_B
            {"status": "ok", "version": 1, "timestamp": 2, "node_id": "node_B"},
            # REPLICATE to node_B
            {"status": "ok"},
        ]

        failure_detector = MagicMock()
        failure_detector.get_alive_nodes.return_value = [
            {"id": "node_A", "host": "127.0.0.1", "port": 5001},
            {"id": "node_B", "host": "127.0.0.1", "port": 5002},
        ]

        # Should not raise and should call send_message to repair node_B
        read_repair("report.txt", local_node, failure_detector)
        assert local_node.send_message.call_count >= 1


