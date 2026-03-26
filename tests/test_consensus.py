# =============================================================================
# tests/test_consensus.py
# CloudDrive — Member 4: Consensus & Agreement Tests
# Run with: pytest tests/test_consensus.py -v
# =============================================================================

import pytest
import time
import threading
from unittest.mock import MagicMock, patch
from consensus.raft           import RaftNode, Role
from consensus.leader_election import LeaderElection
from consensus.log_manager    import LogManager, LogEntry


def make_raft_node(node_id="node_A"):
    """Create a RaftNode with mocked dependencies."""
    node          = MagicMock()
    node.node_id  = node_id
    node.peers    = [
        {"id": "node_B", "host": "127.0.0.1", "port": 5002},
        {"id": "node_C", "host": "127.0.0.1", "port": 5003},
        {"id": "node_D", "host": "127.0.0.1", "port": 5004},
    ]
    node.replication_mgr = MagicMock()
    node.failure_detector = MagicMock()
    node.failure_detector.get_alive_nodes.return_value = []

    log_manager = LogManager(node=node)
    raft = RaftNode(node=node, log_manager=log_manager)
    raft.leader_election = MagicMock()
    return raft


class TestVoteHandling:
    def test_vote_granted_for_valid_candidate(self):
        """Should grant vote to first valid candidate in a new term."""
        raft = make_raft_node("node_A")
        raft.start()

        msg = {
            "type":         "VOTE_REQUEST",
            "term":         1,
            "candidate_id": "node_B",
        }
        response = raft._handle_vote_request(raft.node, msg)
        assert response["vote_granted"] is True
        assert raft.voted_for == "node_B"

    def test_vote_denied_if_already_voted(self):
        """Should deny vote if already voted for someone else this term."""
        raft = make_raft_node("node_A")
        raft.start()
        raft.current_term = 1
        raft.voted_for    = "node_C"   # already voted

        msg = {
            "type":         "VOTE_REQUEST",
            "term":         1,
            "candidate_id": "node_B",
        }
        response = raft._handle_vote_request(raft.node, msg)
        assert response["vote_granted"] is False

    def test_vote_denied_for_stale_term(self):
        """Should deny vote from a candidate with a lower term."""
        raft = make_raft_node("node_A")
        raft.start()
        raft.current_term = 5   # we're on term 5

        msg = {
            "type":         "VOTE_REQUEST",
            "term":         3,   # candidate is behind
            "candidate_id": "node_B",
        }
        response = raft._handle_vote_request(raft.node, msg)
        assert response["vote_granted"] is False


class TestLeaderElection:
    def test_become_leader_on_majority(self):
        """Candidate should become leader after receiving majority votes."""
        raft = make_raft_node("node_A")
        raft.start()

        # Simulate getting 2 votes (+ self-vote = 3 out of 4 = majority)
        raft.node.send_message.return_value = {
            "vote_granted": True,
            "term": 1,
        }

        # Start election manually
        raft.current_term   = 1
        raft.role           = Role.CANDIDATE
        raft.votes_received = 1   # self-vote

        # Simulate receiving 2 more votes
        raft._handle_vote_response(raft.node, {"vote_granted": True, "term": 1})
        raft._handle_vote_response(raft.node, {"vote_granted": True, "term": 1})

        assert raft.role == Role.LEADER

    def test_step_down_on_higher_term(self):
        """Leader should step down if it sees a message with a higher term."""
        raft = make_raft_node("node_A")
        raft.start()
        raft.role         = Role.LEADER
        raft.current_term = 3

        # Receive AppendEntries with higher term
        msg = {
            "type":      "APPEND_ENTRIES",
            "term":      5,
            "leader_id": "node_B",
            "entries":   [],
        }
        response = raft._handle_append_entries(raft.node, msg)

        assert raft.role == Role.FOLLOWER
        assert raft.current_term == 5


class TestLogManager:
    def test_append_increments_index(self):
        """Each append should get a unique, incrementing index."""
        node = MagicMock()
        node.node_id = "node_A"
        node.raft_node = MagicMock()
        node.raft_node.current_term = 1
        log = LogManager(node=node)

        e1 = log.append({"type": "WRITE", "filename": "a.txt"})
        e2 = log.append({"type": "WRITE", "filename": "b.txt"})

        assert e1.index == 1
        assert e2.index == 2

    def test_get_last_index_empty(self):
        """Empty log should return index 0."""
        node = MagicMock()
        node.node_id = "node_A"
        log  = LogManager(node=node)
        assert log.get_last_index() == 0

    def test_get_last_term_empty(self):
        """Empty log should return term 0."""
        node = MagicMock()
        node.node_id = "node_A"
        log  = LogManager(node=node)
        assert log.get_last_term() == 0
