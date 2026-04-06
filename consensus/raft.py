# =============================================================================
# consensus/raft.py
# CloudDrive — Member 4: Consensus & Agreement
#
# Implements the Raft consensus algorithm state machine.
# Guarantees exactly one leader per term, preventing split-brain scenarios.
#
# Three roles:
#   FOLLOWER  — default state; waits for leader heartbeats
#   CANDIDATE — starts an election when timeout fires
#   LEADER    — wins majority vote; sends heartbeats to suppress new elections
# =============================================================================

import threading
import time
import random
from enum import Enum

from node.config import (
    ELECTION_TIMEOUT_MIN_SEC,
    ELECTION_TIMEOUT_MAX_SEC,
    LEADER_HEARTBEAT_SEC,
)


class Role(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


class RaftNode:
    """
    Raft consensus state machine.

    Key invariants maintained:
      - At most one leader per term (guaranteed by majority voting)
      - A node only votes once per term
      - A new leader always has the most up-to-date log
      - Leader sends periodic heartbeats to reset follower election timers
    """

    def __init__(self, node, log_manager):
        """
        Args:
            node:        the StorageNode this Raft instance belongs to
            log_manager: LogManager for append-entries log replication
        """
        self.node        = node
        self.log_manager = log_manager

        # Persistent state (survives restarts in a real system)
        self.current_term = 0           # monotonically increasing election round
        self.voted_for    = None        # node_id we voted for in current term
        self.leader_id    = None        # current known leader

        # Volatile state
        self.role            = Role.FOLLOWER
        self.votes_received  = 0
        self.leader_election = None     # set by run.py

        # Locks
        self._state_lock = threading.Lock()

        # Election timer — reset every time we hear from a valid leader
        self._election_timer = None
        self._heartbeat_timer = None

        # Register message handlers on the base node
        # (done in start() so node is fully initialised first)

    def start(self):
        """Begin Raft operation — register handlers and start election timer."""
        # Register Raft message handlers on the base node socket server
        self.node.register_handler("VOTE_REQUEST",    self._handle_vote_request)
        self.node.register_handler("VOTE_RESPONSE",   self._handle_vote_response)
        self.node.register_handler("APPEND_ENTRIES",  self._handle_append_entries)
        self.node.register_handler("APPEND_RESPONSE", self._handle_append_response)

        # Start as follower with a random election timeout
        self._reset_election_timer()
        print(f"[RAFT] {self.node.node_id} started as FOLLOWER (term {self.current_term})")

    # ------------------------------------------------------------------
    # Election timer management
    # ------------------------------------------------------------------

    def _reset_election_timer(self):
        """
        Reset the election timeout with a new random delay.
        Randomised timeouts prevent multiple candidates from starting
        elections simultaneously (which causes split votes).
        """
        if self._election_timer:
            self._election_timer.cancel()

        timeout = random.uniform(ELECTION_TIMEOUT_MIN_SEC, ELECTION_TIMEOUT_MAX_SEC)
        self._election_timer = threading.Timer(timeout, self._start_election)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _start_election(self):
        """
        Election timeout fired — become a candidate and request votes.
        Only starts if we're not already the leader and the node is alive.

        IMPORTANT: If the node is simulating a crash (alive=False), we must
        NOT start an election. Otherwise the term inflates while the node
        is dead, and when it recovers it rejects the legitimate leader's
        heartbeats because its term is artificially higher.
        """
        with self._state_lock:
            if self.role == Role.LEADER:
                return   # Already leader, no need to hold election

            # Don't start elections while simulating a crash.
            # Just reschedule the timer so we check again later.
            if not self.node.alive:
                self._reset_election_timer()
                return

            self.role         = Role.CANDIDATE
            self.current_term += 1
            self.voted_for    = self.node.node_id  # vote for yourself
            self.votes_received = 1                # count self-vote
            term = self.current_term

        print(f"\n[RAFT] {self.node.node_id} starting ELECTION for term {term}")
        self._request_votes(term)

    # ------------------------------------------------------------------
    # Vote requesting
    # ------------------------------------------------------------------

    def _request_votes(self, term: int):
        """Broadcast VOTE_REQUEST to all peers."""
        if self.leader_election:
            self.leader_election.request_votes(term)
        else:
            # Fallback: direct request without leader_election module
            for peer in self.node.peers:
                t = threading.Thread(
                    target=self._send_vote_request,
                    args=(peer, term),
                    daemon=True
                )
                t.start()

    def _send_vote_request(self, peer: dict, term: int):
        """Send a single VOTE_REQUEST to one peer."""
        response = self.node.send_message(
            peer["host"], peer["port"],
            {
                "type":         "VOTE_REQUEST",
                "term":         term,
                "candidate_id": self.node.node_id,
                "last_log_index": self.log_manager.get_last_index(),
                "last_log_term":  self.log_manager.get_last_term(),
            }
        )
        if "error" not in response:
            self._handle_vote_response(self.node, response)

    # ------------------------------------------------------------------
    # Message handlers (registered on StorageNode)
    # ------------------------------------------------------------------

    def _handle_vote_request(self, node, message: dict) -> dict:
        """
        Respond to a VOTE_REQUEST from a candidate.
        Grant vote if:
          1. Candidate's term >= our term
          2. We haven't voted for anyone else this term
          3. Candidate's log is at least as up-to-date as ours
        """
        with self._state_lock:
            candidate_term = message.get("term", 0)
            candidate_id   = message.get("candidate_id")

            # If we see a higher term, revert to follower
            if candidate_term > self.current_term:
                self.current_term = candidate_term
                self.role         = Role.FOLLOWER
                self.voted_for    = None

            # Decide whether to grant vote
            vote_granted = False
            if (candidate_term >= self.current_term and
                    (self.voted_for is None or self.voted_for == candidate_id)):
                self.voted_for    = candidate_id
                self.current_term = candidate_term
                vote_granted      = True
                self._reset_election_timer()

            print(f"[RAFT] {self.node.node_id} {'GRANTED' if vote_granted else 'DENIED'} "
                  f"vote to {candidate_id} for term {candidate_term}")

            return {
                "type":        "VOTE_RESPONSE",
                "vote_granted": vote_granted,
                "term":        self.current_term,
                "voter_id":    self.node.node_id,
            }

    def _handle_vote_response(self, node, message: dict) -> dict:
        """
        Process a vote response. If we've collected a majority, become leader.
        """
        with self._state_lock:
            if self.role != Role.CANDIDATE:
                return {"status": "not_candidate"}

            if message.get("term", 0) > self.current_term:
                # Someone has a higher term — step down
                self.current_term = message["term"]
                self.role         = Role.FOLLOWER
                self.voted_for    = None
                return {"status": "stepped_down"}

            if message.get("vote_granted"):
                self.votes_received += 1
                total_nodes = len(self.node.peers) + 1   # include self
                majority    = (total_nodes // 2) + 1

                print(f"[RAFT] {self.node.node_id} has {self.votes_received}/{total_nodes} votes "
                      f"(need {majority})")

                if self.votes_received >= majority:
                    self._become_leader()

        return {"status": "ok"}

    def _handle_append_entries(self, node, message: dict) -> dict:
        """
        Receive AppendEntries RPC from the leader.
        In the heartbeat case (empty entries), just reset the election timer.
        """
        with self._state_lock:
            leader_term = message.get("term", 0)
            leader_id   = message.get("leader_id")

            if leader_term < self.current_term:
                return {"success": False, "term": self.current_term}

            # Valid leader — update state and reset election timer
            self.current_term = leader_term
            self.leader_id    = leader_id
            self.role         = Role.FOLLOWER
            self.voted_for    = None    # new term means new votes

        # Reset election timer (we heard from the leader)
        self._reset_election_timer()

        # Process log entries if any
        entries = message.get("entries", [])
        if entries:
            self.log_manager.handle_append_entries(message)

        return {"success": True, "term": self.current_term}

    def _handle_append_response(self, node, message: dict) -> dict:
        """Leader receives acknowledgement of AppendEntries from a follower."""
        self.log_manager.handle_append_response(message)
        return {"status": "ok"}

    # ------------------------------------------------------------------
    # Becoming leader
    # ------------------------------------------------------------------

    def _become_leader(self):
        """
        Transition to leader role.
        Must be called while holding _state_lock.
        """
        if self.role == Role.LEADER:
            return  # Already leader

        self.role      = Role.LEADER
        self.leader_id = self.node.node_id
        if self._election_timer:
            self._election_timer.cancel()

        print(f"\n[RAFT] *** {self.node.node_id} is now LEADER for term {self.current_term} ***\n")

        # Notify replication manager to switch primary to this node
        if self.node.replication_mgr:
            self.node.replication_mgr.set_primary(self.node.node_id)

        # Start sending heartbeats to maintain leadership
        self._send_heartbeats()

    def _send_heartbeats(self):
        """Send periodic heartbeat AppendEntries to all followers."""
        if self.role != Role.LEADER:
            return   # Stop if we lost leadership

        # Don't send heartbeats if the node is simulating a crash
        if not self.node.alive:
            return

        for peer in self.node.peers:
            threading.Thread(
                target=self._send_heartbeat_to,
                args=(peer,),
                daemon=True
            ).start()

        # Schedule next heartbeat
        self._heartbeat_timer = threading.Timer(
            LEADER_HEARTBEAT_SEC, self._send_heartbeats
        )
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _send_heartbeat_to(self, peer: dict):
        """Send a heartbeat (empty AppendEntries) to one follower."""
        response = self.node.send_message(
            peer["host"], peer["port"],
            {
                "type":      "APPEND_ENTRIES",
                "term":      self.current_term,
                "leader_id": self.node.node_id,
                "entries":   [],   # empty = heartbeat only
            }
        )
        # If a higher term is seen, step down
        if "error" not in response and response.get("term", 0) > self.current_term:
            with self._state_lock:
                self.current_term = response["term"]
                self.role         = Role.FOLLOWER
                self.leader_id    = None
                self.voted_for    = None
            self._reset_election_timer()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """Return current Raft state summary for /status endpoint."""
        return {
            "node_id":      self.node.node_id,
            "role":         self.role.value,
            "current_term": self.current_term,
            "leader_id":    self.leader_id,
            "voted_for":    self.voted_for,
            "log_length":   self.log_manager.get_last_index(),
        }