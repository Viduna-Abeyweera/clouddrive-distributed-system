# =============================================================================
# consensus/leader_election.py
# CloudDrive — Member 4: Consensus & Agreement
#
# LeaderElection: manages the vote solicitation and tallying process.
# Works alongside raft.py — RaftNode calls request_votes() here when
# a candidate starts an election.
# =============================================================================

import threading


class LeaderElection:
    """
    Handles the vote-requesting phase of Raft leader election.

    When a RaftNode becomes a CANDIDATE it calls request_votes().
    This module sends VOTE_REQUEST messages to all peers in parallel
    and counts responses. If a majority votes YES, it tells the
    RaftNode to become the leader.
    """

    def __init__(self, raft_node, replication_mgr):
        """
        Args:
            raft_node:       the RaftNode that owns this election service
            replication_mgr: ReplicationManager to notify on leader change
        """
        self.raft         = raft_node
        self.repl_mgr     = replication_mgr
        self._vote_counts = {}   # term → vote count (prevents double-counting)
        self._lock        = threading.Lock()

    def request_votes(self, term: int):
        """
        Send VOTE_REQUEST to all peers in parallel.
        Collects responses and triggers become_leader() if majority reached.

        Args:
            term: the election term being contested
        """
        node       = self.raft.node
        peers      = node.peers
        total      = len(peers) + 1    # peers + self
        majority   = (total // 2) + 1

        # Initialise vote count for this term (we already voted for ourselves)
        with self._lock:
            self._vote_counts[term] = 1   # self-vote

        print(f"[ELECTION] {node.node_id} requesting votes for term {term} "
              f"(need {majority}/{total})")

        # Send requests in parallel threads for speed
        threads = []
        for peer in peers:
            t = threading.Thread(
                target=self._solicit_vote,
                args=(peer, term, majority),
                daemon=True
            )
            threads.append(t)
            t.start()

        # Don't join — we're non-blocking, responses trickle in asynchronously

    def _solicit_vote(self, peer: dict, term: int, majority: int):
        """
        Send one VOTE_REQUEST and process the response.
        Runs in its own thread per peer.
        """
        node     = self.raft.node
        response = node.send_message(
            peer["host"], peer["port"],
            {
                "type":             "VOTE_REQUEST",
                "term":             term,
                "candidate_id":     node.node_id,
                "last_log_index":   self.raft.log_manager.get_last_index(),
                "last_log_term":    self.raft.log_manager.get_last_term(),
            }
        )

        if "error" in response:
            print(f"[ELECTION] No response from {peer['id']} (may be down)")
            return

        # Check if the responder has a higher term (we must step down)
        resp_term = response.get("term", 0)
        if resp_term > self.raft.current_term:
            with self.raft._state_lock:
                self.raft.current_term = resp_term
                from consensus.raft import Role
                self.raft.role       = Role.FOLLOWER
                self.raft.voted_for  = None
            self.raft._reset_election_timer()
            print(f"[ELECTION] Stepped down — saw higher term {resp_term} from {peer['id']}")
            return

        if not response.get("vote_granted"):
            print(f"[ELECTION] {peer['id']} denied vote to {node.node_id}")
            return

        # Vote granted!
        print(f"[ELECTION] {peer['id']} granted vote to {node.node_id} for term {term}")

        with self._lock:
            if term not in self._vote_counts:
                return   # election already decided

            self._vote_counts[term] += 1
            votes = self._vote_counts[term]

        if votes >= majority:
            # Majority reached — become leader (only trigger once)
            with self._lock:
                if term in self._vote_counts:
                    del self._vote_counts[term]   # prevent re-triggering
                else:
                    return   # another thread already triggered

            print(f"[ELECTION] {node.node_id} won election for term {term} "
                  f"with {votes}/{len(node.peers)+1} votes")
            self.become_leader()

    def become_leader(self):
        """
        Transition this node to the LEADER role.
        Notifies ReplicationManager to make this node the primary.
        """
        with self.raft._state_lock:
            from consensus.raft import Role
            self.raft.role      = Role.LEADER
            self.raft.leader_id = self.raft.node.node_id

        # Update primary for file replication
        if self.repl_mgr:
            self.repl_mgr.set_primary(self.raft.node.node_id)

        print(f"\n[ELECTION] *** {self.raft.node.node_id} became LEADER "
              f"(term {self.raft.current_term}) ***\n")

        # Start leader heartbeats
        self.raft._send_heartbeats()
