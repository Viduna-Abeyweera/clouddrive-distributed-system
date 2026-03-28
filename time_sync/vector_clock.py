# =============================================================================
# time_sync/vector_clock.py
# CloudDrive — Member 3: Time Synchronization
#
# Implements a Vector Clock.
# Unlike Lamport clocks, vector clocks can detect CONCURRENT events —
# two events that happened independently with no causal relationship.
# Used by conflict_resolver.py (Member 2) to identify true concurrency.
#
# A vector clock is a dictionary: {node_id: counter}
# One counter per node in the cluster.
# =============================================================================

from node.config import NODES


class VectorClock:
    """
    Vector clock for detecting concurrent writes across 4 nodes.

    Example:
        vc_A = VectorClock("node_A")
        vc_B = VectorClock("node_B")

        vc_A.increment("node_A")     # A does something
        vc_B.increment("node_B")     # B does something independently

        result = vc_A.compare(vc_A.clock, vc_B.clock)
        # → "concurrent"  (neither happened-before the other)
    """

    def __init__(self, owner_id: str):
        """
        Initialise a vector clock for a specific node.

        Args:
            owner_id: the node_id of the node that owns this clock.
        """
        self.owner_id = owner_id
        # Initialise all counters to 0
        self.clock = {node["id"]: 0 for node in NODES}

    # ------------------------------------------------------------------
    # Mutation operations
    # ------------------------------------------------------------------

    def increment(self, node_id: str = None) -> dict:
        """
        Increment the counter for node_id (defaults to owner).
        Call this before any local event or before sending a message.
        Returns a copy of the current clock state.
        """
        nid = node_id or self.owner_id
        if nid in self.clock:
            self.clock[nid] += 1
        return dict(self.clock)

    def merge(self, other: dict):
        """
        Merge another vector clock into this one.
        Sets each counter to max(local, received).
        Call this on receiving a message, then increment own counter.

        Args:
            other: the clock dictionary from a received message.
        """
        for node_id in self.clock:
            self.clock[node_id] = max(
                self.clock.get(node_id, 0),
                other.get(node_id, 0)
            )

    def receive(self, other: dict):
        """
        Convenience method: merge then increment own counter.
        This is the standard receive operation.
        """
        self.merge(other)
        self.clock[self.owner_id] += 1

    def to_dict(self) -> dict:
        """Return a plain dict copy of the clock (safe to serialise as JSON)."""
        return dict(self.clock)

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    @staticmethod
    def compare(vc_a: dict, vc_b: dict) -> str:
        """
        Compare two vector clock dictionaries.

        Returns one of:
            "before"     — vc_a happened-before vc_b  (vc_a ≤ vc_b, not equal)
            "after"      — vc_b happened-before vc_a  (vc_b ≤ vc_a, not equal)
            "concurrent" — neither happened-before the other (conflict!)
            "equal"      — identical clocks

        This is the key function used by conflict_resolver.py:
        if compare() returns "concurrent", it's a genuine write conflict.
        """
        keys = set(vc_a.keys()) | set(vc_b.keys())

        a_leq_b = all(vc_a.get(k, 0) <= vc_b.get(k, 0) for k in keys)
        b_leq_a = all(vc_b.get(k, 0) <= vc_a.get(k, 0) for k in keys)

        if vc_a == vc_b:
            return "equal"
        elif a_leq_b and not b_leq_a:
            return "before"
        elif b_leq_a and not a_leq_b:
            return "after"
        else:
            return "concurrent"

    @staticmethod
    def dominates(vc_a: dict, vc_b: dict) -> bool:
        """
        Returns True if vc_a is strictly greater than or equal to vc_b
        in every component. Used to decide which version wins in replication.
        """
        keys = set(vc_a.keys()) | set(vc_b.keys())
        return all(vc_a.get(k, 0) >= vc_b.get(k, 0) for k in keys)

    def __repr__(self):
        entries = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VectorClock({self.owner_id} → [{entries}])"


# =============================================================================
# Demo — run this file directly to see vector clock behaviour
# =============================================================================
if __name__ == "__main__":
    print("=" * 55)
    print("Vector Clock Demo — demonstrating concurrent detection")
    print("=" * 55)

    vc_A = VectorClock("node_A")
    vc_B = VectorClock("node_B")
    vc_C = VectorClock("node_C")

    # ---- Scenario 1: Sequential (A sends to B) ----
    print("\n--- Scenario 1: A writes, sends to B ---")
    vc_A.increment("node_A")
    print(f"A after write:    {vc_A}")

    vc_B.receive(vc_A.to_dict())
    vc_B.increment("node_B")
    print(f"B after receive:  {vc_B}")

    result = VectorClock.compare(vc_A.clock, vc_B.clock)
    print(f"Compare A vs B:   '{result}'  (expected: 'before')")

    # ---- Scenario 2: Concurrent (A and C write independently) ----
    print("\n--- Scenario 2: A and C write independently (CONCURRENT) ---")
    vc_A2 = VectorClock("node_A")
    vc_C2 = VectorClock("node_C")

    vc_A2.increment("node_A")  # A writes
    vc_C2.increment("node_C")  # C writes — neither knows about the other

    print(f"A after write:    {vc_A2}")
    print(f"C after write:    {vc_C2}")

    result2 = VectorClock.compare(vc_A2.clock, vc_C2.clock)
    print(f"Compare A vs C:   '{result2}'  (expected: 'concurrent')")
    print("→ This is a WRITE CONFLICT — conflict_resolver.py must handle it!")
