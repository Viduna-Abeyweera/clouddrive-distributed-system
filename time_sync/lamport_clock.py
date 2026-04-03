# time_sync/lamport_clock.py
# Implements a Lamport Logical Clock.
# Used by every node on every send and receive to produce a consistent,
# causally-ordered event timeline across all 4 nodes.
# Rules:
#   1. Before sending a message: increment counter, attach to message.
#   2. On receiving a message:   counter = max(local, received) + 1
#   3. For local events:         just increment counter.
#
# This guarantees: if event A causally precedes event B, clock(A) < clock(B).

import threading


class LamportClock:
    """
    Thread-safe Lamport logical clock.

    Example usage in CloudDrive:
        clock = LamportClock()

        # Before sending
        ts = clock.tick()
        message["lamport_ts"] = ts

        # After receiving
        clock.update(message["lamport_ts"])

        # For local events (e.g. file write)
        ts = clock.tick()
    """

    def __init__(self, initial: int = 0):
        self._time = initial
        self._lock = threading.Lock()


    # Core operations

    def tick(self) -> int:
        """
        Increment the clock for a local event or before sending a message.
        Returns the new timestamp to attach to the message/event.
        """
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time: int) -> int:
        """
        Synchronise the clock after receiving a message.
        Sets time = max(local, received) + 1.
        Returns the updated local time.

        Args:
            received_time: the Lamport timestamp embedded in the received message.
        """
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def get_time(self) -> int:
        """Return the current clock value without modifying it."""
        with self._lock:
            return self._time

    def reset(self, value: int = 0):
        """Reset the clock (used in tests)."""
        with self._lock:
            self._time = value


    # Comparison helpers

    @staticmethod
    def happened_before(ts_a: int, ts_b: int) -> bool:
        """
        Returns True if event with timestamp ts_a happened-before ts_b.
        Note: Lamport clocks are not sufficient to determine concurrency —
        use VectorClock for that.
        """
        return ts_a < ts_b

    
    # String representation (useful for debugging / report demos)
   

    def __repr__(self):
        return f"LamportClock(time={self._time})"



# Demo

if __name__ == "__main__":
    print("=" * 50)
    print("Lamport Clock Demo — 3 nodes, 6 events")
    print("=" * 50)

    # Simulate 3 nodes each with their own clock
    clock_A = LamportClock()
    clock_B = LamportClock()
    clock_C = LamportClock()

    # Event 1: Node A does a local write
    ts = clock_A.tick()
    print(f"[A] local write       → clock_A = {ts}")

    # Event 2: Node A sends a message to Node B
    ts_sent = clock_A.tick()
    print(f"[A] sends to B        → clock_A = {ts_sent}  (attached to message)")

    # Event 3: Node B receives the message
    ts_recv = clock_B.update(ts_sent)
    print(f"[B] receives from A   → clock_B = {ts_recv}")

    # Event 4: Node B does a local write
    ts = clock_B.tick()
    print(f"[B] local write       → clock_B = {ts}")

    # Event 5: Node B sends to Node C
    ts_sent2 = clock_B.tick()
    print(f"[B] sends to C        → clock_B = {ts_sent2}  (attached to message)")

    # Event 6: Node C receives (C's clock was at 0)
    ts_recv2 = clock_C.update(ts_sent2)
    print(f"[C] receives from B   → clock_C = {ts_recv2}")

    print()
    print(f"Final clocks  →  A={clock_A.get_time()},  B={clock_B.get_time()},  C={clock_C.get_time()}")
    print("Causal order is preserved: all events on C have higher timestamps than events on A.")
