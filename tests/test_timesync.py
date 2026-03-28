# =============================================================================
# tests/test_timesync.py
# CloudDrive — Member 3: Time Synchronization Tests
# Run with: pytest tests/test_timesync.py -v
# =============================================================================

import pytest
import threading
from time_sync.lamport_clock import LamportClock
from time_sync.vector_clock  import VectorClock
from time_sync.ntp_client    import simulate_skew, synced_time, clear_skew
import time


class TestLamportClock:
    def test_tick_increments(self):
        """tick() should increment and return the counter."""
        clock = LamportClock()
        assert clock.tick() == 1
        assert clock.tick() == 2
        assert clock.tick() == 3

    def test_update_takes_max(self):
        """update() should set time = max(local, received) + 1."""
        clock = LamportClock()
        clock.tick()             # local = 1
        result = clock.update(5) # should become max(1,5)+1 = 6
        assert result == 6

    def test_update_keeps_local_if_higher(self):
        """update() should keep local time if it's already higher."""
        clock = LamportClock()
        for _ in range(8):
            clock.tick()         # local = 8
        result = clock.update(3) # should become max(8,3)+1 = 9
        assert result == 9

    def test_causal_ordering_preserved(self):
        """Events in a causal chain should have strictly increasing timestamps."""
        clock_a = LamportClock()
        clock_b = LamportClock()

        ts_a1 = clock_a.tick()               # A does something: ts=1
        ts_a_send = clock_a.tick()           # A sends to B: ts=2
        ts_b_recv = clock_b.update(ts_a_send)  # B receives: ts=3

        assert ts_a1 < ts_a_send < ts_b_recv

    def test_thread_safety(self):
        """Concurrent ticks from multiple threads should not duplicate values."""
        clock   = LamportClock()
        results = []
        lock    = threading.Lock()

        def do_ticks():
            for _ in range(100):
                ts = clock.tick()
                with lock:
                    results.append(ts)

        threads = [threading.Thread(target=do_ticks) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

        # All timestamps should be unique
        assert len(results) == len(set(results))


class TestVectorClock:
    def test_concurrent_detection(self):
        """Two independent writes should be detected as concurrent."""
        vc_a = VectorClock("node_A")
        vc_b = VectorClock("node_B")

        vc_a.increment("node_A")   # A writes
        vc_b.increment("node_B")   # B writes independently

        result = VectorClock.compare(vc_a.clock, vc_b.clock)
        assert result == "concurrent"

    def test_sequential_detection(self):
        """After B receives from A, A should be 'before' B."""
        vc_a = VectorClock("node_A")
        vc_b = VectorClock("node_B")

        vc_a.increment("node_A")
        vc_b.receive(vc_a.to_dict())

        result = VectorClock.compare(vc_a.clock, vc_b.clock)
        assert result == "before"

    def test_equal_clocks(self):
        """Identical vector clocks should be equal."""
        vc_a = VectorClock("node_A")
        vc_b = VectorClock("node_A")   # same owner, same initial state
        assert VectorClock.compare(vc_a.clock, vc_b.clock) == "equal"


class TestNTPClient:
    def test_skew_simulation_positive(self):
        """Positive skew should make synced_time() return a higher value."""
        t_normal = time.time()
        simulate_skew(+100)
        t_skewed = synced_time()
        clear_skew()
        # synced_time should be ~100ms ahead
        assert t_skewed > t_normal + 0.05    # at least 50ms more

    def test_skew_simulation_negative(self):
        """Negative skew should make synced_time() return a lower value."""
        simulate_skew(-100)
        t_skewed = synced_time()
        t_normal = time.time()
        clear_skew()
        assert t_skewed < t_normal

    def test_clear_skew_restores_normal(self):
        """After clearing skew, synced_time should be close to time.time()."""
        simulate_skew(+500)
        clear_skew()
        diff = abs(synced_time() - time.time())
        assert diff < 0.1   # within 100ms (NTP offset may exist)


