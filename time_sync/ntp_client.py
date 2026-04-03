# time_sync/ntp_client.py
# Provides NTP-based physical clock synchronization.
# Also provides clock skew simulation for testing and report demonstrations.

import time
import threading

try:
    import ntplib
    NTP_AVAILABLE = True
except ImportError:
    NTP_AVAILABLE = False
    print("[NTP] ntplib not installed. Run: pip install ntplib")


# Global artificial skew offset applied to synced_time() — used in tests
_skew_offset_sec = 0.0
_skew_lock       = threading.Lock()


# Core NTP functions

def get_ntp_offset(server: str = "pool.ntp.org") -> float:
    """
    Query an NTP server and return the clock offset in seconds.
    A positive offset means your clock is behind (too slow).
    A negative offset means your clock is ahead (too fast).

    Returns 0.0 if NTP is unavailable or the query fails.
    """
    if not NTP_AVAILABLE:
        return 0.0
    try:
        client   = ntplib.NTPClient()
        response = client.request(server, version=3)
        return response.offset          # seconds your clock is off by
    except Exception as e:
        print(f"[NTP] Query failed ({server}): {e}")
        return 0.0


def synced_time() -> float:
    """
    Return the current time corrected by the NTP offset AND any
    artificial skew applied for testing.

    Use this everywhere instead of time.time() for consistent ordering.
    """
    ntp_correction = get_ntp_offset()
    with _skew_lock:
        skew = _skew_offset_sec
    return time.time() + ntp_correction + skew


def get_ntp_details(server: str = "pool.ntp.org") -> dict:
    """
    Return detailed NTP response information for the report.
    Includes offset, delay, and reference time.
    """
    if not NTP_AVAILABLE:
        return {"error": "ntplib not installed"}
    try:
        client   = ntplib.NTPClient()
        response = client.request(server, version=3)
        return {
            "server":         server,
            "offset_sec":     round(response.offset, 6),
            "offset_ms":      round(response.offset * 1000, 3),
            "round_trip_ms":  round(response.delay  * 1000, 3),
            "ref_time":       response.ref_time,
            "stratum":        response.stratum,
            "tx_time":        response.tx_time,
        }
    except Exception as e:
        return {"error": str(e)}


# Clock skew simulation (for testing and report demos)

def simulate_skew(offset_ms: float):
    """
    Artificially offset this node's clock by offset_ms milliseconds.
    Positive = clock is ahead. Negative = clock is behind.

    Use this in tests to demonstrate that Lamport clocks correctly
    order events even when physical clocks are skewed.

    Example:
        simulate_skew(+50)   # this node thinks it's 50ms ahead
        simulate_skew(-30)   # this node thinks it's 30ms behind
        simulate_skew(0)     # clear the skew
    """
    global _skew_offset_sec
    with _skew_lock:
        _skew_offset_sec = offset_ms / 1000.0
    print(f"[NTP] Clock skew set to {offset_ms:+.1f} ms")


def clear_skew():
    """Remove any artificial clock skew."""
    simulate_skew(0.0)

# Analysis and reporting

def measure_offset_multiple(server: str = "pool.ntp.org",
                             samples: int = 5) -> dict:
    """
    Query NTP multiple times and compute statistics.
    Useful for the report — shows variance in NTP measurements.
    """
    if not NTP_AVAILABLE:
        return {"error": "ntplib not installed"}

    offsets = []
    for i in range(samples):
        offset = get_ntp_offset(server)
        offsets.append(offset)
        time.sleep(0.5)

    if not offsets:
        return {"error": "no measurements"}

    avg    = sum(offsets) / len(offsets)
    mn     = min(offsets)
    mx     = max(offsets)
    spread = mx - mn

    return {
        "server":      server,
        "samples":     samples,
        "offsets_ms":  [round(o * 1000, 3) for o in offsets],
        "avg_ms":      round(avg    * 1000, 3),
        "min_ms":      round(mn     * 1000, 3),
        "max_ms":      round(mx     * 1000, 3),
        "spread_ms":   round(spread * 1000, 3),
    }


def log_skew_analysis(node_skews: dict):
    """
    Print a formatted table of node clock offsets.
    Call this during your demo to show the panel clock skew effects.

    Args:
        node_skews: dict mapping node_id → artificial skew in ms
                    e.g. {"node_A": 0, "node_B": +50, "node_C": -30, "node_D": +10}
    """
    print("\n" + "=" * 55)
    print("  Clock Skew Analysis")
    print("=" * 55)
    print(f"  {'Node':<12} {'Skew (ms)':>12} {'Status'}")
    print(f"  {'-'*12} {'-'*12} {'-'*20}")
    for node_id, skew_ms in node_skews.items():
        status = "SYNCED" if abs(skew_ms) < 5 else "SKEWED"
        print(f"  {node_id:<12} {skew_ms:>+12.1f} {status}")
    print("=" * 55 + "\n")


# Background NTP sync thread
class NTPSyncService:
    """
    Periodically re-queries NTP and logs the offset.
    Run this as a background thread on each node for continuous sync.
    """

    def __init__(self, node_id: str, server: str = "pool.ntp.org",
                 interval_sec: int = 60):
        self.node_id      = node_id
        self.server       = server
        self.interval_sec = interval_sec
        self.last_offset  = 0.0
        self._thread      = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        print(f"[NTP] Sync service started for {self.node_id} "
              f"(interval={self.interval_sec}s)")

    def _run(self):
        while True:
            offset = get_ntp_offset(self.server)
            self.last_offset = offset
            if abs(offset * 1000) > 10:      # warn if > 10ms off
                print(f"[NTP] {self.node_id} clock offset = {offset*1000:+.2f} ms  [WARNING: >10ms]")
            else:
                print(f"[NTP] {self.node_id} clock offset = {offset*1000:+.2f} ms  [OK]")
            time.sleep(self.interval_sec)



# Demo — run directly to show NTP query + skew simulation
if __name__ == "__main__":
    print("=" * 55)
    print("NTP Client Demo")
    print("=" * 55)

    print("\n1. Querying NTP server...")
    details = get_ntp_details()
    for k, v in details.items():
        print(f"   {k:<20} {v}")

    print("\n2. Multiple sample measurement...")
    stats = measure_offset_multiple(samples=3)
    for k, v in stats.items():
        print(f"   {k:<20} {v}")

    print("\n3. Demonstrating clock skew simulation...")
    print(f"   Normal time:        {time.time():.6f}")
    simulate_skew(+50)
    print(f"   After +50ms skew:   {synced_time():.6f}")
    simulate_skew(-30)
    print(f"   After -30ms skew:   {synced_time():.6f}")
    clear_skew()
    print(f"   After clearing:     {synced_time():.6f}")

    print("\n4. Skew analysis table for report demo...")
    log_skew_analysis({
        "node_A":  0.0,
        "node_B": +50.0,
        "node_C": -30.0,
        "node_D": +10.0,
    })
