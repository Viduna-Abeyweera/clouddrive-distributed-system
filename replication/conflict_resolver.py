# =============================================================================
# replication/conflict_resolver.py
# CloudDrive — Member 2: Data Replication & Consistency
#
# Handles write conflicts using Last-Write-Wins (LWW) strategy.
# Uses Lamport timestamps from Member 3 for ordering.
# Uses vector clocks from Member 3 for detecting true concurrency.
# =============================================================================

from time_sync.vector_clock import VectorClock


def resolve(version_a: dict, version_b: dict) -> dict:
    """
    Resolve a conflict between two versions of the same file.
    Strategy: Last-Write-Wins based on Lamport timestamp.

    If timestamps are equal (extremely rare), use node_id alphabetically
    as a tiebreaker to guarantee all nodes make the same deterministic choice.

    Args:
        version_a: file_data dict from one node
        version_b: file_data dict from another node

    Returns: the winning version dict

    Example:
        winner = resolve(
            {"content": "hello", "timestamp": 5, "author": "node_A"},
            {"content": "world", "timestamp": 7, "author": "node_B"}
        )
        # → version_b wins (higher timestamp)
    """
    ts_a = version_a.get("timestamp", 0)
    ts_b = version_b.get("timestamp", 0)

    if ts_a > ts_b:
        return version_a

    if ts_b > ts_a:
        return version_b

    # Tie: same timestamp — use node_id alphabetically as tiebreaker
    # This is deterministic so all nodes make the same decision
    author_a = version_a.get("author", "")
    author_b = version_b.get("author", "")

    if author_a <= author_b:
        return version_a
    return version_b


def detect_conflict(version_a: dict, version_b: dict) -> str:
    """
    Use vector clocks to classify the relationship between two versions.

    Returns one of:
        "no_conflict"  — one version clearly happened-before the other
        "conflict"     — versions are concurrent (true write conflict)
        "equal"        — identical versions

    This is more precise than LWW timestamp comparison because it can
    distinguish "genuinely concurrent" from "sequential with clock skew".

    Args:
        version_a: file_data dict (must include "vector_clock" key)
        version_b: file_data dict (must include "vector_clock" key)
    """
    vc_a = version_a.get("vector_clock")
    vc_b = version_b.get("vector_clock")

    # If either version lacks a vector clock, fall back to timestamp comparison
    if not vc_a or not vc_b:
        ts_a = version_a.get("timestamp", 0)
        ts_b = version_b.get("timestamp", 0)
        if ts_a == ts_b:
            return "equal"
        return "no_conflict"

    relationship = VectorClock.compare(vc_a, vc_b)

    if relationship == "equal":
        return "equal"
    elif relationship == "concurrent":
        return "conflict"
    else:
        return "no_conflict"   # "before" or "after" = causally ordered


def resolve_list(versions: list) -> dict:
    """
    Resolve a conflict among more than 2 versions (e.g. all 4 nodes have
    different copies of the same file after a partition heals).

    Applies resolve() iteratively to find the single winner.

    Args:
        versions: list of file_data dicts

    Returns: the winning version
    """
    if not versions:
        return None
    winner = versions[0]
    for v in versions[1:]:
        winner = resolve(winner, v)
    return winner
