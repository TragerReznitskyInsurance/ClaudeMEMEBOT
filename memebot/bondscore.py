"""
Chance a pump.fun coin still on the bonding curve bonds (reaches ~400 SOL / migrates) within the next 6 hours.

Built 11 Oct from 7 days of pump.fun Explore snapshots (2,302 observations of coins 15+ min old at 30-375 SOL;
label = seen complete or at 400+ SOL within 6 h). Market cap is by far the strongest factor, age second;
how close the coin is to its all-time high made little difference to the bond chance itself (it matters for
how much our trades make, not for whether the coin bonds). Holdout check (first half -> second half) was
within a few points per band. A rough guide, not a promise.
"""
from __future__ import annotations

# (mcap low SOL, mcap high SOL) -> chance for age 15 min-2 h, 2-24 h, 1 day+
TABLE = [
    ((30, 60), (0.012, 0.014, 0.009)),
    ((60, 90), (0.051, 0.021, 0.044)),
    ((90, 130), (0.108, 0.032, 0.029)),
    ((130, 180), (0.206, 0.138, 0.091)),
    ((180, 250), (0.354, 0.308, 0.281)),
    ((250, 375), (0.537, 0.500, 0.370)),
]
# what a loser costs us on average (hold tests: 6 h time limit -14%, -50% stops -58%)
AVG_LOSS = 0.32


def bond_chance(mcap_sol, age_min):
    """0-1, or None when it doesn't apply (graduated / unknown)."""
    if mcap_sol is None or age_min is None or mcap_sol >= 400:
        return None
    if mcap_sol >= 375:
        return 0.75
    col = 0 if age_min < 120 else 1 if age_min < 1440 else 2
    for (lo, hi), p in TABLE:
        if lo <= mcap_sol < hi:
            return p[col]
    return 0.005                                            # under 30 SOL: launch level


def breakeven_chance(mcap_sol, exit_sol=375.0):
    """Bond chance needed for a hold-to-bonding buy at this mcap to break even (payout vs average loser)."""
    if not mcap_sol or mcap_sol >= exit_sol:
        return None
    gain = exit_sol / mcap_sol - 1
    return AVG_LOSS / (gain + AVG_LOSS)
