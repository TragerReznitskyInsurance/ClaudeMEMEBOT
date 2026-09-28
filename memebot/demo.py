"""
Synthetic launch feed for DEMO mode and smoke tests. Every token, wallet and
trade is invented; results on this data say nothing about the real market.
"""
import random
import string

V_SOL0, V_TOK0 = 30.0, 1_073_000_000.0   # pump.fun's initial virtual reserves

ADJ = ["Moon", "Turbo", "Sleepy", "Based", "Tiny", "Giga", "Cosmic", "Wobbly", "Frosty", "Laser",
       "Chunky", "Neon", "Sneaky", "Lucky", "Spicy", "Pixel", "Mega", "Fuzzy", "Hyper", "Golden"]
NOUN = ["Otter", "Toad", "Pengu", "Hamster", "Walrus", "Gecko", "Corgi", "Ferret", "Llama", "Beaver",
        "Moth", "Panda", "Quokka", "Sloth", "Yeti", "Duck", "Badger", "Kiwi", "Axolotl", "Capybara"]


def _token_events(rng: random.Random, t0: float, kind: str):
    def wallet():
        return "".join(rng.choices(string.ascii_letters + string.digits, k=44))

    mint = wallet()[:40] + "pump"
    dev = rng.choice(SERIAL_DEVS) if rng.random() < 0.12 else wallet()
    a, n = rng.choice(ADJ), rng.choice(NOUN)
    symbol = (a[:2] + n[:3]).upper() if rng.random() < 0.6 else n.upper()[:6]
    vs, vt = V_SOL0, V_TOK0
    evs = []

    def trade(ts, side, sol, who):
        nonlocal vs, vt
        k = vs * vt
        if side == "buy":
            vs += sol
            tok = vt - k / vs
            vt -= tok
        else:
            tok = min(sol / (vs / vt), vt * 0.25)
            vt += tok
            new_vs = k / vt
            sol = vs - new_vs
            vs = new_vs
        evs.append({"_ts": ts, "txType": side, "mint": mint, "traderPublicKey": who,
                    "solAmount": round(sol, 6), "tokenAmount": tok,
                    "vSolInBondingCurve": vs, "vTokensInBondingCurve": vt,
                    "marketCapSol": vs / vt * 1e9})

    dev_buy = rng.uniform(0.2, 1.5) if rng.random() > 0.08 else rng.uniform(3.5, 8)
    vs1 = V_SOL0 + dev_buy
    vt1 = V_TOK0 * V_SOL0 / vs1
    evs.append({"_ts": t0, "txType": "create", "mint": mint, "traderPublicKey": dev,
                "name": f"{a} {n}", "symbol": symbol, "solAmount": dev_buy,
                "vSolInBondingCurve": vs1, "vTokensInBondingCurve": vt1, "marketCapSol": vs1 / vt1 * 1e9})
    vs, vt = vs1, vt1
    ts = t0
    buyers = [wallet() for _ in range(70)]
    held = []
    if kind == "dud":
        for _ in range(rng.randint(2, 14)):
            ts += rng.uniform(1, 12)
            trade(ts, "buy" if rng.random() < 0.7 else "sell", rng.uniform(0.05, 0.4), rng.choice(buyers))
    elif kind == "rug":
        for _ in range(rng.randint(35, 55)):
            ts += rng.uniform(0.5, 2.5)
            w = rng.choice(buyers)
            held.append(w)
            trade(ts, "buy", rng.uniform(0.1, 0.6), w)
        ts += rng.uniform(10, 40)
        trade(ts, "sell", 12.0, dev)
        for _ in range(25):
            ts += rng.uniform(0.3, 2)
            trade(ts, "sell", rng.uniform(0.3, 1.0), rng.choice(held))
    elif kind == "runner":
        fade_at = rng.randint(120, 200)
        for i in range(rng.randint(220, 300)):
            ts += rng.uniform(0.6, 3.0)
            w = rng.choice(buyers)
            held.append(w)
            if rng.random() < (0.25 if i < fade_at else 0.62) and held:
                trade(ts, "sell", rng.uniform(0.1, 0.8), rng.choice(held))
            else:
                trade(ts, "buy", rng.uniform(0.1, 0.9), w)
    elif kind == "fader":
        for i in range(rng.randint(90, 140)):
            ts += rng.uniform(0.6, 2.5)
            w = rng.choice(buyers)
            held.append(w)
            if i < 40 or rng.random() < 0.3:
                trade(ts, "buy", rng.uniform(0.1, 0.6), w)
            else:
                trade(ts, "sell", rng.uniform(0.2, 0.8), rng.choice(held))
    elif kind == "surge":   # snipers dump early (bad all-time ratio), then a wave of new buyers
        snipers = [rng.choice(buyers) for _ in range(8)]
        for w in snipers:
            ts += rng.uniform(0.3, 1.2)
            trade(ts, "buy", rng.uniform(0.4, 1.0), w)
        for w in snipers:
            ts += rng.uniform(0.5, 1.5)
            trade(ts, "sell", rng.uniform(0.6, 1.1), w)
        fresh = [b for b in buyers if b not in snipers]
        for i in range(rng.randint(120, 200)):
            ts += rng.uniform(0.5, 1.8)
            if i < 60 or rng.random() < 0.7:
                trade(ts, "buy", rng.uniform(0.1, 0.5), fresh[i % len(fresh)] if i < len(fresh) else rng.choice(fresh))
            else:
                trade(ts, "sell", rng.uniform(0.2, 0.7), rng.choice(fresh))
    elif kind == "whale":   # one wallet does most of the buying
        whale = rng.choice(buyers)
        for _ in range(rng.randint(30, 50)):
            ts += rng.uniform(0.5, 2.5)
            w = whale if rng.random() < 0.3 else rng.choice(buyers)
            trade(ts, "buy", rng.uniform(0.8, 2.0) if w == whale else rng.uniform(0.05, 0.3), w)
    return evs


SERIAL_DEVS = ["Serial1" + "x" * 37, "Serial2" + "y" * 37, "Serial3" + "z" * 37]   # same devs launching over and over
KINDS = ["dud"] * 26 + ["rug"] * 6 + ["runner"] * 5 + ["fader"] * 8 + ["whale"] * 3 + ["surge"] * 5


def generate(t0: float, n_tokens: int, spacing_s: float = 12.0, seed: int | None = None):
    """Returns a time-sorted list of events for n_tokens launches starting at t0."""
    rng = random.Random(seed)
    events = []
    ts = t0
    for _ in range(n_tokens):
        events += _token_events(rng, ts, rng.choice(KINDS))
        ts += rng.uniform(spacing_s * 0.3, spacing_s * 1.7)
    events.sort(key=lambda e: e["_ts"])
    return events
