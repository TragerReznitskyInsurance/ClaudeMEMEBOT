"""
config.yaml holds the defaults (with comments). Changes made in the dashboard are
saved to settings.json next to it and layered on top, so config.yaml stays readable.
"""
import copy
import json
import os

import yaml

SETTINGS_FILE = "settings.json"

# What the dashboard lets you edit. path -> where it lives in the config.
SCHEMA = [
    {"group": "Security gate · checked before a token is watched", "items": [
        {"path": "security.max_dev_buy_pct", "label": "Max dev share of supply", "unit": "%", "type": "float"},
        {"path": "security.max_launches_per_creator_24h", "label": "Max launches per creator / 24h", "type": "int"},
        {"path": "security.block_repeat_dumpers", "label": "Block creators who dumped before", "type": "bool"},
        {"path": "security.rugcheck", "label": "RugCheck screening", "type": "bool"},
        {"path": "security.rugcheck_on_error", "label": "If RugCheck is unreachable", "type": "choice",
         "choices": [["skip", "Skip it (strict)"], ["allow", "Watch anyway"]]},
        {"path": "security.rugcheck_max_score", "label": "Max RugCheck score (0 = off)", "type": "int"},
    ]},
    {"group": "Trigger A · Momentum", "items": [
        {"path": "entry.min_buy_sell_ratio", "label": "Min buy/sell ratio (all time)", "unit": "×", "type": "float"},
        {"path": "entry.min_momentum_pct", "label": "Min price rise", "unit": "%", "type": "float"},
        {"path": "entry.momentum_lookback_s", "label": "Over the last", "unit": "s", "type": "float"},
    ]},
    {"group": "Safety filters · always required", "items": [
        {"path": "entry.min_unique_buyers", "label": "Min unique buyers", "type": "int"},
        {"path": "entry.min_buy_count", "label": "Min number of buys", "type": "int"},
        {"path": "entry.min_buy_volume_sol", "label": "Min buy volume", "unit": "SOL", "type": "float"},

        {"path": "entry.min_mcap_sol", "label": "Min market cap", "unit": "SOL", "type": "float"},
        {"path": "entry.max_mcap_sol", "label": "Max market cap", "unit": "SOL", "type": "float"},
        {"path": "entry.max_top_buyer_share_pct", "label": "Max share for one wallet", "unit": "%", "type": "float"},

        {"path": "entry.max_drawdown_from_peak_pct", "label": "Max drop from peak", "unit": "%", "type": "float"},
        {"path": "watch.min_age_s", "label": "Don't buy before age", "unit": "s", "type": "float"},
        {"path": "watch.max_watch_s", "label": "Stop watching after", "unit": "s", "type": "float"},
    ]},
    {"group": "Trigger B · Buyer surge", "items": [
        {"path": "entry.buyer_surge.enabled", "label": "Buyer-surge trigger", "type": "bool"},
        {"path": "entry.buyer_surge.window_s", "label": "Surge window", "unit": "s", "type": "float"},
        {"path": "entry.buyer_surge.min_new_buyers", "label": "Min new buyers in window", "type": "int"},
        {"path": "entry.buyer_surge.min_recent_buy_sell_ratio", "label": "Min buy/sell in window", "unit": "×", "type": "float"},
        {"path": "entry.buyer_surge.min_momentum_pct", "label": "Min price rise in window", "unit": "%", "type": "float"},
    ]},
    {"group": "Exits", "items": [
        {"path": "exit.stop_loss_pct", "label": "Stop loss", "unit": "%", "type": "float"},
        {"path": "exit.take_profit.0.gain_pct", "label": "Take profit 1 at", "unit": "%", "type": "float"},
        {"path": "exit.take_profit.0.sell_pct", "label": "Take profit 1 sells", "unit": "%", "type": "float"},
        {"path": "exit.take_profit.1.gain_pct", "label": "Take profit 2 at", "unit": "%", "type": "float"},
        {"path": "exit.take_profit.1.sell_pct", "label": "Take profit 2 sells", "unit": "%", "type": "float"},
        {"path": "exit.trailing_stop_pct", "label": "Trailing stop", "unit": "%", "type": "float"},
        {"path": "exit.trailing_arms_after_gain_pct", "label": "Trailing arms after gain", "unit": "%", "type": "float"},
        {"path": "exit.max_hold_s", "label": "Max hold time", "unit": "s", "type": "float"},
        {"path": "exit.stale_after_s", "label": "Exit if no trades for", "unit": "s", "type": "float"},
    ]},
    {"group": "Sizing & risk", "items": [
        {"path": "execution.position_size_sol", "label": "Position size", "unit": "SOL", "type": "float"},
        {"path": "risk.max_open_positions", "label": "Max open positions", "type": "int"},
        {"path": "risk.daily_loss_limit_sol", "label": "Daily loss limit", "unit": "SOL", "type": "float"},
        {"path": "risk.starting_balance_sol", "label": "Starting paper balance", "unit": "SOL", "type": "float",
         "restart": True},
    ]},
    {"group": "Simulated execution", "items": [
        {"path": "execution.latency_s", "label": "Fill latency", "unit": "s", "type": "float"},
        {"path": "execution.platform_fee_pct", "label": "Platform fee per side", "unit": "%", "type": "float"},
        {"path": "execution.extra_slippage_pct", "label": "Extra slippage", "unit": "%", "type": "float"},
        {"path": "execution.priority_fee_sol", "label": "Priority fee per tx", "unit": "SOL", "type": "float"},
    ]},
    {"group": "Data feed", "items": [
        {"path": "universe", "label": "Tokens to watch", "type": "choice",
         "choices": [["new_tokens", "New launches"], ["migrated", "Graduated only (cheaper)"]], "restart": True},
        {"path": "max_trade_messages_per_day", "label": "Daily data budget", "unit": "msgs", "type": "int"},
        {"path": "max_concurrent_watch", "label": "Max tokens watched at once", "type": "int"},
    ]},
]


def _walk(cfg, path):
    parts = path.split(".")
    node = cfg
    for p in parts[:-1]:
        node = node[int(p)] if isinstance(node, list) else node[p]
    last = parts[-1]
    return node, (int(last) if isinstance(node, list) else last)


def get_path(cfg, path):
    node, k = _walk(cfg, path)
    return node[k]


def set_path(cfg, path, value):
    node, k = _walk(cfg, path)
    node[k] = value


def _settings_path(config_path):
    return os.path.join(os.path.dirname(os.path.abspath(config_path)), SETTINGS_FILE)


def load_overrides(config_path="config.yaml") -> dict:
    try:
        with open(_settings_path(config_path)) as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_config(config_path="config.yaml") -> dict:
    with open(config_path) as fh:
        cfg = yaml.safe_load(fh)
    ov = load_overrides(config_path)
    for path, val in ov.get("values", {}).items():
        try:
            set_path(cfg, path, val)
        except (KeyError, IndexError, ValueError):
            pass
    if ov.get("api_key"):
        cfg["feed"]["api_key"] = ov["api_key"]
    return cfg


def save_overrides(values: dict, api_key=None, config_path="config.yaml", helius_key=None):
    ov = load_overrides(config_path)
    ov.setdefault("values", {}).update(values)
    if api_key is not None:
        ov["api_key"] = api_key
    if helius_key is not None:
        ov["helius_key"] = helius_key
    with open(_settings_path(config_path), "w") as fh:
        json.dump(ov, fh, indent=2)


def reset_overrides(config_path="config.yaml"):
    """Reset strategy values; keep API keys."""
    p = _settings_path(config_path)
    if os.path.exists(p):
        ov = load_overrides(config_path)
        keep = {k: ov[k] for k in ("api_key", "helius_key") if ov.get(k)}
        with open(p, "w") as fh:
            json.dump(keep, fh, indent=2)


def helius_key(config_path="config.yaml") -> str:
    return os.environ.get("HELIUS_API_KEY") or load_overrides(config_path).get("helius_key") or ""


def coerce(item, raw):
    if item["type"] == "bool":
        return raw in (True, "true", "True", "1", 1, "on")
    if item["type"] == "int":
        return int(float(raw))
    if item["type"] == "float":
        return float(raw)
    return str(raw)


def schema_with_values(cfg):
    out = copy.deepcopy(SCHEMA)
    for g in out:
        for it in g["items"]:
            it["value"] = get_path(cfg, it["path"])
    return out


# display order: security → safety → trigger A → trigger B → the rest
_ORDER = ["Security gate", "Safety filters", "Trigger A", "Trigger B"]
SCHEMA.sort(key=lambda g: next((i for i, k in enumerate(_ORDER) if g["group"].startswith(k)), len(_ORDER)))
