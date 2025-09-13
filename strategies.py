# strategies.py
import pandas as pd
import numpy as np

# ---- parameter access is injected by app.py ----
_PARAM_GETTER = None

def set_param_getter(fn):
    """app.py will call this to let strategies read saved params."""
    global _PARAM_GETTER
    _PARAM_GETTER = fn

def _get(strategy: str, key: str, default: str):
    if _PARAM_GETTER is None:
        return default
    try:
        return _PARAM_GETTER(strategy, key, default)
    except Exception:
        return default

def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Local ATR so we don't import app.py."""
    if df.empty:
        return pd.Series(dtype=float)
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def strategy_ma(df: pd.DataFrame):
    df = df.copy()
    fast = int(_get("ma", "fast", "50"))
    slow = int(_get("ma", "slow", "200"))
    if len(df) < slow + 2:
        return None, None
    df["fast"] = df["close"].rolling(fast).mean()
    df["slow"] = df["close"].rolling(slow).mean()
    r, p = df.iloc[-1], df.iloc[-2]
    if p.fast <= p.slow and r.fast > r.slow:
        return "LONG", f"SMA crossover fast({fast})>slow({slow})"
    if p.fast >= p.slow and r.fast < r.slow:
        return "EXIT", f"SMA crossover fast({fast})<slow({slow})"
    return None, None

def strategy_rsi(df: pd.DataFrame):
    df = df.copy()
    period = int(_get("rsi", "period", "14"))
    buy_lv = float(_get("rsi", "buy_level", "45"))
    sell_lv = float(_get("rsi", "sell_level", "65"))
    if len(df) < period + 2:
        return None, None
    d = df["close"].diff()
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    ru = pd.Series(up).rolling(period).mean()
    rd = pd.Series(dn).rolling(period).mean()
    rs = ru / (rd + 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    df["rsi"] = rsi
    r, p = df.iloc[-1], df.iloc[-2]
    if p.rsi < buy_lv <= r.rsi:
        return "LONG", f"RSI up-cross {buy_lv}"
    if p.rsi > sell_lv >= r.rsi:
        return "EXIT", f"RSI down-cross {sell_lv}"
    return None, None

def strategy_scalp(df: pd.DataFrame):
    df = df.copy()
    lb = int(_get("scalp", "lookback", "20"))
    atr_mult = float(_get("scalp", "atr_mult", "0.6"))
    vol_mult = float(_get("scalp", "min_vol_mult", "1.5"))
    if len(df) < max(lb, 20) + 5:
        return None, None
    df["atr"] = _atr(df, 14).fillna(0.0)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    hi = df["high"].rolling(lb).max()
    lo = df["low"].rolling(lb).min()
    r = df.iloc[-1]
    if r["close"] > hi.iloc[-2] + atr_mult * r["atr"] and r["volume"] > vol_mult * (df["vol_ma"].iloc[-1] + 1e-9):
        return "LONG", f"Breakout>{lb}-bar high with ATR buffer & vol surge"
    if r["close"] < lo.iloc[-2] - atr_mult * r["atr"] and r["volume"] > vol_mult * (df["vol_ma"].iloc[-1] + 1e-9):
        return "EXIT", f"Breakdown<{lb}-bar low with ATR buffer & vol surge"
    return None, None

def strategy_event(df: pd.DataFrame):
    df = df.copy()
    vol_mult = float(_get("event", "vol_mult", "2.0"))
    atr_mult = float(_get("event", "atr_mult", "1.2"))
    if len(df) < 40:
        return None, None
    atr = _atr(df, 14)
    atr_ma = atr.rolling(20).mean()
    vol_ma = df["volume"].rolling(20).mean()
    r = df.iloc[-1]
    spike_vol = r["volume"] > vol_mult * (vol_ma.iloc[-1] + 1e-9)
    spike_atr = (atr.iloc[-1] > atr_mult * (atr_ma.iloc[-1] + 1e-9))
    if spike_vol and spike_atr:
        if r["close"] > r["open"]:
            return "LONG", "Event spike: volume & volatility up (bullish candle)"
        else:
            return "EXIT", "Event spike: volume & volatility up (bearish candle)"
    return None, None
