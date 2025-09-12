import pandas as pd
from .app import _atr, get_param  # adjust import if needed

def strategy_ma(df: pd.DataFrame):
    df = df.copy()
    fast = int(get_param("ma", "fast", "50"))
    slow = int(get_param("ma", "slow", "200"))
    if len(df) < slow + 2: return None, None
    df["fast"] = df["close"].rolling(fast).mean()
    df["slow"] = df["close"].rolling(slow).mean()
    r, p = df.iloc[-1], df.iloc[-2]
    if p.fast <= p.slow and r.fast > r.slow:
        return "LONG", f"SMA crossover fast({fast}) > slow({slow})"
    if p.fast >= p.slow and r.fast < r.slow:
        return "EXIT", f"SMA crossover fast({fast}) < slow({slow})"
    return None, None

def strategy_rsi(df: pd.DataFrame):
    df = df.copy()
    period = int(get_param("rsi", "period", "14"))
    buy_lv = float(get_param("rsi", "buy_level", "45"))
    sell_lv = float(get_param("rsi", "sell_level", "65"))
    if len(df) < period + 2: return None, None
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.rolling(period).mean() / loss.rolling(period).mean()
    rsi = 100 - (100 / (1 + rs))
    df["rsi"] = rsi
    r, p = df.iloc[-1], df.iloc[-2]
    if p.rsi < buy_lv <= r.rsi: return "LONG", f"RSI up-cross {buy_lv}"
    if p.rsi > sell_lv >= r.rsi: return "EXIT", f"RSI down-cross {sell_lv}"
    return None, None

def strategy_scalp(df: pd.DataFrame):
    df = df.copy()
    lb = int(get_param("scalp", "lookback", "20"))
    atr_mult = float(get_param("scalp", "atr_mult", "0.6"))
    vol_mult = float(get_param("scalp", "min_vol_mult", "1.5"))
    if len(df) < max(lb, 20) + 5: return None, None
    df["atr"] = _atr(df, 14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    hi = df["high"].rolling(lb).max()
    lo = df["low"].rolling(lb).min()
    r = df.iloc[-1]
    if r["close"] > hi.iloc[-2] + atr_mult * r["atr"] and r["volume"] > vol_mult * (df["vol_ma"].iloc[-1] + 1e-9):
        return "LONG", f"Breakout > {lb}-bar high with vol surge"
    if r["close"] < lo.iloc[-2] - atr_mult * r["atr"] and r["volume"] > vol_mult * (df["vol_ma"].iloc[-1] + 1e-9):
        return "EXIT", f"Breakdown < {lb}-bar low with vol surge"
    return None, None

def strategy_event(df: pd.DataFrame):
    df = df.copy()
    vol_mult = float(get_param("event", "vol_mult", "2.0"))
    atr_mult = float(get_param("event", "atr_mult", "1.2"))
    if len(df) < 40: return None, None
    atr = _atr(df, 14)
    atr_ma = atr.rolling(20).mean()
    vol_ma = df["volume"].rolling(20).mean()
    r = df.iloc[-1]
    spike_vol = r["volume"] > vol_mult * (vol_ma.iloc[-1] + 1e-9)
    spike_atr = atr.iloc[-1] > atr_mult * (atr_ma.iloc[-1] + 1e-9)
    if spike_vol and spike_atr:
        if r["close"] > r["open"]:
            return "LONG", "Event spike: bullish candle"
        else:
            return "EXIT", "Event spike: bearish candle"
    return None, None
