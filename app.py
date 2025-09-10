import os
import io
import time
import math
import asyncio
import logging
import sqlite3
import traceback
from statistics import median
from datetime import datetime, timezone
from contextlib import asynccontextmanager

# Matplotlib headless
os.environ.setdefault("MPLBACKEND", "Agg")

from dotenv import load_dotenv
load_dotenv()

# Third-party
import ccxt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import httpx

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import uvicorn

from telegram import Update, LabeledPrice, InputMediaPhoto
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PreCheckoutQueryHandler, MessageHandler, filters
)
from telegram.error import BadRequest

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EXCHANGE  = os.getenv("EXCHANGE", "kucoin")      # primary exchange for OHLCV
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
PAIR      = os.getenv("PAIR", "BTC/USDT")        # legacy single
PAIRS_ENV = os.getenv("PAIRS", PAIR)
PAIRS     = [p.strip() for p in PAIRS_ENV.split(",") if p.strip()]

EXCHANGES_ENV = os.getenv("EXCHANGES", EXCHANGE) # for live consensus
EX_LIST = [e.strip() for e in EXCHANGES_ENV.split(",") if e.strip()]

DISPLAY_TZ = os.getenv("DISPLAY_TZ", "UTC")

STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "10000"))  # ≈$10
PUBLIC_URL = os.getenv("PUBLIC_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "5467277042"))

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

# -----------------------------
# SQLITE
# -----------------------------
DB_PATH = "subs.sqlite"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  expires_at INTEGER NOT NULL DEFAULT 0,
  signals_on INTEGER NOT NULL DEFAULT 1
)""")
# Pairs table for runtime add/remove
cur.execute("""
CREATE TABLE IF NOT EXISTS pairs (
  symbol TEXT PRIMARY KEY
)""")
conn.commit()

def now_ts() -> int:
    return int(time.time())

def is_active(uid: int) -> bool:
    row = cur.execute("SELECT expires_at FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row and row[0] > now_ts())

def set_expiry(uid: int, expires_at: int):
    cur.execute("""
        INSERT INTO users(user_id, expires_at)
        VALUES(?, ?)
        ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at
    """, (uid, expires_at))
    conn.commit()

def set_opt(uid: int, on: bool):
    cur.execute("""
        INSERT INTO users(user_id, signals_on)
        VALUES(?, ?)
        ON CONFLICT(user_id) DO UPDATE SET signals_on=excluded.signals_on
    """, (uid, 1 if on else 0))
    conn.commit()

def active_users():
    return cur.execute(
        "SELECT user_id FROM users WHERE expires_at > ? AND signals_on = 1",
        (now_ts(),)
    ).fetchall()

# Pairs helpers
def db_get_pairs():
    rows = cur.execute("SELECT symbol FROM pairs ORDER BY symbol").fetchall()
    return [r[0] for r in rows]

def db_add_pair(sym: str):
    cur.execute("INSERT OR IGNORE INTO pairs(symbol) VALUES(?)", (sym,))
    conn.commit()

def db_remove_pair(sym: str):
    cur.execute("DELETE FROM pairs WHERE symbol=?", (sym,))
    conn.commit()

# Seed from env if empty
if not db_get_pairs():
    for p in PAIRS:
        db_add_pair(p)

# -----------------------------
# Uphold: quotes-only client (for consensus)
# -----------------------------
class UpholdClient:
    BASE = "https://api.uphold.com/v0"
    def __init__(self, timeout=8.0):
        self.timeout = timeout
    def map_pair(self, ccxt_pair: str) -> list[str]:
        try:
            base, quote = ccxt_pair.split("/")
        except ValueError:
            return []
        cands = []
        if quote.upper() == "USDT":
            cands += [f"{base}-USD", f"{base}-USDT"]
        else:
            cands.append(f"{base}-{quote}")
        return cands
    def _parse_ticker(self, data):
        if not isinstance(data, dict):
            return None
        for k in ("last", "price"):
            v = data.get(k)
            if v is not None:
                try: return float(v)
                except Exception: pass
        bid = data.get("bid"); ask = data.get("ask")
        try:
            bid = float(bid) if bid is not None else None
            ask = float(ask) if ask is not None else None
        except Exception:
            bid = ask = None
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
        for v in data.values():
            try: return float(v)
            except Exception: continue
        return None
    async def fetch_price(self, pair_ccxt: str) -> float | None:
        cands = self.map_pair(pair_ccxt)
        if not cands:
            return None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for sym in cands:
                try:
                    r = await client.get(f"{self.BASE}/ticker/{sym}")
                    if r.status_code != 200:
                        continue
                    js = r.json()
                    if isinstance(js, list) and js:
                        js = js[0]
                    px = self._parse_ticker(js)
                    if px and math.isfinite(px):
                        return float(px)
                except Exception:
                    continue
        return None

# -----------------------------
# Multi-exchange consensus price
# -----------------------------
def _mad(values):
    m = median(values)
    return median([abs(v - m) for v in values]) or 1e-9

class MultiPrice:
    def __init__(self, exchange_ids: list[str]):
        self.exes = []
        self.want_uphold = False
        for ex in exchange_ids:
            if ex.lower() == "uphold":
                self.want_uphold = True
            else:
                try:
                    self.exes.append(getattr(ccxt, ex)({"enableRateLimit": True}))
                except Exception:
                    pass
        self.uphold = UpholdClient() if self.want_uphold else None

    async def get_consensus(self, pair: str):
        async def one(ex):
            def _t():
                try:
                    t = ex.fetch_ticker(pair)
                    for key in ("last", "close", "bid", "ask"):
                        if key in t and t[key]:
                            return float(t[key])
                except Exception:
                    pass
                try:
                    o = ex.fetch_ohlcv(pair, timeframe="1m", limit=1)
                    return float(o[-1][4]) if o else None
                except Exception:
                    return None
            return await asyncio.to_thread(_t)

        tasks = [one(ex) for ex in self.exes]
        if self.uphold is not None:
            async def uphold_task():
                try: return await self.uphold.fetch_price(pair)
                except Exception: return None
            tasks.append(uphold_task())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        raw, per_ex = [], {}
        # map ccxt ones
        for ex, val in zip(self.exes, results[:len(self.exes)]):
            if isinstance(val, Exception) or val is None or not math.isfinite(val):
                continue
            raw.append(val); per_ex[ex.id] = val
        # uphold at end
        if self.uphold is not None:
            uphold_val = results[-1]
            if not isinstance(uphold_val, Exception) and uphold_val is not None and math.isfinite(uphold_val):
                raw.append(float(uphold_val)); per_ex["uphold"] = float(uphold_val)

        if not raw:
            return None, 0, None, per_ex

        m = median(raw)
        mad = _mad(raw)
        kept = [v for v in raw if abs(v - m) <= 3 * mad] or raw
        cons = median(kept)
        spread = max(kept) - min(kept) if len(kept) > 1 else 0.0
        return cons, len(kept), spread, per_ex

price_agg = MultiPrice(EX_LIST)

# -----------------------------
# Predictor helpers
# -----------------------------
def _sigmoid(x: float) -> float:
    try:
        if x >= 0:
            z = math.exp(-x); return 1.0 / (1.0 + z)
        else:
            z = math.exp(x);  return z / (1.0 + z)
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def _zscore(series: pd.Series) -> pd.Series:
    m = series.rolling(200).mean()
    s = series.rolling(200).std(ddof=0).replace(0, 1e-9)
    return (series - m) / s

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _macd(close: pd.Series):
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    macd = ema12 - ema26
    signal = _ema(macd, 9)
    hist = macd - signal
    return macd, signal, hist

def _atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# -----------------------------
# Strategy Engine
# -----------------------------
class Engine:
    def __init__(self, exchange_id, timeframe, requested_pairs):
        self.ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.tf = timeframe
        self.requested_pairs = requested_pairs
        self.valid_pairs = []
        self.last = {}  # last state per pair

    async def init_markets(self):
        def _load():
            self.ex.load_markets()
            return set(self.ex.symbols or [])
        try:
            symbols = await asyncio.to_thread(_load)
        except Exception as e:
            logging.warning("Could not load markets for %s: %s", self.ex.id, e)
            self.valid_pairs = list(self.requested_pairs)
            return
        wanted = set(self.requested_pairs)
        self.valid_pairs = sorted(list(wanted & symbols))
        skipped = sorted(list(wanted - symbols))
        if skipped:
            logging.info("Skipping unsupported on %s: %s", self.ex.id, ", ".join(skipped))
        if not self.valid_pairs:
            logging.warning("No valid pairs found on %s from requested: %s", self.ex.id, ", ".join(self.requested_pairs))

    async def fetch_df(self, pair, limit=300, timeframe: str | None = None):
        tf = timeframe or self.tf
        def get_ohlcv():
            return self.ex.fetch_ohlcv(pair, timeframe=tf, limit=limit)
        try:
            ohlcv = await asyncio.to_thread(get_ohlcv)
        except Exception as e:
            logging.warning("fetch_ohlcv failed for %s: %s", pair, e)
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
        if not ohlcv or len(ohlcv) < 210:
            logging.info("[%s] Not enough candles: %s", pair, len(ohlcv) if ohlcv else 0)
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df

    @staticmethod
    def rsi(series, period=14):
        delta = series.diff()
        up = np.where(delta > 0, delta, 0.0)
        down = np.where(delta < 0, -delta, 0.0)
        roll_up = pd.Series(up).rolling(period).mean()
        roll_down = pd.Series(down).rolling(period).mean()
        rs = roll_up / (roll_down + 1e-9)
        return 100.0 - (100.0 / (1.0 + rs))

    async def analyze(self, pair):
        df = await self.fetch_df(pair)
        if df.empty or len(df) < 200:
            return None, df
        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["rsi"] = self.rsi(df["close"], 14)
        row, prev = df.iloc[-1], df.iloc[-2]
        long_cond = (df["sma50"].iloc[-1] > df["sma200"].iloc[-1]) and (prev.rsi < 45 <= row.rsi)
        exit_cond = (row.rsi > 65) or (df["sma50"].iloc[-1] < df["sma200"].iloc[-1])
        price = row["close"]
        ts = row["time"].strftime("%Y-%m-%d %H:%M UTC")
        last_state = self.last.get(pair)
        if long_cond and last_state != "LONG":
            self.last[pair] = "LONG"
            text = f"LONG {pair} @ {price:.2f} [{self.tf}]  ({ts})\nSMA50>SMA200; RSI up-cross from <45."
            return ("LONG", text, price, ts), df
        if exit_cond and last_state != "EXIT":
            self.last[pair] = "EXIT"
            text = f"EXIT/NEUTRAL {pair} @ {price:.2f} [{self.tf}]  ({ts})\nRSI>65 or SMA50<SMA200."
            return ("EXIT", text, price, ts), df
        return None, df

    async def predict(self, pair: str, horizon: int = 5, timeframe: str | None = None):
        tf = timeframe or self.tf
        df = await self.fetch_df(pair, timeframe=tf)
        if df.empty or len(df) < 220:
            return None, None, "Not enough data", df
        df["ret1"]   = df["close"].pct_change()
        df["ret5"]   = df["close"].pct_change(5)
        df["sma20"]  = df["close"].rolling(20).mean()
        df["sma50"]  = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["slope20"]  = df["sma20"].diff()
        df["slope50"]  = df["sma50"].diff()
        df["slope200"] = df["sma200"].diff()
        macd, sig, hist = _macd(df["close"])
        df["macd_hist"] = hist
        df["atr14"] = _atr(df, 14).fillna(0.0)
        df["rsi"] = self.rsi(df["close"], 14)

        mom_z   = _zscore(df["ret5"]).iloc[-1]
        hist_z  = _zscore(df["macd_hist"]).iloc[-1]
        rsi_d   = (df["rsi"].iloc[-1] - 50.0) / 50.0
        slope_z = _zscore(df["slope20"]).iloc[-1]
        regime  = 1.0 if df["sma50"].iloc[-1] > df["sma200"].iloc[-1] else -1.0
        vol_k   = (df["atr14"].iloc[-1] / (df["close"].iloc[-1] + 1e-9))
        vol_clamped = max(0.05, min(vol_k, 0.20))

        score = (
            0.70 * mom_z +
            0.60 * hist_z +
            0.50 * rsi_d +
            0.40 * slope_z +
            0.35 * regime -
            0.25 * (vol_clamped - 0.10)
        )
        prob_up = _sigmoid(score)
        if prob_up >= 0.60: label = "BUY"
        elif prob_up <= 0.40: label = "SELL"
        else: label = "HOLD"
        explanation = (
            f"mom_z={mom_z:.2f}; macd_z={hist_z:.2f}; rsiΔ={rsi_d:.2f}; "
            f"slope_z={slope_z:.2f}; regime={'bull' if regime>0 else 'bear'}; "
            f"vol={vol_k:.3f}; horizon={horizon} bars"
        )
        return (label, float(prob_up), explanation, df)

# -----------------------------
# Charts (LIVE dot w/ price label)
# -----------------------------
def plot_signal_chart(pair, df, mark, price, title_tf=None, last_tick=None):
    dfp = df.tail(200).copy()
    if dfp.empty: return None
    try:
        dfp["time"] = dfp["time"].dt.tz_convert(DISPLAY_TZ)
    except Exception:
        pass
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=140)
    ax.plot(dfp["time"], dfp["close"], linewidth=1.2, label="Close")
    if "sma50" not in dfp:  dfp["sma50"]  = dfp["close"].rolling(50).mean()
    if "sma200" not in dfp: dfp["sma200"] = dfp["close"].rolling(200).mean()
    ax.plot(dfp["time"], dfp["sma50"], linewidth=1.0, label="SMA50")
    ax.plot(dfp["time"], dfp["sma200"], linewidth=1.0, label="SMA200")

    if mark and price:
        x_sig = dfp["time"].iloc[-1]; y_sig = price
        label = "BUY" if mark == "LONG" else "SELL"
        ax.scatter([x_sig],[y_sig], s=50)
        ax.annotate(label, (x_sig,y_sig), xytext=(10,10), textcoords="offset points")

    try:
        x_last = dfp["time"].iloc[-1]
        ax.axvline(x_last, linewidth=0.7)
    except Exception:
        x_last = None

    if last_tick and x_last is not None:
        try: x_live = x_last + pd.Timedelta(seconds=2)
        except Exception: x_live = x_last
        ax.scatter([x_live],[last_tick], s=65, zorder=5)
        ax.annotate(
            f"{last_tick:.2f}", (x_live,last_tick),
            xytext=(0,12), textcoords="offset points",
            ha="center", fontsize=8, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.2", fc="yellow", alpha=0.7, lw=0)
        )
        try:
            last_close = float(dfp["close"].iloc[-1])
            ax.plot([x_last,x_live],[last_close,last_tick], linewidth=0.8)
        except Exception:
            pass

    ax.set_title(f"{pair} — {title_tf or TIMEFRAME}  ({DISPLAY_TZ})")
    ax.set_xlabel("Time"); ax.set_ylabel("Price")
    ax.legend(loc="best"); ax.grid(True, linewidth=0.3)
    buf = io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf

# -----------------------------
# Telegram helpers
# -----------------------------
def tz_label_from_ts(ts, fallback: str) -> str:
    try:
        return getattr(getattr(ts, "tz", None), "key", None) or ts.tzname() or fallback
    except Exception:
        return fallback

async def broadcast(text: str, app: "Application"):
    for (uid,) in active_users():
        try: await app.bot.send_message(uid, text)
        except Exception as e: logging.warning(f"send fail {uid}: {e}")

def _format_pairs_list(pairs: list[str], max_show: int = 40) -> str:
    """Turn a list like ['BTC/USDT', 'ETH/USDT', ...] into wrapped lines for /help."""
    if not pairs:
        return "(none)"
    shown = pairs[:max_show]
    more = len(pairs) - len(shown)
    lines = []
    line = []
    for i, sym in enumerate(shown, 1):
        line.append(sym)
        if (i % 8) == 0:  # wrap every 8 symbols for readability
            lines.append(", ".join(line)); line = []
    if line:
        lines.append(", ".join(line))
    if more > 0:
        lines.append(f"... and {more} more")
    return "\n".join(lines)

async def broadcast_chart(app: "Application", caption: str, png_buf: io.BytesIO):
    for (uid,) in active_users():
        try:
            await app.bot.send_photo(uid, png_buf, caption=caption)
            png_buf.seek(0)
        except Exception as e:
            logging.warning(f"send photo fail {uid}: {e}")

# -----------------------------
# Commands
# -----------------------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if is_active(update.effective_user.id):
        await update.message.reply_text("You’re active. /signalson to receive alerts. /status shows expiry.")
    else:
        await update.message.reply_text("Access requires a sub. Tap /subscribe to pay with Telegram Stars (30 days).")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🛠 Commands\n"
        "/start – Welcome\n"
        "/subscribe – Buy 30-day access (Stars)\n"
        "/status – Check your expiry\n"
        "/cancel – Stop auto-renew\n"
        "/signalson – Enable alerts\n"
        "/signalsoff – Pause alerts\n"
        "/chart [PAIR] [TF] – Snapshot chart\n"
        "/live [PAIR] [TF] – Auto-refresh ~10s for ~2 min\n"
        "/predict [PAIR] [TF] [H] – BUY/SELL/HOLD next H bars\n"
        "/pairs or /listpairs – Show requested vs active\n"
        "/addpair SYMBOL/QUOTE – Add pair (owner)\n"
        "/removepair SYMBOL/QUOTE – Remove pair (owner)\n"
    )
    if update.effective_user.id == OWNER_ID:
        msg += "\nOwner: /grantme – grant yourself 30 days"
    await update.message.reply_text(msg)

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    title = "Midnight Crypto Bot Trading – 30 days"
    desc  = "SMA/RSI signals + predictions. Educational only."
    prices = [LabeledPrice(label="Access (30 days)", amount=STARS_PRICE_XTR)]
    await ctx.bot.send_invoice(
        chat_id=update.effective_chat.id,
        title=title, description=desc,
        payload=f"sub:{update.effective_user.id}:{now_ts()}",
        provider_token="", currency="XTR", prices=prices,
        subscription_period=2592000
    )

async def precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def successful_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    exp = sp.subscription_expiration_date or (now_ts() + 2592000)
    set_expiry(update.effective_user.id, exp); set_opt(update.effective_user.id, True)
    dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    await update.message.reply_text(f"Payment received. Active until {dt:%Y-%m-%d %H:%M UTC}. Use /signalson to enable alerts.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    row = cur.execute("SELECT expires_at FROM users WHERE user_id=?", (uid,)).fetchone()
    if row and row[0] > now_ts():
        dt = datetime.fromtimestamp(row[0], tz=timezone.utc)
        await update.message.reply_text(f"Active. Expires {dt:%Y-%m-%d %H:%M UTC}.")
    else:
        await update.message.reply_text("Inactive. Use /subscribe.")

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Open the payment message → disable auto-renew. You keep access until expiry.")

async def cmd_signalson(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")
    set_opt(update.effective_user.id, True)
    await update.message.reply_text("OK. Alerts enabled here.")

async def cmd_signalsoff(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    set_opt(update.effective_user.id, False)
    await update.message.reply_text("Alerts paused in this chat.")

async def cmd_grantme(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Not authorized.")
    exp = now_ts() + 2592000
    set_expiry(update.effective_user.id, exp); set_opt(update.effective_user.id, True)
    dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    await update.message.reply_text(f"✅ Free subscription granted until {dt:%Y-%m-%d %H:%M UTC}.")

# Pair management
async def _refresh_pairs():
    engine.requested_pairs = db_get_pairs()
    await engine.init_markets()

async def cmd_addpair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Not authorized.")
    if not ctx.args:
        return await update.message.reply_text("Usage: /addpair SYMBOL/QUOTE  (e.g. /addpair HYPE/USDT)")
    sym = " ".join(ctx.args).upper().strip()
    db_add_pair(sym); await _refresh_pairs()
    await update.message.reply_text(f"Added: {sym}\nActive on {EXCHANGE}: {', '.join(engine.valid_pairs) or '(none)'}")

async def cmd_removepair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Not authorized.")
    if not ctx.args:
        return await update.message.reply_text("Usage: /removepair SYMBOL/QUOTE")
    sym = " ".join(ctx.args).upper().strip()
    db_remove_pair(sym); await _refresh_pairs()
    await update.message.reply_text(f"Removed: {sym}\nActive on {EXCHANGE}: {', '.join(engine.valid_pairs) or '(none)'}")

async def cmd_listpairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req = db_get_pairs()
    active = engine.valid_pairs
    msg = (
        "📊 Pairs\n"
        f"• Requested: {', '.join(req) if req else '(none)'}\n"
        f"• Active on {EXCHANGE}: {', '.join(active) if active else '(none)'}"
    )
    await update.message.reply_text(msg)

# /pairs (alias)
async def cmd_pairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await cmd_listpairs(update, ctx)

# Chart command
async def _pick_default_pair():
    req = db_get_pairs()
    if engine.valid_pairs:
        return engine.valid_pairs[0]
    return (req[0] if req else "BTC/USDT")

async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")
    args = [a.strip() for a in ctx.args] if ctx.args else []
    tf = None; pair = None
    if args:
        maybe_tf = args[-1].lower()
        if maybe_tf.endswith(("m","h","d")) and any(ch.isdigit() for ch in maybe_tf):
            tf = maybe_tf; args = args[:-1]
    if args: pair = " ".join(args).upper()
    pair = pair or await _pick_default_pair()
    tf = tf or TIMEFRAME
    try:
        df = await engine.fetch_df(pair, timeframe=tf)
        if df.empty:
            return await update.message.reply_text(f"No data for {pair} on {tf}. Try later.")
        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["rsi"] = engine.rsi(df["close"], 14)
        mark = price = None
        if len(df) >= 2:
            row, prev = df.iloc[-1], df.iloc[-2]
            long_cond = (df["sma50"].iloc[-1] > df["sma200"].iloc[-1]) and (prev.rsi < 45 <= row.rsi)
            exit_cond = (row.rsi > 65) or (df["sma50"].iloc[-1] < df["sma200"].iloc[-1])
            if long_cond: mark, price = "LONG", float(row["close"])
            elif exit_cond: mark, price = "EXIT", float(row["close"])
        cons, used, spread, _ = await price_agg.get_consensus(pair)
        asof = df["time"].iloc[-1]
        try: asof = asof.tz_convert(DISPLAY_TZ)
        except Exception: pass
        asof_str = asof.strftime("%Y-%m-%d %H:%M:%S"); tzlab = tz_label_from_ts(asof, DISPLAY_TZ)
        png = plot_signal_chart(pair, df, mark, price, title_tf=tf, last_tick=cons)
        caption = f"{pair} — {tf}"
        if mark == "LONG": caption += "  •  BUY signal"
        elif mark == "EXIT": caption += "  •  SELL/EXIT signal"
        caption += f"\nAs of: {asof_str} {tzlab}"
        if cons:
            caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
            if spread is not None: caption += f" (spread {spread:.2f})"
            caption += f"\nLive tick plotted on chart"
        await update.message.reply_photo(png, caption=caption)
    except Exception as e:
        logging.error("chart error: %s", e)
        await update.message.reply_text("Chart error. Try again shortly.")

# Live command
async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")
    args = [a.strip() for a in ctx.args] if ctx.args else []
    tf = None; pair = None
    if args:
        maybe_tf = args[-1].lower()
        if maybe_tf.endswith(("m","h","d")) and any(ch.isdigit() for ch in maybe_tf):
            tf = maybe_tf; args = args[:-1]
    if args: pair = " ".join(args).upper()
    pair = pair or await _pick_default_pair()
    tf = tf or TIMEFRAME
    status_msg = await update.message.reply_text(f"Starting live chart for {pair} ({tf})…")
    photo_msg = None
    for _ in range(12):
        try:
            df = await engine.fetch_df(pair, timeframe=tf)
            if df.empty:
                await status_msg.edit_text(f"No data for {pair} on {tf}."); break
            df["sma50"] = df["close"].rolling(50).mean()
            df["sma200"] = df["close"].rolling(200).mean()
            df["rsi"] = engine.rsi(df["close"], 14)
            mark = price = None
            if len(df) >= 2:
                row, prev = df.iloc[-1], df.iloc[-2]
                long_cond = (df["sma50"].iloc[-1] > df["sma200"].iloc[-1]) and (prev.rsi < 45 <= row.rsi)
                exit_cond = (row.rsi > 65) or (df["sma50"].iloc[-1] < df["sma200"].iloc[-1])
                if long_cond: mark, price = "LONG", float(row["close"])
                elif exit_cond: mark, price = "EXIT", float(row["close"])
            cons, used, spread, _ = await price_agg.get_consensus(pair)
            png = plot_signal_chart(pair, df, mark, price, title_tf=tf, last_tick=cons)
            asof = df["time"].iloc[-1]
            try: asof = asof.tz_convert(DISPLAY_TZ)
            except Exception: pass
            asof_str = asof.strftime("%Y-%m-%d %H:%M:%S"); tzlab = tz_label_from_ts(asof, DISPLAY_TZ)
            updated_str = datetime.utcnow().strftime("%H:%M:%S UTC")
            caption = f"{pair} — {tf}"
            if mark == "LONG": caption += "  •  BUY"
            elif mark == "EXIT": caption += "  •  SELL/EXIT"
            caption += f"\nAs of: {asof_str} {tzlab}"
            if cons:
                caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
                if spread is not None: caption += f" (spread {spread:.2f})"
                caption += f"\nLive tick plotted on chart"
            caption += f"\nUpdated: {updated_str}"
            if photo_msg is None:
                photo_msg = await update.message.reply_photo(png, caption=caption)
                try: await status_msg.delete()
                except Exception: pass
            else:
                png.seek(0)
                try:
                    await ctx.bot.edit_message_media(
                        chat_id=photo_msg.chat_id,
                        message_id=photo_msg.message_id,
                        media=InputMediaPhoto(png, caption=caption)
                    )
                except BadRequest as e:
                    if "Message is not modified" in str(e):
                        try:
                            await ctx.bot.edit_message_caption(
                                chat_id=photo_msg.chat_id,
                                message_id=photo_msg.message_id,
                                caption=caption
                            )
                        except Exception as ee:
                            logging.warning("edit caption fallback failed: %s", ee)
                    else:
                        logging.warning("edit media failed: %s", e)
        except Exception as e:
            logging.warning("live update failed: %s", e)
        await asyncio.sleep(10)
    try: await update.message.reply_text(f"Live session ended for {pair} ({tf}).")
    except Exception: pass

# Predict
async def cmd_predict(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")
    args = [a.strip() for a in ctx.args] if ctx.args else []
    pair = None; tf = None; horizon = 5
    if args and args[-1].isdigit():
        horizon = max(1, min(60, int(args[-1]))); args = args[:-1]
    if args:
        maybe_tf = args[-1].lower()
        if maybe_tf.endswith(("m","h","d")) and any(ch.isdigit() for ch in maybe_tf):
            tf = maybe_tf; args = args[:-1]
    if args: pair = " ".join(args).upper()
    pair = pair or await _pick_default_pair()
    tf = tf or TIMEFRAME
    try:
        label, prob, explanation, df = await engine.predict(pair, horizon=horizon, timeframe=tf)
        if label is None:
            return await update.message.reply_text(f"Not enough data for {pair} on {tf}. Try later.")
        cons, used, spread, _ = await price_agg.get_consensus(pair)
        png = plot_signal_chart(pair, df, mark=None, price=None, title_tf=tf, last_tick=cons)
        asof = df["time"].iloc[-1]
        try: asof = asof.tz_convert(DISPLAY_TZ)
        except Exception: pass
        asof_str = asof.strftime("%Y-%m-%d %H:%M:%S"); tzlab = tz_label_from_ts(asof, DISPLAY_TZ)
        conf = int(round((prob or 0.0) * 100))
        caption = (
            f"🧠 Prediction — {pair} ({tf})\n"
            f"Next {horizon} bars: {label}  (P(up)={conf}%)\n"
            f"As of: {asof_str} {tzlab}\n"
            f"{explanation}"
        )
        if cons:
            caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
            if spread is not None: caption += f" (spread {spread:.2f})"
            caption += f"\nLive tick plotted on chart"
        await update.message.reply_photo(png, caption=caption)
    except Exception as e:
        logging.error("predict error: %s", e)
        await update.message.reply_text("Prediction error. Try again shortly.")

# -----------------------------
# Scheduler
# -----------------------------
engine = Engine(EXCHANGE, TIMEFRAME, db_get_pairs())

async def scheduled_job(app: "Application"):
    try:
        for pair in engine.valid_pairs:
            res, df = await engine.analyze(pair)
            if not res: continue
            mark, text, price, ts = res
            await broadcast(text, app)
            try:
                if "sma50" not in df.columns: df["sma50"] = df["close"].rolling(50).mean()
                if "sma200" not in df.columns: df["sma200"] = df["close"].rolling(200).mean()
                cons, used, spread, _ = await price_agg.get_consensus(pair)
                png = plot_signal_chart(pair, df, mark, price, title_tf=None, last_tick=cons)
                asof = df["time"].iloc[-1]
                try: asof = asof.tz_convert(DISPLAY_TZ)
                except Exception: pass
                asof_str = asof.strftime("%Y-%m-%d %H:%M:%S"); tzlab = tz_label_from_ts(asof, DISPLAY_TZ)
                caption = text + f"\nAs of: {asof_str} {tzlab}"
                if cons:
                    caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
                    if spread is not None: caption += f" (spread {spread:.2f})"
                    caption += f"\nLive tick plotted on chart"
                # OPTIONAL model attachment
                # try:
                #     mdl, p, expl, _ = await engine.predict(pair, horizon=5)
                #     if mdl is not None:
                #         caption += f"\nModel: {mdl} (P(up)={int(round(p*100))}%) — {expl}"
                # except Exception: pass
                if png: await broadcast_chart(app, caption=caption, png_buf=png)
            except Exception as ce:
                logging.warning("chart send failed: %s", ce)
        await asyncio.sleep(0.1)
    except Exception as e:
        logging.error("scheduled_job crashed: %s", e)
        logging.error("Traceback:\n%s", traceback.format_exc())

def schedule_jobs(app: "Application", scheduler: AsyncIOScheduler):
    scheduler.add_job(
        scheduled_job, "interval",
        minutes=1, args=[app],
        next_run_time=datetime.now(timezone.utc)
    )

# -----------------------------
# FastAPI + PTB
# -----------------------------
logging.basicConfig(level=logging.INFO)
request = HTTPXRequest(connect_timeout=20, read_timeout=30, write_timeout=20, pool_timeout=20)
application = Application.builder().token(BOT_TOKEN).request(request).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await application.initialize(); await application.start()
    except Exception as e:
        logging.error("Telegram init/start failed: %s", e)
    await engine.init_markets()
    scheduler = AsyncIOScheduler(); schedule_jobs(application, scheduler); scheduler.start()
    if PUBLIC_URL:
        url = f"{PUBLIC_URL.rstrip('/')}/webhook"
        try:
            await application.bot.set_webhook(url)
            logging.info(f"Webhook set to {url}")
        except Exception as e:
            logging.warning("Could not set webhook now: %s", e)
    yield
    try:
        await application.stop(); await application.shutdown()
    except Exception:
        pass

api = FastAPI(lifespan=lifespan)

@api.api_route("/", methods=["GET", "HEAD"])
def health():
    return {"ok": True}

@api.get("/robots.txt")
def robots():
    return Response("User-agent: *\nDisallow:\n", media_type="text/plain")

@api.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@api.get("/pairs")
def list_pairs():
    return {"requested": db_get_pairs(), "active_on_exchange": engine.valid_pairs}

@api.get("/runjob")
async def run_job_now():
    try:
        await scheduled_job(application); return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@api.get("/setwebhook")
async def set_webhook():
    if not PUBLIC_URL:
        return JSONResponse({"ok": False, "error": "Set PUBLIC_URL env first"}, status_code=400)
    url = f"{PUBLIC_URL.rstrip('/')}/webhook"
    await application.bot.set_webhook(url)
    return {"ok": True, "webhook": url}

@api.post("/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

# Handlers
application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("help", cmd_help))
application.add_handler(CommandHandler("subscribe", cmd_subscribe))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("cancel", cmd_cancel))
application.add_handler(CommandHandler("signalson", cmd_signalson))
application.add_handler(CommandHandler("signalsoff", cmd_signalsoff))
application.add_handler(CommandHandler("grantme", cmd_grantme))
application.add_handler(CommandHandler("chart", cmd_chart))
application.add_handler(CommandHandler("live", cmd_live))
application.add_handler(CommandHandler("predict", cmd_predict))
application.add_handler(CommandHandler("pairs", cmd_pairs))
application.add_handler(CommandHandler("listpairs", cmd_listpairs))
application.add_handler(CommandHandler("addpair", cmd_addpair))
application.add_handler(CommandHandler("removepair", cmd_removepair))
application.add_handler(PreCheckoutQueryHandler(precheckout))
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
