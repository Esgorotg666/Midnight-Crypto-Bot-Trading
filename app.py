# ——— imports & setup (unchanged except a few helpers) ———
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

os.environ.setdefault("MPLBACKEND", "Agg")

from dotenv import load_dotenv
load_dotenv()

import ccxt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import httpx

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import uvicorn

from telegram import Update, LabeledPrice, InputMediaPhoto, BotCommand
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PreCheckoutQueryHandler, MessageHandler, filters
)
from telegram.error import BadRequest

from zoneinfo import ZoneInfo

# ——— env ———
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EXCHANGE  = os.getenv("EXCHANGE", "kucoin")
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
PAIR      = os.getenv("PAIR", "BTC/USDT")
PAIRS_ENV = os.getenv("PAIRS", PAIR)
PAIRS     = [p.strip() for p in PAIRS_ENV.split(",") if p.strip()]

EXCHANGES_ENV = os.getenv("EXCHANGES", EXCHANGE)
EX_LIST = [e.strip() for e in EXCHANGES_ENV.split(",") if e.strip()]

DISPLAY_TZ = os.getenv("DISPLAY_TZ", "UTC")
LOCAL_TZ = ZoneInfo(DISPLAY_TZ)

STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "10000"))
PUBLIC_URL = os.getenv("PUBLIC_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "5467277042"))

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

# ——— sqlite ———
DB_PATH = "subs.sqlite"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  expires_at INTEGER NOT NULL DEFAULT 0,
  signals_on INTEGER NOT NULL DEFAULT 1
)""")
cur.execute("""CREATE TABLE IF NOT EXISTS pairs (symbol TEXT PRIMARY KEY)""")
conn.commit()

def now_ts() -> int: return int(time.time())
def is_active(uid: int) -> bool:
    row = cur.execute("SELECT expires_at FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row and row[0] > now_ts())
def set_expiry(uid: int, expires_at: int):
    cur.execute("""INSERT INTO users(user_id, expires_at) VALUES(?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at""",
                   (uid, expires_at)); conn.commit()
def set_opt(uid: int, on: bool):
    cur.execute("""INSERT INTO users(user_id, signals_on) VALUES(?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET signals_on=excluded.signals_on""",
                   (uid, 1 if on else 0)); conn.commit()
def active_users(): return cur.execute(
    "SELECT user_id FROM users WHERE expires_at > ? AND signals_on = 1", (now_ts(),)
).fetchall()
def db_get_pairs():
    rows = cur.execute("SELECT symbol FROM pairs ORDER BY symbol").fetchall()
    return [r[0] for r in rows]
def db_add_pair(sym: str):
    cur.execute("INSERT OR IGNORE INTO pairs(symbol) VALUES(?)", (sym,)); conn.commit()
def db_remove_pair(sym: str):
    cur.execute("DELETE FROM pairs WHERE symbol=?", (sym,)); conn.commit()
if not db_get_pairs():
    for p in PAIRS: db_add_pair(p)

# ——— uphold (quotes-only) ———
class UpholdClient:
    BASE = "https://api.uphold.com/v0"
    def __init__(self, timeout=8.0): self.timeout = timeout
    def map_pair(self, ccxt_pair: str) -> list[str]:
        try: base, quote = ccxt_pair.split("/")
        except ValueError: return []
        if quote.upper() == "USDT":
            return [f"{base}-USD", f"{base}-USDT"]
        return [f"{base}-{quote}"]
    def _parse_ticker(self, data):
        if not isinstance(data, dict): return None
        for k in ("last","price"):
            v = data.get(k)
            if v is not None:
                try: return float(v)
                except: pass
        bid, ask = data.get("bid"), data.get("ask")
        try:
            bid = float(bid) if bid is not None else None
            ask = float(ask) if ask is not None else None
        except: bid = ask = None
        if bid is not None and ask is not None: return (bid+ask)/2.0
        for v in data.values():
            try: return float(v)
            except: continue
        return None
    async def fetch_price(self, pair_ccxt: str) -> float | None:
        cands = self.map_pair(pair_ccxt)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for sym in cands:
                try:
                    r = await client.get(f"{self.BASE}/ticker/{sym}")
                    if r.status_code != 200: continue
                    js = r.json()
                    if isinstance(js, list) and js: js = js[0]
                    px = self._parse_ticker(js)
                    if px and math.isfinite(px): return float(px)
                except: continue
        return None

# ——— consensus across exchanges (+ uphold) ———
def _mad(values):
    m = median(values); return median([abs(v-m) for v in values]) or 1e-9
class MultiPrice:
    def __init__(self, exchange_ids: list[str]):
        self.exes = []; self.want_uphold = False
        for ex in exchange_ids:
            if ex.lower() == "uphold": self.want_uphold = True
            else:
                try: self.exes.append(getattr(ccxt, ex)({"enableRateLimit": True}))
                except: pass
        self.uphold = UpholdClient() if self.want_uphold else None
    async def get_consensus(self, pair: str):
        async def one(ex):
            def _t():
                try:
                    t = ex.fetch_ticker(pair)
                    for k in ("last","close","bid","ask"):
                        if t.get(k): return float(t[k])
                except: pass
                try:
                    o = ex.fetch_ohlcv(pair, timeframe="1m", limit=1)
                    return float(o[-1][4]) if o else None
                except: return None
            return await asyncio.to_thread(_t)
        tasks = [one(ex) for ex in self.exes]
        if self.uphold is not None:
            async def uphold_task():
                try: return await self.uphold.fetch_price(pair)
                except: return None
            tasks.append(uphold_task())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        raw, per_ex = [], {}
        for ex, val in zip(self.exes, results[:len(self.exes)]):
            if isinstance(val, Exception) or val is None or not math.isfinite(val): continue
            raw.append(val); per_ex[ex.id] = val
        if self.uphold is not None:
            uv = results[-1]
            if not isinstance(uv, Exception) and uv is not None and math.isfinite(uv):
                raw.append(float(uv)); per_ex["uphold"] = float(uv)
        if not raw: return None,0,None,per_ex
        m = median(raw); mad = _mad(raw)
        kept = [v for v in raw if abs(v-m) <= 3*mad] or raw
        cons = median(kept); spread = max(kept)-min(kept) if len(kept)>1 else 0.0
        return cons, len(kept), spread, per_ex
price_agg = MultiPrice(EX_LIST)

# ——— utils & indicators ———
def _sigmoid(x: float) -> float:
    try:
        if x>=0: z = math.exp(-x); return 1/(1+z)
        z = math.exp(x); return z/(1+z)
    except OverflowError: return 0.0 if x<0 else 1.0
def _zscore(s: pd.Series) -> pd.Series:
    m = s.rolling(200).mean(); v = s.rolling(200).std(ddof=0).replace(0,1e-9); return (s-m)/v
def _ema(s: pd.Series, span: int) -> pd.Series: return s.ewm(span=span, adjust=False).mean()
def _macd(close: pd.Series):
    ema12=_ema(close,12); ema26=_ema(close,26); macd=ema12-ema26; signal=_ema(macd,9); hist=macd-signal
    return macd, signal, hist
def _atr(df: pd.DataFrame, period: int=14) -> pd.Series:
    h,l,c = df["high"], df["low"], df["close"]; pc = c.shift(1)
    tr = pd.concat([(h-l), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()
def _tf_seconds(tf: str) -> int:
    tf=(tf or "1m").lower()
    if tf.endswith("m"): return int(tf[:-1])*60
    if tf.endswith("h"): return int(tf[:-1])*3600
    if tf.endswith("d"): return int(tf[:-1])*86400
    if tf.endswith("w"): return int(tf[:-1])*604800
    if tf.endswith("mth") or tf.endswith("mo"): return 2592000
    if tf in ("1m","5m","15m","30m"): return 60
    return 60
def _is_stale(last_dt_local: pd.Timestamp, tf: str) -> bool:
    now_local = pd.Timestamp.now(tz=LOCAL_TZ)
    return (now_local - last_dt_local).total_seconds() > (2 * _tf_seconds(tf))
import matplotlib.dates as mdates  # you already import this

def _choose_time_axis(ax, tf: str):
    sec = _tf_seconds(tf or "1m")
    # Intraday (<= 1h bars): show HH:MM on top line, month-day under it
    if sec <= 3600:
        locator   = mdates.AutoDateLocator(minticks=5, maxticks=8)
        formatter = mdates.DateFormatter("%H:%M\n%b-%d", tz=LOCAL_TZ)
    # Multi-hour but < 1 day (e.g., 2h/4h/6h/12h): show date + hour
    elif sec < 86400:
        locator   = mdates.AutoDateLocator(minticks=4, maxticks=7)
        formatter = mdates.DateFormatter("%b-%d\n%H:%M", tz=LOCAL_TZ)
    # Daily/Weekly/Monthly: show only calendar dates (no time-of-day)
    else:
        locator   = mdates.AutoDateLocator(minticks=5, maxticks=8)
        formatter = mdates.DateFormatter("%b %d\n%Y", tz=LOCAL_TZ)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
# Human aliases → CCXT TF
def _normalize_tf(tf: str | None) -> str:
    if not tf: return TIMEFRAME
    t = tf.strip().lower()
    aliases = {
        "d":"1d","day":"1d","daily":"1d",
        "w":"1w","wk":"1w","weekly":"1w",
        "m":"1M","mo":"1M","month":"1M","monthly":"1M",
    }
    if t in aliases: return aliases[t]
    return t

# ——— strategy engine ———
class Engine:
    def __init__(self, exchange_id, timeframe, requested_pairs):
        self.ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.tf = timeframe
        self.requested_pairs = requested_pairs
        self.valid_pairs = []
        self.last = {}
    async def init_markets(self):
        def _load(): self.ex.load_markets(); return set(self.ex.symbols or [])
        try: symbols = await asyncio.to_thread(_load)
        except Exception as e:
            logging.warning("Could not load markets for %s: %s", self.ex.id, e)
            self.valid_pairs = list(self.requested_pairs); return
        wanted=set(self.requested_pairs)
        self.valid_pairs = sorted(list(wanted & symbols))
        skipped = sorted(list(wanted - symbols))
        if skipped: logging.info("Skipping unsupported on %s: %s", self.ex.id, ", ".join(skipped))
        if not self.valid_pairs:
            logging.warning("No valid pairs on %s from requested.", self.ex.id)
    async def fetch_df(self, pair, limit=300, timeframe: str | None = None):
        tf = timeframe or self.tf
        def get_ohlcv(): return self.ex.fetch_ohlcv(pair, timeframe=tf, limit=limit)
        try: ohlcv = await asyncio.to_thread(get_ohlcv)
        except Exception as e:
            logging.warning("fetch_ohlcv failed for %s: %s", pair, e)
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
        if not ohlcv or len(ohlcv) < 210:
            logging.info("[%s] Not enough candles: %s", pair, len(ohlcv) if ohlcv else 0)
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
        return df
    @staticmethod
    def rsi(series, period=14):
        d = series.diff(); up=np.where(d>0,d,0.0); dn=np.where(d<0,-d,0.0)
        ru=pd.Series(up).rolling(period).mean(); rd=pd.Series(dn).rolling(period).mean()
        rs = ru/(rd+1e-9); return 100.0 - (100.0/(1.0+rs))
    async def analyze(self, pair):
        df = await self.fetch_df(pair)
        if df.empty or len(df)<200: return None, df
        df["sma50"]=df["close"].rolling(50).mean(); df["sma200"]=df["close"].rolling(200).mean()
        df["rsi"]=self.rsi(df["close"],14)
        row, prev = df.iloc[-1], df.iloc[-2]
        long_cond=(df["sma50"].iloc[-1]>df["sma200"].iloc[-1]) and (prev.rsi<45<=row.rsi)
        exit_cond=(row.rsi>65) or (df["sma50"].iloc[-1]<df["sma200"].iloc[-1])
        price=row["close"]; ts=row["time"].strftime("%Y-%m-%d %H:%M %Z")
        last_state=self.last.get(pair)
        if long_cond and last_state!="LONG":
            self.last[pair]="LONG"
            text=f"LONG {pair} @ {price:.2f} [{self.tf}]  ({ts})\nSMA50>SMA200; RSI up-cross from <45."
            return ("LONG", text, price, ts), df
        if exit_cond and last_state!="EXIT":
            self.last[pair]="EXIT"
            text=f"EXIT/NEUTRAL {pair} @ {price:.2f} [{self.tf}]  ({ts})\nRSI>65 or SMA50<SMA200."
            return ("EXIT", text, price, ts), df
        return None, df
    async def predict(self, pair: str, horizon: int=5, timeframe: str | None=None):
        tf = timeframe or self.tf
        df = await self.fetch_df(pair, timeframe=tf)
        if df.empty or len(df)<220: return None, None, "Not enough data", df
        df["ret1"]=df["close"].pct_change(); df["ret5"]=df["close"].pct_change(5)
        df["sma20"]=df["close"].rolling(20).mean(); df["sma50"]=df["close"].rolling(50).mean(); df["sma200"]=df["close"].rolling(200).mean()
        df["slope20"]=df["sma20"].diff(); df["slope50"]=df["sma50"].diff(); df["slope200"]=df["sma200"].diff()
        macd,sig,hist=_macd(df["close"]); df["macd_hist"]=hist
        df["atr14"]=_atr(df,14).fillna(0.0); df["rsi"]=self.rsi(df["close"],14)
        mom_z=_zscore(df["ret5"]).iloc[-1]; hist_z=_zscore(df["macd_hist"]).iloc[-1]
        rsi_d=(df["rsi"].iloc[-1]-50.0)/50.0; slope_z=_zscore(df["slope20"]).iloc[-1]
        regime=1.0 if df["sma50"].iloc[-1]>df["sma200"].iloc[-1] else -1.0
        vol_k=(df["atr14"].iloc[-1]/(df["close"].iloc[-1]+1e-9)); vol_clamped=max(0.05, min(vol_k,0.20))
        score=(0.70*mom_z + 0.60*hist_z + 0.50*rsi_d + 0.40*slope_z + 0.35*regime - 0.25*(vol_clamped-0.10))
        prob_up=_sigmoid(score)
        label = "BUY" if prob_up>=0.60 else ("SELL" if prob_up<=0.40 else "HOLD")
        explanation=(f"mom_z={mom_z:.2f}; macd_z={hist_z:.2f}; rsiΔ={rsi_d:.2f}; slope_z={slope_z:.2f}; "
                     f"regime={'bull' if regime>0 else 'bear'}; vol={vol_k:.3f}; horizon={horizon} bars")
        return (label, float(prob_up), explanation, df)

# ——— plotting ———
def plot_signal_chart(pair, df, mark, price, title_tf=None, last_tick=None):
    dfp = df.tail(200).copy()
    if dfp.empty: return None
    fig, ax = plt.subplots(figsize=(8,4.5), dpi=140)
    ax.plot(dfp["time"], dfp["close"], linewidth=1.2, label="Close")
    if "sma50" not in dfp: dfp["sma50"]=dfp["close"].rolling(50).mean()
    if "sma200" not in dfp: dfp["sma200"]=dfp["close"].rolling(200).mean()
    ax.plot(dfp["time"], dfp["sma50"], linewidth=1.0, label="SMA50")
    ax.plot(dfp["time"], dfp["sma200"], linewidth=1.0, label="SMA200")
    if mark and price:
        x_sig=dfp["time"].iloc[-1]; y_sig=price
        ax.scatter([x_sig],[y_sig], s=50); ax.annotate("BUY" if mark=="LONG" else "SELL",(x_sig,y_sig),xytext=(10,10),textcoords="offset points")
    x_last=dfp["time"].iloc[-1]; ax.axvline(x_last, linewidth=0.7)
    if last_tick is not None:
        x_live = x_last + pd.Timedelta(seconds=2)
        ax.scatter([x_live],[last_tick], s=65, zorder=5)
        ax.annotate(f"{last_tick:.2f}", (x_live,last_tick), xytext=(0,12), textcoords="offset points",
                    ha="center", fontsize=8, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.2", fc="yellow", alpha=0.7, lw=0))
        try:
            last_close=float(dfp["close"].iloc[-1]); ax.plot([x_last,x_live],[last_close,last_tick], linewidth=0.8)
        except: pass
    ax.set_title(f"{pair} — {title_tf or TIMEFRAME}  ({DISPLAY_TZ})")
    ax.set_xlabel("Time"); ax.set_ylabel("Price"); ax.legend(loc="best"); ax.grid(True, linewidth=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M\n%m-%d", tz=LOCAL_TZ))
    for lbl in ax.get_xticklabels(): lbl.set_rotation(0)
    buf=io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf

def plot_compare_chart(pairs: list[str], dfs: list[pd.DataFrame], title_tf: str):
    # Find common time window; rebase each to 100 at first common index
    aligned = []
    for df in dfs:
        if df.empty: continue
        d = df[["time","close"]].copy().set_index("time")
        aligned.append(d)
    if not aligned: return None
    # align on intersection of indices (inner join)
    base = aligned[0]
    for d in aligned[1:]:
        base = base.join(d, how="inner", lsuffix="", rsuffix="", sort=True)
    if base.empty or base.shape[0] < 10:  # need some overlap
        # fallback: plot individually rebased using each's own first
        fig, ax = plt.subplots(figsize=(8,4.5), dpi=140)
        for pair, df in zip(pairs, dfs):
            if df.empty: continue
            tmp = df.tail(300).copy()
            tmp = tmp.set_index("time")["close"]
            rb = tmp / tmp.iloc[0] * 100.0
            ax.plot(rb.index, rb.values, linewidth=1.2, label=pair)
        ax.set_title(f"Compare — {', '.join(pairs)}  ({title_tf})  ({DISPLAY_TZ})")
        ax.set_xlabel("Time"); ax.set_ylabel("Rebased to 100"); ax.legend(loc="best"); ax.grid(True, linewidth=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M", tz=LOCAL_TZ))
        buf=io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
        return buf

    # Rebase each column to 100
    # Build a new plot with up to 3 series
    fig, ax = plt.subplots(figsize=(8,4.5), dpi=140)
    for i, (pair, df) in enumerate(zip(pairs, dfs)):
        if df.empty: continue
        s = df[["time","close"]].copy().set_index("time")["close"]
        s = s.reindex(base.index, method="nearest")
        rb = s / s.iloc[0] * 100.0
        ax.plot(rb.index, rb.values, linewidth=1.2, label=pair)
    ax.set_title(f"Compare — {', '.join(pairs)}  ({title_tf})  ({DISPLAY_TZ})")
    ax.set_xlabel("Time"); ax.set_ylabel("Rebased to 100")
    ax.legend(loc="best"); ax.grid(True, linewidth=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M", tz=LOCAL_TZ))
    for lbl in ax.get_xticklabels(): lbl.set_rotation(0)
    buf=io.BytesIO(); fig.tight_layout(); fig.savefig(buf, format="png"); plt.close(fig); buf.seek(0)
    return buf

# ——— telegram helpers & menu ———
async def broadcast(text: str, app: "Application"):
    for (uid,) in active_users():
        try: await app.bot.send_message(uid, text)
        except Exception as e: logging.warning(f"send fail {uid}: {e}")
async def broadcast_chart(app: "Application", caption: str, png_buf: io.BytesIO):
    for (uid,) in active_users():
        try: await app.bot.send_photo(uid, png_buf, caption=caption); png_buf.seek(0)
        except Exception as e: logging.warning(f"send photo fail {uid}: {e}")
def _format_pairs_list(pairs: list[str], max_show: int = 40) -> str:
    if not pairs: return "(none)"
    shown=pairs[:max_show]; more=len(pairs)-len(shown)
    lines,line=[],[]
    for i,sym in enumerate(shown,1):
        line.append(sym)
        if (i%8)==0: lines.append(", ".join(line)); line=[]
    if line: lines.append(", ".join(line))
    if more>0: lines.append(f"... and {more} more")
    return "\n".join(lines)
async def set_bot_commands(app: "Application"):
    cmds = [
        BotCommand("start","Welcome & subscription info"),
        BotCommand("subscribe","Buy 30-day access (Stars)"),
        BotCommand("status","Check your expiry"),
        BotCommand("signalson","Enable alerts"),
        BotCommand("signalsoff","Pause alerts"),
        BotCommand("chart","Chart [PAIR] [TF]"),
        BotCommand("chart_daily","Daily chart w/ prediction"),
        BotCommand("chart_weekly","Weekly chart w/ prediction"),
        BotCommand("chart_monthly","Monthly chart w/ prediction"),
        BotCommand("predict","Predict [PAIR] [TF] [H]"),
        BotCommand("compare","Compare up to 3 pairs [TF]"),
        BotCommand("pairs","Requested vs active pairs"),
        BotCommand("addpair","Owner: add a pair"),
        BotCommand("removepair","Owner: remove a pair"),
        BotCommand("help","Show commands & available pairs"),
    ]
    try: await app.bot.set_my_commands(cmds)
    except Exception as e: logging.warning(f"set_my_commands failed: {e}")
def tz_label_from_ts(ts: pd.Timestamp) -> str:
    try: return ts.tzname() or DISPLAY_TZ
    except: return DISPLAY_TZ

# ——— commands ———
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if is_active(update.effective_user.id):
        await update.message.reply_text("You’re active. /signalson to receive alerts. /status shows expiry.")
    else:
        await update.message.reply_text("Access requires a sub. Tap /subscribe to pay with Telegram Stars (30 days).")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = engine.valid_pairs or []
    active_block = _format_pairs_list(active)
    msg = (
        "🛠 Commands\n"
        "/chart [PAIR] [TF] – snapshot (ex: /chart BTC/USDT 1m)\n"
        "/chart_daily [PAIR] – daily chart + prediction\n"
        "/chart_weekly [PAIR] – weekly chart + prediction\n"
        "/chart_monthly [PAIR] – monthly chart + prediction\n"
        "/live [PAIR] [TF] – auto-refresh ~10s for ~2min\n"
        "/predict [PAIR] [TF] [H] – BUY/SELL/HOLD for next H bars\n"
        "/compare PAIR1 [PAIR2] [PAIR3] [TF] – overlay up to 3\n"
        "/pairs – requested vs active on the exchange\n"
        "/addpair SYMBOL/QUOTE – owner adds a pair\n"
        "/removepair SYMBOL/QUOTE – owner removes a pair\n"
        "\n"
        "TF can be 1m/5m/15m/1h/1d/1w/1M or words: daily, weekly, monthly\n"
        "\n📈 Available pairs (active):\n"
        f"{active_block}"
    )
    if update.effective_user.id == OWNER_ID:
        msg += "\n\nOwner-only: /grantme – grant yourself 30 days"
    await update.message.reply_text(msg)

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    title="Midnight Crypto Bot Trading – 30 days"
    desc="SMA/RSI signals + predictions. Educational only."
    prices=[LabeledPrice(label="Access (30 days)", amount=STARS_PRICE_XTR)]
    await ctx.bot.send_invoice(chat_id=update.effective_chat.id, title=title, description=desc,
        payload=f"sub:{update.effective_user.id}:{now_ts()}", provider_token="", currency="XTR",
        prices=prices, subscription_period=2592000)

async def precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def successful_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sp=update.message.successful_payment
    exp=sp.subscription_expiration_date or (now_ts()+2592000)
    set_expiry(update.effective_user.id, exp); set_opt(update.effective_user.id, True)
    dt=datetime.fromtimestamp(exp, tz=timezone.utc).astimezone(LOCAL_TZ)
    await update.message.reply_text(f"Payment received. Active until {dt:%Y-%m-%d %H:%M %Z}. Use /signalson to enable alerts.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    row=cur.execute("SELECT expires_at FROM users WHERE user_id=?", (uid,)).fetchone()
    if row and row[0]>now_ts():
        dt=datetime.fromtimestamp(row[0], tz=timezone.utc).astimezone(LOCAL_TZ)
        await update.message.reply_text(f"Active. Expires {dt:%Y-%m-%d %H:%M %Z}.")
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
    dt = datetime.fromtimestamp(exp, tz=timezone.utc).astimezone(LOCAL_TZ)
    await update.message.reply_text(f"✅ Free subscription granted until {dt:%Y-%m-%d %H:%M %Z}.")

# pair mgmt
async def _refresh_pairs():
    engine.requested_pairs = db_get_pairs()
    await engine.init_markets()
async def cmd_addpair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return await update.message.reply_text("⛔ Not authorized.")
    if not ctx.args: return await update.message.reply_text("Usage: /addpair SYMBOL/QUOTE  (e.g. /addpair HYPE/USDT)")
    sym = " ".join(ctx.args).upper().strip(); db_add_pair(sym); await _refresh_pairs()
    await update.message.reply_text(f"Added: {sym}\nActive on {EXCHANGE}: {', '.join(engine.valid_pairs) or '(none)'}")
async def cmd_removepair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return await update.message.reply_text("⛔ Not authorized.")
    if not ctx.args: return await update.message.reply_text("Usage: /removepair SYMBOL/QUOTE")
    sym = " ".join(ctx.args).upper().strip(); db_remove_pair(sym); await _refresh_pairs()
    await update.message.reply_text(f"Removed: {sym}\nActive on {EXCHANGE}: {', '.join(engine.valid_pairs) or '(none)'}")
async def cmd_listpairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req=db_get_pairs(); active=engine.valid_pairs or []
    msg=("📊 Pairs\nActive:\n"+_format_pairs_list(active)+"\n\nRequested:\n"+_format_pairs_list(req))
    await update.message.reply_text(msg)
async def cmd_pairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE): return await cmd_listpairs(update, ctx)

# default pick
async def _pick_default_pair():
    req=db_get_pairs()
    if engine.valid_pairs: return engine.valid_pairs[0]
    return (req[0] if req else "BTC/USDT")

# core chart
async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id): return await update.message.reply_text("Inactive. Use /subscribe.")
    args=[a.strip() for a in ctx.args] if ctx.args else []
    tf=None; pair=None
    if args:
        maybe_tf=args[-1].lower()
        if any(maybe_tf.endswith(suf) for suf in ("m","h","d","w","M")) or maybe_tf in ("daily","weekly","monthly","day","week","month"):
            tf=_normalize_tf(maybe_tf); args=args[:-1]
    if args: pair=" ".join(args).upper()
    pair = pair or await _pick_default_pair()
    tf = tf or TIMEFRAME
    try:
        df=await engine.fetch_df(pair, timeframe=tf)
        if df.empty: return await update.message.reply_text(f"No data for {pair} on {tf}. Try later.")
        df["sma50"]=df["close"].rolling(50).mean(); df["sma200"]=df["close"].rolling(200).mean(); df["rsi"]=engine.rsi(df["close"],14)
        mark=price=None
        if len(df)>=2:
            row,prev=df.iloc[-1],df.iloc[-2]
            long_cond=(df["sma50"].iloc[-1]>df["sma200"].iloc[-1]) and (prev.rsi<45<=row.rsi)
            exit_cond=(row.rsi>65) or (df["sma50"].iloc[-1]<df["sma200"].iloc[-1])
            if long_cond: mark,price="LONG",float(row["close"])
            elif exit_cond: mark,price="EXIT",float(row["close"])
        cons,used,spread,_=await price_agg.get_consensus(pair)
        last_dt=df["time"].iloc[-1]; stale=_is_stale(last_dt, tf)
        asof_str=last_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        png=plot_signal_chart(pair, df, mark, price, title_tf=tf, last_tick=cons)
        caption=f"{pair} — {tf}"
        if mark=="LONG": caption+="  •  BUY signal"
        elif mark=="EXIT": caption+="  •  SELL/EXIT signal"
        caption+=f"\nAs of: {asof_str}"
        if cons is not None:
            caption+=f"\nConsensus: {cons:.2f} from {used} exchanges"
            if spread is not None: caption+=f" (spread {spread:.2f})"
            caption+="\nLive tick plotted on chart"
        if stale: caption+="\n⚠️ Feed looks stale (no new closed candles yet)."
        await update.message.reply_photo(png, caption=caption)
    except Exception as e:
        logging.error("chart error: %s", e); await update.message.reply_text("Chart error. Try again shortly.")

# convenience wrappers
async def _chart_variant(update: Update, ctx: ContextTypes.DEFAULT_TYPE, fixed_tf: str):
    # call chart but with fixed TF
    ctx.args = ([] if not ctx.args else [a for a in ctx.args if "/" in a]) + [fixed_tf]
    return await cmd_chart(update, ctx)
async def cmd_chart_daily(update: Update, ctx: ContextTypes.DEFAULT_TYPE): return await _chart_variant(update, ctx, "1d")
async def cmd_chart_weekly(update: Update, ctx: ContextTypes.DEFAULT_TYPE): return await _chart_variant(update, ctx, "1w")
async def cmd_chart_monthly(update: Update, ctx: ContextTypes.DEFAULT_TYPE): return await _chart_variant(update, ctx, "1M")

# live
async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id): return await update.message.reply_text("Inactive. Use /subscribe.")
    args=[a.strip() for a in ctx.args] if ctx.args else []
    tf=None; pair=None
    if args:
        maybe_tf=args[-1].lower()
        if any(maybe_tf.endswith(suf) for suf in ("m","h","d","w","M")) or maybe_tf in ("daily","weekly","monthly","day","week","month"):
            tf=_normalize_tf(maybe_tf); args=args[:-1]
    if args: pair=" ".join(args).upper()
    pair=pair or await _pick_default_pair(); tf=tf or TIMEFRAME
    status_msg=await update.message.reply_text(f"Starting live chart for {pair} ({tf})…")
    photo_msg=None
    for _ in range(12):
        try:
            df=await engine.fetch_df(pair, timeframe=tf)
            if df.empty: await status_msg.edit_text(f"No data for {pair} on {tf}."); break
            df["sma50"]=df["close"].rolling(50).mean(); df["sma200"]=df["close"].rolling(200).mean(); df["rsi"]=engine.rsi(df["close"],14)
            mark=price=None
            if len(df)>=2:
                row,prev=df.iloc[-1],df.iloc[-2]
                long_cond=(df["sma50"].iloc[-1]>df["sma200"].iloc[-1]) and (prev.rsi<45<=row.rsi)
                exit_cond=(row.rsi>65) or (df["sma50"].iloc[-1]<df["sma200"].iloc[-1])
                if long_cond: mark,price="LONG",float(row["close"])
                elif exit_cond: mark,price="EXIT",float(row["close"])
            cons,used,spread,_=await price_agg.get_consensus(pair)
            png=plot_signal_chart(pair, df, mark, price, title_tf=tf, last_tick=cons)
            last_dt=df["time"].iloc[-1]; stale=_is_stale(last_dt, tf)
            asof_str=last_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            updated_str=pd.Timestamp.now(tz=LOCAL_TZ).strftime("%H:%M:%S %Z")
            caption=f"{pair} — {tf}"
            if mark=="LONG": caption+="  •  BUY"
            elif mark=="EXIT": caption+="  •  SELL/EXIT"
            caption+=f"\nAs of: {asof_str}"
            if cons is not None:
                caption+=f"\nConsensus: {cons:.2f} from {used} exchanges"
                if spread is not None: caption+=f" (spread {spread:.2f})"
                caption+="\nLive tick plotted on chart"
            caption+=f"\nUpdated: {updated_str}"
            if stale: caption+="\n⚠️ Feed looks stale (no new closed candles yet)."
            if photo_msg is None:
                photo_msg=await update.message.reply_photo(png, caption=caption)
                try: await status_msg.delete()
                except: pass
            else:
                png.seek(0)
                try:
                    await ctx.bot.edit_message_media(chat_id=photo_msg.chat_id, message_id=photo_msg.message_id,
                        media=InputMediaPhoto(png, caption=caption))
                except BadRequest as e:
                    if "Message is not modified" in str(e):
                        try:
                            await ctx.bot.edit_message_caption(chat_id=photo_msg.chat_id, message_id=photo_msg.message_id, caption=caption)
                        except Exception as ee:
                            logging.warning("edit caption fallback failed: %s", ee)
                    else:
                        logging.warning("edit media failed: %s", e)
        except Exception as e:
            logging.warning("live update failed: %s", e)
        await asyncio.sleep(10)
    try: await update.message.reply_text(f"Live session ended for {pair} ({tf}).")
    except: pass

# predict (supports daily/weekly/monthly words)
async def cmd_predict(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id): return await update.message.reply_text("Inactive. Use /subscribe.")
    args=[a.strip() for a in ctx.args] if ctx.args else []
    pair=None; tf=None; horizon=5
    if args and args[-1].isdigit(): horizon=max(1, min(60, int(args[-1]))); args=args[:-1]
    if args:
        maybe_tf=args[-1].lower()
        if any(maybe_tf.endswith(suf) for suf in ("m","h","d","w","M")) or maybe_tf in ("daily","weekly","monthly","day","week","month"):
            tf=_normalize_tf(maybe_tf); args=args[:-1]
    if args: pair=" ".join(args).upper()
    pair=pair or await _pick_default_pair(); tf=tf or TIMEFRAME
    try:
        label,prob,explanation,df = await engine.predict(pair, horizon=horizon, timeframe=tf)
        if label is None: return await update.message.reply_text(f"Not enough data for {pair} on {tf}. Try later.")
        cons,used,spread,_ = await price_agg.get_consensus(pair)
        png = plot_signal_chart(pair, df, mark=None, price=None, title_tf=tf, last_tick=cons)
        last_dt=df["time"].iloc[-1]; asof_str=last_dt.strftime("%Y-%m-%d %H:%M:%S %Z"); conf=int(round((prob or 0.0)*100))
        caption=(f"🧠 Prediction — {pair} ({tf})\nNext {horizon} bars: {label}  (P(up)={conf}%)\n"
                 f"As of: {asof_str}\n{explanation}")
        if cons is not None:
            caption+=f"\nConsensus: {cons:.2f} from {used} exchanges"
            if spread is not None: caption+=f" (spread {spread:.2f})"
            caption+="\nLive tick plotted on chart"
        await update.message.reply_photo(png, caption=caption)
    except Exception as e:
        logging.error("predict error: %s", e); await update.message.reply_text("Prediction error. Try again shortly.")

# NEW: compare up to 3 pairs
async def cmd_compare(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id): return await update.message.reply_text("Inactive. Use /subscribe.")
    args=[a.strip().upper() for a in ctx.args] if ctx.args else []
    if not args:
        return await update.message.reply_text("Usage: /compare PAIR1 [PAIR2] [PAIR3] [TF]\nExample: /compare BTC/USDT ETH/USDT SOL/USDT daily")
    # Detect TF as last token if it looks like one
    tf=None
    if args:
        maybe_tf=args[-1].lower()
        if any(maybe_tf.endswith(suf) for suf in ("m","h","d","w","M")) or maybe_tf in ("daily","weekly","monthly","day","week","month"):
            tf=_normalize_tf(maybe_tf); args=args[:-1]
    pairs= [a for a in args if "/" in a][:3]
    if not pairs:
        return await update.message.reply_text("Please include at least one PAIR like BTC/USDT.")
    tf=tf or TIMEFRAME
    # Fetch each df
    dfs=[]
    for p in pairs:
        df=await engine.fetch_df(p, timeframe=tf)
        if df.empty: await update.message.reply_text(f"No data for {p} on {tf}. Skipping.")
        dfs.append(df)
    if all(d.empty for d in dfs):
        return await update.message.reply_text("No data available for the requested pairs/timeframe.")
    png=plot_compare_chart(pairs, dfs, tf)
    last_times=[d["time"].iloc[-1] for d in dfs if not d.empty]
    asof = max(last_times) if last_times else pd.Timestamp.now(tz=LOCAL_TZ)
    caption=(f"Comparison — {', '.join(pairs)}  ({tf})\n"
             f"As of: {asof.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
             f"Series rebased to 100 at start for fair trend comparison.")
    await update.message.reply_photo(png, caption=caption)

# ——— scheduler ———
engine = Engine(EXCHANGE, TIMEFRAME, db_get_pairs())
async def scheduled_job(app: "Application"):
    try:
        for pair in engine.valid_pairs:
            res, df = await engine.analyze(pair)
            if not res: continue
            mark, text, price, ts = res
            await broadcast(text, app)
            try:
                if "sma50" not in df.columns: df["sma50"]=df["close"].rolling(50).mean()
                if "sma200" not in df.columns: df["sma200"]=df["close"].rolling(200).mean()
                cons,used,spread,_=await price_agg.get_consensus(pair)
                png=plot_signal_chart(pair, df, mark, price, title_tf=None, last_tick=cons)
                last_dt=df["time"].iloc[-1]; asof_str=last_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                caption=text+f"\nAs of: {asof_str}"
                if cons is not None:
                    caption+=f"\nConsensus: {cons:.2f} from {used} exchanges"
                    if spread is not None: caption+=f" (spread {spread:.2f})"
                    caption+="\nLive tick plotted on chart"
                if png: await broadcast_chart(app, caption=caption, png_buf=png)
            except Exception as ce:
                logging.warning("chart send failed: %s", ce)
        await asyncio.sleep(0.1)
    except Exception as e:
        logging.error("scheduled_job crashed: %s", e); logging.error("Traceback:\n%s", traceback.format_exc())
def schedule_jobs(app: "Application", scheduler: AsyncIOScheduler):
    scheduler.add_job(scheduled_job, "interval", minutes=1, args=[app],
                      next_run_time=datetime.now(timezone.utc))

# ——— FastAPI + PTB ———
logging.basicConfig(level=logging.INFO)
request = HTTPXRequest(connect_timeout=20, read_timeout=30, write_timeout=20, pool_timeout=20)
application = Application.builder().token(BOT_TOKEN).request(request).build()

from contextlib import asynccontextmanager
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await application.initialize(); await application.start()
    except Exception as e:
        logging.error("Telegram init/start failed: %s", e)
    await engine.init_markets()
    await set_bot_commands(application)
    scheduler=AsyncIOScheduler(); schedule_jobs(application, scheduler); scheduler.start()
    if PUBLIC_URL:
        url=f"{PUBLIC_URL.rstrip('/')}/webhook"
        try:
            await application.bot.set_webhook(url)
            logging.info(f"Webhook set to {url}")
        except Exception as e:
            logging.warning("Could not set webhook now: %s", e)
    yield
    try:
        await application.stop(); await application.shutdown()
    except Exception: pass

api = FastAPI(lifespan=lifespan)

@api.api_route("/", methods=["GET","HEAD"])
def health(): return {"ok": True}
@api.get("/robots.txt")
def robots(): return Response("User-agent: *\nDisallow:\n", media_type="text/plain")
@api.get("/favicon.ico")
def favicon(): return Response(status_code=204)
@api.get("/pairs")
def list_pairs(): return {"requested": db_get_pairs(), "active_on_exchange": engine.valid_pairs}
@api.get("/runjob")
async def run_job_now():
    try: await scheduled_job(application); return {"ok": True}
    except Exception as e: return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
@api.get("/setwebhook")
async def set_webhook():
    if not PUBLIC_URL: return JSONResponse({"ok": False, "error": "Set PUBLIC_URL env first"}, status_code=400)
    url=f"{PUBLIC_URL.rstrip('/')}/webhook"; await application.bot.set_webhook(url)
    return {"ok": True, "webhook": url}
@api.post("/webhook")
async def telegram_webhook(req: Request):
    data=await req.json(); update=Update.de_json(data, application.bot)
    await application.process_update(update); return {"ok": True}

# handlers
application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("help", cmd_help))
application.add_handler(CommandHandler("subscribe", cmd_subscribe))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("signalson", cmd_signalson))
application.add_handler(CommandHandler("signalsoff", cmd_signalsoff))
application.add_handler(CommandHandler("grantme", cmd_grantme))
application.add_handler(CommandHandler("chart", cmd_chart))
application.add_handler(CommandHandler("chart_daily", cmd_chart_daily))
application.add_handler(CommandHandler("chart_weekly", cmd_chart_weekly))
application.add_handler(CommandHandler("chart_monthly", cmd_chart_monthly))
application.add_handler(CommandHandler("live", cmd_live))
application.add_handler(CommandHandler("predict", cmd_predict))
application.add_handler(CommandHandler("compare", cmd_compare))
application.add_handler(CommandHandler("pairs", cmd_pairs))
application.add_handler(CommandHandler("listpairs", cmd_listpairs))
application.add_handler(CommandHandler("addpair", cmd_addpair))
application.add_handler(CommandHandler("removepair", cmd_removepair))
application.add_handler(PreCheckoutQueryHandler(precheckout))
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=int(os.getenv("PORT","8000")))
