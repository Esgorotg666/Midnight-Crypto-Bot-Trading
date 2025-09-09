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

# Use a headless backend for matplotlib on servers
os.environ.setdefault("MPLBACKEND", "Agg")

from dotenv import load_dotenv
load_dotenv()

# Third-party libs
import ccxt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

from telegram import Update, LabeledPrice, InputMediaPhoto
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PreCheckoutQueryHandler, MessageHandler, filters
)

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EXCHANGE  = os.getenv("EXCHANGE", "binanceus")   # default primary exchange
TIMEFRAME = os.getenv("TIMEFRAME", "1m")         # fast cadence
PAIR      = os.getenv("PAIR", "BTC/USDT")        # legacy single pair
PAIRS_ENV = os.getenv("PAIRS", PAIR)             # multiple pairs support
PAIRS     = [p.strip() for p in PAIRS_ENV.split(",") if p.strip()]

EXCHANGES_ENV = os.getenv("EXCHANGES", EXCHANGE) # for consensus price
EX_LIST = [e.strip() for e in EXCHANGES_ENV.split(",") if e.strip()]

DISPLAY_TZ = os.getenv("DISPLAY_TZ", "UTC")      # <— NEW: chart display timezone

STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "10000"))  # ≈ $10
PUBLIC_URL = os.getenv("PUBLIC_URL")  # e.g. https://your-app.onrender.com

# Owner for /grantme (your numeric Telegram ID)
OWNER_ID = int(os.getenv("OWNER_ID", "5467277042"))

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

# -----------------------------
# SQLITE (built-in)
# -----------------------------
DB_PATH = "subs.sqlite"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  expires_at INTEGER NOT NULL DEFAULT 0,
  signals_on INTEGER NOT NULL DEFAULT 1
)
""")
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

def wants(uid: int) -> bool:
    row = cur.execute("SELECT signals_on FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row[0]) if row else True  # default True

def active_users():
    return cur.execute(
        "SELECT user_id FROM users WHERE expires_at > ? AND signals_on = 1",
        (now_ts(),)
    ).fetchall()

# -----------------------------
# Multi-exchange consensus price
# -----------------------------
def _mad(values):
    m = median(values)
    return median([abs(v - m) for v in values]) or 1e-9

class MultiPrice:
    def __init__(self, exchange_ids: list[str]):
        self.exes = []
        for ex in exchange_ids:
            try:
                self.exes.append(getattr(ccxt, ex)({"enableRateLimit": True}))
            except Exception:
                pass

    async def get_consensus(self, pair: str):
        async def one(ex):
            def _t():
                # Prefer fetch_ticker; fallback to last close via OHLCV(1)
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
        results = await asyncio.gather(*tasks, return_exceptions=True)

        raw, per_ex = [], {}
        for ex, val in zip(self.exes, results):
            if isinstance(val, Exception) or val is None or not math.isfinite(val):
                continue
            raw.append(val)
            per_ex[ex.id] = val

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
# STRATEGY ENGINE (multi-pair + valid-pair filtering)
# -----------------------------
class Engine:
    def __init__(self, exchange_id, timeframe, requested_pairs):
        self.ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.tf = timeframe
        self.requested_pairs = requested_pairs
        self.valid_pairs = []
        self.last = {}  # remember last signal per pair ("LONG"/"EXIT")

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
            logging.info("Skipping unsupported pairs on %s: %s", self.ex.id, ", ".join(skipped))
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
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)  # tz-aware UTC
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
        long_cond = (row.sma50 > row.sma200) and (prev.rsi < 45 <= row.rsi)
        exit_cond = (row.rsi > 65) or (row.sma50 < row.sma200)

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

# -----------------------------
# CHARTS (now with timezone & "NOW" line)
# -----------------------------
def plot_signal_chart(pair: str, df: pd.DataFrame, mark: str | None, price: float | None, title_tf: str | None = None):
    """Return a BytesIO PNG of Close + SMA50/200 and optional BUY/SELL marker, displayed in DISPLAY_TZ."""
    dfp = df.tail(200).copy()
    if dfp.empty:
        return None

    # Convert timestamps to display timezone
    try:
        dfp["time"] = dfp["time"].dt.tz_convert(DISPLAY_TZ)
    except Exception:
        # If tz conversion fails, keep UTC silently
        pass

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=140)

    # Price & SMAs
    ax.plot(dfp["time"], dfp["close"], linewidth=1.2, label="Close")
    if "sma50" not in dfp:  dfp["sma50"]  = dfp["close"].rolling(50).mean()
    if "sma200" not in dfp: dfp["sma200"] = dfp["close"].rolling(200).mean()
    ax.plot(dfp["time"], dfp["sma50"], linewidth=1.0, label="SMA50")
    ax.plot(dfp["time"], dfp["sma200"], linewidth=1.0, label="SMA200")

    # Marker for latest signal
    if mark and price:
        x = dfp["time"].iloc[-1]
        y = price
        label = "BUY" if mark == "LONG" else "SELL"
        ax.scatter([x], [y], s=50)
        ax.annotate(label, (x, y), xytext=(10, 10), textcoords="offset points")

    # "NOW" vertical line at the rightmost timestamp in the data
    try:
        ax.axvline(dfp["time"].iloc[-1], linewidth=0.7)
    except Exception:
        pass

    ax.set_title(f"{pair} — {title_tf or TIMEFRAME}  ({DISPLAY_TZ})")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.legend(loc="best")
    ax.grid(True, linewidth=0.3)

    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

# -----------------------------
# TELEGRAM HELPERS
# -----------------------------
async def broadcast(text: str, app: "Application"):
    for (uid,) in active_users():
        try:
            await app.bot.send_message(uid, text)
        except Exception as e:
            logging.warning(f"send fail {uid}: {e}")

async def broadcast_chart(app: "Application", caption: str, png_buf: io.BytesIO):
    for (uid,) in active_users():
        try:
            await app.bot.send_photo(uid, png_buf, caption=caption)
            png_buf.seek(0)  # reuse buffer
        except Exception as e:
            logging.warning(f"send photo fail {uid}: {e}")

# -----------------------------
# TELEGRAM COMMANDS
# -----------------------------
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if is_active(update.effective_user.id):
        await update.message.reply_text("You’re active. /signalson to receive alerts. /status shows expiry.")
    else:
        await update.message.reply_text("Access requires a sub. Tap /subscribe to pay with Telegram Stars (30 days).")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🛠 Commands\n"
        "/start – Welcome + subscription info\n"
        "/subscribe – Buy 30-day access with Telegram Stars\n"
        "/status – Check your expiry\n"
        "/cancel – Stop auto-renew (access stays until expiry)\n"
        "/signalson – Enable alerts here\n"
        "/signalsoff – Pause alerts\n"
        "/chart [PAIR] [TF] – Snapshot chart with markers (e.g. /chart, /chart ETH/USDT 5m)\n"
        "/live [PAIR] [TF] – Auto-refreshing chart every ~10s for ~2 min\n"
        "/pairs – Show requested vs active pairs on the exchange\n"
    )
    if update.effective_user.id == OWNER_ID:
        msg += "\nOwner: /grantme – grant yourself 30 days"
    await update.message.reply_text(msg)

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    title = "Midnight Crypto Bot Trading – 30 days"
    desc  = "SMA/RSI crypto signals. Educational only. Not financial advice."
    prices = [LabeledPrice(label="Access (30 days)", amount=STARS_PRICE_XTR)]
    await ctx.bot.send_invoice(
        chat_id=update.effective_chat.id,
        title=title,
        description=desc,
        payload=f"sub:{update.effective_user.id}:{now_ts()}",
        provider_token="",          # empty for Stars
        currency="XTR",             # Telegram Stars
        prices=prices,
        subscription_period=2592000 # 30 days
    )

async def precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def successful_payment(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    exp = sp.subscription_expiration_date or (now_ts() + 2592000)
    set_expiry(update.effective_user.id, exp)
    set_opt(update.effective_user.id, True)
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

# Owner-only: grant free sub
async def cmd_grantme(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Not authorized.")
    exp = now_ts() + 2592000
    set_expiry(update.effective_user.id, exp)
    set_opt(update.effective_user.id, True)
    dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    await update.message.reply_text(f"✅ Free subscription granted until {dt:%Y-%m-%d %H:%M UTC}.")

# /chart – snapshot with markers; optional [PAIR] [TF]
async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")

    args = [a.strip() for a in ctx.args] if ctx.args else []
    tf = None
    pair = None
    if args:
        maybe_tf = args[-1].lower()
        if maybe_tf.endswith(("m","h","d")) and any(ch.isdigit() for ch in maybe_tf):
            tf = maybe_tf
            args = args[:-1]
    if args:
        pair = " ".join(args).upper()

    pair = pair or (engine.valid_pairs[0] if engine.valid_pairs else PAIRS[0])
    tf = tf or TIMEFRAME

    try:
        df = await engine.fetch_df(pair, timeframe=tf)
        if df.empty:
            return await update.message.reply_text(f"No data for {pair} on {tf}. Try later.")

        # Indicators
        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["rsi"] = engine.rsi(df["close"], 14)

        # Latest signal
        mark = None
        price = None
        if len(df) >= 2:
            row, prev = df.iloc[-1], df.iloc[-2]
            long_cond = (row.sma50 > row.sma200) and (prev.rsi < 45 <= row.rsi)
            exit_cond = (row.rsi > 65) or (row.sma50 < row.sma200)
            if long_cond:
                mark, price = "LONG", float(row["close"])
            elif exit_cond:
                mark, price = "EXIT", float(row["close"])

        # Consensus + "As of"
        cons, used, spread, _ = await price_agg.get_consensus(pair)
        # Convert the last timestamp to display TZ for caption
        asof = df["time"].iloc[-1]
        try:
            asof = asof.tz_convert(DISPLAY_TZ)
        except Exception:
            pass
        asof_str = asof.strftime("%Y-%m-%d %H:%M")

        png = plot_signal_chart(pair, df, mark, price, title_tf=tf)

        caption = f"{pair} — {tf}"
        if mark == "LONG": caption += "  •  BUY signal"
        elif mark == "EXIT": caption += "  •  SELL/EXIT signal"
        caption += f"\nAs of: {asof_str} {DISPLAY_TZ}"
        if cons:
            caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
            if spread is not None:
                caption += f" (spread {spread:.2f})"

        await update.message.reply_photo(png, caption=caption)
    except Exception as e:
        logging.error("chart error: %s", e)
        await update.message.reply_text("Chart error. Try again shortly.")

# /live – refresh chart every ~10s for ~2min
async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")

    args = [a.strip() for a in ctx.args] if ctx.args else []
    tf = None
    pair = None
    if args:
        maybe_tf = args[-1].lower()
        if (maybe_tf.endswith(("m","h","d")) and any(ch.isdigit() for ch in maybe_tf)):
            tf = maybe_tf
            args = args[:-1]
    if args:
        pair = " ".join(args).upper()

    pair = pair or (engine.valid_pairs[0] if engine.valid_pairs else PAIRS[0])
    tf = tf or TIMEFRAME

    status_msg = await update.message.reply_text(f"Starting live chart for {pair} ({tf})…")
    photo_msg = None

    for i in range(12):  # ~2 minutes @ 10s per refresh
        try:
            df = await engine.fetch_df(pair, timeframe=tf)
            if df.empty:
                await status_msg.edit_text(f"No data for {pair} on {tf}.")
                break

            df["sma50"] = df["close"].rolling(50).mean()
            df["sma200"] = df["close"].rolling(200).mean()
            df["rsi"] = engine.rsi(df["close"], 14)

            mark = None
            price = None
            if len(df) >= 2:
                row, prev = df.iloc[-1], df.iloc[-2]
                long_cond = (row.sma50 > row.sma200) and (prev.rsi < 45 <= row.rsi)
                exit_cond = (row.rsi > 65) or (row.sma50 < row.sma200)
                if long_cond:
                    mark, price = "LONG", float(row["close"])
                elif exit_cond:
                    mark, price = "EXIT", float(row["close"])

            cons, used, spread, _ = await price_agg.get_consensus(pair)
            png = plot_signal_chart(pair, df, mark, price, title_tf=tf)

            # "As of" time in display TZ
            asof = df["time"].iloc[-1]
            try:
                asof = asof.tz_convert(DISPLAY_TZ)
            except Exception:
                pass
            asof_str = asof.strftime("%Y-%m-%d %H:%M")

            caption = f"{pair} — {tf}"
            if mark == "LONG": caption += "  •  BUY"
            elif mark == "EXIT": caption += "  •  SELL/EXIT"
            caption += f"\nAs of: {asof_str} {DISPLAY_TZ}"
            if cons:
                caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
                if spread is not None:
                    caption += f" (spread {spread:.2f})"

            if photo_msg is None:
                photo_msg = await update.message.reply_photo(png, caption=caption)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            else:
                png.seek(0)
                await ctx.bot.edit_message_media(
                    chat_id=photo_msg.chat_id,
                    message_id=photo_msg.message_id,
                    media=InputMediaPhoto(png, caption=caption)
                )
        except Exception as e:
            logging.warning("live update failed: %s", e)

        await asyncio.sleep(10)

    try:
        await update.message.reply_text(f"Live session ended for {pair} ({tf}).")
    except Exception:
        pass

# Telegram: show requested vs active pairs
async def cmd_pairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    requested = ", ".join(PAIRS) if PAIRS else "(none)"
    active = ", ".join(engine.valid_pairs) if getattr(engine, "valid_pairs", []) else "(loading… try again in ~10s)"
    msg = (
        "📊 Pairs\n"
        f"• Requested: {requested}\n"
        f"• Active on {EXCHANGE}: {active}"
    )
    await update.message.reply_text(msg)

# -----------------------------
# SCHEDULER (1-minute checks, chart on new signals)
# -----------------------------
engine = Engine(EXCHANGE, TIMEFRAME, PAIRS)

async def scheduled_job(app: "Application"):
    try:
        for pair in engine.valid_pairs:
            res, df = await engine.analyze(pair)
            if not res:
                continue
            mark, text, price, ts = res
            # 1) text signal
            await broadcast(text, app)
            # 2) chart image with consensus & "As of"
            try:
                if "sma50" not in df.columns: df["sma50"] = df["close"].rolling(50).mean()
                if "sma200" not in df.columns: df["sma200"] = df["close"].rolling(200).mean()
                cons, used, spread, _ = await price_agg.get_consensus(pair)
                png = plot_signal_chart(pair, df, mark, price, title_tf=None)

                # caption: add "As of" in display TZ
                asof = df["time"].iloc[-1]
                try:
                    asof = asof.tz_convert(DISPLAY_TZ)
                except Exception:
                    pass
                asof_str = asof.strftime("%Y-%m-%d %H:%M")

                caption = text + f"\nAs of: {asof_str} {DISPLAY_TZ}"
                if cons:
                    caption += f"\nConsensus: {cons:.2f} from {used} exchanges"
                    if spread is not None:
                        caption += f" (spread {spread:.2f})"

                if png:
                    await broadcast_chart(app, caption=caption, png_buf=png)
            except Exception as ce:
                logging.warning("chart send failed: %s", ce)
        await asyncio.sleep(0.1)
    except Exception as e:
        logging.error("scheduled_job crashed: %s", e)
        logging.error("Traceback:\n%s", traceback.format_exc())

def schedule_jobs(app: "Application", scheduler: AsyncIOScheduler):
    scheduler.add_job(
        scheduled_job,
        "interval",
        minutes=1,  # run every minute
        args=[app],
        next_run_time=datetime.now(timezone.utc)
    )

# -----------------------------
# FASTAPI + TELEGRAM APP (lifespan, no deprecation)
# -----------------------------
logging.basicConfig(level=logging.INFO)

# Longer network timeouts to avoid startup failures
request = HTTPXRequest(connect_timeout=20, read_timeout=30, write_timeout=20, pool_timeout=20)
application = Application.builder().token(BOT_TOKEN).request(request).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        await application.initialize()
        await application.start()
    except Exception as e:
        logging.error("Telegram init/start failed (API still boots): %s", e)

    await engine.init_markets()
    if not engine.valid_pairs:
        logging.warning("No valid pairs; scheduler will start but do nothing until markets load.")

    scheduler = AsyncIOScheduler()
    schedule_jobs(application, scheduler)
    scheduler.start()

    if PUBLIC_URL:
        url = f"{PUBLIC_URL.rstrip('/')}/webhook"
        try:
            await application.bot.set_webhook(url)  # PTB v21: no timeout arg
            logging.info(f"Webhook set to {url}")
        except Exception as e:
            logging.warning("Could not set webhook now: %s", e)

    yield  # -------- App is running --------

    # Shutdown
    try:
        await application.stop()
        await application.shutdown()
    except Exception:
        pass

api = FastAPI(lifespan=lifespan)

# Routes
@api.get("/")
def health():
    return {"ok": True}

@api.get("/pairs")
def list_pairs():
    return {"requested": PAIRS, "active_on_exchange": engine.valid_pairs}

@api.get("/runjob")
async def run_job_now():
    try:
        await scheduled_job(application)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@api.get("/setwebhook")
async def set_webhook():
    if not PUBLIC_URL:
        return JSONResponse({"ok": False, "error": "Set PUBLIC_URL env first"}, status_code=400)
    url = f"{PUBLIC_URL.rstrip('/')}/webhook"
    await application.bot.set_webhook(url)  # PTB v21: no timeout arg
    return {"ok": True, "webhook": url}

@api.post("/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

# Register Telegram handlers
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
application.add_handler(CommandHandler("pairs", cmd_pairs))
application.add_handler(PreCheckoutQueryHandler(precheckout))
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
