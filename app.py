import os
import io
import time
import asyncio
import logging
import sqlite3
import traceback
from datetime import datetime, timezone

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

from telegram import Update, LabeledPrice
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PreCheckoutQueryHandler, MessageHandler, filters
)

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EXCHANGE  = os.getenv("EXCHANGE", "binanceus")
TIMEFRAME = os.getenv("TIMEFRAME", "1m")            # fast cadence
PAIR      = os.getenv("PAIR", "BTC/USDT")           # legacy single pair
PAIRS_ENV = os.getenv("PAIRS", PAIR)                # multiple pairs support
PAIRS     = [p.strip() for p in PAIRS_ENV.split(",") if p.strip()]

STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "10000"))  # ≈ $10
PUBLIC_URL = os.getenv("PUBLIC_URL")  # e.g. https://your-app.onrender.com

# Owner for /grantme (your numeric Telegram ID)
OWNER_ID = 5467277042

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
        """Load exchange markets and filter requested pairs to those actually supported."""
        def _load():
            self.ex.load_markets()
            return set(self.ex.symbols or [])
        try:
            symbols = await asyncio.to_thread(_load)
        except Exception as e:
            logging.warning("Could not load markets for %s: %s", self.ex.id, e)
            # fallback: try all (fetch_df will guard per pair)
            self.valid_pairs = list(self.requested_pairs)
            return

        wanted = set(self.requested_pairs)
        self.valid_pairs = sorted(list(wanted & symbols))
        skipped = sorted(list(wanted - symbols))
        if skipped:
            logging.info("Skipping unsupported pairs on %s: %s", self.ex.id, ", ".join(skipped))
        if not self.valid_pairs:
            logging.warning("No valid pairs found on %s from requested: %s", self.ex.id, ", ".join(self.requested_pairs))

    async def fetch_df(self, pair, limit=300):
        def get_ohlcv():
            return self.ex.fetch_ohlcv(pair, timeframe=self.tf, limit=limit)
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
# CHARTS
# -----------------------------
def plot_signal_chart(pair: str, df: pd.DataFrame, mark: str | None, price: float | None):
    """Return a BytesIO PNG of Close + SMA50/200 and optional BUY/SELL marker."""
    dfp = df.tail(200).copy()
    if dfp.empty:
        return None

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=140)
    # Price
    ax.plot(dfp["time"], dfp["close"], linewidth=1.2, label="Close")
    # SMAs (ensure present)
    if "sma50" not in dfp:  dfp["sma50"]  = dfp["close"].rolling(50).mean()
    if "sma200" not in dfp: dfp["sma200"] = dfp["close"].rolling(200).mean()
    ax.plot(dfp["time"], dfp["sma50"], linewidth=1.0, label="SMA50")
    ax.plot(dfp["time"], dfp["sma200"], linewidth=1.0, label="SMA200")

    # Marker
    if mark and price:
        x = dfp["time"].iloc[-1]
        y = price
        label = "BUY" if mark == "LONG" else "SELL"
        ax.scatter([x], [y], s=50)
        ax.annotate(label, (x, y), xytext=(10, 10), textcoords="offset points")

    ax.set_title(f"{pair} — {TIMEFRAME}  (UTC)")
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
def active_users():
    return cur.execute(
        "SELECT user_id FROM users WHERE expires_at > ? AND signals_on = 1",
        (now_ts(),)
    ).fetchall()

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

# On-demand chart: "/chart" or "/chart ETH/USDT"
async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_active(update.effective_user.id):
        return await update.message.reply_text("Inactive. Use /subscribe.")
    pair = " ".join(ctx.args).strip().upper() if ctx.args else (engine.valid_pairs[0] if engine.valid_pairs else PAIRS[0])
    try:
        df = await engine.fetch_df(pair)
        if df.empty:
            return await update.message.reply_text(f"No data for {pair}. Try later.")
        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        png = plot_signal_chart(pair, df, None, None)
        if not png:
            return await update.message.reply_text("Could not render chart right now.")
        await update.message.reply_photo(png, caption=f"{pair} — {TIMEFRAME}")
    except Exception as e:
        logging.error("chart error: %s", e)
        await update.message.reply_text("Chart error. Try again shortly.")

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
            # 2) chart image
            try:
                if "sma50" not in df.columns: df["sma50"] = df["close"].rolling(50).mean()
                if "sma200" not in df.columns: df["sma200"] = df["close"].rolling(200).mean()
                png = plot_signal_chart(pair, df, mark, price)
                if png:
                    await broadcast_chart(app, caption=text, png_buf=png)
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
# FASTAPI + TELEGRAM APP
# -----------------------------
logging.basicConfig(level=logging.INFO)

# Longer network timeouts to avoid startup failures
request = HTTPXRequest(connect_timeout=20, read_timeout=30, write_timeout=20, pool_timeout=20)
application = Application.builder().token(BOT_TOKEN).request(request).build()

# Telegram handlers
application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("subscribe", cmd_subscribe))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("cancel", cmd_cancel))
application.add_handler(CommandHandler("signalson", cmd_signalson))
application.add_handler(CommandHandler("signalsoff", cmd_signalsoff))
application.add_handler(CommandHandler("grantme", cmd_grantme))
application.add_handler(CommandHandler("chart", cmd_chart))
application.add_handler(CommandHandler("pairs", cmd_pairs))
application.add_handler(PreCheckoutQueryHandler(precheckout))
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

# FastAPI app
api = FastAPI()

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
    await application.bot.set_webhook(url)   # ← remove timeout arg
    return {"ok": True, "webhook": url}

@api.post("/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

from contextlib import asynccontextmanager

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
            await application.bot.set_webhook(url)
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

# Then create the FastAPI app like this (replace the old api = FastAPI()):
api = FastAPI(lifespan=lifespan)
