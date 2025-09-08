import os
import time
import asyncio
import logging
import sqlite3
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# Third-party libs
import ccxt
import pandas as pd
import numpy as np

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

from telegram import Update, LabeledPrice
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    PreCheckoutQueryHandler, MessageHandler, filters
)

# -----------------------------
# ENV
# -----------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
PAIR      = os.getenv("PAIR", "BTC/USDT")
EXCHANGE  = os.getenv("EXCHANGE", "binance")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "10000"))   # default ≈ $10
PUBLIC_URL = os.getenv("PUBLIC_URL")  # e.g. https://your-app.onrender.com

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
# STRATEGY (pandas + numpy) with guards
# -----------------------------
class Engine:
    def __init__(self, exchange_id, pair, timeframe):
        self.ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.pair = pair
        self.tf = timeframe
        self.last = None

    async def fetch_df(self, limit=300):
        def get_ohlcv():
            return self.ex.fetch_ohlcv(self.pair, timeframe=self.tf, limit=limit)

        try:
            ohlcv = await asyncio.to_thread(get_ohlcv)
        except Exception as e:
            logging.warning("fetch_ohlcv failed: %s", e)
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

        if not ohlcv or len(ohlcv) < 210:
            logging.info("Not enough candles: %s", len(ohlcv) if ohlcv else 0)
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

    async def signal(self):
        df = await self.fetch_df()
        if df.empty or len(df) < 200:
            return None

        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["rsi"] = self.rsi(df["close"], 14)

        row, prev = df.iloc[-1], df.iloc[-2]

        long_cond = (row.sma50 > row.sma200) and (prev.rsi < 45 <= row.rsi)
        exit_cond = (row.rsi > 65) or (row.sma50 < row.sma200)

        price = row.close
        ts = row.time.strftime("%Y-%m-%d %H:%M UTC")

        if long_cond and self.last != "LONG":
            self.last = "LONG"
            return f"LONG {self.pair} @ {price:.2f} [{self.tf}]  ({ts})\nSMA50>SMA200; RSI crossed up from <45."
        if exit_cond and self.last != "EXIT":
            self.last = "EXIT"
            return f"EXIT/NEUTRAL {self.pair} @ {price:.2f} [{self.tf}]  ({ts})\nRSI>65 or SMA50<SMA200."
        return None

engine = Engine(EXCHANGE, PAIR, TIMEFRAME)

# -----------------------------
# TELEGRAM HANDLERS
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
        currency="XTR",             # Telegram Stars currency
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

async def broadcast(text: str, app: Application):
    rows = cur.execute(
        "SELECT user_id FROM users WHERE expires_at > ? AND signals_on = 1",
        (now_ts(),)
    ).fetchall()
    for (uid,) in rows:
        try:
            await app.bot.send_message(uid, text)
        except Exception as e:
            logging.warning(f"send fail {uid}: {e}")

# -----------------------------
# SCHEDULER (with robust logging)
# -----------------------------
async def scheduled_job(app: Application):
    try:
        sig = await engine.signal()
        if sig:
            await broadcast(sig, app)
    except Exception as e:
        logging.error("scheduled_job crashed: %s", e)
        logging.error("Traceback:\n%s", traceback.format_exc())

def schedule_jobs(app: Application, scheduler: AsyncIOScheduler):
    scheduler.add_job(
        scheduled_job,
        "interval",
        minutes=5,
        args=[app],
        next_run_time=datetime.now(timezone.utc)  # run ASAP, then every 5 min
    )

# -----------------------------
# FASTAPI + TELEGRAM APP
# -----------------------------
logging.basicConfig(level=logging.INFO)
application = Application.builder().token(BOT_TOKEN).build()

# Telegram handlers
application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("subscribe", cmd_subscribe))
application.add_handler(CommandHandler("status", cmd_status))
application.add_handler(CommandHandler("cancel", cmd_cancel))
application.add_handler(CommandHandler("signalson", cmd_signalson))
application.add_handler(CommandHandler("signalsoff", cmd_signalsoff))
application.add_handler(PreCheckoutQueryHandler(precheckout))
application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

# FastAPI app
api = FastAPI()

@api.get("/")
def health():
    return {"ok": True}

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
    await application.bot.set_webhook(url)
    return {"ok": True, "webhook": url}

@api.post("/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

# Lifecycle
@api.on_event("startup")
async def on_startup():
    await application.initialize()
    await application.start()
    scheduler = AsyncIOScheduler()
    schedule_jobs(application, scheduler)
    scheduler.start()
    if PUBLIC_URL:
        url = f"{PUBLIC_URL.rstrip('/')}/webhook"
        await application.bot.set_webhook(url)
        logging.info(f"Webhook set to {url}")

@api.on_event("shutdown")
async def on_shutdown():
    # APScheduler is created in startup; if you need a global ref, promote it.
    await application.stop()
    await application.shutdown()

if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
