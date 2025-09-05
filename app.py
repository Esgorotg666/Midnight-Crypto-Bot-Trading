import os, time, asyncio, logging
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from sqlitedict import SqliteDict
from apscheduler.schedulers.asyncio import AsyncIOScheduler


import ccxt

from fastapi import FastAPI
import uvicorn

from telegram import Update, LabeledPrice
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters
)

# ------------ ENV ------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
PAIR      = os.getenv("PAIR", "BTC/USDT")
EXCHANGE  = os.getenv("EXCHANGE", "binance")
TIMEFRAME = os.getenv("TIMEFRAME", "15m")
STARS_PRICE_XTR = int(os.getenv("STARS_PRICE_XTR", "12000"))  # ~ $20

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

# ------------ DB ------------
DB = SqliteDict("subs.sqlite", autocommit=True)  # key: str(user_id) -> {expires_at:int, signals_on:bool}

def now_ts() -> int: return int(time.time())
def is_active(uid: int) -> bool: return DB.get(str(uid), {}).get("expires_at", 0) > now_ts()
def set_expiry(uid: int, expires_at: int):
    rec = DB.get(str(uid), {})
    rec["expires_at"] = expires_at
    DB[str(uid)] = rec
def set_opt(uid: int, on: bool):
    rec = DB.get(str(uid), {})
    rec["signals_on"] = on
    DB[str(uid)] = rec
def wants(uid: int) -> bool: return DB.get(str(uid), {}).get("signals_on", True)

# ------------ Strategy ------------
class Engine:
    def __init__(self, exchange_id, pair, timeframe):
        self.ex = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        self.pair = pair
        self.tf = timeframe
        self.last = None

    async def fetch_df(self, limit=300):
        def get_ohlcv():
            return self.ex.fetch_ohlcv(self.pair, timeframe=self.tf, limit=limit)
        ohlcv = await asyncio.to_thread(get_ohlcv)
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df

    @staticmethod
    def rsi(series, period=14):
        delta = series.diff()
        up = pd.Series(np.where(delta > 0, delta, 0.0)).rolling(period).mean()
        down = pd.Series(np.where(delta < 0, -delta, 0.0)).rolling(period).mean()
        rs = up / (down + 1e-9)
        return 100.0 - 100.0 / (1.0 + rs)

    async def signal(self):
        df = await self.fetch_df()
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
            return f"LONG {self.pair} @ {price:.2f} [{self.tf}]  ({ts})"
        if exit_cond and self.last != "EXIT":
            self.last = "EXIT"
            return f"EXIT/NEUTRAL {self.pair} @ {price:.2f} [{self.tf}]  ({ts})"
        return None

engine = Engine(EXCHANGE, PAIR, TIMEFRAME)

# ------------ Telegram ------------
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
        provider_token="",         # empty for Stars
        currency="XTR",            # Telegram Stars
        prices=prices,
        subscription_period=2592000  # 30 days
    )

async def precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def paid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    sp = update.message.successful_payment
    exp = sp.subscription_expiration_date or (now_ts() + 2592000)
    set_expiry(update.effective_user.id, exp)
    set_opt(update.effective_user.id, True)
    dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    await update.message.reply_text(f"Payment received. Active until {dt:%Y-%m-%d %H:%M UTC}. Use /signalson to enable alerts.")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_active(uid):
        exp = DB.get(str(uid), {}).get("expires_at")
        dt = datetime.fromtimestamp(exp, tz=timezone.utc)
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
    for k, rec in DB.items():
        try:
            uid = int(k)
            if rec.get("expires_at", 0) > now_ts() and rec.get("signals_on", True):
                await app.bot.send_message(uid, text)
        except Exception as e:
            logging.warning(f"send fail {k}: {e}")

async def scheduled(app: Application):
    try:
        s = await engine.signal()
        if s:
            await broadcast(s, app)
    except Exception as e:
        logging.error(f"job error: {e}")

# ------------ FastAPI (health) ------------
api = FastAPI()
@api.get("/")
def health(): return {"ok": True}

# ------------ Main ------------
async def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("signalson", cmd_signalson))
    application.add_handler(CommandHandler("signalsoff", cmd_signalsoff))
    application.add_handler(MessageHandler(filters.StatusUpdate.PRE_CHECKOUT_QUERY, precheckout))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, paid))

    scheduler = AsyncIOScheduler()
    scheduler.add_job(lambda: asyncio.create_task(scheduled(application)),
                      "interval", minutes=5, next_run_time=datetime.now(timezone.utc))
    scheduler.start()

    await application.initialize()
    await application.start()
    print("Bot started (polling).")

    # Run a web server in the background (keeps hosts happy)
    config = uvicorn.Config(api, host="0.0.0.0", port=int(os.getenv("PORT", "8000")), log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
