import os, math, io, asyncio, logging, sqlite3
from typing import Optional, Tuple, List, Dict
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request
from telegram import Update, BotCommand, InputFile
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import ccxt

# ─────────────── ENV CONFIG ───────────────
BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
EXCHANGE    = os.getenv("EXCHANGE", "kucoin")
EXCHANGES   = [e.strip() for e in os.getenv("EXCHANGES", EXCHANGE).split(",") if e.strip()]
TIMEFRAME   = os.getenv("TIMEFRAME", "1m")
DISPLAY_TZ  = os.getenv("DISPLAY_TZ", "UTC")
LOCAL_TZ    = ZoneInfo(DISPLAY_TZ)
PUBLIC_URL  = os.getenv("PUBLIC_URL", "").strip()
OWNER_ID    = int(os.getenv("OWNER_ID", "0"))
MAX_SCOUT   = int(os.getenv("MAX_SCOUT", "25"))

# ─────────────── DISCLAIMER ───────────────
DISCLAIMER_TEXT = (
    "⚠️ *Disclaimer*\n\n"
    "This bot and all information provided are for *educational and entertainment purposes only*. "
    "Nothing here should be considered financial, investment, or trading advice.\n\n"
    "Cryptocurrency trading is highly volatile and involves significant risk. "
    "You could lose some or all of your capital. "
    "Always do your own research and consult with a licensed financial advisor before making investment decisions.\n\n"
    "By using this bot, you acknowledge that you are solely responsible for your trading decisions "
    "and agree that the creators of this bot are not liable for any losses or damages."
)

# ─────────────── LOGGING ───────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ─────────────── DB HELPERS ───────────────
DB_FILE = "bot.db"

def db_connect():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_param(strategy: str, key: str, default: str):
    conn = db_connect(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS params(strategy TEXT, key TEXT, val TEXT, PRIMARY KEY(strategy,key))")
    cur.execute("SELECT val FROM params WHERE strategy=? AND key=?", (strategy, key))
    row = cur.fetchone()
    conn.close()
    return row["val"] if row else default

def get_setting(key: str, default=None):
    conn = db_connect(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, val TEXT)")
    cur.execute("SELECT val FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["val"] if row else default

# ─────────────── STRATEGIES (external file) ───────────────
import strategies
from strategies import strategy_ma, strategy_rsi, strategy_scalp, strategy_event
strategies.set_param_getter(get_param)  # inject DB getter

# ─────────────── UTILS ───────────────
def _normalize_tf(tf: Optional[str]) -> str:
    if not tf:
        return TIMEFRAME
    t = tf.strip().lower()
    aliases = {
        "d": "1d", "day": "1d", "daily": "1d",
        "w": "1w", "wk": "1w", "weekly": "1w",
        "mo": "1M", "month": "1M", "monthly": "1M",
        "h": "1h", "hr": "1h", "hour": "1h",
        "min": "1m", "minute": "1m",
    }
    return aliases.get(t, t)

def _tf_seconds(tf: str) -> int:
    t = tf.lower()
    if t.endswith("m") and t != "1m" and not t.endswith("M"):  # minutes (except '1M' monthly)
        return int(t[:-1]) * 60
    if t.endswith("h"):
        return int(t[:-1]) * 3600
    if t.endswith("d"):
        return int(t[:-1]) * 86400
    if t.endswith("w"):
        return int(t[:-1]) * 604800
    if t.endswith("m") or t.endswith("M"):  # monthly
        return 30 * 86400
    if t == "1m": return 60
    return 60

def _is_stale(last_dt: pd.Timestamp, tf: str):
    sec = _tf_seconds(tf)
    now = pd.Timestamp.now(tz=LOCAL_TZ)
    return (now - last_dt).total_seconds() > 2 * sec

def _choose_time_axis(ax, tf: str):
    t = (tf or "1m").lower()
    if t == "1d":
        locator   = mdates.DayLocator(interval=1, tz=LOCAL_TZ)
        formatter = mdates.DateFormatter("%b %d\n%Y", tz=LOCAL_TZ)
    elif t == "1w":
        locator   = mdates.WeekdayLocator(byweekday=mdates.MO, tz=LOCAL_TZ)
        formatter = mdates.DateFormatter("Wk %W\n%Y", tz=LOCAL_TZ)
    elif t in ("1m","1M"):
        locator   = mdates.MonthLocator(tz=LOCAL_TZ)
        formatter = mdates.DateFormatter("%b\n%Y", tz=LOCAL_TZ)
    else:
        sec = _tf_seconds(t)
        if sec <= 3600:
            locator   = mdates.AutoDateLocator(minticks=5, maxticks=8, tz=LOCAL_TZ)
            formatter = mdates.DateFormatter("%H:%M\n%b-%d", tz=LOCAL_TZ)
        elif sec < 86400:
            locator   = mdates.AutoDateLocator(minticks=4, maxticks=7, tz=LOCAL_TZ)
            formatter = mdates.DateFormatter("%b-%d\n%H:%M", tz=LOCAL_TZ)
        else:
            locator   = mdates.AutoDateLocator(minticks=5, maxticks=8, tz=LOCAL_TZ)
            formatter = mdates.DateFormatter("%b %d\n%Y", tz=LOCAL_TZ)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

def _atr_local(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def _recommend_sl_tp(df: pd.DataFrame, entry: float, side: str = "long"):
    atr = _atr_local(df, 14).iloc[-1]
    if not math.isfinite(atr) or atr <= 0:
        atr = float(df["close"].rolling(14).std().iloc[-1] or 0.0)
        if atr <= 0: atr = max(0.002 * entry, 1e-8)
    R = 1.5
    if side == "short":
        sl  = entry + 1.5 * atr
        tp1 = entry - 1.0 * atr * R
        tp2 = entry - 2.0 * atr * R
    else:
        sl  = entry - 1.5 * atr
        tp1 = entry + 1.0 * atr * R
        tp2 = entry + 2.0 * atr * R
    return float(sl), float(tp1), float(tp2), float(atr), R

# ─────────────── PRICE AGGREGATOR (consensus) ───────────────
class PriceAggregator:
    def __init__(self, exchange_names: List[str]):
        self.ex_objs: Dict[str, ccxt.Exchange] = {}
        for name in exchange_names:
            if not hasattr(ccxt, name):
                continue
            try:
                self.ex_objs[name] = getattr(ccxt, name)()
            except Exception:
                pass

    async def get_consensus(self, pair: str):
        prices = []
        used = 0
        for name, ex in self.ex_objs.items():
            try:
                async def _run():
                    return ex.fetch_ticker(pair)
                t = await asyncio.to_thread(_run)
                px = t.get("last") or t.get("close") or t.get("bid") or t.get("ask")
                if px and math.isfinite(px):
                    prices.append(float(px)); used += 1
            except Exception:
                continue
        if not prices:
            return None, 0, None, {}
        cons = float(np.mean(prices))
        spread = float((max(prices) - min(prices))) if len(prices) > 1 else 0.0
        return cons, used, spread, {}
price_agg = PriceAggregator(EXCHANGES)

# ─────────────── ENGINE ───────────────
class Engine:
    def __init__(self, exchange="kucoin", tf="1m"):
        self.ex = getattr(ccxt, exchange)()
        self.tf = tf
        self.valid_pairs: List[str] = []

    async def init_markets(self):
        def _load():
            self.ex.load_markets()
            return list(self.ex.markets.keys())
        try:
            pairs = await asyncio.to_thread(_load)
            # keep popular quote markets
            self.valid_pairs = [p for p in pairs if any(p.endswith(q) for q in ("/USDT","/USD","/USDC"))]
            self.valid_pairs.sort()
        except Exception as e:
            log.warning("init_markets failed: %s", e)
            self.valid_pairs = ["BTC/USDT","ETH/USDT","SOL/USDT"]

    def _resample(self, df: pd.DataFrame, tf: str) -> pd.DataFrame:
        if df.empty: return df
        d = df.copy().set_index("time")
        rule = None
        if tf == "1d":
            rule = "1D"
        elif tf == "1w":
            rule = "1W-MON"
        elif tf in ("1m","1M"):
            rule = "1MS"
        else:
            return d.reset_index()
        o = d["open"].resample(rule).first()
        h = d["high"].resample(rule).max()
        l = d["low"].resample(rule).min()
        c = d["close"].resample(rule).last()
        v = d["volume"].resample(rule).sum()
        out = pd.DataFrame({"open":o,"high":h,"low":l,"close":c,"volume":v}).dropna().reset_index()
        return out

    async def fetch_df(self, pair: str, timeframe: Optional[str] = None, limit: int = 400) -> pd.DataFrame:
        tf = _normalize_tf(timeframe or self.tf)
        # try native tf first
        def _get(tf_in, lim):
            return self.ex.fetch_ohlcv(pair, timeframe=tf_in, limit=lim)
        try:
            ohlcv = await asyncio.to_thread(_get, tf, limit)
        except Exception as e:
            log.warning("fetch_ohlcv failed (%s %s): %s", pair, tf, e)
            ohlcv = []
        df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"]) if ohlcv else pd.DataFrame()
        if not df.empty:
            df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
            df = df.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)

        # fallback/resample for higher TFs if stale or empty
        if tf in ("1d","1w","1M"):
            need_resample = True
            if not df.empty:
                last_dt = df["time"].iloc[-1]
                need_resample = _is_stale(last_dt, tf)
                if tf == "1d":
                    need_resample |= (last_dt.date() != pd.Timestamp.now(tz=LOCAL_TZ).date())
                elif tf == "1w":
                    now = pd.Timestamp.now(tz=LOCAL_TZ)
                    need_resample |= (last_dt.isocalendar()[:2] != now.isocalendar()[:2])
            if need_resample:
                base = "1h" if tf == "1d" else "1d"
                try:
                    raw = await asyncio.to_thread(_get, base, 1500 if base=="1h" else 400)
                    bdf = pd.DataFrame(raw, columns=["ts","open","high","low","close","volume"])
                    bdf["time"] = pd.to_datetime(bdf["ts"], unit="ms", utc=True).dt.tz_convert(LOCAL_TZ)
                    bdf = bdf.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
                    df = self._resample(bdf, tf)
                except Exception as e:
                    log.warning("resample fallback failed (%s %s): %s", pair, tf, e)

        return df if not df.empty else pd.DataFrame(columns=["time","open","high","low","close","volume"])

    async def predict(self, pair: str, horizon: int = 5, timeframe: Optional[str] = None):
        df = await self.fetch_df(pair, timeframe)
        if df.empty or len(df) < 60:
            return None, None, None, df
        # very simple placeholder classifier based on short momentum
        look = min(10, max(5, len(df)//10))
        prob = float(df["close"].iloc[-1] / df["close"].iloc[-look] - 1.0)
        p_up = 1/(1+math.exp(-10*prob))
        label = "BUY" if p_up >= 0.5 else "SELL"
        explanation = f"mom({look})={(prob*100):.2f}%"
        return label, p_up, explanation, df

    async def analyze(self, pair: str):
        df = await self.fetch_df(pair, timeframe=self.tf)
        if df.empty or len(df) < 200:
            return None, df
        strat = (get_setting("strategy", "ma") or "ma").lower()
        sig, reason = None, None
        if strat == "ma":
            sig, reason = strategy_ma(df)
        elif strat == "rsi":
            sig, reason = strategy_rsi(df)
        elif strat == "scalp":
            sig, reason = strategy_scalp(df)
        elif strat == "event":
            sig, reason = strategy_event(df)
        else:
            sig, reason = strategy_ma(df); strat = "ma"
        if not sig:
            return None, df
        row = df.iloc[-1]
        price = float(row["close"])
        ts = row["time"].strftime("%Y-%m-%d %H:%M %Z")
        text = f"{sig} {pair} @ {price:.4g} [{self.tf}]  ({ts})\nStrategy: {strat.upper()} — {reason}"
        return (sig, text, price, ts), df
        # ─────────────── FASTAPI + TELEGRAM ───────────────
api = FastAPI()
engine = Engine(exchange=EXCHANGE, tf=TIMEFRAME)
application = Application.builder().token(BOT_TOKEN).build()
scheduler = AsyncIOScheduler()

# ─────────────── BASIC CHART HELPER ───────────────
def _plot_basic_candle(df: pd.DataFrame, pair: str, tf: str, live_price: float | None = None) -> io.BytesIO:
    fig, ax = plt.subplots(figsize=(10,5), dpi=140)
    ax.plot(df["time"], df["close"], linewidth=1.2)
    if live_price is not None and math.isfinite(live_price):
        ax.plot([df["time"].iloc[-1]], [live_price], marker="o")
        ax.annotate(f"{live_price:.6g}", (df["time"].iloc[-1], live_price),
                    xytext=(10, -10), textcoords="offset points")
    _choose_time_axis(ax, tf)
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.set_title(f"{pair} — {tf}")
    ax.set_ylabel("Price")
    fig.autofmt_xdate()
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf

# ─────────────── COMMANDS ───────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(DISCLAIMER_TEXT, parse_mode="Markdown")
    await update.message.reply_text("👋 Welcome to Midnight Crypto Bot Trading!\nUse /help to see commands.")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/pairs — list tracked pairs\n"
        "/chart <PAIR> [TF] — show chart (e.g. /chart BTC/USDT 1d)\n"
        "/scout [TF] [HORIZON] [TOPN] — best longs\n"
        "/scout_short [TF] [HORIZON] [TOPN] — best shorts\n"
        "/scout_best [TF] [HORIZON] [TOPN] — best overall\n"
        "/calc <entry> <exit> [size] — profit/SL/TP helper\n"
        "/dca_plan <budget> <n> — split buys\n"
        "/debug_tf <PAIR> [TF] — inspect candles\n"
        "/foundation_check — data/time checks"
    )

async def cmd_pairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not engine.valid_pairs:
        await engine.init_markets()
    pairs = engine.valid_pairs[:50] if engine.valid_pairs else ["BTC/USDT","ETH/USDT","SOL/USDT"]
    await update.message.reply_text("Available pairs (sample):\n" + "\n".join(pairs))

async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a.strip().upper() for a in (ctx.args or [])]
    pair = args[0] if args else "BTC/USDT"
    tf = _normalize_tf(args[1]) if len(args) > 1 else TIMEFRAME
    df = await engine.fetch_df(pair, tf)
    if df.empty:
        return await update.message.reply_text(f"No data for {pair} on {tf}.")
    cons, used, spread, _ = await price_agg.get_consensus(pair)
    last_dt = df["time"].iloc[-1]
    img = _plot_basic_candle(df, pair, tf, live_price=cons)
    cap = (f"{pair} — {tf}\n"
           f"As of (last closed candle): {last_dt:%Y-%m-%d %H:%M %Z}")
    if cons is not None:
        cap += f"\nConsensus: {cons:.6g} from {used} exchanges"
        if spread and spread > 0:
            cap += f" (spread {spread:.6g})"
        cap += "\nLive tick plotted on chart"
    await update.message.reply_photo(InputFile(img, filename="chart.png"), caption=cap)

async def cmd_debug_tf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a.strip().upper() for a in (ctx.args or [])]
    pair = args[0] if args else "BTC/USDT"
    tf = _normalize_tf(args[1]) if len(args) > 1 else "1d"
    df = await engine.fetch_df(pair, tf)
    if df.empty:
        return await update.message.reply_text(f"No data for {pair} on {tf}.")
    first = df["time"].iloc[0]; last = df["time"].iloc[-1]
    now = pd.Timestamp.now(tz=LOCAL_TZ)
    await update.message.reply_text(
        f"{pair} ({tf}) candles: {len(df)}\n"
        f"First: {first:%Y-%m-%d %H:%M %Z}\n"
        f"Last:  {last:%Y-%m-%d %H:%M %Z}\n"
        f"Now:   {now:%Y-%m-%d %H:%M %Z}\n"
        f"Stale? {'YES' if _is_stale(last, tf) else 'NO'}"
    )

# ── SCOUT HELPERS ──
def _eta_from_horizon(horizon_bars: int, tf: str) -> str:
    secs = _tf_seconds(tf) * max(1, int(horizon_bars))
    eta  = pd.Timestamp.now(tz=LOCAL_TZ) + pd.Timedelta(seconds=secs)
    return eta.strftime("%Y-%m-%d %H:%M %Z")

async def _score_long(pair: str, tf: str, horizon: int):
    label, prob, expl, df = await engine.predict(pair, horizon=horizon, timeframe=tf)
    if df is None or df.empty or len(df) < 120:
        return None
    try:
        d = df.copy()
        d["vol_ma"] = d["volume"].rolling(20).mean()
        look = min(10, max(5, len(d)//10))
        mom = float(d["close"].iloc[-1] / d["close"].iloc[-look] - 1.0)
        volx = float(d["volume"].iloc[-1] / (d["vol_ma"].iloc[-1] + 1e-9))
    except Exception:
        mom, volx = 0.0, 1.0
    cons, used, spread, _ = await price_agg.get_consensus(pair)
    last_close = float(df["close"].iloc[-1])
    px = float(cons) if (cons is not None and math.isfinite(cons)) else last_close
    sl, tp1, tp2, atr, R = _recommend_sl_tp(df, entry=px, side="long")
    p = float(prob or 0.0)
    score = (0.75 * p) + (0.20 * max(mom, -0.10)) + (0.05 * min(volx/3.0, 1.0))
    return dict(pair=pair, prob=p, label=label, score=score, close=last_close,
                cons=(float(cons) if cons is not None else None), used=used, spread=spread,
                mom=mom, volx=volx, sl=sl, tp1=tp1, tp2=tp2, atr=atr, explanation=expl)

async def _score_short(pair: str, tf: str, horizon: int):
    label, prob_up, expl, df = await engine.predict(pair, horizon=horizon, timeframe=tf)
    if df is None or df.empty or len(df) < 120:
        return None
    try:
        d = df.copy()
        d["vol_ma"] = d["volume"].rolling(20).mean()
        look = min(10, max(5, len(d)//10))
        mom = float(d["close"].iloc[-1] / d["close"].iloc[-look] - 1.0)
        volx = float(d["volume"].iloc[-1] / (d["vol_ma"].iloc[-1] + 1e-9))
    except Exception:
        mom, volx = 0.0, 1.0
    cons, used, spread, _ = await price_agg.get_consensus(pair)
    last_close = float(df["close"].iloc[-1])
    px = float(cons) if (cons is not None and math.isfinite(cons)) else last_close
    sl, tp1, tp2, atr, R = _recommend_sl_tp(df, entry=px, side="short")
    p_down = 1.0 - float(prob_up or 0.0)
    neg_m = max(-mom, 0.0)
    score = (0.75 * p_down) + (0.20 * neg_m) + (0.05 * min(volx/3.0, 1.0))
    return dict(pair=pair, prob=p_down, label=label, score=score, close=last_close,
                cons=(float(cons) if cons is not None else None), used=used, spread=spread,
                mom=mom, volx=volx, sl=sl, tp1=tp1, tp2=tp2, atr=atr, explanation=expl)

# ── SCOUT COMMANDS ──
async def cmd_scout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a.strip().lower() for a in (ctx.args or [])]
    tf = TIMEFRAME; horizon = 5; topn = 5
    for a in list(args):
        if any(a.endswith(s) for s in ("m","h","d","w","M")) or a in ("daily","weekly","monthly","day","week","month"):
            tf = _normalize_tf(a); args.remove(a)
    for a in list(args):
        if a.isdigit():
            if horizon == 5: horizon = max(1, min(60, int(a))); args.remove(a)
            elif topn == 5: topn = max(1, min(25, int(a))); args.remove(a)
    if not engine.valid_pairs:
        await engine.init_markets()
    pool = engine.valid_pairs or ["BTC/USDT","ETH/USDT","SOL/USDT"]
    scan_list = pool[:MAX_SCOUT]
    status = await update.message.reply_text(f"🔎 Scouting up to {len(scan_list)} pairs on {tf} (h={horizon})…")
    results = []
    sem = asyncio.Semaphore(6)
    async def _task(p): 
        async with sem:
            try:
                r = await _score_long(p, tf, horizon)
                if r: results.append(r)
            except Exception as e:
                log.warning("scout %s failed: %s", p, e)
    await asyncio.gather(*[_task(p) for p in scan_list])
    if not results: 
        return await status.edit_text("No usable data right now. Try a different TF.")
    for r in results:
        if r["label"] == "SELL": r["score"] *= 0.3
    results.sort(key=lambda r: r["score"], reverse=True)
    picks = results[:topn]
    lines = [f"🧭 Scout — {tf}  (h={horizon}) • {len(results)} scored / {len(scan_list)} scanned",
             "PAIR         P(up)   MOM%   VOLx   Px        SL        TP1       TP2"]
    for r in picks:
        mom_pct = r["mom"] * 100.0
        cons_px = r["cons"] if r["cons"] is not None else r["close"]
        lines.append(f"{r['pair']:<12} {r['prob']*100:>6.1f}%  {mom_pct:>6.2f}%  {r['volx']:>4.2f}x  "
                     f"{cons_px:>8.4g}  {r['sl']:>8.4g}  {r['tp1']:>8.4g}  {r['tp2']:>8.4g}")
    eta = _eta_from_horizon(horizon, tf)
    best = picks[0]
    foot = ["", f"Best candidate: {best['pair']} — P(up)={best['prob']*100:.1f}%", f"Est. review window around: {eta}",
            "Notes: P(up) from model; MOM short-term; VOLx vs 20-bar avg. SL/TP via ATR(14)."]
    await status.edit_text("\n".join(lines + foot))

async def cmd_scout_short(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a.strip().lower() for a in (ctx.args or [])]
    tf = TIMEFRAME; horizon = 5; topn = 5
    for a in list(args):
        if any(a.endswith(s) for s in ("m","h","d","w","M")) or a in ("daily","weekly","monthly","day","week","month"):
            tf = _normalize_tf(a); args.remove(a)
    for a in list(args):
        if a.isdigit():
            if horizon == 5: horizon = max(1, min(60, int(a))); args.remove(a)
            elif topn == 5: topn = max(1, min(25, int(a))); args.remove(a)
    if not engine.valid_pairs:
        await engine.init_markets()
    pool = engine.valid_pairs or ["BTC/USDT","ETH/USDT","SOL/USDT"]
    scan_list = pool[:MAX_SCOUT]
    status = await update.message.reply_text(f"🔎 Short scout: up to {len(scan_list)} pairs on {tf} (h={horizon})…")
    results = []
    sem = asyncio.Semaphore(6)
    async def _task(p):
        async with sem:
            try:
                r = await _score_short(p, tf, horizon)
                if r: results.append(r)
            except Exception as e:
                log.warning("scout_short %s failed: %s", p, e)
    await asyncio.gather(*[_task(p) for p in scan_list])
    if not results:
        return await status.edit_text("No usable data right now. Try a different TF.")
    for r in results:
        if r["label"] == "BUY": r["score"] *= 0.3
    results.sort(key=lambda r: r["score"], reverse=True)
    picks = results[:topn]
    lines = [f"🧭 Short scout — {tf}  (h={horizon}) • {len(results)} scored / {len(scan_list)} scanned",
             "PAIR         P(down) MOM%   VOLx   Px        SL        TP1       TP2"]
    for r in picks:
        mom_pct = r["mom"] * 100.0
        cons_px = r["cons"] if r["cons"] is not None else r["close"]
        lines.append(f"{r['pair']:<12} {r['prob']*100:>6.1f}%  {mom_pct:>6.2f}%  {r['volx']:>4.2f}x  "
                     f"{cons_px:>8.4g}  {r['sl']:>8.4g}  {r['tp1']:>8.4g}  {r['tp2']:>8.4g}")
    eta = _eta_from_horizon(horizon, tf)
    best = picks[0]
    foot = ["", f"Best short: {best['pair']} — P(down)={best['prob']*100:.1f}%", f"Est. review window around: {eta}",
            "Notes: P(down)=1−P(up). MOM negative preferred. SL/TP via ATR(14)."]
    await status.edit_text("\n".join(lines + foot))

async def cmd_scout_best(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a.strip().lower() for a in (ctx.args or [])]
    tf = TIMEFRAME; horizon = 5; topn = 5
    for a in list(args):
        if any(a.endswith(s) for s in ("m","h","d","w","M")) or a in ("daily","weekly","monthly","day","week","month"):
            tf = _normalize_tf(a); args.remove(a)
    for a in list(args):
        if a.isdigit():
            if horizon == 5: horizon = max(1, min(60, int(a))); args.remove(a)
            elif topn == 5: topn = max(1, min(25, int(a))); args.remove(a)
    if not engine.valid_pairs:
        await engine.init_markets()
    pool = engine.valid_pairs or ["BTC/USDT","ETH/USDT","SOL/USDT"]
    scan_list = pool[:MAX_SCOUT]
    status = await update.message.reply_text(f"🔎 Best scout: up to {len(scan_list)} pairs on {tf} (h={horizon})…")
    results = []
    sem = asyncio.Semaphore(6)
    async def _both(p):
        async with sem:
            try: L = await _score_long(p, tf, horizon)
            except Exception as e: log.warning("best long %s: %s", p, e); L=None
            try: S = await _score_short(p, tf, horizon)
            except Exception as e: log.warning("best short %s: %s", p, e); S=None
            pick = None
            if L and S: pick = (L if L["score"] >= S["score"] else S)
            else: pick = L or S
            if pick:
                pick = pick.copy()
                pick["direction"] = ("LONG" if pick is L else "SHORT") if (L and S) else ("LONG" if L else "SHORT")
                results.append(pick)
    await asyncio.gather(*[_both(p) for p in scan_list])
    if not results:
        return await status.edit_text("No usable data right now. Try a different TF.")
    results.sort(key=lambda r: r["score"], reverse=True)
    picks = results[:topn]
    lines = [f"🧭 Best scout — {tf}  (h={horizon}) • {len(results)} kept / {len(scan_list)} scanned",
             "PAIR         DIR    P(±)   MOM%   VOLx   Px        SL        TP1       TP2"]
    for r in picks:
        cons_px = r["cons"] if r["cons"] is not None else r["close"]
        lines.append(f"{r['pair']:<12} {r.get('direction','?'):<5}  {r['prob']*100:>6.1f}%  "
                     f"{r['mom']*100:>6.2f}%  {r['volx']:>4.2f}x  {cons_px:>8.4g}  "
                     f"{r['sl']:>8.4g}  {r['tp1']:>8.4g}  {r['tp2']:>8.4g}")
    eta = _eta_from_horizon(horizon, tf)
    best = picks[0]
    p_label = "P(up)" if best.get("direction") == "LONG" else "P(down)"
    foot = ["", f"Top: {best['pair']} — {best.get('direction')} • {p_label}={best['prob']*100:.1f}%",
            f"Est. review window around: {eta}",
            "Notes: MOM short-term; VOLx vs 20-bar avg. SL/TP via ATR(14)."]
    await status.edit_text("\n".join(lines + foot))

# ── CALC & DCA ──
async def cmd_calc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a for a in (ctx.args or [])]
    if len(args) < 2:
        return await update.message.reply_text("Usage: /calc <entry_price> <exit_price> [size]\nExample: /calc 2.50 3.10 1000")
    entry = float(args[0]); exitp = float(args[1]); size = float(args[2]) if len(args) > 2 else 1.0
    pnl = (exitp - entry) * size
    rr = (exitp - entry) / (entry if entry else 1.0)
    side = "LONG" if exitp >= entry else "SHORT"
    await update.message.reply_text(
        f"Side: {side}\nEntry: {entry:.6g}\nExit: {exitp:.6g}\nSize: {size:.6g}\n"
        f"PnL: {pnl:.6g}\nΔ%: {rr*100:.2f}%"
    )

async def cmd_dca_plan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = [a for a in (ctx.args or [])]
    if len(args) < 2:
        return await update.message.reply_text("Usage: /dca_plan <budget> <n_orders>\nExample: /dca_plan 1000 5")
    budget = float(args[0]); n = max(1, int(args[1]))
    per = budget / n
    lines = [f"DCA plan: total ${budget:.2f} across {n} orders:", *[f"• Order {i+1}: ${per:.2f}" for i in range(n)]]
    await update.message.reply_text("\n".join(lines))

# ── FOUNDATION CHECK ──
async def cmd_foundation_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pairs = ["BTC/USDT","ETH/USDT","SOL/USDT"]
    tfs   = ["1m","1h","1d","1w"]
    report = ["🔧 Foundation Check:"]
    for p in pairs:
        line = [f"\n{p}:"]
        for tf in tfs:
            df = await engine.fetch_df(p, tf)
            ok = (not df.empty)
            last = df["time"].iloc[-1] if ok else None
            stale = _is_stale(last, tf) if ok else True
            tag = "PASS" if (ok and not stale) else "CHECK"
            when = (last.strftime("%Y-%m-%d %H:%M %Z") if last is not None else "—")
            line.append(f"  {tf:<3} {tag:<5} last={when}")
        report.append("\n".join(line))
    await update.message.reply_text("\n".join(report))

# ─────────────── REGISTER COMMANDS ───────────────
application.add_handler(CommandHandler("start", cmd_start))
application.add_handler(CommandHandler("help", cmd_help))
application.add_handler(CommandHandler("pairs", cmd_pairs))
application.add_handler(CommandHandler("chart", cmd_chart))
application.add_handler(CommandHandler("debug_tf", cmd_debug_tf))
application.add_handler(CommandHandler("scout", cmd_scout))
application.add_handler(CommandHandler("scout_short", cmd_scout_short))
application.add_handler(CommandHandler("scout_best", cmd_scout_best))
application.add_handler(CommandHandler("calc", cmd_calc))
application.add_handler(CommandHandler("dca_plan", cmd_dca_plan))
application.add_handler(CommandHandler("foundation_check", cmd_foundation_check))

# ─────────────── SCHEDULER ───────────────
import traceback

async def scheduled_job():
    try:
        # Make sure markets are loaded (safe to call repeatedly)
        if not engine.valid_pairs:
            await engine.init_markets()

        # Pick a small, safe subset to analyze
        candidates = [p for p in engine.valid_pairs if p.endswith(("/USDT","/USD","/USDC"))][:5]
        if not candidates:
            candidates = ["BTC/USDT","ETH/USDT","SOL/USDT"]

        for pair in candidates:
            try:
                res, df = await engine.analyze(pair)
                if res:
                    log.info("Signal: %s", res[1])
            except Exception as e:
                log.error("analyze(%s) failed: %s\n%s", pair, e, traceback.format_exc())

    except Exception as e:
        log.error("scheduled_job top-level crash: %s\n%s", e, traceback.format_exc())
        # re-raise if you want APScheduler to record it as an error (optional)
        # raise

# Replace your add_job line with this (no lambda wrapper; let APScheduler await the coro)
scheduler.add_job(
    scheduled_job,
    trigger="interval",
    seconds=60,
    coalesce=True,
    max_instances=1,
    misfire_grace_time=30,
)

# ─────────────── FASTAPI WEBHOOK ───────────────
@api.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

@api.get("/")
async def root():
    return {"ok": True, "bot": "Midnight Crypto Bot Trading"}

# ─────────────── LIFECYCLE ───────────────
@api.on_event("startup")
async def on_startup():
    log.info("API startup")
    await engine.init_markets()
    await application.bot.set_my_commands([
        BotCommand("start","Start"),
        BotCommand("help","Help"),
        BotCommand("pairs","List pairs"),
        BotCommand("chart","Chart pair"),
        BotCommand("debug_tf","Debug candles"),
        BotCommand("scout","Scan longs"),
        BotCommand("scout_short","Scan shorts"),
        BotCommand("scout_best","Scan best overall"),
        BotCommand("calc","Profit calculator"),
        BotCommand("dca_plan","DCA planner"),
        BotCommand("foundation_check","Data/time checks"),
    ])
    await application.initialize()
    await application.start()
    if PUBLIC_URL:
        try:
            await application.bot.set_webhook(url=f"{PUBLIC_URL}/webhook")
            log.info("Webhook set to %s/webhook", PUBLIC_URL)
        except TypeError:
            # for older PTB versions missing 'timeout' kw
            await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
    scheduler.start()

@api.on_event("shutdown")
async def on_shutdown():
    log.info("API shutdown")
    scheduler.shutdown(wait=False)
    await application.stop()

if __name__ == "__main__":
    import uvicorn, os
    uvicorn.run("app:api", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), workers=1)
