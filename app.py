import os, time, asyncio, logging
from datetime import datetime, timezone

# load .env locally
from dotenv import load_dotenv
load_dotenv()

# DB + scheduling
from sqlitedict import SqliteDict
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# STRATEGY LIBS
import pandas as pd
import numpy as np
import ccxt

# FastAPI server (health check)
from fastapi import FastAPI
import uvicorn

# Telegram
from telegram import Update, LabeledPrice
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters
)

# ----------------------------
# ENV + DB utils (above engine)
# ----------------------------

# ----------------------------
# STRATEGY ENGINE (goes here)
# ----------------------------
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
        up = np.where(delta > 0, delta, 0.0)
        down = np.where(delta < 0, -delta, 0.0)
        roll_up = pd.Series(up).rolling(period).mean()
        roll_down = pd.Series(down).rolling(period).mean()
        rs = roll_up / (roll_down + 1e-9)
        return 100.0 - (100.0 / (1.0 + rs))

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
            return f"LONG {self.pair} @ {price:.2f} [{self.tf}] ({ts})"
        if exit_cond and self.last != "EXIT":
            self.last = "EXIT"
            return f"EXIT/NEUTRAL {self.pair} @ {price:.2f} [{self.tf}] ({ts})"
        return None
# ----------------------------
# END ENGINE
# ----------------------------

# everything else continues below...
