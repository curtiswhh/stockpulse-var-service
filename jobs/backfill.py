"""
Backfill Job — updated for rolling daily VaR.

Steps per ticker:
  1. Fetch price data from Polygon (1000 days)
  2. Upsert into price_history
  3. Read back full price history from Supabase
  4. Compute rolling daily VaR (simple percentile)
  5. Upsert results into var_calculations_precomputed
"""

import logging
from datetime import date, timedelta

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.polygon_client import PolygonClient
from services.var_engine import VaREngine

logger = logging.getLogger(__name__)


class BackfillJob:
    def __init__(self, supabase, polygon, var_engine, settings):
        self.supabase = supabase
        self.polygon = polygon
        self.var_engine = var_engine
        self.settings = settings

    async def backfill_ticker(self, ticker: str):
        """Fetch prices from Polygon, upsert, then compute and store rolling VaR."""
        logger.info(f"Backfilling {ticker}...")
        today = date.today()
        earliest_needed = today - timedelta(days=self.settings.var_max_backfill_days + 30)
        latest_in_db = self.supabase.get_latest_price_date(ticker)

        if latest_in_db and latest_in_db >= today - timedelta(days=5):
            from_date = latest_in_db + timedelta(days=1)
            logger.info(f"  {ticker}: filling gap from {from_date}")
        else:
            from_date = earliest_needed
            logger.info(f"  {ticker}: full backfill from {from_date}")

        # ── Step 1: Fetch price data from Polygon in yearly chunks ──
        current_start = from_date
        all_bars: list[dict] = []
        while current_start <= today:
            chunk_end = min(current_start + timedelta(days=365), today)
            bars = await self.polygon.get_daily_bars(ticker, current_start, chunk_end)
            all_bars.extend(bars)
            current_start = chunk_end + timedelta(days=1)

        # ── Step 2: Upsert price data ──
        if all_bars:
            self.supabase.upsert_price_history(all_bars)
            logger.info(f"  {ticker}: loaded {len(all_bars)} daily bars")
        else:
            logger.warning(f"  {ticker}: no data returned from Polygon")
            return

        # ── Step 3–5: Compute and store VaR ──
        await self.compute_var_ticker(ticker)

    async def compute_var_ticker(self, ticker: str):
        """Read prices from Supabase, compute rolling VaR, write back results.
        No Polygon calls — works purely from data already in Supabase."""
        logger.info(f"Computing rolling VaR for {ticker}...")

        # Read full price history from Supabase
        prices = self.supabase.get_full_price_history(ticker)

        if len(prices) < self.settings.var_lookback_days + 2:
            logger.warning(
                f"  {ticker}: insufficient data for VaR "
                f"({len(prices)} rows, need {self.settings.var_lookback_days + 2})"
            )
            return

        # Compute rolling VaR
        var_rows = self.var_engine.compute_rolling_var(
            ticker=ticker,
            prices=prices,
            confidence_level=self.settings.var_confidence_level,
            lookback_days=self.settings.var_lookback_days,
        )

        # Upsert into var_calculations_precomputed
        if var_rows:
            self.supabase.upsert_var_calculations(var_rows)
            logger.info(f"  {ticker}: upserted {len(var_rows)} VaR rows")

    async def compute_var_all(self):
        """Compute rolling VaR for all configured tickers using existing Supabase data."""
        if self.settings.test_mode:
            tickers = list(self.settings.test_tickers)
            logger.info(f"TEST MODE: computing VaR for {len(tickers)} tickers")
        else:
            rows = self.supabase.get_sp500_tickers()
            tickers = [r["ticker"] for r in rows if r.get("is_active", True)]
            logger.info(f"PRODUCTION: computing VaR for {len(tickers)} tickers")

        for i, ticker in enumerate(tickers):
            logger.info(f"[{i+1}/{len(tickers)}] {ticker}")
            await self.compute_var_ticker(ticker)

        logger.info(f"VaR computation complete — {len(tickers)} tickers processed.")

    async def full_backfill(self):
        """In test mode this only backfills the test tickers."""
        if self.settings.test_mode:
            tickers = list(self.settings.test_tickers)
            logger.info(f"TEST MODE: backfilling {len(tickers)} test tickers only")
        else:
            rows = self.supabase.get_sp500_tickers()
            tickers = [r["ticker"] for r in rows if r.get("is_active", True)]
            logger.info(f"PRODUCTION: backfilling {len(tickers)} tickers")

        for i, ticker in enumerate(tickers):
            logger.info(f"[{i+1}/{len(tickers)}] {ticker}")
            await self.backfill_ticker(ticker)

        logger.info(f"Backfill complete — {len(tickers)} tickers processed.")
