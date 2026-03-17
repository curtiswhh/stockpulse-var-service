"""
Backfill Job — Polygon fetch and VaR computation.
========================================================

METHOD NAMING CONVENTION:
  fetch_and_compute_var_*  → calls Polygon API for prices, THEN computes VaR
  compute_var_*            → computes VaR ONLY from existing Supabase data (NO Polygon)

TICKER RESOLUTION:
  Methods ending in "_all" resolve tickers by querying all distinct tickers
  in the price_history table. This means any ticker you've uploaded price
  data for will automatically be included — no config changes needed.

  The seed_tickers in settings.py are NOT used here. They exist only for
  standalone testing / cold-start seeding and are separate from these commands.
"""

import logging
from datetime import date, timedelta

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.var_engine import VaREngine

logger = logging.getLogger(__name__)


class BackfillJob:
    def __init__(self, supabase, price_client, var_engine, settings, correlation_engine=None):
        self.supabase = supabase
        self.price_client = price_client
        self.var_engine = var_engine
        self.settings = settings
        self.correlation_engine = correlation_engine

    # ══════════════════════════════════════════════════════════
    # FETCH + COMPUTE VAR (fetches price data via yfinance/Polygon)
    # ══════════════════════════════════════════════════════════

    async def fetch_and_compute_var(self, ticker: str):
        """
        Fetch prices + compute VaR for ONE ticker.
        CLI: python main.py --fetch-and-compute-var TSLA

        Steps:
          1. Fetch ~1000 days of daily bars (yfinance → Polygon fallback)
          2. Upsert into price_history in Supabase
          3. Read back full price history from Supabase
          4. Compute rolling daily VaR
          5. Upsert results into var_calculations_precomputed
        """
        logger.info(f"[fetch-and-compute-var] {ticker}: starting...")
        today = date.today()
        earliest_needed = today - timedelta(days=self.settings.var_max_backfill_days + 30)
        latest_in_db = self.supabase.get_latest_price_date(ticker)

        if latest_in_db and latest_in_db >= today - timedelta(days=5):
            from_date = latest_in_db + timedelta(days=1)
            logger.info(f"  {ticker}: filling gap from {from_date}")
        else:
            from_date = earliest_needed
            logger.info(f"  {ticker}: full fetch from {from_date}")

        # ── Step 1: Fetch price data in yearly chunks ────────────
        current_start = from_date
        all_bars: list[dict] = []
        while current_start <= today:
            chunk_end = min(current_start + timedelta(days=365), today)
            bars = await self.price_client.get_daily_bars(ticker, current_start, chunk_end)
            all_bars.extend(bars)
            current_start = chunk_end + timedelta(days=1)

        # ── Step 2: Upsert price data ──
        if all_bars:
            self.supabase.upsert_price_history(all_bars)
            logger.info(f"  {ticker}: loaded {len(all_bars)} daily bars")
        else:
            logger.warning(f"  {ticker}: no data returned from any source")
            return

        # ── Steps 3-5: Compute and store VaR ──
        await self.compute_var(ticker)

    async def fetch_and_compute_var_all(self):
        """
        Fetch prices from Polygon + compute VaR for ALL tickers in price_history.
        CLI: python main.py --fetch-and-compute-var-all

        Resolves tickers from: SELECT DISTINCT ticker FROM price_history
        """
        tickers = self.supabase.get_all_price_history_tickers()
        logger.info(f"[fetch-and-compute-var-all] Processing {len(tickers)} tickers from price_history: {tickers}")

        for i, ticker in enumerate(tickers):
            logger.info(f"[{i+1}/{len(tickers)}] {ticker}")
            await self.fetch_and_compute_var(ticker)

        logger.info(f"Fetch + VaR complete — {len(tickers)} tickers processed.")

    # ══════════════════════════════════════════════════════════
    # COMPUTE VAR ONLY (NO Polygon calls)
    # ══════════════════════════════════════════════════════════

    async def compute_var(self, ticker: str):
        """
        Compute VaR for ONE ticker from existing Supabase price data.
        CLI: python main.py --compute-var TSLA

        No Polygon calls — works purely from data already in price_history.
        Use this after you've manually uploaded prices to Supabase.

        Requires at least 254 rows (252 lookback + 2) in price_history
        for the ticker, otherwise VaR cannot be computed.
        """
        logger.info(f"[compute-var] {ticker}: computing rolling VaR...")

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
        """
        Compute VaR for ALL tickers that have price data in Supabase.
        CLI: python main.py --compute-var-all

        No Polygon calls — works purely from existing data.
        Resolves tickers from: SELECT DISTINCT ticker FROM price_history
        """
        tickers = self.supabase.get_all_price_history_tickers()
        logger.info(f"[compute-var-all] Processing {len(tickers)} tickers from price_history: {tickers}")

        for i, ticker in enumerate(tickers):
            logger.info(f"[{i+1}/{len(tickers)}] {ticker}")
            await self.compute_var(ticker)

        logger.info(f"VaR computation complete — {len(tickers)} tickers processed.")

    # ══════════════════════════════════════════════════════════
    # COMPUTE CORRELATIONS ONLY (NO Polygon calls)
    # ══════════════════════════════════════════════════════════

    async def compute_correlations_all(self):
        """
        Compute rolling pairwise correlations for ALL tickers in price_history.
        CLI: python main.py --compute-correlations-all

        No Polygon calls — works purely from existing data.
        Resolves tickers from: SELECT DISTINCT ticker FROM price_history
        """
        if self.correlation_engine is None:
            logger.error("CorrelationEngine not provided — cannot compute correlations")
            return

        tickers = self.supabase.get_all_price_history_tickers()
        logger.info(f"[compute-correlations-all] Processing {len(tickers)} tickers from price_history: {tickers}")

        # Load all price histories
        min_prices_needed = int(
            min(self.settings.correlation_lookback_periods)
            * self.settings.correlation_min_overlap_pct
        ) + 2

        all_prices: dict[str, list[dict]] = {}
        for ticker in tickers:
            prices = self.supabase.get_full_price_history(ticker)
            if len(prices) >= min_prices_needed:
                all_prices[ticker] = prices
            else:
                logger.info(f"  {ticker}: only {len(prices)} prices — skipping (need {min_prices_needed})")

        logger.info(f"  {len(all_prices)} tickers have sufficient data")

        if len(all_prices) < 2:
            logger.warning("  Not enough tickers for pairwise correlations — skipping")
            return

        for period in self.settings.correlation_lookback_periods:
            logger.info(f"  Computing {period}-day rolling correlations...")
            corr_rows = self.correlation_engine.compute_rolling_correlations(
                all_prices=all_prices,
                period_days=period,
                min_overlap_pct=self.settings.correlation_min_overlap_pct,
            )
            if corr_rows:
                self.supabase.upsert_global_correlations(corr_rows)

        logger.info(f"Correlation computation complete — {len(all_prices)} tickers processed.")