"""
Daily Pipeline — runs as the GitHub Actions cron job.
========================================================

TICKER RESOLUTION (how the pipeline decides which stocks to process):
  1. Query Supabase for all distinct tickers in price_history
  2. That's the list. Every ticker with price data gets processed.

  In PRODUCTION mode, S&P 500 tickers from sp500_constituents are also
  merged in, so newly added S&P 500 stocks get backfilled automatically.

  The seed_tickers in settings.py are NOT used by the daily pipeline.
  They exist only for standalone testing / cold-start seeding.

WHAT THE PIPELINE DOES FOR EACH TICKER:
  Step 1: Refresh S&P 500 constituent list (non-fatal if it fails)
  Step 2: Resolve tickers (from price_history + S&P 500 in production)
  Step 3: Backfill any ticker with no price data yet (full Polygon fetch)
  Step 4: Fetch latest 5 days of prices from Polygon (keeps data current)
  Step 5: Compute rolling VaR and upsert to var_calculations_precomputed
"""

import logging
from datetime import date, timedelta

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.polygon_client import PolygonClient
from services.sp500_tracker import SP500Tracker
from services.var_engine import VaREngine
from jobs.backfill import BackfillJob

logger = logging.getLogger(__name__)


class DailyPipeline:
    def __init__(
        self,
        settings: Settings,
        supabase: SupabaseClient,
        polygon: PolygonClient,
        sp500_tracker: SP500Tracker,
        var_engine: VaREngine,
    ):
        self.settings = settings
        self.supabase = supabase
        self.polygon = polygon
        self.sp500 = sp500_tracker
        self.var_engine = var_engine

    async def run(self):
        today = date.today()
        mode = "TEST" if self.settings.test_mode else "PRODUCTION"
        logger.info(f"═══ Daily Pipeline ({mode} MODE) — {today.isoformat()} ═══")
        logger.info(self.settings.describe())

        # ── Step 1: Refresh S&P 500 list ──────────────────────
        logger.info("Step 1/5: Refreshing S&P 500 constituents...")
        try:
            diff = await self.sp500.refresh()
        except Exception as e:
            logger.warning(f"  S&P 500 refresh failed (non-fatal): {e}")
            diff = {"added": [], "removed": [], "total": 0}

        # ── Step 2: Resolve which tickers to process ──────────
        tickers = self._resolve_tickers()
        logger.info(f"Step 2/5: Processing {len(tickers)} tickers: {tickers[:10]}{'...' if len(tickers) > 10 else ''}")

        # ── Step 3: Backfill any tickers missing data ─────────
        logger.info("Step 3/5: Checking for tickers that need backfill...")
        backfill = BackfillJob(
            supabase=self.supabase,
            polygon=self.polygon,
            var_engine=self.var_engine,
            settings=self.settings,
        )
        for ticker in tickers:
            latest = self.supabase.get_latest_price_date(ticker)
            if latest is None:
                logger.info(f"  {ticker}: no data found, running full backfill...")
                await backfill.fetch_and_compute_var(ticker)

        # ── Step 4: Fetch latest prices from Polygon ──────────
        logger.info(f"Step 4/5: Fetching latest closing prices for {len(tickers)} tickers...")
        await self._fetch_daily_prices(tickers, today)

        # ── Step 5: Compute VaR ───────────────────────────────
        logger.info(f"Step 5/5: Computing VaR for {len(tickers)} tickers...")
        await self._compute_var_all(tickers)

        logger.info(f"═══ Daily Pipeline Complete ({mode} MODE) ═══")

    # ── Core Logic ────────────────────────────────────────────

    def _resolve_tickers(self) -> list[str]:
        """
        Build the combined ticker list for the daily pipeline.

        Resolution logic:
          1. Always include all distinct tickers from price_history
             (this is the primary source — if it has price data, we process it)
          2. In PRODUCTION mode, also merge in active S&P 500 tickers
             (so newly added S&P 500 stocks get picked up for backfill)
          3. Return deduplicated, sorted list

        NOTE: seed_tickers from settings.py are NOT used here.
        The daily pipeline relies entirely on what's in the database.
        """
        # Primary source: every ticker that already has price data
        existing_in_db = set(self.supabase.get_all_price_history_tickers())
        logger.info(f"  Tickers in price_history: {sorted(existing_in_db)}")

        # In production mode, also include S&P 500 constituents
        # (these may not have price data yet — Step 3 will backfill them)
        if not self.settings.test_mode:
            sp500_tickers = set(self.sp500.get_active_tickers())
            logger.info(f"  PRODUCTION MODE: merging {len(sp500_tickers)} S&P 500 tickers")
            combined = sorted(existing_in_db | sp500_tickers)
        else:
            logger.info(f"  TEST MODE: using price_history tickers only")
            combined = sorted(existing_in_db)

        logger.info(f"  Final ticker list ({len(combined)}): {combined[:15]}{'...' if len(combined) > 15 else ''}")
        return combined

    async def _fetch_daily_prices(self, tickers: list[str], today: date):
        """Fetch the last 5 days of prices from Polygon for each ticker."""
        from_date = today - timedelta(days=5)
        for i, ticker in enumerate(tickers):
            logger.info(f"  [{i+1}/{len(tickers)}] Fetching prices for {ticker}...")
            bars = await self.polygon.get_daily_bars(ticker, from_date, today)
            if bars:
                self.supabase.upsert_price_history(bars)

    async def _compute_var_all(self, tickers: list[str]):
        """Compute rolling VaR for each ticker and upsert to var_calculations_precomputed."""
        for i, ticker in enumerate(tickers):
            logger.info(f"  [{i+1}/{len(tickers)}] Computing VaR for {ticker}...")
            prices = self.supabase.get_full_price_history(ticker)

            if len(prices) < self.settings.var_lookback_days + 2:
                logger.warning(f"  {ticker}: only {len(prices)} prices — skipping VaR (need {self.settings.var_lookback_days + 2})")
                continue

            var_rows = self.var_engine.compute_rolling_var(
                ticker=ticker,
                prices=prices,
                confidence_level=self.settings.var_confidence_level,
                lookback_days=self.settings.var_lookback_days,
            )
            if var_rows:
                self.supabase.upsert_var_calculations(var_rows)