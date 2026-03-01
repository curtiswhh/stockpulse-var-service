"""
Daily Pipeline — TEST MODE version.

Key difference: the _resolve_tickers() method checks settings.test_mode.
  - test_mode=True  → only processes settings.test_tickers
  - test_mode=False → processes all active S&P 500 constituents
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
        diff = await self.sp500.refresh()

        # ── Step 2: Resolve which tickers to process ──────────
        tickers = self._resolve_tickers(diff["added"])
        logger.info(f"Step 2/5: Processing {len(tickers)} tickers: {tickers[:10]}{'...' if len(tickers) > 10 else ''}")

        # ── Step 3: Backfill any tickers missing data ─────────
        backfill = BackfillJob(
            supabase=self.supabase,
            polygon=self.polygon,
            var_engine=self.var_engine,
            settings=self.settings,
        )
        for ticker in tickers:
            latest = self.supabase.get_latest_price_date(ticker)
            if latest is None:
                logger.info(f"  {ticker}: no data found, running backfill...")
                await backfill.backfill_ticker(ticker)

        # ── Step 4: Fetch today's prices ──────────────────────
        logger.info(f"Step 3/5: Fetching today's closing prices for {len(tickers)} tickers...")
        await self._fetch_daily_prices(tickers, today)

        # ── Step 5: Compute VaR ───────────────────────────────
        logger.info(f"Step 4/5: Computing VaR for {len(tickers)} tickers...")
        await self._compute_var_all(tickers)

        logger.info(f"═══ Daily Pipeline Complete ({mode} MODE) ═══")

    # ── Core Logic ────────────────────────────────────────────

    def _resolve_tickers(self, newly_added_to_sp500: list[str]) -> list[str]:
        if self.settings.test_mode:
            tickers = list(self.settings.test_tickers)
            logger.info(f"  TEST MODE: using {len(tickers)} hardcoded tickers")
            return tickers
        else:
            tickers = self.sp500.get_active_tickers()
            logger.info(f"  PRODUCTION MODE: using {len(tickers)} S&P 500 tickers")
            return tickers

    async def _fetch_daily_prices(self, tickers: list[str], today: date):
        from_date = today - timedelta(days=5)
        for i, ticker in enumerate(tickers):
            logger.info(f"  [{i+1}/{len(tickers)}] Fetching prices for {ticker}...")
            bars = await self.polygon.get_daily_bars(ticker, from_date, today)
            if bars:
                self.supabase.upsert_price_history(bars)

    async def _compute_var_all(self, tickers: list[str]):
        for i, ticker in enumerate(tickers):
            logger.info(f"  [{i+1}/{len(tickers)}] Computing VaR for {ticker}...")
            prices = self.supabase.get_full_price_history(ticker)

            if len(prices) < self.settings.var_lookback_days + 2:
                logger.warning(f"  {ticker}: only {len(prices)} prices — skipping VaR")
                continue

            var_rows = self.var_engine.compute_rolling_var(
                ticker=ticker,
                prices=prices,
                confidence_level=self.settings.var_confidence_level,
                lookback_days=self.settings.var_lookback_days,
            )
            if var_rows:
                self.supabase.upsert_var_calculations(var_rows)
