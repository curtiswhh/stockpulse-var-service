"""
Price Data Client — unified interface with yfinance primary, Polygon fallback.

This wraps both YFinanceClient and PolygonClient behind a single
get_daily_bars() interface. The rest of the pipeline doesn't need to
know which data source was used.

Strategy:
  1. Try yfinance first (no API key needed, supports ^GSPC, no rate limit)
  2. If yfinance fails or returns no data, fall back to Polygon
  3. If both fail, return empty list (caller handles gracefully)

The pipeline code (daily_pipeline.py, backfill.py) calls this client
instead of PolygonClient directly.
"""

import logging
from datetime import date

from config.settings import Settings
from services.yfinance_client import YFinanceClient
from services.polygon_client import PolygonClient

logger = logging.getLogger(__name__)


class PriceDataClient:
    def __init__(self, settings: Settings):
        self.yfinance = YFinanceClient()
        self.polygon = PolygonClient(settings)
        self._polygon_api_key = settings.polygon_api_key

    async def get_daily_bars(
        self, ticker: str, from_date: date, to_date: date,
    ) -> list[dict]:
        """
        Fetch daily OHLCV bars. Tries yfinance first, Polygon as fallback.

        Returns:
            list of dicts matching price_history schema.
        """
        # ── Primary: yfinance ─────────────────────────────────────
        try:
            bars = await self.yfinance.get_daily_bars(ticker, from_date, to_date)
            if bars and all(b.get("close") is not None for b in bars):
                return bars
            if bars:
                logger.warning(f"  {ticker}: yfinance returned bars with null prices, trying Polygon...")
            else:
                logger.warning(f"  {ticker}: yfinance returned no data, trying Polygon...")
        except Exception as e:
            logger.warning(f"  {ticker}: yfinance failed ({e}), trying Polygon...")

        # ── Fallback: Polygon ─────────────────────────────────────
        if not self._polygon_api_key:
            logger.error(f"  {ticker}: no Polygon API key — cannot fall back")
            return []

        try:
            bars = await self.polygon.get_daily_bars(ticker, from_date, to_date)
            if bars:
                return bars
            logger.warning(f"  {ticker}: Polygon also returned no data")
        except Exception as e:
            logger.error(f"  {ticker}: Polygon fallback also failed — {e}")

        return []