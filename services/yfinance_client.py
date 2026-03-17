"""
Yahoo Finance client — uses yfinance library for price data.

Same interface as PolygonClient: get_daily_bars(ticker, from_date, to_date)
returns a list of dicts matching the price_history table schema.

Key advantages over Polygon:
  - No API key required
  - No rate limit per minute (though Yahoo may throttle heavy use)
  - Supports index tickers like ^GSPC directly
  - Provides split-adjusted close prices via the 'Close' column

Notes:
  - yfinance `end` parameter is EXCLUSIVE (unlike Polygon which is inclusive).
    We add 1 day to to_date to compensate.
  - yfinance returns adjusted prices by default when auto_adjust=True (default).
    The 'Close' column IS the adjusted close in this mode.
"""

import asyncio
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)


class YFinanceClient:
    def __init__(self):
        # Import lazily so yfinance is only required when actually used
        try:
            import yfinance  # noqa: F401
            self._available = True
        except ImportError:
            logger.warning("yfinance not installed — YFinanceClient disabled")
            self._available = False

    async def get_daily_bars(
        self, ticker: str, from_date: date, to_date: date,
    ) -> list[dict]:
        """
        Fetch daily OHLCV bars from Yahoo Finance.

        Args:
            ticker: stock ticker (e.g. 'AAPL', '^GSPC')
            from_date: start date (inclusive)
            to_date: end date (inclusive)

        Returns:
            list of dicts matching price_history schema:
            {ticker, business_date, open, high, low, close, volume, adj_close}
        """
        if not self._available:
            logger.error(f"  {ticker}: yfinance not available")
            return []

        import yfinance as yf

        # yfinance end date is exclusive, so add 1 day
        end_date = to_date + timedelta(days=1)

        all_rows: list[dict] = []

        try:
            # Run yfinance in a thread since it's synchronous/blocking
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: yf.download(
                    ticker,
                    start=from_date.isoformat(),
                    end=end_date.isoformat(),
                    auto_adjust=True,
                    progress=False,
                ),
            )

            if df is None or df.empty:
                logger.warning(f"  {ticker}: no data returned from Yahoo Finance")
                return []

            # Handle MultiIndex columns that yfinance sometimes returns
            # when downloading a single ticker with newer versions
            if isinstance(df.columns, __import__('pandas').MultiIndex):
                df = df.droplevel('Ticker', axis=1)

            for idx, row in df.iterrows():
                bar_date = idx.date() if hasattr(idx, 'date') else idx
                all_rows.append({
                    "ticker": ticker,
                    "business_date": bar_date.isoformat(),
                    "open": round(float(row["Open"]), 4) if not _is_nan(row.get("Open")) else None,
                    "high": round(float(row["High"]), 4) if not _is_nan(row.get("High")) else None,
                    "low": round(float(row["Low"]), 4) if not _is_nan(row.get("Low")) else None,
                    "close": round(float(row["Close"]), 4) if not _is_nan(row.get("Close")) else None,
                    "volume": int(row["Volume"]) if not _is_nan(row.get("Volume")) else None,
                    "adj_close": round(float(row["Close"]), 4) if not _is_nan(row.get("Close")) else None,
                })

            logger.info(f"  {ticker}: fetched {len(all_rows)} bars from Yahoo Finance ({from_date} → {to_date})")

        except Exception as e:
            logger.error(f"  {ticker}: Yahoo Finance error — {e}")

        return all_rows


def _is_nan(value) -> bool:
    """Check if a value is NaN or None."""
    if value is None:
        return True
    try:
        import math
        return math.isnan(float(value))
    except (ValueError, TypeError):
        return True
