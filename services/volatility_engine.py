"""
Volatility Engine — Rolling Daily Standard Deviation (Annualized).

For each day in the price history, computes volatility using a rolling
lookback window of historical returns. Outputs rows matching the
stock_volatility table schema.

Logic:
  - For day t, compute simple daily return: r_t = (close_t - close_{t-1}) / close_{t-1}
  - Gather the lookback_days returns BEFORE day t: [t - lookback, t)
    (same window convention as VaREngine — returns before day t, not including t)
  - daily_vol = np.std(window_returns, ddof=1)   (sample std dev)
  - annualized_vol = daily_vol * sqrt(252)
  - return_count = number of valid returns in the window
  - If fewer than lookback_days returns available → daily_vol/annualized_vol = None

Uses simple returns (not log returns) to match VaREngine and CorrelationEngine.
"""

import logging
from math import sqrt

import numpy as np

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252


class VolatilityEngine:
    def compute_rolling_volatility(
        self,
        ticker: str,
        prices: list[dict],
        lookback_days: int = 252,
    ) -> list[dict]:
        """
        Compute rolling daily volatility for every date in the price series.

        Args:
            ticker: stock ticker
            prices: list of dicts with 'business_date', 'close', 'adj_close'
                    — sorted ascending by business_date
            lookback_days: rolling window size (e.g. 252)

        Returns:
            list of dicts matching stock_volatility table schema
        """
        if len(prices) < 2:
            logger.warning(f"  {ticker}: not enough price data ({len(prices)} rows)")
            return []

        # Build arrays
        dates = [p["business_date"] for p in prices]
        closes = np.array(
            [float(p["adj_close"] or p["close"]) for p in prices],
            dtype=np.float64,
        )

        # Daily simple returns: r_t = (P_t - P_{t-1}) / P_{t-1}
        returns = np.full(len(closes), np.nan)
        returns[1:] = (closes[1:] - closes[:-1]) / closes[:-1]

        annualize_factor = sqrt(TRADING_DAYS_PER_YEAR)

        results: list[dict] = []

        for t in range(len(prices)):
            daily_ret = (
                round(float(returns[t]), 6) if not np.isnan(returns[t]) else None
            )

            row = {
                "ticker": ticker,
                "business_date": dates[t],
                "daily_return": daily_ret,
                "daily_vol": None,
                "lookback_days": lookback_days,
                "annualized_vol": None,
                "return_count": None,
                "computed_at": dates[t] + "T00:00:00Z",
            }

            # Need lookback_days of prior returns to compute vol.
            # returns[0] is NaN, so usable returns start at index 1.
            # For day t, use returns from [t - lookback_days, t) — same as VaREngine.
            if t >= lookback_days + 1:
                window_returns = returns[t - lookback_days : t]
                valid = window_returns[~np.isnan(window_returns)]

                if len(valid) >= lookback_days:
                    daily_vol = float(np.std(valid, ddof=1))
                    annualized_vol = daily_vol * annualize_factor

                    row["daily_vol"] = round(daily_vol, 6)
                    row["annualized_vol"] = round(annualized_vol, 6)
                    row["return_count"] = int(len(valid))

            results.append(row)

        n_with_vol = sum(1 for r in results if r["daily_vol"] is not None)
        logger.info(
            f"  {ticker}: {len(results)} daily rows, "
            f"{n_with_vol} with volatility (lookback={lookback_days})"
        )
        return results
