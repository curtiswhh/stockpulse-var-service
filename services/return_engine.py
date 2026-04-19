"""
Return Engine — Daily Simple Returns.

For each day in the price history, computes the simple daily return
r_t = (P_t - P_{t-1}) / P_{t-1}. Outputs rows matching the
stock_return table schema.

This is the single source of truth for daily_return in StockPulse.
VaREngine and VolatilityEngine compute returns internally only because
they need them for their own rolling-window calculations — they no
longer persist daily_return themselves (that column will be dropped
in a follow-up round once all consumers read from stock_return).

Uses simple returns (not log returns) to match VaREngine, VolatilityEngine,
and CorrelationEngine — keeping every downstream metric consistent.
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)


class ReturnEngine:
    def compute_daily_returns(
        self,
        ticker: str,
        prices: list[dict],
    ) -> list[dict]:
        """
        Compute daily simple returns for every date in the price series.

        Args:
            ticker: stock ticker
            prices: list of dicts with 'business_date', 'close', 'adj_close'
                    — sorted ascending by business_date

        Returns:
            list of dicts matching stock_return table schema
        """
        if len(prices) < 2:
            logger.warning(f"  {ticker}: not enough price data ({len(prices)} rows)")
            return []

        dates = [p["business_date"] for p in prices]
        closes = np.array(
            [float(p["adj_close"] or p["close"]) for p in prices],
            dtype=np.float64,
        )

        # Daily simple returns: r_t = (P_t - P_{t-1}) / P_{t-1}
        returns = np.full(len(closes), np.nan)
        returns[1:] = (closes[1:] - closes[:-1]) / closes[:-1]

        results: list[dict] = []
        for t in range(len(prices)):
            daily_ret = (
                round(float(returns[t]), 6) if not np.isnan(returns[t]) else None
            )
            results.append({
                "ticker": ticker,
                "business_date": dates[t],
                "daily_return": daily_ret,
                "reference_price": round(float(closes[t]), 4),
                "computed_at": dates[t] + "T00:00:00Z",
            })

        n_with_return = sum(1 for r in results if r["daily_return"] is not None)
        logger.info(f"  {ticker}: {len(results)} daily rows, {n_with_return} with return")
        return results
