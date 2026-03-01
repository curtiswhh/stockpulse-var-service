"""
VaR Engine — Simple Percentile (Rolling Daily).

For each day in the price history, computes VaR using a rolling lookback
window of historical returns. Outputs rows matching the
var_calculations_precomputed table schema.

Logic:
  - For day t, gather the lookback_days returns BEFORE day t
  - var_pct_lower = percentile(returns, (1 - confidence) * 100)  e.g. 5th pctile
  - var_pct_upper = percentile(returns, confidence * 100)        e.g. 95th pctile
  - var_lower / var_upper = reference_price * (1 + var_pct)      price levels
  - breach_lower = (daily_return < var_pct_lower)
  - breach_upper = (daily_return > var_pct_upper)
  - If fewer than lookback_days returns available → leave VaR/breach as None
"""

import logging
import numpy as np

logger = logging.getLogger(__name__)


class VaREngine:
    def compute_rolling_var(
        self,
        ticker: str,
        prices: list[dict],
        confidence_level: float,
        lookback_days: int,
    ) -> list[dict]:
        """
        Compute rolling daily VaR for every date in the price series.

        Args:
            ticker: stock ticker
            prices: list of dicts with 'business_date', 'close', 'adj_close' — sorted ASC
            confidence_level: e.g. 0.95
            lookback_days: e.g. 252

        Returns:
            list of dicts matching var_calculations_precomputed schema
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

        # Daily simple returns
        returns = np.full(len(closes), np.nan)
        returns[1:] = (closes[1:] - closes[:-1]) / closes[:-1]

        results: list[dict] = []

        for t in range(len(prices)):
            ref_price = round(float(closes[t]), 4)
            daily_ret = round(float(returns[t]), 6) if not np.isnan(returns[t]) else None

            row = {
                "ticker": ticker,
                "business_date": dates[t],
                "confidence_level": confidence_level,
                "lookback_days": lookback_days,
                "method": "historical_simulation",
                "daily_return": daily_ret,
                "reference_price": ref_price,
                "var_lower": None,
                "var_upper": None,
                "var_pct_lower": None,
                "var_pct_upper": None,
                "breach_lower": None,
                "breach_upper": None,
                "computed_at": dates[t] + "T00:00:00Z",
            }

            # Need lookback_days of prior returns to compute VaR
            # returns[0] is NaN, so usable returns start at index 1
            # For day t, use returns from [t - lookback_days, t)
            if t >= lookback_days + 1:
                window_returns = returns[t - lookback_days : t]
                valid = window_returns[~np.isnan(window_returns)]

                if len(valid) >= lookback_days:
                    var_pct_lower = float(np.percentile(valid, (1 - confidence_level) * 100))
                    var_pct_upper = float(np.percentile(valid, confidence_level * 100))

                    row["var_pct_lower"] = round(var_pct_lower, 6)
                    row["var_pct_upper"] = round(var_pct_upper, 6)
                    row["var_lower"] = round(ref_price * (1 + var_pct_lower), 4)
                    row["var_upper"] = round(ref_price * (1 + var_pct_upper), 4)

                    # Breach: does today's actual return exceed VaR?
                    if daily_ret is not None:
                        row["breach_lower"] = bool(returns[t] < var_pct_lower)
                        row["breach_upper"] = bool(returns[t] > var_pct_upper)

            results.append(row)

        n_with_var = sum(1 for r in results if r["var_pct_lower"] is not None)
        n_breach_lower = sum(1 for r in results if r["breach_lower"] is True)
        n_breach_upper = sum(1 for r in results if r["breach_upper"] is True)
        logger.info(
            f"  {ticker}: {len(results)} daily rows, "
            f"{n_with_var} with VaR, "
            f"{n_breach_lower} lower breaches, {n_breach_upper} upper breaches"
        )
        return results
