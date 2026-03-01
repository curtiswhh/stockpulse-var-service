"""
Configuration — TEST MODE version (AAPL-only, 1000 days).

Changes from previous version:
  - test_tickers reduced to AAPL only
  - var_max_lookback_days set to 1000
  - Single confidence level (0.95) and single lookback (252) for simple VaR
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass


@dataclass
class Settings:
    # ── Supabase ──────────────────────────────────────────────
    supabase_url: str = field(default_factory=lambda: os.environ["SUPABASE_URL"])
    supabase_service_key: str = field(
        default_factory=lambda: os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )

    # ── Polygon.io ────────────────────────────────────────────
    polygon_api_key: str = field(default_factory=lambda: os.environ["POLYGON_API_KEY"])

    # ══════════════════════════════════════════════════════════
    # TEST MODE CONTROLS
    # ══════════════════════════════════════════════════════════
    test_mode: bool = field(
        default_factory=lambda: os.environ.get("TEST_MODE", "true").lower() == "true"
    )

    # AAPL only for focused testing
    test_tickers: tuple = ("AAPL",)

    # ── VaR Parameters ────────────────────────────────────────
    # Simple percentile VaR settings
    var_confidence_level: float = 0.95          # single confidence level
    var_lookback_days: int = 252                # 1-year rolling lookback
    var_max_backfill_days: int = 1000           # fetch 1000 days of price data

    # Legacy settings kept for compatibility
    var_confidence_start: float = 0.800
    var_confidence_end: float = 0.999

    @property
    def var_confidence_step(self) -> float:
        return 0.01 if self.test_mode else 0.001

    var_lookback_windows: tuple = (252,)
    var_max_lookback_days: int = 1000

    # ── Pipeline Tuning ───────────────────────────────────────
    polygon_rate_limit_per_min: int = 5
    polygon_batch_size: int = 5
    price_fetch_delay_sec: float = 12.5

    # ── S&P 500 Source ────────────────────────────────────────
    sp500_wikipedia_url: str = (
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )

    @property
    def var_confidence_levels(self) -> list[float]:
        levels = []
        c = self.var_confidence_start
        step = self.var_confidence_step
        while c <= self.var_confidence_end + 1e-9:
            levels.append(round(c, 3))
            c += step
        return levels

    def describe(self) -> str:
        mode = "TEST" if self.test_mode else "PRODUCTION"
        tickers = list(self.test_tickers) if self.test_mode else "all S&P 500"
        return (
            f"── Settings ({mode} MODE) ──\n"
            f"  Tickers:            {tickers}\n"
            f"  Backfill days:      {self.var_max_backfill_days}\n"
            f"  VaR confidence:     {self.var_confidence_level}\n"
            f"  VaR lookback:       {self.var_lookback_days} trading days\n"
        )
