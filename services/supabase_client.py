"""
Supabase client — thin wrapper around supabase-py.
========================================================

KEY METHOD FOR TICKER RESOLUTION:
  get_all_price_history_tickers() → returns all distinct tickers in price_history.
  This is the primary way the daily pipeline and CLI commands determine
  which stocks to process. If a ticker has rows in price_history, it's included.

Updated: date→business_date, calc_date→business_date, no id columns.
"""

import logging
from datetime import date, datetime
from typing import Any

from supabase import create_client, Client

from config.settings import Settings

logger = logging.getLogger(__name__)


class SupabaseClient:
    def __init__(self, settings: Settings):
        self._client: Client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )

    # ── S&P 500 Constituents ──────────────────────────────────

    def get_sp500_tickers(self) -> list[dict]:
        resp = self._client.table("sp500_constituents").select("*").execute()
        return resp.data

    def upsert_sp500_constituents(self, rows: list[dict]) -> None:
        if not rows:
            return
        self._client.table("sp500_constituents").upsert(
            rows, on_conflict="ticker"
        ).execute()
        logger.info(f"Upserted {len(rows)} S&P 500 constituents")

    def mark_removed_constituents(self, removed_tickers: list[str]) -> None:
        if not removed_tickers:
            return
        self._client.table("sp500_constituents").update(
            {"is_active": False, "removed_at": datetime.utcnow().isoformat()}
        ).in_("ticker", removed_tickers).execute()
        logger.info(f"Marked {len(removed_tickers)} tickers as removed")

    # ── Price History ─────────────────────────────────────────

    def get_all_price_history_tickers(self) -> list[str]:
        """
        Return all distinct tickers that have data in price_history.

        THIS IS THE PRIMARY TICKER RESOLUTION METHOD.
        The daily pipeline and CLI "--all" commands use this to determine
        which stocks to process. If a ticker has rows here, it gets included.

        Uses pagination to handle tables with more than 1000 rows,
        since Supabase caps responses at 1000 per request.
        """
        all_tickers: set[str] = set()
        page_size = 1000
        offset = 0

        while True:
            resp = (
                self._client.table("price_history")
                .select("ticker")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            if not resp.data:
                break
            for row in resp.data:
                all_tickers.add(row["ticker"])
            if len(resp.data) < page_size:
                break
            offset += page_size

        result = sorted(all_tickers)
        logger.info(f"Found {len(result)} distinct tickers in price_history: {result}")
        return result

    def get_latest_price_date(self, ticker: str) -> date | None:
        resp = (
            self._client.table("price_history")
            .select("business_date")
            .eq("ticker", ticker)
            .order("business_date", desc=True)
            .limit(1)
            .execute()
        )
        if resp.data:
            return date.fromisoformat(resp.data[0]["business_date"])
        return None

    def upsert_price_history(self, rows: list[dict]) -> None:
        if not rows:
            return
        for i in range(0, len(rows), 1000):
            chunk = rows[i : i + 1000]
            self._client.table("price_history").upsert(
                chunk, on_conflict="ticker,business_date"
            ).execute()
        logger.info(f"Upserted {len(rows)} price_history rows")

    def get_price_history(self, ticker: str, lookback_days: int) -> list[dict]:
        resp = (
            self._client.table("price_history")
            .select("business_date, close, adj_close")
            .eq("ticker", ticker)
            .order("business_date", desc=True)
            .limit(lookback_days)
            .execute()
        )
        return list(reversed(resp.data)) if resp.data else []

    def get_full_price_history(self, ticker: str) -> list[dict]:
        """Fetch ALL price history for a ticker, sorted ascending by date.
        Paginates through results since Supabase caps at 1000 per request."""
        all_rows = []
        page_size = 1000
        offset = 0

        while True:
            resp = (
                self._client.table("price_history")
                .select("business_date, close, adj_close")
                .eq("ticker", ticker)
                .order("business_date", desc=False)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            if not resp.data:
                break
            all_rows.extend(resp.data)
            if len(resp.data) < page_size:
                break
            offset += page_size

        logger.info(f"  {ticker}: fetched {len(all_rows)} total price rows from Supabase")
        return all_rows

    # ── VaR Calculations (precomputed) ────────────────────────

    def upsert_var_calculations(self, rows: list[dict]) -> None:
        if not rows:
            return
        for i in range(0, len(rows), 1000):
            chunk = rows[i : i + 1000]
            self._client.table("var_calculations_precomputed").upsert(
                chunk,
                on_conflict="ticker,business_date,confidence_level,lookback_days",
            ).execute()
        logger.info(f"Upserted {len(rows)} var_calculations_precomputed rows")

    def delete_old_var(self, before_date: str) -> None:
        self._client.table("var_calculations_precomputed").delete().lt(
            "business_date", before_date
        ).execute()

    # ── Watchlist ─────────────────────────────────────────────

    def get_all_watched_tickers(self) -> list[str]:
        resp = self._client.table("watchlist_stocks").select("ticker").execute()
        return list({row["ticker"] for row in resp.data}) if resp.data else []

    # ── Generic ───────────────────────────────────────────────

    def rpc(self, function_name: str, params: dict | None = None) -> Any:
        return self._client.rpc(function_name, params or {}).execute()