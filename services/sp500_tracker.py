"""
S&P 500 Tracker — identical to production version.
"""

import logging
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from services.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


class SP500Tracker:
    def __init__(self, supabase: SupabaseClient):
        self._supabase = supabase

    async def refresh(self) -> dict:
        current_web = await self._scrape_wikipedia()
        current_db_rows = self._supabase.get_sp500_tickers()
        db_tickers = {r["ticker"] for r in current_db_rows if r.get("is_active", True)}
        web_tickers = {r["ticker"] for r in current_web}

        added = web_tickers - db_tickers
        removed = db_tickers - web_tickers

        self._supabase.upsert_sp500_constituents(current_web)
        if removed:
            self._supabase.mark_removed_constituents(list(removed))
            logger.info(f"Removed from S&P 500: {sorted(removed)}")
        if added:
            logger.info(f"Added to S&P 500: {sorted(added)}")

        logger.info(f"S&P 500 constituent count: {len(web_tickers)}")
        return {"added": sorted(added), "removed": sorted(removed), "total": len(web_tickers)}

    def get_active_tickers(self) -> list[str]:
        rows = self._supabase.get_sp500_tickers()
        return [r["ticker"] for r in rows if r.get("is_active", True)]

    async def _scrape_wikipedia(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(WIKIPEDIA_URL)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        if table is None:
            table = soup.find("table", class_="wikitable")
        if table is None:
            raise RuntimeError("Could not find S&P 500 table on Wikipedia")

        rows = table.find_all("tr")[1:]
        constituents: list[dict] = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 7:
                continue
            ticker = cols[0].get_text(strip=True).replace(".", "-")
            constituents.append({
                "ticker": ticker,
                "company_name": cols[1].get_text(strip=True),
                "sector": cols[3].get_text(strip=True),
                "sub_industry": cols[4].get_text(strip=True),
                "date_added_to_sp500": cols[6].get_text(strip=True) if len(cols) > 6 else None,
                "is_active": True,
                "updated_at": datetime.utcnow().isoformat(),
            })
        return constituents
