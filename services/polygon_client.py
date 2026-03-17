"""
Polygon.io API client — identical to production version.
"""

import asyncio
import logging
from datetime import date

import httpx

from config.settings import Settings

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io"


class PolygonClient:
    def __init__(self, settings: Settings):
        self._api_key = settings.polygon_api_key
        self._delay = settings.price_fetch_delay_sec
        self._semaphore = asyncio.Semaphore(1)

    async def get_daily_bars(
        self, ticker: str, from_date: date, to_date: date,
    ) -> list[dict]:
        all_rows: list[dict] = []
        url = (
            f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{from_date.isoformat()}/{to_date.isoformat()}"
        )
        params = {
            "adjusted": "true", "sort": "asc",
            "limit": 50000, "apiKey": self._api_key,
        }

        async with self._semaphore:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    for bar in data.get("results", []):
                        bar_date = date.fromtimestamp(bar["t"] / 1000)
                        all_rows.append({
                            "ticker": ticker,
                            "business_date": bar_date.isoformat(),
                            "open": bar.get("o"),
                            "high": bar.get("h"),
                            "low": bar.get("l"),
                            "close": bar.get("c"),
                            "volume": int(bar["v"]) if bar.get("v") is not None else None,
                            "adj_close": bar.get("c"),
                        })
                    logger.info(f"  {ticker}: fetched {len(all_rows)} bars ({from_date} → {to_date})")
            except httpx.HTTPStatusError as e:
                logger.error(f"  {ticker}: Polygon HTTP {e.response.status_code}")
            except Exception as e:
                logger.error(f"  {ticker}: Polygon error — {e}")
            await asyncio.sleep(self._delay)

        return all_rows