"""
StockPulse Backend — TEST MODE Entry Point (AAPL-only)
========================================================
Usage:
    python main.py --initial-backfill       ← fetch prices from Polygon + compute rolling VaR
    python main.py --backfill AAPL          ← backfill a single ticker (fetch + VaR)
    python main.py --compute-var            ← recompute VaR from existing Supabase data (no Polygon)
    python main.py --run-daily              ← run the full daily pipeline
    python main.py --refresh-sp500          ← refresh S&P 500 list
    python main.py --describe               ← show current config
"""

import argparse
import asyncio
import logging

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.polygon_client import PolygonClient
from services.sp500_tracker import SP500Tracker
from services.var_engine import VaREngine
from jobs.daily_pipeline import DailyPipeline
from jobs.backfill import BackfillJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stockpulse")


async def main():
    parser = argparse.ArgumentParser(description="StockPulse VaR Pipeline (Test Mode)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-daily", action="store_true", help="Run the full daily pipeline")
    group.add_argument("--backfill", type=str, metavar="TICKER", help="Backfill a single ticker")
    group.add_argument("--refresh-sp500", action="store_true", help="Refresh S&P 500 list only")
    group.add_argument("--initial-backfill", action="store_true", help="Backfill all tickers (fetch + VaR)")
    group.add_argument("--compute-var", action="store_true", help="Recompute VaR from existing Supabase price data (no Polygon calls)")
    group.add_argument("--describe", action="store_true", help="Print current config and exit")
    args = parser.parse_args()

    settings = Settings()

    if args.describe:
        print(settings.describe())
        return

    supabase = SupabaseClient(settings)
    polygon = PolygonClient(settings)
    sp500 = SP500Tracker(supabase)
    var_engine = VaREngine()

    pipeline = DailyPipeline(
        settings=settings, supabase=supabase, polygon=polygon,
        sp500_tracker=sp500, var_engine=var_engine,
    )

    backfill = BackfillJob(
        supabase=supabase, polygon=polygon,
        var_engine=var_engine, settings=settings,
    )

    if args.run_daily:
        await pipeline.run()
    elif args.backfill:
        await backfill.backfill_ticker(args.backfill)
    elif args.refresh_sp500:
        await sp500.refresh()
    elif args.initial_backfill:
        await backfill.full_backfill()
    elif args.compute_var:
        await backfill.compute_var_all()

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
