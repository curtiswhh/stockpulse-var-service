"""
StockPulse Backend — Entry Point
========================================================

CLI COMMANDS (quick reference):
─────────────────────────────────────────────────────────────────────────────
  python main.py --run-daily                     ← Full daily pipeline (Polygon fetch + VaR + correlations) for all tickers
  python main.py --fetch-and-compute-var TSLA    ← Polygon fetch + VaR for ONE ticker
  python main.py --fetch-and-compute-var-all     ← Polygon fetch + VaR for ALL tickers in price_history
  python main.py --compute-var TSLA              ← VaR only for ONE ticker (NO Polygon calls)
  python main.py --compute-var-all               ← VaR only for ALL tickers in price_history (NO Polygon calls)
  python main.py --compute-correlations-all      ← Correlations only for ALL tickers (NO Polygon calls)
  python main.py --refresh-sp500                 ← Refresh S&P 500 constituent list only
  python main.py --describe                      ← Print current config and exit
─────────────────────────────────────────────────────────────────────────────

TICKER RESOLUTION:
  All commands that process "all tickers" resolve them by querying
  SELECT DISTINCT ticker FROM price_history in Supabase.
  This means: if a ticker has rows in price_history, it gets processed.
  No hardcoded config needed.

  The seed_tickers in settings.py is ONLY for standalone testing /
  cold-start seeding. The daily pipeline and CLI commands ignore it.

POLYGON API:
  Commands with "fetch" in the name call Polygon for price data.
  Commands without "fetch" work purely from existing Supabase data.
"""

import argparse
import asyncio
import logging

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.price_data_client import PriceDataClient
from services.sp500_tracker import SP500Tracker
from services.var_engine import VaREngine
from services.correlation_engine import CorrelationEngine
from jobs.daily_pipeline import DailyPipeline
from jobs.backfill import BackfillJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stockpulse")


async def main():
    parser = argparse.ArgumentParser(
        description="StockPulse VaR Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            python main.py --run-daily                     # Daily cron job (VaR + correlations)
            python main.py --fetch-and-compute-var TSLA    # Add a new ticker end-to-end
            python main.py --compute-var TSLA              # Recompute VaR after manual price upload
            python main.py --compute-var-all               # Recompute VaR for everything in price_history
            python main.py --compute-correlations-all      # Recompute correlations from existing price data
                    """,
    )
    group = parser.add_mutually_exclusive_group(required=True)

    # ── Full daily pipeline ───────────────────────────────────
    group.add_argument(
        "--run-daily", action="store_true",
        help="Full daily pipeline: Polygon fetch + VaR + correlations for all tickers",
    )

    # ── Polygon fetch + VaR (calls Polygon API) ──────────────
    group.add_argument(
        "--fetch-and-compute-var", type=str, metavar="TICKER",
        help="Fetch prices from Polygon + compute VaR for ONE ticker",
    )
    group.add_argument(
        "--fetch-and-compute-var-all", action="store_true",
        help="Fetch prices from Polygon + compute VaR for ALL tickers in price_history",
    )

    # ── VaR only (NO Polygon calls) ──────────────────────────
    group.add_argument(
        "--compute-var", type=str, metavar="TICKER",
        help="Compute VaR for ONE ticker from existing Supabase data (no Polygon)",
    )
    group.add_argument(
        "--compute-var-all", action="store_true",
        help="Compute VaR for ALL tickers in price_history (no Polygon)",
    )

    # ── Correlations only (NO Polygon calls) ─────────────────
    group.add_argument(
        "--compute-correlations-all", action="store_true",
        help="Compute pairwise correlations for ALL tickers from existing data (no Polygon)",
    )

    # ── Utilities ─────────────────────────────────────────────
    group.add_argument(
        "--refresh-sp500", action="store_true",
        help="Refresh S&P 500 constituent list only",
    )
    group.add_argument(
        "--describe", action="store_true",
        help="Print current config and exit",
    )

    args = parser.parse_args()

    settings = Settings()

    if args.describe:
        print(settings.describe())
        return

    supabase = SupabaseClient(settings)
    price_client = PriceDataClient(settings)
    sp500 = SP500Tracker(supabase)
    var_engine = VaREngine()
    correlation_engine = CorrelationEngine()

    pipeline = DailyPipeline(
        settings=settings, supabase=supabase, price_client=price_client,
        sp500_tracker=sp500, var_engine=var_engine,
        correlation_engine=correlation_engine,
    )

    backfill = BackfillJob(
        supabase=supabase, price_client=price_client,
        var_engine=var_engine, settings=settings,
        correlation_engine=correlation_engine,
    )

    # ── Route to the correct action ──────────────────────────
    if args.run_daily:
        # Full daily pipeline: resolves tickers from price_history,
        # fetches latest prices (yfinance → Polygon), computes VaR + correlations
        await pipeline.run()

    elif args.fetch_and_compute_var:
        # Polygon fetch + VaR for a single ticker
        # Use this to add a brand new ticker end-to-end
        await backfill.fetch_and_compute_var(args.fetch_and_compute_var)

    elif args.fetch_and_compute_var_all:
        # Polygon fetch + VaR for every ticker in price_history
        await backfill.fetch_and_compute_var_all()

    elif args.compute_var:
        # VaR only for one ticker — no Polygon calls
        # Use this after manually uploading prices to Supabase
        await backfill.compute_var(args.compute_var)

    elif args.compute_var_all:
        # VaR only for every ticker in price_history — no Polygon calls
        await backfill.compute_var_all()

    elif args.compute_correlations_all:
        # Correlations only — no Polygon calls
        # Use this after manually uploading prices for new tickers
        await backfill.compute_correlations_all()

    elif args.refresh_sp500:
        await sp500.refresh()

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())