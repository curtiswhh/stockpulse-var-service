"""
Scheduler — runs the daily pipeline on a cron schedule.
Same as production; the pipeline itself respects settings.test_mode.

Usage:  python scheduler.py
"""

import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config.settings import Settings
from services.supabase_client import SupabaseClient
from services.polygon_client import PolygonClient
from services.sp500_tracker import SP500Tracker
from services.var_engine import VaREngine
from jobs.daily_pipeline import DailyPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("scheduler")


async def run_pipeline():
    settings = Settings()
    supabase = SupabaseClient(settings)
    polygon = PolygonClient(settings)
    sp500 = SP500Tracker(supabase)
    var_engine = VaREngine()

    pipeline = DailyPipeline(
        settings=settings, supabase=supabase, polygon=polygon,
        sp500_tracker=sp500, var_engine=var_engine,
    )
    await pipeline.run()


def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger=CronTrigger(day_of_week="mon-fri", hour=21, minute=30, timezone="UTC"),
        id="daily_var_pipeline",
        name="Daily VaR Pipeline",
        misfire_grace_time=3600,
    )
    logger.info("Scheduler started — daily pipeline at 21:30 UTC (Mon–Fri)")
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
