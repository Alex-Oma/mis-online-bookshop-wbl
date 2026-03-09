import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings

# Set up module-level logger
logger = logging.getLogger(__name__)

# Global scheduler instance (initialized in start_scheduler)
_scheduler: AsyncIOScheduler | None = None


async def _run_ingestion_cycle() -> None:
    """Scheduled job: run full ingestion cycle for all sales channels."""
    from app.ingestion.website_adapter import WebsiteAdapter
    from app.ingestion.rozetka_adapter import RozetkaAdapter
    from app.ingestion.transformer import Transformer

    # Note: each adapter's run() method handles its own exceptions and log them,
    # so that one failing adapter doesn't prevent the others from running.
    # However, we wrap the whole cycle in a try-except to catch any unexpected errors that might occur outside the adapters.
    logger.info("Scheduled ingestion cycle starting")
    try:
        await WebsiteAdapter().run()
        await RozetkaAdapter().run()
        await Transformer().run()
        logger.info("Scheduled ingestion cycle completed")
    except Exception as exc:
        logger.error("Scheduled ingestion cycle failed: %s", exc, exc_info=True)


async def _run_alert_checks() -> None:
    """Scheduled job: evaluate all active alert rules."""
    from app.alerts.engine import AlertEngine

    # Similar to the ingestion cycle, we wrap the alert checks in a try-except to ensure that any unexpected errors are logged without crashing the scheduler.
    try:
        # The AlertEngine is designed to handle exceptions within individual alert rules, so that one failing rule doesn't prevent others from being evaluated.
        await AlertEngine().check_all()
    except Exception as exc:
        logger.error("Alert check failed: %s", exc, exc_info=True)


def start_scheduler() -> None:
    # Initialize and start the APScheduler with the defined jobs and intervals.
    global _scheduler
    # We read the settings here to get the intervals for the scheduled jobs.
    # This allows us to change the intervals via environment variables without modifying the code.
    settings = get_settings()

    # We use AsyncIOScheduler since our jobs are async functions.
    _scheduler = AsyncIOScheduler()

    # The ingestion cycle runs every N hours as defined in settings,
    # and we allow a misfire grace time of 10 minutes to handle cases where the job might be delayed (e.g., due to a long-running previous cycle or temporary resource constraints).
    _scheduler.add_job(
        _run_ingestion_cycle,
        trigger=IntervalTrigger(hours=settings.ingestion_interval_hours),
        id="ingestion_cycle",
        name="Full ingestion cycle (website + Rozetka)",
        replace_existing=True,
        misfire_grace_time=600,  # allow 10 min grace if job misfired
    )

    # The alert checks run every M minutes as defined in settings,
    # with a shorter misfire grace time of 2 minutes since we want alerts to be evaluated more promptly.
    _scheduler.add_job(
        _run_alert_checks,
        trigger=IntervalTrigger(minutes=settings.alert_check_interval_minutes),
        id="alert_checks",
        name="Evaluate alert rules",
        replace_existing=True,
        misfire_grace_time=120,
    )

    # Start the scheduler. It will run in the background and execute the jobs at the defined intervals.
    _scheduler.start()
    # Log the startup of the scheduler with the configured intervals for both jobs.
    logger.info(
        "Scheduler started — ingestion every %dh, alerts every %dm",
        settings.ingestion_interval_hours,
        settings.alert_check_interval_minutes,
    )


def stop_scheduler() -> None:
    '''
        Stop the scheduler gracefully.
        This is called during application shutdown to ensure that the scheduler is properly shut down and any running jobs are stopped.
    :return:
    '''
    # We check if the scheduler is initialized and running before attempting to shut it down.
    global _scheduler
    # The shutdown method will stop the scheduler and optionally wait for any currently running jobs to finish.
    if _scheduler and _scheduler.running:
        # We set wait=False to not block the shutdown process while waiting for jobs to finish, since we want the application to shut down promptly.
        _scheduler.shutdown(wait=False)
        # Log that the scheduler has been stopped. This is useful for confirming that the shutdown process has completed.
        logger.info("Scheduler stopped")

