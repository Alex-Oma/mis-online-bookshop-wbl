"""
Ingestion API endpoints for triggering manual syncs and checking status.
"""
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status

from app.auth.dependencies import get_current_user, require_role
from app.ingestion.rozetka_adapter import RozetkaAdapter
from app.ingestion.transformer import Transformer
from app.ingestion.website_adapter import WebsiteAdapter
from app.models.mis import IngestionRequest, IngestionStatus

# Create a router for ingestion-related endpoints
router = APIRouter()

# In-memory job store
_jobs: dict[str, IngestionStatus] = {}


@router.post(
    "/run",
    response_model=IngestionStatus,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_role("admin"))],
)
async def trigger_ingestion(
    request: IngestionRequest,
    background_tasks: BackgroundTasks,
    current_user=Depends(get_current_user),
):
    """
    Trigger a manual ingestion run. Returns immediately with a job_id.
    Progress can be polled via GET /ingest/status/{job_id}.
    """
    # Generate a unique job ID and create an initial job status entry
    job_id = str(uuid.uuid4())
    # Store the job with initial status "running". The background task will update this entry as it progresses.
    job = IngestionStatus(
        job_id=job_id,
        status="running",
        started_at=datetime.now(),
    )
    _jobs[job_id] = job

    # Start the background task to run the ingestion process. It will update the job status in the _jobs dict as it runs.
    background_tasks.add_task(
        _run_ingestion,
        job_id,
        request.channels,
        request.from_date,
        request.to_date,
    )
    # Return the initial job status immediately.
    # The client can poll this endpoint to get updates on the job's progress.
    return job


@router.get("/status/{job_id}", response_model=IngestionStatus)
async def get_ingestion_status(
    job_id: str,
    current_user=Depends(get_current_user),
):
    """Poll the status of a running or completed ingestion job."""
    job = _jobs.get(job_id)
    if not job:
        # If the job ID is not found in the _jobs dict, return a 404 error.
        # This means either the job ID is invalid or the job has not been created yet.
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get(
    "/history",
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def get_sync_history(
    limit: int = Query(default=20, le=100),
    current_user=Depends(get_current_user),
):
    """Return recent ingestion sync log entries."""
    from app.database import get_pool
    # Fetch recent sync log entries from the database.
    # This is separate from the in-memory _jobs dict, which only tracks currently running jobs.
    # Get a connection from the database pool and execute a query to retrieve the sync log entries, ordered by start time descending, limited by the specified limit.
    pool = await get_pool()
    # Convert the query results to a list of dictionaries and return it.
    # Each dictionary represents a sync log entry with fields like sync_id, channel, sync_type, timestamps, records ingested/failed, status, and error details.
    async with pool.acquire() as conn:
        # The SQL query selects relevant fields from the mis.sync_log table, orders the results by the started_at timestamp in descending order (most recent first), and limits the number of results returned based on the 'limit' parameter.
        rows = await conn.fetch(
            """
            SELECT sync_id, channel, sync_type, started_at, completed_at,
                   records_ingested, records_failed, status, error_detail
            FROM mis.sync_log
            ORDER BY started_at DESC
            LIMIT $1
            """,
            limit,
        )
    # Convert the query results to a list of dictionaries and return it.
    # Each dictionary represents a sync log entry with fields like sync_id, channel, sync_type, timestamps, records ingested/failed, status, and error details.
    return [dict(r) for r in rows]


# ── Background task ───────────────────────────────────────────────────────────

async def _run_ingestion(
    job_id: str,
    channels: list[str],
    from_date: Optional[str],
    to_date: Optional[str],
) -> None:
    '''Background task to run the ingestion process for the specified channels and date range. Updates the job status in the _jobs dict as it progresses.'''
    job = _jobs[job_id]
    total_ingested = 0
    total_failed = 0

    # Convert the from_date and to_date strings to datetime objects.
    # If they are None, use None which indicates to the adapters to use their default date range (e.g. last sync timestamp for from_date, and now for to_date).
    from_dt = datetime.fromisoformat(from_date) if from_date else None
    to_dt = datetime.fromisoformat(to_date) if to_date else None

    # Run the adapters for the specified channels.
    # Each adapter will return stats on how many records were ingested and how many errors occurred.
    try:
        # Run the adapters for the specified channels. Each adapter will return stats on how many records were ingested and how many errors occurred.

        if "website" in channels:
            # Run the WebsiteAdapter to ingest data from the website channel.
            # The adapter will use the provided from_date and to_date to determine the date range for data ingestion.
            # It will return a stats dictionary containing the number of orders ingested and any errors that occurred.
            stats = await WebsiteAdapter().run(from_date=from_dt, to_date=to_dt)
            total_ingested += stats.get("orders", 0)

        if "rozetka" in channels:
            # Run the RozetkaAdapter to ingest data from the Rozetka channel.
            # Similar to the WebsiteAdapter, it will use the provided date range and return stats on the number of orders ingested and any errors that occurred.
            stats = await RozetkaAdapter().run(from_date=from_dt, to_date=to_dt)
            total_ingested += stats.get("orders", 0)
            total_failed += stats.get("errors", 0)

        # Run transformer after all adapters complete
        await Transformer().run()

        # Update the job status to "success" and record the total number of records ingested and failed.
        job.status = "success"
        job.records_ingested = total_ingested
        job.records_failed = total_failed
        job.completed_at = datetime.now()

    except Exception as exc:
        # If any exception occurs during the ingestion process, catch it and update the job status to "failed".
        job.status = "failed"
        job.error_detail = str(exc)
        job.completed_at = datetime.now()

