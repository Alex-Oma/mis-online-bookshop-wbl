"""
RozetkaAdapter — fetches orders from the Rozetka Seller REST API
and upserts them into the MIS staging tables.

API base: https://api.seller.rozetka.com.ua
Auth:     POST /sites  →  Bearer token (valid 24 h)
Orders:   GET  /orders/search?expand=purchases,user,delivery&page=N&...
"""
import json
import logging
from datetime import datetime
from typing import Optional

import httpx

from app.config import get_settings
from app.database import get_pool
from app.models.rozetka import RozetkaOrder, RozetkaOrdersPage

# Set up module-level logger
logger = logging.getLogger(__name__)

# Rozetka API returns 20 orders per page by default, and max is 100. We can adjust if needed.
_ORDERS_PER_PAGE = 20


class RozetkaAdapter:
    """Fetches Rozetka orders via REST API and stages them in PostgreSQL."""

    def __init__(self):
        # Load settings and initialize state
        self._settings = get_settings()
        self._token: Optional[str] = None

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> dict:
        """
        Run a full Rozetka ingestion cycle.
        Paginates through all orders created/changed since the last sync.
        """
        # Get DB pool and determine sync start time
        pool = await get_pool()
        # If from_date is provided, use it; otherwise, get the last successful sync time from the DB.
        since = from_date or await self._get_last_sync(pool)
        # If to_date is not provided, use the current time.
        logger.info("RozetkaAdapter: fetching orders since %s", since)

        # Create a new sync log entry with status 'running' and get its ID for later update.
        sync_id = await self._start_sync_log(pool, since)

        try:
            # Refresh the Bearer token for API authentication.
            await self._refresh_token()
            # Fetch orders page by page and stage them in the database, collecting stats.
            stats = await self._fetch_and_stage(pool, since, to_date)
            # Update the sync log entry with the final status and stats.
            await self._finish_sync_log(pool, sync_id, "success", stats)
            # Log the completion and return stats.
            logger.info("RozetkaAdapter: completed — %s", stats)
            # Return stats for potential use by the caller (e.g. API response).
            return stats
        except Exception as exc:
            # On any exception, update the sync log with 'failed' status and error details, then re-raise.
            await self._finish_sync_log(pool, sync_id, "failed", {}, str(exc))
            # Log the error with stack trace for debugging.
            logger.error("RozetkaAdapter: failed — %s", exc, exc_info=True)
            raise

    # ── Auth ──────────────────────────────────────────────────────────────────

    async def _refresh_token(self) -> None:
        """Obtain a fresh Bearer token from POST /sites."""
        url = f"{self._settings.rozetka_api_base_url}/sites"
        # Payload includes username and password (already base64-encoded as per settings).
        payload = {
            "username": self._settings.rozetka_api_username,
            "password": self._settings.rozetka_api_password_b64,  # already base64
        }
        # Use httpx to make an async POST request to the auth endpoint with a reasonable timeout.
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        # Check if the response indicates success and contains the access token. If not, raise an error with details.
        if not data.get("success"):
            errors = data.get("errors", {})
            # Log the error details for debugging before raising.
            raise RuntimeError(
                f"Rozetka auth failed: {errors.get('message')} (code {errors.get('code')})"
            )
        # Store the new token in the instance variable for use in subsequent API calls.
        self._token = data["content"]["access_token"]
        logger.debug("Rozetka token refreshed successfully")

    # ── Fetch & stage ─────────────────────────────────────────────────────────

    async def _fetch_and_stage(
        self, pool, since: datetime, to_date: Optional[datetime]
    ) -> dict:
        '''Fetch orders page by page and upsert them into staging tables, collecting stats.'''
        # Initialize stats counters and pagination variables.
        stats = {"orders": 0, "order_lines": 0, "errors": 0}
        page = 1

        # Format the since and to_date parameters as strings in YYYY-MM-DD format for the API query.
        since_str = since.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d") if to_date else None

        # Loop through pages of orders until we have fetched all pages.
        # The loop will break when an empty page is returned or when we reach the last page.
        while True:
            # Fetch a single page of orders from the API.
            # This will raise an exception if the API call fails, which will be caught in the run() method.
            page_data = await self._fetch_orders_page(page, since_str, to_str)

            orders = page_data.orders
            # If no orders are returned, we have reached the end of the available data, so break the loop.
            if not orders:
                break

            # For each order in the page, attempt to upsert it and its order lines into the database.
            for order in orders:
                try:
                    # Upsert the main order record into the staging table.
                    # This will insert a new record or update an existing one based on the unique constraint.
                    await self._upsert_order(pool, order)
                    await self._upsert_order_lines(pool, order)
                    stats["orders"] += 1
                    stats["order_lines"] += len(order.purchases)
                except Exception as exc:
                    # If any error occurs during the upsert of an order or its lines,
                    # log a warning with the order ID and error details, and increment the error count in stats.
                    logger.warning(
                        "Failed to stage Rozetka order %s: %s", order.id, exc
                    )
                    stats["errors"] += 1

            # Log the progress after each page is processed, including the current page number,
            # total pages, and number of orders fetched.
            logger.info(
                "Rozetka: page %d/%d — %d orders fetched",
                page, page_data.page_count, len(orders),
            )

            # If we have reached the last page, break the loop.
            # Otherwise, increment the page number to fetch the next page.
            if page >= page_data.page_count:
                break
            page += 1

        return stats

    async def _fetch_orders_page(
        self, page: int, created_from: str, created_to: Optional[str]
    ) -> RozetkaOrdersPage:
        """Call GET /orders/search and return a parsed page of orders."""
        # Build the query parameters for the API call, including pagination, sorting, date filters, and expansions.
        params = {
            "page": page,
            "sort": "created",                          # oldest first for consistent pagination
            "created_from": created_from,
            "expand": "purchases,delivery",
            "type": 1,                                  # all order groups
        }
        # If a created_to date is provided, include it in the parameters to limit the date range of fetched orders.
        if created_to:
            params["created_to"] = created_to

        # Set the Authorization header with the Bearer token obtained from the auth step.
        headers = {"Authorization": f"Bearer {self._token}"}
        # Construct the full URL for the orders search endpoint.
        url = f"{self._settings.rozetka_api_base_url}/orders/search"

        # Make an async GET request to the orders search endpoint with the specified parameters and headers.
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        # Check if the API response indicates success.
        # If not, extract the error details and raise a RuntimeError with that information.
        if not data.get("success"):
            errors = data.get("errors", {})
            raise RuntimeError(
                f"Rozetka orders fetch failed: {errors.get('message')} "
                f"(code {errors.get('code')})"
            )

        # Parse the content of the response to extract the orders and pagination metadata.
        content = data.get("content", {})
        meta = content.get("_meta", {})
        raw_orders = content.get("orders", [])

        # Convert the raw order data into a list of RozetkaOrder objects using the Pydantic model.
        orders = [RozetkaOrder(**o) for o in raw_orders]
        # Return a RozetkaOrdersPage object containing the list of orders and pagination metadata for use by the caller.
        return RozetkaOrdersPage(
            orders=orders,
            total_count=meta.get("totalCount", 0),
            page_count=meta.get("pageCount", 1),
            current_page=meta.get("currentPage", page),
            per_page=meta.get("perPage", _ORDERS_PER_PAGE),
        )

    # ── Upsert helpers ────────────────────────────────────────────────────────

    async def _upsert_order(self, pool, order: RozetkaOrder) -> None:
        '''Upsert a single order into the staging table.'''
        delivery_city = None
        delivery_service = None
        # If the order has delivery information, extract the city name and delivery service name for staging.
        if order.delivery:
            # The delivery city may be None if not provided, so we check for that before accessing the name.
            delivery_city = (
                order.delivery.city.name if order.delivery.city else None
            )
            # The delivery service name may also be None if not provided, so we check for that as well.
            delivery_service = order.delivery.delivery_service_name

        async with pool.acquire() as conn:
            # Use an upsert (INSERT ... ON CONFLICT) to insert the order into the staging table.
            # The unique constraint is on rozetka_order_id, so if a record with the same order ID already exists,
            # it will update certain fields instead of inserting a new record.
            await conn.execute(
                """
                INSERT INTO staging.stg_rozetka_orders (
                    rozetka_order_id, market_id, created, changed,
                    amount, amount_with_discount,
                    cost, cost_with_discount,
                    status, status_group,
                    user_phone, delivery_city, delivery_service,
                    ttn, total_quantity, raw_json
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15,$16
                )
                ON CONFLICT (rozetka_order_id) DO UPDATE SET
                    changed              = EXCLUDED.changed,
                    status               = EXCLUDED.status,
                    status_group         = EXCLUDED.status_group,
                    cost_with_discount   = EXCLUDED.cost_with_discount,
                    raw_json             = EXCLUDED.raw_json,
                    ingested_at          = NOW()
                """,
                order.id,
                order.market_id,
                order.created,
                order.changed,
                float(order.amount) if order.amount else None,
                float(order.amount_with_discount) if order.amount_with_discount else None,
                float(order.cost) if order.cost else None,
                float(order.cost_with_discount) if order.cost_with_discount else None,
                order.status,
                order.status_group,
                order.user_phone,
                delivery_city,
                delivery_service,
                order.ttn,
                order.total_quantity,
                json.dumps(order.model_dump(mode="json")),
            )

    async def _upsert_order_lines(self, pool, order: RozetkaOrder) -> None:
        '''Upsert order lines (purchases) for a given order into the staging table.'''
        # If the order has no purchases, there are no order lines to upsert, so we can return early.
        if not order.purchases:
            return
        # Prepare a list of records to upsert, one for each purchase in the order.
        # Each record is a tuple of values corresponding to the columns in the staging table.
        records = [
            (
                order.id,
                item.id,
                item.item_id,
                item.item_name,
                item.quantity,
                float(item.price) if item.price else None,
                float(item.cost) if item.cost else None,
                float(item.cost_with_discount) if item.cost_with_discount else None,
                json.dumps(item.model_dump(mode="json")),
            )
            for item in order.purchases
        ]

        # Use an executemany with an upsert to insert or update all order lines for the order in a single database call.
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO staging.stg_rozetka_order_lines (
                    rozetka_order_id, purchase_id, item_id, item_name,
                    quantity, price, cost, cost_with_discount, raw_json
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (purchase_id) DO UPDATE SET
                    quantity           = EXCLUDED.quantity,
                    cost_with_discount = EXCLUDED.cost_with_discount,
                    raw_json           = EXCLUDED.raw_json,
                    ingested_at        = NOW()
                """,
                records,
            )

    # ── Sync log ──────────────────────────────────────────────────────────────

    async def _get_last_sync(self, pool) -> datetime:
        '''Query the database for the most recent successful sync timestamp for the Rozetka channel.'''
        async with pool.acquire() as conn:
            # Fetch the most recent completed_at timestamp from the sync_log table for successful Rozetka syncs, ordered by completion time descending.
            row = await conn.fetchrow(
                """
                SELECT completed_at FROM mis.sync_log
                WHERE channel = 'rozetka' AND status = 'success'
                ORDER BY completed_at DESC LIMIT 1
                """
            )
        # If a row is returned and it has a completed_at timestamp, return that timestamp.
        # Otherwise, return a default old date (e.g. Jan 1, 2015) to ensure we fetch all orders on the first run.
        if row and row["completed_at"]:
            return row["completed_at"]
        return datetime(2015, 1, 1)

    async def _start_sync_log(self, pool, since: datetime) -> int:
        '''Insert a new sync_log entry with status 'running' and return the generated sync_id.'''
        async with pool.acquire() as conn:
            # Insert a new record into the sync_log table with the channel set to 'rozetka',
            # sync_type set to 'scheduled', started_at set to the current time, and status set to 'running'.
            row = await conn.fetchrow(
                """
                INSERT INTO mis.sync_log (channel, sync_type, started_at, status)
                VALUES ('rozetka', 'scheduled', NOW(), 'running')
                RETURNING sync_id
                """
            )
        # Return the generated sync_id from the inserted record, which will be used later to update the log entry with the final status and stats.
        return row["sync_id"]

    async def _finish_sync_log(
        self, pool, sync_id: int, status: str, stats: dict, error: str = None
    ) -> None:
        '''Update the sync_log entry with the final status, stats, and error details if applicable.'''
        async with pool.acquire() as conn:
            # Update the sync_log record with the given sync_id, setting the completed_at timestamp to now,
            # the status to either 'success' or 'failed', the records_ingested and records_failed counts from the stats dictionary, and any error details if provided.
            await conn.execute(
                """
                UPDATE mis.sync_log SET
                    completed_at     = NOW(),
                    status           = $1,
                    records_ingested = $2,
                    records_failed   = $3,
                    error_detail     = $4
                WHERE sync_id = $5
                """,
                status,
                stats.get("orders", 0),
                stats.get("errors", 0),
                error,
                sync_id,
            )

