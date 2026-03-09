"""
Alert management endpoints for admins and managers.
Admins can create, toggle, and delete alert rules.
Managers can view alert events but cannot modify rules.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.auth.dependencies import get_current_user, require_role
from app.database import get_pool
from app.models.mis import AlertRuleCreate, AlertRuleRead

# Initialize the router for alert-related endpoints
router = APIRouter()


@router.get(
    "/rules",
    response_model=list[AlertRuleRead],
    dependencies=[Depends(require_role("admin"))],
)
async def list_alert_rules(current_user=Depends(get_current_user)):
    """Return all configured alert rules."""
    # Fetch alert rules from the database, ordered by creation date (newest first)
    pool = await get_pool()
    # Using async context manager to acquire a connection from the pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM mis.alert_rule ORDER BY created_at DESC"
        )
    # Convert each record to a dictionary before returning the list of rules
    return [dict(r) for r in rows]


@router.post(
    "/rules",
    response_model=AlertRuleRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role("admin"))],
)
async def create_alert_rule(
    rule: AlertRuleCreate,
    current_user=Depends(get_current_user),
):
    """Create a new alert rule."""
    # Get a connection from the pool and insert the new alert rule into the database
    pool = await get_pool()
    # Using async context manager to ensure the connection is properly released back to the pool
    async with pool.acquire() as conn:
        # Insert the new alert rule and return the created record
        # The SQL query uses parameterized inputs to prevent SQL injection and ensure data integrity
        row = await conn.fetchrow(
            """
            INSERT INTO mis.alert_rule (
                rule_type, rule_name, threshold,
                channel_id, category_id,
                cooldown_hours, notify_email,
                is_active, created_by
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,$8)
            RETURNING *
            """,
            rule.rule_type, rule.rule_name, float(rule.threshold),
            rule.channel_id, rule.category_id,
            rule.cooldown_hours, rule.notify_email,
            current_user.user_id,
        )
    # Convert the created record to a dictionary before returning it as the response
    return dict(row)


@router.patch(
    "/rules/{rule_id}/toggle",
    dependencies=[Depends(require_role("admin"))],
)
async def toggle_alert_rule(rule_id: int, current_user=Depends(get_current_user)):
    """Toggle an alert rule's active/inactive state."""
    # Get a connection from the pool and update the is_active field of the specified alert rule
    pool = await get_pool()
    # Using async context manager to ensure the connection is properly released back to the pool
    async with pool.acquire() as conn:
        # The SQL query toggles the is_active boolean field by setting it to its opposite value (NOT is_active)
        row = await conn.fetchrow(
            """
            UPDATE mis.alert_rule
            SET is_active = NOT is_active
            WHERE rule_id = $1
            RETURNING rule_id, rule_name, is_active
            """,
            rule_id,
        )
    if not row:
        # If no record was updated (i.e., the rule_id does not exist), raise a 404 Not Found error
        raise HTTPException(status_code=404, detail="Alert rule not found")

    # Convert the updated record to a dictionary before returning it as the response
    return dict(row)


@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_role("admin"))],
)
async def delete_alert_rule(rule_id: int, current_user=Depends(get_current_user)):
    """Delete an alert rule."""
    # Get a connection from the pool and delete the specified alert rule from the database
    pool = await get_pool()
    # Using async context manager to ensure the connection is properly released back to the pool
    async with pool.acquire() as conn:
        # The SQL query deletes the record with the specified rule_id and returns the result of the operation
        result = await conn.execute(
            "DELETE FROM mis.alert_rule WHERE rule_id = $1", rule_id
        )
    if result == "DELETE 0":
        # If no record was deleted (i.e., the rule_id does not exist), raise a 404 Not Found error
        raise HTTPException(status_code=404, detail="Alert rule not found")


@router.get(
    "/events",
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def list_alert_events(
    limit: int = Query(default=50, le=200),
    current_user=Depends(get_current_user),
):
    """Return recent alert events."""
    # Get a connection from the pool and fetch recent alert events, joining with related rule and product information
    pool = await get_pool()
    # Using async context manager to ensure the connection is properly released back to the pool
    async with pool.acquire() as conn:
        # The SQL query retrieves alert events along with their associated rule names and product titles (if applicable), ordered by the time they were triggered (newest first) and limited to the specified number of records
        rows = await conn.fetch(
            """
            SELECT
                e.event_id, e.rule_id, r.rule_name,
                e.product_id, p.title AS product_title,
                e.triggered_at, e.email_sent
            FROM mis.alert_event e
            JOIN mis.alert_rule r ON r.rule_id = e.rule_id
            LEFT JOIN core.dim_product p ON p.product_id = e.product_id
            ORDER BY e.triggered_at DESC
            LIMIT $1
            """,
            limit,
        )
    # Convert each record to a dictionary before returning the list of alert events
    return [dict(r) for r in rows]

