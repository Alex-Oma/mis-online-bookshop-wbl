"""
AlertEngine — evaluates all active alert rules against live data
and dispatches email notifications when thresholds are breached.

Supported rule types:
  low_stock    — product quantity_in_stock <= threshold
  revenue_spike — MTD revenue changed by >= threshold % vs. prior month
  no_orders    — no orders received in the last N hours (threshold = hours)
"""
import logging
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Optional dependency for sending emails asynchronously
try:
    import aiosmtplib
except ImportError:  # pragma: no cover
    aiosmtplib = None  # type: ignore[assignment]

from app.config import get_settings
from app.database import get_pool

# Set up module-level logger
logger = logging.getLogger(__name__)


class AlertEngine:
    """Evaluates alert rules and sends email notifications."""

    def __init__(self):
        # Load settings once during initialization
        self._settings = get_settings()

    async def check_all(self) -> None:
        """Run all active alert rule checks."""
        # get database connection pool
        pool = await get_pool()
        # Fetch active rules from the database
        async with pool.acquire() as conn:
            rules = await conn.fetch(
                "SELECT * FROM mis.alert_rule WHERE is_active = TRUE"
            )

        # Evaluate each rule and handle exceptions to ensure all rules are checked
        for rule in rules:
            # Each rule check is wrapped in a try-except block to prevent one failing rule from stopping the entire process
            try:
                # Determine which check method to call based on the rule type
                rule_type = rule["rule_type"]
                if rule_type == "low_stock":
                    # Call the method to check for low stock levels
                    await self._check_low_stock(rule)
                elif rule_type == "revenue_spike":
                    # Call the method to check for revenue spikes compared to the prior month
                    await self._check_revenue_spike(rule)
                elif rule_type == "no_orders":
                    # Call the method to check for lack of orders in the specified time frame
                    await self._check_no_orders(rule)
                else:
                    # Log a warning if the rule type is unknown, but continue processing other rules
                    logger.warning("Unknown alert rule type: %s", rule_type)
            except Exception as exc:
                # Log the exception with rule details but continue processing other rules
                logger.error(
                    "Alert rule %d (%s) check failed: %s",
                    rule["rule_id"], rule["rule_type"], exc,
                    exc_info=True,
                )

    # ── Rule: low stock ───────────────────────────────────────────────────────

    async def _check_low_stock(self, rule: dict) -> None:
        # Alert when any active product has quantity_in_stock <= threshold, with cooldown to prevent spamming.
        # Get database connection pool
        pool = await get_pool()
        # Convert threshold to float for comparison
        threshold = float(rule["threshold"])

        # Fetch products that are at or below the stock threshold and are active
        async with pool.acquire() as conn:
            # The query selects product details for products that meet the low stock condition, ordered by quantity_in_stock ascending to prioritize the most critical stock levels
            at_risk = await conn.fetch(
                """
                SELECT p.product_id, p.title, p.author, p.quantity_in_stock
                FROM core.dim_product p
                WHERE p.quantity_in_stock <= $1
                  AND p.status = 'active'
                ORDER BY p.quantity_in_stock ASC
                """,
                threshold,
            )

        # If no products are at risk, exit early to avoid unnecessary processing
        if not at_risk:
            return

        # Filter out products in cooldown
        products_to_alert = []
        # For each product that is at or below the stock threshold, check if an alert has been triggered for that product and rule within the cooldown period.
        # If so, skip sending another alert for that product until the cooldown expires.
        async with pool.acquire() as conn:
            # The loop iterates through each at-risk product and checks the most recent alert event for that product and rule.
            # If an alert was triggered within the cooldown period, the product is skipped; otherwise, it is added to the list of products to include in the alert email.
            for product in at_risk:
                # The query retrieves the most recent alert event for the given rule and product, ordered by triggered_at in descending order to get the latest event.
                last_event = await conn.fetchrow(
                    """
                    SELECT triggered_at FROM mis.alert_event
                    WHERE rule_id = $1 AND product_id = $2
                    ORDER BY triggered_at DESC LIMIT 1
                    """,
                    rule["rule_id"], product["product_id"],
                )
                # The cooldown period is determined by the cooldown_hours field in the rule, defaulting to 24 hours if not specified.
                # If a last event exists, the elapsed time since that event is calculated, and if it is less than the cooldown period, the product is skipped for alerting.
                cooldown_hours = rule["cooldown_hours"] or 24
                if last_event:
                    # Calculate the elapsed time since the last alert event for this product and rule
                    elapsed = datetime.now(timezone.utc) - last_event["triggered_at"].replace(
                        tzinfo=timezone.utc
                    )
                    # If the elapsed time is less than the cooldown period, skip adding this product to the alert list to prevent spamming
                    if elapsed < timedelta(hours=cooldown_hours):
                        continue
                # If there is no recent alert event within the cooldown period, add the product to the list of products to include in the alert email
                products_to_alert.append(product)

        # If there are no products to alert after applying the cooldown filter, exit early
        if not products_to_alert:
            return

        # Send email
        # The email subject includes the number of books at risk, and the body is rendered using a helper method that formats the product information into an HTML table.
        subject = f"⚠️ Low Stock Alert: {len(products_to_alert)} book(s) at risk"
        body = self._render_low_stock_email(products_to_alert, threshold)
        # The notification email address is determined by the notify_email field in the rule, falling back to a default alert_to_email from settings if not specified.
        notify_email = rule["notify_email"] or self._settings.alert_to_email
        # The email is sent using an asynchronous helper method that utilizes the aiosmtplib library to send the email without blocking the main execution flow of the alert engine.
        await self._send_email(notify_email, subject, body)

        # Log events
        async with pool.acquire() as conn:
            # For each product that triggered an alert, an entry is inserted into the mis.alert_event table to record the rule_id, product_id, timestamp of when the alert was triggered, details about the alert (in this case, the quantity in stock), and a flag indicating that an email was sent. This allows for tracking of alert events and enforcing cooldown periods for future alerts.
            for product in products_to_alert:
                # The SQL query inserts a new record into the mis.alert_event table for each product that triggered the low stock alert, including the rule_id, product_id, current timestamp, a JSONB detail field containing the quantity in stock, and a boolean flag indicating that an email was sent.
                await conn.execute(
                    """
                    INSERT INTO mis.alert_event
                        (rule_id, product_id, triggered_at, detail, email_sent)
                    VALUES ($1, $2, NOW(), $3::jsonb, TRUE)
                    """,
                    rule["rule_id"],
                    product["product_id"],
                    f'{{"quantity_in_stock": {product["quantity_in_stock"]}}}',
                )

    # ── Rule: revenue spike ───────────────────────────────────────────────────

    async def _check_revenue_spike(self, rule: dict) -> None:
        """Alert when current month revenue differs from prior month by >= threshold %."""
        # The method checks for significant changes in month-to-date revenue compared to the prior month, based on a percentage threshold defined in the rule. It retrieves the current and prior month revenue from the database, calculates the percentage change, and if it exceeds the threshold, sends an email alert with the details of the revenue change.
        threshold_pct = float(rule["threshold"])
        # Get database connection
        pool = await get_pool()

        # The SQL query uses a Common Table Expression (CTE) named "monthly" to calculate the total revenue for the current and prior month, grouped by month.
        # It then selects the current and prior month revenue using conditional aggregation based on the row number ordered by month in descending order.
        # This allows for a direct comparison of the two most recent months' revenue figures.
        async with pool.acquire() as conn:
            # The query calculates the total revenue for the current and prior month by truncating the date_added to the month level and summing the total_amount_uah for orders that match the specified channel_id (or all channels if channel_id is null).
            row = await conn.fetchrow(
                """
                WITH monthly AS (
                    SELECT
                        DATE_TRUNC('month', date_added) AS month,
                        SUM(total_amount_uah) AS revenue
                    FROM core.fact_orders
                    WHERE channel_id = COALESCE($1, channel_id)
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 2
                )
                SELECT
                    MAX(CASE WHEN row_number() OVER (ORDER BY month DESC) = 1
                        THEN revenue END) AS current_rev,
                    MAX(CASE WHEN row_number() OVER (ORDER BY month DESC) = 2
                        THEN revenue END) AS prior_rev
                FROM monthly
                """,
                rule["channel_id"],
            )

        # If there is no prior month revenue data (i.e., the query returns null or zero for prior_rev), the method exits early since a percentage change cannot be calculated without a baseline.
        if not row or not row["prior_rev"] or row["prior_rev"] == 0:
            return

        # The current and prior month revenue values are extracted from the query result, and the percentage change is calculated using the formula: abs((current - prior) / prior) * 100.
        current = float(row["current_rev"] or 0)
        prior = float(row["prior_rev"])
        pct_change = abs((current - prior) / prior) * 100

        # If the absolute percentage change meets or exceeds the threshold defined in the rule, an email alert is composed with the details of the revenue change, including the current and prior month revenue and the percentage change.
        # The alert is then sent to the specified email address, and an entry is logged in the mis.alert_event table to record the alert event with details about the percentage change and direction of the change (increase or decrease).
        if pct_change >= threshold_pct:
            # The direction of the revenue change is determined based on whether the current month revenue is greater than or less than the prior month revenue, which is included in the email subject and body for clarity.
            direction = "increase" if current > prior else "decrease"
            subject = f"📈 Revenue Alert: {pct_change:.1f}% {direction} vs. last month"
            body = (
                f"<p>This month's revenue: <strong>₴{current:,.0f}</strong><br>"
                f"Last month's revenue: <strong>₴{prior:,.0f}</strong><br>"
                f"Change: <strong>{pct_change:.1f}% {direction}</strong></p>"
            )
            # The notification email address is determined by the notify_email field in the rule, falling back to a default alert_to_email from settings if not specified. The email is sent using an asynchronous helper method that utilizes the aiosmtplib library to send the email without blocking the main execution flow of the alert engine.
            notify_email = rule["notify_email"] or self._settings.alert_to_email
            # The email is sent using an asynchronous helper method that utilizes the aiosmtplib library to send the email without blocking the main execution flow of the alert engine.
            await self._send_email(notify_email, subject, body)
            # After sending the email alert, an entry is inserted into the mis.alert_event table to record the rule_id, timestamp of when the alert was triggered, details about the percentage change and direction of the revenue change, and a flag indicating that an email was sent.
            # This allows for tracking of alert events and enforcing cooldown periods for future alerts if needed.
            async with pool.acquire() as conn:
                # The SQL query inserts a new record into the mis.alert_event table for the revenue spike alert, including the rule_id, current timestamp, a JSONB detail field containing the percentage change and direction of the revenue change, and a boolean flag indicating that an email was sent.
                await conn.execute(
                    """
                    INSERT INTO mis.alert_event
                        (rule_id, triggered_at, detail, email_sent)
                    VALUES ($1, NOW(), $2::jsonb, TRUE)
                    """,
                    rule["rule_id"],
                    f'{{"pct_change": {pct_change:.2f}, "direction": "{direction}"}}',
                )

    # ── Rule: no orders ───────────────────────────────────────────────────────

    async def _check_no_orders(self, rule: dict) -> None:
        """Alert when no orders have been received in the last N hours."""
        # The method checks for the absence of orders within a specified time frame (N hours) defined in the rule's threshold. It queries the database to count the number of orders received in the last N hours, and if the count is zero, it sends an email alert indicating that no orders have been received during that period.
        hours = int(rule["threshold"])
        # Get database connection
        pool = await get_pool()

        # The SQL query counts the number of orders in the core.fact_orders table where the date_added is within the last N hours (using NOW() - INTERVAL '1 hour' * $1) and matches the specified channel_id (or all channels if channel_id is null).
        async with pool.acquire() as conn:
            # The result is a single row containing the count of orders that meet the criteria.
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS cnt FROM core.fact_orders
                WHERE date_added >= NOW() - INTERVAL '1 hour' * $1
                  AND channel_id = COALESCE($2, channel_id)
                """,
                hours,
                rule["channel_id"],
            )

        # If the query returns a count of zero orders, an email alert is composed with a subject indicating that no orders have been received in the last N hours, and a body message prompting the recipient to check both sales channels.
        # The alert is then sent to the specified email address using an asynchronous helper method.
        if row and row["cnt"] == 0:
            subject = f"🔔 No Orders Alert: no orders in the last {hours} hours"
            body = f"<p>No orders have been received in the last <strong>{hours} hours</strong>. Please check both sales channels.</p>"
            notify_email = rule["notify_email"] or self._settings.alert_to_email
            # The email is sent using an asynchronous helper method that utilizes the aiosmtplib library to send the email without blocking the main execution flow of the alert engine.
            await self._send_email(notify_email, subject, body)

    # ── Email helpers ─────────────────────────────────────────────────────────

    def _render_low_stock_email(self, products: list, threshold: float) -> str:
        '''Render HTML email body for low stock alert with a table of at-risk products.'''
        rows = "".join(
            f"<tr><td>{p['title']}</td><td>{p['author'] or '—'}</td>"
            f"<td style='color:red;font-weight:bold'>{p['quantity_in_stock']}</td></tr>"
            for p in products
        )
        return f"""
        <html><body>
        <h2>⚠️ Low Stock Alert</h2>
        <p>The following books have stock at or below the threshold of
        <strong>{int(threshold)} copies</strong>:</p>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
          <thead>
            <tr>
              <th>Book Title</th><th>Author</th><th>Stock Remaining</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        <p style="color:grey;font-size:12px">
          Sent by MyEnglishBooks MIS · {datetime.now().strftime('%Y-%m-%d %H:%M')}
        </p>
        </body></html>
        """

    async def _send_email(self, to: str, subject: str, html_body: str) -> None:
        '''Send an email using aiosmtplib with the provided subject and HTML body.'''
        if aiosmtplib is None:
            # If the aiosmtplib library is not installed, log an error and skip sending the email to avoid runtime errors.
            logger.error("aiosmtplib is not installed — cannot send email alert")
            return
        # The email message is constructed using the email.mime library, with the subject, sender, recipient, and HTML body. The sender's name and email address are formatted based on the settings loaded during initialization.
        s = self._settings
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{s.smtp_from_name} <{s.smtp_username}>"
        msg["To"] = to
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        # The email is sent asynchronously using the aiosmtplib library, which allows the alert engine to continue processing other rules without waiting for the email sending operation to complete.
        try:
            await aiosmtplib.send(
                msg,
                hostname=s.smtp_host,
                port=s.smtp_port,
                username=s.smtp_username,
                password=s.smtp_password,
                start_tls=True,
            )
            logger.info("Alert email sent to %s: %s", to, subject)
        except Exception as exc:
            # If an error occurs during the email sending process, it is caught and logged as an error with details about the recipient and the exception message.
            # This ensures that issues with email delivery are recorded for troubleshooting without crashing the alert engine.
            logger.error("Failed to send alert email to %s: %s", to, exc)

