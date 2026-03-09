"""
test_mysql_connection.py — Verify connectivity to the OpenCart MySQL database.

Checks:
  1. TCP connection to host:port
  2. Authentication with supplied credentials
  3. Read access to the target database
  4. Presence of the expected OpenCart core tables
  5. Row counts for the key tables used by the MIS ingestion layer

Usage:
    python scripts/test_mysql_connection.py

Reads credentials from .env (OPENCART_DB_* variables).
"""
import os
import sys
import socket
import time

# ── Load .env before anything else ───────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    print("WARNING: python-dotenv not installed — relying on shell environment variables.")

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✘{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}!{RESET}  {msg}")
def info(msg):  print(f"  {CYAN}i{RESET}  {msg}")
def header(msg):print(f"\n{BOLD}{msg}{RESET}")

# ── Expected OpenCart tables (MIS ingestion depends on these) ─────────────────
EXPECTED_TABLES = [
    "oc_order",
    "oc_order_product",
    "oc_order_total",
    "oc_order_status",
    "oc_product",
    "oc_product_description",
    "oc_product_to_category",
    "oc_category",
    "oc_category_description",
    "oc_manufacturer",
    "oc_manufacturer_description",
    "oc_customer",
    "oc_customer_group",
    "oc_customer_group_description",
    "oc_stock_status",
]


def get_config() -> dict:
    """Read OpenCart DB settings from environment / .env."""
    return {
        "host":     os.environ.get("OPENCART_DB_HOST", ""),
        "port":     int(os.environ.get("OPENCART_DB_PORT", "3306")),
        "database": os.environ.get("OPENCART_DB_NAME", "bookshop"),
        "user":     os.environ.get("OPENCART_DB_USER", ""),
        "password": os.environ.get("OPENCART_DB_PASSWORD", ""),
    }


def check_tcp(host: str, port: int, timeout: float = 5.0) -> bool:
    """Return True if TCP connection to host:port succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def main():
    print(f"\n{BOLD}{'═' * 55}{RESET}")
    print(f"{BOLD}  MyEnglishBooks MIS — OpenCart MySQL Connection Test{RESET}")
    print(f"{BOLD}{'═' * 55}{RESET}")

    cfg = get_config()

    # ── 0. Config presence check ──────────────────────────────────────────────
    header("Step 1 — Configuration")
    missing = [k for k, v in cfg.items() if not str(v)]
    if missing:
        for m in missing:
            fail(f"OPENCART_DB_{m.upper()} is not set in .env")
        print(f"\n{RED}Aborting — fix the missing variables in your .env file.{RESET}\n")
        sys.exit(1)

    ok(f"Host     : {cfg['host']}")
    ok(f"Port     : {cfg['port']}")
    ok(f"Database : {cfg['database']}")
    ok(f"User     : {cfg['user']}")
    info("Password : {'*' * len(cfg['password'])}")

    # ── 1. TCP reachability ───────────────────────────────────────────────────
    header("Step 2 — TCP Reachability")
    t0 = time.perf_counter()
    if check_tcp(cfg["host"], cfg["port"]):
        elapsed = (time.perf_counter() - t0) * 1000
        ok(f"TCP connection to {cfg['host']}:{cfg['port']} succeeded ({elapsed:.0f} ms)")
    else:
        fail(f"Cannot reach {cfg['host']}:{cfg['port']} — check host, port, and firewall rules")
        sys.exit(1)

    # ── 2. MySQL authentication ───────────────────────────────────────────────
    header("Step 3 — MySQL Authentication")
    try:
        import pymysql
    except ImportError:
        fail("pymysql is not installed. Run: pip install pymysql")
        sys.exit(1)

    try:
        t0 = time.perf_counter()
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=10,
            charset="utf8mb4",
        )
        elapsed = (time.perf_counter() - t0) * 1000
        ok(f"Authenticated as '{cfg['user']}' on '{cfg['database']}' ({elapsed:.0f} ms)")
    except pymysql.err.OperationalError as exc:
        fail(f"Authentication failed: {exc}")
        sys.exit(1)

    cursor = conn.cursor()

    # ── 3. MySQL server info ───────────────────────────────────────────────────
    header("Step 4 — Server Info")
    cursor.execute("SELECT VERSION(), @@hostname, @@character_set_database;")
    version, hostname, charset = cursor.fetchone()
    ok(f"MySQL version : {version}")
    ok(f"Server host   : {hostname}")
    ok(f"DB charset    : {charset}")

    # ── 4. Table presence check ───────────────────────────────────────────────
    header("Step 5 — OpenCart Table Presence")
    cursor.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
        (cfg["database"],),
    )
    existing_tables = {row[0] for row in cursor.fetchall()}

    all_present = True
    for table in EXPECTED_TABLES:
        if table in existing_tables:
            ok(f"{table}")
        else:
            fail(f"{table}  ← NOT FOUND")
            all_present = False

    if not all_present:
        warn("Some expected tables are missing — ingestion may fail for those tables.")

    # ── 5. Row counts ─────────────────────────────────────────────────────────
    header("Step 6 — Row Counts (key tables)")
    count_tables = [
        ("oc_order",         "Orders"),
        ("oc_order_product", "Order lines"),
        ("oc_product",       "Products"),
        ("oc_customer",      "Customers"),
        ("oc_category",      "Categories"),
        ("oc_manufacturer",  "Manufacturers / Publishers"),
    ]
    total_orders = 0
    for table, label in count_tables:
        if table not in existing_tables:
            warn(f"{label:<30} — table missing, skipped")
            continue
        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        count = cursor.fetchone()[0]
        if table == "oc_order":
            total_orders = count
        ok(f"{label:<30} {count:>8,} rows")

    # ── 6. Latest order date ──────────────────────────────────────────────────
    header("Step 7 — Latest Order Date")
    if "oc_order" in existing_tables:
        cursor.execute("SELECT MAX(date_added) FROM oc_order")
        latest = cursor.fetchone()[0]
        if latest:
            ok(f"Most recent order : {latest}")
        else:
            warn("oc_order table is empty — no orders found")

    cursor.close()
    conn.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'═' * 55}{RESET}")
    if all_present:
        print(f"{GREEN}{BOLD}  ✔  All checks passed — OpenCart MySQL is reachable{RESET}")
        print(f"     and ready for MIS ingestion ({total_orders:,} orders found).")
    else:
        print(f"{YELLOW}{BOLD}  !  Connected successfully but some tables are missing.{RESET}")
        print(f"     Check the table names above and update EXPECTED_TABLES if needed.")
    print(f"{BOLD}{'═' * 55}{RESET}\n")


if __name__ == "__main__":
    main()

