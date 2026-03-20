"""
Utility script — creates the first admin user in the MIS database.

Usage:
    python scripts/create_admin.py

Environment variables required:
    DATABASE_URL   — PostgreSQL connection string
"""
import asyncio
import os
import sys

# Add parent directory to sys.path to import app modules if needed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Load variables from .env file into os.environ (if .env exists)
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg is not installed. Run: pip install asyncpg")
    sys.exit(1)
from app.auth.password import hash_password


async def create_admin():
    # We read the database URL from the environment variable.
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        # If it's not set, we print an error and exit.
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    # Check for CI environment variables to run non-interactively
    ci_user = os.environ.get("MIS_ADMIN_USER")
    ci_pass = os.environ.get("MIS_ADMIN_PASS")

    if ci_user and ci_pass:
        print(f"Running non-interactively using MIS_ADMIN_USER='{ci_user}'")
        username = ci_user
        password = ci_pass
        email = "admin@example.com"
        full_name = "Admin User"
    else:
        # We prompt the user for the admin username, email, full name, and password.
        # The username defaults to "admin" if left blank.
        username = input("Admin username [admin]: ").strip() or "admin"
        email = input("Admin email: ").strip()
        full_name = input("Full name (optional): ").strip() or None

        # We use getpass to read the password without echoing it to the console.
        import getpass
        password = getpass.getpass("Password (min 8 chars): ")
        if len(password) < 8:
            # If the password is too short, we print an error and exit.
            print("ERROR: Password must be at least 8 characters.")
            sys.exit(1)

    print(f"Creating admin user '{username}'...")

    # We hash the password using the same function as in the app.
    password_hash = hash_password(password)

    # We connect to the database using asyncpg.
    conn = await asyncpg.connect(db_url)
    try:
        # Get admin role_id
        role = await conn.fetchrow(
            "SELECT role_id FROM mis.mis_role WHERE role_name = 'admin'"
        )
        if not role:
            print("ERROR: Run migrations first — 'admin' role not found in mis.mis_role.")
            sys.exit(1)

        # Insert user
        # We fetch the user_id if the user already exists to handle reruns idempotently in CI
        existing_user = await conn.fetchrow(
            "SELECT user_id FROM mis.mis_user WHERE username = $1", username
        )
        if existing_user:
            print(f"User '{username}' already exists (user_id={existing_user['user_id']}). updating password...")
            await conn.execute(
                "UPDATE mis.mis_user SET password_hash = $1 WHERE user_id = $2",
                password_hash, existing_user['user_id']
            )
            user_id = existing_user['user_id']
        else:
            user = await conn.fetchrow(
                """
                INSERT INTO mis.mis_user (username, email, password_hash, full_name, is_active)
                VALUES ($1, $2, $3, $4, TRUE)
                RETURNING user_id
                """,
                username, email, password_hash, full_name,
            )
            user_id = user["user_id"]

            # Assign admin role
            # Check if role assignment exists
            existing_role = await conn.fetchval(
                "SELECT 1 FROM mis.mis_user_role WHERE user_id = $1 AND role_id = $2",
                user_id, role["role_id"]
            )
            if not existing_role:
                await conn.execute(
                    "INSERT INTO mis.mis_user_role (user_id, role_id) VALUES ($1, $2)",
                    user_id, role["role_id"],
                )

        # We print a success message with the new user's ID and instructions to log in.
        print(f"\nAdmin user '{username}' created/updated successfully (user_id={user_id}).")
        print("   You can now log in at /admin/login")

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    finally:
        # We close the database connection in the finally block to ensure it happens even if there's an error.
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_admin())

