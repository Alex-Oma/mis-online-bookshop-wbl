"""
Utility script — run all database migrations in order.

Usage:
    python scripts/migrate.py

Environment variables required:
    DATABASE_URL   — PostgreSQL connection string
"""
import asyncio
import glob
import os
import sys

# Add parent directory to sys.path to import app modules if needed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    # We use asyncpg directly for migrations to avoid adding dependencies like Alembic or SQLAlchemy
    import asyncpg
except ImportError:
    print("ERROR: asyncpg is not installed. Run: pip install asyncpg")
    sys.exit(1)


async def run_migrations():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)

    # db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")

    # Connect to the database
    conn = await asyncpg.connect(db_url)

    # Find all .sql files in the migrations/ directory
    migrations_dir = os.path.join(os.path.dirname(__file__), "..", "migrations")
    migration_files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))

    if not migration_files:
        print("No migration files found in migrations/ directory.")
        sys.exit(1)

    # Run each migration file in order
    print(f"Found {len(migration_files)} migration file(s).\n")

    # We run each migration in a try-except block to report which file failed, but we re-raise the exception to stop further migrations if one fails.
    try:
        for filepath in migration_files:
            filename = os.path.basename(filepath)
            with open(filepath, encoding="utf-8") as f:
                # We read the SQL from the file and execute it.
                sql = f.read()
            try:
                # We execute the SQL directly.
                await conn.execute(sql)
                print(f"  SUCCESS:  {filename}")
            except Exception as exc:
                print(f"  FAILURE:  {filename} — ERROR: {exc}")
                raise

        print("\nAll migrations applied successfully.")
    finally:
        # Close the database connection
        await conn.close()


if __name__ == "__main__":
    # Run the migrations
    asyncio.run(run_migrations())

