import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.database import get_pool, close_pool
from app.scheduler import start_scheduler, stop_scheduler
from app.routers import auth, ingest, reports, alerts, admin

# Configure root logger
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle handler."""
    # Load settings (also configures logging level)
    settings = get_settings()

    # Configure logging with timestamp, level, and message; level set from settings
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    # Initialise DB pool
    await get_pool()
    logger.info("MIS backend starting up (environment: %s)", settings.environment)

    # Start background scheduler
    start_scheduler()

    yield  # Application runs here

    # Shutdown sequence: stop scheduler, close DB pool
    stop_scheduler()
    await close_pool()
    logger.info("MIS backend shut down cleanly")


def create_app() -> FastAPI:
    # Load settings early to determine docs availability based on environment
    settings = get_settings()

    # Create FastAPI app with metadata and conditional docs
    app = FastAPI(
        title="MyEnglishBooks MIS",
        description="Management Information System for MyEnglishBooks online bookshop",
        version="1.0.0",
        docs_url="/api/docs" if settings.environment == "development" else None,
        redoc_url=None,
        lifespan=lifespan,
    )

    # Static files & templates
    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    # Include routers
    app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
    app.include_router(ingest.router, prefix="/ingest", tags=["Ingestion"])
    app.include_router(reports.router, prefix="/reports", tags=["Reports"])
    app.include_router(alerts.router, prefix="/alerts", tags=["Alerts"])
    app.include_router(admin.router, prefix="/admin", tags=["Admin UI"])

    # Health check endpoint
    @app.get("/health", tags=["Health"])
    async def health_check():
        return {"status": "ok", "service": "MyEnglishBooks MIS"}

    return app


app = create_app()

