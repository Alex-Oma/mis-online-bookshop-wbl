# MyEnglishBooks Management Information System (MIS)

A comprehensive data analytics and management platform designed for the **MyEnglishBooks** online bookshop. This system aggregates sales data from multiple channels (Website/OpenCart and Rozetka Marketplace), transforms it into actionable insights, and provides tools for decision-making regarding inventory, pricing, and marketing strategies.

## 📖 Overview

The MIS solves the problem of fragmented data by centralizing information from:
1.  **MyEnglishBooks website** (OpenCart-based website)
2.  **Rozetka.ua** (Marketplace)

It ingests raw sales and product data, normalizes it into a reliable data warehouse (PostgreSQL), and serves it through an interactive web dashboard and automated reports.

## 🚀 Key Features

*   **Multi-Channel Data Ingestion**: Automated extraction from OpenCart MySQL database and Rozetka Seller API.
*   **Data Warehouse**: Normalized star Schema (Facts & Dimensions) optimized for analytics.
*   **Interactive Dashboard**:
    *   KPIs: Revenue, Order Counts, Top Sellers.
    *   Visualizations: Sales trends, Channel splits, Category performance.
*   **Automated Reporting**:
    *   Generate PDF and Excel reports on-demand.
    *   Report types: Weekly Sales, Monthly Revenue, Inventory Status, Seasonal Trends.
*   **Alerting System**:
    *   Real-time alerts for low stock levels.
    *   Configurable thresholds and email notifications via SMTP.
*   **User Management**: Role-based access control (Admin, Manager, Viewer) and audit logging.

## 🏗 Architecture

The system is built as a modular monolithic application using **Python 3.12** and **FastAPI**.

```mermaid
graph TD
    subgraph Sales Channels
        OC[(OpenCart MySQL)]
        RZ[Rozetka API]
    end

    subgraph MIS Backend
        Ingest[Ingestion Engine]
        Trans[Transformer]
        API[FastAPI Server]
        Sched[APScheduler]
        Alerts[Alert Engine]
        Reports[Report Generator]
    end

    subgraph Storage
        DB[(PostgreSQL\nWarehouse)]
    end

    subgraph Users
        Admin[Admin User]
        Mgr[Manager]
    end

    OC -->|Pull| Ingest
    RZ -->|Pull| Ingest
    Ingest --> Trans --> DB
    Sched -->|Trigger| Ingest
    Sched -->|Trigger| Alerts
    Alerts -->|Check| DB
    Alerts -->|Email| Admin
    
    Admin -->|HTTPS| API
    Mgr -->|HTTPS| API
    API -->|Query| DB
    API -->|Generate| Reports
```

### Components

1.  **Ingestion Layer** (`app/ingestion`): Adapters connect to external sources. The `Transformer` maps raw external data (OpenCart orders, Rozetka JSON) into internal Pydantic models and persists them to the Staging area.
2.  **Database Layer** (`app/database.py`, `migrations/`): Uses **asyncpg** for high-performance async database access. The schema follows a data warehouse approach with Staging tables (raw import) and Core tables (clean facts/dimensions).
3.  **Application Logic** (`app/routers`):
    *   `auth.py`: JWT-based authentication.
    *   `admin.py`: Serves server-side rendered UI (Jinja2) for dashboards.
    *   `ingest.py`: Triggering manual syncs.
    *   `reports.py` & `alerts.py`: Management endpoints.
4.  **Scheduler** (`app/scheduler.py`): Runs background jobs for ingestion (every 6h) and alert checks (every 30m).

## 🛠 Tech Stack

*   **Language**: Python 3.12
*   **Framework**: FastAPI
*   **Database**: PostgreSQL (Supabase)
*   **ORM/Querying**: asyncpg (Raw SQL for performance), SQLAlchemy (Core)
*   **Templating**: Jinja2 + Bootstrap 5 (Admin UI)
*   **Reporting**: ReportLab (PDF), openpyxl (Excel)
*   **Testing**: Pytest, Playwright (E2E)
*   **Deployment**: Render.com

## ⚡ Getting Started

### Prerequisites
*   Python 3.12+
*   PostgreSQL 16+
*   Node.js (for Playwright tests)

### 1. Clone & Setup
```bash
git clone https://github.com/Alex-Oma/mis-online-bookshop-wbl.git
cd mis-online-bookshop-wbl

# Create virtual environment
python -m venv .venv
# Activate: 
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
Create a `.env` file in the root directory:
```ini
ENVIRONMENT=development
LOG_LEVEL=INFO

# MIS Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/mis_db

# Security
JWT_SECRET_KEY=your_super_secret_key
JWT_ALGORITHM=HS256

# External Integrations (Optional for local dry-run)
OPENCART_DB_HOST=localhost
OPENCART_DB_USER=root
OPENCART_DB_PASSWORD=root
ROZETKA_API_USERNAME=test
ROZETKA_API_PASSWORD_B64=base64pass
```

### 3. Database Setup
The project uses raw SQL migrations located in `migrations/`.
```bash
# Apply migrations
python scripts/migrate.py

# Create an Admin user
python scripts/create_admin.py
# Follow prompts to set username/password
```

### 4. Run the Server
```bash
# Run with uvicorn reloader
python run.ps1  # Windows
# OR
uvicorn app.main:app --reload
```
Access the Dashboard at: **http://127.0.0.1:8000/admin/login**  
API Documentation: **http://127.0.0.1:8000/docs**

## 🧪 Testing

The project includes Unit, Integration, Data Quality, and E2E tests.

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-playwright playwright

# Install browser binaries for E2E
playwright install chromium

# Run all tests
pytest

# Run specific suite
pytest tests/e2e
pytest tests/unit
```

## ☁️ Deployment

### Render.com
The project is configured for **Render** via `render.yaml` (Infrastructure as Code).

1.  Connect your GitHub repo to Render.
2.  Render will auto-detect the `Blueprint`.
3.  **Environment Variables**: You must set `DATABASE_URL` (Supabase connection string) and other secrets in the Render Dashboard -> Environment.
4.  **Build**: `pip install -r requirements.txt`
5.  **Start**: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

*Note: For connection to Supabase on Render, the code automatically resolves IPv4 addresses to bypass IPv6 routing issues.*

## 📂 Project Structure

```
app/
├── alerts/         # Alerting engine logic
├── auth/           # Authentication & Security
├── ingestion/      # ETL Adapters (Rozetka, Website)
├── models/         # Pydantic schemas (Data validation)
├── reports/        # PDF/Excel generation logic
├── routers/        # API & UI Endpoints
├── static/         # CSS, JS, Images, Fonts
├── templates/      # HTML (Jinja2) templates
├── config.py       # Pydantic settings management
├── database.py     # AsyncPG connection pool
├── main.py         # App entry point
└── scheduler.py    # Background task scheduler
migrations/         # SQL DDL files (Schemas, Tables)
scripts/            # Utility scripts (Admin creation, migrations)
tests/              # Test suites
```

