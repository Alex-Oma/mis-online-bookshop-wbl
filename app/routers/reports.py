"""
This module defines API endpoints for generating and downloading MIS reports.
Endpoints:
- POST /reports/generate: Generate a report based on specified parameters.
- GET /reports/download/{report_id}: Download a previously generated report.
- GET /reports/list: List recently generated reports.
Access to these endpoints is restricted to users with 'admin' or 'manager' roles.
"""
import os

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse

from app.auth.dependencies import get_current_user, require_role
from app.database import get_pool
from app.models.mis import ReportRequest, ReportResponse
from app.reports.generator import ReportGenerator

# Create a router for report-related endpoints
router = APIRouter()


@router.post(
    "/generate",
    response_model=ReportResponse,
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def generate_report(
    request: ReportRequest,
    current_user=Depends(get_current_user),
):
    """Generate a report in PDF or Excel format. Returns a download URL."""
    try:
        # Validate parameters and generate the report asynchronously
        result = await ReportGenerator().generate(
            report_type=request.report_type,
            from_date=request.from_date,
            to_date=request.to_date,
            format=request.format,
            channel_id=request.channel_id,
            category_id=request.category_id,
            generated_by=current_user.user_id,
        )
    except ValueError as exc:
        # Invalid parameters or unsupported report type/format
        raise HTTPException(status_code=400, detail=str(exc))

    # Audit log
    # Get DB connection pool and log the report generation action
    pool = await get_pool()
    # Log the report generation action with details about the report parameters
    async with pool.acquire() as conn:
        # Log the report generation action with details about the report parameters
        # This SQL statement inserts a new record into the mis.audit_log table with the user ID, action type, resource identifier, and a JSON detail of the report parameters
        await conn.execute(
            """
            INSERT INTO mis.audit_log (user_id, action, resource, detail)
            VALUES ($1, 'generate_report', $2, $3::jsonb)
            """,
            current_user.user_id,
            f"report:{request.report_type}",
            f'{{"format": "{request.format}", "from": "{request.from_date}", "to": "{request.to_date}"}}',
        )

    # Return the report generation result, including the report ID and download URL
    return ReportResponse(
        report_id=result["report_id"],
        download_url=f"/reports/download/{result['report_id']}",
        generated_at=result["generated_at"],
        report_type=result["report_type"],
        format=result["format"],
    )


@router.get(
    "/download/{report_id}",
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def download_report(
    report_id: int,
    current_user=Depends(get_current_user),
):
    """Stream the generated report file for download."""
    # Get DB connection pool and fetch the file path for the requested report ID
    pool = await get_pool()
    # Fetch the file path, format, and report type for the given report ID from the mis.scheduled_report table
    async with pool.acquire() as conn:
        # This SQL statement retrieves the file path, format, and report type for the specified report ID from the mis.scheduled_report table.
        row = await conn.fetchrow(
            "SELECT file_path, format, report_type FROM mis.scheduled_report WHERE report_id = $1",
            report_id,
        )

    if not row:
        # If no report is found with the given ID, raise a 404 Not Found error
        raise HTTPException(status_code=404, detail="Report not found")

    file_path = row["file_path"]
    if not os.path.exists(file_path):
        # If the report file does not exist on the filesystem, raise a 404 Not Found error indicating that the report file is no longer available
        raise HTTPException(status_code=404, detail="Report file no longer available")

    # Determine the media type based on the report format (PDF or Excel) and return a FileResponse to stream the file for download.
    # The filename is set to the base name of the file path.
    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if row["format"] == "xlsx"
        else "application/pdf"
    )
    # The filename is extracted from the file path using os.path.basename, which returns the final component of the file path (i.e., the name of the file).
    # This filename is then used in the FileResponse to suggest a name for the downloaded file.
    filename = os.path.basename(file_path)
    # Return a FileResponse that streams the report file for download, with the appropriate media type and filename.
    return FileResponse(path=file_path, media_type=media_type, filename=filename)


@router.get(
    "/list",
    dependencies=[Depends(require_role("admin", "manager"))],
)
async def list_reports(
    limit: int = Query(default=20, le=100),
    current_user=Depends(get_current_user),
):
    """Return recently generated reports."""
    # Get DB connection pool and fetch a list of recently generated reports, ordered by generation time in descending order, limited to the specified number of reports.
    pool = await get_pool()
    # This SQL statement retrieves a list of recently generated reports from the mis.scheduled_report table, including the report ID, type, format, parameters, generation time, and the user who generated it. The results are ordered by generation time in descending order and limited to the specified number of reports.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT report_id, report_type, format, parameters,
                   generated_at, generated_by
            FROM mis.scheduled_report
            ORDER BY generated_at DESC
            LIMIT $1
            """,
            limit,
        )
    # Convert the result rows to a list of dictionaries and return it as the response. Each dictionary represents a report with its details.
    return [dict(r) for r in rows]

