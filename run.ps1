# run.ps1 — Start the MyEnglishBooks MIS backend
# Usage: .\run.ps1              (development, with auto-reload)
#        .\run.ps1 -Prod        (production mode, no reload)
#        .\run.ps1 -Port 9000   (custom port)

param(
    [switch]$Prod,
    [int]$Port = 8000,
    [string]$BindHost = "0.0.0.0"
)

# ── Resolve project root ──────────────────────────────────────────────────────
$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

# ── Check .env exists ─────────────────────────────────────────────────────────
if (-not (Test-Path "$ProjectRoot\.env")) {
    Write-Host "WARNING: .env file not found." -ForegroundColor Yellow
    Write-Host "         Copy .env.example to .env and fill in your secrets." -ForegroundColor Yellow
    Write-Host ""
}

# ── Check uvicorn is available ────────────────────────────────────────────────
if (-not (Get-Command uvicorn -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: uvicorn not found. Activate your environment and run:" -ForegroundColor Red
    Write-Host "       pip install -r requirements.txt" -ForegroundColor Red
    exit 1
}

# ── Launch ────────────────────────────────────────────────────────────────────
if ($Prod) {
    Write-Host ""
    Write-Host "  Starting MyEnglishBooks MIS  [PRODUCTION]" -ForegroundColor Green
    Write-Host "  URL : http://$($BindHost):$Port" -ForegroundColor Cyan
    Write-Host "  Mode: 2 workers, no reload" -ForegroundColor Cyan
    Write-Host ""
    uvicorn app.main:app `
        --host $BindHost `
        --port $Port `
        --workers 2
} else {
    Write-Host ""
    Write-Host "  Starting MyEnglishBooks MIS  [DEVELOPMENT]" -ForegroundColor Green
    Write-Host "  URL : http://$($BindHost):$Port" -ForegroundColor Cyan
    Write-Host "  Mode: auto-reload on file changes" -ForegroundColor Cyan
    Write-Host "  Admin UI : http://$($BindHost):$Port/admin/login" -ForegroundColor Cyan
    Write-Host "  API docs : http://$($BindHost):$Port/docs" -ForegroundColor Cyan
    Write-Host ""
    uvicorn app.main:app `
        --host $BindHost `
        --port $Port `
        --reload `
        --reload-dir app `
        --log-level info
}

