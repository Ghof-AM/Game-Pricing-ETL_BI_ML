@echo off
REM ============================================================
REM run_app.bat — Windows Launcher Streamlit App
REM Game Price Intelligence Dashboard
REM
REM Usage: run_app.bat
REM ============================================================

set PROJECT_DIR=%~dp0
set APP_FILE=%PROJECT_DIR%streamlit_app\app.py
set DATA_FILE=%PROJECT_DIR%data\processed\ml_features.csv

echo ======================================
echo  Game Price Intelligence — Streamlit
echo ======================================

IF NOT EXIST "%DATA_FILE%" (
    echo.
    echo Data belum ada. Jalankan ETL pipeline dulu:
    echo    python run_pipeline.py
    echo.
    pause
    exit /b 1
)

echo.
echo Data OK. Membuka aplikasi di http://localhost:8501
echo Tekan Ctrl+C untuk menghentikan.
echo.

cd /d "%PROJECT_DIR%"
streamlit run "%APP_FILE%" ^
    --server.port 8501 ^
    --theme.base dark ^
    --theme.primaryColor "#3b82f6" ^
    --theme.backgroundColor "#0f1629" ^
    --theme.secondaryBackgroundColor "#1e293b" ^
    --theme.textColor "#f1f5f9"

pause
