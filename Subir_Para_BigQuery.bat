@echo off
title Carga Bolsa Familia para Google BigQuery
cd /d "%~dp0"
start pythonw app_upload_bigquery_gui.py
if %errorlevel% neq 0 (
    python app_upload_bigquery_gui.py
)
exit
