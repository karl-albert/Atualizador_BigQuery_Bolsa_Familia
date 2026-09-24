@echo off
chcp 65001 > nul
title Atualizador Mensal Bolsa Família - Google BigQuery
color 09

echo ================================================================================
echo   BOLSA FAMÍLIA ANALYTICS — ATUALIZADOR MENSAL GOOGLE BIGQUERY
echo ================================================================================
echo.

python "%~dp0atualizador_mensal_bigquery.py"

echo.
echo ================================================================================
echo   Processo finalizado. Pressione qualquer tecla para sair...
echo ================================================================================
pause > nul
