@echo off
title Pombo Data Lake - Servico

cd /d C:\Users\svc-qlikv\PycharmProjects\WelcomeScreen\Pombo

echo ========================================
echo       INICIANDO POMBO DATA LAKE
echo ========================================
echo.

".venv\Scripts\python.exe" "main_servico.py"

echo.
echo ========================================
echo Pombo finalizado.
echo ========================================

pause