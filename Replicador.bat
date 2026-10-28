@echo off
setlocal

REM ============================================================
REM ATENCAO: MODO BOOTSTRAP ATIVO (--bootstrap abaixo).
REM Isto TRUNCA e reinsere TODAS as tabelas a cada execucao.
REM Rode agora para a PRIMEIRA CARGA. DEPOIS, REMOVA o
REM "--bootstrap" da linha do python ANTES de agendar no Windows.
REM ============================================================
REM Uso:
REM   com --bootstrap   -> TRUNCA destino + insert puro (primeira carga)
REM   sem --bootstrap   -> upsert (regime normal do AGENDADOR)

set PYTHON=C:\Users\svc-qlikv\PycharmProjects\WelcomeScreen\Pombo\.venv\Scripts\python.exe
set SCRIPT=C:\Users\svc-qlikv\PycharmProjects\WelcomeScreen\Pombo\replicador_dw_homolog.py

"%PYTHON%" "%SCRIPT%" --bootstrap
endlocal & exit /b %ERRORLEVEL%