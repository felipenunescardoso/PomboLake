@echo off
chcp 65001 >nul
setlocal

REM ==================================================
REM  MIGRACAO METABASE: H2 -> POSTGRES (rodar 1x only)
REM  FIX v2: caminho do H2 vai como ARGUMENTO do
REM          load-from-h2 (MB_DB_FILE eh ignorado)
REM ==================================================

REM --- DESTINO: Postgres (ajuste a senha) ---
set MB_DB_TYPE=postgres
set MB_DB_HOST=localhost
set MB_DB_PORT=54320
set MB_DB_DBNAME=metabase_app
set MB_DB_USER=metabase
set MB_DB_PASS=suasenha

REM --- ORIGEM: arquivo H2 ---
set H2_DIR=D:\Java\bin
set H2_FILE=%H2_DIR%\metabase.db

REM --- Binarios ---
set JAVA_BIN=D:\Java\bin\java.exe
set MB_JAR=D:\Metabase\metabase.jar
set PGBIN=C:\Program Files\PostgreSQL\18\bin

REM --- Pasta de trabalho da migracao (copia limpa do H2) ---
set MIG_DIR=D:\Metabase\migracao

echo ==========================================
echo   Pre-flight checks
echo ==========================================

if exist "%H2_FILE%.lock.db" (
    echo [ERRO] %H2_FILE%.lock.db existe - o Metabase ESTA RODANDO.
    echo        Feche a janela do .bat e tente de novo.
    pause
    exit /b 1
)

if not exist "%H2_FILE%.mv.db" (
    echo [ERRO] Nao achei %H2_FILE%.mv.db
    pause
    exit /b 1
)

"%PGBIN%\pg_isready.exe" -h %MB_DB_HOST% -p %MB_DB_PORT% -U %MB_DB_USER% >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Postgres nao respondeu em %MB_DB_HOST%:%MB_DB_PORT%
    pause
    exit /b 1
)
echo [OK] Pre-flight OK
echo.

echo ==========================================
echo   Backup do H2 original
echo ==========================================
set BACKUP_DIR=D:\Metabase\backup_h2_%DATE:~6,4%%DATE:~3,2%%DATE:~0,2%
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
xcopy /Y "%H2_FILE%.*" "%BACKUP_DIR%\" >nul
echo [OK] Backup em %BACKUP_DIR%
echo.

echo ==========================================
echo   Copiando H2 para area de migracao
echo ==========================================
if not exist "%MIG_DIR%" mkdir "%MIG_DIR%"
copy /Y "%H2_FILE%.mv.db" "%MIG_DIR%\metabase.db.mv.db" >nul
if errorlevel 1 (
    echo [ERRO] Falha ao copiar o H2.
    pause
    exit /b 1
)
echo [OK] Copia em %MIG_DIR%
echo.

echo ==========================================
echo   Migrando H2 -^> %MB_DB_DBNAME%
echo ==========================================
REM --- LINHA CHAVE: caminho SEM .mv.db, como argumento ---
"%JAVA_BIN%" --add-opens java.base/java.nio=ALL-UNNAMED -jar "%MB_JAR%" load-from-h2 "%MIG_DIR%\metabase.db"

if errorlevel 1 (
    echo.
    echo [ERRO] Migracao falhou. Verifique:
    echo   - banco '%MB_DB_DBNAME%' VAZIO ^(sem tabelas^)
    echo   - usuario '%MB_DB_USER%' com OWNER no banco
    echo   - senha correta em MB_DB_PASS
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   [OK] MIGRACAO CONCLUIDA
echo ==========================================
echo   Proximos passos:
echo   1) Script INSERT do admin no pgAdmin
echo   2) Subir com start_metabase.bat
echo   3) NUNCA rodar o .bat antigo sem MB_DB_*
echo.
pause