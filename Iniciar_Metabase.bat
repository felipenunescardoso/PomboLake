@echo off
chcp 65001 >nul

set MB_DB_TYPE=postgres
set MB_DB_HOST=localhost
set MB_DB_PORT=54320
set MB_DB_DBNAME=metabase_app
set MB_DB_USER=metabase
set MB_DB_PASS=suasenha

cd /d D:\Java\bin
.\java.exe -jar D:\Metabase\metabase.jar
pause
