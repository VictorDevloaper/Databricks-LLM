@echo off
set "PATH=%PATH%;C:\Program Files\nodejs"
echo ============================================================
echo   INICIALIZANDO PLATAFORMA LAKEHOUSE ERP
echo ============================================================

:: Inicia o Backend em uma nova janela
echo [1/2] Iniciando API Backend (FastAPI)...
start cmd /k "cd /d c:\Users\victor\Databricks\project_databricks_rag\api && python main.py"

:: Inicia o Frontend
echo [2/2] Iniciando Frontend (Vite)...
cd /d c:\Users\victor\Databricks\project_databricks_rag\frontend
call npm.cmd run dev

pause
