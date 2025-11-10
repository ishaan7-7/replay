@echo off
REM start_uvicorn.bat - robust loader for FastAPI ingest (reads .env, activates venv, starts uvicorn)

SETLOCAL ENABLEDELAYEDEXPANSION

REM compute repo root (one level up from this script)
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%\.."
REM normalize
pushd "%ROOT%" >nul 2>&1
set "ROOT=%CD%"
popd >nul 2>&1

REM ---- load .env (simple parser: KEY=VALUE, skip comments and blank lines) ----
if exist "%ROOT%\.env" (
  for /f "usebackq tokens=1* delims==" %%A in ("%ROOT%\.env") do (
    REM skip comment lines that start with #
    echo %%A | findstr /b "#" >nul
    if errorlevel 1 (
      set "key=%%A"
      set "val=%%B"
      REM trim surrounding quotes (naive)
      set val=!val:"=!
      setx TEMP_ENV_!key! "!val!" >nul
      set "!key!=!val!"
    )
  )
) else (
  echo [WARN] No .env file found at %ROOT%\.env
)

REM If the .env set variables, they are now in the environment for this process.
REM ---- Activate venv if present ----
if exist "%ROOT%\.venv\Scripts\activate.bat" (
  call "%ROOT%\.venv\Scripts\activate.bat"
  echo [INFO] Virtualenv activated from %ROOT%\.venv
) else (
  echo [WARN] Virtualenv activate not found at %ROOT%\.venv\Scripts\activate.bat - continuing
)

REM ---- determine host & port ----
if not defined INGEST_HOST set "INGEST_HOST=127.0.0.1"
if not defined INGEST_PORT set "INGEST_PORT=8000"

echo [INFO] Starting FastAPI ingest on %INGEST_HOST%:%INGEST_PORT%
echo [INFO] FEATURES_CONFIG=%FEATURES_CONFIG%
echo [INFO] PROMETHEUS_PORT=%PROMETHEUS_PORT%

REM run uvicorn (single worker recommended due to aiokafka producer)
python -m uvicorn fastapi_ingest.app.main:app --host %INGEST_HOST% --port %INGEST_PORT% --workers 1

ENDLOCAL
@echo off
REM start_uvicorn.bat - robust loader for FastAPI ingest (reads .env, activates venv, starts uvicorn)

SETLOCAL ENABLEDELAYEDEXPANSION

REM compute repo root (one level up from this script)
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%\.."
REM normalize
pushd "%ROOT%" >nul 2>&1
set "ROOT=%CD%"
popd >nul 2>&1

REM ---- load .env (simple parser: KEY=VALUE, skip comments and blank lines) ----
if exist "%ROOT%\.env" (
  for /f "usebackq tokens=1* delims==" %%A in ("%ROOT%\.env") do (
    REM skip comment lines that start with #
    echo %%A | findstr /b "#" >nul
    if errorlevel 1 (
      set "key=%%A"
      set "val=%%B"
      REM trim surrounding quotes (naive)
      set val=!val:"=!
      setx TEMP_ENV_!key! "!val!" >nul
      set "!key!=!val!"
    )
  )
) else (
  echo [WARN] No .env file found at %ROOT%\.env
)

REM If the .env set variables, they are now in the environment for this process.
REM ---- Activate venv if present ----
if exist "%ROOT%\.venv\Scripts\activate.bat" (
  call "%ROOT%\.venv\Scripts\activate.bat"
  echo [INFO] Virtualenv activated from %ROOT%\.venv
) else (
  echo [WARN] Virtualenv activate not found at %ROOT%\.venv\Scripts\activate.bat - continuing
)

REM ---- determine host & port ----
if not defined INGEST_HOST set "INGEST_HOST=127.0.0.1"
if not defined INGEST_PORT set "INGEST_PORT=8000"

echo [INFO] Starting FastAPI ingest on %INGEST_HOST%:%INGEST_PORT%
echo [INFO] FEATURES_CONFIG=%FEATURES_CONFIG%
echo [INFO] PROMETHEUS_PORT=%PROMETHEUS_PORT%

REM run uvicorn (single worker recommended due to aiokafka producer)
python -m uvicorn fastapi_ingest.app.main:app --host %INGEST_HOST% --port %INGEST_PORT% --workers 1

ENDLOCAL
@echo off
REM start_uvicorn.bat - robust loader for FastAPI ingest (reads .env, activates venv, starts uvicorn)

SETLOCAL ENABLEDELAYEDEXPANSION

REM compute repo root (one level up from this script)
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%\.."
REM normalize
pushd "%ROOT%" >nul 2>&1
set "ROOT=%CD%"
popd >nul 2>&1

REM ---- load .env (simple parser: KEY=VALUE, skip comments and blank lines) ----
if exist "%ROOT%\.env" (
  for /f "usebackq tokens=1* delims==" %%A in ("%ROOT%\.env") do (
    REM skip comment lines that start with #
    echo %%A | findstr /b "#" >nul
    if errorlevel 1 (
      set "key=%%A"
      set "val=%%B"
      REM trim surrounding quotes (naive)
      set val=!val:"=!
      setx TEMP_ENV_!key! "!val!" >nul
      set "!key!=!val!"
    )
  )
) else (
  echo [WARN] No .env file found at %ROOT%\.env
)

REM If the .env set variables, they are now in the environment for this process.
REM ---- Activate venv if present ----
if exist "%ROOT%\.venv\Scripts\activate.bat" (
  call "%ROOT%\.venv\Scripts\activate.bat"
  echo [INFO] Virtualenv activated from %ROOT%\.venv
) else (
  echo [WARN] Virtualenv activate not found at %ROOT%\.venv\Scripts\activate.bat - continuing
)

REM ---- determine host & port ----
if not defined INGEST_HOST set "INGEST_HOST=127.0.0.1"
if not defined INGEST_PORT set "INGEST_PORT=8000"

echo [INFO] Starting FastAPI ingest on %INGEST_HOST%:%INGEST_PORT%
echo [INFO] FEATURES_CONFIG=%FEATURES_CONFIG%
echo [INFO] PROMETHEUS_PORT=%PROMETHEUS_PORT%

REM run uvicorn (single worker recommended due to aiokafka producer)
python -m uvicorn fastapi_ingest.app.main:app --host %INGEST_HOST% --port %INGEST_PORT% --workers 1

ENDLOCAL
@echo off
REM start_uvicorn.bat - robust loader for FastAPI ingest (reads .env, activates venv, starts uvicorn)

SETLOCAL ENABLEDELAYEDEXPANSION

REM compute repo root (one level up from this script)
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%\.."
REM normalize
pushd "%ROOT%" >nul 2>&1
set "ROOT=%CD%"
popd >nul 2>&1

REM ---- load .env (simple parser: KEY=VALUE, skip comments and blank lines) ----
if exist "%ROOT%\.env" (
  for /f "usebackq tokens=1* delims==" %%A in ("%ROOT%\.env") do (
    REM skip comment lines that start with #
    echo %%A | findstr /b "#" >nul
    if errorlevel 1 (
      set "key=%%A"
      set "val=%%B"
      REM trim surrounding quotes (naive)
      set val=!val:"=!
      setx TEMP_ENV_!key! "!val!" >nul
      set "!key!=!val!"
    )
  )
) else (
  echo [WARN] No .env file found at %ROOT%\.env
)

REM If the .env set variables, they are now in the environment for this process.
REM ---- Activate venv if present ----
if exist "%ROOT%\.venv\Scripts\activate.bat" (
  call "%ROOT%\.venv\Scripts\activate.bat"
  echo [INFO] Virtualenv activated from %ROOT%\.venv
) else (
  echo [WARN] Virtualenv activate not found at %ROOT%\.venv\Scripts\activate.bat - continuing
)

REM ---- determine host & port ----
if not defined INGEST_HOST set "INGEST_HOST=127.0.0.1"
if not defined INGEST_PORT set "INGEST_PORT=8000"

echo [INFO] Starting FastAPI ingest on %INGEST_HOST%:%INGEST_PORT%
echo [INFO] FEATURES_CONFIG=%FEATURES_CONFIG%
echo [INFO] PROMETHEUS_PORT=%PROMETHEUS_PORT%

REM run uvicorn (single worker recommended due to aiokafka producer)
python -m uvicorn fastapi_ingest.app.main:app --host %INGEST_HOST% --port %INGEST_PORT% --workers 1

ENDLOCAL
