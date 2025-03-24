@echo off

REM Step 1: Check existing .env file and its contents
if exist .env (
    REM Read variables from .env
    for /f "tokens=1,2 delims==" %%a in (.env) do (
        set %%a=%%b
    )
    
    REM Check if all required variables exist and CREATE_NEW_ENV is FALSE
    if defined DAGSTER_HOME if defined WAREHOUSE_PATH if defined SOCRATA_API_TOKEN (
        if /i "!CREATE_NEW_ENV!"=="FALSE" (
            echo Existing valid .env configuration found, skipping setup...
            echo Starting Dagster development server...
            dagster dev
            exit /b 0
        )
    )
    
    REM If CREATE_NEW_ENV is TRUE, delete the existing .env
    if /i "!CREATE_NEW_ENV!"=="TRUE" (
        del .env 2>nul
    )
)

REM Step 2: Create the virtual environment
uv venv
IF NOT EXIST ".venv\Scripts\activate" (
    echo Error: Virtual environment not found at .venv\Scripts\activate
    exit /b 1
)

REM Step 3: Activate the virtual environment
call .venv\Scripts\activate || (
    echo Error: Failed to activate virtual environment
    exit /b 1
)

REM Step 4: Install dependencies
uv sync
IF ERRORLEVEL 1 (
    echo Error: Failed to sync dependencies
    exit /b 1
)

REM Step 5: Ask user for SOCRATA_API_TOKEN
set /p SOCRATA_API_TOKEN=Please enter your SOCRATA_API_TOKEN (press Enter to use the default community key):
IF "%SOCRATA_API_TOKEN%"=="" (
    set SOCRATA_API_TOKEN=uHoP8dT0q1BTcacXLCcxrDp8z
    echo Note: Using the default community key. Please use your own key if possible.
)

REM Step 6: Create a fresh .env
del .env 2>nul
echo. > .env

REM Step 7: Run exportpathwindows.py to retrieve WAREHOUSE_PATH (line1) and DAGSTER_HOME (line2)
setlocal enabledelayedexpansion
set i=0
for /f "delims=" %%a in ('uv run scripts/exportpathwindows.py') do (
    if !i! == 0 (
        set WAREHOUSE_PATH=%%a
    ) else (
        set DAGSTER_HOME=%%a
    )
    set /a i+=1
)

if "%WAREHOUSE_PATH%"=="" (
    echo Error: Failed to retrieve WAREHOUSE_PATH
    exit /b 1
)
if "%DAGSTER_HOME%"=="" (
    echo Error: Failed to retrieve DAGSTER_HOME
    exit /b 1
)

REM Step 8: Append to .env including CREATE_NEW_ENV=FALSE
echo SOCRATA_API_TOKEN=%SOCRATA_API_TOKEN% >> .env
echo WAREHOUSE_PATH=%WAREHOUSE_PATH% >> .env
echo DAGSTER_HOME=%DAGSTER_HOME% >> .env
echo CREATE_NEW_ENV=FALSE >> .env

REM Step 9: Generate dagster.yaml in DAGSTER_HOME
if not exist "%DAGSTER_HOME%" mkdir "%DAGSTER_HOME%"
uv run scripts\generate_dagsteryaml.py "%DAGSTER_HOME%" > "%DAGSTER_HOME%\dagster.yaml"

REM Step 10: Start Dagster dev server
echo Starting Dagster development server...
dagster dev