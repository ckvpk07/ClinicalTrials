@echo off
setlocal

echo ==================================================
echo        Clinical Trials Data Pipeline
echo Started: %date% %time%
echo ==================================================

REM ==================================================
REM 1. Snowflake configuration
REM ==================================================

echo.
echo Setting Snowflake configuration...

set "SNOWFLAKE_ACCOUNT=al01598"
set "SNOWFLAKE_HOST=al01598.ap-southeast-7.aws.snowflakecomputing.com"
set "SNOWFLAKE_USER=kulkarnichetan5"
set "SNOWFLAKE_WAREHOUSE=COMPUTE_WH"
set "SNOWFLAKE_DATABASE=CLINICAL_TRIALS"
set "SNOWFLAKE_SCHEMA=RAW"
set "SNOWFLAKE_ROLE=DATA_ENGINEER"

REM Private key location
set "SNOWFLAKE_PRIVATE_KEY_PATH=D:\ClinicalTrials\keys\snowflake_key.p8"

echo Snowflake configuration loaded.

REM ==================================================
REM 2. Activate Python virtual environment
REM ==================================================

echo.
echo [1/2] Activating Python virtual environment...

call "D:\ClinicalTrials\.venv\Scripts\activate.bat"

IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to activate Python virtual environment.
    exit /b 1
)

echo Virtual environment activated successfully.

REM ==================================================
REM 3. Run Python API ingestion
REM ==================================================

echo.
echo [1/2] Starting ClinicalTrials API ingestion...
echo.

cd /d "D:\ClinicalTrials\Python API"

IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Could not access Python API directory.
    exit /b 1
)

echo Current directory:
cd

echo.
echo Running:
echo extract_clinical_trials.py
echo.

python extract_clinical_trials.py

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo ==================================================
    echo ERROR: Python API ingestion FAILED
    echo ==================================================
    exit /b 1
)

echo.
echo Python API ingestion completed successfully.

REM ==================================================
REM 4. Run dbt pipeline
REM ==================================================

echo.
echo [2/2] Starting dbt transformation...
echo.

cd /d "D:\ClinicalTrials\clinical_trials_dbt"

IF %ERRORLEVEL% NEQ 0 (
    echo ERROR: Could not access dbt project directory.
    exit /b 1
)

echo Current dbt directory:
cd

echo.
echo Running:
echo dbt build --select +stg_ctgov_studies+ --target dev
echo.

dbt build --select +stg_ctgov_studies+ --target dev

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo ==================================================
    echo ERROR: dbt build FAILED
    echo ==================================================
    exit /b 1
)

echo.
echo dbt build completed successfully.

REM ==================================================
REM 5. Pipeline completed
REM ==================================================

echo.
echo ==================================================
echo     CLINICAL TRIALS PIPELINE COMPLETED
echo ==================================================
echo Started : %date%
echo Finished: %date% %time%
echo ==================================================

exit /b 0