# Clinical Trials Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end data engineering pipeline for
extracting clinical trial data from ClinicalTrials.gov, loading the
data into Snowflake, transforming the data using dbt, and producing
business-ready analytical models.

The pipeline uses a layered data architecture consisting of RAW,
STAGING, INTERMEDIATE, FACT, and MART layers.

## 2. Business Question

The primary business question is:

> Which sponsor classes conduct the most clinical trials, and what
> proportion of their trials are stopped early?

The solution provides metrics such as:

- Total trials
- Early-stopped trials
- Closed trials
- Early-stop rate

## 3. Architecture

ClinicalTrials.gov API
        |
        v
Python API Ingestion
        |
        v
Snowflake RAW
        |
        v
dbt STAGING
        |
        v
dbt INTERMEDIATE
        |
        v
dbt FACT
        |
        v
dbt MART
        |
        v
Business Metrics

## 4. Technology Stack

- Python
- Snowflake
- dbt
- SQL
- REST API
- Windows Task Scheduler
- Windows Batch Script

## 5. Data Pipeline

### Step 1 – API Ingestion

Python connects to the ClinicalTrials.gov API and retrieves clinical
trial data.

The ingestion process retrieves the API response in pages, processes
the JSON payload, creates batches, and loads the data into the
Snowflake RAW layer.

The Python ingestion script is:

    extract_clinical_trials.py

### Step 2 – Raw Layer

The raw API data is stored in:

    RAW_CTGOV_STUDIES

This layer preserves the source data before transformation.

The RAW layer is used as the landing layer for downstream dbt
transformations.

### Step 3 – dbt Staging

The staging layer extracts and standardizes the required attributes
from the raw clinical trial data.

Model:

    stg_ctgov_studies

The staging layer prepares the source data for downstream
transformations.

### Step 4 – Intermediate Layer

The intermediate layer performs additional transformations required
to prepare the data for analytical processing.

Model:

    int_study_interventions

This layer handles transformation logic between the staging and fact
layers.

### Step 5 – Fact Layer

The fact model contains the transformed study and intervention data
used for downstream analytical processing.

Model:

    fct_study_interventions

This model provides the detailed data required by the analytical mart.

### Step 6 – Mart Layer

The mart layer provides business-ready aggregated information.

Model:

    sponsor_trial_summary

Key metrics:

- TOTAL_TRIALS
- EARLY_STOPPED_TRIALS
- CLOSED_TRIALS
- EARLY_STOP_RATE

## 6. dbt Lineage

The dbt dependency flow is:

    RAW_CTGOV_STUDIES
            |
            v
    stg_ctgov_studies
            |
            v
    int_study_interventions
            |
            v
    fct_study_interventions
            |
            v
    sponsor_trial_summary
            |
            v
    test_early_stop_rate

## 7. Data Quality

dbt tests are used to validate data quality and business rules.

Examples include:

- not_null
- unique

These tests help ensure that the transformed data meets the expected
data quality and business requirements.

## 8. Documentation

dbt documentation is generated using:

    dbt docs generate --target dev

Documentation can be viewed using:

    dbt docs serve

The documentation provides:

- Model descriptions
- Column information
- Tests
- Dependencies
- Data lineage

## 9. Pipeline Scheduling

The pipeline is executed using Windows Task Scheduler.

Schedule:

    First Monday of every month
    00:00 AM

The scheduled job executes the pipeline batch script:

    run_clinical_trials_pipeline.bat

## 10. Pipeline Execution

The batch script performs the following steps:

1. Configure Snowflake environment variables
2. Set the Snowflake private key location
3. Activate the Python virtual environment
4. Execute the Python API ingestion
5. Load data into Snowflake RAW
6. Execute dbt transformations
7. Run dbt tests as part of dbt build

The pipeline stops if the Python ingestion or dbt transformation step
fails.

## 11. dbt Build

The transformation pipeline is executed using:

    dbt build --select +stg_ctgov_studies+ --target dev

The `+` operators include the upstream and downstream dependencies
of the selected dbt model.

The dbt build command also executes the applicable tests associated
with the selected models.

## 12. Security

Credentials and private keys are not stored directly in the Python
source code.

Snowflake authentication uses RSA private-key authentication.

The private key is stored separately from the project source code.

Snowflake configuration is supplied through environment variables
during pipeline execution.

Configuration files containing credentials or sensitive information
must not be committed to source control or shared externally.

Private key files must never be included in the project repository.

## 13. Project Structure

    ClinicalTrials/
    |
    +-- README.md
    |
    +-- Python API/
    |   +-- extract_clinical_trials.py
    |   +-- config.json
    |   +-- requirements.txt
    |
    +-- clinical_trials_dbt/
    |   +-- dbt_project.yml
    |   +-- models/
    |       +-- staging/
    |       +-- intermediate/
    |       +-- marts/
    |
    +-- scripts/
    |   +-- run_clinical_trials_pipeline.bat
    |
    +-- docs/
        +-- architecture.pdf
        +-- dbt_lineage.pdf

## 14. Expected Outcome

The pipeline provides a repeatable process for ingesting clinical
trial data, loading the source data into Snowflake, transforming the
data through multiple dbt layers, validating data quality, and
producing business-ready sponsor-level metrics.

The final analytical output provides visibility into clinical trial
volume, closed trials, early-stopped trials, and early-stop rates by
sponsor class.