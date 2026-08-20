{{ config(
    materialized='view'
) }}

WITH parsed_source AS (
    SELECT 
        -- Ensures stringified JSON from write_pandas is converted to an object
        CASE 
            WHEN TYPEOF(RAW_JSON) = 'VARCHAR' THEN PARSE_JSON(RAW_JSON)
            ELSE RAW_JSON 
        END AS CLEAN_JSON,
        LOADED_DATE
    FROM {{ source('clinical_trials', 'RAW_CTGOV_STUDIES') }}
),

staged_and_typed AS (
    SELECT
        -- Using colon notation on CLEAN_JSON to avoid NULLs
        CLEAN_JSON:protocolSection.identificationModule.nctId::VARCHAR
            AS NCT_ID,

        CLEAN_JSON:protocolSection.statusModule.overallStatus::VARCHAR
            AS LATEST_OVERALL_STATUS,

        CLEAN_JSON:protocolSection.sponsorCollaboratorsModule.leadSponsor.class::VARCHAR
            AS SPONSOR_CLASS,

        CLEAN_JSON:protocolSection.sponsorCollaboratorsModule.leadSponsor.name::VARCHAR
            AS SPONSOR_NAME,

        CLEAN_JSON:protocolSection.designModule.studyType::VARCHAR
            AS STUDY_TYPE,

        CLEAN_JSON:protocolSection.designModule.phases
            AS PHASES,

        CLEAN_JSON:protocolSection.identificationModule.briefTitle::VARCHAR
            AS BRIEF_TITLE,

        CLEAN_JSON:protocolSection.identificationModule.officialTitle::VARCHAR
            AS OFFICIAL_TITLE,

        TRY_TO_DATE(
            CLEAN_JSON:protocolSection.statusModule.studyFirstSubmitDate::VARCHAR
        ) AS FIRST_SUBMITTED_DATE,

        TRY_TO_DATE(
            CLEAN_JSON:protocolSection.statusModule.startDateStruct.date::VARCHAR
        ) AS START_DATE,

        TRY_TO_DATE(
            CLEAN_JSON:protocolSection.statusModule.completionDateStruct.date::VARCHAR
        ) AS COMPLETION_DATE,

        CLEAN_JSON:protocolSection.designModule.enrollmentInfo.count::NUMBER
            AS ENROLLMENT,

        CLEAN_JSON:protocolSection.designModule.enrollmentInfo.type::VARCHAR
            AS ENROLLMENT_TYPE,

        CLEAN_JSON:protocolSection.conditionsModule.conditions
            AS CONDITIONS,

        CLEAN_JSON:protocolSection.armsInterventionsModule.interventions
            AS INTERVENTIONS,

        TRY_TO_DATE(
            CLEAN_JSON:protocolSection.statusModule.lastUpdateSubmitDate::VARCHAR
        ) AS STUDY_LAST_UPDATE_DATE,
        TRY_TO_DATE(
            CLEAN_JSON:protocolSection.statusModule.lastUpdatePostDateStruct.date::VARCHAR
        ) AS LAST_UPDATE_POST_DATE,
        LOADED_DATE
    FROM parsed_source
),

deduplicated AS (
    SELECT *
    FROM staged_and_typed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY NCT_ID ORDER BY LOADED_DATE DESC) = 1
)

SELECT * FROM deduplicated
