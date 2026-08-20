{{ config(
    materialized='incremental',
    unique_key='unique_intervention_key',
    incremental_strategy='merge'
) }}

WITH source_intermediate_view AS (
    SELECT
        unique_intervention_key,
        NCT_ID,
        LATEST_OVERALL_STATUS,
        SPONSOR_CLASS,
        SPONSOR_NAME,
        STUDY_TYPE,
        BRIEF_TITLE,
        phase_index,
        study_phase,
        FIRST_SUBMITTED_DATE,
        START_DATE,
        COMPLETION_DATE,
        ENROLLMENT,
        ENROLLMENT_TYPE,
        intervention_index,
        intervention_type,
        intervention_name,
        intervention_description,
        arm_group_labels,
        STUDY_LAST_UPDATE_DATE,
        LAST_UPDATE_POST_DATE,
        src_loaded_date
    FROM {{ ref('int_study_interventions') }}
    
    WHERE study_phase IN ('PHASE2', 'PHASE3')

    {% if is_incremental() %}
    -- FIX: If target is empty, lookback to the earliest date in your raw staging view dynamically
    AND LAST_UPDATE_POST_DATE >= COALESCE(
        (SELECT DATEADD(day, -1, MAX(LAST_UPDATE_POST_DATE)) FROM {{ this }}),
        (SELECT MIN(LAST_UPDATE_POST_DATE) FROM {{ ref('int_study_interventions') }})
    )
    {% endif %}
),

deduplicated_delta AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY unique_intervention_key 
            ORDER BY LAST_UPDATE_POST_DATE DESC, src_loaded_date DESC
        ) AS row_num
    FROM source_intermediate_view
)

SELECT
    unique_intervention_key,
    NCT_ID,
    LATEST_OVERALL_STATUS,
    SPONSOR_CLASS,
    SPONSOR_NAME,
    STUDY_TYPE,
    BRIEF_TITLE,
    phase_index,
    study_phase,
    FIRST_SUBMITTED_DATE,
    START_DATE,
    COMPLETION_DATE,
    ENROLLMENT,
    ENROLLMENT_TYPE,
    intervention_index,
    intervention_type,
    intervention_name,
    intervention_description,
    arm_group_labels,
    STUDY_LAST_UPDATE_DATE,
    LAST_UPDATE_POST_DATE,
    src_loaded_date,
    CURRENT_TIMESTAMP() AS dbt_processed_at
FROM deduplicated_delta
WHERE row_num = 1
