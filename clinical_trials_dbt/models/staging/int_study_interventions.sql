{{ config(materialized='view') }}

WITH source_staging_view AS (
    SELECT
        NCT_ID,
        LATEST_OVERALL_STATUS,
        SPONSOR_CLASS,
        SPONSOR_NAME,
        STUDY_TYPE,
        BRIEF_TITLE,
        FIRST_SUBMITTED_DATE,
        START_DATE,
        COMPLETION_DATE,
        ENROLLMENT,
        ENROLLMENT_TYPE,
        INTERVENTIONS, 
        PHASES, -- Raw array column from step 1
        STUDY_LAST_UPDATE_DATE,
        LAST_UPDATE_POST_DATE,
        LOADED_DATE
    FROM {{ ref('stg_ctgov_studies') }}
),

exploded_data AS (
    SELECT
        --surrogate key to prevent duplicate
        MD5(CONCAT(src.NCT_ID, '_', COALESCE(i.index, 0), '_', COALESCE(p.index, 0))) AS unique_intervention_key,
        
        src.NCT_ID,
        src.LATEST_OVERALL_STATUS,
        src.SPONSOR_CLASS,
        src.SPONSOR_NAME,
        src.STUDY_TYPE,
        src.BRIEF_TITLE,
         p.index AS phase_index,
        p.value::VARCHAR AS study_phase,
         src.FIRST_SUBMITTED_DATE,
        src.START_DATE,
        src.COMPLETION_DATE,
        src.ENROLLMENT,
        src.ENROLLMENT_TYPE,
        
        i.index AS intervention_index,
        i.value:type::VARCHAR AS intervention_type,
        i.value:name::VARCHAR AS intervention_name,
        i.value:description::VARCHAR AS intervention_description,
        i.value:armGroupLabels::VARIANT AS arm_group_labels,
        
        src.STUDY_LAST_UPDATE_DATE,
        src.LAST_UPDATE_POST_DATE,
        src.LOADED_DATE AS src_loaded_date
    FROM source_staging_view src,
    LATERAL FLATTEN(INPUT => src.INTERVENTIONS, outer => true) i,
    LATERAL FLATTEN(INPUT => src.PHASES, outer => true) p
)

SELECT * FROM exploded_data
