{{ config(
    materialized='table'
) }}

SELECT
    SPONSOR_CLASS,
    COUNT(DISTINCT NCT_ID) AS TOTAL_TRIALS,

    COUNT(
        DISTINCT CASE
            WHEN LATEST_OVERALL_STATUS IN ('TERMINATED', 'WITHDRAWN', 'SUSPENDED')
            THEN NCT_ID
        END
    ) AS EARLY_STOPPED_TRIALS,

    COUNT(
        DISTINCT CASE
            WHEN LATEST_OVERALL_STATUS IN ('COMPLETED', 'TERMINATED', 'WITHDRAWN', 'SUSPENDED')
            THEN NCT_ID
        END
    ) AS CLOSED_TRIALS,

    ROUND(
        COUNT(
            DISTINCT CASE
                WHEN LATEST_OVERALL_STATUS IN ('TERMINATED', 'WITHDRAWN', 'SUSPENDED')
                THEN NCT_ID
            END
        )
        /
        NULLIF(
            COUNT(
                DISTINCT CASE
                    WHEN LATEST_OVERALL_STATUS IN ('COMPLETED', 'TERMINATED', 'WITHDRAWN', 'SUSPENDED')
                    THEN NCT_ID
                END
            ),
            0
        ),
        4
    ) AS EARLY_STOP_RATE

FROM {{ ref('fct_study_interventions') }}

GROUP BY SPONSOR_CLASS