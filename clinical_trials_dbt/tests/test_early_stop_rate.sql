SELECT *
FROM {{ ref('sponsor_trial_summary') }}
WHERE EARLY_STOP_RATE < 0
   OR EARLY_STOP_RATE > 1