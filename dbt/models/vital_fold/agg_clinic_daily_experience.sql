{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT /*+ BROADCAST(DD) */
    GFS.clinic_id,
    GFS.calendar_date,
    DD.day_name AS day_of_week,
    ROUND(AVG(GFS.gene_prissy_score), 2)        AS avg_gene_prissy_score,
    ROUND(AVG(GFS.experience_score), 2)         AS avg_experience_score,
    ROUND(AVG(GFS.wait_time_minutes), 2)        AS avg_wait_time_minutes,
    COUNT(*)                                    AS num_surveys,
    COUNT(DISTINCT GFS.patient_visit_id)        AS num_patient_visits
FROM
    {{ ref('fct_survey_visit') }} AS GFS
INNER JOIN
    {{ source('vital_fold_silver', 'dim_dates') }} AS DD
    ON DD.calendar_date = GFS.calendar_date
GROUP BY
    GFS.clinic_id,
    GFS.calendar_date,
    DD.day_name
