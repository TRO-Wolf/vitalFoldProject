{{ config(
    materialized='table',
    file_format='iceberg'
) }}

SELECT /*+ BROADCAST(Provider, DD) */
    DD.calendar_date,
    DD.year_month,
    DD.year,
    DD.year_quarter,
    Surveys.survey_id,
    Surveys.patient_visit_id,
    Surveys.gene_prissy_score,
    Surveys.experience_score,
    CONCAT(Provider.provider_first_name, ' ', Provider.provider_last_name) AS provider_name,
    Appointment.appointment_datetime,
    Appointment.appointment_clinic_id AS clinic_id,
    PatientVisit.provider_seen_time,
    PatientVisit.checkin_time,
    CAST(
        (unix_timestamp(PatientVisit.provider_seen_time) - unix_timestamp(PatientVisit.checkin_time)) / 60
        AS INT
    ) AS wait_time_minutes
FROM 
    {{ source('vital_fold_silver', 'survey') }}        AS Surveys
INNER JOIN 
    {{ source('vital_fold_silver', 'patient_visit') }} AS PatientVisit
    ON PatientVisit.patient_visit_id = Surveys.patient_visit_id
INNER JOIN 
    {{ source('vital_fold_silver', 'provider') }}      AS Provider
    ON Provider.provider_id = PatientVisit.pv_provider_id
INNER JOIN 
    {{ source('vital_fold_silver', 'appointment') }}   AS Appointment
    ON Appointment.appointment_id = PatientVisit.pv_appointment_id
INNER JOIN 
    {{ source('vital_fold_silver', 'dim_dates') }}     AS DD
    ON DD.calendar_date = DATE(Appointment.appointment_datetime)
