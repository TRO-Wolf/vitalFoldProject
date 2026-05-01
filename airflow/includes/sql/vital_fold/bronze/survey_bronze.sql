SELECT 
    Source.survey_id::VARCHAR(255) AS survey_id,
    Source.patient_visit_id::VARCHAR(255) AS patient_visit_id,
    Source.gene_prissy_score,
    Source.experience_score,
    Source.feedback_comments,
    Source.creation_time,
    CURRENT_TIMESTAMP::TIMESTAMPTZ
        AS ingestion_timestamp,
    'APPEND'::VARCHAR(255)
        AS operation_type,
    '{{ ti.dag_version_id }}'::VARCHAR(255)
        AS dag_version_id,
    '{{ dag_run.run_type.name }}'::VARCHAR(255)
        AS run_type
FROM
    vital_fold.survey AS Source
INNER JOIN
    vital_fold.patient_visit AS Visits
    ON Source.patient_visit_id = Visits.patient_visit_id
INNER JOIN
    vital_fold.appointment AS Appointments
    ON Visits.appointment_id = Appointments.appointment_id
WHERE
    Appointments.appointment_datetime >= '{{ ds }}'::DATE - INTERVAL '1' DAY
    AND
    Appointments.appointment_datetime < '{{ ds }}'::DATE