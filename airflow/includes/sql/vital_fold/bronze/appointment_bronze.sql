SELECT 
    Appointments.appointment_id::VARCHAR(255),
    Appointments.patient_id::VARCHAR(255),
    Appointments.provider_id,
    Appointments.clinic_id,
    Appointments.appointment_datetime,
    Appointments.reason_for_visit,
    Appointments.status,
    CURRENT_TIMESTAMP::TIMESTAMPTZ
        AS ingestion_timestamp,
    'APPEND'::VARCHAR(255)
        AS operation_type,
    '{{ ti.dag_version_id }}'::VARCHAR(255)
        AS dag_version_id,
    '{{ dag_run.run_type.name }}'::VARCHAR(255)
        AS run_type
FROM
    vital_fold.appointment AS Appointments
WHERE
    Appointments.appointment_datetime >= '{{ ds }}'::DATE - INTERVAL '1' DAY
    AND 
    Appointments.appointment_datetime < '{{ ds }}'::DATE