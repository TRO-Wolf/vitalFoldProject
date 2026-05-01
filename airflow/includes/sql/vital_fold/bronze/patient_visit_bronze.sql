SELECT
    Source.patient_visit_id::VARCHAR(255) AS patient_visit_id,
    Source.appointment_id::VARCHAR(255) AS appointment_id,
    Source.patient_id::VARCHAR(255) AS patient_id,
    Source.clinic_id AS clinic_id,
    Source.provider_id AS provider_id,
    Source.checkin_time,
    Source.checkout_time,
    Source.provider_seen_time,
    Source.ekg_usage,
    Source.estimated_copay,
    Source.creation_time,
    Source.record_expiration_epoch,
    CURRENT_TIMESTAMP::TIMESTAMPTZ
        AS ingestion_timestamp,
    'APPEND'::VARCHAR(255)
        AS operation_type,
    '{{ ti.dag_version_id }}'::VARCHAR(255)
        AS dag_version_id,
    '{{ dag_run.run_type.name }}'::VARCHAR(255)
        AS run_type
FROM
    vital_fold.patient_visit AS Source
INNER JOIN
    vital_fold.appointment AS Appointments
    ON Source.appointment_id = Appointments.appointment_id
WHERE
    Appointments.appointment_datetime >= '{{ ds }}'::DATE - INTERVAL '1' DAY
    AND
    Appointments.appointment_datetime < '{{ ds }}'::DATE