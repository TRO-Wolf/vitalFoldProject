SELECT
    Source.clinic_id,
    Source.clinic_name,
    Source.region,
    Source.street_address,
    Source.city,
    Source.state,
    Source.zip_code,
    Source.phone_number,
    Source.email,
    CURRENT_TIMESTAMP::TIMESTAMPTZ
        AS ingestion_timestamp,
    'APPEND'::VARCHAR(255)
        AS operation_type,
    '{{ ti.dag_version_id }}'::VARCHAR(255)
        AS dag_version_id,
    '{{ dag_run.run_type.name }}'::VARCHAR(255)
        AS run_type
FROM
    vital_fold.clinic AS Source
