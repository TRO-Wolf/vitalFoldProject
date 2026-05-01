SELECT 
    Core.appointment_cpt_id::VARCHAR(255) AS appointment_cpt_id,
    Core.appointment_id::VARCHAR(255) AS appointment_id,
    Core.cpt_code_id,
    Core.provider_id,
    Core.clinic_id,
    Core.service_date,
    Core.units,
    Core.modifier_1,
    Core.modifier_2,
    Core.work_rvu_snapshot,
    Core.pe_rvu_snapshot,
    Core.mp_rvu_snapshot,
    Core.total_rvu_snapshot,
    Core.conversion_factor,
    Core.expected_amount,
    Core.creation_time,
    CURRENT_TIMESTAMP::TIMESTAMPTZ
        AS ingestion_timestamp,
    'APPEND'::VARCHAR(255)
        AS operation_type,
    '{{ ti.dag_version_id }}'::VARCHAR(255)
        AS dag_version_id,
    '{{ dag_run.run_type.name }}'::VARCHAR(255)
        AS run_type
FROM
    vital_fold.appointment_cpt AS Core
WHERE
    Core.service_date >= '{{ ds }}'::DATE - INTERVAL '1' DAY
    AND
    Core.service_date < '{{ ds }}'::DATE