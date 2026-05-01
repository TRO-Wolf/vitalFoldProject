from __future__ import annotations
from airflow.sdk import (
    dag, task, Param, chain, cross_downstream,
    Variable
)
from includes.operators.dsql_to_s3 import DSQLToS3Operator
from includes.hooks.dsql import DSQLSqlHook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "vital-fold-bronze-bucket-v1"

default_args = {
    'owner': 'jhuntley',
    'retries': 2,
    'retry_delay': 2,
}

operator_args = {
    "s3_bucket": BUCKET,
    "sql_conn_id": "vital_fold_dsql",
    "aws_conn_id": "vital_fold_aws",
    "df_type": 'polars',
    "file_format": "parquet",
    "replace": True,
}


@dag(
    dag_id="vf_bronze_extraction_dag",
    schedule=CronTriggerTimetable("0 5 * * *", timezone="US/Eastern"),
    start_date=pendulum.datetime(2026, 4, 1, tz="US/Eastern"),
    catchup=False,
    default_args=default_args,
    template_searchpath=["/opt/airflow/includes/sql/vital_fold/bronze"],
)
def vf_bronze_extraction_dag():

    start = EmptyOperator(task_id='start')
    middle = EmptyOperator(task_id='middle')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed')

    clinic_extraction = DSQLToS3Operator(
        task_id="clinic_extraction_task",
        query="clinic_bronze.sql",
        s3_key=f"bronze/clinic/{{{{ ds }}}}.parquet",
        **operator_args
    )
    
    provider_extraction = DSQLToS3Operator(
        task_id="provider_extraction_task",
        query="provider_bronze.sql",
        s3_key=f"bronze/provider/{{{{ ds }}}}.parquet",
        **operator_args
    )


    appointment_extraction = DSQLToS3Operator(
        task_id="appointment_extraction_task",
        query="appointment_bronze.sql",
        s3_key=f"bronze/appointments/{{{{ ds }}}}.parquet",
        **operator_args
    )

    cpt_extraction = DSQLToS3Operator(
        task_id="cpt_extraction_task",
        query="cpt_bronze.sql",
        s3_key=f"bronze/appointment_cpt/{{{{ ds }}}}.parquet",
        **operator_args
    )

    survey_extraction = DSQLToS3Operator(
        task_id="survey_extraction_task",
        query="survey_bronze.sql",
        s3_key=f"bronze/survey/{{{{ ds }}}}.parquet",
        **operator_args
    )

    patient_visit_extraction = DSQLToS3Operator(
        task_id="patient_visit_extraction_task",
        query="patient_visit_bronze.sql",
        s3_key=f"bronze/patient_visit/{{{{ ds }}}}.parquet",
        **operator_args
    )


    (
        start
        >> [clinic_extraction, provider_extraction]
        >> middle
        >> [appointment_extraction, cpt_extraction, survey_extraction, patient_visit_extraction]
        >> end
    )

vf_bronze_extraction_dag()




