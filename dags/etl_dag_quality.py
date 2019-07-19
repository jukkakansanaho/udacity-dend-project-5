from datetime import datetime, timedelta
import os
import logging
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

# Getting AWS Credentials from env variables.
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
""" This Apache Airflow DAG provides a pipeline to
* Verify data quality in Start schema Fact and Dimension tables
    (Tasks: Run_data_quality_checks)
* As a default, DASG has been run every hour, starting from 2018-11-1
* In case of failure, DAG doesn't retry
"""
# Importing config file.
dag_config = Variable.get("DAG_CONFIG", deserialize_json=True)

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(seconds=15),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('etl_dag_quality',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['data_quality_check_tables'],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> run_quality_checks
run_quality_checks >> end_operator
