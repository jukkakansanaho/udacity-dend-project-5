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

# Importing config file.
dag_config = Variable.get("DAG_CONFIG", deserialize_json=True)

default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(seconds=15),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('etl_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=dag_config['log_data_target_staging_table'],
    s3_bucket=dag_config['log_data_source_s3_bucket'],
    s3_key=dag_config['log_data_source_s3_key'],
    file_format=dag_config['log_data_file_format'],
    json_paths=dag_config['log_json_path'],
    use_partitioned_data=dag_config['log_data_partitioned'],
    execution_date="{{ ds }}"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table=dag_config['song_data_target_staging_table'],
    s3_bucket=dag_config['song_data_source_s3_bucket'],
    s3_key=dag_config['song_data_source_s3_key'],
    file_format=dag_config['song_data_file_format'],
    use_partitioned_data=dag_config['song_data_partitioned'],
    execution_date="{{ ds }}"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['songplays_target_table'],
    target_columns=dag_config['songplays_target_columns'],
    query=dag_config['songplays_insert_query'],
    insert_mode=dag_config['insert_mode_fact']
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['user_target_table'],
    target_columns=dag_config['user_target_columns'],
    query=dag_config['users_insert_query'],
    insert_mode=dag_config['insert_mode_dim']
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['song_target_table'],
    target_columns=dag_config['song_target_columns'],
    query=dag_config['songs_insert_query'],
    insert_mode=dag_config['insert_mode_dim']
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['artist_target_table'],
    target_columns=dag_config['artist_target_columns'],
    query=dag_config['artists_insert_query'],
    insert_mode=dag_config['insert_mode_dim']
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['time_target_table'],
    target_columns=dag_config['time_target_columns'],
    query=dag_config['time_insert_query'],
    insert_mode=dag_config['insert_mode_dim']
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    target_table=dag_config['data_quality_check_tables']
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
