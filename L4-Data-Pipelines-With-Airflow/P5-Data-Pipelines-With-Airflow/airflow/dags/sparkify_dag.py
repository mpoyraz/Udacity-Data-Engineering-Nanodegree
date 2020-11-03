from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS and Redshift connections
REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"
# S3 bucket and keys
S3_BUCKET = "udacity-dend"
S3_KEY_LOG = "log_data"
S3_KEY_LOG_JSONPATH = "log_json_path.json"
S3_KEY_SONG = "song_data"

default_args = {
    'owner': 'sparkify',
    'start_date': datetime.now(),
    #'start_date': datetime(2020, 11, 2),
}

# Create the dag with default args
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
          #schedule_interval='0 * * * *'
        )

# Define tasks for the dat
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    create_sql_stmt=SqlQueries.stage_event_table_create,
    clear_dest_table=True,
    table="staging_events",
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_LOG,
    json_opt=S3_KEY_LOG_JSONPATH
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    create_sql_stmt=SqlQueries.stage_songs_table_create,
    clear_dest_table=True,
    table="staging_songs",
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_SONG,
    json_opt='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    create_sql_stmt=SqlQueries.songplay_table_create,
    select_sql_stmt=SqlQueries.songplay_table_insert,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    create_sql_stmt=SqlQueries.user_table_create,
    select_sql_stmt=SqlQueries.user_table_insert,
    table='users',
    empty_table_before_load=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    create_sql_stmt=SqlQueries.song_table_create,
    select_sql_stmt=SqlQueries.song_table_insert,
    table='songs',
    empty_table_before_load=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    create_sql_stmt=SqlQueries.artist_table_create,
    select_sql_stmt=SqlQueries.artist_table_insert,
    table='artists',
    empty_table_before_load=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    create_sql_stmt=SqlQueries.time_table_create,
    select_sql_stmt=SqlQueries.time_table_insert,
    table='time',
    empty_table_before_load=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    tables=['songplays', 'songs', 'artists', 'users', 'time'],
    check_empty=True,
    check_pkey_contains_null=True,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Assign task ordering
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
