from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdag_factory import dimension_sub_dag


default_args = {
    'owner': 'Charles',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('etl_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_dimensions = SubDagOperator(
    subdag = dimension_sub_dag(
        "etl_pipeline_dag", 
        "load_dimensions_subdag", 
        datetime(2020, 4, 12), 
        '@hourly',
        redshift_conn_id="redshift",
        table_query_dict={
            'user': ['users', SqlQueries.user_table_insert], 
            'song': ['songs', SqlQueries.song_table_insert], 
            'artist': ['artists', SqlQueries.artist_table_insert], 
            'time': ['time', SqlQueries.time_table_insert]
        },
        append=False,
    ),
    task_id="load_dimensions_subdag",
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=[
        'songplays',
        'users',
        'songs',
        'artists',
        'time'
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dimensions >> run_quality_checks

run_quality_checks >> end_operator
