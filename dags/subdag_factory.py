from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadDimensionOperator)
from helpers import SqlQueries

def dimension_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, redshift_conn_id, table_query_dict, append):
    """This function acts as a factory for creating dags for loading dimension tables
    
    Parameters:
    -----------
    parent_dag_name (str): parent dag name
    child_dag_name (str): child dag name
    start_date (datetime): dag start time 
    schedule_interval: dag schedule interval
    redshift_conn_id (str): RedShift Connection id
    table_query_dict (dict): Dictionary in the form {name: [table, insert_query]}
    append (bool): flag for truncating
    
    """
    dag = DAG('%s.%s' % (parent_dag_name, child_dag_name),
              schedule_interval=schedule_interval,
              start_date=start_date,
             )
    
    
    for index, value in table_query_dict.items():
        # create a task using the LoadDimensionOperator
        setattr(dimension_sub_dag, 
                'load_{}_dimension_table'.format(index), 
                LoadDimensionOperator(
                    task_id='Load_{}_dim_table'.format(index),
                    redshift_conn_id="redshift",
                    table=value[0],
                    append=append,
                    sql=value[1],
                    dag=dag
                )
               )
    
    return dag