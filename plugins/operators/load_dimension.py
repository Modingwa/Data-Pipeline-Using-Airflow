from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_query = """
        TRUNCATE TABLE {table}
    """
    insert_into_query = """
        INSERT INTO {table} 
        {select_query}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append=False,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append = append
        self.select_query = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append:
            # delete data from table
            self.log.info("Deleting data from {table} table.".format(table=self.table))
            redshift.run(LoadDimensionOperator.truncate_query.format(table=self.table))

        self.log.info("Inserting data into {table} table.".format(table=self.table))
        redshift.run(LoadDimensionOperator.insert_into_query.format(
            table=self.table,
            select_query=self.select_query
        ))
