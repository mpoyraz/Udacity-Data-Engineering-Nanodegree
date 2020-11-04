from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_sql_stmt=None,
                 select_sql_stmt=None,
                 table=None,
                 empty_table_before_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.select_sql_stmt = select_sql_stmt
        self.table = table
        self.empty_table_before_load = empty_table_before_load

    def execute(self, context):
        self.log.info('LoadDimensionOperator is starting')

        # Get AWS and Redhsift hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If given, run create SQL statement before insert operation
        if self.create_sql_stmt is not None:
            self.log.info("Executing the given SQL create statement")
            redshift.run(self.create_sql_stmt)

        # Perform the insert operation
        if self.select_sql_stmt and self.table:
            # If enabled, empty the dimention table before insert operation
            if self.empty_table_before_load:
                self.log.info("Clearing the dimention table '{}'".format(self.table))
                redshift.run("DELETE FROM {}".format(self.table))    
            # Construct and execute the insert statement
            insert_sql_stmt = "INSERT INTO {} {}".format(self.table, self.select_sql_stmt)
            self.log.info("Executing the SQL insert statement")
            redshift.run(insert_sql_stmt)
        else:
            self.log.info("No insert operation performed, both SQL select statement and destination table needed for execution")

        self.log.info('LoadDimensionOperator is completed')