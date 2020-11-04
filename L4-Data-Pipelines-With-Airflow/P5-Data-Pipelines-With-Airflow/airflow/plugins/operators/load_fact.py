from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_sql_stmt=None,
                 select_sql_stmt=None,
                 table=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.select_sql_stmt = select_sql_stmt
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator is starting')

        # Get the Redhsift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # If given, run create SQL statement before insert operations
        if self.create_sql_stmt is not None:
            self.log.info("Executing the given SQL create statement")
            redshift.run(self.create_sql_stmt)

        # Perform the insert operation
        if self.select_sql_stmt and self.table:
            # Construct the insert statement
            insert_sql_stmt = "INSERT INTO {} {}".format(self.table, self.select_sql_stmt)
            # Execute the insert statement
            self.log.info("Executing the SQL insert statement")
            redshift.run(insert_sql_stmt)
        else:
            self.log.info("No insert operation performed, both SQL select statement and destination table needed for execution")

        self.log.info('LoadFactOperator is completed')