from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # SQL statement to find primary key columns of a table in Redshift
    find_pkey_sql = """
        select KCU.column_name
            from information_schema.table_constraints AS TC
            inner join information_schema.key_column_usage AS KCU
            on KCU.constraint_catalog = TC.constraint_catalog
            and KCU.constraint_schema = TC.constraint_schema
            and KCU.table_name = TC.table_name
            and KCU.constraint_name = TC.constraint_name
            where TC.constraint_type = 'PRIMARY KEY'
            and TC.table_schema = '{}'
            and TC.table_name = '{}'
            order by KCU.ordinal_position;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 check_empty=True,
                 check_pkey_contains_null=True,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.check_empty = check_empty
        self.check_pkey_contains_null = check_pkey_contains_null

    def execute(self, context):
        self.log.info('DataQualityOperator is starting')

        # Get AWS and Redhsift hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check if tables parameter is defined properly
        if len(self.tables) == 0:
            raise ValueError('"tables" parameter cannot be empty for DataQualityOperator')

        # First, check if given tables contains any row
        self.log.info('First checking if given tables are empty or not')
        valErr = None
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            # No results returned from the table
            if len(records) < 1 or len(records[0]) < 1:
                emptyErr = "Data quality check failed, table {} returned no results".format(table)
                self.log.info(emptyErr)
                if valErr is None:
                    valErr = ValueError(emptyErr)
            else:
                num_records = records[0][0]
                # Table has 0 rows
                if num_records < 1:
                    emptyErr = "Data quality check failed, table {} contains 0 rows".format(table)
                    self.log.info(emptyErr)
                    if valErr is None:
                        valErr = ValueError(emptyErr)
            self.log.info("Table {} has {} records, passed initial data quality check".format(table, num_records))
        # Raise the first value error if exists
        if valErr:
            raise valErr

        # If given table has primary key defined, further check if pkey columns contain null
        self.log.info('Check if primary keys of tables contains NULL values where applicable')
        for table in self.tables:
            records = redshift.get_records(self.find_pkey_sql.format('public', table))
            if len(records) < 1:
                self.log.info('Table {} has no primary keys, passed the data quality check'.format(table))
            else:
                num_pkey_cols = len(records)
                pkey_table = 'Table {} primary key consist of {} column(s)'.format(table, num_pkey_cols)
                # Iterate over each column in the primary key
                for r in records:
                    col_pkey = r[0]
                    null_pkey_records = redshift.get_records('SELECT COUNT(*) FROM {} WHERE {} is NULL'.format(table, col_pkey))
                    count_null_pkey = null_pkey_records[0][0]
                    if count_null_pkey > 0:
                        nullPkeyErr = 'Data quality check failed, table {} primary key column {} contains {} NULL values'\
                            .format(table, col_pkey, count_null_pkey)
                        self.log.info(nullPkeyErr)
                        if valErr is None:
                            valErr = ValueError(nullPkeyErr)
                    else:
                        self.log.info('Table {} primary key column {} has no NULL values, passed data quality check'.format(table, col_pkey))
        # Raise the first value error if exists
        if valErr:
            raise valErr

        self.log.info('DataQualityOperator is completed')