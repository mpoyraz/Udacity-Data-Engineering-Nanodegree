from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_sql_stmt=None,
                 clear_dest_table=False,
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_opt='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_sql_stmt = create_sql_stmt
        self.clear_dest_table = clear_dest_table
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_opt = json_opt
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator is starting')
        
        # Get AWS and Redhsift hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If given, run create SQL statement before copy
        if self.create_sql_stmt is not None:
            self.log.info("Executing the given SQL create statement")
            redshift.run(self.create_sql_stmt)
        
        # Clear destination table before copy if enabled
        if self.clear_dest_table:
            self.log.info("Clearing data from destination Redshift table '{}'".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        # Construct the S3 to Redshift copy statement
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Auto options for json
        if self.json_opt == 'auto' or self.json_opt == 'auto ignorecase':
            self.log.info("Using '{}' option for copy operation".format(self.json_opt))
            formatted_sql = self.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_opt
            )
        # Json path options
        else:
            self.log.info("'auto' or 'auto ignorecase' options are not selected for copy operation")
            if self.json_opt.startswith('s3://'):
                # Full json path is provided
                jsonpath = self.json_opt
            else:
                # Relative json path within the S3 bucket
                jsonpath = "s3://{}/{}".format(self.s3_bucket, self.json_opt)
            self.log.info("Using '{}' jsonpath file for copy operation".format(self.json_opt))
            formatted_sql = self.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                jsonpath
            )
        # Execute the copy statement
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(formatted_sql)
        
        self.log.info('StageToRedshiftOperator is completed')


