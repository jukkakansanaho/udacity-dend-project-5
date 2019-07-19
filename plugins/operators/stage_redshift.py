import datetime
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Apache Airflow Operator to Copy source data
        from AWS S3 to AWS Redshift DB.

    Keyword arguments:
    * redshift_conn_id      -- AWS Redshift connection ID
    * aws_credentials_id    -- AWS S3 credentials
    * target_table          -- AWS Redshift target table name
    * s3_bucket             -- AWS S3 bucket name
    * s3_key                -- AWS S3 key
    * file_format           -- source data file format (JSON | CSV)
    * json_paths            -- path to JSON data structure (optional)
    * delimiter             -- delimiter for CSV data (optional)
    * ignore_headers        -- ignote headers in CSV data (0=no | 1=yes)
    * use_partitioned_data  -- is source data in partitioned structure
                                (e.g. source_data/{year}/{month}/df1.json)
    * execution_date        -- context variable containing date of task's
                                execution date (e.g. 2019-07-19)

    Output:
    * source data is COPIED to AWS Redshift staging tables.
    """
    ui_color = '#358140'
    template_fields = ("s3_key","execution_date")

    # SQL template for CSV input format
    sql_template_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    # SQL template for JSON input format
    sql_template_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json {}
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 json_paths="",
                 delimiter=",",
                 ignore_headers=1,
                 use_partitioned_data="False",
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_paths = json_paths
        self.delimeter = delimiter
        self.ignore_headers = ignore_headers
        self.execution_date = execution_date
        self.use_partitioned_data = use_partitioned_data

    def execute(self, context):
        # Set AWS S3 and Redshift connections
        self.log.info("Setting up Redshift connection...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        self.log.info("Clearing data from Redshift target table...")
        redshift.run("DELETE FROM {}".format(self.target_table))

        # Prepare S3 paths
        self.log.info("Preparing Copying data from S3 to Redshift...")
        exec_date_rendered = self.execution_date.format(**context)
        self.log.info("Execution_date: {}".format(exec_date_rendered))
        exec_date_obj = datetime.datetime.strptime( exec_date_rendered, \
                                                    '%Y-%m-%d')
        self.log.info("Execution_year: {}".format(exec_date_obj.year))
        self.log.info("Execution_month: {}".format(exec_date_obj.month))
        rendered_key_raw = self.s3_key.format(**context)
        self.log.info("Rendered_key_raw: {}".format(rendered_key_raw))

        if self.use_partitioned_data == "True":
            self.log.info("Rendered_key_raw: {}".format(rendered_key_raw))
            rendered_key = rendered_key_raw.format(\
                                        exec_date_obj.year, \
                                        exec_date_obj.month)
        else:
            rendered_key = rendered_key_raw
        self.log.info("Rendered_key: {}".format(rendered_key))

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("S3_path: ".format(s3_path))

        if self.json_paths == "":
            s3_json_path = "\'auto\'"
        else:
            s3_json_path = "\'s3://{}/{}\'".format( self.s3_bucket, \
                                                    self.json_paths)

        self.log.info("S3_PATH: {}".format(s3_path))
        self.log.info("S3_JSON_PATH: {}".format(s3_json_path))

        # Copy data from S3 to Redshift
        # Select oparations based on input file format (JSON or CSV)
        if self.file_format == "json":
            self.log.info("Preparing for JSON input data")
            formatted_sql = StageToRedshiftOperator.sql_template_json.format(
                self.target_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                s3_json_path
            )
        elif self.file_format == "csv":
            self.log.info("Preparing for CSV input data")
            formatted_sql = StageToRedshiftOperator.sql_template_csv.format(
                self.target_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        else:
            self.log.info('File Format defined something else than \
                        JSON or CSV. Other formats not supported.')
            pass

        # Executing COPY operation
        self.log.info("Executing Redshift COPY operation...")
        redshift.run(formatted_sql)
        self.log.info("Redshift COPY operation DONE.")
