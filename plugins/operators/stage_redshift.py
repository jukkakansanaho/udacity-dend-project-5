from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

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

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        # Set AWS S3 and Redshift connections
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift target table...")
        redshift.run("DELETE FROM {}".format(self.target_table))

        # Prepare S3 path
        self.log.info("Copying data from S3 to Redshift...")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        s3_json_path = "\'s3://{}/{}\'".format(self.s3_bucket, self.json_paths
        )

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
