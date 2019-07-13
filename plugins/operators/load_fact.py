from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    sql_template = """
        INSERT INTO {} ({})
        {};
    """
    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="",
                 target_table="",
                 target_columns="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_columns = target_columns

    def execute(self, context):
        # Set AWS Redshift connections
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift target table {}..."\
                        .format(self.target_table))
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Preparing SQL query for {} table".format(self.target_table))
        formatted_sql = LoadFactOperator.sql_template.format(
            self.target_table,
            self.target_columns,
            SqlQueries.songplay_table_insert
        )

        # Executing Load operation
        self.log.info("Executing Redshift load operation to {}..."\
                        .format(self.target_table))
        self.log.info("SQL query: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Redshift load operation DONE.")
