from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    # SQL template for query
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
                 query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_columns = target_columns
        self.query = query

    def execute(self, context):
        # Set AWS Redshift connections
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from Redshift target table {}..."\
                        .format(self.target_table))
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Preparing SQL query for {} table".format(self.target_table))
        query_name = ""
        if self.query == "user_table_insert":
            query_name = SqlQueries.user_table_insert
        elif self.query == "song_table_insert":
            query_name = SqlQueries.song_table_insert
        elif self.query == "artist_table_insert":
            query_name = SqlQueries.artist_table_insert
        elif self.query == "time_table_insert":
            query_name = SqlQueries.time_table_insert
        else:
            query_name = ""
            self.log.info("Invalid value in query parameter.")

        formatted_sql = LoadDimensionOperator.sql_template.format(
            self.target_table,
            self.target_columns,
            query_name
        )

        # Executing Load operation
        self.log.info("Executing Redshift load operation to {}..."\
                        .format(self.target_table))
        self.log.info("SQL query: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Redshift load operation DONE.")
