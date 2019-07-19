from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """Apache Airflow Operator to load data
        from AWS Redshift staging tables to Dimension tables.

    Keyword arguments:
    * redshift_conn_id      -- AWS Redshift connection ID
    * target_table          -- AWS Redshift target table name
    * target_columns        -- AWS Redshift target columns (string)
    * query                 -- Query name to be used from SqlQueries.
    * insert_mode           -- How to insert data to target_table
                                (append = on top of existing data (default)
                                 truncate_insert = delete old data +
                                                    insert new data)

    Output:
    * Staging data in AWS Redshift is inserted from staging tables
        to Dimension table.
    """
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
                 insert_mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_columns = target_columns
        self.query = query
        self.insert_mode = insert_mode

    def execute(self, context):
        # Set AWS Redshift connections
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate_insert":
            self.log.info("Insert_mode = truncate_insert. Clearing data from Redshift target table: {} ..."\
                            .format(self.target_table))
            redshift.run("DELETE FROM {}".format(self.target_table))
        elif self.insert_mode == "append":
            self.log.info("Insert_mode = append. Inserting new data on top of old one in Redshift target table: {} ..."\
                            .format(self.target_table))
        else:
            self.log.info("Insert_mode not defined => using append (default value). Inserting new data on top of old one in Redshift target table: {} ..."\
                            .format(self.target_table))

        # Prepare SQL query
        self.log.info("Preparing SQL query for {} table".format(self.target_table))
        query_name = ""
        if self.query == "user_table_insert":
            if self.insert_mode == "append":
                query_name = SqlQueries.user_table_insert_append
            else:
                query_name = SqlQueries.user_table_insert_delete
        elif self.query == "song_table_insert":
            if self.insert_mode == "append":
                query_name = SqlQueries.song_table_insert_append
            else:
                query_name = SqlQueries.song_table_insert_delete
        elif self.query == "artist_table_insert":
            if self.insert_mode == "append":
                query_name = SqlQueries.artist_table_insert_append
            else:
                query_name = SqlQueries.artist_table_insert_delete
        elif self.query == "time_table_insert":
            if self.insert_mode == "append":
                query_name = SqlQueries.time_table_insert_append
            else:
                query_name = SqlQueries.time_table_insert_delete
        else:
            query_name = ""
            self.log.info("Invalid value in query parameter.")

        formatted_sql = LoadDimensionOperator.sql_template.format(
            self.target_table,
            self.target_columns,
            query_name.format(self.target_table, self.target_table)
        )

        # Executing Load operation
        self.log.info("Executing Redshift load operation to {}..."\
                        .format(self.target_table))
        self.log.info("SQL query: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Redshift load operation DONE.")
