from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ Operator for loadingq
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
        redshift_conn_id,
        table,
        select_data_query,
        truncate=True,
        *args, **kwargs
    ):
        """ Constructor
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_data_query = select_data_query
        self.truncate = truncate

    def execute(self, context):
        """ This method defines the main execution logic of the operator
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info('Delete existing data from table {} !!!'.format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('Loading data in table {}'.format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table, self.select_data_query))
