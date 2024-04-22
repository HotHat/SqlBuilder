
from .connection import Connection
from .postgresgrammar import PostgresGrammar
from .driver import PostgresDriver
from psycopg.conninfo import make_conninfo


class PostgresConnection(Connection):
    def __init__(self, table_prefix='', **config):
        self.table_prefix = table_prefix
        self.driver = PostgresDriver(make_conninfo(**config))
        super().__init__(self.driver, table_prefix)

    def get_grammar(self):
        return PostgresGrammar(self.table_prefix)
