
from .connection import Connection
from .mysqlgrammar import MysqlGrammar
from .driver import MySqlDriver


class MysqlConnection(Connection):
    def __init__(self, table_prefix='', **config):
        self.table_prefix = table_prefix
        self.driver = MySqlDriver(**config)
        super().__init__(self.driver, table_prefix)

    def get_grammar(self):
        return MysqlGrammar(self.table_prefix)
