import unittest
from sqlbuilder.grammar import Grammar, Builder
from sqlbuilder.mysqlgrammar import MysqlGrammar


class BuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = Builder(None, Grammar('tb_'))
        self.mysql_builder = Builder(None, MysqlGrammar('tb_'))

    def test_hello(self):
        sql = self.builder.select('id', 'name').table('user').where('id', 3).to_sql()
        print(sql)

    def test_hello2(self):
        sql = self.mysql_builder.select('id', 'name').table('user').where('id', 3).to_sql()
        print(sql)
