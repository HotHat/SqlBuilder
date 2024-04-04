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

    def test_where1(self):
        sql = (self.mysql_builder.select('id', 'name')
               .table('user')
               .where('id', 3)
               .where('name', 'admin')
               .or_where('id', 4)
               .to_sql()
               )
        print(sql)

    def test_where2(self):
        sql = (self.mysql_builder.table('users')
                .where('votes', '=', 100)
                .where('age', '>', 35)
                .where('votes', 100)
                .where('votes', '>=', 100)
                .where('votes', '<>', 100)
                .where('name', 'like', 'T%')
                .to_sql())
        print(sql)

    def test_where3(self):
        sql = (self.mysql_builder.table('users')
               .where([
            ['status', '=', '1'], ['subscribed', '<>', '1'],
        ])
               .to_sql())
        print(sql)

    def test_or_where1(self):
        sql = (self.mysql_builder
               .table('users')
               .where('votes', '>', 100)
               .or_where('name', 'John')
               .to_sql()
               )
        print(sql)

    def test_or_where(self):
        def fn(query: Builder):
            query.where('name', 'Abigail').where('votes', '>', 50)

        sql = (self.mysql_builder
               .table('users')
               .where('votes', '>', 100)
               .or_where(fn)
               .to_sql()
               )
        print(sql)
