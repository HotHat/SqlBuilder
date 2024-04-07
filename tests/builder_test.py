import unittest
from sqlbuilder.grammar import Grammar, Builder
from sqlbuilder.mysqlgrammar import MysqlGrammar
from sqlbuilder.connection import Connection
from sqlbuilder.driver import MySqlDriver


class BuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = Builder(None, Grammar('tb_'))
        self.connection = Connection(None, '')
        self.mysql_builder = Builder(self.connection, MysqlGrammar('tb_'))

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

    def test_where_in_null(self):
        sql = (self.mysql_builder
               .table('users')
               # where_in / where_not_in
               .where_in('id', [1, 2, 3])
               .where_not_in('id', [4, 5, 6])
               # where_null / where_not_null
               .where_null('updated_at')
               .where_not_null('created_at')
               .to_sql()
               )
        print(sql)

    def test_where_column(self):
        sql = (self.mysql_builder
               .table('users')
               # where_in / where_not_in
               .where_column('first_name', 'last_name')
               .where_column('updated_at', '>', 'created_at')
               .where_column([
            ['first_name_1', '=', 'last_name_1'],
            ['updated_at_1', '>', 'created_at_1']
        ])
               .to_sql()
               )
        print(sql)

    def test_where_exists(self):
        def fn(query: Builder):
            query.select(Builder.raw(1)).table('orders').where_raw('orders.user_id = users.id')

        sql = (self.mysql_builder
               .table('users')
               .where_exists(fn)
               .to_sql()
               )
        print(sql)

    def test_ordering_grouping_limit_offset(self):
        sql = (self.mysql_builder
               .table('users')
               .order_by('name', 'desc')
               .group_by('account_id', 'status')
               .having('account_id', '>', 100)
               .offset(10)
               .limit(5)
               .to_sql()
               )
        print(sql)

    def test_condition(self):
        def fn(query, value):
            query.where('role_id', value)

        def default(query, value):
            query.order_by('name')

        sql = (self.mysql_builder
               .table('users')
               .when(1, fn)
               .when(2, fn)
               .when(None, fn, default)
               .to_sql()
               )
        print(sql)

    def test_insert(self):
        (self.mysql_builder
            .table('users')
            .insert([
                {'email': 'hothat@example.com', 'votes': 0},
                {'email': 'hothat2@example.com', 'votes': 0},
            ])
        )

    def test_insert2(self):
        (self.mysql_builder
            .table('users')
            .insert({'email': 'hothat@example.com', 'votes': 0}))

    def test_update(self):
        (self.mysql_builder
         .table('users')
         .where('id', 1)
         .update({'votes': 1}))
        pass

    def test_delete(self):
        (self.mysql_builder
         .table('users')
         .delete())

    def test_delete2(self):
        (self.mysql_builder
         .table('users')
         .where('votes', '>', 100)
         .delete())
