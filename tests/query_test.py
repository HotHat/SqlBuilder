import unittest
from sqlbuilder.grammar import Grammar, Builder
from sqlbuilder.mysqlconnection import MysqlConnection


class QueryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = MysqlConnection('', host="127.0.0.1",
                                    port=3306,
                                    user="root",
                                    password="123456",
                                    database='xapp')

    def test_where1(self):
        sql = (self.conn.table('users').select('id', 'name')
               .where('id', 2)
               .where('name', 'admin')
               .or_where('id', 1)
               .get()
               )
        print(sql)

    def test_where2(self):
        sql = (self.conn.table('users')
               .where('votes', '=', 100)
               .where('age', '>', 35)
               .where('votes', 100)
               .where('votes', '>=', 100)
               .where('votes', '<>', 100)
               .where('name', 'like', 'T%')
               .get())
        print(sql)

    def test_where3(self):
        sql = (self.conn.table('users')
               .where([
                    ['status', '=', '1'], ['subscribed', '<>', '1'],
                ])
               .get())
        print(sql)

    def test_or_where1(self):
        sql = (self.conn
               .table('users')
               .where('votes', '>', 100)
               .or_where('name', 'John')
               .get()
               )
        print(sql)

    def test_or_where(self):
        def fn(query: Builder):
            query.where('name', 'Abigail').where('votes', '>', 50)

        sql = (self.conn
               .table('users')
               .where('votes', '>', 100)
               .or_where(fn)
               .get()
               )
        print(sql)

    def test_where_in_null(self):
        sql = (self.conn
               .table('users')
               # where_in / where_not_in
               .where_in('id', [1, 2, 3])
               .where_not_in('id', [4, 5, 6])
               # where_null / where_not_null
               .where_null('updated_at')
               .where_not_null('created_at')
               .get()
               )
        print(sql)

    def test_where_column(self):
        sql = (self.conn
               .table('users')
               # where_in / where_not_in
               .where_column('first_name', 'last_name')
               .where_column('updated_at', '>', 'created_at')
               .where_column([
                    ['first_name_1', '=', 'last_name_1'],
                    ['updated_at_1', '>', 'created_at_1']
                ])
               .get()
               )
        print(sql)

    def test_where_exists(self):
        def fn(query: Builder):
            query.select(Builder.raw(1)).table('orders').where_raw('orders.user_id = users.id')

        sql = (self.conn
               .table('users')
               .where_exists(fn)
               .get()
               )
        print(sql)

    def test_ordering_grouping_limit_offset(self):
        sql = (self.conn
               .table('users')
               .order_by('name', 'desc')
               .group_by('account_id', 'status')
               .having('account_id', '>', 100)
               .offset(10)
               .limit(5)
               .get()
               )
        print(sql)

    def test_condition(self):
        def fn(query, value):
            query.where('role_id', value)

        def default(query, value):
            query.order_by('name')

        sql = (self.conn
               .table('users')
               .when(1, fn)
               .when(2, fn)
               .when(None, fn, default)
               .get()
               )
        print(sql)

    def test_insert(self):
        nid = (self.conn
            .table('users')
            .insert([
                {'email': 'hothat@example.com', 'votes': 100, 'name': 'Test1'},
                {'email': 'hothat2@example.com', 'votes': 0, 'name': 'Test2'},
            ])
        )
        self.conn.commit()
        print(nid)

    def test_insert2(self):
        (self.conn
         .table('users')
         .insert({'email': 'hothat@example.com', 'votes': 0, 'name': 'hothat'}))
        self.conn.commit()

    def test_update(self):
        (self.conn
         .table('users')
         .where('id', 1)
         .update({'votes': 1}))
        self.conn.commit()
        pass

    def test_delete(self):
        (self.conn
         .table('users')
         .delete())

    def test_delete2(self):
        (self.conn
         .table('users')
         .where('votes', '>', 100)
         .delete())
        self.conn.commit()
