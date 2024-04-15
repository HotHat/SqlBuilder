import unittest
from sqlbuilder.driver import PostgresDriver
from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row


class PostgresSqlDriverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = Connection.connect(make_conninfo(
            host="127.0.0.1",
            port=5432,
            user="postgres",
            password="123456",
            dbname='xapp'))

    def test_statement(self):
        # insert
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute("insert into users (username, password) values (%s, %s) returning id", ['test1', '123456'])
            data = cursor.fetchone()
            print('insert row id:', data)
            self.conn.commit()

        # update
        # cnt = self.conn.statement("update `user` set username=%s where id=%s", ['test_update', last_id])
        # print(cnt)
        # delete
        # print('delete row id:', self.conn.last_rowid())
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute('delete from "users" where id=%s', [8])
            print(cursor.rowcount)
            self.conn.commit()

    def test_fetch_one(self):
        with self.conn.cursor() as cursor:
            cursor.execute("select * from users where id=%s limit 1", [1])
            data = cursor.fetchone()
            print(data)

    def test_fetch_all(self):
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute("select * from users", [])
            data = cursor.fetchall()
            print(data)
        pass

    def test_last_lowid(self):
        pass

    def test_transaction(self):
        pass
