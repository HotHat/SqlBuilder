import unittest
from sqlbuilder.driver import PostgresDriver


class PostgresDriverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = PostgresDriver(
            host="127.0.0.1",
            port=5432,
            user="postgres",
            password="123456",
            dbname='xapp')

    def test_statement(self):
        self.conn.statement("insert into users (username, password) values (%s, %s) returning id", ['test1', '123456'])
        last_id = self.conn.last_rowid()
        print('insert row id:', last_id)

        # update
        cnt = self.conn.statement('update "users" set username=%s where id=%s', ['test_update', last_id])
        print(cnt)
        # delete
        print('delete row id:', self.conn.last_rowid())
        cnt = self.conn.statement("delete from users where id=%s", [last_id])
        print(cnt)
        self.conn.commit()

    def test_fetch_one(self):
        data = self.conn.fetch_one("select * from users where id=%s", [1])
        print(data)

    def test_fetch_all(self):
        data = self.conn.fetch_all("select * from users")
        print(data)
        pass

    def test_transaction(self):
        pass
