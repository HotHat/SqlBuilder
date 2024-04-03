import unittest
from sqlbuilder.driver import MySqlDriver


class ConnectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = MySqlDriver(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="123456",
            database='xapp')

    def test_statement(self):
        # insert
        self.conn.statement("insert into user (username, password) values (%s, %s)", ['test1', '123456'])
        last_id = self.conn.last_rowid()
        print('insert row id:', last_id)

        # update
        cnt = self.conn.statement("update `user` set username=%s where id=%s", ['test_update', last_id])
        print(cnt)
        # delete
        print('delete row id:', self.conn.last_rowid())
        cnt = self.conn.statement("delete from `user` where id=%s", [last_id])
        print(cnt)
        self.conn.commit()

    def test_fetch_one(self):
        data = self.conn.fetch_one("select * from `user` where id=%s", [1])
        print(data)

    def test_fetch_all(self):
        data = self.conn.fetch_all("select * from `user`")
        print(data)
        pass

    def test_last_lowid(self):
        pass

    def test_transaction(self):
        pass
