import mysql.connector
from psycopg import Connection
from psycopg.rows import dict_row
import re


class DriverBase:
    def statement(self, query, binding):
        pass

    def fetch_one(self, query, binding):
        pass

    def fetch_all(self, query, binding):
        pass

    def last_rowid(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def start_transaction(self):
        pass


class MySqlDriver(DriverBase):
    def __init__(self, **kwargs):
        self.context = mysql.connector.connect(**kwargs)
        self.cursor = self.context.cursor(dictionary=True, buffered=True)
        self.last_rowid_ = None

    def __del__(self):
        self.context.close()

    def statement(self, query, bindings=None):
        if bindings is None:
            bindings = []
        with self.context.cursor(dictionary=True) as cursor:
            cursor.execute(query, tuple(bindings))
            self.last_rowid_ = cursor.lastrowid
            return cursor.rowcount

    def transaction(self, fn):
        fn(self)
        self.context.commit()

    def fetch_one(self, query, bindings=None):
        if bindings is None:
            bindings = []
        with self.context.cursor(dictionary=True) as cursor:
            cursor.execute(query, tuple(bindings))
            return cursor.fetchone()

    def fetch_all(self, query, bindings=None):
        if bindings is None:
            bindings = []
        with self.context.cursor(dictionary=True) as cursor:
            cursor.execute(query, tuple(bindings))
            return cursor.fetchall()

    def last_rowid(self):
        return self.last_rowid_

    def commit(self):
        self.context.commit()

    def rollback(self):
        self.context.rollback()

    def start_transaction(self):
        self.start_transaction()


class PostgresDriver(DriverBase):
    def __init__(self, **kwargs):
        self.context = Connection.connect(**kwargs)
        self.last_rowid_ = None

    def __del__(self):
        self.context.close()

    def statement(self, query, bindings=None):
        if bindings is None:
            bindings = []
        with self.context.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, bindings)
            match = re.search(r'returning\s+\"(\w+)\"', query.lower())
            if match:
                self.last_rowid_ = cursor.fetchone()[match.group(1)]
            else:
                self.last_rowid_ = None
            return cursor.rowcount

    def transaction(self, fn):
        fn(self)
        self.context.commit()

    def fetch_one(self, query, bindings=None):
        if bindings is None:
            bindings = []
        with self.context.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, bindings)
            return cursor.fetchone()

    def fetch_all(self, query, bindings=None):
        if bindings is None:
            bindings = []

        with self.context.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, bindings)
            return cursor.fetchall()

    def last_rowid(self):
        return self.last_rowid_

    def commit(self):
        self.context.commit()

    def rollback(self):
        self.context.rollback()

    def start_transaction(self):
        self.context.transaction()
