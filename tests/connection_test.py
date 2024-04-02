import unittest
import mysql.connector


class ConnectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = None

    def test_start(self):
        # Connect to server
        cnx = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="123456",
            database='xapp'
        )

        # Get a cursor
        cur = cnx.cursor(dictionary=True)

        # Execute a query
        cur.execute("select * from user")

        # Fetch one result
        rows = cur.fetchall()
        print(rows)
        # for row in rows:
        #     print(dict(zip(cur.column_names, row)))

        # Close connection
        cnx.close()
        pass


