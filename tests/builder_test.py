import unittest
import sys
from grammar import Grammar, Builder


class BuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = None
        # self.builder = Builder(None, Grammar('tb_'))

    def test_hello(self):
        print(sys.path)
        sql = self.builder.select(['id', 'name']).to_sql()
        print(sql)
