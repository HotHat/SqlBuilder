from inspect import isfunction
import re
from .builder import Expression, JoinClause, Builder


class Grammar:
    def __init__(self, prefix):
        self.prefix = prefix
        self.operators = []
        self.select_components = ['aggregate',
                                  'columns_',
                                  'from',
                                  'joins_',
                                  'wheres_',
                                  'groups_',
                                  'having_',
                                  'orders_',
                                  'limit_',
                                  'offset_',
                                  'unions_',
                                  'lock', ]

    @staticmethod
    def is_expression(value):
        return isinstance(value, Expression)

    def wrap(self, value, prefix_alias=False):
        if self.is_expression(value):
            return value.get_value()

        if value.lower().find(' as ') != -1:
            return self.wrap_aliased_value(value, prefix_alias)

        return self.wrap_segments(value.split('.'))

    def wrap_segments(self, segments):
        result = []
        sn = len(segments)
        for k, v in enumerate(segments):
            if k == 0 and sn > 1:
                result.append(self.wrap_table(v))
            else:
                result.append(self.wrap_value(v))

        return '.'.join(result)

    def wrap_table(self, table):
        if not self.is_expression(table):
            return self.wrap(self.prefix + table, True)
        return table.get_value()

    @staticmethod
    def wrap_value(value):
        if value != '*':
            v = value.replace('"', '""')
            return f'"{v}"'
        return value

    def wrap_aliased_value(self, value, prefix_alias):
        segments = re.split(r'\s+as\s+', value, flags=re.IGNORECASE)
        if prefix_alias:
            segments[1] = self.prefix + segments[1]

        return "%s as %s" % (self.wrap(segments[0]), self.wrap_value(segments[1]))

    def columnize(self, columns):
        return ', '.join(map(lambda x: self.wrap(x), columns))

    def parameterize(self, values):
        return ', '.join(map(lambda x: self.parameter(x), values))

    def parameter(self, value):
        return value.get_value() if self.is_expression(value) else '?'

    def compile_select(self, query: Builder):
        original = query.columns_

        if not query.columns_:
            query.columns_ = ['*']

        sql = self.concatenate(self.compile_components(query))
        query.columns_ = original
        return sql

    def compile_components(self, query: Builder):
        sql = {}

        for component in self.select_components:
            if hasattr(query, component):
                q_c = getattr(query, component)
                method = '_compile_' + component.lower().strip('_')
                fn = getattr(self, method)
                sql[component] = fn(query, q_c)

        return sql

    @staticmethod
    def concatenate(segments):
        return ' '.join(filter(lambda x: x != '', segments))

    @staticmethod
    def remove_leading_boolean(value):
        return re.sub(r'and |or ', '', value, flags=re.IGNORECASE)

    def _compile_aggregate(self, query: Builder, aggregate):
        column = self.columnize(aggregate['columns_'])

        if query.distinct_ and column != '*':
            column = 'distinct ' + column

        return 'select {}({}) as aggregate'.format(aggregate['function'], column)

    def _compile_columns(self, query, columns):
        if not hasattr(query, 'aggregate'):
            return ''
        select = 'select distinct ' if query.distinct_ else 'select '
        return select + self.columnize(columns)

    def _compile_table(self, query, table):
        return 'from' + self.wrap_table(table)

    def _compile_joins(self, query, joins):
        def mp(join):
            table = self.wrap_table(join.table)
            return "{} join {} {}".format(join.type, table, self._compile_wheres(join))
        return ' '.join(map(mp, joins)).strip()

    def _compile_wheres(self, query, wheres):
        if not query.wheres_:
            return ''

        sql = self._compile_wheres_to_array(query)
        if len(sql) > 0:
            return self._concatenate_where_clauses(query, sql)

        return ''

    def _compile_wheres_to_array(self, query):
        def mf(where):
            fn = getattr(self, '_' + where['type']).lower()
            return where['boolean'] + ' ' + fn(query, where)
        return list(map(mf, query.wheres_))

    def _concatenate_where_clauses(self, query, sql):
        conj = 'on' if isinstance(query, JoinClause) else 'where'
        return conj + ' ' + self.remove_leading_boolean(' '.join(sql))

    def _where_raw(self, query, where):
        return where['sql']

    def _where_basic(self, query, where):
        value = self.parameter(where['value'])
        return self.wrap(where['column']) + ' ' + where['operator'] + ' ' + value

    def _where_in(self, query, where):
        if 'values' in where and where['values']:
            return self.wrap(where['column']) + ' in (' + self.parameterize(where['values']) + ')'

        return '0 = 1'

    def _where_not_in(self, query, where):
        if 'values' in where and where['values']:
            return self.wrap(where['column']) + ' not in (' + self.parameterize(where['values']) + ')'

        return '1 = 1'

    def _where_in_sub(self, query, where):
        return self.wrap(where['column']) + ' in (' + self.compile_select(where['query']) + ')'

    def _where_not_in_sub(self, query, where):
        return self.wrap(where['column']) + ' not in (' + self.compile_select(where['query']) + ')'

    def _where_null(self, query, where):
        return self.wrap(where['column']) + ' is null'

    def _where_not_null(self, query, where):
        return self.wrap(where['column']) + ' is not null'

    def _where_between(self, query, where):
        between = 'not between' if where['not'] else 'between'
        return self.wrap(where['column']) + between + ' ? and ?'

    def _where_column(self, query, where):
        return self.wrap(where['first']) + where['operator'] + ' ' + self.wrap(where['second'])

    def _where_nested(self, query, where):
        offset = 3 if isinstance(query, JoinClause) else 6
        return '(' + self._compile_wheres(where['query'])[offset:] + ')'

    def _where_sub(self, query, where):
        select = self.compile_select(where['query'])
        return self.wrap(where['column']) + ' ' + where['operator'] + ' (' + select + ')'

    def _where_exists(self, query, where):
        return 'exists (' + self.compile_select(where['query']) + ')'

    def _compile_groups(self, query, groups):
        pass









