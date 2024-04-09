import math
from inspect import isfunction
import re
from .builder import Expression, JoinClause, Builder, flatten


class Grammar:
    def __init__(self, prefix):
        self.prefix = prefix
        self.operators = []
        self.select_components = ['aggregate_',
                                  'columns_',
                                  'from_',
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
        return value.get_value() if self.is_expression(value) else self.parameter_chars()

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
                # not None, [], {}
                if q_c:
                    method = '_compile_' + component.lower().strip('_')
                    fn = getattr(self, method)
                    sql[component] = fn(query, q_c)

        return sql

    @staticmethod
    def concatenate(segments):
        return ' '.join(filter(lambda x: x != '', segments.values()))

    @staticmethod
    def remove_leading_boolean(value):
        return re.sub(r'and |or ', '', value, 1, flags=re.IGNORECASE)

    def _compile_aggregate(self, query: Builder, aggregate):
        column = self.columnize(aggregate['columns'])

        if query.distinct_ and column != '*':
            column = 'distinct ' + column

        return 'select {}({}) as aggregate'.format(aggregate['function'], column)

    def _compile_columns(self, query, columns):
        if not hasattr(query, 'aggregate'):
            return ''
        select = 'select distinct ' if query.distinct_ else 'select '
        return select + self.columnize(columns)

    def _compile_from(self, query, table):
        return 'from ' + self.wrap_table(table)

    def _compile_joins(self, query, joins):
        def mp(join):
            table = self.wrap_table(join.table)
            return "{} join {} {}".format(join.type, table, self._compile_wheres(join, None)).lstrip()
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
            attr = '_where_' + where['type'].lower()
            fn = getattr(self, attr)
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
        return self.wrap(where['column']) + between + ' {} and {}'.format(self.parameter_chars(), self.parameter_chars())

    @staticmethod
    def parameter_chars():
        return '?'

    def _where_column(self, query, where):
        return self.wrap(where['first']) + where['operator'] + self.wrap(where['second'])

    def _where_nested(self, query, where):
        offset = 3 if isinstance(query, JoinClause) else 6
        return '(' + self._compile_wheres(where['query'], None)[offset:] + ')'

    def _where_sub(self, query, where):
        select = self.compile_select(where['query'])
        return self.wrap(where['column']) + ' ' + where['operator'] + ' (' + select + ')'

    def _where_exists(self, query, where):
        return 'exists (' + self.compile_select(where['query']) + ')'

    def _compile_groups(self, query, groups):
        return 'group by ' + self.columnize(groups)

    def _compile_having(self, query, havings):
        def mf(having):
            if having['type'] == 'Raw':
                return having['boolean'] + ' ' + having['sql']
            return self._compile_base_having(having)
        sql = ' '.join(map(mf, havings))
        return 'having ' + self.remove_leading_boolean(sql)

    def _compile_base_having(self, having):
        column = self.wrap(having['column'])
        parameter = self.parameter(having['value'])
        return having['boolean'] + ' ' + column + ' ' + having['operator'] + ' ' + parameter

    def _compile_orders(self, query, orders):
        def mf(order):
            if 'sql' in order:
                return order['sql']
            else:
                return self.wrap(order['column']) + ' ' + order['direction']

        if len(orders) > 0:
            return 'order by ' + ', '.join(map(mf, orders))

        return ''

    # def _compile_random(self, seed):
    #     return 'RANDOM()'

    def _compile_limit(self, query, limit):
        return 'limit ' + str(int(limit))

    def _compile_offset(self, query, offset):
        return 'offset ' + str(int(offset))

    def _compile_unions(self, query: Builder, unions):
        sql = ''
        for union in unions:
            sql += self._compile_union(union)

        sql += ' ' + self._compile_orders(query, query.union_orders)

        if query.union_limit:
            sql += ' ' + self._compile_limit(query, query.union_limit)

        if query.union_offset:
            sql += ' ' + self._compile_offset(query, query.union_offset)

        return sql.lstrip()

    def _compile_union(self, union):
        conj = ' union all ' if union['all'] else ' union '
        return conj + union['query'].to_sql()

    def compile_exists(self, query):
        select = self.compile_select(query)
        return 'select exists({}) as {}'.format(select, self.wrap('exists'))

    def compile_insert(self, query, values):
        table = self.wrap_table(query.from_)
        if type(values) != list:
            values = [values]
        columns = self.columnize(values[0].keys())
        parameters = ', '.join(map(lambda record: '(' + self.parameterize(record)+')', values))

        return f'insert into {table} ({columns}) values {parameters}'

    def compile_insert_get_id(self, query, values):
        return self.compile_insert(query, values)

    def compile_update(self, query: Builder, values):
        table = self.wrap_table(query.from_)
        columns = ', '.join(map(lambda p: self.wrap(p[0]) + ' = ' + self.parameter(p[1])+')', enumerate(values)))

        joins = ''
        if query.joins_:
            joins = ' ' + self._compile_joins(query, query.joins_)

        wheres = self._compile_wheres(query)

        return f'update {table}{joins} set {columns} {wheres}'

    def prepare_bindings_for_update(self, bindings, values):
        clean = bindings
        clean['join'] = []
        clean['select'] = []
        return bindings['join'] + list(values.values()) + flatten(clean)

    def compile_delete(self, query: Builder):
        sql = self._compile_wheres(query, query.wheres_) if type(query.wheres_) == list else ''
        return 'delete from ' + self.wrap_table(query.from_) + ' ' + sql.strip()

    @staticmethod
    def prepare_binding_for_delete(self, bindings):
        return flatten(bindings)

    def _compile_lock(self, query, value):
        return value if type(value) == str else ''






