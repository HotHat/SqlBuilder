from inspect import isfunction
import copy


class InvalidArgumentException(Exception):
    pass


class Expression:
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def __str__(self):
        return str(self.value)


class Builder:
    operators = [
        '=', '<', '>', '<=', '>=', '<>', '!=', '<=>',
        'like', 'like binary', 'not like', 'ilike',
        '&', '|', '^', '<<', '>>',
        'rlike', 'regexp', 'not regexp',
        '~', '~*', '!~', '!~*', 'similar to',
        'not similar to', 'not like', '~~*', '!~~*',
    ]

    def __init__(self, connection, grammar):
        self.connection = connection
        self.grammar = grammar
        self.aggregate_ = {}
        self.columns_ = []
        self.bindings = {
            'select': [],
            'join': [],
            'where': [],
            'having': [],
            'order': [],
            'union': [],
        }
        self.distinct_ = False
        self.from_ = ''
        self.joins_ = []
        self.wheres_ = []
        self.groups_ = []
        self.having_ = []
        self.orders_ = []
        self.unions_ = []
        self.union_orders = []
        self.union_offset = 0
        self.offset_ = 0
        self.union_limit = 0
        self.limit_ = 0

    def get_connection(self):
        return self.connection

    def get_grammar(self):
        return self.grammar

    def new_query(self):
        return Builder(self.connection, self.grammar)

    def select(self, *columns):
        self.columns_ = list(columns)
        return self

    def select_raw(self, expression, bindings=None):
        bindings = [] if None else bindings

        self.add_select(Expression(expression))
        if bindings:
            self.add_binding(bindings, 'select')
        return self

    def add_binding(self, value, stype='where'):
        if type in self.bindings:
            raise InvalidArgumentException(f"Invalid binding type: {stype}")
        if type(value) == list:
            self.bindings[stype] = self.bindings[stype] + value
        else:
            self.bindings[stype].append(value)

        return self

    @staticmethod
    def raw(value):
        return Expression(value)

    def add_select(self, *column):
        self.columns_ = self.columns_ + column
        return self

    def distinct(self):
        self.distinct_ = True
        return self

    def table(self, table):
        self.from_ = table
        return self

    def get_bindings(self):
        return self.bindings

    def join(self, table, first, operator=True, second=None, jtype='inner', where=False):
        join_clause = JoinClause(self, jtype, table)

        if isfunction(first):
            first(join_clause)
            self.joins_.append(join_clause)
            self.add_binding(join_clause.get_bindings(), 'join')
        else:
            method = 'where' if where else 'on'
            fn = getattr(join_clause, method)
            self.joins_.append(fn(first, operator, second))
            self.add_binding(join_clause.get_bindings(), 'join')

    def join_where(self, table, first, operator, second, jtype='inner'):
        return self.join(table, first, operator, second, jtype, True)

    def left_join(self, table, first, operator=None, second=None):
        return self.join(table, first, operator, second, 'left')

    def left_join_where(self, table, first, operator, second):
        return self.join_where(table, first, operator, second, 'left')

    def right_join(self, table, first, operator=None, second=None):
        return self.join(table, first, operator, second, 'left')

    def right_join_where(self, table, first, operator, second):
        return self.join_where(table, first, operator, second, 'right')

    def cross_join(self, table, first=None, operator=None, second=None):
        if first:
            return self.join(table, first, operator, second, 'cross')

        self.joins_.append(JoinClause(self, 'cross', table))
        return self

    def merge_wheres(self, wheres, bindings):
        self.wheres_ = self.wheres_ + wheres
        self.bindings['where'] = self.bindings['where'] + bindings

    def for_nested_where(self):
        return self.new_query().table(self.from_)

    def add_nested_where_query(self, query, boolean):
        if len(query.wheres_) > 0:
            self.wheres_.append({
                'type': 'Nested',
                'query': query,
                'boolean': boolean
            })
            self.add_binding(query.get_bindings())
        return self

    def where_nested(self, fn, boolean='and'):
        query = self.for_nested_where()
        fn(query)
        return self.add_nested_where_query(query, boolean)

    def add_dict_wheres(self, column: dict, boolean, method='where'):
        def fn(query):
            query_fn = getattr(query, method)
            if type(column) == list:
                for col in column:
                    query_fn(*col)
            else:
                for key, value in column.items():
                    query_fn(key, '=', value, boolean)

        return self.where_nested(fn, boolean)

    @staticmethod
    def invalid_operator_and_value(operator, value) -> bool:
        return value is None and (operator in Builder.operators) and (operator not in ['=', '<>', '!='])

    def invalid_operator(self, operator) -> bool:
        return (operator not in Builder.operators) and (operator not in self.grammar.operators)

    def prepare_value_and_operator(self, value, operator, default=False):
        if default:
            return [operator, '=']
        elif self.invalid_operator_and_value(operator, value):
            raise InvalidArgumentException('Illegal operator and value combination.')

        return [value, operator]

    def for_sub_query(self):
        return self.new_query()

    def where_sub(self, column, operator, callback, boolean):
        query = self.for_sub_query()
        callback(query)
        self.wheres_.append({
            'type': 'Sub',
            'column': column,
            'operator': operator,
            'query': query,
            'boolean': boolean
        })
        self.add_binding(query.get_bindings, 'where')
        return self

    def where_null(self, column, boolean='and', not_null=False):
        self.wheres_.append({
            'type': 'NotNull' if not_null else 'Null',
            'column': column,
            'boolean': boolean
        })
        return self

    def or_where_null(self, column):
        return self.where_null(column, 'or')

    def where_not_null(self, column, boolean='and'):
        return self.where_null(column, boolean, True)

    def or_where_not_null(self, column):
        return self.where_not_null(column, 'or')

    def where(self, column, operator=None, value=None, boolean='and'):
        if type(column) == dict or type(column) == list:
            return self.add_dict_wheres(column, boolean)

        [value, operator] = self.prepare_value_and_operator(value, operator, (value is None) and (operator is not None))
        if isfunction(column):
            return self.where_nested(column, boolean)
        if self.invalid_operator(operator):
            value, operator = operator, '='
        if isfunction(value):
            return self.where_sub(column, operator, value, boolean)
        if value is None:
            return self.where_null(column, boolean, not operator == '=')

        if type(column) == str and column.find('->') != -1 and type(value) == bool:
            value = Expression('true' if value else 'false')

        self.wheres_.append({
            'type': 'Basic',
            'column': column,
            'operator': operator,
            'value': value,
            'boolean': boolean
        })

        if not isinstance(value, Expression):
            self.add_binding(value, 'where')

        return self

    def or_where(self, column, operator=None, value=None):
        [value, operator] = self.prepare_value_and_operator(value, operator, operator is not None and value is None)
        return self.where(column, operator, value, 'or')

    def where_column(self, first, operator=None, second=None, boolean='and'):
        """
        Add a "where" clause comparing two columns_ to the query.
        :param first:
        :param operator:
        :param second:
        :param boolean:
        :return:
        """
        if type(first) == list or type(first) == dict:
            return self.add_dict_wheres(first, boolean, 'whereColumn')

        if self.invalid_operator(operator):
            second, operator = operator, '='

        self.wheres_.append({
            'type': 'Column',
            'first': first,
            'operator': operator,
            'second': second,
            'boolean': boolean
        })
        return self

    def or_where_column(self, first, operator=None, second=None):
        """
        Add an "or where" clause comparing two columns_ to the query.
        :param first:
        :param operator:
        :param second:
        :return:
        """
        return self.where_column(first, operator, second, 'or')

    def where_raw(self, sql, bindings, boolean='and'):
        bindings = bindings if bindings else []
        self.wheres_.append({
            'type': 'raw',
            'sql': sql,
            'boolean': boolean
        })
        self.add_binding(bindings, 'where')
        return self

    def where_in_existing_query(self, column, query, boolean, not_in):
        self.wheres_.append({
            'type': 'NotInSub' if not_in else 'InSub',
            'column': column,
            'query': query,
            'boolean': boolean
        })
        self.add_binding(query.get_bindings(), 'where')
        return self

    def where_in_sub(self, column, callback, boolean, not_in):
        query = self.for_sub_query()
        self.wheres_.append({
            'type': 'NotInSub' if not_in else 'InSub',
            'column': column,
            'query': query,
            'boolean': boolean
        })
        self.add_binding(query.get_bindings(), 'where')
        return self

    def or_where_raw(self, sql, bindings):
        bindings = bindings if bindings else []
        return self.where_raw(sql, bindings, 'or')

    def where_in(self, column, values, boolean='and', not_in=False):
        if isinstance(values, Builder):
            return self.where_in_existing_query(column, values, boolean, not_in)
        if isfunction(values):
            return self.where_in_sub(column, values, boolean, not_in)

        self.wheres_.append({
            'type': 'NotIn' if not_in else 'In',
            'column': column,
            'values': values,
            'boolean': boolean
        })
        for value in values:
            if not isinstance(value, Expression):
                self.add_binding(value, 'where')

        return self

    def or_where_in(self, column, values):
        return self.where_in(column, values, 'or')

    def where_not_in(self, column, values, boolean='and'):
        return self.where_in(column, values, boolean, True)

    def or_where_not_in(self, column, values, boolean='and'):
        return self.where_not_in(column, values, 'or')

    def where_between(self, column, values, boolean='and', not_bt=False):
        self.wheres_.append({
            'type': 'between',
            'column': column,
            'boolean': boolean,
            'not': not_bt
        })
        self.add_binding(values, 'where')
        return self

    def or_where_between(self, column, values):
        return self.where_between(column, values, 'or')

    def where_not_between(self, column, values, boolean='and'):
        return self.where_between(column, values, boolean, True)

    def or_where_not_between(self, column, values):
        return self.where_not_between(column, values, 'or')

    def where_exists(self, callback, boolean='and', not_ext=False):
        query = self.for_sub_query()
        callback(query)
        return self.add_where_exists_query(query, boolean, not_ext)

    def or_where_exists(self, callback, not_ext=False):
        return self.where_exists(callback, 'or', not_ext)

    def where_not_exists(self, callback, boolean='and'):
        return self.where_exists(callback, boolean, True)

    def or_where_not_exists(self, callback):
        return self.where_not_exists(callback, True)

    def add_where_exists_query(self, query, boolean='and', not_ext=False):
        self.wheres_.append({
            'type': 'NotExists' if not_ext else 'Exists',
            'query': query,
            'boolean': boolean,
        })
        self.add_binding(query.get_bindings(), 'where')
        return self

    def group_by(self, *groups):
        for group in groups:
            if type(group) == list:
                self.groups_ += group
            else:
                self.groups_.append(group)
        return self

    def having(self, column, operator=None, value=None, boolean='and'):
        [value, operator] = self.prepare_value_and_operator(value, operator, (value is None) and (operator is not None))

        if self.invalid_operator(operator):
            value, operator = operator, '='

        self.having_.append({
            'type': 'Basic',
            'column': column,
            'operator': operator,
            'value': value,
            'boolean': boolean
        })

        if not isinstance(value, Expression):
            self.add_binding(value, 'where')

        return self

    def or_having(self, column, operator=None, value=None):
        return self.having(column, operator, value, 'or')

    def having_raw(self, sql, bindings, boolean='and'):
        bindings = bindings if bindings else []
        self.having_.append({
            'type': 'Raw',
            'sql': sql,
            'boolean': boolean
        })
        self.add_binding(bindings, 'having')
        return self

    def or_having_raw(self, sql, bindings):
        bindings = bindings if bindings else []
        return self.having_raw(sql, bindings, 'or')

    def order_by(self, column, direct='asc'):
        item = {
            'column': column,
            'direction': 'asc' if direct.lower() == 'asc' else 'desc'
        }
        if len(self.unions_) > 0:
            self.union_orders.append(item)
        else:
            self.orders_.append(item)

        return self

    def order_by_desc(self, column):
        return self.order_by(column, 'desc')

    def skip(self, value):
        return self.offset(value)

    def take(self, value):
        return self.limit(value)

    def limit(self, value):
        if value < 0:
            return self

        # value > 0
        if len(self.unions_) > 0:
            self.union_limit = value
        else:
            self.limit_ = value

        return self

    def offset(self, value):
        if len(self.unions_) > 0:
            self.union_offset = max(0, value)
        else:
            self.offset_ = max(0, value)
        return self

    def for_page(self, page, per_page=15):
        return self.skip((page - 1) * per_page).take(per_page)

    def union(self, query, all_union=False):
        union_query = query
        if isfunction(query):
            union_query = self.new_query()
            query(union_query)

        self.unions_.append({
            'query': union_query,
            'all': all_union
        })
        self.add_binding(query.get_bindings(), 'union')
        return self

    def union_all(self, query):
        return self.union(query, True)

    def to_sql(self):
        return self.grammar.compile_select(self)

    def first(self, columns):
        columns = ['*'] if len(columns) == 0 else columns
        return self.take(1).get(columns)

    def find(self, qid, columns):
        columns = ['*'] if len(columns) == 0 else columns
        return self.where('id', '=', qid).first(columns)

    def count(self, columns='*'):
        return self.aggregate('count', columns if type(columns) == list else [columns])

    def min(self, columns):
        return self.aggregate('min', [columns])

    def max(self, columns):
        return self.aggregate('max', [columns])

    def sum(self, columns):
        result = self.aggregate('sum', [columns])
        return result if result else 0

    def avg(self, columns):
        return self.aggregate('avg', [columns])

    def aggregate(self, fn, columns):
        columns = columns if columns else ['*']
        clone = copy.copy(self)
        clone.columns_ = []
        clone.bindings['select'] = []
        clone.set_aggregate(fn, columns)
        results = clone.get(columns)
        if results:
            return results[0]['aggregate']

        return None

    def set_aggregate(self, fn, columns):
        self.aggregate_ = {
            'function': fn,
            'columns': columns
        }
        if self.groups_:
            self.orders_ = []
            self.bindings['order'] = []
        return self

    def get(self, columns):
        columns = ['*'] if len(columns) == 0 else columns
        original = self.columns_
        if not original:
            self.columns_ = columns

        result = self.run_select()
        self.columns_ = original
        return result

    def run_select(self):
        sql = self.to_sql()
        bindings = self.get_bindings()
        print({
            'sql': sql,
            'bindings': bindings
        })
        return self.connection.select(sql, bindings)


class JoinClause(Builder):
    def __init__(self, parent, join_type, table):
        self.parent_query = parent
        self.type = join_type
        self.table = table
        super().__init__(parent.get_connection(), parent.get_grammar())

    def on(self, first, operator, second, boolean='and'):
        if isfunction(first):
            return self.where_nested(first, boolean)
        return self.where_column(first, operator, second, boolean)

    def or_on(self, first, operator=None, second=None):
        return self.on(first, operator, second, 'or')

    def new_query(self):
        return JoinClause(self.parent_query, self.type, self.table)

    def for_sub_query(self):
        return self.parent_query.new_query()


