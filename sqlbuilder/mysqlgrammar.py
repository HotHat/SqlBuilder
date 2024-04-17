from .grammar import Grammar, Builder, flatten


class MysqlGrammar(Grammar):
    def __init__(self, prefix):
        super().__init__(prefix)
        self.select_components = [
            'aggregate_',
            'columns_',
            'from_',
            'joins_',
            'wheres_',
            'groups_',
            'having_',
            'orders_',
            'limit_',
            'offset_',
            'lock_',
        ]

    def compile_select(self, query: Builder):
        sql = super().compile_select(query)
        if query.unions_:
            sql = '(' + sql + ')' + self._compile_unions(query, query.unions_)

        return sql

    def _compile_union(self, union):
        conj = ' union all ' if union['all'] else ' union '
        return conj + '(' + union['query'].to_sql() + ')'

    def _compile_lock(self, query, value):
        if str == type(value):
            return 'for update' if value else 'lock in share mode'
        return value

    def _compile_update_columns(self, values):
        def mf(v):
            return self.wrap(v[0]) + ' = ' + self.parameter(v[1])

        return ', '.join(map(mf, values.items()))

    def compile_insert_or_ignore(self, query, values):
        sql = self.compile_insert(query, values)
        sql.replace('insert', 'insert ignore', 1)
        return sql

    def compile_update(self, query: Builder, values):
        table = self.wrap_table(query.from_)
        columns = self._compile_update_columns(values)

        joins = ''
        if query.joins_:
            joins = ' ' + self._compile_joins(query, query.joins_)

        where = self._compile_wheres(query, query.wheres_).rstrip()

        sql = f'update {table}{joins} set {columns} {where}'

        if query.limit_:
            sql += ' ' + self._compile_limit(query, query.limit_)

        return sql.rstrip()

    def prepare_bindings_for_update(self, bindings, values):
        # TODO: json update
        # def fn(v):
        #     return MysqlGrammar.is_json_selector()
        # values = list(filter(fn, values))
        return super().prepare_bindings_for_update(bindings, values)

    def _compile_delete_with_joins(self, query, table, where):
        joins = ' ' + self._compile_joins(query, query.joins_)
        alias = table.split(' as ')[1] if table.lower().find(' as ') != -1 else table
        return f'delete {alias} from {table}{joins} {where}'

    def _compile_delete_without_joins(self, query, table, where):
        sql = f'delete from {table} {where}'

        if query.orders_:
            sql += ' ' + self._compile_orders(query, query.orders_)

        if query.limit_:
            sql += ' ' + self._compile_limit(query, query.limit_)

        return sql

    def compile_delete(self, query: Builder):
        table = self.wrap_table(query.from_)
        where = self._compile_wheres(query, query.wheres_) if query.wheres_ else ''

        return self._compile_delete_with_joins(query, table, where) if query.joins_ else \
            self._compile_delete_without_joins(query, table, where)

    def prepare_binding_for_delete(self, bindings):
        clean = dict(bindings)
        clean['join'] = []
        clean['select'] = []
        return bindings['join'] + flatten(clean)

    def wrap_value(self, value):
        if value == '*':
            return value
        if MysqlGrammar.is_json_selector(value):
            return self.wrap_json_selector(value)

        return '`' + value.replace('`', '``') + '`'

    def wrap_json_selector(self, value):
        path = value.split('->')
        field = self.wrap_value(path[0])
        target = '.'.join(map(lambda x: f'"{x}"', path[1:]))

        return f'{field}->\'$.{target}\''

    @staticmethod
    def is_json_selector(value):
        return value.find('->') != -1

    @staticmethod
    def parameter_chars():
        return '%s'
