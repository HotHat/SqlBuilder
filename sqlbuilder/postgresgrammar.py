from .grammar import Grammar, Builder, flatten


class PostgresGrammar(Grammar):
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
        self.operators = [
            '=', '<', '>', '<=', '>=', '<>', '!=',
            'like', 'not like', 'between', 'ilike', 'not ilike',
            '~', '&', '|', '#', '<<', '>>', '<<=', '>>=',
            '&&', '@>', '<@', '?', '?|', '?&', '||', '-', '-', '#-',
            'is distinct from', 'is not distinct from',
        ]

    def _where_basic(self, query, where):
        if where['operator'].lower().find('like') != -1:
            return '%s::text %s %s' % self.wrap(where['column']), where['operator'], self.parameter(where['value'])

        return super()._where_basic(query, where)


    def compile_select(self, query: Builder):
        if query.unions_ and query.aggregate_:
            return self._compile_union_aggregate(query)

        sql = super().compile_select(query)
        if query.unions_:
            sql = '(' + sql + ')' + self._compile_unions(query, query.unions_)

        return sql

    def _compile_union_aggregate(self, query):
        sql = super()._compile_aggregate(query, query.aggregate_)
        query.aggregate_ = []
        return sql + ' from (' + self.compile_select(query) + ') as' + self.wrap_table('temp_table')

    def _compile_union(self, union):
        conj = ' union all ' if union['all'] else ' union '
        return conj + '(' + union['query'].to_sql() + ')'

    def _compile_lock(self, query, value):
        if str == type(value):
            return 'for update' if value else 'for share'
        return value

    def compile_insert(self, query, values):
        table = self.wrap_table(query.from_)
        return super().compile_insert(query, values) if values else f"insert into {table} DEFAULT VALUES"

    def compile_insert_or_ignore(self, query, values):
        return self.compile_insert(query, values) + ' no conflict on nothing'

    def compile_insert_get_id(self, query, values, sequence):
        return self.compile_insert(query, values) + ' returning ' + self.wrap(sequence if sequence else 'id')

    def _compile_update_columns(self, values):
        def mf(v):
            return self.wrap(v[0]) + ' = ' + self.parameter(v[1])

        return ', '.join(map(mf, values.items()))

    def compile_update(self, query: Builder, values):
        table = self.wrap_table(query.from_)
        columns = self._compile_update_columns(values)
        fm = self._compile_update_from(query)
        where = self._compile_update_wheres(query).rstrip()
        sql = f'update {table} set {columns} {fm} {where}'

        return sql.rstrip()

    def _compile_update_from(self, query):
        if not query.joins_:
            return ''
        fm = list(map(lambda x: self.wrap_table(x.table), query.joins_))

        if len(fm) > 0:
            return ' from ' + ', '.join(fm)
        return ''

    def _compile_update_wheres(self, query):
        base_wheres = self._compile_wheres(query, None)

        if not query.joins_:
            return base_wheres
        join_wheres = self._compile_update_join_wheres(query)
        if join_wheres.strip() == '':
            return 'where ' + self.remove_leading_boolean(join_wheres)
        return base_wheres + ' ' + join_wheres

    def _compile_update_join_wheres(self, query):
        join_wheres = []
        for join in query.joins_:
            for where in join.wheres_:
                method = '_where_%s' % where['type'].lower()
                fn = getattr(self, method)
                join_wheres.append(where['boolean'] + ' ' + fn(query, where))

        return ' '.join(join_wheres)

    def prepare_bindings_for_update(self, bindings, values):
        # TODO: json update
        # def fn(v):
        #     return MysqlGrammar.is_json_selector()
        # values = list(filter(fn, values))
        return super().prepare_bindings_for_update(bindings, values)

    def _compile_delete_with_joins(self, query, table):
        using = ' USING' + ', '.join(list(map(lambda x: self.wrap(x.table), query.joins_)))
        where = self._compile_update_wheres(query)
        return f'delete from {table}{using} {where}'

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

        return self._compile_delete_with_joins(query, table) if query.joins_ else super().compile_delete(query)

    def prepare_binding_for_delete(self, bindings):
        clean = bindings
        clean['join'] = []
        clean['select'] = []
        return bindings['where'] + flatten(clean)

    @staticmethod
    def wrap_json_selector(value):
        path = value.split('->')
        field = Grammar.wrap_value(path[0])
        target = '.'.join(map(lambda x: f'"{x}"', path[1:]))

        return f'{field}->\'$.{target}\''

    @staticmethod
    def parameter_chars():
        return '%s'
