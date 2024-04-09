
# sqlbuilder
sqlbuilder provides a convenient, fluent interface to creating and running database queries.
The `table` method returns a fluent query builder instance for the given table, allowing you to chain more constraints onto the query and then finally retrieve the results of the query using the `get` method
## create connection
```python
conn = MysqlConnection('table_prefix', 
                        host="host",
                        port=3306,
                        user="user",
                        password="password",
                        database='database')
```

## select
```python
(conn.table('users').select('id', 'name')
               .where('id', 2)
               .where('name', 'admin')
               .or_where('id', 1)
               .get())
```

## where
```python
def fn(query: Builder):
    query.where('name', 'Abigail').where('votes', '>', 50)

sql = (self.conn
           .table('users')
           .where('votes', '>', 100)
           .or_where(fn)
            .where([
                ['status', '=', '1'], ['subscribed', '<>', '1'],
            ])
           .get())
print(sql)
```

## where_in where_null
```python
sql = (self.conn
           .table('users')
           .where_in('id', [1, 2, 3])
           .where_not_in('id', [4, 5, 6])
           .where_null('updated_at')
           .where_not_null('created_at')
           .get()
       )
print(sql)
```
    
## group limit offset
```python
sql = (self.conn
           .table('users')
           .order_by('name', 'desc')
           .group_by('account_id', 'status')
           .having('account_id', '>', 100)
           .offset(10)
           .limit(5)
           .get())
print(sql)

```

## when
```python
def fn(query, value):
    query.where('role_id', value)

def default(query, value):
    query.order_by('name')

sql = (conn.table('users')
           .when(1, fn)
           .when(2, fn)
           .when(None, fn, default)
           .get())
print(sql)
```
## join
```python
(conn.table('users')
     .select('users.id AS uid', 'roles.permission_id')
     .join('user_roles', 'user_roles.user_id', '=', 'users.id')
     .left_join('roles', 'roles.id', '=', 'user_roles.role_id')
     .to_sql())
```
## insert
```python
nid = (conn.table('users')
           .insert([
               {'email': 'hothat@example.com', 'votes': 100, 'name': 'Test1'},
               {'email': 'hothat2@example.com', 'votes': 0, 'name': 'Test2'},
           ])
        )
conn.commit()
print(nid)
(conn.table('users')
     .insert({'email': 'hothat@example.com', 'votes': 0, 'name': 'hothat'}))
```

## update
```python
(self.conn
     .table('users')
     .where('id', 1)
     .update({'votes': 1}))
```

## delete   
```python
conn.table('users').delete()
conn.table('users').where('votes', '>', 100).delete()
```