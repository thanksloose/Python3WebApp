# !/usr/bin/env python3
# coding = utf-8

__author__ = 'hmm'

import asyncio, logging, aiomysql
import sys


# import pymysql

# 数据库连接并插入数据生效
# async def connect():
#     conn =  pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123', db='mydb')
#     cur = conn.cursor()
#     cur.execute("insert into users (id, name) values (2, 'bbb');")
#     cur.execute("select * from users;")
#     data = cur.fetchall()
#     for i in data:
#         print (i)
#     conn.commit()
# loop = asyncio.get_event_loop()
# loop.run_until_complete(connect())


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 3.4以前 @asyncio.coroutine
# 创建连接池
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    print('create database connection pool...')
    global __pool
    # 3.4以前 __pool = yield from aiomysql.create_pool(
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

# 关闭连接池
async def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()


# select 返回结果集
async def select(sql, args, size=None):
    log(sql, args)
    # yield from将调用一个子协程（也就是在一个协程中调用另一个协程）并直接获得子协程的返回结果
    # 3.4以前with (yield from __pool) as conn:
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换
            # await表达式用于获取一个coroutine的执行结果
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        # yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


# insert update delete 返回结果数
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            if not autocommit:
                await conn.commit()
                # cur.close()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(50'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


#  元类 任何继承自Model的类会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性中。
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 如果是model类 返回
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise SystemError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise SystemError('Primary key not found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
            tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
            tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# orm映射的基类
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # Model从dict继承，重写get set方法 以便直接调用 user['id']  user.id
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)

# 定义User对象，同数据库表users关联
class User(Model):
    __table__ = 'users'
    id = IntegerField(primary_key=True)
    name = StringField()

    def show(self):
        print(1, '__mappings__:', self.__mappings__)
        print(2, '__table__:', self.__table__)
        print(3, '__primary_key__:', self.__primary_key__)
        print(4, '__fields__:', self.__fields__)
        print(5, '__select__:', self.__select__)
        print(6, '__insert__:', self.__insert__)
        print(7, '__update__:', self.__update__)
        print(8, '__delete__:', self.__delete__)


# 关闭连接池
async def close_pool():
    global __pool
    if __pool is not None:
        await __pool.close()


async def test():
    await create_pool(loop=loop, host='localhost', port=3306, user='root', password='123', db='mydb')
    user = User(id=1, name='aaa', salary=111.1)
    await user.save()
    r = await User.findAll()
    print(r)
    await user.remove()
    r = await User.findAll()
    print(r)
    await destory_pool()


loop = asyncio.get_event_loop()

loop.run_until_complete(test())
loop.close()
if loop.is_closed():
    sys.exit(0)






# user = User(id = 123, name = 'hmm')
# 创建异步事件的句柄
# loop = asyncio.get_event_loop()
# loop.run_until_complete(create_pool(loop=loop,host='localhost', port=3306, user='root', password='123', db='mydb'))
# loop.run_until_complete(create_pool(loop=loop, user='root', password='123', db='mydb'))
# loop.run_until_complete(user.save())
# loop.close()
# if loop.is_closed():
#    sys.exit(0)
# user = User.findAll()
