import time, uuid
from orm import Model,StringField,BooleanField,FloatField,TextField

def next_id():
    return '%015d%s000' % (int(time.time()*1000), uuid.uuid4().hex)

#Use表
class User(Model):
    __table__ = 'users'
    #def __init__(self, name, column_type, primary_key, default):
    id = StringField(primary_key=True, default=next_id())
    email = StringField()
    passwd = StringField()
    admin = BooleanField()
    name = StringField()
    image = StringField(ddl = 'varchar(500)')
    created_at = FloatField(default=time.time)

class Blog(Model):
    __table__ = 'blogs'
    id = StringField(primary_key=True, default=next_id())
    user_id = StringField()
    user_name = StringField()
    user_image = StringField(ddl = 'varchar(500)')
    name = StringField()
    summary = StringField(ddl='varchar(200)')
    content = TextField()
    created_at = FloatField(default=time.time)


class Comment(Model):
    __table__ = 'comments'
    id = StringField(primary_key=True, default=next_id())
    blog_id = StringField()
    user_id = StringField()
    user_name = StringField()
    user_image = StringField(ddl='varchar(500)')
    content = TextField()
    created_at = FloatField(default=time.time)

