from sqlalchemy.orm import registry
from sqlalchemy import MetaData, Table, Column, Integer, String, Text, CHAR, TIMESTAMP

metadata = MetaData()

news = Table('news', metadata,
             Column('id', Integer, primary_key=True),
             Column('source_id', Integer),
             Column('title', String(200)),
             Column('url', Text),
             Column('context', Text),
             Column('publish_time', TIMESTAMP),
             Column('create_time', TIMESTAMP),
             )

source = Table('source', metadata,
               Column('id', Integer, primary_key=True),
               Column('name', String(45))
               )
