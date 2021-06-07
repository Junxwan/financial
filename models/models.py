from sqlalchemy import MetaData, Table, Column, Integer, String, Text, TIMESTAMP, Date, Float

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
               Column('id', Integer, autoincrement=True, primary_key=True, nullable=False),
               Column('name', String(45))
               )

stock = Table('stocks', metadata,
              Column('id', Integer, primary_key=True),
              Column('code', String(10)),
              Column('name', String(10)),
              Column('classification_id', Integer),
              Column('capital', Integer),
              )

profit = Table('profits', metadata,
               Column('id', Integer, primary_key=True),
               Column('stock_id', Integer),
               Column('year', Date),
               Column('season', Integer),
               Column('revenue', Integer),
               Column('cost', Integer),
               Column('gross', Integer),
               Column('market', Integer),
               Column('management', Integer),
               Column('research', Integer),
               Column('fee', Integer),
               Column('profit', Integer),
               Column('outside', Integer),
               Column('other', Integer),
               Column('profit_pre', Integer),
               Column('profit_after', Integer),
               Column('profit_total', Integer),
               Column('profit_main', Integer),
               Column('profit_non', Integer),
               Column('profit_main_total', Integer),
               Column('profit_non_total', Integer),
               Column('tax', Integer),
               Column('eps', Float),
               Column('created_at', TIMESTAMP),
               Column('updated_at', TIMESTAMP),
               )
