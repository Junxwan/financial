from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, String, Text, TIMESTAMP, Date, Float

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
               Column('quarterly', Integer),
               Column('revenue', BigInteger),
               Column('cost', BigInteger),
               Column('gross', BigInteger),
               Column('market', BigInteger),
               Column('management', BigInteger),
               Column('research', BigInteger),
               Column('fee', BigInteger),
               Column('profit', BigInteger),
               Column('outside', BigInteger),
               Column('other', BigInteger),
               Column('profit_pre', BigInteger),
               Column('profit_after', BigInteger),
               Column('profit_total', BigInteger),
               Column('profit_main', BigInteger),
               Column('profit_non', BigInteger),
               Column('profit_main_total', BigInteger),
               Column('profit_non_total', BigInteger),
               Column('tax', BigInteger),
               Column('eps', Float),
               Column('created_at', TIMESTAMP),
               Column('updated_at', TIMESTAMP),
               )

assetsDebt = Table('assets_debts', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('stock_id', Integer),
                   Column('year', Date),
                   Column('quarterly', Integer),
                   Column('cash', BigInteger),
                   Column('stock', BigInteger),
                   Column('bill_receivable', BigInteger),
                   Column('receivable', BigInteger),
                   Column('receivable_person', BigInteger),
                   Column('receivable_other', BigInteger),
                   Column('receivable_other_person', BigInteger),
                   Column('equity_method', BigInteger),
                   Column('real_estate', BigInteger),
                   Column('intangible_assets', BigInteger),
                   Column('flow_assets_total', BigInteger),
                   Column('non_flow_assets_total', BigInteger),
                   Column('assets_total', BigInteger),
                   Column('short_loan', BigInteger),
                   Column('bill_short_payable', BigInteger),
                   Column('payable', BigInteger),
                   Column('payable_person', BigInteger),
                   Column('payable_other', BigInteger),
                   Column('payable_other_person', BigInteger),
                   Column('payable_company_debt', BigInteger),
                   Column('tax_debt', BigInteger),
                   Column('flow_debt_total', BigInteger),
                   Column('non_flow_debt_total', BigInteger),
                   Column('debt_total', BigInteger),
                   Column('capital', BigInteger),
                   Column('main_equity_total', BigInteger),
                   Column('non_equity_total', BigInteger),
                   Column('equity_total', BigInteger),
                   Column('debt_equity_total', BigInteger),
                   Column('created_at', TIMESTAMP),
                   Column('updated_at', TIMESTAMP),
                   )

cash = Table('cashs', metadata,
             Column('id', Integer, primary_key=True),
             Column('stock_id', Integer),
             Column('year', Date),
             Column('quarterly', Integer),
             Column('profit_pre', BigInteger),
             Column('depreciation', BigInteger),
             Column('business_activity', BigInteger),
             Column('real_estate', BigInteger),
             Column('investment_activity', BigInteger),
             Column('fundraising_activity', BigInteger),
             Column('cash_add', BigInteger),
             Column('start_cash_balance', BigInteger),
             Column('end_cash_balance', BigInteger),
             )

equity = Table('equities', metadata,
               Column('id', Integer, primary_key=True),
               Column('stock_id', Integer),
               Column('year', Date),
               Column('quarterly', Integer),
               Column('start_stock', BigInteger),
               Column('end_stock', BigInteger),
               Column('start_capital_reserve', BigInteger),
               Column('end_capital_reserve', BigInteger),
               Column('start_surplus_reserve', BigInteger),
               Column('end_surplus_reserve', BigInteger),
               Column('start_undistributed_surplus', BigInteger),
               Column('end_undistributed_surplus', BigInteger),
               Column('start_equity', BigInteger),
               Column('end_equity', BigInteger),
               )

revenue = Table('revenues', metadata,
                Column('id', Integer, primary_key=True),
                Column('stock_id', Integer),
                Column('year', Date),
                Column('month', Integer),
                Column('value', BigInteger),
                )

dividend = Table('dividends', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('stock_id', Integer),
                 Column('year', Date),
                 Column('cash', Float),
                 Column('stock', Float),
                 )

price = Table('prices', metadata,
              Column('id', Integer, primary_key=True),
              Column('stock_id', Integer),
              Column('date', Date),
              Column('open', Float),
              Column('close', Float),
              Column('high', Float),
              Column('low', Float),
              Column('increase', Float),
              Column('amplitude', Float),
              Column('volume', BigInteger),
              Column('volume_ratio', Float),
              Column('value', BigInteger),
              Column('main', Integer),
              Column('fund', Integer),
              Column('foreign', Integer),
              Column('volume_5', Integer),
              Column('volume_10', Integer),
              Column('volume_20', Integer),
              Column('increase_5', Float),
              Column('increase_23', Float),
              Column('increase_63', Float),
              Column('fund_value', BigInteger),
              Column('foreign_value', BigInteger),
              )

company = Table('companies', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', Text),
                )

fund = Table('funds', metadata,
             Column('id', Integer, primary_key=True),
             Column('company_id', Integer),
             Column('name', Text),
             )

fundStock = Table('fund_stocks', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('fund_id', Integer),
                  Column('stock_id', Integer),
                  Column('year', Date),
                  Column('month', Integer),
                  Column('amount', BigInteger),
                  Column('ratio', Float),
                  )

tagExponent = Table('tag_exponents', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('stock_id', Integer),
                    Column('tag_id', Integer),
                    )

