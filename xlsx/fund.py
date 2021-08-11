import os
import glob
import logging
import pandas as pd
from models import models
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 匯入
def imports(year, month, dir, d: engine):
    paths = []

    if year is not None and month is not None:
        paths.append(os.path.join(dir, f"{year}" + '{0:02d}'.format(month) + '.csv'))
    elif year is not None:
        paths = glob.glob(os.path.join(dir, f"{year}*.csv"))

    session = Session(d)
    companies = {v.code: v.id for v in session.execute(models.company.select()).all()}
    stocks = {v.code: v.id for v in session.execute(models.stock.select()).all()}

    for p in paths:
        for name, v in pd.read_csv(p).groupby('c_name'):
            code = v.iloc[0]['c_code']
            if code not in companies:
                result = session.execute(models.company.insert(), {'name': name, 'code': code})

                if result.is_insert == False:
                    logging.info("insert company error")
                    return False
                else:
                    logging.info(f"save company {name}")
                    session.commit()

                companies[code] = result.lastrowid

            id = companies[code]

            # 基金
            funds = list(v.groupby('f_name').f_name.indices)
            funds = set(funds).difference(
                [v.name for v in session.execute(models.fund.select()).all() if v.name in funds]
            )

            if len(funds) > 0:
                insert = [{'company_id': id, 'name': n} for n in funds]
                result = session.execute(models.fund.insert(), insert)

                if result.is_insert == False or result.rowcount != len(insert):
                    logging.info("insert fund error")
                    return False
                else:
                    logging.info(f"save fund {name} {len(insert)} count")
                    session.commit()

            funds = {v.name: v.id for v in
                     session.execute(
                         "SELECT id, name FROM funds WHERE company_id = :company_id",
                         {'company_id': id}
                     ).all()
                     }

            # 個股
            insert = []
            ym = os.path.split(p)[1].split('.')[0]
            year = ym[:4]
            month = int(ym[4:])
            exists = {i: [] for i in list(funds.values())}
            _ = [exists[v.fund_id].append(v.stock_id) for v in
                 session.execute(
                     "SELECT fund_id, stock_id FROM fund_stocks WHERE year = :year AND month = :month AND fund_id IN :fund_id",
                     {
                         'year': year,
                         'month': month,
                         'fund_id': list(funds.values()),
                     }).all()
                 ]

            for i, data in v.iterrows():
                if data['f_name'] not in funds or data['code'] not in stocks:
                    continue

                if stocks[data['code']] in exists[funds[data['f_name']]]:
                    continue

                insert.append({
                    'fund_id': funds[data['f_name']],
                    'stock_id': stocks[data['code']],
                    'year': year,
                    'month': month,
                    'amount': int(data['amount'].replace(',', '')),
                    'ratio': data['total'],
                })

            if (len(insert) > 0):
                result = session.execute(models.fundStock.insert(), insert)

                if result.is_insert == False or result.rowcount != len(insert):
                    logging.info("insert fund stock error")
                    return False
                else:
                    logging.info(f"save fund stock {name} {len(insert)} count")
                    session.commit()
