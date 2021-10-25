import os
import glob
import logging
import pandas as pd
from crawler import fund as cFund
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
    fundInfos = {v['基金名稱']: v['基金統編'] for v in cFund.info(year, month)}

    for p in paths:
        logging.info(f"read fund {p}")

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
                insert = [{'company_id': id, 'name': n, 'code': ''} for n in funds]

                for i, v in enumerate(insert):
                    if v['name'] in fundInfos:
                        insert[i]['code'] = fundInfos[v['name']]

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

        logging.info(f"end read fund {p}")


def save_detail(data: dict, d: engine):
    insert = []
    session = Session(d)
    funds = {v.code: v.id for v in session.execute("select id, code from funds where code != ''").all()}

    for ym, rows in data.items():
        year = int(ym[:4])
        month = int(ym[4:])

        for v in rows:
            if v['基金統編'] not in funds:
                continue

            insert.append({
                'fund_id': funds[v['基金統編']],
                'year': year,
                'month': month,
                'scale': v['基金規模(台幣)'].replace(",",""),
                'value': v['單位淨值(台幣)'].replace(",",""),
                'natural_person': v['自然人受益人數'].replace(",",""),
                'legal_person': v['法人受益人數'].replace(",",""),
                'person': v['總受益人數'].replace(",",""),
                'buy_amount': v['本月申購總金額(台幣)'].replace(",",""),
                'sell_amount': v['本月買回總金額(台幣)'].replace(",",""),
            })

    if len(insert) > 0:
        result = session.execute(models.fundDetails.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error("insert fund detail")
            return False
        else:
            logging.info(f"save fund detail {len(insert)} count")
            session.commit()
