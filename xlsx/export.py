import datetime
import logging
import os
import pandas as pd
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 匯出加權產業指數
def tse_industry(date, out, d: engine):
    session = Session(d)
    data = session.execute("SELECT stocks.id, stocks.code,prices.date,prices.volume_ratio FROM prices " +
                           "JOIN stocks ON stocks.id = prices.stock_id " +
                           "WHERE stocks.code LIKE 'TSE%' AND prices.date >= :date ORDER BY date ASC, code ASC",
                           {
                               'date': date,
                           }
                           ).all()
    names = []
    dates = []
    list = {}
    for v in data:
        date = v.date.__str__()

        if date not in dates:
            dates.append(date)
            list[date] = {}

        if v.code not in names:
            names.append(v.code)

        if v.code not in list[date]:
            list[date][v.code] = v.volume_ratio

    data = []
    for date in dates:
        data.append([date] + [list[date][v] for v in names])

    stock = session.execute("SELECT code, name FROM stocks").all()
    stock = {v.code: v.name for v in stock}

    file = os.path.join(out, 'export.csv')
    pd.DataFrame(data, columns=['date'] + [stock[v] for v in names]).to_csv(file, encoding='utf_8_sig', index=False)

    logging.info(f"完成 {file}")


def price(year, out, d: engine):
    names = [
        'open', 'close', 'increase', 'volume', 'value', 'fund_value', 'foreign_value', 'fund', 'increase_5',
        'increase_23', 'increase_63'
    ]

    session = Session(d)
    stocks = {v.code: v for v in session.execute("SELECT id, code, name FROM stocks").all()}
    data = {n: {} for n in names}

    for n in names:
        path = os.path.join(out, n, f"{year}.csv")
        if os.path.exists(path):
            for i, v in pd.read_csv(path).iterrows():
                id = stocks[v['代碼']].id

                if id not in data[n]:
                    data[n][id] = {}

                for date, value in v[2:].items():
                    data[n][id][date] = value

        else:
            data[n] = {}

    if len(data['open']) == 0:
        startDate = f"{year}-01-01"
    else:
        startDate = list(data['open'][1])[0]

    endDate = f"{year}-12-31"
    start = 0
    offset = 5000
    cs = ",".join(names)

    while True:
        prices = session.execute(
            f"SELECT date, stock_id, {cs} FROM prices WHERE date between :startDate AND :endDate ORDER BY date, stock_id limit :start, :end",
            {
                'startDate': startDate,
                'endDate': endDate,
                'start': start,
                'end': offset,
            }).all()

        logging.info(f"read {year} price {len(prices)} count start {start}")

        start = start + offset

        if len(prices) == 0:
            break

        for v in prices:
            date = v.date.__str__()
            code = v.stock_id
            v = dict(v)

            for n in names:
                if code not in data[n]:
                    data[n][code] = {}

                if date not in data[n][code]:
                    data[n][code][date] = 0

                data[n][code][date] = v[n]

    stocks = {v.id: v for v in session.execute("SELECT id, code, name FROM stocks").all()}
    dates = session.execute(
        "SELECT date FROM prices JOIN stocks ON stocks.id = prices.stock_id WHERE stocks.code = '2330' AND date between :startDate AND :endDate ORDER BY date DESC",
        {
            'startDate': f"{year}-01-01",
            'endDate': endDate,
        }).all()

    dates = [date._data[0].__str__() for date in dates]
    datas = {n: {} for n in names}
    for date in dates:
        for name, values in data.items():
            for id, value in values.items():
                v = stocks[id]

                if v.code not in datas[name]:
                    datas[name][v.code] = [v.code, v.name]

                if date not in value:
                    datas[name][v.code].append(pd.NaT)
                else:
                    datas[name][v.code].append(value[date])

    for name, v in datas.items():
        dir = os.path.join(out, name)

        if os.path.exists(dir) == False:
            os.mkdir(dir)

        v = [v[1] for v in sorted(v.items(), key=lambda x: x[1], reverse=False)]

        file = os.path.join(dir, f"{year}.csv")
        pd.DataFrame(v, columns=['代碼', '名稱'] + dates).to_csv(
            file, encoding='utf_8_sig', index=False
        )

        logging.info(f"save price {file}")
