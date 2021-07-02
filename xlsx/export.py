import datetime
import glob
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
        'open', 'close', 'increase', 'volume', 'value', 'fund_value',
        'foreign_value', 'increase_5', 'increase_23', 'increase_63'
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


# 股價資料項目合併
def priceMerge(dir, year=None):
    data = {}
    stocks = {}
    names = ['open', 'close', 'increase', 'volume', 'value',
             'fund', 'fund_value', 'foreign_value', 'increase_5',
             'increase_23', 'increase_63'
             ]

    if year is None:
        endDate = f"1911-01-01"
    else:
        endDate = f"{year}-01-01"

    for d in glob.glob(os.path.join(dir, '*')):
        name = os.path.split(d)[1]

        if name not in names or os.path.isdir(d) == False:
            continue

        tmp = {}
        dates = {}

        for path in sorted(glob.glob(os.path.join(d, '*.csv')), reverse=True):
            for i, v in pd.read_csv(path).iterrows():
                code = v['代碼']

                if code not in tmp:
                    tmp[code] = {}
                    stocks[code] = v['名稱']

                for date, value in v[2:].items():
                    if endDate > date:
                        continue

                    if date not in dates:
                        dates[date] = True

                    tmp[code][date] = value

        data[name] = tmp

        logging.info(f"read all price {name}")

    dates = list(dates)
    for name, value in data.items():
        tmp = {}
        for code, items in value.items():
            v = [code, stocks[code]]
            for date in dates:
                if date not in items:
                    v.append(pd.NaT)
                else:
                    v.append(items[date])

            tmp[code] = v

        tmp = [v[1] for v in sorted(tmp.items(), key=lambda x: x[1], reverse=False)]

        data[name] = pd.DataFrame(tmp, columns=['代碼', '名稱'] + dates)
        data[name].to_csv(
            os.path.join(dir, f"{name}.csv"), encoding='utf_8_sig', index=False
        )

        logging.info(f"merge {name} price")

    with pd.ExcelWriter(os.path.join(dir, "price.xlsx")) as writer:
        for k, v in data.items():
            logging.info(f"merge {k} to price")
            v.to_excel(writer, sheet_name=k, index=False)

    logging.info(f"merge price ok")
