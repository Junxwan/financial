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
