import logging
from models import models
from pandas import DataFrame
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 損益表
def profit(dataFrame: DataFrame, d: engine):
    header = {
        '營業收入合計': 'revenue',
        '營業成本合計': 'cost',
        '營業毛利（毛損）': 'gross',
        '推銷費用': 'market',
        '管理費用': 'management',
        '研究發展費用': 'research',
        '營業費用合計': 'fee',
        '營業利益（損失）': 'profit',
        '營業外收入及支出合計': 'outside',
        '其他收益及費損淨額': 'other',
        '稅前淨利（淨損）': 'profit_pre',
        '所得稅費用（利益）合計': 'tax',
        '本期淨利（淨損）': 'profit_after',
        '本期綜合損益總額': 'profit_total',
        '母公司業主（淨利∕損）': 'profit_main',
        '非控制權益（淨利∕損）': 'profit_non',
        '母公司業主（綜合損益）': 'profit_main_total',
        '非控制權益（綜合損益）': 'profit_non_total',
        '基本每股盈餘': 'eps',
    }

    data = {}

    for f in dataFrame.groupby('code'):
        code = f[0]
        h = []

        for v in f[1].items():
            if v[0] == 'code':
                continue

            if v[0] == 'name':
                h = [header[v] for v in list(v[1])]
                continue

            if v[0] not in data:
                data[v[0]] = {}

            data[v[0]][code] = {h[i]: a for i, a in enumerate(v[1])}

    codes = {v.code: v.id for v in Session(d).query(models.stock).all()}

    for ys, items in data.items():
        insert = []
        year = ys[:4]
        season = ys[-1]

        exists = Session(d).execute(
            'SELECT `stocks`.`code` FROM profits JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND season = :season',
            {
                'year': year,
                'season': season,
            }
        ).all()

        exists = [v.code for v in exists]

        for code, value in items.items():
            if str(code) in exists:
                continue

            value['stock_id'] = codes[str(code)]
            value['year'] = year
            value['season'] = season
            insert.append(value)

        if len(insert) < 1:
            continue

        result = d.execute(models.profit.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save {year} Q{season} {len(insert)} count")
