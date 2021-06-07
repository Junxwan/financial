import logging
from models import models
from pandas import DataFrame
from sqlalchemy import schema
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 損益表
def profit(dataFrame: DataFrame, d: engine):
    imports({
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
    }, dataFrame, d, models.profit)


# 資產負責表
def assetsDebt(dataFrame: DataFrame, d: engine):
    imports({
        '現金及約當現金': 'cash',
        '存貨': 'stock',
        '應收票據淨額': 'bill_receivable',
        '應收帳款淨額': 'receivable',
        '應收帳款－關係人淨額': 'receivable_person',
        '其他應收款淨額': 'receivable_other',
        '其他應收款－關係人淨額': 'receivable_other_person',
        '採用權益法之投資': 'equity_method',
        '不動產、廠房及設備': 'real_estate',
        '無形資產': 'intangible_assets',
        '流動資產合計': 'flow_assets_total',
        '非流動資產合計': 'non_flow_assets_total',
        '資產總額': 'assets_total',
        '短期借款': 'short_loan',
        '應付短期票券': 'bill_short_payable',
        '應付帳款': 'payable',
        '應付帳款－關係人': 'payable_person',
        '其他應付款': 'payable_other',
        '其他應付款項－關係人': 'payable_other_person',
        '應付公司債': 'payable_company_debt',
        '遞延所得稅負債': 'tax_debt',
        '流動負債合計': 'flow_debt_total',
        '非流動負債合計': 'non_flow_debt_total',
        '負債總額': 'debt_total',
        '股本合計': 'capital',
        '歸屬於母公司業主之權益合計': 'main_equity_total',
        '非控制權益': 'non_equity_total',
        '權益總額': 'equity_total',
        '負債及權益總計': 'debt_equity_total',
    }, dataFrame, d, models.assetsDebt)


# 匯入財報
def imports(header, dataFrame: DataFrame, d: engine, model: schema):
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
            'SELECT `stocks`.`code` FROM ' + model.name + ' JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND season = :season',
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

        result = d.execute(model.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save {year} Q{season} {len(insert)} count")
