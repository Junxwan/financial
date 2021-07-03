import glob
import os
import logging
import pandas as pd
from models import models
from pandas import DataFrame
from sqlalchemy import schema
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 匯入財報
def imports(type, year, month=None, dir=None, d: engine = None):
    types = ['balance_sheet', 'cash_flow_statement', 'changes_in_equity',
             'consolidated_income_statement', 'dividend', 'month_revenue']

    if type is None:
        paths = {}
    else:
        paths = {type: glob.glob(os.path.join(dir, type, f"{year}*", '*'))}

    for type, paths in paths.items():
        for path in paths:
            if type == 'month_revenue':
                ym = os.path.split(path)[1].split('.')[0].split('-')

                if month is not None and int(ym[1]) != month:
                    continue

                month_revenue(pd.read_csv(path), int(ym[0]), int(ym[1]), d)

        # logging.info(f"read {type} {path}")
        # data = pd.read_csv(path)
        #
        # if type == 'profit':
        #     profit(data, d)
        # elif type == 'assetsDebt':
        #     assetsDebt(data, d)
        # elif type == 'cash':
        #     cash(data, d)
        # elif type == 'equity':
        #     equity(data, d)
        # elif type == 'revenue':
        #     month_revenue(data, d)
        # elif type == 'dividend':
        #     dividend(data, d)


# 綜合損益表
def profit(dataFrame: DataFrame, d: engine):
    _import({
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
    _import({
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


# 現金流量表
def cash(dataFrame: DataFrame, d: engine):
    _import({
        '本期稅前淨利（淨損）': 'profit_pre',
        '折舊費用': 'depreciation',
        '營業活動之淨現金流入（流出）': 'business_activity',
        '取得不動產、廠房及設備': 'real_estate',
        '投資活動之淨現金流入（流出）': 'investment_activity',
        '籌資活動之淨現金流入（流出）': 'fundraising_activity',
        '本期現金及約當現金增加（減少）數': 'cash_add',
        '期初現金及約當現金餘額': 'start_cash_balance',
        '期末現金及約當現金餘額': 'end_cash_balance',
    }, dataFrame, d, models.cash)


# 權益變動表
def equity(dataFrame: DataFrame, d: engine):
    _import({
        '股本合計-期初餘額': 'start_stock',
        '股本合計-期末餘額': 'end_stock',
        '資本公積-期初餘額': 'start_capital_reserve',
        '資本公積-期末餘額': 'end_capital_reserve',
        '法定盈餘公積-期初餘額': 'start_surplus_reserve',
        '法定盈餘公積-期末餘額': 'end_surplus_reserve',
        '未分配盈餘（或待彌補虧損）-期初餘額': 'start_undistributed_surplus',
        '未分配盈餘（或待彌補虧損）-期末餘額': 'end_undistributed_surplus',
        '權益總額-期初餘額': 'start_equity',
        '權益總額-期末餘額': 'end_equity',
    }, dataFrame, d, models.equity)

    q = Session(d)
    yq = dataFrame.columns[2]

    stock = q.execute(
        'SELECT `stocks`.`code`, `equities`.`end_stock` FROM equities JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND quarterly = :quarterly',
        {
            'year': yq[:4],
            'quarterly': yq[-1],
        }
    ).all()

    data = ""
    for v in stock:
        data += f"WHEN code = '{v.code}' THEN {v.end_stock} "

    aff = q.execute("UPDATE stocks SET capital = CASE " + data + " ELSE 0 END")

    logging.info(f"update capital count: {aff.rowcount}")

    q.commit()
    q.close()


# 月營收
def month_revenue(dataFrame: DataFrame, year, month, d: engine):
    session = Session(d)
    codes = {v.code: v.id for v in session.query(models.stock).all()}

    exists = session.execute(
        'SELECT `stocks`.`code` FROM ' + 'revenues' + ' JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND month = :month',
        {
            'year': year,
            'month': month,
        }
    ).all()

    exists = [v.code for v in exists]

    insert = []
    for i, v in dataFrame.iterrows():
        code = str(v['code'])
        if code in exists:
            continue

        insert.append({
            'stock_id': codes[code],
            'year': year,
            'month': month,
            'value': v['value'],
        })

    if len(insert) > 0:
        result = d.execute(models.revenue.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save revenue year:{year} month:{month} count:{len(insert)}")


# 股利
def dividend(dataFrame: DataFrame, d: engine):
    q = Session(d)
    header = {
        '現金股利': 'cash',
        '股票股利': 'stock',
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

    codes = {v.code: v.id for v in q.query(models.stock).all()}

    for year, value in data.items():
        exists = q.execute(
            'SELECT `stocks`.`code` FROM dividends JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year',
            {
                'year': year,
            }
        ).all()

        exists = [v.code for v in exists]

        insert = []
        for code, body in value.items():
            if str(code) in exists:
                continue

            body['stock_id'] = codes[str(code)]
            body['year'] = year
            insert.append(body)

        if len(insert) < 1:
            continue

        result = d.execute(models.dividend.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save {year} year {year} {len(insert)} count")


# 匯入財報
def _import(header, dataFrame: DataFrame, d: engine, model: schema):
    data = {}
    q = Session(d)

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

    codes = {v.code: v.id for v in q.query(models.stock).all()}

    for ys, items in data.items():
        insert = []
        year = ys[:4]
        quarterly = ys[-1]

        exists = q.execute(
            'SELECT `stocks`.`code` FROM ' + model.name + ' JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND quarterly = :quarterly',
            {
                'year': year,
                'quarterly': quarterly,
            }
        ).all()

        exists = [v.code for v in exists]

        for code, value in items.items():
            if str(code) in exists:
                continue

            value['stock_id'] = codes[str(code)]
            value['year'] = year
            value['quarterly'] = quarterly
            insert.append(value)

        if len(insert) < 1:
            continue

        result = d.execute(model.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save {year} Q{quarterly} {len(insert)} count")
