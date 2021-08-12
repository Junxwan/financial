import glob
import os
import logging
import pandas as pd
import numpy as np
from models import models
from pandas import DataFrame
from sqlalchemy import schema
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 匯入財報
def imports(type, year, month=None, quarterly=None, dir=None, d: engine = None):
    paths = {}
    types = []

    if type is not None:
        if type == 'financial':
            types = ['balance_sheet', 'cash_flow_statement', 'changes_in_equity',
                     'consolidated_income_statement']
        else:
            types = [type]

    for t in types:
        if t == 'month_revenue':
            if month is None:
                paths[t] = glob.glob(os.path.join(dir, t, f"{year}*", '*'))
            else:
                m = "%02d" % month
                paths[t] = [os.path.join(dir, t, f"{year}", f"{year}-{m}.csv")]
        else:
            if quarterly is not None:
                paths[t] = glob.glob(os.path.join(dir, t, f"{year}Q{quarterly}", '*'))
            else:
                ps = glob.glob(os.path.join(dir, t, f"{year}*"))
                if len(ps) == 0:
                    continue

                q = os.path.split(ps[-1])[1]
                paths[t] = glob.glob(os.path.join(dir, t, f"{q}", '*'))

    for type, paths in paths.items():
        if type == 'month_revenue':
            for path in paths:
                ym = os.path.split(path)[1].split('.')[0].split('-')

                if month is not None and int(ym[1]) != month:
                    continue

                month_revenue(pd.read_csv(path).replace(np.nan, 0), int(ym[0]), int(ym[1]), d)
        else:
            if len(paths) == 0:
                continue

            data = {}
            for path in paths:
                data[os.path.split(path)[1].split('.')[0]] = pd.read_csv(path)

            yq = os.path.split(os.path.split(paths[0])[0])[1]
            year = int(yq[:4])
            quarterly = int(yq[-1])

            if len(data) > 0:
                def deleteFinancial(type, dir, year, quarterly, codes, model):
                    if len(codes) == 0:
                        return

                    updateCodes = []
                    insert = {}
                    for path in glob.glob(os.path.join(dir, type, f"{year}Q{quarterly}", '*')):
                        code = os.path.split(path)[1].split('.')[0]
                        if code in codes:
                            updateCodes.append(code)
                            insert[code] = pd.read_csv(path)

                    if len(insert) == 0:
                        return

                    session = Session(d)
                    ids = [v.id for v in
                           session.execute("SELECT id FROM stocks WHERE code IN :codes", {'codes': updateCodes}).all()
                           ]

                    result = session.execute(
                        f"DELETE FROM {model.name} WHERE stock_id IN ({','.join(str(e) for e in ids)}) AND year = {year} AND quarterly = {quarterly}"
                    )

                    session.commit()
                    logging.info(f"delete {model.name} year:{year}: quarterly:{quarterly} count:{result.rowcount}")

                    if type == 'consolidated_income_statement':
                        consolidated_income_statement(insert, year, quarterly, d)
                    elif type == 'balance_sheet':
                        balance_sheet(insert, year, quarterly, d)
                    elif type == 'cash_flow_statement':
                        cash_flow_statement(insert, year, quarterly, d)
                    elif type == 'changes_in_equity':
                        changes_in_equity(insert, year, quarterly, d)

                if type == 'consolidated_income_statement':
                    deleteFinancial(
                        'consolidated_income_statement',
                        dir,
                        year - 1, quarterly,
                        consolidated_income_statement(data, year, quarterly, d),
                        models.profit
                    )
                elif type == 'balance_sheet':
                    deleteFinancial(
                        'balance_sheet',
                        dir,
                        year - 1, quarterly,
                        balance_sheet(data, year, quarterly, d),
                        models.assetsDebt
                    )
                elif type == 'cash_flow_statement':
                    deleteFinancial(
                        'cash_flow_statement',
                        dir,
                        year - 1, quarterly,
                        cash_flow_statement(data, year, quarterly, d),
                        models.cash
                    )
                elif type == 'changes_in_equity':
                    deleteFinancial(
                        'changes_in_equity',
                        dir,
                        year - 1, quarterly,
                        changes_in_equity(data, year, quarterly, d),
                        models.equity
                    )


# 綜合損益表
def consolidated_income_statement(dataFrames: dict, year, quarterly, d: engine):
    return _import({
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
    }, dataFrames, year, quarterly, d, models.profit)


# 資產負責表
def balance_sheet(dataFrames: dict, year, quarterly, d: engine):
    return _import({
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
    }, dataFrames, year, quarterly, d, models.assetsDebt)


# 現金流量表
def cash_flow_statement(dataFrames: dict, year, quarterly, d: engine):
    return _import({
        '本期稅前淨利（淨損）': 'profit_pre',
        '折舊費用': 'depreciation',
        '營業活動之淨現金流入（流出）': 'business_activity',
        '取得不動產、廠房及設備': 'real_estate',
        '投資活動之淨現金流入（流出）': 'investment_activity',
        '籌資活動之淨現金流入（流出）': 'fundraising_activity',
        '本期現金及約當現金增加（減少）數': 'cash_add',
        '期初現金及約當現金餘額': 'start_cash_balance',
        '期末現金及約當現金餘額': 'end_cash_balance',
    }, dataFrames, year, quarterly, d, models.cash)


# 權益變動表
def changes_in_equity(dataFrames: dict, year, quarterly, d: engine):
    codes = _import({
        '期初餘額-股本合計': 'start_stock',
        '期末餘額-股本合計': 'end_stock',
        '期初餘額-資本公積': 'start_capital_reserve',
        '期末餘額-資本公積': 'end_capital_reserve',
        '期初餘額-法定盈餘公積': 'start_surplus_reserve',
        '期末餘額-法定盈餘公積': 'end_surplus_reserve',
        '期初餘額-未分配盈餘（或待彌補虧損）': 'start_undistributed_surplus',
        '期末餘額-未分配盈餘（或待彌補虧損）': 'end_undistributed_surplus',
        '期初餘額-權益總額': 'start_equity',
        '期末餘額-權益總額': 'end_equity',
    }, dataFrames, year, quarterly, d, models.equity)

    q = Session(d)

    stock = q.execute(
        'SELECT `stocks`.`code`, `equities`.`end_stock` FROM equities JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND quarterly = :quarterly',
        {
            'year': year,
            'quarterly': quarterly,
        }
    ).all()

    data = ""
    for v in stock:
        data += f"WHEN code = '{v.code}' THEN {v.end_stock} "

    aff = q.execute("UPDATE stocks SET capital = CASE " + data + " ELSE 0 END")

    logging.info(f"update capital count: {aff.rowcount}")

    q.commit()
    q.close()

    return codes


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
            'value': v['當月營收'],
            'qoq': v['qoq'],
            'yoy': v['yoy'],
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
def _import(header, dataFrames: dict, year, quarterly, d: engine, model: schema):
    data = {}
    session = Session(d)

    for code, value in dataFrames.items():
        data[code] = {}
        value = value.dropna(thresh=len(value.columns) - 1)
        columns = list(value.columns)
        indexs = list(value.iloc[:, 0])

        for h in header:
            hs = h.split('-')
            if hs[0] not in indexs:
                continue

            i = indexs.index(hs[0])

            if len(hs) > 1:
                if hs[1] not in columns:
                    continue

                ii = columns.index(hs[1])
            else:
                ii = 1

            data[code][header["-".join(hs)]] = value.iloc[i, ii]

        for k, v in header.items():
            if v not in data[code]:
                data[code][v] = 0

    codes = {v.code: v.id for v in session.query(models.stock).all()}
    exists = session.execute(
        'SELECT `stocks`.`code` FROM ' + model.name + ' JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND quarterly = :quarterly',
        {
            'year': year,
            'quarterly': quarterly,
        }
    ).all()

    insert = []
    insertCodes = []
    exists = [v.code for v in exists]

    for code, value in data.items():
        if code in exists:
            continue

        value['stock_id'] = codes[code]
        value['year'] = year
        value['quarterly'] = quarterly
        insert.append(value)

        insertCodes.append(code)

    if len(insert) > 0:
        result = session.execute(model.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info(f"insert {model.name} error")
        else:
            logging.info(f"save {model.name} year:{year} quarterly:{quarterly} count:{len(insert)}")
            session.commit()

    return insertCodes
