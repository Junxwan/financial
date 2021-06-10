import logging
from models import models
from pandas import DataFrame
from sqlalchemy import schema
from sqlalchemy import engine
from sqlalchemy.orm import Session


# 綜合損益表
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


# 現金流量表
def cash(dataFrame: DataFrame, d: engine):
    imports({
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
    imports({
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
        'SELECT `stocks`.`code`, `equitys`.`end_stock` FROM equities JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND quarterly = :quarterly',
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
def month_revenue(dataFrame: DataFrame, d: engine):
    code = []
    codes = {v.code: v.id for v in Session(d).query(models.stock).all()}
    for k, v in dataFrame.items():
        if k == 'code':
            code = v
            continue
        year = k[:4]
        month = int(k[4:])

        q = Session(d)
        exists = q.execute(
            'SELECT `stocks`.`code` FROM ' + 'revenues' + ' JOIN stocks ON stock_id = `stocks`.`id` WHERE year = :year AND month = :month',
            {
                'year': year,
                'month': month,
            }
        ).all()

        exists = [v.code for v in exists]

        q.close()

        insert = []
        for i, r in enumerate(v):
            if str(code[i]) in exists or r == 0:
                continue

            insert.append({
                'stock_id': codes[str(code[i])],
                'year': year,
                'month': month,
                'value': r,
            })

        if len(insert) < 1:
            continue

        result = d.execute(models.revenue.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.info("insert error")
        else:
            logging.info(f"save {year} month {month} {len(insert)} count")


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
def imports(header, dataFrame: DataFrame, d: engine, model: schema):
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
