import click
import os
import time
import glob
import logging
import smtplib
import pytz
import calendar
import asyncio
import pymysql
import eml_parser
import requests
import crawler.twse as twse
import crawler.price as price
import crawler.cmoney as cmoney
import crawler.news as cnews
import crawler.fund as cFund
import crawler.cb as cb
import pandas as pd
from api import line
from bs4 import BeautifulSoup
from models import models

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from configparser import ConfigParser
from dateutil import parser
from datetime import datetime, timedelta
from xlsx import twse as xtwse, financial, export as csv, fund
from jinja2 import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# 月營收
MONTH_REVENUE = 'month_revenue'

# 資產負債表
BALANCE_SHEET = 'balance_sheet'

# 綜合損益表
CONSOLIDATED_INCOME_STATEMENT = 'consolidated_income_statement'

# 現金流量表
CASH_FLOW_STATEMENT = 'cash_flow_statement'

# 權益變動表
CHANGES_IN_EQUITY = 'changes_in_equity'

# 股利
DIVIDEND = 'dividend'

# 財報種類
FINANCIAL_TYPE = ['all', BALANCE_SHEET, CONSOLIDATED_INCOME_STATEMENT, CASH_FLOW_STATEMENT, CHANGES_IN_EQUITY]

# 財報合併種類
MERGE_TYPE = ['all', MONTH_REVENUE, BALANCE_SHEET, CONSOLIDATED_INCOME_STATEMENT, CASH_FLOW_STATEMENT,
              CHANGES_IN_EQUITY,
              DIVIDEND, 'price']

EXPONENT = ['TSE', 'OTC', 'TSE_INDUSTRY', 'OTC_INDUSTRY', 'XQ_INDUSTRY']

year = datetime.now().year
month = datetime.now().month

TIMEZONE = pytz.timezone('Asia/Taipei')

if month == 1:
    month = 12
else:
    month = month - 1

conf = ConfigParser()
conf.read('config.ini')

notifyApi = line.Notify({
    'cb': conf['line']['notify_cb_token'],
    'system': conf['line']['notify_system_token'],
    'news': conf['line']['notify_news_token'],
    'financial': conf['line']['notify_financial_token'],
    'fund': conf['line']['notify_fund_token'],
})


def log(msg):
    logging.info(msg)
    click.echo(msg)


def error(msg):
    logging.error(msg)
    click.echo(msg)


def _read_template(html):
    with open(os.path.join(f"./template/{html}")) as template:
        return template.read()


def render(html, **kwargs):
    return Template(
        _read_template(html)
    ).render(**kwargs)


def db(file=None):
    if file is not None:
        log(f"read config {file}")
        conf.read(file)

    db = conf['databases']
    return create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
                         encoding='utf8')


def weekCountOfmonth():
    now = datetime.now()
    c = 0
    for w in calendar.Calendar().monthdayscalendar(now.year, now.month):
        for i, d in enumerate(w):
            if i >= 5 or d == 0:
                continue
            c += 1
            if now.day == d:
                return c

    return 0


# 財報
def _get_financial(year, season, outpath, type):
    result = []
    if season == 0:
        seasons = [4, 3, 2, 1]
    else:
        seasons = [season]

    for season in seasons:
        log(f"read {type} {year}-{season}")
        for code in twse.season_codes(year, season):
            outPath = os.path.join(outpath, type)

            if os.path.exists(os.path.join(outPath, f"{year}Q{season}", f"{code}.csv")):
                continue

            def get(code, year, season):
                return []

            if type == BALANCE_SHEET:
                get = twse.balance_sheet
            elif type == CONSOLIDATED_INCOME_STATEMENT:
                get = twse.consolidated_income_statement
            elif type == CASH_FLOW_STATEMENT:
                get = twse.cash_flow_statement
            elif type == CHANGES_IN_EQUITY:
                get = twse.changes_in_equity

            d = {}
            while (True):
                try:
                    d = get(code, year, season)
                    break

                except requests.exceptions.ConnectionError as e:
                    logging.error(e.__str__())
                    logging.info("等待重新執行")
                    time.sleep(10)

                except Exception as e:
                    logging.error(e.__str__())
                    break

            if len(d) == 0:
                log(f"{type} {code} not found")
            else:
                i = 0
                for k, v in d.items():
                    dir = os.path.join(outPath, k)
                    f = os.path.join(dir, f"{code}.csv")

                    if os.path.exists(f) and i == 0:
                        continue

                    if os.path.exists(dir) == False:
                        os.mkdir(dir)

                    v.to_csv(f, index=False, encoding='utf_8_sig')

                    log(f"save {type} {code} {k}")
                    i += 1

                    result.append({'date': k, 'code': code, 'type': type})

            time.sleep(6)

    return result


def setEmail(title, text):
    content = MIMEMultipart()
    content["from"] = conf['smtp']['login_email']
    content["subject"] = title
    content["to"] = conf['smtp']['email']
    content.attach(MIMEText(text))

    with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:
        try:
            smtp.ehlo()  # 驗證SMTP伺服器
            smtp.starttls()  # 建立加密傳輸
            smtp.login(conf['smtp']['login_email'], conf['smtp']['password'])
            smtp.send_message(content)
            log(f"set {title} email ok")
        except Exception as e:
            error(f"set {title} email ok error {e.__str__()}")


@click.group()
def cli():
    dir = os.path.join(os.getcwd(), 'log')

    if os.path.exists(dir) == False:
        os.mkdir(dir)

    filename = os.path.join(dir, datetime.now().strftime(f"%Y-%m-%d-cli.log"))
    log = logging.getLogger()

    for hdlr in log.handlers[:]:
        log.removeHandler(hdlr)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=filename
                        )


# 月營收
@cli.command('month_revenue')
@click.option('-y', '--year', default=0, help="年")
@click.option('-m', '--month', default=0, help="月")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
@click.option('-s', '--save', default=False, type=click.BOOL, help="是否保存在database")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def month_revenue(year, month, outpath, save, config, notify):
    if month == 0:
        month = datetime.now().month

        if month == 1:
            month = 12
        else:
            month = month - 1

    if year == 0:
        year = datetime.now().year

        if datetime.now().month == 1:
            year = year - 1

    m = "%02d" % month

    log(f'read month_revenue {year}-{m}')

    try:
        data = twse.month_revenue(year, month)

        if data is None:
            error('not month_revenue')
            return

        dir = os.path.join(outpath, 'month_revenue', str(year))

        if os.path.exists(dir) == False:
            os.mkdir(dir)

        data.to_csv(os.path.join(dir, f"{year}-{m}.csv"), index=False, encoding='utf_8_sig')

        if save:
            financial.imports('month_revenue', year, month=month, dir=outpath, d=db(file=config))

        log(f"save month_revenue {year}-{m} csv")

        if notify:
            notifyApi.sendFinancial(f"收集 {m} 月營收完成")

    except Exception as e:
        error(f"month_revenue error: {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤-{year}-{m}月營收", f"{e.__str__()}")


# 財報
@cli.command('financial')
@click.option('-y', '--year', default=0, help="年")
@click.option('-q', '--season', default=0, help="季")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
@click.option('-t', '--type', default='all', type=click.Choice(FINANCIAL_TYPE, case_sensitive=False), help="財報類型")
@click.option('-s', '--save', default=False, type=click.BOOL, help="是否保存在database")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def get_financial(year, season, outpath, type, save, config, notify):
    result = []

    if year == 0:
        year = datetime.now().year

        if datetime.now().month < 4:
            year = year - 1

    try:
        if type == 'all':
            FINANCIAL_TYPE.remove('all')

            for t in FINANCIAL_TYPE:
                result += _get_financial(year, season, outpath, t)
        else:
            result = _get_financial(year, season, outpath, type)

        if save:
            for v in list(set([v['date'] for v in result])):
                financial.imports('financial', int(v[:4]), quarterly=int(v[5:6]), dir=outpath, d=db(file=config))

        if notify and len(result) > 0:
            s = ''
            for v in list(result):
                s += v.__str__() + '\n'

            if notify:
                notifyApi.sendFinancial(f"收集 {result[0]['date']} 季報完成")
    except Exception as e:
        error(f"季報 error: {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤-季報", f"{e.__str__()}")


# 股利
@cli.command('dividend')
@click.option('-y', '--year', default=year, help="年")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
@click.option('-s', '--save', default=False, type=click.BOOL, help="是否保存在database")
@click.option('-c', '--config', type=click.STRING, help="config")
def dividend(year, outpath, save, config):
    if year is None:
        year = datetime.now().year

    data = twse.dividend(year)

    if save:
        session = Session(db(config))
        stocks = {v.code: v.id for v in session.execute("SELECT id, code FROM stocks order by code").all()}
        dividend = {v.stock_id: v for v in
                    session.execute("SELECT id, stock_id, cash, stock FROM dividends WHERE year = :year",
                                    {'year': year}).all()
                    }

        deleteId = []
        insert = []
        for i, v in data[1:].iterrows():
            code = v['code']
            id = stocks[code]
            if id in dividend:
                if dividend[id].cash != float(v['現金股利']) or dividend[id].stock != float(v['股票股利']):
                    deleteId.append(dividend[id].id)
                else:
                    continue

            insert.append({
                'stock_id': id,
                'year': year,
                'cash': v['現金股利'],
                'stock': v['股票股利'],
            })

        if len(deleteId) > 0:
            result = session.execute("DELETE FROM dividends WHERE id IN :ids", {'ids': deleteId})
            if result.rowcount != len(deleteId):
                logging.error("delete dividends error")
                return

            logging.info(f"delete dividends year:{year} count:{len(deleteId)}")

        if len(insert) > 0:
            result = session.execute(models.dividend.insert(), insert)
            if result.rowcount != len(insert):
                logging.error("insert dividends error")
                return

            logging.info(f"insert dividends year:{year} count:{len(insert)}")

        session.commit()

    else:
        data.to_csv(os.path.join(os.path.join(outpath, "dividend"), f"{year}.csv"), index=False, encoding='utf_8_sig')
        log(f"save dividend {year}")


# 合併
@cli.command('merge')
@click.option('-t', '--type', default='all', type=click.Choice(MERGE_TYPE, case_sensitive=False), help="合併類型")
@click.option('-i', '--input', type=click.Path(), help="輸入路徑")
@click.option('-o', '--out', type=click.Path(), help="輸出路徑")
@click.option('-y', '--year', default=0, help="年")
def merge(type, input, out, year):
    def m(type):
        if type == BALANCE_SHEET:
            log("合併 資產負債表...")
            xtwse.balance_sheet(os.path.join(input, type), out)
            log("資產負債表 合併完成")

        if type == CONSOLIDATED_INCOME_STATEMENT:
            log("合併 綜合損益表...")
            xtwse.consolidated_income_statement(os.path.join(input, type), out)
            log("綜合損益表 合併完成")

        if type == CASH_FLOW_STATEMENT:
            log("合併 現金流量表...")
            xtwse.cash_flow_statement(os.path.join(input, type), out)
            log("現金流量表 合併完成")

        if type == CHANGES_IN_EQUITY:
            log("合併 權益變動表...")
            xtwse.changes_inEquity(os.path.join(input, type), out)
            log("權益變動表 合併完成")

        if type == MONTH_REVENUE:
            log("合併 月營收...")
            xtwse.month_revenue(os.path.join(input, type), out)
            log("月營收 合併完成")

        if type == DIVIDEND:
            log("合併 股利...")
            xtwse.dividend(os.path.join(input, type), out)
            log("股利 合併完成")

    if type == 'all':
        MERGE_TYPE.remove('all')

        for t in MERGE_TYPE:
            m(t)
    elif type == 'price':
        csv.priceMerge(input, year=year)
    else:
        m(type)


# 合併財報
@cli.command('merge_financial')
@click.option('-i', '--input', type=click.Path(), help="輸入路徑")
def merge_financial(input):
    log("合併 財報...")

    data = {}
    f = os.path.join(input, "財報.xlsx")

    for p in glob.glob(os.path.join(input, "*.csv")):
        name = os.path.basename(p).split('.')[0]
        log(f"read {name}...")

        data[name] = pd.read_csv(p)

    log("開始合併財報")

    with pd.ExcelWriter(f) as writer:
        for k, v in data.items():
            log(f"save {k}...")
            v.to_excel(writer, sheet_name=k, index=False)

    log("合併財報完成")


# 股價
@cli.command('price')
@click.option('-t', '--type', default='all', type=click.Choice(EXPONENT, case_sensitive=False), help="指數類型")
@click.option('-p', '--path', type=click.Path(), help="檔案輸入路徑")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def price(type, path, config, notify):
    d = db(file=config)

    if type in 'TSE':
        twse.twse(d)
    elif type in 'OTC':
        twse.otc(d)
    elif (type == 'TSE_INDUSTRY'):
        twse.tse_industry(d)
    elif (type == 'OTC_INDUSTRY'):
        twse.otc_industry(d)
    elif (type == 'XQ_INDUSTRY'):
        twse.xq_industry(path, d)

    if notify:
        notifyApi.sendSystem(f"執行完 {type} 股價")


# 籌碼
@cli.command('bargaining_chip')
@click.option('-d', '--date', type=click.STRING, help="date")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def bargaining_chip(date, config, notify):
    try:
        session = Session(db(file=config))

        if date is None:
            date = session.execute("SELECT date FROM prices ORDER BY date desc limit 1").first().date
        else:
            date = datetime.fromtimestamp(parser.parse(date).timestamp(), tz=TIMEZONE)

        stock = {v.code: v.id for v in session.execute("SELECT id, code FROM stocks").all()}
        update = []

        for v in twse.credit_transaction(date.year, date.month, date.day):
            if v['code'] not in stock:
                continue

            if v['securities_lending_repay'] > 0:
                update.append(f"WHEN {stock[v['code']]} THEN {v['securities_lending_repay']}")

        if len(update) == 0:
            return

        result = session.execute(
            "UPDATE prices set securities_lending_repay = (CASE stock_id " + " ".join(
                update) + " ELSE securities_lending_repay END) WHERE date = :date",
            {'date': date.__str__()}
        )

        if result.rowcount != 1:
            log(f"update bargaining_chip {date} count:{len(update)}")
            session.commit()

            if notify == True:
                notifyApi.sendSystem('執行收集籌碼')
        else:
            error(f"update bargaining_chip {date} count:{len(update)}")

    except Exception as e:
        error(f"update bargaining_chip {date} error {e.__str__()}")

        if notify == True:
            setEmail(f"系統通知錯誤 {date} 籌碼", f"{e.__str__()}")


# tag產業指數
@cli.command('tag_exponent')
@click.option('-t', '--code', multiple=True, type=click.STRING, help="code")
@click.option('-r', '--restart', default=False, type=click.BOOL, help="重置")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def tag_exponent(code, restart, config, notify):
    session = Session(db(file=config))
    tags = []

    if len(code) == 0:
        for v in session.execute(
                "SELECT stock_id, tag_id, tags.name FROM tag_exponents JOIN tags ON tags.id = tag_exponents.tag_id"
        ).all():
            tags.append(v)
    else:
        rows = session.execute(
            "SELECT stock_id, tag_id, tags.name FROM tag_exponents JOIN tags ON tags.id = tag_exponents.tag_id JOIN stocks ON stocks.id = tag_exponents.stock_id WHERE stocks.code IN :code",
            {'code': code}).all()

        if rows is None:
            return

        for row in rows:
            tags.append(row)

    try:
        for stock in tags:
            if restart:
                session.execute("DELETE FROM prices WHERE stock_id = :id", {'id': stock.stock_id})

            exponent = session.execute(
                "SELECT * FROM prices where stock_id = :id ORDER BY date DESC LIMIT 1",
                {'id': stock.stock_id}
            ).first()

            rows = session.execute("SELECT stock_id FROM stock_tags WHERE tag_id = :tag", {
                'tag': stock.tag_id,
            }).all()

            if len(rows) == 0:
                continue

            if exponent is None:
                date = '2013-01-01'
            else:
                date = exponent.date.__str__()

            prices = session.execute(
                "SELECT stock_id, open, close, high, low, increase, value, date FROM prices "
                "WHERE stock_id IN :stock AND date >= :date "
                "ORDER BY date",
                {'stock': [a.stock_id for a in rows], 'date': date}).all()

            data = {}
            for v in prices:
                if v.stock_id not in data:
                    data[v.stock_id] = []
                data[v.stock_id].append(v)

            dates = {}
            for id, values in data.items():
                for i, v in enumerate(values):
                    date = v.date.__str__()
                    if date not in dates:
                        dates[date] = []

                    if i == 0:
                        dates[date].append({
                            'open': 0,
                            'close': 0,
                            'high': 0,
                            'low': 0,
                            'value': v.value,
                        })
                        continue

                    yClose = values[i - 1].close
                    open = round(((v.open / yClose) - 1) * 100, 2)

                    # 可能因為減資 增資 換股等股本事件造成股價出現大幅度跳或跌價 此時就跳過
                    if open > 10 or open < -10:
                        continue

                    dates[date].append({
                        'open': open,
                        'close': v.increase,
                        'high': round(((v.high / v.open) - 1) * 100, 2),
                        'low': round(((v.low / v.open) - 1) * 100, 2),
                        'value': v.value,
                    })

            if len(dates) <= 1:
                continue

            tmpInsert = {}
            for date, value in dates.items():
                open = 0
                close = 0
                high = 0
                low = 0
                volume = 0

                for v in value:
                    open += v['open']
                    close += v['close']
                    high += v['high']
                    low += v['low']
                    volume += v['value']

                l = len(value)

                if l == 0:
                    continue

                tmpInsert[date] = {
                    'stock_id': stock.stock_id,
                    'date': date,
                    'open': round(open / l, 2),
                    'close': round(close / l, 2),
                    'high': round(high / l, 2),
                    'low': round(low / l, 2),
                    'volume': volume,
                }

            insert = [tmpInsert[key] for key in sorted(tmpInsert.keys())]
            for i in range(len(insert)):
                if i == 0:
                    if exponent is None:
                        insert[i]['open'] = 100
                        insert[i]['close'] = 100
                        insert[i]['high'] = 0
                        insert[i]['low'] = 0
                        insert[i]['increase'] = 0
                    elif exponent.date.__str__() == insert[i]['date']:
                        insert[i]['open'] = exponent.open
                        insert[i]['close'] = exponent.close
                        insert[i]['high'] = exponent.high
                        insert[i]['low'] = exponent.low
                        insert[i]['increase'] = exponent.increase
                    else:
                        return
                else:
                    y = insert[i - 1]
                    insert[i]['open'] = round(y['close'] * (1 + insert[i]['open'] / 100), 2)
                    insert[i]['close'] = round(y['close'] * (1 + insert[i]['close'] / 100), 2)
                    insert[i]['high'] = round(insert[i]['open'] * (1 + insert[i]['high'] / 100), 2)
                    insert[i]['low'] = round(insert[i]['open'] * (1 + insert[i]['low'] / 100), 2)
                    insert[i]['increase'] = round(((insert[i]['close'] / y['close']) - 1) * 100, 2)

            if len(insert) > 0:
                if exponent is not None:
                    del insert[0]

                result = session.execute(models.price.insert(), insert)
                if result.is_insert == False or result.rowcount != len(insert):
                    logging.info("insert exponent tag error")
                else:
                    logging.info(
                        f"save exponent tag:{stock.tag_id} name:{stock.name} stock:{stock.stock_id} count:{len(insert)}")
                    session.commit()

        notifyApi.sendSystem('執行產業指數')

    except Exception as e:
        logging.error(
            f"tag exponent error: {e.__str__()}"
        )

        if notify:
            setEmail("系統錯誤-產業指數", {e.__str__()})


# 面板報價
@cli.command('wits_view')
@click.option('-o', '--out', type=click.Path(), help="輸出路徑")
def wits_view(out):
    out = os.path.join(out, 'wits_view')
    data = price.wits_view()

    for name, item in data.items():
        for date, table in item.items():
            dir = os.path.join(out, name)

            if os.path.exists(dir) == False:
                os.makedirs(dir)

            table.to_csv(os.path.join(dir, f"{date}.csv"), index=False, encoding='utf_8_sig')

            name = name.encode("utf8").decode("cp950", "ignore")
            log(f"save {name} {date}")


# SP 500
@cli.command('sp500')
@click.option('-c', '--code', type=click.STRING, help="代碼")
@click.option('-o', '--out', type=click.Path(), help="輸出路徑")
def sp500(code, out):
    for date, table in cmoney.sp500(code).items():
        dir = os.path.join(out, 'sp500', code)

        if os.path.exists(dir) == False:
            os.makedirs(dir)

        table.to_csv(os.path.join(dir, f"{date}.csv"), index=False, encoding='utf_8_sig')

        log(f"save {code} {date}")


# 投信公會持股明細
@cli.command('fund')
@click.option('-y', '--year', type=click.INT, help="年")
@click.option('-m', '--month', type=click.INT, help="月")
@click.option('-i', '--id', type=click.STRING, help="卷商id")
@click.option('-o', '--out', type=click.Path(), help="輸出")
@click.option('-s', '--save', default=False, type=click.BOOL, help="保存")
@click.option('-c', '--config', type=click.STRING, help="config")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def get_fund(year, month, id, out, save, config, notify):
    if year is None:
        year = datetime.now().year

    if month == 0:
        month = None
    elif month is None:
        month = datetime.now().month - 1
        if month == 0:
            year -= 1
            month = 12

        m = "%02d" % month
        if os.path.exists(os.path.join(out, f"{year}{m}") + ".csv"):
            return

    now = datetime.now()
    if now.year == year and (now.month - 1) == month:
        c = weekCountOfmonth()
        if c < 10 or c > 20:
            return

    isSave = False

    try:
        funds = cFund.get(year=year, month=month, id=id)

        for ym, rows in funds.items():
            data = []
            f = os.path.join(out, str(ym)) + ".csv"

            if os.path.exists(f):
                continue

            for v in rows:
                for value in v['data']:
                    for code in value['data']:
                        data.append([v['name'], v['code'], value['name']] + list(code.values()))

            pd.DataFrame(
                data,
                columns=['c_name', 'c_code', 'f_name', 'code', 'name', 'amount', 'total', 'type']
            ).to_csv(f, index=False, encoding='utf_8_sig')

            isSave = True

            if save:
                log(f'save fund {ym[:4]}-{ym[4:]}')
                fund.imports(int(ym[:4]), int(ym[4:]), out, db(file=config))

        if isSave and notify:
            notifyApi.sendFund(f"執行收集 {year}-{month} 投信持股明細")

        detail = cFund.detail(year, month)
        for ym, rows in detail.items():
            f = os.path.join(out, str(ym), 'detail') + ".csv"

            if os.path.exists(f) or len(rows) == 0:
                continue

            pd.DataFrame(
                [list(v.values()) for v in rows],
                columns=list(rows[0].keys())
            ).to_csv(f, index=False, encoding='utf_8_sig')

            isSave = True

        if save and len(detail) > 0:
            log('save fund detail')
            fund.save_detail(detail, db(file=config))

        if isSave and notify:
            notifyApi.sendFund(f"執行收集 {year}-{month} 投信明細")

    except Exception as e:
        error(f"fund error {e.__str__()}")

        if isSave and notify:
            setEmail(f"系統通知錯誤 投信持股明細", f"{e.__str__()}")


# 新聞
@cli.command('news')
@click.option('-h', '--hours', type=click.INT, help="小時")
@click.option('-s', '--save', type=click.BOOL, help="是否保存在db")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def news(hours, save=False, notify=False):
    try:
        tz = pytz.timezone('Asia/Taipei')
        now = datetime.now(tz)
        date = (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

        log(f"start news {date}")

        data = [
            ['聯合報-產經', cnews.udn('6644', date)],
            ['聯合報-股市', cnews.udn('6645', date)],

            ['中時', cnews.chinatimes(date)],

            ['科技新報', cnews.technews(date)],

            ['經濟日報-產業熱點', cnews.money_udn('5591', '5612', date)],
            ['經濟日報-科技產業', cnews.money_udn('5591', '11162', date)],
            ['經濟日報-今晨必讀', cnews.money_udn('10846', '12926', date)],
            ['經濟日報-總經趨勢', cnews.money_udn('10846', '10869', date)],
            ['經濟日報-經濟周報', cnews.money_udn('10846', '122335', date)],
            ['經濟日報-市場焦點', cnews.money_udn('5590', '5607', date)],
            ['經濟日報-集中市場', cnews.money_udn('5590', '5710', date)],
            ['經濟日報-櫃買市場', cnews.money_udn('5590', '11074', date)],
            ['經濟日報-證券達人', cnews.money_udn('5590', '8543', date)],
            ['經濟日報-外媒解析', cnews.money_udn('5588', '122381', date)],
            ['經濟日報-國際焦點', cnews.money_udn('5588', '5599', date)],
            ['經濟日報-美中貿易戰', cnews.money_udn('5588', '10511', date)],
            ['經濟日報-大陸政經', cnews.money_udn('5589', '5603', date)],
            ['經濟日報-金融脈動', cnews.money_udn('12017', '5613', date)],
            ['經濟日報-外匯市場', cnews.money_udn('12017', '5616', date)],
            ['經濟日報-國際期貨', cnews.money_udn('11111', '11114', date)],

            ['工商時報-財經', cnews.ctee(date, 'aj')],
            ['工商時報-科技', cnews.ctee(date, 'kj')],
            ['工商時報-國際', cnews.ctee(date, 'gj')],
            ['工商時報-兩岸', cnews.ctee(date, 'lm')],

            ['鉅亨網-台股', cnews.cnyes(date, 'tw_stock')],
            ['鉅亨網-國際股', cnews.cnyes(date, 'wd_stock')],
            ['鉅亨網-期貨', cnews.cnyes(date, 'future')],

            ['自由時報-國際財經', cnews.ltn(date, 'international')],
            ['自由時報-證券產業', cnews.ltn(date, 'securities')],

            ['moneydj-頭條新聞', cnews.moneydj(date, 'mb010000')],
            ['moneydj-總體經濟', cnews.moneydj(date, 'mb020000')],
            ['moneydj-債券市場', cnews.moneydj(date, 'mb040200')],
            ['moneydj-科技脈動', cnews.moneydj(date, 'mb070100')],
            ['moneydj-產業情報', cnews.moneydj(date, 'mb07')],

            ['trendforce', cnews.trendforce(date)],

            ['dramx', cnews.dramx(date)],
        ]

        log('get news ok')

        if save:
            db = conf['databases']
            engine = create_engine(
                f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
                encoding='utf8')
            source = {}
            for v in engine.execute(models.source.select()).all():
                source[v['name']] = v['id']

            insert = []

            for item in data:
                if item[0] not in source or len(item[1]) == 0:
                    continue

                for v in item[1]:
                    insert.append({
                        'source_id': source[item[0]],
                        'title': v['title'],
                        'url': v['url'],
                        'publish_time': v['date']
                    })

            session = Session(engine)
            result = session.execute(models.news.insert(), insert)

            if result.is_insert == False or result.rowcount != len(insert):
                error('insert new error ' + date.__str__())

                if notify:
                    setEmail(f"系統通知錯誤 新聞", f"{date.__str__()}")
            else:
                session.commit()

                if notify:
                    notifyApi.sendSystem(f"執行收集 新聞")

    except Exception as e:
        error(f"new error {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤 新聞", f"{e.__str__()}")


# 即時重大公告
@cli.command('line-news')
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
def lineNews(notify):
    d = db()

    log('line news')

    try:
        keys = {v.name: v.ks.split(',') for v in d.execute('SELECT name, `keys` as ks FROM news_key_words').all()}

        data = [
            twse.news(keys)
        ]

        for news in data:
            for v in news:
                notifyApi.sendNews(v['body'])

    except Exception as e:
        error(f"line-news error {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤 Line新聞通知", f"{e.__str__()}")


# 新聞
@cli.command('news-context')
def news_context():
    log('start news context')

    db = conf['databases']
    engine = create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
                           encoding='utf8')
    source = {}
    for v in engine.execute(models.source.select()).all():
        source[v['id']] = v['name']

    for v in engine.execute(models.news.select().where(models.news.c.context == None).limit(500)).all():
        name = source[v['source_id']].split('-')

        context = None
        if name[0] == '聯合報':
            context = cnews.udn_context(v['url'])
        elif name[0] == '中時':
            context = cnews.chinatimes_context(v['url'])
        elif name[0] == '科技新報':
            context = cnews.technews_context(v['url'])
        elif name[0] == '經濟日報':
            context = cnews.money_udn_context(v['url'])
        elif name[0] == '工商時報':
            context = cnews.ctee_context(v['url'])
        elif name[0] == '鉅亨網':
            context = cnews.cnyes_context(v['url'])
        elif name[0] == '自由時報':
            context = cnews.ltn_context(v['url'])
        elif name[0] == 'moneydj':
            context = cnews.moneydj_context(v['url'])
        elif name[0] == '東森新聞':
            context = cnews.ebc_context(v['url'])
        elif name[0] == 'trendforce':
            context = cnews.trendforce_context(v['url'])
        elif name[0] == 'dramx':
            context = cnews.dramx_context(v['url'])
        elif name[0] == 'digitimes':
            context = cnews.digitimes_context(v['url'])

        if context is not None:
            result = engine.execute(models.news.update().where(models.news.c.id == v['id']).values(context=context))

            if result.rowcount != 1:
                error(f"update error id:{v['id']}")
            else:
                log(f"get content {v['id']} {v['title']}")

            time.sleep(2)


# 新聞email匯入
@cli.command('news-email-import')
@click.option('-i', '--input', type=click.Path(), help="輸入路徑")
def new_email_import(input):
    db = conf['databases']
    engine = create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
                           encoding='utf8')

    insert = []
    source = {}
    for v in engine.execute(models.source.select()).all():
        source[v['name']] = v['id']

    for path in glob.glob(f"{input}/*.eml"):
        ep = eml_parser.EmlParser(include_raw_body=True)
        email = ep.decode_email(path)
        source_id = 0
        publish_time = email['header']['subject'][5:-5]

        for v in BeautifulSoup(email.get('body')[0]['content'], 'html.parser').findAll('td'):
            name = v.text.strip().split('(')
            if name[0].split('-')[0].strip() in ['聯合報', '中時', '科技新報', '經濟日報', '工商時報', '鉅亨網', '自由時報', 'moneydj', '東森新聞',
                                                 'trendforce', 'dramx', 'digitimes']:
                nname = v.text.strip().split('(')
                source_id = source[nname[0].strip()]
                continue

            if source_id == 0 or v.text.strip() == '':
                continue

            if name[0].split('-')[0].strip() in ['蘋果', '證交所']:
                source_id = 0
                continue

            if v.find('a') is not None:
                title = v.text.strip()
                insert.append({
                    'source_id': source_id,
                    'title': title[:title.find('(2021')].strip(),
                    'url': v.find('a').attrs['href'],
                    'publish_time': publish_time,
                })
    result = engine.execute(models.news.insert(), insert)
    if result.is_insert == False or result.rowcount != len(insert):
        error('insert error')
    else:
        log(f"save {len(insert)} count")


# 匯入資料到db
@cli.command('import-to-database')
@click.option('-t', '--type', type=click.STRING, help="類型")
@click.option('-i', '--path', type=click.Path(), help="輸入檔案路徑")
@click.option('-d', '--dir', type=click.Path(), help="輸入檔案目錄")
@click.option('-y', '--year', type=click.INT, help="年")
@click.option('-m', '--month', type=click.INT, help="月")
@click.option('-q', '--quarterly', type=click.INT, help="季")
@click.option('-c', '--config', type=click.STRING, help="config")
def imports(type, path, dir, year, month, quarterly, config):
    d = db(file=config)

    if year is None:
        year = datetime.now().year

    if type in [BALANCE_SHEET, CONSOLIDATED_INCOME_STATEMENT, CASH_FLOW_STATEMENT, CHANGES_IN_EQUITY, MONTH_REVENUE,
                'financial']:
        financial.imports(type, year, quarterly=quarterly, month=month, dir=dir, d=d)
    elif type == 'fund':
        fund.imports(year, month, dir, d)
    elif type == 'exponent':
        xtwse.exponent(dir, d)
    elif type == 'price':
        xtwse.price(dir, d, batch=True)
    elif type == 'day_trades':
        xtwse.price(dir, d)


# 匯出
@cli.command('export')
@click.option('-t', '--type', type=click.STRING, help="類型")
@click.option('-o', '--out', type=click.Path(), help="輸出")
@click.option('-d', '--date', type=click.STRING, help="日期")
@click.option('-c', '--config', type=click.STRING, help="config")
def export(type, out, date, config):
    d = db(file=config)

    if (type == 'tse_industry'):
        csv.tse_industry(date, out, d)
    elif (type == 'price'):
        csv.price(date, out, d)

    # cb價格分位數
    elif (type == 'cb_price_quartile'):
        data = []
        session = Session(d)
        s = (
            "SELECT * FROM ( "
            "SELECT close, @curRow:=@curRow + 1 AS number FROM cb_prices WHERE cb_id = :id ORDER BY close DESC "
            ") as p "
            "WHERE p.number IN (1 , ROUND(@curRow / 2), ROUND(@curRow / 4));"
        )

        for v in session.execute("SELECT id, code, name, start_date FROM cbs").all():
            session.execute("SET @curRow = 0;")
            close = [v.close for v in session.execute(s, {'id': v.id}).all()]

            if len(close) == 3:
                data.append([v.code, v.name, v.start_date] + close)

        pd.DataFrame(data, columns=['code', 'name', 'start_date', 'max', 'sec', 'avg']).to_csv(
            os.path.join(out, 'cb_price_quartile.csv'),
            index=False,
            encoding='utf_8_sig')

    # cb漲幅分位數
    elif (type == 'cb_increase_quantile'):
        data = []
        session = Session(d)
        s = (
            "SELECT * FROM ( "
            "SELECT increase, @curRow:=@curRow + 1 AS number FROM cb_prices WHERE cb_id = :id ORDER BY increase DESC "
            ") as p "
            "WHERE p.number IN (1 , ROUND(@curRow / 2), ROUND(@curRow / 4), ROUND(@curRow / 10));"
        )

        for v in session.execute("SELECT id, code, name, start_date FROM cbs").all():
            session.execute("SET @curRow = 0;")
            increase = [v.increase for v in session.execute(s, {'id': v.id}).all()]

            if len(increase) == 4:
                data.append([v.code, v.name, v.start_date] + increase)

        pd.DataFrame(data, columns=['increase', 'name', 'start_date', '1', '0.9', '0.75', '0.5']).to_csv(
            os.path.join(out, 'cb_increase_quantile.csv'),
            index=False,
            encoding='utf_8_sig')

    # cbas價格分位數
    elif (type == 'cbas_quartile'):
        data = []
        session = Session(d)
        s = (
            "SELECT * FROM ( "
            "SELECT close, @curRow:=@curRow + 1 AS number FROM cb_prices WHERE cb_id = :id ORDER BY date "
            ") as p "
            "WHERE p.number = 6;"
        )

        for v in session.execute("SELECT id, code, name, start_date FROM cbs").all():
            session.execute("SET @curRow = 0;")
            d = session.execute(s, {'id': v.id}).first()

            if d is not None:
                data.append([v.code, v.name, v.start_date, d.close])

        pd.DataFrame(data, columns=['code', 'name', 'start_date', 'close']).to_csv(
            os.path.join(out, 'cbas_quartile.csv'),
            index=False,
            encoding='utf_8_sig')

    # cb餘額發行後第幾個月開始減少到一定趴數
    elif (type == 'cb_balances'):
        data = []
        session = Session(d)
        s = (
            "SELECT GROUP_CONCAT(b_rate) as rate FROM ( "
            "SELECT cb_balances.year,cb_balances.month,cb_balances.balance, "
            "ROUND((cb_balances.balance / (cbs.publish_total_amount / 100000)) * 100) AS b_rate "
            "FROM cb_balances JOIN cbs ON cb_balances.cb_id = cbs.id WHERE cb_id = :id "
            "ORDER BY cb_balances.year , cb_balances.month) as b;"
        )

        for v in session.execute("SELECT id, code, name, start_date FROM cbs").all():
            d = session.execute(s, {'id': v.id}).first()

            if d is not None and d.rate is not None:
                b90 = 0
                b50 = 0

                for i, b in enumerate(d.rate.split(',')):
                    b = int(b)
                    # 第一次剩餘90%以下
                    if b90 == 0 and b <= 90:
                        b90 = i

                    # 第一次剩餘50%以下
                    if b50 == 0 and b <= 50:
                        b50 = i

                data.append([v.code, v.name, v.start_date, b90, b50])

        pd.DataFrame(data, columns=['code', 'name', 'start_date', 'b90', 'b50']).to_csv(
            os.path.join(out, 'cb_balances.csv'),
            index=False,
            encoding='utf_8_sig')

    # cb市價區間折溢價分位數
    elif (type == 'cb_price_premium_quantile'):
        data = {
            'cb_close': [],
            'off_price': [],
        }

        rang = [
            [1000, 140], [140, 130], [130, 120], [120, 115], [115, 110], [110, 105],
            [105, 100], [100, 95], [95, 90], [90, 80], [80, 0]
        ]

        columns = ['code', 'name', 'start_date']
        for r in rang:
            columns += [f"1_{r[0]}~{r[1]}", f"0.90_{r[0]}~{r[1]}", f"0.75_{r[0]}~{r[1]}", f"0.50_{r[0]}~{r[1]}"]

        for v in d.execute("SELECT id, code, name, start_date FROM cbs order by start_date").all():
            for name, quantile in cb.priceQuantile(v.code, rang).items():
                c = [v.code, v.name, v.start_date]
                for qs in quantile:
                    c += qs

                data[name].append(c)

        for name, value in data.items():
            pd.DataFrame(value, columns=columns).to_csv(
                os.path.join(out, f'{name}_premium_quantile.csv'),
                index=False,
                encoding='utf_8_sig'
            )


# 可轉債
@cli.command('cb')
@click.option('-t', '--type', type=click.STRING, help="類型")
@click.option('-c', '--code', multiple=True, type=click.STRING, help="code")
@click.option('-y', '--year', type=click.INT, help="年")
@click.option('-m', '--month', type=click.INT, help="月")
@click.option('-s', '--start_ym', type=click.INT, help="開始年月")
@click.option('-e', '--end_ym', type=click.INT, help="結束年月")
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
@click.option('-c', '--config', type=click.STRING, help="config")
def cbs(type, code, year, month, start_ym, end_ym, notify, config):
    d = db(file=config)
    session = Session(d)

    def conversion_price(cbs):
        conversionPrices = []
        for code, id in cbs.items():
            conversionPrices.append({
                'code': code,
                'id': id,
                'prices': cb.conversionPrice(code),
            })

            time.sleep(6)

        for c in conversionPrices:
            insert = []
            code = c['code']
            id = c['id']
            prices = c['prices']

            time.sleep(1)

            value = session.execute("SELECT date FROM cb_conversion_prices WHERE cb_id = :id order by date desc",
                                    {'id': id}).all()
            diff = set([v['date'][0] for v in prices]).difference([v.date.__str__() for v in value])

            for p in prices:
                if p['date'][0] not in diff:
                    continue

                insert.append({
                    'cb_id': id,
                    'value': p['value'],
                    'stock': p['stock'],
                    'date': p['date'][0],
                    'type': p['type'],
                })

            if len(insert) == 0:
                logging.info(f"not save cb conversion price code: {code}")
                continue

            result = session.execute(models.cbConversionPrices.insert(), insert)
            if result.is_insert == False or result.rowcount != len(insert):
                logging.error(f"save cb conversion price code: {code} count: {len(insert)}")
                return False
            else:
                logging.info(f"save cb conversion price code: {code} count: {len(insert)}")

            result = session.execute(
                "update cbs set conversion_price = :price, conversion_stock = :stock where id = :id",
                {'price': prices[0]['value'], 'stock': prices[0]['stock'], 'id': id})

            if result.rowcount != 1:
                logging.error(f"update cb conversion price code: {code}")
            else:
                logging.info(f"update cb conversion price code: {code}")

        return True

    def balance(year, month, cbs):
        exist = {v.code: v.id for v in
                 session.execute(
                     "SELECT cbs.id, cbs.code FROM cb_balances join cbs on cb_balances.cb_id = cbs.id where year = :year and month = :month",
                     {'year': year, 'month': month}
                 ).all()}

        balance = cb.balance(year, month)
        balance2 = cb.stock()
        diff = set(balance.keys()).difference(exist.keys())
        insert = []

        for code in diff:
            if code not in cbs:
                continue

            value = balance[code]
            value2 = balance2[balance2['可轉換公司債代號'] == int(code)]
            people = 0
            m = "%02d" % month

            if len(value2) > 0 and str(value2.iloc[0]['資料年月']) == f'{year - 1911}{m}':
                people = value2.iloc[0]['集保股東戶數']

            insert.append({
                'cb_id': cbs[code],
                'year': year,
                'month': month,
                'change': value['change'],
                'balance': value['balance'],
                'change_stock': value['change_stock'],
                'balance_stock': value['balance_stock'],
                'people': people,
            })

        if len(insert) > 0:
            result = session.execute(models.cbBalance.insert(), insert)
            if result.is_insert == False or result.rowcount != len(insert):
                logging.error(f"save cb balance count: {len(insert)}")
                return False
            else:
                logging.info(f"save cb balance count: {len(insert)}")

        return True

    def price(year, month, cbs):
        exist = None
        if len(cbs) == 1:
            exist = {v.date.__str__(): True for v in session.execute("SELECT date FROM cb_prices where cb_id = :id",
                                                                     {'id': list(cbs.values())[0]}).all()}
        for date, prices in cb.price(year, month).items():
            insert = []

            if exist is None:
                exist = session.execute("SELECT * FROM cb_prices where date = :date limit 1", {'date': date}).first()

                if exist is not None:
                    exist = None
                    continue
            elif date in exist:
                continue

            for price in prices:
                if price['code'] not in cbs:
                    continue

                insert.append({
                    'cb_id': cbs[price['code']],
                    'year': year,
                    'month': month,
                    'date': price['date'],
                    'open': price['open'],
                    'close': price['close'],
                    'high': price['high'],
                    'low': price['low'],
                    'volume': price['volume'],
                    'increase': price['increase'],
                    'amplitude': price['amplitude'],
                    'amount': price['amount'],
                })

            if len(insert) == 0:
                continue

            result = session.execute(models.cbPrice.insert(), insert)
            if result.is_insert == False or result.rowcount != len(insert):
                logging.error(f"save cb price count: {len(insert)}")
                return False
            else:
                logging.info(f"save cb price count: {len(insert)}")

        return True

    try:
        # 近期發行可轉債
        if type == 'info':
            insert = []
            cbs = cb.new()
            stocks = {s.code: s.id for s in session.execute("SELECT id, code FROM stocks").all()}
            cbs = {c[0]: c for c in cbs}
            codes = [c.code for c in session.execute("SELECT code FROM cbs").all()]
            for code in set(cbs.keys()).difference(codes):
                c = cbs[code]
                info = cb.findByUrl(c[6])

                if info is None:
                    continue

                info['stock_id'] = stocks[code[:-1]]
                insert.append(info)
                logging.info(f"read cb info {code} {info['name']}")
                time.sleep(6)

            if len(insert) > 0:
                result = session.execute(models.cb.insert(), insert)
                if result.is_insert == False or result.rowcount != len(insert):
                    logging.error(f"save cb {len(insert)} count")
                    return False
                else:
                    logging.info(f"save cb {len(insert)} count")
                    session.commit()

        # 調整轉換價格
        if type == 'conversion_price':
            cbs = {c.code: c.id for c in
                   session.execute("SELECT id, code FROM cbs where start_date <= :date AND end_date >= :date",
                                   {
                                       'date': datetime.now().strftime("%Y-%m-%d"),
                                   }).all()}

            if conversion_price(cbs):
                session.commit()

        # 餘額
        if type == 'balance':
            if month is None:
                month = datetime.now().month - 1

            if year is None:
                year = datetime.now().year

            if month == 0:
                month = 12
                year = year - 1

            if balance(year, month, {v.code: v.id for v in session.execute("SELECT id, code FROM cbs").all()}):
                session.commit()

        # 價格
        if type == 'price':
            if year is None:
                year = datetime.now().year

            if month is None:
                month = datetime.now().month

            if price(year, month, {v.code: v.id for v in session.execute("SELECT id, code FROM cbs").all()}):
                session.commit()

        if type == 'get':
            my = start_ym.replace("-", "")

            info = cb.findByUrl(
                f"https://mops.twse.com.tw/mops/web/t120sg01?TYPEK=&bond_id={code}&bond_kind=5&bond_subn=%24M00000001&bond_yrn={code[-1]}&come=2&encodeURIComponent=1&firstin=ture&issuer_stock_code={code[:-1]}&monyr_reg={my}&pg=&step=0&tg="
            )

            if info is None:
                return

            info['stock_id'] = session.execute("SELECT id FROM stocks where code = :code",
                                               {'code': code[:-1]}).first().id
            result = session.execute(models.cb.insert(), [info])
            if result.is_insert == False or result.rowcount != 1:
                logging.error(f"save cb {code}")
                return False
            else:
                logging.info(f"save cb {code}")

            cbs = {code: result.inserted_primary_key[0]}

            if conversion_price(cbs) == False:
                return

            year = int(start_ym.split('-')[0])
            for i in range(int(end_ym.split('-')[0]) - year):
                for m in range(12):
                    if price(year + i, m + 1, cbs) == False:
                        return

                    time.sleep(3)

                    if balance(year + i, m + 1, cbs) == False:
                        return

            session.commit()

        if notify:
            notifyApi.sendSystem(f"執行收集可轉債 {type}")

    except Exception as e:
        error(f"cb {type} error {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤 {type} 可轉債", f"{e.__str__()}")


# 最近上市上櫃
@cli.command('stock')
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
@click.option('-c', '--config', type=click.STRING, help="config")
def stock(notify, config):
    d = db(file=config)
    session = Session(d)
    insert = []
    names = []

    try:
        for value in twse.ipo():
            stocks = session.execute("SELECT * FROM stocks where code = :code", {'code': value['code']}).first()

            if stocks is not None:
                continue

            classification = session.execute("SELECT * FROM classifications where name = :name",
                                             {'name': value['classification']}).first()

            if classification is None:
                continue

            market = 1
            if value['market'] == '上櫃':
                market = 2

            insert.append({
                'code': value['code'],
                'name': value['name'],
                'classification_id': classification.id,
                'market': market,
            })

            names.append(value['name'])

        if len(insert) == 0:
            return

        result = session.execute(models.stock.insert(), insert)
        if result.is_insert == False or result.rowcount != len(insert):
            logging.error(f"save stock count:{len(insert)}")
            return False
        else:
            logging.info(f"save stock count:{len(insert)}")
            session.commit()

            log(f'save stock {insert.__str__()}')

            if notify:
                s = ",".join(names)
                notifyApi.sendSystem(f"最近上市上櫃: {s}")

    except Exception as e:
        error(f"stock error {e.__str__()}")

        if notify:
            setEmail(f"系統通知錯誤 最近上市上櫃個股", f"{e.__str__()}")


# 股權分散表
@cli.command('stock_dispersion')
@click.option('-n', '--notify', default=False, type=click.BOOL, help="通知")
@click.option('-c', '--config', type=click.STRING, help="config")
def stockDispersion(notify, config):
    try:
        d = db(file=config)
        session = Session(d)
        codes = {v.code: v.id for v in session.execute("SELECT id, code from stocks")}
        insert = []

        data = twse.stock_dispersion()
        date = str(data.iloc[0]['資料日期'])
        date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"

        if session.execute("SELECT COUNT(1) as count FROM stock_dispersions WHERE date = :date",
                           {'date': date}).first().count > 0:
            return None

        for code, value in data.groupby('證券代號'):
            if code not in codes:
                continue

            for _, v in value.iterrows():
                i = v['持股分級']
                if i == 16:
                    continue

                if i == 17:
                    i = i - 1

                insert.append({
                    'stock_id': codes[code],
                    'date': date,
                    'level': i,
                    'people': v['人數'],
                    'stock': v['股數'],
                    'rate': v['占集保庫存數比例%'],
                })

        if len(insert) > 0:
            result = session.execute(models.stockDispersion.insert(), insert)
            if result.is_insert == False or result.rowcount != len(insert):
                logging.error(f"save stock dispersion count:{len(insert)}")
                return False
            else:
                logging.info(f"save stock dispersion count:{len(insert)}")
                session.commit()

                if notify == True:
                    notifyApi.sendSystem("執行完股權分散表")
    except Exception as e:
        error(f"stock dispersion error {e.__str__()}")

        if notify == True:
            setEmail(f"系統通知錯誤 股權分散表", f"{e.__str__()}")


# line通知
@cli.command('line')
@click.option('-c', '--config', type=click.STRING, help="config")
def line(config):
    date = datetime.now().strftime('%Y-%m-%d')
    d = db(file=config)
    session = Session(d)
    message = []

    log('start line')

    # 接近cb調整轉換價日期
    def a():
        conversionPrice = session.execute(
            (
                "SELECT cbs.code, cbs.name, cb_conversion_prices.date, cb_conversion_prices.value FROM cb_conversion_prices "
                "JOIN cbs on cbs.id = cb_conversion_prices.cb_id "
                "WHERE cb_conversion_prices.date = :date"),
            {'date': (datetime.now() + timedelta(days=7)).strftime(f"%Y-%m-%d")}
        ).all()

        for v in conversionPrice:
            message.append(f"\n代碼: {v.code}\n名稱: {v.name}\n調整日期: {v.date}\n調整轉換價: {v.value}")

    # cb上市第六天開始拆解
    def b():
        s = (
            "SELECT * FROM "
            "(SELECT cbs.id, COUNT(cb_prices.date) AS count "
            "FROM cbs JOIN cb_prices ON cbs.id = cb_prices.cb_id GROUP BY cbs.id) as t "
            "WHERE t.count = 6"
        )

        ids = [v.id for v in session.execute(s).all()]

        if len(ids) > 0:
            for v in session.execute((
                    "SELECT cbs.code,cbs.name, cb_prices.volume, cbs.publish_total_amount "
                    "FROM cbs JOIN cb_prices ON cbs.id = cb_prices.cb_id where cb_prices.date = :date and cb_prices.cb_id IN :id "
            ), {'date': date, 'id': ids}).all():
                message.append(
                    f"\n代碼: {v.code}\n名稱: {v.name}\n日期: {date}\n預估cbas拆解: {round((v.volume / (v.publish_total_amount / 100000)) * 100)}%"
                )

    # 突破轉換價
    def c():
        yData = session.execute(
            "SELECT date FROM prices GROUP BY date ORDER BY date DESC LIMIT 1 OFFSET 1").first().date.__str__()

        data = session.execute(
            "SELECT code, name, date, close FROM cbs JOIN prices ON cbs.stock_id = prices.stock_id WHERE prices.date IN :date order by date",
            {
                'date': [date, yData],
            }
        ).all()

        prices = {}
        for v in data:
            if v.code not in prices:
                prices[v.code] = []

            prices[v.code].append(v)

        conversionPrice = {v.code: v for v in session.execute(
            ("SELECT cbs.code, cbs.name, min(cb_conversion_prices.value) as value FROM cb_conversion_prices "
             "JOIN cbs on cbs.id = cb_conversion_prices.cb_id and cbs.end_date > :date "
             "WHERE cb_conversion_prices.date <= :date group by code"),
            {'date': date}
        ).all()}

        for code, values in prices.items():
            if len(values) != 2:
                continue

            if code not in conversionPrice:
                continue

            cp = conversionPrice[code].value
            if values[0].close < cp and values[1].close >= cp:
                message.append(
                    f'\n代碼: {code}\n名稱: {values[0].name}\n日期: {date}\n收盤價: {values[1].close}\n突破轉換價: {cp}'
                )

    # 今天上市
    def d():
        for v in session.execute("SELECT code,name FROM cbs where :date = start_date", {'date': date}).all():
            message.append(
                f'\n代碼: {v.code}\n名稱: {v.name}\n日期: {date}\n今天上市'
            )

    # 開始轉換
    def e():
        for v in session.execute(
                "SELECT code, name FROM cbs where :now >= start_conversion_date and :date < start_conversion_date",
                {'now': date, 'date': (datetime.now() - timedelta(days=1)).strftime(f"%Y-%m-%d")}).all():
            message.append(
                f'\n代碼: {v.code}\n名稱: {v.name}\n日期: {date}\n今天開始轉換'
            )

    # 上市一個月後
    def f():
        s = (
            "SELECT * FROM "
            "(SELECT cbs.code, cbs.name, TO_DAYS(:now) - TO_DAYS(start_date) AS b_day, TO_DAYS(:date) - TO_DAYS(start_date) AS b1_day "
            "FROM cbs WHERE :now >= start_date AND :now < start_conversion_date) as t "
            "WHERE t.b_day >= 30 and t.b1_day < 30"
        )

        for v in session.execute(s, {'now': date,
                                     'date': (datetime.now() - timedelta(days=1)).strftime(f"%Y-%m-%d")}).all():
            message.append(
                f'\n代碼: {v.code}\n名稱: {v.name}\n日期: {date}\n上市一個月'
            )

    # 明天cbas開始拆解
    def g():
        s = (
            "SELECT * FROM "
            "(SELECT cbs.code, cbs.name, COUNT(cb_prices.date) AS count, cb_prices.date, cb_prices.volume, cbs.publish_total_amount "
            "FROM cbs JOIN cb_prices ON cbs.id = cb_prices.cb_id GROUP BY cbs.id) as t "
            "WHERE t.count = 5"
        )

        for v in session.execute(s).all():
            message.append(
                f"\n代碼: {v.code}\n名稱: {v.name}\n日期: {v.date}\n明天cbas開始拆解"
            )

    # 週轉換
    def h():
        now = datetime.now()
        week5Day = (now + timedelta(days=4 - now.weekday()))
        yWeek5Day = (week5Day - timedelta(days=7))

        if date != week5Day.strftime('%Y-%m-%d'):
            return

        s = (
            "SELECT cbs.code, cbs.name, GROUP_CONCAT(stock order by date desc) as stock "
            "FROM stock_dispersions JOIN cbs ON cbs.stock_id = stock_dispersions.stock_id "
            "WHERE stock_dispersions.date IN :dates "
            "AND stock_dispersions.level = 16 "
            "AND cbs.start_date <= :date "
            "AND cbs.end_date >= :date "
            "GROUP BY cbs.id ORDER BY cbs.code"
        )

        for v in session.execute(s, {'dates': [week5Day.strftime('%Y-%m-%d'), yWeek5Day.strftime('%Y-%m-%d')],
                                     'date': date}).all():
            stock = v.stock.split(',')
            if len(stock) != 2:
                continue

            if int(stock[0]) > int(stock[1]):
                message.append(
                    f"\n代碼: {v.code}\n名稱: {v.name}\n日期: {date}\n集保戶庫數增加: {round((int(stock[0]) - int(stock[1])) / 1000)}張"
                )

    a()
    b()
    c()
    d()
    e()
    f()
    g()
    h()

    for m in message:
        notifyApi.sendCb(m)


@cli.command('google_new')
@click.option('-k', '--key', type=click.STRING, help="key word")
@click.option('-u', '--url', type=click.STRING, help="url")
@click.option('-n', '--num', default=30, type=click.INT, help="url")
@click.option('-s', '--save', type=click.STRING, help="save path")
def google_new(key, url, num, save):
    news = asyncio.get_event_loop().run_until_complete(cnews.google_news(key, url, num=num))
    data = pd.DataFrame([[v['title'], v['date'], v['url']] for v in news], columns=['title', 'date', 'url'])
    data['date'] = pd.to_datetime(data.date)
    data = data.sort_values(by='date', ascending=False)

    if save is not None:
        td = []
        for i, v in data.iterrows():
            td.append((
                    f"<tr><td>{v['date']}</td>"
                    '<td><a href="' + v['url'] + '" target="_blank">' + v['title'] + '</a></td></tr>'
            ))

        html = ('<table border="1" class="dataframe">'
                '<thead>'
                '<tr style="text-align: right;">'
                "<th width='20%'>title</th>"
                "<th width='80%'>date</th>"
                '</tr>'
                '</thead>'
                '<tbody>'
                + "".join(td) +
                '</tbody>'
                '</table>')

        f = open(os.path.join(save, f'{key}-news.html'), "w", encoding="utf-8")
        f.write(html)
        f.close()
    else:
        print(news)


if __name__ == '__main__':
    cli()
