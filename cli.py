import click
import os
import time
import glob
import logging
import smtplib
import pymysql
import eml_parser
import requests
import crawler.twse as twse
import crawler.price as price
import crawler.cmoney as cmoney
import crawler.news as cnews
import crawler.fund as fund
import pandas as pd
from bs4 import BeautifulSoup
from models import models
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from configparser import ConfigParser
from datetime import datetime, timedelta
from xlsx import twse as xtwse, financial
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

# 合併種類
MERGE_TYPE = ['all', MONTH_REVENUE, BALANCE_SHEET, CONSOLIDATED_INCOME_STATEMENT, CASH_FLOW_STATEMENT,
              CHANGES_IN_EQUITY,
              DIVIDEND]

year = datetime.now().year
month = datetime.now().month

if month == 1:
    month = 12
else:
    month = month - 1

conf = ConfigParser()
conf.read('config.ini')


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


def db():
    db = conf['databases']
    return create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
                         encoding='utf8')


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
                        filename=filename)


# 月營收
@cli.command('month_revenue')
@click.option('-y', '--year', default=0, help="年")
@click.option('-m', '--month', default=0, help="月")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
def month_revenue(year, month, outpath):
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

    dir = os.path.join(outpath, 'month_revenue', str(year))

    if os.path.exists(dir) == False:
        os.mkdir(dir)

    log(f'read month_revenue {year}-{m}')

    data = twse.month_revenue(year, month)
    if data is not None:
        data.to_csv(os.path.join(dir, f"{year}-{m}.csv"), index=False, encoding='utf_8_sig')

        log(f"save month_revenue {year}-{m}")
    else:
        error('not month_revenue')


# 財報
@cli.command('financial')
@click.option('-y', '--year', default=0, help="年")
@click.option('-s', '--season', default=0, help="季")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
@click.option('-t', '--type', default='all', type=click.Choice(FINANCIAL_TYPE, case_sensitive=False), help="財報類型")
def get_financial(year, season, outpath, type):
    if year == 0:
        year = datetime.now().year

        if datetime.now().month < 4:
            year = year - 1

    if type == 'all':
        FINANCIAL_TYPE.remove('all')

        for t in FINANCIAL_TYPE:
            _get_financial(year, season, outpath, t)
    else:
        _get_financial(year, season, outpath, type)


# 股利
@cli.command('dividend')
@click.option('-y', '--year', default=year, help="年")
@click.option('-o', '--outPath', type=click.Path(), help="輸出路徑")
def dividend(year, outpath):
    outPath = os.path.join(outpath, "dividend")
    data = twse.dividend(year)
    data.to_csv(os.path.join(outPath, f"{year}.csv"), index=False, encoding='utf_8_sig')

    log(f"save dividend {year}")


# 合併
@cli.command('merge')
@click.option('-i', '--input', type=click.Path(), help="輸入路徑")
@click.option('-o', '--out', type=click.Path(), help="輸出路徑")
@click.option('-t', '--type', default='all', type=click.Choice(MERGE_TYPE, case_sensitive=False), help="合併類型")
def merge(input, out, type):
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
@click.option('-c', '--id', type=click.INT, help="卷商id")
@click.option('-o', '--out', type=click.Path(), help="輸出")
def get_fund(year, month, id, out):
    for ym, rows in fund.get(year=year, month=month, id=id).items():
        data = []
        f = os.path.join(out, str(ym)) + ".csv"

        if os.path.exists(f):
            continue

        for v in rows:
            for value in v['data']:
                for code in value['data']:
                    data.append([v['name'], value['name']] + list(code.values()))

        pd.DataFrame(
            data,
            columns=['c_name', 'f_name', 'code', 'name', 'amount', 'total', 'type']
        ).to_csv(f, index=False, encoding='utf_8_sig')


# 新聞
@cli.command('news')
@click.option('-e', '--email', type=click.STRING, help="email")
@click.option('-h', '--hours', type=click.INT, help="小時")
@click.option('-l', '--login_email', type=click.STRING, help="發送者")
@click.option('-p', '--login_pwd', type=click.STRING, help="發送密碼")
@click.option('-s', '--save', type=click.BOOL, help="是否保存在db")
def news(email, hours, login_email, login_pwd, save=False):
    log('start news')

    date = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

    data = [
        ['聯合報-產經', cnews.udn('6644', date)],
        ['聯合報-股市', cnews.udn('6645', date)],
        ['中時', cnews.chinatimes(date)],
        ['科技新報', cnews.technews(date)],
        ['經濟日報-產業熱點', cnews.money_udn('5591', '5612', date)],
        ['經濟日報-生技醫藥', cnews.money_udn('5591', '10161', date)],
        ['經濟日報-企業CEO', cnews.money_udn('5591', '5649', date)],
        ['經濟日報-總經趨勢', cnews.money_udn('10846', '10869', date)],
        ['經濟日報-2021投資前瞻', cnews.money_udn('10846', '121887', date)],
        ['經濟日報-國際焦點', cnews.money_udn('5588', '5599', date)],
        ['經濟日報-美中貿易戰', cnews.money_udn('5588', '10511', date)],
        ['經濟日報-金融脈動', cnews.money_udn('12017', '5613', date)],
        ['經濟日報-市場焦點', cnews.money_udn('5590', '5607', date)],
        ['經濟日報-集中市場', cnews.money_udn('5590', '5710', date)],
        ['經濟日報-櫃買市場', cnews.money_udn('5590', '11074', date)],
        ['經濟日報-國際期貨', cnews.money_udn('11111', '11114', date)],
        ['經濟日報-國際綜合', cnews.money_udn('12925', '121854', date)],
        ['經濟日報-外媒解析', cnews.money_udn('12925', '12937', date)],
        ['經濟日報-產業動態', cnews.money_udn('12925', '121852', date)],
        ['經濟日報-產業分析', cnews.money_udn('12925', '12989', date)],
        ['工商時報-產業', cnews.ctee(date, 'industry')],
        ['工商時報-科技', cnews.ctee(date, 'tech')],
        ['工商時報-國際', cnews.ctee(date, 'global')],
        ['工商時報-兩岸', cnews.ctee(date, 'china')],
        ['鉅亨網-台股', cnews.cnyes(date, 'tw_stock')],
        ['鉅亨網-國際股', cnews.cnyes(date, 'wd_stock')],
        ['自由時報-國際財經', cnews.ltn(date, 'international')],
        ['自由時報-證券產業', cnews.ltn(date, 'securities')],
        ['moneydj-頭條新聞', cnews.moneydj(date, 'mb010000')],
        ['moneydj-總體經濟', cnews.moneydj(date, 'mb020000')],
        ['moneydj-債券市場', cnews.moneydj(date, 'mb040200')],
        ['moneydj-產業情報', cnews.moneydj(date, 'mb07')],
        ['東森新聞-財經新聞台股', cnews.ebc(date, 'stock')],
        ['trendforce', cnews.trendforce(date)],
        ['dramx', cnews.dramx(date)],
        ['digitimes-報導總欄', cnews.digitimes(date)],
        ['證交所-即時重大訊息', twse.news(date)],
    ]

    log('get news ok')

    if save:
        db = conf['databases']
        engine = create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['table']}",
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
            error('insert error ' + date.__str__())
        else:
            session.commit()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = render('email.html',
                  news=[{'title': v[0], 'news': v[1]} for v in data],
                  date=now,
                  end_date=date,
                  )

    content = MIMEMultipart()
    content["from"] = "bot.junx@gmail.com"
    content["subject"] = f"財經新聞-{now}"
    content["to"] = email
    content.attach(MIMEText(html, 'html'))

    log(f"login email: {login_email}")
    log(f"send email: {email}")

    with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:
        try:
            smtp.ehlo()  # 驗證SMTP伺服器
            smtp.starttls()  # 建立加密傳輸
            smtp.login(login_email, login_pwd)
            smtp.send_message(content)
            log('set news email ok')
        except Exception as e:
            error(f"set news email error {e.__str__()}")


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


# 匯入財報
@cli.command('import-to-database')
@click.option('-t', '--type', type=click.STRING, help="類型")
@click.option('-i', '--path', type=click.Path(), help="輸入檔案路徑")
def import_to_database(type, path):
    data = pd.read_csv(path)
    d = db()

    if type == 'profit':
        financial.profit(data, d)
    elif type == 'assetsDebt':
        financial.assetsDebt(data, d)


# 財報
def _get_financial(year, season, outpath, type):
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
                for k, v in d.items():
                    dir = os.path.join(outPath, k)
                    f = os.path.join(dir, f"{code}.csv")

                    if os.path.exists(f):
                        continue

                    if os.path.exists(dir) == False:
                        os.mkdir(dir)

                    v.to_csv(f, index=False, encoding='utf_8_sig')

                    log(f"save {type} {code} {k}")

            time.sleep(6)


if __name__ == '__main__':
    cli()
