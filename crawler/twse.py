import requests
import time
import logging
import pandas as pd
from io import StringIO
from models import models
from bs4 import BeautifulSoup
from sqlalchemy import engine
from sqlalchemy.orm import Session

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


# 月營收
def month_revenue(year, month):
    t = [
        ['sii', 0],
        ['sii', 1],
        ['otc', 0],
        ['otc', 1],
    ]

    revenue = []
    year = year - 1911

    for n in t:
        r = requests.get(f"https://mops.twse.com.tw/nas/t21/{n[0]}/t21sc03_{year}_{month}_{n[1]}.html",
                         headers=HEADERS)
        r.encoding = 'big5-hkscs'

        try:
            table = pd.read_html(StringIO(r.text), encoding='big5-hkscs')
        except Exception as e:
            continue

        data = pd.concat([df for df in table if df.shape[1] == 11])
        revenue = revenue + data.iloc[:, :5].to_numpy().tolist()

        time.sleep(6)

    data = {}

    for i, index in enumerate([2, 4]):
        d = pd.DataFrame(
            [v[:2] + [v[index]] for v in revenue], columns=['code', 'name', 'value']
        ).sort_values(by=['code'])

        data[year + 1911 - i] = d[d['code'] != '合計']

    return data


# 資產負債表
def balance_sheet(code, year, season):
    data = {}
    year = year - 1911
    season = "%02d" % season

    st = {
        3: "Q1",
        6: "Q2",
        9: "Q3",
        12: "Q4",
    }

    r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t164sb03", {
        'encodeURIComponent': 1,
        'step': 1,
        'firstin': 1,
        'off': 1,
        'TYPEK2': "",
        'keyword4': "",
        'code1': "",
        "checkbtn": "",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": code,
        'year': year,
        'season': season,
    }, headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if len(table) <= 1:
        return {}

    table = table[-1]
    columns = table.columns.tolist()[1:]

    d = columns[0][2]

    if len(d) != 10:
        return {}

    if d[4:6] == '01':
        return {}

    data[f"{int(d[:3]) + 1911}{st[int(d[4:6])]}"] = pd.DataFrame(
        table.iloc[:, [0, 1, 2]].to_numpy().tolist(),
        columns=['項目', '金額', '%']
    )

    return data


# 綜合損益表
def consolidated_income_statement(code, year, season):
    data = {}
    year = year - 1911
    season = "%02d" % season

    r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t164sb04", {
        'encodeURIComponent': 1,
        'step': 1,
        'firstin': 1,
        'off': 1,
        'TYPEK2': "",
        'keyword4': "",
        'code1': "",
        "checkbtn": "",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": code,
        'year': year,
        'season': season,
    }, headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if len(table) <= 1:
        return {}

    table = table[-1]
    columns = table.columns.tolist()[1:]

    for i, d in enumerate([columns[0][2], columns[2][2]]):
        if len(d) == 21:
            if d[4:6] == '01' and d[15:17] == '03':
                s = "Q1"
            elif d[4:6] == '01' and d[15:17] == '06':
                s = "Q2"
            elif d[4:6] == '01' and d[15:17] == '09':
                s = "Q3"
            else:
                return {}
        elif len(d) == 7:
            s = f"Q{d[5:6]}"
        elif len(d) == 5:
            s = "Q4"
        elif d[3:] == '年上半年度':
            s = "Q2"
        elif d[3:] == '年度':
            s = "Q4"
        else:
            return {}

        data[f"{int(d[:3]) + 1911}{s}"] = pd.DataFrame(
            table.iloc[:, [0, 1 + (2 * i), 2 + (2 * i)]].to_numpy().tolist(),
            columns=['項目', '金額', '%']
        )

    return data


# 現金流量表
def cash_flow_statement(code, year, season):
    data = {}
    year = year - 1911
    season = "%02d" % season

    r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t164sb05", {
        'encodeURIComponent': 1,
        'step': 1,
        'firstin': 1,
        'off': 1,
        'TYPEK2': "",
        'keyword4': "",
        'code1': "",
        "checkbtn": "",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": code,
        'year': year,
        'season': season,
    }, headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if len(table) <= 1:
        return {}

    table = table[-1]
    columns = table.columns.tolist()[1:]

    s = ''
    d = columns[0][2]

    if len(d) == 21:
        if d[4:6] == '01' and d[15:17] == '03':
            s = "Q1"
        if d[4:6] == '01' and d[15:17] == '06':
            s = "Q2"
        if d[4:6] == '01' and d[15:17] == '09':
            s = "Q3"
        if d[4:6] == '01' and d[15:17] == '12':
            s = "Q4"
    elif len(d) == 7:
        s = f"Q{d[5:6]}"
    elif len(d) == 5:
        s = "Q4"
    else:
        return {}

    data[f"{int(d[:3]) + 1911}{s}"] = pd.DataFrame(
        table.iloc[:, [0, 1]].to_numpy().tolist(),
        columns=['項目', '金額']
    )

    return data


# 權益變動表
def changes_in_equity(code, year, season):
    data = {}
    year = year - 1911
    season = "%02d" % season

    r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t164sb06", {
        'encodeURIComponent': 1,
        'step': 1,
        'firstin': 1,
        'off': 1,
        'TYPEK2': "",
        'keyword4': "",
        'code1': "",
        "checkbtn": "",
        "queryName": "co_id",
        "inpuType": "co_id",
        "TYPEK": "all",
        "isnew": "false",
        "co_id": code,
        'year': year,
        'season': season,
    }, headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if len(table) <= 1:
        return {}

    if table[1].shape[1] == 1:
        return {}

    c = table[1].columns.tolist()[0][0]

    if c[5:] == '年度':
        s = "Q4"
    elif c[5:] == '年前3季':
        s = 'Q3'
    elif c[5:] == '年上半年度':
        s = 'Q2'
    elif c[5:] == '年第1季':
        s = 'Q1'
    else:
        return {}

    d = table[1].to_numpy().tolist()
    data[f"{int(c[2:5]) + 1911}{s}"] = pd.DataFrame(d[1:], columns=d[0])

    return data


# 股利
def dividend(year):
    data = {}

    qs = [
        f"MARKET_CAT=%E4%B8%8A%E5%B8%82&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR={year}",
        f"MARKET_CAT=%E4%B8%8A%E6%AB%83&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR={year}",
    ]

    for q in qs:
        r = requests.get(
            f"https://goodinfo.tw/StockInfo/StockDividendPolicyList.asp?{q}",
            headers=HEADERS)

        r.encoding = 'utf8'
        table = pd.read_html(r.text)

        for i, v in table[9].iterrows():
            code = str(v[1])

            if code[-1] == 'B' or code[-1] == 'T' or code[0:2] == '00' or code[-1] == 'A' or code[-1] == 'E' or code[
                -1] == 'F' or code[-1] == 'C' or code[
                -1] == 'T' or code[-1] == 'C':
                continue

            if code not in data:
                data[code] = [code, v[9], v[12]]

                logging.info(f"read {year} {code}")

    return pd.DataFrame(list(data.values()), columns=['code', '現金股利', '股票股利'])


# 已公布季報個股code
def season_codes(year, season):
    codes = []
    try:
        year = year - 1911

        for t in ['sii', 'otc']:
            r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t163sb05", {
                'encodeURIComponent': 1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'TYPEK': t,
                'year': year,
                'season': season,
            }, headers=HEADERS)

            r.encoding = 'utf8'
            table = pd.read_html(r.text, header=None)
            codes = codes + \
                    pd.concat([df for df in table if df.shape[1] == 23 or df.shape[1] == 22 or df.shape[1] == 21])[
                        '公司代號'].tolist()

            time.sleep(5)
    except Exception as a:
        pass

    return sorted(codes)


# 即時重大訊息 https://mops.twse.com.tw/mops/web/t05sr01_1
def news(end_date):
    news = []
    r = requests.get(
        "https://mops.twse.com.tw/mops/web/t05sr01_1",
        headers=HEADERS
    )

    if r.status_code != 200:
        return news

    data = BeautifulSoup(r.text, 'html.parser').find("table", class_="hasBorder")

    if data is None:
        return news

    for v in data.findAll("tr")[1:]:
        v = v.findAll("td")
        date = f"{int(v[2].text[:3]) + 1911}-{v[2].text[4:6]}-{v[2].text[7:9]} {v[3].text}"

        if date <= end_date:
            break

        key = ['財務', '處分', '股利', '減資', '增資', '不動產', '辭任', '自結', '澄清']
        msg = v[4].text

        if any(x in msg for x in key):
            title = '<font color="#9818BE">' + v[1].text + '</font>' + v[4].text
        else:
            title = f"{v[1].text} - {v[4].text}"

        news.append({
            "title": title,
            "url": "",
            "date": date,
        })

    return news


# 加權指數
def tse(d: engine):
    r = requests.get('https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=html', headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if (len(table) == 0):
        return True

    table = table[0].iloc[-1:].iloc[0]
    date = f"{int(table.iloc[0][:3]) + 1911}-{table.iloc[0][4:6]}-{table.iloc[0][7:9]}"

    session = Session(d)
    stock = session.execute('SELECT id, code FROM stocks WHERE code = :code', {'code': 'TSE'}).first()
    price = session.execute('SELECT id, date, close FROM prices WHERE stock_id = :id order by date desc limit 1',
                            {'id': stock.id}).first()

    if (price.date.__str__() == date):
        return True

    r = requests.get('https://www.twse.com.tw/exchangeReport/FMTQIK?response=html')

    r.encoding = 'utf8'
    table1 = pd.read_html(r.text)

    if (len(table1) == 0):
        return True

    table1 = table1[0].iloc[-1:].iloc[0]
    date1 = f"{int(table1.iloc[0][:3]) + 1911}-{table1.iloc[0][4:6]}-{table1.iloc[0][7:9]}"

    if (date != date1):
        return False

    insert = [
        {
            'stock_id': stock.id,
            'date': date,
            'open': table.iloc[1],
            'high': table.iloc[2],
            'low': table.iloc[3],
            'close': table.iloc[4],
            'increase': round((table1.iloc[-1] / table.iloc[4]) * 100, 2),
            'amplitude': round(table.iloc[2] / table.iloc[3], 2),
            'volume': int(table1.iloc[2]),
        }
    ]

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return False
        else:
            session.commit()

    return True


# 櫃買
def otc(d: engine):
    r = requests.get('https://www.tpex.org.tw/web/stock/iNdex_info/inxh/Inx_result.php?l=zh-tw&s=0,asc,0&o=htm',
                     headers=HEADERS)

    r.encoding = 'utf8'
    table = pd.read_html(r.text)

    if (len(table) == 0):
        return True

    table = table[0].iloc[-2:-1].iloc[0]
    date = f"{int(table.iloc[0][:4])}-{table.iloc[0][5:7]}-{table.iloc[0][8:10]}"

    session = Session(d)
    stock = session.execute('SELECT id, code FROM stocks WHERE code = :code', {'code': 'OTC'}).first()
    price = session.execute('SELECT id, date, close FROM prices WHERE stock_id = :id order by date desc limit 1',
                            {'id': stock.id}).first()

    if (price.date.__str__() == date):
        return True

    r = requests.get(f"https://mis.twse.com.tw/stock/api/getStatis.jsp?ex=otc&delay=0&_={int(time.time()) * 1000}",
                     headers={
                         'User-Agent': USER_AGENT,
                         'host': 'mis.twse.com.tw',
                     })

    table1 = r.json()

    if (table1['queryTime']['sessionKey'] != f"otc_{date[:4]}{date[5:7]}{date[8:10]}"):
        return False

    insert = [
        {
            'stock_id': stock.id,
            'date': date,
            'open': table.iloc[1],
            'high': table.iloc[2],
            'low': table.iloc[3],
            'close': table.iloc[4],
            'increase': round((float(table.iloc[-1]) / float(table.iloc[4])) * 100, 2),
            'amplitude': round(float(table.iloc[2]) / float(table.iloc[3]), 2),
            'volume': table1['detail']['tz'],
        }
    ]

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return False
        else:
            session.commit()

    return True
