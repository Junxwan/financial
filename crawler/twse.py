import requests
import time
import logging
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

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

    try:
        for n in t:
            r = requests.get(f"https://mops.twse.com.tw/nas/t21/{n[0]}/t21sc03_{year}_{month}_{n[1]}.html",
                             headers=HEADERS)
            r.encoding = 'big5-hkscs'

            table = pd.read_html(StringIO(r.text), encoding='big5-hkscs')
            data = pd.concat([df for df in table if df.shape[1] == 11])
            revenue = revenue + data.iloc[:, :3].to_numpy().tolist()

            time.sleep(6)
    except Exception as e:
        return None

    revenue = pd.DataFrame(revenue, columns=['code', 'name', 'value']).sort_values(by=['code'])
    return revenue[revenue['code'] != '合計']


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

    d = columns[0][2]

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
        table.iloc[:, [0, 1, 2]].to_numpy().tolist(),
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
