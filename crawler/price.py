import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


# 面板報價
def wits_view():
    r = requests.get("https://www.witsview.cn", headers=HEADERS)
    r.encoding = 'utf8'

    table = pd.read_html(r.text)
    soup = BeautifulSoup(r.text, 'html.parser')
    name = soup.find_all('div', class_="table-btn")
    p = soup.select('p')

    name = [
        name[0].text,
        name[1].text,
        name[2].text,
        name[3].text,
    ]

    p = [
        f"{p[0].text[7:12]}-{p[0].text[13:15]}-{p[0].text[16:18]}",
        f"{p[2].text[7:12]}-{p[2].text[13:15]}-{p[2].text[16:18]}",
        f"{p[3].text[7:12]}-{p[3].text[13:15]}-{p[3].text[16:18]}",
        f"{p[4].text[7:12]}-{p[4].text[13:15]}-{p[4].text[16:18]}",
    ]

    return {
        name[0]: {
            p[0]: table[0]
        },
        name[1]: {
            p[1]: table[1]
        },
        name[2]: {
            p[2]: table[2]
        },
        name[3]: {
            p[3]: table[3]
        },
    }


# 上市某日成交情況
def twse_stocks(year, month, day):
    month = "%02d" % month
    day = "%02d" % day
    r = requests.get(
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={year}{month}{day}&type=ALLBUT0999&_={time.time() * 1000}",
        headers=HEADERS)

    data = r.json()

    r = requests.get(
        f"https://www.twse.com.tw/exchangeReport/TWTC7U?response=json&date={year}{month}{day}&selectType=ALLBUT0999&_={time.time() * 1000}",
        headers=HEADERS)

    data1 = r.json()

    if data['date'] != data1['date']:
        return []

    r = requests.get(
        f"https://www.twse.com.tw/exchangeReport/TWT53U?response=json&date={year}{month}{day}&selectType=ALLBUT0999&_={time.time() * 1000}",
        headers=HEADERS)

    data2 = r.json()

    if data['date'] != data2['date']:
        return []

    data = data['data9']
    data1 = {v[0]: v for v in data1['data']}
    data2 = {v[0]: v for v in data2['data']}

    price = []

    for v in data:
        v[2] = int(v[2].replace(",", ""))

        if v[0] in data1:
            v[2] -= int(data1[v[0]][2].replace(",", ""))

        if v[0] in data2:
            v[2] -= int(data2[v[0]][2].replace(",", ""))

        value = float(v[10])

        if v[9].find('-') == -1:
            value *= -1

        price.append({
            'code': v[0],
            'open': v[5],
            'close': v[8],
            'high': v[6],
            'low': v[7],
            'volume': v[2] / 1000,
            'increase': round(((float(v[8]) / (float(v[8]) + value)) - 1) * 100, 2),
            'amplitude': round(((float(v[6]) / float(v[7])) - 1) * 100, 2),
        })

    return data

# 上櫃某日成交情況
def otc_stocks(year, month, day):
    month = "%02d" % month
    day = "%02d" % day
    r = requests.get(
        f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={year - 1911}/{month}/{day}&se=EW&_={time.time() * 1000}",
        headers=HEADERS)

    data = []

    for v in r.json()['aaData']:
        data.append({
            'code': v[0],
            'open': v[4],
            'close': v[2],
            'high': v[5],
            'low': v[6],
            'volume': int(v[7].replace(",", "")) / 1000,
            'increase': round(((float(v[2]) / (float(v[2]) + float(v[3]))) - 1) * 100, 2),
            'amplitude': round(((float(v[5]) / float(v[6])) - 1) * 100, 2),
        })

    return data
