import time
import csv
import requests
import pandas as pd
import numpy as np
from io import StringIO
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


def _url(code, period, year, month):
    return f"https://mops.twse.com.tw/mops/web/t120sg01?TYPEK=&bond_id={code}{period}&bond_kind=5&bond_subn=%24M00000001&bond_yrn={period}&come=2&encodeURIComponent=1&firstin=ture&issuer_stock_code={code}&monyr_reg={year}{month}&pg=&step=0&tg="


# 最近上櫃可轉債
# https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw
def new():
    data = []

    r = requests.get("https://www.tpex.org.tw/web/bond/publish/convertible_bond_search/memo.php?l=zh-tw",
                     headers=HEADERS)
    r.encoding = 'utf-8'
    html = BeautifulSoup(r.text, 'html.parser')
    for tr in html.find('tbody').find_all('tr'):
        tds = tr.find_all('td')
        data.append([
            tds[0].text,
            tds[1].text,
            tds[3].text,
            tds[4].text,
            tds[5].text.strip(),
            tds[6].text.replace(",", ""),
            tds[7].find('a').attrs['href']
        ])

    return data


# 根據年月查詢可轉債
# https://sii.twse.com.tw/server-java/t120sc11?step=0&TYPEK=
def select(year, month):
    data = {}

    r = requests.post("https://sii.twse.com.tw/server-java/t120sc11", {
        'id': '',
        'key': '',
        'TYPEK': '',
        'step': 1,
        'excel': 0,
        'typek': '',
        'type': 2,
        'year': year - 1911,
        'month': month,
    }, headers=HEADERS)
    r.encoding = 'big5-hkscs'

    for index, value in pd.read_html(StringIO(r.text))[1].iterrows():
        if value['發行種類'] != '轉(交)換公司債' or value['證券代號'] == '合計' or value['債券掛牌情形'] == '未掛牌交易':
            continue

        data[value['債券代碼']] = _url(value['證券代號'], value['債券期別'], year, "%02d" % month)

    return data


# 公開資料
# https://mops.twse.com.tw/mops/web/t120sg01?TYPEK=&bond_id=30881&bond_kind=5&bond_subn=%24M00000001&bond_yrn=1&come=2&encodeURIComponent=1&firstin=ture&issuer_stock_code=3088&monyr_reg=202108&pg=&step=0&tg=
def findByUrl(url):
    url = url.replace("http://", "https://")

    try:
        r = requests.get(url, headers=HEADERS)
    except Exception as e:
        return None

    r.encoding = 'utf-8'
    html = BeautifulSoup(r.text, 'html.parser')
    table = html.find('table', class_='hasBorder')

    if table is None:
        return None

    trs = table.find_all('tr')

    startDate = trs[2].contents[0].text.replace('發行日期：', '').split('/')
    startDate[0] = str(int(startDate[0]) + 1911)

    endDate = trs[3].contents[0].text.replace('到期日期：', '').split('/')
    endDate[0] = str(int(endDate[0]) + 1911)

    conversionDate = trs[27].contents[0].text.replace('轉(交)換期間：', '').split('～')
    startConversionDate = conversionDate[0].split('/')
    startConversionDate[0] = str(int(startConversionDate[0]) + 1911)

    endConversionDate = conversionDate[1].split('/')
    endConversionDate[0] = str(int(endConversionDate[0]) + 1911)

    market = {
        '上市': 1,
        '上櫃': 2,
        '興櫃': 3,
    }

    return {
        'code': trs[7].contents[0].text.split("：")[1],
        'name': trs[7].contents[2].text.split("：")[1].strip(),
        'period': trs[7].contents[0].text.split("：")[1][-1],
        'start_date': "-".join(startDate),
        'end_date': "-".join(endDate),
        'active_year': trs[2].contents[2].text.replace("發行期限：", '').split('年')[0],
        'apply_total_amount': int(trs[12].contents[0].text.replace('申請發行總額：', '').replace(',', '').replace('元', '')),
        'publish_total_amount': int(trs[13].contents[0].text.replace('實際發行總額：', '').replace(',', '').replace('元', '')),
        'publish_price': float(trs[24].contents[0].text.replace('發行價格：', '').split('元')[0]),
        'conversion_price': float(trs[26].contents[0].text.replace('發行時轉(交)換價格：', '').split('元')[0]),
        'start_conversion_date': "-".join(startConversionDate),
        'end_conversion_date': "-".join(endConversionDate),
        'conversion_premium_rate': float(trs[26].contents[2].text.replace('轉換溢價率：', '').replace('%', '')),
        'coupon_rate': float(trs[25].contents[0].text.replace('票面利率：', '').replace('%', '')),
        'conversion_stock': 0,
        'market': market[trs[5].contents[0].text.replace('債券掛牌情形：', '')],
        'is_collateral': trs[30].contents[0].text.replace('擔保情形：', '').split('，')[0] == '有',
        'url': url,
    }


# 轉換價格
def conversionPrice(code):
    data = []

    try:
        r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t120sg06", {
            'encodeURIComponent': 1,
            'firstin': True,
            'bond_id': code,
            'step': 1,
            'data_type': '',
            'date1': '',
            'date2': '',
        }, headers=HEADERS)
        r.encoding = 'utf-8'

        type = {
            '掛牌': 1,
            '反稀釋': 2,
            '重設': 3,
            '不重設': 4,
            '特別重設': 5,
            '不特別重設': 6,
        }

        eDates = []
        for index, value in pd.read_html(StringIO(r.text))[0].iterrows():
            dates = value['重設日期(起迄日期)'].split('/')
            dates[0] = str(int(dates[0]) + 1911)
            date = "-".join(dates),

            if date in eDates:
                continue

            data.append({
                'type': type[value['類型']],
                'value': value['轉(交)換價格'],
                'stock': value['轉(交)換股數'],
                'date': date,
            })

            eDates.append(date)
    except ConnectionError as e:
        time.sleep(30)
        return conversionPrice(code)
    except Exception as e:
        return data

    return data


# 餘額
# https://mops.twse.com.tw/mops/web/t98sb05
def balance(year, month):
    data = {}

    for t in ['sii', 'otc']:
        r = requests.post("https://mops.twse.com.tw/mops/web/ajax_t98sb05", {
            'encodeURIComponent': 1,
            'firstin': True,
            'step': 1,
            'off': 1,
            'TYPEK': t,
            'year': year - 1911,
            'month': "%02d" % month,
        }, headers=HEADERS)
        r.encoding = 'utf-8'

        for index, value in pd.read_html(StringIO(r.text))[0].iterrows():
            change = 0
            balance = 0
            change_stock = 0
            balance_stock = 0

            if type(value.iloc[1]) == str and value.iloc[1].isnumeric() == False:
                continue

            if np.isnan(value.iloc[2]) == False:
                change = value.iloc[2]

            if np.isnan(value.iloc[3]) == False:
                balance = value.iloc[3]

            if np.isnan(value.iloc[8]) == False:
                change_stock = value.iloc[8]

            if np.isnan(value.iloc[9]) == False:
                balance_stock = value.iloc[9]

            data[value.iloc[1]] = {
                'code': value.iloc[1],
                'change': change,
                'balance': balance,
                'change_stock': change_stock,
                'balance_stock': balance_stock
            }

    return data


# 價格
# https://www.tpex.org.tw/web/bond/tradeinfo/cb/CBDaily.php?l=zh-tw
def price(year, month):
    data = {}

    r = requests.post("https://www.tpex.org.tw/web/bond/tradeinfo/cb/CBDaily.php?l=zh-tw", {
        'inputY': year,
        'inputM': month,
        'inputFileCode': 'rsta0113',
    }, headers=HEADERS)

    r.encoding = 'utf-8'

    for tr in BeautifulSoup(r.text, 'html.parser').find('table').find_all('tr')[2:]:
        r = requests.get('https://www.tpex.org.tw' + tr.contents[1].find('a').attrs['href'], headers=HEADERS)
        r.encoding = 'big5-hkscs'

        dates = tr.contents[0].text.split('/')
        dates[0] = str(int(dates[0]) + 1911)
        date = "-".join(dates)
        data[date] = []

        for index, value in pd.read_csv(StringIO('\r\n'.join(r.text.split('\r\n')[3:])),
                                        encoding='big5-hkscs').iterrows():
            if type(value['代號']) != str or value['交易'] == '議價' or value['代號'] == '合計':
                continue

            if np.isnan(value['開市']):
                open = value['明日參價']
                close = open
                high = open
                low = open
                volume = 0
                increase = 0
                amplitude = 0
                amount = 0
            else:
                open = value['開市']
                close = value['收市']
                high = value['最高']
                low = value['最低']
                volume = int(value['單位'].replace(",", ""))
                amplitude = round(((high / low) - 1) * 100, 2)
                amount = int(value['金額'].replace(",", ""))

                if np.isnan(value['漲跌']):
                    increase = 0
                else:
                    increase = round(((close / (close - value['漲跌'])) - 1) * 100, 2)

            data[date].append({
                'code': value['代號'],
                'date': date,
                'open': open,
                'close': close,
                'high': high,
                'low': low,
                'volume': volume,
                'increase': increase,
                'amplitude': amplitude,
                'amount': amount,
            })

        time.sleep(6)

    return data


# 同餘額但有股東人數
# https://smart.tdcc.com.tw/opendata/getOD.ashx?id=2-8
def stock():
    r = requests.get("https://smart.tdcc.com.tw/opendata/getOD.ashx?id=2-8", headers=HEADERS)
    r.encoding = 'utf-8'
    return pd.read_csv(StringIO(r.text), encoding='big5-hkscs')


# 理論與市價區間折溢價分位數
def priceQuantile(code, rang):
    quantile = {
        'cb_close': [],
        'off_price': [],
    }

    r = requests.get(f"http://ec2-18-237-97-27.us-west-2.compute.amazonaws.com:54657/cb/price/{code}/premium")
    data = pd.DataFrame(r.json()['data'])

    for r in rang:
        for n in ['cb_close', 'off_price']:
            if len(data) == 0:
                q = ['', '', '', '']
            else:
                q = list(
                    data.loc[lambda df: (df[n] < r[0]) & (df[n] >= r[1]), :]['premium'].quantile([1, 0.9, 0.75, 0.5])
                )

            quantile[n].append(q)

    return quantile
