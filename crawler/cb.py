import time

import requests
import pandas as pd
import numpy as np
from io import StringIO
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

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
        'market': market[trs[5].contents[0].text.replace('債券掛牌情形：', '')],
        'is_collateral': trs[30].contents[0].text.replace('擔保情形：', '').split('，')[0] == '有',
        'url': url,
    }


# 轉換價格
def conversionPrice(code):
    data = []

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

    try:
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