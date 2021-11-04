import os
import glob
import datetime
import requests
import time
import logging
import pytz
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
            if BeautifulSoup(r.text, 'html.parser').find_all('h3')[1].text == '查無資料':
                continue
            return None

        data = pd.concat([df for df in table if df.shape[1] == 11])
        revenue = revenue + data.iloc[:, :10].to_numpy().tolist()

        time.sleep(6)

    d = pd.DataFrame(revenue, columns=['code', 'name', '當月營收', '上月營收', '去年同期營收', 'qoq', 'yoy', '當月累積營收', '去年同期累積營收',
                                       '累積營收比較增減']).sort_values(
        by=['code']
    )

    return d[d['code'] != '合計']


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
    columns = table.columns.tolist()[1:-2]

    for i in range(int(len(columns) / 2)):
        d = columns[i * 2][2]

        if len(d) != 10:
            continue

        if d[4:6] == '01':
            continue

        if d[4:] != columns[0][2][4:]:
            continue

        data[f"{int(d[:3]) + 1911}{st[int(d[4:6])]}"] = pd.DataFrame(
            table.iloc[:, [0, 1 + (2 * i), 2 + (2 * i)]].to_numpy().tolist(),
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
    columns = table.columns.tolist()[1:3]

    for i in range(len(columns)):
        s = ''
        d = columns[i][2]

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
            table.iloc[:, [0, 1 + (1 * i)]].to_numpy().tolist(),
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

    table = table[1:]
    for i in range(len(table)):
        c = table[i].columns.tolist()[0][0]

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

        d = table[i].to_numpy().tolist()
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
def news(keys: dict):
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

    news = []
    tz = pytz.timezone('Asia/Taipei')
    end_date = (datetime.datetime.now(tz) - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    for v in data.findAll("tr")[1:]:
        v = v.findAll("td")
        date = f"{int(v[2].text[:3]) + 1911}-{v[2].text[4:6]}-{v[2].text[7:9]} {v[3].text}"

        if date < end_date:
            continue

        code = v[0].text
        name = v[1].text
        content = v[4].text

        for n, k in keys.items():
            ok = True

            for v in k:
                if content.find(v) < 0:
                    ok = False
                    break

            if ok:
                news.append({
                    'code': code,
                    'name': name,
                    'date': date,
                    'content': content,
                    'type': n,
                    'body': f"\n代碼: {code}\n名稱: {name}\n 時間: {date}\n 種類: {n}\n\n{content}",
                    'texts': [f"代碼: {code}", f"名稱: {name}", f"時間: {date}", f"種類: {n}", content],
                })

    return news


# 加權指數
def twse(d: engine):
    session = Session(d)

    r = requests.get(
        f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=&_={time.time() * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'www.twse.com.tw',
        })

    data = r.json()['data'][-1]
    stock = session.execute('SELECT id FROM stocks WHERE code = :code', {'code': 'TSE'}).first()
    dates = data[0].split('/')

    date = f"{int(dates[0]) + 1911}-{dates[1]}-{dates[2]}"
    open = float(data[1].replace(',', ''))
    high = float(data[2].replace(',', ''))
    low = float(data[3].replace(',', ''))
    close = float(data[4].replace(',', ''))

    r = requests.get(
        f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date=&_={time.time() * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'www.twse.com.tw',
        })

    volume = r.json()['data'][-1]

    price = session.execute(
        'SELECT * FROM prices WHERE date <= :date AND stock_id = :stock_id ORDER BY date DESC LIMIT 2',
        {'date': date, 'stock_id': stock.id}
    ).all()

    if volume[0] != data[0]:
        return

    if price[0].date.__str__() == date:
        return

    insert = [
        {
            'stock_id': stock.id,
            'date': date,
            'open': open,
            'high': high,
            'low': low,
            'close': close,
            'increase': round((round(close - price[0].close, 2) / close) * 100, 2),
            'amplitude': round(((high / low) - 1) * 100, 2),
            'volume': int(volume[2].replace(',', '')),
            'volume_ratio': 0
        }
    ]

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return
        session.commit()


# otc指數
def otc(d: engine):
    session = Session(d)

    r = requests.get(
        f"https://www.tpex.org.tw/web/stock/aftertrading/all_daily_index/sectinx_result.php?l=zh-tw&_={time.time() * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'www.tpex.org.tw',
        })

    data = r.json()
    stock = session.execute('SELECT id FROM stocks WHERE code = :code', {'code': 'OTC'}).first()
    dates = data['reportDate'].split('/')
    price = data['aaData'][-1]

    if price[0] != '櫃買指數':
        return

    date = f"{int(dates[0]) + 1911}-{dates[1]}-{dates[2]}"
    open = float(price[3].replace(',', ''))
    high = float(price[4].replace(',', ''))
    low = float(price[5].replace(',', ''))
    close = float(price[1].replace(',', ''))

    r = requests.get(
        f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php?l=zh-tw&_={time.time() * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'www.tpex.org.tw',
        })

    volume = r.json()['aaData'][-1]

    price = session.execute(
        'SELECT * FROM prices WHERE date <= :date AND stock_id = :stock_id ORDER BY date DESC LIMIT 2',
        {'date': date, 'stock_id': stock.id}
    ).all()

    if volume[0] != data['reportDate']:
        return

    if price[0].date.__str__() == date:
        return

    insert = [
        {
            'stock_id': stock.id,
            'date': date,
            'open': open,
            'high': high,
            'low': low,
            'close': close,
            'increase': round((round(close - price[0].close, 2) / close) * 100, 2),
            'amplitude': round(((high / low) - 1) * 100, 2),
            'volume': int(volume[2].replace(',', '')),
            'volume_ratio': 0
        }
    ]

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return
        session.commit()


# 指數
def exponent(type, d: engine):
    ex_ch = ''

    if (type == 'TSE'):
        ex_ch = 'tse_t00'
    elif (type == 'OTC'):
        ex_ch = 'otc_o00'

    r = requests.get(
        f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}.tw|&json=1&delay=0&_={time.time() * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'mis.twse.com.tw',
        })

    data = r.json()['msgArray'][0]
    date = f"{data['d'][:4]}-{data['d'][4:6]}-{data['d'][6:8]}"

    session = Session(d)
    stock = session.execute('SELECT id, code FROM stocks WHERE code = :code', {'code': type}).first()
    price = session.execute(
        'SELECT id, date, close FROM prices WHERE stock_id = :id AND date = :date order by date desc limit 1',
        {'id': stock.id, 'date': date}
    ).first()

    # if (price is not None):
    #     return True

    r = requests.get(
        f"https://mis.twse.com.tw/stock/api/getStatis.jsp?ex={type.lower()}&delay=0&_={int(time.time()) * 1000}",
        headers={
            'User-Agent': USER_AGENT,
            'host': 'mis.twse.com.tw',
        })

    volume = r.json()

    if (volume['queryTime']['sessionKey'] != f"{type.lower()}_{date[:4]}{date[5:7]}{date[8:10]}"):
        return False

    insert = [
        {
            'stock_id': stock.id,
            'date': date,
            'open': data['o'],
            'high': data['h'],
            'low': data['l'],
            'close': data['z'],
            'increase': round((round(float(data['z']) - float(data['y']), 2) / float(data['z'])) * 100, 2),
            'amplitude': round(((float(data['h']) / float(data['l'])) - 1) * 100, 2),
            'volume': volume['detail']['tz'],
            'volume_ratio': 0
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


# 上市產業指數
def tse_industry(d: engine):
    codes = {
        '水泥類指數': 'TSE11', '食品類指數': 'TSE12', '塑膠類指數': 'TSE13', '紡織纖維類指數': 'TSE14', '電機機械類指數': 'TSE15',
        '電器電纜類指數': 'TSE16', '化學生技醫療類指數': 'TSE17', '化學類指數': 'TSE30', '生技醫療類指數': 'TSE31', '玻璃陶瓷類指數': 'TSE18',
        '造紙類指數': 'TSE19', '鋼鐵類指數': 'TSE20', '橡膠類指數': 'TSE21', '汽車類指數': 'TSE22', '電子類指數': 'TSE23', '半導體類指數': 'TSE32',
        '電腦及週邊設備類指數': 'TSE33', '光電類指數': 'TSE34', '通信網路類指數': 'TSE35', '電子零組件類指數': 'TSE36', '電子通路類指數': 'TSE37',
        '資訊服務類指數': 'TSE38', '其他電子類指數': 'TSE39', '建材營造類指數': 'TSE25', '航運類指數': 'TSE26', '觀光類指數': 'TSE27',
        '金融保險類指數': 'TSE28', '貿易百貨類指數': 'TSE29', '油電燃氣類指數': 'TSE40', '其他類指數': 'TSE99',
    }

    r = requests.get('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?'
                     'ex_ch=tse_t00.tw|tse_TW50.tw|tse_TW50C.tw|tse_TWMC.tw|tse_TWIT.tw|'
                     'tse_TWEI.tw|tse_TWDP.tw|tse_EMP99.tw|tse_HC100.tw|tse_CG100.tw|tse_FRMSA.tw|'
                     'tse_t001.tw|tse_t002.tw|tse_t003.tw|tse_SC300.tw|tse_t011.tw|tse_t031.tw|'
                     'tse_t051.tw|tse_t01.tw|tse_t02.tw|tse_t03.tw|tse_t04.tw|tse_t05.tw|tse_t06.tw|'
                     'tse_t07.tw|tse_t21.tw|tse_t22.tw|tse_t08.tw|tse_t09.tw|tse_t10.tw|tse_t11.tw|'
                     'tse_t12.tw|tse_t13.tw|tse_t24.tw|tse_t25.tw|tse_t26.tw|tse_t27.tw|tse_t28.tw|'
                     'tse_t29.tw|tse_t30.tw|tse_t31.tw|tse_t14.tw|tse_t15.tw|tse_t16.tw|tse_t17.tw|'
                     'tse_t18.tw|tse_t23.tw|tse_t20.tw|tse_TTDRL2.tw|tse_TTDRIN.tw|tse_EDRL2.tw|'
                     'tse_EDRIN.tw|tse_IX0103.tw|tse_IX0108.tw|tse_IX0109.tw|tse_IX0125.tw|tse_IX0133.tw|'
                     'tse_IX0139.tw|tse_IR0129.tw|tse_IR0131.tw|tse_IR0135.tw|tse_IX0142.tw|tse_IX0143.tw|'
                     'tse_IX0145.tw|&json=1&delay=0&_=1623651891053', headers={
        'User-Agent': USER_AGENT,
        'host': 'mis.twse.com.tw',
    })

    data = r.json()

    # 成交量
    r = requests.get('https://www.twse.com.tw/exchangeReport/BFIAMU?response=html', headers=HEADERS)
    r.encoding = 'utf8'
    table = pd.read_html(r.text)
    volume = {v.iloc[0]: v.iloc[2] for i, v in table[0].iterrows()}
    volumeDate = f"{int(table[0].columns[0][0][:3]) + 1911}-{table[0].columns[0][0][4:6]}-{table[0].columns[0][0][7:9]}"

    insert = []
    session = Session(d)
    tse = session.execute("SELECT id, code FROM stocks where code like 'TSE%'").all()
    tse = {v.code: v.id for v in tse}
    stock = session.execute('SELECT * FROM prices where stock_id = :stock_id AND date = :date', {
        'stock_id': tse['TSE'],
        'date': volumeDate,
    }).first()

    if (stock is None):
        logging.error(f"tse {volumeDate} is not date")
        return False

    for v in data['msgArray']:
        if (v['n'] not in codes):
            continue

        if (codes[v['n']] not in tse):
            continue

        date = f"{v['d'][:4]}-{v['d'][4:6]}-{v['d'][6:8]}"

        if (volumeDate != date):
            logging.error(f"volume date != {date}")
            return False

        exist = session.execute('SELECT * FROM prices where stock_id = :stock_id AND date = :date', {
            'stock_id': tse[codes[v['n']]],
            'date': date,
        }).first()

        if (exist is not None):
            continue

        id = tse[codes[v['n']]]

        if (v['n'] == '航運類指數'):
            v['n'] = '航運業類指數'

        if (v['n'] == '觀光類指數'):
            v['n'] = '觀光事業類指數'

        insert.append({
            'date': date,
            'stock_id': id,
            'open': v['o'],
            'high': v['h'],
            'low': v['l'],
            'close': v['z'],
            'increase': round((round(float(v['z']) - float(v['y']), 2) / float(v['z'])) * 100, 2),
            'amplitude': round(((float(v['h']) / float(v['l'])) - 1) * 100, 2),
            'volume': volume[v['n']],
            'volume_ratio': round((volume[v['n']] / stock.volume) * 100, 3)
        })

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return False
        else:
            session.commit()

    return True


# 櫃買產業指數
def otc_industry(d: engine):
    codes = {
        '紡織纖維': 'OTC44',
        '電機機械': 'OTC45',
        '鋼鐵工業': 'OTC50',
        '電子工業': 'OTC53',
        '建材營造': 'OTC55',
        '航運業': 'OTC56',
        '觀光事業': 'OTC57',
        '其他': 'OTC89',
        '化學工業': 'OTC47',
        '生技醫療': 'OTC41',
        '半導體業': 'OTC62',
        '電腦及週邊設備業': 'OTC63',
        '光電業': 'OTC64',
        '通信網路業': 'OTC65',
        '電子零組件業': 'OTC66',
        '電子通路業': 'OTC67',
        '資訊服務業': 'OTC68',
        '其他電子業': 'OTC69',
        '文化創意業': 'OTC70',
    }

    now = datetime.datetime.now()
    date = f"{now.year}-{'{0:02d}'.format(now.month)}-{'{0:02d}'.format(now.day)}"
    qDate = f"{now.year - 1911}/{'{0:02d}'.format(now.month)}/{'{0:02d}'.format(now.day)}"
    r = requests.get(
        f"https://www.tpex.org.tw/web/stock/aftertrading/all_daily_index/sectinx_print.php?l=zh-tw&d={qDate}",
        headers=HEADERS)
    r.encoding = 'utf8'
    table = pd.read_html(r.text)
    insert = []

    session = Session(d)
    otc = session.execute("SELECT id, code FROM stocks where code like 'OTC%'").all()
    otc = {v.code: v.id for v in otc}

    stock = session.execute('SELECT * FROM prices where stock_id = :stock_id AND date = :date', {
        'stock_id': otc['OTC'],
        'date': date,
    }).first()

    if (stock is None):
        return False

    r = requests.get(
        f"https://www.tpex.org.tw/web/stock/historical/trading_vol_ratio/sectr_result.php?l=zh-tw&d={qDate}&s=undefined&o=htm"
    )
    r.encoding = 'utf8'
    table1 = pd.read_html(r.text)
    table1 = {v.iloc[0]: v.iloc[1] for i, v in table1[0].iterrows()}

    for i, v in table[0].iterrows():
        if (v.iloc[0] in ['線上遊戲業', '櫃買指數']):
            continue

        insert.append({
            'date': date,
            'stock_id': otc[codes[v.iloc[0]]],
            'open': v.iloc[3],
            'high': v.iloc[4],
            'low': v.iloc[5],
            'close': v.iloc[1],
            'increase': round((float(v.iloc[2]) / float(v.iloc[1])) * 100, 2),
            'amplitude': round(((float(v.iloc[4]) / float(v.iloc[5])) - 1) * 100, 2),
            'volume': table1[v.iloc[0]],
            'volume_ratio': round((table1[v.iloc[0]] / stock.volume) * 100, 3)
        })

    if (len(insert) > 0):
        result = session.execute(models.price.insert(), insert)

        if result.is_insert == False or result.rowcount != len(insert):
            logging.error('insert error')
            return False
        else:
            session.commit()

    return True


# XQ匯入產業指數
def xq_industry(path, d: engine):
    session = Session(d)

    volume = {}
    data = session.execute(
        "SELECT stocks.id, stocks.code, prices.date, prices.volume FROM prices JOIN stocks ON stocks.id = prices.stock_id WHERE stocks.code IN ('OTC', 'TSE')"
    ).all()

    volume['OTC'] = {v.date.__str__(): v.volume for v in data if v.code == 'OTC'}
    volume['TSE'] = {v.date.__str__(): v.volume for v in data if v.code == 'TSE'}

    stock = session.execute('SELECT id, code FROM stocks').all()
    stock = {v.code: v.id for v in stock}

    for p in glob.glob(os.path.join(path, "*.csv")):
        name = os.path.split(p)[1].split('.')[0]
        insert = []

        for i, v in pd.read_csv(p).iterrows():
            v = dict(v)
            v['date'] = str(v['date'])
            v['date'] = f"{v['date'][:4]}-{v['date'][4:6]}-{v['date'][6:8]}"
            v['volume'] = v['volume ']
            v['amplitude'] = round(((v['high'] / v['low']) - 1) * 100, 2)
            v['stock_id'] = stock[name]
            v['volume_ratio'] = 0

            if (len(name) != 3):
                if (name[:3] == 'TSE'):
                    v['volume_ratio'] = round((v['volume'] / volume['TSE'][v['date']]) * 100, 3)
                elif (name[:3] == 'OTC'):
                    v['volume_ratio'] = round((v['volume'] / volume['OTC'][v['date']]) * 100, 3)

            v.pop('volume ', None)

            insert.append(v)

        if (len(insert) > 0):
            result = session.execute(models.price.insert(), insert)

            if result.is_insert == False or result.rowcount != len(insert):
                logging.error('insert error')
                return False
            else:
                session.commit()

        print(f"{p} --- {len(insert)}")
        # logging.info(f"{p} --- {len(insert)}")

    return True


# 最近上市上櫃
def ipo():
    data = []

    r = requests.get(
        f"https://www.tpex.org.tw/web/regular_emerging/apply_schedule/latest/latest_listed_companies.php?l=zh-tw",
        headers=HEADERS
    )
    r.encoding = 'utf-8'

    date = datetime.datetime.now().strftime("%Y-%m-%d")

    for index, value in pd.read_html(r.text)[0].iterrows():
        dates = value['上櫃日期'].split('/')
        dates[0] = str(int(dates[0]) + 1911)

        if "-".join(dates) < date:
            continue

        data.append({
            'code': value['股票代號'],
            'name': value['公司名稱'],
            'market': '上櫃',
        })

    r = requests.get(
        f"https://www.twse.com.tw/company/newlisting?response=json&yy=&_={time.time() * 1000}",
        headers=HEADERS
    )

    for value in r.json()['data']:
        dates = value[9].split('.')
        dates[0] = str(int(dates[0]) + 1911)

        if "-".join(dates) < date:
            continue

        data.append({
            'code': value[0],
            'name': value[1],
            'market': '上市',
        })

    for index, value in enumerate(data):
        r = requests.post(
            f"https://mops.twse.com.tw/mops/web/ajax_t05st03", {
                'encodeURIComponent': 1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'keyword4': '',
                'code1': '',
                'TYPEK2': '',
                'checkbtn': '',
                'queryName': 'co_id',
                'inpuType': 'co_id',
                'TYPEK': 'all',
                'co_id': value['code'],
            },
            headers=HEADERS
        )
        r.encoding = 'utf-8'

        data[index]['classification'] = pd.read_html(r.text)[1][3][0].replace('工業', '').replace('業', '')

        time.sleep(6)

    return data
