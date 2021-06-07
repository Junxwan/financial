import logging
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
    'Host': "www.sitca.org.tw",
    'Referer': "https://www.sitca.org.tw/ROC/Industry/IN2629.aspx",
    'Origin': "https://www.sitca.org.tw",
}


def get(year=None, month=None, id=None):
    r = requests.get("https://www.sitca.org.tw/ROC/Industry/IN2629.aspx", headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    select = soup.find_all('select')

    dates = [v.attrs['value'] for v in select[0].find_all('option')]
    fund = [v.attrs['value'] for v in select[1].find_all('option')]
    fund_name = [v.text.split(' ')[1] for v in select[1].find_all('option')]
    yms = []

    if year is None and month is None:
        yms = dates
    elif year is not None and month is not None:
        yms = [f"{year}{month:02}"]
    elif year is not None:
        yms = [f"{year}{i + 1:02}" for i in range(12)]
    elif month is not None:
        yms = [f"{datetime.now().year}{month:02}"]

    if id is None:
        ids = fund
    else:
        ids = [id]

    data = {}
    for ym in yms[::-1]:
        data[ym] = []

        for id in ids:
            logging.info(f"{ym}-{id}-{fund_name[fund.index(id)]}")

            r = _get(ym, id, soup)
            time.sleep(1)

            soup = BeautifulSoup(r.text, 'html.parser')
            r = _get(ym, id, soup)
            time.sleep(1)

            rows = BeautifulSoup(r.text, 'html.parser').find_all('table')[3].find_all('td')[10:]
            headers = [row for row in rows
                       if 'rowspan' in row.attrs and int(row.attrs['rowspan']) > 0
                       ]

            tmps = []
            for i in range(len(headers)):

                tmp = []
                num = int(headers[i].attrs['rowspan'])
                list = rows[1:(num * 9) + 1]

                for ii in range(num):
                    v = list[ii * 9:(ii + 1) * 9]

                    tmp.append({
                        'code': v[2].text,
                        'name': v[3].text,
                        'amount': v[4].text,
                        'total': v[8].text,
                        'type': v[1].text,
                    })

                tmps.append({
                    'name': headers[i].text,
                    'data': tmp,
                })

                rows = rows[(num * 9) + 3:]

            data[ym].append({
                'name': fund_name[fund.index(id)],
                'data': tmps,
            })

    return data


def _get(ym, id, soup):
    __VIEWSTATE = soup.find('input', id='__VIEWSTATE')
    if __VIEWSTATE is None:
        __VIEWSTATE = ''
    else:
        __VIEWSTATE = __VIEWSTATE.attrs['value']

    r = requests.post("https://www.sitca.org.tw/ROC/Industry/IN2629.aspx", {
        "__EVENTTARGET": 'ctl00$ContentPlaceHolder1$ddlQ_YM',
        "__EVENTARGUMENT": '',
        "__LASTFOCUS": '',
        "__VIEWSTATE": __VIEWSTATE,
        "__VIEWSTATEGENERATOR": soup.find('input', id='__VIEWSTATEGENERATOR').attrs['value'],
        "__EVENTVALIDATION": soup.find('input', id='__EVENTVALIDATION').attrs['value'],
        "ctl00$ContentPlaceHolder1$ddlQ_YM": ym,
        "ctl00$ContentPlaceHolder1$rdo1": "rbComid",
        "ctl00$ContentPlaceHolder1$ddlQ_Comid": id,
        "ctl00$ContentPlaceHolder1$BtnQuery": "查詢",
    }, headers=HEADERS)

    if r.status_code != 200:
        return None

    return r
