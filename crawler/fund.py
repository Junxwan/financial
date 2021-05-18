import time

import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'

HEADERS = {
    'User-Agent': USER_AGENT,
}


def get(ym=None, id=None):
    r = requests.get("https://www.sitca.org.tw/ROC/Industry/IN2629.aspx", headers=HEADERS)

    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, 'html.parser')
    select = soup.find_all('select')

    dates = [v.attrs['value'] for v in select[0].find_all('option')]
    fund = [v.attrs['value'] for v in select[1].find_all('option')]
    fund_name = [v.text.split(' ')[1] for v in select[1].find_all('option')]

    if ym is None:
        yms = dates
    else:
        yms = [ym]

    if id is None:
        ids = fund
    else:
        ids = [id]

    __VIEWSTATE = soup.find('input', id='__VIEWSTATE').attrs['value']
    __VIEWSTATEGENERATOR = soup.find('input', id='__VIEWSTATEGENERATOR').attrs['value']
    __EVENTVALIDATION = soup.find('input', id='__EVENTVALIDATION').attrs['value']

    data = {}
    for ym in yms[::-1]:
        data[ym] = []
        for id in ids:
            r = requests.post("https://www.sitca.org.tw/ROC/Industry/IN2629.aspx", data={
                "__EVENTTARGET": '',
                "__EVENTARGUMENT": '',
                "__LASTFOCUS": '',
                "__VIEWSTATE": __VIEWSTATE,
                "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                "__EVENTVALIDATION": __EVENTVALIDATION,
                "ctl00$ContentPlaceHolder1$ddlQ_YM": ym,
                "ctl00$ContentPlaceHolder1$rdo1": "rbComid",
                "ctl00$ContentPlaceHolder1$ddlQ_Comid": id,
                "ctl00$ContentPlaceHolder1$BtnQuery": "查詢",
            }, headers={
                'User-Agent': USER_AGENT,
                'Host': "www.sitca.org.tw",
                'Referer': "https://www.sitca.org.tw/ROC/Industry/IN2629.aspx?pid=IN22601_04",
                'Cookie': "ASP.NET_SessionId=oukiunueixjjn5xycsmxcead",
            })

            if r.status_code != 200:
                return None

            time.sleep(2)

            rows = BeautifulSoup(r.text, 'html.parser').find_all('table')[3].find_all('tr')[1:]
            headers = [row.find('td') for row in rows
                       if 'rowspan' in row.find('td').attrs and int(row.find('td').attrs['rowspan']) > 0
                       ]

            index = 0
            tmps = []

            for row in headers:
                offset = int(row.attrs['rowspan']) + index
                index += 1

                tmp = []
                for v in rows[index:offset]:
                    tds = v.find_all('td')
                    tmp.append({
                        'code': tds[2].text,
                        'name': tds[3].text,
                        'amount': tds[4].text,
                        'total': tds[8].text,
                        'type': tds[1].text,
                    })

                tmps.append({
                    'name': row.text,
                    'data': tmp,
                })

                index += int(row.attrs['rowspan'])

            data[ym].append({
                'name': fund_name[fund.index(id)],
                'data': tmps,
            })

    return data
