import logging
import os
import smtplib
import pytz
import pymysql
import crawler.news as news
import crawler.twse as twse
from models import models
from jinja2 import Template
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _read_template(html):
    with open(os.path.join(f"{os.environ['LAMBDA_TASK_ROOT']}/template/{html}")) as template:
        return template.read()


def render(html, **kwargs):
    return Template(
        _read_template(html)
    ).render(**kwargs)


def main(event, context):
    tz = pytz.timezone(os.environ['TIMEZONE'])
    now = datetime.now(tz)
    date = (now - timedelta(hours=event['hours'])).strftime("%Y-%m-%d %H:%M:%S")

    logger.info('start news Event: %s', event)

    data = [
        ['聯合報-產經', news.udn('6644', date)],
        ['聯合報-股市', news.udn('6645', date)],
        ['中時', news.chinatimes(date)],
        ['中時-財經要聞', news.chinatimes_newspapers(date)],
        ['科技新報', news.technews(date)],
        ['經濟日報-產業熱點', news.money_udn('5591', '5612', date)],
        ['經濟日報-生技醫藥', news.money_udn('5591', '10161', date)],
        ['經濟日報-企業CEO', news.money_udn('5591', '5649', date)],
        ['經濟日報-總經趨勢', news.money_udn('10846', '10869', date)],
        ['經濟日報-2021投資前瞻', news.money_udn('10846', '121887', date)],
        ['經濟日報-國際焦點', news.money_udn('5588', '5599', date)],
        ['經濟日報-美中貿易戰', news.money_udn('5588', '10511', date)],
        ['經濟日報-金融脈動', news.money_udn('12017', '5613', date)],
        ['經濟日報-市場焦點', news.money_udn('5590', '5607', date)],
        ['經濟日報-集中市場', news.money_udn('5590', '5710', date)],
        ['經濟日報-櫃買市場', news.money_udn('5590', '11074', date)],
        ['經濟日報-國際期貨', news.money_udn('11111', '11114', date)],
        ['經濟日報-國際綜合', news.money_udn('12925', '121854', date)],
        ['經濟日報-外媒解析', news.money_udn('12925', '12937', date)],
        ['經濟日報-產業動態', news.money_udn('12925', '121852', date)],
        ['經濟日報-產業分析', news.money_udn('12925', '12989', date)],
        ['工商時報-產業', news.ctee(date, 'industry')],
        ['工商時報-科技', news.ctee(date, 'tech')],
        ['工商時報-國際', news.ctee(date, 'global')],
        ['工商時報-兩岸', news.ctee(date, 'china')],
        ['鉅亨網-台股', news.cnyes(date, 'tw_stock')],
        ['鉅亨網-國際股', news.cnyes(date, 'wd_stock')],
        ['自由時報-國際財經', news.ltn(date, 'international')],
        ['自由時報-證券產業', news.ltn(date, 'securities')],
        ['moneydj-頭條新聞', news.moneydj(date, 'mb010000')],
        ['moneydj-總體經濟', news.moneydj(date, 'mb020000')],
        ['moneydj-債券市場', news.moneydj(date, 'mb040200')],
        ['moneydj-產業情報', news.moneydj(date, 'mb07')],
        ['東森新聞-財經新聞台股', news.ebc(date, 'stock')],
        ['trendforce', news.trendforce(date)],
        ['dramx', news.dramx(date)],
        ['digitimes-報導總欄', news.digitimes(date)],
    ]

    logger.info('get news ok')

    if event['save']:
        db = event['databases']
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
            logger.error('insert error ' + date.__str__())
        else:
            session.commit()

    sec = datetime.now(tz).timestamp() - now.timestamp()

    now = now.strftime("%Y-%m-%d %H:%M:%S")

    html = render('email.html',
                  news=[{'title': v[0], 'news': v[1]} for v in data],
                  date=now,
                  end_date=date,
                  )

    content = MIMEMultipart()
    content["from"] = event['login_email']
    content["subject"] = f"財經新聞-{now} ({int(sec)})"
    content["to"] = event['email']
    content.attach(MIMEText(html, 'html'))

    with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:
        try:
            smtp.ehlo()  # 驗證SMTP伺服器
            smtp.starttls()  # 建立加密傳輸
            smtp.login(event['login_email'], os.environ['LOGIN_PWD'])
            smtp.send_message(content)
            logger.info('set news email ok')
        except Exception as e:
            logger.error(f"set news email error {e.__str__()}")