import requests
from linebot import LineBotApi
from linebot.models import FlexSendMessage


class Api(object):
    def __init__(self, to, token):
        self.api = LineBotApi(token)
        self.to = to

    def send(self, title, text: list, header_color='#278FB3'):
        contents = []
        for v in text:
            contents.append({
                "type": "text",
                "text": v,
                "size": "md",
                "margin": "none"
            })

        self.api.push_message(
            self.to,
            FlexSendMessage(
                alt_text='通知',
                contents={
                    "type": "bubble",
                    "header": {
                        "type": "box",
                        "layout": "baseline",
                        "contents": [
                            {
                                "type": "text",
                                "text": title,
                                "size": "lg",
                                "margin": "none",
                                "align": "center"
                            }
                        ]
                    },
                    "body": {
                        "type": "box",
                        "layout": "vertical",
                        "contents": contents
                    },
                    "styles": {
                        "header": {
                            "separator": True,
                            "backgroundColor": header_color
                        },
                        "body": {
                            "backgroundColor": '#C3DAE2'
                        }
                    }
                }
            )
        )

    def sendMonthRevenue(self, text):
        self.send('月營收', [text], '#278FB3')

    def sendFinancial(self, text):
        self.send('季報', [text], '#95CD89')

    def sendFund(self, text):
        self.send('投信持股', [text], '#B5B7F8')

    def sendNews(self, text: list):
        self.send('公告', text, '#E7D6B4')

    def sendSystem(self, text):
        self.send('系統', [text], '#FFFFFF')

    def sendCb(self, text: list):
        self.send('可轉債', text, '#D6DE7B')


class Notify(object):
    def __init__(self, tokens):
        self.tokens = tokens

    def send(self, text, token):
        resp = requests.post("https://notify-api.line.me/api/notify", {
            'message': text,
        }, headers={
            'Authorization': f'Bearer {token}'
        })

        return resp.json()['status'] == 200

    def sendSystem(self, text):
        return self.send(text, self.tokens['system'])

    def sendCb(self, text):
        return self.send(text, self.tokens['cb'])

    def sendNews(self, text):
        return self.send(text, self.tokens['news'])

    def sendFinancial(self, text):
        return self.send(text, self.tokens['financial'])

    def sendFund(self, text):
        return self.send(text, self.tokens['fund'])
