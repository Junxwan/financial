import pandas as pd

ticks = pd.read_csv('F:\\data\\print\\tick\\8155.TW.csv', encoding='big5-hkscs')

# 內外盤量
volume1 = 0
volume0 = 0

# 主力多空量
value1 = 0
value0 = 0

volumeFirst = 0
volume1b = 0
volume0b = 0

main = 0

totalVolume = 0

for time, tick in ticks.groupby('時間'):
    group = []
    group1 = []
    group.append(group1)

    for i, t in tick.iterrows():
        totalVolume += t['單量']

        if t['內外盤'] == 0 or t['戳合'] == -1:
            volumeFirst = t['單量']
            continue

        if t['內外盤'] == 1:
            volume1 += t['單量']
        elif t['內外盤'] == -1:
            volume0 += t['單量']

        if t['戳合'] == 0:
            group.append([t])
        else:
            group[0].append(t)

    for g in group:
        if len(g) == 0:
            continue

        value = 0
        volume = 0
        for t in g:
            value += t['成交'] * 1000 * t['單量']
            volume += t['單量']

        # https://www.xq.com.tw/%e5%8f%b0%e8%82%a1%e9%80%90%e7%ad%86%e5%8a%9f%e8%83%bd%e8%a1%8c%e6%83%85%e7%ab%af%e7%9b%b8%e9%97%9c%e7%95%b0%e5%8b%95/
        f = ''
        if value > 120000:
            if g[0]['內外盤'] == 1:
                value1 += volume
            elif g[0]['內外盤'] == -1:
                value0 += volume

            f = '大單'
            if value > 2000000:
                f = '特大單'

        volume1b = round((volume1 / (totalVolume - volumeFirst)) * 100)
        volume0b = round((volume0 / (totalVolume - volumeFirst)) * 100)
        main = round(((value1 - value0) / totalVolume) * 100)
        print(
            f"({f}) 時間: {int(time)} 內盤%: {volume1b} 外盤%: {volume0b} 主力道: {main}/{(value1 - value0)} 總量: {totalVolume}/{volume1}/{volume0}")
