
# coding: utf-8

import json
import base64
from flask import Flask, request
import numpy as np
import pandas as pd
import csv

app = Flask(__name__)


@app.route("/lianghua_pre", methods=['post'])
def crypto_pre():
    date = request.form.get('date')

    p = []
    with open("/root/lianghua_every_day_open/res_btc_lianghua.csv", 'r', encoding="UTF-8") as fr:
        reader = csv.reader(fr)
        for index, line in enumerate(reader):
            if index == 0:
                continue
            p.append(line)
    res_data = pd.DataFrame(p)
    res_data['date'] = res_data.iloc[:,1]
    res_data['pingjia'] = res_data.iloc[:,2]
    res_data['usdt_logo'] = res_data.iloc[:,3]
    res_data['logo'] = res_data.iloc[:,4]
    res_data['max_value_time'] = res_data.iloc[:,5]
    res_data['min_value_time'] = res_data.iloc[:,6]
    res_data['date'] = pd.to_datetime(res_data['date'])
    #res_data['up_date'] = pd.to_datetime(res_data['up_date'])
    res_data = res_data[res_data.date==pd.to_datetime(date)]

    if len(res_data) == 0:
        r_pingjia = 'error'
        usdt_logo = 0
        logo = 0
        max_value_time = 0
        min_value_time = 0
    else:   
        r_pingjia = res_data['pingjia'][0]
        usdt_logo = res_data['usdt_logo'][0]
        logo = res_data['logo'][0]
        max_value_time = res_data['max_value_time'][0]
        min_value_time = res_data['min_value_time'][0]

    res_dict = {'pingjia':r_pingjia,'usdt_logo':usdt_logo,'logo':logo,'max_value_time':max_value_time,'min_value_time':min_value_time}

    ans_str = json.dumps(res_dict)
    return ans_str

if __name__ == '__main__':
    app.run("0.0.0.0", port=80)


