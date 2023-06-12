
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
    res_data['date'] = pd.to_datetime(res_data['date'])
    #res_data['up_date'] = pd.to_datetime(res_data['up_date'])
    res_data = res_data[res_data.date==pd.to_datetime(date)]

    if len(res_data) == 0:
        r_pingjia = 'error'
    else:   
        r_pingjia = res_data['pingjia'][0]

    res_dict = {'pingjia':r_pingjia}

    ans_str = json.dumps(res_dict)
    return ans_str

if __name__ == '__main__':
    app.run("0.0.0.0", port=80)


