import json
import requests
import pandas as pd
import time
import numpy as np
import os
import re
#from tqdm import tqdm
import datetime


# ======= 正式开始执行

# 每天早上8点05分运行
date_now = str(datetime.datetime.utcnow())[0:10]
# 引入永续合约流动性的概念
# 引入永续合约流动性的概念
url_address = ['https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_long_relative',
              'https://api.glassnode.com/v1/metrics/market/price_usd_close',
              'https://api.glassnode.com/v1/metrics/signals/btc_risk_index']
url_name = ['future','price','risk']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': 'BTC', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    #print(ins)
    ins['date'] =  ins['t']
    ins[url_name[num]] =  ins['v']
    ins = ins[['date',url_name[num]]]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
futures_data = result_data[(result_data.date>='2013-01-01')]
futures_data = futures_data.sort_values(by=['date'])
futures_data = futures_data.reset_index(drop=True)
new_futures_data = futures_data[['date','future','price','risk']]
# 读取原始数据
new_futures_data['next_price'] = new_futures_data['price'].shift(-1)
flag_1 = []
for i in range(len(new_futures_data)):
    if new_futures_data['next_price'][i] > new_futures_data['price'][i]:
        flag_1.append(1)
    else:
        flag_1.append(0)
new_futures_data['flag_1'] = flag_1

ins = new_futures_data
risk = ins['risk'][len(ins)-1]
futures_value_1 = ins['future'][len(ins)-1]
futures_value_2 = ins['future'][len(ins)-2]
futures_value_3 = ins['future'][len(ins)-3]
futures_value_4 = ins['future'][len(ins)-4]
futures_accury_2 = ins['flag_1'][len(ins)-2]
futures_accury_3 = ins['flag_1'][len(ins)-3]
futures_accury_4 = ins['flag_1'][len(ins)-4]
if futures_value_1 > 0.5 and futures_value_2 > 0.5 and futures_value_3 > 0.5 and futures_value_4 > 0.5 and futures_accury_2 + futures_accury_3 + futures_accury_4 ==0:
    pingjia = 'duotou_continue_kill'
elif futures_value_1 < 0.5 and futures_value_2 < 0.5 and futures_value_3 < 0.5 and futures_value_4 < 0.5 and futures_accury_2 == 1 and futures_accury_3 ==1 and futures_accury_4 == 1:
    pingjia = 'kongtou_continue_kill'

elif futures_value_1 > 0.5 and futures_value_2 > 0.5 and futures_value_3 > 0.5 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'duotou_finish_kill'
elif futures_value_1 > 0.5 and futures_value_2 > 0.5 and futures_accury_2 == 0:
    pingjia = 'duotou_ing_kill'
    
elif futures_value_1 < 0.5 and futures_value_2 < 0.5 and futures_value_3 < 0.5 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'kongtou_finish_kill'

elif futures_value_1 < 0.5 and futures_value_2 < 0.5 and futures_accury_2 == 0:
    pingjia = 'kongtou_ing_kill'

elif futures_value_1 > 0.5:
    pingjia = 'duotou_main'
elif futures_value_1 < 0.5:
    pingjia = 'kongtou_main'
else:
    pingjia = 'unknow_reason'

judge_res = pd.DataFrame({'date':date_now,'pingjia':pingjia,'risk':risk},index=[0])
judge_res.to_csv('res_btc_lianghua.csv')