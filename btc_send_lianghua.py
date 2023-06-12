import json
import requests
import pandas as pd
import time
import numpy as np
import os
import re
#from tqdm import tqdm
import datetime
from send_email import email_sender
#=====定义函数====
from HTMLTable import HTMLTable
'''
生成html表格
传入一个dataframe, 设置一个标题， 返回一个html格式的表格
'''
def create_html_table(df, title):
    table = HTMLTable(caption=title)

    # 表头行
    table.append_header_rows((tuple(df.columns),))

    # 数据行
    for i in range(len(df.index)):
        table.append_data_rows((
            tuple(df.iloc[df.index[i],]),
        ))

    # 标题样式
    table.caption.set_style({
        'font-size': '15px',
    })

    # 表格样式，即<table>标签样式
    table.set_style({
        'border-collapse': 'collapse',
        'word-break': 'keep-all',
        'white-space': 'nowrap',
        'font-size': '14px',
    })

    # 统一设置所有单元格样式，<td>或<th>
    table.set_cell_style({
        'border-color': '#000',
        'border-width': '1px',
        'border-style': 'solid',
        'padding': '5px',
        'text-align': 'center',
    })

    # 表头样式
    table.set_header_row_style({
        'color': '#fff',
        'background-color': '#48a6fb',
        'font-size': '15px',
    })

    # 覆盖表头单元格字体样式
    table.set_header_cell_style({
        'padding': '15px',
    })

    # 调小次表头字体大小
    table[0].set_cell_style({
        'padding': '8px',
        'font-size': '15px',
    })

    html_table = table.to_html()
    return html_table

# ======= 正式开始执行

#========监控收益=======

# 读取原始数据
futures_data = pd.read_csv('/root/meaasge_pre/futures_data.csv')
futures_data = futures_data[1:]
futures_data['future'] = futures_data['value_x']
futures_data['price'] = futures_data['value_y']
futures_data['ind'] = futures_data.index
futures_data = futures_data[['ind','date','future','price']]
num_data = futures_data.groupby(['date'],as_index=False)['ind'].min()
new_futures_data = num_data.merge(futures_data,how='left',on=['date','ind'])
new_futures_data['date'] = pd.to_datetime(new_futures_data['date'])
new_futures_data = new_futures_data.sort_values(by='date')
new_futures_data = new_futures_data.reset_index(drop=True)
new_futures_data['next_price'] = new_futures_data['price'].shift(-1)
flag_1 = []
for i in range(len(new_futures_data)):
    if new_futures_data['next_price'][i] > new_futures_data['price'][i]:
        flag_1.append(1)
    else:
        flag_1.append(0)
new_futures_data['flag_1'] = flag_1
date = []
value = []
future = []
flag = []
for i in range(3,len(new_futures_data)):
    ins = new_futures_data[i-3:i]
    ins = ins.reset_index(drop=True)
    date.append(ins['date'][2])
    value.append(ins['future'][2])
    flag.append(ins['flag_1'][2])
    
    futures_value_1 = ins['future'][len(ins)-1]
    futures_value_2 = ins['future'][len(ins)-2]
    futures_value_3 = ins['future'][len(ins)-3]
    futures_accury_2 = ins['flag_1'][len(ins)-2]
    futures_accury_3 = ins['flag_1'][len(ins)-3]
    if futures_value_1 > 0.5 and futures_value_2 > 0.8 and futures_value_3 > 0.8 and futures_accury_2 == 0 and futures_accury_3 == 0:
        pingjia = 'duotou_finish_kill'
    elif futures_value_1 > 0.8 and futures_value_2 > 0.8 and futures_accury_2 == 0:
        pingjia = 'duotou_ing_kill'

    elif futures_value_1 < 0.5 and futures_value_2 < 0.2 and futures_value_3 < 0.2 and futures_accury_2 == 0 and futures_accury_3 == 0:
        pingjia = 'kongtou_finish_kill'
    elif futures_value_1 < 0.2 and futures_value_2 < 0.2 and futures_accury_2 == 0:
        pingjia = 'kongtou_ing_kill'

    elif futures_value_1 > 0.5:
        pingjia = 'duotou_main'
    elif futures_value_1 < 0.5:
        pingjia = 'kongtou_main'
    else:
        pingjia = 'unknow_reason'
    
    future.append(pingjia)
future_df = pd.DataFrame({'date':date,'value':value,'future':future,'flag':flag})
crypto = 'BTC'
url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
url_name = ['k_fold']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': crypto, 'i':'1h', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    ins['date'] =  ins['t']
    ins['value'] =  ins['o']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>='2023-03-25')]
last_data = last_data.sort_values(by=['date'])
last_data = last_data.reset_index(drop=True)
date = []
open_p = []
close_p = []
high_p = []
low_p = []
for i in range(len(last_data)):
    date.append(last_data['date'][i])
    open_p.append(last_data['value'][i]['o'])
    close_p.append(last_data['value'][i]['c'])
    high_p.append(last_data['value'][i]['h'])
    low_p.append(last_data['value'][i]['l'])
last_df = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})

last_df = last_df.sort_values(by='date')
last_df = last_df.reset_index(drop=True)
res_data = last_df
res_data['new_date'] = res_data['date'].map(lambda x: str(x)[0:10])
res_data['new_date'] = pd.to_datetime(res_data['new_date'])
res_data['hour'] = res_data['date'].map(lambda x: x.hour)
res_data
futures = []
logo = []
date_s = []
date_e = []
open_p = []
close_p = []
res = []
per = []
i = 0
num = 23
num_1 = 0.03
num_2 = 0.03
num_4 = 0.02
while i < len(future_df)-1:
    #print(i)
    judge = future_df['future'][i]
    futures.append(judge)
    sub_date = future_df['date'][i+1]
    #得到下一天的数据
    next_res_data = res_data[res_data.new_date==sub_date]
    next_res_data = next_res_data.sort_values(by=['new_date','hour'])
    next_res_data = next_res_data.reset_index(drop=True)
    #print(next_res_data)
    if judge in ('duotou_finish_kill','kongtou_ing_kill','kongtou_start_kill','duotou_main'):
        logo.append('duo')
        open_price = next_res_data['open'][0]
        start_hour = next_res_data['new_date'][0]

        open_p.append(open_price)
        date_s.append(start_hour)
        
        for j in range(1,len(next_res_data)):
            #print('====' +str(j))
            high_price = next_res_data['high'][j]
            low_price = next_res_data['low'][j]
            close_price = next_res_data['close'][j]
            close_hour = next_res_data['hour'][j]
            #print((high_price - open_price)/open_price)
            if j <= num and (high_price - open_price)/open_price >= num_1 and (low_price - open_price)/open_price > -num_2:
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(1)
                per.append(num_1)
                i += 1
                break
            elif j <= num and (low_price - open_price)/open_price <= -num_2 and (high_price - open_price)/open_price < num_1:
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(0)
                per.append(-num_2)
                i += 1
                break
            elif j <= num and (high_price - open_price)/open_price > num_4 and (high_price - open_price)/open_price < num_1:
                next_res_data['label'] = next_res_data.index
                sub_later_data_1 = next_res_data[next_res_data.label > j]
                sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                #print(sub_later_data_1)
                num_3 = len(sub_later_data_1)
                for w in range(num_3):
                    #print(w)
                    high_price = sub_later_data_1['high'][w]
                    low_price = sub_later_data_1['low'][w]
                    close_date = sub_later_data_1['hour'][w]
                    if w <= num_3-1 and (low_price - open_price)/open_price <= num_4:
                        close_p.append(low_price)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_4)

                        i += 1
                        #i += 3
                        break
                    elif w <= num_3-1 and (high_price - open_price)/open_price >= num_1:
                        close_p.append(high_price)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_1)

                        i += 1
                        break
                    elif w == num_3-1:
                        close_p.append(close_price)
                        date_e.append(close_hour)
                        if (close_price - open_price)/open_price > 0:
                            res.append(1)
                        else:
                            res.append(0)
                        per.append((close_price - open_price)/open_price)
                        i += 1
                    else:
                        continue
                break
            elif j == 23:
                close_p.append(close_price)
                date_e.append(close_hour)
                if (close_price - open_price)/open_price > 0:
                    res.append(1)
                else:
                    res.append(0)
                per.append((close_price - open_price)/open_price)
                i += 1
                break
            else:
                continue
    else:
        logo.append('kong')
        open_price = next_res_data['open'][0]
        start_hour = next_res_data['new_date'][0]

        open_p.append(open_price)
        date_s.append(start_hour)
        
        for j in range(1,len(next_res_data)):
            #print('====' +str(j))
            high_price = next_res_data['high'][j]
            low_price = next_res_data['low'][j]
            close_price = next_res_data['close'][j]
            close_hour = next_res_data['hour'][j]
            if j <= num and (low_price - open_price)/open_price <= -num_1 and (high_price - open_price)/open_price < num_2:
                close_p.append(low_price)
                date_e.append(close_hour)
                res.append(1)
                per.append(num_1)
                i += 1
                break
            elif j <= num and (high_price - open_price)/open_price >= num_2 and (low_price - open_price)/open_price > -num_1 :
                close_p.append(high_price)
                date_e.append(close_hour)
                res.append(0)
                per.append(-num_2)
                i += 1
                break
            elif j == 23:
                close_p.append(close_price)
                date_e.append(close_hour)
                if (close_price - open_price)/open_price > 0:
                    res.append(0)
                else:
                    res.append(1)
                per.append(-(close_price - open_price)/open_price)             
                i += 1
                break
            elif j <= num and (low_price - open_price)/open_price < -num_4 and (low_price - open_price)/open_price > -num_1:
                next_res_data['label'] = next_res_data.index
                sub_later_data_1 = next_res_data[next_res_data.label > j]
                sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                #print(sub_later_data_1)
                num_3 = len(sub_later_data_1)
                for w in range(num_3):
                    #print(w)
                    high_price = sub_later_data_1['high'][w]
                    low_price = sub_later_data_1['low'][w]
                    close_date = sub_later_data_1['hour'][w]
                    if w <= num_3-1 and (high_price - open_price)/open_price >= -num_4:
                        close_p.append(high_price)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_4)

                        i += 1
                        #i += 3
                        break
                    elif w <= num_3-1 and (low_price - open_price)/open_price <= -num_1:
                        close_p.append(low_price)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_1)

                        i += 1
                        break
                    elif w == num_3-1:
                        close_p.append(close_price)
                        date_e.append(close_hour)
                        if (close_price - open_price)/open_price > 0:
                            res.append(0)
                        else:
                            res.append(1)
                        per.append(-(close_price - open_price)/open_price)
                        i += 1
                    else:
                        continue
                break
            else:
                continue                

judge_res = pd.DataFrame({'futures':futures,'date_s':date_s,'date_e':date_e,'open_p':open_p,'close_p':close_p,'res':res,'per':per})
judge_res['mm'] = judge_res['date_s'].apply(lambda x: str(x)[5:7])
judge_res_1 = judge_res.groupby(['mm'],as_index=False)['per'].sum()
judge_res_1.rename(columns = {'per':'youhua_per'},inplace=True)


url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
url_name = ['k_fold']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': crypto, 'i':'1h', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    ins['date'] =  ins['t']
    ins['value'] =  ins['o']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>='2023-03-25')]
last_data = last_data.sort_values(by=['date'])
last_data = last_data.reset_index(drop=True)
date = []
open_p = []
close_p = []
high_p = []
low_p = []
for i in range(len(last_data)):
    date.append(last_data['date'][i])
    open_p.append(last_data['value'][i]['o'])
    close_p.append(last_data['value'][i]['c'])
    high_p.append(last_data['value'][i]['h'])
    low_p.append(last_data['value'][i]['l'])
last_df = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})

last_df = last_df.sort_values(by='date')
last_df = last_df.reset_index(drop=True)
res_data = last_df
res_data['new_date'] = res_data['date'].map(lambda x: str(x)[0:10])
res_data['new_date'] = pd.to_datetime(res_data['new_date'])
res_data['hour'] = res_data['date'].map(lambda x: x.hour)
res_data
futures = []
logo = []
date_s = []
date_e = []
open_p = []
close_p = []
res = []
per = []
i = 0
num = 23
num_1 = 0.03
num_2 = 0.03
num_4 = 0.02
while i < len(future_df)-1:
    #print(i)
    judge = future_df['value'][i]
    futures.append(judge)
    sub_date = future_df['date'][i+1]
    #得到下一天的数据
    next_res_data = res_data[res_data.new_date==sub_date]
    next_res_data = next_res_data.sort_values(by=['new_date','hour'])
    next_res_data = next_res_data.reset_index(drop=True)
    #print(next_res_data)
    if judge > 0.5:
        logo.append('duo')
        open_price = next_res_data['open'][0]
        start_hour = next_res_data['new_date'][0]

        open_p.append(open_price)
        date_s.append(start_hour)
        
        for j in range(1,len(next_res_data)):
            #print('====' +str(j))
            high_price = next_res_data['high'][j]
            low_price = next_res_data['low'][j]
            close_price = next_res_data['close'][j]
            close_hour = next_res_data['hour'][j]
            #print((high_price - open_price)/open_price)
            if j <= num and (high_price - open_price)/open_price >= num_1 and (low_price - open_price)/open_price > -num_2:
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(1)
                per.append(num_1)
                i += 1
                break
            elif j <= num and (low_price - open_price)/open_price <= -num_2 and (high_price - open_price)/open_price < num_1:
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(0)
                per.append(-num_2)
                i += 1
                break
            elif j <= num and (high_price - open_price)/open_price > num_4 and (high_price - open_price)/open_price < num_1:
                next_res_data['label'] = next_res_data.index
                sub_later_data_1 = next_res_data[next_res_data.label > j]
                sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                #print(sub_later_data_1)
                num_3 = len(sub_later_data_1)
                for w in range(num_3):
                    #print(w)
                    high_price = sub_later_data_1['high'][w]
                    low_price = sub_later_data_1['low'][w]
                    close_date = sub_later_data_1['hour'][w]
                    if w <= num_3-1 and (low_price - open_price)/open_price <= num_4:
                        close_p.append(open_price*1.02)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_4)

                        i += 1
                        #i += 3
                        break
                    elif w <= num_3-1 and (high_price - open_price)/open_price >= num_1:
                        close_p.append(close_price*1.03)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_1)

                        i += 1
                        break
                    elif w == num_3-1:
                        close_p.append(close_price)
                        date_e.append(close_hour)
                        if (close_price - open_price)/open_price > 0:
                            res.append(1)
                        else:
                            res.append(0)
                        per.append((close_price - open_price)/open_price)
                        i += 1
                    else:
                        continue
                break
            elif j == 23:
                close_p.append(close_price)
                date_e.append(close_hour)
                if (close_price - open_price)/open_price > 0:
                    res.append(1)
                else:
                    res.append(0)
                per.append((close_price - open_price)/open_price)
                i += 1
                break
            else:
                continue
    else:
        logo.append('kong')
        open_price = next_res_data['open'][0]
        start_hour = next_res_data['new_date'][0]

        open_p.append(open_price)
        date_s.append(start_hour)
        
        for j in range(1,len(next_res_data)):
            #print('====' +str(j))
            high_price = next_res_data['high'][j]
            low_price = next_res_data['low'][j]
            close_price = next_res_data['close'][j]
            close_hour = next_res_data['hour'][j]
            if j <= num and (low_price - open_price)/open_price <= -num_1 and (high_price - open_price)/open_price < num_2:
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(1)
                per.append(num_1)
                i += 1
                break
            elif j <= num and (high_price - open_price)/open_price >= num_2 and (low_price - open_price)/open_price > -num_1 :
                close_p.append(close_price)
                date_e.append(close_hour)
                res.append(0)
                per.append(-num_2)
                i += 1
                break
            elif j == 23:
                close_p.append(close_price)
                date_e.append(close_hour)
                if (close_price - open_price)/open_price > 0:
                    res.append(0)
                else:
                    res.append(1)
                per.append(-(close_price - open_price)/open_price)             
                i += 1
                break
            elif j <= num and (low_price - open_price)/open_price < -num_4 and (low_price - open_price)/open_price > -num_1:
                next_res_data['label'] = next_res_data.index
                sub_later_data_1 = next_res_data[next_res_data.label > j]
                sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                #print(sub_later_data_1)
                num_3 = len(sub_later_data_1)
                for w in range(num_3):
                    #print(w)
                    high_price = sub_later_data_1['high'][w]
                    low_price = sub_later_data_1['low'][w]
                    close_date = sub_later_data_1['hour'][w]
                    if w <= num_3-1 and (high_price - open_price)/open_price >= -num_4:
                        close_p.append(open_price*1.01)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_4)

                        i += 1
                        #i += 3
                        break
                    elif w <= num_3-1 and (low_price - open_price)/open_price <= -num_1:
                        close_p.append(close_price*1.03)
                        date_e.append(close_date)
                        res.append(1)
                        per.append(num_1)

                        i += 1
                        break
                    elif w == num_3-1:
                        close_p.append(close_price)
                        date_e.append(close_hour)
                        if (close_price - open_price)/open_price > 0:
                            res.append(0)
                        else:
                            res.append(1)
                        per.append(-(close_price - open_price)/open_price)
                        i += 1
                    else:
                        continue
                break
            else:
                continue                

judge_res = pd.DataFrame({'futures':futures,'date_s':date_s,'date_e':date_e,'open_p':open_p,'close_p':close_p,'res':res,'per':per})
judge_res['mm'] = judge_res['date_s'].apply(lambda x: str(x)[5:7])
judge_res_2 = judge_res.groupby(['mm'],as_index=False)['per'].sum()
judge_res_2.rename(columns = {'per':'raw_per'},inplace=True)


url_address = [ 'https://api.glassnode.com/v1/metrics/market/price_usd_ohlc']
url_name = ['k_fold']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    print(num)
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': crypto, 'i':'1h', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    #ins.to_csv('test.csv')
    #print(ins['o'])
    ins['date'] =  ins['t']
    ins['value'] =  ins['o']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>='2023-03-25')]
last_data = last_data.sort_values(by=['date'])
last_data = last_data.reset_index(drop=True)
date = []
open_p = []
close_p = []
high_p = []
low_p = []
for i in range(len(last_data)):
    date.append(last_data['date'][i])
    open_p.append(last_data['value'][i]['o'])
    close_p.append(last_data['value'][i]['c'])
    high_p.append(last_data['value'][i]['h'])
    low_p.append(last_data['value'][i]['l'])
last_df = pd.DataFrame({'date':date,'open':open_p,'close':close_p,'high':high_p,'low':low_p})

last_df = last_df.sort_values(by='date')
last_df = last_df.reset_index(drop=True)
res_data = last_df
res_data['new_date'] = res_data['date'].map(lambda x: str(x)[0:10])
res_data['new_date'] = pd.to_datetime(res_data['new_date'])
res_data['hour'] = res_data['date'].map(lambda x: x.hour)

judge_res_3 = pd.DataFrame()
for p in range(30):
    futures = []
    logo = []
    date_s = []
    date_e = []
    open_p = []
    close_p = []
    res = []
    per = []
    i = 0
    num = 23
    num_1 = 0.03
    num_2 = 0.03
    num_4 = 0.02
    import random
    while i < len(future_df)-1:
        #print(i)
        judge = random.choice([0,1])
        futures.append(judge)
        sub_date = future_df['date'][i+1]
        #得到下一天的数据
        next_res_data = res_data[res_data.new_date==sub_date]
        next_res_data = next_res_data.sort_values(by=['new_date','hour'])
        next_res_data = next_res_data.reset_index(drop=True)
        #print(next_res_data)
        if judge == 1:
            logo.append('duo')
            open_price = next_res_data['open'][0]
            start_hour = next_res_data['new_date'][0]

            open_p.append(open_price)
            date_s.append(start_hour)

            for j in range(1,len(next_res_data)):
                #print('====' +str(j))
                high_price = next_res_data['high'][j]
                low_price = next_res_data['low'][j]
                close_price = next_res_data['close'][j]
                close_hour = next_res_data['hour'][j]
                #print((high_price - open_price)/open_price)
                if j <= num and (high_price - open_price)/open_price >= num_1 and (low_price - open_price)/open_price > -num_2:
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    res.append(1)
                    per.append(num_1)
                    i += 1
                    break
                elif j <= num and (low_price - open_price)/open_price <= -num_2 and (high_price - open_price)/open_price < num_1:
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    res.append(0)
                    per.append(-num_2)
                    i += 1
                    break
                elif j <= num and (high_price - open_price)/open_price > num_4 and (high_price - open_price)/open_price < num_1:
                    next_res_data['label'] = next_res_data.index
                    sub_later_data_1 = next_res_data[next_res_data.label > j]
                    sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                    sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                    #print(sub_later_data_1)
                    num_3 = len(sub_later_data_1)
                    for w in range(num_3):
                        #print(w)
                        high_price = sub_later_data_1['high'][w]
                        low_price = sub_later_data_1['low'][w]
                        close_date = sub_later_data_1['hour'][w]
                        if w <= num_3-1 and (low_price - open_price)/open_price <= num_4:
                            close_p.append(open_price*1.02)
                            date_e.append(close_date)
                            res.append(1)
                            per.append(num_4)

                            i += 1
                            #i += 3
                            break
                        elif w <= num_3-1 and (high_price - open_price)/open_price >= num_1:
                            close_p.append(close_price*1.03)
                            date_e.append(close_date)
                            res.append(1)
                            per.append(num_1)

                            i += 1
                            break
                        elif w == num_3-1:
                            close_p.append(close_price)
                            date_e.append(close_hour)
                            if (close_price - open_price)/open_price > 0:
                                res.append(1)
                            else:
                                res.append(0)
                            per.append((close_price - open_price)/open_price)
                            i += 1
                        else:
                            continue
                    break
                elif j == 23:
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    if (close_price - open_price)/open_price > 0:
                        res.append(1)
                    else:
                        res.append(0)
                    per.append((close_price - open_price)/open_price)
                    i += 1
                    break
                else:
                    continue
        else:
            logo.append('kong')
            open_price = next_res_data['open'][0]
            start_hour = next_res_data['new_date'][0]

            open_p.append(open_price)
            date_s.append(start_hour)

            for j in range(1,len(next_res_data)):
                #print('====' +str(j))
                high_price = next_res_data['high'][j]
                low_price = next_res_data['low'][j]
                close_price = next_res_data['close'][j]
                close_hour = next_res_data['hour'][j]
                if j <= num and (low_price - open_price)/open_price <= -num_1 and (high_price - open_price)/open_price < num_2:
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    res.append(1)
                    per.append(num_1)
                    i += 1
                    break
                elif j <= num and (high_price - open_price)/open_price >= num_2 and (low_price - open_price)/open_price > -num_1 :
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    res.append(0)
                    per.append(-num_2)
                    i += 1
                    break
                elif j == 23:
                    close_p.append(close_price)
                    date_e.append(close_hour)
                    if (close_price - open_price)/open_price > 0:
                        res.append(0)
                    else:
                        res.append(1)
                    per.append(-(close_price - open_price)/open_price)             
                    i += 1
                    break
                elif j <= num and (low_price - open_price)/open_price < -num_4 and (low_price - open_price)/open_price > -num_1:
                    next_res_data['label'] = next_res_data.index
                    sub_later_data_1 = next_res_data[next_res_data.label > j]
                    sub_later_data_1 = sub_later_data_1.sort_values(by=['new_date','hour'])
                    sub_later_data_1 = sub_later_data_1.reset_index(drop=True)
                    #print(sub_later_data_1)
                    num_3 = len(sub_later_data_1)
                    for w in range(num_3):
                        #print(w)
                        high_price = sub_later_data_1['high'][w]
                        low_price = sub_later_data_1['low'][w]
                        close_date = sub_later_data_1['hour'][w]
                        if w <= num_3-1 and (high_price - open_price)/open_price >= -num_4:
                            close_p.append(open_price*1.01)
                            date_e.append(close_date)
                            res.append(1)
                            per.append(num_4)

                            i += 1
                            #i += 3
                            break
                        elif w <= num_3-1 and (low_price - open_price)/open_price <= -num_1:
                            close_p.append(close_price*1.03)
                            date_e.append(close_date)
                            res.append(1)
                            per.append(num_1)

                            i += 1
                            break
                        elif w == num_3-1:
                            close_p.append(close_price)
                            date_e.append(close_hour)
                            if (close_price - open_price)/open_price > 0:
                                res.append(0)
                            else:
                                res.append(1)
                            per.append(-(close_price - open_price)/open_price)
                            i += 1
                        else:
                            continue
                    break
                else:
                    continue                

    judge_res = pd.DataFrame({'futures':futures,'date_s':date_s,'date_e':date_e,'open_p':open_p,'close_p':close_p,'res':res,'per':per})
    judge_res['mm'] = judge_res['date_s'].apply(lambda x: str(x)[5:7])
    judge_re_sub = judge_res.groupby(['mm'],as_index=False)['per'].sum()
    judge_res_3 = pd.concat([judge_res_3,judge_re_sub])
judge_res_3 = judge_res_3.groupby(['mm'],as_index=False)['per'].mean()
judge_res_3.rename(columns = {'per':'random_per'},inplace=True)

judge_res_last = judge_res_1.merge(judge_res_2,how='left',on=['mm']).merge(judge_res_3,how='left',on=['mm'])

#======自动发邮件
content = create_html_table(judge_res_last, f'多空比合约判断日期{date_now}')
#设置服务器所需信息
#163邮箱服务器地址
mail_host = 'smtp.163.com'  
#163用户名
mail_user = 'lee_daowei@163.com'  
#密码(部分邮箱为授权码) 
mail_pass = 'GKXGKVGTYBGRMAVE'   
#邮件发送方邮箱地址
sender = 'lee_daowei@163.com'  

#邮件接受方邮箱地址，注意需要[]包裹，这意味着你可以写多个邮件地址群发
receivers = ['lee_daowei@163.com']  
context = f'多空比和溢价率btc合约{date_now}'
email_sender(mail_host,mail_user,mail_pass,sender,receivers,context,content)


















































