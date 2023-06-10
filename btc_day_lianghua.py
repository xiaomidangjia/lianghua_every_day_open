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

# 每天早上8点05分运行
# 得到时间标准时时间
date_now = str(datetime.datetime.utcnow())[0:10]
date_before = pd.to_datetime(date_now) - datetime.timedelta(days=8)
# 读取原始数据
raw_data = pd.read_csv('/root/usdt/usdt.csv')
raw_data = raw_data[2:]
raw_data['date'] = raw_data['date'].apply(lambda x: str(x)[6:10] + '/' + str(x)[3:5] + '/' + str(x)[0:2] + ' ' + str(x)[11:19])
raw_data['date'] = pd.to_datetime(raw_data['date'])
raw_data['date'] = raw_data['date'] - datetime.timedelta(hours=8)
#取最近7天的数据
raw_data = raw_data[(raw_data.date >= date_before) & (raw_data.date < date_now)]
raw_data['date_dd'] = raw_data['date'].apply(lambda x: str(x)[0:10])
raw_data['date_hh'] = raw_data['date'].apply(lambda x: str(x)[11:13])
raw_data['per'] = raw_data['usdt']/raw_data['usd']
raw_data = raw_data[['date_dd','date_hh','per']]
raw_data = raw_data.drop_duplicates()
raw_data = raw_data.groupby(['date_dd','date_hh'],as_index=False)['per'].mean()

raw_data = raw_data.sort_values(by=['date_dd','date_hh'])
raw_data = raw_data.reset_index(drop=True)
raw_data['per_1'] = raw_data['per'].shift(1)
raw_data['per_2'] = raw_data['per'].shift(2)
raw_data = raw_data.fillna(0)
per_last = []
for i in range(len(raw_data)):
    #print(i,raw_data['per'][i])
    if raw_data['per'][i] > 0:
        per_last.append(raw_data['per'][i])
    elif raw_data['per'][i] == 0:
        per_last.append(raw_data['per_1'][i])
    elif raw_data['per_1'][i] == 0:
        per_last.append(raw_data['per_2'][i])
    else:
        per_last.append(raw_data['per'][i])
raw_data['per_last'] = per_last
raw_data = raw_data[['date_dd','date_hh','per_last']]
raw_data
url_address = ['https://api.glassnode.com/v1/metrics/market/price_usd_close']
url_name = ['Price']
# insert your API key here
API_KEY = '26BLocpWTcSU7sgqDdKzMHMpJDm'
data_list = []
for num in range(len(url_name)):
    addr = url_address[num]
    name = url_name[num]
    # make API request
    res_addr = requests.get(addr,params={'a': 'BTC','i':'1h', 'api_key': API_KEY})
    # convert to pandas dataframe
    ins = pd.read_json(res_addr.text, convert_dates=['t'])
    ins['date'] =  ins['t']
    ins[name] =  ins['v']
    ins = ins[['date',name]]
    data_list.append(ins)

result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
last_data = result_data[(result_data.date>='2023-05-01')]
last_data = last_data.sort_values(by=['date'])
last_data = last_data.reset_index(drop=True)
last_data['date_dd'] = last_data['date'].apply(lambda x: str(x)[0:10])
last_data['date_hh'] = last_data['date'].apply(lambda x: str(x)[11:13])
next_df = raw_data.merge(last_data,how='left',on=['date_dd','date_hh'])
next_df = next_df[next_df.date >= '2023-06-02']
next_df = next_df.reset_index(drop=True)
date = []
price = []
yijia = []
num = 4
for i in range(num,len(next_df)):
    ins = next_df[i-num:i]
    ins = ins.sort_values(by=['date'])
    ins = ins.reset_index(drop=True)
    date.append(ins['date'][num-1])
    price.append(ins['Price'][num-1])
    yijia.append(np.mean(ins['per_last']))
test_df = pd.DataFrame({'date':date,'price':price,'yijia':yijia}) 

num = []
corr = []
for i in range(4,48):
    name1 = 'yijia_' + str(i)
    test_df[name1] = test_df['yijia'].shift(i) 
    name2 = 'date_' + str(i)
    test_df[name2] = test_df['date'].shift(i)     
    df = test_df.dropna()
    num.append(i)
    corr.append(np.corrcoef(list(df['price']),list(df[name1]))[0][1])

corr_df = pd.DataFrame({'num':num,'corr':corr})
corr_df = corr_df.sort_values(by='corr',ascending=False)
corr_df = corr_df.reset_index(drop=True)
corr_df = corr_df[0:3]
flag = 0
for i in range(3):
    if corr_df['num'][i] in (22,23,24) and corr_df['corr'][i] > 0.75:
        flag += 1
    else:
        flag += 0
if flag > 0 and len(raw_data) > 153:
    # 说明23小时的延迟，相关性大于0.75是有用的，结合 多空比
    test_df_1 = test_df[test_df.date>test_df['date_23'][len(test_df)-1]]
    test_df_1['new_date'] = pd.to_datetime(test_df_1['date']) + datetime.timedelta(hours=23)
    test_df_2 = test_df_1[['new_date','yijia']]
    date_max_min = list(test_df_2['new_date'])
    yijian_max_min = list(test_df_2['yijia'])

    max_value_time = date_max_min[yijian_max_min.index(max(yijian_max_min))]
    min_value_time = date_max_min[yijian_max_min.index(min(yijian_max_min))]
    if max_value_time < min_value_time:
        logo = 'pre_high_next_low'
    else:
        logo = 'pre_low_next_high'
    usdt_logo = 1
elif len(raw_data) <= 153:
    usdt_logo = 0
    logo = 'data not all'
    max_value_time = '2099-12-31 23:59:59'
    min_value_time = '2099-12-31 23:59:59'
else:
    usdt_logo = 0
    logo = 'no > 0.75'
    max_value_time = '2099-12-31 23:59:59'
    min_value_time = '2099-12-31 23:59:59'
# 引入永续合约流动性的概念
url_address = ['https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_long_relative',
              'https://api.glassnode.com/v1/metrics/market/price_usd_close']
url_name = ['futures','price']
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
    ins['value'] =  ins['v']
    ins = ins[['date','value']]
    data_list.append(ins)
result_data = data_list[0][['date']]
for i in range(len(data_list)):
    df = data_list[i]
    result_data = result_data.merge(df,how='left',on='date')
#last_data = result_data[(result_data.date>='2016-01-01') & (result_data.date<='2020-01-01')]
futures_data = result_data[(result_data.date>='2013-01-01')]
futures_data = futures_data.sort_values(by=['date'])
futures_data = futures_data.reset_index(drop=True)
futures_data['next_value'] = futures_data['value_y'].shift(-1)
flag_1 = []
flag_2 = []
flag_3 = []
for i in range(len(futures_data)):
    if futures_data['next_value'][i] > futures_data['value_y'][i]:
        flag_1.append(1)
    else:
        flag_1.append(0)
    if futures_data['value_x'][i] > 0.5:
        flag_2.append(1)
    else:
        flag_2.append(0)
futures_data['flag_1'] = flag_1
futures_data['flag_2'] = flag_2
for i in range(len(futures_data)):
    if futures_data['flag_1'][i] == futures_data['flag_2'][i]:
        flag_3.append(1)
    else:
        flag_3.append(0)
futures_data['flag_3'] = flag_3
futures_value_1 = futures_data['value_x'][len(futures_data)-1]
futures_value_2 = futures_data['value_x'][len(futures_data)-2]
futures_value_3 = futures_data['value_x'][len(futures_data)-3]
futures_accury_2 = futures_data['flag_3'][len(futures_data)-2]
futures_accury_3 = futures_data['flag_3'][len(futures_data)-3]
if futures_value_1 > 0.5 and futures_value_2 > 0.8 and futures_value_3 > 0.8 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'duotou_finish_kill'
elif futures_value_1 > 0.8 and futures_value_2 > 0.8 and futures_accury_2 == 0:
    pingjia = 'duotou_ing_kill'
elif futures_value_1 > 0.8 and futures_value_2 < 0.5 and futures_accury_2 == 1:
    pingjia = 'duotou_start_kill'
elif futures_value_1 < 0.5 and futures_value_2 < 0.2 and futures_value_3 < 0.2 and futures_accury_2 == 0 and futures_accury_3 == 0:
    pingjia = 'kongtou_finish_kill'
elif futures_value_1 < 0.2 and futures_value_2 < 0.2 and futures_accury_2 == 0:
    pingjia = 'kongtou_ing_kill'
elif futures_value_1 < 0.2 and futures_value_2 > 0.5 and futures_accury_2 == 1:
    pingjia = 'kongtou_start_kill'
elif futures_value_1 > 0.5:
    pingjia = 'duotou_main'
elif futures_value_1 < 0.5:
    pingjia = 'kongtou_main'
else:
    pingjia = 'unknow_reason' 



judge_res = pd.DataFrame({'date':date_now,'pingjia':pingjia,'usdt_logo':usdt_logo,'logo':logo,'max_value_time':max_value_time,'min_value_time':min_value_time},index=[0])
corr_df.rename(columns = {'num':'usdt_logo','corr':'logo'},inplace=True)
print(corr_df)
print(judge_res)

judge_res_l = pd.concat([judge_res,corr_df])


judge_res.to_csv('res_btc_lianghua.csv')
#======自动发邮件
content = create_html_table(judge_res_l, f'多空比和溢价率btc合约判断日期{date_now}')
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


















































