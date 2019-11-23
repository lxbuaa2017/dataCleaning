import pandas as pd
import numpy as np
import pymongo as pm
import json
import re
import sys,time

from pandas import Series

df=pd.read_csv('wanfang.csv')
client=pm.MongoClient('10.251.252.10',27017)
db=client['wanfang']
collection=db['wanfang']
#定义解释器
class CleanEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o,Series):
            return str(o.array,encoding='utf-8')

# 正则表达式
fund_rz = r'[\u4e00-\u9fa5]{2,}.*|[A-Za-z]{4,}'
time_rz = r'\d{4}年\d{1,2}月\d{1,2}日'
indexID_rz = r'\d{4},.*'
units_rz = r'\d{6}.*'
count = df.shape[0]
for index, row in df.iterrows():
    if str(row.c_abstract).strip()=='nan' or str(row.c_author).strip()=='nan' or str(row.c_keywords).strip()=='nan':
        continue
    arr = []
    arr.append(str(row.time))
    arr.append(str(row.fund))
    arr.append(str(row.indexID))
    unit = str(row.units).strip()
    if(unit[0]==','):
        unit=unit[1:]
    arr.append(unit)
    for i in arr:
        if re.search(units_rz,i):
            row.units=i
            continue
        elif re.search(time_rz,i):
            row.time=i
            continue
        elif re.search(indexID_rz,i):
            row.indexID=i
            continue
        elif re.search(fund_rz,i):
            row.fund=i
            continue
    if not re.search(fund_rz,str(row.fund)):
        row.fund=''
    if not re.search(time_rz,str(row.time)):
        row.time=str(row.indexID)[0:4]+'年12月31日'
    if not re.search(units_rz,str(row.units).strip()):
        row.units=''
    row.c_title=str(row.c_title).strip()
    collection.insert_one(row.to_dict())
    p=index*100/count
    print('清洗进度 : %s [%d/%d]' % (str(round(p,2)) + '%', index + 1, count), end='\r')
    time.sleep(0.1)
# fund indexID time units