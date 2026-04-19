import numpy as np
import json
from requests.auth import HTTPBasicAuth
import base64
import datetime
import hashlib
import hmac
from typing import List, Optional
from urllib.parse import quote_plus
import traceback
# from myself_public.public_function import *

import time
import pandas as pd
import requests
import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, Float, Integer, DECIMAL, BigInteger
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql

def jiekou_data(sys_name, db_name, table_name):
    token_url = 'http://oauth.java.yibainetwork.com/oauth/token?grant_type=client_credentials'
    while 1:
        try:
            resp = requests.post(token_url, auth=HTTPBasicAuth('prod_data_mgn', 'mgnkk7cytdsD'), timeout=(30, 30))
            break
        except:
            print('获取token接口失败，重新链接')
    token = json.loads(resp.text)['access_token']

    url = 'http://rest.java.yibainetwork.com/data/yibaiLazadaQa/tableApiPages?access_token={}'.format(token)

    final_data = pd.DataFrame()
    for i in range(1, 100):
        print(i)
        data = {'sys_name': sys_name, 'db_name': db_name, 'table_name': table_name, 'page': i}
        down1 = requests.post(url, json=data).json()
        # print("down1",down1)
        print("down1的长度为:", len(down1))
        print('down1的key值:', down1.keys())
        if down1["data"] != None:
            temp = pd.DataFrame(down1['data']['data'])
            if len(temp) == 0:
                break
            final_data = final_data.append(temp, ignore_index=True)

    final_data = final_data.drop_duplicates()

    return final_data


def get_data():
    df = jiekou_data(sys_name='amazonkd_yb_s', db_name='yibai_product', table_name='amazon_coupon_third')
    print(df.head())
    df['end_date'] = pd.to_datetime(df['end_date'].str.replace("T", "\t"))
    df['start_date'] = pd.to_datetime(df['start_date'].str.replace('T', '\t'))
    # df.to_excel('coupon数据.xlsx')
    pd_to_sql(df, database="monitor_process_data", table='amazon_coupon_third', if_exists='replace', data_sys='数据部服务器')
    print(10)
    return df


if __name__ == "__main__":
    get_data()
