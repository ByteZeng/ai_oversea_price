import os
import time
import random
import string
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
import json
import requests
import hashlib
from all_auto_task.dingding import send_msg


# 写数据库时，变更数据类型
def mapping_df_types(df):
    d_type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            d_type_dict.update({i: VARCHAR(length=1000)})
        if "float" in str(j):
            d_type_dict.update({i: DECIMAL(precision=20, scale=4, asdecimal=True)})
        if "int" in str(j):
            if df[i].max() < 2147483647:
                d_type_dict.update({i: Integer()})
            else:
                d_type_dict.update({i: BigInteger()})
    return d_type_dict


# 数据部仓库
def sql_to_pd(database='', sql=''):
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
            'whpro', 'ki&#qXxzkRdgz', '139.9.206.7', 3306, database, 'utf8'))
    conn = engine.connect()  # 创建连接
    df = pd.read_sql(sql, conn)
    conn.close()
    return df




def get_token():
    """获取java接口token"""
    java_url = 'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java'
    response = requests.get(java_url)
    java_token = json.loads(response.text)['data']
    """获取认证token"""
    """
    开发环境 http://rest.dev.java.yibainetworklocal.com/mrp/api/getToken
    测试环境 http://rest.test.java.yibainetworklocal.com/mrp/api/getToken
    线上环境 http://rest.java.yibainetwork.com/mrp/api/getToken
    """
    url = f'http://rest.java.yibainetwork.com/mrp/api/getToken?access_token={java_token}'
    print(url)

    data = {
        "username": "DMD",
        "password": "zU4IzscW"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, json=data, headers=headers)
    print(response.content.decode('utf-8'))

    token = json.loads(response.text)["token"]
    return token


def post_response(data):
    print(f'将要更新数据总行数：{len(data)}')
    headers = {
        'Content-Type': 'application/json'
    }
    """
    开发环境 http://dp.yibai-it.com:10203/api/SaleStatus/update_sale_status?token_info=
    测试环境 http://dp.yibai-it.com:32018/api/SaleStatus/update_sale_status?token_info=
    线上环境 http://plan.yibainetwork.com/api/SaleStatus/update_sale_status?token_info=
    """

    url = 'http://plan.yibainetwork.com/api/SaleStatus/update_sale_status'
    # print(url)
    token = get_token()
    x, y = divmod(len(data), 5000)
    if y != 0:
        x += 1
    i = 0
    while i < x:
        time_stamp = int(time.time())
        nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
        sorted_file = [token, str(time_stamp), nonce]
        sorted_file.sort()
        str_file = str(sorted_file[0]) + str(sorted_file[1]) + str(sorted_file[2])
        signature = hashlib.sha1(str_file.encode('utf-8')).hexdigest()
        token_info = {
            "timestamp": time_stamp,
            "nonce": nonce,
            "signature": signature,
            "token": token,
            "username": "DMD"
        }
        per_data_dict = {"is_last_page": 0, "token_info": token_info}
        # print(token_info)

        per_data = data[5000 * i:5000 * (i + 1)]
        per_data_dict.update({"data": per_data})
        if x == i + 1:
            per_data_dict.update({"is_last_page": 1})

        response = requests.post(url, json=per_data_dict, headers=headers)
        print(response.content.decode('gbk'))
        # print(response.text)
        if response.status_code != 200:
            send_msg('动销组定时任务推送',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{response.content.decode('utf-8')}")
            with open(os.path.join(os.getcwd(), 'err_status.txt'), 'a', encoding='utf-8') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{response.content.decode('utf-8')}")
        i += 1


def get_destroy_sku():
    sql = """
        select distinct 
            sku 
        from 
            yibai_inland_sku_destroy_list 
        where 
            created_at>'2021-05-20 00:00:00' and approve_state=8
    """
    df = sql_to_pd(sql)
    if len(df) > 0:
        df = df.head(5)
        print(df.head())
        df["businessline"] = 3
        df['saleStatus'] = 13
        df_huizong = pd.DataFrame()
        for platform in ["ALI", "AMAZON", "CDISCOUNT", "DAR", "EB", "JOOM", "LAZADA", "OTHER", "SHOPEE", "WALMART",
                         "WISH"]:
            df['platform'] = platform
            df_huizong = df_huizong.append(df)
        df_huizong.drop_duplicates(inplace=True)
        df_huizong = df_huizong.to_dict(orient='records')
        post_response(df_huizong)


if __name__ == '__main__':
    get_destroy_sku()
