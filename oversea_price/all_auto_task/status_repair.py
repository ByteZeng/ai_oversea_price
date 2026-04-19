import datetime
import json
import os
import time
import random
import string
import traceback
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
import requests
import hashlib
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.status import get_china_data_from_mongodb
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd, pd_to_sql


def get_date():
    qu_shu_date_str = datetime.date.today().isoformat()
    date_list = qu_shu_date_str.split("-")
    date_new = date_list[0] + date_list[1] + date_list[2]
    return date_new



# 写数据库时，变更数据类型
def mapping_df_types(df):
    a_type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            a_type_dict.update({i: VARCHAR(length=1000)})
        if "float" in str(j):
            a_type_dict.update({i: DECIMAL(precision=20, scale=4, asdecimal=True)})
        if "int" in str(j):
            if df[i].max() < 2147483647:
                a_type_dict.update({i: Integer()})
            else:
                a_type_dict.update({i: BigInteger()})
    return a_type_dict




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
    res = requests.post(url, json=data, headers=headers)
    print(res.content.decode('utf-8'))
    token = json.loads(res.text)["token"]
    return token


def get_base_sku(conn_mx):
    time_today = datetime.date.today().isoformat()
    sql = f"""
        SELECT distinct sku 
        from (
            SELECT sku, sum(available_qty) as available_qty, sum(on_way_stock) as on_way_stock
            FROM yibai_ods_sync.yibai_warehouse_sku_stock_ods 
            WHERE warehouse_code IN ('FBA_HM_AA', 'HM_ZH', 'HM_XN', 'HM_FBA', 'TCXNCHM', 'TCXNC', 'HM_AA') 
            AND update_date >= today()
            group by sku
        ) a 
        where available_qty=0 and on_way_stock=0
    """
    df = conn_mx.ck_select_to_df(sql)
    # df = df_data.groupby(["sku"])[["available_qty", "on_way_stock"]].sum().reset_index()
    # df = df[(df["available_qty"] == 0) & (df["on_way_stock"] == 0)]
    # df.drop(["available_qty", "on_way_stock"], axis=1, inplace=True)
    # df.drop_duplicates(inplace=True)
    sql2 = """
        SELECT distinct sku from yb_datacenter.yb_product where product_status != 7 
    """
    df_a = conn_mx.ck_select_to_df(sql2)
    df = df.merge(df_a, on="sku")
    return df


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
        the_time_stamp = int(time.time())
        nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
        sorted_file = [token, str(the_time_stamp), nonce]
        sorted_file.sort()
        str_file = str(sorted_file[0]) + str(sorted_file[1]) + str(sorted_file[2])
        signature = hashlib.sha1(str_file.encode('utf-8')).hexdigest()
        token_info = {
            "timestamp": the_time_stamp,
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
        if response.status_code != 200:
            send_msg('动销组定时任务推送',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{response.content.decode('utf-8')}",
                     is_all=False)
            with open(os.path.join(os.getcwd(), 'err_status.txt'), 'a', encoding='utf-8') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{response.content.decode('utf-8')}")
        i += 1


def get_old_status(base_dir):
    conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df = get_base_sku(conn_mx)
    print('df', len(df), df.columns)
    # df_k = get_china_data_from_mongodb('china', 'sku')
    sql = """
    select distinct sale_status as adjust_status,sku 
	from yibai_product_kd_sync.yibai_inland_sale_status
	where platform not in  ('OTHER', '')
    """
    df_k = conn_mx.ck_select_to_df(sql)
    print('df_k', len(df_k), df_k.columns)
    if len(df_k) > 0:
        all_data_df = pd.DataFrame()
        df_k = df_k.drop_duplicates(['sku'], 'first')
        df["saleStatus"] = 11
        df = df.merge(df_k, on="sku", how='left')
        print('df', len(df), df.columns)
        df = df[df["adjust_status"] != 11]
        df.drop(["adjust_status"], axis=1, inplace=True)
        for platform in ["ALI", "AMAZON", "CDISCOUNT", "DAR", "EB", "JOOM", "LAZADA", "OTHER", "SHOPEE", "WALMART",
                         "WISH"]:
            df['platform'] = platform
            all_data_df = all_data_df.append(df)
        all_data_df.drop_duplicates(inplace=True)
        print('all_data_df', len(all_data_df), all_data_df.columns)
        # all_data_df.to_csv(os.path.join(base_dir, f"国内仓销售状态修复{get_date()}.csv"), index=False)
        df_new = all_data_df
        df_new["DATE"] = datetime.date.today().isoformat()
        all_data_df = all_data_df.to_dict(orient='records')
        post_response(all_data_df)


def post_sku_status():
    headers = {
        'Content-Type': 'application/json'
    }
    url = 'http://eblisting.yibainetwork.com/services/ebay/ebayapi/GetSaleStatusByUpdateAt'
    response = requests.post(url, headers=headers)
    print(response.status_code)
    if response.status_code == 200:
        send_msg('动销组定时任务推送', 'ebay平台更新销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}已发送信号,通知ebay平台更新销售状态",
                 is_all=False)


def repair_status():
    try:
        base_dir = os.path.dirname(__file__)
        get_old_status(base_dir)
        send_msg('动销组定时任务推送', '国内仓销售状态修复', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}将可用和在途库存为0条件下的国内仓的销售状态改为正常，程序运行成功",
                 is_all=False)
        post_sku_status()
    except:
        print(traceback.format_exc())
        send_msg('动销组定时任务推送', '国内仓销售状态修复',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}将可用和在途库存为0条件下的国内仓的销售状态改为正常程序出现错误,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    repair_status()
