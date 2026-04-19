# -*- coding: utf-8 -*-
#    @Author: 乐神
#      @Time: 2021.12.15 下午 2:10
import datetime
import hashlib
import json
import random
import time
import string
import pandas as pd
import pymongo
import requests
from requests.auth import HTTPBasicAuth
from retry import retry
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.nacos_api import get_user_kd, get_user
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd, pd_to_sql
from pulic_func.base_api.mysql_connect import connect_to_sql

def json_response(response, per_data_dict):
    try:
        response_dict = response.json()
    except:
        response_dict = {"data": per_data_dict, "result": response.content.decode()}
    return response_dict


@retry(tries=3, delay=2, backoff=2)
def get_token():
    """获取java接口token"""
    java_url = 'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java'
    response = requests.get(java_url)
    token_one = json.loads(response.text)['data']
    print(token_one)
    # url = "http://dp.yibai-it.com:9403/oauth/token?grant_type=client_credentials"
    # token_request = requests.post(url, auth=HTTPBasicAuth('test', 'test'))
    # token_result = token_request.content
    # token_dict_content = json.loads(token_result.decode())
    # token_two = token_dict_content.get('access_token', '')

    """获取认证token"""
    """
    开发环境 http://rest.dev.java.yibainetworklocal.com/mrp/api/getToken
    测试环境 http://rest.test.java.yibainetworklocal.com/mrp/api/getToken
    线上环境 http://rest.java.yibainetwork.com/mrp/api/getToken
    """

    # 线上环境
    url = f'http://rest.java.yibainetwork.com/mrp/api/getToken?access_token={token_one}'
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
    username = json.loads(res.text)["username"]
    return username, token


def post_response(data):
    print(f'将要更新数据总行数：{len(data)}')
    print(data[0])
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
    username, token = get_token()
    x, y = divmod(len(data), 5000)
    if y != 0:
        x += 1
    i = 0
    total_list = []
    error_list = []
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
            "username": username
        }
        per_data_dict = {"is_last_page": 0, "token_info": token_info}

        per_data = data[5000 * i:5000 * (i + 1)]
        per_data_dict.update({"data": per_data})
        if x == i + 1:
            per_data_dict.update({"is_last_page": 1})

        response = requests.post(url, json=per_data_dict, headers=headers)
        print(response.content.decode())

        response_dict = json_response(response, per_data_dict)
        print(response_dict)
        total_list.append(response_dict)
        if response.status_code != 200:
            error_list.append(response_dict)
        i += 1
    return total_list, error_list
def res_to_mongodb(sale_name, total_list, error_list):
    mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
    my_db = mongo_client.yibai_sale_status
    now_date = time.strftime('%Y%m%d%H%M%S', time.localtime())
    # 转化为list形式
    if total_list:
        connect = my_db[f"{sale_name}_{now_date}"]
        connect.insert_many(total_list)
    if error_list:
        connect = my_db[f"{sale_name}_data_error_{now_date}"]
        connect.insert_many(error_list)

def get_oversea_data(the_business):
    headers = {}

    url = f'http://plan.yibainetwork.com/api/SaleStatus/get_{the_business}_sale_status_list'
    print(url)

    username, token = get_token()
    all_data_list = []

    for sale_status in [4, 9, 10, 11, 12, 13]:
        page = 1
        while True:
            # print(page)
            time_stamp = int(time.time())
            nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
            sorted_file = [token, str(time_stamp), nonce]
            sorted_file.sort()
            str_file = str(sorted_file[0]) + str(sorted_file[1]) + str(sorted_file[2])
            signature = hashlib.sha1(str_file.encode('utf-8')).hexdigest()
            token_info = {
                "token": token,
                "username": username,
                "timestamp": time_stamp,
                "nonce": nonce,
                "signature": signature
            }
            per_data = {"page": page, "limit": 3000, "sale_status": sale_status}
            per_data_dict = {'token_info': json.dumps(token_info)}
            per_data_dict.update({'data': json.dumps(per_data)})

            response = requests.request("POST", url, data=per_data_dict, headers=headers, files=[])
            response_dict = json_response(response, per_data_dict)
            # print(response_dict)
            a_list = response_dict.get('data_list', '')
            if not a_list:
                break
            all_data_list += a_list
            page += 1
    df_online = pd.DataFrame(all_data_list)
    print(df_online.columns)
    print(len(df_online))
    return df_online


# 海外仓的获取不同数据
def get_oversea_compare_data():
    sql = f"""
            SELECT sku, warehouse, sale_status, start_time, end_time
            FROM oversea_sale_status
            order by start_time desc
        """
    df = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
    print(len(df))
    if len(df) > 0:
        df = df.drop_duplicates(['sku', 'warehouse'], 'first')
        df_country = pd.DataFrame({'country_code': ['US', 'CA', 'MX', 'JP', 'GB', 'DE', 'FR', 'ES', 'IT', 'NL', 'SE',
                                                    'AE', 'AU', 'BR', 'IN', 'SA', 'SG'],
                                   'warehouse': ['美国仓', '加拿大仓', '墨西哥仓', '日本仓', '英国仓', '德国仓', '法国仓', '西班牙仓',
                                                 '意大利仓', '荷兰仓', '瑞典仓', '中东仓', '澳洲仓', '巴西仓', '印度仓', '沙特仓', '新加坡仓']})
        df = df.merge(df_country, on="warehouse", how='left')
        print('自己的数据去重之后数据', len(df))

        df["end_time"] = df["end_time"].fillna(0)
        df["销售状态"] = '正常'
        df.loc[df["end_time"] == 0, "销售状态"] = df["sale_status"]
        df.loc[df["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"
        df_seller_status = pd.DataFrame(
            {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'], "saleStatus": [4, 9, 10, 11, 12]})
        df = df.merge(df_seller_status, left_on="销售状态", right_on="status")
        print('df', len(df), df.columns)
        df.drop(["sale_status", "status", "销售状态", "start_time", "end_time", "warehouse"], axis=1, inplace=True)
        print(df.columns)

        df_online = get_oversea_data('oversea')
        df_online = df_online.sort_values('status_updated_at')
        df_online = df_online.drop_duplicates(['sku', 'country_code'], 'first')
        print('去重之后的df_line长度', len(df_online))

        df_new = df.merge(df_online, on=["sku", 'country_code'], how='left')
        print('和线上的数据的合并', len(df_new), df_new.columns)
        df_new['saleStatus'] = df_new['saleStatus'].astype(str)
        df_new = df_new[df_new['saleStatus'] != df_new['sale_status']]
        print('oversea的status不同', len(df_new))
        df_new = df_new.dropna(axis='index', how='any', subset=['sale_status', 'sku_name'])
        print(len(df_new))
        if len(df_new):
            conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn.ck_clear_table('temporary_oversea_compare_data')
            conn.write_to_ck_json_type(df_online, 'temporary_oversea_compare_data')


# 获取计划系统inland的数据
def get_inland_data():
    headers = {}
    """
    开发环境 http://dp.yibai-it.com:10203/api/SaleStatus/get_fbc_sale_status_list
    测试环境 http://dp.yibai-it.com:32018/api/SaleStatus/get_fbc_sale_status_list
    线上环境 http://plan.yibainetwork.com/api/SaleStatus/get_fbc_sale_status_list
    """
    conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    url = 'http://plan.yibainetwork.com/api/SaleStatus/get_inland_sale_status_list'
    print(url)

    username, token = get_token()
    conn.ck_clear_table('get_inland_data')
    for sale_status in [4, 9, 10, 11, 12, 13]:
        page = 1
        # all_data_list = []
        while True:
            print(page)
            time_stamp = int(time.time())
            nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
            sorted_file = [token, str(time_stamp), nonce]
            sorted_file.sort()
            str_file = str(sorted_file[0]) + str(sorted_file[1]) + str(sorted_file[2])
            signature = hashlib.sha1(str_file.encode('utf-8')).hexdigest()
            token_info = {
                "token": token,
                "username": username,
                "timestamp": time_stamp,
                "nonce": nonce,
                "signature": signature
            }
            per_data = {"page": page, "limit": 3000, "sale_status": sale_status}
            per_data_dict = {'token_info': json.dumps(token_info)}
            per_data_dict.update({'data': json.dumps(per_data)})

            response = requests.request("POST", url, data=per_data_dict, headers=headers, files=[])
            response_dict = json_response(response, per_data_dict)
            # print(response_dict)
            a_list = response_dict.get('data_list', '')
            if not a_list:
                break
            # all_data_list += a_list
            # print(len(all_data_list))
            page += 1
            conn.write_to_ck_json_type(pd.DataFrame(a_list), 'get_inland_data', l_type='python')

    # df_online = pd.DataFrame(all_data_list)
    # print(df_online.columns)
    # print(len(df_online))
    # df_online = df_online.sort_values('status_updated_at')
    # df_online = df_online.drop_duplicates(["sku"], 'first')
    # print('去重之后的df_line长度', len(df_online))
    sql = """
    select distinct sku from yibai_oversea.get_inland_data
    """
    df_online = conn.ck_select_to_df(sql)
    if df_online.shape[0] > 0:
        conn.ck_clear_table('temporary_other_inland_data')
        sql = """
        insert into yibai_oversea.temporary_other_inland_data
        select * from yibai_oversea.get_inland_data
        order by status_updated_at asc limit 1 by sku
        settings max_memory_usage = 20000000000
        """
        conn.ck_execute_sql(sql)
        # conn.write_to_ck_json_type(df_online, 'temporary_other_inland_data')
    else:
        raise Exception('temporary_other_inland_data没有数据')


# 获取自己inland的数据
def get_inland_own_data():
    sql = f"""
            SELECT sku,status1,end_time
            FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
            order by start_time desc limit 1 by sku
        """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_mx.ck_select_to_df(sql)
    print('df', df.columns, len(df))
    if len(df) > 0:
        df = df.drop_duplicates(['sku'], 'first')
        print('df', df.columns, len(df))

        df["end_time"] = df["end_time"].fillna(0)
        df["销售状态"] = '正常'
        df.loc[df["end_time"] == 0, "销售状态"] = df["status1"]

        if "是否库存为0" in list(df.columns):
            df.loc[df["是否库存为0"] == '是', "销售状态"] = '正常'
            df.drop(["是否库存为0"], axis=1, inplace=True)
        df_seller_status = pd.DataFrame(
            {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'], "saleStatus": [4, 9, 10, 11, 12]})
        df = df.merge(df_seller_status, left_on="销售状态", right_on="status")
        df.drop(["status", "status1", "销售状态", "end_time"], axis=1, inplace=True)
        print('自己数据的最终结果', len(df), df.columns)
        all_data_df = pd.DataFrame()
        for platform in ["ALI", "AMAZON", "CDISCOUNT", "DAR", "EB", "JOOM", "LAZADA", "OTHER", "SHOPEE", "WALMART",
                         "WISH"]:
            df['platform'] = platform
            all_data_df = all_data_df.append(df)
        all_data_df.drop_duplicates(inplace=True)
        print('all_data_df', all_data_df.columns, len(all_data_df))
        if len(all_data_df):
            conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
            conn.ck_clear_table('temporary_own_inland_data')
            conn.write_to_ck_json_type(all_data_df, 'temporary_own_inland_data')
        else:
            raise Exception('temporary_own_inland_data没有数据')


# 将inland两个数据源的数据进行对比
def two_inland_table_compare():
    conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn.ck_clear_table('temporary_last_inland_surplus_data')
    sql = """
        insert into yibai_oversea.temporary_last_inland_surplus_data
        select a.sku as sku,toString(a.saleStatus) as saleStatus,a.platform as platform,
            b.sale_status as sale_status,b.sku_name as sku_name
        from yibai_oversea.temporary_own_inland_data a 
        inner join yibai_oversea.temporary_other_inland_data b 
        on a.sku=b.sku and a.platform=b.platform
        where toString(a.saleStatus) != toString(b.sale_status) and 
        b.sale_status is not null and b.sku_name is not null
    """
    conn.ck_execute_sql(sql)


# 获取计划系统fba的数据
def get_fba_data():
    headers = {}
    """
    开发环境 http://dp.yibai-it.com:10203/api/SaleStatus/get_fbc_sale_status_list
    测试环境 http://dp.yibai-it.com:32018/api/SaleStatus/get_fbc_sale_status_list
    线上环境 http://plan.yibainetwork.com/api/SaleStatus/get_fbc_sale_status_list
    """

    url = 'http://plan.yibainetwork.com/api/SaleStatus/get_fba_sale_status_list'
    print(url)

    username, token = get_token()
    all_data_list = []

    for sale_status in [4, 9, 10, 11, 12, 13]:
        page = 1
        while True:
            # print(page)
            time_stamp = int(time.time())
            nonce = ''.join(random.sample(string.ascii_letters + string.digits, 8))
            sorted_file = [token, str(time_stamp), nonce]
            sorted_file.sort()
            str_file = str(sorted_file[0]) + str(sorted_file[1]) + str(sorted_file[2])
            signature = hashlib.sha1(str_file.encode('utf-8')).hexdigest()
            token_info = {
                "token": token,
                "username": username,
                "timestamp": time_stamp,
                "nonce": nonce,
                "signature": signature
            }
            per_data = {"page": page, "limit": 3000, "sale_status": sale_status}
            per_data_dict = {'token_info': json.dumps(token_info)}
            per_data_dict.update({'data': json.dumps(per_data)})

            response = requests.request("POST", url, data=per_data_dict, headers=headers, files=[])
            response_dict = json_response(response, per_data_dict)
            # print(response_dict)
            a_list = response_dict.get('data_list', '')
            if not a_list:
                break
            all_data_list += a_list
            page += 1

    df_online = pd.DataFrame(all_data_list)
    print(df_online.columns)
    print(len(df_online))
    df_online = df_online.sort_values('status_updated_at', ascending=False)
    # df_online = df_online.drop_duplicates(["seller_sku", "short_name"], 'first')
    # print('去重之后的df_line长度', len(df_online))
    if len(df_online):
        pd_to_sql(df_online, table="temporary_other_fba_data", database='over_sea', data_sys='数据部服务器')
    else:
        raise Exception('temporary_other_fba_data没有数据')


# 获取自己数据库的fba数据
def get_fba_own_data():
    sql = f"""
               select 
                   account_id,
                   seller_sku,
                   adjustment_priority,
                   start_time,
                   end_time 
               from 
                   domestic_warehouse_clear.yibai_fba_clear_new
               order by start_time desc
           """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_data = conn_mx.ck_select_to_df(sql)
    # df_data = sql_to_pd(database='domestic_warehouse_clear', sql=sql)
    print('df_data', len(df_data), df_data.columns)
    if len(df_data) > 0:
        df_data = df_data.drop_duplicates(['account_id', 'seller_sku'], 'first')
        print(len(df_data))

        df_data["end_time"] = df_data["end_time"].fillna(0)
        df_data["销售状态"] = '正常'
        df_data.loc[df_data["end_time"] == 0, "销售状态"] = df_data["adjustment_priority"]
        df_data.loc[df_data["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"

        df_seller_status = pd.DataFrame(
            {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'], "saleStatus": [4, 9, 10, 11, 12]})
        df = df_data.merge(df_seller_status, left_on="销售状态", right_on="status")
        print('df', len(df), df.columns)
        df.drop(["adjustment_priority", "start_time", "end_time", "status", "销售状态"], axis=1,
                inplace=True)
        print('df', len(df), df.columns)
        if len(df):
            pd_to_sql(df, table='temporary_own_fba_data', database='over_sea', data_sys='数据部服务器')
        else:
            raise Exception('temporary_own_fba_data没有数据')


def pipei_short_name():
    sql1 = f"""select account_id, seller_sku, saleStatus from temporary_own_fba_data"""
    df = sql_to_pd(database='over_sea', sql=sql1, data_sys='数据部服务器')
    sql2 = f"""select account_id, short_name, site from temporary_short_name"""
    df_short_name = sql_to_pd(database='over_sea', sql=sql2, data_sys='数据部服务器')
    print('df_short_name', df_short_name.columns, len(df_short_name))
    df = df.merge(df_short_name, on=['account_id'], how='left')
    print('df', df.columns, len(df))
    if len(df):
        pd_to_sql(df, table='temporary_own_fba_short_name', database='over_sea', data_sys='数据部服务器')
    else:
        raise Exception('temporary_own_fba_short_name没有数据')


# 将fba两个数据源的数据进行对比
def two_fba_table_compare():

    sql1 = f"""select seller_sku, short_name, sale_status, fnsku from temporary_other_fba_data"""
    df1 = sql_to_pd(database='over_sea', sql=sql1, data_sys='数据部服务器')
    print(len(df1))
    sql2 = f"""select account_id, seller_sku, saleStatus, short_name from temporary_own_fba_short_name"""
    df_myself = sql_to_pd(database='over_sea', sql=sql2, data_sys='数据部服务器')
    df_new = df_myself.merge(df1, on=["seller_sku", "short_name"], how='left')
    print('和线上的数据的合并', len(df_new), df_new.columns)
    df_new['saleStatus'] = df_new['saleStatus'].astype(str)
    df_new = df_new[df_new['saleStatus'] != df_new['sale_status']]
    print('oversea的status不同', len(df_new))
    df_new = df_new.dropna(axis='index', how='any', subset=['sale_status', 'fnsku'])
    print('oversea的status不同', len(df_new), df_new.to_dict('records'))
    if len(df_new):
        pd_to_sql(df_new, table='temporary_last_fba_surplus_data', database='over_sea', data_sys='数据部服务器')
    else:
        raise Exception('temporary_last_fba_surplus_data没有数据')


# 将fba没有更新的数据重新上传
def again_update_fba_data():
    sql1 = f"""select seller_sku as sellerSku, account_id, saleStatus from temporary_last_fba_surplus_data"""
    df_update = sql_to_pd(database='over_sea', sql=sql1, data_sys='数据部服务器')

    sql3 = """SELECT id,account_name FROM yibai_amazon_account"""
    df_new = sql_to_pd(database="yibai_system", sql=sql3, data_sys='AMAZON刊登库')
    print('df_new', len(df_new))

    df = df_update.merge(df_new, left_on="account_id", right_on="id")
    df["businessline"] = 1
    df = df.rename(columns={'account_name': 'salesAccount'})
    df.drop(["account_id", "id"], axis=1, inplace=True)
    print('df', len(df), df.columns)
    insert_list = df.to_dict(orient='records')
    if len(insert_list):
        total_list, error_list = post_response(insert_list)
        res_to_mongodb('f_again_status', total_list, error_list)
    else:
        print('没有数据')




def get_sku_status():
    sql = """
        SELECT 
            distinct sku
        from 
            yibai_product
        where 
            product_status!=7
    """
    df = sql_to_pd(database='yibai_product', sql=sql, data_sys='ERP')
    return df


# 只更新fba的一个sku数据
def update_a_data():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    sql = f'''
        select count(1),sum(7days_sales),sum(30days_sales) 
        from yibai_sku_sales_statistics_all where statistics_date="{out_date}"
        union all
        select count(1),sum(7days_sales),sum(30days_sales) 
        from yibai_sku_sales_statistics_all where statistics_date="{time.strftime("%Y-%m-%d")}"
    '''
    print(sql)
    df = conn.read_sql(sql)
    df = df.fillna(0)
    print(df)
    df = df.astype(int)
    all_number = df.values.tolist()
    print(all_number)
    d_value_list = [abs((all_number[0][i] - all_number[1][i]) / all_number[0][i]) for i in
                    range(len(all_number[0]))]
    print(d_value_list)
    conn.close()
    # sql = f"""
    #         SELECT
    #             sku,
    #             status1,
    #             end_time
    #         FROM
    #             domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
    #         order by
    #             start_time desc"""
    #
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df = conn_mx.ck_select_to_df(sql)
    # print('df', df.columns, len(df))
    # sql = f"""
    #                select
    #                    account_id,
    #                    seller_sku as sellerSku,
    #                    adjustment_priority,
    #                    start_time,
    #                    end_time
    #                from
    #                    fba_clear_new
    #                where seller_sku='QC04523-WXJYuxx-7800f-FBA' and account_id=2718
    #                order by start_time desc
    #            """
    #
    # df_data = sql_to_pd(database='domestic_warehouse_clear', sql=sql)
    # print('df_data', len(df_data), df_data.columns)
    # print(df_data)
    # df_data = df_data.drop_duplicates(['account_id', 'sellerSku'], 'first')
    # print(len(df_data))
    # df_data["end_time"] = df_data["end_time"].fillna(0)
    # df_data["销售状态"] = '正常'
    # df_data.loc[df_data["end_time"] == 0, "销售状态"] = df_data["adjustment_priority"]
    # df_data.loc[df_data["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"
    # print(df_data)
    # print(df_data.to_dict('records'))
    # sql3 = """SELECT id,account_name FROM yibai_amazon_account"""
    # df_new = sql_to_pd_kd(database="yibai_system", sql=sql3)
    # print('df_new', len(df_new))
    #
    # df_data = df_data.merge(df_new, left_on="account_id", right_on="id")
    # df_data["businessline"] = 1
    # df_seller_status = pd.DataFrame(
    #     {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'], "saleStatus": [4, 9, 10, 11, 12]})
    # df = df_data.merge(df_seller_status, left_on="销售状态", right_on="status")
    # print('df', len(df), df.columns)
    # df = df.rename(columns={'account_name': 'salesAccount'})
    # df.drop(["account_id", "id", "adjustment_priority", "start_time", "end_time", "status", "销售状态"], axis=1,
    #         inplace=True)
    # print('df', len(df), df.columns)
    # insert_list = df.to_dict(orient='records')
    # total_list, error_list = post_response(insert_list)
    # print(total_list, error_list)
    # # res_to_mongodb('f_again_status', total_list, error_list)


if __name__ == '__main__':
    # get_inland_data()
    get_token()
