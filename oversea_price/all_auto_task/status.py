import datetime
import json
import os
import time
import random
import string
import traceback

from retry import retry

from pulic_func.base_api.mysql_connect import sql_to_pd, pd_to_ck, pd_to_sql
import pymongo
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
import requests
import hashlib
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user, get_user_kd
from pulic_func.base_api import yibai_obs
from pulic_func.base_api.mysql_connect import connect_to_sql
from tqdm import tqdm


def get_date():
    qu_shu_date_str = datetime.date.today().isoformat()
    date_list = qu_shu_date_str.split("-")
    date_new = date_list[0] + date_list[1] + date_list[2]
    return date_new


def get_before_sixty_time():
    today_time = time.strftime('%Y-%m-%d')
    dt = datetime.datetime.strptime(today_time, "%Y-%m-%d")
    time_str = (dt - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
    return time_str


def get_old_day(n):
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=n)).strftime("%Y-%m-%d")
    return out_date


def get_fbc_account_data():
    # engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
    #     'luomeng', 'Luomeng#123', '139.9.206.7', 3306, 'yibai_system', 'utf8'))
    sql = 'select id as account_id, short_name, express as site from yibai_cdiscount_account'
    df_account = sql_to_pd(database='yibai_system', sql=sql, data_sys='小平台刊登库2')
    return df_account


def get_sku_status(conn_mx):
    sql = """
    SELECT DISTINCT sku 
    FROM yibai_prod_base_sync.yibai_prod_sku 
    where product_status!=11
    """
    df = conn_mx.ck_select_to_df(sql)
    return df


@retry(tries=3, delay=2, backoff=2)
def get_token():
    """获取java接口token"""
    java_url = 'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java'
    response = requests.get(java_url)
    token_one = json.loads(response.text)['data']

    """获取认证token"""
    """
    开发环境 http://rest.dev.java.yibainetworklocal.com/mrp/api/getToken
    测试环境 http://rest.test.java.yibainetworklocal.com/mrp/api/getToken
    线上环境 http://rest.java.yibainetwork.com/mrp/api/getToken
    """
    url = f'http://rest.java.yibainetwork.com/mrp/api/getToken?access_token={token_one}'
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
    return now_date


def get_status_data_from_mongodb():
    try:
        # mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
        # my_db = mongo_client.yibai_sale_status
        # # 转化为list形式
        # col_list = my_db.list_collection_names()
        # # 遍历文档
        # total_collec = [collection_name for collection_name in col_list]
        # for one_business in ["oversea", "fba", "china"]:
        #     now_business_list = sorted(
        #         [collection_name for collection_name in total_collec if
        #          one_business in collection_name and 'error' not in collection_name], reverse=True)
        #     if now_business_list:
        #         collection_name = now_business_list[0]
        #         connect = my_db[collection_name]
        #         res_ite = connect.find()
        #         all_status_data_list = []
        #         for res in res_ite:
        #             status = res["status"]
        #             error_mess = res["errorMess"]
        #             if "data" in res.keys():
        #                 if res['data']['succ_list']:
        #                     for item in res['data']['succ_list']:
        #                         item_dict = item["data"]
        #                         item_dict["status"] = status
        #                         item_dict["error_mess"] = error_mess
        #                         item_dict["msg"] = item.get("fail_msg", "")
        #                         all_status_data_list.append(item_dict)
        #                 if res['data']['fail_list']:
        #                     for item in res['data']['fail_list']:
        #                         item_dict = item["data"]
        #                         item_dict["status"] = status
        #                         item_dict["error_mess"] = error_mess
        #                         item_dict["msg"] = item.get("fail_msg", "")
        #                         all_status_data_list.append(item_dict)
        #             else:
        #                 all_status_data_list.append({"status": status, "error_mess": error_mess})
        #         # 输出成csv到obs
        #         df = pd.DataFrame(all_status_data_list)
        #         if one_business == "china":
        #             sql1 = f"""select sku from yibai_oversea.temporary_yibai_warehouse_sku"""
        #             # df_data = sql_to_pd(database='over_sea', sql=sql1, data_sys='数据部服务器')
        #             conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
        #             df_data = conn_mx.ck_select_to_df(sql1)
        #             print(len(df_data))
        #             df_data['备注'] = '国内仓在库库存为0，销售状态改为正常'
        #             df = df.merge(df_data, on=['sku'], how='left')
        #         result = df.to_csv(index=False)
        #         yibai_obs.obs_content_upload(
        #             f"{collection_name}.csv",
        #             result.encode("utf-8-sig"),
        #             "yibai_price/yibai_sale_status"
        #         )
        pass
    except:
        send_msg('动销组定时任务推送', '销售状态写入obs异常',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}销售状态写入mongodb异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def get_fbc_status_data_from_mongodb():
    try:
        # mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
        # my_db = mongo_client.yibai_sale_status
        # # 转化为list形式
        # col_list = my_db.list_collection_names()
        # # 遍历文档
        # total_collec = [collection_name for collection_name in col_list]
        # now_business_list = sorted(
        #     [collection_name for collection_name in total_collec if
        #      "fbc" in collection_name and 'error' not in collection_name], reverse=True)
        # if now_business_list:
        #     collection_name = now_business_list[0]
        #     connect = my_db[collection_name]
        #     res_ite = connect.find()
        #     all_status_data_list = []
        #     for res in res_ite:
        #         status = res["status"]
        #         if res['success_list']:
        #             for item in res['success_list']:
        #                 item["status"] = status
        #                 item["error_mess"] = item.get("errorMess", "")
        #                 item["msg"] = item.get("message", "")
        #                 all_status_data_list.append(item)
        #         if res['fail_list']:
        #             for item in res['fail_list']:
        #                 item["status"] = status
        #                 item["error_mess"] = item.get("errorMess", "")
        #                 item["msg"] = item.get("message", "")
        #                 all_status_data_list.append(item)
        #         else:
        #             all_status_data_list.append({"status": status, "error_mess": res["errorMess"]})
        #     # 输出成csv到obs
        #     df = pd.DataFrame(all_status_data_list)
        #     result = df.to_csv(index=False)
        #     yibai_obs.obs_content_upload(
        #         f"{collection_name}.csv",
        #         result.encode("utf-8-sig"),
        #         "yibai_price/yibai_sale_status"
        #     )
        pass
    except:
        send_msg('动销组定时任务推送', '销售状态写入obs异常',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fbc销售状态写入mongodb异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def json_response(response, per_data_dict):
    try:
        response_dict = response.json()
    except:
        response_dict = {"data": per_data_dict, "result": response.content.decode()}
    return response_dict


@retry(tries=3, delay=2, backoff=2)
def post_response(data):
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
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
            "username": "DMD"
        }
        per_data_dict = {"is_last_page": 0, "token_info": token_info}

        per_data = data[5000 * i:5000 * (i + 1)]
        per_data_dict.update({"data": per_data})
        if x == i + 1:
            per_data_dict.update({"is_last_page": 1})

        response = requests.post(url, json=per_data_dict, headers=headers)
        try:
            res = response.json()['data']['fail_list']
            if len(res) > 0:
                df_res = pd.DataFrame()
                for item in res:
                    df0 = pd.DataFrame(item['data'], index=[0])
                    df0['fail_msg'] = item['fail_msg']
                    df_res = df_res.append(df0)
                if df_res.shape[0] >0:
                    # print(df_res.columns)
                    conn_mx.write_to_ck_json_type(df_res, 'status_update_error_data')
        except:
            print(traceback.format_exc())
            pass
        # print(response.content.decode())
        response_dict = json_response(response, per_data_dict)
        total_list.append(response_dict)
        if response.status_code != 200:
            error_list.append(response_dict)
        i += 1
    return total_list, error_list


def get_base_sku(conn_mx):
    sql = f"""
        select sku from (
            select sku,sum(available_qty) as available_qty1,
                sum(on_way_stock) as on_way_stock1
            from (
                SELECT sku, available_qty, on_way_stock
                FROM yibai_ods_sync.yibai_warehouse_sku_stock_ods 
                WHERE warehouse_code IN ('FBA_HM_AA', 'HM_ZH', 'HM_XN', 'HM_FBA', 'TCXNCHM', 'TCXNC', 'HM_AA') 
                AND update_date >= today()
            )
            group by sku
            having available_qty1=0 and on_way_stock1=0
        )
    """
    df = conn_mx.ck_select_to_df(sql, columns=['sku'])
    # df = df_data.groupby(["sku"])[["available_qty", "on_way_stock"]].sum().reset_index()
    # df = df[(df["available_qty"] == 0) & (df["on_way_stock"] == 0)]
    # df.drop(["available_qty", "on_way_stock"], axis=1, inplace=True)
    # df.drop_duplicates(inplace=True)
    df_a = get_sku_status(conn_mx)
    df = df.merge(df_a, on="sku")
    # pd_to_sql(df, database='over_sea', table='temporary_yibai_warehouse_sku', if_exists='replace')
    # conn_mx.ck_clear_table('temporary_yibai_warehouse_sku', db_name='yibai_oversea')
    # conn_mx.write_to_ck_json_type(df, 'temporary_yibai_warehouse_sku', db_name='yibai_oversea')
    return df


def get_china_data_from_mongodb(the_site, the_sku):
    mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
    my_db = mongo_client.yibai_sale_status
    # 获取日期最大值
    col_list = my_db.list_collection_names()
    df_k = pd.DataFrame()
    a_list = [int(i.split('_')[2]) for i in col_list if f'{the_site}' in i and 'error' not in i]
    if a_list:
        max_number = max(a_list)
        connect = my_db[f"{the_site}_status_{max_number}"]
        res_ite = connect.find({'status': 1})
        all_status_data_list = []
        for res in res_ite:
            if res['data']['succ_list']:
                for item in res['data']['succ_list']:
                    insert_list = [item['data'][f'{the_sku}'], item['data']['saleStatus']]
                    all_status_data_list.append(insert_list)
            if res['data']['fail_list']:
                for item in res['data']['fail_list']:
                    if item.get("fail_msg", "") == '该记录销售状态和传入的一致，无需更新':
                        insert_list = [item['data'][f'{the_sku}'], item['data']['saleStatus']]
                        all_status_data_list.append(insert_list)

        if all_status_data_list:
            df_k = pd.DataFrame(all_status_data_list)
            df_k.rename(columns={0: f'{the_sku}', 1: 'adjust_status'}, inplace=True)
    return df_k


def get_china_status():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    with available_qty_0 as (
        select distinct a.sku as sku 
        from (
            select sku,sum(available_qty) as available_qty1,
                sum(on_way_stock) as on_way_stock1
            from (
                SELECT sku, available_qty, on_way_stock
                FROM yibai_ods_sync.yibai_warehouse_sku_stock_ods_new 
                WHERE warehouse_code IN ('FBA_HM_AA', 'HM_ZH', 'HM_XN', 'HM_FBA', 'TCXNCHM', 'TCXNC', 'HM_AA') 
                AND update_date >= today()
            )
            group by sku
            having available_qty1=0 and on_way_stock1=0
        ) a 
        inner join (
            SELECT DISTINCT sku 
            FROM yibai_prod_base_sync.yibai_prod_sku 
            where product_status!=11
        ) b 
        on a.sku=b.sku
    ),
    ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'] as status_list,
    [4, 9, 10, 11, 12] as status_num_list
    
    select distinct a.sku as sku,a.saleStatus as saleStatus,a.saleStatusPa as saleStatusPa,
        3 as businessline, 
        b.platform as platform
        --,c.adjust_status as adjust_status,c.sale_status_pa as sale_status_pa
        ,toString(today()) as `DATE`
    from (
        SELECT sku,status1,end_time,
            if(sku in available_qty_0, '正常', `销售状态`) as `销售状态`,
            transform(if(sku in available_qty_0, '正常', `销售状态`), status_list, status_num_list, 0) as saleStatus,
            `销售状态1`,
            transform(`销售状态1`, status_list, status_num_list, 0) as saleStatusPa
        from (
            SELECT distinct sku,'清仓' status1,toString(today()) as end_time,'清仓' as `销售状态`,`销售状态` as `销售状态1`,0 as num
            from (
                select * FROM yibai_fba.sku_para
                where para_type=6 and start_time<=toString(today()) and toString(today())<=end_time
            )
            union all 
            SELECT sku,status1,end_time,if(end_time is null, status1, '正常') as `销售状态`,`销售状态` as `销售状态1`,1 as num
            FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
            order by start_time desc LIMIT 1 by sku
            union all 
            select distinct sku,'正常' as status1,toString(today()) as end_time,'正常' as `销售状态`,'正常' as `销售状态1`,2 as num
            from yibai_product_kd_sync.yibai_inland_sale_status
            where sku not in (select distinct sku from domestic_warehouse_clear.yibai_domestic_warehouse_clear_status)
        )
        order by num asc limit 1 by sku
    ) a 
    cross join (
        select arrayJoin(['ALI', 'AMAZON', 'CDISCOUNT', 'DAR', 'EB', 'LAZADA', 'OTHER', 'SHOPEE', 'WALMART', 'WISH', 'OZON']) as platform
    ) b 
    left join (
        select distinct sku,platform,sale_status as adjust_status,sale_status_pa 
        from yibai_product_kd_sync.yibai_inland_sale_status
    ) c
    on a.sku=c.sku and b.platform=c.platform
    where a.saleStatus != c.adjust_status or a.saleStatusPa != c.sale_status_pa
    """
    all_data_df = conn_mx.ck_select_to_df(sql)
    if all_data_df.shape[0] > 0:
        result = all_data_df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"国内仓销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
        all_data_df = all_data_df.to_dict(orient='records')
        total_list, error_list = post_response(all_data_df)
        # 国内仓的销售状态传输结果
        # res_to_mongodb('china_status', total_list, error_list)

        if error_list:
            send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}",
                     mobiles=['+86-15872366806', '+86-18986845950'], is_all=False)
            raise Exception(f"国内仓销售状态传输结果传输异常，请下载对应文件检查")

    # sql = f"""
    # SELECT sku,status1,end_time,`销售状态`,`销售状态1`
    # from (
    #     SELECT distinct sku,'清仓' status1,toString(today()) as end_time,'清仓' as `销售状态`,`销售状态` as `销售状态1`,0 as num
    #     FROM yibai_fba.sku_para
    #     where para_type=6 and start_time<=toString(today()) and toString(today())<=end_time
    #     union all
    #     SELECT sku,status1,end_time,if(end_time is null, status1, '正常') as `销售状态`,`销售状态` as `销售状态1`,1 as num
    #     FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
    #     order by start_time desc LIMIT 1 by sku
    #     union all
    #     select distinct sku,'正常' as status1,toString(today()) as end_time,'正常' as `销售状态`,'正常' as `销售状态1`,2 as num
    #     from yibai_product_kd_sync.yibai_inland_sale_status
    #     where sku not in (select distinct sku from domestic_warehouse_clear.yibai_domestic_warehouse_clear_status)
    # )
    # order by num asc limit 1 by sku
    # """
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df = conn_mx.ck_select_to_df(sql)
    # print('df', df.columns, len(df))
    # if len(df) > 0:
    #     # df = df.drop_duplicates(['sku'], 'first')
    #     # df["end_time"] = df["end_time"].fillna(0)
    #     # df["销售状态"] = '正常'
    #     # df.loc[df["end_time"] == 0, "销售状态"] = df["status1"]
    #     df_data = get_base_sku(conn_mx)
    #     df_data['是否库存为0'] = '是'
    #     print('df_data', df_data.columns, len(df_data))
    #     df = df.merge(df_data, on="sku", how="left")
    #     if "是否库存为0" in list(df.columns):
    #         df.loc[df["是否库存为0"] == '是', "销售状态"] = '正常'
    #         df.drop(["是否库存为0"], axis=1, inplace=True)
    #     df["businessline"] = 3
    #     df_seller_status = pd.DataFrame(
    #         {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'],
    #          "saleStatus": [4, 9, 10, 11, 12]})
    #     df = df.merge(df_seller_status, left_on="销售状态", right_on="status")
    #     df_seller_status.rename(columns={"status": "销售状态1", "saleStatus": "saleStatusPa"}, inplace=True)
    #     df = df.merge(df_seller_status, on="销售状态1", how='left')
    #     df.drop(["status", "status1", "销售状态", "end_time", '销售状态1'], axis=1, inplace=True)
    #     all_data_df = pd.DataFrame()
    #     for platform in ["ALI", "AMAZON", "CDISCOUNT", "DAR", "EB", "JOOM", "LAZADA", "OTHER", "SHOPEE", "WALMART",
    #                      "WISH", 'OZON']:
    #         df['platform'] = platform
    #         all_data_df = all_data_df.append(df)
    #     all_data_df.drop_duplicates(inplace=True)
    #     print('all_data_df', all_data_df.columns, len(all_data_df))
    #     # df_k = get_china_data_from_mongodb('china', 'sku')
    #     # print('df_k: ', len(df_k))
    #     sql = """
    #     select distinct sku,platform,sale_status as adjust_status,sale_status_pa
    #     from yibai_product_kd_sync.yibai_inland_sale_status
    #     """
    #     df_k = conn_mx.ck_select_to_df(sql)
    #     if len(df_k) > 0:
    #         all_data_df = all_data_df.merge(df_k, on=["sku", 'platform'], how="left")
    #         all_data_df = all_data_df[(all_data_df["saleStatus"] != all_data_df["adjust_status"]) | (all_data_df["saleStatusPa"] != all_data_df["sale_status_pa"])]
    #         all_data_df.drop(["adjust_status", 'sale_status_pa'], axis=1, inplace=True)
    #     print(len(all_data_df), all_data_df.columns)
    #     print(all_data_df)
    #     df_new = all_data_df
    #     print('def_new', len(df_new), df_new.columns)
    #     df_new["DATE"] = datetime.date.today().isoformat()
    #     # pd_to_sql(df_new, database='over_sea', table='status_data_china', if_exists='append')
    #
    #     result = all_data_df.to_csv(index=False)
    #     yibai_obs.obs_content_upload(f"国内仓销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
    #     all_data_df = all_data_df.to_dict(orient='records')
    #     total_list, error_list = post_response(all_data_df)
    #     # 国内仓的销售状态传输结果
    #     # res_to_mongodb('china_status', total_list, error_list)
    #
    #     if error_list:
    #         send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}",
    #                  mobiles=['+86-15872366806', '+86-18986845950'], is_all=False)
    #         raise Exception(f"国内仓销售状态传输结果传输异常，请下载对应文件检查")

def get_china_status_new():
    # 每小时针对无库存链接刷新状态:
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
    with available_qty_0 as (
        select distinct a.sku as sku 
        from (
            select sku,sum(available_qty) as available_qty1,
                sum(on_way_stock) as on_way_stock1
            from (
                SELECT sku, available_qty, on_way_stock
                FROM yibai_ods_sync.yibai_warehouse_sku_stock_ods_new 
                WHERE warehouse_code IN ('FBA_HM_AA', 'HM_ZH', 'HM_XN', 'HM_FBA', 'TCXNCHM', 'TCXNC', 'HM_AA') 
                AND update_date >= today()
            )
            group by sku
            having available_qty1=0 and on_way_stock1=0
        ) a 
        inner join (
            SELECT DISTINCT sku 
            FROM yibai_prod_base_sync.yibai_prod_sku 
            where product_status!=11
        ) b 
        on a.sku=b.sku
    ),
    ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'] as status_list,
    [4, 9, 10, 11, 12] as status_num_list
    
    select distinct a.sku as sku,a.saleStatus as saleStatus,a.saleStatusPa as saleStatusPa,
        3 as businessline, 
        b.platform as platform
        --,c.adjust_status as adjust_status,c.sale_status_pa as sale_status_pa
        ,toString(today()) as `DATE`
    from (
        SELECT sku,status1,end_time,
            if(sku in available_qty_0, '正常', `销售状态`) as `销售状态`,
            transform(if(sku in available_qty_0, '正常', `销售状态`), status_list, status_num_list, 0) as saleStatus,
            `销售状态1`,
            transform(`销售状态1`, status_list, status_num_list, 0) as saleStatusPa
        from (
            SELECT distinct sku,'清仓' status1,toString(today()) as end_time,'清仓' as `销售状态`,`销售状态` as `销售状态1`,0 as num
            from (
                select * FROM yibai_fba.sku_para
                where para_type=6 and start_time<=toString(today()) and toString(today())<=end_time
            )
            union all 
            SELECT sku,status1,end_time,if(end_time is null, status1, '正常') as `销售状态`,`销售状态` as `销售状态1`,1 as num
            FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
            order by start_time desc LIMIT 1 by sku
            union all 
            select distinct sku,'正常' as status1,toString(today()) as end_time,'正常' as `销售状态`,'正常' as `销售状态1`,2 as num
            from yibai_product_kd_sync.yibai_inland_sale_status
            where sku not in (select distinct sku from domestic_warehouse_clear.yibai_domestic_warehouse_clear_status)
        )
        -- 只针对无库存sku
        where sku in available_qty_0
        order by num asc limit 1 by sku
    ) a 
    cross join (
        select arrayJoin(['ALI', 'AMAZON', 'CDISCOUNT', 'DAR', 'EB', 'LAZADA', 'OTHER', 'SHOPEE', 'WALMART', 'WISH', 'OZON']) as platform
    ) b 
    left join (
        select distinct sku,platform,sale_status as adjust_status,sale_status_pa 
        from yibai_product_kd_sync.yibai_inland_sale_status
    ) c
    on a.sku=c.sku and b.platform=c.platform
    where a.saleStatus != c.adjust_status or a.saleStatusPa != c.sale_status_pa
    """
    all_data_df = conn_mx.ck_select_to_df(sql)
    if all_data_df.shape[0] > 0:
        result = all_data_df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"国内仓销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
        all_data_df = all_data_df.to_dict(orient='records')
        total_list, error_list = post_response(all_data_df)
        # 国内仓的销售状态传输结果
        # res_to_mongodb('china_status', total_list, error_list)

        if error_list:
            send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}",
                     mobiles=['+86-15872366806', '+86-18986845950'], is_all=False)
            raise Exception(f"国内仓销售状态传输结果传输异常，请下载对应文件检查")



def get_oversea_data_from_mongodb(the_site, the_sku):
    mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
    my_db = mongo_client.yibai_sale_status
    # 获取日期最大值
    col_list = my_db.list_collection_names()
    df_k = pd.DataFrame()
    a_list = [int(i.split('_')[2]) for i in col_list if f'{the_site}' in i and 'error' not in i]
    if a_list:
        max_number = max(a_list)
        connect = my_db[f"{the_site}_status_{max_number}"]
        res_ite = connect.find({'status': 1})
        all_status_data_list = []
        for res in res_ite:
            if res['data']['succ_list']:
                for item in res['data']['succ_list']:
                    insert_list = [item['data'][f'{the_sku}'], item['data']['countryCode'], item['data']['saleStatus']]
                    all_status_data_list.append(insert_list)
            if res['data']['fail_list']:
                for item in res['data']['fail_list']:
                    if item.get("fail_msg", "") == '该记录销售状态和传入的一致，无需更新':
                        insert_list = [item['data'][f'{the_sku}'], item['data']['countryCode'],
                                       item['data']['saleStatus']]
                        all_status_data_list.append(insert_list)

        if all_status_data_list:
            df_k = pd.DataFrame(all_status_data_list)
            df_k.rename(columns={0: f'{the_sku}', 1: 'countryCode', 2: 'adjust_status'}, inplace=True)
    return df_k


def get_oversea_status():
    sql = f"""
    SELECT sku,warehouse,sale_status,start_time,
    	CASE 
    		WHEN end_time > CURDATE() THEN NULL
    		ELSE end_time
    	END AS end_time,1 as num 
    FROM over_sea.tt_oversea_sale_status_temp 
    where (end_time is null OR end_time > CURDATE()) AND start_time <= CURDATE()
    union all
    SELECT sku,warehouse,sale_status,start_time,end_time,2 as num 
    FROM over_sea.oversea_sale_status 
    where end_time is null 
    union all 
    SELECT sku,warehouse,'正常' as sale_status,start_time,end_time,3 as num 
    FROM over_sea.oversea_sale_status
    where end_time is not null 
    """
    df = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
    if len(df) > 0:
        df = df.sort_values(['num', 'start_time'], ascending=[True, False])
        df = df.drop_duplicates(['sku', 'warehouse'], 'first')
        df.drop(['num'], axis=1, inplace=True)
        print(len(df))
        # 剔除列表中的sku
        # df_k = get_oversea_data_from_mongodb('oversea', 'sku')
        # print('df_k', len(df_k))
        df_country = pd.DataFrame({'countryCode': ['US', 'CA', 'MX', 'JP', 'GB', 'DE', 'FR', 'ES', 'IT', 'NL', 'SE',
                                                   'AE', 'AU', 'BR', 'IN', 'SA', 'SG', 'PL', 'CS', 'PH', 'MY', 'RU',
                                                   'ID', 'IT', 'VN', 'AE', 'SA', 'TH', 'AE', 'AF', 'BR', 'ID', 'ID'],
                                   'warehouse': ['美国仓', '加拿大仓', '墨西哥仓', '日本仓', '英国仓', '德国仓',
                                                 '法国仓', '西班牙仓', '意大利仓', '荷兰仓', '瑞典仓', '中东仓',
                                                 '澳洲仓', '巴西仓', '印度仓', '沙特仓', '新加坡仓', '波兰仓',
                                                 '德国仓', '菲律宾仓', '马来西亚仓', '俄罗斯仓','印度尼西亚仓', '意大利仓',
                                                 '越南仓', '阿联酋仓', '沙特阿拉伯仓', '泰国仓', '迪拜仓', '非洲仓', '乌拉圭仓', '印度尼西亚仓', '印尼仓']})
        df = df.merge(df_country, on="warehouse", how='left')
        df['countryCode'] = df['countryCode'].fillna('')
        # if len(df_k):
        #     df_k = df_k.drop_duplicates(['sku', 'countryCode'], 'first')
        #     print('df_k', len(df_k))
        #     df = df.merge(df_k, on=["sku", "countryCode"], how="left")
        #     df = df[df["sale_status"] != df["adjust_status"]]
        #     df.drop(["adjust_status"], axis=1, inplace=True)

        print('df', len(df), df.columns)
        df["end_time"] = df["end_time"].fillna(0)
        df["销售状态"] = '正常'
        df.loc[df["end_time"] == 0, "销售状态"] = df["sale_status"]
        df["businessline"] = 2
        df.loc[df["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"
        df_seller_status = pd.DataFrame(
            {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'],
             "saleStatus": [4, 9, 10, 11, 12]})
        df = df.merge(df_seller_status, left_on="销售状态", right_on="status")
        print('df', len(df), df.columns)
        # 20240108 增加和计划系统的对比， 不相同的状态才上传
        conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
        sql = """
        select sku,country_code as countryCode,sale_status_id 
        from yibai_product_kd_sync.yibai_status_oversea_sale_status
        """
        df_plan = conn_mx.ck_select_to_df(sql)
        df_plan1 = df_plan[['sku', 'countryCode']].drop_duplicates()
        df1 = df[['sku', 'countryCode']].drop_duplicates()
        df1['存在'] = 1
        df_plan1 = df_plan1.merge(df1, on=['sku', 'countryCode'], how='left')
        df_plan1 = df_plan1[df_plan1['存在'] != 1]
        df_plan1 = df_plan1[['sku', 'countryCode']].drop_duplicates()
        df_plan1['saleStatus'] = 11
        df_plan1['businessline'] = 2
        df = df.append(df_plan1)

        df = df.merge(df_plan, on=['sku', 'countryCode'], how='left')
        df['sale_status_id'] = df['sale_status_id'].fillna(11)
        df = df[df['saleStatus'] != df['sale_status_id']]

        df.drop(["sale_status", "销售状态", "status", "warehouse", "start_time", "end_time", 'sale_status_id'], axis=1, inplace=True)
        df = df.drop_duplicates()
        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"海外仓销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
        df = df.to_dict(orient='records')
        total_list, error_list = post_response(df)
        # 海外仓的销售状态传输结果
        # res_to_mongodb('oversea_status', total_list, error_list)

        if error_list:
            send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}",
                     mobiles=['+86-15872366806', '+86-18986845950'], is_all=False)
            raise Exception(f"海外仓销售状态传输结果传输异常，请下载对应文件检查")


def get_fba_data_from_mongodb(the_site, the_sku):
    mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
    my_db = mongo_client.yibai_sale_status
    # 获取日期最大值
    col_list = my_db.list_collection_names()
    df_k = pd.DataFrame()
    a_list = [int(i.split('_')[2]) for i in col_list if f'{the_site}' in i and 'error' not in i]
    if a_list:
        max_number = max(a_list)
        connect = my_db[f"{the_site}_status_{max_number}"]
        res_ite = connect.find({'status': 1})
        all_status_data_list = []
        for res in res_ite:
            if res['data']['succ_list']:
                for item in res['data']['succ_list']:
                    insert_list = [item['data'][f'{the_sku}'], item['data']['salesAccount'], item['data']['saleStatus']]
                    all_status_data_list.append(insert_list)
            if res['data']['fail_list']:
                for item in res['data']['fail_list']:
                    if item.get("fail_msg", "") == '该记录销售状态和传入的一致，无需更新':
                        insert_list = [item['data'][f'{the_sku}'], item['data']['salesAccount'],
                                       item['data']['saleStatus']]
                        all_status_data_list.append(insert_list)

        if all_status_data_list:
            df_k = pd.DataFrame(all_status_data_list)
            df_k.rename(columns={0: f'{the_sku}', 1: 'salesAccount', 2: 'adjust_status'}, inplace=True)
    return df_k


def get_fba_status(sql):
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_data = conn_mx.ck_select_to_df(sql)
    print(df_data)
    print('df_data', len(df_data), df_data.columns)
    result = df_data.to_csv(index=False)
    yibai_obs.obs_content_upload(f"1.csv", result, 'yibai_price/yibai_sale_status')
    if len(df_data) > 0:
        df_data = df_data.drop_duplicates(['account_id', 'sellerSku'], 'first')
        print(len(df_data))
        result = df_data.to_csv(index=False)
        yibai_obs.obs_content_upload(f"2.csv", result, 'yibai_price/yibai_sale_status')
        # df_data["end_time"] = df_data["end_time"].fillna(0)
        # df_data["销售状态"] = '正常'
        # df_data.loc[df_data["end_time"] == 0, "销售状态"] = df_data["adjustment_priority"]
        # df_data.loc[df_data["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"
        sql3 = """SELECT id,account_name FROM yibai_system_kd_sync.yibai_amazon_account """
        df_new = conn_mx.ck_select_to_df(sql3)
        print('df_new', len(df_new))

        df_data = df_data.merge(df_new, left_on="account_id", right_on="id")
        result = df_data.to_csv(index=False)
        yibai_obs.obs_content_upload(f"3.csv", result, 'yibai_price/yibai_sale_status')
        df_data["businessline"] = 1
        df_seller_status = pd.DataFrame(
            {"status": ['清仓', '正利润加快动销', '负利润加快动销', '正常', '涨价缩销'],
             "saleStatus": [4, 9, 10, 11, 12]})
        df = df_data.merge(df_seller_status, left_on="销售状态", right_on="status")
        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"4.csv", result, 'yibai_price/yibai_sale_status')
        print('df', len(df), df.columns)
        df = df.rename(columns={'account_name': 'salesAccount'})
        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"5.csv", result, 'yibai_price/yibai_sale_status')
        # df.drop(["account_id", "id", "adjustment_priority", "start_time", "end_time", "status", "销售状态"], axis=1,
        #         inplace=True)
        df.drop(["account_id", "id", "status", "销售状态"], axis=1, inplace=True)
        print('df', len(df), df.columns)
        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"6.csv", result, 'yibai_price/yibai_sale_status')
        # 剔除失败列表中的sku
        # df_k = get_fba_data_from_mongodb('fba', 'sellerSku')
        # print('df_k', len(df_k), df_k.columns)
        # if len(df_k):
        #     df_k = df_k.drop_duplicates(['sellerSku', 'salesAccount'], 'first')
        #     df = df.merge(df_k, on=['sellerSku', 'salesAccount'], how="left")
        #     print('df', len(df), df.columns)
        #     df = df[df["saleStatus"] != df["adjust_status"]]
        #     df.drop(["adjust_status"], axis=1, inplace=True)
        # print('df', len(df), df.columns)

        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"fba销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
        df = df.to_dict(orient='records')
        print(df)
        total_list, error_list = post_response(df)
        # fba的销售状态传输结果
        # res_to_mongodb('fba_status', total_list, error_list)
        # 20221128 失败内容写到obs
        df_total = pd.DataFrame({'total_list': total_list})
        result = df_total.to_csv(index=False)
        yibai_obs.obs_content_upload(f"fba销售状态{get_date()}-total_list.csv", result, 'yibai_price/yibai_sale_status')
        df_error = pd.DataFrame({'error_list': error_list})
        result = df_error.to_csv(index=False)
        yibai_obs.obs_content_upload(f"fba销售状态{get_date()}-error_list.csv", result, 'yibai_price/yibai_sale_status')

        if error_list:
            send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}",
                     mobiles=['+86-15872366806', '+86-18986845950'], is_all=False)
            raise Exception(f"fba销售状态传输结果传输异常，请下载对应文件检查")


def get_fbc_token():
    """获取java接口token"""
    java_url = 'http://tmsservice.yibainetwork.com:92/ordersys/services/ServiceAuthAccount/getAccessToken?type=java'
    response = requests.get(java_url)
    token_one = json.loads(response.text)['data']
    print(token_one)

    """获取认证token"""
    """
    开发环境 http://rest.dev.java.yibainetworklocal.com/mrp/api/getToken
    测试环境 http://rest.test.java.yibainetworklocal.com/mrp/api/getToken
    线上环境 http://rest.java.yibainetwork.com/mrp/api/getToken
    """

    # 线上环境
    url = f'http://rest.java.yibainetwork.com/mrp/api/getToken?access_token={token_one}'
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
    username = json.loads(res.text)["username"]
    return username, token


def fbc_post_response(data):
    print(f'将要更新数据总行数：{len(data)}')
    headers = {}
    """
    开发环境 http://dp.yibai-it.com:10203/api/SaleStatus/update_fbc_sale_status
    测试环境 http://dp.yibai-it.com:32018/api/SaleStatus/update_fbc_sale_status
    线上环境 http://plan.yibainetwork.com/api/SaleStatus/update_fbc_sale_status
    """

    url = 'http://plan.yibainetwork.com/api/SaleStatus/update_fbc_sale_status'

    username, token = get_fbc_token()
    x, y = divmod(len(data), 5000)
    if y != 0:
        x += 1
    i = 0
    total_list = []
    error_list = []
    while i < x:
        print(i)
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

        per_data_dict = {'token_info': json.dumps(token_info)}
        per_data = data[5000 * i:5000 * (i + 1)]

        per_data = per_data.to_dict('records')
        per_data_dict.update({'data': json.dumps(per_data)})
        # print(per_data_dict)
        response = requests.request("POST", url, data=per_data_dict, headers=headers, files=[])
        response_dict = json_response(response, per_data_dict)
        # print(response_dict)
        total_list.append(response_dict)
        if response.status_code != 200:
            error_list.append(response_dict)
        i += 1
    return total_list, error_list


def get_fbc_data_from_mongodb(the_site, the_sku):
    mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
    my_db = mongo_client.yibai_sale_status
    # 获取日期最大值
    col_list = my_db.list_collection_names()
    df_k = pd.DataFrame()
    a_list = [int(i.split('_')[2]) for i in col_list if f'{the_site}' in i and 'error' not in i]
    if a_list:
        max_number = max(a_list)
        connect = my_db[f"{the_site}_status_{max_number}"]
        res_ite = connect.find({'status': 1})
        all_status_data_list = []
        for res in res_ite:
            if res['success_list']:
                for item in res['success_list']:
                    insert_list = [item[f'{the_sku}'], item['sale_status']]
                    all_status_data_list.append(insert_list)
            if res['fail_list']:
                for item in res['fail_list']:
                    if item.get("message", "") == '该记录销售状态和传入的一致，无需更新':
                        insert_list = [item[f'{the_sku}'], item['sale_status']]
                        all_status_data_list.append(insert_list)
        print(all_status_data_list)

        if all_status_data_list:
            df_k = pd.DataFrame(all_status_data_list)
            df_k.rename(columns={0: f'{the_sku}', 1: 'adjust_status'}, inplace=True)
    return df_k


def get_fbc_status():
    # sql = f"""
    #         select
    #             account_id,
    #             seller_sku as platform_sku,
    #             status as adjustment_priority,
    #             # itemid as ean,
    #             start_time,
    #             end_time
    #         from
    #             fbc_clear_new
    #         order by start_time desc
    #     """
    # df_data = sql_to_pd(database='temp_database_wh', sql=sql, data_sys='数据部服务器')
    sql = """
    select a.platform_sku as platform_sku,a.sale_status as sale_status,b.short_name as short_name,a.site as site 
    from (
        select account_id,platform_sku,sale_status,site 
        from (
            select a.account_id as account_id,a.seller_sku as platform_sku,
                a.status as adjustment_priority,a.start_time,a.end_time,
                if(a.end_time is null, a.status, '正常') as `销售状态`,
                case 
                    when `销售状态` in ('清仓', '负利润加快动销') then 10
                    when `销售状态` in ('正利润加快动销', '加快动销') then 9
                    when `销售状态` = '正常' then 11
                end as sale_status,d.cn_name as site,
                1 as num
            from domestic_warehouse_clear.yibai_fbc_clear_new a 
            left join (
		    	select a.account_id as account_id,a.short_name as short_name,b.delivery_country as delivery_country
		    	from yibai_sale_center_system_sync.yibai_system_account a 
		    	left join yibai_sale_center_system_sync.yibai_system_auth_account b 
		    	on a.id=b.account_id
		    	where a.platform_code = 'CDISCOUNT'
		    ) b 
		    on a.account_id=b.account_id
            left join yibai_price_config.site d 
    		on b.delivery_country=d.en_abbr
            where a.status != '回调'
            order by a.start_time desc limit 1 by a.account_id,platform_sku,d.cn_name
            union all 
            select account_id,platform_sku,'' as  adjustment_priority,'' as start_time,'' as end_time,
                '正常' as `销售状态`, 11 as sale_status,site,2 as num
            from yibai_plan_stock_sync.yibai_fbc_sale_status
        )
        order by num asc limit 1 by account_id,platform_sku,site
    ) a 
    left join (
    	select a.account_id as account_id,a.short_name as short_name,b.delivery_country as delivery_country
    	from yibai_sale_center_system_sync.yibai_system_account a 
    	left join yibai_sale_center_system_sync.yibai_system_auth_account b 
    	on a.id=b.account_id
    	where a.platform_code = 'CDISCOUNT'
    ) b 
    on a.account_id=b.account_id
    left join (
        select account_id,platform_sku,sale_status,site 
        from yibai_plan_stock_sync.yibai_fbc_sale_status
    ) c
    on a.account_id=c.account_id and a.platform_sku=c.platform_sku and a.site=c.site
    where a.sale_status != c.sale_status and (not (a.sale_status=11 and c.sale_status=0))
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_mx.ck_select_to_df(sql)

    # print('df', len(df), df.columns)
    if len(df) > 0:
        # df_data = df_data.drop_duplicates(['account_id', 'platform_sku'], 'first')
        # print(len(df_data))
        #
        # df_data["end_time"] = df_data["end_time"].fillna(0)
        # df_data["销售状态"] = '正常'
        # df_data.loc[df_data["end_time"] == 0, "销售状态"] = df_data["adjustment_priority"]
        # df_data.loc[df_data["销售状态"] == "加快动销", "销售状态"] = "正利润加快动销"
        #
        # # df_seller_status = pd.DataFrame(
        # #     {"status": ['清仓', '正利润加快动销', '负利润加快动销'], "sale_status": [4, 9, 10]})
        # df_seller_status = pd.DataFrame(
        #     {"status": ['清仓', '正利润加快动销', '负利润加快动销'], "sale_status": [10, 9, 10]})
        # df = df_data.merge(df_seller_status, left_on="销售状态", right_on="status")
        # # df = df[df['sale_status'].isin([10, 9, 10])]
        # df = df[df['sale_status'].isin([9, 10])]
        # print('df', len(df), df.columns)
        # df_account = get_fbc_account_data()
        # print('df_account', len(df_account), df_account.columns)
        # df = df.merge(df_account, on='account_id')
        #
        # df.drop(["account_id", "adjustment_priority", "start_time", "end_time", "status", "销售状态"], axis=1,
        #         inplace=True)
        # # df['ean'] = df['ean'].apply(lambda m_x: m_x[3:])
        # print('df', len(df), df.columns)

        # 剔除失败列表中的sku
        # df_k = get_fbc_data_from_mongodb('fbc', 'platform_sku')
        # print('df_k', len(df_k), df_k.columns)
        # if len(df_k):
        #     df_k = df_k.drop_duplicates(['platform_sku'], 'first')
        #     print('df_k', len(df_k), df_k.columns)
        #     df = df.merge(df_k, on=['platform_sku'], how="left")
        #     df = df[df["sale_status"] != df["adjust_status"]]
        #     df.drop(["adjust_status"], axis=1, inplace=True)
        print('df', len(df), df.columns)

        result = df.to_csv(index=False)
        yibai_obs.obs_content_upload(f"fbc销售状态{get_date()}.csv", result, 'yibai_price/yibai_sale_status')
        total_list, error_list = fbc_post_response(df)
        # # fba的销售状态传输结果
        # res_to_mongodb('fbc_status', total_list, error_list)

        if error_list:
            send_msg('动销组定时任务推送', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{error_list[0]}")
            raise Exception(f"fbc销售状态传输结果传输异常，请下载对应文件检查")


def post_sku_status():
    try:
        headers = {
            'Content-Type': 'application/json'
        }
        url = 'http://eblisting.yibainetwork.com/services/ebay/ebayapi/GetSaleStatusByUpdateAt'
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            send_msg('动销组定时任务推送', 'ebay平台更新销售状态',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}已发送信号,通知ebay平台更新销售状态",
                     is_all=False)
    except:
        send_msg('动销组定时任务推送', 'ebay平台更新销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}ebay平台更新销售状态异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def fail_msg(context):
    # 调用 rpc 服务
    err_info = context['task_instance'].dag_id + "\n" + context['task_instance'].task_id + "\n" + "任务失败"
    send_msg(
        ding_type='动销组定时任务推送',
        task_type="销售状态上传异常",
        text=f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}销售状态上传出现问题,请及时排查,{err_info}",
        mobiles=['+86-15872366806', '+86-18986845950'],
        is_all=False,
        status='失败'
    )


def fbc_status_update():
    try:
        get_fbc_status()
        send_msg('动销组定时任务推送', '上传fbc销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fbc销售状态上传成功",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '上传fbc销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fbc销售状态上传异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def fba_status_update():
    try:
        sql = f"""   
            select if(adjustment_priority='', account_id1, account_id) as account_id,
                   if(adjustment_priority='', seller_sku1, seller_sku) as sellerSku,
                   if(adjustment_priority='', '正常', adjustment_priority) as `销售状态`
            from (
                select a.*,b.account_id as account_id1,b.seller_sku as seller_sku1,b.`销售状态` as `销售状态` 
                from (
                    select account_id,seller_sku,adjustment_priority 
                    from (
                        select account_id,seller_sku,adjustment_priority,1 as num 
                        from domestic_warehouse_clear.yibai_fba_clear_new 
                        where end_time is null
                        union all 
                        select distinct toInt32(b.id) as account_id,a.seller_sku as seller_sku,'正常' as adjustment_priority,2 as num
                        from yibai_product_kd_sync.yibai_fba_sale_status a 
                        inner join yibai_system_kd_sync.yibai_amazon_account b 
                        on lower(a.sales_account)=lower(b.account_name) 
                    )
                    order by num asc limit 1 by account_id,seller_sku
                ) a 
                FULL outer join (
                    select distinct toInt32(b.id) as account_id,a.seller_sku,
                        case 
                            when sale_status=4 then '清仓'
                            when sale_status=9 then '正利润加快动销'
                            when sale_status=10 then '负利润加快动销'
                            when sale_status=11 then '正常'
                            when sale_status=12 then '涨价缩销'
                            else '其他'
                        end as `销售状态`
                    from yibai_product_kd_sync.yibai_fba_sale_status a 
                    inner join yibai_system_kd_sync.yibai_amazon_account b 
                    on lower(a.sales_account)=lower(b.account_name) 
                ) b 
                on a.account_id=b.account_id and a.seller_sku=b.seller_sku
                where adjustment_priority != `销售状态`
            )
            settings max_memory_usage = 20000000000
            """
        # """
        #     where sellerSku in (
        #         select seller_sku from (
        #             select account_num,seller_sku,count() as num
        #             from (
        #                 select distinct lower(b.account_num) as account_num,a.seller_sku as seller_sku,
        #                     case
        #                         when sale_status=4 then '清仓'
        #                         when sale_status=9 then '正利润加快动销'
        #                         when sale_status=10 then '负利润加快动销'
        #                         when sale_status=11 then '正常'
        #                         when sale_status=12 then '涨价缩销'
        #                         else '其他'
        #                     end as `销售状态`
        #                 from yibai_product_kd_sync.yibai_fba_sale_status a
        #                 inner join yibai_system_kd_sync.yibai_amazon_account b
        #                 on a.sales_account=b.account_name
        #                 left join domestic_warehouse_clear.yibai_site_table_amazon c
        #                 on b.site=c.site
        #                 where c.area = '泛欧'
        #             )
        #             group by account_num,seller_sku
        #             having num>1
        #             )
        #     )
        #     """
        get_fba_status(sql)
        send_msg('动销组定时任务推送', '上传fba销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fba销售状态上传成功",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '上传fba销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}fba销售状态上传异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def fba_all_status_data_update():
    sql = f"""
        select 
            account_id,
            seller_sku as sellerSku,
            adjustment_priority,
            start_time,
            end_time 
        from 
            domestic_warehouse_clear.yibai_fba_clear_new
        order by start_time desc limit 1 by account_id,seller_sku
    """
    try:
        get_fba_status(sql)
    except:
        get_fba_status(sql)


def oversea_status_update():
    try:
        get_oversea_status()
        send_msg('动销组定时任务推送', '上传海外仓销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓销售状态上传成功",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '上传海外仓销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓销售状态上传异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def china_status_update():
    try:
        get_china_status()
        send_msg('动销组定时任务推送', '上传国内仓销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}国内仓销售状态上传成功",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '上传国内仓销售状态',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}国内仓销售状态上传异常,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


def status_info_check():
    try:
        # mongo_client = pymongo.MongoClient('mongodb://tanle:tanle08201148@121.37.248.212:27017/yibai_sale_status')
        # my_db = mongo_client.yibai_sale_status
        # # 转化为list形式
        # col_list = my_db.list_collection_names()
        # # 遍历文档
        # total_collec = [collection_name for collection_name in col_list]
        # print_list = []
        # for one_business in ["china", "oversea", "fba"]:
        #     print(one_business)
        #     now_business_list = sorted(
        #         [collection_name for collection_name in total_collec if
        #          one_business in collection_name and 'error' not in collection_name], reverse=True)
        #     if now_business_list:
        #         collection_name = now_business_list[0]
        #         connect = my_db[collection_name]
        #         res_ite = connect.find()
        #         all_status_data_list = []
        #         for res in res_ite:
        #             status = res["status"]
        #             error_mess = res["errorMess"]
        #             if "data" in res.keys():
        #                 if res['data']['fail_list']:
        #                     for item in res['data']['fail_list']:
        #                         if item.get("fail_msg", "") != '该记录销售状态和传入的一致，无需更新':
        #                             item_dict = item["data"]
        #                             item_dict["status"] = status
        #                             item_dict["error_mess"] = error_mess
        #                             item_dict["msg"] = item.get("fail_msg", "")
        #                             all_status_data_list.append(item_dict)
        #             else:
        #                 all_status_data_list.append({"status": status, "error_mess": error_mess})
        #         if all_status_data_list:
        #             print_list.append({f'{one_business}': f'<{one_business}>存在错误的销售状态，请查看日志，并部分更新'})
        # if print_list:
        #     raise Exception(print_list)
        pass
    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    # fba_status_update()
    # china_status_update()
    # fbc_status_update()
    # oversea_status_update()
    get_china_status_new()