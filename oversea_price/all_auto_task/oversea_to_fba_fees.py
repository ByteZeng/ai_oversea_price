import requests
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
import re
import time
import pandas as pd
import requests
import datetime
import time
from sqlalchemy import create_engine
from pulic_func.base_api import mysql_connect as pbm
import traceback
# from all_auto_task.dingding import send_msg
# from all_auto_task.nacos_api import get_user
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from tqdm import tqdm
from pulic_func.base_api.base_function import mysql_escape
today = datetime.date.today()


def cj_fanmei_report_get(sys_name, db_name, table_name):
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


def jiekou_data():
    # df0 = cj_fanmei_report_get(sys_name="tms_s", db_name="yibai_tms_logistics", table_name="tms_shelf_out_gc_son")
    # df1 = cj_fanmei_report_get(sys_name="tms_s", db_name="yibai_tms_logistics", table_name="tms_shelf_out_gc")
    # print(df1.columns)
    # print(df0.tail())
    # print(df0.columns)
    # df = df0.merge(df1, on="gc_no", how="left")
    # df.drop_duplicates(inplace=True)
    sql = """
    select distinct a.sku,b.account_name,b.warehouse_name,a.amount
    from yibai_tms_logistics_sync.yibai_platform_shelf_out_gc_son a 
    inner join yibai_tms_logistics_sync.yibai_platform_shelf_out_gc b 
    on a.gc_no =b.gc_no 
    """
    conn_ck = pd_to_ck(database='yibai_tms_logistics_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df1 = df[["sku", "account_name", "warehouse_name", "amount"]]
    df1["sku"] = df1["sku"].apply(lambda x: str(x).replace("'", ""))
    df1 = df1.groupby(["sku", "warehouse_name", "account_name"]).agg({"amount": np.sum}).reset_index()
    # sql = """
    # select E.warehouse_name as warehouse_name,F.name as warehouse
    # from yibai_logistics.yibai_warehouse E
    # LEFT JOIN yibai_logistics.yibai_warehouse_category F
    # ON E.ebay_category_id = F.id
    # """
    # wa = sql_to_pd(database='yibai_logistics', data_sys='数据管理部同步服务器1', sql=sql)
    sql = """
    select E.warehouse_name as warehouse_name,F.name as warehouse 
    from yibai_logistics_tms_sync.yibai_warehouse E
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category F 
    ON E.ebay_category_id = F.id
    """
    wa = conn_ck.ck_select_to_df(sql)

    df1 = df1.merge(wa, on=['warehouse_name'], how='left')

    # 把accoun_name改得好匹配
    df1['站点'] = df1['account_name'].apply(lambda x: ''.join(re.findall('[\u4e00-\u9fa5]', x)))
    df1.loc[df1['account_name'].str.contains('[\u4e00-\u9fa5]'), 'account_name'] = df1['account_name'].apply(
        lambda x: ''.join(re.findall('[A-Za-z0-9]', x)))
    df1.loc[df1['account_name'].str.contains('-'), 'account_name'] = df1['account_name'].apply(lambda x: x[:-3])
    df1.loc[df1['站点'] == '法国', 'site'] = 'fr'
    df1.loc[df1['站点'] == '澳大利亚', 'site'] = 'au'
    df1.loc[df1['站点'] == '德国', 'site'] = 'de'
    df1.loc[df1['站点'] == '英国', 'site'] = 'uk'
    df1.loc[df1['站点'] == '美国', 'site'] = 'us'
    df1.loc[df1['站点'] == '意大利', 'site'] = 'it'
    df1.loc[df1['站点'] == '欧洲', 'site'] = 'de'
    df1.loc[df1['站点'] == '西班牙', 'site'] = 'sp'
    df1.loc[(df1['site'].isnull()) & (df1['warehouse'].str.contains('法国')), 'site'] = 'fr'
    df1.loc[(df1['site'].isnull()) & (df1['warehouse'].str.contains('澳洲')), 'site'] = 'au'
    df1.loc[(df1['site'].isnull()) & (df1['warehouse'].str.contains('德国')), 'site'] = 'de'
    df1.loc[(df1['site'].isnull()) & (df1['warehouse'].str.contains('英国')), 'site'] = 'uk'
    df1.loc[(df1['site'].isnull()) & (df1['warehouse'].str.contains('美国仓')), 'site'] = 'us'

    # 匹配上account_id
    # sql = 'select id as account_id,short_name,account_num,site from yibai_system.yibai_amazon_account'
    # account = sql_to_pd(sql=sql, database='yibai_system', data_sys='AMAZON刊登库')
    sql = """
    select id as account_id,short_name,account_num,site from yibai_system_kd_sync.yibai_amazon_account
    """
    account = conn_ck.ck_select_to_df(sql)
    account['short_name'] = account['short_name'].str.upper()
    account['account_num'] = account['account_num'].str.upper()
    ac1 = account[['account_id', 'short_name']]
    df1['account_name'] = df1['account_name'].str.upper()
    df2 = df1.merge(ac1, left_on=['account_name'], right_on=['short_name'], how='left')
    ac2 = account[['account_id', 'account_num', 'site']]
    df2 = df2.merge(ac2, left_on=['account_name', 'site'], right_on=['account_num', 'site'], how='left')
    df2.rename(columns={'account_id_x': 'account_id'}, inplace=True)
    df2.loc[df2['account_id'].isnull(), 'account_id'] = df2['account_id_y']
    df2 = df2[['sku', 'warehouse_name', 'account_name', 'amount', 'warehouse', 'site', 'account_id']]

    df2 = df2[df2['account_id'].notnull()]
    df2['account_id'] = df2['account_id'].astype('int')
    del df2['account_name']
    del df2['site']
    sh = account[['account_id', 'short_name', 'site']]
    sh.columns = ['account_id', 'account_name', 'site']
    df2 = df2.merge(sh, on=['account_id'], how='left')
    df2 = df2[['sku', 'warehouse_name', 'account_name', 'amount', 'warehouse', 'site', 'account_id']]

    print(df2.info())
    return df2

def get_date():
    d = datetime.date.today() - datetime.timedelta(days=2)
    return d


def get_data(conn):
    diaobo = jiekou_data()
    sku_list = tuple(set(diaobo['sku']))
    account_list = tuple(set(diaobo['account_id']))

    # 先得到FBA大货链接
    # sql = f"""SELECT * FROM `yibai_oversea_amazon_asin_fba`  where sku in  {sku_list}"""
    # df0 = conn.read_sql(sql)
    # df0.sort_values(by=['DATE'], ascending=False, inplace=True)
    # df0 = df0[df0['sku'].notnull()]
    # df0.drop_duplicates(subset=['account_id', 'seller_sku', 'sku'], inplace=True)
    diaobo1 = diaobo[['sku']].drop_duplicates()
    diaobo1 = diaobo1.reset_index(drop=True)
    diaobo1['index'] = diaobo1.index
    diaobo1['index'] = diaobo1['index'].apply(lambda m: int(m/100))
    df0 = pd.DataFrame()
    for key, group in tqdm(diaobo1.groupby(['index'])):
        sku_list1 = mysql_escape(group, 'sku')
        sql = f"""
        select account_id,seller_sku,sku,asin,`DATE`,`status` 
        from (
            select *, row_number() over(partition by account_id,seller_sku,sku order by `DATE` desc) as num
            from over_sea.yibai_oversea_amazon_asin_fba
            where sku in ({sku_list1}) and account_id in {account_list}
        ) a
        where num = 1
        """
        df01 = conn.read_sql(sql)
        df0 = df0.append(df01)
    print('大货表匹配前调拨明细的长度', len(df0))

    # 先把account_id转化为整型
    df0["account_id"] = df0["account_id"].astype("int")
    print("df0的数据类型", df0.info())

    df = diaobo.merge(df0, on=['account_id', 'sku'])
    print('匹配到调拨表后的', df.info())

    # 匹配头程费用
    sql2 = f"""
        select sku,warehouseid,warehouseName,firstCarrierCost 
        from over_sea.oversea_transport_fee_useful 
        where platform='AMAZON' and sku in {sku_list}
    """
    df2 = conn.read_sql(sql2)
    print(df2.columns)
    df = df.merge(df2, left_on=["sku", "warehouse_name"], right_on=["sku", "warehouseName"], how="left")
    print(df.head())
    df.drop_duplicates(inplace=True)
    return df


from all_auto_task.scripts_ck_client import CkClient


def product_pipei(data, sku):
    sql = f"""select sku,toFloat64(pur_length_pack) *toFloat64(pur_weight_pack) *toFloat64(pur_width_pack) 
               as "体积重",case when toFloat64(weight_out_storage)=0 
               then toFloat64(pur_weight_pack) else toFloat64(weight_out_storage) end as "实际重"
               from yibai_prod_base_sync.yibai_prod_sku 
               order by create_time desc 
               """
    ck_client = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    # df1=pbm.sql_to_pd(sql=sql,database="yibai_product",data_sys="AMAZON刊登库")
    df1 = ck_client.ck_select_to_df(ck_sql=sql)
    df1["体积重"] = df1["体积重"] / 6000
    df1["实际重"] = df1["实际重"] / 1000
    df1["最终重"] = df1['实际重']
    # df1.loc[df1["体积重"]>=df1["实际重"],"最终重"]=df1["体积重"]
    # df1.loc[df1["体积重"]<df1["实际重"],"最终重"]=df1["实际重"]
    df1.drop(axis=1, columns=["体积重", "实际重"], inplace=True)
    df1.drop_duplicates(subset=["sku"], inplace=True)
    data = data.merge(df1, on="sku", how="left")
    return data


def calu_tiaobo(df, conn):
    df["调拨费"] = 0
    # 20220127换配置
    df.loc[(df["warehouse_name"].str.contains("谷仓")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("东")), "调拨费"] = df["最终重"] * 12.84
    df.loc[(df["warehouse_name"].str.contains("谷仓")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("东")) & (df['warehouse_name'].str.contains("亚马逊")), "调拨费"] = df[
                                                                                                                  "最终重"] * 11.50
    df.loc[(df["warehouse_name"].str.contains("谷仓")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("西")), "调拨费"] = df["最终重"] * 14.62
    df.loc[(df["warehouse_name"].str.contains("谷仓")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("西")) & (df['warehouse_name'].str.contains("亚马逊")), "调拨费"] = df[
                                                                                                                  "最终重"] * 8.65
    df.loc[(df["warehouse_name"].str.contains("万邑通")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("东")), '调拨费'] = df['最终重'] * 28.91
    df.loc[(df["warehouse_name"].str.contains("万邑通")) & (df["warehouse_name"].str.contains("美")) & (
        df["warehouse_name"].str.contains("西")), '调拨费'] = df['最终重'] * 22.52
    df.loc[(df["warehouse_name"] == "万邑通澳洲墨尔本仓"), "调拨费"] = df["最终重"] * 8.95
    df.loc[(df["warehouse_name"] == "谷仓法国仓"), "调拨费"] = df["最终重"] * 10.78
    df.loc[(df["warehouse_name"] == "谷仓法国仓") & (df["site"] == "de"), "调拨费"] = df["最终重"] * 12.29
    df.loc[(df["warehouse_name"] == "谷仓法国仓") & (df["site"] == "fr"), "调拨费"] = df["最终重"] * 9.93
    # df.loc[(df["warehouse_name"]=="谷仓法国仓")&(df["site"].isin(['es','sp'])),"调拨费"]=df["最终重"]*8.52
    df.loc[(df["warehouse_name"] == "谷仓捷克仓"), "调拨费"] = df["最终重"] * 6.23
    df.loc[(df["warehouse_name"] == "谷仓捷克仓") & (df["site"] == "de"), "调拨费"] = df["最终重"] * 6.09
    df.loc[(df["warehouse_name"] == "谷仓捷克仓") & (df["site"] == "fr"), "调拨费"] = df["最终重"] * 9.55
    df.loc[(df["warehouse_name"] == "谷仓捷克仓") & (df["site"] == "it"), "调拨费"] = df["最终重"] * 11.67
    # df.loc[(df['warehouse_name']=='谷仓捷克仓')&(df['site'].isin(['es','sp'])),'调拨费']=df['最终重']*9.33
    df.loc[(df["warehouse_name"] == "谷仓英国仓"), "调拨费"] = df["最终重"] * 12.01
    df.loc[(df["warehouse_name"] == "万邑通澳洲悉尼仓"), "调拨费"] = df["最终重"] * 6.42
    df.loc[(df["warehouse_name"] == "万邑通德国仓-DE Warehouse", "调拨费")] = df["最终重"] * 2.55
    df.loc[(df["warehouse_name"] == "万邑通德国仓-DE Warehouse") & (
        df['warehouse_name'].str.contains('亚马逊')), "调拨费"] = df["最终重"] * 3.69
    # df.loc[(df["warehouse_name"]=="万邑通德国仓-DE Warehouse")&(df['site']=='de'),"调拨费"]=df["最终重"]*3.89
    # df.loc[(df["warehouse_name"]=="万邑通德国仓-DE Warehouse")&(df['site']=='fr'),"调拨费"]=df["最终重"]*5.41
    df.loc[(df["warehouse_name"]) == "万邑通英国仓-UK Warehouse", "调拨费"] = df["最终重"] * 6.91
    df.loc[(df["warehouse_name"].str.contains("万邑通英国")) & (df['warehouse_name'].str.contains("UKTW")), "调拨费"] = \
    df["最终重"] * 3.89
    df.loc[(df["warehouse_name"].str.contains("万邑通英国")) & (df['warehouse_name'].str.contains("UKGF")), "调拨费"] = \
    df["最终重"] * 4.54
    print(df.head())
    df["调拨费"] = df["调拨费"] * df["amount"]

    # 聚合到site,分别计算调拨费和头程费用
    print(df.columns)
    # 注释掉老程序
    # df.columns=['account_id', 'sku', 'seller_sku', 'asin', 'DATE', 'account_name',
    #  'short_name', 'site', 'warehouse_name', 'amount', "account_name.1",
    #  'warehouseid', 'warehouseName', 'firstCarrierCost', '头程费用', '最终重',
    #  '调拨费']
    try:
        df.drop(columns=["account_name.1"], inplace=True)
    except:
        pass
    print("--" * 30)
    print(df.columns)

    # 增加有泛美泛欧账号的链接
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    seller_sku_list = tuple(set(df['seller_sku']))
    # account = sql_to_pd(sql='select id as account_id,account_group,site from yibai_system.yibai_amazon_account',
    #                     database='yibai_system', data_sys='AMAZON刊登库')
    account = conn_ck.ck_select_to_df('select id as account_id,account_group,site from yibai_system_kd_sync.yibai_amazon_account')

    # 给调拨数据匹配上account_group
    # df=df.merge(account,on=['account_id'],how='left')
    print(df.info())

    sql = "SELECT account_id,seller_sku,fnsku FROM yibai_fba.fba_fnsku"
    fanmei = conn_ck.ck_select_to_df(sql)

    fanmei = fanmei.merge(account, on=['account_id'], how='left')
    print("fanmei", fanmei.columns)
    df = df.merge(fanmei, on=['account_id', 'seller_sku', 'site'], how='left')
    df = df.merge(fanmei, on=['account_group', 'seller_sku', 'fnsku'], how='left')

    print('匹配到泛链接之后的数据', df.info())

    df.loc[df['account_id_y'].notnull(), 'account_id_x'] = df['account_id_y']
    df.loc[df['site_y'].notnull(), 'site_x'] = df['site_y']
    df.drop(columns=['account_group', 'site_y', 'account_id_y'], axis=1, inplace=True)
    df.rename(columns={'account_id_x': 'account_id', 'site_x': 'site'}, inplace=True)

    df0 = df.groupby(["account_id", "seller_sku", "site"])["调拨费", "amount", "头程费用"].sum().reset_index()
    df_all = df.copy()
    df1_1 = df_all[['account_id', 'seller_sku', 'sku']]
    df1_1.drop_duplicates(inplace=True)
    print(df0.columns)
    df0["最终调拨费"] = df0["调拨费"] / df0["amount"]
    df0["最终头程费用"] = df0["头程费用"] / df0["amount"]

    df0 = df0.loc[(df0["最终头程费用"].notnull()) & (df0["最终头程费用"] > 0), :]
    print(df0.columns)
    df0["最终调拨费"] = df0["最终调拨费"].apply(lambda x: round(x, 2))
    df0["最终头程费用"] = df0["最终头程费用"].apply(lambda x: round(x, 2))
    df0 = df0.merge(df1_1, on=['account_id', 'seller_sku'], how='left')

    df0['date'] = datetime.date.today()

    df0.drop_duplicates(inplace=True)

    df0.drop_duplicates(inplace=True)

    conn.to_sql(df0, 'yibai_oversea_to_fba_fees', if_exists='append')


def delete_data(conn):
    sql = f" delete from  yibai_oversea_to_fba_fees where date='{datetime.date.today()}'"
    conn.execute(sql)
    print(f'删除{datetime.date.today()}数据')


def url(ding_type):
    secret = ''
    access_token = ''
    if ding_type == '动销组定时任务推送':
        secret = 'SEC2dbc10f676641297b6fd934a80d2041efa54bfe4ff1621b93aa0b6da9a72d905'
        access_token = 'c3f16626d9c431a60dc7b00154fd7c5f0ce47e76aa5a3bebc4f91250bd1c0d98'
    elif ding_type == "测试":
        secret = "SEC601d422a690a5dda9c170d9534f18598711caad33a410e5eb95b74dac94a8fed"
        access_token = '13318b69991cd7388ed4661d826b3dd26bb8dbc18dea7b772d630fcb262f1c8a'

    timestamp = round(time.time() * 1000)
    secret_enc = secret.encode("utf-8")
    string_to_sign = "{}\n{}".format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode("utf-8")
    hmac_code = hmac.new(
        secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
    ).digest()
    sign = quote_plus(base64.b64encode(hmac_code))
    url = f"https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}"
    return url


def send_msg(ding_type: str = None,
             task_type: str = None,
             text: str = None,
             mobiles: Optional[List[str]] = None,
             is_all: bool = True,
             status: str = '成功', ):
    res = requests.post(url=url(ding_type),
                        headers={"Content-Type": "application/json"},
                        json={
                            "msgtype": "text",
                            "at": {
                                "atMobiles": mobiles or [],
                                "isAtAll": is_all,
                            },
                            "text": {"content": text},
                        },
                        )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")

    write_data(task_type, status, text)
    if ding_type == '动销组定时任务推送':
        send_action(url(ding_type),
                    title="动销任务状态",
                    text='动销任务状态',
                    btn_s=[
                        {
                            'title': '查看详细进度',
                            'actionURL': f'http://sjfxapi.yibainetwork.com:5050/api/price/task/{datetime.date.today().strftime("%Y%m%d")}'
                        }
                    ]
                    )


def write_data(task_type, status, text):
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""insert into timed_task_record values w{now_time, task_type, status, text}"""
    print(sql)
    try:
        conn.execute(sql)
    except:
        pass
    conn.close()


def send_action(url, title: str = None, text: str = None, btn_s: List = None):
    res = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "actionCard": {
                "title": title,
                "text": text,
                "btnOrientation": "1",
                "btns": btn_s
            },
            "msgtype": "actionCard"
        },
    )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")


def main(conn):
    df = get_data(conn)
    # df.to_excel("头程调拨费用.xlsx",index=False)
    # df=pd.read_excel("头程调拨费用.xlsx")
    df.drop_duplicates(inplace=True)
    df["头程费用"] = df["amount"] * df["firstCarrierCost"]
    df_sku = tuple(set(df["sku"]))
    df = product_pipei(data=df, sku=df_sku)
    print(df.info())
    calu_tiaobo(df, conn)


def check_data(conn):
    d = datetime.date.today() - datetime.timedelta(days=1)
    while True:
        sql1 = f"""select distinct * from yibai_oversea_to_fba_fees where date='{d}'"""
        df1 = conn.read_sql(sql1)
        if len(df1) > 0:
            break
        else:
            d = d - datetime.timedelta(days=1)
    sql2 = f"""select distinct * from yibai_oversea_to_fba_fees where date='{datetime.date.today()}'"""
    df2 = conn.read_sql(sql2)
    yes_data = len(df1)
    to_data = len(df2)
    df1 = df1.groupby(['site', 'date'])['最终调拨费', '最终头程费用'].sum().reset_index().rename(
        columns={"最终调拨费": "昨日调拨费", "最终头程费用": "昨日头程费"})
    df2 = df2.groupby(['site', 'date'])['最终调拨费', '最终头程费用'].sum().reset_index().rename(
        columns={"最终调拨费": "今日调拨费", "最终头程费用": "今日头程费"})
    # df3 = df1.merge(df2, on=['site'], how='left')

    yes_diaobo = df1['昨日调拨费'].sum()
    to_diaobo = df2['今日调拨费'].sum()
    yes_toucheng = df1['昨日头程费'].sum()
    to_toucheng = df2['今日头程费'].sum()
    # if (to_diaobo - yes_diaobo) / yes_diaobo > 0.1:
    #     send_msg("动销组定时任务推送","FBA大货调拨"
    #              ,f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}今日调拨比昨日调拨上涨了{"%.2f%%" % (((to_diaobo - yes_diaobo) / yes_diaobo) * 100)},超过10%的涨幅,数据异常,请检查数据!"""
    #              ,mobiles=['+86-13419546972','+86-15827138549'],is_all=False)
    #
    # elif (to_diaobo - yes_diaobo) / yes_diaobo < -0.1:
    #     print(f'今日调拨比昨日调拨下降了{"%.2f%%" % (((to_diaobo - yes_diaobo) / yes_diaobo) * 100)},超过10%的降幅,数据异常,请检查数据')
    #     send_msg("动销组定时任务推送", "FBA大货调拨"
    #              , f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}今日调拨比昨日调拨下降了{"%.2f%%" % (((to_diaobo - yes_diaobo) / yes_diaobo) * 100)},超过10%的降幅,请检查数据!"""
    #              , mobiles=['+86-13419546972', '+86-15827138549'], is_all=False)
    #
    # elif to_data - yes_data < -1000:
    #     send_msg("动销组定时任务推送", "FBA大货调拨"
    #              , f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}今日调拨数据比昨日调拨数据减少1000条以上,数据量异常,请检查数据!"""
    #              , mobiles=['+86-13419546972', '+86-15827138549'], is_all=False)
    #
    # else:
    #     send_msg("动销组定时任务推送", "FBA大货调拨"
    #              , f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}今日调拨数据量正常,较昨日调拨费用控制在10%以内,运行正常,请到over_sea.yibai_oversea_to_fba_fees查收数据!"""
    #              , mobiles=['+86-15827138549'], is_all=False)


def run_oversea_to_fba():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        delete_data(conn)
        main(conn)
        check_data(conn)
        #
        conn.close()
    except Exception as e:
        send_msg('动销组定时任务推送', "yibai_oversea_to_fba_fees",
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓到FBA大货头程及调拨费用出现问题,请及时排查,失败原因详情请查看airflow",
                 mobiles=['+86-13419546972', '+86-15827138549'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    run_oversea_to_fba()
