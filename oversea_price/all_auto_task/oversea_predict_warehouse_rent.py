import time

import pandas as pd
# from myself_public.public_function import *
import re
import threading
from multiprocessing.pool import Pool
# from myself_public.public_function import sql_to_pd,pd_to_sql
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
from all_auto_task.nacos_api import get_user

import time
import requests
from sqlalchemy import create_engine
import traceback
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck


def yb_oversea_sku_age(a):
    sql = f"""select distinct asku.*
    from 
    
    (select CASE 
    
    WHEN a.origin_sku LIKE 'GB-%%' THEN REPLACE(a.origin_sku,'GB-','') 
    
    WHEN a.origin_sku LIKE 'DE-%%' THEN REPLACE(a.origin_sku,'DE-','') 
    
    WHEN a.origin_sku LIKE 'FR-%%' THEN REPLACE(a.origin_sku,'FR-','') 
    
    WHEN a.origin_sku LIKE 'ES-%%' THEN REPLACE(a.origin_sku,'ES-','') 
    
    WHEN a.origin_sku LIKE 'IT-%%' THEN REPLACE(a.origin_sku,'IT-','') 
    
    WHEN a.origin_sku LIKE 'AU-%%' THEN REPLACE(a.origin_sku,'AU-','') 
    
    WHEN a.origin_sku LIKE 'CA-%%' THEN REPLACE(a.origin_sku,'CA-','') 
    
    WHEN a.origin_sku LIKE 'JP-%%' THEN REPLACE(a.origin_sku,'JP-','') 
    
    WHEN a.origin_sku LIKE 'US-%%' THEN REPLACE(a.origin_sku,'US-','') 
    
    WHEN a.origin_sku LIKE '%%DE' THEN REPLACE(a.origin_sku,'DE','')
    
    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_datacenter.yb_oversea_sku_age a
    where date='{datetime.date.today() - datetime.timedelta(days=a + 2)}'
    and status in (0,1)
    union ALL 
    select  CASE 
    
    WHEN  a.country in ('GB','UK') and a.origin_sku not LIKE 'GB-%%' THEN concat('GB-',a.origin_sku)
    WHEN  a.country IN ('CZ','CS','DE') and a.origin_sku not LIKE '%DE%%'   THEN concat('DE-',a.origin_sku)
    
    when a.country='FR' and a.origin_sku not like 'FR-%%' then concat('FR-',a.origin_sku)
    
    when a.country in ('ES','SP')  and a.origin_sku not like 'ES-%%' then concat('ES-',a.origin_sku)
    
    when a.country='IT' and a.origin_sku not like 'IT-%%' then concat('IT-',a.origin_sku)
    
    when a.country='AU' and a.origin_sku not like '%%AU%%' then concat('AU-',a.origin_sku)
    
    when a.country='CA' and a.origin_sku not like 'CA-%%' then concat('CA-',a.origin_sku)
    
    when a.country='JP' and a.origin_sku not like 'JP-%%' then concat('JP-',a.origin_sku)
    
    when a.country='US' and a.origin_sku not like 'US-%%' then concat('US-',a.origin_sku)
    
    ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    from yb_datacenter.yb_oversea_sku_age a
    where date='{datetime.date.today() - datetime.timedelta(days=a + 2)}'
    and status in (0,1)) asku"""
    return sql


def get_data(a, conn, ck_client):
    sql1 = f"""select distinct 
    a.sku  as sku,b.warehouse_name,
    a.cargo_type,a.country,sum(a.warehouse_stock) as warehouse_stock,a.inventory_age,
    sum(a.charge_total_price) as charge_total_price,a.charge_currency,a.oversea_type,   CASE 

    WHEN a.country='US' THEN '美国仓'

    WHEN a.country IN ('UK','GB') THEN '英国仓'

    WHEN a.country IN ('CZ','CS','DE') THEN '德国仓'

    WHEN a.country='FR' THEN '法国仓'

    WHEN a.country='IT' THEN '意大利仓'

    WHEN a.country='AU' THEN '澳洲仓'

    WHEN a.country IN ('ES','SP') THEN '西班牙仓'

    WHEN a.country='CA' THEN '加拿大仓'

    WHEN a.country='JP' THEN '日本仓'

    WHEN a.country='PL' THEN '德国仓'

    ELSE '美国仓' END AS warehouse,
    case

    when a.inventory_age between 0 and 30 then  'inv_age_0_to_30_days'

    when a.inventory_age between 30 and 60 then  'inv_age_30_to_60_days'

    when a.inventory_age between 60 and 90 then  'inv_age_60_to_90_days'

    when a.inventory_age between 90 and 120 then  'inv_age_90_to_120_days'

    when a.inventory_age between 120 and 150 then  'inv_age_120_to_150_days'

    when a.inventory_age between 150 and 180 then  'inv_age_150_to_180_days'

    when a.inventory_age between 180 and 270 then  'inv_age_180_to_270_days'

    when a.inventory_age between 270 and 360 then  'inv_age_270_to_360_days'

    when a.inventory_age between 360 and 9999999 then  'inv_age_360_plus_days'

    else null end as age_section
    from ({(yb_oversea_sku_age(a))}) a 
    left join yibai_logistics_tms_sync.yibai_warehouse b on a.warehouse_code=b.warehouse_code
    where a.status in (0,1)  
    group by a.sku,a.cargo_type,a.country,a.inventory_age,a.charge_currency,a.oversea_type,b.warehouse_name 
    """
    df = ck_client.ck_select_to_df(sql1)
    df.columns = ['sku', 'warehouse_name', 'cargo_type', 'country', 'warehouse_stock',
                  'inventory_age', 'charge_total_price', 'charge_currency',
                  'oversea_type', 'warehouse', 'age_section']
    print(df.columns)

    # # 给当前数据匹配上递四方的货型
    # sql1 = """SELECT SKU as sku,`货型`,`单SKU体积` FROM `4px_charge_detail`"""
    # df1 = conn.read_sql(sql1)
    # df = df.merge(df1, on=['sku'], how='left')
    # df.loc[(df['oversea_type'] == "4PX"), 'cargo_type'] = df['货型']
    # del df['货型']
    # 对于4PX跟仓租收费标准的仓库统一
    df['计费仓'] = None
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse_name'].str.contains('英')) & (
        df['warehouse_name'].str.contains('路腾')), '计费仓'] = '英国(路腾仓)'
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse_name'].str.contains('英')) & (
                df['计费仓'] != '英国(路腾仓)'), '计费仓'] = '英国（莱彻斯特仓）'
    try:
        df.loc[(df['oversea_type'] == "4PX") & (
                    ((df['warehouse_name'].str.contains('美')) & (df['warehouse_name'].str.contains('西')))
                    | ((df['warehouse_name'].str.contains('美')) & (
                df['warehouse_name'].str.contains('东')))), '计费仓'] = '美国(美西1、2仓+美东仓)'
    except:
        pass
    try:
        df.loc[(df['oversea_type'] == "4PX") & (((df['warehouse_name'] == '美国仓') & (
            df['warehouse_name'].str.contains('西')) & (df['warehouse_name'].str.contains('3'))) |
                                                (((df['warehouse'] == '美国仓')) & (df['warehouse_name'].str.contains(
                                                    '南')))), '计费仓'] = '美国(美西3仓+美南仓)'
    except:
        pass

    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '德国仓') & (
        df['warehouse_name'].str.contains('BRE')), '计费仓'] = '德国(不来梅仓)'

    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '德国仓') & (
                df['计费仓'] != '德国(不来梅仓)'), '计费仓'] = '德国(法兰仓+高斯海姆仓)'

    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '澳洲仓'), '计费仓'] = '澳洲'
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse_name'].str.contains('捷克')), '计费仓'] = '捷克'
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '意大利仓'), '计费仓'] = '意大利'
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '加拿大仓'), '计费仓'] = '加拿大'
    df.loc[(df['oversea_type'] == "4PX") & (df['warehouse'] == '日本仓'), '计费仓'] = '日本'
    df.loc[(df['oversea_type'] == "4PX") & (df['计费仓'].isnull()), '计费仓'] = '西/比/波/法'
    df.loc[df['计费仓'].isnull(), '计费仓'] = df['warehouse']

    print(1)
    # 不带仓表的sku
    # df1 = df.copy()
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "GB-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "DE-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "FR-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "ES-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "IT-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "AU-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "CA-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "JP-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[:3] == "US-"), 'sku'] = df1['sku'].apply(lambda x: str(x)[3:])
    # df1.loc[df1['sku'].apply(lambda x: str(x)[-2:] == "DE"), 'sku'] = df1['sku'].apply(lambda x: str(x)[:-2])
    #
    # df2 = df.copy()
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "GB-"))) & (df2['country'].isin(['UK', 'GB'])), 'sku'] = 'GB-' + \
    #                                                                                                              df2[
    #                                                                                                                  'sku']
    # df2.loc[
    #     (~(df2['sku'].apply(lambda x: str(x)[:3] == "DE-"))) & (~(df2['sku'].apply(lambda x: str(x)[-2:] == "DE"))) & (
    #         df2['country'].isin(['CZ', 'CS', 'DE'])), 'sku'] = 'DE-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "FR-"))) & (df2['country'] == 'FR'), 'sku'] = 'FR-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "ES-"))) & (df2['country'].isin(['ES', 'SP'])), 'sku'] = 'ES-' + \
    #                                                                                                              df2[
    #                                                                                                                  'sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "IT-"))) & (df2['country'] == 'IT'), 'sku'] = 'IT-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "AU-"))) & (df2['country'] == 'AU'), 'sku'] = 'AU-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "CA-"))) & (df2['country'] == 'CA'), 'sku'] = 'CA-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "JP-"))) & (df2['country'] == 'JP'), 'sku'] = 'JP-' + df2['sku']
    # df2.loc[(~(df2['sku'].apply(lambda x: str(x)[:3] == "US-"))) & (df2['country'] == 'US'), 'sku'] = 'US-' + df2['sku']
    #
    # df3 = df[
    #     (~(df['sku'].apply(lambda x: str(x)[:3] == "DE-"))) & (~(df['sku'].apply(lambda x: str(x)[-2:] == "DE"))) & (
    #         df['country'].isin(['CZ', 'CS', 'DE']))]
    # df3.loc[
    #     (~(df3['sku'].apply(lambda x: str(x)[:3] == "DE-"))) & (~(df3['sku'].apply(lambda x: str(x)[-2:] == "DE"))) & (
    #         df3['country'].isin(['CZ', 'CS', 'DE'])), 'sku'] = df3['sku'] + 'DE'
    #
    # df0 = pd.concat([df1, df2, df3])
    # df0.drop_duplicates(inplace=True)
    df = df.sort_values(by=['inventory_age'], ascending=False).reset_index(drop=True)

    # 如果当天有数据就用当天的否则就用最近的数据
    df1 = pd.DataFrame()

    while True:
        sql1 = f"""
            select sku as sku,warehouse,day_sales as day_sales from over_sea.dwm_sku_temp_info 
            where date_id='{datetime.date.today() - datetime.timedelta(days=a)}' and available_stock>0
        """
        print(sql1)
        df11 = conn.read_sql(sql1)
        print(df11.head())
        if len(df11) == 0:
            a += 1
        else:
            df1 = df11.copy()
            break

    df = df.merge(df1, on=["sku", "warehouse"], how="inner")
    df["day_sales"].fillna(0, inplace=True)
    df["estimated_sales_days"] = 0
    df.loc[df["day_sales"] > 0, "estimated_sales_days"] = df["warehouse_stock"] / df["day_sales"]
    df.loc[df["day_sales"] == 0, "estimated_sales_days"] = 365
    print(df.head())
    return df


def base_data(df_all):
    # 优化代码20220104
    df_all = df_all.sort_values(by=['inventory_age'], ignore_index=True, ascending=False)

    # 2022/1/17要求万邑通优先出，其次按照库龄最大出库卖
    df_1 = df_all[df_all['oversea_type'] == 'WYT']
    df_2 = df_all[df_all['oversea_type'] != 'WYT']
    df_all = pd.concat([df_1, df_2])
    df_all['辅助列'] = df_all.groupby(['sku', 'warehouse'])['estimated_sales_days'].cumsum().values

    df_all.loc[df_all["day_sales"] == 0, "day_sales"] = 0.001
    df = df_all.copy()
    df.drop_duplicates(inplace=True)
    df['开始库龄'] = 0
    df['预计库龄'] = df['inventory_age'] + df['辅助列']
    df['开始库龄'] = df['预计库龄'] - df['estimated_sales_days']
    ## 计算在这个阶段上仓租的库存
    df["0<x<=30"] = 0
    df["30<x<=60"] = 0
    df["60<x<=90"] = 0
    df["90<x<=120"] = 0
    df["120<x<=180"] = 0
    df["180<x<=270"] = 0
    df["270<x<=360"] = 0
    df["x>360"] = 0
    df.loc[df['inventory_age'] <= 30, '0<x<=30'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 30) & (df['预计库龄'] > 30), '30<x<=60'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (30 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 30) & (df['inventory_age'] <= 60)), '30<x<=60'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 60) & (df['预计库龄'] > 60), '60<x<=90'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (60 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 60) & (df['inventory_age'] <= 90)), '60<x<=90'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 90) & (df['预计库龄'] > 90), '90<x<=120'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (90 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 90) & (df['inventory_age'] <= 120)), '90<x<=120'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 120) & (df['预计库龄'] > 120), '120<x<=180'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (120 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 120) & (df['inventory_age'] <= 180)), '120<x<=180'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 180) & (df['预计库龄'] > 180), '180<x<=270'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (180 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 180) & (df['inventory_age'] <= 270)), '180<x<=270'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 270) & (df['预计库龄'] > 270), '270<x<=360'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (270 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[((df['inventory_age'] > 270) & (df['inventory_age'] <= 360)), '270<x<=360'] = df['warehouse_stock']
    df.loc[(df['inventory_age'] <= 360) & (df['预计库龄'] > 360), 'x>360'] = df.apply(
        lambda x: min(x['warehouse_stock'], x['warehouse_stock'] - (360 - x['开始库龄']) * x['day_sales']), axis=1)
    df.loc[df['inventory_age'] > 360, 'x>360'] = df['warehouse_stock']

    # 收仓租库存不能小于0
    df.loc[df["0<x<=30"] < 0, '0<x<=30'] = 0
    df.loc[df["30<x<=60"] < 0, '30<x<=60'] = 0
    df.loc[df["60<x<=90"] < 0, '60<x<=90'] = 0
    df.loc[df["90<x<=120"] < 0, '90<x<=120'] = 0
    df.loc[df["120<x<=180"] < 0, "120<x<=180"] = 0
    df.loc[df["180<x<=270"] < 0, "180<x<=270"] = 0
    df.loc[df["270<x<=360"] < 0, "270<x<=360"] = 0
    df.loc[df["x>360"] < 0, "x>360"] = 0

    df_new_1 = df.copy()

    print(1, len(df_new_1))
    df_new_1.rename(columns={"预计库龄": "结束库龄"}, inplace=True)

    df_new_1 = shouzutianshu(df_new_1)
    del df_new_1['辅助列']

    return df_new_1


def shouzutianshu(df):
    df['0~30收租天数'] = 0
    df['30~60收租天数'] = 0
    df['60~90收租天数'] = 0
    df["90~120收租天数"] = 0
    df['120~180收租天数'] = 0
    df['180~270收租天数'] = 0
    df['270~360收租天数'] = 0
    df['360以上收租天数'] = 0
    df.loc[(df['inventory_age'] <= 30), '0~30收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - x['inventory_age'], 30 - x['inventory_age']]), axis=1)
    df.loc[(df['inventory_age'] <= 30) & (df['结束库龄'] > 30), '30~60收租天数'] = df.apply(
        lambda x: min(30, x['结束库龄'] - 30),
        axis=1)
    df.loc[(df['inventory_age'] <= 60) & (df['inventory_age'] > 30), '30~60收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 60 - x['inventory_age']), axis=1)
    df.loc[(df['inventory_age'] <= 60) & (df['结束库龄'] > 60), '60~90收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - 60, 30]),
        axis=1)
    df.loc[(df['inventory_age'] <= 90) & (df['inventory_age'] > 60), '60~90收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 90 - x['inventory_age']), axis=1)
    df.loc[(df['inventory_age'] <= 90) & (df['结束库龄'] > 90), '90~120收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - 90, 30]), axis=1)
    df.loc[(df['inventory_age'] <= 120) & (df['inventory_age'] > 90), '90~120收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 120 - x['inventory_age']), axis=1)
    df.loc[(df['inventory_age'] <= 120) & (df['结束库龄'] > 120), '120~180收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - 120, 60]), axis=1)
    df.loc[(df['inventory_age'] <= 180) & (df['inventory_age'] > 120), '120~180收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 180 - x['inventory_age']), axis=1)
    df.loc[(df['inventory_age'] <= 180) & (df['结束库龄'] > 180), '180~270收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - 180, 90]), axis=1)
    df.loc[(df['inventory_age'] <= 270) & (df['inventory_age'] > 180), '180~270收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 270 - x['inventory_age']), axis=1)
    df.loc[(df['inventory_age'] <= 270) & (df['结束库龄'] > 270), '270~360收租天数'] = df.apply(
        lambda x: min([x['结束库龄'] - 270, 90]), axis=1)
    df.loc[(df['inventory_age'] <= 360) & (df['inventory_age'] > 270), '270~360收租天数'] = df.apply(
        lambda x: min(x['结束库龄'] - x['inventory_age'], 360 - x['inventory_age']), axis=1)

    df.loc[(df['inventory_age'] <= 360) & (df['结束库龄'] > 360), '360以上收租天数'] = df['结束库龄'] - 360
    df.loc[(df['inventory_age'] > 360), '360以上收租天数'] = df['结束库龄'] - df['inventory_age']

    return df


def calcu_data(df_all, a, conn, ck_client):
    df_new = base_data(df_all)
    # for i in df_new.columns:
    #     import re
    #     if i.__contains__(">") or i.__contains__("<"):
    #         df_new.loc[df_new[i]>0,i]=df_new.apply(lambda x:round(x,2))
    df_new_sku = tuple(set(df_new["sku"]))
    df_new = product_pipei(df_new, df_new_sku, ck_client)

    df_fee = conn.read_sql('select * from cangzu_charge_detail')
    # 2022/1/6取消旺季仓租附加费
    # for i in df_fee.columns:
    #     if i.__contains__('(') and i.__contains__(','):
    #         if datetime.date.today() - datetime.timedelta(days=a) >= datetime.date(2021, 10, 1):
    #             df_fee[i] = df_fee[i] + df_fee['旺季存储附加费']
    # import re
    # df_fee["from_currency_code"]=df_fee.apply(lambda x:re.findall("[A-Z]",str(x)))
    df_fee["from_currency_code"] = df_fee["计费单位"].apply(lambda x: str(x)[:3])
    sql_rate = """select from_currency_code,rate from domestic_warehouse_clear.erp_rate"""
    df_rate = conn.read_sql(sql_rate)
    df_fee = df_fee.merge(df_rate, on=["from_currency_code"], how="left")
    # for i in df_fee.columns:
    #     if i.__contains__("(") and i.__contains__(','):
    #         df_fee[i] = df_fee[i] * df_fee["rate"]

    df_new['国家'] = df_new["计费仓"]
    df_new.loc[(df_new['oversea_type'] != "4PX") & (
        ~df_new['国家'].isin(['美国仓', '英国仓', '德国仓', '澳洲仓'])), '国家'] = '其他仓'
    df_new = df_new.merge(df_fee, left_on=["国家", "cargo_type"], right_on=["国家", "货型代号"], how="left")
    print(df_new['国家'].unique())

    # 删除计费仓和warehouse_name字段
    # df_new.loc[df_new['单SKU体积'].notnull(), '最终重'] = df_new['单SKU体积']
    # df_new.drop(columns=['计费仓', 'warehouse_name', '单SKU体积'], inplace=True)
    df_new.drop(columns=['计费仓', 'warehouse_name'], inplace=True)
    print(df_new.columns)

    # 2022/04/01万邑通单价打八折(2022/02/15第二次修改，万邑通超180天以上库龄，仓租单价打5折，180天以下打8折)
    # if datetime.date.today()<datetime.date(2022,4,1):
    #     for i in df_new.columns:
    #         if re.findall("^\([0-9].*,.*(\]|\))$",str(i)):
    #             df_new.loc[(df_new['oversea_type'] == "WYT") & (df_new["inventory_age"] <180), i] = \
    #                 df_new.loc[(df_new['oversea_type'] == "WYT") & (df_new["inventory_age"] < 180), i] * 0.8
    #             df_new.loc[(df_new['oversea_type']=="WYT")&(df_new["inventory_age"]>=180),i]=\
    #                 df_new.loc[(df_new['oversea_type']=="WYT")&(df_new["inventory_age"]>=180),i]*0.5

    # 单件要求收费体积不得小于0.001
    df_new.loc[(df_new['最终重'] < 0.001), '最终重'] = 0.001

    # 如果总件体积小于最小计费体积(CBM)就按最小体积算
    df_new["0<x<=30预测仓租金额"] = df_new["0<x<=30"] * df_new['(0,30]'] * df_new["最终重"] * df_new["rate"] * df_new[
        '0~30收租天数']

    df_new.loc[(df_new['最终重'] * df_new['0<x<=30'] < df_new['最小计费体积(CBM)'])
    , '0<x<=30预测仓租金额'] = df_new["0<x<=30"] * df_new['(0,30]'] * df_new["最小计费体积(CBM)"] * df_new["rate"] * \
                               df_new[
                                   '0~30收租天数']

    df_new["30<x<=60预测仓租金额"] = df_new["30<x<=60"] * df_new["(30,60]"] * df_new["最终重"] * df_new["rate"] * \
                                     df_new[
                                         '30~60收租天数']
    df_new.loc[(df_new['最终重'] * df_new['30<x<=60'] < df_new['最小计费体积(CBM)'])
    , '30<x<=60预测仓租金额'] = df_new["30<x<=60"] * df_new["(30,60]"] * df_new["最小计费体积(CBM)"] * df_new["rate"] * \
                                df_new[
                                    '30~60收租天数']

    df_new["60<x<=90预测仓租金额"] = df_new["60<x<=90"] * df_new["(60,90]"] * df_new["最终重"] * df_new["rate"] * \
                                     df_new[
                                         '60~90收租天数']
    df_new.loc[(df_new['最终重'] * df_new['60<x<=90'] < df_new['最小计费体积(CBM)']),
    "60<x<=90预测仓租金额"] = df_new["60<x<=90"] * df_new["(60,90]"] * df_new["最小计费体积(CBM)"] * df_new["rate"] * \
                              df_new['60~90收租天数']

    df_new["90<x<=120预测仓租金额"] = df_new["90<x<=120"] * df_new["(90,120]"] * df_new["最终重"] * df_new["rate"] * \
                                      df_new[
                                          '90~120收租天数']
    df_new.loc[(df_new['最终重'] * df_new['90<x<=120'] < df_new['最小计费体积(CBM)']),
    "90<x<=120预测仓租金额"] = df_new["90<x<=120"] * df_new["(90,120]"] * df_new["最小计费体积(CBM)"] * df_new["rate"] * \
                               df_new['90~120收租天数']

    df_new["120<x<=180预测仓租金额"] = df_new["120<x<=180"] * df_new["(120,180]"] * df_new["最终重"] * df_new["rate"] * \
                                       df_new[
                                           '120~180收租天数']
    df_new.loc[(df_new['最终重'] * df_new['120<x<=180'] < df_new['最小计费体积(CBM)']),
    "120<x<=180预测仓租金额"] = df_new["120<x<=180"] * df_new["(120,180]"] * df_new["最小计费体积(CBM)"] * df_new[
        "rate"] * df_new['120~180收租天数']

    df_new["180<x<=270预测仓租金额"] = df_new["180<x<=270"] * df_new["(180,270]"] * df_new["最终重"] * df_new["rate"] * \
                                       df_new[
                                           '180~270收租天数']
    df_new.loc[(df_new['最终重'] * df_new['180<x<=270'] < df_new['最小计费体积(CBM)']),
    "180<x<=270预测仓租金额"] = df_new["180<x<=270"] * df_new["(180,270]"] * df_new["最小计费体积(CBM)"] * df_new[
        "rate"] * df_new['180~270收租天数']

    df_new["270<x<=360预测仓租金额"] = df_new["270<x<=360"] * df_new["(270,360]"] * df_new["最终重"] * df_new["rate"] * \
                                       df_new[
                                           '270~360收租天数']
    df_new.loc[(df_new['最终重'] * df_new['270<x<=360'] < df_new['最小计费体积(CBM)']),
    "270<x<=360预测仓租金额"] = df_new["270<x<=360"] * df_new["(270,360]"] * df_new["最小计费体积(CBM)"] * df_new[
        "rate"] * df_new['270~360收租天数']

    df_new["x>360预测仓租金额"] = df_new["x>360"] * df_new["(360,+∞)"] * df_new["最终重"] * df_new["rate"] * df_new[
        '360以上收租天数']
    df_new.loc[(df_new['最终重'] * df_new['x>360'] < df_new['最小计费体积(CBM)']),
    "x>360预测仓租金额"] = df_new["x>360"] * df_new["(360,+∞)"] * df_new["最小计费体积(CBM)"] * df_new["rate"] * df_new[
        '360以上收租天数']

    # df_new.to_excel(f'warehouse_stock_charges_{datetime.date.today()-datetime.timedelta(days=a)}123.xlsx')

    for i in df_new.columns:
        if i.__contains__('收租天数'):
            del df_new[i]

    print(df_new.info())
    for i in df_new.columns:

        if i.__contains__(">") or i.__contains__("<"):
            df_new[i].fillna(0, inplace=True)
            df_new[i] = df_new[i].astype("float")
            df_new[i] = round(df_new[i], 2)

    # 换列名
    df_new.columns = ['sku', 'cargo_type', 'country', 'warehouse_stock', 'inventory_age', 'charge_total_price',
                      'charge_currency'
        , 'oversea_type', 'warehouse', 'age_section', 'day_sales', 'estimated_sales_days', 'start_age', 'end_age',
                      '0_to_30_quantity', '30_to_60_quantity', '60_to_90_quantity', '90_to_120_quantity',
                      '120_to_180_quantity',
                      '180_to_270_quantity', '270_to_360_quantity', '360_plus_quantity', 'weight', 'warehouse_sub',
                      'cargo_type1'
        , '0_to_30', '30_to_60', '60_to_90', '90_to_120', '120_to_180', '180_to_270', '270_to_360', '360_plus',
                      'extra_charge', 'charge_unit', 'min_volume_CBM', 'from_currency_code', 'rate',
                      "0_to_30_predict_amt_rmb"
        , '30_to_60_predict_amt_rmb', '60_to_90_predict_amt_rmb', '90_to_120_predict_amt_rmb',
                      '120_to_180_predict_amt_rmb'
        , '180_to_270_predict_amt_rmb', '270_to_360_predict_amt_rmb', '360_plus_predict_amt_rmb']

    df_new['DATE'] = datetime.date.today() - datetime.timedelta(days=a)
    df_new.drop_duplicates(inplace=True)
    print(df_new.info())

    # #写到华为云
    # f_path = os.getcwd()
    # file_name = os.path.join(f_path, '测试.xlsx')
    # df_new.to_excel(file_name, index=False)
    # obs_putFile(item='over_sea', file_name=file_name, open_dir=None, isall=False)
    # os.remove(file_name)
    conn.to_sql(df_new, 'warehouse_stock_charges', if_exists='append')
    return df_new


def delete_data(a, conn):
    sql = f"delete from  warehouse_stock_charges where DATE='{datetime.date.today() - datetime.timedelta(days=a)}' "
    conn.execute(sql)


def product_pipei(data, sku, ck_client):
    sql = f"""select sku,toFloat64(pur_length_pack) *toFloat64(pur_height_pack) *toFloat64(pur_width_pack) 
            as "体积重",case when toFloat64(weight_out_storage)=0 
            then toFloat64(product_weight_gross) else toFloat64(weight_out_storage) end as "实际重"
            from yibai_prod_base_sync.yibai_prod_sku 
            order by create_time desc 
            """
    df1 = ck_client.ck_select_to_df(ck_sql=sql)
    df1["体积重"] = df1["体积重"] / 1000000
    df1["实际重"] = df1["实际重"] / 1000
    df1["最终重"] = df1["体积重"]
    # df1.loc[df1["体积重"]>=df1["实际重"],"最终重"]=df1["体积重"]
    # df1.loc[df1["体积重"]<df1["实际重"],"最终重"]=df1["实际重"]
    df1.drop(axis=1, columns=["体积重", "实际重"], inplace=True)
    df1.drop_duplicates(subset=["sku"], inplace=True)
    print(df1.head())
    data = data.merge(df1, on="sku", how="left")
    return data


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
    sql = f"""insert into timed_task_record values {now_time, task_type, status, text}"""
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


def main():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yb_datacenter')
        ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
        for i in range(7):
            if i == 0:
                print(datetime.date.today() - datetime.timedelta(days=i))
                delete_data(i, conn)
                print('删除完成')
                df_all = get_data(i, conn, ck_client)
                print(len(df_all))
                calcu_data(df_all, i, conn, ck_client)
        send_msg("动销组定时任务推送", "warehouse_stock_charges", "海外仓预计收仓租近7天明细数据已上传至over_sea数据库,请查收！"
                 , mobiles=['+86-13419546972'], is_all=False)
        conn.close()
    except Exception as e:
        send_msg('动销组定时任务推送', "warehouse_stock_charges",
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓预计收仓租明细数据出现问题,请及时排查,失败原因详情请查看airflow",
                 status='失败'
                 , mobiles=['+86-13419546972'], is_all=False)

        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    main()
