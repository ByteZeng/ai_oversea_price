import logging
import math
import os
import re
# import numpy as np

# import pandas as pd
import warnings
# import pymysql
import datetime
# from clickhouse_driver import Client
# from dateutil.relativedelta import relativedelta

from all_auto_task.scripts_ck_client import CkClient
from utils.utils import str_to_datetime, read_sql_ck, get_ck_client, expand_timespan_by_row, get_date, \
    df_to_list_tuple, save_df, get_mysql_con, \
    get_filter_by_sql_mysql_in_large_tuple, df_to_tuple_tuple, is_df_exist, read_df, make_path, get_or_read_df, \
    get_filter_by_sql_ck_in_large_list, get_path, filter_by_df_filter
import utils
# from sqlalchemy import create_engine
import time
# import requests
# from requests.auth import HTTPBasicAuth
import json
# author: marmot
import json
import time
import traceback
# from clickhouse_driver import Client
# import pandas as pd
import datetime
import os
# import redis
# import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED, as_completed
import pandas as pd
import os
# from sqlalchemy.types import VARCHAR, Float, Integer, DECIMAL, BigInteger
# from sqlalchemy import create_engine
# import pymysql
import re
import traceback

warnings.filterwarnings('ignore')


def filter_by_platform(df):
    if len(df) > 1:
        df_output = df[df['platform'] == 'AMAZON']
        if df_output.empty:
            df_output = df[df['platform'] == 'EB']
            if df_output.empty:
                df_output = df[df['platform'] == 'WALMART']
                if df_output.empty:
                    df_output = df[df['platform'] == 'CDISOUNT']
                    if df_output.empty:
                        df_output = df[df['platform'] == 'WISH']
                        if df_output.empty:
                            df_output = df[df['platform'] == 'ALI']
        return df_output
    else:
        return df


def warehouse_country_fendang(data, col, c):
    """

    :param data: 数据集
    :param col: 传进去进行分档的列
    :param c: 分档后新列的名字
    :return:


    """

    data.loc[(data[col] == '澳洲仓'), c] = "AU"
    data.loc[(data[col] == '波兰仓'), c] = "PL"
    data.loc[(data[col] == '德国仓'), c] = "DE"
    data.loc[(data[col] == '法国仓'), c] = "FR"
    data.loc[(data[col] == '美国仓'), c] = "US"
    data.loc[(data[col] == '日本仓'), c] = "JP"
    data.loc[(data[col] == '英国仓'), c] = "UK"

    return data


def day_sales_fendang(data, col, c):
    """
    :param data: 数据集
    :param col: 传进去进行分档的列
    :param c: 分档后新列的名字
    :return:
    """
    data.loc[(data[col] >= 0) & (data[col] < 1), c] = "A.日均销量，S∈[0,1)"
    data.loc[(data[col] >= 1) & (data[col] < 2), c] = "B.日均销量，S∈[1,2)"
    data.loc[(data[col] >= 2) & (data[col] < 3), c] = "C.日均销量，S∈[2,3)"
    data.loc[(data[col] >= 3) & (data[col] < 4), c] = "D.日均销量，S∈[3,4)"
    data.loc[(data[col] >= 4) & (data[col] < 5), c] = "E.日均销量，S∈[4,5)"
    data.loc[(data[col] >= 5) & (data[col] < 6), c] = "F.日均销量，S∈[5,6)"
    data.loc[(data[col] >= 6) & (data[col] < 7), c] = "G.日均销量，S∈[6,7)"
    data.loc[(data[col] >= 7) & (data[col] < 8), c] = "H.日均销量，S∈[7,8)"
    data.loc[(data[col] >= 8) & (data[col] < 9), c] = "I.日均销量，S∈[8,9)"
    data.loc[(data[col] >= 9) & (data[col] < 10), c] = "J.日均销量，S∈[9,10)"
    data.loc[(data[col] >= 10), c] = "K.日均销量，S∈[10,∞)"
    return data


def estimated_sale_days_fendang(data, col, c):
    """
    :param data: 数据集
    :param col: 传进去进行分档的列
    :param c: 分档后新列的名字
    :return:
    """
    data.loc[(data[col] >= 0) & (data[col] < 30), c] = "A.可售天数，D∈[0,30)"
    data.loc[(data[col] >= 30) & (data[col] < 60), c] = "B.可售天数，D∈[30,60)"
    data.loc[(data[col] >= 60) & (data[col] < 90), c] = "C.可售天数，D∈[60,90)"
    data.loc[(data[col] >= 90) & (data[col] < 120), c] = "D.可售天数，D∈[90,120)"
    data.loc[(data[col] >= 120) & (data[col] < 150), c] = "E.可售天数，D∈[120,150)"
    data.loc[(data[col] >= 150) & (data[col] < 180), c] = "F.可售天数，D∈[150,180)"
    data.loc[(data[col] >= 180) & (data[col] < 210), c] = "G.可售天数，D∈[180,210)"
    data.loc[(data[col] >= 210) & (data[col] < 240), c] = "H.可售天数，D∈[210,240)"
    data.loc[(data[col] >= 240) & (data[col] < 270), c] = "I.可售天数，D∈[240,270)"
    data.loc[(data[col] >= 270) & (data[col] < 300), c] = "J.可售天数，D∈[270,300)"
    data.loc[(data[col] >= 300) & (data[col] < 330), c] = "K.可售天数，D∈[300,330)"
    data.loc[(data[col] >= 330) & (data[col] < 360), c] = "L.可售天数，D∈[330,360)"
    data.loc[(data[col] >= 360), c] = "M.可售天数，D∈[360,∞)"
    return data


def kuisun_chengben_ratio_fendang(data, col, c):
    """
    :param data: 数据集
    :param col: 传进去进行分档的列
    :param c: 分档后新列的名字
    :return:
    """
    data.loc[(data[col] >= -0.2), c] = "A.毛利润和成本的比值，P∈[-0.2,∞)"
    data.loc[(data[col] >= -0.4) & (data[col] < -0.2), c] = "B.毛利润和成本的比值，P∈[-0.4,-0.2)"
    data.loc[(data[col] >= -0.6) & (data[col] < -0.4), c] = "C.毛利润和成本的比值，P∈[-0.6,-0.4)"
    data.loc[(data[col] >= -0.8) & (data[col] < -0.6), c] = "D.毛利润和成本的比值，P∈[-0.8,-0.6)"
    data.loc[(data[col] >= -1) & (data[col] < -0.8), c] = "E.毛利润和成本的比值，P∈[-1,-0.8)"
    data.loc[(data[col] >= -1.2) & (data[col] < -1), c] = "F.毛利润和成本的比值，P∈[-1.2,-1)"
    data.loc[(data[col] >= -1.5) & (data[col] < -1.2), c] = "G.毛利润和成本的比值，P∈[-1.5,-1.2)"
    data.loc[(data[col] >= -2) & (data[col] < -1.5), c] = "H.毛利润和成本的比值，P∈[-2,-1.5)"
    data.loc[(data[col] < -2), c] = "K.毛利润和成本的比值，P∈(-∞,-2)"
    return data


def filter_by_country(df):
    if len(df) > 1:
        if df[df['warehouse_country'] == df['country']].empty:
            return df
        else:
            return df[df['warehouse_country'] == df['country']]
    else:
        return df


def main():
    """
    统计近三日的
    只要订单毛利亏损达到产品成本的80%就报废
    """
    utils.utils.program_name = '海外仓销毁_毛利润和成本的比值'
    make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()

    date_start = date_today - datetime.timedelta(days=3)
    date_end = datetime.date.today() - datetime.timedelta(days=1)

    ck_client_212 = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                             db_name='over_sea')
    ck_client_78 = get_ck_client()
    client = get_ck_client(url='121.37.30.78', user='zhangyilan', password='zhangyilan2109221544')
    con = get_mysql_con(host='121.37.248.212', user='wangj', password='Wangj#01', database='over_sea')

    # 全量数据sku
    if is_df_exist('df'):
        df = read_df('df')
    else:
        sql = """
        SELECT * FROM oversea_adjust_platform_dtl_temp
        """
        df = pd.read_sql(sql, con)
        save_df(df, 'df')

    df.rename(columns={'SKU': 'sku'}, inplace=True)
    df['sku'] = df['sku'].astype(str)
    df = warehouse_country_fendang(df, 'warehouse', 'warehouse_country')
    df = df.groupby(['sku', 'warehouse_id']).apply(lambda x: filter_by_country(x)).reset_index(drop=True)
    df = df.groupby(['sku', 'warehouse_id']).apply(lambda x: filter_by_platform(x)).reset_index(drop=True)

    # 近三日订单全量数据
    if is_df_exist('df_order'):
        df_order = read_df('df_order')
    else:
        sql_order = f"""
            select
            sku,
            platform_code as platform,
            warehouse_name as best_warehouse,
        	round(sum(true_profit_new1)/sum(release_money), 1) `毛利润和成本的比值`,
        	'有' as `是否有销库`
        FROM
        	over_sea.dashbord_new1
        WHERE 
        	paytime >= \'{date_start}\'
        	and paytime <= \'{date_end}\'
        	and sales_status IN ('总计')
        group by sku, platform, best_warehouse
        """
        df_order = ck_client_212.ck_select_to_df(sql_order)
        save_df(df_order, 'df_order')

    df = df.merge(df_order, on=['sku', 'best_warehouse', 'platform'], how='left')
    df.drop('platform', axis=1, inplace=True)
    df['是否有销库'] = df['是否有销库'].fillna('否')

    df['毛利润和成本的比值'] = df.apply(
        lambda x: x['profit_rate'] * x['price_rmb'] / x['new_price'] if pd.isna(x['毛利润和成本的比值'])
        else x['毛利润和成本的比值'], axis=1)

    df['在库pcs数量'] = df['available_stock']
    df['在库金额'] = df['available_stock_money']

    # 已申请销毁
    df_1 = read_df('已申请销毁sku')
    df_1.drop_duplicates(inplace=True)
    save_df(df_1, '已申请销毁sku')
    df_1['是否已提交销毁'] = '是'
    df_1.rename(columns={'仓库': 'best_warehouse'}, inplace=True)

    df = df.merge(df_1, on=['sku', 'best_warehouse'], how='left')
    df['是否已提交销毁'] = df['是否已提交销毁'].fillna('否')

    # 可售天数
    df['可售天数>4.30'] = (
            df['estimated_sales_days'] > (pd.to_datetime('2023-04-30') - pd.to_datetime(date_today)).days).map(
        lambda x: '是' if x == True else '否')

    # 分段
    df = day_sales_fendang(df, 'day_sales', '日销分段')
    df = estimated_sale_days_fendang(df, 'estimated_sales_days', '可售天数分段')
    df = kuisun_chengben_ratio_fendang(df, '毛利润和成本的比值', '毛利润和成本的比值分段')

    # 是否库龄缺失筛选
    df['是否库龄缺失'] = (
            (df['在库pcs数量'] != 0) & (df['inv_age_0_to_40_days'] == 0) & (df['inv_age_40_to_70_days'] == 0) & (
            df['inv_age_70_plus_days'] == 0)).apply(lambda x: '是' if x == True else '否')

    # 是否存在超180天库存筛选
    df['超180天库存'] = df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days']

    save_df(df, '海外仓销毁_毛利润和成本的比值', file_type='xlsx')

    print('done!')


if __name__ == '__main__':
    main()
