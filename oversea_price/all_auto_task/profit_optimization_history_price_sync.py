import os
import time
import traceback
import warnings
import pyarrow.parquet as pq

import requests
from tqdm import tqdm

from pulic_func.base_api.base_function import mysql_escape_list, roundup, mysql_escape
from pulic_func.base_api.mysql_connect import pd_to_ck
import datetime
import pandas as pd

from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut, tou_cheng_ky, tou_cheng_hy
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, astype_int, \
    str_int, mobanxiuzheng, jisuanfenlei, chaxun_chengben, tar_profit, yunfei_biangeng, sku_and_num_split, \
    DMS_getCalculation_api_fbm
from pulic_func.price_adjust_web_service.make_price import amazon_fbm_para
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee

warnings.filterwarnings('ignore')


def history_price_check():
    print('1')
    if os.path.exists('/data_obs'):
        print('路径正确')
    else:
        print('路径不正确')
    date_start = pd.to_datetime('2023-06-16')
    date_end = pd.to_datetime(datetime.date.today())
    while date_start <= date_end:
        path = rf'/data_obs/price/{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}/support_document.amazon_listing_profit/95'
        if os.path.exists(path):
            print(f'{path}有')
        else:
            print(f'{path}没有')
        date_start = date_start + pd.Timedelta(days=1)


def get_history_price(date_start, account_id):
    df = pq.read_table(f'/data_obs/price/{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}'
                       f'/support_document.amazon_listing_profit/{account_id}/'
                       f'support_document.amazon_listing_profit.parquet').to_pandas()
    # print(df.columns)
    df['create_date'] = date_start
    df['seller_sku'] = df['seller_sku'].str.decode('utf-8')
    df['fulfillment_channel'] = df['fulfillment_channel'].str.decode('utf-8')
    df['sku'] = df['sku'].str.decode('utf-8')
    df['asin'] = df['asin'].str.decode('utf-8')
    df['价格状态'] = df['价格状态'].str.decode('utf-8')
    return df.drop_duplicates()


def history_price_sync():
    """
    每天同步指定账户链接的历史价格信息（从共享盘到ck数据库）
    """
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_end = pd.to_datetime(datetime.date.today())
    account_id = 95

    # 美国账户
    sql_account = """
    select id as account_id from yibai_system_kd_sync.yibai_amazon_account
    where account_name like '%美国'
    """
    df_account = conn_mx.ck_select_to_df(sql_account)

    # 获取更新起始时间
    table_name = 'profit_optimization_history_price'
    sql = f"""
        select max(create_date) create_date from support_document.{table_name}
        where account_id = {account_id}
    """
    date_start = conn_mx.ck_select_to_df(sql)
    if date_start['create_date'].values[0] != '':
        date_start = pd.to_datetime(date_start['create_date'].values[0]) + pd.Timedelta(days=1)
    else:
        date_start = pd.to_datetime('2023-06-19')

    # 表准备
    # sql = f"""
    #     DROP TABLE IF EXISTS support_document.{table_name}
    # """
    # conn_mx.ck_execute_sql(sql)
    # print('历史价格表删除成功！')
    sql = f"""  
        -- support_document.{table_name} definition
        
        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `id` Int64,
            `account_id` Int32,
            `seller_sku` String,
            `status` Int32,
            `your_price` Decimal(15, 2),
            `fulfillment_channel` String,
            `sku` String,
            `deliver_mode` Int32,
            `asin` String,
            `税率` Decimal(8, 4),
            `your_price_point` Decimal(8, 2),
            `promotion_percent` Decimal(8, 2),
            `promotion_amount` Int32,
            `percentage_off` Decimal(8, 2),
            `money_off` Int32,
            `Coupon_handling_fee` Decimal(8, 2),
            `成本` Nullable(Decimal(8, 2)),
            `rate` Decimal(10, 6),
            `fba_fees` Nullable(Decimal(8, 2)),
            `头程费_人民币` Nullable(Decimal(8, 2)),
            `运费` Nullable(Decimal(8, 2)),
            `毛净利率差值` Decimal(8, 4),
            `佣金率` Decimal(8, 4),
            `当前售价利润率-毛利润率` Nullable(Decimal(8, 4)),
            `当前售价利润率-净毛利润率` Nullable(Decimal(8, 4)),
            `价格状态` String,
            `create_date` String,
            `market_price` Decimal(15, 2),
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        PARTITION BY create_date
        ORDER BY (account_id,
         seller_sku)
        SETTINGS index_granularity = 8192;
        """
    conn_mx.ck_create_table(sql)
    print('历史价格表创建成功！')

    # 历史价格信息sync
    while date_start < date_end:
        # print(f'本轮此日期 {date_start}')
        try:
            df1 = get_history_price(date_start, account_id)
            df1 = df1[df1['fulfillment_channel'] == 'AMA']
            df1['is_filter'] = 0
            df2 = pd.DataFrame()
            for index, row in df_account.iterrows():
                df3 = pd.DataFrame()
                try:
                    df3 = get_history_price(date_start, row['account_id'])
                    # print(f"account_id {row['account_id']}历史数据获取成功！")
                except:
                    # print(f"account_id {row['account_id']}历史数据获取失败！")
                    pass
                if not df3.empty:
                    df3 = df3.merge(df1[['account_id', 'seller_sku', 'is_filter']], on=['account_id', 'seller_sku'],
                                    how='left')
                    df3 = df3[df3['is_filter'] != 0][['sku', 'your_price']].rename(columns={'your_price': 'market_price'})
                    df2 = df3 if df2.empty else pd.concat([df2, df3], ignore_index=True)
                # print(f'process: {index + 1}/{len(df_account)}')
            df2 = df2.groupby('sku').mean().reset_index()
            df = df1.drop('is_filter', axis=1).merge(df2, on='sku', how='left')
            df['market_price'] = df['market_price'].fillna(0)
            df = df.drop_duplicates()

            # 写入ck
            conn_mx.ck_insert(df, table_name, if_exist='append')
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步完成！')
        except:
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步失败！')
        date_start = date_start + pd.Timedelta(days=1)
    print(f'每日历史价格信息全部完成! account_id: {account_id}')


def history_price_sync2():
    """
    每天同步指定账户链接的历史价格信息（从共享盘到ck数据库）
    """
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_end = pd.to_datetime(datetime.date.today())
    account_id = 322

    # 美国账户
    sql_account = """
    select id as account_id from yibai_system_kd_sync.yibai_amazon_account
    where account_name like '%美国'
    """
    df_account = conn_mx.ck_select_to_df(sql_account)

    # 获取更新起始时间
    table_name = 'profit_optimization_history_price'
    sql = f"""
        select max(create_date) create_date from support_document.{table_name}
        where account_id = {account_id}
    """
    date_start = conn_mx.ck_select_to_df(sql)
    if date_start['create_date'].values[0] != '':
        date_start = pd.to_datetime(date_start['create_date'].values[0]) + pd.Timedelta(days=1)
    else:
        date_start = pd.to_datetime('2023-06-19')

    # 表准备
    # sql = f"""
    #     DROP TABLE IF EXISTS support_document.{table_name}
    # """
    # conn_mx.ck_execute_sql(sql)
    # print('历史价格表删除成功！')
    sql = f"""  
        -- support_document.{table_name} definition

        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `id` Int64,
            `account_id` Int32,
            `seller_sku` String,
            `status` Int32,
            `your_price` Decimal(15, 2),
            `fulfillment_channel` String,
            `sku` String,
            `deliver_mode` Int32,
            `asin` String,
            `税率` Decimal(8, 4),
            `your_price_point` Decimal(8, 2),
            `promotion_percent` Decimal(8, 2),
            `promotion_amount` Int32,
            `percentage_off` Decimal(8, 2),
            `money_off` Int32,
            `Coupon_handling_fee` Decimal(8, 2),
            `成本` Nullable(Decimal(8, 2)),
            `rate` Decimal(10, 6),
            `fba_fees` Nullable(Decimal(8, 2)),
            `头程费_人民币` Nullable(Decimal(8, 2)),
            `运费` Nullable(Decimal(8, 2)),
            `毛净利率差值` Decimal(8, 4),
            `佣金率` Decimal(8, 4),
            `当前售价利润率-毛利润率` Nullable(Decimal(8, 4)),
            `当前售价利润率-净毛利润率` Nullable(Decimal(8, 4)),
            `价格状态` String,
            `create_date` String,
            `market_price` Decimal(15, 2),
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        PARTITION BY create_date
        ORDER BY (account_id,
         seller_sku)
        SETTINGS index_granularity = 8192;
        """
    conn_mx.ck_create_table(sql)
    print('历史价格表创建成功！')

    # 历史价格信息sync
    while date_start < date_end:
        # print(f'本轮此日期 {date_start}')
        try:
            df1 = get_history_price(date_start, account_id)
            df1 = df1[df1['fulfillment_channel'] == 'AMA']
            df1['is_filter'] = 0
            df2 = pd.DataFrame()
            for index, row in df_account.iterrows():
                df3 = pd.DataFrame()
                try:
                    df3 = get_history_price(date_start, row['account_id'])
                    # print(f"account_id {row['account_id']}历史数据获取成功！")
                except:
                    # print(f"account_id {row['account_id']}历史数据获取失败！")
                    pass
                if not df3.empty:
                    df3 = df3.merge(df1[['account_id', 'seller_sku', 'is_filter']], on=['account_id', 'seller_sku'],
                                    how='left')
                    df3 = df3[df3['is_filter'] != 0][['sku', 'your_price']].rename(
                        columns={'your_price': 'market_price'})
                    df2 = df3 if df2.empty else pd.concat([df2, df3], ignore_index=True)
                # print(f'process: {index + 1}/{len(df_account)}')
            df2 = df2.groupby('sku').mean().reset_index()
            df = df1.drop('is_filter', axis=1).merge(df2, on='sku', how='left')
            df['market_price'] = df['market_price'].fillna(0)
            df = df.drop_duplicates()

            # 写入ck
            conn_mx.ck_insert(df, table_name, if_exist='append')
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步完成！')
        except:
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步失败！')
        date_start = date_start + pd.Timedelta(days=1)
    print(f'每日历史价格信息全部完成! account_id: {account_id}')


def history_price_sync3():
    """
    每天同步指定账户链接的历史价格信息（从共享盘到ck数据库）
    """
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    date_end = pd.to_datetime(datetime.date.today())
    account_id = 677

    # 美国账户
    sql_account = """
    select id as account_id from yibai_system_kd_sync.yibai_amazon_account
    where account_name like '%美国'
    """
    df_account = conn_mx.ck_select_to_df(sql_account)

    # 获取更新起始时间
    table_name = 'profit_optimization_history_price'
    sql = f"""
        select max(create_date) create_date from support_document.{table_name}
        where account_id = {account_id}
    """
    date_start = conn_mx.ck_select_to_df(sql)
    if date_start['create_date'].values[0] != '':
        date_start = pd.to_datetime(date_start['create_date'].values[0]) + pd.Timedelta(days=1)
    else:
        date_start = pd.to_datetime('2023-06-19')

    # 表准备
    # sql = f"""
    #     DROP TABLE IF EXISTS support_document.{table_name}
    # """
    # conn_mx.ck_execute_sql(sql)
    # print('历史价格表删除成功！')
    sql = f"""  
        -- support_document.{table_name} definition

        CREATE TABLE IF NOT EXISTS support_document.{table_name}
        (
            `id` Int64,
            `account_id` Int32,
            `seller_sku` String,
            `status` Int32,
            `your_price` Decimal(15, 2),
            `fulfillment_channel` String,
            `sku` String,
            `deliver_mode` Int32,
            `asin` String,
            `税率` Decimal(8, 4),
            `your_price_point` Decimal(8, 2),
            `promotion_percent` Decimal(8, 2),
            `promotion_amount` Int32,
            `percentage_off` Decimal(8, 2),
            `money_off` Int32,
            `Coupon_handling_fee` Decimal(8, 2),
            `成本` Nullable(Decimal(8, 2)),
            `rate` Decimal(10, 6),
            `fba_fees` Nullable(Decimal(8, 2)),
            `头程费_人民币` Nullable(Decimal(8, 2)),
            `运费` Nullable(Decimal(8, 2)),
            `毛净利率差值` Decimal(8, 4),
            `佣金率` Decimal(8, 4),
            `当前售价利润率-毛利润率` Nullable(Decimal(8, 4)),
            `当前售价利润率-净毛利润率` Nullable(Decimal(8, 4)),
            `价格状态` String,
            `create_date` String,
            `market_price` Decimal(15, 2),
            `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        PARTITION BY create_date
        ORDER BY (account_id,
         seller_sku)
        SETTINGS index_granularity = 8192;
        """
    conn_mx.ck_create_table(sql)
    print('历史价格表创建成功！')

    # 历史价格信息sync
    while date_start < date_end:
        # print(f'本轮此日期 {date_start}')
        try:
            df1 = get_history_price(date_start, account_id)
            df1 = df1[df1['fulfillment_channel'] == 'AMA']
            df1['is_filter'] = 0
            df2 = pd.DataFrame()
            for index, row in df_account.iterrows():
                df3 = pd.DataFrame()
                try:
                    df3 = get_history_price(date_start, row['account_id'])
                    # print(f"account_id {row['account_id']}历史数据获取成功！")
                except:
                    # print(f"account_id {row['account_id']}历史数据获取失败！")
                    pass
                if not df3.empty:
                    df3 = df3.merge(df1[['account_id', 'seller_sku', 'is_filter']], on=['account_id', 'seller_sku'],
                                    how='left')
                    df3 = df3[df3['is_filter'] != 0][['sku', 'your_price']].rename(
                        columns={'your_price': 'market_price'})
                    df2 = df3 if df2.empty else pd.concat([df2, df3], ignore_index=True)
                # print(f'process: {index + 1}/{len(df_account)}')
            df2 = df2.groupby('sku').mean().reset_index()
            df = df1.drop('is_filter', axis=1).merge(df2, on='sku', how='left')
            df['market_price'] = df['market_price'].fillna(0)
            df = df.drop_duplicates()

            # 写入ck
            conn_mx.ck_insert(df, table_name, if_exist='append')
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步完成！')
        except:
            print(f'{date_start.year:04d}{date_start.month:02d}{date_start.day:02d}历史数据同步失败！')
        date_start = date_start + pd.Timedelta(days=1)
    print(f'每日历史价格信息全部完成! account_id: {account_id}')


if __name__ == "__main__":
    history_price_sync3()
