"""

国内仓清仓sku进行涨价测试。

触发销毁产品提价测试注意事项：
1.每个品线能否选100个sku，刊登数量不要太少，库存大于10个或者5个以上、产品成本大于30
2.取达到销毁价30天以上的sku
3.测试前一天进行锁价给到星星
4.纯国内仓清仓链接
5.yibai_domestic.yibai_amazon_listing_profit  用于计算上传价格【可以把100个sku分成两份，50个用成本的3折定价，50个用成本的6折定价】
分析方式：对此品线成本段维度前后销量对比

"""

import pandas as pd
import os
import numpy as np
import datetime, time
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck

import warnings
warnings.filterwarnings('ignore')

##
def get_clear_sku():
    """
    确定测试sku
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = """
        SELECT sku, linelist1, `30days_sales`, Total_inventory_quantity, cost
        FROM support_document.domestic_warehouse_clear_sku_20250418
        WHERE cost > 30 and Total_inventory_quantity > 5
        
    """
    df_sku = conn_ck.ck_select_to_df(sql)
    print(df_sku.info())
    date = datetime.date.today() - pd.Timedelta(days=14)

    sql = f"""
    select 
        sku, start_time `抵达销毁价开始时间`
        from domestic_warehouse_clear.domestic_warehouse_clear_destroy_time
        where start_time <= \'{date}\'
    """
    df = conn_ck.ck_select_to_df(sql)
    df['已达销毁价天数'] = (pd.to_datetime(datetime.date.today()) - pd.to_datetime(
        df['抵达销毁价开始时间'])).dt.days + 1

    df = pd.merge(df_sku, df, how='left', on=['sku'])
    df['已达销毁价天数'] = df['已达销毁价天数'].fillna(0).astype(int)

    df = df[df['已达销毁价天数']>30]
    print(df.info())

    df = get_listing_num(df)

    df = df[df['listing_num']> 5]
    # df = get_listing_order(df)

    grouped = df.groupby('linelist1')
    sampled_df = grouped.apply(lambda x: x.sample(min(len(x), 100), random_state=1))

    # 重置索引，如果需要的话
    sampled_df = sampled_df.reset_index(drop=True)

    # df.to_excel('F://Desktop//df_dom_test.xlsx', index=0)
    # sampled_df.to_excel('F://Desktop//sampled_df.xlsx', index=0)

    return sampled_df

def get_listing_num(df):
    """ 获取sku链接数量  """
    sql = """
        SELECT sku, count(1) as listing_num
        FROM yibai_domestic.yibai_amazon_listing_profit
        GROUP BY sku 
    """
    conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
    df_listing_num = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_listing_num, how='left', on=['sku'])

    return df

# 获取测试链接
def get_test_listing():
    """ 根据确定的sku, 获取测试链接 """
    df = get_clear_sku()
    df['sku'] = df['sku'].astype(str)
    print(f'sku数量共{len(df)}条。')
    sku_list = tuple(df['sku'].unique())
    sql = f"""
    with temp_listing as (
        SELECT distinct asin
        FROM yibai_domestic.yibai_amazon_listing_profit
        WHERE sku in {sku_list}  and fulfillment_channel = 'AMA'
    )
    SELECT *
    FROM yibai_domestic.yibai_amazon_listing_profit
    WHERE sku in {sku_list} and fulfillment_channel = 'DEF'
    and asin not in (SELECT distinct asin FROM temp_listing)
    and `销售状态` != '正常' and `运费` is not null
    """
    conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)

    # 获取测试链接前30天的销量
    df_listing = get_listing_order(df_listing)

    df_listing = pd.merge(df_listing, df[['sku','linelist1']], how='left', on=['sku'])

    # 按类别均分样本
    group_sizes = df_listing.groupby(['sku','linelist1']).size()
    group_sizes = group_sizes.reset_index(name='group_size')

    df_listing = df_listing.merge(group_sizes, on=['sku','linelist1'])

    # 创建一个新列，表示每个样本在其组内的索引
    df_listing['group_index'] = df_listing.groupby(['sku','linelist1']).cumcount()

    # 计算每个样本应该属于的子集（例如，均分到两个子集中）
    import math
    df_listing['subset'] = df_listing.apply(lambda row: math.ceil((row['group_index'] + 1) / (row['group_size'] / 2)), axis=1)

    df_listing.to_excel('F://Desktop//df_listing_test.xlsx', index=0)


def get_listing_order(df, is_before=1):
    """ 获取测试链接销量 """
    if is_before == 1:
        date_today = datetime.datetime.strptime('2025-04-24', '%Y-%m-%d')
        date_start = date_today - datetime.timedelta(days=30)
    else:
        date_start = datetime.datetime.strptime('2025-04-24', '%Y-%m-%d')
        date_today = date_start + datetime.timedelta(days=30)
    sql = f"""
        SELECT account_id, seller_sku, count(1) order_num, sum(`销售额`) `销售额`, sum(`毛利润`) `毛利润`
        FROM domestic_warehouse_clear.monitor_dom_order
        WHERE platform_code = 'AMAZON' and created_time > '{date_start}' and created_time < '{date_today}'
        GROUP BY account_id, seller_sku
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_order = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_order, how='left', on=['account_id','seller_sku'])

    return df

def after_testing():
    """ 测试结果检验 """
    df = pd.read_excel('F://Desktop//国内仓滞销品sku涨价测试0421.xlsx')
    print(df.info())
    col = ['account_id', 'seller_sku','site','your_price','sku','分组','成本','产品大类','测试定价']
    df = df[col]

    sku_list = tuple(df['sku'].unique())
    sql = f"""
    with temp_listing as (
        SELECT distinct asin
        FROM yibai_domestic.yibai_amazon_listing_profit
        WHERE sku in {sku_list}  and fulfillment_channel = 'AMA'
    )
    SELECT *
    FROM yibai_domestic.yibai_amazon_listing_profit
    WHERE sku in {sku_list} and fulfillment_channel = 'DEF'
    and asin not in (SELECT distinct asin FROM temp_listing)
    and `销售状态` != '正常' 
    -- and `运费` is not null
    """
    conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_listing[['account_id','seller_sku','status','your_price']], how='inner', on=['account_id','seller_sku'])

    # # 取测试前后销量数据
    # df = get_listing_order(df, is_before=1)
    # df = get_listing_order(df, is_before=0)

    df.to_excel('F://Desktop//df_test_result_2.xlsx', index=0)

def dwm_sku_temp_info():
    # sql = """
    # SELECT date_id, count(1) as sku_num
    # FROM (
    #     SELECT sku, title, type, warehouse, best_warehouse_name, available_stock, new_price, available_stock_money, overage_level,
    #     age_90_plus, `30days_sales`, day_sales, date_id
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id > '2025-05-01' and available_stock > 0
    # ) a
    # GROUP BY date_id
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df1 = conn.read_sql(sql)
    # print(df1.info())
    sql =  """
    SELECT date_id, `30days_sales`, count(1) as sku_num, sum(available_stock) as available_stock, 
    sum(available_stock_money) available_stock_money
    FROM (
        SELECT sku, title, type, warehouse, best_warehouse_name, available_stock, new_price,available_stock_money, overage_level,
        age_90_plus, `30days_sales`, day_sales, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id > '2025-05-01' and available_stock > 0 and `30days_sales` <= 3
    ) a
    GROUP BY date_id, `30days_sales`
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df2 = conn.read_sql(sql)
    # print(df.info())

    # df1.to_excel('F://Desktop//df_dwm_all.xlsx', index=0)
    df2.to_excel('F://Desktop//df_dwm.xlsx', index=0)

if __name__ == '__main__':
    # get_clear_sku()
    # get_test_listing()

    # after_testing()
    dwm_sku_temp_info()

