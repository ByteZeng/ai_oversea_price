##
import pandas as pd
import numpy as np
import warnings
import datetime, time
from clickhouse_driver import Client
from utils.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate, get_sku_sales_new, cut_bins, \
    is_new_sku, write_to_sql, get_mx_stock_age
from all_auto_task.oversea_listing_detail_2023 import get_amazon_listing_data, get_price_data, get_platform_fee
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang
import utils
warnings.filterwarnings('ignore')
##
def get_order():
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '2026-03-01' and paytime < '2026-04-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and platform_code in ('AMAZON', 'EB','WALMART','CDISCOUNT','WISH','ALLEGRO')
            and warehouse_name not like '%精品%'
            and complete_status = '已完结'
            -- and sales_status = '正常'
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    #
    df_order_info['负利润订单'] = np.where(df_order_info['real_profit']<0, 1, 0)
    # # 聚合
    # df_group = df_order_info.groupby('sales_status').agg({'total_price':'sum','true_profit_new1':'sum',
    #                                                       'real_profit':'sum','release_money':'sum'})
    # df_group.to_excel('F://Desktop//df_groupQ3.xlsx', index=0)
    df_order_info.to_excel('F://Desktop//df_order_info_3.xlsx', index=0)

get_order()
##
def get_order_group():
    sql = f"""
    SELECT warehouse, sales_status, platform_code, formatDateTime(toDateTime(paytime), '%Y-%m') AS date_month, 
    sum(total_price), sum(release_money), sum(true_profit_new1), sum(real_profit)
    FROM (
        SELECT DISTINCT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, 
            case 
                when sales_status in ('正利润加快动销','负利润加快动销') then '清仓' 
                when sales_status in ('涨价缩销','') then '正常' else sales_status 
            end as sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '2026-01-01' and paytime < '2026-04-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and warehouse_name not like '%精品%'
            -- and platform_code in ('AMAZON', 'EB','WALMART','CDISCOUNT','WISH')
            -- and sales_status = '正常'
    ) a
    GROUP BY warehouse, sales_status, platform_code, date_month
    """

    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_group = conn_ck.ck_select_to_df(sql)
    #
    df_group.to_excel('F://Desktop//df_group_2026Q1.xlsx', index=0)
    # df_order_info.to_excel('F://Desktop//df_order_info_10.xlsx', index=0)

get_order_group()

##
def tt_order_group():
    sql = f"""
    SELECT warehouse, sales_status, platform_code, sum(total_price), sum(release_money),sum(true_profit_new1), sum(real_profit)
    FROM (
        SELECT DISTINCT
            order_id, platform_code, sku, seller_sku, account_id, payment_time, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, release_money, 
            case 
                when sales_status in ('正利润加快动销','负利润加快动销','清仓') then '清仓' 
                when sales_status in ('涨价缩销') then '正常' else sales_status 
            end as sales_status
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '2026-01-01' and payment_time < '2026-04-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and warehouse_name not like '%精品%'
            -- and platform_code in ('AMAZON', 'EB','WALMART','CDISCOUNT','WISH')
            -- and sales_status = '正常'
    ) a
    GROUP BY warehouse, sales_status, platform_code
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_group = conn_ck.ck_select_to_df(sql)
    #
    df_group.to_excel('F://Desktop//tt_group_2026Q1.xlsx', index=0)
    # df_order_info.to_excel('F://Desktop//df_order_info_10.xlsx', index=0)

tt_order_group()

##
def tt_order_group_new():
    sql = f"""
    SELECT warehouse, sales_status, platform_code, formatDateTime(toDateTime(payment_time), '%Y-%m') AS date_month, 
    sum(total_price), sum(release_money), sum(true_profit_new1), sum(real_profit)
    FROM (
        SELECT DISTINCT
            order_id, platform_code, sku, seller_sku, account_id, payment_time, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, release_money, 
            case 
                when sales_status in ('正利润加快动销','负利润加快动销','清仓') then '清仓' 
                when sales_status in ('涨价缩销','-') then '正常' else sales_status 
            end as sales_status
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '2025-01-01' and payment_time < '2026-01-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and warehouse_name not like '%精品%'
            -- and platform_code in ('AMAZON', 'EB','WALMART','CDISCOUNT','WISH')
            -- and sales_status = '正常'
    ) a
    GROUP BY date_month, warehouse, sales_status, platform_code
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_group = conn_ck.ck_select_to_df(sql)
    #
    df_group.to_excel('F://Desktop//tt_group_2025.xlsx', index=0)
    # df_order_info.to_excel('F://Desktop//df_order_info_10.xlsx', index=0)

tt_order_group_new()
##
def get_stock():
    sql = f"""
        SELECT
            date_id, sku, type, best_warehouse_name, warehouse, new_price, available_stock,available_stock_money, warehouse_stock, overage_level, 
            age_90_plus, age_120_plus, age_150_plus, age_180_plus, age_270_plus,age_360_plus, charge_total_price_rmb,day_sales,
            estimated_sales_days,overage_esd, age_60_plus-age_90_plus as 60_90,age_90_plus-age_120_plus as 90_120, age_120_plus-age_180_plus as 120_180,
            age_180_plus-age_270_plus 180_270, age_270_plus-age_360_plus 270_360, age_360_plus `360+`,
            age_90_plus*new_price age_90_money, age_120_plus*new_price age_120_money, 
            age_180_plus*new_price age_180_money, age_270_plus*new_price age_270_money, age_360_plus*new_price age_360_money
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2026-03-31' and available_stock > 0 
        and best_warehouse_name not like '%%精品%%'
        
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock = conn.read_sql(sql)
    #
    df_stock[['available_stock_money','age_90_money']] = df_stock[['available_stock_money','age_90_money']].astype(float)
    #
    print(df_stock['available_stock_money'].sum(), df_stock['age_90_money'].sum())
    #
    df_stock.to_excel('F://Desktop//df_stock_3.xlsx', index=0)
get_stock()

##
def get_stock_group():
    sql = f"""
    SELECT date_id, warehouse, sum(available_stock_money) available_stock_money, sum(0_90_money) 0_90_money, 
    sum(90_180_money) 90_180_money,sum(180_360_money) 180_360_money, sum(age_360_money) age_360_money,
    sum(90_plus_money) 90_plus_money, sum(180_plus_money) 180_plus_money
    FROM (
        SELECT
            date_id, sku, type, best_warehouse_name, warehouse, new_price, available_stock, available_stock_money, 
            warehouse_stock, overage_level, 
            (available_stock-age_90_plus)*new_price as 0_90_money, (age_90_plus-age_180_plus)*new_price as 90_180_money, 
            (age_180_plus-age_360_plus)*new_price as 180_360_money, age_360_plus*new_price age_360_money,
            age_90_plus*new_price 90_plus_money, age_180_plus*new_price 180_plus_money
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id in ('2025-12-15') and available_stock > 0
        and best_warehouse_name not like '%%精品%%'
    ) a
    GROUP BY date_id, warehouse
    """
    # ('2024-12-16', '2024-12-15', '2025-01-15', '2025-02-14', '2025-03-14', '2025-04-15', '2025-05-15', '2025-06-16',
    #  '2025-07-15', '2025-08-15', '2025-09-15', '2025-10-15', '2025-11-14', '2025-12-15')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock = conn.read_sql(sql)
    #

    df_stock.to_excel('F://Desktop//df_stock_group.xlsx', index=0)
get_stock_group()

## 定价比
sql = f"""
    SELECT *
    FROM over_sea.ads_pricing_ratio
    WHERE date = '2024-10-10'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_pr = conn.read_sql(sql)

##
df_pr.to_excel('df_pr.xlsx', index=0)




