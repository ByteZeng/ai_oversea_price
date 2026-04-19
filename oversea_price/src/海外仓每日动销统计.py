
##
import pandas as pd
import numpy as np
import warnings
import pymysql
import datetime
from clickhouse_driver import Client
from utils.scripts_ck_client import CkClient
from utils.utils import str_to_datetime, read_sql_ck, get_ck_client, expand_timespan_by_row, get_date, \
    df_to_list_tuple, save_df, get_mysql_con, \
    get_filter_by_sql_mysql_in_large_tuple, df_to_tuple_tuple, is_df_exist, read_df, make_path, get_or_read_df, \
    get_filter_by_sql_ck_in_large_list, get_path, filter_by_df_filter
from all_auto_task.oversea_price_adjust_2023 import is_new_sku
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
import utils
import datetime,time
import os
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang

warnings.filterwarnings('ignore')

##
def main():
    """
    """
    # utils.utils.program_name = '海外仓每日动销统计'
    # make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()
    print(date_today.weekday())
    if date_today.weekday() == 4:
        date_start = date_today - datetime.timedelta(days=7)
    # elif date_today.weekday() == 3:
    #     date_start = date_today - datetime.timedelta(days=7)
    else:
        date_start = date_today - datetime.timedelta(days=3)
    date_end = datetime.date.today() - datetime.timedelta(days=1)

    # ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
    #                      db_name='over_sea')
    ck_client = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # 总计
    sql_total = f"""
    select
        -- sales_status `销售状态`,
        paytime,
        round(sum(release_money)/10000, 2) `销库金额（万RMB）`,
        round(sum(true_profit_new1)/10000, 2) as `订单毛利润（万RMB）`,
        round(sum(total_price)/10000, 2) as `订单销售额（万RMB）`,
        round(100 * sum(true_profit_new1) /sum(total_price), 2) as `订单毛利润率`
    FROM (
        SELECT 
            DISTINCT order_id, sku, paytime, release_money, true_profit_new1, total_price, sales_status,
            platform_code,warehouse_name
        FROM yibai_oversea.dashbord_new_data1
    ) a
    WHERE 
        paytime >= \'{date_start}\'
        and paytime <= \'{date_end}\'
        and `total_price` > 0 
        and `sales_status` not in ('总计')
        and warehouse_name not like '%精品%'
        -- and platform_code != 'TEMU'
        -- and sales_status IN ('总计', '负利润加快动销')
    GROUP BY
        paytime
    order by paytime
    """
    df_total = ck_client.ck_select_to_df(sql_total)
    print(df_total.info())
    for index, row in df_total.iterrows():
        print(f"{row['paytime']}:")
        print(f"海外仓销库{row['销库金额（万RMB）']}W，订单毛利润{row['订单毛利润（万RMB）']}W，"
              f"订单毛利润率{row['订单毛利润率']}%")

    # 清仓
    sql_qingcang = f"""
            select
        	-- sales_status `销售状态`,
        	paytime,
        	round(sum(release_money)/10000, 2) `销库金额（万RMB）`,
        	round(sum(true_profit_new1)/10000, 2) as `订单毛利润（万RMB）`,
        	round(sum(total_price)/10000, 2) as `订单销售额（万RMB）`,
        	round(100 * sum(true_profit_new1) /sum(total_price), 2) as `订单毛利润率`
        FROM (
            SELECT 
                DISTINCT order_id, sku, paytime, release_money, true_profit_new1, total_price, sales_status,
                platform_code,warehouse_name
            FROM yibai_oversea.dashbord_new_data1
        ) a
        WHERE 
        	paytime >= \'{date_start}\'
        	and paytime <= \'{date_end}\'
        	and `total_price` > 0 
        	and sales_status IN ('正利润加快动销', '负利润加快动销', '清仓')
        	and warehouse_name not like '%精品%'
        	-- and platform_code != 'TEMU'
        GROUP BY
        	paytime
        order by paytime
        """
    df_qingcang = ck_client.ck_select_to_df(sql_qingcang)
    for index, row in df_qingcang.iterrows():
        print(f"{row['paytime']}:")
        print(f"其中清仓销库{row['销库金额（万RMB）']}W，订单毛利润{row['订单毛利润（万RMB）']}W，"
              f"订单毛利润率{row['订单毛利润率']}%")

    # 周数据汇总
    print(f"{date_start}至{date_end}：")
    print(df_total.info())
    df_total_week = round(100*df_total['订单毛利润（万RMB）'].sum()/df_total['订单销售额（万RMB）'].sum(),2)
    print(f"本周海外仓销库{round(df_total['销库金额（万RMB）'].sum(),2)}W，订单毛利润{round(df_total['订单毛利润（万RMB）'].sum(),2)}W，"
          f"订单毛利润率{df_total_week}%")

    df_qingcang_week = round(100*df_qingcang['订单毛利润（万RMB）'].sum()/df_qingcang['订单销售额（万RMB）'].sum(),2)
    print(f"其中清仓销库{round(df_qingcang['销库金额（万RMB）'].sum(),2)}W，订单毛利润{round(df_qingcang['订单毛利润（万RMB）'].sum(),2)}W，"
          f"订单毛利润率{df_qingcang_week}%")
    print('done!')
##
def temp_data():
    # 临时。统计墨西哥FBA销量
    sql = f"""
        WITH order_temp as (
            SELECT distinct order_id
            FROM domestic_warehouse_clear.monitor_fba_order
            WHERE created_time >= '2023-09-27' and `站点` = '墨西哥'
        ) 
        SELECT account_id, seller_sku, m.order_id order_id, m.sku sku, created_time, sales_status, `sku数量` as `销量`, `净利润`, `销售额`
        FROM (
            SELECT account_id, seller_sku, sku,order_id, created_time, `sku数量`, `销售额`, `净利润`
            FROM domestic_warehouse_clear.monitor_fba_order
            WHERE created_time >= '2023-09-27' and `站点` = '墨西哥'
        ) m
        LEFT JOIN (
            SELECT id, order_id
            FROM yibai_oms_sync.yibai_oms_order_detail
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) d ON m.order_id = d.order_id
        LEFT JOIN (
            SELECT order_detail_id, sales_status, sku
            FROM yibai_oms_sync.yibai_oms_order_package_detail
            WHERE sales_status <> ''
        ) p ON p.order_detail_id = d.id and p.sku = m.sku
    
    """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_mx_sales = conn_ck.ck_select_to_df(sql)
    ##
    # 产品成本、重量、体积
    df_mx_sales['数量'] = 1
    df_mx_sales = chicun_zhongliang(df_mx_sales, 1, conn_ck)
    ##
    df_mx_sales.to_excel('df_mx_sales_info.xlsx', index=0)
    ##
    sql = """
            SELECT
                order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, warehouse,
                quantity, release_money, sales_status
            FROM over_sea.dashbord_new1
            WHERE 
                paytime >= '2023-09-27'
                and `total_price` > 0 
                and `sales_status` not in ('','nan','总计')
                and warehouse_name = 'YM墨西哥2仓'
    """
    ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                         db_name='over_sea')
    df_order_info = ck_client.ck_select_to_df(sql)
    ##
    df_order_info.to_excel('YM墨西哥2仓近90天订单.xlsx', index=0)

##
def get_order_info():
    """
    获取海外仓订单数据明细
    输出：每月各平台工业海外仓订单利润率情况
    分仓位、分平台、分到仓天数、区分新老品
    """
    #订单时间范围
    now = datetime.datetime.now()
    last_month = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)
    # 上个月开始日期。往前推三天，避免订单获取不全
    start_date = (datetime.datetime(last_month.year, last_month.month, 1) - datetime.timedelta(days=2)).strftime(
        '%Y-%m-%d')
    # 上个月结束日期。往后延两天
    end_date = (last_month + datetime.timedelta(days=3)).strftime('%Y-%m-%d')
    ##
    sql = f"""
     
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, warehouse,
            quantity, release_money, sales_status
        FROM over_sea.dashbord_new1
        WHERE 
            paytime >= '{start_date}' and paytime <= '{now}'
            and `total_price` > 0 
            and `sales_status` not in ('总计')
    """
    ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                         db_name='over_sea')
    df_order_info = ck_client.ck_select_to_df(sql)
    df_order_info = df_order_info.drop_duplicates(subset=['order_id', 'sku', 'account_id'])
    ##
    sku_list = tuple(df_order_info['sku'].unique())
    ## 到仓天数、是否新品
    # 产品线
    sql_line = f"""
            select a.sku sku, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id)
            WHERE sku in {sku_list}
        """
    client = get_ck_client(user='zhangyilan', password='zhangyilan2109221544')
    df_line = read_sql_ck(sql_line, client)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    ##
    # 是否新品
    df_new = is_new_sku()
    ## 到仓天数
    sql = """
        SELECT
            sku, warehouse, available_stock, warehouse_stock_age, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2023-12-18'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku_info = conn.read_sql(sql)
    ## 匹配数据
    df = pd.merge(df_order_info, df_line, how='left', on=['sku'])
    df = pd.merge(df, df_new, how='left', on=['sku'])
    df = pd.merge(df, df_sku_info, how='left', on=['sku', 'warehouse'])

    df = df.drop_duplicates(subset=['order_id', 'sku', 'account_id'])
    ##
    # df.to_excel('df_order.xlsx', index=0)
    return df
##
def tt_get_oversea_order():
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=15)
    # date_start = '2025-04-01'
    # date_end = '2025-05-01'
    # 仅更新近1个月订单
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"delete from over_sea.ads_tt_oversea_order where payment_time >= '{date_start}' "
    conn.execute(sql)
    conn.close()

    sql = f"""
        SELECT
            *
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '{date_start}'
            -- and payment_time < 'date_end'
            -- and `total_price` > 0 
            -- and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info.drop(['id', 'platform_order_id', 'update_time'], axis=1, inplace=True)
    df_order_info = df_order_info.drop_duplicates(subset=['order_id', 'sku', 'account_id'])

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_order_info, 'ads_tt_oversea_order', if_exists='append')


def tt_main():
    """
    """
    # utils.utils.program_name = '海外仓每日动销统计'
    # make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()
    print(date_today.weekday())
    if date_today.weekday() == 4:
        date_start = date_today - datetime.timedelta(days=7)
    # elif date_today.weekday() == 3:
    #     date_start = date_today - datetime.timedelta(days=7)
    else:
        date_start = date_today - datetime.timedelta(days=3)
    date_end = datetime.date.today() - datetime.timedelta(days=1)


    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # 总计
    sql_total = f"""
    select
	-- sales_status `销售状态`,
	payment_time,
	round(sum(release_money)/10000, 2) `销库金额（万RMB）`,
	round(sum(true_profit_new1)/10000, 2) as `订单毛利润（万RMB）`,
	round(sum(total_price)/10000, 2) as `订单销售额（万RMB）`,
	round(100 * sum(true_profit_new1) /sum(total_price), 2) as `订单毛利润率`
FROM (
	SELECT 
	    DISTINCT order_id, sku, payment_time, release_money, true_profit_new1, total_price, sales_status,
	    platform_code
	FROM yibai_oversea.tt_dashbord_new_data1
) a
WHERE 
	payment_time >= \'{date_start}\'
	and payment_time <= \'{date_end}\'
	and `total_price` > 0 
	and `sales_status` not in ('总计')
	and warehouse_name not like '%精品%'
	-- and platform_code != 'TEMU'
	-- and sales_status IN ('总计', '负利润加快动销')
GROUP BY
	payment_time
order by payment_time
"""
    df_total = conn_ck.ck_select_to_df(sql_total)
    print(df_total.info())
    for index, row in df_total.iterrows():
        print(f"{row['payment_time']}:")
        print(f"海外仓销库{row['销库金额（万RMB）']}W，订单毛利润{row['订单毛利润（万RMB）']}W，"
              f"订单毛利润率{row['订单毛利润率']}%")

    # 清仓
    sql_qingcang = f"""
            select
        	-- sales_status `销售状态`,
        	payment_time,
        	round(sum(release_money)/10000, 2) `销库金额（万RMB）`,
        	round(sum(true_profit_new1)/10000, 2) as `订单毛利润（万RMB）`,
        	round(sum(total_price)/10000, 2) as `订单销售额（万RMB）`,
        	round(100 * sum(true_profit_new1) /sum(total_price), 2) as `订单毛利润率`
        FROM (
            SELECT 
                DISTINCT order_id, sku, payment_time, release_money, true_profit_new1, total_price, sales_status,
                platform_code
            FROM yibai_oversea.tt_dashbord_new_data1
        ) a
        WHERE 
        	payment_time >= \'{date_start}\'
        	and payment_time <= \'{date_end}\'
        	and `total_price` > 0 
        	and sales_status IN ('正利润加快动销', '负利润加快动销', '清仓')
        	and warehouse_name not like '%精品%'
        	-- and platform_code != 'TEMU'
        GROUP BY
        	payment_time
        order by payment_time
        """
    df_qingcang = conn_ck.ck_select_to_df(sql_qingcang)
    for index, row in df_qingcang.iterrows():
        print(f"{row['payment_time']}:")
        print(f"其中清仓销库{row['销库金额（万RMB）']}W，订单毛利润{row['订单毛利润（万RMB）']}W，"
              f"订单毛利润率{row['订单毛利润率']}%")

    # 周数据汇总
    print(f"{date_start}至{date_end}：")
    print(df_total.info())
    df_total_week = round(100*df_total['订单毛利润（万RMB）'].sum()/df_total['订单销售额（万RMB）'].sum(),2)
    print(f"本周海外仓销库{round(df_total['销库金额（万RMB）'].sum(),2)}W，订单毛利润{round(df_total['订单毛利润（万RMB）'].sum(),2)}W，"
          f"订单毛利润率{df_total_week}%")

    df_qingcang_week = round(100*df_qingcang['订单毛利润（万RMB）'].sum()/df_qingcang['订单销售额（万RMB）'].sum(),2)
    print(f"其中清仓销库{round(df_qingcang['销库金额（万RMB）'].sum(),2)}W，订单毛利润{round(df_qingcang['订单毛利润（万RMB）'].sum(),2)}W，"
          f"订单毛利润率{df_qingcang_week}%")
    print('done!')

if __name__ == '__main__':
    main()
    # tt_main()