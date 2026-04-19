"""
限时清仓任务完成情况：
1、各平台整体任务
2、sku利润率监控

海外仓正常品负利润订单调价
1、取订单
2、取调价逻辑信息
3、比较发货仓和运费
4、计算目标利润率

"""

import time, datetime
import pandas as pd
import numpy as np
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from requests.auth import HTTPBasicAuth
import requests
import tqdm
from all_auto_task.oversea_amazon_asin_fba import amazon_fba_listing
from all_auto_task.oversea_price_adjust_2023 import dwm_oversea_bundle
from all_auto_task.oversea_sku_price import get_flash_sku


def get_np_orders(platform='EB'):
    """ 取订单 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=90)
    # date_start = '2024-12-01'
    # date_end = '2024-12-31'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_today}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and platform_code = '{platform}'
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info = df_order_info.drop_duplicates(subset=['order_id','sku','account_id'])
    # 取订单国家、订单费用明细
    order_list = df_order_info['order_id'].to_list()
    df_country = pd.DataFrame()
    for n in range(0, len(order_list), 500):
        sku = order_list[n:n+500]
        sql = f"""
        WITH order_temp as (
            SELECT DISTINCT order_id
            FROM yibai_oms_sync.yibai_oms_order
            WHERE 
                order_id in ({sku})
        )
        SELECT distinct 
        B.order_id,
        B.ship_country country,
        B.ship_code,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B 
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        """
        conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        df_temp = conn_ck.ck_select_to_df(sql)
        df_temp.columns = [i.split('.')[-1] for i in df_temp.columns]
        df_country = pd.concat([df_temp, df_country])
    df_country['country'] = df_country['country'].replace('GB','UK')
    df_order_info = pd.merge(df_order_info, df_country, how='left', on=['order_id'])
    print(df_order_info.info())
    # 监控
    # monitor_order(df_order_info)

    # 取链接
    c1 = (df_order_info['real_profit']<0) & (df_order_info['sales_status']=='正常')
    df = df_order_info[c1]

    print(f'{platform}平台共{len(df)}个订单')

    return df


##
def get_adjust_listing(df, table='oversea_ebay_listing_all'):
    """ 取各平台调价链接信息 """
    sql = f"""
        SELECT account_id, item_id, sku, online_price, warehouse, best_warehouse_name, new_price, total_cost,
        price, target_profit_rate, rate
        FROM yibai_oversea.{table}
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table} WHERE date_id > '2025-08-20')
    """
    # sql = f"""
    #     SELECT account_id, item_id, sku, online_price, warehouse, best_warehouse_name, new_price, total_cost,
    #     price, target_profit_rate, rate
    #     FROM yibai_oversea.{table}
    #     WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table} WHERE date_id > '2025-08-20')
    # """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_listing, how='left', on=['account_id', 'item_id','sku'])

    df.to_excel('F://Desktop//df_listing_info.xlsx', index=0)

def get_adjust_price(platform='EB'):
    """ 获取sku目标价格信息 """
    now = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT 
            SKU as sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level, available_stock, sales_status,price,
            target_profit_rate,is_adjust, day_sales, country, ship_name, lowest_price
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE platform = '{platform}' and date_id = '{now}'
    """
    df_price_dtl = conn_ck.ck_select_to_df(sql)
    c1 = (df_price_dtl['warehouse'] == '英国仓') & (df_price_dtl['country'] != 'UK')
    c2 = (df_price_dtl['warehouse'] != '英国仓') & (df_price_dtl['country'] == 'UK')
    df_price_dtl = df_price_dtl[~(c1 | c2)]

    return df_price_dtl


def monitor_order(df_order_info):
    """ 监控函数 """
    p_all = len(df_order_info)
    p1 = len(df_order_info[(df_order_info['real_profit']<0) & (df_order_info['sales_status']=='正常')])
    print(f'近三个月正常品负利润订单占比{p1/p_all:.2%}')

    # 确保pay_time是datetime类型
    df_order_info['paytime'] = pd.to_datetime(df_order_info['paytime'])

    # 获取最近三个月的日期范围
    latest_date = df_order_info['paytime'].max()
    three_months_ago = latest_date - pd.DateOffset(months=3)

    # 筛选最近三个月的数据
    df_recent = df_order_info[df_order_info['paytime'] >= three_months_ago]

    # 按月份分组计算
    monthly_stats = df_recent.groupby(df_recent['paytime'].dt.to_period('M')).apply(
        lambda x: pd.Series({
            'total_orders': len(x),
            'negative_profit_orders': len(x[(x['real_profit'] < 0) & (x['sales_status'] == '正常')])
        })
    )
    # 计算占比并打印
    for month, data in monthly_stats.iterrows():
        ratio = data['negative_profit_orders'] / data['total_orders']
        print(f'{month}月正常品负利润订单占比{ratio:.2%}')

def main():
    """ 主函数 """

    df = get_np_orders('EB')
    df = df.rename(columns={'seller_sku':'item_id'})
    df_price = get_adjust_price('EB')

    df = pd.merge(df, df_price, how='left', on=['sku', 'country'])

    # df = df.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
    #     drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    # df.drop('overage_level', axis=1, inplace=True)
    print(df.info())
    df.to_excel('F://Desktop//df_ebay_order.xlsx', index=0)


def get_order_detail():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    date_end= date_today - datetime.timedelta(days=90)
    # date_start = '2025-04-01'
    # date_end = '2025-05-01'
    # 仅更新近1个月订单
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"delete from over_sea.oversea_order_monitor where payment_time >= '{date_start}' "
    conn.execute(sql)
    conn.close()

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_today}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            -- and platform_code ='TEMU' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        W.warehouse_name,
        W.warehouse,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM yibai_logistics_tms_sync.yibai_warehouse yw
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # 渠道名称替换
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')
    # df.to_excel('F://Desktop//oversea_order_monitor.xlsx', index=0)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_order_monitor', if_exists='append')
    conn.close()

    # 只保留近90天数据
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"delete from over_sea.oversea_order_monitor where payment_time < '{date_end}' "
    conn.execute(sql)
    conn.close()

def get_jp_account(org='YB'):
    """ 获取精品子仓的数据 """
    if org=='YB':
        sql = """
            select distinct platform_code,account_id 
            from yibai_sale_center_system_sync.yibai_system_account
            where is_yibai != 1
        """
        conn_ck = pd_to_ck(database='yibai_sale_center_system_sync', data_sys='调价明细历史数据')
        df_account = conn_ck.ck_select_to_df(sql)
    elif org=='TT':
        sql = """
            select distinct platform_code,account_id 
            from tt_sale_center_system_sync.tt_system_account
            where is_yibai != 1
        """
        conn_ck = pd_to_ck(database='tt_sale_center_system_sync', data_sys='通拓-新')
        df_account = conn_ck.ck_select_to_df(sql)

    return df_account

def get_order_detail_temp():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    date_end= date_today - datetime.timedelta(days=90)
    date_start = '2026-01-01'
    # date_end = '2025-05-01'
    sku_temp = get_supplier_sku()
    sku_list = tuple(sku_temp['sku'].unique())

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_today}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            -- and platform_code ='TEMU' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        W.warehouse_name,
        W.warehouse,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM yibai_logistics_tms_sync.yibai_warehouse yw
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and warehouse in ('美国仓','德国仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    df = df[df['sku'].isin(sku_list)]
    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # 渠道名称替换
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')

    # 20260106 去掉精品账号的销库数据
    df_jp_account = get_jp_account()
    df_jp_account['is_jp'] = 1
    df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])

    # df[~df['sales_status'].isin(['总计','','nan'])].to_excel('F://Desktop//df_order_temp.xlsx', index=0)

    # df = df[~df['is_jp'].isna()]
    # df.drop('is_jp', axis=1, inplace=True)

    # 2、配库sku
    df_order_sku = get_supplier_order_sku()
    df_order = pd.merge(df, df_order_sku, how='left', on='order_id')
    df_order['配库sku'] = np.where(df_order['配库sku'].isna(), df_order['sku'], df_order['配库sku'])

    # 3、供应商sku
    df_sku = get_sku_type()

    df_order = df_order[df_order['配库sku'].isin(df_sku['配库sku'].unique())]
    df_order = pd.merge(df_order, df_sku[['配库sku', 'develop_source_name']], how='left', on=['配库sku'])

    # 4、净利率计算
    df_order = get_platform_zero(df_order)
    df_order["real_profit"] = df_order["true_profit_new1"] - df_order["差值"] * df_order["total_price"]
    df_order["real_profit"] = df_order["real_profit"].astype(float)
    df_order["real_profit"] = df_order["real_profit"].round(4)
    # df_order.drop(['差值2', '差值'], axis=1, inplace=True)
    df_order.drop(['差值2'], axis=1, inplace=True)

    df_order['org'] = 'YB'
    df_order_2 = tt_get_order_detail_temp()
    df_order_2['org'] = 'TT'
    df_order = pd.concat([df_order, df_order_2])

    df_order.to_excel('F://Desktop//oversea_order_monitor_sup.xlsx', index=0)

def tt_get_order_detail_temp():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=10)
    date_end= date_today - datetime.timedelta(days=90)
    date_start = '2026-01-01'
    # date_end = '2025-05-01'
    sku_temp = get_supplier_sku()
    sku_list = tuple(sku_temp['sku'].unique())

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM tt_oms_sync.tt_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_today}'
            and payment_status=1 and order_status <> 80
            and warehouse_id not in (60,323)                  
            -- and platform_code ='TEMU' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN (C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        W.warehouse_name,
        W.warehouse,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN (C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        INNER JOIN (
            SELECT 
                yw.id AS warehouse_id,   
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
                yw.country, ebay_category_id,
                ywc.name AS warehouse, yw.warehouse_type
            FROM tt_logistics_tms_sync.tt_warehouse yw
            LEFT JOIN tt_logistics_tms_sync.tt_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and warehouse in ('德国仓')
        ) W ON B.warehouse_id = W.warehouse_id
    """
    conn_ck = pd_to_ck(database='tt_oms_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    df = df[df['sku'].isin(sku_list)]
    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # 渠道名称替换
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')

    # # 20260106 去掉精品账号的销库数据
    # df_jp_account = get_jp_account(org='TT')
    # df_jp_account['is_jp'] = 1
    # df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])

    # df[~df['sales_status'].isin(['总计','','nan'])].to_excel('F://Desktop//df_order_temp.xlsx', index=0)

    # df = df[df['is_jp'].isna()]
    # df.drop('is_jp', axis=1, inplace=True)

    # 2、配库sku
    df_order_sku = get_supplier_order_sku(org='TT')
    df_order = pd.merge(df, df_order_sku, how='left', on='order_id')
    df_order['配库sku'] = np.where(df_order['配库sku'].isna(), df_order['sku'], df_order['配库sku'])

    # 3、供应商sku
    df_sku = get_sku_type()

    # df_order = df_order[df_order['配库sku'].isin(df_sku['配库sku'].unique())]
    df_order = pd.merge(df_order, df_sku[['配库sku', 'develop_source_name']], how='left', on=['配库sku'])

    # 4、净利率计算
    df_order = get_platform_zero(df_order)
    df_order["real_profit"] = df_order["true_profit_new1"] - df_order["差值"] * df_order["total_price"]
    df_order["real_profit"] = df_order["real_profit"].astype(float)
    df_order["real_profit"] = df_order["real_profit"].round(4)
    # df_order.drop(['差值2', '差值'], axis=1, inplace=True)
    df_order.drop(['差值2'], axis=1, inplace=True)

    df_order.to_excel('F://Desktop//oversea_order_monitor_tt.xlsx', index=0)
    return df_order


def generate_platform_summary():
    """
    从订单明细生成platform_code维度汇总表（仅保留org-platform_code行，无任何汇总行）
    输入字段：org、platform_code、payment_time、total_price、quantity、true_profit_new1（毛利润）、real_profit（净利润）
    输出特性：
    1. 所有月份的销量/销售额/利润等指标横向排列
    2. platform_code按「总计销售额」降序排序（按org分组内排序）
    3. 仅保留org-platform_code明细行，无org总计/全局汇总行
    """
    df = pd.read_excel('F://Desktop//oversea_order_monitor_sup.xlsx')
    print(df.info())

    # 1. 日期处理：提取年月
    df['payment_time'] = pd.to_datetime(df['payment_time'], errors='coerce')
    df['year_month'] = df['payment_time'].dt.strftime('%Y-%m')
    months = sorted(df['year_month'].dropna().unique())  # 按时间排序月份

    # 2. 汇率转换：核心字段转为USD（先转再聚合，保证精度）
    us_rate = 6.9194
    df['total_price_usd'] = df['total_price'] / us_rate       # 销售额转USD
    df['true_profit_new1_usd'] = df['true_profit_new1'] / us_rate  # 毛利润转USD
    df['real_profit_usd'] = df['real_profit'] / us_rate        # 净利润转USD

    # 2. 按【org+platform_code+年月】聚合
    group_cols = ['org', 'platform_code', 'year_month']
    df_agg = df.groupby(group_cols).agg(
        quantity=('quantity', 'sum'),
        total_price=('total_price', 'sum'),
        true_profit_new1=('true_profit_new1', 'sum'),  # 毛利润
        real_profit=('real_profit', 'sum')  # 净利润
    ).reset_index()

    # 3. 计算衍生指标
    df_agg['avg客单价USD'] = df_agg['total_price'] / df_agg['quantity'].replace(0, np.nan)
    df_agg['毛利率'] = df_agg['true_profit_new1'] / df_agg['total_price'].replace(0, np.nan)
    df_agg['净利率'] = df_agg['real_profit'] / df_agg['total_price'].replace(0, np.nan)

    # 4. 透视成宽表
    pivot_names = {
        'quantity': '{ym}销量',
        'total_price': '{ym}销售额USD',
        'true_profit_new1': '{ym}利润_USD',  # 对齐示例中“利润USD”
        'real_profit': '{ym}净利润USD',
        'avg客单价USD': '{ym}平均客单价USD',
        '毛利率': '{ym}平均订单利润率',  # 对齐示例中“平均订单利润率”
        '净利率': '{ym}净利率'
    }

    result = df_agg.pivot_table(
        index=['org', 'platform_code'],
        columns='year_month',
        values=['quantity', 'total_price', 'true_profit_new1', 'real_profit', 'avg客单价USD', '毛利率', '净利率']
    )
    result.columns = [pivot_names[col[0]].format(ym=col[1].replace('-', '')) for col in result.columns]
    result = result.reset_index()

    # 5. 按【org+总计销售额】降序排序
    sales_cols = [col for col in result.columns if '销售额USD' in col]
    result['总计销售额'] = result[sales_cols].sum(axis=1)
    result = result.sort_values(by=['org', '总计销售额'], ascending=[True, False]).reset_index(drop=True)

    # 6. 核心：按示例图重新排列列顺序
    # 指标分组顺序：销量 → 销售额 → 毛利润 → 净利润 → 客单价 → 平均订单利润率(毛利率) → 净利率
    col_order = ['org', 'platform_code']
    metric_order = [
        '销量',
        '销售额USD',
        '利润_USD',
        '净利润USD',
        '平均客单价USD',
        '平均订单利润率',
        '净利率'
    ]
    month_suffixes = [m.replace('-', '') for m in months]
    for metric in metric_order:
        for suffix in month_suffixes:
            col_name = f'{suffix}{metric}'
            if col_name in result.columns:
                col_order.append(col_name)

    final_df = result[col_order].drop(columns=['总计销售额'], errors='ignore')

    # 7. 数值格式化
    for col in final_df.columns:
        if any(x in col for x in ['销售额USD', '利润_USD', '净利润USD', '平均客单价USD']):
            final_df[col] = final_df[col].round(2)
        elif any(x in col for x in ['平均订单利润率', '净利率']):
            final_df[col] = (final_df[col].round(4) * 100).round(2)  # 百分比保留2位小数

    final_df.to_excel('F://Desktop//final_df_2.xlsx', index=0)
    return final_df

# 取中台差值
def get_platform_zero(df, org='YB'):
    """ 取中台差值表 """
    # 20251205 切换取中台差值表。先用平台+国家匹配，再用平台匹配，最后匹配不上的填充0.17
    if org=='YB':
        sql = """
    
             SELECT platform_code, site ship_country , toFloat32(net_profit2)/100 as `差值`
             FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
             WHERE shipping_type = 5 and is_del = 0 and status = 1
         """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    elif org=='TT':
        sql = """

             SELECT platform_code, site ship_country , toFloat32(net_profit2)/100 as `差值`
             FROM tt_sale_center_listing_sync.tt_listing_profit_config
             WHERE shipping_type = 5 and is_del = 0 and status = 1
         """
        conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['ship_country'] == 'other']
    df_fee2 = df_fee2.rename(columns={'差值': '差值2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    df = pd.merge(df, df_fee1, how='left', on=['platform_code', 'ship_country'])
    df = pd.merge(df, df_fee2[['platform_code', '差值2']], how='left', on=['platform_code'])
    df['差值'] = np.where(df['差值'].isna(), df['差值2'], df['差值'])
    df['差值'] = df['差值'].fillna(0.13).astype(float)



    return df

# fba订单
def get_order_detail_fba():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    date_end= date_today - datetime.timedelta(days=90)
    date_start = '2026-02-01'
    date_end = '2026-03-01'
    # sku_temp = get_supplier_sku()
    # sku_list = tuple(sku_temp['sku'].unique())

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id in (323)
            and account_id in (
                SELECT DISTINCT account_id FROM yibai_sale_center_system_sync.yibai_system_account
                WHERE platform_code = 'AMAZON' and site_code = 'MX')               
            -- and platform_code ='TEMU' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        E.is_business_order `是否B2B`,
        C.product_price, adjust_amount, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        LEFT JOIN (
            select order_id,is_business_order from yibai_oms_sync.yibai_oms_order_extend
            where order_id in (SELECT order_id FROM order_temp) 
        ) E  ON A.order_id=E.order_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]

    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount', 'adjust_amount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # # 渠道名称替换
    # df_ship_code = get_yibai_logistics_logistics()
    # df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')

    # 20260106 去掉精品账号的销库数据
    df_jp_account = get_jp_account()
    df_jp_account['is_jp'] = 1
    df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])

    # df[~df['sales_status'].isin(['总计','','nan'])].to_excel('F://Desktop//df_order_temp.xlsx', index=0)

    df = df[df['is_jp'].isna()]
    df.drop('is_jp', axis=1, inplace=True)


    # # 4、净利率计算
    # df_order = get_platform_zero(df)
    # df_order["real_profit"] = df_order["true_profit_new1"] - df_order["差值"] * df_order["total_price"]
    # df_order["real_profit"] = df_order["real_profit"].astype(float)
    # df_order["real_profit"] = df_order["real_profit"].round(4)
    # df_order.drop(['差值2', '差值'], axis=1, inplace=True)

    # df_order_2 = tt_get_order_detail_temp()
    # df_order = pd.concat([df_order, df_order_2])

    df.to_excel('F://Desktop//oversea_order_monitor_mx.xlsx', index=0)


def get_order_detail_dom():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    date_end= date_today - datetime.timedelta(days=90)
    date_start = '2026-02-01'
    date_end = '2026-03-01'
    # sku_temp = get_supplier_sku()
    # sku_list = tuple(sku_temp['sku'].unique())

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id in (478, 481)             
            and platform_code ='WALMART' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        E.is_business_order `是否B2B`,
        C.product_price, adjust_amount, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64OrZero(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM yibai_oms_sync.yibai_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
        LEFT JOIN (
            select order_id,is_business_order from yibai_oms_sync.yibai_oms_order_extend
            where order_id in (SELECT order_id FROM order_temp) 
        ) E  ON A.order_id=E.order_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]

    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount', 'adjust_amount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # # 渠道名称替换
    # df_ship_code = get_yibai_logistics_logistics()
    # df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')

    # 20260106 去掉精品账号的销库数据
    df_jp_account = get_jp_account()
    df_jp_account['is_jp'] = 1
    df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])

    # df[~df['sales_status'].isin(['总计','','nan'])].to_excel('F://Desktop//df_order_temp.xlsx', index=0)

    df = df[df['is_jp'].isna()]
    df.drop('is_jp', axis=1, inplace=True)


    # # 4、净利率计算
    # df_order = get_platform_zero(df)
    # df_order["real_profit"] = df_order["true_profit_new1"] - df_order["差值"] * df_order["total_price"]
    # df_order["real_profit"] = df_order["real_profit"].astype(float)
    # df_order["real_profit"] = df_order["real_profit"].round(4)
    # df_order.drop(['差值2', '差值'], axis=1, inplace=True)

    # df_order_2 = tt_get_order_detail_temp()
    # df_order = pd.concat([df_order, df_order_2])

    df.to_excel('F://Desktop//oversea_order_monitor_walmart.xlsx', index=0)


def tt_get_order_detail_dom():
    """ 获取不同平台订单明细数据 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    date_end= date_today - datetime.timedelta(days=90)
    date_start = '2026-03-01'
    date_end = '2026-04-01'
    # sku_temp = get_supplier_sku()
    # sku_list = tuple(sku_temp['sku'].unique())

    sql = f"""
    WITH order_temp as (
        SELECT DISTINCT order_id
        FROM tt_oms_sync.tt_oms_order
        WHERE 
            payment_time>='{date_start}'
            and payment_time<'{date_end}'
            and payment_status=1 and order_status <> 80
            and warehouse_id in (478, 481)             
            and platform_code ='WALMART' 
    ) 
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku, A.item_id, B.account_id,
        B.payment_time, C.total_price, C.shipping_price, purchase_time,
        case 
            WHEN toFloat64(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1,
        CASE
            when order_status=1 then '下载'
            when order_status=10 then '待确认'
            when order_status=20 then '初始化'
            when order_status=30 then '正常'
            when order_status=40 then '待处理'
            when order_status=50 then '部分出库'
            when order_status=60 then '已出库'
            when order_status=70 then '已完结'
            when order_status=80 then '已取消'
            ELSE ''  
        END AS complete_status,
        case
            when B.order_type=1 then '常规线上平台客户订单'
            when B.order_type=2 then '线下客户订单'
            when B.order_type=3 then '线上客户补款单'
            when B.order_type=4 then '重寄单'
            when B.order_type=5 then '港前重寄'
            when B.order_type=6 then '虚拟发货单'
            ELSE '未知'
        END AS order_typr,
        CASE
            WHEN B.payment_status = 1 THEN '已付款' 
            ELSE '未付款'
        END AS pay_status,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id,
        B.ship_country,
        B.ship_code,
        A.quantity,
        F.sales_status as sales_status,
        C.product_price, adjust_amount, commission_fees, pay_cost, seller_discount, escrow_tax, purchase_cost_new1,
        shipping_cost, true_shipping_fee, 
        case 
            WHEN toFloat64(C.true_shipping_fee) > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, oversea_package_fee,pack,
        extra_price,exceedprice,residence_price, stock_price, exchange_price,profit_new1,profit_rate_new1
        FROM (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_detail 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) A
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) B ON A.order_id=B.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_profit 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) C ON B.order_id=C.order_id
        LEFT JOIN (
            SELECT *
            FROM tt_oms_sync.tt_oms_order_sku 
            WHERE order_id in (SELECT order_id FROM order_temp)
        ) F on F.order_detail_id=A.id
    """
    conn_ck = pd_to_ck(database='tt_oms_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]

    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print(df.info())
    col = ['total_price','shipping_price','true_profit_new1','product_price','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_cost', 'true_shipping_fee','shipping_fee',
           'first_carrier_cost','duty_cost','processing','package_cost','extra_price','exceedprice',
           'oversea_package_fee', 'pack', 'seller_discount', 'adjust_amount',
           'residence_price','stock_price','exchange_price','profit_new1','profit_rate_new1']
    for i in col:
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    # df['escrow_tax'] = np.where(df['ship_country'].isin(col), 0, df['escrow_tax'])

    # # 渠道名称替换
    # df_ship_code = get_yibai_logistics_logistics()
    # df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    # df['ship_name'] = df['ship_name'].fillna('')

    # 20260106 去掉精品账号的销库数据
    df_jp_account = get_jp_account()
    df_jp_account['is_jp'] = 1
    df = pd.merge(df, df_jp_account, how='left', on=['platform_code', 'account_id'])

    # df[~df['sales_status'].isin(['总计','','nan'])].to_excel('F://Desktop//df_order_temp.xlsx', index=0)

    df = df[df['is_jp'].isna()]
    df.drop('is_jp', axis=1, inplace=True)


    # # 4、净利率计算
    # df_order = get_platform_zero(df)
    # df_order["real_profit"] = df_order["true_profit_new1"] - df_order["差值"] * df_order["total_price"]
    # df_order["real_profit"] = df_order["real_profit"].astype(float)
    # df_order["real_profit"] = df_order["real_profit"].round(4)
    # df_order.drop(['差值2', '差值'], axis=1, inplace=True)

    # df_order_2 = tt_get_order_detail_temp()
    # df_order = pd.concat([df_order, df_order_2])

    df.to_excel('F://Desktop//tt_oversea_order_monitor_walmart3.xlsx', index=0)



def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')


    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()


def dashbord_order():
    date_start = '2026-03-01'
    date_end = '2026-10-01'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            -- and paytime < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('总计')
            and warehouse_name not like '%%精品%%'
            -- and platform_code in ('TEMU')


    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn_ck.ck_select_to_df(sql)
    # df_temp = conn.read_sql(sql)
    print(df_temp.info())

    df_temp.to_excel('F://Desktop//df_dashbord_order.xlsx', index=0)

    return df_temp

def tt_dashbord_order():
    date_start = '2026-02-01'
    date_end = '2026-10-01'
    sql = f"""
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, payment_time, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, release_money, sales_status
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '{date_start}'
            -- and payment_time < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and warehouse_name not like '%%精品%%'
            -- and platform_code in ('TEMU')


    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn_ck.ck_select_to_df(sql)
    # df_temp = conn.read_sql(sql)
    print(df_temp.info())

    df_temp.to_excel('F://Desktop//tt_df_dashbord_order.xlsx', index=0)

    return df_temp


def get_supplier_order_sku(org='YB'):
    """ 获取供应商订单实际配库sku """
    if org == 'YB':
        sql = """
        select DISTINCT
            d.order_id as order_id,b.sku as package_detail_sku,a.similar_sku as `配库sku`
        from yibai_oms_sync.yibai_oms_order_package_sku_allot_match_sup a
        join yibai_oms_sync.yibai_oms_order_package_detail b on b.id = a.package_detail_id and b.sku = a.merchant_sku
        join yibai_oms_sync.yibai_oms_order_package_relation d on d.package_id = a.package_id 
        where a.is_delete = 0 and a.use_status in (5) and d.is_delete = 0 
        """
        conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
    elif org == 'TT':
        sql = """
        select DISTINCT
            d.order_id as order_id,b.sku as package_detail_sku,a.similar_sku as `配库sku`
        from tt_oms_sync.tt_oms_order_package_sku_allot_match_sup a
        join tt_oms_sync.tt_oms_order_package_detail b on b.id = a.package_detail_id and b.sku = a.merchant_sku
        join tt_oms_sync.tt_oms_order_package_relation d on d.package_id = a.package_id 
        where a.is_delete = 0 and a.use_status in (5) and d.is_delete = 0 
        """
        conn_ck = pd_to_ck(database='tt_oms_sync', data_sys='通拓-新')
        df = conn_ck.ck_select_to_df(sql)

    df = df[['order_id', '配库sku']].drop_duplicates()

    return df

def get_dwm_sku():
    """ 获取库存信息 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT sku, warehouse, type, best_warehouse_name, available_stock, available_stock_money, date_id, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and type not in ('泛品铺货')
    """
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_jp_sku.xlsx', index=0)

    return df


def get_flash_order():
    """ 限时清仓sku订单监控 """

    # 1、获取限时清仓sku
    df_sku = get_flash_sku()
    df_sku = df_sku[['sku', 'warehouse', 'source']]
    print(df_sku.info())

    # 2、获取库存信息
    # df_stock = get_dwm_sku()
    #
    # col = ['sku','warehouse', 'available_stock_money']
    # df_stock = pd.merge(df_sku, df_stock[col], how='left', on=['sku','warehouse'])
    # df_stock['available_stock_money'] = df_stock['available_stock_money'].fillna(0).astype(float)
    # 2.1 库存变化监控

    # 3、获取订单
    df_order = dashbord_order()
    df_order['paytime'] = pd.to_datetime(df_order['paytime'], format="%Y-%m-%d")
    df_order['paytime'] = df_order['paytime'].dt.date
    df_order = pd.merge(df_order, df_sku, how='left', on=['sku','warehouse'])

    df_order.to_excel('F://Desktop//df_order_m.xlsx', index=0)

    print(df_order.info())

def get_dwm_sku_2():
    """ 获取库存信息 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT *
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_status.xlsx', index=0)

    return df

def get_center_zero():
    """ 中台差值表 """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')

    sql = """
        SELECT platform_code, shipping_type, first_product_line, cost_range, site, net_profit2, net_interest_rate_target
        FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
        WHERE shipping_type = 2 AND site in ('US','DE','other') AND is_del = 0
    """
    df_yb = conn_ck.ck_select_to_df(sql)

    conn_ck_tt = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    sql = """
        SELECT platform_code, shipping_type, first_product_line, cost_range, site, net_profit2, net_interest_rate_target
        FROM tt_sale_center_listing_sync.tt_listing_profit_config
        WHERE shipping_type = 2 AND site in ('US','DE','other') AND is_del = 0
    """
    df_tt = conn_ck_tt.ck_select_to_df(sql)

    # df_yb.to_excel('F://Desktop//df_yb.xlsx', index=0)
    df_tt.to_excel('F://Desktop//df_tt.xlsx', index=0)

def get_supplier_sku():
    """ 获取供应商sku """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
        SELECT YM_sku sku, warehouse, 1 as is_supplier_sku
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and warehouse != 'CN'
        
        UNION ALL
        
        SELECT YB_sku sku, warehouse, 0 as is_supplier_sku
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and warehouse != 'CN'
    """
    df = conn_ck.ck_select_to_df(sql)
    df = df[~df['sku'].isna()]
    df = df.drop_duplicates(subset=['sku','warehouse'])

    return df

# 根据开发来源筛选供应商sku
def get_sku_type():
    # 开发来源
    sql = f"""
            select a.sku `配库sku`,  develop_source_name
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
            INNER JOIN (
                SELECT distinct id as develop_source, develop_source_name
                FROM yibai_prod_base_sync.yibai_prod_develop_source
                WHERE develop_source_name = '供应商货盘'
            ) c on a.develop_source = c.develop_source

        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df.drop_duplicates(subset='配库sku')

    return df

# 供应商sku订单情况
def get_supplier_order():
    """
    1、获取订单明细
    2、获取供应商sku
    3、匹配供应商sku订单
    """
    # 1、海外仓订单
    df_order = dashbord_order()

    # 2、配库sku
    df_order_sku = get_supplier_order_sku()
    df_order = pd.merge(df_order, df_order_sku, how='left', on='order_id')
    df_order['配库sku'] = np.where(df_order['配库sku'].isna(), df_order['sku'], df_order['配库sku'])


    # 3、供应商sku
    df_sku = get_sku_type()

    df_order = df_order[df_order['配库sku'].isin(df_sku['配库sku'].unique())]
    df_order = pd.merge(df_order, df_sku[['配库sku','develop_source_name']], how='left', on=['配库sku'])

    df_order.to_excel('F://Desktop//df_sup_order.xlsx', index=0)


def get_sup_stock():
    # sql = """
    #     SELECT *
    #     FROM over_sea.dwd_supplier_sku_stock
    #     WHERE date_id = '2026-01-28' and warehouse = '美国仓'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)
    #
    # sql = """
    #     SELECT DISTINCT YM_sku, YB_sku
    #     FROM yibai_oversea.dwm_supplier_sku_price
    #     WHERE date_id = '2026-01-28'
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_sku = conn_ck.ck_select_to_df(sql)
    #
    # df = pd.merge(df, df_sku, how='left', on='YM_sku')

    # # 包含供应商sku
    # sql = """
    #     SELECT trimBoth(ps.sku) AS sku, ywc.name AS warehouse, yw.warehouse_name AS warehouse_name,
    #     yw.warehouse_code AS warehouse_code, yw.id AS warehouse_id, yw.warehouse_other_type AS warehouse_other_type,
    #     stock, available_stock,allot_on_way_count AS on_way_stock,wait_outbound,frozen_stock, cargo_owner_id
    #     FROM yb_datacenter.yb_stock AS ps
    #     INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
    #     LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    #     WHERE (ps.date_id = toYYYYMMDD(now())) AND (ps.cargo_owner_id = 8) AND (yw.warehouse_type IN (2,
    #     3)) AND (yw.warehouse_name NOT IN ('云仓美西仓', '云仓德国仓', 'HC美国西仓', '英国仓UK2(移仓换标)','云仓波兰仓',
    #     'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')) AND available_stock > 0
    #
    #     UNION ALL
    #
    #     SELECT trimBoth(ps.sku) AS sku, ywc.name AS warehouse, yw.warehouse_name AS warehouse_name,
    #     yw.warehouse_code AS warehouse_code, yw.id AS warehouse_id, yw.warehouse_other_type AS warehouse_other_type,
    #     stock, available_stock,allot_on_way_count AS on_way_stock,wait_outbound,frozen_stock, cargo_owner_id
    #     FROM yb_datacenter.yb_stock AS ps
    #     INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
    #     LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    #     WHERE
    #         (ps.date_id = toYYYYMMDD(now()))
    #         AND (ps.cargo_owner_id != 8)
    #         AND (yw.warehouse_type IN (2,3))
    #         AND (yw.warehouse_name NOT IN ('云仓美西仓', '云仓德国仓', 'HC美国西仓', '英国仓UK2(移仓换标)',
    #         '云仓波兰仓','JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓'))
    #         AND sku in (
    #             SELECT distinct sku
    #             FROM yibai_prod_base_sync.yibai_prod_sku_pallet_price
    #             WHERE is_del = 0 and sku like '%YM%'
    #         )
    #         AND available_stock > 0
    # """
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    #
    # # 成本
    # df = get_sku_cost(df)

    sql = """
        SELECT distinct sku
        FROM yibai_prod_base_sync.yibai_prod_sku_pallet_price
        WHERE is_del = 0 and sku like '%YM%'
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sup_sku = conn_ck.ck_select_to_df(sql)
    df_sup_sku.to_excel('F://Desktop//df_sup_sku.xlsx', index=0)

    # df.to_excel('F://Desktop//df_sup_stock2.xlsx', index=0)

def get_sku_cost(df):
    """
    获取tt的sku
    """
    sql = """
    SELECT 
        sku, title_cn `产品名称`, develop_source, b.develop_source_name,
        CASE 
            when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
            when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
            else toFloat64(product_cost) 
        END as `new_price`
    FROM yibai_prod_base_sync.yibai_prod_sku a
    LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source b
    ON a.develop_source = b.id
    -- where 
        -- develop_source in (14, 15, 22)
        -- or title_cn like '%通拓%' 
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)

    df_sku = df_sku.drop_duplicates(subset=['sku'], keep='first')
    df = pd.merge(df, df_sku[['sku','new_price']], how='left', on=['sku'])

    return df

def check_temu_order():
    """ 检查temu订单 """


## =====================
# 订单运费与调价运费核对
def get_yibai_logistics_logistics():
    """  渠道名 """
    sql = """
        SELECT distinct ship_code, ship_name
        FROM yibai_tms_logistics_sync.yibai_logistics_logistics
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # df.to_excel('F://Desktop//df_logistics.xlsx', index=0)
    return df


# 复制自oversea_price_adjust_2023
def get_transport_fee():
    """
    获取当前最新运费数据
    """
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost_origin,
        firstCarrierCost, dutyCost,  available_stock, shippingCost, 
        (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price,
        case when platform = 'OZON' then 'Wildberries' 
        when platform = 'DMSOzon' then 'Wildberries' else platform end as platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH')
 
    UNION ALL
    
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost_origin,
        firstCarrierCost, dutyCost, 0 available_stock, shippingCost, 
        (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name, 0 lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_temu
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # 东南亚有一版接口拉取的运费数据。需要去重
    df = df.sort_values(by='lowest_price', ascending=False).drop_duplicates(
        subset=['sku', 'warehouse_id', 'platform', 'country'])
    # print(df.info())
    # 20250305 ALLEGRO平台运费临时处理
    df_alle = df[(df['platform'] == 'AMAZON') & (df['warehouse'].isin(['德国仓', '法国仓'])) &
                 (df['country'].isin(['PL', 'CZ', 'SK', 'HU']))]
    df_alle['platform'] = 'ALLEGRO'
    df = pd.concat([df, df_alle])

    # 20260202 运费取大仓下有库存子仓最便宜的渠道
    df = df[~df['warehouse_name'].str.contains('精品')]
    df_1 = df[df['available_stock'] > 0]
    df_1 = df_1.sort_values(by='ship_fee', ascending=True)
    df_1 = df_1.drop_duplicates(subset=['sku', 'warehouse', 'platform', 'country'], keep='first')
    df_2 = df[df['available_stock'] <= 0]
    df_2 = df_2.sort_values(by='ship_fee', ascending=True)
    df_2 = df_2.drop_duplicates(subset=['sku', 'warehouse', 'platform', 'country'], keep='first')
    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['sku', 'warehouse', 'platform', 'country'], keep='first')
    # df.drop(['totalCost_origin','firstCarrierCost','dutyCost','available_stock'], axis=1, inplace=True)
    df.drop(['available_stock'], axis=1, inplace=True)
    df = df.rename(columns={'warehouse_name': 'best_fee_warehouse', 'warehouse_id': 'best_fee_id'})

    df['best_fee_id'] = df['best_fee_id'].astype(int)


    return df


def get_real_warehouse_name():
    """ 获取真实子仓名 """
    sql = """
        SELECT 
            yw.id AS warehouse_id, a.real_warehouse_id real_warehouse_id, 
            if(empty(b.name), a.name, b.name) real_warehouse_name,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
            yw.country, ebay_category_id,
            ywc.name AS warehouse, yw.warehouse_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yb_datacenter.yb_warehouse a on yw.id = a.id
        LEFT JOIN yb_datacenter.yb_warehouse b on a.real_warehouse_id = b.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df.drop_duplicates(subset=['warehouse_id'])
    # df.to_excel('F://Desktop//yb_warehouse.xlsx', index=0)

    return df


def group_order(df):
    """ 指标聚合数据 """

def cut_bins(df):
    """ """
    df['运费偏差率分段'] = pd.cut(
        df['运费偏差率'], bins=[-1, -0.8, -0.5, -0.2, -0.05, 0.05, 0.2, 0.5, 1, 1000],
        labels=['A:<-0.8', 'B:(-0.8,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.05]','E:(-0.05, 0.05]',
                'F:(0.05,0.2]', 'G:(0.2,0.5]', 'H:(0.5,1]', 'I:>1'])
    df['毛利侵蚀率分段'] = pd.cut(
        df['毛利侵蚀率'], bins=[-1, -0.2, -0.1, -0.05, -0.01, 0.01, 0.05, 0.1, 0.2, 10],
        labels=['A:<-0.2', 'B:(-0.2,-0.1]', 'C:(-0.1,-0.05]', 'D:(-0.05,-0.01]','E:(-0.01, 0.01]',
                'F:(0.01,0.05]', 'G:(0.05,0.1]', 'H:(0.1,0.2]', 'I:>0.2'])

    return df

def check_order_fee():
    """ 订单尾程与调价尾程对比 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
    sql = f"""
        SELECT order_id, platform_code platform, sku, seller_sku, item_id, account_id, payment_time,  total_price,
        true_profit_new1, warehouse_id, warehouse, sales_status, ship_country country, ship_code, quantity, shipping_fee
        FROM over_sea.oversea_order_monitor
        WHERE payment_time >= '{date_start}'
        and payment_time < '{date_today}'
        and warehouse_name not like '%%精品%%'
        and total_price > 1  -- 剔除异常订单
        and shipping_fee > 1
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df['country'] = df['country'].replace({'GB':'UK'})
    df['payment_time'] = pd.to_datetime(df['payment_time']).dt.date
    print('订单获取完成')
    # 替换真实子仓
    df_warehouse = get_real_warehouse_name()
    df = pd.merge(df, df_warehouse[['warehouse_id', 'real_warehouse_name']], how='left', on=['warehouse_id'])
    # 获取尾程渠道
    df_ship_code = get_yibai_logistics_logistics()
    df = pd.merge(df, df_ship_code, how='left', on=['ship_code'])
    df['ship_name'] = df['ship_name'].fillna('')
    df = df[~df['ship_name'].str.contains('VC')]  # 剔除VC订单

    # 获取调价尾程信息（子仓、渠道、运费）
    df_useful_fee = get_transport_fee()
    print('运费获取完成')
    df_useful_fee = df_useful_fee.rename(columns={'best_fee_id':'warehouse_id'})
    df_useful_fee = pd.merge(df_useful_fee, df_warehouse[['warehouse_id', 'real_warehouse_name']], how='left', on=['warehouse_id'])
    col = ['sku','warehouse', 'platform','country', 'real_warehouse_name','shippingCost','ship_fee','ship_name']
    df = pd.merge(df, df_useful_fee[col], how='left', on=['sku','warehouse', 'platform', 'country'])

    # 指标计算
    df['是否有调价运费'] = np.where(df['real_warehouse_name_y'].isna(), 0, 1)
    df['子仓一致性'] = np.where(df['real_warehouse_name_x'] == df['real_warehouse_name_y'], 1, 0)
    df['渠道一致性'] = np.where(df['ship_name_y'] == df['ship_name_x'], 1, 0)
    df['尾程占比销售额'] = df['shipping_fee']/df['total_price']
    df['运费偏差率'] = df['shipping_fee']/df['shippingCost'] - 1
    df['毛利侵蚀率'] = (df['shipping_fee'] - df['shippingCost'])/df['total_price']

    # 分段
    df = cut_bins(df)

    # 缩销国家范围
    country_list = ['US', 'DE', 'UK', 'AU', 'CA', 'MX']
    df = df[df['country'].isin(country_list)]
    # df.to_excel('F://Desktop//df_order_monitor.xlsx', index=0)


    # # 仅更新近1个月订单
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = f"delete from over_sea.oversea_shipfee_monitor where payment_time >= '{date_start}' "
    # conn.execute(sql)
    # conn.close()

    # 存表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_shipfee_monitor', if_exists='replace')
    conn.close()




# 获取转泛品sku的链接（amazon）
def temp_sku():
    """ 转泛品sku """
    sql = """
        SELECT sku
        FROM yibai_oversea.temp_oversea_jp_to_fp_sku
    """
    conn_ck = pd_to_ck(database='yibai_ovesea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    sku_list = tuple(df_sku['sku'].unique())

    # 2、链接
    sql = f"""
        SELECT sku, seller_sku, account_id, account_name, owner_name, saler_name, deliver_mode
        FROM yibai_product.yibai_amazon_sku_map 
        WHERE sku in {sku_list}
    """
    conn = connect_to_sql(database='yibai_product', data_sys='AMAZON刊登库')
    df_listing = conn.read_sql(sql)

    sql = """
        SELECT account_name, short_name, b.account_type, b.is_yibai, b.group_id, c.group_name
        FROM yibai_sale_center_system_sync.yibai_system_account a
        LEFT JOIN (
            select distinct toInt32(account_id) account_id, account_type, is_yibai, group_id
            from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1
        ) b on a.account_id = b.account_id
        LEFT JOIN (
            select group_id, group_name
            from yibai_system_kd_sync.yibai_amazon_group
        ) c on b.group_id = c.group_id
        WHERE platform_code = 'AMAZON'
    """
    df_account = conn_ck.ck_select_to_df(sql)

    df_listing = pd.merge(df_listing, df_account, how='left', on=['account_name'])

    print(df_listing.info())

    df_listing.to_excel('F://Desktop//df_listing_2.xlsx', index=0)


def temp_temp():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    sql = f"""
        select
            distinct sku, title_cn, product_status, `sku改在售时间`, new_price, `重量`, `长`, `宽`, `高`
        from (
            SELECT 
                distinct sku, title_cn, product_status, toDate(b.end_time) as `sku改在售时间`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`,
                case 
                    when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
                    else toFloat64(weight_out_storage) 
                end as `重量`,
                toFloat64(pur_length_pack) as `长`,
                toFloat64(pur_width_pack) as `宽`,
                toFloat64(pur_height_pack) as `高`
            FROM yibai_prod_base_sync.yibai_prod_sku a
            LEFT JOIN yibai_prod_base_sync.yibai_prod_spu b
            ON a.spu=b.spu
        ) a
        INNER JOIN (
            SELECT DISTINCT sku
            FROM yibai_prod_base_sync.yibai_prod_sku_select_attr
            where attr_value_id = 67
            ) b
        ON a.sku = b.sku
        WHERE 1 = 1
        and sku = 'YX01785'
        -- a.new_price < 50 
        -- and `重量` < 450 
        -- and `长` < 40 and `宽` < 40 and `高` < 40 
        -- and a.new_price > 0
        """
    df = conn_mx.ck_select_to_df(sql)

    df_temp = pd.read_excel('F://Ding_workspace//低价商城热销品汇总sku匹配0319.xlsx', dtype={'sku':str})
    print(df_temp.info())
    sku_list = tuple(df_temp['sku'].unique())

    df = df[df['sku'].isin(sku_list)]
    print(df)


def get_account_group():
    """小组大部"""
    sql = """
        select distinct a.account_id account_id, c.group_name as group_name, e.name as dep_name
        from yibai_sale_center_system_sync.yibai_system_account as a
        left join yibai_sale_center_common_sync.yibai_common_account_config as b
        on b.account_id = a.id and b.platform_code='AMAZON'
        left join (
            SELECT a.id as id,
                case when a.level = 2  then concat(a.group_name,'/',b.group_name) else a.group_name end as group_name,
                case when a.level = 2  then b.department_id else a.department_id end as department_id
            from yibai_sale_center_common_sync.yibai_common_account_group a
            left join yibai_sale_center_common_sync.yibai_common_account_group b on a.pid = b.id
            where a.platform_code='AMAZON' 
        ) c
        on toString(c.id) = toString(b.account_group_id)
        left join yibai_sale_center_common_sync.yibai_common_department_config as e
        on e.id = c.department_id and e.platform_code='AMAZON' and e.is_del=0
        where a.platform_code='AMAZON'
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df_group = conn_ck.ck_select_to_df(sql)

    df_group.to_excel('F://Desktop//df_group.xlsx', index=0)


if __name__ == '__main__':
    # main()
    # get_dwm_sku()
    # get_flash_order()
    # get_center_zero()
    # get_supplier_order()
    # get_sup_stock()
    # get_order_detail()
    # get_supplier_order_sku()

    # check_order_fee()

    # dashbord_order()

    # tt_dashbord_order()
    # temp_sku()
    # get_order_detail_temp()
    # tt_get_order_detail_temp()
    # temp_temp()

    # get_order_detail_fba()
    # tt_get_order_detail_dom()
    generate_platform_summary()

    # get_account_group()

