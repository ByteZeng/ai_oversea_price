"""
时间：2023年11月28日
背景：Amazon、walmart正常品订单利润率偏低
统计海外仓各平台，【正常】品链接的调价准确率：
1、统计海外仓近三个月销售数据，透视各平台每周的毛利润率和净利润率变化
2、各平台近一个月出单的链接，统计其调价准确率
3、各平台全量链接，调价覆盖率、调价准确率的统计
"""
##
import datetime,time
import pandas as pd
import numpy as np
from utils.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd, connect_to_sql
from pulic_func.base_api.base_function import mysql_escape
# from all_auto_task.scripts_ck_client import CkClient
##
##
ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                     db_name='over_sea')
date_start = '2023-08-01'
date_end = datetime.datetime.today().strftime('%Y-%m-%d')
# date_end = '2023-11-28'
# 总计
sql = f"""
    select
        order_id, sku, seller_sku, account_id,
        platform_code,
        sales_status `销售状态`,
        paytime,
        release_money,
        true_profit_new1,
        real_profit,
        total_price,
        quantity,
        round(100 * true_profit_new1 /total_price, 2) as `订单毛利润率`,
        round(100 * real_profit / total_price, 2) as `订单净利润率`
    FROM
        over_sea.dashbord_new1
    WHERE 
        paytime >= \'{date_start}\'
        and paytime <= \'{date_end}\'
        and `total_price` > 0 
        and `sales_status` not in ('','nan','总计')
        -- and sales_status IN ('总计', '负利润加快动销')

"""
df_sales = ck_client.ck_select_to_df(sql)

##
df_sales.to_excel('df_sales.xlsx', index=0)

##
# 调价覆盖率，及调价准确率
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT 
        account_id, short_name, country,status,seller_sku,asin,online_price,best_warehouse_name,warehouse,new_price,total_cost,
        available_stock,day_sales,sales_status,price,target_profit_rate,`站点`,rate,ppve,platform_zero,platform_must_percent,is_uk_cdt,is_normal_cdt,
        is_small_diff,is_white_account,is_white_listing,is_fba_asin,date_id
    FROM over_sea.oversea_amazon_listing_all 
    WHERE date_id = '2023-12-20'
"""
df_amazon_listing_y = conn.read_sql(sql)
##
sql = """
    SELECT account_id, seller_sku, online_price as online_price_t
    FROM over_sea.yibai_amazon_oversea_listing_price 
    WHERE DATE = '2023-12-21'
"""
df_amazon_listing_t = conn.read_sql(sql)
##
df_amazon_listing_t['account_id'] = df_amazon_listing_t['account_id'].astype(int)
df_amazon_listing_y = pd.merge(df_amazon_listing_y, df_amazon_listing_t, how='left', on=['account_id', 'seller_sku'])
##
df_amazon_listing_y['price_diff'] = df_amazon_listing_y['price'] / df_amazon_listing_y['online_price_t'] - 1
##
df_amazon_listing_y['is_same'] = np.where(df_amazon_listing_y['price_diff'].abs() < 0.05, 1, 0)
##
df_amazon_listing_y[df_amazon_listing_y['is_same'] == 0].to_csv('df_amazon_listing_y.csv', index=0)
##

conn.close()
##
def delete_date(table_name):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT distinct date_id
        FROM over_sea.{table_name}
        ORDER BY date_id DESC
    """
    df_temp = conn.read_sql(sql)
    if len(df_temp)>2:
        df_date = df_temp.iloc[2,0]
    else:
        df_date = df_temp.min()
    sql = f"""
        delete from over_sea.{table_name} where date_id < '{df_date}'
    """
    conn.execute(sql)

##
# 调价成功率
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT account_id, seller_sku, sku, country, online_price, price, warehouse, available_stock,sales_status, date_id
    FROM over_sea.oversea_amazon_listing_upload_temp
    WHERE date_id = '2023-11-22'
"""
df_adjust_price = conn.read_sql(sql)
#
account_list = tuple(df_adjust_price['account_id'].unique())
sql = f"""
    SELECT account_id, seller_sku, online_price
    FROM over_sea.yibai_amazon_oversea_listing_price
    WHERE `DATE` = '2023-11-23' and account_id in {account_list}
"""
df_listing_price = conn.read_sql(sql)
#
df_listing_price['account_id'] = df_listing_price['account_id'].astype(int)
df_price = pd.merge(df_adjust_price, df_listing_price, how='inner',on=['account_id', 'seller_sku'])
#
df_price['price_diff'] = df_price['price'] / df_price['online_price_y'] - 1
df_price['是否匹配'] = np.where(df_price['price_diff'].abs()<= 0.05, 1, 0)
#
print(len(df_price[df_price['是否匹配']==1])/len(df_price))
##
# 出单价准确率
def get_rate(platform='amazon'):
    """获取各国家的汇率"""

    sql = f"""
    SELECT id account_id, upper(site) as country, b.rate rate
    FROM yibai_system_kd_sync.yibai_{platform}_account a
    LEFT JOIN (
        SELECT distinct case when country='ES' then 'SP' else country end as country, from_currency_code as charge_currency, rate
        FROM domestic_warehouse_clear.erp_rate
        WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    ) b
    ON upper(a.site) = b.country
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate['country'] = df_rate['country'].replace('SP', 'ES')
    return df_rate
##
df_rate = get_rate()
##
# df_sales.drop(['country','rate'], axis=1, inplace=True)
df_amazon_sales = pd.merge(df_sales[(df_sales['platform_code']=='AMAZON') & (df_sales['销售状态']=='正常')], df_rate, how='left', on=['account_id'])
df_amazon_sales['order_price'] = df_amazon_sales['total_price']/df_amazon_sales['quantity']/df_amazon_sales['rate']
#
col_list = ['account_id','seller_sku','online_price','price','sales_status','platform_zero','platform_must_percent','available_stock','best_warehouse_name','is_uk_cdt',
            'is_normal_cdt','is_small_diff','is_white_account','is_white_listing','is_fba_asin']
df_amazon_sales = pd.merge(df_amazon_sales, df_amazon_listing_y[col_list], how='left', on=['account_id','seller_sku'])
##
df_amazon_sales.to_excel('df_amazon_sales.xlsx',index=0)
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT 
        sku, best_warehouse_name, warehouse, available_stock, date_id
    FROM over_sea.dwm_sku_temp_info 
    WHERE date_id = '2023-11-28'
"""
df_sku_info = conn.read_sql(sql)
##
country_list = {'美国仓':'US','澳洲仓':'AU','德国仓':'DE','英国仓':'UK','墨西哥仓':'MX'}
df_sku_info['country'] = df_sku_info['warehouse'].replace(country_list)
##
df_amazon_sales = pd.merge(df_amazon_sales, df_sku_info, how='left', on=['sku', 'country'])
##
df_amazon_sales.to_excel('df_amazon_sales.xlsx',index=0)
def test_fee():
    # 测试
    sql = """
    SELECT *
    FROM dwm_sku_temp_info
    WHERE date_id = curdate() and sku = 'US-JY09180-01'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku = conn.read_sql(sql)
    ##
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, 
        shipName as ship_name,lowest_price, platform, country
    FROM oversea_transport_fee_useful
    WHERE sku = 'US-JY09180-01'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    ##
    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_rate = ck_client.ck_select_to_df(sql)
    ##
    df_fee = pd.merge(df_fee, df_rate[['country', 'rate']], how='left', on='country')
    df_fee = df_fee[
        ['sku', 'warehouse_id', 'ship_name', 'total_cost', 'lowest_price', 'platform', 'country',
         'rate']].drop_duplicates()

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate']

    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_fee, how='inner',
                             left_on=['sku', 'best_warehouse_id'], right_on=['sku', 'warehouse_id'])
    ##

    sql = """
        SELECT distinct warehouse_name, warehouse_id
        FROM yb_datacenter.v_oversea_stock
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_warehouse = ck_client.ck_select_to_df(sql)
    ##
    sql = """
        SELECT distinct warehouse_name
        FROM over_sea.yibai_toucheng
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_warehouse_tc = conn.read_sql(sql)
    ##
    df_warehouse[~df_warehouse['warehouse_name'].isin(df_warehouse_tc['warehouse_name'].unique())].to_excel('temp.xlsx',index=0)
    ##
    df_new_warehouse = pd.read_excel('temp.xlsx')
    ##
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_new_warehouse, 'yibai_toucheng', if_exists='append')

##

# ebay
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT 
        account_id, sku,item_id,online_price,best_warehouse_name,warehouse,new_price,total_cost,
        available_stock,day_sales,sales_status,price,target_profit_rate,rate,ppve,platform_zero,platform_must_percent,is_uk_cdt,is_normal_cdt,
        is_small_diff,is_white_account,is_90_overage,date_id
    FROM over_sea.oversea_ebay_listing_all 
    WHERE date_id = '2023-12-20'
"""
df_ebay_listing_y = conn.read_sql(sql)
##
sql = """
    SELECT account_id, item_id, sku, online_price as online_price_t, country
    FROM over_sea.yibai_ebay_oversea_listing_price 
    WHERE DATE = '2023-12-21'
"""
df_ebay_listing_t = conn.read_sql(sql)
##
sql = """
    SELECT account_id, item_id, sku
    FROM over_sea.yibai_ebay_oversea_listing_price 
    WHERE DATE = '2023-12-20'
"""
df_ebay_listing_t_2 = conn.read_sql(sql)
##
df_ebay_listing_t = pd.merge(df_ebay_listing_t, df_ebay_listing_t_2, how='inner', on=['account_id','item_id','sku'])

##
df_ebay_listing_t = pd.merge(df_ebay_listing_t, df_ebay_listing_y, how='left', on=['account_id', 'item_id','sku'])
##
sql = """
    select distinct sku, warehouse, 1 as is_90_overage_sku
    from dwm_sku_temp_info 
    WHERE age_90_plus = 0 and available_stock > 0 and date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
    """
df_overage_90 = conn.read_sql(sql)
##
df_ebay_listing_t = pd.merge(df_ebay_listing_t, df_overage_90[['sku','is_90_overage_sku']], how='left', on=['sku'])
df_ebay_listing_t['is_90_overage_sku'] = df_ebay_listing_t['is_90_overage_sku'].fillna(0).astype(int)
##
df_ebay_listing_t['price_diff'] = df_ebay_listing_t['online_price'] / df_ebay_listing_t['online_price_t'] - 1
##
df_ebay_listing_t['is_same'] = np.where(df_ebay_listing_t['price_diff'].abs() < 0.05, 1, 0)
##
df_ebay_listing_t.to_excel('df_ebay.xlsx', index=0)
##
# walmart
def get_sku_tuple(conn):
    D_T = (datetime.date.today() - datetime.timedelta(7)).isoformat()
    sql = f"""
    select distinct sku from dwm_sku_temp_info 
    WHERE warehouse in ('美国仓', '加拿大仓') AND date_id>='{D_T}'
    """
    df = conn.read_sql(sql)
    # sku_tuple = tuple(df["sku"])
    return df
def write_data(conn):
    df = get_sku_tuple(conn)
    df_all = pd.DataFrame()
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m/1000))
    conn_mx = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_sale_center_listing_sync')
    # conn_mx = pd_to_ck(database='temp_database_hxx', data_sys='调价明细历史数据')

    for key, group in df.groupby(['index']):
        sku_list = mysql_escape(group, 'sku')
        sql = f"""
            SELECT 
                a.erp_id as account_id,b.short_name as short_name,
                case 
                    when b.site='us_dsv' then 'us' 
                    else b.site 
                end as site, 
                a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
            FROM (
                select * from yibai_sale_center_listing_sync.yibai_walmart_report 
                where upper(publish_status)='PUBLISHED' and sku in ({sku_list}) 
                order by updated_unix desc limit 1 by erp_id,seller_sku
            ) a
            left join yibai_system_kd_sync.yibai_walmart_account b 
            on a.erp_id=b.id
            WHERE b.warehouse_delivery<>4 and b.status=1 and b.site<>'us_dsv'
        """
        df = conn_mx.ck_select_to_df(sql)
        df["DATE"] = datetime.date.today().isoformat()
        df_all = pd.concat([df, df_all])
    print('抓链接完成')
    print(df_all.info())
    # 2023-06-15 剔除平台仓链接
    # 2023-07-28 更新剔除方式，按item_id剔除
    sql = """
        SELECT distinct toString(item_id) item_id, 1 as is_platform_cdt
        FROM yibai_product_kd_sync.yibai_walmart_wfs_inventory_health_report
    """
    df_temp = conn_mx.ck_select_to_df(sql)
    df_all = pd.merge(df_all, df_temp, how='left', on='item_id')
    df_all['is_platform_cdt'] = df_all['is_platform_cdt'].fillna(0)
    # df_all =pd.concat([df_all,df_temp,df_temp]).drop_duplicates(subset=['item_id'],keep=False)
    df_all.drop('item_id', axis=1, inplace=True)
    print(df_all.info())

    return df_all
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_walmart_listing_all = write_data(conn)
##
# df_walmart_listing_all_2 = write_data(conn)
##
df_walmart_listing_all.is_platform_cdt.value_counts()
##
sql = f"""
    SELECT 
        SKU as sku, best_warehouse_name, warehouse, available_stock,sales_status,price target_price,lower(country) as site
    FROM dwm_oversea_price_dtl
    WHERE platform = 'WALMART' and date_id = '2023-12-21'
"""
df_walmart_dtl = conn.read_sql(sql)
##
df_walmart = pd.merge(df_walmart_listing_all, df_walmart_dtl, how='left',on=['sku', 'site'])
##
df_walmart['target_price'] = df_walmart['target_price'].astype(float)
df_walmart['price'] = df_walmart['price'].astype(float)
df_walmart['price_diff'] = df_walmart['target_price'] / df_walmart['price'] - 1
df_walmart['is_same'] = np.where(df_walmart['price_diff'].abs() < 0.05, 1, 0)
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT 
        account_id, short_name, country,seller_sku,item_id,online_price,best_warehouse_name,warehouse,is_normal_cdt,
        is_platform_cdt,is_ca, is_small_diff,is_white_account,is_white_listing,date_id
    FROM over_sea.oversea_walmart_listing_all 
    WHERE date_id = '2023-12-21'
"""
df_walmart_listing_y = conn.read_sql(sql)
##
df_walmart = pd.merge(df_walmart, df_walmart_listing_y, how='left', on=['account_id', 'seller_sku'])
# df_walmart_listing_y.to_excel('df_walmart_listing_y.xlsx', index=0)
##
df_walmart.to_excel('df_walmart.xlsx', index=0)
##
sql = """
    SELECT account_id, seller_sku, price as online_price_t
    FROM over_sea.yibai_walmart_oversea_listing_price 
    WHERE DATE = '2023-12-22'
"""
df_walmart_listing_t = conn.read_sql(sql)
##
df_walmart_listing_t['account_id'] = df_walmart_listing_t['account_id'].astype(int)
df_walmart_listing_y = pd.merge(df_walmart_listing_y, df_walmart_listing_t, how='left', on=['account_id', 'seller_sku'])
df_walmart_listing_y['price_diff'] = df_walmart_listing_y['price'] / df_walmart_listing_y['online_price_t'] - 1
# df_walmart_listing_y.to_csv('df_walmart_listing_y.csv', index=0)
##
# 调价成功率
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT account_id, seller_sku, sku, country, online_price, price, warehouse, available_stock,sales_status, date_id
    FROM over_sea.oversea_cdiscount_listing_upload_temp
    WHERE date_id = '2023-12-19'
"""
df_adjust_price = conn.read_sql(sql)
#
account_list = tuple(df_adjust_price['account_id'].unique())
sql = f"""
    SELECT account_id, seller_sku, sku, online_price
    FROM over_sea.yibai_cdiscount_oversea_listing_price
    WHERE `DATE` = '2023-12-20' and account_id in {account_list}
"""
df_listing_price = conn.read_sql(sql)
#
df_listing_price['account_id'] = df_listing_price['account_id'].astype(int)
df_adjust_price['account_id'] = df_adjust_price['account_id'].astype(int)
df_price = pd.merge(df_adjust_price, df_listing_price, how='inner',on=['account_id', 'seller_sku', 'sku'])
#
df_price['price_diff'] = df_price['price'] / df_price['online_price_y'] - 1
df_price['是否匹配'] = np.where(df_price['price_diff'].abs()<= 0.05, 1, 0)
#
print(len(df_price[df_price['是否匹配']==1])/len(df_price))
##

##
df_rate = get_rate('walmart')
##
df_rate.to_excel('df_rate_walmart.xlsx', index=0)
##
df_walmart_sales = pd.merge(df_sales[(df_sales['platform_code']=='WALMART') & (df_sales['销售状态']=='正常')], df_rate, how='left', on=['account_id'])
df_walmart_sales['order_price'] = df_walmart_sales['total_price']/df_walmart_sales['quantity']/df_walmart_sales['rate']
#
col_list = ['account_id','seller_sku','online_price','price','sales_status','platform_zero','platform_must_percent','available_stock','best_warehouse_name','is_normal_cdt',
        'is_platform_cdt','is_ca', 'is_small_diff','is_white_account','is_white_listing']
df_walmart_sales = pd.merge(df_walmart_sales, df_walmart_listing_y[col_list], how='left', on=['account_id','seller_sku'])
##
df_walmart_sales = pd.merge(df_walmart_sales, df_walmart_listing_all, how='left', on=['account_id','seller_sku'])
##
df_walmart_sales.to_excel('df_walmart_sales.xlsx', index=0)
##
date_today = datetime.datetime.today()
# table_now = f'pricing_ratio_mx_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
table_name = f'pricing_ratio_mx_{date_today.year:04d}{date_today.month:02d}{date_today.day:02d}'
##
# ebay
def insert_sku_to_sku_messige(conn2):
    sql = f"""
        select distinct sku 
        from dwm_sku_temp_info 
        WHERE date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    df = conn2.read_sql(sql)
    return df
def write_data(conn2):
    df_f = insert_sku_to_sku_messige(conn2)
    df_all = pd.DataFrame()

    df_f = df_f.reset_index(drop=True)
    df_f['index'] = df_f.index
    df_f['index'] = df_f['index'].apply(lambda m: int(m / 1000))
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_product_kd_sync')
    # conn = connect_to_sql(database='yibai_product', data_sys='ebay刊登库')
    for key, group in df_f.groupby(['index']):
        sku_list = mysql_escape(group, 'sku')
        sql = f"""
            with listing_table as (
                select distinct itemid 
                from yibai_product_kd_sync.yibai_ebay_online_listing 
                where listing_status='Active' and listing_type <> 'Chinese' and variation_multi=0 and sku in ({sku_list})
            )
            
            select 
                a.account_id, a.itemid as item_id, a.sku, a.system_sku, a.sell_sku, a.site,
            CASE
                WHEN a.site='Germany' then 'DE'
                WHEN a.site='Spain' then 'ES'
                WHEN a.site='Australia' then 'AU'
                WHEN a.site='France' then 'FR'
                WHEN a.site='Canada' then 'CA'
                WHEN a.site='Italy' then 'IT'
                WHEN a.site='eBayMotors' then 'US'
                ELSE a.site 
                END as country, a.seller_user, a.product_line, a.listing_status, a.start_price, 
                b.shipping_service_cost as shipping_fee,a.start_price+b.shipping_service_cost as online_price,
            CASE 
                WHEN e.name in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' 
                ELSE e.name 
                end AS name, formatDateTime(NOW(),'%Y-%m-%d') as DATE
            from (
                SELECT 
                    account_id, itemid, sku, system_sku, sell_sku,site,seller_user, product_line, listing_status, start_price,
                    listing_status, listing_type, variation_multi, location
                FROM yibai_product_kd_sync.yibai_ebay_online_listing 
                WHERE listing_status='Active' and listing_type <> 'Chinese' and variation_multi=0 and sku in ({sku_list})
                ) a
            left join (
                SELECT item_id, shipping_service_cost,shipping_service_priority,shipping_status
                FROM yibai_product_kd_sync.yibai_ebay_online_listing_shipping 
                WHERE item_id in (SELECT itemid FROM listing_table)
                ) b on a.itemid=b.item_id
            left join 
                yibai_product_kd_sync.yibai_ebay_location_map_warehouse d 
            on a.location=d.location
            left join 
                yibai_product_kd_sync.yibai_ebay_warehouse_warehouse_category e 
            on d.warehouse_category_id=e.id
            where 
                a.listing_status='Active' and a.listing_type <> 'Chinese' and b.shipping_status=1 and b.shipping_service_priority=1 and 
                e.name  not in ('中国仓','临时仓') and a.variation_multi=0 and sku in ({sku_list})
            LIMIT 1000
            settings max_memory_usage = 20000000000
            """
        df = ck_client.ck_select_to_df(sql)
        sql = f"""
            with listing_table as (
                select distinct itemid 
                from yibai_product_kd_sync.yibai_ebay_online_listing 
                where listing_status='Active' and listing_type <> 'Chinese' and variation_multi=0 and sku in ({sku_list})
            )
            
            select
                b.account_id, a.item_id, a.sku, b.system_sku, a.sell_sku, site,
            CASE
                WHEN site='Germany' then 'DE'
                WHEN site='Spain' then 'ES'
                WHEN site='Australia' then 'AU'
                WHEN site='France' then 'FR'
                WHEN site='Canada' then 'CA'
                WHEN site='Italy' then 'IT'
                WHEN site='eBayMotors' then 'US'
                ELSE site
                END as country, b.seller_user, b.product_line, b.listing_status, a.start_price,
                c.shipping_service_cost as shipping_fee,a.start_price+c.shipping_service_cost as online_price,
            CASE WHEN e.name in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓'
                ELSE e.name
                end AS name, formatDateTime(NOW(),'%Y-%m-%d') as DATE
            from
                yibai_ebay_online_listing_variation a
            left join (
                SELECT account_id, itemid, system_sku, seller_user, listing_status, listing_type, location, product_line, site
                FROM yibai_ebay_online_listing
                WHERE itemid in (SELECT distinct item_id FROM yibai_ebay_online_listing_variation) and itemid in (SELECT itemid FROM listing_table)
                ) b on a.item_id=b.itemid
            left join (
                SELECT item_id, shipping_service_cost,shipping_service_priority,shipping_status
                FROM yibai_product_kd_sync.yibai_ebay_online_listing_shipping 
                WHERE item_id in (SELECT itemid FROM listing_table)
                ) c on a.item_id=c.item_id
            left join
                yibai_product_kd_sync.yibai_ebay_location_map_warehouse d on b.location=d.location
            left join
                yibai_product_kd_sync.yibai_ebay_warehouse_warehouse_category e on d.warehouse_category_id=e.id
            where
                b.listing_status='Active' and b.listing_type <> 'Chinese' and c.shipping_status=1 and shipping_service_priority=1
                and e.name not in ('中国仓','临时仓') and sku in ({sku_list})
                LIMIT 1000
                settings max_memory_usage = 20000000000
            """
        df2 = ck_client.ck_select_to_df(sql)
        df = pd.concat([df, df2])
        df_all = pd.concat([df, df_all])
    print(df_all.info())
    # 20220629剔除折扣链接
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    sql_discount = f"""
        SELECT 
            item_id 
        FROM `yibai_ebay_discount_status_analysis` 
        where 
            (
            (status=1 and sale_start_time<={date_today} and sale_end_time>={date_today})
            or (status=2)
            or ((status=1 and need_task=10 and sale_end_time<={date_today})
            or (status=2 and next_cycle_time>0)))
    """
    df_ebay_discount = ck_client.ck_select_to_df(sql_discount)
    df_ebay_discount['折扣'] = 1
    df_all['item_id'] = df_all['item_id'].astype('int')
    df_ebay_discount['item_id'] = df_ebay_discount['item_id'].astype('int')
    df_all = df_all.merge(df_ebay_discount, on=['item_id'], how='left')
    df_all.columns = [i.split('.')[-1] for i in df_all.columns]
    # df_all = df_all[df_all['折扣'] != 1]
    # del df_all['折扣']
    return df_all
##
conn2 = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_ebay = write_data(conn2)
##
# CD
sql = """
    SELECT 
        account_id, country,seller_sku,product_id,online_price,price,best_warehouse_name,warehouse,is_normal_cdt,
        is_platform_cdt,is_uk_warehouse, is_clear_sku,is_small_diff,is_white_account,date_id
    FROM over_sea.oversea_cdiscount_listing_all 
    WHERE date_id = '2023-12-21'
"""
df_cd_listing_y = conn.read_sql(sql)

##
sql = """
    SELECT account_id, seller_sku, product_id, online_price as online_price_t
    FROM over_sea.yibai_cdiscount_oversea_listing_price 
    WHERE DATE = '2023-12-22'
"""
df_cd_listing_t = conn.read_sql(sql)
##
df_cd_listing_t['account_id'] = df_cd_listing_t['account_id'].astype(int)
df_cd_listing_y = pd.merge(df_cd_listing_y, df_cd_listing_t, how='left', on=['account_id', 'seller_sku','product_id'])
df_cd_listing_y['price_diff'] = df_cd_listing_y['price'] / df_cd_listing_y['online_price_t'] - 1
##
df_cd_listing_y['is_same'] = np.where(df_cd_listing_y['price_diff'].abs()<= 0.05, 1, 0)
# df_walmart_listing_y.to_csv('df_walmart_listing_y.csv', index=0)
##
df_cd_listing_y.to_csv('df_cd_listing_y.csv', index=0)
##
# wish
sql = """
    SELECT account_id, seller_sku, product_id, sku, `子SKU`, country, `WE当前价格`
    FROM yibai_oversea.yibai_wish_oversea
"""
ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                     db_name='yibai_oversea')
df_wish = ck_client.ck_select_to_df(sql)
##
sql = """
    SELECT 
        SKU as sku, best_warehouse_name, warehouse, available_stock,sales_status,price target_price,country
    FROM dwm_oversea_price_dtl
    WHERE platform = 'WISH' and date_id = '2023-12-21'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_wish_dtl = conn.read_sql(sql)
##
df_wish_listing = pd.merge(df_wish, df_wish_dtl, how='left',on=['sku', 'country'])
##
df_wish_listing[['target_price','WE当前价格']] = df_wish_listing[['target_price','WE当前价格']].astype(float)
df_wish_listing['price_diff'] = df_wish_listing['target_price'] / df_wish_listing['WE当前价格'] - 1
df_wish_listing['is_same'] = np.where(df_wish_listing['price_diff'].abs()<= 0.05, 1, 0)
##
print(df_wish_listing.is_same.value_counts())