##
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import src.fetch_data as fd
from utils.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
import datetime,time
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut
from all_auto_task.oversea_price_adjust_2023 import get_fine_sku_age
##
def get_sku_sales():
    df_sku = pd.read_excel('F:\Desktop\中转仓待处理冗余库存SKU_20240126.xlsx', engine='openpyxl')
    df_sku = df_sku.rename(columns={'SKU':'sku'})
    df_sku['sku'] = df_sku['sku'].astype(str)
    sku_list = tuple(df_sku['sku'].unique())
    #
    date_now = datetime.date.today()
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=91)).strftime('%Y-%m-%d')
    sql=f"""
    WITH order_temp AS (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order_sku
        WHERE sku in {sku_list}
        )
    
    select 
        a.order_id, a.platform_code, sku, quantity, ship_country,
        a.payment_time  AS `付款时间`,
        a.purchase_time  AS `创建时间`
    from (
        SELECT order_id,  platform_code, account_id, warehouse_id, payment_time, purchase_time, ship_country
        FROM yibai_oms_sync.yibai_oms_order 
        WHERE 
            payment_time >= '{start_date}' and purchase_time < '{end_date}' 
            and order_id not like '%%-RE%%' 
            and platform_status not in ('Canceled', 'Pending') 
            and (order_status=70 OR ship_status=2)
            and order_id in (select order_id from order_temp)
        ) a
    left join  (
        select order_id, sku, quantity
        from yibai_oms_sync.yibai_oms_order_sku
        where order_id in (select order_id from order_temp)
    ) b on a.order_id = b.order_id
    """
    df = fd.fetch_ck(sql, 78, 'yibai_oms_sync')

    #
    date_60 = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
    date_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    df_90 = df[df['付款时间'] > start_date].groupby(['sku', 'ship_country']).agg({'quantity':'sum'}).reset_index().rename(columns={'quantity':'近90天销量'})
    df_60 = df[df['付款时间'] > date_60].groupby(['sku', 'ship_country']).agg({'quantity':'sum'}).reset_index().rename(columns={'quantity':'近60天销量'})
    df_30 = df[df['付款时间'] > date_30].groupby(['sku', 'ship_country']).agg({'quantity':'sum'}).reset_index().rename(columns={'quantity':'近30天销量'})

    df_sku_sales = pd.merge(df_sku, df_90, how='left', on=['sku'])
    df_sku_sales = pd.merge(df_sku_sales, df_60, how='left', on=['sku','ship_country'])
    df_sku_sales = pd.merge(df_sku_sales, df_30, how='left', on=['sku','ship_country'])
    #
    sql = """
    SELECT
        id as warehouse_id, name as warehouse_name, code as warehouse_code, type, country,
        CASE 
            WHEN type IN ('third', 'overseas') and country='US' THEN '美国仓'
            WHEN type IN ('third', 'overseas') and country='UK' THEN '英国仓'
            WHEN type IN ('third', 'overseas') and country='GB' THEN '英国仓'
            WHEN type IN ('third', 'overseas') and country='CS' THEN '德国仓'
            WHEN type IN ('third', 'overseas') and country='FR' THEN '法国仓'
            WHEN type IN ('third', 'overseas') and country='IT' THEN '意大利仓'
            WHEN type IN ('third', 'overseas') and country='AU' THEN '澳洲仓'
            WHEN type IN ('third', 'overseas') and country='ES' THEN '西班牙仓'
            WHEN type IN ('third', 'overseas') and country='CA' THEN '加拿大仓'
            WHEN type IN ('third', 'overseas') and country='DE' THEN '德国仓'
            WHEN type IN ('third', 'overseas') and country='JP' THEN '日本仓'
            WHEN type IN ('third', 'overseas') and country='PL' THEN '德国仓'
            WHEN type IN ('third', 'overseas') and country='MX' THEN '墨西哥仓'
            WHEN type IN ('third', 'overseas') and country='UY' THEN '乌拉圭仓'
            WHEN type IN ('third', 'overseas') and country='BR' THEN '巴西仓'
            WHEN type IN ('third', 'overseas') and country='RU' THEN '俄罗斯仓'
            ELSE NULL 
        END AS warehouse
    FROM yb_datacenter.yb_warehouse
    """
    df_warehouse = fd.fetch_ck(sql, 78, 'yb_datacenter')

    #
    sql = """
    
        SELECT 
            SKU sku,warehouse_id, platform_code, 30days_sales, 60days_sales, 90days_sales 
        FROM `yibai_sku_sales_statistics` 
        WHERE SKU in {}
    """.format(sku_list)
    df_sales_2 = fd.fetch_mysql(sql, 212, 'over_sea')

    #
    df_sku_sales_2 = pd.merge(df_sales_2, df_warehouse[['warehouse_id', 'warehouse_name', 'type', 'country', 'warehouse']], how='left', on=['warehouse_id'])

    #
    df_sku_sales_2.to_excel('df_sku_sales_2.xlsx', index=0)

def get_dashbord_order():
    """ 海外仓订单 """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=31)
    date_start = '2025-02-01'
    date_end = '2026-12-01'
    sql = f"""
         SELECT
             order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
             warehouse_name, warehouse, quantity, new_price, release_money, sales_status
         FROM yibai_oversea.dashbord_new_data1
         WHERE 
             paytime >= '{date_start}'
             and paytime < '{date_end}'
             and `total_price` > 0 
             and `sales_status` not in ('','nan','总计')
             and warehouse_name not like '%精品%'
             -- and platform_code = 'ALI'
     """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)

    return df_order_info



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
    #
    # 产品成本、重量、体积
    df_mx_sales['数量'] = 1
    df_mx_sales = chicun_zhongliang(df_mx_sales, 1, conn_ck)
    #
    df_mx_sales.to_excel('df_mx_sales_info.xlsx', index=0)
    #
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
    #
    df_order_info.to_excel('YM墨西哥2仓近90天订单.xlsx', index=0)
##
def get_dcm_order():
    sql = """
        SELECT 
            a.order_id, a.sku, a.quantity `订单销量`, b.is_ship_process,
            b.platform_code, b.shop_id AS distributor_id, b.account_id, c.seller_sku,
            b.payment_time start_time, b.warehouse_id, b.total_price, b.currency, b.ship_country
        FROM yibai_dcm_order.dcm_order b
        LEFT JOIN yibai_dcm_order.dcm_order_sku a ON a.order_id = b.order_id
        LEFT JOIN yibai_dcm_order.dcm_order_detail c ON a.order_detail_id = c.id 
        WHERE 
            b.payment_time >= '2024-08-26'
            AND b.payment_time <= '2024-09-26'
            AND b.order_status != 40  -- 订单状态（1=待处理、10=未发货、20=部分发货、30=已发货、 40=已作废）
            AND b.payment_status = 1 -- 平台付款状态（客户向亚马逊平台的付款状态，0=未付款，1=已付款）
            AND b.is_abnormal = 0 -- 是否有异常（0=否，1=是）
            AND b.is_intercept = 0 -- 是否拦截（0=未拦截，1=已拦截）
            AND b.refund_status = 0 -- 退款状态（0=未退款，1=退款中、2=部分退款，3=全部退款）
            AND b.total_price != 0 -- 订单总金额 平台付款金额
            -- AND b.is_ship_process = 1 -- 是否平台仓发货（0否--FBM；1：是--FBA）
            AND b.ship_country = 'MX'
    """
    # -- AND
    #
    # -- AND
    # e.fulfillment_type = 2 - - 发货类型（0 = 未知、1 = 本地仓发货、2 = 海外仓、10 = 云盒直发）
    conn = connect_to_sql(database='yibai_dcm_order', data_sys='新数仓')
    df_dcm_order = conn.read_sql(sql)
    # 匹配大仓
    sql = """
        SELECT 
            distinct id warehouse_id, name warehouse_name, type
        FROM yb_datacenter.yb_warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_dcm_order = pd.merge(df_dcm_order, df_warehouse, how='left', on=['warehouse_id'])

    return df_dcm_order

def get_yb_order():
    sql = """
        WITH temp as (
            SELECT distinct order_id
            FROM yibai_oms_sync.yibai_oms_order
            WHERE payment_time>='2024-08-26' and ship_country = 'MX'
        )
        SELECT 
            distinct A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as seller_sku,B.account_id,
            B.payment_time as paytime,
            B.payment_time as created_time,
            CASE
                WHEN B.payment_status = 1 THEN '已付款' 
                ELSE '未付款'
            END AS pay_status,
            B.warehouse_id,
            ship_country,
            A.quantity,
            F.sales_status as sales_status
            FROM yibai_oms_sync.yibai_oms_order_detail A
            LEFT JOIN (
                SELECT *
                FROM yibai_oms_sync.yibai_oms_order 
                WHERE payment_time>='2024-06-01' and ship_country = 'MX' and order_id in (SELECT order_id FROM temp)
            ) B ON A.order_id=B.order_id
            left join yibai_oms_sync.yibai_oms_order_sku F on F.order_detail_id=A.id
            WHERE  
            B.payment_time>='2024-08-26'
            and B.payment_status=1 
            and B.order_status <> 80
            and A.quantity > 0
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df_order_yb = conn_ck.ck_select_to_df(sql)
    df_order_yb.columns = [i.split('.')[-1] for i in df_order_yb.columns]
    # 匹配大仓
    sql = """
        SELECT 
            distinct id warehouse_id, name warehouse_name, type
        FROM yb_datacenter.yb_warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_yb_order = pd.merge(df_order_yb, df_warehouse, how='left', on=['warehouse_id'])

    return df_yb_order
##
# df_yb_order = get_yb_order()
# ##
# df_dcm_order = get_dcm_order()
# ##
# df_dcm_order.to_excel('df_dcm_order.xlsx', index=0)
##  海外仓墨西哥库存信息汇总
def get_mx_stock():
    sql = """
    
        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count `调拨在途`, transit_on_way_count `中转在途`, other_on_way `其他在途`,
            yps.new_price as new_price, cargo_owner_id
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        LEFT JOIN (
            SELECT
                sku, product_status `产品状态`, title_cn `产品名称`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`
            FROM yibai_prod_base_sync.yibai_prod_sku    
        ) yps ON ps.sku = yps.sku
        WHERE 
            ps.date_id = '20240808' 
            -- and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            -- and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            and yw.warehouse_name not in ('HC美国西仓','出口易美西仓-安大略仓','万邑通美南仓-USTX','亿迈CA01仓')  -- 剔除不常用仓库
            and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','俄罗斯仓','乌拉圭仓')
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp_mx = conn_ck.ck_select_to_df(sql)

    return df_temp_mx
##
# df_temp_mx = get_mx_stock()
# df_temp_mx.to_excel('df_temp_mx.xlsx', index=0)


### YM墨西哥仓冗余库存
def mx_stock_temp():
    sql = """
        SELECT 
            a.sku, a.warehouse_id, a.available_stock, a.redundancy_stock, redundant_attribute, cargo_owner_id,
            b.warehouse_name, c.name warehouse
        FROM yb_stock_center_sync.yb_mlcode_stock a
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse b ON a.warehouse_id = b.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category c ON b.ebay_category_id = c.id
        WHERE 
            b.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and cargo_owner_id != 8          -- 筛选商户ID非易佰的
            -- and a.redundant_attribute = 2    -- 筛选冗余属性是外部的
            -- and a.specified_information = ''  -- 筛选未指定商户的
            and c.name = '墨西哥仓'
            -- and (a.available_stock > 0 or a.redundancy_stock > 0)
    """
    conn_ck = pd_to_ck(database='yb_stock_center_sync', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)
    df_mx_stock.columns = [i.split('.')[-1] for i in df_mx_stock.columns]

    #
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT
            date_id, sku, best_warehouse_name, warehouse, new_price, available_stock,available_stock_money, 
            warehouse_stock, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}' and available_stock > 0 and warehouse = '墨西哥仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_dwm_mx = conn.read_sql(sql)
    #
    df_result = pd.merge(df_mx_stock, df_dwm_mx, how='left', on=['sku', 'warehouse'])

    return df_result


def mx_stock_age_temp():
    """ 获取saas系统墨西哥库龄数据 """
    sql = """

        SELECT 
            w, b.sku sku, b.client_sku client_sku, 
            a.sku as oversea_sku, cargo_owner_id, 
            instock_stock-out_stock as saas_stock, storage_age_date, 
            toInt32(today() - toDate(storage_age_date)) as stock_age,
            today()
        FROM yb_datacenter.yibai_stock_age_detail a
        LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
        ON a.sku = b.oversea_sku
        WHERE w = 'YM-MX-2'


    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)

    df_mx_stock.to_excel('F://Desktop//df_mx_stock_age.xlsx', index=0)


# mx_stock_age_temp()
##
## tt库龄数据检查
def age_temp():
    # 取库龄
    date_today = datetime.date.today() - datetime.timedelta(days=7)
    # date_today = '2024-10-10'
    sql = f"""
        SELECT
            date as date_id, sku, yw.id warehouse_id, warehouse_name,ya.warehouse_code, ya.order_warehouse_code,
            case
                when ya.country = 'GB' then 'UK'
                when ya.country = 'CS' then 'DE'
                when ya.warehouse_code = 'WYT_CATO' then 'CA'
                else ya.country
            end as country,
            ywc.name warehouse,
            warehouse_stock, inventory_age, charge_total_price,
            case
                when inventory_age <= 60 then 0
                when inventory_age <= 90 and inventory_age > 60 then 60
                when inventory_age <= 120 and inventory_age > 90 then 90
                when inventory_age <= 150 and inventory_age > 120 then 120
                when inventory_age <= 180 and inventory_age > 150 then 150
                when inventory_age <= 270 and inventory_age > 180 then 180
                when inventory_age <= 360 and inventory_age > 270 then 270
                when inventory_age > 360 then 360
            end as overage_level
        FROM yb_datacenter.yb_oversea_sku_age ya
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.order_warehouse_code = yw.warehouse_code
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
        WHERE
            date >= '{date_today}' and status in (0,1)
            -- and yw.warehouse_name not like '%独享%'
            -- and yw.warehouse_name not like '%TT%'
            -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code

    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)

    df_age.to_excel('F://Desktop//df_age.xlsx', index=0)
    return df_age

# age_temp()

##
def clear_order():
    """ 海外仓清仓订单 """
    sql = """
        SELECT *
        FROM clear_order_cw.oversea_clear_orders
        WHERE update_month = '2025-02'
    """
    conn_ck = pd_to_ck(database='clear_order_cw', data_sys='调价明细历史数据')
    df_clear = conn_ck.ck_select_to_df(sql)

    df_account = get_temu_account()

    df_clear = pd.merge(df_clear, df_account[['account_id','main_name']], how='left', on=['account_id'])

    df_clear.to_excel('F://Desktop//df_clear.xlsx', index=0)

# clear_order()

def warehouse_mark(s, codes):
    for code in codes:
        # 替换前缀，包括连字符
        if s.startswith(f"{code}-"):
            s = s[len(code)+1:]
        # 替换后缀，包括连字符
        elif s.endswith(f"-{code}"):
            s = s[:-len(code)-1]
        elif s.endswith(f"{code}"):
            s = s[:-len(code)]
    return s

## 海外仓sku在不同模式下的销库金额数据
def get_sku_info():
    """ 获取海外仓sku基础信息 """
    df_sku = pd.read_excel('F://Desktop//海外仓库存20250217.xlsx', dtype={'sku':str})
    print(df_sku.info())

    # sku仓标处理
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_sku['new_sku'] = df_sku['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))

    # 大仓处理
    df_warehouse = get_warehouse()
    df_sku = pd.merge(df_sku, df_warehouse[['warehouse_name','warehouse','country']], how='left', on=['warehouse_name'])
    dic = {'CZ':'DE', 'CS':'DE', 'PL':'DE', 'AS':'PH', 'GBA':'MY', 'UY':'BR', 'ES':'DE', 'IT':'DE',
           'GB':'UK', 'FR':'DE'}
    df_sku['country'] = df_sku['country'].replace(dic)

    df_sku = df_sku.drop_duplicates(subset=['warehouse_name','sku'])
    df = df_sku.groupby(['new_sku','country']).agg({'在库库存数量':'sum', '云仓可用库存数量':'sum',
                                                    '在途库存数量':'sum','海外仓不可用库存数量':'sum',}).reset_index()

    # df.to_excel('F://Desktop//df_sku_0.xlsx', index=0)

    # 获取易佰各模式销量：海外仓、国内仓、FBA仓、其他平台仓

    return df

def get_sku_status():
    """ 获取海外仓销售状态 """
    sql = """
    SELECT
        a.sku sku,  a.warehouse warehouse, new_price, 
        IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`
    FROM (
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
        -- and (available_stock > 0)
    ) a
    LEFT JOIN (
        SELECT *
        FROM oversea_sale_status
        WHERE end_time IS NULL
    ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    # sku仓标处理
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_sku['new_sku'] = df_sku['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))

    df_sku.to_excel('F://Desktop//df_sku_status.xlsx', index=0)

def get_new_price():
    """ """
    df = get_sku_info()

    sql = """
    SELECT 
        sku, sku new_sku, title_cn `产品名称`, develop_source, b.develop_source_name,
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
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    df_sku = df_sku.drop_duplicates(subset='sku')

    df = pd.merge(df, df_sku[['sku','new_price','develop_source_name']], how='left', on='sku')
    # df = pd.merge(df, df_sku[['new_sku', 'new_price']], how='left', on='new_sku')
    # print(df.info())
    #
    # df['new_price'] = np.where(df['new_price_x'].isna(),  df['new_price_y'], df['new_price_x'])

    df.to_excel('F://Desktop//df_cost_2.xlsx', index=0)


def get_warehouse():
    sql = """
    SELECT 
        a.id as warehouse_id, a.warehouse_name AS warehouse_name, a.country country, b.name AS warehouse
    FROM yibai_logistics_tms_sync.yibai_warehouse a
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b 
    ON toInt64(a.ebay_category_id) = toInt64(b.id)
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)

    return df_warehouse

def yb_order():
    """ 获取yb订单表 """
    sql = """
    SELECT id warehouse_id,case when type in ('fba','platform') then '平台仓' WHEN type = 'inland' THEN '国内仓' when type in ('third','overseas') then '海外仓'
    WHEN type in ('consignment','transit') THEN '分销'  ELSE '' END AS `仓库类型`   FROM yb_datacenter.yb_warehouse
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    warehouse = conn_mx.ck_select_to_df(sql)

    sql = f"""
    with order_table as (
        SELECT distinct order_id from yibai_oms_sync.yibai_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        AND order_id not like '%%-RE' 
        AND payment_time>='2025-01-18 00:00:00'
        AND payment_time<'2025-02-17 00:00:00'
        and total_price!=0
    )
    SELECT sku, platform_code, ship_country, warehouse_id, sum(`销量`) `销量`, sum(`销库金额`) `销库金额`
    FROM (
        SELECT 
        a.platform_code platform_code,
        a.ship_country `站点`,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        b.asinval as asin,
        d.sku as sku,
        ship_country,
        toInt64(d.quantity) as `销量`,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.purchase_time as created_time,
        toYearWeek(toDate(  a.purchase_time) ) `周`,
        toFloat64(c.total_price) as `销售额`,
        case when toFloat64(c.true_profit_new1) = 0 then toFloat64(c.profit_new1) else toFloat64(c.true_profit_new1) end as `利润`,
        toFloat64(purchase_cost) `销库金额`,
        d.sales_status  `销售状态`
        FROM  (
            select * from yibai_oms_sync.yibai_oms_order_detail
            where order_id in (select order_id from order_table)
        ) b 
        inner JOIN (
            select * from yibai_oms_sync.yibai_oms_order
            where order_id in (select order_id from order_table)
        ) a 
        ON b.order_id=a.order_id 
        inner JOIN (
            select * from yibai_oms_sync.yibai_oms_order_profit
            where order_id in (select order_id from order_table)
        ) c 
        ON a.order_id=c.order_id 
        inner join (
            select * from yibai_oms_sync.yibai_oms_order_sku
            where order_id in (select order_id from order_table)
        ) d 
        on b.id=d.order_detail_id 
    ) a
    GROUP BY sku, platform_code, ship_country, warehouse_id
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    order = conn_mx.ck_select_to_df(sql)
    order = order.merge(warehouse, on='warehouse_id', how='left')

    # df_warehouse = get_warehouse()
    # order = order.merge(df_warehouse[['warehouse_id','warehouse','country']], on='warehouse_id', how='left')
    dic = {'PL': 'DE', 'NL': 'DE', 'SE': 'DE', 'FR': 'DE', 'IT': 'DE', 'CH': 'DE', 'CZ': 'DE', 'AT': 'DE', 'BE': 'DE',
           'ES': 'DE', 'GB': 'UK', 'SK': 'DE', 'SI': 'DE', 'PT': 'DE', 'RO': 'DE', 'LU': 'DE', 'IE': 'DE', 'FI': 'DE',
           'HU': 'DE', 'HR': 'DE', 'NO': 'DE', 'GR': 'DE', 'EE': 'DE', 'DK': 'DE', 'CY': 'DE', 'BG': 'DE', 'LV': 'DE',
           'LT': 'DE', 'MT': 'DE', 'MK': 'DE', 'IS': 'DE'}
    order['country'] = order['ship_country'].replace(dic)
    # sku仓标处理
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    order['new_sku'] = order['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))

    order_oversea = order[order['仓库类型']=='海外仓']
    order_oversea = order_oversea.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_oversea = order_oversea.rename(columns={'销量': '销量海外仓', '销库金额': '销库金额海外仓'})

    order_dom = order[order['仓库类型']=='国内仓']
    order_dom = order_dom.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_dom = order_dom.rename(columns={'销量': '销量国内仓', '销库金额': '销库金额国内仓'})

    order_fba = order[(order['仓库类型']=='平台仓') & (order['platform_code']=='AMAZON')]
    order_fba = order_fba.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_fba = order_fba.rename(columns={'销量': '销量FBA', '销库金额': '销库金额FBA'})

    order_pla = order[(order['仓库类型']=='平台仓') & (order['platform_code']!='AMAZON')]
    order_pla = order_pla.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_pla = order_pla.rename(columns={'销量': '销量其他平台仓', '销库金额': '销库金额其他平台仓'})

    # order = order.groupby(['new_sku', 'country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()


    df = get_sku_info()

    df = pd.merge(df, order_oversea, how='left', on=['new_sku','country'])
    df = pd.merge(df, order_dom, how='left', on=['new_sku', 'country'])
    df = pd.merge(df, order_fba, how='left', on=['new_sku', 'country'])
    df = pd.merge(df, order_pla, how='left', on=['new_sku', 'country'])
    # print(df['ship_country'].unique())

    print(df.info())
    # order = order.drop_duplicates('order_id')
    # order = order[order['仓库类型'] != '进口仓']
    # print('仓库类型为空', order['仓库类型'].isnull().sum())
    # print('销库金额为0', (order['销库金额'] == 0).sum())
    # order = order[order['销库金额'] > 0]
    df.to_excel('F://Desktop//order_yb.xlsx', index=0)

def tt_order():
    """ 获取tt订单表 """
    sql = """
    SELECT id warehouse_id,case when warehouse_type in (5,6) then '平台仓' WHEN warehouse_type=1 THEN '国内仓' when warehouse_type=8 then '海外仓'
    WHEN warehouse_type=7 THEN '分销' WHEN warehouse_type=9 THEN '进口仓' ELSE '' END AS `仓库类型` FROM tt_logistics_tms_sync.tt_warehouse_tt 
    """
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    warehouse = conn.ck_select_to_df(sql)
    warehouse['warehouse_id'] = warehouse['warehouse_id'].apply(lambda x: int('10000' + str(x)))

    sql = """
    SELECT id warehouse_id,case when type in ('fba','platform') then '平台仓' WHEN type = 'inland' THEN '国内仓' when type in ('third','overseas') then '海外仓'
    WHEN type in ('consignment','transit') THEN '分销'  ELSE '' END AS `仓库类型`   FROM yb_datacenter.yb_warehouse
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    yb_warehouse = conn_mx.ck_select_to_df(sql)
    warehouse = warehouse.append(yb_warehouse, ignore_index=True)

    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        AND order_id not like '%%-RE' 
        AND payment_time>='2025-01-18 00:00:00'
        AND payment_time<'2025-02-17 00:00:00'
        and platform_code!='SPH' AND total_price!=0
    )
    SELECT sku, platform_code, ship_country, warehouse_id, sum(`销量`) `销量`, sum(`销库金额`) `销库金额`
    FROM (
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        b.asinval as asin,
        d.sku as sku,
        toInt64(d.quantity) as `销量`,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.purchase_time as created_time,
        toYearWeek(toDate(  a.purchase_time),1 ) `周`,
        toFloat64(c.total_price) as `销售额`,
        -- case when toFloat64(c.true_profit_new1) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit_new1) end as `利润`,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as `利润`,
        toFloat64(purchase_cost) `销库金额`,
        toFloat64(c.refund_amount) as `退款金额`,
        toFloat64(c.shipping_price) as shipping_price,
        toFloat64(c.shipping_cost) as `预估运费`,
        toFloat64(c.true_shipping_fee) as `实际运费`
        FROM  (
            select * from tt_oms_sync.tt_oms_order_detail
            where order_id in (select order_id from order_table)
        ) b 
        inner JOIN (
            select * from tt_oms_sync.tt_oms_order
            where order_id in (select order_id from order_table)
        ) a 
        ON b.order_id=a.order_id 
        inner JOIN (
            select * from tt_oms_sync.tt_oms_order_profit
            where order_id in (select order_id from order_table)
        ) c 
        ON a.order_id=c.order_id 
        inner join (
            select * from tt_oms_sync.tt_oms_order_sku
            where order_id in (select order_id from order_table)
        ) d 
        on b.id=d.order_detail_id 
    ) a
    GROUP BY sku, platform_code, ship_country, warehouse_id
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓MRP')
    order = conn_mx.ck_select_to_df(sql)
    order = order.merge(warehouse, on='warehouse_id', how='left')

    # df_warehouse = get_warehouse()
    # order = order.merge(df_warehouse[['warehouse_id','warehouse','country']], on='warehouse_id', how='left')
    dic = {'PL': 'DE', 'NL': 'DE', 'SE': 'DE', 'FR': 'DE', 'IT': 'DE', 'CH': 'DE', 'CZ': 'DE', 'AT': 'DE', 'BE': 'DE',
           'ES': 'DE', 'GB': 'UK', 'SK': 'DE', 'SI': 'DE', 'PT': 'DE', 'RO': 'DE', 'LU': 'DE', 'IE': 'DE', 'FI': 'DE',
           'HU': 'DE', 'HR': 'DE', 'NO': 'DE', 'GR': 'DE', 'EE': 'DE', 'DK': 'DE', 'CY': 'DE', 'BG': 'DE', 'LV': 'DE',
           'LT': 'DE', 'MT': 'DE', 'MK': 'DE', 'IS': 'DE'}
    order['country'] = order['ship_country'].replace(dic)
    # sku仓标处理
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    order['new_sku'] = order['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))

    order_oversea = order[order['仓库类型']=='海外仓']
    order_oversea = order_oversea.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_oversea = order_oversea.rename(columns={'销量': '销量海外仓', '销库金额': '销库金额海外仓'})

    order_dom = order[order['仓库类型']=='国内仓']
    order_dom = order_dom.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_dom = order_dom.rename(columns={'销量': '销量国内仓', '销库金额': '销库金额国内仓'})

    order_fba = order[(order['仓库类型']=='平台仓') & (order['platform_code']=='AMAZON')]
    order_fba = order_fba.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_fba = order_fba.rename(columns={'销量': '销量FBA', '销库金额': '销库金额FBA'})

    order_pla = order[(order['仓库类型']=='平台仓') & (order['platform_code']!='AMAZON')]
    order_pla = order_pla.groupby(['new_sku','country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()
    order_pla = order_pla.rename(columns={'销量': '销量其他平台仓', '销库金额': '销库金额其他平台仓'})

    # order = order.groupby(['new_sku', 'country']).agg({'销量':'sum', '销库金额':'sum'}).reset_index()


    df = get_sku_info()

    df = pd.merge(df, order_oversea, how='left', on=['new_sku','country'])
    df = pd.merge(df, order_dom, how='left', on=['new_sku', 'country'])
    df = pd.merge(df, order_fba, how='left', on=['new_sku', 'country'])
    df = pd.merge(df, order_pla, how='left', on=['new_sku', 'country'])
    # print(df['ship_country'].unique())

    print(df.info())
    # order = order.drop_duplicates('order_id')
    # order = order[order['仓库类型'] != '进口仓']
    # print('仓库类型为空', order['仓库类型'].isnull().sum())
    # print('销库金额为0', (order['销库金额'] == 0).sum())
    # order = order[order['销库金额'] > 0]
    df.to_excel('F://Desktop//order_tt.xlsx', index=0)

def temu_cant_sales():
    """ temu平台无法销售sku """
    sql = """
        SELECT distinct sku, country, online_status
        FROM yibai_oversea.yibai_oversea_temu_listing
        WHERE online_status in ('已发布到站点', '已加入站点') and date_id = '2025-02-17' 
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())
    dic = {'PL': 'DE', 'NL': 'DE', 'SE': 'DE', 'FR': 'DE', 'IT': 'DE', 'CH': 'DE', 'CZ': 'DE', 'AT': 'DE', 'BE': 'DE',
           'ES': 'DE', 'GB': 'UK', 'SK': 'DE', 'SI': 'DE', 'PT': 'DE', 'RO': 'DE', 'LU': 'DE', 'IE': 'DE', 'FI': 'DE',
           'HU': 'DE', 'HR': 'DE', 'NO': 'DE', 'GR': 'DE', 'EE': 'DE', 'DK': 'DE', 'CY': 'DE', 'BG': 'DE', 'LV': 'DE',
           'LT': 'DE', 'MT': 'DE', 'MK': 'DE', 'IS': 'DE'}
    df['country'] = df['country'].replace(dic)

    df.to_excel('F://Desktop//df_temu.xlsx', index=0)

def get_mx_dwm():
    """ 墨西哥销售状态库存库龄 """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT
            a.sku sku, title, type, product_status, linest, a.warehouse warehouse, available_stock, 
            product_package_size,new_price, gross, best_warehouse_id, best_warehouse_name,  
            overage_level, age_90_plus, age_180_plus,  age_270_plus, warehouse_stock_age,
            IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info
            WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
            and (available_stock > 0) and warehouse = '墨西哥仓'
        ) a
        LEFT JOIN (
            SELECT *
            FROM oversea_sale_status
            WHERE end_time IS NULL
        ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    df_sku = conn.read_sql(sql)

    df_sku.to_excel('F://Desktop//df_mx_sku_info.xlsx', index=0)

def get_clear_order():
    """ 海外仓清仓订单 """
    sql = """
        SELECT *
        FROM clear_order_cw.oversea_clear_orders
        WHERE update_month = '2025-05'
    """
    conn_ck = pd_to_ck(database='clear_order_cw', data_sys='调价明细历史数据')
    df_clear = conn_ck.ck_select_to_df(sql)
    print(df_clear.info())

    df_clear.to_excel('F://Desktop//df_clear.xlsx', index=0)

# 海外仓和fba仓尾程测算-尺寸分段对比
def fba_oversea_fee():
    """
    海外仓和fba仓尾程测算-尺寸分段对比：
    1.用fba的尺寸分段测算，美国英国德国
    2.海外仓最低运费用Fedfx Ground 以及实际最低两种测算
    """
    sql = """
        SELECT sku, type, warehouse, best_warehouse_name, available_stock, new_price, gross `重量`, product_package_size
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-01-01')
        and warehouse in ('美国仓', '德国仓', '英国仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    # 海外仓尺寸处理
    df_sku['站点'] = df_sku['warehouse'].replace({'美国仓':'美国', '德国仓':'德国', '英国仓':'英国'})
    df_sku[['长','宽','高']] = df_sku['product_package_size'].str.split('*', expand=True)
    df_sku[['长', '宽', '高','重量']] = df_sku[['长','宽','高','重量']].fillna(1).astype(float)
    df_sku['fbafee计算方式'] = '普通'
    df_sku['最小购买数量'] = 1
    print(df_sku.info())
    df_sku = fba_ding_jia_biao(df_sku)
    col = ['sku', 'type','warehouse','best_warehouse_name','available_stock','new_price','重量','站点',
           '长','宽','高','尺寸分段','fba_fees']
    df_sku = df_sku[col]
    print(df_sku.info())

    # 获取海外仓运费
    sql = """
        SELECT sku, warehouseName as best_warehouse_name, shipName `调价尾程渠道`, shippingCost `调价尾程`
        FROM over_sea.oversea_transport_fee_useful
        WHERE warehouse in ('美国仓','德国仓','英国仓') and platform = 'AMAZON' and country in ('US','DE','UK')
    """
    df_fee = conn.read_sql(sql)

    df = pd.merge(df_sku, df_fee, how='left', on=['sku','best_warehouse_name'])

    sql = """
        SELECT sku, shipName `指定渠道`, shippingCost `指定渠道尾程`
        FROM yibai_oversea.oversea_transport_fee_daily
        WHERE date_id = '20250324' and warehouseName in ('谷仓美国东仓','谷仓美国西仓') 
        and platform = 'AMAZON' and shipName in ('GC-美西FEDEX GROUND','GC-美东FEDEX GROUND')
        ORDER BY shippingCost ASC
        LIMIT 1 BY sku
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_us_fee = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_us_fee, how='left', on=['sku'])
    # df_us_fee.to_excel('F://Desktop//df_us_fee.xlsx', index=0)

    df.to_excel('F://Desktop//df_fba_oversea.xlsx', index=0)

# 海外仓库存库龄数据
def get_stock_and_age():
    """
    统计海外仓库存库龄数据
    """

    # 获取库存数据
    sql = """
    SELECT
        ps.sku AS sku,ywc.name AS warehouse,yw.warehouse_name AS warehouse_name,yw.warehouse_code AS warehouse_code,
        yw.id AS warehouse_id,yw.warehouse_other_type AS warehouse_other_type,stock,
        available_stock,allot_on_way_count AS on_way_stock,wait_outbound,frozen_stock, cargo_owner_id
    FROM yb_datacenter.yb_stock AS ps
    INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    WHERE 
        (ps.date_id = toYYYYMMDD(now())) 
        -- AND (ps.cargo_owner_id = 8) 
        AND (yw.warehouse_type IN (2,3)) 
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = conn_ck.ck_select_to_df(sql)

    # 获取库龄数据
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_mx_age = get_mx_stock_age()
    df_stock_age_warehouse = pd.concat([df_stock_age_warehouse, df_mx_age])

    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])


def get_mx_stock_age():
    """
    20240807
    获取YM墨西哥仓的库龄数据。
    由于saas系统库龄是YM墨西哥实仓数据，无法区分出虚拟子仓的明细。而易佰销售可使用的仅限子仓YM墨西哥2仓。
    为分离出虚拟子仓YM墨西哥2仓的库龄数据，采用【实体仓库龄库存】 * 【子仓库存】/【实体仓库存】的方式等比例计算子仓的库龄数据。
    """
    # 获取saas系统库龄数据
    sql = """
    SELECT 
        sku, cargo_owner_id, sum(saas_stock) saas_stock,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus
    FROM (
        SELECT
            sku, cargo_owner_id, saas_stock, stock_age,
            case
                when stock_age >= 30 and stock_age < 60 then 30
                when stock_age >= 60 and stock_age < 90 then 60
                when stock_age >= 90 and stock_age < 120 then 90
                when stock_age >= 120 and stock_age < 150 then 120
                when stock_age >= 150 and stock_age < 180 then 150
                when stock_age >= 180 and stock_age < 270 then 180
                when stock_age >= 270 and stock_age < 360 then 270
                when stock_age >= 360 then 360
            else 0 end as overage_level, 
            case when stock_age >= 30 then saas_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then saas_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then saas_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then saas_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then saas_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then saas_stock else 0 end as age_180_plus,
            case when stock_age >= 270 then saas_stock else 0 end as age_270_plus,
            case when stock_age >= 360 then saas_stock else 0 end as age_360_plus
        FROM (
            SELECT sku, cargo_owner_id, sum(saas_stock) as saas_stock, stock_age
            FROM (
                SELECT 
                    w, b.sku sku, b.client_sku client_sku, 
                    a.sku as oversea_sku, cargo_owner_id, 
                    instock_stock-out_stock as saas_stock, storage_age_date, 
                    toInt32(today() - toDate(storage_age_date)) as stock_age,
                    today()
                FROM yb_datacenter.yibai_stock_age_detail a
                LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                ON a.sku = b.oversea_sku
                WHERE w = 'YM-MX-2'
            ) a
            GROUP BY sku, cargo_owner_id, stock_age
            HAVING saas_stock > 0 
            -- and cargo_owner_id = 8
        )
    ) a
    GROUP BY sku, cargo_owner_id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)

    # 获取云仓库存数据
    date_today = time.strftime('%Y%m%d')
    sql = f"""
    SELECT 
        sku, warehouse, cargo_owner_id, arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age, sum(available_stock) as available_stock,
        sum(on_way_stock) as on_way_stock, sum(wait_outbound) as wait_outbound, sum(frozen_stock) frozen_stock, max(new_price) new_price
    FROM (
        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, yps.new_price as new_price, cargo_owner_id,
            concat(toString(warehouse_id), ':', toString(available_stock)) as stock_info
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        LEFT JOIN (
            SELECT
                sku, product_status `产品状态`, title_cn `产品名称`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`
            FROM yibai_prod_base_sync.yibai_prod_sku    
        ) yps ON ps.sku = yps.sku
        WHERE 
            ps.date_id = '{date_today}'          -- 根据需要取时间
            -- and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            -- and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            -- and yw.id not in (646, 648) -- BD、WEIS墨西哥仓
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    ) a
    GROUP BY sku, warehouse, cargo_owner_id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_temp = conn_ck.ck_select_to_df(sql)

    df_mx = pd.merge(df_stock_temp, df_mx_stock, how='left', on=['sku'])
    # YM库龄库存按比例分解
    df_mx['占比'] = df_mx['available_stock'] / df_mx['saas_stock']
    df_mx['占比'] = np.where(df_mx['占比'] > 1, 1, df_mx['占比'])
    #
    col_list = ['age_30_plus', 'age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus',
                'age_270_plus',
                'age_360_plus']
    for i in col_list:
        df_mx[i] = df_mx[i] * df_mx['占比']
    #
    df_mx['warehouse_stock'] = np.where(df_mx['saas_stock'].isna(), df_mx['saas_stock'], df_mx['available_stock'])
    df_mx['best_warehouse_id'] = 956
    df_mx['best_warehouse_name'] = 'YM墨西哥仓'
    df_mx['charge_currency'] = 'MXN'
    df_mx['charge_total_price'] = 0

    #
    df_mx = df_mx[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus', 'age_60_plus', 'age_90_plus',
         'age_120_plus','age_150_plus',
         'age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock_age', 'charge_currency', 'overage_level',
         'best_warehouse_id', 'best_warehouse_name']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    return df_mx

def get_stock_age():
    """
    获取库龄数据
    处理库龄数据
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # 调整库龄取数的日期，针对某个日期库龄数据不完整的情况
    n = 2
    sql = f"""
    SELECT  
        sku, charge_currency, cargo_type, ya.warehouse_code warehouse_code, yw.id as warehouse_id, 
        yw.warehouse_name warehouse_name, ywc.name warehouse,
        warehouse_stock, inventory_age, charge_total_price, 
        case when inventory_age >= 30 then warehouse_stock else 0 end as age_30_plus,
        case when inventory_age >= 60 then warehouse_stock else 0 end as age_60_plus,
        case when inventory_age >= 90 then warehouse_stock else 0 end as age_90_plus,
        case when inventory_age >= 120 then warehouse_stock else 0 end as age_120_plus,
        case when inventory_age >= 150 then warehouse_stock else 0 end as age_150_plus,
        case when inventory_age >= 180 then warehouse_stock else 0 end as age_180_plus,
        case when inventory_age >= 210 then warehouse_stock else 0 end as age_210_plus,
        case when inventory_age >= 270 then warehouse_stock else 0 end as age_270_plus,
        case when inventory_age >= 360 then warehouse_stock else 0 end as age_360_plus
    FROM yb_datacenter.yb_oversea_sku_age ya
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.warehouse_code = yw.warehouse_code
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    WHERE 
        date = formatDateTime(subtractDays(now(),{n}), '%Y-%m-%d') and status in (0,1) 
        and yw.warehouse_name not like '%独享%' 
        -- and yw.warehouse_name not like '%TT%'
        -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        and yw.id not in (339)   -- 剔除不常用仓库，避免最优子仓选到无运费的子仓上
    """
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 20250211 补充精铺转泛品sku的库龄数据
    df_fine_age = get_fine_sku_age()
    df_stock_age = pd.concat([df_stock_age, df_fine_age])
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id, warehouse, available_stock, warehouse_name
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 and warehouse_other_type = 2
    """
    df_stock = ck_client.ck_select_to_df(sql)
    df_stock_age = pd.merge(df_stock_age, df_stock[['sku','warehouse_id']], how='inner', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse_id 聚合
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].astype('float')
    df_stock_age_base = df_stock_age[['sku', 'charge_currency', 'cargo_type', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    df_stock_age_info = df_stock_age[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock', 'inventory_age', 'charge_total_price',
                                      'age_30_plus','age_60_plus',
                                      'age_90_plus','age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']]
    df_stock_age_info.loc[:,'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg({'warehouse_stock':'sum','inventory_age':'max','charge_total_price':'sum','age_30_plus':'sum','age_60_plus':'sum','age_90_plus':'sum',
                                                                              'age_120_plus':'sum','age_150_plus':'sum','age_180_plus':'sum','age_270_plus':'sum','age_360_plus':'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus','age_60_plus','age_90_plus',
                               'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']]
    df_temp_2 = df_stock_age_id[['sku', 'warehouse', 'warehouse_stock_age']]
    df_temp_2 = df_temp_2.groupby(['sku', 'warehouse']).agg({'warehouse_stock_age': list}).reset_index()
    df_temp_3 = df_stock_age_id[['sku', 'warehouse', 'charge_currency']].drop_duplicates()
    df_stock_age_warehouse = df_temp.groupby(['sku', 'warehouse']).sum().reset_index()
    df_stock_age_warehouse = pd.merge(df_stock_age_warehouse, df_temp_2, how='left', on=['sku', 'warehouse'])
    df_stock_age_warehouse = pd.merge(df_stock_age_warehouse, df_temp_3, how='left', on=['sku', 'warehouse'])
    # warehouse_stock_age数据类型为 list , 可转化为 str
    df_stock_age_warehouse['warehouse_stock_age'] = df_stock_age_warehouse['warehouse_stock_age'].astype(str)
    # 判断是否存在超库龄库存, 及超出天数
    def exist_overage_stock(df):
        c1 = df['age_360_plus'] > 0
        c2 = df['age_270_plus'] > 0
        c3 = df['age_180_plus'] > 0
        c4 = df['age_150_plus'] > 0
        c5 = df['age_120_plus'] > 0
        c6 = df['age_90_plus'] > 0
        c7 = df['age_60_plus'] > 0
        c8 = df['age_30_plus'] > 0
        df['overage_level'] = np.select([c1, c2, c3, c4, c5, c6,c7,c8], [360, 270, 180, 150, 120, 90,60,30],0)
        return df
    df_stock_age_id = exist_overage_stock(df_stock_age_id)
    df_stock_age_warehouse = exist_overage_stock(df_stock_age_warehouse)

    # 最优子仓的选择，按优先级：库龄、仓租、库存数
    df_stock_age_id = df_stock_age_id.sort_values(
        by=['sku', 'warehouse', 'inventory_age', 'charge_total_price', 'warehouse_stock'],
        ascending=[True, True, False, False, False])
    df_best_id = df_stock_age_id[['sku', 'warehouse_id', 'warehouse_name', 'warehouse']].drop_duplicates(
        subset=['sku', 'warehouse'])
    df_best_id.rename(columns={'warehouse_id': 'best_warehouse_id', 'warehouse_name': 'best_warehouse_name'},
                      inplace=True)
    df_stock_age_id = pd.merge(df_stock_age_id, df_best_id, how='left', on=['sku', 'warehouse'])
    df_stock_age_warehouse = pd.merge(df_stock_age_warehouse, df_best_id, how='left', on=['sku', 'warehouse'])

    return df_stock_age_id, df_stock_age_warehouse

def temp_temp():
    """ """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # 调整库龄取数的日期，针对某个日期库龄数据不完整的情况
    n = 2
    sql = f"""
    SELECT  
        ya.sku sku, charge_currency, cargo_type, ya.warehouse_code warehouse_code, yw.id as warehouse_id, 
        yw.warehouse_name warehouse_name, ywc.name warehouse, c.new_price new_price,
        warehouse_stock, inventory_age, charge_total_price, 
        case when inventory_age >= 30 then warehouse_stock else 0 end as age_30_plus,
        case when inventory_age >= 60 then warehouse_stock else 0 end as age_60_plus,
        case when inventory_age >= 90 then warehouse_stock else 0 end as age_90_plus,
        case when inventory_age >= 120 then warehouse_stock else 0 end as age_120_plus,
        case when inventory_age >= 150 then warehouse_stock else 0 end as age_150_plus,
        case when inventory_age >= 180 then warehouse_stock else 0 end as age_180_plus,
        case when inventory_age >= 210 then warehouse_stock else 0 end as age_210_plus,
        case when inventory_age >= 270 then warehouse_stock else 0 end as age_270_plus,
        case when inventory_age >= 360 then warehouse_stock else 0 end as age_360_plus
    FROM yb_datacenter.yb_oversea_sku_age ya
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.warehouse_code = yw.warehouse_code
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    LEFT JOIN (
        SELECT
            distinct sku, product_status `产品状态`, title_cn `产品名称`,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
        FROM yibai_prod_base_sync.yibai_prod_sku
        ) c
    ON ya.sku = c.sku
    WHERE 
        date = formatDateTime(subtractDays(now(),{n}), '%Y-%m-%d') and status in (0,1) 
        -- and yw.warehouse_name not like '%独享%' 
        -- and yw.warehouse_name not like '%TT%'
        -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        -- and yw.id not in (339)   -- 剔除不常用仓库，避免最优子仓选到无运费的子仓上
    """
    df_stock_age = ck_client.ck_select_to_df(sql)
    df_stock_age.to_excel('F://Desktop//df_stock_age.xlsx', index=0)

def mx_age():
    """ """

    sql = """
         SELECT
             sku, w, cargo_owner_id, saas_stock, stock_age, new_price,
             case
                 when stock_age >= 30 and stock_age < 60 then 30
                 when stock_age >= 60 and stock_age < 90 then 60
                 when stock_age >= 90 and stock_age < 120 then 90
                 when stock_age >= 120 and stock_age < 150 then 120
                 when stock_age >= 150 and stock_age < 180 then 150
                 when stock_age >= 180 and stock_age < 270 then 180
                 when stock_age >= 270 and stock_age < 360 then 270
                 when stock_age >= 360 then 360
             else 0 end as overage_level, 
             case when stock_age >= 30 then saas_stock else 0 end as age_30_plus,       
             case when stock_age >= 60 then saas_stock else 0 end as age_60_plus,
             case when stock_age >= 90 then saas_stock else 0 end as age_90_plus,
             case when stock_age >= 120 then saas_stock else 0 end as age_120_plus,
             case when stock_age >= 150 then saas_stock else 0 end as age_150_plus,
             case when stock_age >= 180 then saas_stock else 0 end as age_180_plus,
             case when stock_age >= 270 then saas_stock else 0 end as age_270_plus,
             case when stock_age >= 360 then saas_stock else 0 end as age_360_plus
         FROM (
             SELECT sku, w, cargo_owner_id, saas_stock, stock_age, new_price
             FROM (
                 SELECT 
                     w, b.sku sku, b.client_sku client_sku, 
                     a.sku as oversea_sku, cargo_owner_id, 
                     instock_stock-out_stock as saas_stock, storage_age_date, 
                     toInt32(today() - toDate(storage_age_date)) as stock_age,
                     today()
                 FROM yb_datacenter.yibai_stock_age_detail a
                 LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                 ON a.sku = b.oversea_sku
                 -- WHERE w = 'YM-MX-2'
             ) a
             LEFT JOIN (
                SELECT
                    distinct sku, product_status `产品状态`, title_cn `产品名称`,
                    CASE 
                        when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                        when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                        when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                        when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                        else toFloat64(product_cost) 
                    END as `new_price`
                FROM yibai_prod_base_sync.yibai_prod_sku
             ) b on a.sku = b.sku
             WHERE saas_stock > 0
         )


     """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)


    df_mx_stock.to_excel('F://Desktop//df_mx_age.xlsx', index=0)


def generate_sku_monthly_report(
        df: pd.DataFrame,
        sku_col: str = 'sku',
        warehouse_col: str = 'warehouse',
        time_col: str = 'paytime',
        sales_col: str = 'total_price',
        profit_col: str = 'true_profit_new1',
        time_format: str = '%Y-%m',  # 年月格式，如'%Y-%m'→2026-02，'%Y%m'→202602
        fill_na: int = 0  # 无数据月份填充值
) -> pd.DataFrame:
    """
    生成SKU+仓库维度的分月销售额/利润宽表（匹配目标表头格式）

    参数说明：
    --------
    df : pd.DataFrame
        原始数据框，必须包含传入的各列
    sku_col : str
        SKU列名（默认'sku'）
    warehouse_col : str
        仓库列名（默认'warehouse'）
    time_col : str
        时间列名（默认'paytime'）
    stock_col : str
        库存列名（默认'available_stock'）
    sales_col : str
        销售额列名（默认'total_price'）
    profit_col : str
        利润列名（默认'true_profit_new1'）
    time_format : str
        年月格式化字符串（默认'%Y-%m'，可选'%Y%m'/'%m-%Y'等）
    stock_agg : str
        库存聚合方式（默认'max'，可选'sum'/'mean'/'first'）
    fill_na : int/float
        无数据月份的填充值（默认0）

    返回：
    ----
    pd.DataFrame
        宽表格式结果：sku + warehouse + 库存 + 分月销售额 + 分月利润
    """
    # ========== 步骤1：数据校验 ==========
    required_cols = [sku_col, warehouse_col, time_col, sales_col, profit_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"原始数据缺失必要列：{missing_cols}")

    # ========== 步骤2：时间处理 ==========
    df_base = df[['sku','warehouse']].drop_duplicates(subset=['sku','warehouse'])
    df_copy = df.copy()  # 避免修改原数据
    # 转换时间列并提取年月
    df_copy[time_col] = pd.to_datetime(df_copy[time_col], errors='coerce')
    if df_copy[time_col].isnull().all():
        raise ValueError(f"时间列{time_col}无有效日期，请检查数据格式")
    df_copy['year_month'] = df_copy[time_col].dt.strftime(time_format)

    # ========== 步骤3：聚合分月销售额/利润 ==========
    # 按SKU+仓库+年月聚合
    df_monthly = df_copy.groupby(
        [sku_col, warehouse_col, 'year_month'],
        as_index=False
    ).agg({
        sales_col: 'sum',
        profit_col: 'sum'
    })

    # ========== 步骤5：透视成宽表 ==========
    # 销售额透视
    pivot_sales = df_monthly.pivot_table(
        index=[sku_col, warehouse_col],
        columns='year_month',
        values=sales_col,
        fill_value=fill_na
    )
    pivot_sales.columns = [f'{col}_{sales_col}' for col in pivot_sales.columns]

    # 利润透视
    pivot_profit = df_monthly.pivot_table(
        index=[sku_col, warehouse_col],
        columns='year_month',
        values=profit_col,
        fill_value=fill_na
    )
    pivot_profit.columns = [f'{col}_{profit_col}' for col in pivot_profit.columns]

    # ========== 步骤6：合并数据并整理列顺序 ==========
    # 合并库存+销售额+利润
    df_final = df_base.merge(
        pivot_sales.reset_index(),
        on=[sku_col, warehouse_col],
        how='left'
    ).merge(
        pivot_profit.reset_index(),
        on=[sku_col, warehouse_col],
        how='left'
    ).fillna(fill_na)

    # 整理列顺序：SKU→仓库→库存→分月销售额→分月利润（按年月倒序）
    # 提取所有年月列并排序（倒序，最新月份在前）
    sales_cols = sorted([col for col in df_final.columns if f'_{sales_col}' in col], reverse=True)
    profit_cols = sorted([col for col in df_final.columns if f'_{profit_col}' in col], reverse=True)

    # 交替排列销售额和利润列（如2026-02_total_price → 2026-02_true_profit_new1 → 2026-01_...）
    mixed_cols = []
    for s_col, p_col in zip(sales_cols, profit_cols):
        mixed_cols.append(s_col)
        mixed_cols.append(p_col)

    # 最终列顺序
    final_cols = [sku_col, warehouse_col] + mixed_cols
    df_final = df_final[final_cols]

    return df_final

def get_line_new(df):
    # 一级产品线
    sql = f"""
        SELECT a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线`, c.develop_source_name type
        FROM yibai_prod_base_sync.yibai_prod_sku a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_category b on toInt32(a.product_category_id) = toInt32(b.id)
        LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source c ON a.develop_source = c.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    df_line['末级产品线'] = df_line['产品线路线'].str.split('->', expand=True).iloc[:, -1]
    df_line = df_line.drop_duplicates(subset='sku')

    col = ['sku', 'type', '一级产品线', '二级产品线', '末级产品线']

    df_result = pd.merge(df, df_line[col], how='left', on=['sku'])

    return df_result

# 海外仓大件sku的销库情况
def get_large_sku():
    """ 海外仓大件sku """
    sql = """
        SELECT sku, warehouse, best_warehouse_name, new_price, available_stock, overage_level, age_90_plus
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_sku_temp_info)
        and best_warehouse_name not like '%精品%'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = get_line_new(df)

    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    df.drop(['数量', '重量来源'], axis=1, inplace=True)

    # 筛选大件
    c1 = ((df['长']<=45) & (df['宽']<= 35) & (df['高']<=20) & (df['重量']<=9000))
    c2 = ((df['长'] <= 120) & (df['宽'] <= 100) & (df['高'] <= 80) & (df['重量'] <= 31500))
    c3 = ((df['长'] <= 245) & (df['宽'] <= 200) & (df['高'] <= 150) & (df['重量'] > 31500))
    df['货型'] = np.select([c1, c2, c3], ['小件', '大件', '超大件'], '异形件')
    df = df[df['货型'] != '小件']

    # 销库情况(按日聚合）
    df_order = get_dashbord_order()

    # 1、按月聚合
    df_order['paytime'] = pd.to_datetime(df_order['paytime'], errors='coerce')
    df_order['year_month'] = df_order['paytime'].dt.strftime('%Y-%m')
    # 按SKU+仓库+年月聚合
    df_group = df_order.groupby(['sku', 'warehouse', 'year_month'],as_index=False).agg(
        {'total_price': 'sum','true_profit_new1': 'sum'})
    df_group = pd.merge(df_group, df, how='inner', on=['sku', 'warehouse'])
    df_group.to_excel('F://Desktop//df_group_sales.xlsx', index=0)
    # 2、按日聚合
    # df_group = df_order.groupby(['sku', 'warehouse','paytime']).agg(
    #     {'total_price':'sum', 'true_profit_new1':'sum', 'release_money':'sum'}).reset_index()

    # 统计指标
    # 1、月度销售额、利润率
    # df_sales = generate_sku_monthly_report(df_group)
    #
    # df_sales = pd.merge(df, df_sales, how='left', on=['sku','warehouse'])
    # df_sales.to_excel('F://Desktop//df_sales.xlsx', index=0)





if __name__ == '__main__':
    # df = get_sku_info()
    # yb_order()
    # tt_order()
    # get_sku_status()
    # get_new_price()
    # temu_cant_sales()
    # get_mx_dwm()
    # get_clear_order()

    # fba_oversea_fee()

    # temp_temp()
    # mx_age()
    get_large_sku()