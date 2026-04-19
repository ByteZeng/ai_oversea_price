"""
TT海外仓相关需求
1、订单
2、库存库龄
3、运费
"""
##
import warnings
import datetime,time
import pandas as pd
import numpy as np
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_price_adjust_tt import tt_get_sku, tt_get_warehouse, get_rate
from all_auto_task.oversea_temu_price import get_temu_account
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from utils.utils import  save_df, make_path
from utils import utils

warnings.filterwarnings('ignore')

##
def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, develop_source, b.category_path as `产品线路线`
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    # df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    # df_line['四级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')
    c1 = df_line['develop_source'].isin([14,15,22])
    c2 = df_line['产品名称'].str.contains('通拓')
    df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)
    df = pd.merge(df, df_line[['sku','is_tt_sku','一级产品线','二级产品线']], how='left', on=['sku'])

    return df

def tt_get_listing_order():
    """
    获取temu订单数据
    """
    sql = """
    SELECT 
        id warehouse_id,warehouse_name,
        case when warehouse_type in (5,6) then '平台仓' WHEN warehouse_type=1 THEN '国内仓' when warehouse_type=8 then '海外仓'
        WHEN warehouse_type=7 THEN '分销' WHEN warehouse_type=9 THEN '进口仓' ELSE '' END AS `仓库类型`, country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country='UK' THEN '英国仓'
            WHEN country='GB' THEN '英国仓'
            WHEN country in ('CS','DE','CZ','PL') THEN '德国仓'
            WHEN country='FR' THEN '法国仓'
            WHEN country='IT' THEN '意大利仓'
            WHEN country='AU' THEN '澳洲仓'
            WHEN country='ES' THEN '西班牙仓'
            WHEN country='CA' THEN '加拿大仓'
            WHEN country='JP' THEN '日本仓'
            WHEN country='MX' THEN '墨西哥仓'
            WHEN country='UY' THEN '乌拉圭仓'
            WHEN country='BR' THEN '巴西仓'
            WHEN country='RU' THEN '俄罗斯仓'
            ELSE '其他' 
        END AS warehouse
    FROM tt_logistics_tms_sync.tt_warehouse 
    """
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据t')
    warehouse = conn.ck_select_to_df(sql)
    warehouse['warehouse_id'] = warehouse['warehouse_id'].apply(lambda x:int('10000'+str(x)))

    sql = """
    SELECT 
        id warehouse_id,name warehouse_name,
        case when type in ('fba','platform') then '平台仓' WHEN type = 'inland' THEN '国内仓' when type in ('third','overseas') then '海外仓'
        WHEN type in ('consignment','transit') THEN '分销'  ELSE '' END AS `仓库类型`,country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country='UK' THEN '英国仓'
            WHEN country='GB' THEN '英国仓'
            WHEN country in ('CS','DE','CZ','PL') THEN '德国仓'
            WHEN country='FR' THEN '法国仓'
            WHEN country='IT' THEN '意大利仓'
            WHEN country='AU' THEN '澳洲仓'
            WHEN country='ES' THEN '西班牙仓'
            WHEN country='CA' THEN '加拿大仓'
            WHEN country='JP' THEN '日本仓'
            WHEN country='MX' THEN '墨西哥仓'
            WHEN country='UY' THEN '乌拉圭仓'
            WHEN country='BR' THEN '巴西仓'
            WHEN country='RU' THEN '俄罗斯仓'
            ELSE '其他'
        END AS warehouse
    FROM yb_datacenter.yb_warehouse
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    yb_warehouse = conn_mx.ck_select_to_df(sql)
    warehouse = warehouse.append(yb_warehouse,ignore_index=True)

    # 筛选海外仓
    warehouse = warehouse[warehouse['仓库类型']=='海外仓']
    #
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
    date_30 = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        AND order_id not like '%%-RE' 
        AND payment_time>='{date_start}'
        and platform_code!='SPH' AND total_price!=0
        and platform_code = 'ALI'
        -- LIMIT 1000
    )
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        b.asinval as asin,
        d.sku as sku,
        toInt64(d.quantity) as quantity,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.payment_time as created_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        toFloat64(c.refund_amount) as refund_amount,
        toFloat64(c.shipping_price) as shipping_price,
        toFloat64(c.shipping_cost) as `预估运费`,
        toFloat64(c.true_shipping_fee) as `实际运费`,
        c.product_price, c.commission_fees, c.pay_cost, c.escrow_tax, c.purchase_cost_new1,
        c.ship_cost_second, c.true_shipping_fee, c.first_carrier_cost, c.duty_cost, c.processing, c.package_cost, 
        c.extra_price, c.exceedprice, c.stock_price, c.exchange_price, c.true_profit2, c.true_profit_rate2
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
        """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓MRP')
    order_base = conn_mx.ck_select_to_df(sql)
    print(order_base.info())
    # order_base['total_price'] = order_base['total_price'] * order_base['currency_rate']
    # order_base['true_profit'] = order_base['true_profit'] * order_base['currency_rate']
    # order_base = order_base.sample(10000)
    #
    order_base = order_base.merge(warehouse,on='warehouse_id',how='inner')
    #
    order_base['warehouse_id'] = order_base['warehouse_id'].astype(str)
    order_base = order_base[~order_base['warehouse_id'].str.contains('10000')]

    return order_base
# order_ebay = tt_get_listing_order()

## 运费数据
def temp():
    sql = """
        SELECT 
            sku, best_warehouse_id,best_warehouse_name,  new_price, total_cost, shippingCost, firstCarrierCost,
            overage_level, sales_status, platform platform_code, country, target_profit_rate, price_rmb, price
        FROM over_sea.tt_dwm_oversea_price
        WHERE date_id = '2024-10-26' and total_cost > 0 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_price = conn.read_sql(sql)
    ##
    df_order = order_base[order_base['platform_code'].isin(['ALI','AMAZON','EB'])]
    df_order['warehouse_id'] = df_order['warehouse_id'].astype(int)
    df_order['ship_country'] = df_order['ship_country'].replace('GB','UK')
    df_order = pd.merge(df_order, df_price, how='left', left_on=['sku','platform_code','ship_country'],right_on=['sku','platform_code','country'])
    ##
    df_order.to_excel('F:\Desktop\df_order.xlsx', index=0)

    ##
    sql = f"""
        with  account_list as (
            select distinct id,account_id from tt_sale_center_system.tt_system_account
            where platform_code='EB' and status=1 and is_del=0
        )
        SELECT 
            a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
            CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
            b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
            d.account_id, a.start_price,
            f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
        FROM (
            SELECT item_id,sku,start_price 
            FROM tt_sale_center_listing_sync.tt_ebay_online_listing_sales_sku 
            WHERE sku = 'RM15433Y-2'
        ) a 
        INNER JOIN (
            SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
            from tt_sale_center_listing_sync.tt_ebay_online_listing 
            where  warehouse_category_id !=1 and listing_status = 1 
            and account_id in (select distinct id from account_list)
        ) b  ON a.item_id=b.item_id
        LEFT JOIN (
            SELECT item_id, shipping_service_cost
            FROM tt_sale_center_listing_sync.tt_ebay_item_shipping
            WHERE shipping_status=1 and shipping_service_priority=1
        ) f  ON a.item_id = f.item_id
        LEFT JOIN (
            SELECT site_id,site,site1 AS `站点`,is_open,site_code 
            FROM domestic_warehouse_clear.site_table_ebay 
            where is_open='是'
        ) c  ON b.siteid = c.site_id
        LEFT JOIN account_list d on b.account_id = d.id
        INNER JOIN (
            SELECT id, warehouse
            FROM tt_sale_center_common_sync.tt_common_big_warehouse
            WHERE warehouse_type_id in (2, 3)
        ) e ON b.warehouse_category_id = e.id
        
    """
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    df_ebay_listing = conn_ck.ck_select_to_df(sql)

## tt海外仓订单宽表 & 订单监控报表

def get_sku_info(df):
    sql = """
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
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_temp[['sku','new_price']], how='left', on=['sku'])
    df['new_price'] = df['new_price'].fillna(0).astype(float)
    return df

def tt_dwm_order():
    """
    TT获取订单数据
    """
    # 获取tt海外仓
    warehouse = tt_get_warehouse()

    # 取订单表
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    # date_start = '2025-04-01'
    # date_end = '2025-05-01'
    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        and payment_status=1
        and order_id not like '%%-RE' 
        AND payment_time>='{date_start}'
        -- AND payment_time<'date_end'
        and platform_code!='SPH' AND total_price!=0
        -- and platform_code = 'TEMU'
        -- LIMIT 1000
    )
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        d.sku as sku,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.payment_time as payment_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        toInt64(d.quantity) as quantity,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        d.quantity as release_money,
        d.sales_status as sales_status
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
        """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    order_base = conn_mx.ck_select_to_df(sql)
    # 匹配成本
    order_base = get_sku_info(order_base)
    order_base['release_money'] = order_base['quantity'] * order_base['new_price']
    print(order_base.info())

    # 匹配仓库
    col = ['warehouse_id', 'warehouse_name', 'warehouse']
    order_base = order_base.merge(warehouse[col], on='warehouse_id', how='inner')
    # 净利润计算。先取默认净利率，再取站点维度
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    sql = """
        select platform_code, max(net_profit2)/100 as platform_zero_1
        from tt_sale_center_listing_sync.tt_listing_profit_config 
        where shipping_type = 2 and  is_del = 0 and status = 1
        group by platform_code
    """
    df_fee1 = conn_ck.ck_select_to_df(sql)

    sql = """
        select
            platform_code,
            CASE
                WHEN site = 'US' THEN '美国仓'
                WHEN site IN ('UK', 'GB') THEN '英国仓'
                WHEN site IN ('CZ', 'CS', 'DE', 'PL') THEN '德国仓'
                WHEN site = 'FR' THEN '法国仓'
                WHEN site = 'IT' THEN '意大利仓'
                WHEN site = 'AU' THEN '澳洲仓'
                WHEN site IN ('ES', 'SP') THEN '西班牙仓'
                WHEN site = 'CA' THEN '加拿大仓'
                WHEN site = 'JP' THEN '日本仓'
                WHEN site = 'MX' THEN '墨西哥仓'
                WHEN site ='RU' THEN '俄罗斯仓'
                WHEN site ='BR' THEN '巴西仓'
                WHEN site ='PH' THEN '菲律宾仓'
                WHEN site ='TH' THEN '泰国仓'
                WHEN site ='MY' THEN '马来西亚仓'
                WHEN site ='VN' THEN '越南仓'
                WHEN site ='ID' THEN '印尼仓'
                ELSE 'UNKNOWN WAREHOUSE'
            END as warehouse,
            min(net_profit2)/100 as platform_zero_2
        from tt_sale_center_listing_sync.tt_listing_profit_config 
        where 
            platform_code in ('ALI', 'EB', 'AMAZON') 
            and shipping_type = 2 and  is_del = 0 and status = 1
        group by platform_code,warehouse 
    """
    df_fee2 = conn_ck.ck_select_to_df(sql)
    order_base = order_base.merge(df_fee1, on=['platform_code'], how='left')
    order_base = order_base.merge(df_fee2, on=['platform_code', 'warehouse'], how='left')
    order_base['platform_zero'] = np.where(~order_base['platform_zero_2'].isna(),
                                           order_base['platform_zero_2'], order_base['platform_zero_1'])

    order_base["platform_zero"] = order_base["platform_zero"].astype(float)
    order_base["real_profit"] = order_base["true_profit"] - order_base["platform_zero"] * order_base["total_price"]
    order_base["real_profit"] = order_base["real_profit"].astype(float).round(4)

    order_base.drop(['platform_zero_1', 'platform_zero_2', 'platform_zero', 'currency', 'new_price'], axis=1,
                    inplace=True)
    order_base = order_base.rename(columns={'true_profit': 'true_profit_new1'})
    order_base['payment_time'] = order_base['payment_time'].dt.strftime('%Y-%m-%d')
    #
    # order_base['warehouse_id'] = order_base['warehouse_id'].astype(str)
    # order_base = order_base[~order_base['warehouse_id'].str.contains('10000')]
    # order_base['warehouse_id'] = order_base['warehouse_id'].astype(int)

    # 重复订单号的销售额、利润置为0
    order_base['sales_status'] = order_base['sales_status'].fillna('')
    column_order = ['负利润加快动销', '正利润加快动销', '正常','','nan','-']
    print(order_base['sales_status'].unique())
    order_base['sales_status'] = pd.Categorical(order_base['sales_status'], categories=column_order, ordered=True)
    order_base = order_base.sort_values(by=['order_id', 'sales_status'])

    order_base['rank'] = order_base.groupby(['order_id', 'sales_status']).cumcount() + 1
    for c in ['total_price','true_profit_new1','real_profit']:
        order_base[c] = np.where(order_base['rank'] != 1, 0, order_base[c])
    order_base.drop('rank', axis=1, inplace=True)
    order_base['sales_status'] = order_base['sales_status'].astype(str)

    # CK存表
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    table_name = 'tt_dashbord_new_data1'
    sql = f"""
        ALTER TABLE yibai_oversea.{table_name}
        DELETE where payment_time >= '{date_start}'
    """
    conn_ck.ck_execute_sql(sql)
    #
    conn_ck.ck_insert(order_base, table_name, if_exist='append')

    return None


tt_dwm_order()
## 监控报表
def tt_get_oversea_order():
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)
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


tt_get_oversea_order()


## tt线下库龄明细存表
def save_data():
    df = pd.read_csv('F://Ding_workspace//xls-download-2024-10-30-14-24-33.csv', dtype={'sku':str}, encoding='GBK')
    col = ['spu','sku','最小类别','品牌','仓库','入仓时间','库存数','可用库存数','上线时间','销售状态','库龄']
    df = df[col]
    print(df.head(5))
    print(df.info())
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_stock_age_temp', if_exists='replace')
# save_data()

## ebay链接sku校验
def check_ebay_sku():
    sql = """
        SELECT
            item_id, account_id, sku, warehouse
        FROM over_sea.tt_oversea_ebay_listing_all
        WHERE date_id = '2024-10-30' and warehouse is null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay = conn.read_sql(sql)
    df_ebay = df_ebay.drop_duplicates(subset='sku')

    df_sku = tt_get_sku()
    df_sku['is_tt_sku'] = 1
    df = pd.merge(df_ebay, df_sku, how='left', on='sku')
    print(df.info())
# check_ebay_sku()

## 清仓清单备份
def clear_sku_backup():
    sql = """
        SELECT * 
        FROM oversea_sale_status 
        WHERE end_time IS NULL
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)
    df_status.to_excel('F://Desktop//df_status.xlsx', index=0)
    #
    df = pd.read_excel('F://Desktop//tt清仓sku备份.xlsx')
    #
    df['start_time'] = '2024-10-31'
    df['end_time'] = np.nan
    #
    conn.to_sql(df, 'tt_oversea_sale_status_temp', if_exists='replace')
##
sql = """
    SELECT *
    FROM oversea_sale_status
    WHERE end_time IS NULL
    
    UNION ALL
    
    SELECT *
    FROM tt_oversea_sale_status_temp
    WHERE end_time IS NULL
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_status = conn.read_sql(sql)
df_status = df_status.drop_duplicates()
##
df_status = df_status.drop_duplicates(subset=['sku','warehouse'])

##
df_status.to_excel('F://Desktop//df_status.xlsx', index=0)
##
sql = """
 SELECT 
     sku, title, linest, last_linest,warehouse_id, warehouse_name, warehouse, available_stock, on_way_stock, 
     new_price
 FROM yb_datacenter.v_oversea_stock a
 LEFT JOIN (
     SELECT 
         a.sku sku, title_cn title, splitByString('->', b.category_path)[1] as linest,
         arrayElement(splitByString('->', b.category_path),-1) as last_linest,
         CASE 
             when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
             when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
             when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
             when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
             else toFloat64(product_cost) 
         END as `new_price`
     FROM yibai_prod_base_sync.yibai_prod_sku a
     LEFT JOIN yibai_prod_base_sync.yibai_prod_category b
     on toInt32(a.product_category_id) = toInt32(b.id)
 ) b on a.sku = b.sku
  WHERE 
     warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
     and warehouse_other_type = 2 and warehouse_name not like '%独享%'
     and available_stock > 0
 ORDER BY available_stock desc
 """
ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
df_sku_stock = ck_client.ck_select_to_df(sql)
## tt库存中间表重写优化：dwm_sku_info
def tt_get_stock():
    """
    从海外仓库存表，获取SKU及库存信息详情
    """
    # 初始库存表的视图：v_oversea_stock
    t1 = time.time()
    sql = '''
    select
        sku, title, new_price, gross, warehouse_id, dev_type as type, product_status, last_linest, linest, 
        sum_available_stock as available_stock, sum_available_stock_money as available_stock_money, 
        sum_on_way_stock as on_way_stock, sku_create_time as create_time, 
        product_size, product_package_size, best_warehouse, warehouse
    from (
        with 
        [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
        ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
        '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
        '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
        '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr	 
        select
            ps.sku as sku, pd.title_cn as title, ps.new_price as new_price, ps.product_weight_pross as gross, 
            ps.warehouse_code as warehouse_code, ps.warehouse_id as warehouse_id,
            case
                when p.state_type = '1' then '常规产品'
                when p.state_type = '2' then '试卖产品'
                when p.state_type = '3' then '亚马逊产品'
                when p.state_type = '4' then '通途产品'
                when p.state_type = '5' then '亚马逊服装'
                when p.state_type = '6' then '国内仓转海外仓'
                when p.state_type = '9' then '代销产品'
                else '未知类型'
            end as dev_type,
            transform(p.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
            pl.linelist_cn_name as last_linest, splitByString('>>', pl.path_name)[1] as linest,
            ps.available_stock as available_stock, ps.available_stock*toFloat64(ps.new_price) as available_stock_money,  
            ps.on_way_stock as on_way_stock, ps.create_time as sku_create_time, 
            concat(toString(ps.product_length), '*', toString(ps.product_width), '*', toString(ps.product_height)) as product_size, 
            concat(toString(ps.pur_lenght_pack), '*',toString(ps.pur_width_pack), '*', toString(ps.pur_height_pack) ) as product_package_size, 
            ps.warehouse_name as best_warehouse,
            ps.warehouse as warehouse,
            sum(ps.available_stock) over w as sum_available_stock, 
            sum(available_stock_money) over w as sum_available_stock_money, 
            sum(ps.on_way_stock) over w as sum_on_way_stock
        from
            (
            select 
                ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name 
                ,ps.warehouse_code as warehouse_code,ps.warehouse_id as warehouse_id,ps.on_way_stock on_way_stock 
                ,ps.available_stock available_stock ,
                yps.new_price as new_price
                -- if(isnull(yps.new_price),yps1.new_price,yps.new_price) as new_price
                ,if(empty(yps.pur_weight_pack),yps1.pur_weight_pack,yps.pur_weight_pack) as product_weight_pross
                ,if(empty(yps.product_length),yps1.product_length ,yps.product_length )  as product_length
                ,if(empty(yps.product_width),yps1.product_width ,yps.product_width ) as product_width
                ,if(empty(yps.product_height),yps1.product_height ,yps.product_height ) as product_height
                ,if(empty(yps.pur_length_pack),yps1.pur_length_pack ,yps.pur_length_pack ) as pur_lenght_pack
                ,if(empty(yps.pur_width_pack),yps1.pur_width_pack ,yps.pur_width_pack ) pur_width_pack
                ,if(empty(yps.pur_height_pack),yps1.pur_height_pack ,yps.pur_height_pack ) pur_height_pack
                ,if(empty(toString(yps.create_time)),yps1.create_time,yps.create_time) as create_time
            from 
                (
                select 
                    * except (available_stock),
                    case 
                        WHEN sku LIKE 'GB-%' THEN REPLACE(sku,'GB-','') 
                        WHEN sku LIKE 'DE-%' THEN REPLACE(sku,'DE-','') 
                        WHEN sku LIKE 'FR-%' THEN REPLACE(sku,'FR-','') 
                        WHEN sku LIKE 'ES-%' THEN REPLACE(sku,'ES-','') 
                        WHEN sku LIKE 'IT-%' THEN REPLACE(sku,'IT-','') 
                        WHEN sku LIKE 'AU-%' THEN REPLACE(sku,'AU-','') 
                        WHEN sku LIKE 'CA-%' THEN REPLACE(sku,'CA-','') 
                        WHEN sku LIKE 'JP-%' THEN REPLACE(sku,'JP-','') 
                        WHEN sku LIKE 'US-%' THEN REPLACE(sku,'US-','') 
                        WHEN sku LIKE '%DE' THEN REPLACE(sku,'DE','') 
                        else sku 
                    end as skuu,
                    -- 2023-04-26 非澳洲仓下万邑通仓库可用库存不加干预（原来为全部为设置为0），进入调价逻辑
                    available_stock
                from yb_datacenter.v_oversea_stock 
                where 
                    warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
                    and warehouse_other_type = 2 and warehouse_name not like '%独享%'
                    and available_stock > 0
                order by available_stock desc
                ) ps
            left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            having new_price<>''
            ) ps
        left join yibai_prod_base_sync.yibai_prod_sku p on ps.sku = p.sku
        left join yibai_prod_base_sync.yibai_prod_category pl
        on toInt32(a.product_category_id) = toInt32(pl.id)
        -- left join yb_datacenter.yb_product_linelist pl on pl.id = toUInt64(p.product_linelist_id)
        -- 2023-04-26 剔除原有的基于 warehouse_id, state_type, product_status 的筛选
        window w as (partition by sku, warehouse_id)
        order by available_stock desc, warehouse_id desc
    ) a
    limit 1 by sku, warehouse_id
    '''
    sql = """
    SELECT 
        sku, warehouse_id, warehouse_name, warehouse, available_stock, on_way_stock
    FROM yb_datacenter.v_oversea_stock a
    WHERE 
        warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
        and warehouse_other_type = 2 and warehouse_name not like '%独享%'
        and available_stock > 0
    ORDER BY available_stock desc
    LEFT JOIN (
        SELECT 
            a.sku sku, title_cn title, splitByString('>>', b.category_path)[1] as linest,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
        FROM yibai_prod_base_sync.yibai_prod_sku a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_category b
        on toInt32(a.product_category_id) = toInt32(b.id)
    ) b on a.sku = b.sku
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df_sku_stock = ck_client.ck_select_to_df(sql)

    print('Time passed {:.4f}'.format(time.time() - t1))
    df_sku_stock.columns = [i.split('.')[-1] for i in df_sku_stock.columns.to_list()]
    df_sku_stock['available_stock'] = np.where(df_sku_stock['available_stock'] < 0, 0, df_sku_stock['available_stock'])
    df_sku_stock['available_stock_money'] = np.where(df_sku_stock['available_stock_money'] < 0, 0,
                                                     df_sku_stock['available_stock_money'])

    return df_sku_stock
# tt_sku = tt_get_stock()
##
# 获取SKU的库龄数据
def tt_get_stock_age():
    """
    获取库龄数据
    处理库龄数据
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = '''
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
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.order_warehouse_code = yw.warehouse_code
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    WHERE 
        date = formatDateTime(subtractDays(now(),2), '%Y-%m-%d') and status in (0,1)
        and yw.warehouse_name not like '%独享%' 
        -- and ya.order_warehouse_code like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    '''
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 
    and warehouse_other_type = 2
    """
    df_stock = ck_client.ck_select_to_df(sql)
    df_stock_age = pd.merge(df_stock_age, df_stock, how='inner', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse_id 聚合
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].astype('float')
    df_stock_age_base = df_stock_age[
        ['sku', 'charge_currency', 'cargo_type', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    df_stock_age_info = df_stock_age[
        ['sku', 'warehouse_id', 'warehouse', 'warehouse_stock', 'inventory_age', 'charge_total_price', 'age_30_plus',
         'age_60_plus',
         'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']]
    df_stock_age_info.loc[:, 'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg(
        {'warehouse_stock': 'sum', 'inventory_age': 'max', 'charge_total_price': 'sum', 'age_30_plus': 'sum',
         'age_60_plus': 'sum', 'age_90_plus': 'sum',
         'age_120_plus': 'sum', 'age_150_plus': 'sum', 'age_180_plus': 'sum', 'age_270_plus': 'sum',
         'age_360_plus': 'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus', 'age_60_plus', 'age_90_plus',
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
        df['overage_level'] = np.select([c1, c2, c3, c4, c5, c6, c7, c8], [360, 270, 180, 150, 120, 90, 60, 30])
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


##
# 2、库存结构
def tt_get_oversea_stock():
    date_today = datetime.datetime.now().strftime('%Y-%m-%d')
    # date_today = datetime.date.today() - datetime.timedelta(days=1)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    delete from ads_oversea_stock where date_id='{date_today}'
    """
    conn.execute(sql)

    sql = f"""
        SELECT
            date_id, sku, best_warehouse_name, warehouse, new_price, available_stock,available_stock_money, warehouse_stock, overage_level, 
            age_90_plus, age_120_plus, age_150_plus, age_180_plus, age_270_plus,age_360_plus, charge_total_price_rmb,day_sales,
            estimated_sales_days,overage_esd, age_90_plus-age_120_plus as 90_120, age_120_plus-age_150_plus as 120_150,
            age_150_plus-age_180_plus 150_180, age_180_plus-age_270_plus 180_270, age_270_plus-age_360_plus 270_360,
            age_90_plus*new_price age_90_money, age_120_plus*new_price age_120_money, age_150_plus*new_price age_150_money,
            age_180_plus*new_price age_180_money, age_270_plus*new_price age_270_money, age_360_plus*new_price age_360_money
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock = conn.read_sql(sql)
    df_stock = df_stock.drop_duplicates()
    conn.to_sql(df_stock, 'ads_oversea_stock', if_exists='append')

##
def get_stock():
    """
    从海外仓库存表，获取SKU及库存信息详情
    """
    # 初始库存表的视图：v_oversea_stock
    t1 = time.time()
    sql = '''
    select
        sku, title, new_price, gross, warehouse_id, dev_type as type, product_status, last_linest, linest, 
        sum_available_stock as available_stock, sum_available_stock_money as available_stock_money, 
        sum_on_way_stock as on_way_stock, sku_create_time as create_time, 
        product_size, product_package_size, best_warehouse, warehouse
    from (
        with 
        [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
        ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
        '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
        '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
        '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr	 
        select
            ps.sku as sku, pd.title as title, ps.new_price as new_price, ps.product_weight_pross as gross, 
            ps.warehouse_code as warehouse_code, ps.warehouse_id as warehouse_id,
            case
                when p.state_type = '1' then '常规产品'
                when p.state_type = '2' then '试卖产品'
                when p.state_type = '3' then '亚马逊产品'
                when p.state_type = '4' then '通途产品'
                when p.state_type = '5' then '亚马逊服装'
                when p.state_type = '6' then '国内仓转海外仓'
                when p.state_type = '9' then '代销产品'
                else '未知类型'
            end as dev_type,
            transform(p.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
            pl.linelist_cn_name as last_linest, splitByString('>>', pl.path_name)[1] as linest,
            ps.available_stock as available_stock, ps.available_stock*toFloat64(ps.new_price) as available_stock_money,  
            ps.on_way_stock as on_way_stock, ps.create_time as sku_create_time, 
            concat(toString(ps.product_length), '*', toString(ps.product_width), '*', toString(ps.product_height)) as product_size, 
            concat(toString(ps.pur_lenght_pack), '*',toString(ps.pur_width_pack), '*', toString(ps.pur_height_pack) ) as product_package_size, 
            ps.warehouse_name as best_warehouse,
            ps.warehouse as warehouse,
            sum(ps.available_stock) over w as sum_available_stock, 
            sum(available_stock_money) over w as sum_available_stock_money, 
            sum(ps.on_way_stock) over w as sum_on_way_stock
        from
            (
            select 
                ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name 
                ,ps.warehouse_code as warehouse_code,ps.warehouse_id as warehouse_id,ps.on_way_stock on_way_stock 
                ,ps.available_stock available_stock ,if(isnull(yps.new_price),yps1.new_price,yps.new_price) as new_price
                ,if(isnull(yps.pur_weight_pack),yps1.pur_weight_pack,yps.pur_weight_pack) as product_weight_pross
                ,if(isnull(yps.product_length),yps1.product_length ,yps.product_length )  as product_length
                ,if(isnull(yps.product_width),yps1.product_width ,yps.product_width ) as product_width
                ,if(isnull(yps.product_height),yps1.product_height ,yps.product_height ) as product_height
                ,if(isnull(yps.pur_length_pack),yps1.pur_length_pack ,yps.pur_length_pack ) as pur_lenght_pack
                ,if(isnull(yps.pur_width_pack),yps1.pur_width_pack ,yps.pur_width_pack ) pur_width_pack
                ,if(isnull(yps.pur_height_pack),yps1.pur_height_pack ,yps.pur_height_pack ) pur_height_pack
                ,if(empty(toString(yps.create_time)),yps1.create_time,yps.create_time) as create_time
            from 
                (
                select 
                    * except (available_stock),
                    case 
                        WHEN sku LIKE 'GB-%' THEN REPLACE(sku,'GB-','') 
                        WHEN sku LIKE 'DE-%' THEN REPLACE(sku,'DE-','') 
                        WHEN sku LIKE 'FR-%' THEN REPLACE(sku,'FR-','') 
                        WHEN sku LIKE 'ES-%' THEN REPLACE(sku,'ES-','') 
                        WHEN sku LIKE 'IT-%' THEN REPLACE(sku,'IT-','') 
                        WHEN sku LIKE 'AU-%' THEN REPLACE(sku,'AU-','') 
                        WHEN sku LIKE 'CA-%' THEN REPLACE(sku,'CA-','') 
                        WHEN sku LIKE 'JP-%' THEN REPLACE(sku,'JP-','') 
                        WHEN sku LIKE 'US-%' THEN REPLACE(sku,'US-','') 
                        WHEN sku LIKE '%DE' THEN REPLACE(sku,'DE','') 
                        else sku 
                    end as skuu,
                    -- 2023-04-26 非澳洲仓下万邑通仓库可用库存不加干预（原来为全部为设置为0），进入调价逻辑
                    available_stock
                from yb_datacenter.v_oversea_stock 
                where warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
                and warehouse_other_type = 2 and warehouse_name not like '%独享%'
                order by available_stock desc
                ) ps
            left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            -- having new_price<>''
            ) ps
        left join yb_datacenter.yb_product p on ps.sku = p.sku
        left join yb_datacenter.yb_product_description pd on pd.sku = p.sku and pd.language_code = 'Chinese'
        left join yb_datacenter.yb_product_linelist pl on pl.id = toUInt64(p.product_linelist_id)
        -- 2023-04-26 剔除原有的基于 warehouse_id, state_type, product_status 的筛选
        window w as (partition by sku, warehouse)
        order by available_stock desc, warehouse_id desc
    ) a
    limit 1 by sku, warehouse
    '''

    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yb_datacenter')
    df_sku_stock = ck_client.ck_select_to_df(sql)

    print('Time passed {:.4f}'.format(time.time() - t1))
    df_sku_stock.columns = [i.split('.')[-1] for i in df_sku_stock.columns.to_list()]
    df_sku_stock['available_stock'] = np.where(df_sku_stock['available_stock'] < 0, 0, df_sku_stock['available_stock'])
    df_sku_stock['available_stock_money'] = np.where(df_sku_stock['available_stock_money'] < 0, 0,
                                                     df_sku_stock['available_stock_money'])
    df_sku_stock = df_sku_stock.sample(1000)
    return df_sku_stock
# stock = get_stock()


##
def get_14548_stock():
    sql = """
        SELECT *
        FROM yb_datacenter.v_oversea_stock
        WHERE warehouse_name like '%14548%'
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_ym = conn_ck.ck_select_to_df(sql)
    #
    df_stock_ym.to_excel('F://Desktop//df_stock_ym.xlsx', index=0)

def get_order_temp():
    sql = f"""
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, payment_time, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity,  release_money, sales_status, ship_country
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '2025-01-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and platform_code = 'AMAZON'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info.to_excel('F://Desktop//tt_amazon_order2.xlsx', index=0)
get_order_temp()
##


##
def yunfei_temp():
    sql = """
        SELECT 
            sku, warehouseId, warehouseName, shipCode, shipName, totalCost as `总运费`, shippingCost `尾程`, 
            firstCarrierCost `头程`, dutyCost `关税`,
            overseasFee `海外仓处理费`, remoteExtraFee `偏远附加费`, extraSizeFee `超尺寸附加费`
        FROM yibai_oversea.oversea_transport_fee_daily
        WHERE date_id = 20241107 and warehouseName in ('谷仓澳洲悉尼仓', '万邑通澳洲墨尔本仓') and platform = 'AMAZON'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_data = conn_ck.ck_select_to_df(sql)

    df_data.to_excel('F://Desktop//df_data.xlsx', index=0)

yunfei_temp()

##
def get_ali_listing():
    """

    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2024-11-07'
    sql = f"""
    SELECT *
    FROM over_sea.tt_ali_oversea_listing
    WHERE date_id = '{date_today}'
    """
    # product_id, account_id, pop_choice_status, sku, sku_price, sku_code, aliexpress_currency_code, country
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df['product_id'] = df['product_id'].astype(str)
    df.to_excel('F://Desktop//tt_ali_listing.xlsx', index=0)


get_ali_listing()
##
sql = """
    SELECT count(1)
    FROM (
        SELECT 
            sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t, 
            (toFloat64(overseasFee) + toFloat64(packFee)) as class2_t,
            (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(firstCarrierCost) + toFloat64(dutyCost) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_t
        FROM yibai_oversea.oversea_transport_fee_daily
        WHERE date_id = toYYYYMMDD(today()) and toFloat64(firstCarrierCost) = 0
    ) t
    settings max_memory_usage = 20000000000
"""
conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
df_fee_2 = conn_ck.ck_select_to_df(sql)
##
def stock_temp():
    sql = f"""
        select sku, warehouse, best_warehouse_name warehouse_name, available_stock, overage_level,
        estimated_sales_days, esd_bins
        from dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
        and warehouse in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓')
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)

    df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)
stock_temp()
##
def yibai_get_aliexpress_listing():
    print("===aliexpress刊登链接数据===")
    sql = """
        SELECT 
            a.product_id, d.account_id, a.sku sku,sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
            b.sku_property_id, c.name_en, d.freight_template_id, f.price_mode_name,pop_choice_status,
            aliexpress_currency_code1 aliexpress_currency_code
            -- if(e.aliexpress_currency_code1='', 'USD', e.aliexpress_currency_code1) AS aliexpress_currency_code
        FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing_skus a
        INNER JOIN (
            SELECT
                aeop_s_k_u_property_list,aeop_s_k_u_property_list_str,
                arrayJoin(JSONExtractArrayRaw(aeop_s_k_u_property_list_str)) as aeop_ae_product_skus1,
                visitParamExtractFloat(aeop_ae_product_skus1, 'property_value_id') as property_value_id,
                visitParamExtractFloat(aeop_ae_product_skus1, 'sku_property_id') as sku_property_id
            FROM yibai_domestic.yibai_aliexpress_listing_skus_aeop_s_k_u_property_list
            -- 国外发货地的链接
            WHERE sku_property_id=200007763 and property_value_id!=201336100
        ) b ON a.aeop_s_k_u_property_list = b.aeop_s_k_u_property_list
        INNER JOIN (
            SELECT
                product_id, account_id, product_price, freight_template_id, pop_choice_status
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing
            WHERE 
                account_id in (
                    select id as account_id from yibai_sale_center_system.yibai_system_account
                    where platform_code = 'ALI' and is_del=0 and `status`=1 )
                and product_status_type = 1
        ) d ON a.product_id=d.product_id
        LEFT join (
            SELECT t1.*,t2.template_name_id as template_name_id,t2.account_id as account_id
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_fee_template_setting t1 
            left join yibai_sale_center_listing_sync.yibai_aliexpress_price_type_setting t2 
            on t1.price_mode_name = t2.price_mode_name
            where t1.is_delete=0 and t2.is_delete=0
        ) f
        on toString(d.freight_template_id)=f.template_name_id and d.account_id=f.account_id
        LEFT JOIN (
            select account_id,if(aliexpress_currency_code='', 'USD', aliexpress_currency_code) AS aliexpress_currency_code1
            from yibai_sale_center_common_sync.yibai_common_account_config
            where platform_code IN ('ALI') and is_del = 0
        ) e  on d.account_id=e.account_id
        -- 具体国外发货地
        LEFT JOIN (
            SELECT DISTINCT parent_attr_id, attr_id, name_en
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_category_attribute
        ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
        where a.sku !='' and f.price_mode_name is not null
    """
    # conn_ck = pd_to_ck(database='temp_database_hxx', data_sys='调价明细历史数据1')
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据1')
    df_ali_listing = conn_ck.ck_select_to_df(sql)
    df_ali_listing.columns = [i.split('.')[-1] for i in df_ali_listing.columns]
    #
    df_ali_listing[['property_value_id', 'sku_property_id']] = df_ali_listing[['property_value_id', 'sku_property_id']].astype(int).astype(str)
    dic = {'United States':'US', 'CZECH REPUBLIC':'DE', 'Czech Republic':'DE', 'Poland':'PL', 'france':'FR', 'France':'FR',
           'Australia':'AU', 'CN':'CN', 'spain':'ES', 'SPAIN':'ES', 'Russian Federation':'RU', 'UNITED KINGDOM':'UK',
           'United Kingdom':'UK','GERMANY':'DE', 'Mexico':'MX', 'cz':'DE', 'ITALY':'IT', 'Italy':'IT','brazil':'BR'}
    df_ali_listing['country'] = df_ali_listing['name_en'].replace(dic)
    df_ali_listing = df_ali_listing.drop_duplicates(subset=['product_id','sku','country'])
    df_ali_listing = df_ali_listing[(df_ali_listing['country']!='CN') & (df_ali_listing['country']!='CN')]
    # df_ali_listing = df_ali_listing.sample(10000)

    # # 处理仓标数据
    # df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
    #                          df['sku'].str[3:], df['sku'])
    # df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
    #                          df['sku'].str[:-2], df['new_sku'])
    df_ali_listing.drop('aeop_s_k_u_property_list_str', axis=1, inplace=True)
    df_ali_listing['date_id'] = time.strftime('%Y-%m-%d')
    print(df_ali_listing.info())

    # 存表
    # write_to_sql(df_ali_listing, 'yibai_ali_oversea_listing')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_ali_listing, 'yibai_ali_oversea_listing', if_exists='replace')

    return None
yibai_get_aliexpress_listing()
##
def get_temu_listing_ad():
    sql = """
        SELECT 
            account_id, product_sku_id, sku, online_status, site_code, warehouse, added_to_site_time, country,
            best_warehouse_name, available_stock, sales_status, overage_level, new_price, total_cost, supplier_price, 
            freight_subsidy, rate, online_profit_rate+0.14 online_gross_profit
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = '2024-11-11' and online_status = '已发布到站点'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    temu_account = get_temu_account()
    temu_listing = pd.merge(df, temu_account[['account_id','main_name']], how='left', on=['account_id'])
    print(df.info())
    temu_listing.to_excel('F://Desktop//temu_listing.xlsx', index=0)

get_temu_listing_ad()
##
sql = """
    SELECT sku, warehouse, available_stock, age_90_plus, age_60_plus
    FROM over_sea.dwm_sku_info_temu
    WHERE date_id = '2024-11-14' and available_stock > 0
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df = conn.read_sql(sql)
df.to_excel('F://Desktop//df.xlsx', index=0)

##
def stemp():
    sql = """
        SELECT sku, warehouse, sum(available_stock) tk_available_stock
        FROM yb_datacenter.v_oversea_stock
        WHERE available_stock > 0 and warehouse_name like '%TK独享%'
        GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tk = conn_ck.ck_select_to_df(sql)
    #
    sql = """
        SELECT sku, warehouse, available_stock, warehouse_stock_age, new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2024-11-15' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock = conn.read_sql(sql)
    #
    df_re = pd.read_excel('F://Desktop//TK独享责任sku.xlsx')
    df_re['warehouse'] = df_re['站点'].replace({'美国':'美国仓','英国':'英国仓','欧洲':'德国仓','澳大利亚':'澳洲仓','加拿大':'加拿大仓'})
    df_re.info()
    #
    df = pd.merge(df_stock, df_tk, how='left', on=['sku','warehouse'])
    df = pd.merge(df, df_re, how='left', on=['sku','warehouse'])
    df.to_excel('F://Desktop//df.xlsx', index=0)

def get_ban_info():
    sql_js = f"""
    SELECT distinct sku ,risk_grade_type as `禁售等级` 
    FROM yibai_prod_base_sync.yibai_prod_forbidden_grade
    WHERE is_del = 0 and platform_code in ('AMAZON') 
    -- and risk_grade_type in ('III','IV','V')
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_js = conn_ck.ck_select_to_df(sql_js)
    df_js['sku'] = df_js['sku'].astype('str')
    df_js.to_excel('F://Desktop//df_js.xlsx', index=0)

get_ban_info()
##


##
sql = """

    SELECT *
    FROM over_sea.tt_oversea_sale_status_temp
    WHERE end_time is null
    
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_status = conn.read_sql(sql)
df_status = df_status.drop_duplicates(subset=['sku','warehouse'])
df_status.to_excel('F://Desktop//df_status2.xlsx', index=0)
##
df = pd.read_excel('F://Desktop//df_statusV3.xlsx', dtype={'sku':str})
##
df['end_time'] = df['end_time'].astype(str)
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
conn.to_sql(df, 'tt_oversea_sale_status_temp', if_exists='append')
##
sql = """

    SELECT 
        sku, warehouse_id, platform_code, 3days_sales,7days_sales,15days_sales,30days_sales,
        60days_sales,90days_sales 
    FROM tt_oversea.tt_sku_sales_statistics a
    WHERE toString(warehouse_id) not like '%10000%'
    LIMIT 100
"""
ck_client = pd_to_ck(database='tt_oversea', data_sys='调价明细历史数据t')

# conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_tt_sales = ck_client.ck_select_to_df(sql)
##

sql = """
    SELECT *
    FROM over_sea.tt_ali_oversea_listing
    WHERE date_id = '{date_today)'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_status = conn.read_sql(sql)
##
def get_date():
    df = pd.read_excel('F://Desktop//dwm_ali_sku.xlsx')
    df['new_sku'] = df['sku']
    df = get_line(df)

    df.to_excel('F://Desktop//df_lines.xlsx', index=0)
    return None
get_date()


## 分摊校验
sql = """
SELECT sku, warehouse_code, end_qty, end_amount, avg_freight_fee, upper_time, create_date
FROM yibai_oversea.yibai_oversea_freight_inventory_share_new x
WHERE warehouse_code = 'GC_USEA' and create_date > '2024-09-01'
ORDER BY x.create_date DESC
"""
conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
df_tk = conn_ck.ck_select_to_df(sql)
##
df_tk.to_excel('F://Desktop//df_tk.xlsx', index=0)
## 尺寸校验
sql = """
    SELECT 
        a.*, b.sku erp_sku, b.client_sku client_sku, 
        instock_stock-out_stock as saas_stock, storage_age_date, 
        toInt32(today() - toDate(storage_age_date)) as stock_age
    FROM yb_datacenter.yibai_stock_age_detail a
    LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
    ON a.sku = b.oversea_sku
"""
conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
df_mx_stock = conn_ck.ck_select_to_df(sql)
##
df_mx_stock.to_excel('F://Desktop//df_mx_stock.xlsx', index=0)
##

def stock_temp():
    date_today = time.strftime('%Y%m%d')
    sql = f"""

        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,stock,
            available_stock, purchase_on_way_count, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, 
            yps.new_price as new_price, cargo_owner_id
        FROM yb_datacenter.yb_stock AS ps
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
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
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            and (ps.available_stock > 0 or ps.allot_on_way_count > 0)
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and (yw.warehouse_other_type = 2 or yw.warehouse_name like '%独享%')   -- 筛选公共仓（非子仓）
            and (yw.warehouse_other_type = 2)   -- 筛选公共仓（非子仓）
            and (yw.warehouse_name not like '%独享%')   -- 筛选公共仓（非子仓）
            and yw.warehouse_name not in ('HC美国西仓','出口易美西仓-安大略仓','万邑通美南仓-USTX','亿迈CA01仓')  -- 剔除不常用仓库
            -- and ywc.name in ('美国仓','加拿大仓','墨西哥仓','澳洲仓','英国仓','德国仓','法国仓','俄罗斯仓','乌拉圭仓','巴西仓')
            -- and yw.warehouse_name not like '%TT%'
            -- and yw.warehouse_name in ('DHS乌拉圭toC代发仓(TT)')
        ORDER BY date_id DESC
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    print(df_temp.info())
    df_temp = df_temp.groupby(['sku','warehouse'])['stock'].sum().reset_index()
    # df = df_temp.groupby('warehouse_name')['available_stock'].sum().reset_index()
    return df_temp

# 精铺sku库存数据
def get_tt_fine_sku():
    sql = """
        SELECT
            *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2024-12-16'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT 
        sku,
        develop_source
    FROM yibai_prod_base_sync.yibai_prod_sku 
    where 
        develop_source in (14, 15, 22)
        or title_cn like '%通拓%' 
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    tt_sku = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, tt_sku, how='inner', on=['sku'])
    df_stock_all = stock_temp()
    df = pd.merge(df, df_stock_all, how='left', on=['sku','warehouse'])
    df.to_excel('F://Desktop//df_tt_skuV2.xlsx', index=0)

get_tt_fine_sku()
##
def get_ebay_upload():
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM over_sea.tt_oversea_ebay_upload
        where date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)

    sql = """
        SELECT *
        FROM tt_oversea_sale_status_temp
        WHERE end_time IS NULL
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)

    df_temp = pd.merge(df_temp, df_status[['sku','warehouse','sale_status']], how='left', on=['sku','warehouse'])

    # df_temp.to_excel('F://Desktop//df_ebay_uplaod_check.xlsx', index=0)

get_ebay_upload()
##
def get_sku_age():
    """

    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = '''
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
        date = formatDateTime(subtractDays(now(),3), '%Y-%m-%d') and status in (0,1) 
        and yw.warehouse_name not like '%独享%' 
        -- and yw.warehouse_name not like '%TT%'
        -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        and yw.id not in (339)   -- 剔除不常用仓库，避免最优子仓选到无运费的子仓上
    '''
    df_stock_age = ck_client.ck_select_to_df(sql)

    df_stock_age = get_line(df_stock_age)

    df_stock_age.to_excel('F://Desktop//tt_stock_age.xlsx', index=0)

get_sku_age()
##

def get_mx_sku_age():
    sql = """
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
        )
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)

    df_mx_stock.to_excel('F://Desktop//df_mx_stock.xlsx', index=0)
get_mx_sku_age()

## 速卖通有订单链接
def get_ali_order_listing():
    # 取ali的总链接
    sql = f"""
        SELECT *
        FROM over_sea.tt_ali_oversea_listing
        where date_id = '2024-12-16'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ali = conn.read_sql(sql)
    print(df_ali.info())
    # 取ali订单链接
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=30)

    sql = f"""
        SELECT
            *
        FROM yibai_oversea.tt_dashbord_new_data1
        WHERE 
            payment_time >= '{date_start}'
            -- and `total_price` > 0 
            -- and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info['item_id'] = df_order_info['item_id'].astype(str)
    df_order_info = df_order_info.groupby(['item_id', 'account_id']).agg({'order_id':'count', 'sku':'max'}).reset_index()
    df_order_info = df_order_info.rename(columns={'item_id':'product_id'})

    df_ali = pd.merge(df_ali, df_order_info, how='left', on=['product_id', 'account_id'])


    df_ali.to_excel('F://Desktop//df_ali.xlsx', index=0)

## amazon有订单链接
def get_amazon_order_listing():
    """ """
    sql = """
        SELECT *
        FROM  over_sea.tt_oversea_ali_listing_all
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    tt_amazon_listing = conn.read_sql(sql)

    tt_amazon_listing.to_excel('F://Desktop//tt_amazon_listing.xlsx', index=0)

get_amazon_order_listing()
## tt amazon调价数据


## tt头程分摊问题
def tt_order_fc():
    """  TT订单：ERP和OMS """

    # TT ERP订单
    sql = """

        SELECT transaction_id platform_order_id, sku tt_sku, date_day, item_id tt_item_id, 
        gross, net, transfer_fee, country
        FROM temp.dwd_order_sku_input_date_list_di
        WHERE platform in ('TEMU','AMAZON') and date_day >= '2024-11-15'
        -- LIMIT 100
    """
    conn = connect_to_sql(database='temp', data_sys='TT新数仓')
    tt_order = conn.read_sql(sql)
    print(tt_order.info())

    # TT 订单OMS
    # 获取tt海外仓
    warehouse = tt_get_warehouse()

    # 取订单表
    date_start = '2024-11-15'
    # date_end = '2024-12-31'
    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        AND refund_status in (0, 1) 
        and order_status<>80
        and payment_status=1
        and order_id not like '%%-RE' 
        AND payment_time>='{date_start}'
        and platform_code!='SPH' AND total_price!=0
        and platform_code in ('TEMU', 'AMAZON')
        -- LIMIT 1000
    )
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        d.sku as sku,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.payment_time as payment_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        toInt64(d.quantity) as quantity,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        c.first_carrier_cost, c.first_carrier_cost_second, c.duty_cost, c.duty_cost_second,
        d.quantity as release_money,
        d.sales_status as sales_status
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
        """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓MRP')
    order_base = conn_mx.ck_select_to_df(sql)

    # 匹配仓库
    col = ['warehouse_id', 'warehouse_name', 'warehouse']
    order_base = order_base.merge(warehouse[col], on='warehouse_id', how='inner')

    print(order_base.info())

    df = pd.merge(order_base, tt_order, how='left', on='platform_order_id')

    print(df.info())

    df.to_excel('F://Desktop//df_tt.xlsx', index=0)

tt_order_fc()

##
def temp_temp():
    sql = """
        SELECT item_id,sku,start_price 
        FROM tt_sale_center_listing_sync.tt_ebay_online_listing_sales_sku 
        limit 100
    """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())
temp_temp()

## walmart mx站点链接sku信息
def walmart_mx_sku():
    """ 查看walmart mx站点链接信息，判断是否可以调价 """
    sql = """
        SELECT *
        FROM over_sea.yibai_walmart_oversea_listing_price
        WHERE `DATE` = '2025-02-10' and site = 'mx'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    walmart_listing = conn.read_sql(sql)

    # 云仓库存信息
    sql = """
    SELECT sku, warehouse_name, sum(available_stock) available_stock
    FROM (
        SELECT
            ps.sku AS sku, ywc.name AS warehouse, cargo_owner_id,
            yw.warehouse_name AS warehouse_name,yw.warehouse_code AS warehouse_code,
            yw.id AS warehouse_id,yw.warehouse_other_type AS warehouse_other_type,
            stock,available_stock
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
        WHERE 
            (ps.date_id = toYYYYMMDD(now())) 
            AND (ps.cargo_owner_id = 8) 
            AND (yw.warehouse_type IN (2,3)) 
            AND ywc.name = '墨西哥仓'
        ) a
    GROUP BY sku, warehouse_name 
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = conn_ck.ck_select_to_df(sql)

    df_stock_group = pd.pivot_table(df_stock, values='available_stock', index=['sku'],
                                columns=['warehouse_name'], aggfunc='sum', fill_value=0, margins=True)

    df = pd.merge(walmart_listing, df_stock_group, how='left', on=['sku'])

    df.to_excel('F://Desktop//df_walmart_mx.xlsx', index=0)

walmart_mx_sku()
