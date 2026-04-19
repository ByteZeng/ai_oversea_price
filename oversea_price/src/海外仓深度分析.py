"""
海外仓深度分析报告：
1、销库、出单利润率情况
2、库存库龄情况
3、链接数量情况
4、调价覆盖率与准确率
5、
"""
##
import pandas as pd
import os
import numpy as np
import warnings
import datetime, time
from clickhouse_driver import Client
from utils.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate, get_sku_sales_new, cut_bins, \
    is_new_sku, write_to_sql,get_mx_stock_age
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account, get_freight_subsidy,get_main_resp
from all_auto_task.oversea_listing_detail_2023 import get_amazon_listing_data, get_price_data, get_platform_fee
from all_auto_task.oversea_price_adjust_tt import tt_get_sku, tt_get_warehouse
# from all_auto_task.oversea_temu_shield import get_main_resp
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split
import utils
from utils.utils import  save_df, make_path
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
warnings.filterwarnings('ignore')
## 1、销库、出单利润率情况
def get_oversea_order():
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=10)
    date_start = '2024-12-01'
    date_end = '2024-12-31'
    # 仅更新近1个月订单
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"delete from over_sea.ads_oversea_order where paytime >= '{date_start}' and paytime < '{date_end}'"
    conn.execute(sql)
    conn.close()

    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM over_sea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    df_order_info = conn_ck.ck_select_to_df(sql)
    df_order_info = df_order_info.drop_duplicates(subset=['order_id','sku','account_id'])

    # 重复订单号的销售额、利润置为0
    column_order = ['负利润加快动销', '正利润加快动销', '正常',' ']
    df_order_info['sales_status'] = pd.Categorical(df_order_info['sales_status'], categories=column_order, ordered=True)
    df_order_info = df_order_info.sort_values(by=['order_id', 'sales_status'])

    df_order_info['rank'] = df_order_info.groupby(['order_id', 'sales_status']).cumcount() + 1
    for c in ['total_price','true_profit_new1','real_profit']:
        df_order_info[c] = np.where(df_order_info['rank'] != 1, 0, df_order_info[c])
    df_order_info.drop('rank', axis=1, inplace=True)

    # 存表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_order_info, 'ads_oversea_order', if_exists='append')

get_oversea_order()
##
def order_duplicated():
    """
    处理海外仓订单表中，同一个订单号多个sku的情况：
    1、只保留一个销售额、毛利润、净利润，sku销量销库金额保留多个
    2、同一个订单的不同sku还不同销售状态时，优先保留负利润加快动销的值
    """
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=5)
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM over_sea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    df_order_info = conn_ck.ck_select_to_df(sql)

    column_order = ['负利润加快动销', '正利润加快动销', '正常',' ']
    df_order_info['sales_status'] = pd.Categorical(df_order_info['sales_status'], categories=column_order, ordered=True)
    df_order_info = df_order_info.sort_values(by=['order_id', 'sales_status'])

    df_order_info['rank'] = df_order_info.groupby(['order_id', 'sales_status']).cumcount() + 1
    # 重复订单号的销售额、利润置为0
    for c in ['total_price','true_profit_new1','real_profit']:
        df_order_info[c] = np.where(df_order_info['rank'] != 1, 0, df_order_info[c])

    df_order_info.to_excel('F://Desktop//df_order_info.xlsx', index=0)

order_duplicated()
##
# 2、库存结构
def get_oversea_stock():
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

# 3、海外仓仓租校验
def get_oversea_rent():
    # date_today = datetime.datetime.now().strftime('%Y-%m-%d')
    date_today = datetime.date.today() - datetime.timedelta(days=10)
    # date_today = '2024-01-01'
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    delete from over_sea.ads_oversea_rent where date_id >='{date_today}'
    """
    conn.execute(sql)
    sql = f"""
        SELECT  
            date as date_id, sku, yw.id warehouse_id, warehouse_name,
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
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.warehouse_code = yw.warehouse_code
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
        WHERE 
            date >= '{date_today}' and status in (0,1) 
            and yw.warehouse_name not like '%独享%' 
            -- and yw.warehouse_name not like '%TT%'
            -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)
    #
    df_rate = get_rate()
    #
    df_age = pd.merge(df_age, df_rate[['country', 'rate']], how='left', on=['country'])
    df_age = df_age.drop_duplicates()
    #
    df_age['charge_total_price'] = df_age['charge_total_price'].astype(float)
    df_age['charge_total_price_rmb'] = df_age['charge_total_price'] * df_age['rate']
    df_age.drop('rate', axis=1, inplace=True)
    #

    # write_to_sql(df_age, 'ads_oversea_rent')
    conn.to_sql(df_age, 'ads_oversea_rent', if_exists='append')

##
get_oversea_order()
##
get_oversea_rent()
##
get_oversea_stock()

##
def get_dashbord_order():
    # date_start = date_today - datetime.timedelta(days=30)
    # date_start = '2025-08-01'
    # sql = f"""
    #     SELECT
    #         order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit,
    #         warehouse_name, warehouse, quantity, new_price, release_money, sales_status
    #     FROM over_sea.dashbord_new_data1
    #     WHERE
    #         paytime >= '{date_start}'
    #         -- and paytime < '2024-12-16'
    #         -- and platform_code = 'TEMU'
    #         and `total_price` > 0
    #         and `sales_status` not in ('','nan','总计')
    # """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    # df_order_info = conn_ck.ck_select_to_df(sql)
    # df_order_info = df_order_info.drop_duplicates(subset=['order_id','sku','account_id'])
    # #
    temu_account = get_temu_account()
    # print(temu_account.info())
    # df_order_info = pd.merge(df_order_info, temu_account[['account_id','main_name']], how='left', on=['account_id'])

    temu_account.to_excel('F://Desktop//temu_account.xlsx', index=0)

get_dashbord_order()
##

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线` 
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    # df_line['四级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')
    df = pd.merge(df, df_line, how='left', on=['sku'])

    return df

##
def get_platform_fee():
    sql = f"""
    SELECT 
        *
    FROM over_sea.tt_yibai_platform_fee

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_tt = conn.read_sql(sql)

    df_tt.to_excel('F://Desktop//df_tt.xlsx', index=0)

    sql = f"""
    SELECT 
        *
    FROM over_sea.yibai_platform_fee

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yb = conn.read_sql(sql)

    df_yb.to_excel('F://Desktop//df_yb.xlsx', index=0)
get_platform_fee()
##

############# YM墨西哥调价，国内仓链接中有海外仓sku，也需进行调价
# 取墨西哥仓的sku
def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    return out_date

def get_mx_dom_listing():
    sql = f"""
        select distinct sku 
        from dwm_sku_temp_info 
        WHERE date_id>='{get_date()}' and warehouse = '墨西哥仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    print(f'墨西哥仓shu数量共{len(df_sku)}')
    #

    sql = """ 
    with listing_table as (
        select distinct account_id, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map a
        inner join (
            select toInt32(b.id) as account_id, account_name, group_id, short_name, site
            from yibai_system_kd_sync.yibai_amazon_account b
            where site = 'mx' and (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
            and account_type = 1 and  main_code='YB_ZT_FP'
        ) b
        on (a.account_id= b.account_id)
        where a.deliver_mode=1
    )
    
    select b.account_id as account_id, b.account_name as account_name, 
        group_name, short_name, 'AMAZON' as platfrom,
        if(b.site ='sp', 'es', b.site) as site,
        status, e.sku as sku, a.seller_sku as seller_sku,  
        open_date,
        a.asin1 as asin,
        a.price AS your_price, fulfillment_channel, a.price as online_price
    from (
        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a 
    inner join ( 
        select account_id, sku, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where deliver_mode=1 and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) e
    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    inner join (
        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where site = 'mx' and (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_type = 1 and  main_code='YB_ZT_FP'
    ) b
    on (a.account_id= b.account_id)
    inner join (
        select group_id, group_name
        from yibai_system_kd_sync.yibai_amazon_group 
        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    ) c 
    on (b.group_id=c.group_id)
    order by a.create_time desc limit 1
    by a.account_id, a.seller_sku
    settings max_memory_usage = 20000000000
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_all = conn_ck.ck_select_to_df(sql)
    print(df_all.info())
    df_all = df_all.merge(df_sku, on=['sku'])

    return len(df_all)


## 代销SKU取数
def daixiao_sku():
    sql = """
        SELECT 
            distinct a.sku sku, b.product_status product_status, c.category_path `产品路线`, b.title_cn title_cn, b.title_en title_en, 
            a.site site, devp_source, 
            latest_quotation `最新报价人民币`, 
            latest_quotation_foreign `最新报价_外币`,
            latest_inventory `最新库存`, a.modify_time modify_time
        FROM yibai_prod_base_sync.yibai_prod_sku_consignment_inventory a
        INNER JOIN yibai_prod_base_sync.yibai_prod_sku_consignment b
        ON a.sku = b.sku
        LEFT JOIN yibai_prod_base_sync.yibai_prod_category c
        ON a.product_category_id = c.id
        WHERE latest_inventory > 30
        ORDER BY modify_time DESC
        LIMIT 1 by sku
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_daixiao = conn_ck.ck_select_to_df(sql)

    # df_daixiao.to_excel('df_daixiao.xlsx', index=0)

    return df_daixiao
##
def get_season_sku():
    """ 季节性产品标签 """
    sql = """
    SELECT DISTINCT sku, 1 as `冬季产品_产品系统标记`
    from yibai_prod_base_sync.yibai_prod_sku 
    where (feature_val  like '%4%' and feature_val not like '%1%' and feature_val not like '%2%' 
    and feature_val not like '%3%')  or for_holiday = 1
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    # df.to_excel('F://Desktop//df_season_sku.xlsx', index=0)
    return df

def dwm_sku_temp():
    sql = f"""
        select sku, warehouse, title, linest, type, best_warehouse_name, new_price, available_stock, available_stock_money,
        overage_level, age_180_plus, estimated_sales_days, date_id
        from dwm_sku_temp_info 
        WHERE 
        date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id > '2025-07-22')
        and available_stock > 0 
        and type in ('海兔转泛品','易通兔')
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT *
        FROM over_sea.dwm_oversea_profit
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit WHERE date_id > '2025-07-22')
    """
    df_profit = conn.read_sql(sql)

    df = pd.merge(df, df_profit, how='left', on=['sku', 'warehouse'])

    print(df.info())
    df.to_excel('F://Desktop//dwm_sku_temp0902.xlsx', index=0)


    return None

dwm_sku_temp()
##
def dwm_sku_temp_temp():
    sql = f"""
        select sku, type, best_warehouse_name, warehouse, available_stock, new_price, age_90_plus, age_180_plus, age_270_plus, age_360_plus
        from over_sea.dwm_sku_temp_info 
        WHERE date_id = '2025-11-13' and available_stock > 0
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # sql = """
    #     SELECT *
    #     FROM over_sea.dwm_oversea_profit
    #     WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit WHERE date_id > '2025-09-01')
    # """
    # df_profit = conn.read_sql(sql)
    #
    # df =pd.merge(df, df_profit, how='left', on=['sku','warehouse'])


    df.to_excel('F://Desktop//dwm_sku_temp_info.xlsx', index=0)


    return None

dwm_sku_temp_temp()
##
def warehouse_temp():

    sql = """
        SELECT 
            yw.id AS warehouse_id,   
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
            yw.country, ebay_category_id,
            ywc.name AS warehouse, yw.warehouse_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_warehouse.columns = [i.split('.')[-1] for i in df_warehouse.columns]

    return df_warehouse

df_warehouse = warehouse_temp()
df_warehouse.to_excel('F://Desktop//df_warehouse.xlsx', index=0)

## 分摊头程与自算头程差异
def useful_fee_contrast():

    sql = f"""
        select sku, warehouse, best_warehouse_name warehouseName, new_price, available_stock,  overage_level
        from dwm_sku_temp_info 
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT 
            distinct sku, warehouseName, warehouse,  dutyCost,country,
            totalCost_origin as `总运费_分摊头程`, totalCost as `总运费_报价头程`,
            firstCarrierCost as `分摊头程`, new_firstCarrierCost as `报价头程`
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee_cts = conn.read_sql(sql)
    #
    col = ['总运费_分摊头程','总运费_报价头程','分摊头程', '报价头程']
    df_fee_cts[col] = df_fee_cts[col].astype(float)
    df_fee_cts['总运费差异率'] = round(df_fee_cts['总运费_报价头程'] / df_fee_cts['总运费_分摊头程'] - 1, 4)
    #
    df_fee_cts['总运费差异率分段'] = pd.cut(df_fee_cts['总运费差异率'],
                                              bins=[-2,-1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                              labels=['A:<-1', 'B:(-1,-0.5]','C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                                      'E:(-0.1, -0.05]','F:(-0.05, 0.05]','G:(0.05, 0.1]','H:(0.1, 0.2]',
                                                      'I:(0.2, 0.5]','J:(0.5, 1]','K:(1, +]'])
    df_fee_cts['总运费差异率分段'] = np.where(df_fee_cts['总运费差异率'] < -1, 'A:<-1', df_fee_cts['总运费差异率分段'])
    df_fee_cts['总运费差异率分段'] = np.where(df_fee_cts['总运费差异率'] > 1, 'K:(1, +]', df_fee_cts['总运费差异率分段'])

    df = pd.merge(df, df_fee_cts, how='inner', on=['sku','warehouse','warehouseName'])
    df.to_excel('df.xlsx', index=0)

    return None

useful_fee_contrast()


## vc的asin在美国站近30天的最低价，区分fba和fbm
def vc_asin_price():
    # 获取美国站的账号
    sql = """
        SELECT erp_id as account_id
        FROM yibai_sale_center_system.yibai_system_account
        WHERE platform_code = 'AMAZON' and is_del = 0 and site_code = 'US' 
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_system', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    # 获取目标asin
    sql = """
        SELECT *
        FROM yibai_fba.amazon_sevc_product_para
    """
    conn_ck = pd_to_ck(database='yibai_fba', data_sys='调价明细历史数据')
    df_asin = conn_ck.ck_select_to_df(sql)
    asin_list = tuple(df_asin['asin'].unique())
    # 获取fba历史价格
    sql = f"""
        SELECT account_id, seller_sku, asin, sale_price, adjust_date
        FROM support_document.fba_price_20241218
        WHERE asin in {asin_list} and status = 1
    """
    conn_ck = pd_to_ck(database='yibai_fba', data_sys='调价明细历史数据')
    df_asin = conn_ck.ck_select_to_df(sql)


## 分销测算
def mrp_oversea_sales():
    """ 计划采购需求有效销量数据获取 """
    date_today = time.strftime('%Y%m%d')
    sql = f"""
        SELECT `标识`, `区域大仓`, `总日销` effective_sales
        FROM yibai_mrp_oversea.yibai_order_point_all_{date_today}
    """
    conn_ck = pd_to_ck(database='yibai_mrp_oversea', data_sys='易佰MRP')
    df_point_sales = conn_ck.ck_select_to_df(sql)
    print(df_point_sales.info())

    df_point_sales['sku'] = df_point_sales['标识'].str.split('$').str[0]
    df_point_sales['warehouse'] = np.where(df_point_sales['区域大仓'].isin(['美东仓','美西仓']), '美国仓', df_point_sales['区域大仓'])
    df_point_sales['warehouse'] = np.where(df_point_sales['warehouse'].isin(['澳洲悉尼仓', '澳洲墨尔本仓']), '澳洲仓',
                                           df_point_sales['warehouse'])
    print(df_point_sales['warehouse'].unique())
    df_point_sales = df_point_sales.groupby(['sku','warehouse']).agg({'effective_sales':'sum'})

    #
    # sql = """
    #     SELECT sku, warehouse, available_stock, best_warehouse_name, day_sales, `30days_sales`, overage_level
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = '2025-01-14'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_temp = conn.read_sql(sql)
    #
    # df_temp = pd.merge(df_temp, df_point_sales, how='left', on=['sku','warehouse'])
    #
    # df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)

    return df_point_sales

def sku_lower_price():
    """ sku在TEMU链接最低价（责任人链接）"""
    sql = """
        SELECT account_id, country, product_sku_id, sku, online_status, warehouse, is_same, responsible_name,
        supplier_price, online_price_rate
        FROM yibai_oversea.dwd_temu_listing
        WHERE date_id = '2025-01-15' and is_same = 1 and online_status != '已下架'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)

    df_listing = df_listing.sort_values(by='supplier_price', ascending=True).drop_duplicates(subset=['sku','warehouse'])

    col = ['sku','warehouse','product_sku_id','supplier_price','online_price_rate']
    df_listing = df_listing[col]

    return df_listing

def fenxiao_test():
    """ 分销高销sku不开发。测算不开放的sku情况"""
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
            SELECT
                a.sku sku, title, type, product_status, linest, a.warehouse warehouse, available_stock, 
                best_warehouse_name, age_180_plus,product_package_size,
                new_price, gross,best_warehouse_id, overage_level, `30days_sales`,
                IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`,
                new_price*0.013 as `国内采购运费`
            FROM (
                SELECT *
                FROM over_sea.dwm_sku_temp_info
                WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
                and (available_stock > 0)
            ) a
            LEFT JOIN (
                SELECT *
                FROM oversea_sale_status
                WHERE end_time IS NULL
            ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    """
    df_sku = conn.read_sql(sql)

    df_point_sales = mrp_oversea_sales()
    df_sku = pd.merge(df_sku, df_point_sales, how='left', on=['sku','warehouse'])

    df_listing = sku_lower_price()
    df_sku = pd.merge(df_sku, df_listing, how='left', on=['sku','warehouse'])

    df_sku.to_excel('F://Desktop//df_fenxiao.xlsx', index=0)

mrp_oversea_sales()
## 海外仓优先配库sku

def oversea_sku_first():
    """
    1. 满足SKU冗余判断条件：
        a.销售状态为正负利润加快动销
    2. 满足经济性判断：海外仓定价/国内仓定价<1
    3. 冗余库存预计可售天数>90天
    4. 单sku库存pcs数>10 pcs

    或  Temu无资质上架的sku
    """

    sql = f"""
        select 
            a.sku, title, linest, a.warehouse, best_warehouse_name, available_stock, new_price,
            b.sale_status
        from dwm_sku_temp_info a
        LEFT JOIN (
            SELECT sku, warehouse, sale_status
            FROM over_sea.oversea_sale_status
            where end_time is Null
        ) b 
        ON a.sku = b.sku and a.warehouse = b.warehouse
        WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
        and a.warehouse in ('澳洲仓', '德国仓', '法国仓', '意大利仓', '西班牙仓','乌拉圭仓','墨西哥仓','加拿大仓',
        '英国仓', '美国仓')
        and available_stock > 0
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)

    df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)

oversea_sku_first()
##

##
def get_line(df):
    # 一级产品线
    sql = f"""
        SELECT a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线`, c.develop_source_name type
        FROM yibai_prod_base_sync.yibai_prod_sku a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_category b on toInt32(a.product_category_id) = toInt32(b.id)
        LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source c ON a.develop_source = c.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql)
    df_line['linest'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    df_line['last_linest'] = df_line['产品线路线'].str.split('->', expand=True).iloc[:, -1]
    df_line = df_line.drop_duplicates(subset='sku')

    col = ['sku', 'type', 'linest', 'last_linest']

    df = pd.merge(df, df_line[col], how='left', on=['sku'])

    return df

def get_stock():
    """
    从海外仓库存表，获取SKU及库存信息详情
    """
    # 初始库存表的视图：v_oversea_stock
    t1 = time.time()
    sql = '''
    select
        sku, title, new_price, gross, warehouse_id, product_status,
        -- '' as type, product_status, '' last_linest, '' linest, 
        sum_available_stock as available_stock, sum_available_stock_money as available_stock_money, 
        sum_on_way_stock as on_way_stock, sku_create_time as create_time, 
        product_size, product_package_size, best_warehouse, warehouse
    from (
        with 
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] as product_status_arr,
        ['已创建', '已开发', '待买样', '待品检', '待编辑', '待拍摄', '待编辑待拍摄', '待修图', '在售中', '审核不通过', '停售', 
        '待清仓', '已滞销', '待物流审核', '待关务审核', 'ECN资料变更中', 'ECN资料变更驳回'] as product_status_desc_arr	 
        select
            ps.sku as sku, ps.title_cn as title, ps.new_price as new_price, ps.product_weight_pross as gross, 
            ps.warehouse_code as warehouse_code, ps.warehouse_id as warehouse_id,
            -- case when p.state_type = '1' then '常规产品' end as dev_type,
            transform(ps.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
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
                ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name, yps.title_cn title_cn,
                yps.product_status product_status
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
            having new_price > 0
            ) ps
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
    df_sku_stock['available_stock'] = np.where(df_sku_stock['available_stock'] < 0, 0,
                                               df_sku_stock['available_stock'])
    df_sku_stock['available_stock_money'] = np.where(df_sku_stock['available_stock_money'] < 0, 0,
                                                     df_sku_stock['available_stock_money'])

    # 产品开发来源及品类
    df_sku_stock = get_line(df_sku_stock)
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock
get_stock()


## 海外仓sku最近采购数量
def purchase_num():
    """ 海外仓采购信息 """
    sql = """
    with shipment_data as (
        select
            sku, destination_warehouse_code, purchase_sn as purchase_number,
            shipment_status, if(shipment_status = 21, shipment_num, 0) shipment_num, shelf_num
        from yibai_plan_common_sync.yibai_oversea_shipment_list_detail a
        left join (
            select shipment_sn,shipment_status,shipment_type,a.destination_warehouse_code
            from yibai_plan_common_sync.yibai_oversea_shipment_list a
        ) b on a.shipment_sn = b.shipment_sn
        WHERE
            a.shipment_type in (1, 2)
            and b.shipment_status not in (3, 14, 15, 23, 24, 26)
            and shipment_type in (1, 2)
            and date(a.created_at) >today()-180
    )
    select *
        from
            (
                select
                    a.*,
                    greatest(shipment_num - shelf_num, 0) as exclusive_onway_stock
                from
                    shipment_data a
            )
    where shipment_num>0 AND exclusive_onway_stock>0 
    """
    # conn_ck = pd_to_ck(database='yibai_plan_common_sync', data_sys='易佰MRP')
    # df_plan = conn_ck.ck_select_to_df(sql)

    # 海外仓采购单信息
    sql = """
        SELECT sku, destination_warehouse, country, regular_quantity, logistics_type, sales_note, import_time
        FROM yibai_plan_common_sync.yibai_purchase_pr_import_list
        WHERE purchase_type_id = 2 and import_time > '2024-03-28'
    """
    conn_ck = pd_to_ck(database='yibai_plan_common_sync', data_sys='易佰MRP')
    df_plan = conn_ck.ck_select_to_df(sql)

    df_plan.to_excel('F://Desktop//df_plan.xlsx', index=0)
purchase_num()

## 用库龄表找最近到货数量
def get_recent_stock():
    """ """
    sql = '''
    SELECT  
        sku, yw.id as warehouse_id, 
        yw.warehouse_name warehouse_name, ywc.name warehouse,
        warehouse_stock, inventory_age, date
    FROM yb_datacenter.yb_oversea_sku_age ya
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.order_warehouse_code = yw.warehouse_code
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    WHERE 
        date >= formatDateTime(subtractDays(now(),180), '%Y-%m-%d') and status in (0,1) 
    '''
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)
    # 筛选当前还有库存的sku
    sql = """
    SELECT sku, warehouse_id, warehouse, available_stock, warehouse_name
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 and warehouse_other_type = 2
    """
    df_stock = conn_ck.ck_select_to_df(sql)
    df_age = pd.merge(df_age, df_stock[['sku','warehouse_id']], how='inner', on=['sku', 'warehouse_id'])
    print(df_age.info())

    df_age_result = df_age.groupby(['sku','warehouse','date']).agg({'warehouse_stock':'sum', 'inventory_age':'min'}).reset_index()

    df_age_result['date'] = pd.to_datetime(df_age_result['date'], errors='coerce')
    print(df_age_result.info())
    # df_age_result.to_excel('F://Desktop//df_age_base.xlsx', index=0)
    # 执行分组处理
    result = df_age_result.groupby(['sku','warehouse']).apply(find_last_min_age).reset_index()
    # 格式化输出
    result['result_date'] = result['result_date'].dt.strftime('%Y-%m-%d')
    result.to_excel('F://Desktop//df_age_result.xlsx', index=0)

    col = ['sku', 'warehouse', 'last_min_age', 'result_date', 'fisrt_stock']
    result = result[col]

    return result



def find_last_min_age(group):
    """处理每个SKU分组"""
    # 按日期倒序排序
    sorted_group = group.sort_values('date', ascending=False)

    min_age = None
    result_date = None

    for _, row in sorted_group.iterrows():
        if pd.isna(row['inventory_age']):
            continue

        # 初始化第一个有效值
        if min_age is None:
            min_age = row['inventory_age']
            fisrt_stock = row['warehouse_stock']
            result_date = row['date']
            continue

        # 遇到更大值则终止查找
        if row['inventory_age'] >= min_age:
            break

        # 更新更小年龄
        if row['inventory_age'] < min_age:
            min_age = row['inventory_age']
            fisrt_stock = row['warehouse_stock']
            result_date = row['date']

    return pd.Series({'last_min_age': min_age, 'result_date': result_date,'fisrt_stock':fisrt_stock})

get_recent_stock()


##
def get_yb_hwc_age():
    """ """
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
        date = formatDateTime(subtractDays(now(),2), '%Y-%m-%d') and status in (0,1) 
        and yw.warehouse_name not like '%独享%' 

    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)

    df_age.to_excel('F://Desktop//df_age.xlsx', index=0)

get_yb_hwc_age()


##  清仓效果监控。6月18日开启的限时清仓逻辑
def clear_order_monitor():
    """ 海外仓清仓效果监控
    1、6月18日超180天sku，至今的销库金额统计
    """
    sql = """
        SELECT sku, type, warehouse, best_warehouse_name, overage_level, available_stock, new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-06-18'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    date_start = '2025-05-18'
    date_end = '2025-07-03'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM over_sea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    df_order = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df_order, df_sku[['sku', 'warehouse','overage_level']], how='left', on=['sku', 'warehouse'])

    df.to_excel('F://Desktop//df_order.xlsx', index=0)

clear_order_monitor()

##

##
def get_yb_clear_order():
    """

    """
    df_sku = pd.read_excel('F://Desktop//YB清仓sku7月.xlsx', dtype={'sku':str})
    df_sku = df_sku.drop_duplicates(subset=['sku','warehouse'])
    date_start = '2025-07-01'
    date_end = '2025-07-31'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM over_sea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and paytime < '{date_end}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            -- and platform_code = 'AMAZON'
    """
    conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    df_order_info = conn_ck.ck_select_to_df(sql)

    # df_order_info = pd.merge(df_order_info, df_sku, how='inner', on=['sku','warehouse'])
    df_order_info.to_excel('F://Desktop//df_yb_clear_order_all.xlsx', index=0)

get_yb_clear_order()

##
def oversea_clear_order():
    """ 给财务的清仓订单 """
    sql = """
        SELECT *
        FROM clear_order_cw.oversea_clear_orders
        WHERE update_month = '2025-10' and platform_code = 'TEMU'
    """
    conn_ck = pd_to_ck(database='clear_order_cw', data_sys='调价明细历史数据')
    df_cw = conn_ck.ck_select_to_df(sql)

    df_cw.to_excel('F://Desktop//df_cw.xlsx', index=0)

oversea_clear_order()
##
def v_oversea_stock():
    """ """
    sql = """
    SELECT sku, warehouse_id, warehouse, available_stock, warehouse_name, warehouse_other_type
    FROM yb_datacenter.v_oversea_stock
    WHERE warehouse = '美国仓'
    -- and warehouse_name like '%SLM%'
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # 限时清仓BG3sku
    sql = """
        SELECT sku, warehouse, 'BG3' source
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = '2025-09-02'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_bg3 = conn.read_sql(sql)
    df = pd.merge(df_bg3, df, how='left', on=['sku', 'warehouse'])

    sku_list = tuple(df['sku'].unique())
    sql = f"""
        SELECT *
        FROM yibai_oversea.oversea_amazon_listing_all
        WHERE sku in {sku_list} and date_id = '2025-09-24'
    """
    df_bg3_listing = conn_ck.ck_select_to_df(sql)

    # 销售状态
    sql = """
        SELECT *
        FROM over_sea.dwm_oversea_profit 
        WHERE date_id = '2025-09-24'
        and warehouse = '美国仓'
    """
    df_destroy = conn.read_sql(sql)

    sql = f"""
        SELECT sku, new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-09-24'
        and sku in {sku_list}
        and warehouse = '美国仓'
    """
    df_sku = conn.read_sql(sql)

    df_sku = pd.merge(df_sku, df_destroy, how='left', on=['sku'])

    # sql = f"""
    #     SELECT a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线`, c.develop_source_name type,
    #     CASE
    #         when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price)
    #         when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price)
    #         when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost)
    #         when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
    #         else toFloat64(product_cost)
    #     END as `new_price`
    #     FROM yibai_prod_base_sync.yibai_prod_sku a
    #     LEFT JOIN yibai_prod_base_sync.yibai_prod_category b on toInt32(a.product_category_id) = toInt32(b.id)
    #     LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source c ON a.develop_source = c.id
    #     WHERE c.develop_source_name in ('海兔转泛品','易通兔')
    # """
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df_line = conn_ck.ck_select_to_df(sql)
    #
    # df = pd.merge(df, df_line, how='inner', on='sku')

    # df_sku_temp = pd.read_excel('F://Ding_workspace//精品北美清仓sku 需要拉取库存数量 0813.xlsx', dtype={'sku':str})
    # df = pd.merge(df, df_sku_temp, how='inner', on=['sku'])

    df_sku.to_excel('F://Desktop//df_bg3_sku.xlsx', index=0)
    # yibai_lazada_account.to_excel('F://Desktop//yb_lazada.xlsx', index=0)

v_oversea_stock()
##
def ebay_tt_temp():
    sql = f"""
        with  account_list as (
            select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
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
            FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
        ) a 
        INNER JOIN (
            SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
            from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
            where  warehouse_category_id !=1 and listing_status = 1 
            and account_id in (select distinct id from account_list) and sku in ('I2066G')
        ) b  ON a.item_id=b.item_id
        LEFT JOIN (
            SELECT item_id, shipping_service_cost
            FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
            WHERE shipping_status=1 and shipping_service_priority=1
        ) f  ON a.item_id = f.item_id
        LEFT JOIN (
            SELECT site_id,site,site1 AS `站点`,is_open,site_code 
            FROM domestic_warehouse_clear.yibai_site_table_ebay 
            where is_open='是'
        ) c  ON b.siteid = c.site_id
        LEFT JOIN account_list d on b.account_id = d.id
        LEFT JOIN (
            SELECT id, warehouse
            FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
            WHERE warehouse_type_id in (2, 3)
        ) e ON b.warehouse_category_id = e.id

    """
    conn_ck = pd_to_ck(database='yibai_sale_center_common_sync', data_sys='调价明细历史数据')
    df_ebay_listing = conn_ck.ck_select_to_df(sql)
    df_ebay_listing.to_excel('F://Desktop//df_ebay_listing.xlsx', index=0)
ebay_tt_temp()
##
def get_account_temp():
    sql = f"""
        with  account_list as (
            select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
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
            FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
            WHERE sku in ('3117220062011')
        ) a 
        LEFT JOIN (
            SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
            from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
            where  warehouse_category_id !=1 and listing_status = 1 
            and account_id in (select distinct id from account_list)
        ) b  ON a.item_id=b.item_id
        LEFT JOIN (
            SELECT item_id, shipping_service_cost
            FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
            WHERE shipping_status=1 and shipping_service_priority=1
        ) f  ON a.item_id = f.item_id
        LEFT JOIN (
            SELECT site_id,site,site1 AS `站点`,is_open,site_code 
            FROM domestic_warehouse_clear.yibai_site_table_ebay 
            where is_open='是'
        ) c  ON b.siteid = c.site_id
        LEFT JOIN account_list d on b.account_id = d.id
        LEFT JOIN (
            SELECT id, warehouse
            FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
            WHERE warehouse_type_id in (2, 3)
        ) e ON b.warehouse_category_id = e.id

    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_ebay_listing = conn_ck.ck_select_to_df(sql)
    df_ebay_listing.to_excel('F://Desktop//df_ebay_listing_temp.xlsx', index=0)

get_account_temp()

##
def get_lazada_temp():

    df = pd.read_excel('F://Ding_workspace//来赞宝泰国在售0723.xlsx', dtype={'sku':str})
    col = ['sku','warehouse','best_warehouse_name','available_stock','total_cost', 'seller_sku']
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    conn.to_sql(df[col], 'lzb_stock_and_fee', if_exists='replace')


get_lazada_temp()

##
def get_ebay_filter():
    """ ebay锁价表 """
    # sql = f"""
    #     SELECT item_id, sku, 1 as is_lock_listing
    #     FROM yibai_sale_center_listing_sync.yibai_ebay_price_adjustment_filter_sku
    #     WHERE end_time > '2025-07-21' and is_del = 0
    # """
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    # df_lock = conn_ck.ck_select_to_df(sql)
    #
    # df_lock.to_excel('F://Desktop//df_ebay_lock.xlsx', index=0)

    # date_today = time.strftime('%Y-%m-%d')
    # sql_discount = f"""
    #     SELECT item_id
    #     FROM yibai_sale_center_listing_sync.yibai_ebay_discount_status_analysis
    #     where ((status=1 and formatDateTime(toDateTime(sale_start_time), '%Y-%m-%d') <= '{date_today}'
    #     and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d') >= '{date_today}')
    #     or (status=2)
    #     or ((status=1 and need_task=10 and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d')<='{date_today}')
    #     or(status=2 and next_cycle_time>0)))
    #  """
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    # df_ebay_discount = conn_ck.ck_select_to_df(sql_discount)
    # df_ebay_discount['折扣'] = 1
    # df_ebay_discount['item_id'] = df_ebay_discount['item_id'].astype('str')
    #
    # df_ebay_discount.to_excel('F://Desktop//df_ebay_discount.xlsx', index=0)

    # ebay不调价账号
    sql = """
        SELECT *
        FROM over_sea.oversea_ebay_account_temp
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_account_temp = conn.read_sql(sql)
    df_account_temp['is_white_account'] = 1
    df_account_temp.to_excel('F://Desktop//df_account_temp.xlsx', index=0)

get_ebay_filter()

##

def sku_temp():
    """ """
    sql = """
        SELECT *
        FROM over_sea.dwm_sku_price_haitu
        WHERE date_id in ('2025-07-24')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df.to_excel('F://Desktop//df_haitu_temp.xlsx', index=0)
sku_temp()

## 通拓amazon不调价表
def tt_get_amazon_filter():
    """ """
    sql = """
    SELECT DISTINCT account_id ,asin from tt_fba.amazon_listing_para where para_type = 7  and date(end_time) >= today()
    """
    conn_ck = pd_to_ck(database='tt_fba', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

tt_get_amazon_filter()
##
def get_sales_status():
    """ """
    sql = """
        SELECT *
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_status.xlsx', index=0)

get_sales_status()

## 海外仓超360天库龄库存达到销毁价情况
def get_360_price():
    """ """
    sql = """
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-08-01')
        and overage_level = 360
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT sku, warehouse, target_profit_rate, is_destroy, is_destroy_final, date_id date
        FROM dwm_oversea_profit
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit WHERE date_id > '2025-08-01')
    """
    df_profit = conn.read_sql(sql)

    df = pd.merge(df, df_profit, how='left', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_360.xlsx', index=0)

get_360_price()

##
def dwm_sku_info_1():
    """ """
    sql = """
        SELECT sku, warehouse, available_stock, overage_level, best_warehouse_name
        FROM over_sea.dwm_sku_temp_info
        WHERE 
        date_id = '2025-10-13'
        -- date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        -- and available_stock > 0
        -- and overage_level >= 180 and overage_esd >= 180
        -- and warehouse = '美国仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//df_dwm_sku.xlsx', index=0)
dwm_sku_info_1()
##
def dwd_stock_age():
    """ """
    sql = """
        SELECT *
        FROM over_sea.dwd_oversea_age
        WHERE 
        date_id = '2025-10-09'
        -- date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        -- and available_stock > 0
        -- and overage_level >= 180 and overage_esd >= 180
        -- and warehouse = '美国仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)

    sql = """
        SELECT *
        FROM over_sea.tt_yibai_platform_fee
    """
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//tt_yibai_platform_fee.xlsx', index=0)
dwd_stock_age()
##
def flash_clearout_temp():
    """ """
    sql = """
        SELECT *
        FROM over_sea.dwm_sku_temp_info 
        WHERE date_id = '2025-08-20'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_base = conn.read_sql(sql)
    sql = """
        SELECT *
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id in ('2025-09-02','2025-10-10')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash = conn.read_sql(sql)

    # df = pd.merge(df_base, df_flash[['sku','warehouse']], how='inner', on=['sku','warehouse'])
    # print(df.info())

    df_flash.to_excel('F://Desktop//df_flash_10.xlsx', index=0)
flash_clearout_temp()

##
def listing_num():
    """ 海外仓刊登覆盖率 """
    sql = """
        SELECT *
        FROM yibai_oversea.oversea_platform_listing_num_tt
        WHERE `取数日期` = (SELECT max(`取数日期`) FROM yibai_oversea.oversea_platform_listing_num_tt)
    """
    sql = """
        SELECT *
        FROM yibai_oversea.oversea_transport_fee_daily
        WHERE date_id = 20251125
        LIMIT 1 
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tt = conn_ck.ck_select_to_df(sql)
    df_tt.to_excel('F://Desktop//df_tt_num.xlsx', index=0)

    # sql = """
    #     SELECT *
    #     FROM yibai_oversea.oversea_platform_listing_num
    #     WHERE `取数日期` = (SELECT max(`取数日期`) FROM yibai_oversea.oversea_platform_listing_num)
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_yibai = conn_ck.ck_select_to_df(sql)
    # df_yibai.to_excel('F://Desktop//df_yibai_num.xlsx', index=0)

listing_num()
##
def fee_temp():

    # sql = """
    #     SELECT *
    #     FROM clear_order_cw.oversea_clear_orders
    #     WHERE update_month in ('2025-08', '2025-09')
    #     and platform_code = 'TEMU'
    # """
    # conn_ck = pd_to_ck(database='clear_order_cw', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    df = pd.read_excel('F://Desktop//df_flash_sku_order.xlsx',dtype={'sku':str,'seller_sku':str})
    df = df[df['platform_code']=='TEMU']
    print(df.info())

    sql = """
    select 
        a.account_id account_id,a.account_status `账号状态`,a.account_id `平台账号ID`, b.main_name `主体账号`,a.oa_department_name `大部`,
        a.oa_group_name `小组` ,a.account_name `账号全称`,a.account_s_name `账号简称`,a.account_num_name `账号店铺名`,a.account_operation_mode `账号运营模式` 
    from yibai_account_manage_sync.yibai_temu_account a
    left join yibai_account_manage_sync.yibai_account_main b
    on a.main_id=b.main_id 
    where a.account_type=1
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    # df_temu_account = conn_ck.ck_select_to_df(sql)

    # df = pd.merge(df, df_temu_account[['account_id','账号简称']], how='left', on=['account_id'])
    # print(df.info())

    # sql = f"""
    #     SELECT
    #         order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit,
    #         warehouse_name, warehouse, quantity, new_price, release_money, sales_status
    #     FROM over_sea.dashbord_new_data1
    #     WHERE
    #         paytime >= '2025-06-10'
    #         and paytime < '2025-09-10'
    #         and `total_price` > 0
    #         and `sales_status` not in ('','nan','总计')
    #         and platform_code = 'TEMU'
    # """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='海外仓订单')
    # df_order_info = conn_ck.ck_select_to_df(sql)
    # df_order_info = df_order_info.drop_duplicates(subset=['order_id'])

    # sql = f"""
    #     SELECT sku,
    #     CASE
    #         when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price)
    #         when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price)
    #         when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost)
    #         when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
    #         else toFloat64(product_cost)
    #     END as `new_price`
    #     FROM yibai_prod_base_sync.yibai_prod_sku
    # """
    # conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df_cost = conn_ck.ck_select_to_df(sql)
    #
    # order_list = df['order_id'].to_list()
    # df_qu = pd.DataFrame()
    # for n in range(0,len(order_list), 5000):
    #     sku_list_x = order_list[n:n+5000]
    #     sql = f"""
    #     SELECT distinct
    #         A.order_id as order_id,
    #         F.sku as sku,
    #         A.quantity as quantity
    #     FROM (select * from yibai_oms_sync.yibai_oms_order_detail where order_id in  {sku_list_x} ) A
    #     inner join (select * from yibai_oms_sync.yibai_oms_order_sku where order_id in  {sku_list_x} ) F
    #     on F.order_detail_id=A.id
    #     """
    #     df_temp = conn_ck.ck_select_to_df(sql)
    #     df_qu = pd.concat([df_temp, df_qu])
    #
    # df = pd.merge(df, df_cost[['sku','new_price']], how='left', on='sku')
    # df = pd.merge(df, df_qu[['order_id', 'sku', 'quantity']], how='left', on=['order_id', 'sku'])
    # df['销库金额'] = df['new_price'] * df['quantity']

    seller_sku = df['seller_sku'].to_list()
    df_qu = pd.DataFrame()
    for n in range(0,len(seller_sku), 5000):
        sku_list_x = tuple(seller_sku[n:n+5000])
        print(sku_list_x)
        sql = f"""
        SELECT b.account_id account_id, product_sku_id, sku, seller_sku
        FROM (
            SELECT account_id, product_sku_id, sku, seller_sku
            FROM yibai_sale_center_listing_sync.yibai_temu_listing 
            where seller_sku in {sku_list_x}
        ) a
        LEFT JOIN yibai_sale_center_system_sync.yibai_system_account b
        ON a.account_id = b.id
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_qu = pd.concat([df_temp, df_qu])
    print(df_qu.info())
    df = pd.merge(df, df_qu, how='left', on=['account_id', 'seller_sku'])

    df.to_excel('F://Desktop//df_temu.xlsx', index=0)

fee_temp()

##
def ads_rent():
    """ 海外仓仓租数据 """
    sql = """
    SELECT date_id, sku, warehouse, sum(charge_total_price_rmb) charge_total_price_rmb
    FROM (
        SELECT *
        FROM over_sea.ads_oversea_rent
        WHERE date_id >= '2025-08-01' and date_id < '2025-09-01'
    ) a
    GROUP BY date_id, sku, warehouse
    """
    sql = """
        SELECT sku, warehouse, available_stock, `30days_sales`
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-09-22' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_rent = conn.read_sql(sql)

    print(df_rent.info())
    df_rent.to_excel('F://Desktop//df_dwm_stock.xlsx', index=0)

ads_rent()

## 海兔amazon和walmart链接
def get_haitu_listing():
    """ 海兔链接 """

    sql = """
        SELECT sku, title, type, new_price, warehouse, available_stock
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-09-12'
        and type in ('海兔转泛品', '易通兔')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    print(df_sku.info())
    sku_list = tuple(df_sku['sku'].to_list())

    # # amazon
    # sql = f"""
    #     SELECT *
    #     FROM over_sea.yibai_amazon_oversea_listing_price
    #     WHERE `DATE` = '2025-09-11'
    #     and sku in {sku_list}
    # """
    # df_amazon_listing = conn.read_sql(sql)
    # df_amazon_listing['account_id'] = df_amazon_listing['account_id'].astype(int)
    #
    # sql = f"""
    #     SELECT *
    #     FROM yibai_oversea.oversea_amazon_listing_all
    #     WHERE date_id = '2025-09-11' and sku in {sku_list}
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # amazon_listing_price = conn_ck.ck_select_to_df(sql)
    #
    # # 是否销毁价
    # sql = f"""
    #     SELECT sku, warehouse, lowest_price, target_profit_rate target_profit_rate_dtl,
    #     price_rmb price_rmb_dlt, price price_dtl, is_distory
    #     FROM over_sea.dwm_oversea_price_dtl
    #     WHERE date_id = '2025-09-11' and sku in {sku_list}
    #     and platform = 'AMAZON' and country = 'US'
    # """
    # df_dtl = conn.read_sql(sql)
    #
    # df = pd.merge(amazon_listing_price, df_dtl, how='left', on=['sku', 'warehouse'])

    # walmart
    sql = f"""
        SELECT *
        FROM over_sea.yibai_walmart_oversea_listing_price
        WHERE `DATE` = '2025-09-11'
        and sku in {sku_list}
    """
    walmart_listing = conn.read_sql(sql)
    walmart_listing['account_id'] = walmart_listing['account_id'].astype(int)

    sql = f"""
        SELECT *
        FROM yibai_oversea.oversea_walmart_listing_all
        WHERE date_id = '2025-09-11' and sku in {sku_list}
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    walmart_listing_price = conn_ck.ck_select_to_df(sql)

    # 是否销毁价
    sql = f"""
        SELECT sku, warehouse, lowest_price, target_profit_rate target_profit_rate_dtl, 
        price_rmb price_rmb_dlt, price price_dtl, is_distory
        FROM over_sea.dwm_oversea_price_dtl
        WHERE date_id = '2025-09-11' and sku in {sku_list}
        and platform = 'WALMART' and country = 'US'
    """
    df_dtl = conn.read_sql(sql)

    df = pd.merge(walmart_listing_price, df_dtl, how='left', on=['sku', 'warehouse'])

    df.to_excel('F://Desktop//haitu_walmart_listing.xlsx', index=0)

get_haitu_listing()
## 云仓库存数据
def stock_temp():
    """ 云仓库存数据 """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = time.strftime('%Y%m%d')
    date_today = '20251009'
    sql = f"""
          SELECT
              sku, warehouse, warehouse_name, Max(date_id) date_id,
              argMax(warehouse_id, stock) AS max_stock_warehouse_id,
              argMax(warehouse_name, stock) AS max_stock_warehouse_name,
              sum(stock) AS total_stock,
              arrayStringConcat(groupArray(warehouse_stock), ', ') warehouse_stock_info       
          FROM (
              SELECT
                  ps.sku sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
                  yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
                  available_stock, warehouse_stock, stock
              FROM (
                  SELECT 
                      trim(sku) sku, warehouse_id, date_id, cargo_owner_id, available_stock, stock,
                      concat(toString(warehouse_id),':',toString(stock)) as warehouse_stock
                  FROM yb_datacenter.yb_stock
                  WHERE 
                      date_id >= 20220901
                      and date_id <= '{date_today}' -- 根据需要取时间
                      and stock > 0 
                      -- and cargo_owner_id = 8  -- 筛选货主ID为8的
                      and warehouse_id in (
                          SELECT id FROM yb_datacenter.yb_warehouse WHERE type IN ('third', 'overseas'))
                  ORDER BY date_id DESC
                  LIMIT 1 BY sku, warehouse_id
              )AS ps
              INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
              LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
              WHERE         
                  yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
                  and (yw.warehouse_other_type = 2 or warehouse_id in (958))   -- 筛选公共仓（非子仓\独享仓）
                  and yw.warehouse_name not like '%独享%'
              ORDER BY date_id DESC
          ) a
          GROUP BY sku, warehouse, warehouse_name
      """
    df_stock = conn_ck.ck_select_to_df(sql)

    df_stock.to_excel('F://Desktop//df_stock_temp.xlsx', index=0)

stock_temp()
##
def supplier_sku_price():
    # 2、取供货价
    # sql = """
    #     SELECT sku YM_sku, country_code country, warehouse_price
    #     FROM yibai_prod_base_sync.yibai_prod_sku_pallet_price
    #     WHERE is_del = 0
    # """
    # conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    # df_sup_cost = conn_ck.ck_select_to_df(sql)
    #
    # df_sup_cost.to_excel('F://Desktop//df_sup_cost.xlsx', index=0)

    # sql = """
    # SELECT Distinct
    #     trimBoth(YB_sku) AS sku, warehouse, warehouse_name,
    #     '' warehouse_code, warehouse_id, 3 warehouse_other_type,
    #     available_stock stock, available_stock, 0 on_way_stock, 0 wait_outbound, 0 frozen_stock
    # FROM yibai_oversea.dwm_supplier_sku_price a
    # WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price) and warehouse != 'CN'
    # and warehouse_name is not Null
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_sup = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT *
        FROM cj_temp_szz.yibai_oversea_warehouse_destroy_evaluate
        WHERE insert_date = (SELECT max(insert_date) FROM cj_temp_szz.yibai_oversea_warehouse_destroy_evaluate)
    """
    # conn_ck = pd_to_ck(database='cj_temp_szz', data_sys='cj_本地库')
    conn = connect_to_sql(database='cj_temp_szz', data_sys='cj_本地库')
    df_sup = conn.read_sql(sql)

    print(df_sup.info())
    df_sup.to_excel('F://Desktop//df_destroy_temp.xlsx', index=0)

supplier_sku_price()
##
if __name__ == '__main__':
    utils.program_name = '临时文件夹'
    make_path()

