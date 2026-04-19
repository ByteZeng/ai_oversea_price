"""
海外仓库龄数据汇总
1、优先服务商库龄、saas墨西哥仓库龄、精铺库龄
2、其次云仓库龄数据
"""

import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_line, get_stock,  get_sku_sales_new,get_transport_fee,\
    get_rate,get_platform_fee, write_to_sql, cut_bins, is_new_sku, flash_clearout, get_destroy_profit
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse, tt_get_oversea_order
warnings.filterwarnings("ignore")

def get_fine_sku_age():
    """ 获取精铺转泛品sku的库龄信息 """
    # 取库龄
    sql = """
        SELECT 
            sku, warehouse_name, '' charge_currency, '' cargo_type, warehouse_stock, 
            90 inventory_age, 0 charge_total_price,
            age_30_plus, age_60_plus,age_90_plus,age_120_plus,
            age_150_plus,age_180_plus, 0 age_270_plus, 0 age_360_plus
        FROM over_sea.fine_sku_age
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_age = conn.read_sql(sql)

    sql = """
        SELECT a.warehouse_code warehouse_code, a.id as warehouse_id, a.warehouse_name warehouse_name,
        b.name warehouse
        FROM yibai_logistics_tms_sync.yibai_warehouse a
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df_age, df_warehouse, how='left', on=['warehouse_name'])

    print(df.info())
    # df_age.to_excel('F://Desktop//df_age.xlsx', index=0)
    return df

def get_stock_age():
    """
    获取服务商库龄数据
    """
    # 服务商库龄数据校验
    # check_age_data()

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
        -- and yw.warehouse_name not like '%独享%' 
        -- and yw.warehouse_name not like '%TT%'
        -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
        -- and yw.id not in (339)   -- 剔除不常用仓库，避免最优子仓选到无运费的子仓上
    """
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 20250211 补充精铺转泛品sku的库龄数据
    df_fine_age = get_fine_sku_age()
    df_stock_age = pd.concat([df_stock_age, df_fine_age])
    # 仓租处理
    df_rate = get_rate()
    df_rate = df_rate.drop_duplicates(subset='charge_currency')
    df_stock_age = pd.merge(df_stock_age, df_rate, how='left', on='charge_currency')
    df_stock_age['rate'] = df_stock_age['rate'].fillna(0)
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].fillna(0).astype(float)
    df_stock_age['charge_total_price_rmb'] = df_stock_age['charge_total_price'] * df_stock_age['rate']
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].astype('float')

    # 按 sku + warehouse_id 聚合
    df_stock_age_base = df_stock_age[['sku', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    col = ['sku', 'warehouse_id', 'warehouse_stock', 'inventory_age', 'charge_total_price_rmb','age_30_plus',
           'age_60_plus','age_90_plus','age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']
    df_stock_age_info = df_stock_age[col]
    df_stock_age_info = df_stock_age_info[~df_stock_age_info['warehouse_id'].isna()]
    df_stock_age_info[['warehouse_id','inventory_age']] = df_stock_age_info[['warehouse_id','inventory_age']].astype(int)

    df_stock_age_info.loc[:,'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'stock_age']].groupby(
        ['sku', 'warehouse_id']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg({'warehouse_stock':'sum','inventory_age':'max','charge_total_price_rmb':'sum','age_30_plus':'sum','age_60_plus':'sum','age_90_plus':'sum',
                                                                              'age_120_plus':'sum','age_150_plus':'sum','age_180_plus':'sum','age_270_plus':'sum','age_360_plus':'sum'}).reset_index()
    df_stock_age_id = df_stock_age_id.rename(columns={'inventory_age':'max_age'})
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    return df_stock_age_id

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
            SELECT sku, `virtual_warehosue`, warehouse_name, cargo_owner_id, sum(saas_stock) as saas_stock, stock_age
            FROM (
                SELECT 
                    w, c.name `virtual_warehosue`, d.name `warehouse_name`,b.sku sku, b.client_sku client_sku, 
                    a.sku as oversea_sku, cargo_owner_id, 
                    instock_stock-out_stock as saas_stock, storage_age_date, 
                    toInt32(today() - toDate(storage_age_date)) as stock_age,
                    today()
                FROM yb_datacenter.yibai_stock_age_detail a
                LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                ON a.sku = b.oversea_sku
                LEFT JOIN yb_datacenter.yb_warehouse c 
                on a.w = c.code
                LEFT JOIN yb_datacenter.yb_warehouse d 
                on c.real_warehouse_id = d.id
                WHERE w = 'YM-MX-2'
            ) a
            GROUP BY sku, cargo_owner_id, c.name, d.name, stock_age
            HAVING saas_stock > 0 and cargo_owner_id = 8
        )
    ) a
    GROUP BY sku, cargo_owner_id
    """
    sql = """
    SELECT 
        sku, cargo_owner_id, warehouse_name, sum(saas_stock) saas_stock, max(stock_age) max_age,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus
    FROM (
        SELECT
            sku, warehouse_name, cargo_owner_id, saas_stock, stock_age,
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
            SELECT sku, warehouse_name, cargo_owner_id, sum(saas_stock) as saas_stock, stock_age
            FROM (
                SELECT 
                    w, c.name warehouse_name, b.sku sku, b.client_sku client_sku, 
                    a.sku as oversea_sku, cargo_owner_id, 
                    instock_stock-out_stock as saas_stock, storage_age_date, 
                    toInt32(today() - toDate(storage_age_date)) as stock_age,
                    today()
                FROM yb_datacenter.yibai_stock_age_detail a
                LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                ON a.sku = b.oversea_sku
                LEFT JOIN yb_datacenter.yb_warehouse c 
                on a.w = c.code
                WHERE w = 'YM-MX-2'
            ) a
            GROUP BY sku, cargo_owner_id, warehouse_name, stock_age
            HAVING saas_stock > 0
        ) b
    ) c
    GROUP BY sku, cargo_owner_id, warehouse_name
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)
    # 墨西哥实体仓就一个warehouse_name:YM墨西哥仓
    df_mx_stock.drop('warehouse_name', axis=1, inplace=True)
    # 获取云仓库存数据
    date_today = time.strftime('%Y%m%d')
    sql = f"""
    SELECT 
        sku, warehouse_id, warehouse_name, warehouse, arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age, sum(available_stock) as available_stock,
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
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            -- and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            -- and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            -- and yw.id not in (646, 648) -- BD、WEIS墨西哥仓
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    ) a
    GROUP BY sku, warehouse_id, warehouse_name, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_temp = conn_ck.ck_select_to_df(sql)

    df_mx = pd.merge(df_stock_temp, df_mx_stock[df_mx_stock['cargo_owner_id']==8], how='left', on=['sku'])
    # YM库龄库存按比例分解
    df_mx['占比'] = df_mx['available_stock'] / df_mx['saas_stock']
    df_mx['占比'] = np.where(df_mx['占比'] > 1, 1, df_mx['占比'])
    #
    col_list = ['age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus',
                'age_360_plus']
    for i in col_list:
        df_mx[i] = df_mx[i] * df_mx['占比']
    #
    df_mx['warehouse_stock'] = np.where(df_mx['saas_stock'].isna(), df_mx['saas_stock'], df_mx['available_stock'])

    df_mx = df_mx[
        ['sku', 'warehouse_stock', 'age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus',
         'age_150_plus','age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock_age',
         'warehouse_id', 'warehouse_name','max_age']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    # df_mx.to_excel('F://Desktop//df_mx.xlsx', index=0)

    return df_mx

def get_sku_sales_new_id():
    """
    20241118:合并通拓和易佰的sku销量
    """
    # yibai
    sql = """

        SELECT 
            SKU as sku,a.warehouse_id warehouse_id, warehouse_name, platform_code,3days_sales,
            7days_sales,15days_sales,30days_sales,
            60days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE 
            platform_code not in ('DIS','WYLFX') and b.warehouse is not Null 
            and b.warehouse_name not like '%%独享%%' 
            -- and b.warehouse_name not like '%%TT%%'

    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yibai_sales = conn.read_sql(sql)

    # TT
    sql = """

        SELECT 
            sku, warehouse_id, platform_code, 3days_sales,7days_sales,15days_sales,30days_sales,
            60days_sales,90days_sales 
        FROM tt_oversea.tt_sku_sales_statistics a
        -- WHERE toString(warehouse_id) not like '%10000%'

    """
    # ck_client = pd_to_ck(database='tt_oversea', data_sys='调价明细历史数据t')
    ck_client = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_tt_sales = ck_client.ck_select_to_df(sql)
    tt_warehouse = tt_get_warehouse()
    df_tt_sales = pd.merge(df_tt_sales, tt_warehouse[['warehouse_id', 'warehouse_name', 'country', 'warehouse']],
                           how='inner', on=['warehouse_id'])

    df_sales = pd.concat([df_yibai_sales, df_tt_sales])
    # 按大仓聚合
    df = df_sales.groupby(['sku', 'warehouse_id','warehouse']).agg({'3days_sales': 'sum', '7days_sales': 'sum', '15days_sales': 'sum',
                                                     '30days_sales': 'sum', '60days_sales': 'sum','90days_sales': 'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales'] / 7 + 0.2 * df['15days_sales'] / 15 + 0.1 * df['30days_sales'] / 30
    df['recent_day_sales'] = 0.8 * df['3days_sales'] / 3 + 0.2 * df['7days_sales'] / 7
    # df_sales = df_sales.sample(50000)
    return df

def dwm_oversea_sku_temp():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """

    print('获取库存信息...')
    df_stock = get_stock()
    # df_stock = df_stock[df_stock['available_stock']>0]
    print('获取库龄信息...')
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_mx_age = get_mx_stock_age()
    df_stock_age_warehouse = pd.concat([df_stock_age_id, df_mx_age])

    # 服务商库龄
    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse_id'])

    # 云仓库龄
    cloud_age = cloud_stock_age()
    print(dwm_sku.info())

    df_rate = get_rate()
    df_rate = df_rate.drop_duplicates(subset='charge_currency')
    dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    dwm_sku.drop(['charge_total_price',  'rate'], axis=1, inplace=True)

    dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')

    dwm_sku[dwm_sku['stock']>0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)


    return None

##库龄数据整理
def cloud_stock_age():
    """ 云仓库存库龄数据 """
    # 云仓库龄
    sql = """
    SELECT
        sku, cargo_owner_id, warehouse_name, real_warehouse_name,warehouse_id, 
        sum(age_stock) warehouse_stock, max(stock_age) max_age,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus,
        arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age
    FROM (
        SELECT
            sku, warehouse_name, real_warehouse_name, warehouse_id, cargo_owner_id,  age_stock, stock_age,
            concat(toString(age_stock), ':', toString(stock_age)) as stock_info,
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
            case when stock_age >= 30 then age_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then age_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then age_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then age_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then age_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then age_stock else 0 end as age_180_plus,
            case when stock_age >= 270 then age_stock else 0 end as age_270_plus,
            case when stock_age >= 360 then age_stock else 0 end as age_360_plus
        FROM (
            SELECT
                yw0.warehouse_name `warehouse_name`,
                yw0.id `warehouse_id`,
                yw2.warehouse_name `real_warehouse_name`,
                -- ifnull(yw2.name, yw.name) `real_warehouse_name`,
                ysd.cargo_owner_id `cargo_owner_id`,
                ysd.sku sku,
                receipt_quantity - delivery_quantity `age_stock`,
                dateDiff('day', date(receipt_time), today()) `stock_age`,
                '云仓' `data_source`
            FROM yb_stock_center_sync.yb_stockage_detail ysd
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse yw0 ON ysd.warehouse_id = yw0.id
            LEFT JOIN yb_datacenter.yb_warehouse yw ON ysd.warehouse_id = yw.id
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse yw2 on yw.real_warehouse_id = yw2.id
            WHERE
                yw.`type` IN ('overseas', 'third')
                AND receipt_quantity - delivery_quantity>0
        ) a
    ) b
    GROUP BY sku, cargo_owner_id, real_warehouse_name, warehouse_name, warehouse_id
    """
    conn_ck = pd_to_ck(database='yb_stock_center_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df['warehouse_id'] = df['warehouse_id'].astype(int)
    df['warehouse_stock_age'] = (
            df['warehouse_id'].astype(str) + ":{'" + df['warehouse_stock_age'] + "'}")
    # df_cloud_age.to_excel('F://Desktop//df_cloud_age.xlsx', index=0)

    return df

def warehouse_temp():

    sql = """
        SELECT 
            yw.id AS warehouse_id,   
            yw.warehouse_name AS real_warehouse_name, yw.warehouse_code AS warehouse_code,
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

def oversea_stock_age():
    """
    海外仓库龄数据汇总：
    1、优先服务商库龄、saas墨西哥仓库龄、精铺库龄
    2、其次云仓库龄数据
    """
    print('获取库龄信息...')

    # 服务商库龄数据
    df_stock_age_id = get_stock_age()
    df_mx_age = get_mx_stock_age()
    stock_age = pd.concat([df_stock_age_id, df_mx_age])
    # 服务商库龄的货主ID默认算8
    stock_age['cargo_owner_id'] = 8
    # 获取真实子仓名
    sql = """
        SELECT a.id warehouse_id, a.real_warehouse_id, b.name real_warehouse_name
        FROM yb_datacenter.yb_warehouse a
        LEFT JOIN yb_datacenter.yb_warehouse b
        ON a.real_warehouse_id = b.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    stock_age = pd.merge(stock_age, df_warehouse[['warehouse_id', 'real_warehouse_name']],
                                      how='left', on=['warehouse_id'])
    stock_age['source'] = 1
    print(stock_age.info())

    # 云仓库龄
    cloud_age = cloud_stock_age()
    cloud_age['source'] = 2

    df_age = pd.concat([stock_age, cloud_age])
    # dwm_sku = pd.merge(dwm_sku, cloud_age, how='left', on=['sku','warehouse_id'])

    # 大仓
    df_warehouse_2 = warehouse_temp()
    df_warehouse_2 = df_warehouse_2.drop_duplicates(subset=['real_warehouse_name','warehouse'])
    df_age = pd.merge(df_age, df_warehouse_2[['real_warehouse_name','warehouse']], how='left', on=['real_warehouse_name'])

    df_age = df_age.sort_values(by='source', ascending=True)
    df_age = df_age.drop_duplicates(subset=['sku','warehouse_name', 'cargo_owner_id'])

    df_age.drop('overage_level', axis=1, inplace=True)
    df_age['date_id'] = time.strftime('%Y-%m-%d')

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_age, 'dwd_oversea_age', if_exists='replace')
    write_to_sql(df_age, 'dwd_oversea_age')
    print(df_age.info())

    # df_age.to_excel('F://Desktop//df_age_2.xlsx', index=0)

    return df_age

def dwm_stock_age():
    """
    获取库龄数据
    处理库龄数据
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # 调整库龄取数的日期，针对某个日期库龄数据不完整的情况
    df_stock_age = oversea_stock_age()
    df_stock_age = df_stock_age[~df_stock_age['warehouse_name'].isna()]
    # YB调价程序涉及到的仓库库龄数据
    df_stock_age = df_stock_age[(df_stock_age['cargo_owner_id']==8) & (~df_stock_age['warehouse_name'].str.contains('独享'))]
    col_id = [1662,1661,1667,1786,777,1736,1731,847]   # 移仓换标、测试仓
    df_stock_age = df_stock_age[~df_stock_age['warehouse_id'].isin(col_id)]
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id, warehouse, available_stock, warehouse_name
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 and (warehouse_other_type = 2 or warehouse_id in (958,1833) or warehouse_name like '%%帮清%%')
    and warehouse_name NOT IN ('云仓美西仓','云仓波兰仓','HC美国西仓','英国仓UK2(移仓换标)',
    'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')
    """
    df_stock = ck_client.ck_select_to_df(sql)
    df_stock_age_id = pd.merge(df_stock_age, df_stock[['sku','warehouse_id']], how='inner', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[['sku', 'warehouse', 'warehouse_stock', 'charge_total_price_rmb', 'age_30_plus','age_60_plus','age_90_plus',
                               'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']]
    df_temp_2 = df_stock_age_id[['sku', 'warehouse', 'source','warehouse_stock_age']]
    df_temp_2 = df_temp_2.groupby(['sku', 'warehouse']).agg({'warehouse_stock_age': list,'source':'max'}).reset_index()
    df_stock_age_warehouse = df_temp.groupby(['sku', 'warehouse']).sum().reset_index()
    df_stock_age_warehouse = pd.merge(df_stock_age_warehouse, df_temp_2, how='left', on=['sku', 'warehouse'])
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
        by=['sku', 'warehouse', 'max_age', 'charge_total_price_rmb', 'warehouse_stock'],
        ascending=[True, True, False, False, False])
    df_best_id = df_stock_age_id[['sku', 'warehouse_id', 'warehouse_name', 'warehouse']].drop_duplicates(
        subset=['sku', 'warehouse'])
    df_best_id.rename(columns={'warehouse_id': 'best_warehouse_id', 'warehouse_name': 'best_warehouse_name'},
                      inplace=True)
    df_stock_age_id = pd.merge(df_stock_age_id, df_best_id, how='left', on=['sku', 'warehouse'])
    df_stock_age_warehouse = pd.merge(df_stock_age_warehouse, df_best_id, how='left', on=['sku', 'warehouse'])

    df_stock_age_warehouse = df_stock_age_warehouse[df_stock_age_warehouse['warehouse_stock']>0]
    df_stock_age_warehouse['date_id'] = time.strftime('%Y-%m-%d')

    # df_stock_age_warehouse.to_excel('F://Desktop//dwm_stock_age.xlsx', index=0)
    print(df_stock_age_warehouse.info())
    write_to_sql(df_stock_age_warehouse, 'dwm_stock_age')

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_stock_age_warehouse, 'dwm_stock_age', if_exists='append')

    return None

def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = time.strftime('%Y-%m-%d')

    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()


def dwm_oversea_sku_temp():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    print('获取库存信息...')
    df_stock = get_stock()
    # df_stock = df_stock[df_stock['available_stock']>0]
    # df_stock.drop(['create_time','warehouse_other_type'], axis=1, inplace=True)
    print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    # df_mx_age = get_mx_stock_age()
    # df_stock_age_warehouse = pd.concat([df_stock_age_warehouse, df_mx_age])
    sql = """
        SELECT *
        FROM over_sea.dwm_stock_age
        WHERE date_id = (SELECT max(date_id) FROM dwm_stock_age WHERE date_id > '2025-08-01')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock_age_warehouse = conn.read_sql(sql)
    print('获取日销信息...')
    sku_sales = get_sku_sales_new()

    col = ['sku','warehouse','3days_sales','7days_sales','30days_sales','60days_sales','90days_sales','recent_day_sales','day_sales']
    dwm_sku = pd.merge(df_stock, sku_sales[col], how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、库龄缺失
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 24:33] = dwm_sku.iloc[:, 24:33].fillna(0)
    # 2025-01-15 乌拉圭仓手动置为清仓状态
    dwm_sku['overage_level'] = np.where((dwm_sku['warehouse'] == '乌拉圭仓') & (dwm_sku['available_stock']>0),
                                        360, dwm_sku['overage_level'])
    dwm_sku['age_360_plus'] = np.where(dwm_sku['warehouse'] == '乌拉圭仓', dwm_sku['available_stock'], dwm_sku['age_360_plus'])
    # 2025-05-16 东南亚手动置为清仓状态
    col = ['泰国仓','越南仓','菲律宾仓','马来西亚仓','印度尼西亚仓']
    dwm_sku['overage_level'] = np.where((dwm_sku['warehouse'].isin(col)) & (dwm_sku['available_stock']>0),
                                        180, dwm_sku['overage_level'])
    for i in ['age_30_plus','age_60_plus','age_90_plus','age_120_plus','age_150_plus','age_180_plus']:
        dwm_sku[i] = np.where(dwm_sku['warehouse'].isin(col), dwm_sku['available_stock'], dwm_sku[i])
    # 20250717 海兔sku手动置为清仓状态
    c1 = (dwm_sku['type'].str.contains('海兔|易通兔'))
    dwm_sku['overage_level'] = np.where(c1 & (dwm_sku['available_stock']>0),
                                        180, dwm_sku['overage_level'])
    for i in ['age_30_plus','age_60_plus','age_90_plus','age_120_plus','age_150_plus','age_180_plus']:
        dwm_sku[i] = np.where(c1, dwm_sku['available_stock'], dwm_sku[i])

    # dwm_sku.info()
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])
    # 20241125 当sku+大仓下的库存为0时，最优子仓取当前大仓下常用的子仓（大仓下库存最多的子仓，非精铺仓）。避免最优子仓选到不常用仓库上。
    df_stock_max = df_stock.groupby(['warehouse', 'warehouse_id', 'best_warehouse'])[
        'available_stock'].sum().reset_index()
    df_stock_max = df_stock_max.rename(columns={'warehouse_id': 'max_id', 'best_warehouse': 'max_name'})
    df_stock_max = df_stock_max.sort_values(by='available_stock', ascending=False).drop_duplicates(subset=['warehouse'])
    dwm_sku = pd.merge(dwm_sku, df_stock_max[['warehouse', 'max_id', 'max_name']], how='left', on=['warehouse'])
    # 20250324 精铺子仓的最优子仓限定在精铺仓，不转化为常用子仓
    c1 = (dwm_sku['available_stock'] <= 0) & (~dwm_sku['best_warehouse_name'].str.contains('精铺'))
    dwm_sku['best_warehouse_id'] = np.where(c1, dwm_sku['max_id'], dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(c1, dwm_sku['max_name'], dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['max_id','max_name','warehouse_id', 'best_warehouse'], axis=1, inplace=True)

    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus', 'age_60_plus','age_90_plus',
                     'age_120_plus',
                     'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb',
                     '3days_sales', '7days_sales', '30days_sales', '60days_sales','90days_sales', 'day_sales', 'recent_day_sales', ]
    dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']] = dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']].astype(int)
    dwm_sku = dwm_sku[columns_order]

    # 可售天数 (大仓的总库存）
    dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(
        np.inf, 9999).replace(np.nan, 0)
    d1 = (pd.to_datetime('2025-09-30') - datetime.datetime.now()).days
    dwm_sku['all_esd'] = ((dwm_sku['available_stock']+dwm_sku['on_way_stock']) / dwm_sku['recent_day_sales']).replace(
        np.inf, 9999).replace(np.nan, 0) - d1

    # 超库龄的可售天数（超 i 天库龄库存的可售天数）
    for i in dwm_sku['overage_level'].unique():
        if np.isnan(i) or i == 0:
            continue
        else:
            c = dwm_sku['overage_level'] == i
            dwm_sku.loc[c, 'overage_esd'] = (
                        dwm_sku.loc[c, 'age_{}_plus'.format(int(i))] / dwm_sku.loc[c, 'day_sales']).replace(np.inf,
                                                                                                            9999).replace(
                np.nan, 0)

    # 销售状态分类:根据超库龄情况判断分类。
    dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print('条件分箱...')
    dwm_sku_2 = cut_bins(dwm_sku)

    # 判断是否新品
    df_new_sku = is_new_sku()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_new_sku, how='left', on=['sku'])
    dwm_sku_2['overage_level'] = dwm_sku_2['overage_level'].fillna(0).astype(int)
    #
    dwm_sku_2['is_new'] = dwm_sku_2['is_new'].fillna(0).astype(int)

    # 获取【降价及回调阶梯】
    sql = """
    SELECT *
    FROM profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    profit_rate_section = conn.read_sql(sql)

    dwm_sku_2 = pd.merge(dwm_sku_2, profit_rate_section, how='left',
                         on=['overage_level', 'overage_esd_bins', 'day_sales_bins'])

    # 20250619补充限时清仓逻辑
    dwm_sku_2 = flash_clearout(dwm_sku_2)

    sql = """
    SELECT *
    FROM up_rate_section
    """
    up_rate_section = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, up_rate_section, how='left', on=['overage_level', 'esd_bins'])

    # dwm_sku_2[dwm_sku_2['available_stock']>0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)

    # 20250321 精铺转泛品的兜底价单独设置
    c1 = (dwm_sku_2['overage_level'] >= 360) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺'))
    c2 = (dwm_sku_2['overage_level'] >= 270) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺'))
    c3 = (dwm_sku_2['overage_level'] >= 180) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺'))
    c4 = (dwm_sku_2['overage_level'] >= 150) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺'))
    c5 = (dwm_sku_2['overage_level'] >= 120) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺'))
    dwm_sku_2['lowest_profit'] = np.select([c1, c2, c3, c4, c5],[-0.5, -0.4, -0.3, -0.3, -0.3], dwm_sku_2['lowest_profit'])
    c1 = (dwm_sku_2['linest'].str.contains('家具')) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺')) & \
         (dwm_sku_2['lowest_profit'] > -0.4)
    dwm_sku_2['lowest_profit'] = np.where(c1, -0.4, dwm_sku_2['lowest_profit'])
    c1 = (dwm_sku_2['last_linest'].str.contains('镜子')) & (dwm_sku_2['best_warehouse_name'].str.contains('精铺')) & \
         (dwm_sku_2['lowest_profit'] > -0.4)
    dwm_sku_2['lowest_profit'] = np.where(c1, -0.5, dwm_sku_2['lowest_profit'])
    # 20250616 低销sku兜底下调30%
    # c1 = dwm_sku_2['day_sales'] < 0.1
    # dwm_sku_2['lowest_profit'] = np.where(c1, dwm_sku_2['lowest_profit']-0.3, dwm_sku_2['lowest_profit'])
    dwm_sku_2['lowest_profit'] = np.where((dwm_sku_2['warehouse'].str.contains('乌拉圭')), -999, dwm_sku_2['lowest_profit'])
    # 20250516 东南亚仓兜底手动设置-50%
    col = ['泰国仓','越南仓','菲律宾仓','马来西亚仓','印度尼西亚仓']
    dwm_sku_2['lowest_profit'] = np.where(dwm_sku_2['warehouse'].isin(col), -0.5, dwm_sku_2['lowest_profit'])

    # 20250724 高仓租逻辑：库存金额/月仓租不同区间， 兜底下调
    dwm_sku_2['charge_temp'] = dwm_sku_2['available_stock_money']/(dwm_sku_2['charge_total_price_rmb']*30)
    dwm_sku_2['charge_temp'] = dwm_sku_2['charge_temp'].replace([np.inf, -np.inf], 999)
    c1 = (dwm_sku_2['charge_temp'] > 0) & (dwm_sku_2['charge_temp'] <= 2)
    c2 = (dwm_sku_2['charge_temp'] > 2) & (dwm_sku_2['charge_temp'] <= 4)
    c3 = (dwm_sku_2['charge_temp'] > 4) & (dwm_sku_2['charge_temp'] <= 6)
    c4 = (dwm_sku_2['charge_temp'] > 6) & (dwm_sku_2['charge_temp'] <= 12)
    dwm_sku_2['lowest_profit'] = np.select([c1,c2,c3,c4],
                                           [-999, dwm_sku_2['lowest_profit']-0.2,
                                            dwm_sku_2['lowest_profit']-0.1, dwm_sku_2['lowest_profit']-0.05],
                                           dwm_sku_2['lowest_profit'])
    dwm_sku_2['charge_level'] = np.select([c1, c2, c3, c4], [1,2, 3, 4],0)
    # 20250724 销毁价过低时，增加降幅
    df_destroy = get_destroy_profit()
    col = ['sku','warehouse','target_profit_rate','lowest_price_profit']
    dwm_sku_2 = pd.merge(dwm_sku_2, df_destroy[col], how='left', on=['sku','warehouse'])
    c1 = dwm_sku_2['target_profit_rate'] < -0.6
    c2 = dwm_sku_2['lowest_price_profit'] < -1
    c3 = dwm_sku_2['section'] <= -0.1
    c4 = dwm_sku_2['target_profit_rate'] > dwm_sku_2['lowest_price_profit']
    dwm_sku_2['单次最大降幅'] = dwm_sku_2.apply(lambda row: max(abs(row['section']),
                                                                abs(-0.6 - row['lowest_price_profit']) / 10),axis=1)
    dwm_sku_2['section'] = np.where(c1&c2&c3&c4, -dwm_sku_2['单次最大降幅'], dwm_sku_2['section'])

    # 控制单次降幅最高100%
    dwm_sku_2['section'] = np.where(dwm_sku_2['section']<-1, -1, dwm_sku_2['section'])

    conn.to_sql(dwm_sku_2, 'dwm_sku_temp_temp', if_exists='replace')
    dwm_sku_2.drop(['charge_temp','charge_level','单次最大降幅','lowest_price_profit','target_profit_rate'], axis=1, inplace=True)

    # 获取前一天的after_profit
    sql = f"""
    SELECT distinct sku, warehouse, after_profit as after_profit_yest
    FROM dwm_sku_temp_info
    WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id < '{time.strftime('%Y-%m-%d')}')
    """
    df_after_yest = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_after_yest, how='left', on=['sku', 'warehouse'])
    dwm_sku_2['begin_profit'] = np.where(dwm_sku_2['after_profit_yest'].isna(), 0, dwm_sku_2['after_profit_yest'])
    dwm_sku_2.drop('after_profit_yest', axis=1, inplace=True)
    dwm_sku_2[['section']] = dwm_sku_2[['section']].fillna(0)
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    print(dwm_sku_2.info())
    # # 2024-12-04 近30天无销的可以无视兜底
    # c1 = (dwm_sku_2['30days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 30)
    # c2 = (dwm_sku_2['begin_profit'] <= (dwm_sku_2['lowest_profit']-0.08))
    # dwm_sku_2['section'] = np.where(c1 & c2, -0.03, dwm_sku_2['section'])
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['up_profit_rate'] > 0,
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['up_profit_rate'],
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['section'])
    # 20241206 调价周期设置在sku+大仓位置
    # print(f'调价周期函数前共{len(dwm_sku_2)}条数据。')
    # dwm_sku_2 = adjust_cycle_new(dwm_sku_2)
    # print(f'调价周期函数后，共{len(dwm_sku_2)}条数据。')


    # 调价最高幅度不超过 0 （暂无涨价缩销的情况）
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] > 0, 0, dwm_sku_2['after_profit'])
    # 库存为0时，调价幅度置为0
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['available_stock'] <= 0, 0, dwm_sku_2['after_profit'])

    # 低销降价未回调bug修复
    # 待核查影响
    # c1 = (dwm_sku_2['30days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 30)
    c2 = (dwm_sku_2['overage_level'] < 60)
    dwm_sku_2['after_profit'] = np.where(c2 , 0, dwm_sku_2['after_profit'])

    # 20231227 (调价幅度+平台最低净利率)最低不超过保底净利率。不超90天库龄的兜底为0净利率
    dwm_sku_2['lowest_profit'] = np.where((dwm_sku_2['overage_level']<60), 0, dwm_sku_2['lowest_profit'])
    c3 = dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.08)
    dwm_sku_2['after_profit'] = np.where(c3 ,
                                         dwm_sku_2['lowest_profit']-0.08, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    dwm_sku_2.drop('60days_sales', axis=1,inplace=True)

    # 20241224 白名单sku修改清仓状态。
    # 第一批截止时间2025-02-01
    sql = """
        SELECT sku, warehouse, is_clear
        FROM over_sea.sales_status_temp
        WHERE is_clear = 0
    """
    df_temp_sku = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_temp_sku, how='left', on=['sku', 'warehouse'])
    dwm_sku_2['after_profit']= np.where(~dwm_sku_2['is_clear'].isna(), 0, dwm_sku_2['after_profit'])
    dwm_sku_2.drop('is_clear', axis=1, inplace=True)

    print(dwm_sku_2.info())
    print('SKU信息及调价幅度已获取，准备写入dwm_sku_temp_info_1...')
    write_to_sql(dwm_sku_2, 'dwm_sku_temp_info_1')
    # dwm_sku_2 = dwm_sku_2[dwm_sku_2['available_stock']>0]
    # dwm_sku_2.to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)


    # write_to_ck(dwm_sku_2, 'dwm_sku_temp_info')

    return None

def dwm_sku_price():
    """
    销售状态设置
    调价周期设置 *
    价格计算
    """

    # 读取dwm_sku_2
    sql = """
    SELECT *
    FROM dwm_sku_temp_info_1
    WHERE date_id = curdate()
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku = conn.read_sql(sql)
    print(dwm_sku.info())

    # 匹配运费
    # 运费数据需要处理！！！
    # WISH平台的汇率设置为1：
    df_transport_fee = get_transport_fee()
    df_transport_fee = df_transport_fee[df_transport_fee['platform'] == 'AMAZON']
    print(df_transport_fee.info())
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='left', on='country')
    df_transport_fee = df_transport_fee[
        ['sku', 'warehouse_id', 'ship_name', 'total_cost', 'lowest_price', 'platform', 'country',
         'rate']].drop_duplicates()

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate',
               'is_adjust','begin_profit']
    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level',]

    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_transport_fee, how='left',
                             left_on=['sku', 'best_warehouse_id'], right_on=['sku', 'warehouse_id'])
    # 无运费数据处理
    dwm_sku_price.drop(['warehouse_id'], axis=1, inplace=True)
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]

    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_sku_price = pd.merge(dwm_sku_price, df_platform_fee, how='left', on=['platform', 'country'])
    print(dwm_sku_price.info())
    dwm_sku_price.to_excel('F://Desktop//dwm_sku_price.xlsx', index=0)

def check_age_data():
    """ 服务商库龄数据校验 """
    sql = """
        with 
            groupArray((date, sku_cnt)) as sku_arr,
            toString(today()-2) as this_day,
            toString(today()-3) as last_day,
            ['WYT', 'GC', '4PX'] as warehouse_e_name,
            ['万邑通', '谷仓', '递四方'] as warehouse_cn_name,
            concat('【同步', transform(oversea_type,warehouse_e_name,warehouse_cn_name,oversea_type), '库龄】') as title
        select
            this_day, last_day, oversea_type, 
            arrayFirst(x->x.1==this_day, sku_arr).2 as this_day_sku,
            arrayFirst(x->x.1==last_day, sku_arr).2 as last_day_sku,
            abs(if(last_day_sku==0, 100, intDivOrZero((this_day_sku-last_day_sku)*10000, last_day_sku)/100)) as pctg,
            if(pctg <= 10, '正常', '异常') as status,
            concat(title, '\n', '状态：', status, '\n', this_day, '的sku数量：', toString(this_day_sku), '\n', 
                    last_day, '的sku数量：', toString(last_day_sku), '\n', '差异：', concat(toString(pctg), '%'), '\n',
                    '通知时间：', toString(now())) as msg
          from 
            (
                select date, oversea_type, count(distinct sku) as sku_cnt
                from  yb_datacenter.yb_oversea_sku_age 
                where date in (toString(today()-2), toString(today()-3))
                and sku not like 'RM%%'
                group by date,oversea_type
            )
        group by 
            oversea_type
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    df_age = conn_ck.ck_select_to_df(sql)
    for tup in zip(df_age['status'], df_age['msg']):
        print(tup[1])

    for tup in zip(df_age['status'], df_age['msg']):
        if tup[0] == '异常':
            raise Exception("库龄库存sku_age数量不正确")

if __name__ == '__main__':
    # dwm_oversea_sku()

    # get_mx_stock_age()
    # cloud_stock_age()
    # oversea_stock_age()

    dwm_stock_age()
    # get_stock_age()
    # dwm_oversea_sku_temp()

    # dwm_sku_price()

    # check_age_data()