"""
1、海外仓sku各平台目标价信息
2、海外仓调价sku达到销毁价情况、清仓sku出单利润率情况
"""
import pandas as pd
import numpy as np
import time, datetime
import warnings
import os
# from utils.utils import save_df, make_path
# from utils import utils
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import  get_rate
from all_auto_task.oversea_temu_price import get_temu_account
from all_auto_task.oversea_price_adjust_tt import tt_get_platform_fee
from all_auto_task.oversea_add_logic import get_temp_clear_sku
warnings.filterwarnings("ignore")

def save_df(df, data_name, program_name):
    cur_path = os.path.abspath(os.path.dirname(__file__))
    root_path = cur_path[:cur_path.find("yibai-price-strategy\\") + len("yibai-price-strategy\\")]
    date_now = time.strftime('%Y-%m-%d')
    if not os.path.exists(f"{root_path}/data/{date_now}"):
        os.mkdir(f"{root_path}/data/{date_now}")
    else:
        pass
    if not os.path.exists(f"{root_path}/data/{date_now}/{program_name}"):
        os.mkdir(f"{root_path}/data/{date_now}/{program_name}")
        print('data program path created!')
    else:
        pass

    df.to_excel(f'{root_path}/data/{date_now}/{program_name}/{data_name}.xlsx', index=False)

##
def get_sku_info():
    """ 获取海外仓sku信息 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT a.* ,b.sale_status as `销售状态` 
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info 
            WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
            -- and warehouse in ('美国仓','德国仓','西班牙仓','意大利仓', '法国仓', '英国仓','澳洲仓','加拿大仓','墨西哥仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓','英国仓','墨西哥仓','俄罗斯仓','澳洲仓','加拿大仓')
            and (available_stock > 0 or on_way_stock > 0)
            ) a
        left join (
            select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL) b
        on a.sku = b.sku AND a.warehouse = b.warehouse;
    """
    df = conn.read_sql(sql)
    # df.drop(['charge_currency','age_30_plus'], axis=1, inplace=True)
    df = df.sort_values(['sku', 'warehouse'])
    df['销售状态'] = df['销售状态'].fillna('正常')
    # 20250321 隔离精铺非转泛品的sku
    c0 = df['type'].str.contains('海兔|易通兔') | df['type'].str.contains('转VC|转泛品')
    c1 = (df['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) & (~c0)
    df = df[~c1]

    # 是否限时清仓sku
    # df_flash = get_flash_sku()
    df_flash = get_temp_clear_sku()
    df_flash = df_flash[['sku', 'warehouse']]
    # df_flash.columns = ['sku','warehouse','限时清仓分类']
    df_flash['是否限时清仓sku'] = 1
    df_flash = df_flash.drop_duplicates()
    df = pd.merge(df, df_flash[['sku', 'warehouse', '是否限时清仓sku']], how='left', on=['sku','warehouse'])

    # 是否2月清仓任务sku


    return df

def haitu_sku_info():
    """ 获取海外仓sku信息 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT a.* ,b.sale_status as `销售状态` 
        FROM (
            SELECT *
            FROM over_sea.dwd_sku_info 
            WHERE date_id = (SELECT max(date_id) FROM dwd_sku_info)
            -- and warehouse in ('美国仓','德国仓','西班牙仓','意大利仓', '法国仓', '英国仓','澳洲仓','加拿大仓','墨西哥仓')
            -- and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓','英国仓','墨西哥仓','俄罗斯仓','澳洲仓','加拿大仓')
            and (available_stock > 0 or on_way_stock > 0)
            ) a
        left join (
            select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL) b
        on a.sku = b.sku AND a.warehouse = b.warehouse;
    """
    df = conn.read_sql(sql)
    # df.drop(['charge_currency','age_30_plus'], axis=1, inplace=True)
    df = df.sort_values(['sku', 'warehouse'])
    df['销售状态'] = df['销售状态'].fillna('正常')
    df = df.rename(columns={'warehouse_name':'best_warehouse_name', 'warehouse_id':'best_warehouse_id'})

    # 指定sku
    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    df = df[df['sku'].isin(earphones_list) | df['type'].str.contains('海兔') |
            df['type'].str.contains('易通兔')]

    # 匹配调价降幅
    sql = f"""
        SELECT sku, warehouse, after_profit,overage_level, age_90_plus, age_180_plus, age_360_plus
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-08-01')
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓')
        and available_stock > 0
    """
    df_after_profit = conn.read_sql(sql)
    df = pd.merge(df, df_after_profit, how='left', on=['sku','warehouse'])
    col = ['overage_level', 'age_90_plus', 'age_180_plus', 'age_360_plus', 'after_profit']
    df[col] = df[col].fillna(0)

    # 是否限时清仓sku
    df_flash = get_flash_sku()
    df_flash['是否不暂缓清仓sku'] = 1
    df_flash = df_flash.drop_duplicates()
    df_flash = df_flash.rename(columns={'source':'限时清仓分类', '是否不暂缓清仓sku':'是否限时清仓sku'})
    df = pd.merge(df, df_flash[['sku', 'warehouse','限时清仓分类', '是否限时清仓sku']], how='left', on=['sku','warehouse'])
    df['限时清仓分类'] = np.where(df['type'].isin(['海兔转泛品','易通兔']), 'PartC', df['限时清仓分类'])

    return df



def get_useful_fee():
    """ 获取运费数据"""
    # 取调价程序
    pass


def yb_get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # sql = """
    #     SELECT platform_code platform, site country, toFloat32(net_profit2)/100 as net_profit2,
    #     toFloat32(net_interest_rate_target)/100 as net_interest_rate_target
    #     FROM tt_sale_center_listing_sync.tt_listing_profit_config
    #     WHERE shipping_type = 2 and is_del = 0 and status = 1
    # """
    # conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    # df_pc = conn_ck.ck_select_to_df(sql)
    #
    # # allegro, lazada, shopee按平台更新
    # col = ['platform', 'net_profit2', 'net_interest_rate_target']
    # df = pd.merge(df, df_pc[df_pc['platform'].isin(['ALLEGRO', 'LAZADA', 'SHOPEE','ALI'])][col], how='left', on=['platform'])
    # df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    # df['platform_must_percent'] = np.where(df['net_interest_rate_target'].isna(), df['platform_must_percent'],
    #                                        df['net_interest_rate_target'])
    # df.drop(['net_profit2', 'net_interest_rate_target'], axis=1, inplace=True)

    # df['platform_zero'] = df['platform'].replace({'ALLEGRO': 0.15, 'LAZADA': 0.2, 'SHOPEE': 0.19})

    return df

def get_mx_tob_stock():
    """ YM墨西哥toB打发仓库存信息获取"""
    df_stock = get_958_stock()
    print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_stock_age_warehouse = get_958_stock_age()
    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])

    dwm_sku = dwm_sku[dwm_sku['available_stock']>0]
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])

    col = ['sku','title','new_price','gross','product_package_size','best_warehouse_id', 'best_warehouse_name',
            'warehouse', 'available_stock', 'on_way_stock', 'warehouse_stock_age', 'overage_level']
    dwm_sku = dwm_sku[col]


    # dwm_sku.to_excel('F://Desktop//dwm_sku_mx.xlsx', index=0)

    return dwm_sku

def get_958_stock():
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
                where warehouse_id in (958)
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
    df_sku_stock = get_line_new(df_sku_stock)
    # 20250321 隔离精铺非转泛品的sku
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺')) & (~df_sku_stock['type'].str.contains('转泛品'))
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock

def get_958_stock_age():
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
            HAVING saas_stock > 0 and cargo_owner_id = 8
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
        sku, warehouse, arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age, sum(available_stock) as available_stock,
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
            and yw.id = 958  
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    ) a
    GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_temp = conn_ck.ck_select_to_df(sql)

    df_mx = pd.merge(df_stock_temp, df_mx_stock, how='left', on=['sku'])
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
    df_mx['best_warehouse_id'] = 958
    df_mx['best_warehouse_name'] = 'YM墨西哥toB代发仓'
    df_mx['charge_currency'] = 'MXN'
    df_mx['charge_total_price'] = 0

    #
    df_mx = df_mx[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus',
         'age_150_plus',
         'age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock_age', 'charge_currency', 'overage_level',
         'best_warehouse_id', 'best_warehouse_name']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    return df_mx

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
    df_line['linest'] = df_line['产品线路线'].str.split('->', expand=True)[0]
    df_line['二级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[1]
    df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    df_line['last_linest'] = df_line['产品线路线'].str.split('->', expand=True).iloc[:, -1]
    df_line = df_line.drop_duplicates(subset='sku')

    col = ['sku', 'type', 'linest', 'last_linest']

    df_result = pd.merge(df, df_line[col], how='left', on=['sku'])

    return df_result


def get_transport_fee(platform='AMAZON'):
    """
    获取当前最新运费数据
    """
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost totalCost_origin,
        firstCarrierCost, dutyCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH')
    -- and platform in ('ALI', 'ALLEGRO')

    UNION ALL

    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost totalCost_origin,
        firstCarrierCost, dutyCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name, 0 lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_shopee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df = df.drop_duplicates()
    # ali去掉指定渠道
    if platform == 'ALI':
        df = df[df['ship_name'] != 'Amazon线上-CK1美东2仓本地派送（VC-DF）']
    # 20250305 ALLEGRO平台运费临时处理
    df_alle = df[(df['platform'] == 'AMAZON') & (df['warehouse'].isin(['德国仓', '法国仓','西班牙仓','意大利仓'])) &
                 (df['country'].isin(['PL', 'CZ', 'SK', 'HU']))]
    df_alle['platform'] = 'ALLEGRO'
    df_plat = yb_get_platform_fee()
    df_plat = df_plat[df_plat['platform'] == 'ALLEGRO']
    df_alle = pd.merge(df_alle, df_plat[['country', 'ppve', 'refound_fee']], how='left', on=['country'])
    df_alle['lowest_price'] = 1.2 * (
                df_alle['totalCost_origin'] - df_alle['firstCarrierCost'] - df_alle['dutyCost']) / (
                                      1 - df_alle['ppve'] - df_alle['refound_fee'] - 0.05)
    df_alle['lowest_price'] = df_alle['lowest_price'].fillna(0).astype(float).round(2)
    df_alle['total_cost'] = df_alle['totalCost_origin']
    df_alle.drop(['ppve', 'refound_fee'], axis=1, inplace=True)

    df = pd.concat([df, df_alle])
    # df.drop(['warehouse', 'totalCost_origin', 'firstCarrierCost', 'dutyCost'], axis=1, inplace=True)

    df = df.drop_duplicates(subset=['sku', 'warehouse_id','country'])
    df['warehouse_id'] = df['warehouse_id'].astype(int)
    df = df.rename(columns={'warehouse_id':'best_warehouse_id'})

    return df


def flash_sku_logic(df):
    """ 限时清仓sku降价逻辑（针对线下出建议定价的平台） """
    # 20260104 限时清仓不同part指定清仓截止日期。（分批次降到销毁价）
    d1 = (pd.to_datetime('2026-01-31') - datetime.datetime.now()).days
    d2 = (pd.to_datetime('2026-02-28') - datetime.datetime.now()).days
    d3 = (pd.to_datetime('2026-03-31') - datetime.datetime.now()).days
    d4 = (pd.to_datetime('2026-04-30') - datetime.datetime.now()).days
    c0 = (~df['是否限时清仓sku'].isna()) & (df['sku调价目标毛利率'] > df['销毁价毛利率']) & (df['available_stock'] > 0)
    c1 = c0 & (df['限时清仓分类']=='PartA')
    c2 = c0 & (df['限时清仓分类']=='PartB')
    c3 = c0 & (df['限时清仓分类']=='PartC')
    c4 = c0 & (df['限时清仓分类']=='PartD')
    r1 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d1/7 - 1, 1)
    r2 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d2/7 - 1, 1)
    r3 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d3/7 - 1, 1)
    r4 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d4/7 - 1, 1)
    # df['label'] = np.select([c1,c2,c3,c4], [1, 2, 3, 4], 0)
    df['sku调价目标毛利率'] = np.select([c1,c2,c3,c4], [r1, r2, r3, r4], df['sku调价目标毛利率'])

    return df

def get_ali_bundle_sku():
    """  """
    sql = """
        SELECT sku, new_price, gross, warehouse, warehouseName best_warehouse_name, warehouseId best_warehouse_id,
        available_stock, overage_level, shipCountry country, platform, shipName ship_name, totalCost totalCost_origin, 
        totalCost - firstCarrierCost - dutyCost ship_fee, firstCarrierCost
        FROM yibai_oversea.oversea_transport_fee_bundle
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_bundle)
        and platform = 'ALI'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    df_sku['country'] = df_sku['country'].replace('GB', 'UK')

    df_sku = df_sku.sort_values(by='ship_fee', ascending=True).drop_duplicates(
        subset=['sku','warehouse','platform','country'])


    return df_sku


def get_sku_price(platform='ALI', org='YB'):
    """
    计算各平台的调价目标价
    ali、allegro、shopee、lazada
    """
    # 1. 获取sku信息
    if org in ['YB','TT']:
        df = get_sku_info()
    elif org == 'haitu':
        df = haitu_sku_info()
    # TT 替换暂缓清仓sku
    # if org == 'TT':
    #     df_flash = pd.read_excel('F://Ding_workspace//202512海外仓国内仓清仓SKU明细(1).xlsx', dtype={'sku': str})
    #     df_flash['flash_clearout_new'] = df_flash['是否放缓'].replace({'不可放缓':1, '可放缓':0})
    #     df_flash = df_flash[['sku', 'warehouse','flash_clearout_new']]
    #     df = pd.merge(df, df_flash, how='left', on=['sku', 'warehouse'])
    #     df['flash_clearout_new'] = df['flash_clearout_new'].fillna(2).astype(int)
    #     df['是否不暂缓清仓sku'] = np.where(df['flash_clearout_new'] == 1, 1, df['是否不暂缓清仓sku'])
    #     df['是否不暂缓清仓sku'] = np.where(df['flash_clearout_new'] == 0, 0, df['是否不暂缓清仓sku'])
    # 筛选国家
    if platform == 'ALI':
        col = ['美国仓','德国仓','法国仓','西班牙仓','意大利仓','英国仓','墨西哥仓','俄罗斯仓','澳洲仓','加拿大仓']
        df = df[df['warehouse'].isin(col)]
    elif platform == 'ALLEGRO':
        col = ['德国仓','法国仓','西班牙仓','意大利仓']
        df = df[df['warehouse'].isin(col)]
    elif platform in ('SHOPEE', 'LAZADA'):
        col = ['泰国仓','菲律宾仓','越南仓','马来西亚仓','印度尼西亚仓']
        df = df[df['warehouse'].isin(col)]
    # print(df.info())
    # 2. 匹配运费
    df_transport_fee = get_transport_fee()
    col = ['sku', 'best_warehouse_id', 'ship_name', 'totalCost_origin', 'firstCarrierCost', 'ship_fee',
           'lowest_price', 'platform', 'country']
    df_transport_fee = df_transport_fee[col].drop_duplicates()
    # print(df_transport_fee.info())
    order_1 = ['sku', 'title','type','new_price','gross', 'product_package_size', 'best_warehouse_id',
               'best_warehouse_name', 'warehouse', 'available_stock', '是否限时清仓sku',
               'on_way_stock', '销售状态', 'overage_level', 'age_90_plus', 'age_180_plus','age_360_plus','after_profit']
    df.loc[df['best_warehouse_name'] == 'YM墨西哥toB代发仓', 'best_warehouse_id'] = 956
    df = pd.merge(df[order_1].fillna(0), df_transport_fee, how='left', on=['sku', 'best_warehouse_id'])
    df.loc[df['best_warehouse_name'] == 'YM墨西哥toB代发仓', 'best_warehouse_id'] = 958

    # 捆绑sku信息获取
    df_bundle = get_ali_bundle_sku()

    df = pd.concat([df, df_bundle])
    # 筛选有运费的数据
    df = df[~df['totalCost_origin'].isna()]
    df['platform'] = platform
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on='country')
    print(df.info())
    # 3. 匹配费率项
    if org in ['YB','haitu']:
        df_platform = yb_get_platform_fee()
    elif org == 'TT':
        df_platform = tt_get_platform_fee()
    if platform in ('ALI', 'ALLEGRO','SHOPEE', 'LAZADA'):
        df_platform = df_platform.drop_duplicates(subset='platform')
        df_platform.drop('country', axis=1, inplace=True)
    # 20251015 ali的ppve可能会随着是否海外托管而变化
    # 20251211 ali的费率项修改为16% + 15%，其中ppve占比2%， 差值+净利率29%（临时处理）
    if platform == 'ALI':
        df_platform.loc[df_platform['platform']=='ALI', 'ppve'] = 0.05
        df_platform.loc[df_platform['platform'] == 'ALI', 'platform_zero'] = 0.18
        df_platform.loc[df_platform['platform'] == 'ALI', 'platform_must_percent'] = 0.08
    df = pd.merge(df, df_platform, how='left', on=['platform'])

    col = ['new_price', 'totalCost_origin','ppve']
    df[col] = df[col].fillna(0).astype(float)
    df.loc[df['销售状态']!='正常','销毁价'] = 1.2 * df['ship_fee']/(1-df['ppve']-df['refound_fee']-0.03)
    df.loc[df['销售状态']!='正常','销毁价毛利率'] = 1 - df['ppve'] - (df['new_price'] + df['totalCost_origin']) / (df['销毁价'])
    # df['销毁价'] = 1.2 * df['ship_fee'] / (1 - df['ppve'] - df['refound_fee'] - 0.03)
    # df['销毁价毛利率'] = 1 - df['ppve'] - (df['new_price'] + df['totalCost_origin']) / (df['销毁价'])
    # 4. 计算定价
    col = [20, 10, 2, 0, -10, -20, -30, -40]
    for i in col:
        df.loc[df['销售状态']!='正常',f'毛利率{i}%_人民币'] = (df['new_price'] + df['totalCost_origin']) / (1 - df['ppve'] - i / 100)
    df['正常品定价_人民币'] = (df['new_price'] + df['totalCost_origin']) / (1 - df['ppve'] - df['platform_zero'] - df['platform_must_percent'])
    # 4.1 建议定价
    df['sku调价目标毛利率'] = df['after_profit'] + df['platform_zero'] + df['platform_must_percent']

    # 限时清仓降价逻辑
    # df = flash_sku_logic(df)
    # c1 = (df['sku调价目标毛利率'] > df['销毁价毛利率']) & (df['overage_level'] > 60) & (df['是否限时清仓sku']==1)
    # df['sku调价目标毛利率'] = np.where(c1, df['销毁价毛利率'], df['sku调价目标毛利率'])

    # 不放缓清仓sku兜底
    p = -0.1
    # p = df['sku调价目标毛利率']
    c1 = (df['是否限时清仓sku']!=1) & (df['sku调价目标毛利率'] < (p+df['platform_zero']))
    df['sku调价目标毛利率'] = np.where(c1, p+df['platform_zero'], df['sku调价目标毛利率'])
    df['sku建议兜底毛利率'] = df['sku调价目标毛利率']
    df['sku建议兜底价'] = (df['new_price'] + df['totalCost_origin']) / (1 - df['ppve'] - df['sku建议兜底毛利率'])
    # 4.2 正常品按目标净利率定价
    df['sku建议兜底价'] = np.where(df['销售状态']=='正常', df['正常品定价_人民币'], df['sku建议兜底价'])
    df['sku建议兜底毛利率'] = np.where(df['销售状态'] == '正常', df['platform_zero'] + df['platform_must_percent'], df['sku建议兜底毛利率'])

    # 4.5 供应商货盘sku补充逻辑
    if org in ('TT','YB'):
        df = supplier_sku_price(df, org=org)

    # 4.3 销毁价兜底
    df['sku建议兜底价'] = np.where(df['sku建议兜底价'] < df['销毁价'], df['销毁价'], df['sku建议兜底价'])
    df['sku建议兜底价'] = df['sku建议兜底价'].replace('', np.nan).fillna(0).astype(float)
    df['sku建议兜底毛利率'] = np.where(abs(df['sku建议兜底价']-df['销毁价'])<=0.0001, df['销毁价毛利率'],df['sku建议兜底毛利率'])

    # 4.4 海兔补充按正常品建议兜底
    sku_list = ['RYE006','RYE007','MF007W','24EE005B10001','24EE005B10002','AM004B','24EE005G10001','GZ031','24EE005G10002','MF006W']
    c1 = (df['sku'].isin(sku_list)) & (df['warehouse']=='美国仓')
    df['sku建议兜底毛利率'] = np.where(c1, df['platform_zero'] + df['platform_must_percent'], df['sku建议兜底毛利率'])
    df['sku建议兜底价'] = np.where(c1, df['正常品定价_人民币'], df['sku建议兜底价'])


    df['sku建议兜底价_本币'] = df['sku建议兜底价']/df['rate']
    df['是否继续清仓sku'] = df['是否限时清仓sku']
    col = ['lowest_price','refound_fee','sku调价目标毛利率','after_profit','age_360_plus','是否限时清仓sku']
    df.drop(col, axis=1, inplace=True)

    # 5. 存表
    df[['available_stock','on_way_stock']] = df[['available_stock','on_way_stock']].fillna(0).astype(int)
    dic = {'new_price':'成本','type':'开发来源','gross':'毛重','product_package_size':'包装尺寸','best_warehouse_id':'最优子仓ID',
           'best_warehouse_name':'最优子仓','available_stock':'可用库存','on_way_stock':'在途库存',
           'overage_level':'超库龄等级', 'age_90_plus':'超90库龄库存', 'age_180_plus':'超180库龄库存','ship_name':'尾程渠道','totalCost_origin':'总运费',
           'firstCarrierCost':'头程','ship_fee':'尾程','country':'目的国','ppve':'佣金+库损汇损+vat',
           'platform_zero':'差值','platform_must_percent':'平台要求净利率'}
    df = df.rename(columns=dic)
    df['销售状态'] = df['销售状态'].fillna('正常')
    df['可用库存'] = np.where(df['可用库存']>5, '5+', df['可用库存'])
    df.drop(['超90库龄库存','超180库龄库存'], axis=1, inplace=True)

    # df.to_excel('F://Desktop//df_sku_price.xlsx', index=0)

    return df

def tts_get_useful_fee(df):
    """ TTS的运费 """
    # 1、包含线上渠道
    sql = """
    SELECT 
        sku, warehouseId best_warehouse_id, country, shipName, totalCost totalCost_origin, firstCarrierCost,
        (toFloat64(totalCost) - toFloat64(firstCarrierCost) - toFloat64(dutyCost)) ship_fee
    FROM yibai_oversea.oversea_transport_fee_daily a
    where date_id = (
       SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily
       WHERE date_id > toYYYYMMDD(subtractDays(today(), 15)) 
       )
        AND platform in ('TTS')
        AND (NOT (warehouseName like '%英国%' and country not in ('UK','GB')))
        AND (NOT (warehouseName not like '%英国%' and country in ('UK','GB')))
        AND shipName like '%线上%'
        AND (shipName like '%USPS%' or shipName like '%usps%')
        order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
    LIMIT 1 by sku, platform, country, warehouseId
    
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_1 = conn_ck.ck_select_to_df(sql)
    print(df_1.info())

    # 2、不包含线上渠道
    sql = """
    SELECT 
        sku, warehouseId best_warehouse_id, country, shipName shipName_2, totalCost totalCost_origin_2, 
        firstCarrierCost firstCarrierCost_2,
        (toFloat64(totalCost) - toFloat64(firstCarrierCost) - toFloat64(dutyCost)) ship_fee_2
    FROM yibai_oversea.oversea_transport_fee_daily a
    where date_id = (
       SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily
       WHERE date_id > toYYYYMMDD(subtractDays(today(), 15)) 
       )
        AND platform in ('TTS')
        AND (NOT (warehouseName like '%英国%' and country not in ('UK','GB')))
        AND (NOT (warehouseName not like '%英国%' and country in ('UK','GB')))
        AND (NOT(toFloat64(shippingCost) < 1))
        AND shipName not like '%USPS%'
        AND shipName not like '%线上%'
        order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
    LIMIT 1 by sku, platform, country, warehouseId
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_2 = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_2, how='left', on=['sku', 'best_warehouse_id'])
    print(df.info())
    df = pd.merge(df, df_1, how='left', on=['sku','best_warehouse_id', 'country'])
    df['country'] = df['country'].replace('GB','UK')
    return df


# TTS 定价数据  临时
def tts_get_sku_price():
    """
    TTS定价数据：
    1、需提供线上渠道和线下渠道两种定价
    2、
    """
    # 1、 取sku信息
    df = get_sku_info()
    col = ['sku', 'title','type','new_price','gross', 'product_package_size', 'best_warehouse_id',
           'best_warehouse_name', 'warehouse', 'available_stock','是否限时清仓sku',
           'on_way_stock', '销售状态', 'overage_level', 'age_90_plus', 'age_180_plus','age_360_plus','after_profit']
    df = df[col]
    df = df[df['warehouse'].isin(['美国仓','德国仓','英国仓'])]  # TTS暂时只有美国、德国运费
    print('TTS：sku信息获取完成')
    # 2、 取运费
    df = tts_get_useful_fee(df)
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on='country')
    # df = df[~df['country'].isna()]
    print('TTS：运费获取完成')
    print(df.info())
    # 3、取费率项
    df['佣金'] = 0.06
    df['库损汇损'] = 0.04
    df['税率'] = 0
    df['税率'] = np.where(df['country'] == 'UK', 0.1667, 0)
    df['refound_fee'] = 0.06
    df['platform_zero'] = 0.18
    df['platform_must_percent'] = 0.1
    col = ['new_price']
    df[col] = df[col].astype(float)
    # 4. 计算定价
    # col = [20, 10, 2, 0, -10, -20, -30, -40]
    # for i in col:
    #     df.loc[df['销售状态']!='正常',f'毛利率{i}%_人民币'] = (df['new_price'] + df['totalCost_origin']) / (1 - df['ppve'] - i / 100)
    for i, j, n in zip(['totalCost_origin', 'totalCost_origin_2'], ['ship_fee','ship_fee_2'], ['含线上渠道','不含线上渠道']):
        df[f'正常品定价_{n}'] = (df['new_price'] + df[i]) / (1 - df['佣金']-df['税率']-df['库损汇损'] - df['platform_zero'] - df['platform_must_percent'])
        # 4.1 建议定价
        df['sku调价目标毛利率'] = df['after_profit'] + df['platform_zero'] + df['platform_must_percent']
        df['sku调价目标毛利率'] = np.where(df['sku调价目标毛利率']<= (0.08+0.02), 0.08, df['sku调价目标毛利率'])
        # 调价兜底毛利率：非限时清仓0.1
        # df['sku调价目标毛利率'] = np.where(df['是否限时清仓sku']!=1, -0.3 + df['platform_zero'], df['sku调价目标毛利率'])
        df['sku建议兜底毛利率'] = df['sku调价目标毛利率']
        df['sku建议兜底价'] = (df['new_price'] + df[i]) / (1 - df['佣金']-df['税率']-df['库损汇损'] - df['sku建议兜底毛利率'])
        # 4.2 正常品按目标净利率定价
        df['sku建议兜底价'] = np.where(df['销售状态']=='正常', df[f'正常品定价_{n}'], df['sku建议兜底价'])
        df['sku建议兜底毛利率'] = np.where(df['销售状态'] == '正常', df['platform_zero'] + df['platform_must_percent'], df['sku建议兜底毛利率'])

        # 4.3 销毁价兜底
        df[f'销毁价_{n}'] = 1.2 * df[j] / (
                    1 - df['佣金'] - df['税率']-df['库损汇损'] - df['refound_fee'])
        df[f'销毁价毛利率_{n}'] = 1 - df['佣金'] - df['库损汇损'] - (
                    df['new_price'] + df[i]) / (df[f'销毁价_{n}'])
        df['sku建议兜底价'] = np.where(df['sku建议兜底价'] < df[f'销毁价_{n}'], df[f'销毁价_{n}'], df['sku建议兜底价'])
        df['sku建议兜底价'] = df['sku建议兜底价'].replace('', np.nan).fillna(0).astype(float)
        df[f'sku建议兜底毛利率'] = np.where(abs(df['sku建议兜底价']-df[f'销毁价_{n}'])<=0.0001, df[f'销毁价毛利率_{n}'],df['sku建议兜底毛利率'])

        # 4.4 海兔补充按正常品建议兜底
        sku_list = ['RYE006','RYE007','MF007W','24EE005B10001','24EE005B10002','AM004B','24EE005G10001','GZ031','24EE005G10002','MF006W']
        c1 = (df['sku'].isin(sku_list)) & (df['warehouse']=='美国仓')
        df[f'sku建议兜底毛利率_{n}'] = np.where(c1, df['platform_zero'] + df['platform_must_percent'], df['sku建议兜底毛利率'])
        df[f'sku建议兜底价_{n}'] = np.where(c1, df[f'正常品定价_{n}'], df['sku建议兜底价'])


    col = ['refound_fee','sku调价目标毛利率','after_profit','age_360_plus','sku建议兜底毛利率','sku建议兜底价']
    df.drop(col, axis=1, inplace=True)

    # 5. 存表
    dic = {'new_price':'成本','type':'开发来源','gross':'毛重','product_package_size':'包装尺寸','best_warehouse_id':'最优子仓ID',
           'best_warehouse_name':'最优子仓','available_stock':'可用库存','on_way_stock':'在途库存',
           'overage_level':'超库龄等级', 'age_90_plus':'超90库龄库存', 'age_180_plus':'超180库龄库存',
           'shipName':'尾程渠道_含线上渠道','totalCost_origin':'总运费_含线上渠道',
           'firstCarrierCost':'头程_含线上渠道','ship_fee':'尾程_含线上渠道','country':'目的国','shipName_2':'尾程渠道_不含线上渠道',
           'totalCost_origin_2': '总运费_不含线上渠道', 'firstCarrierCost_2': '头程_不含线上渠道', 'ship_fee_2': '尾程_不含线上渠道',
           'platform_zero':'差值','platform_must_percent':'平台要求净利率'}
    df = df.rename(columns=dic)

    df['可用库存'] = np.where(df['可用库存']>5, '5+', df['可用库存'])
    df.drop(['在途库存','超90库龄库存','超180库龄库存'], axis=1, inplace=True)

    df.to_excel('F://Desktop//df_tk.xlsx', index=0)

# 销毁价利润率
def get_destroy_profit(df_base):
    """
    计算amazon、ebay销毁价利润率。
    将低值回写到dwm_sku_temp_info。
    用于判断【加快动销补充逻辑】：销毁价过低时，是否需要增大降价幅度
    """
    sql = """

        SELECT sku, warehouse, new_price, total_cost, lowest_price, country, platform , target_profit_rate, is_distory, date_id
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform in ('AMAZON', 'EB')
    """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'美国仓':'US','德国仓':'DE','英国仓':'UK','法国仓':'FR','澳洲仓':'AU','加拿大仓':'CA','墨西哥仓':'MX',
           '西班牙仓':'ES','意大利仓':'IT','乌拉圭仓':'BR','日本仓':'JP','泰国仓':'TH','菲律宾仓':'PH','马来西亚仓':'MY','越南仓':'VN'}
    df['warehouse_country'] = df['warehouse'].replace(dic)
    # 利润率、是否销毁价，可以只取一个国家（仓库所在国）
    df = df[(df['warehouse_country'] == df['country']) | (df['warehouse_country'].isna())]

    # 取配置表
    df_platform_fee = yb_get_platform_fee()
    df = pd.merge(df, df_platform_fee[['platform','country','ppve','platform_zero', 'platform_must_percent']],
                  how='left', on=['platform','country'])
    df['lowest_price_profit'] = 1 - df['ppve'] - df['platform_zero'] - df['platform_must_percent'] - \
                                (df['new_price']+df['total_cost'])/df['lowest_price']

    df = df.sort_values(by='lowest_price_profit', ascending=True).drop_duplicates(subset=['sku','warehouse'])

    df = df[['sku','warehouse','target_profit_rate','lowest_price_profit', 'is_distory']]

    # df.to_excel('F://Desktop//df_destroy.xlsx', index=0)

    df_base = pd.merge(df_base, df[['sku','warehouse','lowest_price_profit']], how='left', on=['sku', 'warehouse'])

    return df_base

def get_sku_lowest_price():
    """  sku维度销毁价情况 """
    sql = """
        SELECT sku, warehouse, 1 is_flash
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)
    print(df_flash_sku.info())

    # 当前库存信息
    sql = """
        SELECT sku, warehouse, type, available_stock, available_stock_money, overage_level, lowest_profit
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-09-01')
        and available_stock > 0
        and best_warehouse_name not like '%%精品%%'
    """
    df_dwm = conn.read_sql(sql)
    sql = """
        SELECT sku, warehouse, target_profit_rate, is_destroy, is_destroy_final
        FROM over_sea.dwm_oversea_profit
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit WHERE date_id > '2025-09-01')
    """
    df_profit = conn.read_sql(sql)

    df = pd.merge(df_dwm, df_flash_sku, how='left', on=['sku','warehouse'])
    df = pd.merge(df, df_profit, how='left', on=['sku', 'warehouse'])

    # 是否抵达销毁价
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 销毁价利润率
    df = get_destroy_profit(df)

    df.to_excel('F://Desktop//df_sku_profit.xlsx', index=0)

def site_table():
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = """
    SELECT distinct site, site1, area as area0
    FROM domestic_warehouse_clear.site_table
    """
    # df_site = pd.DataFrame(con.execute(sql), columns=['site', 'site1', 'area'])
    df_site = conn_ck.ck_select_to_df(sql)
    df_site['area'] = np.where(df_site['area0'].str.contains('泛欧'), '欧洲', df_site['site1'])
    df_site['area'] = df_site['area'].replace('澳大利亚', '澳洲')
    print(df_site['area'].unique())
    return df_site

def get_flash_sku():
    """  限时清仓sku """
    sql = """
        SELECT *
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)
    # print(df_flash_sku.info())

    return df_flash_sku

def tt_check_ali_listing_price():
    """ 检查ali链接到达销毁价情况 """
    sql = """                                                                                                        
        SELECT account_id, short_name, product_id, pop_choice_status, product_status_type, sku, sku_price, 
        current_effective_supply_price,        
        name_en, country, currency_code, warehouse_country             
        FROM yibai_oversea.ads_tt_oversea_ali_listing                                                                 
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.ads_tt_oversea_ali_listing)                                                       
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'US':'美国仓', 'UK':'英国仓', 'DE':'德国仓', 'AU':'澳洲仓', 'FR':'法国仓', 'MX':'墨西哥仓',
           'RU':'俄罗斯仓', 'JP':'日本仓','ES':'西班牙仓', 'IT':'意大利仓'}
    df['warehouse'] = df['warehouse_country'].replace(dic)
    print(df.info())

    df_sku = get_sku_price(platform='ALI')
    df_sku = df_sku.rename(columns={'目的国':'country'})
    c1 = (df_sku['warehouse']=='美国仓') & (df_sku['country'].isin(['MX','CA']))
    df_sku = df_sku[~c1]

    col = ['sku','warehouse', '可用库存', '销售状态', '超库龄等级', '尾程渠道', '成本', '总运费', '头程', '尾程', 'rate', 'country',
           '佣金+库损汇损+vat','退款率','差值', '销毁价', 'sku建议兜底毛利率','sku建议兜底价']
    df = pd.merge(df, df_sku[col], how='inner', on=['sku','country','warehouse'])

    # 是否销毁价sku
    df_destroy = destroy_sku()

    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 在线价利润率情况
    us_rate = df.loc[df['country'] == 'US', 'rate'].iloc[0]
    df['在线价人民币'] = np.where(df['current_effective_supply_price']>0, df['current_effective_supply_price'], df['sku_price'])
    df['在线价人民币'] = np.where(df['currency_code']== 'USD', df['在线价人民币']*us_rate, df['在线价人民币'])

    df['在线价毛利润率'] = 1 - 0.05 - (df['成本'] + df['总运费'])/(df['在线价人民币'])
    df['利润率分段'] = pd.cut(df['在线价毛利润率'], [-np.inf, -0.5, -0.2, -0.1, 0, 0.05, 0.1, 0.2, 0.3, 0.5, np.inf],
                                 labels=['A:-50%以下', 'B:-50%~-20%', 'C:-20%~-10%', 'D:-10%~0%', 'E:0~5%', 'F:5%~10%',
                                         'G:10%~20%', 'H:20%~30%', 'I:30%~50%', 'J:50%以上'])

    df['净利润率'] = df['在线价毛利润率'] - 0.12
    df['净利润率分段'] = pd.cut(df['净利润率'],
                                   [-np.inf, -0.5, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2, np.inf],
                                   labels=['A:-50%以下', 'B:-50%~-20%', 'C:-20%~-10%', 'D:-10%~-5%', 'E:-5%~0%',
                                           'F:0%~5%', 'G:5%~10%', 'H:10%~15%', 'I:15%~20%', 'J:20%以上'])

    df.to_excel('F://Desktop//df_ali_listing_price_tt.xlsx', index=0)

def destroy_sku():
    # sql = "SELECT * FROM over_sea.dwm_oversea_profit where date_id >DATE_SUB( CURDATE(), INTERVAL 35 DAY ) and is_destroy like '%%1%%'"
    # df = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
    #
    # df.loc[df['is_destroy'].str.contains('0'), 'is_destroy_final'] = 2
    # # 取部分达到销毁价的数据做补充
    # all_date = list(df['date_id'].drop_duplicates())
    # all_date.sort()
    #
    # df1 = df[(df['is_destroy_final'] == 2) & (df['date_id'] == all_date[-1])]
    # df1['type'] = '部分平台达销毁价'
    # df1 = df1[['sku', 'warehouse', 'type']]
    # df = df[df['is_destroy_final'] == 1]
    # df['最近日期'] = df['date_id'].copy()
    # df['最远日期'] = df['date_id'].copy()
    # df2 = df.groupby(['sku', 'warehouse']).agg(
    #     {'最近日期': max, '最远日期': min, 'is_destroy_final': sum}).reset_index()
    #
    # df2['达到销毁价天数'] = pd.to_datetime(all_date[-1]) - pd.to_datetime(df2['最远日期'])
    # df2['达到销毁价天数'] = df2['达到销毁价天数'].apply(lambda x: x.days)
    #
    # # 先用0.5取卡一个比例，1是防止费用波动导致的进出
    # df2 = df2[(df2['is_destroy_final'] / df2['达到销毁价天数'] >= 0.5) & (df2['最近日期'] >= all_date[-2])]
    # df2['type'] = '完全达销毁价'
    # df2 = df2.append(df1, ignore_index=True)
    #
    # sql = """
    # SELECT
    #     a.sku,a.warehouse,a.available_stock `可售库存`,available_stock_money `可售库存金额`,a.estimated_sales_days `可售天数`,
    #     IF(b.sale_status IS NULL ,'正常',b.sale_status) as `销售状态`
    # FROM (
    #     SELECT *
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
    #     and (available_stock > 0)
    # ) a
    # LEFT JOIN (
    #     SELECT *
    #     FROM over_sea.oversea_sale_status
    #     WHERE end_time IS NULL
    # ) b ON a.sku=b.sku AND a.warehouse=b.warehouse
    # """
    # stock = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
    #
    # df2 = df2.merge(stock, on=['sku', 'warehouse'])
    # df2.loc[df2['type'] == '部分平台达销毁价', '是否达销毁价'] = '部分平台已达销毁价'
    # df2.loc[df2['type'] == '完全达销毁价', '是否达销毁价'] = '已达销毁价-天数未超'
    # df2.loc[((df2['可售天数'] > 15) & (df2['达到销毁价天数'] >= 30)), '是否达销毁价'] = '已达销毁价'
    # df2.loc[((df2['可售天数'] > 30) & (df2['达到销毁价天数'] >= 15)), '是否达销毁价'] = '已达销毁价'
    # df2 = df2.drop_duplicates(['sku', 'warehouse'])
    #
    # df2 = df2[df2['销售状态'] != '正常']
    # df2 = df2[['sku', 'warehouse', '可售库存', '可售库存金额', '可售天数', '是否达销毁价']]

    sql = """
        SELECT *
        FROM cj_temp_szz.yibai_oversea_warehouse_destroy_evaluate
        WHERE insert_date = (SELECT max(insert_date) FROM cj_temp_szz.yibai_oversea_warehouse_destroy_evaluate)
    """
    # conn_ck = pd_to_ck(database='cj_temp_szz', data_sys='cj_本地库')
    conn = connect_to_sql(database='cj_temp_szz', data_sys='cj_本地库')
    df2 = conn.read_sql(sql)

    return df2

def get_temu_sku_price():
    """ temu sku 定价数据"""


# 临时sku
def get_sku_temp():
    """  sku维度销毁价情况 """
    sql = """
        SELECT sku, warehouse, 1 is_flash
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)
    print(df_flash_sku.info())

    # 当前库存信息
    sql = """
        SELECT sku, warehouse, best_warehouse_name, type, available_stock, available_stock_money, overage_level, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id in ('2025-12-02', '2025-12-25')
        and available_stock > 0
        and best_warehouse_name not like '%%精品%%'
    """
    df_dwm = conn.read_sql(sql)

    df_dwm = pd.merge(df_dwm, df_flash_sku, how='left', on=['sku', 'warehouse'])

    df_dwm_1 = df_dwm[df_dwm['date_id']=='2025-12-02']
    df_dwm_2 = df_dwm[df_dwm['date_id']=='2025-12-25']
    df_dwm_2 = df_dwm_2[['sku','warehouse','available_stock_money']]
    df_dwm_2 = df_dwm_2.rename(columns={'available_stock_money':'available_stock_money_t'})

    df = pd.merge(df_dwm_1, df_dwm_2, how='outer', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_dwm.xlsx', index=0)

    return None

def check_temu_listing_price(org='YB'):
    """ temu链接价格信息 """
    # 1、取链接
    date_today = time.strftime('%Y-%m-%d')
    # sku_list = get_sku_temp()
    # date_today = '2025-12-01'
    if org == 'YB':
        sql = f"""                                                                                                        
            SELECT account_id, short_name, product_sku_id, site_code, online_status, sku, warehouse, country,            
            best_warehouse_name, available_stock, overage_level, supplier_price,online_profit_rate                    
            FROM yibai_oversea.oversea_temu_listing_all                                                                  
            WHERE date_id = '{date_today}' 
            and available_stock > 0                                                        
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
        dic = {'DE':1, 'ES':2,'FR':3, 'IT':4, 'AU':5}
        df['rank'] = df['country'].map(dic).fillna(6)
        df = df.sort_values(by='rank', ascending=True).drop_duplicates(subset=['account_id','product_sku_id','sku','warehouse'])

        print(df.info())
        # 取账号小组
        df_temu_account = get_temu_account()
        col = ['account_id', 'main_name']
        df = pd.merge(df, df_temu_account[col], how='left', on=['account_id'])

        # 取销售状态
        sql = """
        select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_sale = conn.read_sql(sql)

        df = pd.merge(df, df_sale, how='left', on=['sku','warehouse'])
        df['sale_status'] = df['sale_status'].fillna('正常')
        # df.to_excel('F://Desktop//df_temu_listing_2.xlsx', index=0)
    elif org == 'TT':
        sql = """
            SELECT account_id, short_name, product_id product_sku_id, listing_status online_status, sku, warehouse, country,            
            best_warehouse_name, available_stock, overage_level, online_price supplier_price, sales_status,
            online_profit online_profit_rate
            FROM yibai_oversea.tt_oversea_listing_profit
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_oversea_listing_profit)
            and platform = 'temu'
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)

    # 2、取销毁价信息。运费、费率、价格
    if org == 'YB':
        df_price = pd.read_excel('F://Desktop//日常任务//TEMU定价需求//TEMU海外仓清仓定价0413.xlsx', dtype={'sku':'str'})
        col = ['sku', 'warehouse', '成本','总运费','尾程','目的国',
               '销毁价(扣除运费补贴)','销毁价毛利率','sku建议兜底毛利率','sku建议兜底价','是否继续清仓sku']
        df_price = df_price[col]
        df_price = df_price.rename(columns={'目的国':'country'})
    elif org == 'TT':
        df_price = pd.read_excel('F://Desktop//日常任务//TEMU定价需求//TEMU海外仓清仓定价0317_TT.xlsx', dtype={'sku':'str'})
        col = ['sku', 'warehouse', '成本','总运费','尾程','目的国',
               '销毁价(扣除运费补贴)','销毁价毛利率','sku建议兜底毛利率','sku建议兜底价','是否继续清仓sku']
        df_price = df_price[col]
        df_price = df_price.rename(columns={'目的国':'country'})
    df = pd.merge(df, df_price, how='inner', on=['sku','country','warehouse'])

    # 3、取是否销毁价sku
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # # 是否限时清仓sku
    # df_flash = get_flash_sku()
    # df_flash['12月不暂缓清仓sku'] = 1
    # df = pd.merge(df, df_flash[['sku', 'warehouse', '12月不暂缓清仓sku']], how='left', on=['sku','warehouse'])
    # df = df.drop_duplicates()
    # df_flash = pd.read_excel('F://Ding_workspace//YB泛品海外仓12月清仓SKU明细.xlsx', dtype={'sku': str})
    # df_flash = df_flash[['sku', 'warehouse', '是否可放缓']]
    # df_flash = df_flash[df_flash['是否可放缓']=='不可放缓']
    # df_flash = df_flash.drop_duplicates(subset=['sku','warehouse'])
    # df_flash['是否不暂缓清仓sku'] = df_flash['是否可放缓']
    # df = pd.merge(df, df_flash[['sku', 'warehouse', '是否不暂缓清仓sku']], how='left', on=['sku','warehouse'])
    # df = df.drop_duplicates()

    col = ['销毁价(扣除运费补贴)', 'sku建议兜底价', 'supplier_price']
    df[col] = df[col].fillna(0).astype(float)
    df['在线价/sku建议兜底价'] = df['supplier_price']/df['sku建议兜底价']

    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    # 分段（right=False确保左闭右开）
    df['建议兜底价对比分段'] = pd.cut(df['在线价/sku建议兜底价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 销毁价对比
    df['在线价/销毁价'] = df['supplier_price']/df['销毁价(扣除运费补贴)']
    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    # 分段（right=False确保左闭右开）
    df['销毁价对比分段'] = pd.cut(df['在线价/销毁价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    table_name = 'df_temu_price'
    if org=='TT':
        table_name = 'df_temu_price_tt'
    df.to_excel(f'F://Desktop//{table_name}.xlsx', index=0)

def check_ali_listing_price(org='YB'):
    """ 检查ali链接到达销毁价情况 """
    # sku_list = get_sku_temp()
    # date_today = '2025-12-02'
    if org == 'YB':
        sql = f"""                                                                                                        
            SELECT account_id, short_name, product_id, pop_choice_status, product_status_type, sku, sku_price, 
            current_effective_supply_price,        
            name_en, country, currency_code, warehouse_country             
            FROM yibai_oversea.yibai_ads_oversea_ali_listing                                                                 
            WHERE 
            date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_ali_listing)  
            and product_status_type = 1
            and pop_choice_status = 5  -- 不确定                                     
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    elif org == 'TT':
        sql = f"""                                                                                                        
            SELECT account_id, short_name, product_id, pop_choice_status, product_status_type, sku, sku_price, 
            current_effective_supply_price,        
            name_en, country, currency_code, warehouse_country             
            FROM tt_oversea.tt_ads_oversea_ali_listing                                                                 
            WHERE 
            date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_ali_listing)  
            and product_status_type = 1
            and pop_choice_status = 5  -- 不确定                                     
        """
        conn_ck = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'US':'美国仓', 'UK':'英国仓', 'DE':'德国仓', 'AU':'澳洲仓', 'FR':'法国仓', 'MX':'墨西哥仓',
           'RU':'俄罗斯仓', 'JP':'日本仓','ES':'西班牙仓', 'IT':'意大利仓'}
    df['warehouse'] = df['warehouse_country'].replace(dic)
    print(df.info())

    # df_sku = yb_get_sku_price(platform='ALI')
    df_sku = pd.read_excel('F://Desktop//日常任务//海外仓各平台建议定价//YB海外仓ALI建议定价20260413_sku维度.xlsx', dtype={'sku':str})
    if org == 'haitu':
        df_sku = pd.read_excel('F://Desktop//日常任务//海外仓各平台建议定价//YB海外仓ALI建议定价0214_海兔耳机.xlsx', dtype={'sku':str})
    if org == 'TT':
        df_sku = pd.read_excel('F://Desktop//日常任务//海外仓各平台建议定价//TT-海外仓ALI&ALLEGRO建议定价20260214.xlsx',
                               dtype={'sku': str})
    df_sku = df_sku.rename(columns={'目的国':'country'})
    c1 = (df_sku['warehouse']=='美国仓') & (df_sku['country'].isin(['MX','CA']))
    df_sku = df_sku[~c1]

    # 国家为GLO时，使用运费最贵的国家数据
    df_sku_glo = df_sku[df_sku['warehouse'].isin(['德国仓','英国仓','法国仓'])]
    df_sku_glo = df_sku_glo.sort_values(by=['总运费'], ascending=False).drop_duplicates(subset=['sku','warehouse'])
    df_sku_glo['country'] = 'GLO'
    df_sku = pd.concat([df_sku, df_sku_glo])

    col = ['sku','warehouse', '可用库存', '销售状态', '超库龄等级', '尾程渠道', '成本', '总运费', '头程', '尾程', 'rate', 'country',
           '佣金+库损汇损+vat','差值', '销毁价', '正常品定价_人民币', 'sku建议兜底毛利率','sku建议兜底价']
    df = pd.merge(df, df_sku[col], how='inner', on=['sku','country','warehouse'])

    # # 是否销毁价sku
    # df_destroy = destroy_sku()
    # df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])
    # 是否限时清仓sku
    # df_flash = get_flash_sku()
    # df_flash = df_flash[['sku','warehouse','source']]
    # df_flash['是否不暂缓清仓sku'] = 1
    df_flash = pd.read_excel('F://Ding_workspace//YB泛品海外仓202604清仓方案 0409.xlsx', dtype={'sku': str})
    df_flash = df_flash[['sku', 'warehouse', '清仓类别']]
    df_flash['是否限时清仓sku'] = 1
    df_flash.columns = ['sku', 'warehouse', '限时清仓分类','是否限时清仓sku']
    df_flash = df_flash.drop_duplicates(subset=['sku','warehouse'])
    # df_flash['是否不暂缓清仓sku'] = df_flash['是否可放缓']
    df = pd.merge(df, df_flash[['sku', 'warehouse', '限时清仓分类', '是否限时清仓sku']], how='left', on=['sku','warehouse'])
    df = df.drop_duplicates()
    # 在线价利润率情况
    us_rate = df.loc[df['country'] == 'US', 'rate'].iloc[0]
    df['在线价人民币'] = np.where(df['current_effective_supply_price']>0, df['current_effective_supply_price'], df['sku_price'])
    df['在线价人民币'] = np.where(df['currency_code']== 'USD', df['在线价人民币']*us_rate, df['在线价人民币'])

    df['在线价毛利润率'] = 1 - 0.05 - (df['成本'] + df['总运费'])/(df['在线价人民币'])
    df['利润率分段'] = pd.cut(df['在线价毛利润率'], [-np.inf, -0.5, -0.2, -0.1, 0, 0.05, 0.1, 0.2, 0.3, 0.5, np.inf],
                                 labels=['A:-50%以下', 'B:-50%~-20%', 'C:-20%~-10%', 'D:-10%~0%', 'E:0~5%', 'F:5%~10%',
                                         'G:10%~20%', 'H:20%~30%', 'I:30%~50%', 'J:50%以上'])
    df['净利润率'] = df['在线价毛利润率'] - 0.18
    df['净利润率分段'] = pd.cut(df['净利润率'],
                                   [-np.inf, -0.5, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2, np.inf],
                                   labels=['A:-50%以下', 'B:-50%~-20%', 'C:-20%~-10%', 'D:-10%~-5%', 'E:-5%~0%',
                                           'F:0%~5%', 'G:5%~10%', 'H:10%~15%', 'I:15%~20%', 'J:20%以上'])

    df['在线价/sku建议兜底价'] = df['在线价人民币']/df['sku建议兜底价']

    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    # 分段（right=False确保左闭右开）
    df['分段'] = pd.cut(df['在线价/sku建议兜底价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 销毁价对比
    df['在线价/销毁价'] = df['在线价人民币']/df['销毁价']
    df['销毁价对比分段'] = pd.cut(df['在线价/销毁价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 剔除部分账号
    account_list = ['sysn06-HT','ssl01-HT','sjl01-HT','ssf01-HT','slx01-HT','ssf05-HT',
                    'sjsj02-HT','Mxiz01-HT','Smxi01-HT','Smxi02-HT','sjyl02-HT']
    df = df[~df['short_name'].isin([account_list])]

    table_name = 'df_ali_price'
    if org=='TT':
        table_name = 'df_ali_price_tt'
    df.to_excel(f'F://Desktop//{table_name}.xlsx', index=0)

def check_ebay_listing_price(org='YB'):
    """ ebay链接价格信息 """
    # 1、取链接
    date_today = time.strftime('%Y-%m-%d')
    # sku_list = get_sku_temp()
    # date_today = '2025-12-01'
    if org == 'YB':
        sql = f"""                                                                                                        
            SELECT a.*, b.price price                   
            FROM (
                SELECT account_id, item_id, sku, warehouse, country, day_sales, sales_status,  is_white_account,         
                best_warehouse_name, available_stock, online_price,ppve, platform_zero, refound_fee, rate, 
                new_price, total_cost, target_profit_rate
                FROM yibai_oversea.oversea_ebay_listing_all                                                                  
                WHERE date_id = '{date_today}' and available_stock > 0  
            ) a
            LEFT JOIN (
                SELECT item_id, sku, price
                FROM yibai_oversea.oversea_ebay_listing_all                                                                  
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_ebay_listing_all WHERE date_id < '{date_today}')
            ) b
            ON a.item_id = b.item_id and a.sku = b.sku
                                                
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
    if org == 'TT':
        sql = f"""                                                                                                        
            SELECT account_id, item_id, sku, warehouse, country, day_sales, sales_status,           
            best_warehouse_name, available_stock, online_price,ppve, platform_zero,  rate, 
            new_price, total_cost, target_profit_rate
            FROM over_sea.tt_oversea_ebay_listing_all                                                                  
            WHERE date_id = '{date_today}' and available_stock > 0  
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df1 = conn.read_sql(sql)
        sql = f"""                                                                                                        
            SELECT item_id, sku, price
            FROM over_sea.tt_oversea_ebay_listing_all                                                                   
            WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_ebay_listing_all WHERE date_id < '{date_today}')
        """
        df2 = conn.read_sql(sql)
        df = pd.merge(df1, df2, how='left', on=['item_id','sku'])

    print(df.info())
    # df.to_excel('F://Desktop//df_temu_listing_2.xlsx', index=0)
    # 2. 取销毁价信息
    sql = f"""
        SELECT sku, warehouse, country, lowest_price, overage_level
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = '{date_today}' 
        -- and available_stock > 0  
        and platform = 'EB'
    """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_lowest = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_lowest, how='left', on=['sku','warehouse','country'])

    # 3、取是否销毁价sku
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 是否限时清仓sku
    df_flash = get_flash_sku()
    df_flash['是否不暂缓清仓sku'] = 1
    df = pd.merge(df, df_flash[['sku', 'warehouse', '是否不暂缓清仓sku']], how='left', on=['sku','warehouse'])
    df = df.drop_duplicates()

    col = ['online_price', 'price', 'lowest_price', 'rate']
    df[col] = df[col].fillna(0).astype(float)
    df['在线价/sku建议兜底价'] = df['online_price']/df['price']

    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    df['建议价对比分段'] = pd.cut(df['在线价/sku建议兜底价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 销毁价对比分段
    df['在线价/销毁价'] = df['online_price']*df['rate']/df['lowest_price']
    df['销毁价对比分段'] = pd.cut(df['在线价/销毁价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    table_name = 'df_ebay_price'
    if org=='TT':
        table_name = 'df_ebay_price_tt'

    df.to_excel(f'F://Desktop//{table_name}.xlsx', index=0)


def check_amazon_listing_price(org='AMAZON'):
    """ amazon链接价格信息 """
    # 1、取链接
    date_today = time.strftime('%Y-%m-%d')
    # sku_list = get_sku_temp()
    # date_today = '2025-12-01'
    if org == 'YB':
        sql = f"""                                                                                                        
            SELECT a.*, b.price price                   
            FROM (
                SELECT account_id, seller_sku, sku, warehouse, country, day_sales, sales_status,           
                best_warehouse_name, available_stock, online_price,ppve, platform_zero, refound_fee, rate, 
                new_price, total_cost, target_profit_rate
                FROM yibai_oversea.oversea_amazon_listing_all                                                                  
                WHERE date_id = '{date_today}' and available_stock > 0  
            ) a
            LEFT JOIN (
                SELECT seller_sku, sku, account_id, price
                FROM yibai_oversea.oversea_amazon_listing_all                                                                  
                WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_amazon_listing_all WHERE date_id < '{date_today}')
            ) b
            ON a.seller_sku = b.seller_sku and a.account_id = b.account_id
    
        """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
    if org == 'TT':
        sql = f"""                                                                                                        
            SELECT account_id, seller_sku, sku, warehouse, country, day_sales, sales_status,           
            best_warehouse_name, available_stock, online_price,ppve, platform_zero,  rate, 
            new_price, total_cost, target_profit_rate
            FROM over_sea.tt_oversea_amazon_listing_all                                                                  
            WHERE date_id = '{date_today}' and available_stock > 0  
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df1 = conn.read_sql(sql)
        sql = f"""                                                                                                        
            SELECT seller_sku, sku, account_id, price
            FROM over_sea.tt_oversea_amazon_listing_all                                                                   
            WHERE date_id = (SELECT max(date_id) FROM over_sea.tt_oversea_amazon_listing_all  WHERE date_id < '{date_today}')
        """
        df2 = conn.read_sql(sql)
        df = pd.merge(df1, df2, how='left', on=['seller_sku', 'sku', 'account_id'])
    print(df.info())

    # df.to_excel('F://Desktop//df_temu_listing_2.xlsx', index=0)
    # 2. 取销毁价信息
    sql = f"""
        SELECT sku, warehouse, country, lowest_price, overage_level
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = '{date_today}' 
        -- and available_stock > 0  
        and platform = 'AMAZON'
    """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_lowest = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_lowest, how='left', on=['sku','warehouse','country'])


    # 3、取是否销毁价sku
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 是否限时清仓sku
    df_flash = get_flash_sku()
    df_flash['是否不暂缓清仓sku'] = 1
    df = pd.merge(df, df_flash[['sku', 'warehouse', '是否不暂缓清仓sku']], how='left', on=['sku','warehouse'])
    df = df.drop_duplicates()

    col = ['online_price', 'price', 'lowest_price', 'rate']
    df[col] = df[col].fillna(0).astype(float)
    df['在线价/sku建议兜底价'] = df['online_price']/df['price']

    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    df['分段'] = pd.cut(df['在线价/sku建议兜底价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 销毁价对比分段
    df['在线价/销毁价'] = df['online_price']*df['rate']/df['lowest_price']
    df['销毁价对比分段'] = pd.cut(df['在线价/销毁价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    table_name = 'df_amazon_price'
    if org=='TT':
        table_name = 'df_amazon_price_tt'
    df.to_excel(f'F://Desktop//{table_name}.xlsx', index=0)

def check_lazada_listing_price():
    """ amazon链接价格信息 """
    # 1、取链接
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""                                                                                                        
        SELECT account_id, seller_sku, item_id, sku, warehouse, country, day_sales,           
        best_warehouse_name, available_stock, online_price,ppve, platform_zero, refound_fee, rate, 
        new_price, total_cost, target_profit_rate, price,lowest_price                     
        FROM yibai_oversea.oversea_lazada_listing_all                                                                  
        WHERE date_id = '{date_today}' and available_stock > 0                                                         
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    # 3、取是否销毁价sku
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 是否限时清仓sku
    df_flash = get_flash_sku()
    df_flash['是否不暂缓清仓sku'] = 1
    df = pd.merge(df, df_flash[['sku', 'warehouse', '是否不暂缓清仓sku']], how='left', on=['sku','warehouse'])
    df = df.drop_duplicates()

    col = ['online_price', 'price', 'lowest_price', 'rate']
    df[col] = df[col].fillna(0).astype(float)
    df['在线价/sku建议兜底价'] = df['online_price']/df['price']

    bins = [float('-inf'), 0.5, 0.8, 1, 1.2, 1.5, 2, 3, 5, float('inf')]
    labels = ['A:<0.5','B:[0.5, 0.8)','C:[0.8, 1)','D:[1, 1.2)','E:[1.2, 1.5)','F:[1.5, 2)',
              'G:[2, 3)','H:[3, 5)','I:>=5']
    df['分段'] = pd.cut(df['在线价/sku建议兜底价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # 销毁价对比分段
    df['在线价/销毁价'] = df['online_price']*df['rate']/df['lowest_price']
    df['销毁价对比分段'] = pd.cut(df['在线价/销毁价'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    df.to_excel('F://Desktop//df_lazada_price.xlsx', index=0)


def get_sku_order():
    """

    """
    sql = """
        SELECT *
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)

    date_start = '2025-09-01'
    sql = f"""
    SELECT
        order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
        warehouse_name, warehouse, quantity, release_money, sales_status
    FROM yibai_oversea.dashbord_new_data1
    WHERE 
        paytime >= '{date_start}'
        and `total_price` > 0 
        and `sales_status` not in ('nan','总计')
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    col = ['total_price','release_money','true_profit_new1']
    df[col] = df[col].astype(float)

    df = pd.merge(df, df_flash_sku[['sku','warehouse']], how='inner', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_flash_sku_order.xlsx', index=0)



def oversea_sku_price(org):
    program_name = '海外仓sku各平台建议定价'
    # make_path()

    # col = ['ALLEGRO', 'LAZADA', 'SHOPEE','ALI']
    if org in ['YB','haitu']:
        col = ['ALI']
    elif org == 'TT':
        col = ['ALLEGRO', 'ALI']
    df = pd.DataFrame()
    for i in col:
        df_temp = get_sku_price(i, org)
        df = pd.concat([df_temp, df])
        print(f"{i}平台建议定价数据计算完成.")
    save_df(df, f'{org}-海外仓{i}建议定价', program_name)

def haitu_destroy():
    """ 海兔sku抵达销毁价情况 """
    sql = """
        SELECT sku, warehouse, type,  best_warehouse_name, available_stock, available_stock_money
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-10-15' and available_stock > 0 and type in ('海兔转泛品', '易通兔')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df_destroy = destroy_sku()

    df = pd.merge(df, df_destroy, how='left', on=['sku', 'warehouse'])

    sql = "SELECT * FROM over_sea.dwm_oversea_profit where date_id = '2025-09-26'"
    df_temp = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')

    df = pd.merge(df, df_temp, how='left', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_haitu.xlsx', index=0)

def bg3_listing_price():
    """ bg3 sku调价情况 """
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
         WHERE sku in {sku_list} and date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_amazon_listing_all)
     """
    df_bg3_listing = conn_ck.ck_select_to_df(sql)

    # 销售状态
    sql = """
         SELECT *
         FROM over_sea.dwm_oversea_profit 
         WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit )
         and warehouse = '美国仓'
     """
    df_destroy = conn.read_sql(sql)

    sql = f"""
         SELECT sku, warehouse, best_warehouse_name, available_stock, new_price
         FROM over_sea.dwm_sku_temp_info
         WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
         and sku in {sku_list}
         and warehouse = '美国仓'
     """
    df_sku = conn.read_sql(sql)

    df_sku = pd.merge(df_sku, df_destroy, how='left', on=['sku','warehouse'])
    df_bg3_listing = pd.merge(df_bg3_listing, df_sku, how='inner', on=['sku','warehouse'])

    df_sku.to_excel('F://Desktop//df_sku_bgs.xlsx', index=0)
    df_bg3_listing.to_excel('F://Desktop//df_bg3_listing.xlsx', index=0)

def sku_destroy_info():
    """ 海外仓sku维度达到销毁价明细 """
    sql = """
        SELECT sku, title, type, warehouse, new_price, best_warehouse_name, available_stock, available_stock_money, overage_level,
        `30days_sales`, `90days_sales`, age_90_plus * new_price age_90_stock_money, age_180_plus * new_price age_180_stock_money,
        age_360_plus * new_price age_360_stock_money, charge_total_price_rmb
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and available_stock > 0
        
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # 取是否不暂缓清仓sku
    df_flash = get_flash_sku()
    df_flash['is_flash_sku'] = 1
    df = pd.merge(df, df_flash[['sku', 'warehouse', 'is_flash_sku']], how='left', on=['sku','warehouse'])

    # 取销毁价信息
    df_destroy = destroy_sku()
    df = pd.merge(df, df_destroy[['sku', 'warehouse', '是否达销毁价']], how='left', on=['sku','warehouse'])

    # 取调价销毁价信息
    sql = """
        SELECT sku, warehouse, target_profit_rate, is_destroy, is_destroy_final
        FROM over_sea.dwm_oversea_profit
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_oversea_profit)
    """
    df_dwm = conn.read_sql(sql)
    df = pd.merge(df, df_dwm, how='left', on=['sku', 'warehouse'])

    df.to_excel('F://Desktop//df_destroy_v3.xlsx', index=0)


def sku_profit_cost():
    """ 海外仓sku各库龄段兜底净利率对应亏损值，与成本折扣测算 """
    # 1、取sku库龄及兜底
    sql = """ 
        SELECT sku, warehouse, type, best_warehouse_name, available_stock, available_stock_money, new_price, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and available_stock > 0 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 2、取不同平台的兜底净利率的定价及净利润
    sql = """
        SELECT distinct sku, warehouse, total_cost, sales_status, platform,
        country, ppve, platform_zero, platform_must_percent
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform in ('AMAZON', 'EB') and not (warehouse = '德国仓' and country != 'DE')
        and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_dtl, how='inner', on=['sku', 'warehouse'])
    df = df.drop_duplicates(subset=['sku', 'warehouse'], keep='first')
    # 3、测算
    dic = {'美国仓': 'US', '德国仓': 'DE', '英国仓': 'UK', '法国仓': 'FR', '澳洲仓': 'AU', '加拿大仓': 'CA',
           '墨西哥仓': 'MX','西班牙仓': 'ES', '意大利仓': 'IT', '乌拉圭仓': 'BR', '日本仓': 'JP',
           '阿联酋仓': 'AE', '巴西仓': 'BR', '俄罗斯仓': 'RU',
           '菲律宾仓': 'PH', '马来西亚仓': 'MY', '泰国仓': 'TH', '越南仓': 'VN'}
    df['warehouse_country'] = df['warehouse'].replace(dic)
    df = df[df['warehouse_country'] == df['country']]
    df['new_price'] = df['new_price'].astype(float)
    df = df[df['new_price']>0]


    df.to_excel('F://Desktop//df_sku_cost.xlsx', index=0)



def xyd_to_destroy_price():
    """ 海外仓sku维度达到销毁价明细 """
    # 取链接
    # sql = """
    #     SELECT
    #         account_name, seller_sku, account_id, price, item_id,
    #         target_profit_rate, new_price, sales_status,country,
    #         sku, warehouse, best_warehouse_name, available_stock, rate
    #     FROM yibai_oversea.oversea_lazada_listing_all
    #     WHERE date_id = '2025-12-18' and available_stock > 0 and best_warehouse_name in ('XYD泰国海外仓')
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_th = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT 
           account_name, item_id, account_id, is_mulit, parent_sku, price, target_profit_rate, sales_status,country,
            sku, warehouse, best_warehouse_name, available_stock, rate
        FROM yibai_oversea.oversea_shopee_listing_all
        WHERE date_id = '2025-12-18' and available_stock > 0 and best_warehouse_name in ('XYD泰国海外仓')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_th = conn_ck.ck_select_to_df(sql)


    # 取调价销毁价信息
    sql = """
        SELECT distinct sku, warehouse, lowest_price
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and available_stock > 0 and best_warehouse_name in ('XYD泰国海外仓')
        and country = 'TH' and platform = 'SHOPEE'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dwm = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df_th, df_dwm, how='left', on=['sku', 'warehouse'])


    df.to_excel('F://Desktop//df_shopee_th.xlsx', index=0)

def dwm_sku_temp():
    """ """

    sql = f"""
        select A.sku sku, A.warehouse warehouse, type, title, linest, new_price, gross, product_package_size, 
        available_stock, available_stock_money, on_way_stock, overage_level, age_60_plus, age_90_plus, age_180_plus, 
        age_180_plus*new_price `超180库存金额`, age_270_plus, age_360_plus,
        IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        from over_sea.dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE date_id = '2025-10-17' and available_stock > 0
        -- and ~((type like '%%转泛品%%' or type like '%%VC%%') and (type not in ('海兔转泛品')))
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    c1 = df['type'].str.contains('转泛品', na=False) & ~df['type'].str.contains('海兔转泛品', na=False)
    df = df[~c1]
    # 是否达到销毁价情况
    df_profit = destroy_sku()
    df = pd.merge(df, df_profit, how='left', on=['sku', 'warehouse'])

    # 调价利润率
    sql = """
    SELECT sku, warehouse, target_profit_rate FROM over_sea.dwm_oversea_profit where date_id = '2025-10-17' 
    """
    df_target = conn.read_sql(sql)
    df = pd.merge(df, df_target, how='left', on=['sku', 'warehouse'])
    # 日销
    sql = """
    SELECT sku, warehouse, sum(`7days_sales`) `7days_sales`, sum(`15days_sales`) `15days_sales`,
    sum(`30days_sales`) `30days_sales`, sum(`60days_sales`) `60days_sales`
    FROM over_sea.dwd_sku_sales
    WHERE date_id = '2025-10-17'
    GROUP BY sku, warehouse
    """
    df_sales = conn.read_sql(sql)
    df = pd.merge(df, df_sales, how='left', on=['sku', 'warehouse'])
    col = ['7days_sales', '15days_sales', '30days_sales', '60days_sales']
    df[col] = df[col].fillna(0).astype(int)

    df['day_sales'] = np.maximum(0.9 * df['7days_sales'] / 7 + 0.1 * df['15days_sales'] / 15,
                                0.4 * df['7days_sales'] / 7 + 0.4 * df['15days_sales'] / 15 + 0.4 * df['30days_sales'] / 30)
    df['day_sales_60'] = df['60days_sales'] / 60
    df['预计可售天数'] = df['available_stock']/df['day_sales']
    df['近60天预计可售天数'] = df['available_stock'] / df['day_sales_60']

    c1 = (df['overage_level']>=90) & (df['available_stock']>=50) & (df['available_stock']<500) & \
         (df['day_sales']<=1) & (df['预计可售天数']>=150)
    c2 = (df['overage_level'] >= 90) & (df['available_stock'] >= 500) & (df['available_stock'] < 1000) & \
         (df['day_sales'] <= 3) & (df['预计可售天数'] >= 150)
    c3 = (df['overage_level'] >= 90) & (df['available_stock'] >= 1000) & \
         (df['day_sales'] <= 5) & (df['预计可售天数'] >= 150)
    c4 = (df['overage_level'] >= 90) & (df['available_stock'] < 50) & (df['近60天预计可售天数'] >= 150)
    df['是否低销sku'] = np.select([c1,c2,c3,c4], [1,2,3,4], 0)

    df = df[df['销售状态'].isin(['正利润加快动销','负利润加快动销','清仓'])]

    # 计算排名
    df = df.sort_values(by="available_stock_money", ascending=False).reset_index(drop=True)
    total_rows = len(df)
    df["排名"] = df.index + 1  # 索引从0开始，排名+1
    df["排名百分比"] = (df["排名"] / total_rows * 100).round(2)

    df.to_excel('F://Desktop//dwm_sku_temp_info.xlsx', index=0)

    return None


def supplier_sku_price(df_price, org='YB'):
    """
    供应商货盘sku
    1、用供应商货盘sku的信息，匹配TEMU定价数据
    2、能匹配到的，用定价逻辑判断怎么定价
    3、不能匹配到的，用供应商sku定价
    """
    # 1. 供应商货盘sku
    if org == 'YB':
        table_name = 'dwm_supplier_sku_price'
    elif org == 'TT':
        table_name = 'tt_dwm_supplier_sku_price'
    sql = f"""
         SELECT 
             YB_sku sku, YM_sku, warehouse_price, total_cost total_cost_s, shippingCost, ship_name ship_name_s, 
             warehouse_id warehouse_id_s , platform,
             warehouse_name, warehouse, available_stock sup_stock, country, platform_must_percent platform_must_percent_s,
             platform_zero platform_zero_s, sup_price , platform_must_percent+platform_zero target_profit_rate 
         FROM yibai_oversea.{table_name}
         WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name})
         and sup_price is not Null 
         and platform = 'ALI'
         and sup_stock > 0
     """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)

    # 2. YB sku
    df = pd.merge(df_price, df_sup_price, how='outer', on=['sku','country','warehouse','platform'])
    df['available_stock'] = df['available_stock'].fillna(0).astype(int)
    df['sup_price'] = df['sup_price'].astype(float)
    # df['sup_price'] = df['sup_price']/us_rate

    # 替换逻辑
    c1 = (df['available_stock'] > 0) & (df['sup_stock'] == 0)
    c2 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['销售状态'].isin(['正常', '涨价缩销','']))
    c3 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (
        df['销售状态'].isin(['正利润加快动销', '负利润加快动销', '清仓', '补充清仓sku']))
    c4 = (df['available_stock'] == 0 | df['available_stock'].isna()) & (df['sup_stock'] > 0)
    df['condition'] = np.select([c1, c2, c3, c4],
                                ['供应商sku无库存取建议兜底价', '都有库存且状态正常取小值', '动销品取建议兜底价', 'YBsku无库存'], 0)
    df['最终建议价'] = np.select([c1, c2, c3, c4], [
        df['sku建议兜底价'], np.minimum(df['sup_price'], df['sku建议兜底价'].fillna(np.inf)), df['sku建议兜底价'], df['sup_price']],
        df['sku建议兜底价'])
    df['最终建议兜底毛利率'] = np.where(df['最终建议价'] == df['sku建议兜底价'], df['sku建议兜底毛利率'], df['target_profit_rate'])
    df['是否供应商货盘sku'] = np.where(~df['YM_sku'].isna(), 1, 0)
    df['是否使用货盘sku定价'] = np.where(df['最终建议价']==df['sup_price'], 1, 0)

    # 替换字段
    dic = {'YM_sku':'供应商sku', 'warehouse_price':'供货价', 'total_cost_s':'供应商sku总运费', 'shippingCost':'供应商sku尾程运费',
           'ship_name_s':'供应商sku尾程渠道', 'warehouse_id_s':'供应商sku库存子仓id', 'warehouse_name':'供应商sku库存子仓',
           'sup_stock':'供应商sku可用库存', 'condition':'替换为货盘价的原因',
           'sup_price':'供应商供货价的定价', 'target_profit_rate':'供应商sku目标毛利率'}
    df = df.rename(columns=dic)
    # 定义列映射关系：
    col_mapping = {
        'new_price': '供货价',
        'totalCost_origin': '供应商sku总运费',
        'ship_fee': '供应商sku总运费',
        'ship_name': '供应商sku尾程渠道',
        'platform_must_percent': 'platform_must_percent_s',
        'platform_zero': 'platform_zero_s',
        'best_warehouse_id': '供应商sku库存子仓id',
        'best_warehouse_name': '供应商sku库存子仓',
        'available_stock': '供应商sku可用库存',
        'sku建议兜底毛利率': '最终建议兜底毛利率',
        'sku建议兜底价': '最终建议价'
    }
    # ,
    # 'sku建议兜底毛利率': '最终建议兜底毛利率',
    # 'sku建议兜底价': '最终建议价'
    mask = df['是否使用货盘sku定价'] == 1
    for target_col, replace_col in col_mapping.items():
        df[target_col] = np.where(mask, df[replace_col], df[target_col])

    # 整理字段
    col = ['供货价', '供应商sku总运费', '供应商sku尾程运费', '供应商sku尾程渠道', '供应商sku库存子仓id', '供应商sku库存子仓',
           '供应商sku可用库存','供应商供货价的定价','供应商sku目标毛利率','最终建议价','最终建议兜底毛利率',
           'platform_must_percent_s', 'platform_zero_s']
    df.drop(col, axis=1, inplace=True)

    return df


def temp_dwm():
    sql = """
        SELECT *
        FROM over_sea.oversea_flash_clearout_sku
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    df_sku.to_excel('F://Desktop//df_dwm.xlsx', index=0)


def dwm_info_temp():
    """ info临时排查 """
    sql = """
        SELECT date_id, sku, warehouse, best_warehouse_name, overage_level, available_stock_money
        FROM over_sea.dwm_sku_temp_info
        WHERE  date_id = '2025-12-22'
        -- and warehouse in ('美国仓','德国仓','英国仓','墨西哥仓','澳洲仓','加拿大仓')
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_t = conn.read_sql(sql)

    sql = """
        SELECT date_id, sku, warehouse, best_warehouse_name, overage_level, available_stock_money
        FROM over_sea.dwm_sku_temp_info
        WHERE  date_id = '2025-12-18'
        -- and warehouse in ('美国仓','德国仓','英国仓','墨西哥仓','澳洲仓','加拿大仓')
        and available_stock > 0
    """
    df_y = conn.read_sql(sql)

    sql = """
        SELECT sku, warehouse, 1 as is_flash_sku
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = '2025-12-02'
    """
    df_flash = conn.read_sql(sql)

    df_t = pd.merge(df_t, df_flash, how='inner', on=['sku','warehouse'])
    df_y = pd.merge(df_y, df_flash, how='inner', on=['sku','warehouse'])

    df = pd.merge(df_y, df_t, how='outer', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_dwm.xlsx', index=0)


def get_v_stock():
    """ """
    # sql = """
    #     SELECT *
    #     FROM yb_datacenter.v_oversea_stock
    #     WHERE available_stock > 0
    # """
    # conn = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df = conn.ck_select_to_df(sql)

    # 成本
    sql = """
        SELECT distinct sku, warehouse, available_stock, overage_level, new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2026-02-02' and warehouse = '德国仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_cost = conn.read_sql(sql)

    # df = pd.merge(df, df_cost, how='left', on=['sku'])
    #
    # sql = """
    #     SELECT sku, warehouseName, warehouse, available_stock, shipName, totalCost_origin, totalCost, firstCarrierCost
    #     FROM over_sea.oversea_transport_fee_useful
    #     WHERE available_stock > 0
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_useful = conn.read_sql(sql)
    # print(df_useful.info())
    df_cost.to_excel('F://Desktop//df_cost.xlsx', index=0)


def get_amazon_all():
    """ """
    sql = """
        SELECT account_id, seller_sku, sku
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing
        WHERE date_id = '2026-01-30'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())
    df.to_excel('F://Desktop//df_amazon_listing.xlsx', index=0)

def get_account_temp():

    sql = """
        select * from yibai_sale_center_system_sync.yibai_system_account
        where platform_code = 'EB' AND `status` = 1 AND is_del =0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    df.to_excel('F://Desktop//df_ebay_account.xlsx', index=0)


if __name__ == '__main__':


    oversea_sku_price(org='YB')
    # oversea_sku_price(org='haitu')
    # oversea_sku_price(org='TT')
    # #
    org = 'YB'
    # check_temu_listing_price(org=org)
    check_ali_listing_price(org=org)
    # check_ebay_listing_price(org=org)
    # check_amazon_listing_price(org=org)
    # check_lazada_listing_price()

    # tts_get_sku_price()

    # get_amazon_all()
    # get_account_temp()