"""
TEMU海外仓调价程序
"""
##
import pandas as pd
import numpy as np
import time, datetime
import warnings
warnings.filterwarnings("ignore")
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split, get_oversea_ship_type_list
import re
import os
import pymysql
from all_auto_task.oversea_price_adjust_2023 import get_mx_stock_age
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse, tt_get_oversea_order
##
# 获取SKU的库存和库龄数据
def extract_correct_string(s):
    # Check if '*' or '+' is in the string
    if '*' in s or '+' in s:
        # Split the string using * and + as delimiters
        parts = re.split(r'[*+]', s)

        # Iterate over the parts and return the first non-numeric string
        for part in parts:
            if not (part.isdigit() and len(part) == 1):
                return part

    # If no '*' or '+' is found, or no non-numeric string is found after splitting, return the original string
    return s

def get_main_resp(temu_listing):
    """
    匹配链接的责任归属。当前版本：第九版责任账户
    输入：listing:new_sku、主体账号
         责任人明细表
    输出：责任人、is_same
    """
    #  获取责任人明细
    print('获取责任人明细表...')
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_resp = pd.read_excel('F://Desktop//TEMU责任人明细.xlsx', dtype={'sku': str})
    sql = """
        SELECT sku, `站点`, `责任人`
        FROM over_sea.temu_responsible_name
    """
    df_resp = conn.read_sql(sql)
    df_resp = df_resp.rename(columns={'sku':'new_sku'})
    # df_resp['new_sku'] = df_resp['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    temu_listing_0 = pd.merge(temu_listing, df_resp[['new_sku','站点','责任人']], how='left', on=['new_sku','站点'])

    #
    # df_resp_line = pd.read_excel('F://Desktop//TEMU责任人明细_类目.xlsx')
    sql = """
        SELECT *
        FROM over_sea.temu_responsible_line_name
    """
    df_resp_line = conn.read_sql(sql)
    df_resp_line = df_resp_line.rename(columns={'一级类目':'一级产品线','二级级类目':'二级产品线','主体账号':'责任账号'})
    line1 = df_resp_line[df_resp_line['二级产品线'] == '全']
    line2 = df_resp_line[df_resp_line['二级产品线'] != '全']
    temu_listing_0 = pd.merge(temu_listing_0, line1[['一级产品线','责任账号']], how='left', on=['一级产品线'])
    # print(temu_listing_0.info())
    temu_listing_0['责任人'] = np.where(temu_listing_0['责任人'].isna(), temu_listing_0['责任账号'], temu_listing_0['责任人'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    #
    temu_listing_0 = pd.merge(temu_listing_0, line2[['一级产品线','二级产品线','责任账号']], how='left', on=['一级产品线','二级产品线'])
    temu_listing_0['责任人'] = np.where(temu_listing_0['责任人'].isna(), temu_listing_0['责任账号'], temu_listing_0['责任人'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    # 剩下未归属成功的，按链接初始的主体账号
    temu_listing_0['责任人'] = np.where(temu_listing_0['站点']=='墨西哥','易佰泛品：刘丽萍temu', temu_listing_0['责任人'])
    temu_listing_0['责任人'] = np.where(temu_listing_0['责任人'].isna(), temu_listing_0['主体账号'], temu_listing_0['责任人'])
    temu_listing_0['is_same'] = np.where(temu_listing_0['主体账号'] == temu_listing_0['责任人'], 1, 0)

    return temu_listing_0
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
def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku new_sku, title_cn `产品名称`, b.category_path as `产品线路线` 
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
    df_line = df_line.drop_duplicates(subset='new_sku')
    df = pd.merge(df, df_line[['new_sku','一级产品线','二级产品线']], how='left', on=['new_sku'])

    return df

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
                where 
                (warehouse_other_type = 2 or warehouse_name like '%Temu独享%' or warehouse_id in (958, 1653))
                and warehouse_name not like '%TT%'
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
    c0 = df_sku_stock['type'].str.contains('海兔|易通兔') | df_sku_stock['type'].str.contains('转VC|转泛品')
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺|精品|凯美晨')) & (~c0)
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock

def get_stock_tt():
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
                where
                (warehouse_other_type = 2 or warehouse_name like '%Temu独享%' or warehouse_id in (958))
                -- and warehouse_name not like '%TT%'
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

    df_sku_stock = df_sku_stock[df_sku_stock['type'].str.contains('通拓')]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock
# 获取SKU的库龄数据
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
    WHERE date = formatDateTime(subtractDays(now(),{n}), '%Y-%m-%d') and status in (0,1) 
    -- and yw.warehouse_name not like '%独享%' 
    and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    and yw.warehouse_name not in ('递四方美西洛杉矶仓')  -- 剔除不常用子仓
    """
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id
    FROM yb_datacenter.v_oversea_stock
    WHERE 
        (warehouse_other_type = 2 or warehouse_name like '%独享%' or warehouse_id in (958))
        and warehouse_name not like '%TT%'
        and available_stock > 0 
        
    """
    df_stock = ck_client.ck_select_to_df(sql)
    df_stock_age = pd.merge(df_stock_age, df_stock, how='inner', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse_id 聚合
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].astype('float')
    df_stock_age_base = df_stock_age[
        ['sku', 'charge_currency', 'cargo_type', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    df_stock_age_info = df_stock_age[
        ['sku', 'warehouse_id', 'warehouse', 'warehouse_stock', 'inventory_age', 'charge_total_price', 'age_30_plus','age_60_plus',
         'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_210_plus','age_270_plus', 'age_360_plus']]
    df_stock_age_info.loc[:, 'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg(
        {'warehouse_stock': 'sum', 'inventory_age': 'max', 'charge_total_price': 'sum',
         'age_30_plus': 'sum', 'age_60_plus': 'sum',  'age_90_plus': 'sum',
         'age_120_plus': 'sum', 'age_150_plus': 'sum', 'age_180_plus': 'sum', 'age_210_plus': 'sum',
         'age_270_plus': 'sum', 'age_360_plus': 'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus', 'age_60_plus', 'age_90_plus',
         'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_210_plus', 'age_270_plus', 'age_360_plus']]
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
        c31 = df['age_210_plus'] > 0
        c3 = df['age_180_plus'] > 0
        c4 = df['age_150_plus'] > 0
        c5 = df['age_120_plus'] > 0
        c6 = df['age_90_plus'] > 0
        c7 = df['age_60_plus'] > 0
        c8 = df['age_30_plus'] > 0
        df['overage_level'] = np.select([c1, c2, c31, c3, c4, c5, c6, c7,c8], [360, 270, 210, 180, 150, 120, 90, 60, 30])
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
    df_stock_age_warehouse = df_stock_age_warehouse.drop_duplicates(subset=['sku','warehouse'])

    return df_stock_age_id, df_stock_age_warehouse


def get_stock_age_tt():
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
    WHERE date = formatDateTime(subtractDays(now(),{n}), '%Y-%m-%d') and status in (0,1) 
    -- and yw.warehouse_name not like '%独享%' 
    -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    and yw.warehouse_name not in ('递四方美西洛杉矶仓')  -- 剔除不常用子仓
    """
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id
    FROM yb_datacenter.v_oversea_stock
    WHERE 
        (warehouse_other_type = 2 or warehouse_name like '%独享%' or warehouse_id in (958))
        -- and warehouse_name not like '%TT%'
        and available_stock > 0 

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
         'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_210_plus', 'age_270_plus', 'age_360_plus']]
    df_stock_age_info.loc[:, 'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg(
        {'warehouse_stock': 'sum', 'inventory_age': 'max', 'charge_total_price': 'sum',
         'age_30_plus': 'sum', 'age_60_plus': 'sum', 'age_90_plus': 'sum',
         'age_120_plus': 'sum', 'age_150_plus': 'sum', 'age_180_plus': 'sum', 'age_210_plus': 'sum',
         'age_270_plus': 'sum', 'age_360_plus': 'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus', 'age_60_plus', 'age_90_plus',
         'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_210_plus', 'age_270_plus', 'age_360_plus']]
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
        c31 = df['age_210_plus'] > 0
        c3 = df['age_180_plus'] > 0
        c4 = df['age_150_plus'] > 0
        c5 = df['age_120_plus'] > 0
        c6 = df['age_90_plus'] > 0
        c7 = df['age_60_plus'] > 0
        c8 = df['age_30_plus'] > 0
        df['overage_level'] = np.select([c1, c2, c31, c3, c4, c5, c6, c7, c8],
                                        [360, 270, 210, 180, 150, 120, 90, 60, 30])
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
    df_stock_age_warehouse = df_stock_age_warehouse.drop_duplicates(subset=['sku', 'warehouse'])

    return df_stock_age_id, df_stock_age_warehouse
def get_sku_sales():
    sql = """

    SELECT  
        SKU as sku,SUM(3days_sales) as 3days_sales,SUM(7days_sales) as 7days_sales,SUM(15days_sales) as 15days_sales,SUM(30days_sales) as 30days_sales,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales', 
        warehouse
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        -- and b.warehouse_name not like '%%独享%%' 
        and b.warehouse_name not like '%%TT%%'
        )A 
    GROUP BY SKU, warehouse
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    return df_sales
#
def get_sku_sales_tt():
    sql = """

    SELECT  
        SKU as sku,SUM(3days_sales) as 3days_sales,SUM(7days_sales) as 7days_sales,SUM(15days_sales) as 15days_sales,SUM(30days_sales) as 30days_sales,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales', 
        warehouse
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        -- and b.warehouse_name not like '%%独享%%' 
        -- and b.warehouse_name not like '%%TT%%'
        )A 
    GROUP BY SKU, warehouse
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    return df_sales

def get_sku_sales_new():
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
    df = df_sales.groupby(['sku', 'warehouse']).agg({'3days_sales': 'sum', '7days_sales': 'sum', '15days_sales': 'sum',
                                                     '30days_sales': 'sum', '60days_sales': 'sum','90days_sales': 'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales'] / 7 + 0.2 * df['15days_sales'] / 15 + 0.1 * df['30days_sales'] / 30
    df['recent_day_sales'] = 0.8 * df['3days_sales'] / 3 + 0.2 * df['7days_sales'] / 7
    # df_sales = df_sales.sample(50000)
    return df


def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct 
        case when from_currency_code = 'HUF' then 'HU' else country end as country, 
        from_currency_code as charge_currency, erp_rate rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)

    return df_rate

##
def cut_bins(df):
    """
    对主要条件分箱：超库龄天数、日销、总库存的可售天数、超库龄库存的可售天数
    """
    df['esd_bins'] = pd.cut(df['estimated_sales_days'], bins=[-1, 10, 20, 30, 40, 50, 80, 100, 150, 200, 300, 1000],
                            labels=['A∈[0,10)', 'A∈[10,20)',  'A∈[20,30)', 'A∈[30,40)',
                                    'A∈[40,50)', 'A∈[50,80)', 'A∈[80,100)', 'A∈[100,150)', 'A∈[150,200)', 'A∈[200,300)',
                                    'A∈[300,∞)'], right=False)
    df['esd_bins'] = np.where(df['estimated_sales_days'] < 0, 'A∈[0,10)', df['esd_bins'])
    df['esd_bins'] = np.where(df['estimated_sales_days'] > 300, 'A∈[300,∞)', df['esd_bins'])

    df['day_sales_bins'] = pd.cut(df['day_sales'], bins=[-1, 0.1, 0.3, 0.6, 1, 3, 5, 10],
                                  labels=['S∈(0,0.1]', 'S∈(0.1,0.3]', 'S∈(0.3,0.6]', 'S∈(0.6,1]', 'S∈(1,3]', 'S∈(3,5)',
                                          'S∈(5,∞)'])
    df['day_sales_bins'] = np.where(df['day_sales'] > 5, 'S∈(5,∞)', df['day_sales_bins'])
    return df

def write_to_sql(df, table_name):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = datetime.date.today()
    sql = f"""
    delete from over_sea.{table_name} where date_id >='{date_today}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')


def write_to_ck(df, table_name, datebase='yibai_oversea'):
    """
    将中间表数据写入CK
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    ALTER TABLE {datebase}.{table_name} DELETE where date_id = '{date_id}'
    """
    conn_ck.ck_execute_sql(sql)
    # 确认当天日期数据已删除
    n = 1
    while n < 5:
        print(f'删除表中已存在的当日数据...')
        sql = f"""
            SELECT count()
            FROM {datebase}.{table_name}
            where date_id = '{date_id}'
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('数据存入ck...')
            for i in df['warehouse'].unique():
                df_temp = df[df['warehouse']==i]
                conn_ck.ck_insert(df_temp, table_name, if_exist='append')
            break
        else:
            n += 1
            time.sleep(60)
    if n == 5:
        print('备份CK失败，当天数据未删除完成，CK未备份')

def dwm_oversea_sku():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    print('获取库存信息...')
    df_stock = get_stock()
    # df_stock = df_stock[df_stock['available_stock']>0]
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

    dwm_sku = pd.merge(df_stock, sku_sales, how='left', on=['sku', 'warehouse'])
    dwm_sku.info()
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0).astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 23:35] = dwm_sku.iloc[:, 23:35].fillna(0)
    # dwm_sku.info()
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])
    # 20241125 当sku+大仓下的库存为0时，最优子仓取当前大仓下常用的子仓（大仓下库存最多的子仓）。避免最优子仓选到不常用仓库上。
    df_stock_max = df_stock.groupby(['warehouse', 'warehouse_id', 'best_warehouse'])[
        'available_stock'].sum().reset_index()
    df_stock_max = df_stock_max.rename(columns={'warehouse_id': 'max_id', 'best_warehouse': 'max_name'})
    df_stock_max = df_stock_max.sort_values(by='available_stock', ascending=False).drop_duplicates(subset=['warehouse'])
    dwm_sku = pd.merge(dwm_sku, df_stock_max[['warehouse', 'max_id', 'max_name']], how='left', on=['warehouse'])
    # 20250324 精铺子仓的最优子仓限定在精铺仓，不转化为常用子仓
    c1 = (dwm_sku['available_stock'] <= 0) & (~dwm_sku['best_warehouse_name'].str.contains('精铺'))
    dwm_sku['best_warehouse_id'] = np.where(c1, dwm_sku['max_id'], dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(c1, dwm_sku['max_name'], dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['max_id', 'max_name'], axis=1, inplace=True)
    # 仓租数据，汇率转化
    # df_rate = get_rate()
    # dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    # dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    # dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    # dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)
    dwm_sku['charge_currency'] = ''
    dwm_sku['age_210_plus'] = dwm_sku['age_180_plus']
    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus', 'age_60_plus','age_90_plus',
                     'age_120_plus', 'age_150_plus', 'age_180_plus','age_210_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '15days_sales', '30days_sales', 'day_sales' ]
    dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']] = dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']].astype(int)
    dwm_sku = dwm_sku[columns_order]

    # 可售天数 (大仓的总库存）
    dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(np.inf, 9999).replace(
        np.nan, 0)
    print('条件分箱...')
    dwm_sku = cut_bins(dwm_sku)
    # 销售状态分类:根据超库龄情况判断分类。
    dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')

    c1 = (dwm_sku['overage_level'] >= 210)
    c2 = (dwm_sku['overage_level'] == 180) & ((dwm_sku['age_180_plus'] / dwm_sku['available_stock']) >= 0.3) & (
                dwm_sku['age_180_plus'] > 1)
    c3 = (dwm_sku['overage_level'] == 150) & ((dwm_sku['age_150_plus'] / dwm_sku['available_stock']) >= 0.4) & (
                dwm_sku['age_150_plus'] > 2)
    c4 = (dwm_sku['overage_level'] == 120) & ((dwm_sku['age_120_plus'] / dwm_sku['available_stock']) >= 0.5) & (
                dwm_sku['age_120_plus'] > 3)
    c5 = (dwm_sku['overage_level'] == 90) & ((dwm_sku['age_90_plus'] / dwm_sku['available_stock']) >= 0.5) & (
                dwm_sku['age_90_plus'] > 4)
    dwm_sku['clearance_level'] = np.select([c1, c2, c3, c4, c5], [210, 180, 150, 120, 90], 0)

    sql = """
        SELECT *
        FROM over_sea.temu_profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_section = conn.read_sql(sql)
    #
    dwm_sku_2 = pd.merge(dwm_sku, df_section, how='left', on=['clearance_level', 'esd_bins'])
    #
    dwm_sku_2[['section']] = dwm_sku_2[['section']].fillna(0)
    dwm_sku_2[['lowest_profit']] = dwm_sku_2[['lowest_profit']].fillna(0.11)
    dwm_sku_2['begin_profit'] = 0
    dwm_sku_2['after_profit'] = dwm_sku_2['begin_profit'] + dwm_sku_2['section']

    # (调价幅度+平台最低净利率)最低不超过保底净利率
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.11),
                                         dwm_sku_2['lowest_profit']-0.11, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    dwm_sku_2[['new_price','gross']] = dwm_sku_2[['new_price','gross']].astype(str)
    print(dwm_sku_2.info())
    print('SKU信息及调价幅度已获取，准备写入dwm_sku_info_temu...')
    write_to_sql(dwm_sku_2, 'dwm_sku_info_temu')

    # CK备份
    table_name = 'dwm_sku_info_temu'
    date_now = time.strftime('%Y-%m-%d')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_now}'
    """
    conn_ck.ck_execute_sql(sql)
    conn_ck.ck_insert(dwm_sku_2, table_name, if_exist='append')
    return None



def dwm_oversea_sku_tt():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    print('获取库存信息...')
    df_stock = get_stock_tt()
    # df_stock = df_stock[df_stock['available_stock']>0]
    print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age_tt()
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

    dwm_sku = pd.merge(df_stock, sku_sales, how='left', on=['sku', 'warehouse'])
    dwm_sku.info()
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0).astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 23:35] = dwm_sku.iloc[:, 23:35].fillna(0)
    # dwm_sku.info()
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])
    # 20241125 当sku+大仓下的库存为0时，最优子仓取当前大仓下常用的子仓（大仓下库存最多的子仓）。避免最优子仓选到不常用仓库上。
    df_stock_max = df_stock.groupby(['warehouse', 'warehouse_id', 'best_warehouse'])[
        'available_stock'].sum().reset_index()
    df_stock_max = df_stock_max.rename(columns={'warehouse_id': 'max_id', 'best_warehouse': 'max_name'})
    df_stock_max = df_stock_max.sort_values(by='available_stock', ascending=False).drop_duplicates(subset=['warehouse'])
    dwm_sku = pd.merge(dwm_sku, df_stock_max[['warehouse', 'max_id', 'max_name']], how='left', on=['warehouse'])
    # 20250324 精铺子仓的最优子仓限定在精铺仓，不转化为常用子仓
    c1 = (dwm_sku['available_stock'] <= 0) & (~dwm_sku['best_warehouse_name'].str.contains('精铺'))
    dwm_sku['best_warehouse_id'] = np.where(c1, dwm_sku['max_id'], dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(c1, dwm_sku['max_name'], dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['max_id', 'max_name'], axis=1, inplace=True)
    # # 仓租数据，汇率转化
    # df_rate = get_rate()
    # dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    # dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    # dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    # dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)
    dwm_sku['charge_currency'] = ''
    dwm_sku['age_210_plus'] = dwm_sku['age_180_plus']
    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus', 'age_60_plus','age_90_plus',
                     'age_120_plus', 'age_150_plus', 'age_180_plus','age_210_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '15days_sales', '30days_sales', 'day_sales' ]
    dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']] = dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']].astype(int)
    dwm_sku = dwm_sku[columns_order]

    # 可售天数 (大仓的总库存）
    dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(np.inf, 9999).replace(
        np.nan, 0)
    print('条件分箱...')
    dwm_sku = cut_bins(dwm_sku)
    # 销售状态分类:根据超库龄情况判断分类。
    dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')

    c1 = (dwm_sku['overage_level'] >= 210)
    c2 = (dwm_sku['overage_level'] == 180) & ((dwm_sku['age_180_plus'] / dwm_sku['available_stock']) >= 0.3) & (
                dwm_sku['age_180_plus'] > 1)
    c3 = (dwm_sku['overage_level'] == 150) & ((dwm_sku['age_150_plus'] / dwm_sku['available_stock']) >= 0.4) & (
                dwm_sku['age_150_plus'] > 2)
    c4 = (dwm_sku['overage_level'] == 120) & ((dwm_sku['age_120_plus'] / dwm_sku['available_stock']) >= 0.5) & (
                dwm_sku['age_120_plus'] > 3)
    c5 = (dwm_sku['overage_level'] == 90) & ((dwm_sku['age_90_plus'] / dwm_sku['available_stock']) >= 0.5) & (
                dwm_sku['age_90_plus'] > 4)
    dwm_sku['clearance_level'] = np.select([c1, c2, c3, c4, c5], [210, 180, 150, 120, 90], 0)

    sql = """
        SELECT *
        FROM over_sea.temu_profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_section = conn.read_sql(sql)
    #
    dwm_sku_2 = pd.merge(dwm_sku, df_section, how='left', on=['clearance_level', 'esd_bins'])
    #
    dwm_sku_2[['section']] = dwm_sku_2[['section']].fillna(0)
    dwm_sku_2[['lowest_profit']] = dwm_sku_2[['lowest_profit']].fillna(0.11)
    dwm_sku_2['begin_profit'] = 0
    dwm_sku_2['after_profit'] = dwm_sku_2['begin_profit'] + dwm_sku_2['section']

    # (调价幅度+平台最低净利率)最低不超过保底净利率
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.11),
                                         dwm_sku_2['lowest_profit']-0.11, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    dwm_sku_2[['new_price','gross']] = dwm_sku_2[['new_price','gross']].astype(str)
    print(dwm_sku_2.info())
    print('SKU信息及调价幅度已获取，准备写入dwm_sku_info_temu_tt...')
    write_to_sql(dwm_sku_2, 'dwm_sku_info_temu_tt')
    # conn.to_sql(dwm_sku_2, 'dwm_sku_info_temu_tt', if_exists='replace')
    # # CK备份
    # table_name = 'dwm_sku_info_temu_tt'
    # date_now = time.strftime('%Y-%m-%d')
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # sql = f"""
    # ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_now}'
    # """
    # conn_ck.ck_execute_sql(sql)
    # conn_ck.ck_insert(dwm_sku_2, table_name, if_exist='append')
    return None

def get_transport_fee():
    """
    获取当前最新运费数据
    """
    # 头程取分摊头程
    # 2026-02-24 暂取报价头程
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, warehouse,
        -- case 
        --    when firstCarrierCost >= 0.01 then totalCost_origin 
        --    else totalCost
        -- end as total_cost, 
        totalCost total_cost,
        shipName as ship_name, shippingCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee, extraSizeFee,
        lowest_price, platform, country
    FROM oversea_transport_fee_useful_temu
    WHERE not (warehouse = '德国仓' and country in ('GB','UK'))
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_1 = conn.read_sql(sql)
    df_1['is_temu'] = 1
    if len(df_1) == 0:
        raise ValueError("无TEMU运费数据")
    sql = """
    SELECT
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, warehouse,
        -- case 
        --    when firstCarrierCost >= 0.01 then totalCost_origin 
        --    else totalCost
        -- end as total_cost, 
        totalCost total_cost,
        shipName as ship_name,shippingCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee, extraSizeFee,
        lowest_price, platform, country
    FROM oversea_transport_fee_useful
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_2 = conn.read_sql(sql)
    df_2['is_temu'] = 0

    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='is_temu', ascending=False)
    df = df.drop_duplicates(subset=['sku', 'warehouse_id','country'])
    print(df.info())

    return df

def get_platform_fee():
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

    return df

#
def get_temu_account(org='YB'):
    # temu账号获取
    if org=='YB':
        sql = """
        select 
            a.account_id ,a.account_status, b.main_name main_name_old,a.oa_department_name,
            a.oa_group_name main_name, a.account_s_name short_name,a.account_operation_mode 
        from yibai_account_manage_sync.yibai_temu_account a
        left join yibai_account_manage_sync.yibai_account_main b
        on a.main_id=b.main_id 
        where a.account_type=1
        """
    elif org=='TT':
        sql = """
        select 
            a.account_id ,a.account_status, b.main_name main_name_old,a.oa_department_name,
            a.oa_group_name main_name, a.account_s_name short_name,a.account_operation_mode 
        from yibai_account_manage_sync.yibai_temu_account a
        left join yibai_account_manage_sync.yibai_account_main b
        on a.main_id=b.main_id 
        where a.account_type=20
        """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_temu_account = conn_ck.ck_select_to_df(sql)
    df_temu_account.columns = [i.split('.')[-1] for i in df_temu_account.columns]
    df_temu_account['account_status'] = df_temu_account['account_status'].map({10: '启用', 20: '停用', 30: '异常', })
    df_temu_account['account_operation_mode'] = df_temu_account['account_operation_mode'].map({1: '全托管', 2: '半托管'})
    print(df_temu_account.info())
    df_temu_account = df_temu_account.drop_duplicates(subset=['account_id'])

    return df_temu_account



def get_freight_subsidy(listing_t, dim='listing'):
    # 运费补贴计算
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    listing_t = pd.merge(listing_t, df_rate0[['country','rate']], how='left', on=['country'])

    print(f"链接表国家范围有：{listing_t['country'].unique()}")
    # 获取运费补贴
    sql = """
        SELECT country, currency charge_currency, limit_price, freight_subsidy
        FROM over_sea.temu_freight_subsidy
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fre = conn.read_sql(sql)
    listing_t = pd.merge(listing_t, df_fre, how='left', on=['country'])

    # 补齐汇率
    df_rate0 = df_rate0.rename(columns={'rate':'rate_2'})
    df_rate0 = df_rate0.drop_duplicates(subset=['charge_currency'])
    listing_t = pd.merge(listing_t, df_rate0[['charge_currency','rate_2']], how='left', on=['charge_currency'])
    listing_t['rate'] = np.where(listing_t['rate'].isna(), listing_t['rate_2'], listing_t['rate'])
    listing_t.drop(['charge_currency', 'rate_2'], axis=1, inplace=True)

    if dim == 'sku':
        listing_t = listing_t.drop_duplicates(subset=['sku','warehouse', 'country'])
    # listing_t['limit_price'] = listing_t['limit_price'].fillna(0).astype(float)
    # listing_t['freight_subsidy'] = listing_t['freight_subsidy'].fillna(0).astype(float)
    # listing 维度返回最终判断后的补贴
    if dim == 'listing':
        col = ['limit_price', 'supplier_price', 'freight_subsidy']
        listing_t[col] = listing_t[col].fillna(0).astype(float)
        listing_t['freight_subsidy'] = np.where((listing_t['supplier_price']*df_rate.iloc[0,2]/listing_t['rate']) < listing_t['limit_price'],
                                         listing_t['freight_subsidy'] * listing_t['rate'], 0)

    return listing_t


##
def temu_listing_temp():
    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id,e.short_name,a.site_code,a.item_id,a.product_sku_id,a.product_skc_id,
        a.stock_number,c.online_status,a.sku,b.lazada_account_operation_mode,
        c.added_to_site_time,c.supplier_price,date(a.create_time) as create_time, a.select_status
    from yibai_sale_center_listing_sync.yibai_temu_listing a
    left join yibai_sale_center_common_sync.yibai_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    left join yibai_sale_center_system_sync.yibai_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    print(listing_t.info())

    c1 = (listing_t['select_status'] != -1)
    c2 = (listing_t['select_status'] == -1) & (listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止']))
    listing_t = listing_t[c1 | c2]
    listing_t['online_status'] = np.where(listing_t['select_status']==-1,
                                          listing_t['online_status'], listing_t['select_status'])
    dic = {-1:'未同步', 0:'已弃用', 1:'待平台选品', 14:'待卖家修改', 15:'已修改', 16:'服饰可加色', 2:'待上传生产资料',
           3:'待寄样', 4:'寄样中',5:'待平台审版', 6:'审版不合格', 7:'平台核价中', 8:'待修改生产资料',
           9:'核价未通过', 10:'待下首单', 11:'已下首单', 12:'已加入站点', 13:'已下架', 17:'已终止'}
    listing_t['online_status'] = listing_t['online_status'].replace(dic)
    print(listing_t.info())
    a = listing_t.groupby(['select_status','online_status']).count()
    print(a)

# 获取多站点链接
def get_site_code_price(df):
    """ 获取多站点链接的价格 """

    sql = """
        SELECT account_id, product_sku_id, site_code country, supplier_price site_price
        FROM yibai_sale_center_listing_sync.yibai_temu_listing_supplier_price_site
        WHERE site_code in ('DE','FR','ES','IT')
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_temu = conn_ck.ck_select_to_df(sql)

    df_temu['product_sku_id'] = df_temu['product_sku_id'].astype(str)
    print(df.info())
    print(df_temu.info())
    df = pd.merge(df, df_temu, how='left', on=['account_id','product_sku_id', 'country'])

    df['supplier_price'] = np.where(~df['site_price'].isna(), df['site_price'], df['supplier_price'])

    if len(df) < 1000000:
        col = ['account_id','product_sku_id','site_code','country','supplier_price','site_price']
        df[col].to_excel('F://Desktop//df_site_price.xlsx', index=0)



def get_temu_listing():
    print("===temu刊登链接数据===")

    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id,e.short_name,a.site_code,p.site_code country, a.item_id,a.product_sku_id,a.product_skc_id,
        a.stock_number,c.online_status,a.sku, p.select_status lazada_account_operation_mode,
        c.added_to_site_time,p.supplier_price,date(a.create_time) as create_time, a.select_status
    from yibai_sale_center_listing_sync.yibai_temu_listing a
    left join yibai_sale_center_common_sync.yibai_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    left join yibai_sale_center_listing_sync.yibai_temu_listing_supplier_price_site p 
    on a.product_sku_id = p.product_sku_id and a.account_id = p.account_id
    left join yibai_sale_center_system_sync.yibai_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    and a.select_status = 12
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    # 20250212 更新链接状态取数表
    # c1 = (listing_t['select_status'] != -1)
    # c2 = (listing_t['select_status'] == -1) & (listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止']))
    # listing_t = listing_t[c1 | c2]
    listing_t['online_status'] = np.where(listing_t['select_status']==-1,
                                          listing_t['online_status'], listing_t['select_status'])
    dic = {-1:'未同步', 0:'已弃用', 1:'待平台选品', 14:'待卖家修改', 15:'已修改', 16:'服饰可加色', 2:'待上传生产资料',
           3:'待寄样', 4:'寄样中',5:'待平台审版', 6:'审版不合格', 7:'平台核价中', 8:'待修改生产资料',
           9:'核价未通过', 10:'待下首单', 11:'已下首单', 12:'已加入站点', 13:'已下架', 17:'已终止'}
    listing_t['online_status'] = listing_t['online_status'].replace(dic)
    listing_t.drop('select_status', axis=1, inplace=True)

    listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False).\
        drop_duplicates(subset=['account_id','product_sku_id','sku','country'])
    # 有活动的链接，申报价格替换为活动价
    promotion_listing = get_promotion_listing()
    promotion_listing['activity_price'] = promotion_listing['activity_price'].astype(float)
    idx = promotion_listing.groupby(['account_id','product_skc_id'])['activity_price'].idxmin()
    # 使用这些索引来选择记录
    listing_prom = promotion_listing.loc[idx].rename(columns={'activity_price':'链接最低活动价'})
    listing_prom = listing_prom[['account_id','product_skc_id','activity_stock','链接最低活动价']]
    col = ['product_sku_id','product_skc_id','sku','item_id']
    listing_t[col] = listing_t[col].astype('str')

    listing_t = pd.merge(listing_t, listing_prom[[
        'account_id','product_skc_id','链接最低活动价']], how='left', on=['account_id','product_skc_id'])
    listing_t['origin_supplier_price'] = listing_t['supplier_price']
    listing_t['is_promotion'] = np.where(~listing_t['链接最低活动价'].isna(), 1, 0)
    listing_t['supplier_price'] = np.where(~listing_t['链接最低活动价'].isna(), listing_t['链接最低活动价'], listing_t['supplier_price'])
    listing_t.drop('链接最低活动价', axis=1, inplace=True)
    # listing_t['运营模式'] = listing_t['lazada_account_operation_mode'].map({1: '全托管', 2: '半托管'})
    # 获取捆绑链接表的sku信息
    sql = """
    select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku 
    from yibai_sale_center_listing_sync.yibai_temu_bind_sku
    order by update_time desc
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    yibai_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    yibai_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)

    listing_t = listing_t.merge(yibai_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
                                      suffixes=['', '1'])
    listing_t.loc[listing_t['sku']=='','sku']=np.nan
    listing_t['sku'].fillna(listing_t['sku1'],inplace=True)
    listing_t.drop('sku1', axis=1, inplace=True)
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)
    listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    listing_t['country'] = listing_t['country'].replace('SP','ES')
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    listing_t['站点'] = listing_t['site_code'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))
    #
    listing_t['warehouse'] = listing_t['站点'].replace({'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓',
                                                        '英国': '英国仓', '新西兰': '澳洲仓', '加拿大': '加拿大仓',
                                                        '墨西哥':'墨西哥仓'})
    # listing_t.drop('站点', axis=1, inplace=True)
    # 处理站点
    # listing_t = listing_t.assign(column_to_split=listing_t['site_code'].str.split(',')).explode('column_to_split')
    # listing_t['column_to_split'] = listing_t['column_to_split'].replace({'SP':'ES','GB':'UK','NZ':'AU'})
    # listing_t = listing_t.rename(columns={'column_to_split':'country'})
    # listing_t['site'] = listing_t['column_to_split'].replace({'AU':'澳大利亚','US':'美国','DE':'德国','FR':'法国','IT':'意大利',
    #                                                     'ES':'西班牙','UK':'英国','NZ':'新西兰'})
    listing_t = listing_t.drop_duplicates()
    # print(listing_t.info())
    # temu账号获取
    df_temu_account = get_temu_account()
    # print(df_temu_account.info())
    listing_t = pd.merge(listing_t,
                         df_temu_account[['account_id', 'main_name','account_status','account_operation_mode']],
                         how='left', on=['account_id'])

    # 有效链接筛选
    listing_t = listing_t[listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止','已加入站点'])]
    # listing_t = listing_t[(~listing_t['sku'].isna()) & ~(listing_t['sku']=='')]
    listing_t = listing_t[(listing_t['account_status']=='启用') & (listing_t['account_operation_mode']=='半托管')]
    listing_t['date_id'] = time.strftime('%Y-%m-%d')
    listing_t['added_to_site_time'] = listing_t['added_to_site_time'].astype(str)

    # 运费补贴计算
    listing_t = listing_t.rename(columns={'supplier_price':'supplier_price_final','origin_supplier_price':'supplier_price'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate','limit_price'], axis=1, inplace=True)
    listing_t = listing_t.rename(columns={'supplier_price':'origin_supplier_price',
                                          'supplier_price_final':'supplier_price',
                                          'freight_subsidy':'origin_freight_subsidy'})
    listing_t = get_freight_subsidy(listing_t)
    listing_t.drop(['rate', 'limit_price'], axis=1, inplace=True)

    print(listing_t.info())

    return listing_t

def get_promotion_listing():
    """
    获取temu活动链接信息
    """
    sql = f"""
        SELECT 
            c.account_id account_id, product_sku_id, a.product_skc_id, name, a.promotion_id, e.start_time, e.end_time, 
            activity_price, currency,activity_stock,e.status,e.session_status
        FROM (
            SELECT
                a.account_id, a.product_sku_id, a.product_skc_id product_skc_id, a.name, a.id as promotion_id, a.end_time,
                b.activity_price, b.currency, a.activity_stock
            FROM yibai_sale_center_listing_sync.yibai_temu_listing_promotion a
            LEFT JOIN yibai_sale_center_listing_sync.yibai_temu_listing_promotion_price b
            ON a.id = b.promotion_id
            WHERE 
                a.status != 2
                -- and a.start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的 
                -- and a.end_time >= '{time.strftime('%Y-%m-%d')}' 
                and b.is_del = 0 and b.activity_price > 0
        ) a
        LEFT JOIN yibai_sale_center_system_sync.yibai_system_account as c 
        ON a.account_id=c.id
        -- 筛选活动状态：进行中、报名成功待开始
        INNER JOIN (
            SELECT id, promotion_id, session_status, status, start_time, end_time
            FROM yibai_sale_center_listing_sync.yibai_temu_listing_promotion_join_record
            WHERE status not in (1, 5) and is_del = 0
            and start_time > '2024-09-26'  -- 筛选报名时间大于9月26日的
            and end_time >= '{time.strftime('%Y-%m-%d')}'
        ) e ON toInt64(a.promotion_id) = toInt64(e.promotion_id)
        ORDER BY activity_price ASC
        LIMIT 1 BY account_id, product_sku_id, product_skc_id
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_prom = conn_ck.ck_select_to_df(sql)
    print(df_prom.info())
    df_prom.columns = [i.split('.')[-1] for i in df_prom.columns]
    df_prom[['product_sku_id', 'product_skc_id']] = df_prom[['product_sku_id', 'product_skc_id']].astype(str)

    return df_prom

##
#
def main_temu_listing():
    """
    temu调价主程序之一：链接获取及存表
    """
    listing_t = get_temu_listing()
    listing_t.drop(['站点'], axis=1, inplace=True)
    # listing_temp = listing_t.sample(10000)
    # listing_temp.to_excel('F://Desktop//temu_listing_temp.xlsx', index=0)
    write_to_ck(listing_t, 'yibai_oversea_temu_listing', datebase='yibai_oversea')
    print('temu链接处理程序完成.')

# TEMU宽表制作
def min_max_order_time():
    """
    获取链接的最早出单日期和最晚出单日期
    """
    # 订单数据
    sql_order = """
    select
        b.platform_order_id,b.order_id as order_id,b.account_id as account_id,c.seller_sku as seller_sku,
        c.quantity,toDate(b.create_time)as create_time,c.item_id,b.order_status,c.total_price,b.currency
    from (
        select *
        from yibai_oms_sync.yibai_oms_order
        where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4
    ) b
    left join (
        select *
        from yibai_oms_sync.yibai_oms_order_detail
        where order_id in (
            select order_id from yibai_oms_sync.yibai_oms_order
            where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4)
    ) c on b.order_id=c.order_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    order = conn_ck.ck_select_to_df(sql_order)
    order.columns = [i.split('.')[-1] for i in order.columns]
    order = order[order['order_status'] != 80]
    order['create_time'] = pd.to_datetime(order['create_time'], format='%Y-%m-%d')

    min_order_time = order.groupby(['account_id', 'seller_sku'])['create_time'].agg(['min', 'max']).reset_index()
    min_order_time.columns = ['account_id', 'seller_sku', '最早出单时间', '最晚出单时间']
    min_order_time = min_order_time[min_order_time['seller_sku'] != '']
    min_order_time['seller_sku'] = min_order_time['seller_sku'].astype(str)

    min_order_time = min_order_time.rename(columns={'seller_sku': 'product_sku_id'})

    return min_order_time

def mysql_escape(df, col='sku', d_type=None):
    # pymysql.escape_string
    # pymysql.converters.escape_string
    dfM = df[[col]].drop_duplicates()
    sku_list = list(dfM[col])
    if d_type:
        erp_list_str = ','.join(list(map(lambda m: f"{m}", sku_list)))
        return erp_list_str
    sku_list = list(map(lambda m: pymysql.converters.escape_string(m), sku_list))
    erp_list_str = ','.join(list(map(lambda m: f"'{m}'", sku_list)))
    return erp_list_str

def get_interf_fee(df):
    df_bundle = df[df['运费类型'].isna()]
    df_bundle = df_bundle[['sku', 'country']].drop_duplicates()
    #
    df_bundle['数量'] = 1
    df_bundle = df_bundle.reset_index(drop=True).reset_index()
    dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
           'AU': '353,769','CZ': '325','PL': '325','HU':'325','PT':'325','NL':'325','MX':'956'}
    df_bundle['warehouse_id'] = df_bundle['country'].replace(dic)
    df_bundle['sku'] = df_bundle['sku'].replace(
        {'1*JY03556': 'JY03556', '1*JYA02556-01': 'JYA02556-01', '1*DS00500': 'DS00500', '1*DS01567': 'DS01567',
         '5591*6-':'5591*6','5591*5-':'5591*5','JY14007*AA':'JY14007'})
    df_bundle = df_bundle[~df_bundle['sku'].str.contains('*AA')]
    #
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df_bundle)
    df_bundle_fee = get_bundle_fee(df_bundle)
    df_bundle_fee = df_bundle_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost']]

    return df_bundle_fee

def get_bundle_fee(df):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    w_list1 = get_oversea_ship_type_list()
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('TEMU', key1, key2, w_list1, '')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    return df_result
##
def check_temu_online_price():
    """
    TEMU在线链接的毛利润率测算。
    因不确定链接模式，运费数据获取顺序：
        海外仓调价数据的运费 >> 海外仓接口运费 >> 剔除代销仓 >> 虚拟仓运费
    """

    date_today = time.strftime('%Y-%m-%d')
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    # 测算temu申报价格的订单利润率
    print('获取TEMU链接信息...')
    # df_temu_base = get_temu_listing()
    sql = f"""
        SELECT *
        FROM yibai_oversea.yibai_ads_oversea_temu_listing
        WHERE date_id = '{date_today}' 
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temu_base = conn_ck.ck_select_to_df(sql)
    df_temu_base = df_temu_base.sort_values(by=['supplier_price']).drop_duplicates(subset=['account_id','product_sku_id','sku'])
    # 抽样
    # df_temu_base = df_temu_base.sample(1000)
    print(df_temu_base.info())
    df_temu_base = df_temu_base[(~df_temu_base['sku'].isna()) & ~(df_temu_base['sku'] == '')]
    df_temu_base = df_temu_base[df_temu_base['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止'])]
    df_temu_base = df_temu_base.rename(columns={'supplier_price':'申报价格','main_name':'主体账号'})
    df_temu_base['sku'] = df_temu_base['sku'].astype(str)

    col = ['stock_number','lazada_account_operation_mode','create_time','account_status','account_operation_mode']
    df_temu_base.drop(col, axis=1, inplace=True)
    order_time = min_max_order_time()
    df_temu_base = df_temu_base.merge(order_time, on=['account_id', 'product_sku_id'], how='left')
    df_temu_base = df_temu_base.reset_index(drop=True).reset_index()
    # df_temu_base = df_temu_base.rename(columns={'index':'唯一标识'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    #
    df_temu_base['站点'] = df_temu_base['site_code'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))
    # sku拆分捆绑、仓标。责任类目匹配
    df_temu_base['nb_sku'] = df_temu_base['sku'].map(extract_correct_string)
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_temu_base['new_sku'] = df_temu_base['nb_sku']
    df_temu_base['new_sku'] = df_temu_base['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    df_temu_base = get_line(df_temu_base)
    df_temu_base = get_main_resp(df_temu_base)

    print(f'有效链接共{len(df_temu_base)}条.')
    # 处理站点
    df_sku = df_temu_base[['product_sku_id','account_id', 'sku', 'country','warehouse']]
    # df_sku = df_sku.assign(column_to_split=df_sku['site_code'].str.split(',')).explode('column_to_split')
    df_sku['country'] = df_sku['country'].replace({'SP':'ES','GB':'UK','NZ':'AU'})
    # df_sku['site'] = df_sku['column_to_split'].replace({'AU':'澳大利亚','US':'美国','DE':'德国','FR':'法国','IT':'意大利',
    #                                                     'ES':'西班牙','UK':'英国','NZ':'新西兰'})
    df_sku['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = chicun_zhongliang(df_sku, 1, conn_ck)
    df_sku.drop(['数量','重量','重量来源','长','宽','高'], axis=1, inplace=True)
    # print('')
    # 获取海外仓运费
    # date_today = '2024-11-16'
    sql = f"""
        SELECT sku, warehouse, best_warehouse_name, country, total_cost, sales_status
        FROM over_sea.dwm_oversea_price_temu
        WHERE date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    df_fee['运费类型'] = '海外仓'
    # df_result = get_useful_fee(df_sku)
    df_result = pd.merge(df_sku, df_fee, how='left', on=['sku','warehouse','country'])
    df_result = df_result.sort_values(by='total_cost', ascending=False).drop_duplicates(
        subset=['product_sku_id','account_id','sku'],keep='first')
    print(f'海外仓调价运费共{len(df_result)}条')
    # # 获取虚拟仓运费
    # print('获取虚拟仓运费数据...')
    # def get_virtual_fee(df_sku):
    #     df_yunfei = pd.DataFrame()
    #     conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器2')
    #     for key, group in df_sku.groupby(['site']):
    #         sql = f"""
    #             SELECT distinct sku,ship_name,total_cost as `虚拟仓总运费`,site
    #             from yibai_temp_hxx.old_freight_interface_temu
    #             where site='{key}' and warehouse_id = 481
    #             order by total_cost asc limit 1 by sku
    #             """
    #         df_yunfei0 = conn_ck.ck_select_to_df(sql)
    #         df_yunfei = df_yunfei.append(df_yunfei0)
    #     df_sku = pd.merge(df_sku, df_yunfei, how='left', on=['sku','site'])
    #
    #     return df_sku
    # #
    # df_virtual_fee = get_virtual_fee(df_sku)
    # df_virtual_fee = df_virtual_fee.sort_values(by='虚拟仓总运费', ascending=True).drop_duplicates(subset=['唯一标识'])
    # print(f'虚拟仓运费数据共{len(df_virtual_fee)}条.')
    # 代销sku匹配
    print('获取代销仓SKU信息...')
    sql = """
        SELECT
            sku, latest_quotation,
            case
                when site = 1 then 'US'
                when site = 2 then 'UK'
                when site = 3 then 'FR'
                when site = 4 then 'IT'
                when site = 5 then 'ES'
                when site = 6 then 'DE'
                when site = 7 then 'AU'
                when site = 9 then 'CA'
            else '其他' end as country
        FROM yibai_prod_base_sync.yibai_prod_sku_consignment_inventory
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_cons = conn_ck.ck_select_to_df(sql)
    df_cons = df_cons.drop_duplicates(subset='sku')
    df_cons['latest_quotation'] = df_cons['latest_quotation'].fillna(0).astype(float)
    # #
    df = pd.merge(df_temu_base, df_sku[['product_sku_id','account_id', 'sku', 'country','成本']],how='left',
                  on=['product_sku_id','account_id', 'sku', 'country'])
    df = pd.merge(df, df_result[['product_sku_id','account_id', 'sku', 'country',
                                           'total_cost','best_warehouse_name','sales_status','运费类型']],
                  how='left', on=['product_sku_id','account_id', 'sku', 'country',])
    # 剔除重复项
    df = df.sort_values(by='total_cost', ascending=False).drop_duplicates(subset=['product_sku_id','account_id','sku'])
    # df = pd.merge(df, df_virtual_fee[['唯一标识','成本','虚拟仓总运费']], how='left', on=['唯一标识'])
    # # df = pd.merge(df, df_cons, how='left', on=['sku'])
    df = pd.merge(df, df_cons, how='left', on=['sku', 'country'])
    df['成本'] = np.where(~df['latest_quotation'].isna(), df['latest_quotation'], df['成本'])
    df['运费类型'] = np.where(~df['latest_quotation'].isna(), '代销品', df['运费类型'])
    # df_rate0 = df_rate0.rename(columns={'country':'column_to_split'})
    df['us_rate'] = df_rate.iloc[0, 2]
    df = pd.merge(df, df_rate0[['country','rate']], how='left', on='country')
    print(df.info())
    print('计算完成，开始存表...')
    # save_df(df, 'TEMU链接订单利润率核算_不含接口', file_type='xlsx')
    #
    print('获取接口运费信息...')
    df_bundle_fee = get_interf_fee(df)
    df_bundle_fee = df_bundle_fee.rename(columns={'shipCountry':'country'})
    # df_bundle_fee = pd.DataFrame(columns=['sku', 'country', 'warehouseName', 'shipName', 'totalCost'])

    # # 临时
    # sql = """
    #     SELECT product_sku_id, account_id, sku, country, warehouseName, shipName, totalCost
    #     FROM over_sea.dwd_temu
    #     WHERE totalCost is not null
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_bundle_fee = conn.read_sql(sql)
    # df = pd.merge(df, df_bundle_fee, how='left', on=['product_sku_id','account_id','sku','country'])

    df = pd.merge(df, df_bundle_fee, how='left', on=['sku','country'])
    df['运费类型'] = np.where((~df['totalCost'].isna())&(df['运费类型'].isna()), '接口', df['运费类型'])
    df['total_cost'] = np.where(df['total_cost'].isna(), df['totalCost'], df['total_cost'])
    df['best_warehouse_name'] = np.where(df['运费类型']=='接口', df['warehouseName'], df['best_warehouse_name'])

    df.drop(['warehouseName','totalCost','shipName','latest_quotation','index','id','item_id',
             'update_time'], axis=1, inplace=True)
    dic = {'主体账号':'main_name','申报价格':'supplier_price','最早出单时间':'min_order_time','最晚出单时间':'max_order_time',
           '站点':'site','一级产品线':'first_line','二级产品线':'secend_line','责任人':'responsible_name',
           '成本':'new_price','运费类型':'shipping_fee_type'}
    df = df.rename(columns=dic)

    col = ['new_price', 'total_cost','supplier_price','freight_subsidy','origin_supplier_price','origin_freight_subsidy']
    df[col] = df[col].fillna(0).astype(float)
    df['online_price_rate'] = 1 - 0.03 - (df['new_price']+df['total_cost'])/(
            df['supplier_price']*df['us_rate']+df['freight_subsidy'])

    # 20250113 补充初始申报价利润率
    df['origin_price_rate'] = 1 - 0.03 - (df['new_price']+df['total_cost'])/(
            df['origin_supplier_price']*df['us_rate']+df['origin_freight_subsidy'])
    # 利润率是否达标
    c1 = ((df['is_same'] == 1) & (df['online_price_rate'] >= 0.28))
    c2 = ((df['is_same'] != 1) & (df['online_price_rate'] >= 0.30))
    df['is_target'] = np.where(c1 | c2, 1, 0)

    df.drop(['nb_sku','new_sku'], axis=1, inplace=True)
    print(df.info())
    #
    # df.to_excel('F://Desktop//TEMU链接订单利润率核算.xlsx', index=0)
    # save_df(df, 'TEMU链接订单利润率核算', file_type='xlsx')
    # write_to_sql(df, 'dwd_temu_listing')
    write_to_ck(df, 'dwd_temu_listing')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'dwd_temu_listing', if_exists='replace')
    return None

def temp_fuc():
    """ 临时   """

    date_today = time.strftime('%Y-%m-%d')
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    # 测算temu申报价格的订单利润率
    print('获取TEMU链接信息...')
    # df_temu_base = get_temu_listing()
    sql = f"""
        SELECT *
        FROM yibai_oversea.yibai_ads_oversea_temu_listing
        WHERE date_id = '{date_today}' 
        LIMIT 10000
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temu_base = conn_ck.ck_select_to_df(sql)
    df_temu_base = df_temu_base.sort_values(by=['supplier_price']).drop_duplicates(
        subset=['account_id', 'product_sku_id', 'sku'])
    # 抽样
    # df_temu_base = df_temu_base.sample(1000)
    print(df_temu_base.info())
    df_temu_base = df_temu_base[(~df_temu_base['sku'].isna()) & ~(df_temu_base['sku'] == '')]
    df_temu_base = df_temu_base.rename(columns={'supplier_price': '申报价格', 'main_name': '主体账号'})
    df_temu_base['sku'] = df_temu_base['sku'].astype(str)

    col = ['stock_number', 'lazada_account_operation_mode', 'create_time', 'account_status',
           'account_operation_mode']
    df_temu_base.drop(col, axis=1, inplace=True)
    order_time = min_max_order_time()
    df_temu_base = df_temu_base.merge(order_time, on=['account_id', 'product_sku_id'], how='left')
    df_temu_base = df_temu_base.reset_index(drop=True).reset_index()
    # df_temu_base = df_temu_base.rename(columns={'index':'唯一标识'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲', 'SE': '欧洲', 'BE': '欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大',
           'MX': '墨西哥'}
    #
    df_temu_base['站点'] = df_temu_base['site_code'].apply(
        lambda x: next((dic[code] for code in x.split(',') if code in dic), None))
    # sku拆分捆绑、仓标。责任类目匹配
    df_temu_base['nb_sku'] = df_temu_base['sku'].map(extract_correct_string)
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_temu_base['new_sku'] = df_temu_base['nb_sku']
    df_temu_base['new_sku'] = df_temu_base['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    print(df_temu_base.info())
    df_temu_base = get_line(df_temu_base)
    df_temu_base = get_main_resp(df_temu_base)
    df_temu_base.to_excel('F://Desktop//df_temu_base_temp.xlsx', index=0)

# df = check_temu_online_price()
##
# print(df[['site_code','站点','country']].drop_duplicates())
##
# 获取运费
def dwm_sku_price():
    """
    销售状态设置
    调价周期设置 *
    价格计算
    """

    # 读取dwm_sku_2
    sql = """
    SELECT *
    FROM over_sea.dwm_sku_info_temu
    WHERE date_id = curdate()
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku = conn.read_sql(sql)
    print(dwm_sku.info())

    # 匹配运费
    df_transport_fee = get_transport_fee()
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='left', on='country')
    col = ['sku', 'warehouse_id', 'ship_name', 'total_cost','shippingCost','ship_fee', 'warehouse',
           'lowest_price', 'platform', 'country','rate']
    df_transport_fee = df_transport_fee[col].drop_duplicates()
    # 销毁价计算
    c1 = 1.2*df_transport_fee['ship_fee']/(1-0.04-0.08-0.1)
    c2 = 1.2*df_transport_fee['ship_fee']/(1-0.04-0.08-0.1-0.12)
    df_transport_fee['lowest_price'] = np.where(df_transport_fee['warehouse']=='加拿大仓', c2, c1)
    # df_transport_fee['lowest_price'] = 1.2*df_transport_fee['ship_fee']/(1-0.04-0.08-0.1)
    df_transport_fee.drop(['shippingCost','ship_fee','warehouse'], axis=1, inplace=True)
    # 独享仓需替换为非独享仓的运费
    sql = """
        SELECT warehouse_id, real_warehouse_id
        FROM over_sea.yibai_warehouse_oversea_temp
        WHERE warehouse_name like '%%独享%%'
    """
    df_warehouse = conn.read_sql(sql)

    dwm_sku['warehouse_id'] = dwm_sku['best_warehouse_id']
    dwm_sku = pd.merge(dwm_sku, df_warehouse, how='left', on=['warehouse_id'])
    dwm_sku['warehouse_id'] = np.where(dwm_sku['real_warehouse_id'].isna(),
                                                dwm_sku['warehouse_id'], dwm_sku['real_warehouse_id'])

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'clearance_level','day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate']
    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_transport_fee, how='left',
                             left_on=['sku', 'warehouse_id'], right_on=['sku', 'warehouse_id'])
    # 无运费数据处理
    dwm_sku_price.drop(['warehouse_id'], axis=1, inplace=True)
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]

    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_sku_price['platform'] = 'TEMU'
    df_platform_fee.drop('country', axis=1, inplace=True)
    dwm_sku_price = pd.merge(dwm_sku_price, df_platform_fee[['platform','ppve','platform_zero',
                    'platform_must_percent']], how='inner', on=['platform'])

    # 数据类型转化
    type_columns = ['new_price', 'total_cost', 'lowest_price', 'ppve', 'platform_zero',
                    'platform_must_percent']
    dwm_sku_price[type_columns] = dwm_sku_price[type_columns].astype('float64').round(4)

    # 净利率的处理
    dwm_sku_price['sku_target_profit'] = np.where(
        (dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']) < dwm_sku_price['lowest_profit'],
        dwm_sku_price['lowest_profit'],
        dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit'])

    # 销售状态处理
    c1 = (dwm_sku_price['sku_target_profit'] == dwm_sku_price['platform_must_percent'])
    c2 = (dwm_sku_price['up_profit_rate'] > 0)
    c3 = (dwm_sku_price['sku_target_profit'] > 0)
    c4 = (dwm_sku_price['sku_target_profit'] <= 0)
    dwm_sku_price['sales_status'] = np.select([c1, c2, c3, c4], ['正常', '回调', '正利润加快动销', '负利润加快动销'])
    dwm_sku_price['date_id'] = time.strftime('%Y-%m-%d')
    dwm_sku_price = dwm_sku_price.drop_duplicates()
    print(dwm_sku_price.info())
    print('SKU信息及调价幅度已获取，准备写入dwm_oversea_price_temu...')
    write_to_sql(dwm_sku_price, 'dwm_oversea_price_temu')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(dwm_sku_price, 'dwm_oversea_price_temu', if_exists='replace')

    return None


## listing销量
# 订单数据
def get_listing_order():
    date_start = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    date_15 = (datetime.date.today() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
    date_7 = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    sql_order = f"""
    select 
        b.platform_order_id,b.order_id as order_id,b.account_id as account_id,c.seller_sku as seller_sku,
        c.quantity,toDate(b.payment_time)as payment_time,c.item_id,b.order_status,c.total_price,b.currency
    from (
        select * 
        from yibai_oms_sync.yibai_oms_order 
        where 
            platform_code ='TEMU' and order_id not like '%RE%'
            and order_id not like '%CJ%' 
            and total_price !=0 and order_type !=4
            and order_status != 80
            and payment_time >= '{date_start}'
    ) b 
    left join (
        select * 
        from yibai_oms_sync.yibai_oms_order_detail 
        where order_id in (
            select order_id from yibai_oms_sync.yibai_oms_order 
            where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4)
    ) c on b.order_id=c.order_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    order = conn_ck.ck_select_to_df(sql_order)
    order.columns = [i.split('.')[-1] for i in order.columns]
    order['payment_time'] = pd.to_datetime(order['payment_time']).dt.strftime('%Y-%m-%d')

    order = order.drop_duplicates(subset=['account_id','seller_sku'])
    order_30 = order.groupby(['account_id','seller_sku']).agg(
        {'quantity':'sum'}).reset_index().rename(columns={'quantity': '30days_sales'})
    order_15 = order[order['payment_time'] >= date_15].groupby(['account_id','seller_sku']).agg(
        {'quantity':'sum'}).reset_index().rename(columns={'quantity': '15days_sales'})
    order_7 = order[order['payment_time'] >= date_7].groupby(['account_id', 'seller_sku']).agg(
        {'quantity': 'sum'}).reset_index().rename(columns={'quantity': '7days_sales'})
    order_info = pd.merge(order_30, order_15, how='left', on=['account_id','seller_sku'])
    order_info = pd.merge(order_info, order_7, how='left', on=['account_id','seller_sku'])
    order_info = order_info.fillna(0)
    c1 = order_info['30days_sales'] > 0
    c2 = order_info['15days_sales'] > 0
    c3 = order_info['7days_sales'] > 0
    order_info['listing_sales_level'] = np.select([c3, c2,c1],[7, 15, 30],0)
    return order_info


# 调价周期设置
def adjust_cycle(df):
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""

    SELECT account_id, product_sku_id, sku, target_profit as target_profit_y, is_adjust, date_id
    FROM yibai_oversea.oversea_temu_listing_all
    WHERE target_price > 0 and toDate(date_id) >= today() - 7 and date_id < '{date_today}'
    ORDER BY date_id DESC

    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df_temp_t = conn_ck.ck_select_to_df(sql)
    print(f'近7天temu调价数据，共{len(df_temp_t)}条')
    # 如果上一次调价周期超过7天，则取最近的调价记录，判断是否调价
    if len(df_temp_t) == 0:
        sql = f"""
        
        SELECT account_id, product_sku_id, sku, target_profit as target_profit_y, is_adjust, date_id
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE target_price > 0 and date_id = (
            SELECT max(date_id) FROM yibai_oversea.oversea_temu_listing_all WHERE date_id < '{date_today}')

        """
        conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        df_temp_t = conn_ck.ck_select_to_df(sql)

    # 匹配前一天的利润率, 及前*天的is_adjust
    temp_columns = ['account_id', 'product_sku_id', 'sku', 'target_profit_y']
    df_temp_last = df_temp_t[df_temp_t['date_id'] != str(datetime.date.today())]
    df_temp_last = df_temp_last[df_temp_last['date_id'] == df_temp_last.date_id.unique().max()]
    print(f'匹配前一次的利润率， 前一次调价数据共{len(df_temp_last)}条。')
    print(df_temp_last.info())
    print(df.info())
    dwm_sku_price_temp = pd.merge(df, df_temp_last[temp_columns], how='left', on=['account_id', 'product_sku_id', 'sku'])
    print(1)
    dwm_sku_price_temp['target_profit_y'] = dwm_sku_price_temp['target_profit_y'].astype(float)
    # 匹配最近7天内的最近降价日期
    df_down_date = df_temp_last[df_temp_last['is_adjust']=='降价'].sort_values(by='date_id', ascending=False)
    df_down_date = df_down_date.drop_duplicates(subset=['account_id', 'product_sku_id', 'sku'])
    if len(df_down_date) == 0:
        df_down_date = pd.DataFrame(columns=['account_id', 'product_sku_id', 'sku', 'date_id'])
    df_down_date = df_down_date.rename(columns={'date_id':'recent_down_date'})
    dwm_sku_price_temp = pd.merge(dwm_sku_price_temp, df_down_date[['account_id', 'product_sku_id', 'sku', 'recent_down_date']],
                                  how='left', on=['account_id', 'product_sku_id', 'sku'])
    print(2)
    # 调价周期设置
    # 实现方式：获取近 * 天的调价状态，如果当前最新的调价状态已出现，则将调价状态置为保持、利润率置为前一日
    is_adjust_7 = df_temp_t.groupby(['account_id', 'product_sku_id', 'sku'])['is_adjust'].apply(
        lambda x: x.str.cat(sep=',')).reset_index()
    is_adjust_7 = is_adjust_7.rename(columns={'is_adjust': 'adjust_record_7'})
    date_3 = (datetime.date.today() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    if len(df_temp_t[df_temp_t['date_id']>=date_3]) == 0:
        # df_temp_t_3 = df_temp_t[df_temp_t['date_id'] == df_temp_t.date_id.unique().max()]
        df_temp_t_3 = pd.DataFrame(columns=['account_id','product_sku_id','sku','is_adjust'])
    else:
        df_temp_t_3 = df_temp_t[df_temp_t['date_id']>=date_3]
    is_adjust_3 = df_temp_t_3.groupby(['account_id', 'product_sku_id', 'sku'])['is_adjust'].apply(
        lambda x: x.str.cat(sep=',')).reset_index()
    is_adjust_3 = is_adjust_3.rename(columns={'is_adjust': 'adjust_record_3'})

    dwm_sku_price_temp = pd.merge(dwm_sku_price_temp, is_adjust_7, how='left',
                                  on=['account_id', 'product_sku_id', 'sku'])
    print(3)
    if len(is_adjust_3) == 0:
        # df_temp_t_3 = df_temp_t[df_temp_t['date_id'] == df_temp_t.date_id.unique().max()]
        is_adjust_3 = pd.DataFrame(columns=['account_id','product_sku_id','sku','adjust_record_3'])
    dwm_sku_price_temp = pd.merge(dwm_sku_price_temp, is_adjust_3, how='left',
                                  on=['account_id', 'product_sku_id', 'sku'])
    c1 = (dwm_sku_price_temp['sales_label'] == '超库龄加快动销') & (dwm_sku_price_temp['clearance_level'] == 210)
    c2 = (dwm_sku_price_temp['sales_label'] == '超库龄加快动销') & (dwm_sku_price_temp['clearance_level'] < 210)
    c3 = (dwm_sku_price_temp['sales_label'] == '正常品低销降价')
    dwm_sku_price_temp['adjust_record'] = np.select([c1, c2, c3], [dwm_sku_price_temp['adjust_record_3'],
                                                                   dwm_sku_price_temp['adjust_record_7'],
                                                                   dwm_sku_price_temp['adjust_record_3']])
    dwm_sku_price_temp.drop(['adjust_record_3','adjust_record_7'], axis=1, inplace=True)
    # split_list = dwm_sku_price_temp['adjust_record'].str.split(',', expand=True).stack()
    dwm_sku_price_temp['adjust_record'] = dwm_sku_price_temp['adjust_record'].fillna(' ').astype(str)
    dwm_sku_price_temp['adjust_record'] = dwm_sku_price_temp['adjust_record'].str.split(', ', expand=True).apply(
        lambda x: [item for item in x])
    dwm_sku_price_temp['is_in'] = dwm_sku_price_temp.apply(
        lambda row: all(item in row['adjust_record'] for item in row['is_adjust'].split()), axis=1)
    c1 = (dwm_sku_price_temp['is_adjust'] == '降价') & (dwm_sku_price_temp['is_in'] == True)
    c2 = (dwm_sku_price_temp['is_adjust'] == '涨价') & (dwm_sku_price_temp['is_in'] == True)
    dwm_sku_price_temp['target_profit'] = np.select([c1, c2], [dwm_sku_price_temp['target_profit_y'],
                                                                    dwm_sku_price_temp['target_profit_y']],
                                                         dwm_sku_price_temp['target_profit'])
    dwm_sku_price_temp['target_profit'] = np.where(dwm_sku_price_temp['target_profit']>=dwm_sku_price_temp['online_profit_rate'],
                                             dwm_sku_price_temp['online_profit_rate'], dwm_sku_price_temp['target_profit'])
    dwm_sku_price_temp['is_adjust'] = np.select([c1, c2], ['保持', '保持'], dwm_sku_price_temp['is_adjust'])
    dwm_sku_price_temp.drop(['is_in'], axis=1, inplace=True)

    return dwm_sku_price_temp
##
def temp_temp():
    """调试"""
    date_today = time.strftime('%Y-%m-%d')
    date_today = '2025-04-03'
    sql = f"""
        SELECT account_id, product_sku_id, sku, online_status, is_adjust,sales_label,clearance_level,
        target_profit, online_profit_rate
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = '{date_today}'
        LIMIT 10000
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df['clearance_level'] = df['clearance_level'].fillna(0).astype(int)
    df = adjust_cycle(df)

    print(df.info())


def get_temu_adjust_listing():
    """
    temu调价链接明细表
    """

    # 获取今日链接数据
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-09-25'
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # 站点链接表select_status(lazada_account_operation_mode) 0未知 1待卖家确认 2已生效 3已作废 4流量竞争力不足 5流量严重异常 6流量异常
    sql = f"""
        SELECT *
        FROM yibai_oversea.yibai_ads_oversea_temu_listing
        WHERE date_id = '{date_today}' and lazada_account_operation_mode in (1,2,4,5,6)
    """
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t = listing_t.sort_values(by=['supplier_price']).drop_duplicates(subset=['account_id','product_sku_id','sku','country'])
    listing_t['country'] = listing_t['country'].replace({'SP':'ES','GB':'UK'})
    print(f'temu链接数量共{len(listing_t)}条.')
    # dwm_sku_price_dtl = dwm_sku_price()
    sql = f"""
        SELECT *
        FROM over_sea.dwm_oversea_price_temu
        WHERE date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku_price_dtl = conn.read_sql(sql)

    col1 = ['account_id','short_name','product_sku_id','site_code','item_id','online_status','sku','added_to_site_time','supplier_price',
            'freight_subsidy','warehouse','country','date_id']
    col2 = ['sku', 'warehouse','country','best_warehouse_name','available_stock','sales_status','overage_level', 'clearance_level',
            'day_sales', 'estimated_sales_days','new_price', 'total_cost','lowest_price','rate','ppve','platform_zero',
            'platform_must_percent','after_profit','sku_target_profit','lowest_profit']
    temu_listing = pd.merge(listing_t[col1], dwm_sku_price_dtl[col2], how='left', on=['sku', 'warehouse', 'country'])
    temu_listing['total_cost'] = temu_listing['total_cost'].fillna(0).astype(float)
    #
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    temu_listing['rate'] = df_rate.iloc[0,2]
    col = ['ppve','platform_zero','new_price','total_cost','rate','supplier_price','freight_subsidy']
    temu_listing[col] = temu_listing[col].fillna(0).astype(float)
    print(temu_listing.info())
    # 20241125 修改temu链接在线利润率的计算公式：1 - 0.03 - (成本+总运费)/(申报价+运费补贴)
    temu_listing['online_profit_rate'] = 1 - temu_listing['ppve'] - temu_listing['platform_zero'] - (
        temu_listing['new_price']+temu_listing['total_cost'])/temu_listing['rate']/(temu_listing['supplier_price'] + (
        temu_listing['freight_subsidy']/temu_listing['rate']))
    temu_listing['online_profit_rate'] = temu_listing['online_profit_rate'].round(4)
    #
    temu_listing['sales_label'] = np.where(temu_listing['after_profit'] < 0, '超库龄加快动销','')

    temu_listing['listing_days'] = np.where(temu_listing['added_to_site_time'] > '2024-01-01',
                                            (datetime.datetime.today() - pd.to_datetime(temu_listing['added_to_site_time'])).dt.days,
                                            -1)
    print('获取temu链接销量数据...')
    temu_order = get_listing_order()
    temu_order = temu_order.rename(columns={'seller_sku':'product_sku_id'})
    temu_listing = pd.merge(temu_listing, temu_order[['account_id','product_sku_id','listing_sales_level']],
                            how='left', on=['account_id','product_sku_id'])
    # sku维度降价幅度二次修正
    # 7天有销量的最多降5%；listing利润率高于sku利润率的最多降3%
    c1 = (temu_listing['after_profit'] < -0.05) & (temu_listing['listing_sales_level'].isin([7]))
    temu_listing['sku_section'] = np.where(c1, -0.05, temu_listing['after_profit'])

    c1 = (temu_listing['sku_section'] < -0.03) & (temu_listing['sku_target_profit'] < temu_listing['online_profit_rate'])
    temu_listing['sku_section'] = np.where(c1, -0.03, temu_listing['sku_section'])

    # 正常品低销降价
    c1 = (temu_listing['overage_level'] >= 90) & (temu_listing['listing_days'] >= 30) & (~temu_listing['listing_sales_level'].isin([15, 7]))
    c2 = (temu_listing['overage_level'] >= 90) & (temu_listing['listing_days'] >= 30) & (~temu_listing['listing_sales_level'].isin([7]))
    c3 = (temu_listing['overage_level'] >= 90) & (temu_listing['listing_days'] >= 15) & (~temu_listing['listing_sales_level'].isin([7]))
    c4 = (temu_listing['overage_level'] < 90) & (temu_listing['listing_days'] >= 30) & (~temu_listing['listing_sales_level'].isin([30,15,7]))
    c5 = (temu_listing['overage_level'] < 90) & (temu_listing['listing_days'] >= 30) & (~temu_listing['listing_sales_level'].isin([15,7]))
    c6 = (temu_listing['overage_level'] < 90) & (temu_listing['listing_days'] >= 15) & (~temu_listing['listing_sales_level'].isin([15,7]))
    temu_listing['listing_section'] = np.select([c1,c2,c3,c4,c5,c6], [-0.02,-0.01,-0.01,-0.02,-0.01,-0.01],0)
    temu_listing['listing_lowest'] = np.select([c1,c2,c3,c4,c5,c6], [0.05,0.05,0.08,0.08,0.11,0.15],0)

    temu_listing['sales_label'] = np.where((temu_listing['sales_label']=='') & (temu_listing['listing_section'] < 0),
                                           '正常品低销降价', temu_listing['sales_label'])
    #
    c1 = (temu_listing['sales_label']=='超库龄加快动销') & ((temu_listing['online_profit_rate'] +
                                                             temu_listing['sku_section']) <= temu_listing['lowest_profit'])
    c2 = (temu_listing['sales_label']=='超库龄加快动销') & ((temu_listing['online_profit_rate'] +
                                                             temu_listing['sku_section']) > temu_listing['lowest_profit'])
    c3 = (temu_listing['sales_label']=='正常品低销降价') & ((temu_listing['online_profit_rate'] +
                                                             temu_listing['listing_section']) <= temu_listing['listing_lowest'])
    c4 = (temu_listing['sales_label']=='正常品低销降价') & ((temu_listing['online_profit_rate'] +
                                                             temu_listing['listing_section']) > temu_listing['listing_lowest'])
    temu_listing['target_profit'] = np.select([c1,c2,c3,c4],[temu_listing['lowest_profit'],
                                                             temu_listing['online_profit_rate'] + temu_listing['sku_section'],
                                                             temu_listing['listing_lowest'],
                                                             temu_listing['online_profit_rate'] + temu_listing['listing_section']
                                                             ], np.nan)
    # TEMU只能降价，不能涨价
    temu_listing['target_profit'] = np.where(temu_listing['target_profit']>=temu_listing['online_profit_rate'],
                                             temu_listing['online_profit_rate'], temu_listing['target_profit'])
    c1 = (temu_listing['target_profit'] - temu_listing['online_profit_rate']) < -0.001
    temu_listing['is_adjust'] = np.where(c1, '降价', '保持')
    print(temu_listing.info())
    # 调价周期设置
    # temu_listing = adjust_cycle(temu_listing)
    #
    # 价格计算.(价格不能低于销毁价，也不能高于当前链接在线价)
    temu_listing['target_price'] = (temu_listing['new_price']+temu_listing['total_cost']-temu_listing['freight_subsidy'])/(
            1-temu_listing['ppve']-temu_listing['platform_zero']-temu_listing['target_profit'])/temu_listing['rate']
    temu_listing['target_price'] = np.where((temu_listing['target_price']*temu_listing['rate'])<temu_listing['lowest_price'],
                                            temu_listing['lowest_price']/temu_listing['rate'], temu_listing['target_price'])
    temu_listing['target_price'] = np.where(temu_listing['is_adjust']=='保持', temu_listing['supplier_price'], temu_listing['target_price'])
    temu_listing['target_price'] = np.where(temu_listing['target_price'] > temu_listing['supplier_price'],
                                            temu_listing['supplier_price'], temu_listing['target_price'])
    temu_listing['target_price'] = temu_listing['target_price'].round(2)
    temu_listing[['online_profit_rate','target_profit']] = temu_listing[['online_profit_rate','target_profit']].round(4)
    #
    temu_listing[['product_sku_id','item_id']] = temu_listing[['product_sku_id','item_id']].astype(str)
    #
    print('temu调价数据计算完成，数据存入ck.')
    # conn_ck.ck_insert(temu_listing, 'oversea_temu_listing_all', if_exist='append')
    write_to_ck(temu_listing, 'oversea_temu_listing_all', datebase='yibai_oversea')

    return None


## 价格幅度变化
def zj_qujian(df, col='价格涨降幅度'):
    df['涨降幅区间'] = 'L.1~'
    df.loc[(df[col] < -1), '涨降幅区间'] = 'A.-1~'
    df.loc[(df[col] >= -1) & (df[col] < -0.5), '涨降幅区间'] = 'B.-1~-0.5'
    df.loc[(df[col] >= -0.5) & (df[col] < -0.2), '涨降幅区间'] = 'C.-0.5~-0.2'
    df.loc[(df[col] >= -0.2) & (df[col] < -0.1), '涨降幅区间'] = 'D.-0.2~-0.1'
    df.loc[(df[col] >= -0.1) & (df[col] < -0.05), '涨降幅区间'] = 'E.-0.1~-0.05'
    df.loc[(df[col] >= -0.05) & (df[col] < 0), '涨降幅区间'] = 'F.-0.05~0'
    df.loc[(df[col] == 0), '涨降幅区间'] = 'G.0'
    df.loc[(df[col] > 0) & (df[col] < 0.05), '涨降幅区间'] = 'H.0~0.05'
    df.loc[(df[col] > 0.05) & (df[col] <= 0.1), '涨降幅区间'] = 'I.0.05~0.1'
    df.loc[(df[col] > 0.1) & (df[col] <= 0.2), '涨降幅区间'] = 'J.0.1~0.2'
    df.loc[(df[col] > 0.2) & (df[col] <= 0.5), '涨降幅区间'] = 'K.0.2~0.5'
    df.loc[(df[col] > 0.5) & (df[col] <= 1), '涨降幅区间'] = 'L.0.5~1'
    return df

def lv_qujian(df, col='利润率涨降幅度', new_col='利润率涨降幅度分段'):

    df[new_col] = 'k.[0.3,∞)'
    df.loc[df[col] < -0.3, new_col] = 'a.(-∞，-0.3)'
    df.loc[(df[col] >= -0.3) & (df[col] < -0.151), new_col] = 'b.[-0.3,-0.15)'
    df.loc[(df[col] >= -0.151) & (df[col] < -0.1), new_col] = 'c.[-0.15,-0.1)'
    df.loc[(df[col] >= -0.1) & (df[col] < -0.05), new_col] = 'd.[-0.1,-0.05)'
    df.loc[(df[col] >= -0.05) & (df[col] < 0), new_col] = 'e.[-0.05,0)'
    df.loc[(df[col] == 0), new_col] = 'f.0'
    df.loc[(df[col] > 0) & (df[col] < 0.05), new_col] = 'g.(0,0.05)'
    df.loc[(df[col] >= 0.05) & (df[col] < 0.1), new_col] = 'h.[0.05,0.1)'
    df.loc[(df[col] >= 0.1) & (df[col] < 0.15), new_col] = 'i.[0.1,0.15)'
    df.loc[(df[col] >= 0.15) & (df[col] < 0.3), new_col] = 'j.[0.15,0.3)'
    return df

def save_df(df, data_name, file_type='xlsx', index=False, program_name='临时文件'):
    cur_path = os.path.abspath(os.path.dirname(__file__))
    root_path = cur_path[:cur_path.find("yibai-price-strategy\\") + len("yibai-price-strategy\\")]
    date_save = datetime.date.today()
    if file_type == 'csv':
        df.to_csv(f'{root_path}/data/{date_save}/{program_name}/{data_name}.csv', index=index)
    elif file_type == 'xlsx':
        df.to_excel(f'{root_path}/data/{date_save}/{program_name}/{data_name}.xlsx', index=index)
def make_path(program_name):
    date_now = datetime.date.today()
    cur_path = os.path.abspath(os.path.dirname(__file__))
    root_path = cur_path[:cur_path.find("yibai-price-strategy\\") + len("yibai-price-strategy\\")]
    if not os.path.exists(f"{root_path}/data/{date_now}"):
        os.mkdir(f"{root_path}/data/{date_now}")
        print('data source path created!')
    else:
        pass
    if not os.path.exists(f"{root_path}/data/{date_now}/{program_name}"):
        os.mkdir(f"{root_path}/data/{date_now}/{program_name}")
        print('data program path created!')
    else:
        pass
    if not os.path.exists(f"{root_path}/docs/logs/{date_now}"):
        os.mkdir(f"{root_path}/docs/logs/{date_now}")
        print('log source path created!')
    else:
        pass
    if not os.path.exists(f"{root_path}/docs/logs/{date_now}/{program_name}"):
        os.mkdir(f"{root_path}/docs/logs/{date_now}/{program_name}")
        print('log program path created!')
    else:
        pass


def temu_check():
    """
    temu调价数据检查
    """
    program_name = 'TEMU调价数据检查'
    make_path(program_name)
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            account_id, product_sku_id, online_status, sku, added_to_site_time, warehouse, country, best_warehouse_name,
            available_stock, sales_status, overage_level, supplier_price, freight_subsidy, online_profit_rate,new_price,total_cost,
        target_profit, target_price, is_adjust
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = '{date_today}' and online_status = '已发布到站点' and sku != '' and available_stock > 0
        -- LIMIT 10000
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    temu_listing = conn_ck.ck_select_to_df(sql)
    print(f'调价数据获取完成，共{len(temu_listing)}条。')
    temu_account = get_temu_account()
    temu_listing = pd.merge(temu_listing, temu_account[['account_id','main_name','account_s_name']], how='left', on=['account_id'])
    temu_listing['价格涨降幅度'] = np.where(temu_listing['target_price'] == temu_listing['online_profit_rate'], 0,
                                            (temu_listing['target_price'] - temu_listing['supplier_price']) / temu_listing['supplier_price'])
    temu_listing['利润率涨降幅度'] = temu_listing['target_profit'] - temu_listing['online_profit_rate']

    temu_listing = zj_qujian(temu_listing, col='价格涨降幅度')
    temu_listing = lv_qujian(temu_listing, col='利润率涨降幅度', new_col='利润率涨降幅度分段')

    save_df(temu_listing, f'TEMU调价数据检查{date_today}', file_type='xlsx',program_name='TEMU调价数据检查')
    return temu_listing


##

if __name__ == '__main__':
    # dwm_oversea_sku()
    # main_temu_listing()
    # dwm_sku_price()
    # get_temu_adjust_listing()
    # get_stock()
    # temp_fuc()
    # temp_temp()
    # check_temu_online_price()
    # temp_fuc()
    # df.to_excel('F://Desktop//df_check_test.xlsx', index=0)
    # df_stock = get_stock()
    dwm_oversea_sku_tt()
    # dwm_oversea_sku()
    # b = get_transport_fee()
    # get_temu_listing()
    # main_temu_listing()
    # df = get_temu_account()
    # df.to_excel('F://Desktop//df_account.xlsx', index=0)
    # temu_listing_temp()
    # temu_check()
    # temp_temp()
    # get_temu_listing()
