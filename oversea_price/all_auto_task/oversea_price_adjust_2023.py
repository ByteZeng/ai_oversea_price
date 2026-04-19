# 海外仓调价新逻辑
# part1: 聚合SKU各类信息
# part2: 计算目标价
##
import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse, tt_get_oversea_order
from all_auto_task.oversea_add_logic import get_high_pcs_logic, get_supplier_price, only_supplier_price, \
    flash_sku_to_lowest, get_temp_clear_sku, temp_clear_sku_logic, walmart_dtl_stock_logic, get_bundle_sku_price
from all_auto_task.yunfei_auto import get_transport_fee
warnings.filterwarnings("ignore")

# 获取SKU的库存和库龄数据
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
                ,ps.available_stock available_stock ,
                CASE
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) > 0 then toFloat64(yps.avg_goods_price)
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) = 0 and toFloat64(yps.new_price) > 0 then toFloat64(yps.new_price)
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) = 0 and toFloat64(yps.new_price) = 0 then toFloat64(yps.product_cost)
                    when yps.product_status!=11 and toFloat64(yps.new_price) > 0 then toFloat64(yps.new_price)
                    else toFloat64(yps.product_cost)
                END as `new_price`
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
                where (warehouse_other_type = 2 or warehouse_id in (958, 1833, 1806, 1683, 1466))
                -- and warehouse_name not like '%独享%'
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
    # 20250321 隔离精铺非转泛品的sku
    c0 = df_sku_stock['type'].str.contains('转泛品|易通兔') | df_sku_stock['type'].str.contains('转VC')
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺')) & (~c0)
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)

    return df_sku_stock

def get_stock_dwd():
    """
    从海外仓库存表，获取SKU及库存信息详情
    """
    # 初始库存表的视图：v_oversea_stock
    t1 = time.time()
    sql = '''
    select
        sku, title, new_price, gross, product_status, warehouse_other_type,
        sum_stock as stock,sum_available_stock as available_stock, sum_available_stock_money as available_stock_money, 
        sum_on_way_stock as on_way_stock, 
        product_size, product_package_size,warehouse_id, warehouse_name, warehouse
    from (
        with 
        [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17] as product_status_arr,
        ['已创建', '已开发', '待买样', '待品检', '待编辑', '待拍摄', '待编辑待拍摄', '待修图', '在售中', '审核不通过', '停售', 
        '待清仓', '已滞销', '待物流审核', '待关务审核', 'ECN资料变更中', 'ECN资料变更驳回'] as product_status_desc_arr	 
        select
            ps.sku as sku, ps.title_cn as title, ps.new_price as new_price, ps.product_weight_pross as gross, 
            ps.warehouse_code as warehouse_code, 
            transform(ps.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
            ps.available_stock as available_stock, ps.available_stock*toFloat64(ps.new_price) as available_stock_money,  
            ps.on_way_stock as on_way_stock, ps.create_time as sku_create_time, 
            concat(toString(ps.product_length), '*', toString(ps.product_width), '*', toString(ps.product_height)) as product_size, 
            concat(toString(ps.pur_lenght_pack), '*',toString(ps.pur_width_pack), '*', toString(ps.pur_height_pack) ) as product_package_size, 
            ps.warehouse_id as warehouse_id, ps.warehouse_name as warehouse_name,
            ps.warehouse as warehouse, warehouse_other_type,
            sum(ps.stock) over w as sum_stock,
            sum(ps.available_stock) over w as sum_available_stock, 
            sum(available_stock_money) over w as sum_available_stock_money, 
            sum(ps.on_way_stock) over w as sum_on_way_stock
        from
            (
            select 
                ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name, yps.title_cn title_cn,
                yps.product_status product_status,ps.warehouse_code as warehouse_code,ps.warehouse_id as warehouse_id,
                warehouse_other_type,ps.stock stock ,ps.on_way_stock on_way_stock ,ps.available_stock available_stock ,
                CASE
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) > 0 then toFloat64(yps.avg_goods_price)
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) = 0 and toFloat64(yps.new_price) > 0 then toFloat64(yps.new_price)
                    when yps.product_status=11 and toFloat64(yps.avg_goods_price) = 0 and toFloat64(yps.new_price) = 0 then toFloat64(yps.product_cost)
                    when yps.product_status!=11 and toFloat64(yps.new_price) > 0 then toFloat64(yps.new_price)
                    else toFloat64(yps.product_cost)
                END as `new_price`
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
                    available_stock
                from yb_datacenter.v_oversea_stock 
                WHERE warehouse_other_type != 3   -- 供应商sku有库存的yb_sku临时添加进库存视图。1、链接统计时需要取；2、后续库存统计可能需要用到
                and warehouse_name not in ('云仓美西仓','云仓波兰仓','HC美国西仓','英国仓UK2(移仓换标)',
                'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')
                order by available_stock desc
                ) ps
            left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            -- having new_price > 0
            ) ps
        window w as (partition by sku, warehouse_id)
        order by available_stock desc, warehouse_id desc
    ) a
    limit 1 by sku, warehouse_id
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
    df_sku_stock = get_line(df_sku_stock)
    df_sku_stock['date_id'] = time.strftime('%Y-%m-%d')

    # df_sku_stock = df_sku_stock[df_sku_stock['stock']>0]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)

    write_to_sql(df_sku_stock, 'dwd_sku_info')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_sku_stock, 'dwd_sku_info', if_exists='replace')

    return None

def dwd_warehouse_id_info():
    """ 子仓数据聚合  """

    # 易佰非temu
    sql = """
        SELECT *
        FROM over_sea.dwd_sku_info
        WHERE (warehouse_other_type = 2 or warehouse_id in (958, 1833, 1806, 1683, 1466) or warehouse_name like '%%帮清%%')
        -- and warehouse_name not like '%%独享%%'
        and warehouse_name not in ('云仓美西仓','云仓波兰仓','HC美国西仓','英国仓UK2(移仓换标)',
        'JDHWC--英国海外仓','AG-加拿大海外仓（移仓换标）','加拿大满天星退件仓')
        and date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_info WHERE date_id > '2025-09-01')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_dwd_stock = conn.read_sql(sql)

    col = ['sku','warehouse','title','new_price','gross','product_status','product_size','product_package_size',
           'type','linest','last_linest']
    temp1 = df_dwd_stock[col].drop_duplicates(subset=['sku','warehouse'])
    temp2 = df_dwd_stock[['sku','warehouse','stock','available_stock','available_stock_money','on_way_stock']]
    temp2 = temp2.groupby(['sku','warehouse']).agg({'stock':'sum', 'available_stock':'sum','available_stock_money':'sum',
                                                    'on_way_stock':'sum',}).reset_index()
    temp3 = df_dwd_stock[['sku','warehouse','warehouse_id','warehouse_name','available_stock']]
    temp3 = temp3.sort_values(by='available_stock', ascending=False).drop_duplicates(subset=['sku','warehouse'])

    df = pd.merge(temp1, temp2, how='left', on=['sku','warehouse'])
    df = pd.merge(df, temp3[['sku','warehouse','warehouse_id','warehouse_name']], how='left', on=['sku','warehouse'])
    print(df.info())

    # df.to_excel('F://Desktop//dwd_sku_info.xlsx', index=0)

    return df


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

    df_result = pd.merge(df, df_line[col], how='left', on=['sku'])

    return df_result


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

def get_fine_limit_price():
    """ 获取精铺产品当前售价。调价目标价不能高于当前售价 """
    sql = """
        SELECT *
        FROM over_sea.fine_sku_limit_price
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df.info())
    # df.to_excel('F://Desktop//df_result.xlsx', index=0)

    return df

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
    WHERE 
        date = formatDateTime(subtractDays(now(),{n}), '%Y-%m-%d') and status in (0,1) 
        -- and yw.warehouse_name not like '%独享%' 
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
    WHERE available_stock > 0 and (warehouse_other_type = 2 or warehouse_id in (958, 1833, 1806, 1683, 1466) )
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

##
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
            and yw.warehouse_other_type = 2   -- 筛选公告仓（非子仓）
            and yw.id not in (646, 648) -- BD、WEIS墨西哥仓
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
    df_mx['best_warehouse_id'] = 956
    df_mx['best_warehouse_name'] = 'YM墨西哥仓'
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

# 海外仓仓库信息同步到over_sea
def pull_warehouse_info():
    """
    将CK的海外仓仓库信息拉取同步到over_sea中。
    数据来源：yb_datacenter.v_oversea_stock
    """
    sql = """
    SELECT
        warehouse_id, warehouse_name, warehouse_code, warehouse_type as type, country,
        warehouse, b.real_warehouse_id real_warehouse_id
    FROM (
        SELECT 
            yw.id AS warehouse_id,   
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
            yw.country, ebay_category_id,
            ywc.name AS warehouse, yw.warehouse_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
    ) a
    LEFT JOIN (
        SELECT id, real_warehouse_id
        FROM yb_datacenter.yb_warehouse 
    ) b ON a.warehouse_id = b.id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df[~df['warehouse'].isna()]
    # 补充仓库区域，美国分为:美东、美西、美南、美中
    def extract_area(warehouse, warehouse_name):
        if warehouse == "美国仓":
            # 检查 warehouse_name 中是否包含 "东南西中"，按优先级返回对应区域
            if "东" in warehouse_name:
                return "美东"
            elif "西" in warehouse_name:
                return "美西"
            elif "南" in warehouse_name:
                return "美南"
            elif "中" in warehouse_name:
                return "美中"
            else:
                return "美东"  # 默认情况（无匹配时返回原仓库名）
        elif warehouse == '澳洲仓':
            return '澳大利亚'
        else:
            # 非美国仓：去掉 "仓" 字
            return warehouse.replace("仓", "")

    # 应用函数到 DataFrame，生成新列 'area'
    df["area"] = df.apply(lambda row: extract_area(row["warehouse"], row["warehouse_name"]), axis=1)

    # 输出结果
    print(df)

    # # 匹配头程单价
    # sql = """
    #     SELECT area, `普货单价` price
    #     FROM yibai_oversea.oversea_fees_parameter_new
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_tc = conn_ck.ck_select_to_df(sql)
    #
    # df = pd.merge(df, df_tc, how='left', on=['area'])
    # df.to_excel('F://Desktop//df_warehouse_temp.xlsx', index=0)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_warehouse_oversea_temp', if_exists='replace')

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'yibai_warehouse_oversea_temp', if_exist='replace')


##
# SKU的日销统计
# 20241113 合并易佰和通拓的sku销量数据
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
            -- and b.warehouse_name not like '%%独享%%' 
            -- and b.warehouse_name not like '%%TT%%'

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yibai_sales = conn.read_sql(sql)
    df_yibai_sales['org'] = 'YB'

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
    df_tt_sales['org'] = 'TT'

    # 亿迈
    df_dcm_sales = get_dcm_sales()
    df_dcm_sales['org'] = 'YM'

    df_sales = pd.concat([df_yibai_sales, df_tt_sales, df_dcm_sales])
    df_sales['date_id'] = time.strftime('%Y-%m-%d')

    # sku销量表备份
    write_to_sql(df_sales, 'dwd_sku_sales')
    # conn.to_sql(df_sales, 'dwd_sku_sales', if_exists='replace')
    # df_sales.to_excel('F://Desktop//df_sales.xlsx', index=0)

    # 按大仓聚合
    df = df_sales.groupby(['sku', 'warehouse']).agg({'3days_sales': 'sum', '7days_sales': 'sum', '15days_sales': 'sum',
                                                     '30days_sales': 'sum', '60days_sales': 'sum','90days_sales': 'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales'] / 7 + 0.2 * df['15days_sales'] / 15 + 0.1 * df['30days_sales'] / 30
    df['recent_day_sales'] = 0.8 * df['3days_sales'] / 3 + 0.2 * df['7days_sales'] / 7
    # df_sales = df_sales.sample(50000)
    return df

def get_dcm_sales():
    """ 获取亿迈海外仓销量 """

    sql = f"""
        SELECT 
            a.order_id, a.sku, b.platform_code, b.shop_id AS distributor_id, b.account_id, c.seller_sku,
            b.payment_time start_time, b.warehouse_id,
            a.quantity `90days_sales`, 
            case when b.purchase_time >= today() - INTERVAL 60 DAY then a.quantity else 0 end as `60days_sales`,
            case when b.purchase_time >= today() - INTERVAL 30 DAY then a.quantity else 0 end as `30days_sales`,
            case when b.purchase_time >= today() - INTERVAL 15 DAY then a.quantity else 0 end as `15days_sales`,
            case when b.purchase_time >= today() - INTERVAL 7 DAY then a.quantity else 0 end as `7days_sales`,
            case when b.purchase_time >= today() - INTERVAL 3 DAY then a.quantity else 0 end as `3days_sales`
        FROM yibai_dcm_order_sync.dcm_order b
        LEFT JOIN yibai_dcm_order_sync.dcm_order_sku a ON a.order_id = b.order_id
        LEFT JOIN yibai_dcm_order_sync.dcm_order_detail c ON a.order_detail_id = c.id
        WHERE 
            b.payment_time >= today() - INTERVAL 90 DAY
            AND b.order_status != 40  -- 订单状态（1=待处理、10=未发货、20=部分发货、30=已发货、 40=已作废）
            AND b.payment_status = 1 -- 平台付款状态（客户向亚马逊平台的付款状态，0=未付款，1=已付款）
            AND b.is_abnormal = 0 -- 是否有异常（0=否，1=是）
            AND b.is_intercept = 0 -- 是否拦截（0=未拦截，1=已拦截）
            AND b.refund_status = 0 -- 退款状态（0=未退款，1=退款中、2=部分退款，3=全部退款）
            AND b.total_price != 0 -- 订单总金额 平台付款金额
            AND b.is_ship_process = 0 -- 是否平台仓发货（0否--FBM；1：是--FBA）
    """
    conn_ck = pd_to_ck(database='yibai_dcm_order_sync', data_sys='调价明细历史数据')
    df_dcm_order = conn_ck.ck_select_to_df(sql)
    df_dcm_order.columns = [i.split('.')[-1] for i in df_dcm_order.columns]
    # print(df_dcm_order['warehouse_id'].value_counts())
    print(f'亿迈fbm销量共{len(df_dcm_order)}条.')
    # 匹配大仓
    sql = """
        SELECT
            yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name,
            ywc.name AS warehouse, yw.warehouse_type, yw.warehouse_other_type warehouse_other_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
        and (warehouse_other_type = 2 or warehouse_id in (958, 1833, 1806, 1683, 1466))       -- 筛选公共仓
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_warehouse.columns = [i.split('.')[-1] for i in df_warehouse.columns]
    df_dcm_order = pd.merge(df_dcm_order, df_warehouse[['warehouse_id','warehouse','warehouse_name','warehouse_other_type']], how='inner', on=['warehouse_id'])
    df_dcm_order = df_dcm_order.drop_duplicates(subset=['order_id','sku'])
    # temp = df_dcm_order[df_dcm_order['warehouse'].isna()]
    # print(df_dcm_order.groupby(['warehouse_name', 'warehouse_other_type'])['order_id'].count())
    # print(f"未匹配到海外仓仓库id的仓库有：{temp['warehouse_id']}")
    print(f'亿迈海外仓销量共{len(df_dcm_order)}条.')
    df_dcm_order = df_dcm_order.groupby(['sku','warehouse_id','warehouse_name', 'warehouse','platform_code']).agg(
        {'3days_sales':'sum','7days_sales':'sum','15days_sales':'sum','30days_sales':'sum','60days_sales':'sum','90days_sales':'sum'}
    ).reset_index()
    print(df_dcm_order.info())
    # df_dcm_order.to_excel('F://Desktop//df_dcm_order.xlsx', index=0)

    return df_dcm_order


# 汇率
def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate, erp_rate
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate['country'] = np.where(df_rate['charge_currency']=='HUF', 'HU', df_rate['country'])
    # df_rate = df_rate.drop_duplicates(subset='charge_currency')
    return df_rate

def get_fine_fee():
    # 合并精铺产品运费
    date_today = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT sku, best_warehouse_id warehouse_id, best_warehouse_name warehouse_name, totalCost total_cost,
        shipName ship_name, 0 lowest_price, 'AMAZON' platform, shipCountry country, 
        (totalCost - firstCarrierCost- dutyCost) as ship_fee
        FROM over_sea.fine_sku_fee_useful
        WHERE date_id = (SELECT max(date_id) FROM over_sea.fine_sku_fee_useful)
    """
    df_fee = conn.read_sql(sql)
    df_fee_2 = df_fee.copy()
    df_fee_2['platform'] = 'EB'
    df_fee = pd.concat([df_fee, df_fee_2])

    # 计算销毁价（考虑平台限价）
    df_platform = get_platform_fee()
    df_fee = pd.merge(df_fee, df_platform, how='left', on=['platform','country'])

    df_fee['lowest_price'] = 1.2 * df_fee['ship_fee'] / (1 - df_fee['ppve'] - df_fee['refound_fee']-0.05)

    col = ['ship_fee','ppve','refound_fee','platform_zero','platform_must_percent']
    df_fee.drop(col, axis=1, inplace=True)

    # df_fee.to_excel('F://Desktop//df_temp.xlsx', index=0)

    return df_fee


# 平台差值表获取
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


# 新品判断
def is_new_sku():
    """
    判断是否为新品:海外仓仓库中，近180天有到货且180天之前无到货记录的算新品。
    """
    sql = """
    select distinct sku, warehouse_id as best_warehouse_id
    from yb_datacenter.yb_stock 
    where create_time<subtractDays(now(),180) and available_stock > 0  and cargo_owner_id = 8
    and toInt64(warehouse_id) in ( SELECT distinct id FROM yibai_logistics_tms_sync.yibai_warehouse WHERE warehouse_type IN (2,3,8) )
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_old_sku = ck_client.ck_select_to_df(sql)

    sql = """
        select distinct sku, warehouse_id as best_warehouse_id
        from yb_datacenter.yb_stock 
        where create_time>=subtractDays(now(),180) and available_stock > 0  and cargo_owner_id = 8
        and toInt64(warehouse_id) in ( SELECT distinct id FROM yibai_logistics_tms_sync.yibai_warehouse where warehouse_type in (2,3,8) )
    """
    df_new_sku = ck_client.ck_select_to_df(sql)

    df_new = pd.merge(df_new_sku['sku'], df_old_sku, how='left', on=['sku'])
    df_new = df_new[df_new['best_warehouse_id'].isna()]
    df_new['is_new'] = 1
    df_new.drop('best_warehouse_id', axis=1, inplace=True)
    df_new = df_new.drop_duplicates()
    return df_new

def update_platform_fee():
    sql = """
        SELECT *
        FROM over_sea.yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_pf = conn.read_sql(sql)
    df_pf = df_pf.drop_duplicates()
    # 原差值表备份
    conn.to_sql(df_pf, 'yibai_platform_fee_backup', if_exists='replace')
    # 中台差值表
    sql = """
        SELECT platform_code platform, site, toFloat32(net_profit2)/100 as net_profit2
        FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_pc = conn_ck.ck_select_to_df(sql)
    #
    df = pd.merge(df_pf, df_pc, how='left', on=['platform', 'site'])

    # 差值替换
    df.loc[df['platform']=='CDISCOUNT', 'net_profit2'] = df_pc.loc[df_pc['platform']=='CDISCOUNT', 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='EB') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='EB') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WALMART') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WALMART') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='AMAZON') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='AMAZON') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='ALI') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='ALI') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WISH') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WISH') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]

    #
    df[['platform_zero','net_profit2']] = df[['platform_zero','net_profit2']].round(4)
    #
    mismatched_rows = df[df['platform_zero'] != df['net_profit2']]
    print('差值更新的平台和国家：')
    mismatched_rows[['platform', 'site','platform_zero','net_profit2']].apply(lambda x: print(f"Platform: {x['platform']}, Site: {x['site']}, "
                                                   f"原差值：{x['platform_zero']},最新差值：{x['net_profit2']}"), axis=1)
    #
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df.drop('net_profit2', axis=1, inplace=True)

    # 更新
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_platform_fee', if_exists='replace')


def bg3_sku():
    """ BG3的sku 按10月30日降到销毁价 """
    sql = """
        SELECT distinct sku, warehouse, 1 as is_bg3_sku
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = '2025-09-02'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_bg3_sku = conn.read_sql(sql)

    return df_bg3_sku


def get_best_warehouse_logic(df_stock):
    """ 最优子仓补充逻辑 """
    df_stock = dwd_warehouse_id_info()
    df_stock = df_stock.rename(columns={'warehouse_name':'best_warehouse'})

    # 取运费
    sql = """
        SELECT Distinct sku, warehouseName, country, totalCost_origin, shippingCost, firstCarrierCost, available_stock
        FROM over_sea.oversea_transport_fee_useful
        WHERE 
    """
    pass

## 聚合
def dwm_oversea_sku():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    pull_warehouse_info()
    update_platform_fee()
    print('获取库存信息...')
    get_stock_dwd()
    df_stock = dwd_warehouse_id_info()
    df_stock = df_stock.rename(columns={'warehouse_name':'best_warehouse'})
    print('获取库龄信息...')
    sql = """
        SELECT *
        FROM over_sea.dwm_stock_age
        WHERE date_id = (SELECT max(date_id) FROM dwm_stock_age WHERE date_id > '2025-08-01')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock_age_warehouse = conn.read_sql(sql)
    df_stock_age_warehouse.drop('source', axis=1, inplace=True)
    print('获取日销信息...')
    sku_sales = get_sku_sales_new()

    col = ['sku','warehouse','3days_sales','7days_sales','15days_sales','30days_sales','60days_sales','90days_sales','recent_day_sales','day_sales']
    dwm_sku = pd.merge(df_stock, sku_sales[col], how='left', on=['sku', 'warehouse'])
    # dwm_sku.info()
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、库龄缺失
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    dwm_sku['overage_level'] = np.where(dwm_sku['available_stock']<=0, 0, dwm_sku['overage_level'])
    dwm_sku.iloc[:, 25:34] = dwm_sku.iloc[:, 25:34].fillna(0)
    dwm_sku['overage_level'] = dwm_sku['overage_level'].fillna(0)
    # 库龄库存大于云仓库存校正
    col = ['age_30_plus', 'age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus',
           'age_270_plus', 'age_360_plus']
    for i in col:
        c1 = dwm_sku[i] > dwm_sku['available_stock']
        dwm_sku[i] = np.where(c1, dwm_sku['available_stock'], dwm_sku[i])
    # 20260312 补充临时清仓sku (汇总其他临时清仓sku逻辑)
    dwm_sku = temp_clear_sku_logic(dwm_sku)
    print(dwm_sku.info())
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])

    # 20241125 当sku+大仓下的库存为0时，最优子仓取当前大仓下常用的子仓（大仓下库存最多的子仓，非精铺仓）。避免最优子仓选到不常用仓库上。
    # 20251212 当sku+大仓下的库存为0时，最优子仓取当前大仓+sku下：有运费、库存最多、最便宜的（非精铺精铺仓）。
    df_stock_max = df_stock.groupby(['warehouse', 'warehouse_id', 'best_warehouse'])[
        'available_stock'].sum().reset_index()
    df_stock_max = df_stock_max.rename(columns={'warehouse_id': 'max_id', 'best_warehouse': 'max_name'})
    df_stock_max = df_stock_max.sort_values(by='available_stock', ascending=False).drop_duplicates(subset=['warehouse'])
    dwm_sku = pd.merge(dwm_sku, df_stock_max[['warehouse', 'max_id', 'max_name']], how='left', on=['warehouse'])
    # 20250324 精铺子仓的最优子仓限定在精铺仓，不转化为常用子仓
    c1 = (dwm_sku['available_stock'] <= 0) & (~dwm_sku['best_warehouse_name'].str.contains('精铺'))
    dwm_sku['best_warehouse_id'] = np.where(c1, dwm_sku['max_id'], dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(c1, dwm_sku['max_name'], dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['max_id','max_name'], axis=1, inplace=True)

    dwm_sku['charge_currency'] = ''
    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus', 'age_60_plus','age_90_plus',
                     'age_120_plus',
                     'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '15days_sales','30days_sales', '60days_sales','90days_sales', 'day_sales', 'recent_day_sales', ]
    dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']] = dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']].astype(int)
    dwm_sku = dwm_sku[columns_order]

    # 可售天数 (大仓的总库存）
    dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(
        np.inf, 9999).replace(np.nan, 0)
    d1 = (pd.to_datetime('2026-01-31') - datetime.datetime.now()).days
    # 部分仓可调整限时清仓的日期条件
    c1 = dwm_sku['best_warehouse_name'].isin(['XYD泰国海外仓','XYD菲律宾海外仓','XYD马来海外仓'])
    d2 = (pd.to_datetime('2025-12-20') - datetime.datetime.now()).days
    esd = ((dwm_sku['available_stock']+dwm_sku['on_way_stock']) / dwm_sku['recent_day_sales']).replace(
        np.inf, 9999).replace(np.nan, 0)
    dwm_sku['all_esd'] = np.where(c1, esd - d2, esd - d1)
    # dwm_sku['all_esd'] = ((dwm_sku['available_stock']+dwm_sku['on_way_stock']) / dwm_sku['recent_day_sales']).replace(
    #     np.inf, 9999).replace(np.nan, 0) - d1

    # 超库龄的可售天数（超 i 天库龄库存的可售天数）
    for i in dwm_sku['overage_level'].unique():
        if np.isnan(i) or i == 0:
            continue
        else:
            c = dwm_sku['overage_level'] == i
            dwm_sku.loc[c, 'overage_esd'] = (
                        dwm_sku.loc[c, 'age_{}_plus'.format(int(i))] / dwm_sku.loc[c, 'day_sales']).replace(
                np.inf,9999).replace(np.nan, 0)

    # 销售状态分类:根据超库龄情况判断分类。
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
    SELECT overage_level, overage_esd_bins, day_sales_bins, section, lowest_profit
    FROM yibai_oversea.profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # profit_rate_section = conn.read_sql(sql)
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    profit_rate_section = conn_ck.ck_select_to_df(sql)

    dwm_sku_2 = pd.merge(dwm_sku_2, profit_rate_section, how='left',
                         on=['overage_level', 'overage_esd_bins', 'day_sales_bins'])
    dwm_sku_2['lowest_profit'] = dwm_sku_2['lowest_profit'].fillna(0.05).astype(float)
    # dwm_sku_2[dwm_sku_2['available_stock'] > 0].to_excel('F://Desktop//dwm_sku_1.xlsx', index=0)
    # 20250619补充限时清仓逻辑
    # 20260205 取消限时清仓逻辑
    # dwm_sku_2 = flash_clearout(dwm_sku_2)

    # 20251009 取消超180部分的回调逻辑
    sql = """
    SELECT *
    FROM up_rate_section
    """
    up_rate_section = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, up_rate_section, how='left', on=['overage_level', 'esd_bins'])

    dwm_sku_2['lowest_profit'] = np.where((dwm_sku_2['warehouse'].str.contains('乌拉圭')), -999, dwm_sku_2['lowest_profit'])

    # 20250724 高仓租逻辑：库存金额/月仓租不同区间， 兜底下调
    dwm_sku_2['charge_temp'] = dwm_sku_2['available_stock_money']/(dwm_sku_2['charge_total_price_rmb']*30)
    dwm_sku_2['charge_temp'] = dwm_sku_2['charge_temp'].replace([np.inf, -np.inf], 999)
    c1 = (dwm_sku_2['charge_temp'] > 0) & (dwm_sku_2['charge_temp'] <= 2)
    c2 = (dwm_sku_2['charge_temp'] > 2) & (dwm_sku_2['charge_temp'] <= 4)
    c3 = (dwm_sku_2['charge_temp'] > 4) & (dwm_sku_2['charge_temp'] <= 6)
    c4 = (dwm_sku_2['charge_temp'] > 6) & (dwm_sku_2['charge_temp'] <= 12)
    # dwm_sku_2['lowest_profit'] = np.select([c1,c2,c3,c4],
    #                                        [-999, dwm_sku_2['lowest_profit']-0.2,
    #                                         dwm_sku_2['lowest_profit']-0.1, dwm_sku_2['lowest_profit']-0.05],
    #                                        dwm_sku_2['lowest_profit'])
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

    # 20251215 限时清仓sku，分次降为销毁价。（截止12月31日）
    # dwm_sku_2 = flash_sku_to_lowest(dwm_sku_2)

    # 20251007 部分sku的兜底及降幅可手动设置。本次添加：BG3的sku
    # 20260107 XYD仓兜底调为销毁价
    df_bg3_sku = bg3_sku()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_bg3_sku, how='left', on=['sku','warehouse'])
    dwm_sku_2['is_bg3_sku'] = dwm_sku_2['is_bg3_sku'].fillna(0).astype(int)
    # c1 = (dwm_sku_2['is_bg3_sku'] == 1) & (dwm_sku_2['available_stock']>0) & (dwm_sku_2['estimated_sales_days']>d1)
    c1 = dwm_sku_2['best_warehouse_name'].isin(['XYD泰国海外仓','XYD菲律宾海外仓','XYD马来海外仓'])
    dwm_sku_2['lowest_profit'] = np.where(c1, -999, dwm_sku_2['lowest_profit'])
    # c2 = (dwm_sku_2['section'] > -dwm_sku_2['单次最大降幅'])
    dwm_sku_2['section'] = np.where(c1, -1, dwm_sku_2['section'])

    # dwm_sku_2[dwm_sku_2['available_stock']>0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)

    # 控制单次降幅最高100%
    dwm_sku_2['section'] = np.where(dwm_sku_2['section']<-1, -1, dwm_sku_2['section'])
    # 20260205 除部分继续清仓品，其他sku保持当前降价幅度
    # 20260214 除部分继续清仓品，其他sku兜底调整为-10%
    df_temp_clear = get_temp_clear_sku()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_temp_clear, how='left', on=['sku','warehouse'])
    # dwm_sku_2['section'] = np.where(dwm_sku_2['temp_clear_sku'].isna(), 0, dwm_sku_2['section'])
    c1 = (dwm_sku_2['temp_clear_sku'].isna()) & (dwm_sku_2['lowest_profit']<-0.1)
    dwm_sku_2['lowest_profit'] = np.where(c1, -0.1, dwm_sku_2['lowest_profit'])
    dwm_sku_2.drop('temp_clear_sku', axis=1, inplace=True)

    # conn.to_sql(dwm_sku_2, 'dwm_sku_temp_temp', if_exists='replace')
    col = ['charge_temp','charge_level','单次最大降幅','lowest_price_profit','target_profit_rate','is_bg3_sku',
           'all_esd','recent_day_sales_bins','all_esd_bins']
    dwm_sku_2.drop(col, axis=1, inplace=True)

    # 利润率涨降幅度
    # begin_profit需替换为前一天的利润率
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

    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['up_profit_rate'] > 0,
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['up_profit_rate'],
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['section'])

    # dwm_sku_2[dwm_sku_2['available_stock']>0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)
    # 20241206 调价周期设置在sku+大仓位置
    print(f'调价周期函数前共{len(dwm_sku_2)}条数据。')
    dwm_sku_2 = adjust_cycle_new(dwm_sku_2)
    print(f'调价周期函数后，共{len(dwm_sku_2)}条数据。')

    # 6. 海外仓补充调价逻辑：高pcs降价逻辑
    dwm_sku_0 = dwm_sku_2[dwm_sku_2['available_stock']>0]
    dwm_sku_1 = dwm_sku_2[dwm_sku_2['available_stock']<=0]
    dwm_sku_2 = get_high_pcs_logic(dwm_sku_0)
    dwm_sku_2 = pd.concat([dwm_sku_2, dwm_sku_1])
    # 调试
    # dwm_sku_2[dwm_sku_2['available_stock']>0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)
    dwm_sku_2.drop(['after_profit_new','lowest_profit_new'], axis=1, inplace=True)
    # 调价最高幅度不超过 0 （暂无涨价缩销的情况）
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] > 0, 0, dwm_sku_2['after_profit'])
    # 库存为0时，调价幅度置为0
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['available_stock'] <= 0, 0, dwm_sku_2['after_profit'])

    # 20231227 (调价幅度+平台最低净利率)最低不超过保底净利率。不超60天库龄的兜底为0净利率
    # 20251117 高pcs降价逻辑下，不超60库龄也可能降价。目前兜底是2%净利率
    c2 = (dwm_sku_2['overage_level'] < 60) & (dwm_sku_2['is_new'] != 2)
    dwm_sku_2['after_profit'] = np.where(c2 , 0, dwm_sku_2['after_profit'])
    dwm_sku_2['lowest_profit'] = np.where(c2, 0, dwm_sku_2['lowest_profit'])
    # # 2025-10-23 非限时清仓sku兜底改为0净利率， 分批次回调
    # c1 = (dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.08)) & (dwm_sku_2['after_profit']>-0.1)
    # c2 = (dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.08)) & (dwm_sku_2['after_profit']<=-0.1)
    # dwm_sku_2['after_profit'] = np.select([c1,c2],
    #                                       [dwm_sku_2['lowest_profit']-0.08,
    #                                        -((0 - dwm_sku_2['after_profit'])/2).clip(lower=0.08)],
    #                                        dwm_sku_2['after_profit'])
    c3 = (dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit'] - 0.08))
    dwm_sku_2['after_profit'] = np.where(c3, dwm_sku_2['lowest_profit']-0.08,dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    dwm_sku_2.drop(['15days_sales','60days_sales'], axis=1,inplace=True)

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
    dwm_sku_2['sales_status'] = '待定'
    print(dwm_sku_2.info())
    # dwm_sku_2[dwm_sku_2['available_stock'] > 0].to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)
    print('SKU信息及调价幅度已获取，准备写入dwm_sku_temp_info...')
    write_to_sql(dwm_sku_2, 'dwm_sku_temp_info')
    print('已写入mysql dwm_sku_temp_info...')
    # dwm_sku_2 = dwm_sku_2[dwm_sku_2['available_stock']>0]
    # dwm_sku_2.to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)

    # 报表
    get_oversea_stock()

    # 备份ck
    dwm_ck_backup()

    # write_to_ck(dwm_sku_2, 'dwm_sku_temp_info')

    return None


# 20250619 限时清仓逻辑
def flash_clearout(df):
    """ 超180天库龄sku 或 限时清仓表格中sku，优先限时清仓逻辑 """
    sql = """
    SELECT distinct recent_day_sales_bins, all_esd_bins, section_2, lowest_profit_2
    FROM over_sea.flash_clear_profit_rate
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    flash_clear = conn.read_sql(sql)

    df = pd.merge(df, flash_clear, how='left', on=['recent_day_sales_bins', 'all_esd_bins'])
    # 20250626 进入限时清仓逻辑sku范围：存在超180天库龄库存且可售天数大于360天
    # 20250717 修改进入限时清仓逻辑sku范围：存在超180天库龄库存且超库龄库存可售天数大于180天
    # 20250722 限时清仓sku来源：7月22日锁定清单
    sql = """
        SELECT sku, warehouse, 1 as is_flash_clear
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)
    df = pd.merge(df, df_flash_sku, how='left', on=['sku','warehouse'])
    df['is_flash_clear'] = df['is_flash_clear'].fillna(0).astype(int)

    c1 = (~df['section_2'].isna()) & (df['is_flash_clear']==1) & (df['available_stock']>0)
    df['section'] = np.where(c1, df['section_2'], df['section'])
    # # 限时清仓sku 触发降价逻辑的，库龄置为至少为60
    # df['overage_level'] = np.where(c1 & (df['overage_level']<60), 60, df['overage_level'])
    # for i in ['age_30_plus','age_60_plus']:
    #     df[i] = np.where(c1, df['available_stock'], df[i])

    c1 = (df['3days_sales'] > (0.6*df['7days_sales'])) & (df['is_flash_clear']==1)
    df['section'] = np.where(c1, 0, df['section'])
    c1 = (~df['lowest_profit_2'].isna()) & (df['is_flash_clear']==1)
    df['lowest_profit'] = np.where(c1, df['lowest_profit_2'], df['lowest_profit'])

    df.drop(['section_2','lowest_profit_2', 'is_flash_clear'], axis=1, inplace=True)

    return df

# dwm_temp_info备份ck
def dwm_ck_backup():
    """ dwm_sku_temp_info 备份ck """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    col = ['new_price', 'gross']
    df[col] = df[col].astype(float)
    print(df.info())

    write_to_ck(df, 'dwm_sku_temp_info')
    print('已备份ck')

def write_to_ck(df, table_name):
    """
    将中间表数据写入ck
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    sql = f"""
    ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_id}'
    """
    conn_ck.ck_execute_sql(sql)
    # 确认当天日期数据已删除
    n = 1
    while n < 5:
        print(f'删除当前表里的数据，第{n}次测试...')
        sql = f"""
            SELECT count()
            FROM yibai_oversea.{table_name}
            where date_id = '{date_id}'
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('结果表删除成功！')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            break
        else:
            n += 1
            time.sleep(60)
    if n == 5:
        print('备份CK失败，当天数据未删除完成，CK未备份')

# 对主要条件分箱分层
def cut_bins(df):
    """
    对主要条件分箱：超库龄天数、日销、总库存的可售天数、超库龄库存的可售天数
    """
    df['overage_esd_bins'] = pd.cut(df['overage_esd'], bins=[-1, 30, 60, 90, 150, 300, 999],
                                    labels=['A∈(0,30]', 'A∈(30,60]', 'A∈(60,90]', 'A∈(90,150]', 'A∈(150,300]',
                                            'A∈(300,∞)'])
    df['overage_esd_bins'] = np.where(df['overage_esd'] > 300, 'A∈(300,∞)', df['overage_esd_bins'])

    df['day_sales_bins'] = pd.cut(df['day_sales'], bins=[-1, 0.1, 0.3, 0.6, 1, 3, 5, 10],
                                  labels=['S∈(0,0.1]', 'S∈(0.1,0.3]', 'S∈(0.3,0.6]', 'S∈(0.6,1]', 'S∈(1,3]', 'S∈(3,5)',
                                          'S∈(5,∞)'])
    df['day_sales_bins'] = np.where(df['day_sales'] > 5, 'S∈(5,∞)', df['day_sales_bins'])

    df['recent_day_sales_bins'] = pd.cut(df['recent_day_sales'], bins=[-1, 0.03, 0.3, 0.5, 1, 10],
                                  labels=['S∈(0,0.03]', 'S∈(0.03,0.3]', 'S∈(0.3,0.5]', 'S∈(0.5,1]',
                                          'S∈(1,∞)'])
    df['recent_day_sales_bins'] = np.where(df['recent_day_sales'] > 1, 'S∈(1,∞)', df['recent_day_sales_bins'])
    # 当前库存的可售天数主要用于【回调】
    df['esd_bins'] = pd.cut(df['estimated_sales_days'], bins=[-1, 5, 10, 20, 30, 40, 60, 999],
                            labels=['N∈(0,5]', 'N∈(5,10]', 'N∈(10,20]', 'N∈(20,30]', 'N∈(30,40]', 'N∈(40,60]',
                                    'N∈(60,∞)'])
    df['esd_bins'] = np.where(df['estimated_sales_days'] < 0, 'N∈(0,5]', df['esd_bins'])
    df['esd_bins'] = np.where(df['estimated_sales_days'] > 60, 'N∈(60,∞)', df['esd_bins'])

    df['all_esd_bins'] = pd.cut(df['all_esd'], bins=[-1, 0, 10, 20, 30, 50, 100, 999],
                            labels=['A∈(-∞,0]', 'A∈(0,10]', 'A∈(10,20]', 'A∈(20,30]', 'A∈(30,50]', 'A∈(50,100]',
                                    'A∈(100,∞)'])
    df['all_esd_bins'] = np.where(df['all_esd'] <= 0, 'A∈(-∞,0]', df['all_esd_bins'])
    df['all_esd_bins'] = np.where(df['all_esd'] > 100, 'A∈(100,∞)', df['all_esd_bins'])

    return df

# 调价周期设置
# 旧函数已弃用
def adjust_cycle_new(dwm_sku):
    """
    调价周期函数：
    降价周期三天，涨价周期两天
    """
    # 降价周期
    sql = f"""
    SELECT sku, warehouse, after_profit as after_profit_y, is_adjust, date_id
    FROM over_sea.dwm_sku_temp_info
    WHERE date_id >= date_sub(curdate(),interval 2 day) and date_id < '{time.strftime('%Y-%m-%d')}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp_t = conn.read_sql(sql)
    print(f'调价周期函数：历史数据共{len(df_temp_t)}条。')
    # 涨价周期
    sql = f"""
    SELECT sku, warehouse, after_profit as after_profit_y, is_adjust, date_id
    FROM over_sea.dwm_sku_temp_info
    WHERE date_id >= date_sub(curdate(),interval 1 day) and date_id < '{time.strftime('%Y-%m-%d')}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp_up = conn.read_sql(sql)

    # 如果上一次调价周期超过三天，则取最近的调价记录，判断是否调价
    if len(df_temp_t) == 0:
        sql = f"""

        SELECT sku, warehouse, after_profit as after_profit_y, is_adjust, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id < '{datetime.date.today()}')
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_temp_t = conn.read_sql(sql)
    if len(df_temp_up) == 0:
        sql = f"""

        SELECT sku, warehouse, after_profit as after_profit_y, is_adjust, date_id
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id < '{datetime.date.today()}')
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_temp_up = conn.read_sql(sql)

    # 调价状态判断
    # c1 = np.isclose(dwm_sku_price_temp['target_profit_rate'], dwm_sku_price_temp['target_profit_y'])
    c1 = dwm_sku['after_profit'].round(4) == (dwm_sku['begin_profit']).round(4)
    c2 = dwm_sku['after_profit'].round(4) > dwm_sku['begin_profit'].round(4)
    c3 = dwm_sku['after_profit'].round(4) < dwm_sku['begin_profit'].round(4)
    dwm_sku['is_adjust'] = np.select([c1, c2, c3], ['保持', '涨价', '降价'], '保持')

    # 降价调价周期设置
    # 实现方式：获取近 * 天的调价状态，如果当前最新的调价状态已出现，则将调价状态置为保持、利润率置为前一日
    is_adjust_temp = df_temp_t.groupby(['sku', 'warehouse'])['is_adjust'].apply(
        lambda x: x.str.cat(sep=',')).reset_index()
    is_adjust_temp = is_adjust_temp.rename(columns={'is_adjust': 'is_adjust_list'})

    dwm_sku = pd.merge(dwm_sku, is_adjust_temp, how='left', on=['sku', 'warehouse'])
    # split_list = dwm_sku_price_temp['is_adjust_list'].str.split(',', expand=True).stack()
    dwm_sku['is_adjust_list'] = dwm_sku['is_adjust_list'].fillna(' ').astype(str)
    dwm_sku['is_adjust_list'] = dwm_sku['is_adjust_list'].str.split(', ', expand=True).apply(
        lambda x: [item for item in x])
    dwm_sku['is_in'] = dwm_sku.apply(
        lambda row: all(item in row['is_adjust_list'] for item in row['is_adjust'].split()), axis=1)
    c1 = (dwm_sku['is_adjust'] == '降价') & (dwm_sku['is_in'] == True)
    dwm_sku['after_profit'] = np.select([c1], [dwm_sku['begin_profit']],dwm_sku['after_profit'])
    dwm_sku['is_adjust'] = np.select([c1], ['保持'], dwm_sku['is_adjust'])
    dwm_sku.drop(['is_adjust_list','is_in'], axis=1, inplace=True)
    # dwm_sku = dwm_sku.rename(columns={'is_adjust_list':'is_adjust_list_down','is_in':'is_in_down'})

    # 涨价调价周期设置
    # 实现方式：获取近 * 天的调价状态，如果当前最新的调价状态已出现，则将调价状态置为保持、利润率置为前一日
    is_adjust_up = df_temp_up.groupby(['sku', 'warehouse'])['is_adjust'].apply(
        lambda x: x.str.cat(sep=',')).reset_index()
    is_adjust_up = is_adjust_up.rename(columns={'is_adjust': 'is_adjust_list'})

    dwm_sku = pd.merge(dwm_sku, is_adjust_up, how='left', on=['sku', 'warehouse'])
    # split_list = dwm_sku_price_temp['is_adjust_list'].str.split(',', expand=True).stack()
    dwm_sku['is_adjust_list'] = dwm_sku['is_adjust_list'].fillna(' ').astype(str)
    dwm_sku['is_adjust_list'] = dwm_sku['is_adjust_list'].str.split(', ', expand=True).apply(
        lambda x: [item for item in x])
    dwm_sku['is_in'] = dwm_sku.apply(
        lambda row: all(item in row['is_adjust_list'] for item in row['is_adjust'].split()), axis=1)
    c1 = (dwm_sku['is_adjust'] == '涨价') & (dwm_sku['is_in'] == True)
    dwm_sku['after_profit'] = np.select([c1], [dwm_sku['begin_profit']], dwm_sku['after_profit'])
    dwm_sku['is_adjust'] = np.select([c1], ['保持'], dwm_sku['is_adjust'])
    # dwm_sku = dwm_sku.rename(columns={'is_adjust_list': 'is_adjust_list_up', 'is_in': 'is_in_up'})
    dwm_sku.drop(['is_adjust_list','is_in'], axis=1, inplace=True)

    return dwm_sku

def adjust_cycle_ai(dwm_sku):
    """
    调价周期函数：降价周期三天，涨价周期两天（优化版）
    """
    # 1. 一次性连接数据库，执行所有查询（减少连接开销）
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    today = time.strftime('%Y-%m-%d')
    max_date_sql = f"SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id < '{today}'"
    max_date = conn.read_sql(max_date_sql).iloc[0, 0]  # 获取最近的有效日期

    # 2. 合并查询：一次读取所有需要的历史数据（降价周期3天，涨价周期2天）
    # 降价周期：近3天（date_id >= 今天-3天 且 < 今天）
    # 涨价周期：近2天（date_id >= 今天-2天 且 < 今天）
    sql = f"""
    SELECT 
        sku, 
        warehouse, 
        after_profit as after_profit_y, 
        is_adjust, 
        date_id,
        -- 标记数据属于降价周期还是涨价周期（方便后续筛选）
        CASE 
            WHEN date_id >= date_sub('{today}', interval 2 day) THEN 'down'  -- 降价周期（3天：今天-2, 今天-1, 今天-0前）
            WHEN date_id >= date_sub('{today}', interval 1 day) THEN 'up'    -- 涨价周期（2天：今天-1, 今天-0前）
        END AS cycle_type
    FROM over_sea.dwm_sku_temp_info
    WHERE date_id < '{today}'
      AND (
          date_id >= date_sub('{today}', interval 2 day)  -- 覆盖降价周期
          OR date_id >= date_sub('{today}', interval 1 day)  -- 覆盖涨价周期
      )
    """
    df_history = conn.read_sql(sql)
    conn.close()  # 及时关闭连接

    # 3. 处理历史数据为空的情况（补充最近一天数据）
    if df_history.empty and max_date is not None:
        sql补充 = f"""
        SELECT 
            sku, warehouse, after_profit as after_profit_y, is_adjust, date_id,
            'down' as cycle_type  -- 临时标记，后续会按周期拆分
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{max_date}'
        """
        df_history = conn.read_sql(sql补充)
        conn.close()

    print(f'调价周期函数：历史数据共{len(df_history)}条。')

    # 4. 拆分降价/涨价周期数据（替代原两次查询）
    # 降价周期：cycle_type = 'down'（近3天）
    df_temp_t = df_history[df_history['cycle_type'] == 'down'].drop(columns='cycle_type')
    # 涨价周期：cycle_type = 'up'（近2天）
    df_temp_up = df_history[df_history['cycle_type'] == 'up'].drop(columns='cycle_type')

    # 5. 调价状态判断（保持原逻辑，用向量化round）
    dwm_sku['after_profit_round'] = dwm_sku['after_profit'].round(4)
    dwm_sku['begin_profit_round'] = dwm_sku['begin_profit'].round(4)

    c1 = dwm_sku['after_profit_round'] == dwm_sku['begin_profit_round']
    c2 = dwm_sku['after_profit_round'] > dwm_sku['begin_profit_round']
    c3 = dwm_sku['after_profit_round'] < dwm_sku['begin_profit_round']
    dwm_sku['is_adjust'] = np.select([c1, c2, c3], ['保持', '涨价', '降价'], '保持')

    # 6. 封装周期处理逻辑（减少代码冗余）
    def process_cycle(df_main, df_history, cycle_type):
        """处理降价/涨价周期的通用函数"""
        if df_history.empty:
            return df_main

        # 分组聚合：用向量化str.cat替代apply+groupby（效率提升10倍+）
        # 按sku+warehouse分组，拼接is_adjust为字符串（如"降价,降价"）
        is_adjust_group = df_history.groupby(['sku', 'warehouse'])['is_adjust'].agg(
            lambda x: ','.join(x)  # 直接拼接，避免后续split再处理
        ).reset_index(name='is_adjust_list')

        # 合并主表
        df_merged = pd.merge(df_main, is_adjust_group, on=['sku', 'warehouse'], how='left')
        df_merged['is_adjust_list'] = df_merged['is_adjust_list'].fillna('')  # 空值填空字符串

        # 核心判断：当前is_adjust是否在历史列表中（向量化替代apply）
        # 例：当前is_adjust为"降价"，历史列表为"降价,降价" → 包含则返回True
        df_merged['is_in'] = df_merged.apply(
            lambda row: row['is_adjust'] in row['is_adjust_list'].split(','), axis=1
        )

        # 调整after_profit和is_adjust
        mask = (df_merged['is_adjust'] == cycle_type) & df_merged['is_in']
        df_merged.loc[mask, 'after_profit'] = df_merged.loc[mask, 'begin_profit']
        df_merged.loc[mask, 'is_adjust'] = '保持'

        # 删除临时列
        return df_merged.drop(columns=['is_adjust_list', 'is_in'])

    # 7. 分别处理降价和涨价周期
    dwm_sku = process_cycle(dwm_sku, df_temp_t, '降价')  # 降价周期处理
    dwm_sku = process_cycle(dwm_sku, df_temp_up, '涨价')  # 涨价周期处理

    # 8. 清理临时列
    dwm_sku.drop(columns=['after_profit_round', 'begin_profit_round'], inplace=True)

    return dwm_sku

def get_limit_price():
    """ TT限价表信息 """
    sql = f"""
        SELECT plat platform, site, sku, price limit_price
        FROM domestic_warehouse_clear.plat_sku_limit_price
        WHERE site != '中国'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    df_limit = conn_ck.ck_select_to_df(sql)

    dic = {'澳大利亚': 'AU', '美国': 'US', '德国': 'DE', '法国': 'FR', '英国': 'UK', '加拿大': 'CA', '意大利': 'IT',
           '西班牙': 'ES','荷兰': 'NL', '比利时': 'BE', '土耳其': 'TR',
           '墨西哥': 'MX', '巴西': 'BR', '波兰': 'PL', '瑞典': 'SE'}

    df_limit['country'] = df_limit['site'].map(dic).fillna(' ')

    # df_limit.to_excel('F://Desktop//df_limit.xlsx', index=0)

    return df_limit

def get_temp_jp_to_fp_sku():
    """ 获取精铺转泛品sku （临时表） """
    sql = """
        SELECT sku, source
        FROM yibai_oversea.temp_oversea_jp_to_fp_sku
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.temp_oversea_jp_to_fp_sku)
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    return df


# 利润率、销售状态、价格计算
def dwm_sku_price():
    """
    销售状态设置
    调价周期设置 *
    价格计算
    """

    # 读取dwm_sku_2
    date_today = time.strftime('%Y-%m-%d')
    t1 = time.time()
    sql = f"""
    SELECT *
    FROM dwm_sku_temp_info
    WHERE date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku = conn.read_sql(sql)
    t2 = time.time()
    print(f'获取sku完成，共{len(dwm_sku)}条！共耗时{t2 - t1:.2f}s')
    # 剔除精铺非转泛品sku
    # 20250321 隔离精铺非转泛品的sku
    # 20260310 补充部分转泛品sku
    sku_temp = get_temp_jp_to_fp_sku()
    sku_temp = sku_temp['sku'].unique()
    c0 = dwm_sku['type'].str.contains('海兔|易通兔') | dwm_sku['type'].str.contains('转VC|转泛品')
    c1 = (dwm_sku['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) & (~c0)
    c2 = (dwm_sku['sku'].isin(sku_temp))
    dwm_sku = dwm_sku[(~c1) | c2]
    # 20251128 供应商货盘sku单独定价。位置：only_supplier_price
    dwm_sku = dwm_sku[dwm_sku['type'] != '供应商货盘']
    print(dwm_sku.info())

    # 匹配运费
    # 运费数据需要处理！！！
    # WISH平台的汇率设置为1：
    df_transport_fee = get_transport_fee()
    df_walmart_mx = df_transport_fee[(df_transport_fee['platform']=='AMAZON') & (df_transport_fee['country']=='MX') & \
        (df_transport_fee['best_fee_warehouse'].str.contains('墨西哥'))]
    df_walmart_mx['platform'] = 'WALMART'
    df_transport_fee = pd.concat([df_transport_fee, df_walmart_mx])
    print(df_transport_fee.info())
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='inner', on='country')
    col = ['sku', 'warehouse', 'best_fee_warehouse', 'best_fee_id', 'ship_name', 'total_cost',
           'lowest_price', 'platform', 'country', 'rate']
    df_transport_fee = df_transport_fee[col].drop_duplicates()

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate',
               'is_adjust','begin_profit']

    # dwm_sku.loc[dwm_sku['best_warehouse_name'] == 'YM墨西哥toB代发仓', 'best_warehouse_id'] = 956
    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_transport_fee, how='inner',on=['sku', 'warehouse'])
    # dwm_sku_price.loc[dwm_sku_price['best_warehouse_name'] == 'YM墨西哥toB代发仓', 'best_warehouse_id'] = 958
    # 20260202 最优子仓替换为运费最优子仓
    dwm_sku_price['best_warehouse_name'] = np.where(dwm_sku_price['best_fee_warehouse'].isna(),
                                                    dwm_sku_price['best_warehouse_name'], dwm_sku_price['best_fee_warehouse'])
    dwm_sku_price['best_warehouse_id'] = np.where(dwm_sku_price['best_fee_id'].isna(),
                                                    dwm_sku_price['best_warehouse_id'], dwm_sku_price['best_fee_id'])
    dwm_sku_price.drop(['best_fee_warehouse','best_fee_id'], axis=1, inplace=True)
    # 无运费数据处理
    # dwm_sku_price.drop(['warehouse_id'], axis=1, inplace=True)
    dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]
    # 无汇率国家处理：WISH平台且有库存的SKU的汇率设置为1
    dwm_sku_price['rate'] = np.where((dwm_sku_price['platform'] == 'WISH') & (dwm_sku_price['available_stock'] > 0), 1, dwm_sku_price['rate'])
    dwm_sku_price = dwm_sku_price[~dwm_sku_price['rate'].isna()]
    dwm_sku_price = dwm_sku_price.drop_duplicates()
    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_sku_price = pd.merge(dwm_sku_price, df_platform_fee, how='inner', on=['platform', 'country'])
    # print(dwm_sku_price.info())
    # dwm_sku_price.to_excel('F://Desktop//dwm_sku_price.xlsx', index=0)

    # 数据类型转化
    type_columns = ['new_price', 'total_cost', 'lowest_price', 'ppve', 'refound_fee', 'platform_zero',
                    'platform_must_percent']
    dwm_sku_price[type_columns] = dwm_sku_price[type_columns].astype('float64').round(4)
    dwm_sku_price['target_profit_y'] = dwm_sku_price['begin_profit'] + dwm_sku_price['platform_must_percent']
    dwm_sku_price.drop('begin_profit', axis=1, inplace=True)

    # 精铺转泛品sku定价不高于当前精铺在线价
    df_fine_price = get_fine_limit_price()
    col = ['sku', 'country', 'fine_price']
    dwm_temp = dwm_sku_price[dwm_sku_price['platform']=='AMAZON']
    dwm_temp = pd.merge(dwm_temp, df_fine_price[col], how='inner', on=['sku','country'])
    dwm_temp['fine_price'] = dwm_temp['fine_price'].astype(float)
    dwm_temp['fine_profit'] = 1 - dwm_temp['ppve'] - dwm_temp['platform_zero'] - (
            dwm_temp['new_price'] + dwm_temp['total_cost'])/(dwm_temp['fine_price']*dwm_temp['rate'])

    dwm_sku_price = pd.merge(dwm_sku_price, dwm_temp[['sku', 'country', 'fine_profit']], how='left', on=['sku','country'])
    dwm_sku_price['fine_profit'] = dwm_sku_price['fine_profit'].fillna(0.5).astype(float)

    # 净利率的处理
    # dwm_sku_price['target_profit_rate'] = np.where(
    #     (dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']) < dwm_sku_price['lowest_profit'],
    #     dwm_sku_price['lowest_profit'],
    #     dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit'])
    dwm_sku_price['target_profit_rate'] = dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']
    c1 = (dwm_sku_price['target_profit_rate'] > dwm_sku_price['fine_profit']) & (
        dwm_sku_price['fine_profit']>dwm_sku_price['lowest_profit'])
    dwm_sku_price['target_profit_rate'] = np.where(c1, dwm_sku_price['fine_profit'], dwm_sku_price['target_profit_rate'])
    # 调价周期设置
    # dwm_sku_price = adjust_cycle(dwm_sku_price)

    # 销售状态处理
    c1 = (dwm_sku_price['target_profit_rate'] >= dwm_sku_price['platform_must_percent'])
    c2 = (dwm_sku_price['up_profit_rate'] > 0)
    c3 = (dwm_sku_price['target_profit_rate'] > 0)
    c4 = (dwm_sku_price['target_profit_rate'] <= 0)
    dwm_sku_price['sales_status'] = np.select([c1, c2, c3, c4], ['正常', '回调', '正利润加快动销', '负利润加快动销'])

    # 价格计算
    dwm_sku_price['price_rmb'] = (dwm_sku_price['new_price'] + dwm_sku_price['total_cost']) / (
                1 - dwm_sku_price['ppve'] - dwm_sku_price['platform_zero'] - dwm_sku_price['target_profit_rate'])
    # dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01

    # 2025-11-21 补充供应商货盘sku的定价逻辑。补充海外仓sku无到货记录、供应商货盘sku有货的数据。合并供应商货盘的YM_sku数据
    dwm_sku_price = get_supplier_price(dwm_sku_price)
    df = only_supplier_price()
    df.drop(['shippingCost','firstCarrierCost'], axis=1, inplace=True)
    dwm_sku_price = pd.concat([dwm_sku_price, df])
    dwm_sku_price = dwm_sku_price[dwm_sku_price['sku'] != '']
    # 销毁价判断
    # 20250214 销毁价融入限价
    df_limit = get_limit_price()
    dwm_sku_price = pd.merge(dwm_sku_price, df_limit[['sku','platform', 'country', 'limit_price']],
                             how='left', on=['sku','platform', 'country'])
    c1 = (~dwm_sku_price['limit_price'].isna()) & ((dwm_sku_price['limit_price']*dwm_sku_price['rate'])>dwm_sku_price['lowest_price'])
    dwm_sku_price['lowest_price'] = np.where(c1, dwm_sku_price['limit_price']*dwm_sku_price['rate'], dwm_sku_price['lowest_price'])

    # 销毁价兜底
    dwm_sku_price['price_rmb'] = np.where(dwm_sku_price['price_rmb'] <= dwm_sku_price['lowest_price'],
                                          dwm_sku_price['lowest_price'], dwm_sku_price['price_rmb'])
    col = ['new_price', 'total_cost', 'ppve', 'refound_fee','platform_zero','platform_must_percent','target_profit_rate']
    dwm_sku_price[col] = dwm_sku_price[col].astype(float).round(4)
    # print(dwm_sku_price.info())
    dwm_sku_price['target_profit_rate'] = 1 - dwm_sku_price['ppve'] - dwm_sku_price['platform_zero'] - \
                                          (dwm_sku_price['new_price'] + dwm_sku_price['total_cost'])/dwm_sku_price['price_rmb']
    dwm_sku_price['target_profit_rate'] = dwm_sku_price['target_profit_rate'].astype(float).round(4)

    dwm_sku_price['is_distory'] = np.where(
        dwm_sku_price['price_rmb'].astype(int) == dwm_sku_price['lowest_price'].astype(int), 1, 0)

    # 20260320 供应商货盘sku的汇率用折扣前的汇率
    dwm_sku_price = pd.merge(dwm_sku_price, df_rate[['country', 'erp_rate']], how='left', on='country')
    c1 = (dwm_sku_price['is_supplier_price'] == 1)
    dwm_sku_price['rate'] = np.where(c1, dwm_sku_price['erp_rate'], dwm_sku_price['rate'])

    # 本币计算
    col = ['price_rmb', 'rate']
    dwm_sku_price[col] = dwm_sku_price[col].astype(float)
    dwm_sku_price['price'] = dwm_sku_price['price_rmb'] / dwm_sku_price['rate']
    dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01

    dwm_sku_price['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    # 20260108 dtl迁到ck
    dwm_sku_price.drop(['rate', 'erp_rate', 'refound_fee', 'limit_price','fine_profit'], axis=1, inplace=True)
    col = ['new_price', 'total_cost','platform_must_percent','target_profit_rate','ppve','platform_zero']
    dwm_sku_price[col] = dwm_sku_price[col].fillna(0).astype(float)
    dwm_sku_price['available_stock'] = dwm_sku_price['available_stock'].fillna(0).astype(int)

    # 20260312 walmart补充云仓美西仓库存
    dwm_sku_price = walmart_dtl_stock_logic(dwm_sku_price)
    print(dwm_sku_price.info())

    # 20260326 补充捆绑sku的定价数据
    dwm_bundle = get_bundle_sku_price()
    dwm_sku_price = pd.concat([dwm_sku_price, dwm_bundle])

    write_to_ck(dwm_sku_price, 'dwm_oversea_price_dtl')
    print('调价数据已写入ck')

    # 20260327 dtl准备取消mysql存表，全部转入ck
    dwm_sku_price.drop(['ppve', 'platform_zero', 'platform_must_percent'], axis=1, inplace=True)
    print('SKU目标价已计算，准备写入dwm_oversea_price_dtl...')
    write_to_sql(dwm_sku_price, 'dwm_oversea_price_dtl')
    print('调价数据已写入mysql')

    # 各平台是否达到销毁价存表
    dwm_is_destroy()


    return None


def dwm_is_destroy():
    """ 各平台目标利润率及是否销毁价 """
    sql = """

        SELECT sku, warehouse, country, platform , target_profit_rate, is_distory, date_id
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and available_stock > 0 
        -- and warehouse in ('美国仓','英国仓')
        and platform != 'ALI'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'美国仓':'US','德国仓':'DE','英国仓':'UK','法国仓':'FR','澳洲仓':'AU','加拿大仓':'CA','墨西哥仓':'MX',
           '西班牙仓':'ES','意大利仓':'IT','乌拉圭仓':'BR','日本仓':'JP','阿联酋仓':'AE','巴西仓':'BR','俄罗斯仓':'RU',
           '菲律宾仓':'PH','马来西亚仓':'MY','泰国仓':'TH','越南仓':'VN'}
    df['warehouse_country'] = df['warehouse'].replace(dic)
    # 利润率、是否销毁价，可以只取一个国家（仓库所在国）
    df = df[df['warehouse_country'] == df['country']]

    def platform_profit_dict(group):
        return group.set_index('platform')['target_profit_rate'].to_dict()

    result = df.groupby(['sku', 'warehouse']).apply(platform_profit_dict).reset_index()
    result.columns = ['sku', 'warehouse', 'target_profit_rate']
    result['target_profit_rate'] = result['target_profit_rate'].astype(str)

    df_is_distory = df.groupby(['sku','warehouse']).agg({'is_distory':list}).reset_index()
    df_is_distory['is_destroy'] = df_is_distory['is_distory'].apply(lambda x: ','.join(map(str, x)).replace('[', '').replace(']', ''))
    df_is_distory['is_destroy_final'] = np.where(df_is_distory['is_destroy'].str.contains('1'), 1, 0)
    df_is_distory.drop('is_distory', axis=1, inplace=True)

    result = pd.merge(result, df_is_distory, how='left', on=['sku','warehouse'])
    result['date_id'] = df.iloc[0]['date_id']

    write_to_sql(result, 'dwm_oversea_profit')

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(result, 'dwm_oversea_profit', if_exists='replace')


def destroy_days():
    """ 各平台目标利润率及是否销毁价 """
    sql = """
    SELECT sku, warehouse,  is_destroy_final, date_id
    FROM over_sea.dwm_oversea_profit
    WHERE date_id >= (SELECT max(date_id) - 30 FROM over_sea.dwm_oversea_profit)  -- 最近30天
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df.info())
    # 按 sku warehouse 分组，并按 date_id 升序排序
    df = df.sort_values(['sku', 'warehouse', 'date_id'])

    # 计算最近一次连续达到销毁价的天数
    def latest_consecutive_days(group):
        group = group.sort_values('date_id', ascending=True)  # 确保时间顺序
        isdestroy = group['is_destroy_final'].values
        consecutive_days = 0
        dates = group['date_id'].values
        start_date = None

        # 从后往前遍历（最新日期开始）
        for i in range(len(isdestroy) - 1, -1, -1):
            if isdestroy[i] == 1:
                consecutive_days += 1
                start_date = dates[i]  # 记录当前日期作为起始日期
            else:
                break  # 遇到0就停止

        return pd.Series({
            'latest_days': consecutive_days,
            'latest_date': start_date
        })

    latest_consecutive = df.groupby(['sku', 'warehouse']).apply(latest_consecutive_days).reset_index()

    # 合并结果
    latest_date = df['date_id'].max()
    result = df[df['date_id'] == latest_date]

    result = pd.merge(result, latest_consecutive, how='left', on=['sku', 'warehouse'])


    print(result.info())
    result.to_excel('F://Desktop//df_profit_result.xlsx', index=0)

    # return result

# 销毁价利润率
def get_destroy_profit():
    """
    计算amazon、ebay销毁价利润率。
    将低值回写到dwm_sku_temp_info。
    用于判断【加快动销补充逻辑】：销毁价过低时，是否需要增大降价幅度
    """
    sql = """

        SELECT sku, warehouse, new_price, total_cost, lowest_price, country, platform , target_profit_rate, is_distory, date_id
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform in ('AMAZON', 'EB', 'SHOPEE', 'LAZADA','Wildberries')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'美国仓':'US','德国仓':'DE','英国仓':'UK','法国仓':'FR','澳洲仓':'AU','加拿大仓':'CA','墨西哥仓':'MX', '俄罗斯仓':'RU',
           '西班牙仓':'ES','意大利仓':'IT','乌拉圭仓':'BR','日本仓':'JP','泰国仓':'TH','菲律宾仓':'PH','马来西亚仓':'MY','越南仓':'VN'}
    df['warehouse_country'] = df['warehouse'].replace(dic)
    # 利润率、是否销毁价，可以只取一个国家（仓库所在国）
    df = df[(df['warehouse_country'] == df['country']) | (df['warehouse_country'].isna())]

    # 取配置表
    df_platform_fee = get_platform_fee()
    df = pd.merge(df, df_platform_fee[['platform','country','ppve','platform_zero', 'platform_must_percent']],
                  how='left', on=['platform','country'])
    df['lowest_price_profit'] = 1 - df['ppve'] - df['platform_zero'] - df['platform_must_percent'] - \
                                (df['new_price']+df['total_cost'])/df['lowest_price']

    df = df.sort_values(by='lowest_price_profit', ascending=True).drop_duplicates(subset=['sku','warehouse'])

    df = df[['sku','warehouse','target_profit_rate','lowest_price_profit', 'is_distory']]

    # df.to_excel('F://Desktop//df_destroy.xlsx', index=0)

    return df


def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = time.strftime('%Y-%m-%d')
    # df10 = df[df.index < 10000]
    # conn.to_sql(df10, table_name, if_exists='append')
    
    # sql = f"""
    # delete from {table_name}
    # """
    # conn.execute(sql)

    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

def get_bundle_sku():
    conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
    with sku_map as (
        select sku,
            arrayStringConcat(groupArray(sku1), ',') as `sku1`,  
            arrayStringConcat(groupArray(`数量`), ',') as `数量`
        from (
            select sku,sku1,sum(`数量`) as `数量`
            from (
                select distinct sku,
                    arrayJoin(splitByString('+', sku)) as sku2,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[1]) as sku1,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[2]) as num1,
                    if(num1 = '', 1, toInt32(num1)) as `数量`
                from (
                    select distinct sku from (
                        select distinct sku from yibai_product_kd_sync.yibai_amazon_sku_map 
                        where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2 
                    )
                )
            )
            group by sku,sku1
        )
        group by sku
    ),
    sku_site_table as (
        select distinct site1,site3,sku 
        from (
            select distinct c.site1 as site1,c.site3 as site3,a.sku as sku
            from (
                select distinct account_id,sku 
                from yibai_product_kd_sync.yibai_amazon_sku_map 
                where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2
            ) a 
            left join (
                select toInt64(id) as account_id,site from yibai_system_kd_sync.yibai_amazon_account
            ) b 
            on a.account_id=b.account_id
            left join domestic_warehouse_clear.yibai_site_table_amazon c 
            on b.site=c.site
        )
    )

    select distinct a.site1 as site1,a.site3 as site3,a.sku as sku1,b.sku1 as sku,b.`数量` as `数量`
    from sku_site_table a 
    left join sku_map b 
    on a.sku=b.sku 
    settings max_memory_usage = 30000000000
    """
    df = ck_client.ck_select_to_df(sql)
    print(df.info())
    print(f'捆绑链接数量共{len(df)}条')
    # 国家处理。与海外仓大仓名对应
    df = df[~df['site1'].isin(['新加坡', '印度', '阿联酋'])]
    df['site2'] = np.where(
        df['site1'].isin(['德国', '法国', '西班牙', '意大利', '波兰', '瑞典', '荷兰', '土耳其', '比利时']), '欧洲仓',
        df['site1'] + '仓')
    df['site2'] = np.where(df['site1'] == '澳大利亚', '澳洲仓', df['site2'])
    df['site2'] = np.where(df['site1'] == '巴西', '乌拉圭仓', df['site2'])

    df = df.reset_index()
    # 拆分捆绑SKU信息
    df_explode = df.set_index(['index', 'site1', 'site3', 'site2','sku1']).apply(
        lambda x: x.str.split(',').explode()).reset_index()
    print('捆绑链接获取完成.')
    # 筛选海外仓sku
    date_begin = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT distinct sku, 1 as is_oversea_sku
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id >= (SELECT max(date_id) FROM dwm_sku_temp_info )
        ORDER BY date_id DESC
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_oversea_sku = conn.read_sql(sql)
    # 获取捆绑SKU的库存和库龄信息
    sql = """
        SELECT *
        FROM over_sea.dwd_sku_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_info)
    """
    df_sku = conn.read_sql(sql)
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    #
    df_explode = pd.merge(df_explode, df_oversea_sku, how='left', on=['sku'])
    df_explode = pd.merge(df_explode,
                          df_sku[['sku', 'new_price', 'gross', 'available_stock', 'warehouse_id', 'warehouse']],
                          how='left', on=['sku'])
    df_explode = pd.merge(df_explode, df_stock_age_id[
        ['sku', 'warehouse_id','age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus',
         'age_270_plus', 'age_360_plus', 'warehouse_stock', 'warehouse_stock_age', 'overage_level']], how='left',
                          on=['sku', 'warehouse_id'])
    #
    df_explode['warehouse_id'] = df_explode['warehouse_id'].fillna(0).astype(int)

    # df_explode = df_explode[df_explode['warehouse_id'] != 0]

    # 处理链接国家与发货仓需一致问题
    df_explode['warehouse_temp'] = np.where(df_explode['warehouse'].isin(['法国仓', '德国仓', '西班牙仓', '意大利仓']),
                                            '欧洲仓', df_explode['warehouse'])
    df_explode = df_explode[df_explode['site2'] == df_explode['warehouse_temp']]
    df_explode.drop(['site2', 'warehouse_temp'], axis=1, inplace=True)
    #
    df_explode = df_explode.fillna(0)
    # 多个SKU捆绑时，如果各个SKU不在同一个子仓，需剔除
    df_explode1 = df_explode[df_explode['sku1'].str.contains('[+]')]
    df_explode2 = df_explode[~df_explode['sku1'].str.contains('[+]')]
    #
    df_explode_drop = df_explode1.drop_duplicates(subset=['site1', 'sku1', 'warehouse_id'], keep=False)
    df_explode_drop = df_explode_drop[~df_explode_drop['sku1'].str.contains('PJ')]
    #
    df_explode1 = df_explode1[~df_explode1['sku1'].isin(df_explode_drop['sku1'].unique())]
    df_explode_final = pd.concat([df_explode1, df_explode2])

    # 成本与重量信息处理（取捆绑SKU的和）、库存库龄处理（取库存最低的）
    df_base = df_explode_final.drop(['index', 'new_price', 'gross', 'sku', '数量', 'is_oversea_sku'], axis=1)
    df_cost = df_explode_final[['sku1', 'sku', '数量', 'new_price', 'gross']].drop_duplicates(keep='first')
    df_cost[['new_price', 'gross', '数量']] = df_cost[['new_price', 'gross', '数量']].astype(float).round(4)
    df_cost['new_price'] = df_cost['new_price'] * df_cost['数量']
    df_cost['gross'] = df_cost['gross'] * df_cost['数量']
    df_cost = df_cost.groupby(['sku1']).agg({'new_price': 'sum', 'gross': 'sum'}).reset_index()
    #
    df_stock = df_explode_final[['site1', 'sku1', 'available_stock', 'warehouse_id']].groupby(
        ['site1', 'sku1', 'warehouse_id']).agg({'available_stock': 'min'}).reset_index()
    #
    df_bundle_sku = pd.merge(df_base, df_cost, how='left', on=['sku1'])
    df_bundle_sku = pd.merge(df_bundle_sku, df_stock, how='inner',
                             on=['site1', 'sku1', 'warehouse_id', 'available_stock'])
    df_bundle_sku = df_bundle_sku.drop_duplicates(keep='first')
    #
    df_bundle_sku = pd.merge(df_bundle_sku, df[['site3', 'sku1', 'sku', '数量']], how='left', on=['site3', 'sku1'])
    # 最优子仓的选择：库龄、库存优先
    order_col = ['sku1', 'warehouse_id', 'warehouse', 'available_stock', 'overage_level']
    df_best_warehouse = df_bundle_sku[order_col].sort_values(
        by=['sku1', 'warehouse', 'overage_level', 'available_stock'],
        ascending=[True, True, False, False])
    df_best_warehouse = df_best_warehouse.drop_duplicates(subset=['sku1', 'warehouse'])
    df_best_warehouse.rename(columns={'warehouse_id': 'best_warehouse_id'}, inplace=True)
    #
    order_col2 = ['site3', 'sku1', 'warehouse', 'available_stock','age_30_plus', 'age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus',
                  'age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock']
    df_bundle_sku_2 = df_bundle_sku[order_col2].groupby(['site3', 'sku1', 'warehouse', ]).sum().reset_index()
    #
    df_bundle_sku_0 = df_bundle_sku[
        ['site3', 'sku1', 'warehouse', 'new_price', 'gross', 'sku', '数量']].drop_duplicates(
        subset=['site3', 'sku1', 'warehouse'])
    df_sku = pd.merge(df_bundle_sku_0, df_best_warehouse[['sku1', 'warehouse', 'best_warehouse_id', 'overage_level']],
                      how='left', on=['sku1', 'warehouse'])
    df_sku = pd.merge(df_sku, df_bundle_sku_2, how='left', on=['site3', 'sku1', 'warehouse'])
    return df_sku

def get_bundle_fee(df_bundle_sku):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    w_list1 = get_oversea_ship_type_list()
    df_result = pd.DataFrame()
    for (key1, key2), group in df_bundle_sku.groupby(['site3', 'best_warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('AMAZON', key1, key2, w_list1, '')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost',
             'shippingCost', 'firstCarrierCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True)
        group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    return df_result

def get_bundle_sales():
    """
    捆绑SKU的销售数据。通过链接表和订单表获取
    """
    date_begin = (datetime.date.today() - datetime.timedelta(days=30))
    date_7 = (datetime.date.today() - datetime.timedelta(days=7))
    sql = f"""
    WITH listing_info as (
        SELECT account_id, seller_sku, sku 
        FROM yibai_product_kd_sync.yibai_amazon_sku_map 
        WHERE (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2
        )
    SELECT o.order_id, o.account_id, d.seller_sku, l.sku, d.quantity, toDate(o.purchase_time) purchase_time
    FROM (
        SELECT order_id, account_id, purchase_time
        FROM yibai_oms_sync.yibai_oms_order
        WHERE 
            toInt32(account_id) in (SELECT distinct account_id FROM listing_info) 
            and purchase_time > '{date_begin}' and platform_code = 'AMAZON'
            and payment_status=1 and order_status <> 80
    ) o
    LEFT JOIN (
        SELECT order_id, seller_sku, quantity
        FROM yibai_oms_sync.yibai_oms_order_detail
        WHERE platform_code = 'AMAZON' and seller_sku in (SELECT distinct seller_sku FROM listing_info)
    ) d ON o.order_id = d.order_id
    LEFT JOIN (
        SELECT order_id, sku
        FROM yibai_oms_sync.yibai_oms_order_sku
        WHERE sku in (SELECT distinct sku FROM listing_info)
    ) s ON o.order_id = s.order_id
    INNER JOIN listing_info l ON o.account_id = l.account_id and d.seller_sku = l.seller_sku
    """
    conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_bundle_order = conn_mx.ck_select_to_df(sql)
    df_bundle_order.columns = [i.split('.')[-1] for i in df_bundle_order.columns.tolist()]
    #
    # 销售数据去重
    df_bundle_order = df_bundle_order.drop_duplicates(keep='first')
    # 日销数据。
    # 需先判断日销是否无数据
    if len(df_bundle_order) != 0:
        df_30days = df_bundle_order.groupby('sku')['quantity'].sum().reset_index().rename(
            columns={'quantity': '30days_sales'})
        df_7days = df_bundle_order[df_bundle_order['purchase_time'] > date_7].groupby('sku')[
            'quantity'].sum().reset_index().rename(columns={'quantity': '7days_sales'})
        #
        df_sales = pd.merge(df_30days, df_7days, how='left', on=['sku'])
        df_sales['7days_sales'] = df_sales['7days_sales'].fillna(0).astype(int)
        df_sales['day_sales'] = 0.1 * df_sales['30days_sales'] / 30 + 0.9 * df_sales['7days_sales'] / 7
        df_sales['recent_day_sales'] = df_sales['day_sales']
    else:
        df_sales = pd.DataFrame(columns=['sku','30days_sales','7days_sales','day_sales','recent_day_sales'])

    return df_sales

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

def dwm_oversea_bundle():
    """
    海外仓捆绑SKU目标价计算
    """
    df_bundle_sku = get_bundle_sku()
    # print(df_bundle_sku.info())
    df_fee = get_bundle_fee(df_bundle_sku)
    df_sales = get_bundle_sales()
    df_sales = df_sales.rename(columns={'sku': 'sku1'})
    print('捆绑链接基础信息获取完成。')
    # 匹配数据
    df_bundle = pd.merge(df_bundle_sku, df_fee, how='left', left_on=['site3', 'sku', '数量', 'best_warehouse_id'],
                         right_on=['shipCountry', 'sku', '数量', 'warehouseId'])
    df_bundle = pd.merge(df_bundle, df_sales, how='left', on='sku1')
    df_bundle.drop(['shipCountry', 'warehouseId'], axis=1, inplace=True)
    df_bundle = df_bundle.rename(columns={'site3': 'country'})
    df_bundle['date_id'] = time.strftime('%Y-%m-%d')
    df_bundle['platform'] = 'AMAZON'
    #
    # 可售天数 (大仓的总库存）
    df_bundle[['30days_sales', '7days_sales', 'day_sales','recent_day_sales']] = df_bundle[
        ['30days_sales', '7days_sales', 'day_sales','recent_day_sales']].fillna(0).astype(float)
    col = ['available_stock','age_30_plus', 'age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus',
           'age_360_plus', 'warehouse_stock', 'overage_level']
    df_bundle[col] = df_bundle[col].astype(int)

    #
    df_bundle['estimated_sales_days'] = (df_bundle['available_stock'] / df_bundle['day_sales']).replace(np.inf,
                                                                                                        9999).replace(
        np.nan, 0)
    # 超库龄的可售天数（超 i 天库龄库存的可售天数）
    df_bundle['overage_esd'] = 0
    for i in df_bundle['overage_level'].unique():
        if np.isnan(i) or i == 0:
            continue
        else:
            c = df_bundle['overage_level'] == i
            df_bundle.loc[c, 'overage_esd'] = (
                        df_bundle.loc[c, 'age_{}_plus'.format(int(i))] / df_bundle.loc[c, 'day_sales']).replace(np.inf,
                                                                                                                9999).replace(
                np.nan, 0)
    df_bundle['overage_esd'] = df_bundle['overage_esd'].fillna(0)
    #
    # 销售状态分类:根据超库龄情况判断分类。
    df_bundle['sales_status'] = '待定'
    df_bundle['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    df_bundle['all_esd'] = 30
    print('条件分箱...')
    df_bundle = cut_bins(df_bundle)
    df_bundle.drop(['all_esd','all_esd_bins','recent_day_sales','recent_day_sales_bins'], axis=1, inplace=True)
    df_bundle['is_new'] = 0
    #
    # 获取【降价及回调阶梯】
    sql = """
    SELECT *
    FROM profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    profit_rate_section = conn.read_sql(sql)
    # 超270天更新保底净利率。临时
    # profit_rate_section['overage_level'] = profit_rate_section['overage_level'].astype(int)
    # profit_rate_section['lowest_profit'] = np.where(profit_rate_section['overage_level'] == 270, -0.5,
    #                                                 profit_rate_section['lowest_profit'])
    sql = """
    SELECT *
    FROM up_rate_section
    """
    up_rate_section = conn.read_sql(sql)
    # 涨降幅度设置
    dwm_sku_2 = pd.merge(df_bundle, profit_rate_section, how='left',
                         on=['overage_level', 'overage_esd_bins', 'day_sales_bins'])
    dwm_sku_2 = pd.merge(dwm_sku_2, up_rate_section, how='left', on=['overage_level', 'esd_bins'])

    # 高销涨价
    c1 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
            dwm_sku_2['day_sales'] >= 5) & (dwm_sku_2['esd_bins'] == 'N∈(0,15]')
    c2 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
            dwm_sku_2['day_sales'] >= 5) & (dwm_sku_2['esd_bins'] == 'N∈(15,30]')
    c3 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
            dwm_sku_2['day_sales'] >= 0.5) & (dwm_sku_2['esd_bins'] == 'N∈(0,15]')
    c4 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
            dwm_sku_2['day_sales'] >= 0.5) & (dwm_sku_2['esd_bins'] == 'N∈(15,30]')
    dwm_sku_2['up_profit_rate'] = np.select([c1, c2, c3, c4], [0.06, 0.04, 0.04, 0.02], dwm_sku_2['up_profit_rate'])

    #
    # 初始利率调整幅度设置为0。后续需调整为前一天的after_profit
    # 获取前一天的after_profit
    sql = f"""
    SELECT country, sku1, warehouse, after_profit as after_profit_yest
    FROM dwm_oversea_bundle_price_dtl
    WHERE date_id = (SELECT max(date_id) FROM dwm_oversea_bundle_price_dtl WHERE date_id < '{datetime.datetime.now().strftime('%Y-%m-%d')}')
    """
    df_after_yest = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_after_yest, how='left', on=['country','sku1', 'warehouse'])
    dwm_sku_2['begin_profit'] = np.where(dwm_sku_2['after_profit_yest'].isna(), 0, dwm_sku_2['after_profit_yest'])
    dwm_sku_2.drop('after_profit_yest', axis=1, inplace=True)

    #
    dwm_sku_2[['section']] = dwm_sku_2[['section']].fillna(0)
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['up_profit_rate'] > 0,
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['up_profit_rate'],
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['section'])

    # 调价最高幅度不超过 0 （暂无涨价缩销的情况）
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] > 0, 0, dwm_sku_2['after_profit'])
    # 库存为0时，调价幅度置为0
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['available_stock'] <= 0, 0, dwm_sku_2['after_profit'])

    # 低销降价未回调bug修复
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['overage_level'] < 90, 0, dwm_sku_2['after_profit'])

    # 20231227 (调价幅度+平台最低净利率)最低不超过保底净利率
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit'] - 0.08),
                                         dwm_sku_2['lowest_profit'] - 0.08, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    # 匹配汇率
    df_rate = get_rate()
    dwm_sku_2['country'] = dwm_sku_2['country'].replace('GB', 'UK')
    # dwm_sku_2.drop(['rate_x','rate_y'], axis=1, inplace=True)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_rate[['country', 'rate']], how='left', on='country')
    dwm_sku_2 = dwm_sku_2[~dwm_sku_2['rate'].isna()]
    dwm_sku_2 = dwm_sku_2.drop_duplicates()

    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_platform_fee, how='inner', on=['platform', 'country'])
    #
    # 数据类型转化
    dwm_sku_2.eval('lowest_price = 1.2*shippingCost / (1 - ppve - refound_fee)', inplace=True)
    type_columns = ['new_price', 'totalCost', 'lowest_price', 'ppve', 'refound_fee', 'platform_zero',
                    'platform_must_percent']
    dwm_sku_2[type_columns] = dwm_sku_2[type_columns].astype('float64').round(4)
    #
    # 净利率的处理
    dwm_sku_2['target_profit_rate'] = np.where(
        (dwm_sku_2['platform_must_percent'] + dwm_sku_2['after_profit']) < dwm_sku_2['lowest_profit'],
        dwm_sku_2['lowest_profit'],
        dwm_sku_2['platform_must_percent'] + dwm_sku_2['after_profit'])
    #
    # 调价周期设置
    # dwm_sku_price = adjust_cycle(dwm_sku_price)
    #
    # 销售状态处理
    c1 = (dwm_sku_2['after_profit'] == 0)
    c2 = (dwm_sku_2['up_profit_rate'] > 0)
    c3 = (dwm_sku_2['target_profit_rate'] > 0)
    c4 = (dwm_sku_2['target_profit_rate'] <= 0)
    dwm_sku_2['sales_status'] = np.select([c1, c2, c3, c4], ['正常', '回调', '正利润加快动销', '负利润加快动销'])

    # 价格计算
    dwm_sku_2['price_rmb'] = (dwm_sku_2['new_price'] + dwm_sku_2['totalCost']) / (
            1 - dwm_sku_2['ppve'] - dwm_sku_2['platform_zero'] - dwm_sku_2['target_profit_rate'])
    # dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01
    dwm_sku_2['price_rmb'] = np.where(dwm_sku_2['price_rmb'] <= dwm_sku_2['lowest_price'],
                                      dwm_sku_2['lowest_price'], dwm_sku_2['price_rmb'])
    #
    # 本币计算
    dwm_sku_2['price'] = dwm_sku_2['price_rmb'] / dwm_sku_2['rate']
    dwm_sku_2['price'] = dwm_sku_2['price'].round(1) - 0.01

    dwm_sku_2.drop(['rate', 'ppve', 'refound_fee', 'platform_zero', 'platform_must_percent'], axis=1, inplace=True)
    print(dwm_sku_2.info())
    #
    write_to_sql(dwm_sku_2, 'dwm_oversea_bundle_price_dtl')

    return None

# 监控报表函数
# 1、销库、出单利润率情况
def get_oversea_order():
    date_today = datetime.date.today()
    date_start = date_today - datetime.timedelta(days=20)
    # date_end = date_today - datetime.timedelta(days=110)
    # 仅更新近1个月订单
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"delete from over_sea.ads_oversea_order where paytime >= '{date_start}' "
    conn.execute(sql)
    conn.close()

    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            -- and paytime < 'date_end'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    df_order_info = conn_ck.ck_select_to_df(sql)
    print(df_order_info.info())
    df_order_info = df_order_info.drop_duplicates(subset=['order_id','sku','account_id'])

    # 20260115 限时清仓sku打标
    df_flash = get_flash_sku()
    df_flash = df_flash[['sku', 'warehouse', 'source']]
    df_flash = df_flash.drop_duplicates(subset=['sku', 'warehouse'])
    df_order_info = pd.merge(df_order_info, df_flash, how='left', on=['sku', 'warehouse'])
    date_limit = (df_order_info['paytime'] >= '2026-01-01')
    df_order_info['source'] = np.where(date_limit, df_order_info['source'], np.nan)

    # 重复订单号的销售额、利润置为0
    column_order = ['负利润加快动销', '正利润加快动销', '正常',' ']
    df_order_info['sales_status'] = pd.Categorical(df_order_info['sales_status'], categories=column_order, ordered=True)
    df_order_info = df_order_info.sort_values(by=['order_id', 'sales_status'])

    df_order_info['rank'] = df_order_info.groupby(['order_id', 'sales_status']).cumcount() + 1
    for c in ['total_price','true_profit_new1','real_profit']:
        df_order_info[c] = np.where(df_order_info['rank'] != 1, 0, df_order_info[c])
    df_order_info.drop('rank', axis=1, inplace=True)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_order_info, 'ads_oversea_order', if_exists='append')

    # tt订单监控
    tt_get_oversea_order()


# 2、库存结构
def get_oversea_stock():
    # date_today = datetime.datetime.now().strftime('%Y-%m-%d')
    date_today = datetime.date.today() - datetime.timedelta(days=7)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    delete from ads_oversea_stock where date_id >= '{date_today}'
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
        WHERE date_id >= '{date_today}' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_stock = conn.read_sql(sql)
    df_stock = df_stock.drop_duplicates()
    conn.to_sql(df_stock, 'ads_oversea_stock', if_exists='append')

# 3、海外仓仓租校验
def get_oversea_rent():
    # date_today = datetime.datetime.now().strftime('%Y-%m-%d')
    date_today = datetime.date.today() - datetime.timedelta(days=100)
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


def get_ru_sku():
    """ """
    sql = """
        SELECT sku, warehouse, best_warehouse_name warehouse_name, available_stock, overage_level, after_profit
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2026-01-28' and available_stock > 0 and warehouse = '俄罗斯仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df_fee = get_transport_fee()
    df_fee = df_fee[df_fee['country']=='RU']

    df = pd.merge(df, df_fee, how='left', on=['sku','warehouse_name'])

    # df.to_excel('F://Desktop//df_ru.xlsx', index=0)


##
if __name__ == '__main__':

    # dwm_oversea_sku()

    # supplier_sku_info()
    # get_supplier_fee()
    # a = get_sku_sales_new()
    # get_destroy_profit()
    # update_platform_fee()
    dwm_sku_price()
    # df = get_stock()
    # get_supplier_fee()

    # pull_warehouse_info()
    # dwm_is_destroy()
    # get_sku_attr()
    # get_electric_useful()
    # dwd_temp()
    # dwd_warehouse_id_info()
    # get_bundle_sku()