"""
海外仓调价程序测试版
"""
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
    # 20250321 隔离精铺非转泛品的sku
    c0 = df_sku_stock['type'].str.contains('转泛品') | df_sku_stock['type'].str.contains('转VC')
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺')) & (~c0)
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)

    return df_sku_stock

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
        case when inventory_age >= 240 then warehouse_stock else 0 end as age_240_plus,
        case when inventory_age >= 270 then warehouse_stock else 0 end as age_270_plus,
        case when inventory_age >= 300 then warehouse_stock else 0 end as age_300_plus,
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
                                      'age_30_plus','age_60_plus','age_90_plus','age_120_plus', 'age_150_plus',
                                      'age_180_plus','age_210_plus','age_240_plus', 'age_270_plus', 'age_300_plus','age_360_plus']]
    df_stock_age_info.loc[:,'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby([
        'sku', 'warehouse_id']).agg({'warehouse_stock':'sum','inventory_age':'max','charge_total_price':'sum',
                                     'age_30_plus':'sum','age_60_plus':'sum','age_90_plus':'sum','age_120_plus':'sum',
                                     'age_150_plus':'sum','age_180_plus':'sum','age_210_plus':'sum','age_240_plus':'sum',
                                     'age_270_plus':'sum','age_300_plus':'sum','age_360_plus':'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus','age_60_plus','age_90_plus',
                               'age_120_plus', 'age_150_plus', 'age_180_plus','age_210_plus','age_240_plus', 'age_270_plus', 'age_300_plus','age_360_plus']]
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
        c10 = df['age_300_plus'] > 0
        c2 = df['age_270_plus'] > 0
        c20 = df['age_240_plus'] > 0
        c21 = df['age_210_plus'] > 0
        c3 = df['age_180_plus'] > 0
        c4 = df['age_150_plus'] > 0
        c5 = df['age_120_plus'] > 0
        c6 = df['age_90_plus'] > 0
        c7 = df['age_60_plus'] > 0
        c8 = df['age_30_plus'] > 0
        df['overage_level'] = np.select([c1,c10, c2,c20,c21, c3, c4, c5, c6,c7,c8], [360, 300, 270,240,210, 180, 150, 120, 90,60,30],0)
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
        sum(age_210_plus) age_210_plus,sum(age_240_plus) age_240_plus,
        sum(age_270_plus) age_270_plus, sum(age_300_plus) age_300_plus,sum(age_360_plus) age_360_plus
    FROM (
        SELECT
            sku, cargo_owner_id, saas_stock, stock_age,
            case
                when stock_age >= 30 and stock_age < 60 then 30
                when stock_age >= 60 and stock_age < 90 then 60
                when stock_age >= 90 and stock_age < 120 then 90
                when stock_age >= 120 and stock_age < 150 then 120
                when stock_age >= 150 and stock_age < 180 then 150
                when stock_age >= 180 and stock_age < 210 then 180
                when stock_age >= 210 and stock_age < 240 then 210
                when stock_age >= 240 and stock_age < 270 then 240
                when stock_age >= 270 and stock_age < 300 then 270
                when stock_age >= 300 and stock_age < 360 then 300
                when stock_age >= 360 then 360
            else 0 end as overage_level, 
            case when stock_age >= 30 then saas_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then saas_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then saas_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then saas_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then saas_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then saas_stock else 0 end as age_180_plus,
            case when stock_age >= 210 then saas_stock else 0 end as age_210_plus,
            case when stock_age >= 240 then saas_stock else 0 end as age_240_plus,
            case when stock_age >= 270 then saas_stock else 0 end as age_270_plus,
            case when stock_age >= 300 then saas_stock else 0 end as age_300_plus,
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
    col_list = ['age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus',
                'age_210_plus', 'age_240_plus', 'age_270_plus','age_300_plus',
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
         'age_150_plus','age_180_plus', 'age_210_plus', 'age_240_plus', 'age_270_plus', 'age_300_plus', 'age_360_plus',
         'warehouse_stock_age', 'charge_currency', 'overage_level',
         'best_warehouse_id', 'best_warehouse_name']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    return df_mx

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
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate['country'] = np.where(df_rate['charge_currency']=='HUF', 'HU', df_rate['country'])
    # df_rate = df_rate.drop_duplicates(subset='charge_currency')
    return df_rate

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

def flash_clearout(df):
    """ 超180天库龄sku，优先限时清仓逻辑 """
    sql = """
    SELECT *
    FROM over_sea.flash_clear_profit_rate
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    flash_clear = conn.read_sql(sql)

    df = pd.merge(df, flash_clear, how='left',
                         on=['overage_level', 'recent_day_sales_bins', 'all_esd_bins'])
    df['section'] = np.where(df['section_2'].isna(), df['section'], df['section_2'])
    c1 = df['3days_sales'] > (0.6*df['7days_sales'])
    df['section'] = np.where(c1, 0, df['section'])
    df['lowest_profit'] = np.where(df['lowest_profit_2'].isna(), df['lowest_profit'], df['lowest_profit_2'])

    df.drop(['recent_day_sales_bins','all_esd_bins','section_2','lowest_profit_2','all_esd'], axis=1, inplace=True)

    return df

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


def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')
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
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_mx_age = get_mx_stock_age()
    df_stock_age_warehouse = pd.concat([df_stock_age_warehouse, df_mx_age])
    print('获取日销信息...')
    sku_sales = get_sku_sales_new()

    col = ['sku','warehouse','3days_sales','7days_sales','30days_sales','60days_sales','90days_sales','recent_day_sales','day_sales']
    dwm_sku = pd.merge(df_stock, sku_sales[col], how='left', on=['sku', 'warehouse'])
    # dwm_sku.info()
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
    dwm_sku.drop(['max_id','max_name'], axis=1, inplace=True)
    # 仓租数据，汇率转化
    df_rate = get_rate()
    df_rate = df_rate.drop_duplicates(subset='charge_currency')
    dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)

    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus', 'age_60_plus','age_90_plus',
                     'age_120_plus','age_150_plus', 'age_180_plus','age_210_plus', 'age_240_plus',  'age_270_plus','age_300_plus',  'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '30days_sales', '60days_sales','90days_sales', 'day_sales', 'recent_day_sales', ]
    dwm_sku[['best_warehouse_id']] = dwm_sku[['best_warehouse_id']].astype(int)
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
    # # 超270天更新保底净利率。临时
    # profit_rate_section['overage_level'] = profit_rate_section['overage_level'].astype(int)
    # profit_rate_section['lowest_profit'] = np.where(profit_rate_section['overage_level']==270, -0.5, profit_rate_section['lowest_profit'] )

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
    # 20250422 低销sku兜底下调10%
    # 20250606 低销sku兜底下调20%
    # 20250616 低销sku兜底下调30%
    # c1 = dwm_sku_2['day_sales'] < 0.1
    # dwm_sku_2['lowest_profit'] = np.where(c1, dwm_sku_2['lowest_profit']-0.3, dwm_sku_2['lowest_profit'])
    dwm_sku_2['lowest_profit'] = np.where((dwm_sku_2['warehouse'].str.contains('乌拉圭')), -999, dwm_sku_2['lowest_profit'])
    # 20250516 东南亚仓兜底手动设置-50%
    col = ['泰国仓','越南仓','菲律宾仓','马来西亚仓','印度尼西亚仓']
    dwm_sku_2['lowest_profit'] = np.where(dwm_sku_2['warehouse'].isin(col), -0.5, dwm_sku_2['lowest_profit'])
    # 高销涨价
    # c1 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #             dwm_sku_2['recent_day_sales'] >= 3) & (dwm_sku_2['esd_bins'].isin(['N∈(0,5]', 'N∈(5,10]', 'N∈(10,20]', 'N∈(20,30]']))
    # c2 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #             dwm_sku_2['recent_day_sales'] >= 3) & (dwm_sku_2['esd_bins'].isin(['N∈(30,40]', 'N∈(40,60]']))
    # c3 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #             dwm_sku_2['recent_day_sales'] >= 1) & (dwm_sku_2['recent_day_sales'] < 3) & (
    #         dwm_sku_2['esd_bins'].isin(['N∈(0,5]', 'N∈(5,10]', 'N∈(10,20]', 'N∈(20,30]']))
    # c4 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #         dwm_sku_2['recent_day_sales'] >= 1) & (dwm_sku_2['recent_day_sales'] < 3) & (
    #          dwm_sku_2['esd_bins'].isin(['N∈(30,40]', 'N∈(40,60]']))
    # c5 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #             dwm_sku_2['recent_day_sales'] >= 0.3) & (dwm_sku_2['recent_day_sales'] < 1) & (
    #         dwm_sku_2['esd_bins'].isin(['N∈(0,5]', 'N∈(5,10]', 'N∈(10,20]', 'N∈(20,30]']))
    # c6 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
    #         dwm_sku_2['recent_day_sales'] >= 0.3) & (dwm_sku_2['recent_day_sales'] < 1) & (
    #          dwm_sku_2['esd_bins'].isin(['N∈(30,40]', 'N∈(40,60]']))
    # dwm_sku_2['up_profit_rate'] = np.select([c1,c2,c3,c4,c5,c6], [0.08,0.06,0.05,0.03,0.03,0.02], dwm_sku_2['up_profit_rate'])

    # 低销降价
    # 20241213 取消低销降价逻辑
    # c1 = (dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (dwm_sku_2['available_stock'] >= 3) & \
    # ((dwm_sku_2['30days_sales'] == 0) | (dwm_sku_2['60days_sales'] < 3)) & (dwm_sku_2['overage_level'] >= 30)
    # c2 = (dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (dwm_sku_2['available_stock'] < 3) & \
    #      (dwm_sku_2['60days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 30)
    # dwm_sku_2['section'] = np.select([c1, c2], [-0.02, -0.02], dwm_sku_2['section'])

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
    # # 2024-12-04 近30天无销的可以无视兜底
    # c1 = (dwm_sku_2['30days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 30)
    # c2 = (dwm_sku_2['begin_profit'] <= (dwm_sku_2['lowest_profit']-0.08))
    # dwm_sku_2['section'] = np.where(c1 & c2, -0.03, dwm_sku_2['section'])
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['up_profit_rate'] > 0,
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['up_profit_rate'],
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['section'])
    # 20241206 调价周期设置在sku+大仓位置
    print(f'调价周期函数前共{len(dwm_sku_2)}条数据。')
    # dwm_sku_2 = adjust_cycle_new(dwm_sku_2)
    print(f'调价周期函数后，共{len(dwm_sku_2)}条数据。')


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
    conn.to_sql(dwm_sku_2, 'dwm_sku_temp_info_1', if_exists='replace')
    # write_to_sql(dwm_sku_2, 'dwm_sku_temp_info_1')
    # dwm_sku_2 = dwm_sku_2[dwm_sku_2['available_stock']>0]
    # dwm_sku_2.to_excel('F://Desktop//dwm_sku_2.xlsx', index=0)



    return None


if __name__ == '__main__':
    dwm_oversea_sku()