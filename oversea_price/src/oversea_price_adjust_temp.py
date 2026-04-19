"""
海外仓临时调价需求
"""

import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_line, get_sku_sales_new,  \
    get_rate,get_platform_fee, write_to_sql
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse, tt_get_oversea_order
warnings.filterwarnings("ignore")

# 海兔sku调价
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
                -- and warehouse_other_type = 2 
                and warehouse_name not like '%独享%'
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
    # 筛选海兔转泛品产品
    c1 = df_sku_stock['type'].str.contains('海兔|易通兔')
    df_sku_stock = df_sku_stock[c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)

    return df_sku_stock

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
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id, warehouse, available_stock, warehouse_name
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 
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
    # 海兔sku手动置为清仓状态
    c1 = (dwm_sku['type'].str.contains('海兔|易通兔'))
    dwm_sku['overage_level'] = np.where(c1 & (dwm_sku['available_stock']>0),
                                        180, dwm_sku['overage_level'])
    for i in ['age_30_plus','age_60_plus','age_90_plus','age_120_plus','age_150_plus','age_180_plus']:
        dwm_sku[i] = np.where(c1, dwm_sku['available_stock'], dwm_sku[i])

    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['warehouse_id','create_time'], axis=1, inplace=True)
    # dwm_sku.to_excel('F://Desktop//dwm_sku.xlsx', index=0)

    return dwm_sku


def get_transport_fee():
    """
    获取当前最新运费数据
    """
    sql = """
    SELECT 
        sku, warehouseId as best_warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost_origin,
        firstCarrierCost, dutyCost, (totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE warehouse = '美国仓' and country = 'US'
    
    UNION ALL
    
    SELECT 
        sku, warehouseId as best_warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, totalCost_origin,
        firstCarrierCost, dutyCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_temu
    WHERE warehouse = '美国仓' and country = 'US'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    df = df.drop_duplicates()

    df.drop(['warehouse', 'totalCost_origin',  'dutyCost'], axis=1, inplace=True)

    df['best_warehouse_id'] = df['best_warehouse_id'].astype(int)

    return df

def dwm_sku_price():
    # 读取dwm_sku_2
    dwm_sku = dwm_oversea_sku()
    print(dwm_sku.info())

    # 匹配运费
    # 运费数据需要处理！！！
    # WISH平台的汇率设置为1：
    df_transport_fee = get_transport_fee()

    print(df_transport_fee.info())
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='left', on='country')
    df_transport_fee = df_transport_fee[
        ['sku', 'best_warehouse_id', 'ship_name', 'total_cost','ship_fee','firstCarrierCost', 'lowest_price', 'platform', 'country',
         'rate']].drop_duplicates()

    # order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
    #            'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
    #            'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate',
    #            'is_adjust','begin_profit']

    df = pd.merge(dwm_sku, df_transport_fee, how='left', on=['sku', 'best_warehouse_id'])
    # 无运费数据处理
    # dwm_sku_price.drop(['warehouse_id'], axis=1, inplace=True)
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]
    # 无汇率国家处理：WISH平台且有库存的SKU的汇率设置为1
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['rate'].isna()]
    df = df.drop_duplicates()
    # 匹配差值表
    df_platform_fee = get_platform_fee()
    df = pd.merge(df, df_platform_fee, how='left', on=['platform', 'country'])

    #
    # 计算目标价：销毁价对应利润率+30%
    df['lowest_profit'] = 1 - df['ppve'] - df['refound_fee'] - \
                          (df['new_price']+df['total_cost'])/df['lowest_price']
    df['final_profit'] = np.where(df['lowest_profit']+0.3 > 0, 0, df['lowest_profit']+0.3)

    df['price_rmb'] = (df['new_price']+df['total_cost'])/(1-df['ppve']-df['final_profit'])
    df['price'] = df['price_rmb']/df['rate']

    df = df.replace([np.inf, -np.inf], np.nan)
    df['date_id'] = time.strftime('%Y-%m-%d')
    print(df.info())
    # write_to_sql(df, 'dwm_sku_price_haitu')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'dwm_sku_price_haitu', if_exists='replace')


    df.to_excel('F://Desktop//dwm_sku_price.xlsx', index=0)

def get_new_last_date():
    # 获取当前日期、上一次调价日期
    time_today = time.strftime('%Y-%m-%d')
    sql = f"""
    SELECT max(date_id)
    FROM dwm_sku_price_haitu
    WHERE date_id < '{time_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_max_date = conn.read_sql(sql=sql)
    time_last = df_max_date.iloc[0, 0]
    return time_today, time_last

def zj_qujian(df, col='涨降幅度'):
    df['涨降幅区间'] = 'L.1~'
    df.loc[(df[col] < -1), '涨降幅区间'] = 'A.-1~'
    df.loc[(df[col] >= -1) & (df[col] < -0.5), '涨降幅区间'] = 'B.-1~-0.5'
    df.loc[(df[col] >= -0.5) & (df[col] < -0.2), '涨降幅区间'] = 'C.-0.5~-0.2'
    df.loc[(df[col] >= -0.2) & (df[col] < -0.1), '涨降幅区间'] = 'D.-0.2~-0.1'
    df.loc[(df[col] >= -0.1) & (df[col] < -0.05), '涨降幅区间'] = 'E.-0.1~-0.05'
    df.loc[(df[col] >= -0.05) & (df[col] < 0), '涨降幅区间'] = 'F.-0.05~0'

    df.loc[(df[col] >= 0) & (df[col] < 0.05), '涨降幅区间'] = 'G.0~0.05'
    df.loc[(df[col] > 0.05) & (df[col] <= 0.1), '涨降幅区间'] = 'H.0.05~0.1'
    df.loc[(df[col] > 0.1) & (df[col] <= 0.2), '涨降幅区间'] = 'I.0.1~0.2'
    df.loc[(df[col] > 0.2) & (df[col] <= 0.5), '涨降幅区间'] = 'J.0.2~0.5'
    df.loc[(df[col] > 0.5) & (df[col] <= 1), '涨降幅区间'] = 'K.0.5~1'
    return df

def lv_qujian(df, col='今天较上次定价利润率涨降', new_col='今天较上次定价利润率涨降分段'):

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


def contrast_info(df):
    # 获取当前日期、上一次调价日期
    time_today, time_last = get_new_last_date()

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 价格涨降幅度
    df['涨降幅度'] = 999
    df['涨降幅度'] = np.where(df['online_price'] > 0, (df['price'] - df['online_price']) / df['online_price'], df['涨降幅度'])
    # df.loc[df['online_price'] > 0, '涨降幅度'] = (df['price'] - df['online_price']) / df['online_price']
    df = zj_qujian(df)

    # 计算在线价格的实际毛利率
    col = ['new_price','ppve','total_cost','online_price','rate','target_profit_rate']
    df[col] = df[col].astype(float)
    # print(df.info())
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
                df['online_price'] * df['rate'])
    df['今天较当前在线利润率涨降'] = df['target_profit_rate'] - df['online_profit']
    df = lv_qujian(df,'今天较当前在线利润率涨降', '今天较当前在线利润率涨降分段')

    return df

def get_walmart_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    sql = f"""
        SELECT * 
        FROM yibai_walmart_oversea_listing_price 
        WHERE DATE = '{now_time}'
    """
    df_walmart_listing = conn.read_sql(sql)
    df_walmart_listing.drop('DATE', axis=1, inplace=True)
    df_walmart_listing['site'] = df_walmart_listing['site'].str.upper()
    df_walmart_listing = df_walmart_listing.rename(columns={'site': 'country'})
    df_walmart_listing['account_id'] = df_walmart_listing['account_id'].astype(int)
    print(df_walmart_listing.info())
    # 获取SKU价格信息
    # 筛选库存数大于4的
    sql = f"""
        SELECT 
            SKU as sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level,
            available_stock, '负利润加快动销' sales_status, price, final_profit target_profit_rate, 
            day_sales, country, ship_name,lowest_price, ppve, rate
        FROM over_sea.dwm_sku_price_haitu
        WHERE platform = 'WALMART' and date_id = '{now_time}'
    """
    df_walmart_dtl = conn.read_sql(sql)

    # 合并链接与价格数据
    df_walmart_listing = df_walmart_listing.rename(columns={'price':'online_price'})
    df = pd.merge(df_walmart_listing, df_walmart_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)

    # df.to_excel('F://Desktop//df_walmart_haitu.xlsx', index=0)
    # pass

    # 检查
    df = contrast_info(df)

    df.to_excel('F://Desktop//df_walmart_check.xlsx', index=0)


def get_amazon_listing():
    """ amazon海兔sku """
    now_time = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            SKU as sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level,
            available_stock, '负利润加快动销' sales_status, price, final_profit target_profit_rate, 
            day_sales, country, ship_name,lowest_price, ppve, rate
        FROM over_sea.dwm_sku_price_haitu
        WHERE platform = 'AMAZON' and date_id = '{now_time}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    sku_list = tuple(df_sku['sku'].unique())

    sql = f""" 
    with listing_table as (
        select distinct account_id, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where sku in {sku_list}
    )

    select b.account_id as account_id, b.account_name as account_name, 
        group_name, short_name, 'AMAZON' as platfrom,deliver_mode,
        if(b.site ='sp', 'es', b.site) as site,
        status, e.sku as sku, a.seller_sku as seller_sku,  
        open_date,
        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
        a.price AS your_price, fulfillment_channel, f.sale_price as sale_price, a.price as online_price
    from (
        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a 
    inner join ( 
        select account_id, sku, seller_sku, deliver_mode
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where sku in {sku_list}
    ) e
    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    left join (
        select account_id, seller_sku, asin1 
        from yibai_product_kd_sync.yibai_amazon_listing_alls 
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) t
    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
    inner join (
        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
        and account_id in (
            select distinct toInt32(account_id) from yibai_account_manage_sync.yibai_amazon_account
            where account_type = 1 and is_yibai =1 )
    ) b
    on (a.account_id= b.account_id)
    inner join (
        select group_id, group_name
        from yibai_system_kd_sync.yibai_amazon_group 
        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    ) c 
    on (b.group_id=c.group_id)
    left join (
        select account_id, seller_sku, ListingPrice as sale_price
        from yibai_product_kd_sync.yibai_amazon_listing_price 
        where  (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) f
    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
    order by a.create_time desc limit 1
    by a.account_id, a.seller_sku
    settings max_memory_usage = 20000000000
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df_all = conn_ck.ck_select_to_df(sql)
    print(df_all.info())

    df_all['date_id'] = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_all, 'haitu_sku_amazon_listing', if_exists='replace')

def get_amazon_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    sql = f"""
        SELECT * 
        FROM over_sea.haitu_sku_amazon_listing 
        WHERE date_id = '{now_time}'
    """
    df_listing = conn.read_sql(sql)
    df_listing['site'] = df_listing['site'].str.upper()
    df_listing = df_listing.rename(columns={'site': 'country'})
    df_listing['account_id'] = df_listing['account_id'].astype(int)
    df_listing['online_price'] = df_listing['online_price'].astype(float)
    print(df_listing.info())
    # 获取SKU价格信息
    sql = f"""
        SELECT 
            SKU as sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level,
            available_stock, '负利润加快动销' sales_status, price, final_profit target_profit_rate, 
            day_sales, country, ship_name,lowest_price, ppve, rate
        FROM over_sea.dwm_sku_price_haitu
        WHERE platform = 'AMAZON' and date_id = '{now_time}'
    """
    df_dtl = conn.read_sql(sql)

    # 合并链接与价格数据
    df = pd.merge(df_listing, df_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)

    # df.to_excel('F://Desktop//df_walmart_haitu.xlsx', index=0)
    # pass

    # 检查
    df = contrast_info(df)

    df.to_excel('F://Desktop//df_amazon_check.xlsx', index=0)

if __name__ == '__main__':
    # get_stock()
    # dwm_oversea_sku()

    dwm_sku_price()

    # get_walmart_adjust_listing()

    # get_amazon_listing()
    # get_amazon_adjust_listing()