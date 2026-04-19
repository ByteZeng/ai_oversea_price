##
import pandas as pd
import numpy as np
import time, datetime
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from concurrent.futures._base import as_completed
import warnings
# from sqlalchemy import create_engine
warnings.filterwarnings("ignore")
from pulic_func.base_api.base_function import get_token, mysql_escape
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_stock,get_stock_age
from all_auto_task.oversea_listing_detail_2023 import xiaozu_dabu
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from all_auto_task.oversea_listing_detail_2023 import amazon_coupon_fu, get_amazon_coupon,get_amazon_promotion,get_white_listing,get_fba_asin
import src.fetch_data as fd

##
# 新增listing维度的日销数据，用于修改【正常品且日销>0.1】不降价逻辑
# 订单时间范围
def get_listing_day_sales():
    """
    获取链接维度的日销数据
    """
    now = datetime.datetime.now()
    date_7 = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    date_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    #
    # 近30天销量、近7天销量
    sql = f"""
    
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, warehouse,
            quantity, release_money, sales_status
        FROM over_sea.dashbord_new1
        WHERE 
            paytime >= '{date_30}' and paytime < '{now}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                         db_name='over_sea')
    df_order_info = ck_client.ck_select_to_df(sql)
    #
    # df_order_info.to_excel('df_order_info.xlsx', index=0)
    # 30天销量数据
    df_amazon = df_order_info[df_order_info['platform_code']=='AMAZON'].groupby(['seller_sku','account_id'])['quantity'].sum().rename('30_days_sales').reset_index()
    # 7天销量数据
    df_amazon_7 = df_order_info[(df_order_info['platform_code']=='AMAZON') & (df_order_info['paytime'] >= date_7)]
    df_amazon_7 = df_amazon_7.groupby(['seller_sku','account_id'])['quantity'].sum().rename('7_days_sales').reset_index()
    df_amazon = pd.merge(df_amazon, df_amazon_7, how='left', on=['seller_sku','account_id'])
    #
    df_amazon['7_days_sales'] = df_amazon['7_days_sales'].fillna(0).astype(int)
    df_amazon['listing_day_sales'] = 0.9 * df_amazon['7_days_sales']/7 + 0.1 * df_amazon['30_days_sales']/30
    df_amazon['listing_day_sales'] = df_amazon['listing_day_sales'].astype(float).round(4)
    df_amazon = df_amazon[['seller_sku', 'account_id','listing_day_sales']]

    return df_amazon

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
                ,ps.type as type,ps.available_stock available_stock ,if(empty(yps.new_price),yps1.new_price,yps.new_price) as new_price
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
                where warehouse_other_type != 3
                order by available_stock desc
                ) ps
            left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            having new_price<>''
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

    return df_sku_stock

# 获取SKU的库龄数据
def get_stock_age():
    """
    获取库龄数据
    处理库龄数据
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = '''
    SELECT  
        sku, charge_currency, cargo_type, ya.warehouse_code,warehouse_id, warehouse_name,
        case
            when country = 'US' then '美国仓'
            when country in ('UK', 'GB') then '英国仓'
            when country in ('CZ', 'CS', 'DE') then '德国仓'
            when country = 'FR' then '法国仓'
            when country = 'IT' then '意大利仓'
            when country = 'AU' then '澳洲仓'
            when country in ('ES', 'SP') then '西班牙仓'
            when country = 'CA' then '加拿大仓'
            when country = 'JP' then '日本仓'
            when country = 'PL' then '德国仓'
            when country = 'MX' then '墨西哥仓'
            else '美国仓'
        end as warehouse,
        warehouse_stock, inventory_age, charge_total_price, 
        case when inventory_age >= 60 then warehouse_stock else 0 end as age_60_plus,
        case when inventory_age >= 90 then warehouse_stock else 0 end as age_90_plus,
        case when inventory_age >= 120 then warehouse_stock else 0 end as age_120_plus,
        case when inventory_age >= 150 then warehouse_stock else 0 end as age_150_plus,
        case when inventory_age >= 180 then warehouse_stock else 0 end as age_180_plus,
        case when inventory_age >= 270 then warehouse_stock else 0 end as age_270_plus,
        case when inventory_age >= 360 then warehouse_stock else 0 end as age_360_plus
    FROM yb_datacenter.yb_oversea_sku_age ya
    LEFT JOIN (SELECT warehouse_code, warehouse_id, warehouse_name FROM yb_datacenter.v_warehouse_erp) ve
    ON ya.warehouse_code = ve.warehouse_code
    WHERE date = formatDateTime(subtractDays(now(),2), '%Y-%m-%d') and status in (0,1)
    '''
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0
    """
    df_stock = ck_client.ck_select_to_df(sql)
    df_stock_age = pd.merge(df_stock_age, df_stock, how='inner', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse_id 聚合
    df_stock_age['charge_total_price'] = df_stock_age['charge_total_price'].astype('float')
    df_stock_age_base = df_stock_age[
        ['sku', 'charge_currency', 'cargo_type', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    df_stock_age_info = df_stock_age[
        ['sku', 'warehouse_id', 'warehouse', 'warehouse_stock', 'inventory_age', 'charge_total_price', 'age_60_plus',
         'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']]
    df_stock_age_info.loc[:, 'stock_age'] = df_stock_age_info['warehouse_stock'].astype(str).str.cat(
        df_stock_age_info['inventory_age'].astype(str), sep=':')
    df_stock_age_temp = df_stock_age_info[['sku', 'warehouse_id', 'warehouse', 'stock_age']].groupby(
        ['sku', 'warehouse_id', 'warehouse']).agg({'stock_age': set}).reset_index()
    df_stock_age_temp['warehouse_stock_age'] = df_stock_age_temp['warehouse_id'].apply(str) + ':' + df_stock_age_temp[
        'stock_age'].apply(str)

    df_stock_age_id = df_stock_age_info.groupby(['sku', 'warehouse_id']).agg(
        {'warehouse_stock': 'sum', 'inventory_age': 'max', 'charge_total_price': 'sum', 'age_60_plus': 'sum',
         'age_90_plus': 'sum',
         'age_120_plus': 'sum', 'age_150_plus': 'sum', 'age_180_plus': 'sum', 'age_270_plus': 'sum',
         'age_360_plus': 'sum'}).reset_index()
    df_stock_age_id = pd.merge(df_stock_age_base, df_stock_age_id, how='left', on=['sku', 'warehouse_id'])
    df_stock_age_id = pd.merge(df_stock_age_id,
                               df_stock_age_temp[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock_age']],
                               how='left', on=['sku', 'warehouse_id'])

    # 按 sku + warehouse 聚合
    df_temp = df_stock_age_id[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_60_plus', 'age_90_plus',
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
        df['overage_level'] = np.select([c1, c2, c3, c4, c5, c6, c7], [360, 270, 180, 150, 120, 90, 60])
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

# 海外仓仓库信息同步到over_sea
def pull_warehouse_info():
    """
    将CK的海外仓仓库信息拉取同步到over_sea中。
    数据来源：yb_datacenter.v_oversea_stock
    """
    sql = """
    SELECT
        id as warehouse_id, name as warehouse_name, code as warehouse_code, type, country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country='UK' THEN '英国仓'
            WHEN country='GB' THEN '英国仓'
            WHEN country='CS' THEN '德国仓'
            WHEN country='FR' THEN '法国仓'
            WHEN country='IT' THEN '意大利仓'
            WHEN country='AU' THEN '澳洲仓'
            WHEN country='ES' THEN '西班牙仓'
            WHEN country='CA' THEN '加拿大仓'
            WHEN country='DE' THEN '德国仓'
            WHEN country='JP' THEN '日本仓'
            WHEN country='PL' THEN '德国仓'
            WHEN country='MX' THEN '墨西哥仓'
            WHEN country='UY' THEN '乌拉圭仓'
            WHEN country='BR' THEN '巴西仓'
            WHEN country='RU' THEN '俄罗斯仓'
            ELSE NULL 
        END AS warehouse
    FROM yb_datacenter.yb_warehouse
    WHERE type IN ('third', 'overseas')
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df = ck_client.ck_select_to_df(sql)

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_warehouse_oversea_temp', if_exists='replace')

# SKU的日销统计
def get_sku_sales():
    sql = """

    SELECT  
        SKU as sku,SUM(3days_sales) as 3days_sales,SUM(7days_sales) as 7days_sales,SUM(30days_sales) as 30days_sales,SUM(90days_sales) as 90days_sales,
        SUM(3days_sales)/3*0.9+SUM(7days_sales)/7*0.1 AS 'recent_day_sales',
        SUM(7days_sales)/7*0.9+SUM(30days_sales)/30*0.1 AS 'day_sales', 
        warehouse
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        )A 
    GROUP BY SKU, warehouse

    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    return df_sales

# 汇率
def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='domestic_warehouse_clear')
    df_rate = conn_ck.ck_select_to_df(sql)

    return df_rate

# 运费数据获取
def get_transport_fee():
    """
    获取当前最新运费数据
    """
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost as total_cost, 
        shipName as ship_name,lowest_price, platform, country
    FROM oversea_transport_fee_useful
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

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
    and warehouse_id in ( SELECT distinct id FROM yb_warehouse WHERE type IN ('third', 'overseas') )
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df_old_sku = ck_client.ck_select_to_df(sql)

    sql = """
        select distinct sku, warehouse_id as best_warehouse_id
        from yb_datacenter.yb_stock 
        where create_time>=subtractDays(now(),180) and available_stock > 0  and cargo_owner_id = 8
        and warehouse_id in ( SELECT distinct id FROM yb_warehouse WHERE type IN ('third', 'overseas') )
    """
    df_new_sku = ck_client.ck_select_to_df(sql)

    df_new = pd.merge(df_new_sku['sku'], df_old_sku, how='left', on=['sku'])
    df_new = df_new[df_new['best_warehouse_id'].isna()]
    df_new['is_new'] = 1
    df_new.drop('best_warehouse_id', axis=1, inplace=True)
    df_new = df_new.drop_duplicates()
    return df_new
#
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

    # 当前库存的可售天数主要用于【回调】
    df['esd_bins'] = pd.cut(df['estimated_sales_days'], bins=[-1, 5, 10, 20, 30, 40, 60, 999],
                            labels=['N∈(0,5]', 'N∈(5,10]', 'N∈(10,20]', 'N∈(20,30]', 'N∈(30,40]', 'N∈(40,60]',
                                    'N∈(60,∞)'])
    df['esd_bins'] = np.where(df['estimated_sales_days'] < 0, 'N∈(0,5]', df['esd_bins'])
    df['esd_bins'] = np.where(df['estimated_sales_days'] > 60, 'N∈(60,∞)', df['esd_bins'])

    return df
#
# # 仓标问题处理
# # 对于清单内的sku，合并带仓标&不带仓标的sku。
# df_stock = get_stock()
# ##
# # 清单内sku
# df_target_sku = pd.read_excel('F:\yibai-price-strategy\data\海外仓主子SKU映射关系维护20240118.xlsx')
# ##
# df_target_sku = df_target_sku.rename(columns={'海外仓SKU':'sku'})
# df_stock_info = pd.merge(df_stock, df_target_sku, how='left', on=['sku'])
# ##
# df_stock_info.to_excel('df_stock_info.xlsx', index=0)
#
# 获取捆绑SKU信息
def get_bundle_sku():
    conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
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
            left join domestic_warehouse_clear.site_table c 
            on b.site=c.site
        )
    )
    
    select distinct a.site1 as site1,a.site3 as site3,a.sku as sku1,b.sku1 as sku,b.`数量` as `数量`
    from sku_site_table a 
    left join sku_map b 
    on a.sku=b.sku 
    settings max_memory_usage = 30000000000
    """
    df = conn_mx.ck_select_to_df(sql)
    #
    # 国家处理。与海外仓大仓名对应
    df = df[~df['site1'].isin(['新加坡','印度','中东'])]
    df['site2'] = np.where(df['site1'].isin(['德国','法国','西班牙','意大利','波兰','瑞典','荷兰','土耳其','比利时']), '欧洲仓', df['site1'] + '仓')
    df['site2'] = np.where(df['site1'] == '澳大利亚', '澳洲仓', df['site2'])
    df['site2'] = np.where(df['site1'] == '巴西', '乌拉圭仓', df['site2'])

    df = df.reset_index()
    # 拆分捆绑SKU信息
    df_explode = df.set_index(['index','site1','site3','sku1']).apply(lambda x: x.str.split(',').explode()).reset_index()

    # 筛选海外仓sku
    sql = """
        SELECT distinct sku, 1 as is_oversea_sku
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id > '2023-12-30'
        ORDER BY date_id DESC
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_oversea_sku = conn.read_sql(sql)
    # 获取捆绑SKU的库存和库龄信息
    df_sku = get_stock()
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    #
    df_explode = pd.merge(df_explode, df_oversea_sku, how='left', on=['sku'])
    df_explode = pd.merge(df_explode, df_sku[['sku', 'new_price','gross','available_stock', 'warehouse_id', 'warehouse']], how='left', on=['sku'])
    df_explode = pd.merge(df_explode, df_stock_age_id[['sku', 'warehouse_id', 'age_90_plus','age_120_plus','age_150_plus','age_180_plus',
                                                       'age_270_plus','age_360_plus','warehouse_stock','warehouse_stock_age','overage_level']], how='left', on=['sku','warehouse_id'])
    #
    df_explode['warehouse_id'] = df_explode['warehouse_id'].fillna(0).astype(int)

    # df_explode = df_explode[df_explode['warehouse_id'] != 0]

    # 处理链接国家与发货仓需一致问题
    df_explode['warehouse_temp'] = np.where(df_explode['warehouse'].isin(['法国仓','德国仓','西班牙仓','意大利仓']),'欧洲仓',df_explode['warehouse'])
    df_explode = df_explode[df_explode['site2'] == df_explode['warehouse_temp']]
    df_explode.drop(['site2','warehouse_temp'], axis=1, inplace=True)
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
    order_col2 = ['site3', 'sku1', 'warehouse', 'available_stock', 'age_90_plus', 'age_120_plus', 'age_150_plus',
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
    df_result = pd.DataFrame()
    for (key1, key2), group in df_bundle_sku.groupby(['site3','best_warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('AMAZON', key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode','shipName', 'totalCost',
                     'shippingCost','firstCarrierCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode','shipName', 'totalCost',
                     'shippingCost','firstCarrierCost']]
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
    # 日销数据
    df_30days = df_bundle_order.groupby('sku')['quantity'].sum().reset_index().rename(columns={'quantity':'30days_sales'})
    df_7days = df_bundle_order[df_bundle_order['purchase_time']>date_7].groupby('sku')['quantity'].sum().reset_index().rename(columns={'quantity':'7days_sales'})
    #
    df_sales = pd.merge(df_30days, df_7days, how='left', on=['sku'])
    df_sales['7days_sales'] = df_sales['7days_sales'].fillna(0).astype(int)
    df_sales['day_sales'] = 0.1 * df_sales['30days_sales']/30 + 0.9 * df_sales['7days_sales']/7

    return df_sales

# 调价周期设置
def adjust_cycle(dwm_sku_price_temp):
    sql = """

    SELECT sku, warehouse, platform, country, target_profit_rate as target_profit_y, is_adjust, date_id
    FROM dwm_oversea_price_dtl
    WHERE date_id >= date_sub(curdate(),interval 3 day)

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp_t = conn.read_sql(sql)

    # 如果上一次调价周期超过三天，则取最近的调价记录，判断是否调价
    if len(df_temp_t) == 0:
        sql = f"""

        SELECT sku, warehouse, platform, country, target_profit_rate as target_profit_y, is_adjust, date_id
        FROM dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM dwm_oversea_price_dtl WHERE date_id < '{datetime.date.today()}')

        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_temp_t = conn.read_sql(sql)

    # 匹配前一天的利润率
    temp_columns = ['sku', 'warehouse', 'platform', 'country', 'target_profit_y']
    df_temp_last = df_temp_t[df_temp_t['date_id'] != str(datetime.date.today())]
    dwm_sku_price_temp = pd.merge(dwm_sku_price_temp,
                                  df_temp_last[df_temp_last['date_id'] == df_temp_last.date_id.unique().max()][temp_columns],
                                  how='left', on=['sku', 'warehouse', 'platform', 'country'])

    dwm_sku_price_temp['target_profit_y'] = dwm_sku_price_temp['target_profit_y'].astype(float)

    # 调价状态判断
    # c1 = np.isclose(dwm_sku_price_temp['target_profit_rate'], dwm_sku_price_temp['target_profit_y'])
    c1 = dwm_sku_price_temp['target_profit_rate'].round(4) == (dwm_sku_price_temp['target_profit_y']).round(4)
    c2 = dwm_sku_price_temp['target_profit_rate'].round(4) > dwm_sku_price_temp['target_profit_y'].round(4)
    c3 = dwm_sku_price_temp['target_profit_rate'].round(4) < dwm_sku_price_temp['target_profit_y'].round(4)
    dwm_sku_price_temp['is_adjust'] = np.select([c1, c2, c3], ['保持', '涨价', '降价'], '保持')

    # 调价周期设置
    # 实现方式：获取近 * 天的调价状态，如果当前最新的调价状态已出现，则将调价状态置为保持、利润率置为前一日
    is_adjust_temp = df_temp_t.groupby(['sku', 'warehouse', 'platform', 'country'])['is_adjust'].apply(
        lambda x: x.str.cat(sep=',')).reset_index()
    is_adjust_temp = is_adjust_temp.rename(columns={'is_adjust': 'is_adjust_list'})

    dwm_sku_price_temp = pd.merge(dwm_sku_price_temp, is_adjust_temp, how='left',
                                  on=['sku', 'warehouse', 'platform', 'country'])
    # split_list = dwm_sku_price_temp['is_adjust_list'].str.split(',', expand=True).stack()
    dwm_sku_price_temp['is_adjust_list'] = dwm_sku_price_temp['is_adjust_list'].fillna(' ').astype(str)
    dwm_sku_price_temp['is_adjust_list'] = dwm_sku_price_temp['is_adjust_list'].str.split(', ', expand=True).apply(
        lambda x: [item for item in x])
    dwm_sku_price_temp['is_in'] = dwm_sku_price_temp.apply(
        lambda row: all(item in row['is_adjust_list'] for item in row['is_adjust'].split()), axis=1)
    c1 = (dwm_sku_price_temp['is_adjust'] == '降价') & (dwm_sku_price_temp['is_in'] == True)
    c2 = (dwm_sku_price_temp['is_adjust'] == '涨价') & (dwm_sku_price_temp['is_in'] == True)
    dwm_sku_price_temp['target_profit_rate'] = np.select([c1, c2], [dwm_sku_price_temp['target_profit_y'],
                                                                    dwm_sku_price_temp['target_profit_y']],
                                                         dwm_sku_price_temp['target_profit_rate'])
    dwm_sku_price_temp['is_adjust'] = np.select([c1, c2], ['保持', '保持'], dwm_sku_price_temp['is_adjust'])
    dwm_sku_price_temp.drop(['is_adjust_list','is_in'], axis=1, inplace=True)

    return dwm_sku_price_temp

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

    # sql = f"""
    # delete from {table_name} where date_id='{date_id}'
    # """
    # conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='replace')

    conn.close()

def dwm_oversea_bundle():
    """
    海外仓捆绑SKU目标价计算
    """
    df_bundle_sku = get_bundle_sku()
    df_fee = get_bundle_fee(df_bundle_sku)
    df_sales = get_bundle_sales()
    df_sales = df_sales.rename(columns={'sku':'sku1'})
    # 匹配数据
    df_bundle = pd.merge(df_bundle_sku, df_fee, how='left', left_on=['site3','sku','数量','best_warehouse_id'], right_on=['shipCountry','sku','数量','warehouseId'])
    df_bundle = pd.merge(df_bundle, df_sales, how='left', on='sku1')
    df_bundle.drop(['shipCountry','warehouseId'],axis=1,inplace=True)
    df_bundle = df_bundle.rename(columns={'site3':'country'})
    df_bundle['date_id'] = time.strftime('%Y-%m-%d')
    df_bundle['platform'] = 'AMAZON'
    #
    # 可售天数 (大仓的总库存）
    df_bundle[['30days_sales','7days_sales','day_sales']] = df_bundle[['30days_sales','7days_sales','day_sales']].fillna(0).astype(float)
    col = ['available_stock', 'age_90_plus', 'age_120_plus', 'age_150_plus','age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock','overage_level']
    df_bundle[col] = df_bundle[col].astype(int)

    #
    df_bundle['estimated_sales_days'] = (df_bundle['available_stock'] / df_bundle['day_sales']).replace(np.inf, 9999).replace(np.nan, 0)
    # 超库龄的可售天数（超 i 天库龄库存的可售天数）
    for i in df_bundle['overage_level'].unique():
        if np.isnan(i) or i == 0:
            continue
        else:
            c = df_bundle['overage_level'] == i
            df_bundle.loc[c, 'overage_esd'] = (df_bundle.loc[c, 'age_{}_plus'.format(int(i))] / df_bundle.loc[c, 'day_sales']).replace(np.inf,9999).replace(np.nan, 0)
    df_bundle['overage_esd'] = df_bundle['overage_esd'].fillna(0)
    #
    # 销售状态分类:根据超库龄情况判断分类。
    df_bundle['sales_status'] = '待定'
    df_bundle['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print('条件分箱...')
    df_bundle = cut_bins(df_bundle)
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
    profit_rate_section['overage_level'] = profit_rate_section['overage_level'].astype(int)
    profit_rate_section['lowest_profit'] = np.where(profit_rate_section['overage_level'] == 270, -0.5,
                                                    profit_rate_section['lowest_profit'])
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
    dwm_sku_2['country'] = dwm_sku_2['country'].replace('GB','UK')
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

# 获取捆绑SKU的链接信息, 并匹配上目标价
def get_bundle_adjust_listing():
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT
            country, sku1 as sku, warehouse, best_warehouse_id, available_stock, day_sales, overage_level,
            sales_status, target_profit_rate, price, price_rmb, new_price, totalCost as total_cost
        FROM over_sea.dwm_oversea_bundle_price_dtl
        WHERE date_id = '{date_today}' and price is not Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_bundle_price = conn.read_sql(sql)

    # 捆绑SKU的链接信息
    sql = """
    WITH listing_table as (
        SELECT account_id, seller_sku, sku 
        FROM yibai_product_kd_sync.yibai_amazon_sku_map 
        WHERE (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2
    ) 
    
    SELECT
        group_name, short_name, if(b.site ='sp', 'ES', upper(b.site)) as country,b.account_name, a.account_id, a.seller_sku, 
        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin, l.sku, a.status, a.open_date, a.price online_price
    FROM (
        SELECT account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        FROM yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
        WHERE fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a
    INNER JOIN listing_table l ON a.account_id = l.account_id and a.seller_sku = l.seller_sku
    LEFT JOIN (
        SELECT account_id, seller_sku, asin1 
        FROM yibai_product_kd_sync.yibai_amazon_listing_alls 
        WHERE fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) t ON (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
    INNER JOIN (
        SELECT toInt32(b.id) as account_id, account_name, group_id, short_name, site
        FROM yibai_system_kd_sync.yibai_amazon_account b
        WHERE account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163
    ) b on (a.account_id= b.account_id)
    INNER JOIN (
        SELECT group_id, group_name
        FROM yibai_system_kd_sync.yibai_amazon_group 
        WHERE group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    ) c on (b.group_id=c.group_id)
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_oversea')
    df_listing_all = ck_client.ck_select_to_df(sql)
    df_listing_all.columns = [i.split('.')[-1] for i in df_listing_all.columns.to_list()]
    #
    df = pd.merge(df_listing_all, dwm_bundle_price, how='inner', on=['sku','country'])
    #
    # 剔除逻辑
    # 判断是否涨价
    df[['price','online_price']] = df[['price','online_price']].astype(float)
    df['is_up'] = np.where(df['price'] >= df['online_price'], '涨价', '降价')

    site_name_dic = {'UK': '英国', 'US': '美国', 'DE': '德国', 'JP': '日本', 'PL': '波兰', 'FR': '法国', 'AU': '澳洲',
                     'NL': '荷兰', 'ES': '西班牙', 'IT': '意大利', 'SE': '瑞典', 'CA': '加拿大', 'MX': '墨西哥'}
    df['站点'] = df['country'].replace(site_name_dic)
    df = amazon_coupon_fu(df)

    df_amazon_promotion = get_amazon_promotion()
    df = pd.merge(df, df_amazon_promotion, how='left', on=['account_id', 'seller_sku'])

    # 日本站积分数据：your_price_point 当前基本为0, 暂时直接置为0
    df['your_price_point'] = 0
    df['no_coupon_price'] = df['price']
    df[['promotion_percent', 'promotion_amount']] = df[['promotion_percent', 'promotion_amount']].fillna(0)

    df['price'] = (df['price'] + (df['money_off'] + df['Coupon_handling_fee'] + df['promotion_amount'])) / \
                  (1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])
    df['price'] = df['price'].round(1) - 0.01

    #
    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    df_platform_fee = get_platform_fee()
    df_platform_fee = df_platform_fee[df_platform_fee['platform']=='AMAZON']
    df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    #
    # 2、正常品且日销大于0.1，不降价
    c1 = (df['sales_status'].isin(['正常', '回调']) & (df['is_up'] == '降价') & (df['day_sales'] > 0.1))
    df['is_normal_cdt'] = np.where(c1, 1, 0)
    # 3、目标价与当前链接价差不超过0.3或变化率小于1%
    c1 = ((df['online_price'] - df['price']).abs() <= 0.3) | (
                ((df['price'] - df['online_price']) / df['online_price']).abs() <= 0.01)
    df['is_small_diff'] = np.where(c1, 1, 0)
    # 4、不调价账号
    short_name = ['GTE', 'GTG', 'GTF', 'GTS', 'GTI', 'GTN', 'GTQ', 'GTP', 'A1P', 'A1W', 'A1E', 'A1G', 'A1F', 'A1S',
                  'A1I', 'A1N', 'H9E', 'H9G', 'H9F', 'H9S', 'H9I', 'H9W', 'H9N', 'GTW', 'H9P', '3RG']
    account_id = [228, 265, 881, 1349, 6594, 6614, 6615, 930, 6851]
    c1 = (df['short_name'].isin(short_name)) | (df['account_id'].isin(account_id))
    df['is_white_account'] = np.where(c1, 1, 0)
    # 5、白名单链接：get_white_listing()
    df_white_sku, df_white_listing = get_white_listing(conn)
    # df_white_sku实际未过滤。（过滤方式需采用匹配后去nan）
    df = pd.merge(df, df_white_listing, how='left', on=['account_id', 'seller_sku'])
    df['is_white_listing'] = df['is_white_listing'].fillna(0).astype(int)
    #
    # 6、剔除同大部、同站点、同asin下有FBA链接的FBM。复用原程序
    df = get_fba_asin(df, conn)
    #
    df = df.drop(columns=['percentage_off','money_off','Coupon_handling_fee','promotion_percent','promotion_amount','promotion_source',
                  'your_price_point','platform'], axis=1)
    #
    df['date_id'] = time.strftime('%Y-%m-%d')
    write_to_sql(df, 'oversea_amazon_bundle_listing_all')
    #
    # 筛选
    df_final = df[(df['is_normal_cdt'] == 0) & (df['is_small_diff'] == 0) & (
            df['is_white_account'] == 0) & (df['is_white_listing'] == 0) & (df['is_fba_asin'] == 0)]
    # 20231016 巴西站点的数据暂时先不调价，销售人工调价测试中
    df_final = df_final[df_final['country'] != 'BR']
    #
    # 筛选后的数据再次存入sql
    del_col = ['is_normal_cdt','is_small_diff','is_white_account','is_white_listing','is_fba_asin']
    df_final.drop(columns=del_col, axis=1, inplace=True)
    #
    write_to_sql(df_final, 'oversea_amazon_bundle_listing_upload')

    return df_final

def get_14_group():
    # 获取大部和账号
    df = xiaozu_dabu()
    # 筛选14部的账号
    group_list = tuple(df['group_name'][df['大部'] == '武汉产品线十四部'].unique())
    sql = f"""
    SELECT a.id as account_id, a.group_id, g.group_name
    FROM yibai_system_kd_sync.yibai_amazon_account a
    LEFT JOIN yibai_system_kd_sync.yibai_amazon_group g
    ON a.group_id = g.group_id
    WHERE g.group_name in {group_list}
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_oversea')
    df_account = ck_client.ck_select_to_df(sql)
    #
    account_list = tuple(df_account['account_id'].unique())
    #
    df_account.to_excel('df_account.xlsx', index=0)
    #
    sql = """
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2024-01-10'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_oversea = conn.read_sql(sql)
    #
    df_oversea.to_excel('df_oversea.xlsx', index=0)

def get_order_fee():
    sql = f"""
    
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, warehouse,
            quantity, release_money, sales_status
        FROM over_sea.dashbord_new1
        WHERE 
            paytime >= '2023-10-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
                         db_name='over_sea')
    df_order_info = ck_client.ck_select_to_df(sql)
    #
    df_order_ama = df_order_info[df_order_info['platform_code']=='AMAZON']
    #
    sku_list = tuple(df_order_ama['sku'].unique())
    order_list1 = tuple(df_order_ama.iloc[0:10000,0].unique())
    order_list2 = tuple(df_order_ama.iloc[10000:,0].unique())
    #
    len(order_list2)
    #
    # 获取订单运费数据
    sql = f"""
            SELECT 
                order_id, platform_code, total_price, product_price, commission_fees, escrow_tax, purchase_cost,
                shipping_cost,ship_cost_second, first_carrier_cost, duty_cost, duty_cost_second, processing, package_cost, stock_price,
                exchange_price, profit2,profit_rate2,true_profit2,true_profit_rate2
            FROM yibai_oms_sync.yibai_oms_order_profit
            WHERE order_id in {order_list1}
    """
    df_order_profit = fd.fetch_ck(sql, 78, 'yibai_oms_sync')
    #
    sql = f"""
            SELECT 
                order_id, platform_code, total_price, product_price, commission_fees, escrow_tax, purchase_cost,
                shipping_cost,ship_cost_second, first_carrier_cost, duty_cost, duty_cost_second, processing, package_cost, stock_price,
                exchange_price, profit2,profit_rate2,true_profit2,true_profit_rate2
            FROM yibai_oms_sync.yibai_oms_order_profit
            WHERE order_id in {order_list2}
    """
    df_order_profit2 = fd.fetch_ck(sql, 78, 'yibai_oms_sync')
    #
    df_order_profit = pd.concat([df_order_profit, df_order_profit2])
    a = df_order_ama[~df_order_ama['order_id'].isin(df_order_profit['order_id'].unique())]
    b = tuple(a.order_id.unique())
    df_order_info.to_excel('df_order_info.xlsx', index=0)
    # 多组织订单表
    sql = f"""
        WITH order_temp as (
            SELECT order_id, sku, quantity
            FROM yibai_dcm_order_sync.dcm_order_sku
            WHERE sku in {sku_list} and create_time > '2023-12-01'
        )
        SELECT sku, sum(quantity) as 30days_sales_dcm
        FROM order_temp t
        INNER JOIN (
            SELECT order_id
            FROM yibai_dcm_order_sync.dcm_order
            WHERE order_status<>40 and is_abnormal<>1 and is_intercept <> 1 
            and order_id in (SELECT order_id FROM order_temp)
        ) d ON t.order_id = d.order_id
        GROUP BY sku
    """
    df_dcm_order = fd.fetch_ck(sql, 34, 'yibai_dcm_order_sync')

    #
    sql = """
        SELECT
            sku,
            sum(quantity) 30days_sales_dcm
        FROM (
            SELECT
                ac.other_shop_id account_id,
                b.seller_sku,
                b.sku sku,
                b.asinval as asin,
                quantity,
                case a.is_ship_process when 1 then 'AMA' else 'DEF' END AS fulfillment_channel
            FROM `dcm_order` a
            LEFT JOIN dcm_order_detail b ON a.order_id = b.order_id 
            LEFT JOIN yibai_dcm_base_sync.dcm_shop_binding ac on ac.distributor_id = a.shop_id and ac.shop_id = a.account_id
            WHERE
                a.payment_status = 1 
                AND a.shop_id IN ( 223, 224, 225, 233 , 425) 
                AND a.purchase_time >= '2023-12-10'
                AND order_status <> 40 
        ) w 
        GROUP BY sku
    """
    df_dcm_order = fd.fetch_ck(sql, 34, 'yibai_dcm_order_sync')

def write_pricing_ratio_config():
    df_temp = pd.read_excel('F://Desktop//pricing_ratio_config_table.xlsx')
    #
    write_to_sql(df_temp, 'pricing_ratio_config_table')
##
write_pricing_ratio_config()
##
def write_section():
    section = pd.read_excel('F://Desktop//up_profit_section_eod.xlsx')
    # section = pd.read_excel('F://Desktop//down_profit_section_eod.xlsx')

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(section, 'up_profit_section_eod', if_exists='replace')
    # conn.to_sql(section, 'down_profit_section_eod', if_exists='replace')
write_section()
## 聚合
def dwm_oversea_sku_base():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    pull_warehouse_info()
    print('获取库存信息...')
    df_stock = get_stock()
    print('获取库龄信息...')
    df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    print('获取日销信息...')
    sku_sales = get_sku_sales()

    dwm_sku = pd.merge(df_stock, sku_sales, how='left', on=['sku', 'warehouse'])
    # dwm_sku.info()
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 23:32] = dwm_sku.iloc[:, 23:32].fillna(0)
    # dwm_sku.info()
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])
    # 仓租数据，汇率转化
    df_rate = get_rate()
    dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)

    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_60_plus','age_90_plus',
                     'age_120_plus',
                     'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '30days_sales', '90days_sales', 'day_sales', 'recent_day_sales', ]
    dwm_sku[['best_warehouse_id','age_60_plus']] = dwm_sku[['best_warehouse_id','age_60_plus']].astype(int)
    dwm_sku = dwm_sku[columns_order]

    # 可售天数 (大仓的总库存）
    dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(np.inf, 9999).replace(
        np.nan, 0)
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
    # dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print('条件分箱...')
    dwm_sku_2 = cut_bins(dwm_sku)

    # 判断是否新品
    df_new_sku = is_new_sku()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_new_sku, how='left', on=['sku'])
    dwm_sku_2['overage_level'] = dwm_sku_2['overage_level'].fillna(0).astype(int)
    dwm_sku_2['is_new'] = dwm_sku_2['is_new'].fillna(0).astype(int)
    # 获取上一次的销售状态
    sql = """
        SELECT SKU sku, warehouse, adjust_recent_clean sales_status
        FROM yibai_oversea.oversea_sale_status_remake
        WHERE `DATE` = (SELECT max(`DATE`) FROM yibai_oversea.oversea_sale_status_remake)
    """
    conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sales_status = conn.ck_select_to_df(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_sales_status, how='left', on=['sku', 'warehouse'])
    dwm_sku_2['sales_status'] = dwm_sku_2['sales_status'].fillna('正常')

    return dwm_sku_2

def dwm_oversea_sku(dwm_sku_2):
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    # pull_warehouse_info()
    # print('获取库存信息...')
    # df_stock = get_stock()
    # print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    # print('获取日销信息...')
    # sku_sales = get_sku_sales()
    #
    # dwm_sku = pd.merge(df_stock, sku_sales, how='left', on=['sku', 'warehouse'])
    # # dwm_sku.info()
    # dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    # dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # # 匹配库龄、仓租数据
    # # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    # dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    # print(dwm_sku.info())
    # dwm_sku.iloc[:, 23:32] = dwm_sku.iloc[:, 23:32].fillna(0)
    # # dwm_sku.info()
    # dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
    #                                         dwm_sku['best_warehouse_id'])
    # dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
    #                                           dwm_sku['best_warehouse_name'])
    # # 仓租数据，汇率转化
    # df_rate = get_rate()
    # dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    # dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    # dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    # dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)
    #
    # columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
    #                  'product_size',
    #                  'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
    #                  'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_60_plus','age_90_plus',
    #                  'age_120_plus',
    #                  'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
    #                  'charge_total_price_rmb', 'charge_currency',
    #                  '3days_sales', '7days_sales', '30days_sales', '90days_sales', 'day_sales', 'recent_day_sales', ]
    # dwm_sku[['best_warehouse_id','age_60_plus']] = dwm_sku[['best_warehouse_id','age_60_plus']].astype(int)
    # dwm_sku = dwm_sku[columns_order]
    #
    # # 可售天数 (大仓的总库存）
    # dwm_sku['estimated_sales_days'] = (dwm_sku['available_stock'] / dwm_sku['day_sales']).replace(np.inf, 9999).replace(
    #     np.nan, 0)
    # # 超库龄的可售天数（超 i 天库龄库存的可售天数）
    # for i in dwm_sku['overage_level'].unique():
    #     if np.isnan(i) or i == 0:
    #         continue
    #     else:
    #         c = dwm_sku['overage_level'] == i
    #         dwm_sku.loc[c, 'overage_esd'] = (
    #                     dwm_sku.loc[c, 'age_{}_plus'.format(int(i))] / dwm_sku.loc[c, 'day_sales']).replace(np.inf,
    #                                                                                                         9999).replace(
    #             np.nan, 0)
    #
    # # 销售状态分类:根据超库龄情况判断分类。
    # # dwm_sku['sales_status'] = '待定'
    # dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    # print('条件分箱...')
    # dwm_sku_2 = cut_bins(dwm_sku)
    #
    # # 判断是否新品
    # df_new_sku = is_new_sku()
    # dwm_sku_2 = pd.merge(dwm_sku_2, df_new_sku, how='left', on=['sku'])
    # dwm_sku_2['overage_level'] = dwm_sku_2['overage_level'].fillna(0).astype(int)
    # dwm_sku_2['is_new'] = dwm_sku_2['is_new'].fillna(0).astype(int)
    # # 获取上一次的销售状态
    # sql = """
    #     SELECT SKU sku, warehouse, adjust_recent_clean sales_status
    #     FROM yibai_oversea.oversea_sale_status_remake
    #     WHERE `DATE` = (SELECT max(`DATE`) FROM yibai_oversea.oversea_sale_status_remake)
    # """
    # conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_sales_status = conn.ck_select_to_df(sql)
    # dwm_sku_2 = pd.merge(dwm_sku_2, df_sales_status, how='left', on=['sku', 'warehouse'])
    # dwm_sku_2['sales_status'] = dwm_sku_2['sales_status'].fillna('正常')

    print('开始设置涨降幅度...')
    # 涨价缩销逻辑
    df_edt = get_edt_date()
    # 合并预计到仓时间，计算预计缺货天数
    col = ['sku', 'warehouse', 'estimated_delivery_time', '预计到货天数']
    dwm_sku_2 = pd.merge(dwm_sku_2, df_edt[col], how='left', on=['sku', 'warehouse'])
    dwm_sku_2['预计缺货天数'] = dwm_sku_2['预计到货天数'] - dwm_sku_2['estimated_sales_days']
    dwm_sku_2['eod_bins'] = pd.cut(dwm_sku_2['预计缺货天数'], bins=[-100, -3, 10, 20, 30, 40, 999],
                                 labels=['E∈(-∞,-3]', 'E∈(-3,10]', 'E∈(10,20]', 'E∈(20,30]', 'E∈(30,40]', 'E∈(40,∞)'])
    dwm_sku_2['eod_bins'] = np.where(dwm_sku_2['预计缺货天数'] < -99, 'E∈(,-3]', dwm_sku_2['eod_bins'])
    dwm_sku_2['eod_bins'] = np.where(dwm_sku_2['预计缺货天数'] > 40, 'E∈(40,∞)', dwm_sku_2['eod_bins'])
    # 触发涨价缩销逻辑
    c1 = (dwm_sku_2['sales_status']=='正常') & (dwm_sku_2['overage_level'] == 0) & (dwm_sku_2['available_stock'] >= 3) & \
         (dwm_sku_2['day_sales'] >= 0.3) & (dwm_sku_2['estimated_sales_days'] < 30) & (dwm_sku_2['预计缺货天数'] > -3)
    df_temp = dwm_sku_2[c1][['sku', 'warehouse', 'day_sales_bins', 'esd_bins', 'eod_bins']]
    sql = """
        SELECT *
        FROM over_sea.up_profit_section_eod
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    section = conn.read_sql(sql)
    df_temp = pd.merge(df_temp, section, how='left', on=['day_sales_bins', 'esd_bins', 'eod_bins'])
    df_temp['section'] = df_temp['section'].fillna(0)

    # 获取【降价及回调阶梯】
    sql = """
    SELECT *
    FROM profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    profit_rate_section = conn.read_sql(sql)
    # # 超270天更新保底净利率。临时
    # profit_rate_section['overage_level'] = profit_rate_section['overage_level'].astype(int)
    # profit_rate_section['lowest_profit'] = np.where(profit_rate_section['overage_level']==270, -0.5, profit_rate_section['lowest_profit'] )

    sql = """
    SELECT *
    FROM up_rate_section
    """
    up_rate_section = conn.read_sql(sql)

    dwm_sku_2 = pd.merge(dwm_sku_2, profit_rate_section, how='left',
                         on=['overage_level', 'overage_esd_bins', 'day_sales_bins'])
    dwm_sku_2 = pd.merge(dwm_sku_2, up_rate_section, how='left', on=['overage_level', 'esd_bins'])

    #
    # 匹配涨价缩销的sku
    df_temp['is_increase'] = 1
    df_temp = df_temp.rename(columns={'section': 'increase_section'})

    dwm_sku_2 = pd.merge(dwm_sku_2, df_temp[['sku', 'warehouse', 'increase_section', 'is_increase']], how='left',
                         on=['sku', 'warehouse'])
    dwm_sku_2['up_profit_rate'] = np.where(dwm_sku_2['is_increase'] == 1, dwm_sku_2['increase_section'],
                                           dwm_sku_2['up_profit_rate'])
    # 涨价缩销销售状态
    dwm_sku_2['sales_status'] = np.where(dwm_sku_2['is_increase']==1, '涨价缩销', dwm_sku_2['sales_status'])
    # 匹配涨价缩销回调
    def increase_collback(dwm_sku_info):
        # 涨价缩销的回调。（预计缺货天数 <= -3）
        c1 = (dwm_sku_info['sales_status'] == '涨价缩销') & (dwm_sku_info['available_stock'] >= 3) & (
                    dwm_sku_info['预计缺货天数'] <= 0)
        df_temp_3 = dwm_sku_info[c1][['sku', 'warehouse', 'sales_status', 'day_sales_bins', 'esd_bins', 'eod_bins']]

        sql = """
        SELECT day_sales_bins, esd_bins, section as down_section, lowest_profit as down_lowest_profit
        FROM down_profit_section_eod
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        down_eod = conn.read_sql(sql)
        #
        df_temp_3 = pd.merge(df_temp_3, down_eod, how='left', on=['day_sales_bins', 'esd_bins'])
        #
        df_temp_3['down_section'] = -0.05
        df_temp_3['down_lowest_profit'] = 0.12
        df_temp_3['is_down'] = 1
        # dwm_sku_info = dwm_sku_info.iloc[:,0:53]

        #
        dwm_sku_info = pd.merge(dwm_sku_info,
                                df_temp_3[['sku', 'warehouse', 'down_section', 'down_lowest_profit', 'is_down']],
                                how='left', on=['sku', 'warehouse'])
        #
        dwm_sku_info[['section', 'lowest_profit']] = dwm_sku_info[['section', 'lowest_profit']].fillna(0)
        c1 = (dwm_sku_info['section'] == 0) & (~dwm_sku_info['down_section'].isna())
        dwm_sku_info['section'] = np.where(c1, dwm_sku_info['down_section'], dwm_sku_info['section'])

        c1 = (dwm_sku_info['lowest_profit'] == 0) & (~dwm_sku_info['down_lowest_profit'].isna())
        dwm_sku_info['lowest_profit'] = np.where(c1, dwm_sku_info['down_lowest_profit'], dwm_sku_info['lowest_profit'])
        #
        dwm_sku_info.drop(['down_section', 'down_lowest_profit'], axis=1, inplace=True)

        return dwm_sku_info

    dwm_sku_2 = increase_collback(dwm_sku_2)


    # 高销涨价
    c1 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
                dwm_sku_2['recent_day_sales'] >= 10) & (dwm_sku_2['esd_bins'] == 'N∈(0,15]')
    c2 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
                dwm_sku_2['recent_day_sales'] >= 10) & (dwm_sku_2['esd_bins'] == 'N∈(15,30]')
    c3 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
                dwm_sku_2['recent_day_sales'] >= 1) & (dwm_sku_2['esd_bins'] == 'N∈(0,15]')
    c4 = (~dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (
                dwm_sku_2['recent_day_sales'] >= 1) & (dwm_sku_2['esd_bins'] == 'N∈(15,30]')
    dwm_sku_2['up_profit_rate'] = np.select([c1, c2, c3, c4], [0.06, 0.04, 0.04, 0.02], dwm_sku_2['up_profit_rate'])

    # 低销降价
    c1 = (dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (dwm_sku_2['is_new'] == 0) & \
         (dwm_sku_2['available_stock'] >= 3) & (dwm_sku_2['90days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 90)
    c2 = (dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (dwm_sku_2['is_new'] == 0) & \
         (dwm_sku_2['available_stock'] >= 3) & (dwm_sku_2['90days_sales'] > 0) & (dwm_sku_2['30days_sales'] == 0) & \
         (dwm_sku_2['overage_level'] >= 90)
    c3 = (dwm_sku_2['section'].isna()) & (dwm_sku_2['up_profit_rate'].isna()) & (dwm_sku_2['is_new'] == 1) & \
         (dwm_sku_2['available_stock'] >= 3) & (dwm_sku_2['90days_sales'] == 0) & (dwm_sku_2['overage_level'] >= 90)
    dwm_sku_2['section'] = np.select([c1, c2, c3], [-0.04, -0.03, -0.02], dwm_sku_2['section'])

    # 利润率涨降幅度
    # begin_profit需替换为前一天的利润率
    # 获取前一天的after_profit
    sql = f"""
    SELECT sku, warehouse, after_profit as after_profit_yest
    FROM dwm_sku_temp_info
    WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id < '{datetime.datetime.now().strftime('%Y-%m-%d')}')
    """
    df_after_yest = conn.read_sql(sql)
    dwm_sku_2 = pd.merge(dwm_sku_2, df_after_yest, how='left', on=['sku', 'warehouse'])
    dwm_sku_2['begin_profit'] = np.where(dwm_sku_2['after_profit_yest'].isna(), 0, dwm_sku_2['after_profit_yest'])
    dwm_sku_2.drop('after_profit_yest', axis=1, inplace=True)

    dwm_sku_2[['section']] = dwm_sku_2[['section']].fillna(0)
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['up_profit_rate'] > 0,
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['up_profit_rate'],
                                         dwm_sku_2['begin_profit'] + dwm_sku_2['section'])

    # 调价最高幅度不超过 0 （涨价缩销时可大于0）
    c1 = (dwm_sku_2['after_profit'] > 0) & (dwm_sku_2['is_increase'] != 1) & (dwm_sku_2['is_down'] != 1)
    dwm_sku_2['after_profit'] = np.where(c1, 0, dwm_sku_2['after_profit'])
    # 库存为0时，调价幅度置为0
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['available_stock'] <= 0, 0, dwm_sku_2['after_profit'])

    # 低销降价未回调bug修复
    c1 = (dwm_sku_2['overage_level'] < 90) & (dwm_sku_2['is_increase'] != 1)
    dwm_sku_2['after_profit'] = np.where(c1, 0, dwm_sku_2['after_profit'])

    # 20231227 (调价幅度+平台最低净利率)最低不超过保底净利率，最高不超过20%
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit'] - 0.08),
                                         dwm_sku_2['lowest_profit'] - 0.08, dwm_sku_2['after_profit'])
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] >= 0.2, 0.2, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    # print(dwm_sku_2.info())

    # print('SKU信息及调价幅度已获取，准备写入dwm_sku_temp_info...')
    # write_to_sql(dwm_sku_2, 'dwm_sku_temp_info')

    return dwm_sku_2
##
def get_edt_date():
    """
    获取海外仓sku的预计到货时间
    """
    # 预计到货时间
    sql = """
        SELECT 
            a.inbound_number, a.shipment_time, a.departure_time, a.arrival_warehouse_time,a.goal_warehouse_code, a.goal_country_code, 
            a.ship_company_code, a.ship_code, b.plan_order_id, c.clearance_warehouse, d.shipment_status, e.sku
        FROM yibai_tms_logistics.yibai_platform_track_oversea a
        LEFT JOIN yibai_tms_logistics.yibai_order_stocking_forecast b
        ON a.inbound_number = b.inbound_number
        LEFT JOIN yibai_tms_logistics.yibai_shipping_aging_config c
        ON a.ship_company_code = c.company_code and a.ship_code = c.ship_code
        LEFT JOIN yibai_plan_common.yibai_oversea_shipment_list d
        ON b.plan_order_id = d.shipment_sn
        LEFT JOIN yibai_plan_common.yibai_oversea_shipment_list_detail e
        ON b.plan_order_id = e.shipment_sn

    """
    conn = connect_to_sql(database='yibai_tms_logistics', data_sys='新数仓')
    df_oversea_number = conn.read_sql(sql)

    # 预计到货时间处理：1、无时效的，填充为70天；2、时效已过，还未到货的，填充10天后
    df_oversea_number['shipment_time'] = df_oversea_number['shipment_time'].astype(str)
    c1 = (df_oversea_number['shipment_time'] >= '2024-01-01') & (df_oversea_number['arrival_warehouse_time'].isna())
    df_edt = df_oversea_number[c1]
    #
    c1 = (df_edt['clearance_warehouse'].isna()) | (df_edt['clearance_warehouse'] <= 0)
    df_edt['clearance_warehouse'] = np.where(c1, 70, df_edt['clearance_warehouse']).astype(int)

    df_edt['estimated_delivery_time'] = pd.to_datetime(df_edt['shipment_time']) + df_edt[
        'clearance_warehouse'] * pd.Timedelta(days=1)
    df_edt['estimated_delivery_time'] = df_edt['estimated_delivery_time'].astype(str)

    time_today = time.strftime('%Y-%m-%d')
    time_10 = (datetime.datetime.today() + pd.Timedelta(days=10)).strftime('%Y-%m-%d')
    df_edt['estimated_delivery_time'] = np.where(df_edt['estimated_delivery_time'] <= time_today, time_10,
                                                 df_edt['estimated_delivery_time'])

    dic = {'DE': '德国仓', 'CS': '德国仓', 'GB': '英国仓', 'UK': '英国仓', 'FR': '法国仓', 'US': '美国仓',
           'AU': '澳洲仓', 'RU': '俄罗斯仓',
           'MX': '墨西哥仓'}
    df_edt['warehouse'] = df_edt['goal_country_code'].replace(dic)

    df_edt = df_edt.sort_values(by=['estimated_delivery_time'], ascending=True).drop_duplicates(
        subset=['sku', 'warehouse'])
    #
    df_edt['estimated_delivery_time'] = pd.to_datetime(df_edt['estimated_delivery_time'])
    df_edt['预计到货天数'] = df_edt.apply(lambda row: (row['estimated_delivery_time'] - datetime.datetime.now()).days,
                                          axis=1)

    return df_edt
# 利润率、销售状态、价格计算
def dwm_sku_price(dwm_sku):
    """
    销售状态设置
    调价周期设置 *
    价格计算
    """

    # 读取dwm_sku_2
    # sql = """
    # SELECT *
    # FROM dwm_sku_temp_info
    # WHERE date_id = curdate()
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # dwm_sku = conn.read_sql(sql)
    print(dwm_sku.info())

    # 匹配运费
    # 运费数据需要处理！！！
    # WISH平台的汇率设置为1：
    df_transport_fee = get_transport_fee()
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='left', on='country')
    df_transport_fee = df_transport_fee[
        ['sku', 'warehouse_id', 'ship_name', 'total_cost', 'lowest_price', 'platform', 'country',
         'rate']].drop_duplicates()

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate']

    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_transport_fee, how='inner',
                             left_on=['sku', 'best_warehouse_id'], right_on=['sku', 'warehouse_id'])
    # 无运费数据处理
    dwm_sku_price.drop(['warehouse_id'], axis=1, inplace=True)
    dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]
    # 无汇率国家处理：WISH平台且有库存的SKU的汇率设置为1
    dwm_sku_price['rate'] = np.where((dwm_sku_price['platform'] == 'WISH') & (dwm_sku_price['available_stock'] > 0), 1,
                                     dwm_sku_price['rate'])
    dwm_sku_price = dwm_sku_price[~dwm_sku_price['rate'].isna()]
    dwm_sku_price = dwm_sku_price.drop_duplicates()
    # 匹配差值表
    df_platform_fee = get_platform_fee()
    dwm_sku_price = pd.merge(dwm_sku_price, df_platform_fee, how='inner', on=['platform', 'country'])

    # 数据类型转化
    type_columns = ['new_price', 'total_cost', 'lowest_price', 'ppve', 'refound_fee', 'platform_zero',
                    'platform_must_percent']
    dwm_sku_price[type_columns] = dwm_sku_price[type_columns].astype('float64').round(4)

    # 净利率的处理
    dwm_sku_price['target_profit_rate'] = np.where(
        (dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']) < dwm_sku_price['lowest_profit'],
        dwm_sku_price['lowest_profit'],
        dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit'])

    # 调价周期设置
    dwm_sku_price = adjust_cycle(dwm_sku_price)

    # 销售状态处理
    c0 = (dwm_sku_price['target_profit_rate'] > dwm_sku_price['platform_must_percent']) & \
         (dwm_sku_price['target_profit_rate'] > dwm_sku_price['lowest_profit'])
    c1 = (dwm_sku_price['target_profit_rate'] == dwm_sku_price['platform_must_percent'])
    # c2 = (dwm_sku_price['up_profit_rate'] > 0)
    c3 = (dwm_sku_price['target_profit_rate'] > 0)
    c4 = (dwm_sku_price['target_profit_rate'] <= 0)
    dwm_sku_price['sales_status'] = np.select([c0, c1, c3, c4],
                                              ['涨价缩销', '正常', '正利润加快动销', '负利润加快动销'], '正常')

    # 价格计算
    dwm_sku_price['price_rmb'] = (dwm_sku_price['new_price'] + dwm_sku_price['total_cost']) / (
            1 - dwm_sku_price['ppve'] - dwm_sku_price['platform_zero'] - dwm_sku_price['target_profit_rate'])
    # dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01
    # 销毁价判断
    dwm_sku_price['price_rmb'] = np.where(dwm_sku_price['price_rmb'] <= dwm_sku_price['lowest_price'],
                                          dwm_sku_price['lowest_price'], dwm_sku_price['price_rmb'])
    dwm_sku_price['is_distory'] = np.where(
        dwm_sku_price['price_rmb'].astype(int) == dwm_sku_price['lowest_price'].astype(int), 1, 0)

    # 本币计算
    dwm_sku_price['price'] = dwm_sku_price['price_rmb'] / dwm_sku_price['rate']
    dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01

    dwm_sku_price.drop(['rate', 'ppve', 'refound_fee', 'platform_zero', 'platform_must_percent'], axis=1, inplace=True)
    dwm_sku_price['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print(dwm_sku_price.info())
    print('SKU目标价已计算，准备写入dwm_oversea_price_dtl...')
    # write_to_sql(dwm_sku_price, 'dwm_oversea_price_dtl')
    print('调价数据已写入mysql')
    # 最新日期的数据，也写入CK中，方便CK相关数据调用.
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yibai_oversea')
    # ck_client.ck_execute_sql(sql='truncate table yibai_oversea.dwm_oversea_price_dtl_temp')
    # 空值处理
    # dwm_sku_price['target_profit_y'] = dwm_sku_price['target_profit_y'].fillna(2.0)
    # time.sleep(120)
    # print('CK备份数据...')
    # ck_client.write_to_ck_json_type('yibai_oversea.dwm_oversea_price_dtl_temp', dwm_sku_price)

    return None
##
dwm_sku_2 = dwm_oversea_sku_base()
##
dwm_sku_info = dwm_oversea_sku(dwm_sku_2)
##
# col = ['estimated_delivery_time','预计到货天数','预计缺货天数','increase_section','is_increase']
# dwm_sku_info = dwm_sku_info.drop(col, axis=1, inplace=True)
##
# 运费头程替换
sql = """   select 
        a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, 
        remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
        firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
        a.country as country,
        round(toDecimal64(1.2,4)*(toDecimal64OrZero(a.shippingCost,4)+toDecimal64OrZero(a.extraSizeFee,4)) 
                / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
        b.new_price as new_price, b.pur_weight_pack pur_weight_pack, 
        toFloat64(b.pur_length_pack )*toFloat64(b.pur_width_pack)*toFloat64(pur_height_pack) as weight_volume,
        multiIf(arrayExists(x->x.1='ALL', d.limit_arr), arrayFirst(x->x.1='ALL',d.limit_arr).3, 
                arrayExists(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr), 
                    arrayFirst(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr).3,
                arrayExists(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)), d.limit_arr),
                    arrayFirst(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)),d.limit_arr).3, 0) as limit_price_rmb,
        null as zero_percent,
        null as five_percent,
        available_stock,
        0 as warehouse,
        case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
             subString(a.sku, 4)
        when subString(a.sku, 1, 2) in ['DE', 'GB'] then 
             subString(a.sku, 3)
        when subString(a.sku, -2) in ['DE', 'GB'] then 
             subString(a.sku, 1, -2)
        else 
             a.sku
        end as son_sku
        from 
        (
            select * except(date_id) 
            from yibai_oversea.oversea_transport_fee_daily
            where not (platform in ['AMAZON', 'EB', 'WALMART']
              and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                                         '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
              and date_id =  toYYYYMMDD(today())
            order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
            by sku, platform, country, warehouseId
        ) a
        join
        (
         select 
            sku,toFloat64(new_price) new_price,pur_length_pack,pur_width_pack,pur_height_pack,pur_weight_pack 
         from yibai_prod_base_sync.yibai_prod_sku 
         where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
        ) b
        on (a.sku = b.sku)
        join 
        (
            select sku, warehouse_id as warehouseId, available_stock
              from yb_datacenter.v_oversea_stock 
        ) g
        on (a.sku = g.sku and a.warehouseId = g.warehouseId)
        left join 
        (
          select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
          from yibai_wish.yibai_platform_fee
        ) c
        on (a.country = c.country and a.platform = c.platform)
        left join 
        (
           select 
                 sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
             from 
                 yibai_oversea.sku_limit_price 
           group by sku
        ) d
        on (a.sku = d.sku)
        limit 100000
    """
# ck_client = CkClient(user='datax', password='datax#07231226', host='172.16.51.140', port='9001',
#                      db_name='yibai_oversea')
# df = ck_client.ck_select_to_df(sql)
conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
df = conn_ck.ck_select_to_df(sql)
##
# 20220711取最新头程
sql = """SELECT * FROM over_sea.`yibai_toucheng` """
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_toucheng = conn.read_sql(sql)
##
df_toucheng.to_excel('df_toucheng.xlsx', index=0)
##
df = df.merge(df_toucheng, on=['warehouseId'])
df['price'] = df['price'].astype('float')
df[['weight_volume', 'pur_weight_pack']] = df[['weight_volume', 'pur_weight_pack']].astype(float)
df['计费重'] = np.where(df['weight_volume'] / 6000 > df['pur_weight_pack'] / 1000,
                        df['weight_volume'] / 6000, df['pur_weight_pack'] / 1000)
df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961, 1019, 847]),
                                      df['计费重'] * df['price'], df['weight_volume'] / 1000000 * df['price'])
##
sql = """
    SELECT
        sku, warehouseId, warehouseName, shipName, totalCost, firstCarrierCost, dutyCost, new_firstCarrierCost, country
    FROM over_sea.oversea_transport_fee_useful
    WHERE platform = 'AMAZON'
"""
# conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_fee_all = conn.read_sql(sql)
##
df_fee_all['is_dutycost'] = np.where(df_fee_all['dutyCost'] > 0, 1, 0)
##
df_fee_temp = df_fee_all.groupby(['warehouseName','country', 'is_dutycost']).agg({'sku':'count'}).reset_index()
##
df_fee_temp.to_excel('df_fee_temp.xlsx', index=0)
##
sql = """
    SELECT 
        a.sku, title, linest, warehouse, best_warehouse_id, best_warehouse_name, available_stock, new_price,
        totalCost total_cost, country
    FROM over_sea.dwm_sku_temp_info a
    INNER JOIN (
        SELECT sku, warehouseId, totalCost, country
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON'
    ) b 
    ON a.sku = b.sku and a.best_warehouse_id = b.warehouseId
    WHERE available_stock > 20 and date_id = '2024-05-06'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_sku_info = conn.read_sql(sql)
##
df_rate = get_rate()
sql = """
SELECT 
    site as country, pay_fee, paypal_fee, vat_fee, extra_fee, platform_zero, platform_must_percent
FROM yibai_platform_fee
WHERE platform = 'AMAZON'
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_platform = conn.read_sql(sql)
##
dwm_sku_price = pd.merge(df_sku_info, df_platform, how='left', on=['country'])
dwm_sku_price = pd.merge(dwm_sku_price, df_rate, how='left', on=['country'])
##
dwm_sku_price.to_excel('dwm_sku_price.xlsx', index=0)