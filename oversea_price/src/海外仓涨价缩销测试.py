"""
海外仓涨价缩销逻辑试运行
"""
##
import pandas as pd
import numpy as np
import time, datetime
from pulic_func.base_api.base_function import get_token, mysql_escape
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from all_auto_task.oversea_price_adjust_2023 import get_stock,get_stock_age
from all_auto_task.oversea_listing_detail_2023 import xiaozu_dabu
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from all_auto_task.oversea_listing_detail_2023 import amazon_coupon_fu, get_amazon_coupon,get_amazon_promotion,get_white_listing,get_fba_asin
import src.fetch_data as fd
import warnings
warnings.filterwarnings("ignore")

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
                where warehouse_other_type = 2 and warehouse_name not like '%TT%'
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
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    df_rate = ck_client.ck_select_to_df(sql)

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

    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

##
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
    c1 = (dwm_sku_2['sales_status'].isin(['正常','涨价缩销'])) & (dwm_sku_2['overage_level'] == 0) & (dwm_sku_2['available_stock'] >= 3) & \
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
    df_temp = df_temp.rename(columns={'section': 'increase_section'})
    df_temp['is_increase'] = np.where(df_temp['increase_section'] > 0, 1, 0)

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
                    dwm_sku_info['预计缺货天数'] <= -3)
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
        df_temp_3[['down_section','down_lowest_profit']] = df_temp_3[['down_section','down_lowest_profit']].fillna(0)
        # df_temp_3['down_section'] = -0.05
        # df_temp_3['down_lowest_profit'] = 0.12
        df_temp_3['is_down'] = np.where(df_temp_3['down_section'] < 0, 1, 0)
        # print(df_temp_3.info())
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
    print('涨价缩销sku判断...')
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

    print('SKU信息及调价幅度已获取，准备写入dwm_sku_test...')
    write_to_sql(dwm_sku_2, 'dwm_sku_test')

    return dwm_sku_2

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
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate','is_increase','is_down']

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
    # dwm_sku_price = adjust_cycle(dwm_sku_price)

    # 销售状态处理
    c0 = (dwm_sku_price['target_profit_rate'] > dwm_sku_price['platform_must_percent']) & \
         (dwm_sku_price['target_profit_rate'] > dwm_sku_price['lowest_profit'])
    c1 = (dwm_sku_price['target_profit_rate'] == dwm_sku_price['platform_must_percent'])
    # c2 = (dwm_sku_price['up_profit_rate'] > 0)
    c3 = (dwm_sku_price['target_profit_rate'] > 0) & (dwm_sku_price['target_profit_rate'] < dwm_sku_price['platform_must_percent'])
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

    dwm_sku_price.drop(['rate', 'ppve', 'refound_fee', 'platform_zero'], axis=1, inplace=True)
    dwm_sku_price['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print(dwm_sku_price.info())
    print('SKU目标价已计算，准备写入dwm_oversea_price_dtl...')
    write_to_sql(dwm_sku_price, 'dwm_oversea_price_test')
    # print('调价数据已写入mysql')
    # 最新日期的数据，也写入CK中，方便CK相关数据调用.
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yibai_oversea')
    # ck_client.ck_execute_sql(sql='truncate table yibai_oversea.dwm_oversea_price_dtl_temp')
    # 空值处理
    # dwm_sku_price['target_profit_y'] = dwm_sku_price['target_profit_y'].fillna(2.0)
    # time.sleep(120)
    # print('CK备份数据...')
    # ck_client.write_to_ck_json_type('yibai_oversea.dwm_oversea_price_dtl_temp', dwm_sku_price)

    return dwm_sku_price
##
dwm_sku_2 = dwm_oversea_sku_base()

dwm_sku_info = dwm_oversea_sku(dwm_sku_2)
##
dwm_price = dwm_sku_price(dwm_sku_info)
##
dwm_price[dwm_price['available_stock'] > 0].to_excel('dwm_price_temp.xlsx', index=0)
##
dwm_sku_info[dwm_sku_info['available_stock'] > 0].to_excel('dwm_sku_info.xlsx', index=0)

##
df_edt = get_edt_date()
##
df_edt.to_excel('df_edt.xlsx', index=0)
##
# 预计到货时间
sql = """
    SELECT 
        a.inbound_number, a.shipment_time, a.departure_time, a.arrival_warehouse_time, a.arrival_warehouse_time_estimate,
        a.goal_warehouse_code, a.goal_country_code, 
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
##
# 预计到货时间处理：1、无时效的，填充为70天；2、时效已过，还未到货的，填充10天后
df_oversea_number['shipment_time'] = df_oversea_number['shipment_time'].astype(str)
c1 = (df_oversea_number['shipment_time'] >= '2024-01-01') & (df_oversea_number['arrival_warehouse_time'].isna())
df_edt = df_oversea_number[c1]
##
c1 = (df_edt['clearance_warehouse'].isna()) | (df_edt['clearance_warehouse'] <= 0)
df_edt['clearance_warehouse'] = np.where(c1, 70, df_edt['clearance_warehouse']).astype(int)

df_edt['estimated_delivery_time'] = pd.to_datetime(df_edt['shipment_time']) + df_edt[
    'clearance_warehouse'] * pd.Timedelta(days=1)
df_edt['estimated_delivery_time'] = df_edt['estimated_delivery_time'].astype(str)
##
df_edt.to_excel('df_edt.xlsx', index=0)