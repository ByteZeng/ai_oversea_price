# TT海外仓调价新逻辑
# part1: 聚合SKU各类信息
# part2: 计算目标价
##
import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_add_logic import get_high_pcs_logic, tt_get_supplier_price, tt_only_supplier_price
from all_auto_task.yunfei_auto import get_transport_fee
warnings.filterwarnings("ignore")
##
# 获取ttsku
def tt_get_sku():
    """
    获取tt的sku
    """
    sql = """
    SELECT 
        sku, title_cn `产品名称`, develop_source, b.develop_source_name,
        CASE 
            when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
            when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
            else toFloat64(product_cost) 
        END as `new_price`
    FROM yibai_prod_base_sync.yibai_prod_sku a
    LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source b
    ON a.develop_source = b.id
    -- where 
        -- develop_source in (14, 15, 22)
        -- or title_cn like '%通拓%' 
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    tt_sku = conn_ck.ck_select_to_df(sql)
    tt_sku = tt_sku.drop_duplicates(subset='sku')

    c1 = tt_sku['develop_source'].isin([14,15,22])
    c2 = tt_sku['产品名称'].str.contains('通拓')
    tt_sku['is_tt_sku'] = np.where(c1 | c2, 1, 0)

    return tt_sku
# tt_sku = tt_get_sku()
#
# 获取SKU的库存和库龄数据
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
                where 
                    warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
                    and warehouse_other_type = 2 and warehouse_name not like '%独享%'
                order by available_stock desc
                ) ps
            left join (
                SELECT 
                    sku, pur_weight_pack, product_length,product_width, product_height, pur_length_pack, pur_width_pack,
                    pur_height_pack, create_time,
                    CASE 
                        when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                        when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                        when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                        when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                        else toFloat64(product_cost) 
                    END as `new_price`
                FROM yibai_prod_base_sync.yibai_prod_sku 
            ) yps on ps.sku=yps.sku
            left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
            having new_price > 0
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
    df_sku_stock['available_stock_money'] = np.where(df_sku_stock['available_stock_money'] < 0, 0, df_sku_stock['available_stock_money'])
    
    return df_sku_stock

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
    df_stock_age_base = df_stock_age[['sku', 'charge_currency', 'cargo_type', 'warehouse_code', 'warehouse_id', 'warehouse_name']]
    df_stock_age_base = df_stock_age_base.drop_duplicates()
    df_stock_age_info = df_stock_age[['sku', 'warehouse_id', 'warehouse', 'warehouse_stock', 'inventory_age', 'charge_total_price','age_30_plus','age_60_plus',
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
        df['overage_level'] = np.select([c1, c2, c3, c4, c5, c6,c7,c8], [360, 270, 180, 150, 120, 90,60,30])
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


# print('获取库存信息...')
# df_stock = tt_get_stock()
# print('获取库龄信息...')
# df_stock_age_id, df_stock_age_warehouse = tt_get_stock_age()
##
def tt_get_warehouse():
    """
    获取tt海外仓仓库信息
    将tt仓库表和yb仓库表合并，tt仓库id都按【10000+id】处理
    """
    sql = """
    SELECT 
        id warehouse_id,warehouse_name,
        case when warehouse_type in (5,6) then '平台仓' WHEN warehouse_type=1 THEN '国内仓' when warehouse_type=8 then '海外仓'
        WHEN warehouse_type=7 THEN '分销' WHEN warehouse_type=9 THEN '进口仓' ELSE '' END AS `仓库类型`, country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country in ('UK', 'GB') THEN '英国仓'
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
            WHEN country='PH' THEN '菲律宾仓'
            WHEN country='TH' THEN '泰国仓'
            WHEN country='MY' THEN '马来西亚仓'
            WHEN country='VN' THEN '越南仓'
            WHEN country='ID' THEN '印尼仓'
            ELSE '其他仓' 
        END AS warehouse
    FROM tt_logistics_tms_sync.tt_warehouse_tt
    """
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    # conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    warehouse = conn.ck_select_to_df(sql)
    warehouse['warehouse_id'] = warehouse['warehouse_id'].apply(lambda x:int('10000'+str(x)))

    sql = """
    SELECT 
        id warehouse_id,name warehouse_name,
        case when type in ('fba','platform') then '平台仓' WHEN type = 'inland' THEN '国内仓' when type in ('third','overseas') then '海外仓'
        WHEN type in ('consignment','transit') THEN '分销'  ELSE '' END AS `仓库类型`,country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country in ('UK', 'GB') THEN '英国仓'
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
            WHEN country='PH' THEN '菲律宾仓'
            WHEN country='TH' THEN '泰国仓'
            WHEN country='MY' THEN '马来西亚仓'
            WHEN country='VN' THEN '越南仓'
            WHEN country='ID' THEN '印尼仓'
            ELSE '其他仓' 
        END AS warehouse
    FROM yb_datacenter.yb_warehouse
    """
    sql = """
    SELECT 
        E.id as warehouse_id, F.name as warehouse,
        case when warehouse_type in (2, 3) then '海外仓' else '非海外仓' end as `仓库类型`,
        E.warehouse_name as warehouse_name, E.country
    FROM yibai_logistics_tms_sync.yibai_warehouse E
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category F ON E.ebay_category_id = F.id
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    yb_warehouse = conn_mx.ck_select_to_df(sql)
    warehouse = warehouse.append(yb_warehouse,ignore_index=True)

    # 筛选海外仓
    warehouse = warehouse[warehouse['仓库类型']=='海外仓']

    return warehouse
# tt_warehouse = tt_get_warehouse()
##
# SKU的日销统计
def tt_get_sku_sales():
    """
    合并通拓和易佰的sku销量
    """
    # yibai
    sql = """

        SELECT 
            SKU as sku,a.warehouse_id warehouse_id, warehouse_name, platform_code,3days_sales,7days_sales,
            15days_sales,30days_sales,
            60days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE 
            platform_code not in ('DIS','WYLFX') and b.warehouse is not Null 
            and b.warehouse_name not like '%%独享%%' and b.warehouse_name not like '%%TT%%'

    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yibai_sales = conn.read_sql(sql)

    # TT
    sql = """

        SELECT 
            sku, warehouse_id, platform_code, 3days_sales,7days_sales,15days_sales,30days_sales,
            60days_sales,90days_sales 
        FROM tt_oversea.tt_sku_sales_statistics a
        WHERE toString(warehouse_id) not like '%10000%'

    """
    ck_client = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_tt_sales = ck_client.ck_select_to_df(sql)
    tt_warehouse = tt_get_warehouse()
    df_tt_sales = pd.merge(df_tt_sales, tt_warehouse[['warehouse_id','warehouse_name','country','warehouse']], how='inner', on=['warehouse_id'])

    df_sales = pd.concat([df_yibai_sales, df_tt_sales])
    # 按大仓聚合
    df = df_sales.groupby(['sku','warehouse']).agg({'3days_sales':'sum','7days_sales':'sum','15days_sales':'sum',
                                                    '30days_sales':'sum','90days_sales':'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales']/7 + 0.2 * df['15days_sales']/15 + 0.1 *df['30days_sales']/30
    df['recent_day_sales'] = df['3days_sales'] / 3
    # df_sales = df_sales.sample(50000)
    return df
##
# df_sales = tt_get_sku_sales()

##
# df_sales.to_excel('F:\Desktop\df_sales_warehouse.xlsx', index=0)
##
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
    df_rate = df_rate[(~df_rate['country'].isna()) & (df_rate['country'] != '')]
    # df_rate = df_rate.drop_duplicates(subset=['charge_currency'])
    return df_rate

def tt_get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """

    sql = f"""
    SELECT 
        platform, site as country, pay_fee+paypal_fee+vat_fee+extra_fee ppve,refound_fee,
        platform_zero, platform_must_percent
    FROM over_sea.tt_yibai_platform_fee
    WHERE platform in ('AMAZON', 'EB','ALI', 'SHOPEE', 'LAZADA','ALLEGRO')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT platform_code platform, site country, toFloat32(net_profit2)/100 as net_profit2,
        toFloat32(net_interest_rate_target)/100 as net_interest_rate_target
        FROM tt_sale_center_listing_sync.tt_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    df_pc = conn_ck.ck_select_to_df(sql)

    # amazon按站点更新
    df = pd.merge(df, df_pc[df_pc['platform']=='AMAZON'], how='left', on=['platform', 'country'])
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df['platform_must_percent'] = np.where(df['net_interest_rate_target'].isna(), df['platform_must_percent'], df['net_interest_rate_target'])
    df.drop(['net_profit2','net_interest_rate_target'], axis=1, inplace=True)

    # eb\ali按平台更新
    col = ['platform','net_profit2','net_interest_rate_target']
    df = pd.merge(df, df_pc[df_pc['platform'].isin(['EB','ALI','ALLEGRO', 'SHOPEE', 'LAZADA'])][col], how='left', on=['platform'])
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df['platform_must_percent'] = np.where(df['net_interest_rate_target'].isna(), df['platform_must_percent'], df['net_interest_rate_target'])
    df.drop(['net_profit2','net_interest_rate_target'], axis=1, inplace=True)

    # df.to_excel('F://Desktop//df_tt.xlsx', index=0)

    return df
# df_rate = get_rate()
##
# 运费数据获取



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

## 聚合
def dwm_oversea_sku():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    print('获取库存信息...')
    df_stock = tt_get_stock()
    print('获取库龄信息...')
    df_stock_age_id, df_stock_age_warehouse = tt_get_stock_age()
    print('获取日销信息...')
    sku_sales = tt_get_sku_sales()

    col = ['sku', 'warehouse', '3days_sales','7days_sales','15days_sales',
           '30days_sales','90days_sales','day_sales', 'recent_day_sales']
    dwm_sku = pd.merge(df_stock, sku_sales[col], how='left', on=['sku', 'warehouse'])
    dwm_sku.info()
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    col = ['sku', 'warehouse','warehouse_stock_age', 'warehouse_stock','age_30_plus','age_60_plus','age_90_plus','age_120_plus',
           'age_150_plus','age_180_plus','age_270_plus','age_360_plus','charge_total_price','charge_currency','overage_level',
           'best_warehouse_id','best_warehouse_name']
    dwm_sku = pd.merge(dwm_sku, df_stock_age_warehouse[col], how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    dwm_sku.iloc[:, 25:35] = dwm_sku.iloc[:, 25:35].fillna(0)
    dwm_sku['overage_level'] = dwm_sku['overage_level'].fillna(0).astype(int)
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
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['available_stock'] <= 0,
                                            dwm_sku['max_id'], dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['available_stock'] <= 0,
                                              dwm_sku['max_name'], dwm_sku['best_warehouse_name'])
    dwm_sku.drop(['max_id', 'max_name'], axis=1, inplace=True)

    # 仓租数据，汇率转化
    df_rate = get_rate()
    dwm_sku = pd.merge(dwm_sku, df_rate, how='left', on='charge_currency')
    dwm_sku = dwm_sku.drop_duplicates()
    dwm_sku['rate'] = dwm_sku['rate'].fillna(0)
    dwm_sku['charge_total_price_rmb'] = dwm_sku['charge_total_price'] * dwm_sku['rate']
    dwm_sku.drop(['charge_total_price', 'warehouse_id', 'best_warehouse', 'rate'], axis=1, inplace=True)

    columns_order = ['sku', 'title', 'type', 'product_status', 'linest', 'last_linest', 'new_price', 'gross',
                     'product_size',
                     'product_package_size', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
                     'available_stock_money', 'on_way_stock', 'warehouse_stock', 'warehouse_stock_age', 'age_30_plus','age_60_plus','age_90_plus',
                     'age_120_plus',
                     'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus', 'overage_level',
                     'charge_total_price_rmb', 'charge_currency',
                     '3days_sales', '7days_sales', '30days_sales', '90days_sales', 'day_sales', 'recent_day_sales', ]
    dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']] = dwm_sku[['best_warehouse_id','age_30_plus','age_60_plus']].astype(int)
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
    print(dwm_sku.info())
    # 销售状态分类:根据超库龄情况判断分类。
    dwm_sku['sales_status'] = '待定'
    dwm_sku['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    print('条件分箱...')
    dwm_sku_2 = cut_bins(dwm_sku)

    # 判断是否新品
    df_new_sku = is_new_sku()
    dwm_sku_2 = pd.merge(dwm_sku_2, df_new_sku, how='left', on=['sku'])

    dwm_sku_2['is_new'] = dwm_sku_2['is_new'].fillna(0).astype(int)

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
    FROM tt_dwm_sku_info
    WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id < '{datetime.datetime.now().strftime('%Y-%m-%d')}')
    """
    df_after_yest = conn.read_sql(sql)
    # 20241030 暂时从0开始
    # df_after_yest['after_profit_yest'] = 0

    dwm_sku_2 = pd.merge(dwm_sku_2, df_after_yest, how='left', on=['sku', 'warehouse'])
    dwm_sku_2['begin_profit'] = np.where(dwm_sku_2['after_profit_yest'].isna(), 0, dwm_sku_2['after_profit_yest'])
    dwm_sku_2.drop('after_profit_yest', axis=1, inplace=True)

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
    dwm_sku_2['after_profit'] = np.where(dwm_sku_2['after_profit'] <= (dwm_sku_2['lowest_profit']-0.08),
                                         dwm_sku_2['lowest_profit']-0.08, dwm_sku_2['after_profit'])
    dwm_sku_2 = dwm_sku_2.drop_duplicates()
    print(dwm_sku_2.info())
    print('SKU信息及调价幅度已获取，准备写入tt_dwm_sku_info...')
    # conn.to_sql(dwm_sku_2, 'tt_dwm_sku_info', if_exists='replace')
    write_to_sql(dwm_sku_2, 'tt_dwm_sku_info')

    return dwm_sku_2

##
# dwm_tt_sku = dwm_oversea_sku()
# ##
# dwm_tt_sku.to_excel('F:\Desktop\dwm_tt_sku.xlsx',index=0)
##
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
    FROM tt_dwm_oversea_price
    WHERE date_id >= date_sub(curdate(),interval 3 day)

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp_t = conn.read_sql(sql)

    # 如果上一次调价周期超过三天，则取最近的调价记录，判断是否调价
    if len(df_temp_t) == 0:
        sql = f"""

        SELECT sku, warehouse, platform, country, target_profit_rate as target_profit_y, is_adjust, date_id
        FROM tt_dwm_oversea_price
        WHERE date_id = (SELECT max(date_id) FROM tt_dwm_oversea_price WHERE date_id < '{datetime.date.today()}')

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

def tt_update_platform_fee():
    sql = """
        SELECT *
        FROM over_sea.tt_yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_pf = conn.read_sql(sql)
    # 原差值表备份
    conn.to_sql(df_pf, 'tt_yibai_platform_fee_backup', if_exists='replace')
    #
    sql = """
        SELECT platform_code platform, site, toFloat32(net_profit2)/100 as net_profit2, 
        toFloat32(net_interest_rate_target)/100 as net_interest_rate_target2
        FROM tt_sale_center_listing_sync.tt_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    # conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_pc = conn_ck.ck_select_to_df(sql)
    #

    df = pd.merge(df_pf, df_pc, how='left', on=['platform', 'site'])

    # 差值替换
    for i in ['net_profit2','net_interest_rate_target2']:
        df.loc[df['platform']=='CDISCOUNT', i] = df_pc.loc[df_pc['platform']=='CDISCOUNT', i].iloc[0]
        df.loc[(df['platform']=='EB') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='EB') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='WALMART') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='WALMART') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='AMAZON') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='AMAZON') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='ALI') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='ALI') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='WISH') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='WISH') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='SHOPEE') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='SHOPEE') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='LAZADA') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='LAZADA') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='ALLEGRO') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='ALLEGRO') & (df_pc['site']=='other'), i].iloc[0]
        df.loc[(df['platform']=='TEMU') & (df[i].isna()), i] = df_pc.loc[
            (df_pc['platform']=='TEMU') & (df_pc['site']=='other'), i].iloc[0]

    #
    df[['platform_zero','net_profit2','net_interest_rate_target2']] = df[['platform_zero','net_profit2','net_interest_rate_target2']].round(4)
    #
    mismatched_rows = df[df['platform_zero'] != df['net_profit2']]
    print('差值更新的平台和国家：')
    mismatched_rows[['platform', 'site','platform_zero','net_profit2']].apply(lambda x: print(f"Platform: {x['platform']}, Site: {x['site']}, "
                                                   f"原差值：{x['platform_zero']},最新差值：{x['net_profit2']}"), axis=1)
    #
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df['platform_must_percent'] = np.where(df['net_interest_rate_target2'].isna(), df['platform_must_percent'], df['net_interest_rate_target2'])
    df.drop(['net_profit2','net_interest_rate_target2'], axis=1, inplace=True)

    # 更新
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_yibai_platform_fee', if_exists='replace')

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
    # 差值表更新
    tt_update_platform_fee()
    # 读取dwm_sku_2
    date_now = time.strftime('%Y-%m-%d')
    sql = f"""
    SELECT *
    FROM dwm_sku_temp_info
    WHERE date_id = '{date_now}'
    -- and warehouse = '美国仓'
    -- LIMIT 10000
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku = conn.read_sql(sql)
    print(dwm_sku.info())

    # 剔除精铺非转泛品sku
    # 20250321 隔离精铺非转泛品的sku
    # 20260310 补充部分转泛品sku
    sku_temp = get_temp_jp_to_fp_sku()
    sku_temp = sku_temp['sku'].unique()
    c0 = dwm_sku['type'].str.contains('海兔|易通兔') | dwm_sku['type'].str.contains('转VC|转泛品')
    c1 = (dwm_sku['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) & (~c0)
    c2 = (dwm_sku['sku'].isin(sku_temp))
    dwm_sku = dwm_sku[(~c1) | c2]
    # 20251128 供应商货盘sku单独定价。位置：tt_only_supplier_price
    dwm_sku = dwm_sku[dwm_sku['type'] != '供应商货盘']
    print(dwm_sku.info())

    # 匹配运费
    # 运费数据需要处理！！！
    df_transport_fee = get_transport_fee()
    col = ['AMAZON','EB','ALLEGRO','SHOPEE','LAZADA','TEMU']
    df_transport_fee = df_transport_fee[df_transport_fee['platform'].isin(col)]
    df_rate = get_rate()
    df_transport_fee = pd.merge(df_transport_fee, df_rate[['country', 'rate']], how='left', on='country')
    df_transport_fee = df_transport_fee[
        ['sku', 'best_fee_id', 'ship_name', 'total_cost', 'ship_fee','firstCarrierCost','lowest_price', 'platform', 'country',
         'rate']].drop_duplicates()
    df_transport_fee = df_transport_fee.rename(columns={'ship_fee':'shippingCost'})

    order_1 = ['sku', 'new_price', 'best_warehouse_id', 'best_warehouse_name', 'warehouse', 'available_stock',
               'sales_status', 'overage_level', 'is_new', 'day_sales', 'recent_day_sales',
               'estimated_sales_days', 'section', 'after_profit', 'lowest_profit', 'up_profit_rate']

    dwm_sku_price = pd.merge(dwm_sku[order_1].fillna(0), df_transport_fee, how='left',
                             left_on=['sku', 'best_warehouse_id'], right_on=['sku', 'best_fee_id'])
    # # 无运费数据处理
    dwm_sku_price.drop(['best_fee_id'], axis=1, inplace=True)
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['total_cost'].isna()]
    # dwm_sku_price = dwm_sku_price[~dwm_sku_price['rate'].isna()]
    dwm_sku_price = dwm_sku_price.drop_duplicates()
    # # 匹配差值表
    df_platform_fee = tt_get_platform_fee()
    dic = {'US':0.3, 'UK':0.3, 'DE':0.35, 'FR':0.35, 'ES':0.35, 'IT':0.35, 'AU':0.3, 'CA':0.3}
    df_platform_fee['fixed_fee'] = df_platform_fee['country'].map(dic)
    # df_platform_fee['fixed_fee'] = df_platform_fee['fixed_fee'].fillna(0).astype(float)
    dwm_sku_price = pd.merge(dwm_sku_price, df_platform_fee, how='left', on=['platform', 'country'])
    # 筛选other行并取第一行（确保other数据存在）
    amazon_de_vals = df_platform_fee[(df_platform_fee['platform'] == 'AMAZON') &
                                     (df_platform_fee['country'] == 'other')].iloc[0]

    fill_mask = (dwm_sku_price['platform'] == 'AMAZON') & (dwm_sku_price['ppve'].isna())
    fill_cols = ['ppve', 'refound_fee', 'platform_zero', 'platform_must_percent']
    for col in fill_cols:
        dwm_sku_price.loc[fill_mask, col] = amazon_de_vals[col]
    #
    # 数据类型转化
    type_columns = ['new_price', 'total_cost', 'lowest_price', 'ppve', 'refound_fee', 'platform_zero',
                    'platform_must_percent', 'fixed_fee']
    dwm_sku_price[type_columns] = dwm_sku_price[type_columns].fillna(0).astype('float64').round(4)
    #
    # 净利率的处理
    # dwm_sku_price['target_profit_rate'] = np.where(
    #     (dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']) < dwm_sku_price['lowest_profit'],
    #     dwm_sku_price['lowest_profit'],
    #     dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit'])
    dwm_sku_price['target_profit_rate'] = dwm_sku_price['platform_must_percent'] + dwm_sku_price['after_profit']
    #
    # # 调价周期设置
    # dwm_sku_price = adjust_cycle(dwm_sku_price)
    #
    # 销售状态处理
    c1 = (dwm_sku_price['target_profit_rate'] == dwm_sku_price['platform_must_percent'])
    c2 = (dwm_sku_price['up_profit_rate'] > 0)
    c3 = (dwm_sku_price['target_profit_rate'] > 0)
    c4 = (dwm_sku_price['target_profit_rate'] <= 0)
    dwm_sku_price['sales_status'] = np.select([c1, c2, c3, c4], ['正常', '回调', '正利润加快动销', '负利润加快动销'])

    # # 20250206 受美国站关税影响，海外仓清仓可以适当减少降幅。临时涨价一次
    # c1 = (dwm_sku_price['day_sales'] >= 1) & (dwm_sku_price['target_profit_rate'] < -0.01)
    # c2 = (dwm_sku_price['day_sales'] >= 0.5) & (dwm_sku_price['day_sales'] < 1) & (dwm_sku_price['target_profit_rate'] < 0.02)
    # c3 = (dwm_sku_price['day_sales'] >= 0.3) & (dwm_sku_price['day_sales'] < 0.5) & (dwm_sku_price['target_profit_rate'] < 0.04)
    # c4 = (dwm_sku_price['day_sales'] >= 0.2) & (dwm_sku_price['day_sales'] < 0.3) & (dwm_sku_price['target_profit_rate'] < 0.05)
    # c5 = (dwm_sku_price['day_sales'] >= 0.1) & (dwm_sku_price['day_sales'] < 0.2) & (
    #             dwm_sku_price['target_profit_rate'] < 0.06)
    # dwm_sku_price['target_profit_rate'] = np.select([c1, c2, c3, c4, c5],
    #                                                 [dwm_sku_price['target_profit_rate']+0.08,
    #                                                  dwm_sku_price['target_profit_rate']+0.05,
    #                                                  dwm_sku_price['target_profit_rate']+0.03,
    #                                                  dwm_sku_price['target_profit_rate']+0.02,
    #                                                  dwm_sku_price['target_profit_rate']+0.01], dwm_sku_price['target_profit_rate'])

    # 价格计算
    dwm_sku_price['price_rmb'] = (dwm_sku_price['new_price'] + dwm_sku_price['total_cost']+dwm_sku_price['fixed_fee']*dwm_sku_price['rate']) / (
                1 - dwm_sku_price['ppve'] - dwm_sku_price['platform_zero'] - dwm_sku_price['target_profit_rate'])
    dwm_sku_price['price_rmb'] = dwm_sku_price['price_rmb'].fillna(0).astype(float).round(2)
    # dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01

    # 2025-12-05 补充供应商货盘sku的定价逻辑。补充海外仓sku无到货记录、供应商货盘sku有货的数据。合并供应商货盘的YM_sku数据
    dwm_sku_price = tt_get_supplier_price(dwm_sku_price)
    df = tt_only_supplier_price()
    dwm_sku_price = pd.concat([dwm_sku_price, df])
    dwm_sku_price = dwm_sku_price[dwm_sku_price['sku']!='']
    # 销毁价判断
    # 20250214 销毁价融入限价
    df_limit = get_limit_price()
    dwm_sku_price = pd.merge(dwm_sku_price, df_limit[['sku','platform', 'country', 'limit_price']],
                             how='left', on=['sku','platform', 'country'])
    c1 = (~dwm_sku_price['limit_price'].isna()) & ((dwm_sku_price['limit_price']*dwm_sku_price['rate'])>dwm_sku_price['lowest_price'])
    dwm_sku_price['lowest_price'] = np.where(c1, dwm_sku_price['limit_price']*dwm_sku_price['rate'], dwm_sku_price['lowest_price'])

    dwm_sku_price['price_rmb'] = np.where(dwm_sku_price['price_rmb'] <= dwm_sku_price['lowest_price'],
                                          dwm_sku_price['lowest_price'], dwm_sku_price['price_rmb'])
    dwm_sku_price['is_distory'] = np.where(
        dwm_sku_price['price_rmb'].astype(int) == dwm_sku_price['lowest_price'].astype(int), 1, 0)

    # 20260320 供应商货盘sku的汇率用折扣前的汇率
    dwm_sku_price = pd.merge(dwm_sku_price, df_rate[['country', 'erp_rate']], how='left', on='country')
    c1 = (dwm_sku_price['is_supplier_price'] == 1)
    dwm_sku_price['rate'] = np.where(c1, dwm_sku_price['erp_rate'], dwm_sku_price['rate'])

    # 本币计算
    dwm_sku_price['price'] = dwm_sku_price['price_rmb'] / dwm_sku_price['rate']
    dwm_sku_price['price'] = dwm_sku_price['price'].round(1) - 0.01

    # dwm_sku_price.drop(['rate', 'ppve', 'refound_fee', 'platform_zero', 'platform_must_percent'], axis=1, inplace=True)
    dwm_sku_price['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')

    dwm_sku_price.drop(['limit_price','erp_rate'], axis=1, inplace=True)
    col = ['available_stock']
    dwm_sku_price[col] = dwm_sku_price[col].fillna(0).astype(int)
    print(dwm_sku_price.info())
    # print('SKU目标价已计算，tt_dwm_oversea_price...')
    # conn.to_sql(dwm_sku_price, 'tt_dwm_oversea_price', if_exists='replace')
    # write_to_sql(dwm_sku_price, 'tt_dwm_oversea_price')

    write_to_ck(dwm_sku_price, 'tt_dwm_oversea_price')
    print('TT调价数据已写入ck：tt_dwm_oversea_price')

    return None

##
# dwm_price = dwm_sku_price()
##

# dwm_price.to_excel('F:\Desktop\dwm_price.xlsx', index=0)
##
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

##
def tt_get_price_data(platform='EB'):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    now = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            sku as sku, best_warehouse_name, warehouse, new_price, total_cost, overage_level, available_stock, sales_status,price,
            price_rmb,target_profit_rate, ppve, fixed_fee, platform_zero, platform_must_percent,
            day_sales, country, ship_name, lowest_price, platform, is_supplier, is_supplier_price
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE (platform = '{platform}' or platform is NULL) and date_id = '{now}' 
        and new_price > 0 and total_cost > 0 and ppve > 0
        -- and warehouse = '德国仓' and country = 'DE' 
        -- and warehouse = '美国仓'
    """
    df_price_dtl = conn_ck.ck_select_to_df(sql)
    if platform == 'EB':
        dic = {'US': 0.3, 'UK': 0.3, 'DE': 0.35, 'FR': 0.35, 'ES': 0.35, 'IT': 0.35, 'AU': 0.3, 'CA': 0.3}
        df_price_dtl['fixed_fee'] = df_price_dtl['country'].map(dic)

    c1 = (df_price_dtl['warehouse'] == '英国仓') & (df_price_dtl['country'] != 'UK')
    c2 = (df_price_dtl['warehouse'] != '英国仓') & (df_price_dtl['country'] == 'UK')
    c3 = (df_price_dtl['warehouse'] == '美国仓') & (df_price_dtl['country'] != 'US')
    df_price_dtl = df_price_dtl[~(c1 | c2 | c3)]
    # df_price_dtl = df_price_dtl.rename(columns={'SKU':'sku','country':'site'})
    # 20240603 amazon平台需要lowest_price字段
    # if platform != 'AMAZON':
    #     df_price_dtl.drop('lowest_price', axis=1, inplace=True)
    print(df_price_dtl['platform'].unique())
    return df_price_dtl
# tt_get_price_data()
##
def tt_get_ebay_commission(df):

    conn_ck = pd_to_ck(database='tt_domestic', data_sys='通拓-新')
    sql = f"""
        SELECT DISTINCT item_id, sku, commission_rate
        FROM  tt_domestic.tt_ebay_commission
        WHERE commission_rate != 0.15
    """
    df_temp = conn_ck.ck_select_to_df(sql)
    if len(df_temp) == 0:
        df_temp = pd.DataFrame(columns=['item_id', 'sku', 'commission_rate'])

    df = pd.merge(df, df_temp, how='left', on=['item_id', 'sku'])
    df['commission_rate'] = df['commission_rate'].fillna(0.11).astype(float)
    # df.drop('commission_rate', axis=1, inplace=True)

    return df


def tt_get_ebay_adjust():
    """
    TTebay平台链接调价数据
    """
    date_today = time.strftime('%Y-%m-%d')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    sql= f"""
        SELECT *
        FROM tt_oversea.tt_ads_oversea_ebay_listing
        WHERE `date_id` = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    tt_ebay_listing = conn_ck_tt.ck_select_to_df(sql)
    tt_ebay_listing.drop(['id','update_time'], axis=1, inplace=True)
    print(f'tt_ebay海外仓链接数量共{len(tt_ebay_listing)}条.')
    df_dtl = tt_get_price_data(platform='EB')
    # 无运费数据，country填充为仓库所在国
    c1 = (df_dtl['warehouse'] == '德国仓') & (df_dtl['country'].isna())
    c2 = (df_dtl['warehouse'] == '英国仓') & (df_dtl['country'].isna())
    c3 = (df_dtl['warehouse'] == '澳洲仓') & (df_dtl['country'].isna())
    c4 = (df_dtl['warehouse'] == '西班牙仓') & (df_dtl['country'].isna())
    c5 = (df_dtl['warehouse'] == '加拿大仓') & (df_dtl['country'].isna())
    c6 = (df_dtl['warehouse'] == '法国仓') & (df_dtl['country'].isna())
    c7 = (df_dtl['warehouse'] == '美国仓') & (df_dtl['country'].isna())
    df_dtl['country'] = np.select([c1,c2,c3,c4,c5,c6,c7], ['DE','UK','AU','ES','CA','FR','US'], df_dtl['country'])
    # 合并链接与价格数据
    df = pd.merge(tt_ebay_listing, df_dtl, how='left', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'item_id', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)

    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform= tt_get_platform_fee()
    # df_platform = df_platform[df_platform['platform']=='EB']
    # df = pd.merge(df, df_platform[['country','ppve','platform_zero','platform_must_percent']], how='left', on=['country'])
    col = ['rate', 'platform_zero','ppve' ,'platform_must_percent']
    df[col] = df[col].fillna(0).astype(float)
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    df['target_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # # 20260304 接入真实佣金率
    df = tt_get_ebay_commission(df)
    df['ppve'] = df['ppve'] - 0.11 + df['commission_rate']
    df.drop('commission_rate', axis=1, inplace=True)

    # 20250106 控制降价幅度，单次不超过15%
    df['online_price'] = df['online_price'].astype(float)
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']+df['fixed_fee']*df['rate']) / (
            df['online_price'] * df['rate'])
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']

    c1 = (df['profit_diff'] < -0.15)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 0.15, df['target_profit_rate'])
    df['profit_diff'] = np.where(c1, -0.15, df['profit_diff'])
    # 价格计算
    c2 = (df['new_price'] + df['total_cost']+df['fixed_fee']*df['rate']) / (
                1 - df['ppve'] - df['target_profit_rate'])
    df['price_rmb'] = np.where(c1, c2, df['price_rmb'])
    df['price_rmb'] = df['price_rmb'].fillna(0).astype(float).round(2)
    # 销毁价判断
    df['price_rmb'] = np.where(df['price_rmb'] <= df['lowest_price'], df['lowest_price'], df['price_rmb'])
    print(df.info())
    # 本币计算
    df['price'] = df['price_rmb'] / df['rate']
    df['price'] = df['price'].round(1) - 0.01

    # 阶梯定价
    df['flag'] = df.groupby(['sku', 'warehouse','country'])['price'].rank(method='first')
    df['price'] = df['price'] + df['flag'] * 0.01
    df.drop('flag', axis=1, inplace=True)
    print(df.info())
    write_to_sql(df, 'tt_oversea_ebay_listing_all')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_ebay_listing_all', if_exists='replace')
    print('ttebay海外仓链接调价数据存表完成.')
    return None

def tt_get_ali_adjust():
    """
    TT_ali平台链接调价数据
    """
    date_today = time.strftime('%Y-%m-%d')

    sql= f"""
        SELECT *
        FROM over_sea.tt_ali_oversea_listing
        WHERE `date_id` = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    tt_listing = conn.read_sql(sql)
    tt_listing = tt_listing.rename(columns={'sku_price':'online_price'})
    df_dtl = tt_get_price_data(platform='ALI')
    # 无运费数据，country填充为仓库所在国
    c1 = (df_dtl['warehouse'] == '德国仓') & (df_dtl['country'].isna())
    c2 = (df_dtl['warehouse'] == '英国仓') & (df_dtl['country'].isna())
    c3 = (df_dtl['warehouse'] == '澳洲仓') & (df_dtl['country'].isna())
    c4 = (df_dtl['warehouse'] == '西班牙仓') & (df_dtl['country'].isna())
    c5 = (df_dtl['warehouse'] == '加拿大仓') & (df_dtl['country'].isna())
    c6 = (df_dtl['warehouse'] == '法国仓') & (df_dtl['country'].isna())
    c7 = (df_dtl['warehouse'] == '美国仓') & (df_dtl['country'].isna())
    c8 = (df_dtl['warehouse'] == '俄罗斯仓') & (df_dtl['country'].isna())
    c9 = (df_dtl['warehouse'] == '巴西仓') & (df_dtl['country'].isna())
    df_dtl['country'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9],
                                  ['DE','UK','AU','ES','CA','FR','US','RU','BR'], df_dtl['country'])
    # 合并链接与价格数据
    tt_listing.drop('warehouse', axis=1, inplace=True)
    df = pd.merge(tt_listing, df_dtl, how='left', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'product_id', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)

    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform= tt_get_platform_fee()
    # # 取平台各项费率
    # df_platform = df_platform[df_platform['platform']=='ALI']
    # df = pd.merge(df, df_platform[['country','ppve','platform_zero','platform_must_percent']], how='left', on=['country'])
    col = ['platform_zero','ppve' ,'platform_must_percent']
    df[col] = df[col].fillna(0).astype(float)
    df['rate'] = df['rate'].fillna(1).astype(float)
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 20240223 控制正常品的涨降幅度，单次不超过10%
    # 20240911 控制降价幅度，单次不超过10%.
    # df = df[df['online_price'] > 0]
    df['online_price'] = df['online_price'].astype(float)
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']
    print(df.info())
    df.drop(['price_list','platform_sku'], axis=1, inplace=True)
    write_to_sql(df, 'tt_oversea_ali_listing_all')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_ali_listing_all', if_exists='replace')
    print('tt_ali海外仓链接调价数据存表完成.')
    return None

def get_amazon_coupon(ck_client):
    today = time.strftime('%Y-%m-%d')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yibai_oversea')
    sql = f"""
        with table1 as (
            select toInt32(id) from tt_system_kd_sync.tt_amazon_account 
            where site ='us'
        )
        SELECT account_id,asin,coupon_type,percentage_off,money_off 
        FROM tt_product_kd_sync.tt_amazon_coupon 
        where coupon_status in (8, 11) and start_date<='{today} 00:00:00' and end_date>=DATE_ADD(day, -2, now()) 
        and is_delete=0 and disable in (0, 5) 
        order by start_date desc limit 1 by account_id,asin 

        """
    """
            union all 
        -- 20230227 增加销售中台数据
        select erp_id as account_id,asin,coupon_type,percentage_off,money_off 
        from tt_sale_center_operatemanage_sync.tt_amazon_coupon 
        where (
            (
                coupon_status in (2,5,6,8) or 
                (coupon_status = 7 and error_message like '%%Discount for following skus is not in the range of 5%-50% of sale price%%')
            ) 
        or (erp_id in table1 and coupon_status=99 and now() <'2023-07-14')
        )
        and start_date<=today() and end_date>=DATE_ADD(day, -2, now()) and stop_progress != 1 
        order by start_date desc limit 1 by account_id,asin
    """
    df0 = ck_client.ck_select_to_df(sql)
    return df0
def amazon_coupon_fu(df):
    ck_client = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    df0 = get_amazon_coupon(ck_client)
    print(df0.info())
    # percentage_off
    if len(df0) == 0:
        df0 = pd.DataFrame(columns=['account_id','asin','coupon_type','percentage_off','money_off'])
    df1 = df0[df0['coupon_type'] == 1]
    df1 = df1[['account_id', 'asin', 'percentage_off']]
    df1['percentage_off'] = df1['percentage_off'].apply(lambda m: float(m) / 100)
    df1 = df1.sort_values(by=['percentage_off'], ascending=False)
    df1 = df1.drop_duplicates(subset=['account_id', 'asin'], keep='first')
    df = df.merge(df1, on=['account_id', 'asin'], how='left')
    df['percentage_off'].fillna(0, inplace=True)
    print('money_off')
    # money_off
    df2 = df0[df0['coupon_type'] == 2]
    df2 = df2[['account_id', 'asin', 'money_off']]
    df2['money_off'] = df2['money_off'].apply(lambda m: float(m))
    df2 = df2.sort_values(['money_off'], ascending=False)
    df2 = df2.drop_duplicates(subset=['account_id', 'asin'], keep='first')
    df = df.merge(df2, on=['account_id', 'asin'], how='left')
    df['money_off'].fillna(0, inplace=True)
    print('coupon')
    # 优惠券手续费
    coupon_shouxufei = pd.DataFrame(
        {'站点': ['美国', '加拿大', '墨西哥', '英国', '法国', '德国', '西班牙', '意大利', '日本'],
         'Coupon_handling_fee': [0.6, 0.6, 0, 0.45, 0.5, 0.5, 0.5, 0.5, 60]})
    df = df.merge(coupon_shouxufei, on=['站点'], how='left')
    df['Coupon_handling_fee'].fillna(0, inplace=True)
    df.loc[(df['percentage_off'] == 0) & (df['money_off'] == 0), 'Coupon_handling_fee'] = 0
    return df
def get_amazon_promotion():
    """
    获取Amazon的促销数据
    """
    ck_client = pd_to_ck(database='tt_fba', data_sys='通拓-新')
    sql = """
    SELECT account_id, seller_sku, promotion_percent,promotion_amount,promotion_source
    FROM tt_fba.tt_amazon_promotion
    """
    df_amazon_promotin = ck_client.ck_select_to_df(sql)

    print(df_amazon_promotin.info())

    return df_amazon_promotin

## 通拓amazon不调价表
def tt_get_amazon_filter():
    """ """
    now = time.strftime('%Y-%m-%d')
    sql = """
    SELECT DISTINCT account_id ,seller_sku, 1 as is_white_listing 
    from tt_fba.amazon_listing_para where para_type = 7  and date(end_time) >= today()
    """
    conn_ck = pd_to_ck(database='tt_fba', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)

    sql = f"""
    SELECT DISTINCT account_id, seller_sku, 1 as is_white_listing
    FROM tt_product_kd_sync.tt_amazon_adjustprice_filter_sku
    WHERE start_time <= '{now}' and end_time >= '{now}' and `status` = 1
    """
    df_filter = conn_ck.ck_select_to_df(sql)

    df = pd.concat([df, df_filter])

    print(f'AMAZON不调价链接数量共{len(df)}条.')

    return df

# amazon真实佣金率
# 分账号取数据
def get_amazon_commission():
    now = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT distinct account_id
        FROM tt_oversea.tt_ads_oversea_amazon_listing_all 
        where date_id = '{now}'
    """
    conn_ck = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    df_account = conn_ck.ck_select_to_df(sql)

    #
    step = 200
    account_tuple = tuple(df_account['account_id'].unique())

    df_re = pd.DataFrame()
    for i in range(int(len(account_tuple)/step)+1):
        account_id = account_tuple[i*step:(i+1)*step]
        sql = f"""
            SELECT account_id, seller_sku, `佣金率`
            FROM  tt_fba.tt_amazon_referral_fee
            WHERE `佣金率` != 0.15 and account_id in {account_id}
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_re = pd.concat([df_temp, df_re])

    df_re = df_re.drop_duplicates(subset=['account_id', 'seller_sku'])

    return df_re


def tt_get_amazon_adjust():
    """
    TT_amazon平台链接调价数据
    """
    date_today = time.strftime('%Y-%m-%d')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    sql= f"""
        SELECT account_id, short_name, upper(site) country, sku, seller_sku,
        asin, online_price,  date_id
        FROM tt_oversea.tt_ads_oversea_amazon_listing_all
        WHERE `date_id` = '{date_today}' and deliver_mode = 2
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    tt_listing = conn_ck_tt.ck_select_to_df(sql)
    country_list = ['AE','BE','SA','BR','TR']
    print(f'TTAmazon链接获取完成，共{len(tt_listing)}条.')
    tt_listing = tt_listing.rename(columns={'sku_price':'online_price'})
    df_dtl = tt_get_price_data(platform='AMAZON')
    print(f'TTAmazon调价数据获取完成，共{len(df_dtl)}条.')
    # 无运费数据，country填充为仓库所在国
    c1 = (df_dtl['warehouse'] == '德国仓') & (df_dtl['country'].isna())
    c2 = (df_dtl['warehouse'] == '英国仓') & (df_dtl['country'].isna())
    c3 = (df_dtl['warehouse'] == '澳洲仓') & (df_dtl['country'].isna())
    c4 = (df_dtl['warehouse'] == '西班牙仓') & (df_dtl['country'].isna())
    c5 = (df_dtl['warehouse'] == '加拿大仓') & (df_dtl['country'].isna())
    c6 = (df_dtl['warehouse'] == '法国仓') & (df_dtl['country'].isna())
    c7 = (df_dtl['warehouse'] == '美国仓') & (df_dtl['country'].isna())
    c8 = (df_dtl['warehouse'] == '俄罗斯仓') & (df_dtl['country'].isna())
    c9 = (df_dtl['warehouse'] == '巴西仓') & (df_dtl['country'].isna())
    df_dtl['country'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9],
                                  ['DE','UK','AU','ES','CA','FR','US','RU','BR'], df_dtl['country'])
    # 合并链接与价格数据
    # tt_listing.drop('warehouse', axis=1, inplace=True)
    df = pd.merge(tt_listing, df_dtl, how='left', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)
    print('开始处理coupon及promotion数据...')
    site_name_dic = {'UK': '英国', 'US': '美国', 'DE': '德国', 'JP': '日本', 'PL': '波兰', 'FR': '法国', 'AU': '澳洲',
                     'NL': '荷兰', 'ES': '西班牙', 'IT': '意大利', 'SE': '瑞典', 'CA': '加拿大', 'MX': '墨西哥'}
    df['站点'] = df['country'].replace(site_name_dic)
    # coupon和promotion数据
    # 沿用原调价逻辑代码
    df['account_id'] = df['account_id'].astype("int")
    df = amazon_coupon_fu(df)
    df_amazon_promotion = get_amazon_promotion()
    if len(df_amazon_promotion) == 0:
        df_amazon_promotion = pd.DataFrame(columns=['account_id', 'seller_sku', 'promotion_percent','promotion_amount','promotion_source'])
    df = pd.merge(df, df_amazon_promotion, how='left', on=['account_id', 'seller_sku'])
    # 日本站积分数据：your_price_point 当前基本为0, 暂时直接置为0
    df['your_price_point'] = 0
    df[['promotion_percent', 'promotion_amount']] = df[['promotion_percent', 'promotion_amount']].fillna(0)
    print(df.info())
    #
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform= tt_get_platform_fee()
    # # 取平台各项费率
    # df_platform = df_platform[df_platform['platform']=='AMAZON']
    # df = pd.merge(df, df_platform[['country','ppve','platform_zero','platform_must_percent']], how='left', on=['country'])
    col = ['platform_zero','ppve' ,'platform_must_percent','online_price']
    df[col] = df[col].fillna(0).astype(float)
    df['rate'] = df['rate'].fillna(1).astype(float)
    df = df.drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'])
    # 计算毛利率
    df['target_profit_rate'] = df['target_profit_rate'] + df['platform_zero']

    # 20260105 接入真实佣金率
    df_com = get_amazon_commission()
    df['account_id'] = df['account_id'].astype(int)
    df = pd.merge(df, df_com, how='left', on=['account_id', 'seller_sku'])
    df['佣金率'] = df['佣金率'].fillna(0.15).astype(float)
    df['ppve'] = df['ppve'] - 0.15 + df['佣金率']
    df['price'] = (df['new_price']+df['total_cost']) / (1-df['ppve']-df['target_profit_rate']) / df['rate']
    df['price'] = np.where(df['price'] * df['rate'] < df['lowest_price'], df['lowest_price'] / df['rate'], df['price'])

    # 供应商货盘sku定价的链接不重新计算价格
    df_2 = df[df['is_supplier_price']==1]
    df = df[df['is_supplier_price']!=1]

    # 20240223 控制正常品的涨降幅度，单次不超过10%
    # 20240911 控制降价幅度，单次不超过10%.
    df = df[df['online_price'] > 0]
    df['online_price'] = df['online_price'].astype(float)
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate'])
    # df['online_profit'] = 0
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']
    c1 = (df['profit_diff'] < -0.15)
    # c2 = (df['profit_diff'] > 0.15) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.15)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 0.15, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    df['profit_diff'] = np.where(c1, -0.15, df['profit_diff'])
    # df['profit_diff'] = np.where(c2, df['target_profit_rate'] - df['online_profit'], df['profit_diff'])

    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = np.where(c1, cp, df['price'])
    # df['price'] = np.where(c2, cp, df['price'])
    # df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)
    df['no_coupon_price'] = df['price']
    df['price'] = (df['price'] + (df['money_off'] + df['Coupon_handling_fee'] + df['promotion_amount'])) / \
                  (1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])
    df['price'] = df['price'].round(1) - 0.01

    # 合并供应商货盘sku的定价数据
    df = pd.concat([df, df_2])
    df.drop(['站点','your_price_point','money_off','Coupon_handling_fee', '佣金率',
             'promotion_amount','percentage_off','promotion_percent','promotion_source'], axis=1, inplace=True)
    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price'] / df['rate']), df['lowest_price'] / df['rate'],
                           df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost']) / df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price'] / df['rate']), c1, df['target_profit_rate'])
    # df.drop(['online_profit', 'profit_diff', '佣金率', 'lowest_price'], axis=1, inplace=True)

    # 1、不调价链接
    df_filter = tt_get_amazon_filter()
    # df_filter['is_white_listing'] = 1
    df = pd.merge(df, df_filter, how='left', on=['account_id','seller_sku'])
    df['is_white_listing'] = df['is_white_listing'].fillna(0).astype(int)
    print(f"不调价链接共{len(df[df['is_white_listing'] == 1])}条.")
    print(df.info())
    # df = df[df['is_white_listing'] != 1]

    # df.drop('is_white_listing', axis=1, inplace=True)
    # df.to_excel('F://Desktop//tt_amazon_listing.xlsx', index=0)
    write_to_sql(df, 'tt_oversea_amazon_listing_all')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_amazon_listing_all', if_exists='replace')
    print('tt_amazon_海外仓链接调价数据存表完成.')

    return None
##
def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku, title_cn `产品名称`, b.category_path as `产品线路线` 
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
    df = pd.merge(df, df_line[['sku','一级产品线','二级产品线']], how='left', on=['sku'])

    return df

def normal_sku_price_adjust(df):
    """
    正常品sku链接调价逻辑
    profit_diff = online_profit - target_profit_rate
    """
    col_name = 'target_profit_rate'
    c1 = (df['profit_diff'] > 0.03) & (df['sales_status'] == '正常')  & (df['available_stock'] > 0)
    c2 = (df['profit_diff'] < -0.05) & (df['sales_status'] == '正常') & (df['available_stock'] > 0)
    df[col_name] = np.where(c1, df['online_profit'] - 0.03, df['target_profit_rate'])
    df[col_name] = np.where(c1 & (df['online_profit'] > 0.15), 0.15, df[col_name])
    # df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    df[col_name] = np.where(c2, df['online_profit'] + 0.05, df[col_name])
    df[col_name] = np.where(c2 & (df['online_profit'] < 0), 0.01, df[col_name])

    # sku大仓维度 30天销量<=2，且可售天数超150天降低目标利润率要求2%
    sql = """
        SELECT sku, warehouse, `30days_sales`, estimated_sales_days
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-06-20')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)
    df = pd.merge(df, df_temp, how='left', on=['sku','warehouse'])
    c1 = (df['30days_sales'] <= 2) & (df['estimated_sales_days'] > 150) & (
            df['sales_status'] == '正常') & (df[col_name] >= 0.05)
    df[col_name] = np.where(c1, df[col_name]-0.02, df[col_name])

    df.drop(['30days_sales','estimated_sales_days'], axis=1, inplace=True)

    return df


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
    df_order_info = df_order_info.rename(columns={'item_id':'product_id', 'sku':'order_sku'})

    df_ali = pd.merge(df_ali, df_order_info, how='left', on=['product_id', 'account_id'])
    print(df_ali.info())

    return df_ali


def tt_get_shopee_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    sql = f"""
        SELECT
            account_id, short_name account_name, item_id, sku, parent_sku, is_mulit, date_id, online_price, country
        FROM tt_oversea.tt_ads_oversea_shopee_listing
        WHERE date_id = '{now_time}'
        and status_online = 'NORMAL'
    """
    df_listing = conn_ck_tt.ck_select_to_df(sql)

    df_dtl = tt_get_price_data(platform='SHOPEE')

    # 合并链接与价格数据
    df = pd.merge(df_listing, df_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'item_id', 'country'], keep='first')
    # df.drop('overage_level', axis=1, inplace=True)
    print(df.info())
    # # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])

    # df_platform_fee = tt_get_platform_fee()
    # df_platform_fee = df_platform_fee[df_platform_fee['platform'] == 'SHOPEE']
    # df_platform_fee.drop('platform', axis=1, inplace=True)
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    # df = df.drop_duplicates()
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 控制降价幅度，单次不超过20%.
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']

    t = 0.05
    c1 = (df['profit_diff'] < -t)
    # c2 = (df['profit_diff'] < -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - t, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.1, df['target_profit_rate'])
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    # df['profit_diff'] = df['online_profit'] - df['target_profit_rate']

    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')

    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & (df['day_sales'] > 0.3))
    c2 = (df['available_stock'] == 0)
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2, 1, 0)

    #
    # 4、不调价账号
    account_id = [30812,30813,30814,30815,30816,30817,30818,20787] # 2025-10-24销售备注非3pf账号
    short_name = ["andoerofficial.th", "toolme.th.th", "cnoffschool.ph", "fannydream.ph", "carstudio.my",
                  "carhome0.ph", "kkmoon.th", "lemon_tree.ph", "loca609.th", "lixadaofficial.th", "gamedada.ph",
                  "mamamoo1.my", "mamamoo1.ph", "joyfeel.ph", "kkmoon.os", "malory.th", "muswanna.th",
                  "musicalhome.ph", "bentu609.ph", "musicalbase.ph", "girlstool.th", "bestopt0.th", "cameraworld1.th",
                  "docooler.os", "outstyle.ph", "outstyle.th", "tools0.th", "toolwe.th.th", "xiaomiyoupin.th",
                  "keipong.th", "localcamera.th", "lifesun.ph", "loca609.ph", "andoerofficial.ph",
                  "blue_coconut.ph", "pusatbeli0.my"]
    c1 = (df['account_id'].isin(account_id))
    c2 = (df['account_name'].isin(short_name))
    df['is_white_account'] = np.where(c1 | c2, 1, 0)

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)

    df.drop(['is_supplier','is_supplier_price'], axis=1, inplace=True)

    print(df.info())
    # df.to_excel('F://Desktop//df_shopdtl.xlsx', index=0)
    print('全量shopee海外仓链接存表...')
    write_to_sql(df, 'tt_oversea_shopee_listing_all')
    # conn.to_sql(df, 'tt_oversea_shopee_listing_all', if_exists='replace')


def tt_get_lazada_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')

    sql = f"""
        SELECT
            account_id, short_name account_name, item_id, seller_sku, sku, parent_sku, is_mulit, date_id, online_price, country
        FROM tt_oversea.tt_ads_oversea_lazada_listing
        WHERE date_id = '{now_time}'
        and status_online != 'Suspended'
    """
    df_listing = conn_ck_tt.ck_select_to_df(sql)

    df_dtl = tt_get_price_data(platform='LAZADA')

    # 合并链接与价格数据
    df = pd.merge(df_listing, df_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'item_id', 'country'], keep='first')
    # df.drop('overage_level', axis=1, inplace=True)
    print(df.info())
    # # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])

    # df_platform_fee = tt_get_platform_fee()
    # df_platform_fee = df_platform_fee[df_platform_fee['platform'] == 'LAZADA']
    # df_platform_fee.drop('platform', axis=1, inplace=True)
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    # df = df.drop_duplicates()
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 控制降价幅度，单次不超过20%.
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']

    t = 0.05
    c1 = (df['profit_diff'] < -t)
    # c2 = (df['profit_diff'] < -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - t, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.1, df['target_profit_rate'])
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    # df['profit_diff'] = df['online_profit'] - df['target_profit_rate']

    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')

    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & (df['day_sales'] > 0.3))
    c2 = (df['available_stock'] == 0)
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2, 1, 0)

    #
    # 4、不调价账号
    # account_id = []
    # c1 = (df['account_id'].isin(account_id))
    sku_list = ['I2214']
    c1 = (df['sku'].isin(sku_list))
    df['is_white_account'] = np.where(c1, 1, 0)

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)
    df.drop(['is_supplier', 'is_supplier_price'], axis=1, inplace=True)
    print(df.info())
    # df.to_excel('F://Desktop//df_dtl.xlsx', index=0)
    print('全量lazada海外仓链接存表...')
    write_to_sql(df, 'tt_oversea_lazada_listing_all')
    # conn.to_sql(df, 'tt_oversea_lazada_listing_all', if_exists='replace')

    tt_get_allegro_adjust_listing()

def tt_get_allegro_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    sql = f"""
        SELECT 
            account_id, account_name, offer_id, sku, date_id, online_price, selling_mode_currency, location,
            country, group_name
        FROM tt_oversea.tt_ads_oversea_allegro_listing 
        WHERE date_id = '{now_time}' and status in (1, 4)
    """
    df_listing = conn_ck.ck_select_to_df(sql)
    df_dtl = tt_get_price_data(platform='ALLEGRO')

    # 合并链接与价格数据
    df = pd.merge(df_listing, df_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'offer_id', 'country'], keep='first')
    # df.drop('overage_level', axis=1, inplace=True)
    print(df.info())
    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform_fee = get_platform_fee('ALLEGRO')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df = df.drop_duplicates()
    # 20240223 控制正常品的涨降幅度，单次不超过10%
    # 20240911 控制降价幅度，单次不超过10%.
    # 20250217 兜底价上调，单次最大涨幅不超过20%
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']
    #
    c1 = (df['profit_diff'] < -0.2) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    c2 = (df['profit_diff'] > 0.2) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 0.2, df['target_profit_rate'])
    df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.2, df['target_profit_rate'])
    # 20250627 接入正常品调价逻辑
    # df = normal_sku_price_adjust(df)
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    df['profit_diff'] = df['target_profit_rate'] - df['online_profit']
    # df.to_excel('F://Desktop//df_allegro_all.xlsx', index=0)
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # SKU+大仓+国家维度下，存在多个item_id时，阶梯定价.
    # 价格需要减去 shipping_fee
    # df['flag'] = df.groupby(['sku', 'warehouse','country'])['price'].rank(method='first')
    # df['price'] = df['price'] + df['flag'] * 0.01
    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')
    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    # # 1、20250630 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    # listing_day_sales = get_listing_day_sales_new(platform='ALLEGRO')
    # listing_day_sales['account_id'] = listing_day_sales['account_id'].astype(int)
    # listing_day_sales['seller_sku'] = listing_day_sales['seller_sku'].astype(str)
    # listing_day_sales = listing_day_sales.rename(columns={'seller_sku':'offer_id'})
    # df = pd.merge(df, listing_day_sales, how='left', on=['account_id','offer_id'])
    # c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))

    c2 = (df['available_stock'] == 0) & (df['price'] < df['online_price'])
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c2, 1, 0)

    #
    # # 4、不调价账号
    account_id = []
    c1 = (df['account_id'].isin(account_id))
    df['is_white_account'] = np.where(c1, 1, 0)
    # sql = """
    #     SELECT a.account_id account_id, b.id id, start_time, end_time
    #     FROM tt_domestic.allegro_no_adjust_account a
    #     LEFT JOIN tt_sale_center_system_sync.tt_system_account b
    #     ON a.account_id = b.account_id
    # """
    # df_a = conn_ck.ck_select_to_df(sql)
    # c2 = (df['account_id'].isin(df_a['id'].unique())) & (df['is_white_account']==0)
    # df['is_white_account'] = np.where(c2, 1, df['is_white_account'])

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)
    df.drop(['lowest_price','price_rmb','fixed_fee','platform'], axis=1, inplace=True)
    # df.drop(['listing_day_sales'], axis=1, inplace=True)
    print(df.info())
    print('全量allegro海外仓链接存表...')
    write_to_ck(df, 'tt_oversea_allegro_listing_all')

    # # 筛除后存表
    # df = df[(df['is_normal_cdt']==0) & (df['is_small_diff']==0)]
    # print('剔除筛选逻辑后共{}条数据.'.format(len(df)))
    # # 筛选后的数据再次存入sql
    # del_col = ['is_normal_cdt', 'is_small_diff']
    # df.drop(del_col, axis=1, inplace=True)
    #
    # print('数据存入ck:oversea_allegro_listing_upload_temp')
    # # 20240425调价记录数据准备迁入CK
    # write_to_ck(df, 'oversea_allegro_listing_upload_temp')



# 订单监控报表
def tt_sales_status():
    """  获取tt的销售状态 """
    date_today = time.strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM oversea_sale_status
        WHERE end_time IS NULL

        UNION ALL

        SELECT *
        FROM tt_oversea_sale_status_temp
        WHERE end_time IS NULL AND start_time <= '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)
    df_status = df_status.drop_duplicates()
    #
    df_status = df_status.drop_duplicates(subset=['sku', 'warehouse'])

    return df_status
##
def tt_order_fc():
    """  TT订单：ERP和OMS """

    # TT ERP订单
    sql = """

        SELECT transaction_id platform_order_id, sku tt_sku, date_day, item_id tt_item_id, 
        gross, net, transfer_fee, country
        FROM temp.dwd_order_sku_input_date_list_di
        WHERE platform in ('TEMU','amazon') and date_day >= '2024-11-15'
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

def get_yibai_logistics_logistics():
    """  渠道名 """
    sql = """
        SELECT distinct ship_code, ship_name
        FROM yibai_tms_logistics_sync.yibai_logistics_logistics
    """
    conn_ck = pd_to_ck(database='', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # df.to_excel('F://Desktop//df_logistics.xlsx', index=0)
    return df

def tt_get_supplier_order_sku():
    """ 获取供应商订单实际配库sku """
    sql = """
    select 
        d.order_id as order_id,b.sku as package_detail_sku,a.similar_sku as `配库sku`
    from tt_oms_sync.tt_oms_order_package_sku_allot_match_sup a
    join tt_oms_sync.tt_oms_order_package_detail b on b.id = a.package_detail_id and b.sku = a.merchant_sku
    join tt_oms_sync.tt_oms_order_package_relation d on d.package_id = a.package_id 
    where a.is_delete = 0 and a.use_status in (5) and d.is_delete = 0 
    """
    conn_ck = pd_to_ck(database='tt_oms_sync', data_sys='通拓-新')
    df = conn_ck.ck_select_to_df(sql)

    col = ['order_id','配库sku']
    df = df[col].drop_duplicates()

    return df

def get_sku_type(df_order):
    # 开发来源
    sql = f"""
            select a.sku `配库sku`,  develop_source_name
            from yibai_prod_base_sync.yibai_prod_sku a
            left join yibai_prod_base_sync.yibai_prod_category b
            on toInt32(a.product_category_id) = toInt32(b.id)
            INNER JOIN (
                SELECT distinct id as develop_source, develop_source_name
                FROM yibai_prod_base_sync.yibai_prod_develop_source
                -- WHERE develop_source_name = '供应商货盘'
            ) c on a.develop_source = c.develop_source

        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df = df.drop_duplicates(subset='配库sku')

    df_order = pd.merge(df_order, df[['配库sku', 'develop_source_name']], how='left', on=['配库sku'])

    return df_order

def get_supplier_order(df_order):
    """ 供应商sku订单的差值需要减5% """

    # 1、配库sku
    df_order_sku = tt_get_supplier_order_sku()
    df_order = pd.merge(df_order, df_order_sku, how='left', on='order_id')
    df_order['配库sku'] = np.where(df_order['配库sku'].isna(), df_order['sku'], df_order['配库sku'])

    # 2、供应商sku
    df_order = get_sku_type(df_order)

    # 3、差值替换
    sql = """

         SELECT platform_code, site, toFloat32(net_profit2)/100 as `差值sup`
         FROM tt_sale_center_listing_sync.tt_listing_profit_config
         WHERE shipping_type = 5 and is_del = 0 and status = 1
     """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['site'] == 'other']
    df_fee2 = df_fee2.rename(columns={'差值sup': '差值sup2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    df_order = pd.merge(df_order, df_fee1, how='left', on=['platform_code', 'site'])
    df_order = pd.merge(df_order, df_fee2[['platform_code', '差值sup2']], how='left', on=['platform_code'])

    df_order['差值sup'] = np.where(df_order['差值sup'].isna(), df_order['差值sup2'], df_order['差值sup'])

    c1 = (df_order['develop_source_name'] == '供应商货盘') & (~df_order['差值sup'].isna())
    df_order['差值'] = np.where(c1, df_order['差值sup'], df_order['差值'])
    # df_order['差值'] = np.where(c1, df_order['差值']-0.05, df_order['差值'])

    df_order.drop(['配库sku', '差值sup', '差值sup2'], axis=1, inplace=True)

    return df_order


def tt_dwm_order():
    """
    TT获取订单数据
    """
    # 获取tt海外仓
    warehouse = tt_get_warehouse()

    # 取订单表
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
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
        a.ship_country site,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        d.sku as sku,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.ship_code,
        a.platform_order_id as platform_order_id,
        a.payment_time as payment_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        toInt64(d.quantity) as quantity,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        d.quantity as release_money,
        d.sales_status as sales_status,
        commission_fees, pay_cost, seller_discount, escrow_tax,
        purchase_cost_new1,
        case 
            WHEN c.true_shipping_fee > 0 THEN true_shipping_fee
            else shipping_cost 
        end as shipping_fee,
        toFloat64(first_carrier_cost)+toFloat64(duty_cost) first_carrier_cost,
        toFloat64(processing)+toFloat64(package_cost)+toFloat64(oversea_package_fee)+toFloat64(pack) processing,
        toFloat64(extra_price)+toFloat64(exceedprice)+toFloat64(residence_price) extra, 
        toFloat64(stock_price)+toFloat64(exchange_price) stock_exchange
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
    # 20251205 切换取中台差值表。先用平台+国家匹配，再用平台匹配，最后匹配不上的填充0.17
    sql = """

         SELECT platform_code, site, toFloat32(net_profit2)/100 as `差值`
         FROM tt_sale_center_listing_sync.tt_listing_profit_config
         WHERE shipping_type = 2 and is_del = 0 and status = 1
     """
    conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['site'] == 'other']
    df_fee2 = df_fee2.rename(columns={'差值': '差值2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    order_base = pd.merge(order_base, df_fee1, how='left', on=['platform_code', 'site'])
    order_base = pd.merge(order_base, df_fee2[['platform_code', '差值2']], how='left', on=['platform_code'])
    order_base['差值'] = np.where(order_base['差值'].isna(), order_base['差值2'], order_base['差值'])
    order_base['差值'] = order_base['差值'].fillna(0.17).astype(float)
    # 20260316 补充sku开发来源。可以识别出供应商sku
    order_base = get_supplier_order(order_base)

    order_base["real_profit"] = order_base["true_profit"] - order_base["差值"] * order_base["total_price"]
    order_base["real_profit"] = order_base["real_profit"].astype(float)
    order_base["real_profit"] = order_base["real_profit"].round(4)
    order_base.drop(['差值2', '差值'], axis=1, inplace=True)

    order_base = order_base.rename(columns={'true_profit': 'true_profit_new1','site': 'ship_country'})
    col = ['total_price','true_profit_new1','commission_fees','pay_cost',
           'escrow_tax','purchase_cost_new1','shipping_fee', 'extra',
           'first_carrier_cost', 'seller_discount', 'stock_exchange']
    for i in col:
        order_base[i] = pd.to_numeric(order_base[i], errors='coerce')
        order_base[i] = order_base[i].fillna(0).astype(float)

    # 消费税金处理
    col = ['AU','CA','US','MX','JP','TR','AE','IN','SG']
    order_base['escrow_tax'] = np.where(order_base['ship_country'].isin(col), 0, order_base['escrow_tax'])
    # 渠道名称替换
    df_ship_code = get_yibai_logistics_logistics()
    order_base = pd.merge(order_base, df_ship_code, how='left', on=['ship_code'])
    order_base.drop('ship_code', axis=1, inplace=True)

    order_base['payment_time'] = order_base['payment_time'].dt.strftime('%Y-%m-%d')

    # 重复订单号的销售额、利润置为0
    order_base['sales_status'] = order_base['sales_status'].fillna('')
    column_order = ['清仓','负利润加快动销', '正利润加快动销', '正常','','nan','-']
    # 如果有值未在column_order中，抛出报错
    unique_values = order_base['sales_status'].unique()
    print(order_base['sales_status'].unique())
    missing_values = set(unique_values) - set(column_order)
    if missing_values:
        error_message = f"以下值未在 column_order 中定义: {missing_values}"
        raise ValueError(error_message)  # 抛出错误

    order_base['sales_status'] = pd.Categorical(order_base['sales_status'], categories=column_order, ordered=True)
    order_base = order_base.sort_values(by=['order_id', 'sales_status'])

    order_base['rank'] = order_base.groupby(['order_id', 'sales_status']).cumcount() + 1
    for c in ['total_price','true_profit_new1','real_profit']:
        order_base[c] = np.where(order_base['rank'] != 1, 0, order_base[c])
    order_base.drop('rank', axis=1, inplace=True)
    order_base['sales_status'] = order_base['sales_status'].astype(str)

    order_base.drop(['currency','new_price'], axis=1, inplace=True)
    # order_base.to_excel('F://Desktop//tt_order_base.xlsx', index=0)

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

## 监控报表
def tt_get_oversea_order():
    """ TT海外仓订单监控报表 """

    # 订单表 --> tt_dashbord_new_data1
    tt_dwm_order()

    # # dashbord --> ads_tt_oversea_order
    # date_today = datetime.date.today()
    # date_start = date_today - datetime.timedelta(days=20)
    # # date_start = '2025-04-01'
    # # date_end = '2025-05-01'
    # # 仅更新近1个月订单
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = f"delete from over_sea.ads_tt_oversea_order where payment_time >= '{date_start}' "
    # conn.execute(sql)
    # conn.close()
    #
    # sql = f"""
    #     SELECT
    #         *
    #     FROM yibai_oversea.tt_dashbord_new_data1
    #     WHERE
    #         payment_time >= '{date_start}'
    #         -- and payment_time < 'date_end'
    #         -- and `total_price` > 0
    #         -- and `sales_status` not in ('','nan','总计')
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_order_info = conn_ck.ck_select_to_df(sql)
    # df_order_info.drop(['id', 'platform_order_id', 'update_time'], axis=1, inplace=True)
    # df_order_info = df_order_info.drop_duplicates(subset=['order_id', 'sku', 'account_id'])
    #
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_order_info, 'ads_tt_oversea_order', if_exists='append')



if __name__ == '__main__':
    # df = tt_get_stock()

    # dwm_sku_price()
    # tt_get_ebay_adjust()

    # ali_temp()
    # tt_get_aliexpress_listing()
    # tt_get_ali_adjust()

    tt_get_amazon_adjust()
    # tt_get_oversea_order()
    # tt_get_shopee_adjust_listing()
    # tt_get_lazada_adjust_listing()
    # tt_get_allegro_adjust_listing()

    # tt_update_platform_fee()
    # get_amazon_commission()
    # limit_price_info()
    # tt_get_amazon_filter()
    tt_dwm_order()
    # tt_get_warehouse()