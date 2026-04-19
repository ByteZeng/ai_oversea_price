"""
tt  TEMU平台相关需求

"""
##
import warnings
import datetime,time
import pandas as pd
import numpy as np
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from utils.utils import  save_df, make_path
from utils import utils
warnings.filterwarnings('ignore')

##
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
                ,ps.available_stock available_stock ,if(empty(yps.new_price),yps1.new_price,yps.new_price) as new_price
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
                where warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
                and warehouse_other_type = 2 and warehouse_name like '%TT%'
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
        and ya.order_warehouse_code like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    '''
    df_stock_age = ck_client.ck_select_to_df(sql)
    # 存在库龄表和库存表数据对不上的情况：以库存表为准。暂将库存表无库存、库龄表有库存的数据置为0
    sql = """
    SELECT sku, warehouse_id
    FROM yb_datacenter.v_oversea_stock
    WHERE available_stock > 0 and warehouse_other_type = 2
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

def tt_dwm_oversea_sku():
    """
    DWM：数据中间层，部分聚合
    按SKU + warehouse 维度，汇总相关信息
    利润率涨降幅度设置
    """
    print('获取库存信息...')
    df_stock = tt_get_stock()
    print('获取库龄信息...')
    df_stock_age_id, df_stock_age_warehouse = tt_get_stock_age()
    # print('获取日销信息...')
    # sku_sales = tt_get_sku_sales()

    # dwm_sku = pd.merge(df_stock, sku_sales, how='left', on=['sku', 'warehouse'])
    # # dwm_sku.info()
    # dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].fillna(0)
    # dwm_sku.iloc[:, 17:] = dwm_sku.iloc[:, 17:].astype(float)
    # 匹配库龄、仓租数据
    # 匹配不到的数据：库存为0、墨西哥仓、库龄缺失
    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])
    print(dwm_sku.info())
    # dwm_sku.iloc[:, 23:32] = dwm_sku.iloc[:, 23:32].fillna(0)
    # dwm_sku.info()
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])

    return dwm_sku
##
dwm_sku = tt_dwm_oversea_sku()
# ##
# dwm_sku.to_excel('dwm_sku_tt.xlsx', index=0)
##
sku_sales = dwm_sku[['sku','warehouse']]
sku_sales['sale_status'] = '正常'
sku_sales['start_time'] = '2024-10-25'
sku_sales['end_time'] = np.nan
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

conn.to_sql(sku_sales, 'tt_oversea_sale_status', if_exists='replace')
##
# tt_dwm_sku = tt_dwm_oversea_sku()
# ##
# sql = """
#     SELECT *
#     FROM yb_datacenter.v_oversea_stock
#     WHERE warehouse_id not in (958, 902)  -- 墨西哥仓不考虑子仓YB跨境墨西哥仓库存（易佰销售无法使用）
#     and warehouse_other_type = 2 and warehouse_name like '%TT%'
# """
# conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
# df_stock = conn_ck.ck_select_to_df(sql)
# ##
# df_stock.to_excel('df_stock.xlsx', index=0)
##
def get_freight_subsidy(listing_t):
    # 运费补贴计算
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    df_rate0 = df_rate0[(~df_rate0['country'].isna()) & (df_rate0['country'] != '')]
    df_rate0 = df_rate0.rename(columns={'country': 'column_to_split'})
    listing_t['us_rate'] = df_rate.iloc[0, 2]
    listing_t = pd.merge(listing_t, df_rate0[['column_to_split','rate']], how='left', on=['column_to_split'])
    print(f"链接表国家范围有：{listing_t['column_to_split'].unique()}")
    listing_t = listing_t.rename(columns={'column_to_split':'country'})
    listing_t['limit_price'] = listing_t['country'].replace({'US':26, 'UK':28, 'DE':24,'FR':24,'IT':24,'ES':24,'AU':34,
                                                             'CA':31,'CZ':592,'PL':50,'MX':475,'':0,'HU':6750,'PT':24})
    listing_t['freight_subsidy'] = listing_t['country'].replace({'US':2.99, 'UK':2, 'DE':2.99,'FR':2.99,'IT':2.99,
                                                                 'ES':2.99,'CZ':75,'PL':12.99, 'AU':2.99, 'CA':3.99,
                                                                 'MX':80,'':0,'HU':1390,'PT':2.99})
    listing_t = listing_t.rename(columns={'country': 'column_to_split'})
    col = ['limit_price','申报价格','freight_subsidy']
    # listing_t = listing_t[~listing_t['column_to_split'].isin(['JP',''])]
    for c in col:
        listing_t[c] = pd.to_numeric(listing_t[c], errors='coerce')
    # listing_t[col] = listing_t[col].fillna(0).astype(float)
    listing_t['freight_subsidy'] = np.where((listing_t['申报价格']*df_rate.iloc[0,2]/listing_t['rate']) < listing_t['limit_price'],
                                     listing_t['freight_subsidy'] * listing_t['rate'], 0)

    return listing_t
##
def tt_get_temu_listing():
    print("===temu刊登链接数据===")
    sql = """
    with d as
    (select product_spu_id,product_sku_id,max(id) as id from tt_sale_center_listing_sync.tt_temu_listing_crawling_log
    group by product_spu_id,product_sku_id),
    c as (select * from tt_sale_center_listing_sync.tt_temu_listing_crawling_log where id in (select id from d))

    select 
        e.account_id,e.short_name,a.site_code,a.item_id,a.product_sku_id,a.stock_number,c.online_status,a.sku,b.lazada_account_operation_mode,
        c.added_to_site_time,c.supplier_price,date(a.create_time) as `刊登时间` 
    from tt_sale_center_listing_sync.tt_temu_listing a
    left join tt_sale_center_common_sync.tt_common_account_config b on a.account_id=b.account_id
    left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    left join tt_sale_center_system.tt_system_account as e on a.account_id=e.id
    where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    """
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False).drop_duplicates(subset=['account_id','product_sku_id','sku'])
    listing_t['运营模式'] = listing_t['lazada_account_operation_mode'].map({1: '全托管', 2: '半托管'})
    del listing_t['lazada_account_operation_mode']
    #
    # 获取捆绑链接表的sku信息
    sql = """
     select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku 
     from tt_sale_center_listing_sync.tt_temu_bind_sku
     order by update_time desc
     """
    yibai_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    yibai_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)
    listing_t[['product_sku_id', 'sku', 'item_id']] = listing_t[['product_sku_id', 'sku', 'item_id']].astype('str')
    listing_t = listing_t.merge(yibai_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
                                suffixes=['', '1'])
    listing_t.loc[listing_t['sku'] == '', 'sku'] = np.nan
    listing_t['sku'].fillna(listing_t['sku1'], inplace=True)
    listing_t.drop('sku1', axis=1, inplace=True)

    listing_t.rename(columns={'added_to_site_time': '加入站点时间'}, inplace=True)
    listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    listing_t['是否核价通过链接'] = np.where(
        listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止']), 1, 0)
    country = {'AU': '澳大利亚', 'AU,NZ': '澳大利亚', 'DE': '欧洲', 'DE,ES,SP': '欧洲', 'DE,FR,IT,ES,SP': '欧洲',
               'DE,IT': '欧洲', 'ES,SP': '欧洲', 'FR': '欧洲', 'FR,ES,SP': '欧洲', 'FR,IT': '欧洲',
               'FR,IT,ES,SP': '欧洲','CZ':'欧洲','PL':'欧洲','DE,FR,IT,ES,SP,PL,CZ':'欧洲', 'PL,CZ':'欧洲',
               'IT': '欧洲', 'IT,ES,SP': '欧洲', 'DE,FR,IT': '欧洲', 'UK,GB': '英国', 'US': '美国', 'NZ': '新西兰',
               'CA': '加拿大','MX':'墨西哥','DE,PL,CZ':'欧洲','FR,IT,ES,SP,PL,CZ':'欧洲','ES,SP,PL,CZ':'欧洲',
               'JP':'日本','DE,CZ':'欧洲','FR,IT,PL,CZ':'欧洲','FR,PL,CZ':'欧洲','FR,IT,ES,SP,CZ':'欧洲','DE,FR':'欧洲',
               'DE,IT,ES,SP':'欧洲','DE,FR,ES,SP':'欧洲'}
    listing_t['站点'] = listing_t['site_code'].map(country)
    #
    listing_t['warehouse'] = listing_t['站点'].replace({'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓',
                                                        '英国': '英国仓', '新西兰': '澳洲仓', '加拿大': '加拿大仓',
                                                        '墨西哥':'墨西哥仓'})
    listing_t = listing_t.drop_duplicates()
    # # temu账号获取

    # 1、有效链接
    df_temu_listing = listing_t[
        (listing_t['online_status'] == '已发布到站点') & (~listing_t['sku'].isna()) & (listing_t['sku'] != '')]

    # 2、捆绑链接覆盖率
    df_bind_listing = listing_t[(~listing_t['sku'].isna()) & (listing_t['sku'] != '')]


    return df_bind_listing

def temu_listing():
    """
    tt
    temu链接预处理
    """
    df_bind_listing = tt_get_temu_listing()
    df_bind_listing = df_bind_listing.reset_index(drop=True).reset_index()
    df_bind_listing = df_bind_listing.rename(columns={'index':'唯一标识'})
    print(df_bind_listing.info())
    return df_bind_listing

def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = conn_ck.ck_select_to_df(sql)

    return df_rate

def tt_check_temu_online_price():
    """
    TEMU在线链接的毛利润率测算。
    因不确定链接模式，运费数据获取顺序：
        海外仓调价数据的运费 >> 海外仓接口运费 >> 剔除代销仓 >> 虚拟仓运费
    """
    # df_rate0 = get_rate()
    # df_rate = df_rate0[df_rate0['country'] == 'US']
    # 测算temu申报价格的订单利润率
    print('获取TEMU链接信息...')
    # df_temu = pd.read_excel('F:\Desktop\Temu数据统计20240821.xlsx')
    df_temu_base = temu_listing()
    # 数据筛选
    df_temu_base = df_temu_base.rename(columns={'added_to_site_time':'加入站点时间','supplier_price':'申报价格'})
    # df_temu_base = df_temu_base[df_temu_base['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止'])]
    df_temu_base = df_temu_base[~df_temu_base['sku'].isna()]
    df_temu_base['sku'] = df_temu_base['sku'].astype(str)
    print(f'有效链接共{len(df_temu_base)}条.')
    # df_temu_base = df_temu_base.sample(10000)
    # 拆分站点
    df_temu_base = df_temu_base.assign(column_to_split=df_temu_base['site_code'].str.split(',')).explode('column_to_split')
    df_temu_base['column_to_split'] = df_temu_base['column_to_split'].replace({'SP':'ES','GB':'UK','NZ':'AU'})
    # 运费补贴
    df_temu_base = get_freight_subsidy(df_temu_base)
    df_temu_base.drop(['limit_price'], axis=1, inplace=True)
    print(df_temu_base.info())
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = df_temu_base[['唯一标识', 'site_code', 'online_status', 'sku', '加入站点时间', '站点','column_to_split']]
    df_sku['数量'] = 1
    df_sku = chicun_zhongliang(df_sku, 1, conn_ck)
    df_sku.drop(['数量', '重量', '重量来源', '长', '宽', '高'], axis=1, inplace=True)

    # 获取运费数据
    print('获取海外仓运费...')
    df_fee = tt_get_useful_fee(df_sku)
    df_fee = df_fee.sort_values(by='total_cost', ascending=False).drop_duplicates(subset=['唯一标识'])
    df_fee['运费类型'] = np.where(df_fee['total_cost'].isna(), 0, '海外仓')
    col = ['唯一标识','成本','total_cost','best_warehouse_name','sale_status','运费类型']
    df_result = pd.merge(df_temu_base, df_fee[col], how='left', on='唯一标识')

    # 接口运费
    print('获取接口运费...')
    df_bundle_fee = get_interf_fee(df_fee)
    df_bundle_fee = df_bundle_fee.rename(columns={'shipCountry':'column_to_split'})

    df_result = pd.merge(df_result, df_bundle_fee, how='left', on=['sku','column_to_split'])

    df_result['运费类型'] = np.where(~df_result['totalCost'].isna(), '接口', df_result['运费类型'])
    df_result['total_cost'] = np.where(~df_result['totalCost'].isna(), df_result['totalCost'], df_result['total_cost'])
    df_result['best_warehouse_name'] = np.where(~df_result['totalCost'].isna(), df_result['warehouseName'], df_result['best_warehouse_name'])

    df_result = df_result.sort_values(by='total_cost', ascending=False).drop_duplicates(subset=['唯一标识'])

    df_result.drop(['warehouseName', 'shipName', 'totalCost'], axis=1, inplace=True)
    # # 汇率
    # df_rate0 = get_rate()
    # df_rate = df_rate0[df_rate0['country'] == 'US']
    # df_rate0 = df_rate0[(~df_rate0['country'].isna()) & (df_rate0['country'] != '')]
    # df_rate0 = df_rate0.rename(columns={'country':'column_to_split'})
    # df_result['us_rate'] = df_rate.iloc[0, 2]
    # df_result = pd.merge(df_result, df_rate0[['column_to_split','rate']], how='left', on='column_to_split')

    return df_result

def tt_get_useful_fee(df):
    """
    获取tt海外仓运费数据
    """
    # temu取分摊头程对应的总运费
    sql = f"""
        SELECT sku, totalCost_origin as total_cost, warehouseName as best_warehouse_name, country, warehouse
        FROM over_sea.oversea_transport_fee_useful
        WHERE 
            platform = 'AMAZON' 
            and warehouse in ('美国仓','澳洲仓', '德国仓', '英国仓','加拿大仓','墨西哥仓')
            and !(warehouse = '美国仓' and country in ('CA','MX'))
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    c1 = (df_fee['warehouse'].isin(['英国仓'])) & (df_fee['country'] != 'UK')
    c2 = (~df_fee['warehouse'].isin(['英国仓'])) & (df_fee['country'] == 'UK')
    df_fee = df_fee[~(c1 | c2)]
    # 匹配
    df_fee = df_fee.rename(columns={'country': 'column_to_split'})
    df = pd.merge(df, df_fee, how='left', on=['sku', 'column_to_split'])

    # 销售状态
    sql = f"""
        SELECT 
            sku, warehouse, sale_status
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)
    df = pd.merge(df, df_status, how='left', on=['sku', 'warehouse'])

    return df

##
def tt_get_line(df):
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
    # df_line['三级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[2]
    # df_line['四级产品线'] = df_line['产品线路线'].str.split('->', expand=True)[3]
    df_line = df_line.drop_duplicates(subset='sku')
    df = pd.merge(df, df_line[['sku','一级产品线','二级产品线']], how='left', on=['sku'])

    return df
##
def get_bundle_fee(df):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['column_to_split', 'warehouse_id']):
        print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('TEMU', key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30')
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

def get_interf_fee(df):
    df_bundle = df[df['total_cost'].isna()]
    df_bundle = df_bundle[['sku', 'column_to_split']].drop_duplicates()
    print(df_bundle.info())
    #
    df_bundle['数量'] = 1
    df_bundle = df_bundle.reset_index(drop=True).reset_index()
    # dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
    #        'AU': '353,769','CZ': '325','PL': '325'}
    dic = {'US': '1465,1473,1477,1521', 'UK': '1463,1472', 'AU': '1469', 'DE': '1462,1475,1520', 'FR': '1462,1475,1520',
           'ES': '1462,1475,1520', 'IT': '1462,1475,1520', 'CA': '1474',
           'CZ': '1462,1475,1520', 'PL': '1462,1475,1520'}
    df_bundle['warehouse_id'] = df_bundle['column_to_split'].replace(dic)
    df_bundle['sku'] = df_bundle['sku'].replace(
        {'1*JY03556': 'JY03556', '1*JYA02556-01': 'JYA02556-01', '1*DS00500': 'DS00500', '1*DS01567': 'DS01567',
         '5591*6-':'5591*6','5591*5-':'5591*5'})
    #
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df_bundle)
    df_bundle_fee = get_bundle_fee(df_bundle)
    df_bundle_fee = df_bundle_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost']]

    return df_bundle_fee
##
# tt_temu_listing = tt_check_temu_online_price()
## 责任类目
def tt_get_resp(tt_temu_listing):
    """
    tt链接当前主体账号、责任账号匹配
    """
    tt_main_account = pd.read_excel('F://Desktop//TEMU账号负责人_TT.xlsx')
    tt_main_account = tt_main_account.drop_duplicates(subset=['account_id'])
    temu_listing = pd.merge(tt_temu_listing, tt_main_account[['account_id','main_name']], how='left', on=['account_id'])
    temu_listing = temu_listing.rename(columns={'main_name':'主体账号'})
    #
    temu_listing = tt_get_line(temu_listing)
    #
    df_resp_line = pd.read_excel('F://Desktop//TEMU责任人明细_类目TT.xlsx')
    df_resp_line = df_resp_line.rename(
        columns={'一级类目': '一级产品线', '二级类目': '二级产品线', '责任人': '责任账号'})
    line1 = df_resp_line[df_resp_line['二级产品线'] == '全']
    line2 = df_resp_line[df_resp_line['二级产品线'] != '全']
    temu_listing['第九版责任账号'] = np.nan
    temu_listing_0 = pd.merge(temu_listing, line1[['一级产品线', '责任账号']], how='left', on=['一级产品线'])
    temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'],
                                                temu_listing_0['第九版责任账号'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)

    temu_listing_0 = pd.merge(temu_listing_0, line2[['一级产品线', '二级产品线', '责任账号']], how='left',
                              on=['一级产品线', '二级产品线'])
    temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['责任账号'],
                                                temu_listing_0['第九版责任账号'])
    temu_listing_0.drop(['责任账号'], axis=1, inplace=True)
    #
    # 剩下未归属成功的，按链接初始的主体账号
    temu_listing_0['第九版责任账号'] = np.where(temu_listing_0['第九版责任账号'].isna(), temu_listing_0['主体账号'],
                                                temu_listing_0['第九版责任账号'])
    temu_listing_0['is_same'] = np.where(temu_listing_0['主体账号'] == temu_listing_0['第九版责任账号'], 1, 0)

    return temu_listing_0

##
def write_to_sql(df, table_name):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = datetime.date.today()
    sql = f"""
    delete from over_sea.{table_name} where date_id >='{date_today}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

## temu订单数据
def tt_get_listing_order():
    """
    获取temu订单数据
    """
    sql = """
    SELECT 
        id warehouse_id,warehouse_name,
        case when warehouse_type in (5,6) then '平台仓' WHEN warehouse_type=1 THEN '国内仓' when warehouse_type=8 then '海外仓'
        WHEN warehouse_type=7 THEN '分销' WHEN warehouse_type=9 THEN '进口仓' ELSE '' END AS `仓库类型`, country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country='UK' THEN '英国仓'
            WHEN country='GB' THEN '英国仓'
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
            ELSE NULL 
        END AS warehouse
    FROM tt_logistics_tms_sync.tt_warehouse 
    """
    conn = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据t')
    warehouse = conn.ck_select_to_df(sql)
    warehouse['warehouse_id'] = warehouse['warehouse_id'].apply(lambda x:int('10000'+str(x)))

    sql = """
    SELECT 
        id warehouse_id,name warehouse_name,
        case when type in ('fba','platform') then '平台仓' WHEN type = 'inland' THEN '国内仓' when type in ('third','overseas') then '海外仓'
        WHEN type in ('consignment','transit') THEN '分销'  ELSE '' END AS `仓库类型`,country,
        CASE 
            WHEN country='US' THEN '美国仓'
            WHEN country='UK' THEN '英国仓'
            WHEN country='GB' THEN '英国仓'
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
            ELSE NULL 
        END AS warehouse
    FROM yb_datacenter.yb_warehouse
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    yb_warehouse = conn_mx.ck_select_to_df(sql)
    warehouse = warehouse.append(yb_warehouse,ignore_index=True)
    #
    date_today = time.strftime('%Y-%m-%d')
    date_start = (datetime.date.today() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
    date_30 = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    sql = f"""
    with order_table as (
        SELECT distinct order_id from tt_oms_sync.tt_oms_order 
        where 
        platform_status not in ('Canceled', 'Pending') 
        -- AND refund_status in (0, 1) 
        and order_status<>80
        AND order_id not like '%%-RE' 
        AND payment_time>='{date_start}'
        and platform_code!='SPH' AND total_price!=0
        and platform_code = 'TEMU'
    )
        SELECT 
        a.platform_code platform_code,
        a.ship_country ship_country,
        a.account_id as account_id,
        b.item_id as item_id,
        b.seller_sku seller_sku,
        b.asinval as asin,
        d.sku as sku,
        toInt64(d.quantity) as quantity,
        a.order_id as order_id,
        a.warehouse_id warehouse_id,
        a.platform_order_id as platform_order_id,
        a.payment_time as created_time,
        a.currency as currency,
        toFloat64(c.currency_rate) as currency_rate,
        toFloat64(c.total_price) as total_price,
        case when toFloat64(c.true_profit) = 0 then toFloat64(c.profit) else toFloat64(c.true_profit) end as true_profit,
        toFloat64(c.refund_amount) as refund_amount,
        toFloat64(c.shipping_price) as shipping_price,
        toFloat64(c.shipping_cost) as `预估运费`,
        toFloat64(c.true_shipping_fee) as `实际运费`
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
    # order_base['total_price'] = order_base['total_price'] * order_base['currency_rate']
    # order_base['true_profit'] = order_base['true_profit'] * order_base['currency_rate']
    # order_base = order_base.sample(10000)
    #
    order_base = order_base.merge(warehouse,on='warehouse_id',how='left')
    #
    order_60 = order_base.groupby(['account_id', 'item_id']).agg(
        {'total_price': 'sum', 'quantity': 'sum', 'true_profit': 'sum'}).reset_index().rename(
        columns={'total_price': '60_sales', 'quantity': '60_num', 'true_profit': '60_profit'})
    order_60['60_sales_avg'] = order_60['60_sales'] / 60

    order_30_base = order_base[order_base['created_time'] >= date_30]
    order_30 = order_30_base.groupby(['account_id', 'item_id']).agg(
        {'total_price': 'sum', 'quantity': 'sum', 'true_profit': 'sum'}).reset_index().rename(
        columns={'total_price': '30_sales', 'quantity': '30_num', 'true_profit': '30_profit'})
    order_30['30_sales_avg'] = order_30['30_sales'] / 30

    mode_unit_price = order_30_base.groupby(
        ['account_id', 'item_id'])['total_price'].idxmax().map(order_30_base['total_price']).reset_index()
    order_30 = pd.merge(order_30, mode_unit_price, how='left', on=['account_id', 'item_id'])
    order_30 = order_30.rename(columns={'total_price': '链接近30天出单最多售价'})
    order_30['链接近30天出单利润率'] = order_30['30_profit'] / order_30['30_sales']
    order_info = pd.merge(order_60, order_30, how='left', on=['account_id', 'item_id'])
    order_info = order_info.fillna(0)

    order_info = order_info.rename(columns={'item_id': 'product_sku_id'})
    order_info['account_id'] = order_info['account_id'].astype(int)
    order_info['product_sku_id'] = order_info['product_sku_id'].astype(str)

    return order_info
# order_info = tt_get_listing_order()
##
# order.to_excel('order.xlsx', index=0)
def main():
    """
    tt链接表信息汇总，主程序
    """
    date_today = time.strftime('%Y-%m-%d')
    df = tt_check_temu_online_price()
    print('获取责任类目...')
    df = tt_get_resp(df)
    print('获取链接订单数据...')
    order_info = tt_get_listing_order()
    df = pd.merge(df, order_info, how='left', on=['account_id','product_sku_id'])

    df['date_id'] = date_today
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    table_name = 'tt_temu_listing_check'
    conn.to_sql(df, table_name, if_exists='replace')

##
def local_save():
    """
    本地运行
    """
    utils.program_name = 'TT-TEMU链接订单利润率核算'
    make_path()

    df = tt_check_temu_online_price()
    print('获取责任类目...')
    df = tt_get_resp(df)
    print('获取链接订单数据...')
    order_info = tt_get_listing_order()
    df = pd.merge(df, order_info, how='left', on=['account_id','product_sku_id'])
    save_df(df, 'TEMU链接订单利润率核算tt', file_type='xlsx')

##
if __name__ == '__main__':
    # main()
    local_save()
