
"""
【统计海外仓链接数据条数】
"""
##
import numpy as np
import pandas as pd
import time, datetime
import tqdm
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from warnings import filterwarnings
from utils import utils
from utils.utils import  save_df, make_path, get_path
from all_auto_task.oversea_temu_price import get_main_resp, get_temu_account
filterwarnings('ignore')

##
def get_sku_info():
    # 库存表获取
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = time.strftime('%Y%m%d')
    # date_today = '20241106'
    sql = f"""
        SELECT
            sku, warehouse, Max(date_id) date_id,
            argMax(warehouse_id, available_stock) AS max_stock_warehouse_id,
            argMax(warehouse_name, available_stock) AS max_stock_warehouse_name,
            sum(available_stock) AS total_available_stock,
            arrayStringConcat(groupArray(warehouse_stock), ', ') warehouse_stock_info       
        FROM (
            SELECT
                ps.sku sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
                available_stock, warehouse_stock
            FROM (
                SELECT 
                    trim(sku) sku, warehouse_id, date_id, cargo_owner_id, available_stock,
                    concat(toString(warehouse_id),':',toString(available_stock)) as warehouse_stock
                FROM yb_datacenter.yb_stock
                WHERE 
                    date_id >= 20220901
                    and date_id <= '{date_today}' -- 根据需要取时间
                    and available_stock > 0 
                    and cargo_owner_id = 8  -- 筛选货主ID为8的
                    and warehouse_id in (
                        SELECT id FROM yb_datacenter.yb_warehouse WHERE type IN ('third', 'overseas'))
                ORDER BY date_id DESC
                LIMIT 1 BY sku, warehouse_id
            )AS ps
            INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE         
                yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
                and (yw.warehouse_other_type = 2 or warehouse_id in (958))   -- 筛选公共仓（非子仓\独享仓）
                and yw.warehouse_name not like '%独享%'
            ORDER BY date_id DESC
        ) a
        GROUP BY sku, warehouse
    """
    df_stock = conn_ck.ck_select_to_df(sql)
    # 当前库存数据修改
    date = time.strftime('%Y-%m-%d')
    df_stock['total_available_stock'] = np.where(df_stock['date_id'] < date, 0, df_stock['total_available_stock'])
    df_stock['warehouse_stock_info'] = np.where(df_stock['date_id'] < date, '',
                                                 df_stock['warehouse_stock_info'])
    print(df_stock.info())
    print(df_stock['total_available_stock'].describe())
    df_stock = get_line(df_stock)

    # 销售状态和库龄表获取
    sql = """
        SELECT
            A.sku sku, A.warehouse warehouse, overage_level, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        FROM dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE 
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
    """
    df_status = conn.read_sql(sql)
    #
    df_oversea_sku_final = pd.merge(df_stock, df_status, how='left', on=['sku','warehouse'])
    df_oversea_sku_final = df_oversea_sku_final.drop_duplicates(subset=['sku','warehouse'])

    # df_oversea_sku = pd.merge(df_stock, df_stock_age[['sku', 'warehouse_id', 'inventory_age']], how='left',
    #                           on=['sku', 'warehouse_id'])
    # 筛选常见仓库
    df_sku_listing_new = df_oversea_sku_final[~df_oversea_sku_final['warehouse'].isna()]
    df_sku_listing_new['area'] = np.where(df_sku_listing_new['warehouse'].str.contains('德国|法国|西班牙|意大利'),
                                            '欧洲', df_sku_listing_new['warehouse'].str[:-1])
    df_sku_listing_new['area'] = np.where(df_sku_listing_new['warehouse']=='乌拉圭仓',
                                            '巴西', df_sku_listing_new['area'])
    df_sku_listing_new = df_sku_listing_new[~df_sku_listing_new['area'].isin(['日本'])]
    df_sku_listing_new['site'] = df_sku_listing_new['warehouse'].replace(
        {'德国仓': 'de', '美国仓': 'us', '澳洲仓': 'au', '英国仓': 'uk', '墨西哥仓':'mx',  '加拿大仓': 'ca', '法国仓': 'fr',
         '乌拉圭仓':'br', '西班牙仓':'es', '意大利仓':'it','马来西亚仓':'my', '泰国仓':'th', '菲律宾仓':'ph',
         '印度尼西亚仓':'id', '越南仓':'vn'})

    # 处理仓标数据
    df_sku_listing_new['sku'] = df_sku_listing_new['sku'].str.strip()
    df_sku_listing_new = df_sku_listing_new.sort_values(by='date_id', ascending=False).drop_duplicates(subset=['sku','warehouse'])

    df_sku_listing_new['new_sku'] = np.where(
        df_sku_listing_new['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_sku_listing_new['sku'].str[3:], df_sku_listing_new['sku'])
    df_sku_listing_new['new_sku'] = np.where(df_sku_listing_new['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                             df_sku_listing_new['sku'].str[:-2], df_sku_listing_new['new_sku'])
    df_sku_listing_new['overage_level'] = df_sku_listing_new['overage_level'].fillna(0).astype(int)
    df_sku_listing_new['销售状态'] = df_sku_listing_new['销售状态'].fillna('正常')
    # 责任人
    df_sku_listing_new['主体账号'] = ' '
    df_sku_listing_new['站点'] = df_sku_listing_new['area'].replace('澳洲','澳大利亚')
    df_sku_listing_new = get_main_resp(df_sku_listing_new)
    df_sku_listing_new.drop(['一级产品线','二级产品线','主体账号','is_same','站点'], axis=1, inplace=True)
    print(df_sku_listing_new.info())

    return df_sku_listing_new

def get_old_record():
    """
    读取月初的sku+大仓信息。匹配最新的链接数量
    """
    df_old = pd.read_excel('F://Desktop//日常任务//海外仓调价覆盖率相关//海外仓SKU各平台刊登情况1106.xlsx')
    col = ['sku','warehouse','最大库存子仓ID','最大库存子仓','最近有库存日期','当前库存','子仓库存明细','是否通拓sku',
           '超库龄等级','销售状态','仓库区域','仓库所在国家','new_sku（主SKU）','第十版责任账号']
    df_old = df_old[col]

    df_old.columns= ['sku', 'warehouse','max_stock_warehouse_id','max_stock_warehouse_name','date_id','total_available_stock',
                     'warehouse_stock_info','is_tt_sku','overage_level','销售状态','area','site','new_sku','责任人']

    return df_old

def get_sku(df):
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
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    df_sku = df_sku.drop_duplicates(subset='sku')
    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    c1 = df_sku['sku'].isin(earphones_list)
    c2 = df_sku['develop_source_name'].str.contains('转泛品|VC|易通兔')
    df_sku = df_sku[c1 | c2]
    print(df_sku.info())

    df = pd.merge(df, df_sku[['sku', 'develop_source_name']], how='inner', on='sku')

    return df

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

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, develop_source, b.category_path as `产品线路线`, spu,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
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

    # 取开发来源
    sql = """
        SELECT distinct id as develop_source, develop_source_name
        FROM yibai_prod_base_sync.yibai_prod_develop_source
    """
    df_source = conn_ck.ck_select_to_df(sql)
    df_line = pd.merge(df_line, df_source, how='left', on=['develop_source'])
    # c1 = df_line['develop_source'].isin([14,15,22])
    # c2 = df_line['产品名称'].str.contains('通拓')
    # df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)
    df = pd.merge(df, df_line[['sku','spu','develop_source_name','一级产品线','二级产品线','new_price']], how='left', on=['sku'])

    return df
##
# df_stock = get_sku_info()
# ##
# df_stock.to_excel('df_stock.xlsx', index=0)
##
def get_amazon_listing_num(sku_list, df_site):
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===Amazon刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sku_list = sku_list[0:100]
    li_am = []
    print(len(sku_list))
    for n in range(0,len(sku_list), 50):
        sku_list_x = sku_list[n:n+50]
        sql = """
        with listing_table as (
            select distinct account_id, seller_sku
            from yibai_product_kd_sync.yibai_amazon_sku_map
            where sku in {}
            -- and deliver_mode=2
        )
        SELECT sku, platform, site, count(1) as amazon_listing_num
        FROM (
            select b.account_id as account_id, b.account_name as account_name,
                group_name, short_name, 'AMAZON' as platform,
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
                select account_id, sku, seller_sku
                from yibai_product_kd_sync.yibai_amazon_sku_map
                where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                -- where deliver_mode=2
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
                    where main_code='YB_ZT_FP' and account_type = 1 )
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
                where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
            ) f
            on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
            order by a.create_time desc
            limit 1 by sku, site, asin
        ) a
        GROUP BY sku, platform, site
        settings max_memory_usage = 50000000000
        """.format(sku_list_x)
        # data = con.execute(sql)
        # columns1 = ['sku', 'platform', 'site', 'amazon_listing_num']
        # df_amazon_listing_x = pd.DataFrame(data=data, columns=columns1)
        df_amazon_listing_x = conn_ck.ck_select_to_df(sql)
        li_am.append(df_amazon_listing_x)

    df_amazon_listing = pd.concat(li_am)

    df_amazon_listing_temp = pd.merge(df_amazon_listing, df_site[['site', 'area']], on='site', how='left')
    # es 不在site_table中，单独处理
    df_amazon_listing_temp['area'] = np.where(df_amazon_listing_temp['site'] == 'es', '欧洲',
                                              df_amazon_listing_temp['area'])

    # 链接中间表
    # df_amazon_listing_temp.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-3.xlsx")

    # 处理仓标数据
    df_amazon_listing_temp_2 = df_amazon_listing_temp
    df_amazon_listing_temp_2['new_sku'] = np.where(
        df_amazon_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_amazon_listing_temp_2['sku'].str[3:], df_amazon_listing_temp_2['sku'])
    df_amazon_listing_temp_2['new_sku'] = np.where(df_amazon_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                   df_amazon_listing_temp_2['sku'].str[:-2],
                                                   df_amazon_listing_temp_2['new_sku'])
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.drop_duplicates()
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.groupby(['new_sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()


    # df_amazon_listing_temp_2.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-4.xlsx")
    print(df_amazon_listing_temp_2.info())

    return df_amazon_listing_temp_2

def get_amazon_listing_num_new(sku_list, df_site):
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===Amazon刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    sql = """
        SELECT sku, site, count(1) as amazon_listing_num
        FROM (
            SELECT DISTINCT sku, site, account_id, asin
            FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all)
        ) a 
        GROUP BY sku, site
    """
    df_amazon_listing = conn_ck.ck_select_to_df(sql)

    df_amazon_listing_temp = pd.merge(df_amazon_listing, df_site[['site', 'area']], on='site', how='left')
    # es 不在site_table中，单独处理
    df_amazon_listing_temp['area'] = np.where(df_amazon_listing_temp['site'] == 'es', '欧洲',
                                              df_amazon_listing_temp['area'])

    # 链接中间表
    # df_amazon_listing_temp.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-3.xlsx")

    # 处理仓标数据
    df_amazon_listing_temp_2 = df_amazon_listing_temp
    df_amazon_listing_temp_2['new_sku'] = np.where(
        df_amazon_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_amazon_listing_temp_2['sku'].str[3:], df_amazon_listing_temp_2['sku'])
    df_amazon_listing_temp_2['new_sku'] = np.where(df_amazon_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                   df_amazon_listing_temp_2['sku'].str[:-2],
                                                   df_amazon_listing_temp_2['new_sku'])
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.drop_duplicates()
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.groupby(['new_sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()


    # df_amazon_listing_temp_2.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-4.xlsx")
    print(df_amazon_listing_temp_2.info())

    return df_amazon_listing_temp_2

def get_cj_amazon_listing_num(sku_list, df_site):
    """
    20241206 统计清仓sku链接数量时，需要包含国内仓链接
    """
    print("===Amazon刊登链接数据==")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sku_list = sku_list[0:100]
    li_am = []
    for n in range(0,len(sku_list), 5000):
        sku_list_x = sku_list[n:n+5000]
        sql = """
        with listing_table as (
            select distinct account_id, seller_sku
            from cj_product_kd_sync.cj_amazon_sku_map
            where sku in {}
            -- and deliver_mode=2
        )
        SELECT sku, platform, site, count(1) as amazon_listing_num
        FROM (
            select b.account_id as account_id, b.account_name as account_name,
                group_name, short_name, 'AMAZON' as platform,
                if(b.site ='sp', 'es', b.site) as site,
                status, e.sku as sku, a.seller_sku as seller_sku,
                open_date,
                if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
                a.price AS your_price, fulfillment_channel, f.sale_price as sale_price, a.price as online_price
            from (
                select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
                from cj_product_kd_sync.cj_amazon_listings_all_raw2
                where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
            ) a
            inner join (
                select account_id, sku, seller_sku
                from cj_product_kd_sync.cj_amazon_sku_map
                where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
                -- where deliver_mode=2
            ) e
            on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
            left join (
                select account_id, seller_sku, asin1
                from cj_product_kd_sync.cj_amazon_listing_alls
                where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
            ) t
            on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
            inner join (
                select toInt32(b.id) as account_id, account_name, group_id, short_name, site
                from cj_system_kd_sync.cj_amazon_account b
                where (account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163)
                -- and account_id in (
                --     select distinct toInt32(account_id) from cj_account_manage_sync.cj_amazon_account
                --     where main_code='YB_ZT_FP' and account_type = 1 )
            ) b
            on (a.account_id= b.account_id)
            inner join (
                select group_id, group_name
                from cj_system_kd_sync.cj_amazon_group
                where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
                or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
            ) c
            on (b.group_id=c.group_id)
            left join (
                select account_id, seller_sku, ListingPrice as sale_price
                from cj_product_kd_sync.cj_amazon_listing_price
                where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
            ) f
            on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
            order by a.create_time desc
            limit 1 by sku, site, asin
        ) a
        GROUP BY sku, platform, site
        settings max_memory_usage = 500000000000
        """.format(sku_list_x)
        # data = con.execute(sql)
        # columns1 = ['sku', 'platform', 'site', 'amazon_listing_num']
        # df_amazon_listing_x = pd.DataFrame(data=data, columns=columns1)
        df_amazon_listing_x = conn_ck.ck_select_to_df(sql)
        li_am.append(df_amazon_listing_x)

    df_amazon_listing = pd.concat(li_am)

    df_amazon_listing_temp = pd.merge(df_amazon_listing, df_site[['site', 'area']], on='site', how='left')
    # es 不在site_table中，单独处理
    df_amazon_listing_temp['area'] = np.where(df_amazon_listing_temp['site'] == 'es', '欧洲',
                                              df_amazon_listing_temp['area'])

    # 链接中间表
    # df_amazon_listing_temp.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-3.xlsx")

    # 处理仓标数据
    df_amazon_listing_temp_2 = df_amazon_listing_temp
    df_amazon_listing_temp_2['new_sku'] = np.where(
        df_amazon_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_amazon_listing_temp_2['sku'].str[3:], df_amazon_listing_temp_2['sku'])
    df_amazon_listing_temp_2['new_sku'] = np.where(df_amazon_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                   df_amazon_listing_temp_2['sku'].str[:-2],
                                                   df_amazon_listing_temp_2['new_sku'])
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.drop_duplicates()
    df_amazon_listing_temp_2 = df_amazon_listing_temp_2.groupby(['new_sku', 'site', 'area'])['amazon_listing_num'].sum().reset_index()


    # df_amazon_listing_temp_2.to_excel(r"C:\Users\Administrator\Desktop\df_sku_listing_site_change_columns-4.xlsx")
    print(df_amazon_listing_temp_2.info())

    return df_amazon_listing_temp_2

def get_ebay_listing_num_new(sku_list, df_site):
    print( "===Ebay刊登链接数据===")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay_listing_all = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku = sku_list[n:n+5000]
        sql = f"""
            with  account_list as (
                select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
                where platform_code='EB' and status=1 and is_del=0
            )
            SELECT 
                a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
                CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                d.account_id, a.start_price,
                f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
            FROM (
                SELECT item_id,sku,start_price 
                FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
                WHERE sku in ({sku})
            ) a 
            INNER JOIN (
                SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
                where  warehouse_category_id !=1 and listing_status = 1 
                and account_id in (select distinct id from account_list)
            ) b  ON a.item_id=b.item_id
            LEFT JOIN (
                SELECT item_id, shipping_service_cost
                FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
                WHERE shipping_status=1 and shipping_service_priority=1
            ) f  ON a.item_id = f.item_id
            LEFT JOIN (
                SELECT site_id,site,site1 AS `站点`,is_open,site_code 
                FROM domestic_warehouse_clear.yibai_site_table_ebay 
                where is_open='是'
            ) c  ON b.siteid = c.site_id
            LEFT JOIN account_list d on b.account_id = d.id
            INNER JOIN (
                SELECT id, warehouse
                FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
                WHERE warehouse_type_id in (2, 3)
            ) e ON b.warehouse_category_id = e.id

        """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        df_ebay_listing = conn_ck.ck_select_to_df(sql)
        df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns]
        df_ebay_listing['DATE'] = time.strftime('%Y-%m-%d')
        df_ebay_listing_all = pd.concat([df_ebay_listing, df_ebay_listing_all])

    df_ebay_listing_all['item_id'] = df_ebay_listing_all['item_id'].astype(str)
    print(df_ebay_listing_all.info())
    # print(df_ebay_listing_all[df_ebay_listing_all['sku'] == '2611220060911'].head(10))
    # 按 SKU + country 聚合计算 item的数量
    df_ebay_listing_all = df_ebay_listing_all.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
    df_ebay_listing_temp = df_ebay_listing_all.groupby(['sku', 'country'])['item_id'].count().reset_index()
    df_ebay_listing_temp.rename(columns={'item_id': 'ebay_listing_num', 'country': 'site'}, inplace=True)
    df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].str.lower()
    df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].replace('motor','us')
    df_ebay_listing_temp = pd.merge(df_ebay_listing_temp, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df_ebay_listing_temp_2 = df_ebay_listing_temp
    df_ebay_listing_temp_2['new_sku'] = np.where(
        df_ebay_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_ebay_listing_temp_2['sku'].str[3:], df_ebay_listing_temp_2['sku'])
    df_ebay_listing_temp_2['new_sku'] = np.where(df_ebay_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                 df_ebay_listing_temp_2['sku'].str[:-2], df_ebay_listing_temp_2['new_sku'])

    df_ebay_listing_temp_2 = df_ebay_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
        'ebay_listing_num'].sum().reset_index()

    return df_ebay_listing_temp_2

def get_ebay_listing_num_new_temp(sku_list, df_site):
    print( "===Ebay刊登链接数据===")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay_listing_all = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku = sku_list[n:n+5000]
        sql = f"""
            with  account_list as (
                select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
                where platform_code='EB' and status=1 and is_del=0
            )
            SELECT 
                a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
                CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                d.account_id, a.start_price,
                f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
            FROM (
                SELECT item_id,sku,start_price 
                FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
            ) a 
            INNER JOIN (
                SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
                where  warehouse_category_id !=1 and listing_status = 1 
                and account_id in (select distinct id from account_list) and sku in ({sku})
            ) b  ON a.item_id=b.item_id
            LEFT JOIN (
                SELECT item_id, shipping_service_cost
                FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
                WHERE shipping_status=1 and shipping_service_priority=1
            ) f  ON a.item_id = f.item_id
            LEFT JOIN (
                SELECT site_id,site,site1 AS `站点`,is_open,site_code 
                FROM domestic_warehouse_clear.yibai_site_table_ebay 
                where is_open='是'
            ) c  ON b.siteid = c.site_id
            LEFT JOIN account_list d on b.account_id = d.id
            INNER JOIN (
                SELECT id, warehouse
                FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
                WHERE warehouse_type_id in (2, 3)
            ) e ON b.warehouse_category_id = e.id

        """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        df_ebay_listing = conn_ck.ck_select_to_df(sql)
        df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns]
        df_ebay_listing['DATE'] = time.strftime('%Y-%m-%d')
        df_ebay_listing_all = pd.concat([df_ebay_listing, df_ebay_listing_all])

    df_ebay_listing_all['item_id'] = df_ebay_listing_all['item_id'].astype(str)
    print(df_ebay_listing_all.info())
    print(df_ebay_listing_all[df_ebay_listing_all['sku'] == '2611220060911'].head(10))
    # 按 SKU + country 聚合计算 item的数量
    df_ebay_listing_all = df_ebay_listing_all.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
    df_ebay_listing_temp = df_ebay_listing_all.groupby(['sku', 'country'])['item_id'].count().reset_index()
    df_ebay_listing_temp.rename(columns={'item_id': 'ebay_listing_num', 'country': 'site'}, inplace=True)
    df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].str.lower()
    df_ebay_listing_temp = pd.merge(df_ebay_listing_temp, df_site[['site', 'area']], how='left', on='site')

    # # 处理仓标数据
    # df_ebay_listing_temp_2 = df_ebay_listing_temp
    # df_ebay_listing_temp_2['new_sku'] = np.where(
    #     df_ebay_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
    #     df_ebay_listing_temp_2['sku'].str[3:], df_ebay_listing_temp_2['sku'])
    # df_ebay_listing_temp_2['new_sku'] = np.where(df_ebay_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
    #                                              df_ebay_listing_temp_2['sku'].str[:-2], df_ebay_listing_temp_2['new_sku'])
    #
    # df_ebay_listing_temp_2 = df_ebay_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
    #     'ebay_listing_num'].sum().reset_index()

    return df_ebay_listing_temp

def get_cd_listing_num(sku_list, df_site):
    print("===CD刊登链接数据===")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    # li_cd = []
    # for n in range(0, len(sku_list), 5000):
    #     sku_list_x = sku_list[n : n+5000]
    #     sql_cd = """
    #         SELECT
    #             a.account_id,
    #             b.short_name AS account_name,
    #             'CDISCOUNT' as platform,
    #             'FR' AS site,
    #             a.erp_sku as sku,
    #             a.product_id as product_id,
    #             a.seller_sku,
    #             best_shipping_charges,
    #             a.price + best_shipping_charges AS online_price,
    #             a.offer_state
    #         FROM  yibai_sale_center_listing_sync.yibai_cdiscount_listing a
    #         LEFT JOIN yibai_system_kd_sync.yibai_cdiscount_account b ON a.account_id = b.id
    #         WHERE
    #             a.offer_state = 'Active'
    #             and a.used_status = 1
    #             and b.token_status=1
    #             AND warehouse <> '中国' and sku in {}
    #             AND isfbc = 0 and used_status <> 2
    #           """.format(sku_list_x)
    #     cd_columns = ['account_id', 'account_name', 'platform', 'site', 'sku', 'product_id', 'seller_sku',
    #                   'best_shipping_charges', 'online_price', 'offer_state']
    #     cd_data = con.execute(sql_cd)
    #     df_cd_listing_x = pd.DataFrame(cd_data, columns=cd_columns)
    #     li_cd.append(df_cd_listing_x)
    #
    # df_cd_listing = pd.concat(li_cd)
    # print(df_cd_listing)



    # 固定WE国家账号 代码写死
    we_country_account_id = [17841, 17891, 17791, 17792, 17858, 17782, 17892, 17864, 17859, 17778, 17790,
          17839, 17715, 17885, 17735, 17802, 17803, 17769, 17700, 17787, 17832, 17824, 17910,
          17718, 17768, 17784, 17733, 17773, 17697, 17825, 17695, 17685, 17849, 17936, 17757,
          17761, 17889, 17709, 17765, 17683, 17771, 17756, 17777, 17681, 17762, 17822, 17861,
          17755, 17820, 17691, 17863, 17732, 17703, 17729, 17862, 17707, 17894, 17912, 17844,
          17914, 17878, 17766, 17958, 17739, 17682, 17719, 17749, 17873, 17818, 17731, 17713,
          17780, 17875, 17919, 17753, 17763, 17706, 17686, 17918, 17711, 17874, 17795, 17767,
          17816, 17817, 17759, 17870, 17796, 17897, 17915, 17712, 17708, 17736, 17852, 17721,
          17722, 17855, 17764, 17884, 17850, 17848, 17781, 17843, 17929, 17931, 17699, 17730,
          17737, 17908, 17969, 17702, 17813, 17951, 17962, 17977, 17943, 17779, 17701
          ]
    # ==========================================================================
    df_cd_listing = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku_list_x = sku_list[n : n+5000]
        sql_cd = f"""
            SELECT
                a.account_id,
                b.short_name AS account_name,
                'CDISCOUNT' as platform,
                'FR' AS site,
                a.sku as sku,
                a.product_id as product_id,
                a.seller_sku,
                best_shipping_charges,
                a.price + best_shipping_charges AS online_price,
                a.offer_state
            FROM  yibai_sale_center_listing_sync.yibai_cdiscount_listing a
            inner join (
                SELECT x.id account_id, a.delivery_country delivery_country, x.short_name
                FROM yibai_sale_center_system_sync.yibai_system_account x
                INNER JOIN yibai_sale_center_system_sync.yibai_system_auth_account a
                ON a.account_id = x.id
                WHERE platform_code  = 'CDISCOUNT' and delivery_country !='CN' and status=1
            ) b 
            on a.account_id=b.account_id
            WHERE
                a.offer_state = 1
                and a.used_status = 1
                and sku in {sku_list_x}
                and used_status <> 2
              """
        df_cd_listing_x = conn_ck.ck_select_to_df(sql_cd)
        df_cd_listing = pd.concat([df_cd_listing_x, df_cd_listing])

    df_cd_listing.columns = [i.split('.')[-1] for i in df_cd_listing.columns]
    print(df_cd_listing)

    # 按 SKU + country 聚合计算 item的数量
    df_cd_listing_temp = df_cd_listing.drop_duplicates(subset=['product_id', 'sku'])

    df_cd_listing_temp = df_cd_listing_temp.groupby(['sku', 'site'])['product_id'].count().reset_index()
    df_cd_listing_temp.rename(columns={'product_id': 'cd_listing_num'}, inplace=True)
    df_cd_listing_temp['site'] = df_cd_listing_temp['site'].str.lower()
    df_cd_listing_temp = pd.merge(df_cd_listing_temp, df_site[['site', 'area']], how='left', on='site')

    # 仓标处理
    df_cd_listing_temp_2 = df_cd_listing_temp.copy()
    df_cd_listing_temp_2['new_sku'] = np.where(
        df_cd_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_cd_listing_temp_2['sku'].str[3:], df_cd_listing_temp_2['sku'])
    df_cd_listing_temp_2['new_sku'] = np.where(df_cd_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                               df_cd_listing_temp_2['sku'].str[:-2], df_cd_listing_temp_2['new_sku'])

    df_cd_listing_temp_2 = df_cd_listing_temp_2.groupby(['new_sku', 'site', 'area'])['cd_listing_num'].sum().reset_index()

    # CD平台链接国家都为FR。但是对于德国仓有库存的SKU，统计时也应算上FR的链接数量
    df_cd_listing_temp_de = df_cd_listing_temp_2.copy()
    df_cd_listing_temp_de['site'] = 'de'
    df_cd_listing_temp_2 = pd.concat([df_cd_listing_temp_de, df_cd_listing_temp_2])

    return df_cd_listing_temp_2

def get_walmart_listing_num(sku_list, df_site):
    print("===Walmart刊登链接数据===")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # [old]
    # sql_walmart = """
    # SELECT
    #     a.account_id as account_id,b.short_name as short_name,
    #     case
    #         when b.site='us_dsv' then 'us'
    #         else b.site
    #     end as site,
    #     a.sku as sku, a.seller_sku as seller_sku, item_id, price
    # FROM
    #     yibai_product_kd_sync.yibai_walmart_listing a
    # left join
    #     yibai_system_kd_sync.yibai_walmart_account b on a.account_id=b.id
    # WHERE
    #     upper(publish_status)='PUBLISHED' and b.warehouse_delivery<>4 and b.status=1 and b.site<>'us_dsv'
    #     and sku in {}
    # """.format(sku_list)

    #[20230831]
    li_wal = []
    for n in range(0, len(sku_list), 1000):
        sku_list_x = sku_list[n : n+1000]
        # sql_walmart = """
        #     SELECT
        #         a.erp_id as account_id,b.short_name as short_name,
        #         case
        #             when b.site='us_dsv' then 'us'
        #             else b.site
        #         end as site,
        #         a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
        #     FROM (
        #         select * from yibai_sale_center_listing_sync.yibai_walmart_report
        #         where upper(publish_status)='PUBLISHED' and sku in {}
        #         order by updated_unix desc limit 1 by erp_id,seller_sku
        #     ) a
        #     left join yibai_system_kd_sync.yibai_walmart_account b
        #     on a.erp_id=b.id
        #     WHERE b.warehouse_delivery<>4 and b.status=1 and b.site<>'us_dsv'
        # """.format(sku_list_x)



        #  【20240524 修改】
        sql_walmart = """
                SELECT 
                    a.erp_id as account_id,b.short_name as short_name,
                    case 
                        when b.site='us_dsv' then 'us' 
                        else b.site 
                    end as site, 
                    a.sku as sku,a.seller_sku as seller_sku, toString(a.item_id) item_id, price
                FROM (
                    select * from yibai_sale_center_listing_sync.yibai_walmart_report 
                    where upper(publish_status)='PUBLISHED' and sku in {}
                    order by updated_unix desc limit 1 by erp_id,seller_sku
                ) a
                left join (
                SELECT erp_id, short_name, lower(site_code) as site, status
                FROM yibai_sale_center_system_sync.yibai_system_account
                WHERE platform_code = 'WALMART'
                ) b 
                on a.erp_id=b.erp_id
                WHERE b.status=1 and b.site<>'us_dsv'
            """.format(sku_list_x)

        # wal_columns = ['account_id', 'short_name', 'site', 'sku', 'seller_sku', 'item_id', 'price']
        # wal_data = con.execute(sql_walmart)
        # df_walmart_listing_x = pd.DataFrame(wal_data, columns=wal_columns)
        df_walmart_listing_x = conn_ck.ck_select_to_df(sql_walmart)
        li_wal.append(df_walmart_listing_x)

    df_walmart_listing = pd.concat(li_wal)
    df_walmart_listing.columns = [i.split('.')[-1] for i in df_walmart_listing.columns]
    print(df_walmart_listing)

    df_walmart_listing_temp = df_walmart_listing.drop_duplicates(subset=['item_id', 'sku'])

    df_walmart_listing_temp = df_walmart_listing_temp.groupby(['sku', 'site'])['item_id'].count().reset_index()
    df_walmart_listing_temp.rename(columns={'item_id': 'walmart_listing_num'}, inplace=True)
    df_walmart_listing_temp['site'] = df_walmart_listing_temp['site'].str.lower()
    df_walmart_listing_temp = pd.merge(df_walmart_listing_temp, df_site[['site', 'area']], how='left', on='site')

    df_walmart_listing_temp_2 = df_walmart_listing_temp
    df_walmart_listing_temp_2['new_sku'] = np.where(
        df_walmart_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_walmart_listing_temp_2['sku'].str[3:], df_walmart_listing_temp_2['sku'])
    df_walmart_listing_temp_2['new_sku'] = np.where(
        df_walmart_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']), df_walmart_listing_temp_2['sku'].str[:-2],
        df_walmart_listing_temp_2['new_sku'])
    df_walmart_listing_temp_2 = df_walmart_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
        'walmart_listing_num'].sum().reset_index()

    return df_walmart_listing_temp_2


def get_wish_listing_num(sku_list, df_site):
    print("===Wish刊登链接数据===")
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    li_wish = []
    li_wish_df_wish_listing = []
    for n in range(0, len(sku_list), 5000):
        sku_list_x = sku_list[n : n+5000]
        sql_wish = """
        SELECT sku, lower(country_code) as site, count(1) as wish_listing_num
        FROM yibai_oversea.yibai_wish_over_sea_listing
        WHERE sku in {}
        GROUP BY sku, site
        """.format(sku_list_x)
        # columns_wish = ['sku', 'site', 'wish_listing_num']
        # data_wish = con.execute(sql_wish)
        # df_wish_listing_x = pd.DataFrame(data_wish, columns=columns_wish)
        df_wish_listing_x = conn_ck.ck_select_to_df(sql_wish)
        li_wish_df_wish_listing.append(df_wish_listing_x)

        #
        # sql_base = """
        # SELECT account_id, sku, lower(country_code) as site, seller_sku, product_id, country_code, `直发当前价格` as online_price
        # FROM yibai_oversea.yibai_wish_over_sea_listing
        # WHERE sku in {}
        # settings max_memory_usage = 20000000000
        # """.format(sku_list_x)
        # # columns_base = ['account_id', 'sku', 'site', 'seller_sku', 'product_id', 'country_code', 'online_price']
        # # data_base = con.execute(sql_base)
        # # df_wish_listing_base_x = pd.DataFrame(data=data_base, columns=columns_base)
        # df_wish_listing_base_x = conn_ck.ck_select_to_df(sql_base)
        # li_wish.append(df_wish_listing_base_x)


    df_wish_listing = pd.concat(li_wish_df_wish_listing)
    # df_wish_listing_base = pd.concat(li_wish)
    # df_wish_listing_base.columns = [i.split('.')[-1] for i in df_wish_listing_base.columns]
    # print(df_wish_listing_base)
    #
    #
    # li_wish1 = []
    # for n in range(0, len(sku_list), 5000):
    #     sku_list_x = sku_list[n : n+5000]
    #     # 获取 wish 链接中，已勾选的国家信息
    #     sql = """
    #     SELECT account_id, sku, seller_sku, product_id, we_listing
    #     FROM yibai_wish.yibai_wish_listing
    #     WHERE status = 1 and sku in {}
    #     settings max_memory_usage = 20000000000
    #     """.format(sku_list_x)
    #     # columns_wish = ['account_id', 'sku', 'seller_sku', 'product_id', 'we_listing']
    #     # data_wish_listing = con.execute(sql)
    #     # df_wish_listing_code1 = pd.DataFrame(data_wish_listing, columns=columns_wish)
    #     df_wish_listing_code1 = conn_ck.ck_select_to_df(sql)
    #     li_wish1.append(df_wish_listing_code1)
    # df_wish_listing_code = pd.concat(li_wish1)
    #
    # # 将 we_listing(已勾选的国家）拆分成行
    # df_wish_listing_code = df_wish_listing_code.drop(['we_listing'], axis=1).join(df_wish_listing_code['we_listing'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename(
    #         'we_listing_code'))
    #
    # df_wish_listing_code.rename(columns={'we_listing_code': 'country_code'}, inplace=True)
    # df_wish_listing_code['country_code'] = df_wish_listing_code['country_code'].replace('GB', 'UK')
    # df_wish_listing_code.drop_duplicates(inplace=True)
    #
    # # 按 SKU + site 维度统计链接数量
    # df_wish_listing_temp = pd.merge(df_wish_listing_base, df_wish_listing_code, how='inner',
    #                                 on=['account_id', 'sku', 'product_id', 'seller_sku', 'country_code'])
    # df_wish_listing_temp = df_wish_listing.drop_duplicates()
    # df_wish_listing_temp = df_wish_listing_temp.groupby(['sku', 'site'])['seller_sku'].count().reset_index()
    # df_wish_listing_temp.rename(columns={'seller_sku': 'wish_listing_num'}, inplace=True)
    df_wish_listing_temp = pd.merge(df_wish_listing, df_site[['site', 'area']], how='left', on='site')

    # 仓标的处理
    df_wish_listing_temp_2 = df_wish_listing_temp
    df_wish_listing_temp_2['new_sku'] = np.where(
        df_wish_listing_temp_2['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df_wish_listing_temp_2['sku'].str[3:], df_wish_listing_temp_2['sku'])
    df_wish_listing_temp_2['new_sku'] = np.where(df_wish_listing_temp_2['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                 df_wish_listing_temp_2['sku'].str[:-2], df_wish_listing_temp_2['new_sku'])

    df_wish_listing_temp_2 = df_wish_listing_temp_2.groupby(['new_sku', 'site', 'area'])[
        'wish_listing_num'].sum().reset_index()

    return df_wish_listing_temp_2

def get_allegro_listing_num(sku_list, df_site):
    print("===allegro刊登链接数据===")
    sql = f"""
    select 
        a.account_id as account_id, b.erp_id, b.short_name as account_name,  
        a.offer_id as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price,
        toFloat64(a.delivery_amount) as delivery_amount, a.location1 as location, b.group_name as group_name
    FROM (
        SELECT 
            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
        FROM yibai_sale_center_listing_sync.yibai_allegro_listing
        WHERE 
            selling_mode_price > 0 
            -- and status in (1,4)
            and location1 not in ('CN', 'GB')
        ) a 
    INNER JOIN (
        SELECT b.id, b.erp_id,  b.short_name, c.group_name
        FROM yibai_sale_center_system_sync.yibai_system_account b
        inner join (
            select account_id,splitByString('->', oa_group_name)[-1] as group_name
            from yibai_account_manage_sync.yibai_allegro_account
            where account_type = 1 and is_platform_account != 2 and account_num_name != 'DMSAllegro'
        ) c
        on b.account_id=c.account_id
        WHERE platform_code  = 'ALLEGRO' 
    ) b 
    on a.account_id=b.id

    """
    conn_kd = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df = conn_kd.ck_select_to_df(sql)
    # 获取海外仓sku
    sql = f"""
        select distinct sku
        from dwm_sku_temp_info
        WHERE date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    # df = pd.merge(df, df_sku, how='inner', on=['sku'])
    df = df[df['sku'].isin(sku_list)]

    # 按 SKU + country 聚合计算 item的数量
    df = df[~df['location'].isna()]
    df['location'] = df['location'].replace({'CZ':'DE'})
    df = df.drop_duplicates(subset=['account_id', 'offer_id', 'sku'])
    df = df.groupby(['sku', 'location'])['offer_id'].count().reset_index()
    df.rename(columns={'offer_id': 'allegro_listing_num', 'location': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
        df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                                                 df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['allegro_listing_num'].sum().reset_index()

    return df

def get_aliexpress_listing_num(df_site):
    print("===aliexpress刊登链接数据===")
    sql = """
        SELECT 
            product_id,a.sku sku,sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
            b.sku_property_id, c.name_en
        FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing_skus a
        INNER JOIN (
            SELECT
                aeop_s_k_u_property_list,aeop_s_k_u_property_list_str,
                arrayJoin(JSONExtractArrayRaw(aeop_s_k_u_property_list_str)) as aeop_ae_product_skus1,
                visitParamExtractFloat(aeop_ae_product_skus1, 'property_value_id') as property_value_id,
                visitParamExtractFloat(aeop_ae_product_skus1, 'sku_property_id') as sku_property_id
            FROM yibai_domestic.yibai_aliexpress_listing_skus_aeop_s_k_u_property_list
            -- 国外发货地的链接
            WHERE sku_property_id=200007763 and property_value_id!=201336100
        ) b ON a.aeop_s_k_u_property_list = b.aeop_s_k_u_property_list
        -- 具体国外发货地
        LEFT JOIN (
            SELECT DISTINCT parent_attr_id, attr_id, name_en
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_category_attribute
        ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
        where a.sku !='' 
    """
    # conn_ck = pd_to_ck(database='temp_database_hxx', data_sys='调价明细历史数据1')
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_ali_listing = conn_ck.ck_select_to_df(sql)
    df_ali_listing.columns = [i.split('.')[-1] for i in df_ali_listing.columns]
    #
    df_ali_listing[['property_value_id', 'sku_property_id']] = df_ali_listing[['property_value_id', 'sku_property_id']].astype(int).astype(str)
    dic = {'United States':'US', 'CZECH REPUBLIC':'DE', 'Czech Republic':'DE', 'Poland':'DE', 'france':'DE', 'France':'DE',
           'Australia':'AU', 'CN':'CN', 'spain':'DE', 'SPAIN':'DE', 'Russian Federation':'RU', 'UNITED KINGDOM':'UK',
           'United Kingdom':'UK','GERMANY':'DE', 'Mexico':'MX', 'cz':'DE', 'ITALY':'DE', 'Italy':'DE'}
    df_ali_listing['country'] = df_ali_listing['name_en'].replace(dic)
    df_ali_listing = df_ali_listing.drop_duplicates(subset=['product_id','sku','country'])
    #
    df_ali_listing = df_ali_listing[df_ali_listing['country']!='CN']
    print(df_ali_listing.info())
    #
    # 按 SKU + country 聚合计算 item的数量
    df = df_ali_listing[~df_ali_listing['country'].isna()]
    df = df.groupby(['sku', 'country'])['product_id'].count().reset_index()
    df.rename(columns={'product_id': 'aliexpress_listing_num', 'country': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')

    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['aliexpress_listing_num'].sum().reset_index()

    return df

##
def get_temu_listing_num():
    print("===temu刊登链接数据===")
    # sql = """
    # with d as
    # (select product_spu_id,product_sku_id,max(id) as id from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log
    # group by product_spu_id,product_sku_id),
    # c as (select * from yibai_sale_center_listing_sync.yibai_temu_listing_crawling_log where id in (select id from d))
    #
    # select
    #     e.account_id,e.short_name,a.site_code,a.item_id,a.product_sku_id,a.stock_number,c.online_status,a.sku,b.lazada_account_operation_mode,
    #     c.added_to_site_time,c.supplier_price,date(a.create_time) as `刊登时间`
    # from yibai_sale_center_listing_sync.yibai_temu_listing a
    # left join yibai_sale_center_common_sync.yibai_common_account_config b on a.account_id=b.account_id
    # left join c on a.item_id =c.product_spu_id and a.product_sku_id=c.product_sku_id
    # left join yibai_sale_center_system_sync.yibai_system_account as e on a.account_id=e.id
    # where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    # -- and a.sku = '1210230023711'
    # """
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    # listing_t = conn_ck.ck_select_to_df(sql)
    # listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    # listing_t = listing_t.sort_values(by='added_to_site_time', ascending=False).drop_duplicates(subset=['account_id','product_sku_id','sku'])
    # listing_t['运营模式'] = listing_t['lazada_account_operation_mode'].map({1: '全托管', 2: '半托管'})
    # del listing_t['lazada_account_operation_mode']
    #
    #
    #
    # # 获取捆绑链接表的sku信息
    # sql = """
    #  select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku
    #  from yibai_sale_center_listing_sync.yibai_temu_bind_sku
    #  order by update_time desc
    #  """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # yibai_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    # yibai_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)
    # listing_t[['product_sku_id', 'sku', 'item_id']] = listing_t[['product_sku_id', 'sku', 'item_id']].astype('str')
    # listing_t = listing_t.merge(yibai_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
    #                             suffixes=['', '1'])
    # listing_t.loc[listing_t['sku'] == '', 'sku'] = np.nan
    # listing_t['sku'].fillna(listing_t['sku1'], inplace=True)
    # listing_t.drop('sku1', axis=1, inplace=True)
    #
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)
    # listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    # listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    # listing_t['是否核价通过链接'] = np.where(
    #     listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止']), 1, 0)
    # dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
    #        'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
    #        'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    # listing_t['站点'] = listing_t['site_code'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))
    # #
    # listing_t['warehouse'] = listing_t['站点'].replace({'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓',
    #                                                     '英国': '英国仓', '新西兰': '澳洲仓', '加拿大': '加拿大仓','墨西哥':'墨西哥仓'})
    # listing_t = listing_t.drop_duplicates()
    #
    # # temu账号获取
    # sql = """
    # select
    #     a.account_id `账号ID`,a.account_status `账号状态`,a.account_id `平台账号ID`, b.main_name `主体账号`,a.oa_department_name `大部`,
    #     a.oa_group_name `小组` ,a.account_name `账号全称`,a.account_s_name `账号简称`,a.account_num_name `账号店铺名`,a.account_operation_mode `账号运营模式`
    # from yibai_account_manage_sync.yibai_temu_account a
    # left join yibai_account_manage_sync.yibai_account_main b
    # on a.main_id=b.main_id
    # where a.account_type=1
    # """
    # conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    # df_temu_account = conn_ck.ck_select_to_df(sql)
    # df_temu_account['账号状态'] = df_temu_account['账号状态'].map({10: '启用', 20: '停用', 30: '异常', })
    # df_temu_account['账号运营模式'] = df_temu_account['账号运营模式'].map({1: '全托管', 2: '半托管'})
    # df_temu_account.rename(columns={'账号ID': 'account_id'}, inplace=True)
    #
    # listing_t = pd.merge(listing_t,
    #                          df_temu_account[['account_id', '主体账号', '大部', '小组', '账号运营模式']],
    #                          how='left', on=['account_id'])
    #
    # # listing_t.to_excel('F://Desktop//listing_t.xlsx', index=0)
    sql = """
        SELECT *
        FROM yibai_oversea.yibai_ads_oversea_temu_listing
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_temu_listing )
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    listing_t = conn_ck.ck_select_to_df(sql)
    listing_t = listing_t.drop_duplicates(subset=['account_id','product_sku_id','sku'])
    print(listing_t.info())
    # 1、有效链接
    df_temu_listing = listing_t[
        (listing_t['online_status'] == '已加入站点') & (~listing_t['sku'].isna()) & (listing_t['sku'] != '')]
    # 整体的链接数量
    df_temu = df_temu_listing.groupby(['sku', 'warehouse']).agg({'product_sku_id': 'count'}).reset_index()
    df_temu = df_temu[~df_temu['sku'].isna()]
    df_temu = df_temu.rename(columns={'product_sku_id': 'temu_listing_num'})

    # df_temu.to_excel('F://Desktop//df_temu_listing.xlsx', index=0)

    return df_temu


##
def get_shopee_listing_num():
    """ 统计shopee平台海外仓链接 """
    print("===shopee刊登链接数据===")
    # 取shopee海外仓账号

    sql = """

        SELECT account_id, account_s_name, account_type
        FROM yibai_account_manage_sync.yibai_shopee_account a
        WHERE account_mold != 1 and account_status = 10 and account_type != 20 -- 剔除tt账号

    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_shopee_account = conn_ck.ck_select_to_df(sql)
    print(df_shopee_account.info())
    sql = """
        select distinct account_id,a.short_name,b.shopee_is_3pf, a.id as id
        from yibai_sale_center_system_sync.yibai_system_account as a
        left join yibai_sale_center_common_sync.yibai_common_account_config as b
        on b.account_id = a.id and  b.platform_code='SHOPEE' and b.is_del=0
        where a.platform_code='SHOPEE' and a.is_del=0
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_shopee_account = conn_ck.ck_select_to_df(sql)
    account_1 = yibai_shopee_account[yibai_shopee_account['shopee_is_3pf']==1]
    account_1 = tuple(account_1['id'].unique())

    df_shopee_account = pd.merge(df_shopee_account, yibai_shopee_account, how='inner', on='account_id')
    account_2 = tuple(df_shopee_account['id'].unique())

    # temp = pd.concat([yibai_shopee_account, df_shopee_account])
    # temp.to_excel('F://Desktop//df_shopee_account.xlsx', index=0)
    account_list = tuple(set(account_1).union(set(account_2)))
    sql = f"""
        SELECT item_id, account_id, price, status_online, sku, country_code
        FROM yibai_sale_center_listing_sync.yibai_shopee_listing
        WHERE is_delete = 0 and account_id in {account_list}
        and status_online in ('NORMAL','UNLIST','MODEL_NORMAL')
        -- and sku in ('QC15580-01', 'QC15580')
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_shopee_listing = conn_ck.ck_select_to_df(sql)
    print(yibai_shopee_listing.info())

    # yibai_shopee_listing.to_excel('F://Desktop//yibai_shopee_listing.xlsx', index=0)

    # 按 SKU + country 聚合计算 item的数量
    df = yibai_shopee_listing[(yibai_shopee_listing['country_code']!='') & (yibai_shopee_listing['sku']!='')]
    df = df.groupby(['sku', 'country_code'])['item_id'].count().reset_index()
    df.rename(columns={'item_id': 'shopee_listing_num', 'country_code': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    # df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    dic = {'my':'马来西亚', 'th':'泰国', 'ph':'菲律宾', 'id':'印度尼西亚', 'vn':'越南'}
    df['area'] = df['site'].replace(dic)
    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['shopee_listing_num'].sum().reset_index()

    # df.to_excel('F://Desktop//yibai_shopee_listing_2.xlsx', index=0)

    return df

def get_lazada_listing_num(sku_list):
    """ 统计lazada平台海外仓链接 """
    print("===lazada刊登链接数据===")
    # 取lazada海外仓账号

    sql = """

        SELECT account_id, account_s_name, account_type
        FROM yibai_account_manage_sync.yibai_lazada_account a
        WHERE account_mold = 1 and account_status = 10 and account_type != 20 -- 筛选tt账号

    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_lazada_account = conn_ck.ck_select_to_df(sql)
    print(df_lazada_account.info())
    sql = """
        select distinct account_id,a.short_name, a.id as id
        from yibai_sale_center_system_sync.yibai_system_account as a
        left join yibai_sale_center_common_sync.yibai_common_account_config as b
        on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
        where a.platform_code='LAZADA' and a.is_del=0
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_lazada_account = conn_ck.ck_select_to_df(sql)

    df_lazada_account = pd.merge(df_lazada_account, yibai_lazada_account, how='inner', on='account_id')
    # YB lazada 3pf账号 Ytools-ph, 24955
    account_list = df_lazada_account['id'].unique()
    account_list = np.append(account_list, '24955')
    account_list = tuple(account_list)
    # sku_list = sku_list[0:100]
    yibai_lazada_listing = pd.DataFrame()
    for n in range(0,len(sku_list), 5000):
        sku_list_x = sku_list[n:n+5000]
        sql = f"""
            SELECT item_id, account_id, price, status_online, sku, country_code
            FROM yibai_sale_center_listing_sync.yibai_lazada_listing
            WHERE is_delete = 0 and account_id in {account_list}
            and status_online not in ('Deleted') and sku in {sku_list_x}
        """
        conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
        yibai_lazada_listing_temp = conn_ck.ck_select_to_df(sql)
        yibai_lazada_listing = pd.concat([yibai_lazada_listing_temp, yibai_lazada_listing])

    print(yibai_lazada_listing.info())

    # 按 SKU + country 聚合计算 item的数量
    df = yibai_lazada_listing[(yibai_lazada_listing['country_code']!='') & (yibai_lazada_listing['sku']!='')]
    df = df.groupby(['sku', 'country_code'])['item_id'].count().reset_index()
    df.rename(columns={'item_id': 'lazada_listing_num', 'country_code': 'site'}, inplace=True)
    df['site'] = df['site'].str.lower()
    # df_site = site_table()
    # df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    dic = {'my':'马来西亚', 'th':'泰国', 'ph':'菲律宾', 'id':'印度尼西亚', 'vn':'越南'}
    df['area'] = df['site'].replace(dic)
    # 处理仓标数据
    df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
                             df['sku'].str[3:], df['sku'])
    df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
                             df['sku'].str[:-2], df['new_sku'])

    df = df.groupby(['new_sku', 'site', 'area'])['lazada_listing_num'].sum().reset_index()

    # df.to_excel('F://Desktop//yibai_lazada_listing.xlsx', index=0)

    return df
##
def get_lazada_listing_num_temp():
    """ 统计lazada平台海外仓链接 """
    print("===lazada刊登链接数据===")
    df_sku_listing_new = get_sku_info()
    sku_list = df_sku_listing_new['sku'].to_list()
    # 取lazada海外仓账号

    sql = """

        SELECT account_id, account_s_name, account_type
        FROM yibai_account_manage_sync.yibai_lazada_account a
        WHERE account_mold = 1 and account_status = 10 and account_type != 20 -- 筛选tt账号

    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_lazada_account = conn_ck.ck_select_to_df(sql)
    print(df_lazada_account.info())
    sql = """
        select distinct account_id,a.short_name, a.id as id
        from yibai_sale_center_system_sync.yibai_system_account as a
        left join yibai_sale_center_common_sync.yibai_common_account_config as b
        on b.account_id = a.id and  b.platform_code='LAZADA' and b.is_del=0
        where a.platform_code='LAZADA' and a.is_del=0
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_lazada_account = conn_ck.ck_select_to_df(sql)

    df_lazada_account = pd.merge(df_lazada_account, yibai_lazada_account, how='inner', on='account_id')
    # YB lazada 3pf账号 Ytools-ph, 24955
    account_list = df_lazada_account['id'].unique()
    account_list = np.append(account_list, '24955')
    account_list = tuple(account_list)
    sql = f"""
        SELECT item_id, account_id, price, status_online, sku, country_code
        FROM yibai_sale_center_listing_sync.yibai_lazada_listing
        WHERE is_delete = 0 and account_id in {account_list}
        and status_online not in ('Deleted')  and sku in {sku_list}
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    yibai_lazada_listing = conn_ck.ck_select_to_df(sql)
    print(yibai_lazada_listing.info())

    # # 按 SKU + country 聚合计算 item的数量
    df = yibai_lazada_listing[(yibai_lazada_listing['country_code']!='') & (yibai_lazada_listing['sku']!='')]
    # df = df.groupby(['sku', 'country_code'])['item_id'].count().reset_index()
    # df.rename(columns={'item_id': 'lazada_listing_num', 'country_code': 'site'}, inplace=True)
    # df['site'] = df['site'].str.lower()
    # # df_site = site_table()
    # # df = pd.merge(df, df_site[['site', 'area']], how='left', on='site')
    # dic = {'my':'马来西亚', 'th':'泰国', 'ph':'菲律宾仓', 'id':'印度尼西亚仓', 'vn':'越南仓'}
    # df['area'] = df['site'].replace(dic)
    # # 处理仓标数据
    # df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
    #                          df['sku'].str[3:], df['sku'])
    # df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
    #                          df['sku'].str[:-2], df['new_sku'])
    #
    # df = df.groupby(['new_sku', 'site', 'area'])['lazada_listing_num'].sum().reset_index()

    df.to_excel('F://Desktop//yibai_lazada_listing.xlsx', index=0)

    return df


def listing_num_section(df):
    """
    对各平台链接数量添加分段列
    """
    # 分段值可根据实际数量分布调整
    for i in df.columns[16:]:
        new_column = i + '_section'
        df[new_column] = pd.cut(df[i], bins=[-1, 0, 1, 3, 5, 10, 20, 40, 60, 10000],
                                labels=['A:0', 'B:1', 'C:(1,3]', 'D:(3,5]', 'E:(5,10]', 'F:(10,20]', 'G:(20,40]',
                                        'H:(40,60]', 'I:60+'])
    return df


def get_ban_info(df):
    # 侵权违禁信息数据读取
    # 读取侵权信息数据表
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    sql_info = f"""
    SELECT sku, arrayStringConcat(groupArray(country_code), ',') AS info_country, any(risk_grade) as `侵权信息`, any(risk_grade_type) as `侵权等级` 
    FROM yibai_prod_base_sync.yibai_prod_inf_country_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_info = conn_ck.ck_select_to_df(sql_info)
    df_info['sku'] = df_info['sku'].astype('str')
    # 读取禁售信息数据表
    sql_js = f"""
    SELECT sku ,arrayStringConcat(groupArray(platform_code), ',') as forbid_plateform, any(risk_grade) as `禁售信息` , any(risk_grade_type) as `禁售等级` 
    FROM yibai_prod_base_sync.yibai_prod_forbidden_grade
    WHERE is_del = 0 
    GROUP BY sku
    """
    df_js = conn_ck.ck_select_to_df(sql_js)
    df_js['sku'] = df_js['sku'].astype('str')

    df = pd.merge(df, df_js, on='sku', how='left')
    df = pd.merge(df, df_info, on='sku', how='left')

    return df

def temp(stock_date='new'):
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()
    if stock_date == 'new':
        df_sku_listing_new = get_sku_info()
    else:
        df_sku_listing_new = get_old_record()
    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()
    # 20241206 给物流部优先海外仓发货清单
    df_clear_sku = pd.read_excel('F://Ding_workspace//优先配库海外仓发货sku明细20241203.xlsx', dtype={'sku':str})
    df_sku = pd.merge(df_sku_listing_new, df_clear_sku, how='left', on=['sku', 'warehouse'])
    df_sku_1 = df_sku[df_sku['目的国'].isna()]
    df_sku_1.drop('目的国', axis=1, inplace=True)
    sku_list1 = df_sku_1['sku'].to_list()
    df_sku_2 = df_sku[~df_sku['目的国'].isna()]
    df_sku_2.drop('目的国', axis=1, inplace=True)
    sku_list2 = df_sku_2['sku'].to_list()

    df_amazon_1 = get_amazon_listing_num(sku_list1, df_site)
    df_amazon_1 = pd.merge(df_amazon_1, df_sku_1[['new_sku','area']], how='inner', on=['new_sku','area'])
    df_amazon_2 = get_amazon_listing_num(sku_list2, df_site)
    df_amazon_2 = pd.merge(df_amazon_2, df_sku_2[['new_sku', 'area']], how='inner', on=['new_sku', 'area'])
    df_amazon_1['is_clear_sku'] = 0
    df_amazon_2['is_clear_sku'] = 1
    df_amazon_listing_temp_2 = pd.concat([df_amazon_1, df_amazon_2])

    #
    # # 按 sku + site 合并
    # df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df_amazon_listing_temp_2 = pd.concat([df_amazon_1, df_amazon_2])
    # df_ebay_listing_temp_2 = get_ebay_listing_num_new_temp(sku_list, df_site)
    # df_ebay_listing_temp_3 = get_ebay_listing_num_new(sku_list, df_site)
    # df_cd_listing_temp_2 = get_cd_listing_num(sku_list, df_site)
    # df_wish = get_wish_listing_num(sku_list, df_site)
    # temu_group, df_temu_listing = get_temu_listing_num()
    # df = get_allegro_listing_num(sku_list, df_site)

    # df_sku_listing_site = pd.merge(df_sku_listing_new, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_3[['new_sku', 'ebay_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df = pd.merge(df_sku_listing_new, df[['new_sku', 'allegro_listing_num', 'site']], how='left', on=['new_sku', 'site'])
    # df = pd.merge(df, df_wish[['new_sku', 'wish_listing_num', 'site']],
    #                                how='left', on=['new_sku', 'site'])
    # df = pd.merge(df, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
    #                                how='left', on=['sku', 'warehouse'])
    # save_df(df_sku_listing_new, '海外仓SKU各平台刊登情况_sku', file_type='xlsx')
    save_df(df_amazon_listing_temp_2, '海外仓SKU各平台刊登情况_test', file_type='xlsx')

    return None
##
def temu_group():
    """
    temu链接覆盖海外仓sku情况，分团队
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    df_sku_listing_new = get_sku_info()

    temu_group, df_temu_listing = get_temu_listing_num()
    temu_group = temu_group.fillna(0)  #

    df_sku_listing_new = pd.merge(df_sku_listing_new, temu_group, how='left', on=['sku', 'warehouse'])
    df_sku_listing_new.iloc[:, 14:] = df_sku_listing_new.iloc[:, 14:].fillna(0).astype(int)
    df_sku_listing_new = listing_num_section(df_sku_listing_new)

    save_df(df_sku_listing_new, 'TEMU团队海外仓SKU刊登情况', file_type='xlsx')

##
def main(stock_date='new'):
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()
    if stock_date == 'new':
        df_sku_listing_new = get_sku_info()
    else:
        df_sku_listing_new = get_old_record()

    # df_sku_listing_new = df_sku_listing_new.sample(2000)

    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    df_amazon_listing_temp_2 = get_amazon_listing_num_new(sku_list, df_site)
    df_ebay_listing_temp_2 = get_ebay_listing_num_new(sku_list, df_site)
    df_cd_listing_temp_2 = get_cd_listing_num(sku_list, df_site)
    df_walmart_listing_temp_2 = get_walmart_listing_num(sku_list, df_site)
    df_wish_listing_temp_2 = get_wish_listing_num(sku_list, df_site)
    df_temu_listing = get_temu_listing_num()
    df_allegro = get_allegro_listing_num(sku_list, df_site)
    df_ali = get_aliexpress_listing_num(df_site)
    df_shopee = get_shopee_listing_num()
    df_lazada = get_lazada_listing_num(sku_list)

    # 按 sku + site 合并
    df_sku_listing_site = pd.merge(df_sku_listing_new, df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_cd_listing_temp_2[['new_sku', 'cd_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site,
                                   df_walmart_listing_temp_2[['new_sku', 'walmart_listing_num', 'site']], how='left',
                                   on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_wish_listing_temp_2[['new_sku', 'wish_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
                                   how='left', on=['sku', 'warehouse'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_allegro[['new_sku','site', 'allegro_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ali[['new_sku','area', 'aliexpress_listing_num']],
                                   how='left', on=['new_sku', 'area'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_shopee[['new_sku','site', 'shopee_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_lazada[['new_sku','site', 'lazada_listing_num']],
                                   how='left', on=['new_sku', 'site'])

    # 统计欧洲区域sku亚马逊和ebay平台欧洲各站点（德法西意）的刊登情况
    # amazon_temp = df_amazon_listing_temp_2[df_amazon_listing_temp_2['area'] == '欧洲']
    #
    amazon_temp = df_amazon_listing_temp_2.loc[df_amazon_listing_temp_2['area'].isin(['欧洲','巴西', '墨西哥'])]  # 新增墨西哥
    amazon_temp.drop(['area'], axis=1, inplace=True)

    amazon_temp = amazon_temp.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()

    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)

    df_sku_listing_site = pd.merge(df_sku_listing_site, amazon_temp[['new_sku', 'amazon_de', 'amazon_fr', 'amazon_es', 'amazon_it','amazon_br', 'amazon_mx']], how='left', on=['new_sku'])

    ebay_temp = df_ebay_listing_temp_2[df_ebay_listing_temp_2['area'] == '欧洲']
    ebay_temp.drop(['area'], axis=1, inplace=True)

    ebay_temp = ebay_temp.pivot_table(index='new_sku', columns='site').reset_index()
    ebay_temp.columns = ebay_temp.columns.droplevel(0)
    ebay_temp.columns = 'ebay_' + ebay_temp.columns
    ebay_temp.rename(columns={'ebay_': 'new_sku'}, inplace=True)
    ebay_temp['ebay_fr'] = 0
    print(ebay_temp.info())
    df_sku_listing_site = pd.merge(df_sku_listing_site, ebay_temp[['new_sku', 'ebay_de', 'ebay_fr']], how='left', on=['new_sku'])
    df_sku_listing_site.fillna(0, inplace=True)
    #
    print(df_sku_listing_site.info())
    df_sku_listing_site_test = listing_num_section(df_sku_listing_site)
    # 侵权违禁
    df_sku_listing_site_test = get_ban_info(df_sku_listing_site_test)

    need_columns = ["sku","spu", "warehouse", "max_stock_warehouse_id","max_stock_warehouse_name","date_id","total_available_stock",
                    "new_price","warehouse_stock_info","develop_source_name","overage_level","销售状态","area","site","new_sku","责任人",
                    "amazon_listing_num","amazon_de","amazon_fr","amazon_es","amazon_it",
                    'amazon_br','amazon_mx',"ebay_listing_num","ebay_de","ebay_fr","cd_listing_num","walmart_listing_num",
                    "wish_listing_num","temu_listing_num","allegro_listing_num","aliexpress_listing_num",
                    "shopee_listing_num", "lazada_listing_num",
                    "amazon_listing_num_section","amazon_de_section","amazon_fr_section","amazon_es_section",
                    "amazon_it_section","ebay_listing_num_section","ebay_de_section","ebay_fr_section","cd_listing_num_section",
                    "walmart_listing_num_section","wish_listing_num_section","temu_listing_num_section",
                    "allegro_listing_num_section","aliexpress_listing_num_section",
                    "shopee_listing_num_section", "lazada_listing_num_section","info_country","侵权信息","侵权等级",
                    "forbid_plateform","禁售信息","禁售等级"]
    df_sku_listing_site_test = df_sku_listing_site_test[need_columns]
    change_columns = ["sku","spu","warehouse","最大库存子仓ID","最大库存子仓","最近有库存日期","当前库存","成本","子仓库存明细","开发来源","超库龄等级","销售状态",
                      "仓库区域","仓库所在国家","new_sku（主SKU）","责任人","amazon_listing_num","amazon_DE","amazon_FR","amazon_ES",
                      "amazon_IT","amazon_BR","amazon_MX","ebay_listing_num","ebay_DE","ebay_FR","cd_listing_num",
                      "walmart_listing_num","wish_listing_num","temu_listing_num","allegro_listing_num","aliexpress_listing_num",
                      "shopee_listing_num", "lazada_listing_num","Amazon链接数量分段","Amazon_DE链接数量分段",
                      "Amazon_FR链接数量分段","Amazon_ES链接数量分段","Amazon_IT链接数量分段","ebay链接数量分段",
                      "ebay_DE链接数量分段","ebay_FR链接数量分段","cd链接数量分段","walmat链接数量分段","wish链接数量分段",
                      "temu链接数量分段","allegro链接数量分段","aliexpress链接数量分段",
                      "shopee链接数量分段", "lazada链接数量分段",
                      "info_country","侵权信息", "侵权等级","forbid_plateform","禁售信息","禁售等级"]
    df_sku_listing_site_test.columns = change_columns

    # 匹配日销
    df_sales = get_sales()

    df = pd.merge(df_sku_listing_site_test, df_sales[['sku','warehouse','day_sales']],
                                        how='left', on=['sku','warehouse'])
    daysales = df.pop('day_sales')
    df.insert(df.columns.get_loc('超库龄等级') + 1, '日销', daysales)

    # 库存转化
    df['库存_分段'] = np.where(df['当前库存'] > 5, '5+', df['当前库存'])
    stock_bin = df.pop('库存_分段')
    df.insert(df.columns.get_loc('当前库存') + 1, '库存_分段', stock_bin)

    df['库存金额'] = df['当前库存'] * df['成本']
    stock_money = df.pop('库存金额')
    df.insert(df.columns.get_loc('库存_分段') + 1, '库存金额', stock_money)

    save_df(df, '海外仓SKU各平台刊登情况V2', file_type='xlsx')
    print('done!')
    # df_sku_listing_site_test.to_excel('df_sku_listing_site.xlsx', index=0)


def main_ym():
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    # df_sku_listing_new = get_sku_info()
    # df_sku_listing_new = df_sku_listing_new.sample(1)
    df_sku_listing_new = get_ym_sku()
    # df_sku_listing_new = pd.concat([df_sku_listing_new, df_ym])
    df_sku_listing_new['site'] = df_sku_listing_new['site'].apply(lambda x: x.lower())
    df_sku_listing_new['new_sku'] = df_sku_listing_new['sku']
    df_sku_listing_new['warehouse'] = df_sku_listing_new['site'].replace({'us':'美国仓','de':'德国仓'})
    df_sku_listing_new['area'] = df_sku_listing_new['site'].replace({'us': '美国', 'de': '欧洲'})
    print(df_sku_listing_new.info())

    # df_sku_listing_new = df_sku_listing_new.sample(2000)

    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    df_ali = get_aliexpress_listing_num(df_site)
    df_amazon_listing_temp_2 = get_amazon_listing_num(sku_list, df_site)
    df_ebay_listing_temp_2 = get_ebay_listing_num_new(sku_list, df_site)
    df_walmart_listing_temp_2 = get_walmart_listing_num(sku_list, df_site)
    df_temu_listing = get_temu_listing_num()
    df_allegro = get_allegro_listing_num(sku_list, df_site)

    # df_amazon_listing_temp_2.to_excel('F://Desktop//df_amazon_listing_temp_2.xlsx', index=0)
    # 按 sku + site 合并
    df_sku_listing_site = pd.merge(df_sku_listing_new,
                                   df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ebay_listing_temp_2[['new_sku', 'ebay_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site,
                                   df_walmart_listing_temp_2[['new_sku', 'walmart_listing_num', 'site']], how='left',
                                   on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_temu_listing[['sku', 'warehouse', 'temu_listing_num']],
                                   how='left', on=['sku', 'warehouse'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_allegro[['new_sku', 'site', 'allegro_listing_num']],
                                   how='left', on=['new_sku', 'site'])
    df_sku_listing_site = pd.merge(df_sku_listing_site, df_ali[['new_sku','area', 'aliexpress_listing_num']],
                                   how='left', on=['new_sku', 'area'])
    # 统计欧洲区域sku亚马逊和ebay平台欧洲各站点（德法西意）的刊登情况
    # amazon_temp = df_amazon_listing_temp_2[df_amazon_listing_temp_2['area'] == '欧洲']
    #
    amazon_temp = df_amazon_listing_temp_2.loc[
        df_amazon_listing_temp_2['area'].isin(['欧洲', '巴西', '墨西哥'])]  # 新增墨西哥
    amazon_temp.drop(['area'], axis=1, inplace=True)

    amazon_temp = amazon_temp.pivot_table(index='new_sku', columns='site', aggfunc='sum').reset_index()

    amazon_temp.columns = amazon_temp.columns.droplevel(0)
    amazon_temp.columns = 'amazon_' + amazon_temp.columns
    amazon_temp.rename(columns={'amazon_': 'new_sku'}, inplace=True)
    amazon_temp['area'] = '欧洲'
    df_sku_listing_site = pd.merge(df_sku_listing_site, amazon_temp[
        ['new_sku', 'area','amazon_de', 'amazon_fr', 'amazon_es', 'amazon_it']], how='left',
                                   on=['new_sku','area'])

    # ebay_temp = df_ebay_listing_temp_2[df_ebay_listing_temp_2['area'] == '欧洲']
    # ebay_temp.drop(['area'], axis=1, inplace=True)
    #
    # ebay_temp = ebay_temp.pivot_table(index='new_sku', columns='site').reset_index()
    # ebay_temp.columns = ebay_temp.columns.droplevel(0)
    # ebay_temp.columns = 'ebay_' + ebay_temp.columns
    # ebay_temp.rename(columns={'ebay_': 'new_sku'}, inplace=True)
    # ebay_temp['ebay_fr'] = 0
    # print(ebay_temp.info())
    # df_sku_listing_site = pd.merge(df_sku_listing_site, ebay_temp[['new_sku', 'ebay_de', 'ebay_fr']], how='left',
    #                                on=['new_sku'])

    df_sku_listing_site.fillna(0, inplace=True)
    #
    print(df_sku_listing_site.info())
    df_sku_listing_site_test = listing_num_section(df_sku_listing_site)
    # 侵权违禁
    df = get_ban_info(df_sku_listing_site_test)

    # need_columns = ["sku", "spu", "warehouse", "max_stock_warehouse_id", "max_stock_warehouse_name", "date_id",
    #                 "total_available_stock",
    #                 "new_price", "warehouse_stock_info", "develop_source_name", "overage_level", "销售状态", "area",
    #                 "site", "new_sku", "责任人",
    #                 "amazon_listing_num", "amazon_de", "amazon_fr", "amazon_es", "amazon_it",
    #                 'amazon_br', 'amazon_mx', "ebay_listing_num", "ebay_de", "ebay_fr", "cd_listing_num",
    #                 "walmart_listing_num",
    #                 "temu_listing_num", "allegro_listing_num",
    #                 "amazon_listing_num_section", "amazon_de_section", "amazon_fr_section", "amazon_es_section",
    #                 "amazon_it_section", "ebay_listing_num_section", "ebay_de_section", "ebay_fr_section",
    #                 "walmart_listing_num_section", "temu_listing_num_section",
    #                 "allegro_listing_num_section", "info_country", "侵权信息", "侵权等级",
    #                 "forbid_plateform", "禁售信息", "禁售等级"]
    # df_sku_listing_site_test = df_sku_listing_site_test[need_columns]
    # change_columns = ["sku", "spu", "warehouse", "最大库存子仓ID", "最大库存子仓", "最近有库存日期", "当前库存", "成本",
    #                   "子仓库存明细", "开发来源", "超库龄等级", "销售状态",
    #                   "仓库区域", "仓库所在国家", "new_sku（主SKU）", "责任人", "amazon_listing_num", "amazon_DE",
    #                   "amazon_FR", "amazon_ES",
    #                   "amazon_IT", "amazon_BR", "amazon_MX", "ebay_listing_num", "ebay_DE", "ebay_FR",
    #                   "walmart_listing_num", "temu_listing_num", "allegro_listing_num","Amazon链接数量分段", "Amazon_DE链接数量分段",
    #                   "Amazon_FR链接数量分段", "Amazon_ES链接数量分段", "Amazon_IT链接数量分段", "ebay链接数量分段",
    #                   "ebay_DE链接数量分段", "ebay_FR链接数量分段",  "walmat链接数量分段",
    #                   "temu链接数量分段", "allegro链接数量分段",
    #                   "info_country", "侵权信息", "侵权等级", "forbid_plateform", "禁售信息", "禁售等级"]
    # df_sku_listing_site_test.columns = change_columns
    #
    # # 匹配日销
    # df_sales = get_sales()
    #
    # df = pd.merge(df_sku_listing_site_test, df_sales[['sku', 'warehouse', 'day_sales']],
    #               how='left', on=['sku', 'warehouse'])
    # daysales = df.pop('day_sales')

    # df.insert(df.columns.get_loc('超库龄等级') + 1, '日销', daysales)
    date_today = time.strftime('%m%d')
    save_df(df, f'YM-YB海外仓SKU各平台刊登情况{date_today}', file_type='xlsx')
    print('done!')
    # df_sku_listing_site_test.to_excel('df_sku_listing_site.xlsx', index=0)

def main_ym_cj():
    """
    合并信息
    """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    # df_sku_listing_new = get_sku_info()
    # df_sku_listing_new = df_sku_listing_new.sample(1)
    df_sku_listing_new = get_ym_sku()
    # df_sku_listing_new = pd.concat([df_sku_listing_new, df_ym])
    df_sku_listing_new['site'] = df_sku_listing_new['site'].apply(lambda x: x.lower())
    df_sku_listing_new['new_sku'] = df_sku_listing_new['sku']
    df_sku_listing_new['warehouse'] = df_sku_listing_new['site'].replace({'us':'美国仓','de':'德国仓'})
    df_sku_listing_new['area'] = df_sku_listing_new['site'].replace({'us': '美国', 'de': '欧洲'})
    print(df_sku_listing_new.info())

    # df_sku_listing_new = df_sku_listing_new.sample(2000)

    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    df_amazon_listing_temp_2 = get_cj_amazon_listing_num(sku_list, df_site)

    # 按 sku + site 合并
    df = pd.merge(df_sku_listing_new,
                                   df_amazon_listing_temp_2[['new_sku', 'amazon_listing_num', 'site']],
                                   how='left', on=['new_sku', 'site'])

    date_today = time.strftime('%m%d')
    save_df(df, f'YM-CJ海外仓SKU各平台刊登情况{date_today}', file_type='xlsx')
    print('done!')
    # df_sku_listing_site_test.to_excel('df_sku_listing_site.xlsx', index=0)

def get_ym_sku():
    """ 供应商货盘sku """
    df = pd.read_excel('F://Ding_workspace//供应商货盘SKU链接数量统计-20260324-给Roger.xlsx', dtype={'公司SKU':str})
    # df = pd.read_excel('F://Ding_workspace//海兔SKU链接数据更新-20251016.xlsx', dtype={'公司SKU': str})
    # df_1 = df[['sku', '仓库所在国家']]
    df_1 = df[['公司SKU','仓库所在国家']]
    df_1.columns = ['sku','site']
    df_1['develop_source_name'] = '供应商货盘_YMSKU'
    # df_2 = df[['YB-SKU','仓库所在国家']]
    # df_2.columns = ['sku', 'site']
    # df_2['develop_source_name'] = '供应商货盘_YBSKU'

    # df = pd.concat([df_1, df_2])
    df = df_1[~df_1['sku'].isna()]

    return df



def ebay_temp():
    print( "===Ebay刊登链接数据===")
    df_sku_listing_new = get_sku_info()
    sku_list = df_sku_listing_new['sku'].to_list()
    df_site = site_table()

    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay_listing_all = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku = sku_list[n:n+5000]
        sql = f"""
            with  account_list as (
                select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
                where platform_code='EB' and status=1 and is_del=0
            )
            SELECT 
                a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
                CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
                b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
                d.account_id, a.start_price,
                f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
            FROM (
                SELECT item_id,sku,start_price 
                FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
            ) a 
            INNER JOIN (
                SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
                from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
                where  warehouse_category_id !=1 and listing_status = 1 
                and account_id in (select distinct id from account_list) and sku in ({sku})
            ) b  ON a.item_id=b.item_id
            LEFT JOIN (
                SELECT item_id, shipping_service_cost
                FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
                WHERE shipping_status=1 and shipping_service_priority=1
            ) f  ON a.item_id = f.item_id
            LEFT JOIN (
                SELECT site_id,site,site1 AS `站点`,is_open,site_code 
                FROM domestic_warehouse_clear.yibai_site_table_ebay 
                where is_open='是'
            ) c  ON b.siteid = c.site_id
            LEFT JOIN account_list d on b.account_id = d.id
            INNER JOIN (
                SELECT id, warehouse
                FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
                WHERE warehouse_type_id in (2, 3)
            ) e ON b.warehouse_category_id = e.id

        """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        df_ebay_listing = conn_ck.ck_select_to_df(sql)
        df_ebay_listing.columns = [i.split('.')[-1] for i in df_ebay_listing.columns]
        df_ebay_listing['DATE'] = time.strftime('%Y-%m-%d')
        df_ebay_listing_all = pd.concat([df_ebay_listing, df_ebay_listing_all])

    df_ebay_listing_all['item_id'] = df_ebay_listing_all['item_id'].astype(str)
    print(df_ebay_listing_all.info())
    print(df_ebay_listing_all[df_ebay_listing_all['sku'] == '2611220060911'].head(10))
    # # 按 SKU + country 聚合计算 item的数量
    # df_ebay_listing_all = df_ebay_listing_all.drop_duplicates(subset=['account_id', 'item_id', 'sku'])
    # df_ebay_listing_temp = df_ebay_listing_all.groupby(['sku', 'country'])['item_id'].count().reset_index()
    # df_ebay_listing_temp.rename(columns={'item_id': 'ebay_listing_num', 'country': 'site'}, inplace=True)
    # df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].str.lower()
    # df_ebay_listing_temp['site'] = df_ebay_listing_temp['site'].replace('motor','us')
    # df_ebay_listing_temp = pd.merge(df_ebay_listing_temp, df_site[['site', 'area']], how='left', on='site')

    df_group = df_ebay_listing_all.groupby(['name'])['sku'].count().reset_index()
    print(df_group)
    df_group.to_excel('F://Desktop//df_eaby_listing_temp_2.xlsx', index=0)

def ebay_temp_2():
    sql = f"""
        with  account_list as (
            select distinct id,account_id from yibai_sale_center_system_sync.yibai_system_account
            where platform_code='EB' and status=1 and is_del=0
        )
        SELECT name, count(1)
        FROM (
        SELECT 
            a.item_id, a.sku, '' system_sku, b.sell_sku, b.siteid as site, c.site_code country,
            CASE WHEN e.warehouse in ('捷克仓','德仓（捷克仓Neumark）') THEN '德国仓' ELSE e.warehouse end AS name,
            b.seller_work_no seller_user, b.product_line_id product_line, b.listing_status,
            d.account_id, a.start_price,
            f.shipping_service_cost shipping_fee, a.start_price+f.shipping_service_cost as online_price
        FROM (
            SELECT item_id,sku,start_price 
            FROM yibai_sale_center_listing_sync.yibai_ebay_online_listing_sales_sku 
        ) a 
        INNER JOIN (
            SELECT account_id,warehouse_category_id,siteid,item_id,sell_sku,seller_work_no,product_line_id,listing_status
            from yibai_sale_center_listing_sync.yibai_ebay_online_listing 
            where  warehouse_category_id !=1 and listing_status = 1 
            and account_id in (select distinct id from account_list) 
        ) b  ON a.item_id=b.item_id
        LEFT JOIN (
            SELECT item_id, shipping_service_cost
            FROM yibai_sale_center_listing_sync.yibai_ebay_item_shipping
            WHERE shipping_status=1 and shipping_service_priority=1
        ) f  ON a.item_id = f.item_id
        LEFT JOIN (
            SELECT site_id,site,site1 AS `站点`,is_open,site_code 
            FROM domestic_warehouse_clear.yibai_site_table_ebay 
            where is_open='是'
        ) c  ON b.siteid = c.site_id
        LEFT JOIN account_list d on b.account_id = d.id
        INNER JOIN (
            SELECT id, warehouse
            FROM yibai_sale_center_common_sync.yibai_common_big_warehouse
            WHERE warehouse_type_id in (2, 3)
        ) e ON b.warehouse_category_id = e.id
    ) a
    GROUP BY name
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_ebay_listing = conn_ck.ck_select_to_df(sql)
    print(df_ebay_listing)

# 获取链接利润率分布
def get_listing_rate():
    """ 链接利润率分段 """

    df_sku = get_sku_info()
    df_sku['is_tt_sku'] = df_sku['is_tt_sku'].fillna('泛品铺货')

    col = ['sku','warehouse','overage_level','available_stock', 'new_price']
    df_temp = df_sku[df_sku['is_tt_sku'].str.contains('转泛品')]
    df_temp = df_temp[col]
    # df_sku = df_sku[df_sku['is_tt_sku'].str.contains('转泛品')]

    # # amazon
    # sql = """
    #     SELECT sku, warehouse, account_id, group_name, seller_sku, new_price, total_cost, ppve,
    #     platform_zero, platform_must_percent, online_price, rate
    #     FROM yibai_oversea.oversea_amazon_listing_all
    #     WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_amazon_listing_all where date_id > '2025-04-10')
    #     and best_warehouse_name like '%精铺%'
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    #
    # df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
    #         df['online_price'] * df['rate']) - df['platform_zero']
    #
    # df['online_profit_section'] = pd.cut(df['online_profit'], bins=[-1, -0.2, -0.1, 0, 0.1, 0.2, 1000],
    #                         labels=['A:(-1,-0.2]', 'B:(-0.2,-0.1]', 'C:(-0.1,0]', 'D:(0,0.1]', 'E:(0.1,0.2]', 'F:(0.2,+)'])
    #
    # df_amazon = pd.pivot_table(df, values='account_id', index=['sku', 'warehouse'],
    #                             columns=['online_profit_section'], aggfunc='count', fill_value=0, margins=True)
    #
    # df_result = pd.merge(df, df_sku[['sku','warehouse','is_tt_sku','total_available_stock']],
    #                   how='left', on=['sku','warehouse'])
    #
    # # ebay
    # sql = """
    #     SELECT sku, warehouse, account_id, item_id, new_price, total_cost, ppve,
    #     platform_zero, platform_must_percent, online_price, rate
    #     FROM yibai_oversea.oversea_ebay_listing_all
    #     WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_ebay_listing_all where date_id > '2025-04-10')
    #     and best_warehouse_name like '%精铺%'
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df = conn_ck.ck_select_to_df(sql)
    #
    # df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
    #         df['online_price'] * df['rate']) - df['platform_zero']
    #
    # df['online_profit_section'] = pd.cut(df['online_profit'], bins=[-1, -0.2, -0.1, 0, 0.1, 0.2, 1000],
    #                         labels=['A:(-1,-0.2]', 'B:(-0.2,-0.1]', 'C:(-0.1,0]', 'D:(0,0.1]', 'E:(0.1,0.2]', 'F:(0.2,+)'])
    #
    # df_result = pd.merge(df, df_sku[['sku','warehouse','is_tt_sku','total_available_stock']],
    #                   how='left', on=['sku','warehouse'])

    df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)

def get_order_info():
    """ 获取sku+大仓的销售信息 """
    date_start = '2026-02-01'
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
    # 开发来源
    df = get_line(df)
    print(df.info())
    df.drop(['一级产品线','二级产品线','new_price'], axis=1, inplace=True)
    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    c1 = df['sku'].isin(earphones_list)
    c2 = df['develop_source_name'].str.contains('转泛品|VC|易通兔')
    df['develop_source_name'] = np.where(c1, '耳机专项', df['develop_source_name'])
    df = df[c1 | c2]

    df_account = get_account()
    df_age = get_fine_stock_age()

    df = pd.merge(df, df_account[['account_id', 'platform_code','is_yibai']], how='left', on=['account_id', 'platform_code'])
    df = pd.merge(df, df_age[['sku', 'warehouse','overage_level']], how='left', on=['sku', 'warehouse'])



    # df = df.groupby(['sku','warehouse','paytime']).agg({'total_price':'sum','release_money':'sum',
    #                                                     'quantity':'sum','true_profit_new1':'sum'})
    # df = df.reset_index()
    df.to_excel('F://Desktop//df_jp_order.xlsx', index=0)

def get_order_info_2():
    """
    1. 转泛品SKU的库存、刊登情况（亚马逊+ebay）、销库+销量+订单利润率（日维度）
    2. '存在超180天'且'日销<0.1'的'SKU&国家'，刊登情况、可售天数、每日销量+销库+利润率情况
    3. Temu平台正负加快动销SKU中，近7天出单的订单利润率高于X%的SKU明细和责任团队；针对'存在超180天'且'日销<0.1'的'SKU&国家'，Temu活动价高于X%呃SKU明细和责任团队
    """
    sql = """
        SELECT
            A.sku sku, A.warehouse warehouse, overage_level,day_sales, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        FROM dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE 
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
            and overage_level >= 180 and day_sales <= 0.1
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)

    sku_list = tuple(df_sku['sku'].unique())

    date_start = '2025-01-01'
    sql = f"""
    SELECT
        order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
        warehouse_name, warehouse, quantity, release_money, sales_status
    FROM yibai_oversea.dashbord_new_data1
    WHERE 
        paytime >= '{date_start}'
        and `total_price` > 0 
        and `sales_status` not in ('','nan','总计')
        and sku in {sku_list}
        -- and platform_code in ('AMAZON','EB')
    """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn.read_sql(sql)
    col = ['total_price','release_money','true_profit_new1']
    df[col] = df[col].astype(float)

    df = pd.merge(df, df_sku, how='left', on=['sku','warehouse'])
    df.to_excel('F://Desktop//df_clear_sku.xlsx', index=0)


def get_temu_listing_price():
    """ temu链接活动价利润率 """
    utils.program_name = '海外仓SKU各平台刊登情况'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT account_id, product_sku_id, sku, online_status, site_code, country, warehouse,
        best_warehouse_name, available_stock,  overage_level, day_sales, supplier_price, sales_status,
        online_profit_rate
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = '{date_today}'
        and overage_level >=180 and day_sales <= 0.1
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # 账号责任人
    df_account = get_temu_account()

    df = pd.merge(df, df_account[['account_id','main_name']], how='left', on='account_id')

    # 获取责任人
    df['new_sku'] = df['sku']
    df = get_line(df)
    df['主体账号'] = ' '
    dic = {'德国仓': '欧洲', '澳洲仓': '澳大利亚', '美国仓': '美国', '英国仓': '英国',
           '加拿大仓': '加拿大', '墨西哥仓': '墨西哥', '法国仓': '欧洲'}
    df['站点'] = df['warehouse'].replace(dic)
    print(df['站点'].unique())
    df = get_main_resp(df)

    df['online_profit_section'] = pd.cut(df['online_profit_rate'], bins=[-1, -0.2, -0.1, 0, 0.1, 0.2, 1000],
                                         labels=['A:(-1,-0.2]', 'B:(-0.2,-0.1]', 'C:(-0.1,0]', 'D:(0,0.1]', 'E:(0.1,0.2]', 'F:(0.2,+)'])

    col1 = df.pop('online_profit_section')
    df.insert(df.columns.get_loc('online_profit_rate') + 1, 'online_profit_section', col1)
    col1 = df.pop('new_price')
    df.insert(df.columns.get_loc('available_stock') + 1, 'new_price', col1)

    df.drop(['主体账号','new_sku', 'is_same'], axis=1, inplace=True)
    df = df.rename(columns={'is_tt_sku':'开发来源', 'main_name':'主体责任人'})
    save_df(df, '海外仓超180天且低销sku在TEMU链接利润率', file_type='xlsx')

def get_sales():
    sql = """
        SELECT
            A.sku sku, A.warehouse warehouse, overage_level,day_sales, IF(D.sale_status IS NULL ,'正常', D.sale_status) as '销售状态'
        FROM dwm_sku_temp_info A
        LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
        WHERE 
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    # df_sku.to_excel('F://Desktop//df_sku_sales.xlsx', index=0)
    return df_sku

def get_fine_orders():
    """ 转泛品销库数据 """
    date_start = '2025-07-01'
    sql = f"""
        SELECT
            order_id, pay_status,refound_status,platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    # conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)

    df_temp = get_sku(df_temp)
    # df_temp = pd.merge(df_temp, df_sku, how='inner', on=['sku'])
    df_age = get_fine_stock_age()

    df = pd.merge(df_temp, df_age[['sku','warehouse','overage_level']], how='left', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_fine_orders.xlsx', index=0)

def get_account():
    """ 获取账号是否泛品 """
    sql = """
        SELECT id, account_id, platform_code, is_yibai
        FROM yibai_sale_center_system_sync.yibai_system_account
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_system_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # df.to_excel('F://Desktop//df_account.xlsx', index=0)

    return df

def get_fine_stock_age():
    """ 在途库存 """

    sql = """
        SELECT sku, type, warehouse, available_stock, available_stock_money, overage_level,day_sales,new_price
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-08-19' 
        AND available_stock > 0
        -- (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-05-15')
        
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    c1 = df['sku'].isin(earphones_list)
    c2 = df['type'].str.contains('转泛品|VC|易通兔')
    df = df[c1 | c2]

    # df.to_excel('F://Desktop//fine_stock_age.xlsx', index=0)

    return df

def get_fba_fbm_order():
    """ 耳机专项的fba和fbm订单统计 """
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    sql = f"""
    SELECT order_id, 'AMAZON_fba' platform_code, sku, seller_sku, account_id, created_time paytime, `销售额` total_price,
    `毛利润-oms` true_profit_new1, `净利润-oms` real_profit, '' warehouse_name, `站点` warehouse, quantity, 
    `销库金额` release_money, '清仓' sales_status, 
    sku spu, '耳机专项' develop_source_name, 1 is_yibai, 180 overage_level, 'fba' `模式`
    from domestic_warehouse_clear.yibai_monitor_fba_order 
    where sku in {earphones_list} and created_time >= '2026-02-01'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    order = conn_ck.ck_select_to_df(sql)

    order.to_excel('F://Desktop//fba_order.xlsx', index=0)

    # fbm
    sql = f"""
    SELECT order_id, platform_code, sku, seller_sku, account_id, created_time paytime, `销售额` total_price,
    `毛利润` true_profit_new1, `净利润` real_profit, '' warehouse_name, `站点` warehouse, quantity, 
    `销库金额` release_money, '清仓' sales_status, 
    sku spu, '耳机专项' develop_source_name, 1 is_yibai, 180 overage_level, 'fbm' `模式`
    from domestic_warehouse_clear.monitor_dom_order 
    where sku in {earphones_list} and created_time >= '2026-02-01'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    order_fbm = conn_ck.ck_select_to_df(sql)
    order_fbm.to_excel('F://Desktop//fbm_order.xlsx', index=0)

def v_oversea_stock():
    """ """
    sql = """
    SELECT sku, warehouse_id, warehouse, warehouse_name, stock, available_stock
    FROM yb_datacenter.v_oversea_stock
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']

    sql = f"""
        SELECT a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线`, c.develop_source_name type,
        CASE
            when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price)
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price)
            when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost)
            when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
            else toFloat64(product_cost)
        END as `new_price`
        FROM yibai_prod_base_sync.yibai_prod_sku a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_category b on toInt32(a.product_category_id) = toInt32(b.id)
        LEFT JOIN yibai_prod_base_sync.yibai_prod_develop_source c ON a.develop_source = c.id
        WHERE c.develop_source_name in ('海兔转泛品','易通兔') or c.develop_source_name like '%%转泛品%%'
        or sku in {earphones_list}
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_line, how='inner', on='sku')
    df['可用库存金额'] = df['available_stock'] * df['new_price']
    df['在库库存金额'] = df['stock'] * df['new_price']

    c1 = df['sku'].isin(earphones_list)
    df['type'] = np.where(c1, '耳机专项', df['type'])

    # 日销
    sql = """
        SELECT sku, warehouse, day_sales
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-09-01')
        -- AND available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    df = pd.merge(df, df_sales, how='left', on=['sku','warehouse'])

    df.to_excel('F://Desktop//df_v.xlsx', index=0)

def get_fba_fbm_stock():
    """ 耳机专项中fba和fbm的库存数据 """
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']

    date_id = time.strftime('%Y%m%d')
    date_id = '20260402'
    sql = f"""
    SELECT '{date_id}' `日期`,account_id ,seller_sku ,afn_fulfillable_quantity `可售库存`,`超90天库龄`
    FROM yibai_price_fba.yibai_amazon_fba_inventory_age_self_calculated_{date_id} 
    """
    conn_ck = pd_to_ck(database='yibai_price_fba', data_sys='调价明细历史数据')
    fba_stock = conn_ck.ck_select_to_df(sql)
    sql = f"""
        select account_id ,seller_sku, sku,cost
        from yibai_fba.fba_fees where sku in {earphones_list}
    """
    conn_ck = pd_to_ck(database='yibai_fba', data_sys='调价明细历史数据')
    fba_sku = conn_ck.ck_select_to_df(sql)

    fba = pd.merge(fba_stock, fba_sku, how='inner', on=['account_id','seller_sku'])
    fba['可售库存金额'] = fba['可售库存'] * fba['cost']
    print(fba.info())

    fba.to_excel('F://Desktop//fba_stock.xlsx', index=0)

    # date_id = '20251222'
    # fbm
    sql = f"""
     SELECT '{date_id}' `日期`,sku,cost,Total_inventory_quantity `总库存`,(cost*Total_inventory_quantity) `库存金额` ,
     Total_inventory_quantity-inventory_of_age_over_180_days `0-180天库存`,
     inventory_of_age_over_180_days- inventory_of_age_over_270_days `180-270天库存`,
     inventory_of_age_over_270_days-inventory_of_age_over_360_days `270-360天库存`,inventory_of_age_over_360_days `超360天库存`  
     from support_document.domestic_warehouse_clear_sku_{date_id} 
     where sku in {earphones_list}
    """
    fbm_stock = conn_ck.ck_select_to_df(sql)

    fbm_stock.to_excel('F://Desktop//fbm_stock.xlsx', index=0)


if __name__ == '__main__':

    # main()
    main_ym()
    main_ym_cj()
    # get_sales()
    # get_order_info() # 转泛品清仓订单
    # v_oversea_stock() # 转泛品库存统计
    # get_fba_fbm_order() # 耳机清仓品fba
    # get_fba_fbm_stock() # 耳机专项fba、fbm库存（需要改日期）

    # get_temu_listing_price()
    # get_temu_listing_num()
    # get_lazada_listing_num_temp()
    # get_fine_stock_age()

    # main(stock_date='old')
    # temu_group()
    # temp()
    # get_shopee_listing_num()

    # df_account = get_temu_account()

    # df = get_shopee_listing_num()
    # df = get_lazada_listing_num()
    # get_temu_listing_num()
    # site_table()
    # get_temu_listing_num()