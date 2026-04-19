# 海外仓调价数据对接各平台海外仓链接

##
import time, datetime
import pandas as pd
import numpy as np
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from requests.auth import HTTPBasicAuth
import requests
import tqdm
from all_auto_task.oversea_amazon_asin_fba import amazon_fba_listing
from all_auto_task.oversea_add_logic import get_bundle_sku
from all_auto_task.oversea_price_adjust_2023 import dwm_oversea_bundle
##
def get_amazon_listing_data():
    now = time.strftime('%Y-%m-%d')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT *
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing 
        where date_id = '{now}'
        -- and deliver_mode = 2
    """
    df_amazon_listing = conn_ck.ck_select_to_df(sql)
    print(len(df_amazon_listing))
    # 如果yibai_amazon_oversea_listing_price中没有数据，从CK库重新读取Amazon的链接
    if len(df_amazon_listing) == 0:
        sql = f"""select distinct sku from dwm_sku_temp_info WHERE date_id>='{now}'"""
        df_sku = conn.read_sql(sql)

        sql = """ 
        with listing_table as (
            select distinct account_id, seller_sku 
            from yibai_product_kd_sync.yibai_amazon_sku_map 
            where deliver_mode=2
        )

        select b.account_id as account_id, b.account_name as account_name, 
            group_name, short_name, 'AMAZON' as platfrom,
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
            where deliver_mode=2 
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
            where account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163
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
        ck_client = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
        df_amazon_listing = ck_client.ck_select_to_df(sql)
        # 20230626 处理map表中SKU字段带*的问题
        # 20240116 捆绑sku单独进行调价
        # df_amazon_listing['sku'] = df_amazon_listing['sku'].str.split('*').str[0]
        df_amazon_listing = df_amazon_listing.merge(df_sku, on=['sku'])

    df_amazon_listing['site'] = df_amazon_listing['site'].str.upper()
    df_amazon_listing = df_amazon_listing.rename(columns={'site': 'country'})
    return df_amazon_listing
# 获取调价数据
def get_price_data(conn, platform='AMAZON'):
    now = time.strftime('%Y-%m-%d')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT 
            sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level, available_stock, sales_status,price,
            target_profit_rate,is_adjust, day_sales, country, ship_name, lowest_price,is_supplier_price,is_supplier,
            ppve, platform_zero, platform_must_percent
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE platform = '{platform}' and date_id = '{now}'
    """
    # df_price_dtl = conn.read_sql(sql)
    df_price_dtl = conn_ck.ck_select_to_df(sql)
    c1 = (df_price_dtl['warehouse'] == '英国仓') & (df_price_dtl['country'] != 'UK')
    c2 = (df_price_dtl['warehouse'] != '英国仓') & (df_price_dtl['country'] == 'UK')
    df_price_dtl = df_price_dtl[~(c1 | c2)]
    # df_price_dtl = df_price_dtl.rename(columns={'SKU':'sku','country':'site'})
    # 20240603 amazon平台需要lowest_price字段
    # if platform not in ('AMAZON', 'ALLEGRO','SHOPEE','LAZADA'):
    #     df_price_dtl.drop('lowest_price', axis=1, inplace=True)

    return df_price_dtl


# coupon和promotion数据
# 沿用原调价逻辑代码
def get_amazon_coupon(ck_client):
    today = time.strftime('%Y-%m-%d')
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yibai_oversea')
    sql = f"""
        with table1 as (
            select toInt32(id) from yibai_system_kd_sync.yibai_amazon_account 
            where site ='us'
        )
        SELECT account_id,asin,coupon_type,percentage_off,money_off 
        FROM yibai_product_kd_sync.yibai_amazon_coupon 
        where coupon_status in (8, 11) and start_date<='{today} 00:00:00' and end_date>=DATE_ADD(day, -2, now()) 
        and is_delete=0 and disable in (0, 5) 
        order by start_date desc limit 1 by account_id,asin 
        union all 
        -- 20230227 增加销售中台数据
        select erp_id as account_id,asin,coupon_type,percentage_off,money_off 
        from yibai_sale_center_operatemanage_sync.yibai_amazon_coupon 
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
    ck_client = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
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


# promotion
def get_amazon_promotion():
    """
    获取Amazon的促销数据
    """
    ck_client = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    sql = """
    SELECT account_id, seller_sku, promotion_percent,promotion_amount,promotion_source
    FROM yibai_fba.yibai_amazon_promotion
    """
    df_amazon_promotion = ck_client.ck_select_to_df(sql)
    if len(df_amazon_promotion) == 0:
        df_amazon_promotion = pd.DataFrame(
            columns=['account_id','seller_sku','promotion_percent','promotion_amount','promotion_source'])

    print(df_amazon_promotion.info())

    return df_amazon_promotion

# 4、白名单链接获取
def get_white_listing(conn):
    now = time.strftime('%Y-%m-%d')
    sql = """
    SELECT sku, warehouse
    FROM yibai_oversea_no_adjust_sku
    
    """
    df_white_sku = conn.read_sql(sql)

    sql = f"""
    SELECT account_id, seller_sku, 1 as is_white_listing
    FROM yibai_amazon_adjustprice_filter_sku
    WHERE start_time <= '{now}' and end_time >= '{now}' and `status` = 1
    """
    df_white_listing = conn.read_sql(sql)
    df_white_listing = df_white_listing.drop_duplicates()

    return df_white_sku, df_white_listing

def get_oa_dep():
    def create_attr_token():
        token_url = "http://oauth.java.yibainetwork.com/oauth/" \
                    "token?grant_type=client_credentials"
        # 获取token
        token_request = requests.post(token_url, auth=HTTPBasicAuth("prod_libo", "libo321"))
        token_dict_content = token_request.json(strict=True)
        now_token = token_dict_content['access_token']
        return now_token

    header = {'Content-Type': "application/json;charset=UTF-8"}
    base_url = f"http://rest.java.yibainetwork.com/oa/oaDepartment/getOaDepartment"
    # 合成url
    now_token = create_attr_token()
    url = f"{base_url}?access_token={now_token}"
    response = requests.post(url, json={'isDel': 0}, headers=header)
    response_result = response.json(strict=True)
    if "error_description" in response_result.keys():
        err_reason = response_result['error_description']
        if "Access token expired" in err_reason:
            now_token = create_attr_token()
            url = f"{base_url}?access_token={now_token}"
            response = requests.post(url, json={'isDel': 0}, headers=header)
            response_result = response.json(strict=True)
    return response_result

def get_top_dep():
    # 1085984-为最大层级, 54495400-为销售团队, 30046131-Amazon部
    total_data = (get_oa_dep())['data']
    # amazon_sale_dep_list = ["产品线团队", "海外仓团队", "精品团队", "自发货团队"]
    amazon_sale_dep_id = [1079248, 1079249, 1079250, 1079251]
    dep_child_list, dep_sale_df = [], pd.DataFrame()
    for one_top_dep in total_data:
        if one_top_dep["userNumberDetail"] != "" and one_top_dep['pid'] in amazon_sale_dep_id:
            now_dep = one_top_dep['name']
            short_dep = now_dep.split("部")[0] + "部" if "部" in now_dep \
                else now_dep.split("仓")[0] + "仓" if "仓" in now_dep else ""
            update_time = one_top_dep['updateTime']
            now_child_dep_list = list(map(
                lambda m_x: m_x.split("(")[0].strip() if "(" in m_x else m_x.split("（")[0].strip(),
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            # 当1个小组同时在两个大部时，优先取组中有括号的
            now_child_dep_list1 = list(map(
                lambda m_x: 1 if "(" in m_x or "（" in m_x else 2,
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            dep_child_list.append(
                pd.DataFrame({'dep_name': [short_dep] * len(now_child_dep_list), 'sale_group_name': now_child_dep_list,
                              'update_time': [update_time] * len(now_child_dep_list),
                              'sale_group_name1': now_child_dep_list1, })
            )
    dep_sale_df = dep_sale_df.append(dep_child_list, sort=False)
    return dep_sale_df

# 匹配大部
def xiaozu_dabu():
    df = get_top_dep()
    df = df.sort_values(['sale_group_name1'], ascending=True)
    df = df.drop_duplicates(subset=['sale_group_name'], keep='first')
    df = df[['dep_name', 'sale_group_name']].drop_duplicates()
    df.rename(columns={'dep_name': '大部', 'sale_group_name': 'group_name'}, inplace=True)
    df = df.drop_duplicates()

    df1 = df[df['group_name'] == '深圳3组']
    if df1.shape[0] == 0:
        df1 = pd.DataFrame({'group_name': ['深圳3组'],
                            '大部': ['深圳产品线一部']})
        df = df.append(df1)
    return df

# 6、剔除同大部、同站点、同asin下有FBA链接的FBM。复用原程序
# yibai_oversea_amazon_asin_fba 该表在另一个py文件中更新：oversea_amazon_asin_fba
def get_fba_asin(df_log, conn):
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
    select distinct account_id, asin, '需去除' `需去除`
    from yibai_oversea.yibai_oversea_amazon_asin_fba 
    where date_id >= formatDateTime(today() - INTERVAL 3 DAY, '%Y-%m-%d') and status = '1'
    """
    asin = conn_ck.ck_select_to_df(sql)
    asin['account_id'] = asin['account_id'].astype('int')
    asin = asin.drop_duplicates(subset=['account_id', 'asin'])

    sql = """
    select a.id as account_id,b.group_name,
        case when a.site='sp' then 'es' else a.site end as country
    from yibai_system_kd_sync.yibai_amazon_account a
    left join yibai_system_kd_sync.yibai_amazon_group b 
    on a.group_id=b.group_id
    """
    ck_client = pd_to_ck(database='yibai_system_kd_sync', data_sys='调价明细历史数据')
    group = ck_client.ck_select_to_df(sql)

    asin1 = asin.merge(group, on=['account_id'], how='left')

    del asin1['account_id']
    # 20220517黄星星告诉FBA对应的FBM已改为同大部，同站点，同asin下有FBA链接的FBM
    dabu = xiaozu_dabu()
    dabu = dabu[['group_name', '大部']]
    asin1 = asin1.merge(dabu, on=['group_name'], how='left')
    asin1.loc[asin1['大部'].isnull(), '大部'] = asin1['group_name']
    asin1 = asin1[['大部', 'country', 'asin', '需去除']]
    df_log = df_log.merge(dabu, on=['group_name'], how='left')
    df_log.loc[df_log['大部'].isnull(), '大部'] = df_log['group_name']
    # 20220517 去除同大部，同站点下，同asin有FBA链接的FBM
    df_log['country'] = df_log['country'].str.upper()
    asin1['country'] = asin1['country'].str.upper()
    df_log = df_log.merge(asin1, on=['大部', 'country', 'asin'], how='left')
    # 同大部FBA的asin
    df_log = df_log.rename(columns={'需去除':'is_fba_asin'})
    df_log['is_fba_asin'] = np.where(df_log['is_fba_asin'].isnull(), 0, 1)

    # 删除大部和需去除列
    df_log.drop(['大部'], axis=1, inplace=True)

    return df_log

# 汇率
def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate = df_rate.drop_duplicates(subset=['country'])
    return df_rate

# 平台配置表
def get_platform_fee(platform='AMAZON'):
    """
    获取配置表：平台费率、差值等
    """
    sql = f"""
    SELECT 
        site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    WHERE platform = '{platform}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    return df


def get_listing_day_sales():
    """
    获取链接维度的日销数据
    """
    now = datetime.datetime.now()
    date_7 = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    date_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')

    # 近30天销量，近7天销量
    sql = f"""

        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, warehouse,
            quantity, release_money, sales_status
        FROM yibai_oversea.dashbord_new1
        WHERE 
            paytime >= '{date_30}' and paytime < '{now}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
    """
    conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # ck_client = CkClient(user='gaoyuzhou', password='3gao3Yu45ZhO3u', host='121.37.248.212', port='9003',
    #                      db_name='over_sea')
    df_order_info = conn_mx.ck_select_to_df(sql)
    #
    # df_order_info.to_excel('df_order_info.xlsx', index=0)
    # 30天销量数据
    df_amazon = df_order_info[df_order_info['platform_code'] == 'AMAZON'].groupby(['seller_sku', 'account_id'])[
        'quantity'].sum().rename('30_days_sales').reset_index()
    # 7天销量数据
    df_amazon_7 = df_order_info[(df_order_info['platform_code'] == 'AMAZON') & (df_order_info['paytime'] >= date_7)]
    df_amazon_7 = df_amazon_7.groupby(['seller_sku', 'account_id'])['quantity'].sum().rename(
        '7_days_sales').reset_index()
    df_amazon = pd.merge(df_amazon, df_amazon_7, how='left', on=['seller_sku', 'account_id'])
    #
    df_amazon['7_days_sales'] = df_amazon['7_days_sales'].fillna(0).astype(int)
    df_amazon['listing_day_sales'] = 0.9 * df_amazon['7_days_sales'] / 7 + 0.1 * df_amazon['30_days_sales'] / 30
    df_amazon['listing_day_sales'] = df_amazon['listing_day_sales'].astype(float).round(4)
    df_amazon = df_amazon[['seller_sku', 'account_id', 'listing_day_sales']]

    return df_amazon

def get_listing_day_sales_new(platform='AMAZON'):
    """
    获取链接维度的日销数据
    """
    # 近30天销量，近7天销量
    if platform == 'AMAZON':
        sql = f"""
            SELECT
                account_id, seller_sku, `3days_sales`, `7days_sales`,`15days_sales`,`30days_sales`
            FROM yibai_domestic.sales_amazon
            WHERE 
                dt = (SELECT max(dt) FROM yibai_domestic.sales_amazon)
                and fulfillment_channel = 'DEF' 
        """
        conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
        df = conn_mx.ck_select_to_df(sql)
    elif platform == 'EB':
        sql = f"""
            SELECT
                sku account_id, item_id seller_sku, `7天销量` `7days_sales`, `15天销量` `15days_sales`,
                `30天销量` `30days_sales`
            FROM yibai_price_dom.yibai_ebay_120_days_sales_merge
            WHERE  toDate(move_date) = (select max(toDate(move_date)) from yibai_price_dom.yibai_ebay_120_days_sales_merge)
        """
        conn_mx = pd_to_ck(database='yibai_price_dom', data_sys='调价明细历史数据')
        df = conn_mx.ck_select_to_df(sql)
    elif platform == 'WALMART':
        sql = f"""
            SELECT
                account_id, seller_sku, `3days_sales`, `7days_sales`,`15days_sales`,`30days_sales`
            FROM yibai_domestic.sales_walmart
            WHERE 
                dt = (SELECT max(dt) FROM yibai_domestic.sales_walmart)
        """
        conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
        df = conn_mx.ck_select_to_df(sql)
    elif platform == 'CDISCOUNT':
        sql = f"""
            SELECT
                account_id, seller_sku, `7天销量` `7days_sales`, `15天销量` `15days_sales`,
                `30天销量` `30days_sales`
            FROM yibai_price_dom.yibai_cd_120_days_sales_merge
            WHERE  toDate(move_date) = (select max(toDate(move_date)) from yibai_price_dom.yibai_cd_120_days_sales_merge)
        """
        conn_mx = pd_to_ck(database='yibai_price_dom', data_sys='调价明细历史数据')
        df = conn_mx.ck_select_to_df(sql)
    elif platform == 'ALLEGRO':
        sql = f"""
            SELECT
                account_id, offer_id seller_sku, `3days_sales`, `7days_sales`,`15days_sales`,`30days_sales`
            FROM yibai_domestic.sales_allegro
            WHERE 
                dt = (SELECT max(dt) FROM yibai_domestic.sales_allegro)
        """
        conn_mx = pd_to_ck(database='yibai_price_dom', data_sys='调价明细历史数据')
        df = conn_mx.ck_select_to_df(sql)

    col = ['7days_sales','15days_sales','30days_sales']
    df[col] = df[col].astype(float)
    df['listing_day_sales'] = 0.7 * df['7days_sales'] / 7 + \
                                     0.2 * df['15days_sales'] / 15 + \
                                     0.1 * df['30days_sales'] / 30
    df['listing_day_sales'] = df['listing_day_sales'].astype(float).round(4)
    df = df[['seller_sku', 'account_id', 'listing_day_sales']]
    print(df.info())
    # df_order_info.to_excel('F://Desktop//df_order_info.xlsx', index=0)

    return df

# amazon真实佣金率
# 分账号取数据
def get_amazon_commission():
    now = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT distinct account_id
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing 
        where date_id = '{now}'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)

    #
    step = 200
    account_tuple = tuple(df_account['account_id'].unique())

    df_re = pd.DataFrame()
    for i in range(int(len(account_tuple)/step)+1):
        account_id = account_tuple[i*step:(i+1)*step]
        sql = f"""
            SELECT account_id, seller_sku, `佣金率`
            FROM  yibai_fba.yibai_amazon_referral_fee
            WHERE `佣金率` != 0.15 and account_id in {account_id}
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        df_re = pd.concat([df_temp, df_re])

    df_re = df_re.drop_duplicates(subset=['account_id', 'seller_sku'])

    return df_re

def get_ebay_commission(df):

    conn_ck = pd_to_ck(database='yibai_domestic', data_sys='调价明细历史数据')
    sql = f"""
        SELECT DISTINCT item_id, sku, commission_rate
        FROM  yibai_domestic.yibai_ebay_commission
        WHERE commission_rate != 0.15
    """
    df_temp = conn_ck.ck_select_to_df(sql)
    if len(df_temp) == 0:
        df_temp = pd.DataFrame(columns=['item_id', 'sku', 'commission_rate'])

    df = pd.merge(df, df_temp, how='left', on=['item_id', 'sku'])
    df['commission_rate'] = df['commission_rate'].fillna(0.15).astype(float)
    # df.drop('commission_rate', axis=1, inplace=True)

    return df


def normal_sku_price_adjust(df):
    """
    正常品sku链接调价逻辑
    profit_diff = online_profit - target_profit_rate
    """
    col_name = 'target_profit_rate'
    # 降价进入条件
    c0 = (df['day_sales'] <= 0.3) | (df['listing_day_sales'] <= 0.1)
    # 涨降幅度控制
    c1 = ((df['online_profit'] - df['target_profit_rate']) > 0.03) & (df['sales_status'] == '正常')  & (df['available_stock'] > 0)
    c2 = ((df['online_profit'] - df['target_profit_rate']) < -0.05) & (df['sales_status'] == '正常') & (df['available_stock'] > 0)
    df[col_name] = np.where(c0 & c1, df['online_profit'] - 0.03, df['target_profit_rate'])
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


def haitu_sku_price():
    """
    海兔sku调价
    最高价不超过【成本+头程】的三折，按0净利率定价
    """
    # 取海兔sku、取运费、取平台费率、计算最高定价。返回不同平台最高定价
    sql = """
        SELECT sku, warehouse, new_price, best_warehouse_name, best_warehouse_id, available_stock, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-08-20')
        and type in ('海兔转泛品', '易通兔')    
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 取运费
    sql = """
    SELECT 
        sku, warehouseId as warehouse_id, warehouseName as best_warehouse_name, totalCost as total_cost, totalCost_origin,
        case when new_firstCarrierCost > 0 then new_firstCarrierCost else firstCarrierCost end as firstCarrierCost, 
        dutyCost,(totalCost_origin - firstCarrierCost - dutyCost) ship_fee,
        shipName as ship_name,lowest_price, platform, country
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH','ALI')
    and country = 'US'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)

    # 取费率
    sql = f"""
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform = conn.read_sql(sql)

    df = pd.merge(df, df_fee, how='left', on=['sku','best_warehouse_name'])
    df = pd.merge(df, df_platform, how='left', on=['platform','country'])

    col = ['new_price', 'firstCarrierCost','dutyCost','ppve','platform_zero']
    df[col] = df[col].fillna(0).astype('float')
    df['high_price'] = 0.3*(df['new_price']+df['firstCarrierCost']+df['dutyCost'])/(1-df['ppve']-df['platform_zero'])

    # df.to_excel('F://Desktop//df_haitu.xlsx', index=0)

    return df

# 链接价格涨降幅度控制函数
def control_listing_price(df, platform='AMAZON'):
    """
    listing维度价格单次涨降幅度控制：利润率幅度、价格幅度
    """
    # 非正常品
    # 1、利润率幅度控制
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    c1 = (df['profit_diff'] > 1) & (df['sales_status'] != '正常')
    c2 = (df['profit_diff'] < -1) & (df['sales_status'] != '正常') & (df['available_stock'] > 0) & (
                df['online_profit'] <= -0.3)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    df['target_profit_rate'] = np.where(c2, df['online_profit'] + 1, df['target_profit_rate'])

    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # 2、价格幅度控制
    df['normal_price'] = (df['new_price'] + df['total_cost']) / (
            1 - df['ppve'] - df['platform_zero'] - df['platform_must_percent']) / df['rate']
    diff = 0.2
    limit = df['normal_price'] * diff
    df['price'] = df['price'].where(
        df['sales_status'] == '正常',  # 条件为真时保留原值
        df['price'].clip(df['online_price'] - limit, df['online_price'] + limit)  # 条件为假时执行裁剪
    )
    df.drop('normal_price', axis=1, inplace=True)

    return df


def get_amazon_adjust_listing():
    print("开始运行时间：{}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
    # 获取amazon的FBA链接
    print('获取FBA链接...')
    amazon_fba_listing()
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # AMAZON链接、调价数据获取
    print('获取Amazon海外仓链接、当日调价数据...')
    df_amazon_listing = get_amazon_listing_data()
    # df_amazon_listing = df_amazon_listing.rename(columns={'platfrom':'platform'})
    df_price_dtl = get_price_data(conn,'AMAZON')
    # 合并链接与价格数据
    df_listing_price_dtl = pd.merge(df_amazon_listing, df_price_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 优先取有库存的仓库、再取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df_listing_price_dtl = df_listing_price_dtl.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df_listing_price_dtl.drop('overage_level', axis=1, inplace=True)
    print('调价数据已获取，共{}条listing。'.format(len(df_listing_price_dtl)))
    print('开始处理coupon及promotion数据...')

    # 亚马逊阶梯定价
    df_listing_price_dtl.sort_values(by=['sku', 'warehouse', 'country', 'price'], inplace=True)
    df_listing_price_dtl.reset_index(drop=True, inplace=True)
    df_listing_price_dtl['price'] = df_listing_price_dtl['price'] + df_listing_price_dtl.index.values % 30 * 0.01

    # 判断是否涨价
    df_listing_price_dtl['is_up'] = np.where(df_listing_price_dtl['price'] >= df_listing_price_dtl['online_price'],
                                             '涨价', '降价')

    site_name_dic = {'UK': '英国', 'US': '美国', 'DE': '德国', 'JP': '日本', 'PL': '波兰', 'FR': '法国', 'AU': '澳洲',
                     'NL': '荷兰', 'ES': '西班牙', 'IT': '意大利', 'SE': '瑞典', 'CA': '加拿大', 'MX': '墨西哥'}
    df_listing_price_dtl['站点'] = df_listing_price_dtl['country'].replace(site_name_dic)

    # coupon和promotion数据
    # 沿用原调价逻辑代码
    df_listing_price_dtl['account_id'] = df_listing_price_dtl['account_id'].astype("int")
    df_listing_price_dtl = amazon_coupon_fu(df_listing_price_dtl)

    df_amazon_promotion = get_amazon_promotion()
    df_listing_price_dtl = pd.merge(df_listing_price_dtl, df_amazon_promotion, how='left', on=['account_id', 'seller_sku'])
    df_listing_price_dtl['promotion_source'] = df_listing_price_dtl['promotion_source'].fillna(0).astype(float)
    # 日本站点积分
    # 日本站积分数据：your_price_point 当前基本为0, 暂时直接置为0
    df_listing_price_dtl['your_price_point'] = 0
    df_listing_price_dtl['no_coupon_price'] = df_listing_price_dtl['price']
    df_listing_price_dtl[['promotion_percent', 'promotion_amount']] = df_listing_price_dtl[['promotion_percent', 'promotion_amount']].fillna(0)

    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df_listing_price_dtl, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform_fee = get_platform_fee('AMAZON')
    # df = pd.merge(df, df_platform_fee[['country','refound_fee']], how='left', on=['country'])
    # 20240605 亚马逊暂时取消清仓，兜底净利率设置为0
    # 20240611 亚马逊除欧洲站继续清仓，其他仓兜底净利率设置为0
    # 20240703 亚马逊兜底净利率都上调至0净利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']

    # 20240603 接入真实佣金率
    df_com = get_amazon_commission()
    df['account_id'] = df['account_id'].astype(int)
    df = pd.merge(df, df_com, how='left', on=['account_id', 'seller_sku'])
    df['佣金率'] = df['佣金率'].fillna(0.15).astype(float)
    df['ppve'] = df['ppve'] - 0.15 + df['佣金率']

    # 4、供应商货盘sku定价的链接不重新计算价格
    df_2 = df[df['is_supplier_price']==1]
    df = df[df['is_supplier_price']!=1]

    # print(df.info())
    df['price'] = (df['new_price']+df['total_cost']) / (1-df['ppve']-df['platform_zero']-df['target_profit_rate']) / df['rate']
    df['price'] = np.where(df['price'] * df['rate'] < df['lowest_price'], df['lowest_price'] / df['rate'], df['price'])
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']

    # 20250627 接入正常品调价逻辑
    listing_day_sales = get_listing_day_sales_new(platform='AMAZON')
    listing_day_sales['account_id'] = listing_day_sales['account_id'].astype(int)
    df['account_id'] = df['account_id'].astype(int)
    df = pd.merge(df, listing_day_sales, how='left', on=['seller_sku','account_id'])
    df['listing_day_sales'] = df['listing_day_sales'].fillna(0)
    df = normal_sku_price_adjust(df)

    # # 非正常品调价力度控制
    # c1 = (df['profit_diff'] > 1) & (df['sales_status'] != '正常')
    # c2 = (df['profit_diff'] < -1) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.3)
    # df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    # # df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 1, df['target_profit_rate'])
    # cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
    #     'rate']
    # df['price'] = cp
    df = control_listing_price(df, platform='AMAZON')

    df['price'] = (df['price'] + (df['money_off'] + df['Coupon_handling_fee'] + df['promotion_amount'])) / \
                  (1 - df['percentage_off'] - df['your_price_point'] - df['promotion_percent'])
    # # 海兔sku价格控制
    # df_haitu = haitu_sku_price()
    # df_haitu = df_haitu[df_haitu['platform']=='AMAZON']
    # df = pd.merge(df, df_haitu[['sku','warehouse','high_price']], how='left', on=['sku','warehouse'])
    # c1 = (~df['high_price'].isna()) & (df['high_price']<(df['price']*df['rate']))
    # df['price'] = np.where(c1, df['high_price']/df['rate'], df['price'])
    # df.drop('high_price', axis=1, inplace=True)
    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])

    df['price'] = df['price'].round(1) - 0.01
    df['target_profit_rate'] = df['target_profit_rate'].round(4)

    # 20251204 合并供应商货盘sku的定价数据
    df = pd.concat([df, df_2])
    df.drop(['online_profit', 'profit_diff', '佣金率', 'lowest_price'], axis=1, inplace=True)
    #  过滤条件汇总
    print('开始过滤待调价链接...')
    # 1、英国仓发往其他站点和英国站点发往其仓的
    c1 = (((df['country'] == 'UK') & (df['warehouse'] != '英国仓')) | (
                (df['country'] != 'UK') & (df['warehouse'] == '英国仓')) | (
            (df['country'] != 'US') & (df['warehouse'] == '美国仓')))
    df['is_uk_cdt'] = np.where(c1, 1, 0)

    # 2、正常品且日销大于0.1，不降价
    # c1 = (df['sales_status'].isin(['正常', '回调']) & (df['is_up'] == '降价') & (df['day_sales'] > 0.1))
    # df['is_normal_cdt'] = np.where(c1, 1, 0)

    # 2.1
    # 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    c1 = (df['sales_status'].isin(['正常']) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))
    # 20250109海外仓无库存不降价
    # 20260402 无库存非供应商sku
    c2 = (df['available_stock'] == 0) & (df['price'] < df['online_price']) & (df['is_supplier_price'] == 0)
    df['is_normal_cdt'] = np.where(c1 | c2, 1, 0)

    # 3、目标价与当前链接价差不超过0.3或变化率小于1%
    c1 = ((df['online_price'] - df['price']).abs() <= 0.3) | (((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01)
    df['is_small_diff'] = np.where(c1, 1, 0)

    # 4、不调价账号 & 停用账号 & VC专项组
    short_name = ['GTE', 'GTG', 'GTF', 'GTS', 'GTI', 'GTN', 'GTQ', 'GTP', 'A1P', 'A1W', 'A1E', 'A1G', 'A1F', 'A1S',
                  'A1I', 'A1N', 'H9E', 'H9G', 'H9F', 'H9S', 'H9I', 'H9W', 'H9N', 'GTW', 'H9P','3RG']
    account_id = [228, 265, 881, 1349, 6594, 6614, 6615, 930, 6851]
    # 20260319 补充虚拟仓账号临时不调价
    sql = """
        SELECT distinct erp_id
        FROM yibai_sale_center_system_sync.yibai_system_account
        WHERE platform_code = 'AMAZON' and status != 1
        
        UNION ALL
        
        SELECT DISTINCT account_id erp_id 
        from yibai_fba.amazon_account_para  
        where para_type = 37 and end_time >= toString(today()) and start_time <= toString(today())
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_system', data_sys='调价明细历史数据')
    df_account = conn_ck.ck_select_to_df(sql)
    deact_account = df_account.erp_id.unique()
    c1 = (df['short_name'].isin(short_name)) | (df['account_id'].isin(account_id)) | (df['account_id'].isin(deact_account))
    df['is_white_account'] = np.where(c1, 1, 0)
    df['is_white_account'] = np.where(df['group_name'].str.contains('VC项目专项组'), 1, df['is_white_account'])
    # 5、白名单链接：get_white_listing()
    df_white_sku, df_white_listing = get_white_listing(conn)
    # df_white_sku实际未过滤。（过滤方式需采用匹配后去nan）
    df = pd.merge(df, df_white_listing, how='left', on=['account_id', 'seller_sku'])
    df['is_white_listing'] = df['is_white_listing'].fillna(0).astype(int)

    # 6、剔除同大部、同站点、同asin下有FBA链接的FBM。复用原程序
    df = get_fba_asin(df, conn)
    df['date_id'] = datetime.datetime.now().strftime('%Y-%m-%d')
    df['account_id'] = df['account_id'].astype(int)
    #
    # 获取头程
    df = get_yunfei_info(df)

    df = df.drop_duplicates(subset=['account_id', 'seller_sku', 'sku'])
    df.drop(['listing_day_sales','deliver_mode'], axis=1, inplace=True)
    print(df.info())
    print('全量Amazon海外仓链接存表...')
    # write_to_sql(df, 'oversea_amazon_listing_all')
    write_to_ck(df, 'oversea_amazon_listing_all')
    # 全量数据只保留近三次的数据

    # 筛选
    df = df[(df['is_uk_cdt'] == 0) & (df['is_normal_cdt'] == 0) & (df['is_small_diff'] == 0) & (
        df['is_white_account']==0) & (df['is_white_listing']==0) & (df['is_fba_asin']==0)]
    # 20231016 巴西站点的数据暂时先不调价，销售人工调价测试中
    # 20250116 乌拉圭置为清仓
    # df = df[df['country'] != 'BR']
    print('过滤完成后，共{}条listing.'.format(len(df)))

    # 筛选后的数据再次存入sql
    del_col = ['站点', 'percentage_off', 'money_off', 'Coupon_handling_fee', 'promotion_percent', 'promotion_amount',
               'promotion_source','your_price_point', 'no_coupon_price', 'is_uk_cdt', 'is_normal_cdt', 'is_small_diff',
               'is_white_account','is_white_listing','is_fba_asin','new_firstCarrierCost']
    df.drop(del_col, axis=1, inplace=True)
    print(df.info())
    print('数据存入ck:oversea_amazon_listing_upload_temp')
    # write_to_sql(df, 'oversea_amazon_listing_upload_temp')
    # 20240425 迁入CK
    # df.drop('DATE', axis=1, inplace=True)
    write_to_ck(df, 'oversea_amazon_listing_upload_temp')


def get_yunfei_info(df):
    """ 获取运费明细数据 """
    sql = """
        SELECT sku, warehouseName best_warehouse_name, country, new_firstCarrierCost
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)

    df = pd.merge(df, df_fee, how='left', on=['best_warehouse_name', 'sku', 'country'])

    return df

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
    ck_client = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_listing_all = ck_client.ck_select_to_df(sql)
    df_listing_all.columns = [i.split('.')[-1] for i in df_listing_all.columns.to_list()]
    #
    df = pd.merge(df_listing_all, dwm_bundle_price, how='inner', on=['sku', 'country'])
    #
    # 剔除逻辑
    # 判断是否涨价
    df[['price', 'online_price']] = df[['price', 'online_price']].astype(float)
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
    # df_platform_fee = df_platform_fee[df_platform_fee['platform'] == 'AMAZON']
    df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    #
    # 2、正常品且日销大于0.1，不降价
    c1 = (df['sales_status'].isin(['正常']) & (df['is_up'] == '降价') & (df['day_sales'] > 0.3))
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
    df = df.drop(columns=['percentage_off', 'money_off', 'Coupon_handling_fee', 'promotion_percent', 'promotion_amount',
                          'promotion_source', 'your_price_point'], axis=1)
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
    del_col = ['is_normal_cdt', 'is_small_diff', 'is_white_account', 'is_white_listing', 'is_fba_asin']
    df_final.drop(columns=del_col, axis=1, inplace=True)
    #
    write_to_sql(df_final, 'oversea_amazon_bundle_listing_upload')

    return df_final

# 捆绑SKU运行程序
def amazon_bundle():
    # dwm_oversea_bundle()
    # get_bundle_adjust_listing()

    # 20260401 捆绑sku数据汇总：先取各平台捆绑sku+国家、再统一接口获取运费、再计算不同平台定价数据
    # 接口获取运费
    get_bundle_sku()

def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

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

    # 删除360天前的数据
    sql = f"""
    ALTER TABLE yibai_oversea.{table_name}
    DELETE WHERE date_id < formatDateTime(today() - 360, '%Y-%m-%d')
    """
    conn_ck.ck_execute_sql(sql)

def delete_date(table_name):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
        SELECT distinct date_id
        FROM over_sea.{table_name}
        ORDER BY date_id DESC
    """
    df_temp = conn.read_sql(sql)
    if len(df_temp)>2:
        df_date = df_temp.iloc[2,0]
    else:
        df_date = df_temp.min()
    sql = f"""
        delete from over_sea.{table_name} where date_id < '{df_date}'
    """
    conn.execute(sql)

##
# 获取ebay调价链接
def get_ebay_no_adjust_listing(df_all):
    """ ebay不调价链接 """
    # 20220629剔除折扣链接
    date_today = time.strftime('%Y-%m-%d')
    sql_discount = f"""
        SELECT item_id 
        FROM yibai_sale_center_listing_sync.yibai_ebay_discount_status_analysis 
        where ((status=1 and formatDateTime(toDateTime(sale_start_time), '%Y-%m-%d') <= '{date_today}'
        and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d') >= '{date_today}')
        or (status=2)
        or ((status=1 and need_task=10 and formatDateTime(toDateTime(sale_end_time), '%Y-%m-%d')<='{date_today}')
        or(status=2 and next_cycle_time>0)))
     """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_ebay_discount = conn_ck.ck_select_to_df(sql_discount)
    df_ebay_discount['折扣'] = 1
    df_all['item_id'] = df_all['item_id'].astype('str')
    df_ebay_discount['item_id'] = df_ebay_discount['item_id'].astype('str')
    df_all = df_all.merge(df_ebay_discount, on=['item_id'], how='left')
    df_all['is_white_account'] = np.where(df_all['折扣'].isna(), df_all['is_white_account'], 2)
    del df_all['折扣']

    # 20250411 eba剔除锁价表链接
    print(f'剔除锁价表链接前有{len(df_all)}条数据。')
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT item_id, sku, 1 as is_lock_listing
        FROM yibai_sale_center_listing_sync.yibai_ebay_price_adjustment_filter_sku
        WHERE end_time > '{date_today}' and is_del = 0
    """
    df_lock = conn_ck.ck_select_to_df(sql)
    df_lock['item_id'] = df_lock['item_id'].astype(str)
    df_all = df_all.merge(df_lock, on=['item_id', 'sku'], how='left')
    df_all['is_white_account'] = np.where(df_all['is_lock_listing'].isna(), df_all['is_white_account'], 3)
    del df_all['is_lock_listing']

    return df_all


def get_normal_bad_orders(platform='EB'):
    """ 获取正常品负利润订单 """
    today = datetime.datetime.now().date()
    last_month_first = (today.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
    date_start = last_month_first.strftime('%Y-%m-%d')
    net_profit_line = 0.04  # 阈值
    print(date_start)
    # date_start = '2026-01-01'
    # date_end = '2025-10-01'
    sql = f"""
        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, new_price, release_money, sales_status, real_profit/total_price net_profit,
            1 as is_bad_order
        FROM yibai_oversea.dashbord_new_data1
        WHERE 
            paytime >= '{date_start}'
            -- and paytime < '{today}'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and warehouse_name not like '%精品%'
            and platform_code = '{platform}'
            and sales_status = '正常'
            and net_profit < {net_profit_line}
            -- and sku = '3118240156511'

    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)
    print(df_temp.info())
    col = ['sku', 'seller_sku','account_id','is_bad_order']
    df_temp = df_temp[col].drop_duplicates(subset=col)

    return df_temp

def get_ebay_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT * 
        FROM yibai_oversea.yibai_ads_oversea_ebay_listing 
        WHERE date_id = '{now_time}'
    """
    df_ebay_listing = conn_ck.ck_select_to_df(sql)
    df_ebay_listing['system_sku'] = ''
    df_ebay_listing = df_ebay_listing[~df_ebay_listing['name'].str.contains('虚拟|FBA')]
    # df_ebay_listing.drop('DATE', axis=1, inplace=True)
    df_ebay_dtl = get_price_data(conn, platform='EB')
    print('ebay海外仓链接数量{}条'.format(len(df_ebay_listing)))
    print('ebay海外仓调价记录{}条'.format((len(df_ebay_dtl))))

    # 合并链接与价格数据
    df = pd.merge(df_ebay_listing, df_ebay_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'item_id', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)
    print(df.info())
    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform_fee = get_platform_fee('EB')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df = df.drop_duplicates()
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']

    # # 20260304 接入真实佣金率
    df = get_ebay_commission(df)
    df['ppve'] = df['ppve'] - 0.15 + df['commission_rate']
    df.drop('commission_rate', axis=1, inplace=True)
    # 4、供应商货盘sku定价的链接不重新计算价格
    df_2 = df[df['is_supplier_price'] == 1]
    df = df[df['is_supplier_price'] != 1]
    # 20240223 控制正常品的涨降幅度，单次不超过10%
    # 20240911 控制降价幅度，单次不超过10%.
    # 20250217 兜底价上调，单次最大涨幅不超过20%
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    #
    # 20250627 接入正常品调价逻辑
    listing_day_sales = get_listing_day_sales_new(platform='EB')
    listing_day_sales = listing_day_sales.rename(columns={'seller_sku':'item_id', 'account_id':'sku'})
    df = pd.merge(df, listing_day_sales, how='left', on=['item_id','sku'])
    df['listing_day_sales'] = df['listing_day_sales'].fillna(0)
    df = normal_sku_price_adjust(df)

    # c1 = (df['profit_diff'] > 1) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    # c2 = (df['profit_diff'] < -0.5) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.2)
    # df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    # # df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.5, df['target_profit_rate'])
    # cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
    #     'rate']
    # df['price'] = cp

    df = control_listing_price(df, platform='EB')
    # df[df['available_stock']>0].to_excel('F://Desktop//df_ebay_test.xlsx', index=0)
    # # 海兔sku价格控制
    # df_haitu = haitu_sku_price()
    # df_haitu = df_haitu[df_haitu['platform']=='EB']
    # df = pd.merge(df, df_haitu[['sku','warehouse','high_price']], how='left', on=['sku','warehouse'])
    # c1 = (~df['high_price'].isna()) & (df['high_price']<(df['price']*df['rate']))
    # df['price'] = np.where(c1, df['high_price']/df['rate'], df['price'])
    # df.drop('high_price', axis=1, inplace=True)
    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])

    df.drop(['profit_diff'], axis=1, inplace=True)

    # 20251204 合并供应商sku调价链接
    df = pd.concat([df, df_2])
    # SKU+大仓+国家维度下，存在多个item_id时，阶梯定价.
    # 价格需要减去 shipping_fee
    df['flag'] = df.groupby(['sku', 'warehouse','country'])['price'].rank(method='first')
    df['price'] = df['price'] + df['flag'] * 0.01
    df['price'] = df['price'] - df['shipping_fee']
    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')
    # 筛选条件
    # 1、正常品且日销大于0.3不降价
    # 1、20250630 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))
    c2 = (df['available_stock'] == 0) & (df['price'] < df['online_price']) & (df['is_supplier_price'] == 0)
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2, 1, 0)

    # 2、按SKU+大仓维度，剔除未满90天的SKU.(20230810)
    sql = """
        select distinct sku, warehouse, 1 as is_90_overage
        from dwm_sku_temp_info
        WHERE age_90_plus = 0 and available_stock > 0 and date_id>=(SELECT max(date_id) FROM dwm_sku_temp_info)
        """
    df_overage_90 = conn.read_sql(sql)
    df = pd.merge(df, df_overage_90, how='left', on=['sku', 'warehouse'])
    df['is_90_overage'] = df['is_90_overage'].fillna(0).astype(int)

    # 3、英国仓筛除逻辑
    c1 = (((df['country'] == 'UK') & (df['warehouse'] != '英国仓')) | (
                (df['country'] != 'UK') & (df['warehouse'] == '英国仓')))
    df['is_uk_cdt'] = np.where(c1, 1, 0)

    # 4、不调价账号
    account_id = []
    sql = """
        SELECT *
        FROM over_sea.oversea_ebay_account_temp
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_account_temp = conn.read_sql(sql)
    df_account_temp['is_white_account'] = 1
    df['account_id'] = df['account_id'].astype(int)
    df_account_temp['account_id'] = df_account_temp['account_id'].astype(int)

    df = pd.merge(df, df_account_temp[['account_id', 'is_white_account']], how='left', on=['account_id'])
    # c1 = (df['account_id'].isin(account_id))
    # df['is_white_account'] = np.where(c1, 1, 0)
    df['is_white_account'] = df['is_white_account'].fillna(0).astype(int)
    df = get_ebay_no_adjust_listing(df)
    # 20260130  近一个月正常品负利润订单的链接，放出来调价
    df_bad = get_normal_bad_orders(platform='EB')
    df_bad = df_bad.rename(columns={'seller_sku':'item_id'})
    df_bad['item_id'] = df_bad['item_id'].astype(str)
    df = pd.merge(df, df_bad[['item_id','sku','is_bad_order']], how='left', on=['item_id','sku'])
    df['is_white_account'] = np.where(~df['is_bad_order'].isna(), 0, df['is_white_account'])
    df.drop('is_bad_order', axis=1, inplace=True)

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time

    df.drop(['online_profit','listing_day_sales','lowest_price'], axis=1, inplace=True)
    df[['site','product_line','listing_status']] = df[['site','product_line','listing_status']].astype(str)
    print(df.info())
    print('全量eBay海外仓链接存表...')
    #
    write_to_ck(df, 'oversea_ebay_listing_all')

    # 筛除后存表
    # 20250407 ebay不调价测试：部分账号开放所有sku
    df = df[(df['is_normal_cdt']==0) & (df['is_uk_cdt']==0) & \
            (df['is_white_account']==0) & (df['is_small_diff']==0) & (df['listing_status']=='1')]
    c1 = (df['warehouse'] == '美国仓') & (df['country'] != 'US')
    df = df[~c1]
    print('剔除筛选逻辑后共{}条数据.'.format(len(df)))
    # 筛选后的数据再次存入sql
    del_col = ['is_uk_cdt', 'is_normal_cdt', 'is_small_diff','is_white_account','is_90_overage']
    df.drop(del_col, axis=1, inplace=True)

    print('数据存入ck:oversea_ebay_listing_upload_temp')
    # write_to_sql(df, 'oversea_ebay_listing_upload_temp')
    # 20240425调价记录数据准备迁入CK
    write_to_ck(df, 'oversea_ebay_listing_upload_temp')
    # conn.to_sql(df, 'oversea_ebay_listing_upload_temp', if_exists='replace')

##
# 获取walmart调价链接
def get_walmart_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    sql = f"""
        SELECT * 
        FROM yibai_oversea.yibai_ads_oversea_walmart_listing 
        WHERE date_id = '{now_time}'
    """
    df_walmart_listing = conn_ck.ck_select_to_df(sql)
    df_walmart_listing.drop('date_id', axis=1, inplace=True)
    df_walmart_listing['site'] = df_walmart_listing['site'].str.upper()
    df_walmart_listing = df_walmart_listing.rename(columns={'site': 'country'})
    df_walmart_listing['account_id'] = df_walmart_listing['account_id'].astype(int)
    print(df_walmart_listing.info())
    # 获取SKU价格信息
    # 筛选库存数大于4的
    sql = f"""
        SELECT 
            sku, best_warehouse_name, warehouse,new_price, total_cost, overage_level,available_stock, sales_status,price,target_profit_rate,is_adjust, 
            day_sales, country, ship_name,lowest_price, is_supplier, is_supplier_price,ppve, platform_zero, platform_must_percent
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE platform = 'WALMART' and date_id = '{now_time}' and available_stock > 4
    """
    df_walmart_dtl = conn_ck.ck_select_to_df(sql)

    # 合并链接与价格数据
    df_walmart_listing = df_walmart_listing.rename(columns={'price':'online_price'})
    df = pd.merge(df_walmart_listing, df_walmart_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)
    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform_fee = get_platform_fee('WALMART')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']

    # 4、供应商货盘sku定价的链接不重新计算价格
    df_2 = df[df['is_supplier_price'] == 1]
    df = df[df['is_supplier_price'] != 1]
    # 20240223 控制正常品的涨降幅度，单次不超过10%
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    #
    c1 = (df['profit_diff'] > 1) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    c2 = (df['profit_diff'] < -0.3) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.3, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where((df['profit_diff'] < -0.15) & (df['available_stock']>0) & (df['online_profit'] <= -0.15),
    #                                     (df['online_profit'] + 0)/2,
    #                                     df['target_profit_rate'])
    # 20250627 接入正常品调价逻辑
    listing_day_sales = get_listing_day_sales_new(platform='WALMART')
    listing_day_sales['account_id'] = listing_day_sales['account_id'].astype(int)
    df = pd.merge(df, listing_day_sales, how='left', on=['account_id','seller_sku'])
    df['listing_day_sales'] = df['listing_day_sales'].fillna(0)
    df = normal_sku_price_adjust(df)

    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # # 海兔sku价格控制
    # df_haitu = haitu_sku_price()
    # df_haitu = df_haitu[df_haitu['platform']=='WALMART']
    # df = pd.merge(df, df_haitu[['sku','warehouse','high_price']], how='left', on=['sku','warehouse'])
    # c1 = (~df['high_price'].isna()) & (df['high_price']<(df['price']*df['rate']))
    # df['price'] = np.where(c1, df['high_price']/df['rate'], df['price'])
    # df.drop('high_price', axis=1, inplace=True)

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    df.drop(['profit_diff'], axis=1, inplace=True)

    # 20251204 合并供应商货盘sku的定价数据
    df = pd.concat([df, df_2])

    # 同一个SKU存在多条链接的情况
    df['flag'] = df.groupby(['sku', 'warehouse','country'])['price'].rank(method='first')
    # 阶梯定价
    df['price'] = df['price'] + df['flag'] % 50 * 0.01
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')

    # 筛选
    # 1、正常品且日销大于0.3不降价
    # 1、20250630 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))

    df['is_normal_cdt'] = np.where(c1, 1, 0)

    # 2、平台仓链接
    sql = """
        SELECT distinct toString(item_id) item_id
        FROM yibai_sale_center_listing_sync.yibai_walmart_health_report
    """
    df_temp = conn_ck.ck_select_to_df(sql)
    df['is_platform_cdt'] = np.where(df['item_id'].isin(df_temp['item_id']),1,0)
    # 3、库存数小于4不调价

    # 4、筛选锁价表：yibai_walmart_listing_lock_price
    sql = """
    select b.account_id account_id,a.seller_sku seller_sku, 1 as is_white_listing
    from (
        select distinct account_id,seller_sku 
        from yibai_sale_center_listing_sync.yibai_walmart_change_price_lock
        where status = 1 and toUnixTimestamp(now()) <deadline_time
    ) a
    inner join (
        select * from yibai_sale_center_system_sync.yibai_system_account
        where platform_code = 'WALMART' AND `status` = 1 AND is_del =0
    ) b 
    on a.account_id=b.id
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_lock_listing = conn_ck.ck_select_to_df(sql)
    # df_lock_listing = conn.read_sql(sql)
    df_lock_listing['account_id'] = df_lock_listing['account_id'].astype(int)
    df = pd.merge(df, df_lock_listing, how='left', on=['account_id', 'seller_sku'])
    df['is_white_listing'] = df['is_white_listing'].fillna(0).astype(int)

    # 5、白名单账号 (20240325：接入ck的walmart不调价账号表）
    # 20240321 销售反馈精铺账号不调价：US-XXLK,US-DZMK,US-BZBL,US-SLLN,US-HRKJ,US-QNXW,US-XCKW,US-ZXQK,US-GRZW,US-YMSD,US-NYKJ,
    # 638,664,621,643,665,666,667,668,669,670,671,
    # account_id = [474, 638,664,621,643,665,666,667,668,669,670,671]
    sql = """
    select distinct account_id from yibai_domestic.walmart_account_para
    where para_type=2 and end_time >= toString(today()) and start_time<=toString(today())
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_wal = conn_ck.ck_select_to_df(sql)
    account_id = tuple(df_wal['account_id'].unique())
    c1 = (df['account_id'].isin(account_id))
    c2 = (df['account_id'].isin([792]))  # 临时不调价账号
    df['is_white_account'] = np.where(c1 | c2, 1, 0)

    # 6、目标价与当前链接价差不超过0.3
    c1 = ((df['online_price'] - df['price']).abs() <= 0.3) | (((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01)
    df['is_small_diff'] = np.where(c1, 1, 0)

    # 7、剔除CA站点数据
    df['is_ca'] = np.where(df['country'].isin(['CA','MX']), 1, 0)

    #
    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)

    df.drop(['online_profit', 'listing_day_sales','lowest_price'], axis=1, inplace=True)
    print(df.info())
    print('全量Walmart海外仓链接存表...')
    # write_to_sql(df, 'oversea_walmart_listing_all')
    write_to_ck(df, 'oversea_walmart_listing_all')

    # 筛除后存表
    df = df[(df['is_normal_cdt']==0) & (df['is_platform_cdt']==0) & (df['is_ca']==0) &
            (df['is_white_listing']==0) & (df['is_small_diff']==0) & (df['is_white_account']==0)]
    print('剔除筛选逻辑后共{}条数据.'.format(len(df)))
    # 筛选后的数据再次存入sql
    del_col = ['is_normal_cdt', 'is_small_diff','is_white_listing','is_platform_cdt','is_ca','is_white_account']
    df.drop(del_col, axis=1, inplace=True)
    print('数据存入ck:oversea_walmart_listing_upload_temp')
    # write_to_sql(df, 'oversea_walmart_listing_upload_temp')
    write_to_ck(df, 'oversea_walmart_listing_upload_temp')

# CD平台调价数据
def get_cd_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT * 
        FROM yibai_oversea.yibai_ads_oversea_cdiscount_listing 
        WHERE date_id = '{now_time}'
    """
    df_cdiscount_listing = conn_ck.ck_select_to_df(sql)
    df_cdiscount_listing.drop(['date_id'], axis=1, inplace=True)
    df_cdiscount_listing = df_cdiscount_listing.rename(columns={'short_name':'account_name'})
    df_cdiscount_listing['site'] = df_cdiscount_listing['site'].str.upper()
    df_cdiscount_listing = df_cdiscount_listing.rename(columns={'site': 'country'})
    print(f'CD链接数量共{len(df_cdiscount_listing)}条.')
    #
    df_cdiscount_dtl = get_price_data(conn, platform='CDISCOUNT')

    # 合并链接与价格数据
    df = pd.merge(df_cdiscount_listing, df_cdiscount_dtl, how='inner', on=['sku', 'country'])
    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level','available_stock','total_cost'], ascending=[False,False,True]).\
        drop_duplicates(subset=['sku', 'account_id', 'seller_sku', 'country'], keep='first')
    df.drop('overage_level', axis=1, inplace=True)
    df[['online_price', 'price', 'day_sales']] = df[['online_price', 'price', 'day_sales']].astype(float)
    print(df.info())
    # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])
    # df_platform_fee = get_platform_fee('CDISCOUNT')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']

    # 4、供应商货盘sku定价的链接不重新计算价格
    df_2 = df[df['is_supplier_price'] == 1]
    df = df[df['is_supplier_price'] != 1]
    # 20240223 控制正常品且库存大于0的涨降幅度，单次不超过10%。
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    #
    c1 = (df['profit_diff'] > 0.15) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    c2 = (df['profit_diff'] < -0.3) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 0.15, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.3, df['target_profit_rate'])
    # 20260331 正常品目标净利率最高5%
    df['target_profit_rate'] = np.where(df['target_profit_rate']>0.05, 0.05, df['target_profit_rate'])
    # 20250627 接入正常品调价逻辑
    listing_day_sales = get_listing_day_sales_new(platform='CDISCOUNT')
    df['account_id'] = df['account_id'].astype(int)
    df = pd.merge(df, listing_day_sales, how='left', on=['account_id','seller_sku'])
    df['listing_day_sales'] = df['listing_day_sales'].fillna(0)
    df = normal_sku_price_adjust(df)

    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])

    df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)

    # 20251204 合并供应商货盘sku的定价数据
    df = pd.concat([df, df_2])

    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'], '涨价', '降价')

    # 筛选
    # 1、正常品且日销大于0.3不降价
    # 1、20250630 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))

    c2 = (df['available_stock'] == 0) & (df['price'] < df['online_price']) & (df['is_supplier_price'] == 0)
    df['is_normal_cdt'] = np.where(c1|c2, 1, 0)

    # 2、价差较小不调价
    c1 = ((df['online_price'] - df['price']).abs() <= 0.3) | (((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01)
    df['is_small_diff'] = np.where(c1, 1, 0)

    # 3、去除英国仓SKU
    df['is_uk_warehouse'] = np.where(df['warehouse'] == '英国仓', 1, 0)

    # 4、只执行负利润加快动销和负利润加快动销的回调以及正利润加快动销的回调
    c1 = (df['sales_status'] != '正常') & (df['sales_status'] != '正利润加快动销')
    df['is_clear_sku'] = np.where(c1, 1, 0)

    # 5、海外仓冲业绩不调账号（2023-03-30）;  20240522  LEC（227）cd禁止调价账号
    account_id = [271, 313, 227]
    df['is_white_account'] = np.where(df['account_id'].isin(account_id), 1, 0)

    # 6、剔除平台仓链接
    sql = """
        SELECT distinct toString(sku) as product_id
        FROM yibai_sale_center_listing_sync.yibai_cdiscount_fbc_stock_detail
        where create_date >= toString(today()-30)
    """
    ck_client = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_temp = ck_client.ck_select_to_df(sql)
    print('CD平台仓链接数量共{}条.'.format(len(df_temp)))
    df['is_platform_cdt'] = np.where(df['product_id'].isin(df_temp['product_id']), 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)

    df.drop(['listing_day_sales','lowest_price'], axis=1, inplace=True)
    print(df.info())
    print('全量cd海外仓链接存表...')
    # write_to_sql(df, 'oversea_cdiscount_listing_all')
    write_to_ck(df, 'oversea_cdiscount_listing_all')
    # conn.to_sql(df, 'oversea_cdiscount_listing_all', if_exists='append')

    # 筛除后存表
    # 20250214 取消【只执行负利润加快动销】条件
    df = df[(df['is_normal_cdt']==0) & (df['is_platform_cdt']==0) & (df['is_white_account']==0) &
            (df['is_small_diff']==0)& (df['is_uk_warehouse']==0)]
    print('剔除筛选逻辑后共{}条数据.'.format(len(df)))
    # 筛选后的数据再次存入sql
    del_col = ['is_normal_cdt', 'is_small_diff','is_white_account','is_platform_cdt','is_clear_sku','is_uk_warehouse']
    df.drop(del_col, axis=1, inplace=True)
    print('数据存入ck:oversea_cdiscount_listing_upload_temp')
    # write_to_sql(df, 'oversea_cdiscount_listing_upload_temp')
    # conn.to_sql(df, 'oversea_cdiscount_listing_upload_temp', if_exists='replace')
    write_to_ck(df, 'oversea_cdiscount_listing_upload_temp')

def get_allegro_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT 
            account_id, account_name, offer_id, sku, date_id, online_price, selling_mode_currency, location,
            country, group_name
        FROM yibai_oversea.yibai_ads_oversea_allegro_listing 
        WHERE date_id = '{now_time}'
    """
    df_listing = conn_ck.ck_select_to_df(sql)
    df_dtl = get_price_data(conn, platform='ALLEGRO')

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
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 20240223 控制正常品的涨降幅度，单次不超过10%
    # 20240911 控制降价幅度，单次不超过10%.
    # 20250217 兜底价上调，单次最大涨幅不超过20%
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    #
    c1 = (df['profit_diff'] > 1) & (df['sales_status'] != '正常') & (df['available_stock']>0)
    c2 = (df['profit_diff'] < -0.2) & (df['sales_status'] != '正常') & (df['available_stock']>0) & (df['online_profit'] <= -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    df['target_profit_rate'] = np.where(c2, (df['online_profit'] + 0)/2, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.2, df['target_profit_rate'])
    # 20250627 接入正常品调价逻辑
    listing_day_sales = get_listing_day_sales_new(platform='ALLEGRO')
    listing_day_sales['account_id'] = listing_day_sales['account_id'].astype(int)
    listing_day_sales['seller_sku'] = listing_day_sales['seller_sku'].astype(str)
    listing_day_sales = listing_day_sales.rename(columns={'seller_sku':'offer_id'})
    df = pd.merge(df, listing_day_sales, how='left', on=['account_id','offer_id'])
    df['listing_day_sales'] = df['listing_day_sales'].fillna(0)
    df = normal_sku_price_adjust(df)
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    # df.to_excel('F://Desktop//df_allegro_all.xlsx', index=0)

    # SKU+大仓+国家维度下，存在多个item_id时，阶梯定价.
    # 价格需要减去 shipping_fee
    # df['flag'] = df.groupby(['sku', 'warehouse','country'])['price'].rank(method='first')
    # df['price'] = df['price'] + df['flag'] * 0.01
    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')
    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    # 1、20250630 正常品且sku维度日销大于0.3且链接维度日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & ((df['day_sales'] > 0.3) & (df['listing_day_sales'] > 0.1)))
    c2 = (df['available_stock'] == 0) & (df['price'] < df['online_price']) & (df['is_supplier_price'] == 0)
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2, 1, 0)

    #
    # # 4、不调价账号
    account_id = [18714]
    c1 = (df['account_id'].isin(account_id))
    df['is_white_account'] = np.where(c1, 1, 0)
    sql = """
        SELECT a.account_id account_id, b.id id, start_time, end_time
        FROM yibai_domestic.allegro_no_adjust_account a
        LEFT JOIN yibai_sale_center_system_sync.yibai_system_account b
        ON a.account_id = b.account_id
    """
    df_a = conn_ck.ck_select_to_df(sql)
    c2 = (df['account_id'].isin(df_a['id'].unique())) & (df['is_white_account']==0)
    df['is_white_account'] = np.where(c2, 1, df['is_white_account'])

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)
    df.drop(['lowest_price'], axis=1, inplace=True)
    df.drop(['listing_day_sales'], axis=1, inplace=True)
    print(df.info())
    print('全量allegro海外仓链接存表...')
    write_to_ck(df, 'oversea_allegro_listing_all')

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

def get_wait_destroy():
    """ 获取待销毁数据 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT sku, warehouse, 1 as is_wait_destroy
        FROM over_sea.oversea_wait_destroy_sku
    """
    df = conn.read_sql(sql)

    return df

def get_shopee_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT
            account_id, short_name account_name, item_id, sku, parent_sku, is_mulit, date_id, online_price, country
        FROM yibai_oversea.yibai_ads_oversea_shopee_listing
        WHERE date_id = '{now_time}'
        and status_online = 'NORMAL'
    """
    df_listing = conn_ck.ck_select_to_df(sql)

    df_dtl = get_price_data(conn, platform='SHOPEE')

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

    # df_platform_fee = get_platform_fee('SHOPEE')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df = df.drop_duplicates()
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 控制降价幅度，单次不超过20%.
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']

    c1 = (df['profit_diff'] > 1)
    # c2 = (df['profit_diff'] < -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.1, df['target_profit_rate'])
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']

    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')

    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & (df['day_sales'] > 0.3))
    c2 = (df['available_stock'] == 0) | (df['best_warehouse_name'].isin([ 'XYD泰国海外仓','XYD菲律宾海外仓','XYD马来海外仓']))
    # 1.2 待销毁数据不调价
    # 20260324 取消剔除待销毁数据(暂未执行）
    df_destroy = get_wait_destroy()
    df = pd.merge(df, df_destroy[['sku','warehouse','is_wait_destroy']], how='left', on=['sku','warehouse'])
    c3 = (~df['is_wait_destroy'].isna())
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2 | c3, 1, 0)
    df.drop('is_wait_destroy', axis=1, inplace=True)
    #
    # 4、不调价账号
    account_id = [26696,28411,28416,28417,28418,28831,29396,29422,32076,35054,35055,35056]
    c1 = (df['account_id'].isin(account_id))
    df['is_white_account'] = np.where(c1, 1, 0)

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)
    # df.drop(['lowest_price'], axis=1, inplace=True)

    print(df.info())
    # df.to_excel('F://Desktop//df_dtl.xlsx', index=0)
    print('全量shopee海外仓链接存表...')
    write_to_ck(df, 'oversea_shopee_listing_all')

def get_sku_type():
    """ 获取dwm_sku表信息 """
    sql = """
        SELECT sku, type, warehouse
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info WHERE date_id > '2025-07-01')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df


def get_lazada_TH_dtl():
    """
    lazada泰国来赞宝仓运费和库存数据
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    # 取来赞宝仓库存、运费和绑定链接
    sql = """
        SELECT *
        FROM over_sea.lzb_stock_and_fee
    """
    df_lzb = conn.read_sql(sql)
    df_lzb = df_lzb.rename(columns={'available_stock':'available_stock_lzb', 'total_cost':'total_cost_lzb'})
    # df = pd.merge(df, df_lzb[['sku','available_stock_lzb', 'total_cost_lzb','seller_sku']], how='left', on=['sku'])
    #
    # df['available_stock'] = np.where(df['available_stock_lzb'].isna(), df['available_stock'], df['available_stock_lzb'])
    # df['total_cost'] = np.where(df['total_cost_lzb'].isna(), df['total_cost'], df['total_cost_lzb'])
    #
    # df.to_excel('F://Desktop//df_lzb.xlsx', index=0)

    return df_lzb

def get_lazada_adjust_listing():
    now_time = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT
            account_id, short_name account_name, item_id, sku, seller_sku, parent_sku, is_mulit, date_id, online_price, country
        FROM yibai_oversea.yibai_ads_oversea_lazada_listing
        WHERE date_id = '{now_time}'
        and status_online != 'Suspended'
        -- and status_online = 'NORMAL'
    """
    df_listing = conn_ck.ck_select_to_df(sql)
    # 临时的链接
    seller_list = ['GFSA0F0U5Y7K1K','QYCC0A0E2U5B1Y','GY00709-01','QACG0W0E0E1B5E','1Y4G1I6E1Z9G0G0X1H0C3Q1N1N',
                   '1X4O1N1U2X1Y0A0A8V7O6N1H1J','JRYR2X8M5P5U6U','JAYKAD0J2H2I3A7S','JUYF3F9J1Z3M2B',
                   '2D7Z1V8O2W1E0A1M3W1V6W1V1C','TGJK0G6B2F6Y2A-05','2F1G1E4A2J1Q0Z0W3K2C1P1G3R','JOYPAN0I1E2R6I9P',
                   '1V6K1Q0T2K1T0R1Z0A9P7K1K1X','GISO0O7Z3E9K4G','3H1W1O1F2D1X0W3V1K2O0Q1Q1H','1I0W1P1I2K1G0Q3B8B2W1C1E1V',
                   'JSYO1J3W8H9S7E','3W1D1M1G2R1Q0N2F0K2F2B1R1Y','TWJM0G6Y6R4F1U','JKYZ4X0W1X3U7N','JDMA1P2I9S3P2L-03',
                   'JKYNAN0X1U2P1Y7J-03','TYJX0U3E4P4B9B']

    df_dtl = get_price_data(conn, platform='LAZADA')
    # LZB泰国仓运费和库存单独处理
    df_type = get_sku_type()
    df_type = df_type.drop_duplicates(subset=['sku'])
    df_dtl = pd.merge(df_dtl, df_type[['sku', 'type']], how='left', on=['sku'])
    # 合并链接与价格数据
    # 20250716 非3pf账号不可用菜鸟子仓
    account_3pf = [24955, 24250, 24507, 24951, 24052, 24064, 24419, 24950, 24502, 24699]
    df_listing_1 = df_listing[df_listing['account_id'].isin(account_3pf)]
    df_listing_2 = df_listing[~df_listing['account_id'].isin(account_3pf)]
    df_dtl_2 = df_dtl[~df_dtl['best_warehouse_name'].str.contains('菜鸟|Cainiao')]
    df_1 = pd.merge(df_listing_1, df_dtl, how='inner', on=['sku', 'country'])
    df_2 = pd.merge(df_listing_2, df_dtl_2, how='inner', on=['sku', 'country'])
    df = pd.concat([df_1, df_2])
    # # LZB泰国仓运费替换
    # df_lzb = get_lazada_TH_dtl()
    # df = pd.merge(df, df_lzb[['sku','seller_sku','available_stock_lzb', 'total_cost_lzb']],
    #                 how='left', on=['sku','seller_sku'])
    # c1 = (df['country']=='TH') & (~df['total_cost_lzb'].isna())
    # df['total_cost'] = np.where(c1, df['total_cost_lzb'], df['total_cost'])
    # df['available_stock'] = np.where(c1, df['available_stock_lzb'], df['available_stock'])
    # df['best_warehouse_name'] = np.where(c1, 'BBH-TH03', df['best_warehouse_name'])
    # 20250826 lazada库存数据取中台库存列表
    sql = """ 
        SELECT account_id, sku, seller_sku, warehouse_stock
        FROM yibai_sale_center_listing_sync.yibai_lazada_price_fbl_list
    """
    df_fbl_stock = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_fbl_stock[['account_id','seller_sku','warehouse_stock']],
                  how='left', on=['account_id','seller_sku'])
    df['warehouse_stock'] = df['warehouse_stock'].fillna(0).astype(int)
    # 20250930 中台库存表和云仓库存，优先取云仓库存
    c1 = (df['available_stock'] > 0) & (df['best_warehouse_name'].str.contains('菜鸟|Cainiao'))
    df['available_stock'] = np.where(c1, df['available_stock'], df['warehouse_stock'])

    # 相同account_id + seller_sku下，如果存在多个warehouse, 取运费最低的那一条。
    # 20240117 补充：优先取超库龄等级高的仓库、有库存的仓库
    df = df.sort_values(by=['overage_level', 'available_stock', 'total_cost'],
                        ascending=[False, False, True]). \
        drop_duplicates(subset=['sku', 'account_id', 'item_id', 'country'], keep='first')
    df.drop(['type','warehouse_stock'], axis=1, inplace=True)
    # df.to_excel('F://Desktop//df_lazada.xlsx', index=0)
    print(df.info())
    # # 匹配汇率、平台配置表
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])

    # df_platform_fee = get_platform_fee('LAZADA')
    # df = pd.merge(df, df_platform_fee, how='left', on=['country'])
    df = df.drop_duplicates()
    # 计算毛利率
    df['gross_profit_rate'] = df['target_profit_rate'] + df['platform_zero']
    # 控制降价幅度，单次不超过20%.
    df = df[df['online_price'] > 0]
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
            df['online_price'] * df['rate']) - df['platform_zero']
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    #
    c1 = (df['profit_diff'] > 1)
    # c2 = (df['profit_diff'] < -0.2)
    df['target_profit_rate'] = np.where(c1, df['online_profit'] - 1, df['target_profit_rate'])
    # df['target_profit_rate'] = np.where(c2, df['online_profit'] + 0.1, df['target_profit_rate'])
    cp = (df['new_price'] + df['total_cost']) / (1 - df['ppve'] - df['platform_zero'] - df['target_profit_rate']) / df[
        'rate']
    df['price'] = cp

    # df.drop(['online_profit', 'profit_diff'], axis=1, inplace=True)
    # df.to_excel('F://Desktop//df_lazada_listing.xlsx', index=0)

    # 销毁价兜底
    df['price'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), df['lowest_price']/df['rate'], df['price'])
    c1 = 1 - df['ppve'] - df['platform_zero'] - (df['new_price'] + df['total_cost'])/df['lowest_price']
    df['target_profit_rate'] = np.where(df['price'] <= (df['lowest_price']/df['rate']), c1, df['target_profit_rate'])
    df['profit_diff'] = df['online_profit'] - df['target_profit_rate']
    # df.to_excel('F://Desktop//df_lazada_all.xlsx', index=0)

    print('阶梯定价已完成...')
    # 判断是否涨价
    df['is_up'] = np.where(df['price'] >= df['online_price'],'涨价', '降价')

    # 筛选条件
    # 1、正常品且日销大于0.1不降价
    c1 = ((df['sales_status'].isin(['正常'])) & (df['is_up'] == '降价') & (df['day_sales'] > 0.3))
    c2 = (df['available_stock'] == 0) | (df['best_warehouse_name'].isin([ 'XYD泰国海外仓','XYD菲律宾海外仓','XYD马来海外仓']))
    # 1.2 待销毁数据不调价
    df_destroy = get_wait_destroy()
    df = pd.merge(df, df_destroy[['sku','warehouse','is_wait_destroy']], how='left', on=['sku','warehouse'])
    c3 = (~df['is_wait_destroy'].isna())
    # df['is_normal_cdt'] = np.where(df[c1], 1, 0)
    df['is_normal_cdt'] = np.where(c1 | c2 | c3, 1, 0)
    df.drop('is_wait_destroy', axis=1, inplace=True)
    #
    # 4、不调价账号
    # account_id = [26696,28411,28416,28417,28418,28831,29396,29422,32076,35054,35055,35056]
    # sku_list = ['I2214']
    # c1 = (df['sku'].isin(sku_list))
    # df['is_white_account'] = np.where(c1, 1, 0)
    df['is_white_account'] = 0

    # 5、目标价与当前链接价差不超过0.3或价格变化率小于1%
    c1 = (df['online_price'] - df['price']).abs() <= 0.3
    c2 = ((df['price'] - df['online_price'])/df['online_price']).abs() <= 0.01
    df['is_small_diff'] = np.where(c1 | c2, 1, 0)

    # # 6、暂时剔除泰国仓的链接
    # df = df[df['warehouse'] != '泰国仓']

    df['date_id'] = now_time
    df['account_id'] = df['account_id'].astype(int)
    col = ['online_profit','price','target_profit_rate','profit_diff','rate']
    df[col] = df[col].astype(float).round(4)
    # df.drop(['lowest_price'], axis=1, inplace=True)

    print(df.info())
    # df.to_excel('F://Desktop//df_dtl.xlsx', index=0)
    print('全量lazada海外仓链接存表...')
    write_to_ck(df, 'oversea_lazada_listing_all')

    # shopee调价数据
    get_shopee_adjust_listing()

def get_mx_listing():
    """
    获取墨西哥仓在Amazon的链接
    """
    sql = """

    SELECT
        sku, cargo_owner_id, warehouse_id, available_stock
    FROM yb_datacenter.yb_stock
    WHERE date_id = 20231012 and warehouse_id = 820 and cargo_owner_id = 8 and available_stock > 0
    """
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = ck_client.ck_select_to_df(sql)
    sku_list = tuple(df_sku['sku'].unique())
    print(len(sku_list))
    sql = """
    with listing_table as (
        select distinct account_id, seller_sku 
        from yibai_product_kd_sync.yibai_amazon_sku_map 
        where sku in {}
    )

    select b.account_id as account_id, b.account_name as account_name, 
        short_name, 'AMAZON' as platform,
        if(b.site ='sp', 'es', b.site) as site,
        status, e.sku as sku, a.seller_sku as seller_sku,  
        open_date, deliver_mode,
        a.asin1 as asin,
        a.price AS your_price, fulfillment_channel, a.price as online_price
    from (
        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        from yibai_product_kd_sync.yibai_amazon_listings_all_raw2 
        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a 
    inner join (
        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from yibai_system_kd_sync.yibai_amazon_account b
        where account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163 and site = 'mx'
    ) b
    on (a.account_id= b.account_id)
    inner join ( 
        select account_id, sku, seller_sku, deliver_mode
        from yibai_product_kd_sync.yibai_amazon_sku_map
        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) e
    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    settings max_memory_usage = 30000000000
    """.format(sku_list)
    df_amazon_listing = ck_client.ck_select_to_df(sql)

    print(df_amazon_listing.info())
    print(df_amazon_listing.deliver_mode.value_counts())

## 调价数据同步CK
def mysql_to_ck():
    """  mysql数据表迁移CK """
    # 按日期同步插入
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_start = '2024-12-31'
    # 取近30天日期
    sql = f"""
        SELECT distinct date_id
        FROM over_sea.oversea_ebay_listing_all
        WHERE date_id < '2025-01-13' and date_id >= '{date_start}'
        ORDER BY date_id DESC
    """
    date_list = conn.read_sql(sql)
    conn.close()

    for i in tqdm.tqdm(date_list['date_id'].unique()):
        # print(i)
        sql = f"""
            SELECT *
            FROM over_sea.oversea_ebay_listing_all
            WHERE date_id = '{i}'
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        mysql_data = conn.read_sql(sql)
        conn.close()
        print(mysql_data.info())
        mysql_data['account_id'] = mysql_data['account_id'].astype(int)
        conn_ck.ck_insert(mysql_data, 'oversea_ebay_listing_all', if_exist='append')

    return None


##
if __name__ == '__main__':
    get_amazon_adjust_listing()
    # get_listing_day_sales_new()
    get_ebay_adjust_listing()
    # get_walmart_adjust_listing()
    get_cd_adjust_listing()
    # amazon_bundle()
    # test()
    # get_allegro_adjust_listing()
    # mysql_to_ck()
    # get_shopee_adjust_listing()
    # get_lazada_adjust_listing()
    # get_listing_day_sales_new(platform='EB')
    # haitu_sku_price()
    # get_lazada_TH_dtl()
    # get_normal_bad_orders(platform='EB')
    # get_fba_asin()
