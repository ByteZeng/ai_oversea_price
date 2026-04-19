##
import pandas as pd
import numpy as np
import os
import pulic_func.base_api.upload_zip as uz
import pymysql
from all_auto_task.scripts_ck_client import CkClient
##
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck, get_ck_conf_data


# from pulic_func.base_api.all_freight_interface import *
# from pulic_func.adjust_price_base_api.public_function import *
# import warnings
# import time, datetime
# warnings.filterwarnings("ignore")

##

##
# amazon会员日SKU定价
##
def get_listing_lowest_price():
    ## 获取SKU链接信息
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='support_document')

    # 动销品链接的历史价格获取
    df_listing_price_all = pd.DataFrame()
    for i in range(20230906, 20230922):
        table_name = 'domestic_warehouse_clear_amazon_listing_' + str(i)
        print(table_name)
        try:
            sql = f"""
            SELECT fp.account_id, fp.seller_sku, fp.asin, fp.sku, fp.your_price, aps.amazon_listing_num, '{i}' as date_id
            FROM {table_name} fp
            INNER JOIN amazon_prime_sku_0921 aps
            ON fp.sku = aps.sku
            """
            df_listing_price = ck_client.ck_select_to_df(sql)
            # 取历史最低的价格，account_id + seller_sku 维度
            df_listing_price_all = pd.concat([df_listing_price, df_listing_price_all])
            df_listing_price_all = df_listing_price_all.sort_values(by=['account_id', 'seller_sku', 'your_price'],
                                                                    ascending=True).drop_duplicates(
                subset=['account_id', 'seller_sku'])
        except:
            pass

    # 存入sql
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_listing_price_all, table_name, if_exists='replace')
    conn.close()


##
# 取临时表数据
sql = """
SELECT al.*
FROM amazon_prime_sku_listing_info_temp al
INNER JOIN (
    SELECT sku 
    FROM amazon_prime_sku_0921 
    WHERE available_stock >= 5) ap
ON al.sku = ap.sku
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_amazon_listing = conn.read_sql(sql)
## 获取链接价格
sql = """
SELECT account_id, seller_sku, sku, asin, your_price, `站点`,rate,`运费` as total_cost, new_price
FROM amazon_prime_sku_listing_info

"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_amazon_listing = conn.read_sql(sql)
##
df_amazon_listing = df_amazon_listing[~df_amazon_listing['total_cost'].isna()]
## 反算利润率
# 获取国家
sql = """
SELECT 
    yac.id account_id,  short_name, yac.site site, st.site1 country_code ,rate, 
    yac.group_id group_id, account_num,group_name, FBA_profit, FBA_difference,
    FBM_profit, FBM_difference
FROM yibai_system_kd_sync.yibai_amazon_account yac
LEFT JOIN yibai_system_kd_sync.yibai_amazon_group yag
ON yac.group_id = yag.group_id
LEFT JOIN domestic_warehouse_clear.site_table st
ON yac.site = st.site
LEFT JOIN (
    SELECT
        site, argMax(rate, date_archive) as rate
    FROM domestic_warehouse_clear.erp_rate
    GROUP BY site
) er
ON st.site1 = er.site
"""
ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                     db_name='yibai_system_kd_sync')
df_site_info = ck_client.ck_select_to_df(sql)


## 税率
def cj_vat(df):
    df['税率'] = 0
    # 英国VAT税率,英国VAT税率20%，在定价中计算是 0.2/（1+0.2） = 0.16
    df.loc[df['站点'] == '英国', '税率'] = 0.1667
    df.loc[df['站点'] == '德国', '税率'] = 0.1597
    df.loc[df['站点'] == '法国', '税率'] = 0.1667
    df.loc[df['站点'] == '意大利', '税率'] = 0.1803
    df.loc[df['站点'] == '西班牙', '税率'] = 0.1736
    df.loc[df['站点'] == '荷兰', '税率'] = 0.1736
    df.loc[df['站点'] == '瑞典', '税率'] = 0.2
    df.loc[df['站点'] == '波兰', '税率'] = 0.187
    df.loc[df['站点'] == '比利时', '税率'] = 0.1736
    df.loc[df['站点'] == '印度', '税率'] = 0.3
    # 20211104 土耳其关税由28%改为15%，（申报50% * 关税率30% = 15%）
    df.loc[df['站点'] == '土耳其', '税率'] = 0.15
    df.loc[df['站点'] == '澳大利亚', '税率'] = 0.09
    df.loc[df['站点'] == '巴西', '税率'] = 0.02

    return df


##
# 评分不足3.5分的 asin + account_id
sql = """

SELECT accountid as account_id, asin, site, total_stars, crawl_time
FROM yibai_amazon_reviewspider_items
WHERE is_latest = 1

"""
conn = connect_to_sql(database='yibai_product', data_sys='外网')
df_stars = conn.read_sql(sql)
##
df_amazon_listing = pd.merge(df_amazon_listing, df_stars[['account_id', 'asin', 'total_stars']], how='left',
                             on=['account_id', 'asin'])
##
df_amazon_listing['total_stars'] = df_amazon_listing['total_stars'].astype(float)
df_amazon_listing = df_amazon_listing[
    (df_amazon_listing['total_stars'].isna()) | (df_amazon_listing['total_stars'] >= 3.5)]
##
df_amazon_listing = pd.merge(df_amazon_listing, df_site_info[['account_id', 'group_name', 'FBM_difference']],
                             how='left', on='account_id')
df_amazon_listing = cj_vat(df_amazon_listing)

## 反算利润率
df_amazon_listing['new_price'] = df_amazon_listing['new_price'].astype(float)
df_amazon_listing['total_cost'] = df_amazon_listing['total_cost'].fillna(0).astype(float)
df_amazon_listing['gross_profit'] = 1 - 0.18 - df_amazon_listing['税率'] - \
                                    (df_amazon_listing['new_price'] + df_amazon_listing['total_cost']) / (
                                                df_amazon_listing['your_price'] * df_amazon_listing['rate'])

## 参考价及参考价的利润率
df_amazon_listing['target_price'] = df_amazon_listing['your_price'] * 0.8
df_amazon_listing['target_gross_profit'] = 1 - 0.18 - df_amazon_listing['税率'] - \
                                           (df_amazon_listing['new_price'] + df_amazon_listing['total_cost']) / (
                                                       df_amazon_listing['target_price'] * df_amazon_listing['rate'])

##
# df_amazon_listing['net_profit'] = df_amazon_listing['gross_profit'] - df_amazon_listing['FBM_difference']

# df_amazon_listing.sample(100000).to_excel('df_amazon_listing.xlsx', index=0)
##
# 取四维差值

# 一级产品线
sql = """
select distinct a.sku as sku,c.category_name as `一级产品线`
from yibai_prod_base_sync.yibai_prod_sku a 
left join yibai_prod_base_sync.yibai_prod_category b 
on a.product_category_id=b.id
left join yibai_prod_base_sync.yibai_prod_category c 
on b.category_id_level_1=c.id
"""
ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                     db_name='yibai_prod_base_sync')
df_sku_cat = ck_client.ck_select_to_df(sql)

##
df_amazon_listing = pd.merge(df_amazon_listing, df_sku_cat, how='left', on='sku')


##
def get_4_difference(df, type1='FBA'):
    df.rename(columns={'new_price': '成本'}, inplace=True)
    # df.rename(columns={'site': '站点'}, inplace=True)
    # type: 'FBA' 'FBM'  'ALL'
    sql = """
    SELECT case when a.shipping_type =1 THEN 'FBM' WHEN 3 THEN 'FBA' END AS `渠道`,
    case when a.site = 'other' then 'other' else c.site1  end as `站点`,
    case when b.category_name ='' then 'other' else b.category_name end as `一级产品线`,a.cost_range `成本段`,
    toFloat64(net_profit2) `毛净利差值`,true_refund_profit
    FROM yibai_sale_center_listing_sync.yibai_listing_profit_config a
    left join yibai_prod_base_sync.yibai_prod_category b on a.first_product_line = b.id
    left join domestic_warehouse_clear.site_table c on a.site = UPPER(c.site) 
    where a.platform_code ='AMAZON' AND a.shipping_type in (1,3)  and a.is_del = 0 and a.status = 1
    HAVING `站点` <> ''
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='domestic_warehouse_clear')
    cha = ck_client.ck_select_to_df(sql)
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # cha = conn_mx.ck_select_to_df(sql)
    cha['毛净利差值'] = cha['毛净利差值'] / 100
    cha = cha[cha['渠道'] == type1]
    cha = cha.drop_duplicates(['站点', '一级产品线', '成本段'])
    cha.drop(['渠道'], axis=1, inplace=True)

    cha1 = cha.loc[(cha['成本段'] == 'other'), ['站点', '一级产品线', '毛净利差值']].drop_duplicates(
        ['站点', '一级产品线'])
    cha1.rename(columns={'毛净利差值': '毛净利差值1'}, inplace=True)

    cha2 = cha.loc[(cha['一级产品线'] == 'other') & (cha['成本段'] == 'other'), ['站点', '毛净利差值']].drop_duplicates(
        ['站点'])
    cha2.rename(columns={'毛净利差值': '毛净利差值2'}, inplace=True)
    if type1 == 'FBA':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 20) & (df['成本'] > 0), '成本段'] = '0,20'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 20), '成本段'] = '20,100'
        df.loc[df['成本'] > 100, '成本段'] = '100,100000'
    elif type1 == 'FBM':
        df['成本段'] = 'other'
        df.loc[(df['成本'] <= 100) & (df['成本'] > 0), '成本段'] = '0,100'
        df.loc[(df['成本'] <= 300) & (df['成本'] > 100), '成本段'] = '100,300'
        df.loc[df['成本'] > 300, '成本段'] = '300,100000'
    df = df.merge(cha, on=['站点', '一级产品线', '成本段'], how='left')
    df = df.merge(cha1, on=['一级产品线', '站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值1']
    df = df.merge(cha2, on=['站点'], how='left')
    df.loc[df['毛净利差值'].isnull(), '毛净利差值'] = df['毛净利差值2']
    df.drop(['毛净利差值1', '毛净利差值2'], axis=1, inplace=True)
    return df


##
# df_amazon_listing.drop('毛净利差值', axis=1, inplace=True)
df_amazon_listing = get_4_difference(df_amazon_listing, type1='FBM')

##
df_amazon_listing['net_profit'] = df_amazon_listing['gross_profit'] - df_amazon_listing['毛净利差值']

## 判断是否达到销毁价
df_amazon_listing['true_refund_profit'] = df_amazon_listing['true_refund_profit'].fillna(1.45).astype(float)
# df_amazon_listing[df_amazon_listing['true_refund_profit'].isna()].info()

df_amazon_listing['lowest_price'] = df_amazon_listing['total_cost'] / (
            1 - 0.18 - df_amazon_listing['税率'] - df_amazon_listing['true_refund_profit'] / 100 - 0.1)
df_amazon_listing['lowest_price'] = df_amazon_listing['lowest_price'] / df_amazon_listing['rate']

df_amazon_listing['is_lowest_1'] = np.where(df_amazon_listing['your_price'] >= df_amazon_listing['lowest_price'], 1, 0)
df_amazon_listing['is_lowest_2'] = np.where(df_amazon_listing['target_price'] >= df_amazon_listing['lowest_price'], 1,
                                            0)

##
##
##
columns_order = ['account_id', 'seller_sku', 'sku', 'asin', 'group_name', 'total_stars', '站点', 'rate', 'total_cost',
                 '成本', '税率', '成本段', '一级产品线',
                 '毛净利差值', 'your_price', 'gross_profit', 'net_profit', 'target_price', 'target_gross_profit',
                 'true_refund_profit', 'lowest_price',
                 'is_lowest_1', 'is_lowest_2']
df_amazon_listing = df_amazon_listing[columns_order]
df_amazon_listing.columns = ['account_id', 'seller_sku', 'sku', 'asin', 'group_name', '星级评分', '站点', '汇率',
                             '运费', '成本', '税率', '成本段', '一级产品线',
                             '四维差值', '近12天最低价', '近12天最低价对应毛利率', '近12天最低价对应净利率',
                             '参考价（20%折扣定价）', '参考价对应毛利率', '退款率',
                             '销毁价', '当前价不低于销毁价', '参考价不低于销毁价']
df_amazon_listing.info()

##
five_group = ['武汉7组', '武汉10组', '武汉15组', '武汉25组', '武汉35组', '武汉37组', '武汉45组']
df_amazon_listing_temp = df_amazon_listing[df_amazon_listing['group_name'].isin(five_group)]
df_amazon_listing_temp = df_amazon_listing_temp[df_amazon_listing_temp['参考价不低于销毁价'] == 1]
df_amazon_listing_temp.to_excel('df_amazon_listing_five_group.xlsx', index=0)
##
df_amazon_listing[df_amazon_listing['group_name'].isin(five_group)].to_excel('df_amazon_listing_five_group_all.xlsx',
                                                                             index=0)

## 订单数据。（确认并验证当前定价实际的应用情况）
sql = """
with order_table as (
    SELECT distinct order_id 
    from yibai_oms_sync.yibai_oms_order_sku yo
    WHERE create_time>='2023-09-10 00:00:00' 
        AND sku in (
            SELECT sku
            FROM support_document.amazon_prime_sku_0921 
            WHERE available_stock >= 5)

)
SELECT 
    a.order_id as order_id,
    a.purchase_time as purchase_time,
    a.platform_code as platform_code,
    a.currency as currency,
    a.account_id as account_id,
    b.asinval as asin,
    b.item_id as item_id,
    b.seller_sku seller_sku,
    toFloat64(c.total_price) as total_price,
    toFloat64(c.product_price) as product_price,    
    toFloat64(c.true_profit_new1) as profit,
    toFloat64(c.profit_rate) as profit_rate,    
    toFloat64(c.shipping_price) as shipping_price,
    toFloat64(c.shipping_cost) as shipping_cost,
    toFloat64(c.currency_rate) as currency_rate,
    c.is_sale_promotion is_sale_promotion,
    c.seller_rebate seller_rebate, 
    c.seller_discount seller_discount
FROM  (
    select order_id, seller_sku, asinval, item_id
    from yibai_oms_sync.yibai_oms_order_detail
    where order_id in (select order_id from order_table)
) b 
inner JOIN (
    select order_id, purchase_time, platform_code, currency, account_id
    from yibai_oms_sync.yibai_oms_order
    where order_id in (select order_id from order_table)
        AND platform_status not in ('Canceled', 'Pending') 
        AND refund_status in (0, 1) AND (order_status=70 OR ship_status=2) 
        AND order_id not like '%%-RE' 
        AND purchase_time>='2023-09-10 00:00:00' 
        and platform_code!='SPH' AND total_price!=0
        and platform_code = 'AMAZON'
) a 
ON b.order_id=a.order_id 
inner JOIN (
    select * from yibai_oms_sync.yibai_oms_order_profit
    where order_id in (select order_id from order_table)
) c 
ON a.order_id=c.order_id 
"""
ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                     db_name='yibai_prod_base_sync')
df_order = ck_client.ck_select_to_df(sql)

##
# 订单数据匹配连接信息
order_columns = ['account_id', 'seller_sku', 'asin', 'group_name', '近12天最低价', '近12天最低价对应毛利率',
                 '近12天最低价对应净利率', '参考价（20%折扣定价）', '参考价对应毛利率']
df_order_info = pd.merge(df_order, df_amazon_listing[order_columns], how='left',
                         on=['account_id', 'seller_sku', 'asin'])
##
df_order_info.to_excel('df_order_info.xlsx', index=0)
## 数据分发
f_path = os.path.dirname(os.path.abspath('.'))  # 文件当前路径
f_path1 = os.path.join(f_path, 'amazon_prime_price')
f_path_ZIP = os.path.join(f_path1, 'AMAZON', '2023-09-25', '会员折扣参考价')
if not os.path.exists(f_path_ZIP):
    os.makedirs(f_path_ZIP)
##
for item, group in df_amazon_listing_temp.groupby(['group_name']):
    excel_name = os.path.join(f_path_ZIP, f'测试-国内仓目标SKU在Amazon会员折扣参考价{item}.xlsx')
    # group.to_excel(excel_name, sheet_name=f'{item}', index=False)
    if group.shape[0] < 1048576:
        group.to_excel(excel_name, sheet_name=f'{item}', index=False)
    else:
        group = group.reset_index(drop=True)
        group['index'] = group.index
        group['index'] = group['index'].apply(lambda m: int(m / 1000000))

        writer = pd.ExcelWriter(excel_name)
        for key, sub_group in group.groupby(['index']):
            sub_group.drop(['index'], axis=1, inplace=True)
            sub_group.to_excel(writer, sheet_name=f'{item}{key}', index=False)
        writer.save()
        writer.close()
##
uz.upload_zip(f_path1, 'AMAZON', '209313')
##
# conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
# conn.to_sql(df_amazon_listing, 'amazon_prime_sku_listing_info_temp', if_exists='replace')
##
df_amazon_listing.info()
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
##
conn.to_sql(df_amazon_listing, 'amazon_prime_price_0925', if_exists='replace')
##
if __name__ == '__main__':
    get_listing_lowest_price()