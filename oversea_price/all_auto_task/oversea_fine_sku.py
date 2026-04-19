"""

海外仓精品转泛品调价
"""
import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, sku_and_num_split
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate
from all_auto_task.oversea_listing_detail_2023 import get_amazon_listing_data, get_price_data, get_platform_fee
from concurrent.futures import ThreadPoolExecutor, as_completed
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse
warnings.filterwarnings("ignore")

##
def get_fine_sku():
    """  """
    # df = pd.read_excel('F://Desktop//精铺转泛品第一批20250122.xlsx', dtype={'sku':str,'warehouse_id':int})
    # print(df.info())

    sql = """
        SELECT sku, type, best_warehouse_id warehouse_id, best_warehouse_name warehouse_name, warehouse, 
        available_stock, new_price, overage_level, age_90_plus,age_120_plus, age_150_plus,age_180_plus
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and best_warehouse_name like '%%精铺%%'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df.info())

    return df

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

def get_interf_fee(df):
    """ 调用接口获取sku的运费 """
    df = df[['sku', 'country','best_warehouse_id','best_warehouse_name', 'warehouse']].drop_duplicates()
    df = df.rename(columns={'best_warehouse_id':'warehouse_id'})
    #
    df['数量'] = 1
    df = df.reset_index(drop=True).reset_index()
    # dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
    #        'AU': '353,769','CZ': '325','PL': '325'}
    # df['warehouse_id'] = df['country'].replace(dic)

    # df_bundle = df_bundle.sample(1000)
    df_result = sku_and_num_split(df)
    # df_result = df_result[df_result['warehouse']=='加拿大仓']
    print(df_result.info())
    # df_result.to_excel('F://Desktop//df_num_split.xlsx', index=0)
    # test
    # df_result = df_result[df_result['sku'] == 'YM888TM01-24-CE001-GG-0032']
    n = 0
    df_fee = pd.DataFrame()
    while n < 2:
        print(f'调用运费计算接口，未取到的最多重复5次调用，第{n}次拉取...')
        threadPool = ThreadPoolExecutor(max_workers=1)
        thread_list = []
        df_result = df_result.reset_index(drop=True)
        df_result['index'] = df_result.index
        # df_result['index'] = df_result['index'].apply(lambda m_data: int(m_data / 100))
        for key, group in df_result.groupby(['index']):
            future = threadPool.submit(interf_fuc, group)
            dataframe = future.result()
            thread_list.append(dataframe)
        # dataframes = []
        # # 遍历完成的任务，并收集结果
        # for future in as_completed(thread_list):
        #     dataframe = future.result()
        #     dataframes.append(dataframe)
        df_result_temp = pd.concat(thread_list, ignore_index=True)
        # df_result_temp = interf_fuc(df_result)
        df_result_temp = df_result_temp[['sku', 'shipCountry', 'warehouseId','warehouseName',  'shipName', 'totalCost','shippingCost','firstCarrierCost',
             'dutyCost','overseasFee','extraSizeFee','remoteExtraFee']]
        df_result_temp = df_result_temp.rename(columns={'warehouseId':'best_warehouse_id','warehouseName':'best_warehouse_name'})
        is_all = df_result[~df_result['sku'].isin(df_result_temp['sku'].unique())]
        df_fee = pd.concat([df_result_temp, df_fee])
        if len(is_all) > 0:
            print(f'还有{len(is_all)}个sku未拉取到...')
            df_result = is_all.copy()
            n = n + 1
        else:
            break

    df_fee = df_fee.drop_duplicates(subset=['sku', 'best_warehouse_id', 'shipCountry'])
    # 大仓库名补充
    df = df.rename(columns={'warehouse_id':'best_warehouse_id'})
    df_result = pd.merge(df_fee, df[['best_warehouse_id', 'warehouse']].drop_duplicates(), how='left', on='best_warehouse_id')
    # 存表
    df_result['date_id'] = time.strftime('%Y-%m-%d')
    df_result.to_excel('F://Desktop//df_result_fee_test.xlsx', index=0)
    # write_to_sql(df_result, 'fine_sku_fee_useful')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'fine_sku_fee_useful', if_exists='replace')
    return df

def interf_fuc(df):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        # print(key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea('AMAZON', key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost','dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost','dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])
    # , 'dutyCost', 'overseasFee', 'extraSizeFee', 'remoteExtraFee'
    return df_result

def get_ebay_listing(df):
    """ 获取当前ebay平台刊登的链接 """
    date_today = time.strftime('%Y-%m-%d')
    sku_list = df['sku'].to_list()
    print(df.info())
    df_ebay_listing_all = pd.DataFrame()
    for n in range(0, len(sku_list), 5000):
        sku = sku_list[n:n + 5000]
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
                FROM domestic_warehouse_clear.site_table_ebay 
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
    print(df_ebay_listing_all['name'].value_counts())
    # df_ebay_listing_all.to_excel('F://Desktop//df_ebay_fine.xlsx', index=0)

    return df_ebay_listing_all

def get_amazon_listing(df):
    """ 获取当前ebay平台刊登的链接 """
    sku_list = tuple(df['sku'].unique())
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM over_sea.yibai_amazon_oversea_listing_price
        WHERE `DATE` = '{date_today}' and sku in {sku_list}
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_amazon_fine = conn.read_sql(sql)
    print(df_amazon_fine.info())

    df_amazon_fine.to_excel('F://Desktop//df_amazon_fine.xlsx', index=0)

    return None

def get_amazon_listing_all(df):
    """ 获取当前ebay平台刊登的链接 """
    sku_list = tuple(df['sku'].unique())
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM yibai_oversea.oversea_amazon_listing_all
        WHERE `DATE` = '{date_today}' and sku in {sku_list}
    """
    conn_ck = pd_to_ck(database='yibai_oversae', data_sys='调价明细历史数据')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_amazon_fine = conn.read_sql(sql)
    df_amazon_fine = conn_ck.ck_select_to_df(sql)
    print(df_amazon_fine.info())

    df_amazon_fine.to_excel('F://Desktop//df_amazon_fine_all.xlsx', index=0)

    return None

def dwm_sku_info():
    """
    精铺转泛品sku：库存、库龄
    """
    df = get_fine_sku()
    dic = {'美国':'美国仓', '德国':'德国仓', '英国':'英国仓', '澳大利亚':'澳洲仓', '加拿大':'加拿大仓'}
    df['warehouse'] = df['国家'].map(dic)
    print(df['warehouse'].value_counts())


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
    delete from over_sea.{table_name} where date_id='{date_id}'
    """
    print(sql)
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

def dwm_sku_price():
    """ 精铺转泛品定价数据 """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}' and best_warehouse_name like '%%精铺%%'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    dic = {'美国仓':'US', '德国仓':'DE', '英国仓':'UK','澳洲仓':'AU','加拿大仓':'CA'}
    df['country'] = df['warehouse'].replace(dic)
    print(df.info())
    # df_sku_base = get_fine_sku()
    # df_sku_base = df_sku_base.rename(columns={'warehouse_name':'best_warehouse_name'})
    # col = ['sku','best_warehouse_name','Amazon当前售价', 'country']
    # df = pd.merge(df, df_sku_base[col], how='left', on=['sku','best_warehouse_name'])
    # print(df.info())
    #
    # # 汇率
    # df_rate = get_rate()
    # df = pd.merge(df, df_rate[['country','rate']], how='left', on='country')

    # 接口拉取精品sku的运费
    df_fee = get_interf_fee(df)
    # sql = f"""
    #     SELECT sku, best_warehouse_id, shipCountry, shipName, totalCost,
    #     shippingCost, firstCarrierCost
    #     FROM over_sea.fine_sku_fee_useful
    #     WHERE date_id = '{date_today}'
    # """
    # df_fee = conn.read_sql(sql)
    # df_fee['best_warehouse_id'] = df_fee['best_warehouse_id'].astype(int)
    # df = pd.merge(df, df_fee, how='left', on=['sku','best_warehouse_id'])
    #
    # # 差值
    # df_platform_fee = get_platform_fee()
    # col = ['country','ppve','platform_zero','platform_must_percent']
    # df = pd.merge(df, df_platform_fee[col], how='left', on=['country'])
    #
    # # 精铺链接当前利润率
    # col = ['new_price', 'totalCost', 'Amazon当前售价','ppve','platform_zero','rate']
    # df[col] = df[col].fillna(0).astype(float)
    # print(df.info())
    # df['limit_profit'] = 1 - df['ppve'] - df['platform_zero'] - (df['new_price']+df['totalCost'])/df['Amazon当前售价']/df['rate']
    # df['limit_profit'] = df['limit_profit'].replace({np.inf:999, -np.inf:999})
    # # df['date_id'] = date_today
    # write_to_sql(df, 'dwm_fine_sku_info')

    # conn.to_sql(df, 'dwm_fine_sku_info', if_exists='replace')

    # df.to_excel('F://Desktop//df_fine_sku.xlsx', index=0)

    return df
def dwm_sku_price_temp():

    df = get_fine_sku()
    # 汇率
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='left', on='country')

    # 接口拉取精品sku的运费
    df_fee = get_interf_fee(df)
    df = pd.merge(df, df_fee, how='left', on=['sku','warehouse_id'])

    # 获取库存和库龄
    sql = """
        SELECT sku, warehouse, warehouse_id, available_stock
        FROM yb_datacenter.v_oversea_stock
        WHERE warehouse_other_type != 3
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df, df_stock, how='left', on=['sku','warehouse_id'])

    # 差值
    df_platform_fee = get_platform_fee()
    col = ['country','ppve','platform_zero','platform_must_percent']
    df = pd.merge(df, df_platform_fee[col], how='left', on=['country'])

    # 暂无库龄数据
    # df_age = get_oversea_age()
    # col = ['sku','warehouse','warehouse_stock','inventory_age','overage_level']
    # df = pd.merge(df, df_age[col], how='left', on=['sku','warehouse'])

    # df.to_excel('F://Desktop//df_fine_sku_0.xlsx', index=0)
    return df
def temp():
    df_platform_fee = get_platform_fee()
    df_platform_fee.to_excel('F://Desktop//df_platform_fee.xlsx', index=0)

def get_ebay_adjust_listing():
    """ 获取ebay精品链接 """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM over_sea.dwm_fine_sku_info
        WHERE date_id = '{date_today}' and totalCost > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku = conn.read_sql(sql)
    # df_sku = dwm_sku_price()
    print(df_sku.info())
    col = ['sku', 'warehouse', 'best_warehouse_name', 'best_warehouse_id', 'rate', 'shipName', 'totalCost',
           'shippingCost', 'firstCarrierCost', 'available_stock', 'overage_level', 'day_sales', 'after_profit',
           'limit_profit']
    df_sku = df_sku[col]
    df_ebay_listing = get_ebay_listing(df_sku)
    df_ebay_listing = df_ebay_listing.rename(columns={'name':'warehouse'})
    print(df_ebay_listing.info())
    df = pd.merge(df_ebay_listing, df_sku, how='left', on=['sku','warehouse'])

    # 计算定价


    # df.to_excel('F://Desktop//df_fine.xlsx', index=0)



if __name__ == '__main__':
    df = get_fine_sku()
    # df_age = get_fine_sku_age()
    #
    # df.to_excel('F://Desktop//df_fine_sku.xlsx', index=0)
    # df_age.to_excel('F://Desktop//df_fine_age.xlsx', index=0)
    # temp()
    # dwm_sku_info()
    # dwm_sku_price()

    # get_ebay_adjust_listing()
    # df = get_fine_sku()
    # get_ebay_listing(df)
    # get_amazon_listing(df)
    # get_amazon_listing_all(df)