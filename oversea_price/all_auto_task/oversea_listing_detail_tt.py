
# TT海外仓调价链接检查、上传、监控

##
import pandas as pd
import numpy as np
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
import traceback
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import as_completed
import json
import requests
import hashlib
from all_auto_task.oversea_adjust_upload_2023 import get_shopee_precision
from all_auto_task.oversea_listing_price import tt_listing_info
from all_auto_task.oversea_price_adjust_tt import tt_get_platform_fee, tt_get_sku,tt_get_warehouse,get_rate
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea

# from concurrent.futures._base import as_completed
# from pulic_func.base_api.base_function import get_token
# import requests
warnings.filterwarnings("ignore")
##
# 获取ttsku


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


## ebay调价数据上传
def data_section(df, col='price_up_range'):

    new_column = col + '_section'
    if col == 'price_up_range':
        df[new_column] = pd.cut(df[col], bins=[-1000, -1, -0.5, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2, 0.5, 1, 10000],
                                labels=['A:<-1', 'B:(-1,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.1]', 'E:(-0.1,-0.05]', 'F:(-0.05,0]', 'G:(0,0.05]',
                                        'H:(0.05,0.1]', 'I:(0.1,0.2]', 'J:(0.2,0.5]', 'K:(0.5,1]', 'L:>1'])
    elif col == 'profit_diff':
        df[new_column] = pd.cut(df[col], bins=[-1000, -0.3, -0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.3, 1000],
                                labels=['a:<-0.3', 'b:(-0.3,-0.15]', 'c:(-0.15,-0.1]', 'd:(-0.1,-0.05]', 'e:(-0.05,0]', 'f:(0,0.05]', 'g:(0.05,0.1]',
                                        'h:(0.1,0.15]', 'i:(0.15,0.3]', 'j:>0.3'])

    return df

def get_account():
    """
    取账号
    """
    sql = """
    select distinct id,account_id,account_name, short_name 
    from tt_sale_center_system_sync.tt_system_account
    where platform_code='EB' and status=1 and is_del=0
    """
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    df_account= conn_ck.ck_select_to_df(sql)

    return df_account

def tt_get_sales_status():
    """ 获取tt清仓sku的销售状态 """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT sku, warehouse
        FROM tt_oversea_sale_status_temp
        WHERE end_time IS NULL AND start_time <= '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)

    return df_status

def get_limit_price(platform='EB'):
    """ TT限价表信息 """
    sql = f"""
        SELECT site, sku, price limit_price
        FROM domestic_warehouse_clear.plat_sku_limit_price
        WHERE plat = '{platform}' and site != '中国'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    df_limit = conn_ck.ck_select_to_df(sql)

    dic = {'澳大利亚':'AU', '美国':'US', '德国':'DE', '法国':'FR', '英国':'UK', '加拿大':'CA', '意大利':'IT', '西班牙':'ES'}
    df_limit['country'] = df_limit['site'].map(dic).fillna(' ')

    # df_limit.to_excel('F://Desktop//df_limit.xlsx', index=0)

    return df_limit

def check_ebay_adjust():
    """
    检查ebay待调价链接
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-02-19'
    # 取有运费链接
    sql = f"""
        SELECT
            a.sku, item_id, account_id, site, a.country, name, start_price, shipping_fee, online_price,
            online_profit online_gross_profit, date_id, best_warehouse_name, a.warehouse, new_price,
            total_cost, a.ship_name,  a.rate,available_stock, fixed_fee,
            sales_status,price, ppve, platform_zero, platform_must_percent,
            target_profit_rate target_gross_profit, price_rmb, target_profit_rate, profit_diff,
            is_supplier, is_supplier_price
        FROM (
            SELECT *
            FROM over_sea.tt_oversea_ebay_listing_all 
            WHERE date_id = '{date_today}' and total_cost > 0
            and listing_status = 1
            -- and a.warehouse = '英国仓'
        ) a

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_ebay = conn.read_sql(sql)
    print(f'链接数量共{len(df_ebay)}条.')
    sql = f"""
        SELECT sku, warehouse, country, overage_level, shippingCost, firstCarrierCost
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = '{date_today}' and platform = 'EB'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)
    df_ebay = pd.merge(df_ebay, df_dtl, how='left', on=['sku','warehouse','country'])
    col = ['start_price','shipping_fee']
    df_ebay[col] = df_ebay[col].fillna(0).astype(float)
    df_account = get_account()
    df_ebay = pd.merge(df_ebay, df_account[['account_id','account_name','short_name']], how='left', on=['account_id'])
    df_ebay = df_ebay.drop_duplicates()
    df_ebay['is_up'] = np.where(df_ebay['price'] > df_ebay['online_price'], 1, 0)
    df_ebay['price_up_range'] = (df_ebay['price']/df_ebay['online_price'] - 1).round(4)
    df_ebay = data_section(df_ebay, 'price_up_range')
    df_ebay = data_section(df_ebay, 'profit_diff')

    # 读表上传
    # df_ebay = pd.read_excel('F://Desktop//df_ebay_upload.xlsx')
    # 筛选预计调价数据
    # df_ebay = df_ebay[(df_ebay['is_up']==1) & (df_ebay['warehouse']!='澳洲仓')
    #                   & (df_ebay['sales_status']=='正常')]

    df_ebay = df_ebay[~((df_ebay['price_up_range'] <= 0.01) & (df_ebay['price_up_range'] >= -0.01))]
    df_ebay['shipping_fee'] = 0
    df_ebay['start_price_target'] = df_ebay['price'] - df_ebay['shipping_fee']
    df_ebay['start_price_target'] = np.where(df_ebay['start_price_target'] <= 0.1, 0.1, df_ebay['start_price_target'])
    df_ebay['target_gross_profit'] = df_ebay['target_gross_profit']*100
    df_ebay = df_ebay.rename(columns={'shipping_fee':'shipping_service_cost','target_gross_profit':'target_profit'})
    df_ebay.drop(['target_profit_rate'], axis=1, inplace=True)
    # df_ebay['date_id'] = date_today
    df_ebay['date_id'] = time.strftime('%Y-%m-%d')
    print(df_ebay.info())

    # 调价日期
    today = datetime.datetime.now().weekday()
    # if (today == 1 or today == 2 or today == 4) :
    #     print("今天是周二、周三或周五，只保留正常品和回调数据")
    #     df_ebay = df_ebay[df_ebay['sales_status'].isin(['正常', '回调'])]

    # df_ebay.drop('overage_level', axis=1, inplace=True)
    # 预计调价数据备份
    write_to_sql(df_ebay, 'tt_oversea_ebay_upload')

    return None

# def get_main_name():
#     """
#     TT ebay账号销售员
#     """
#     sql = """
#     select transaction_id as platform_order_id,ebay_transaction_id,
#     gross_default `销售额tt`,
#     profit-order_list_product_marketing_cost-order_list_product_sale_marketing_cost `利润tt`,dept_no `三级部门`,saler_staff_name `销售员`
#     from temp.dwd_order_sku_input_date_list_di
#     where input_date >='2024-09-01'
#     and is_yellow_mark=0
#     and is_blue_mark =0
#     and order_level  !=50
#     and is_red_mark  =0
#     and shipping_type=10
#     """
#     df = sql_to_pd(database='temp', sql=sql, data_sys='TT新数仓')
#     df = df.drop_duplicates('platform_order_id')
#
#     df1 = df.copy()
#     df1['platform_order_id'] = df1['ebay_transaction_id'].copy()
#     df = df.append(df1, ignore_index=True)
#     del df['ebay_transaction_id']
#     df = df.drop_duplicates('platform_order_id')
#
#     order = order.merge(df, on=['platform_order_id'])  # 找不到销售的暂时剔除

# ##
# df_ebay.to_excel('F://Desktop//df_ebay.xlsx', index=0)
##
def upload_ebay():
    """
    ebay调价待上传数据存表
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-01-06'
    # df_temp = check_ebay_adjust()
    #
    sql = f"""
        SELECT *
        FROM over_sea.tt_oversea_ebay_upload
        where date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)
    print(f'今日上传调价数据共{len(df_temp)}条.')
    #
    col = ['sku','item_id','account_id','online_price','start_price_target','target_profit','shipping_service_cost']
    df_ebay = df_temp[col]
    df_ebay['item_id'] = df_ebay['item_id'].astype(float).astype('int64').astype(str)
    df_ebay['account_id'] = df_ebay['account_id'].astype(int)
    df_ebay['shipping_service_cost'] = df_ebay['shipping_service_cost'].astype(float)

    df_ebay = df_ebay.reset_index(drop=True)
    # 每次传100个item_id
    df_ebay['index'] = df_ebay.index
    df_ebay['index'] = df_ebay['index'].apply(lambda m_data: int(m_data / 100))

    print(df_ebay.info())
    # 失败数据存档
    table_name = 'tt_oversea_ebay_upload_result'
    date_id = time.strftime('%Y-%m-%d')
    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)
    conn.close()
    # 调价上传
    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df_ebay.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(ebay_Importprice_new, group, key, org_code='org_00235', environment='formal')
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='ebay') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

    return None


#
def ebay_Importprice_new(group, item, org_code,environment='formal'):
    if environment == 'formal':
        url0 = f'http://salescenter.yibainetwork.com:91/common/common_price_adjustment_api/data_upload'
        app_id = 1005
        app_secret_key = 'g7ydn2xitgxx39btqv5eq7bc27'
    elif environment == 'test':
        url0 = f'http://dp.yibai-it.com:11031/common/common_price_adjustment_api/data_upload'
        app_id = 1005
        app_secret_key = '96rlpzgt8k47mj0femgwjjawve'
    #
    group['work_number'] = '209313'
    data_post = {'platform_code': 'EB',
                 'data': json.dumps(group.to_dict(orient='records'), ensure_ascii=False)}
    # print(data_post)
    try:
        # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
        # while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
        #     time.sleep(30)
        timestamp = int(time.time())
        token_str = f'{app_id}{timestamp}{app_secret_key}'
        access_token = hashlib.md5(token_str.encode()).hexdigest()
        url = f'{url0}?timestamp={timestamp}&access_token={access_token}&org_code={org_code}&app_id={app_id}'
        # print(url)
        res = requests.post(url, data=data_post, timeout=(180, 120)).json()
        # print(res)
        if res['status'] == 1:
            if res['data_list']['error_count'] > 0:
                df_err = pd.DataFrame({'error_list': res['data_list']['error_list'], })
                df_err['date_id'] = time.strftime('%Y-%m-%d')
                df_err['item'] = item
                conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
                conn.to_sql(df_err, 'tt_oversea_ebay_upload_result', if_exists='append')
                # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
                # conn.to_sql(df_err, 'tt_oversea_ebay_upload_result', if_exists='replace')
        else:
            print(f'ebay{item}:', res['data_list'])
            # self.adjust_post_error_log_to_ck(f"ebay{item}: {res['data_list']}")
    except:
        time.sleep(5)
        print(f"ebay{item}接口失败，重新上传: {traceback.format_exc()}")
        # self.adjust_post_error_log_to_ck(f"ebay{item}接口失败，重新上传: {traceback.format_exc()}")


##
def tt_ebay_success_rate():
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2024-11-16'
    # date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    # 获取调价记录表日期
    sql = f"""

        SELECT 
            distinct account_id, item_id,  sales_status, sku, 
            online_price as `昨天调价前价格`, price as `昨天目标价格` 
        FROM over_sea.tt_oversea_ebay_upload
        WHERE 
            date_id = (
                SELECT max(date_id) 
                FROM over_sea.tt_oversea_ebay_upload
                WHERE date_id < '{date_today}'
                )

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_upload_temp = conn.read_sql(sql)
    print(df_upload_temp.info())

    sql = f"""

        SELECT account_id, item_id, sku, online_price as `今天价格`
        FROM over_sea.tt_ebay_oversea_listing 
        WHERE `date_id` = '{date_today}'

    """
    df_price_temp = conn.read_sql(sql)
    #
    df_temp = pd.merge(df_upload_temp, df_price_temp, how='inner', on=['account_id', 'item_id', 'sku'])
    col = ['昨天目标价格','今天价格','昨天调价前价格']
    df_temp[col] = df_temp[col].fillna(0).astype(float)
    print(df_temp.info())
    c1 = (df_temp['昨天目标价格'] - df_temp['今天价格']).abs() <= 1.0
    c2 = (df_temp['昨天调价前价格'] == df_temp['今天价格'])
    df_temp['标签'] = np.select([c1, c2], ['成功', '失败'], '其他')
    df_temp['date_id'] = date_today
    #
    table_name = 'tt_oversea_success_rate_ebay'
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_temp, table_name, if_exists='replace')
    sql = f"""
    delete from {table_name} where date_id='{date_today}'
    """
    conn.execute(sql)
    conn.to_sql(df_temp, table_name, if_exists='append')
    conn.close()

##
def yibai_get_aliexpress_listing():
    print("===aliexpress刊登链接数据===")
    sql = """
        SELECT 
            a.product_id, d.account_id, a.sku sku,sku_price,sku_code, aeop_s_k_u_property_list_str, b.property_value_id, 
            b.sku_property_id, c.name_en, d.freight_template_id, f.price_mode_name,
            if(e.aliexpress_currency_code1='', 'USD', e.aliexpress_currency_code1) AS aliexpress_currency_code
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
        INNER JOIN (
            SELECT
                product_id, account_id, product_price, freight_template_id
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_listing
            WHERE 
                account_id in (
                    select id as account_id from yibai_sale_center_system_sync.yibai_system_account
                    where platform_code = 'ALI' and is_del=0 and `status`=1 )
                and product_status_type = 1
        ) d ON a.product_id=d.product_id
        LEFT join (
            SELECT t1.*,t2.template_name_id as template_name_id,t2.account_id as account_id
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_fee_template_setting t1 
            left join yibai_sale_center_listing_sync.yibai_aliexpress_price_type_setting t2 
            on t1.price_mode_name = t2.price_mode_name
            where t1.is_delete=0 and t2.is_delete=0
        ) f
        on toString(d.freight_template_id)=f.template_name_id and d.account_id=f.account_id
        LEFT JOIN (
            select account_id,if(aliexpress_currency_code='', 'USD', aliexpress_currency_code) AS aliexpress_currency_code1
            from yibai_sale_center_common_sync.yibai_common_account_config
            where platform_code IN ('ALI') and is_del = 0
        ) e  on d.account_id=e.account_id
        -- 具体国外发货地
        LEFT JOIN (
            SELECT DISTINCT parent_attr_id, attr_id, name_en
            FROM yibai_sale_center_listing_sync.yibai_aliexpress_category_attribute
        ) c ON toInt64(b.sku_property_id) = toInt64(c.parent_attr_id) and toInt64(b.property_value_id) = toInt64(c.attr_id)
        where a.sku !='' and f.price_mode_name is not null
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据1')
    # conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='调价明细历史数据t')
    df_ali_listing = conn_ck.ck_select_to_df(sql)
    df_ali_listing.columns = [i.split('.')[-1] for i in df_ali_listing.columns]
    #
    df_ali_listing[['property_value_id', 'sku_property_id']] = df_ali_listing[['property_value_id', 'sku_property_id']].astype(int).astype(str)
    dic = {'United States':'US', 'CZECH REPUBLIC':'DE', 'Czech Republic':'DE', 'Poland':'PL', 'france':'FR', 'France':'FR',
           'Australia':'AU', 'CN':'CN', 'spain':'ES', 'SPAIN':'ES', 'Russian Federation':'RU', 'UNITED KINGDOM':'UK',
           'United Kingdom':'UK','GERMANY':'DE', 'Mexico':'MX', 'cz':'DE', 'ITALY':'IT', 'Italy':'IT','brazil':'BR'}
    df_ali_listing['country'] = df_ali_listing['name_en'].replace(dic)
    df_ali_listing = df_ali_listing.drop_duplicates(subset=['product_id','sku','country'])
    df_ali_listing = df_ali_listing[(df_ali_listing['country']!='CN') & (df_ali_listing['country']!='CN')]
    # df_ali_listing = df_ali_listing.sample(10000)

    # # 处理仓标数据
    # df['new_sku'] = np.where(df['sku'].str[:3].isin(['US-', 'GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-']),
    #                          df['sku'].str[3:], df['sku'])
    # df['new_sku'] = np.where(df['sku'].str[-2:].isin(['HW', 'DE', 'US', 'AU']),
    #                          df['sku'].str[:-2], df['new_sku'])
    df_ali_listing.drop('aeop_s_k_u_property_list_str', axis=1, inplace=True)
    df_ali_listing['date_id'] = time.strftime('%Y-%m-%d')
    print(df_ali_listing.info())

    # 存表
    # write_to_sql(df_ali_listing, 'yibai_ali_oversea_listing')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_ali_listing, 'yibai_ali_oversea_listing', if_exists='replace')

    return None
# yibai_get_aliexpress_listing()

def check_amazon_adjust():
    """
    检查amazon待调价链接
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-02-19'
    # 取有运费链接
    sql = f"""
        SELECT
            a.sku,  account_id, seller_sku, asin,  a.country, online_price,
            online_profit online_gross_profit, date_id, best_warehouse_name, a.warehouse, new_price,
            total_cost, a.ship_name,  a.rate,available_stock, day_sales,
            sales_status,price, price_rmb, target_profit_rate, ppve,profit_diff,
            platform_zero,platform_must_percent, is_supplier, is_supplier_price, is_white_listing         
        FROM (
            SELECT *
            FROM over_sea.tt_oversea_amazon_listing_all 
            WHERE date_id = '{date_today}' and total_cost > 0
            and online_price > 0
            -- and a.warehouse = '英国仓'
        ) a
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_amazon = conn.read_sql(sql)
    print(f'链接数量共{len(df_amazon)}条.')
    sql = f"""
        SELECT sku, warehouse, country, overage_level, shippingCost, firstCarrierCost
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = '{date_today}' and platform = 'AMAZON'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)
    df_amazon = pd.merge(df_amazon, df_dtl, how='left', on=['sku','warehouse','country'])
    # df_account = get_account()
    # df_ebay = pd.merge(df_ebay, df_account[['account_id','account_name','short_name']], how='left', on=['account_id'])
    df_amazon = df_amazon.drop_duplicates()
    df_amazon['is_up'] = np.where(df_amazon['price'] > df_amazon['online_price'], 1, 0)
    df_amazon['price_up_range'] = (df_amazon['price']/df_amazon['online_price'] - 1).round(4)
    df_amazon = data_section(df_amazon, 'price_up_range')
    df_amazon = data_section(df_amazon, 'profit_diff')

    # df_amazon.to_excel('F://Desktop//tt_amazon.xlsx', index=0)
    # 筛选预计调价数据
    # df_amazon = df_amazon[(df_amazon['is_up']==1)]
    df_amazon = df_amazon[df_amazon['is_white_listing'] != 1]
    df_amazon.drop('is_white_listing', axis=1, inplace=True)

    df_amazon = df_amazon[~((df_amazon['price_up_range'] <= 0.01) & (df_amazon['price_up_range'] >= -0.01))]
    df_amazon['date_id'] = date_today

    # 调价日期
    # today = datetime.datetime.now().weekday()
    # if (today == 1 or today == 2 or today == 4) :
    #     print("今天是周二、周三或周五，只保留正常品和回调数据")
    #     df_amazon = df_amazon[df_amazon['sales_status'].isin(['正常', '回调'])]

    print(df_amazon.info())
    # df_amazon.to_excel('F://Desktop//df_amazon.xlsx', index=0)
    # 测试上传案例
    # df_amazon = df_amazon[df_amazon['sku']=='1010190002220']
    # 预计调价数据备份
    write_to_sql(df_amazon, 'tt_oversea_amazon_upload')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df_amazon, 'tt_oversea_amazon_upload', if_exists='replace')

    return None

def amazon_Importprice(df1, item, org_code, environment='formal',):
    org_data = {
        'org_00008': {
            # 正式环境
            'url_prod': 'http://amazon.yibainetwork.com/services/products/Amazonlistingpriceadjustment/Importprice?token=',
            # 'url_prod': 'http://172.16.50.160/services/products/Amazonlistingpriceadjustment/Importprice?token=',
            # 测试环境
            'url_test': 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token=',
            # 'url_test': 'http://192.168.71.210:30088/services/products/Amazonlistingpriceadjustment/Importprice?token=',
            # 用户名
            'user_id': 'W01596',
        },
        'org_00235': {
            # 正式环境
            'url_prod': 'http://124.71.2.235:91/services/products/Amazonlistingpriceadjustment/Importprice?token=',

            # 测试环境
            'url_test': 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token=',
            # 用户名
            'user_id': 'TT202655',
        },
    }


    if environment == 'formal':
        # 正式环境
        url0 = org_data[org_code]['url_prod']
    elif environment == 'test':
        # 测试环境
        url0 = org_data[org_code]['url_test']

    headers = {'Content-Type': 'application/json', }

    data0 = []
    for i in range(len(df1.index)):
        data1 = {'account_id': int(df1['account_id'][i]),
                 'seller_sku': df1['seller_sku'][i],
                 'price': df1['price'][i],
                 'sale_price': '',
                 'business_price': '',
                 'lowest_price': '',
                 'start_time': '',
                 'end_time': '',
                 'reason': df1['sales_status'][i],
                 # 'standard_price_points': int(df1['standard_price_points'][i]),
                 # 'sale_price_points': int(df1['sale_price_points'][i]),
                 # 'minprice_del': int(df1['minprice_del'][i]),
                 # 'maxprice_del': int(df1['maxprice_del'][i]),
                 }
        data0.append(data1)

    data_post = {'user_id': org_data[org_code]['user_id'],
                 'price_data': data0}
    data_post0 = json.dumps(data_post)
    # print(data_post0)
    token_str = data_post0 + 'saagdfaz'
    token = hashlib.md5(token_str.encode()).hexdigest()
    # print(token)
    url = url0 + token
    # print(data_post)
    # print(url)
    try:
        # 20220713 为了避免推送数据系统来不及消化造成阻塞，当每次执行时间超过30秒时，停60s再推下一次
        t_start = time.time()
        res1 = requests.post(url, json=data_post, headers=headers, timeout=(600, 600))
        res = res1.json()
        t_end = time.time()
        # print(res)
        if res['status'] == 1:
            pass
            # print(res['msg'])
        else:
            print(f'amazon{item}:', res['msg'])
            # 20220715 遇到系统阻塞时，停60s
            if res['msg'] == '数据阻塞，请稍后重试':
                print('数据阻塞，请稍后重试')
    except:
        print(f"amazon{item}接口失败，重新上传: {traceback.format_exc()}")
        # print(f"amazon{item}接口失败，接口返回数据:", res1)
        # time.sleep(30)

def upload_amazon():
    """
    amazon调价待上传数据存表
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-01-06'
    # df_temp = check_ebay_adjust()
    #
    sql = f"""
        SELECT *
        FROM over_sea.tt_oversea_amazon_upload
        where date_id = '{date_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_amazon = conn.read_sql(sql)

    # df_amazon = pd.read_excel('F://Desktop//tt_amazon.xlsx', dtype={'sku':str})
    print(f'今日上传调价数据共{len(df_amazon)}条.')

    col = ['sku','seller_sku','account_id','online_price', 'price','sales_status']
    df_amazon = df_amazon[col]
    df_amazon['account_id'] = df_amazon['account_id'].astype(int)

    df_amazon = df_amazon.reset_index(drop=True)
    # 每次传100个item_id
    df_amazon['index'] = df_amazon.index
    df_amazon['index'] = df_amazon['index'].apply(lambda m_data: int(m_data / 100))
    print(df_amazon.info())
    # 调价上传
    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df_amazon.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(amazon_Importprice, group, key, org_code='org_00235', environment='formal')
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='Amazon') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)
    return None

def check_shopee_adjust():
    """
    检查shopee待调价链接
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-02-19'
    # 取有运费链接
    sql = f"""
        SELECT
            a.sku, item_id, account_id, account_name, a.country,  online_price, parent_sku, is_mulit,
            online_profit online_gross_profit, date_id, best_warehouse_name, a.warehouse, new_price,
            total_cost, a.ship_name,  a.rate,available_stock,is_white_account,
            sales_status,price, pay_fee,paypal_fee, vat_fee, extra_fee,c.platform_zero,c.platform_must_percent,
            target_profit_rate target_gross_profit, price_rmb, target_profit_rate, profit_diff
        FROM (
            SELECT *
            FROM over_sea.tt_oversea_shopee_listing_all 
            WHERE date_id = '{date_today}' and total_cost > 0
        ) a
        LEFT JOIN (
            SELECT site country, pay_fee, paypal_fee, vat_fee, extra_fee,platform_zero,platform_must_percent
            FROM over_sea.tt_yibai_platform_fee
            WHERE platform = 'SHOPEE'
        ) c ON a.country = c.country
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_shopee = conn.read_sql(sql)
    print(f'链接数量共{len(df_shopee)}条.')
    sql = f"""
        SELECT sku, warehouse, country, overage_level, shippingCost, firstCarrierCost, fixed_fee
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = '{date_today}' and platform = 'SHOPEE'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df_shopee, df_dtl, how='left', on=['sku','warehouse','country'])

    df['is_up'] = np.where(df['price'] > df['online_price'], 1, 0)
    df['price_up_range'] = (df['price']/df['online_price'] - 1).round(4)
    df = data_section(df, 'price_up_range')
    df = data_section(df, 'profit_diff')

    print(df_shopee.info())

    # 筛选预计调价数据
    df = df[~((df['price_up_range'] <= 0.01) & (df['price_up_range'] >= -0.01))]
    df = df[df['is_white_account'] != 1]
    df = df[~df['best_warehouse_name'].isin(['XYD泰国海外仓','XYD菲律宾海外仓','XYD马来海外仓'])]

    df['date_id'] = time.strftime('%Y-%m-%d')
    print(df.info())

    # df.to_excel('F://Desktop//df_shopee.xlsx', index=0)

    df.drop('is_white_account', axis=1, inplace=True)
    # 预计调价数据备份
    write_to_sql(df, 'tt_oversea_shopee_upload')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_shopee_upload', if_exists='replace')

    return None

def upload_shopee():
    """
    amazon调价待上传数据存表
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-01-06'
    # df_temp = check_ebay_adjust()
    #
    sql = f"""
        SELECT *
        FROM over_sea.tt_oversea_shopee_upload
        where date_id = '{date_today}'
        and available_stock > 0 
        -- and item_id = '42867140032'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 暂时只传涨价链接
    # df = df[df['is_up']==1]
    print(f'今日上传调价数据共{len(df)}条.')

    df = df[["account_name", "item_id", "account_id", "sku", "is_mulit", "parent_sku", "price", "target_profit_rate", "sales_status","country"]]
    df.columns = ['account_name', 'item_id', "account_id", 'sku', "is_mulit", "parent_sku", 'final_price', "import", 'adjustment_reason',"country"]
    df = df.sort_values(['account_name'], ascending=True)
    df['parent_sku'] = df['parent_sku'].astype(str)
    df_precison = get_shopee_precision()
    df = pd.merge(df, df_precison, how='left', on='country')

    df = df.reset_index(drop=True)
    # 每次传100个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 100))
    print(df.info())

    # 调价上传
    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(shopee_Importprice, group, key, org_code='org_00235')
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='SHOPEE') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

    return None

def shopee_Importprice(group, item, work_number='209313', org_code='org_00235'):
    # if self.environment == 'formal':
    url = f'http://salescenter.yibainetwork.com:91/apis/open/listing_info_operate_api/shopee_batch_price?org_code={org_code}'

    headers = {'Content-Type': 'application/json',
               'Cookie': 'PHPSESSID=uuf68q5ljhjvmp3tde8ltsnj71'}
    start_time = (datetime.datetime.today() + datetime.timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    end_time = (datetime.datetime.today() + datetime.timedelta(179)).strftime('%Y-%m-%d %H:%M:%S')

    data_post = []

    for (key1, key2), group in group.groupby(['account_id', 'item_id']):
        group = group.reset_index(drop=True)
        sonSku = {}
        for i in range(len(group['item_id'])):
            if group['Precision'][i] == 1:
                x = round(group['final_price'][i], 1)
            elif group['Precision'][i] == 2:
                x = round(group['final_price'][i], 2)
            else:
                x = int(group['final_price'][i])
            sonSku[str(i)] = {
                "sku": str(group['sku'][i]),
                "final_price": f"{x}",
                "rate": "",
                "import": float(group['import'][i]),
                'adjustment_reason': group['adjustment_reason'][i]
            }

        data_dict = {
            'item': {
                "sonSku": sonSku,
                "account_id": int(group['account_id'][0]),
                "account_name": group['account_name'][0],
                "site": group['country'][0],
                "item_id": int(group['item_id'][0]),
                "discount_purchase_limit": 100,
                "type": 1,
                "create_user_id": '209313',
                "start_time": start_time,
                "end_time": end_time,
                "sku": str(group['sku'][0]),
                'is_mulit': int(group['is_mulit'][0])
            }
        }
        data_post.append(data_dict)
    # print(data_post)
    n = 0
    while n < 1:
        n = n + 1
        try:
            res1 = requests.post(url, json=data_post, headers=headers, timeout=(180, 120))
            res = res1.json()
            # print(res)
            if res['code'] == 200:
                print('上传成功！')
                break
            else:
                print(f'shopee{item}:', res['data_list'])
                # self.adjust_post_error_log_to_ck(f"cd{item}: {res['data_list']}")
        except:
            # time.sleep(30)
            print(f"shopee{item}接口失败，重新上传！")
            # self.adjust_post_error_log_to_ck(f"cd{item}接口失败，重新上传: {traceback.format_exc()}")

def check_lazada_adjust():
    """
    lazada
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-02-19'
    # 取有运费链接
    sql = f"""
        SELECT
            a.sku, item_id, seller_sku, account_id, account_name, a.country,  online_price, parent_sku, is_mulit,
            online_profit online_gross_profit, date_id, best_warehouse_name, a.warehouse, new_price,
            total_cost, a.ship_name,  a.rate,available_stock,is_white_account,
            sales_status,price, pay_fee,paypal_fee, vat_fee, extra_fee,c.platform_zero,c.platform_must_percent,
            target_profit_rate target_gross_profit, price_rmb, target_profit_rate, profit_diff
        FROM (
            SELECT *
            FROM over_sea.tt_oversea_lazada_listing_all 
            WHERE date_id = '{date_today}' and total_cost > 0
        ) a
        LEFT JOIN (
            SELECT site country, pay_fee, paypal_fee, vat_fee, extra_fee,platform_zero,platform_must_percent
            FROM over_sea.tt_yibai_platform_fee
            WHERE platform = 'LAZADA'
        ) c ON a.country = c.country
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_lazada = conn.read_sql(sql)
    print(f'链接数量共{len(df_lazada)}条.')
    sql = f"""
        SELECT sku, warehouse, country, overage_level, shippingCost, firstCarrierCost, fixed_fee
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = '{date_today}' and platform = 'LAZADA'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df_lazada, df_dtl, how='left', on=['sku','warehouse','country'])

    df['is_up'] = np.where(df['price'] > df['online_price'], 1, 0)
    df['price_up_range'] = (df['price']/df['online_price'] - 1).round(4)
    df = data_section(df, 'price_up_range')
    df = data_section(df, 'profit_diff')

    # 读表上传
    # df = pd.read_excel('F://Desktop//df_upload.xlsx')

    print(df_lazada.info())

    # 筛选预计调价数据
    df = df[~((df['price_up_range'] <= 0.01) & (df['price_up_range'] >= -0.01))]
    df = df[df['is_white_account'] != 1]
    df = df[~df['best_warehouse_name'].str.contains('XYD')]   # 20251223 lazada没有配置XYD仓库库存

    df['date_id'] = time.strftime('%Y-%m-%d')
    print(df.info())

    # df = df[(df['overage_level']>=90) & (df['overage_level']<180) & (df['is_up']==1)]

    # df.to_excel('F://Desktop//df_lazada.xlsx', index=0)

    df.drop('is_white_account', axis=1, inplace=True)
    # 预计调价数据备份
    write_to_sql(df, 'tt_oversea_lazada_upload')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_lazada_upload', if_exists='replace')

    check_allegro_adjust()

    return None

def upload_lazada():
    """
    amazon调价待上传数据存表
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-01-06'
    # df_temp = check_ebay_adjust()
    #
    sql = f"""
        SELECT *
        FROM over_sea.tt_oversea_lazada_upload
        where date_id = '{date_today}'
        and available_stock > 0 
        -- and item_id = '42867140032'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 暂时只传涨价链接
    # df = df[df['is_up']==1]
    # df = df.sample(1)
    # df = pd.read_excel('F://Desktop//LAZADA调价数据检查_2025-12-23.xlsx', dtype={'sku':str})
    print(f'今日上传调价数据共{len(df)}条.')

    df = df[["account_name", "seller_sku", "account_id", "sku",  "price", "item_id",
             "target_profit_rate", "new_price", "sales_status","country"]]
    df.columns = ['account_name', 'seller_sku', "account_id", 'sku', 'final_price', "item_id",
                  "import_rate", "new_price", 'adjustment_reason',"country"]

    df = df.reset_index(drop=True)
    # 每次传100个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 100))
    print(df.info())

    # 调价上传
    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(lazada_Importprice, group, key, org_code='org_00235')
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='LAZADA') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

    return None

def lazada_Importprice(df1, item, work_number='209313', org_code='org_00235'):
    # if self.environment == 'formal':
    url = f'http://salescenter.yibainetwork.com:91/apis/open/listing_info_operate_api/lazada_receive_batch_price?org_code={org_code}'
    headers = {'Content-Type': 'application/json',
               'Cookie': 'PHPSESSID=uuf68q5ljhjvmp3tde8ltsnj71'}

    data_post = []

    for i in range(len(df1['account_name'])):
        if df1['country'][i] == 'MY':
            x = round(df1['final_price'][i], 2)
        elif df1['country'][i] == 'SG':
            x = round(df1['final_price'][i], 1)
        else:
            x = int(df1['final_price'][i])
        data_dict = {"seller_name": df1['account_name'][i],
                     "seller_sku": df1['seller_sku'][i],
                     "sell_price": f"{x}",
                     # "user_id": 4494,
                     "user_id": "209313",
                     "import_rate": "{}".format(df1['import_rate'][i]),
                     'adjustment_reason': df1['adjustment_reason'][i],
                     'product_cost': df1['new_price'][i],
                     'item_id': df1['item_id'][i],
                     }
        data_post.append(data_dict)
    # print(data_post)
    n = 0
    while n < 1:
        n = n + 1
        try:
            res1 = requests.post(url, json=data_post, headers=headers, timeout=(180, 120))
            res = res1.json()
            # print(res)
            code_list = [item.get('code', None) for item in res.get('data_list', [])]
            # code_list = res['data_list']
            if 200 in code_list:
                print('上传成功！')
                break
            else:
                print(f'lazada{item}:', res['data_list'])
                # self.adjust_post_error_log_to_ck(f"cd{item}: {res['data_list']}")
        except:
            time.sleep(30)
            print(f"lazada{item}接口失败，重新上传！")
            # self.adjust_post_error_log_to_ck(f"cd{item}接口失败，重新上传: {traceback.format_exc()}")

def tt_shopee_success_rate():
    date_today = time.strftime('%Y-%m-%d')
    # date_today = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    # 获取调价记录表日期
    sql = f"""
        WITH adjust_listing as (
            SELECT 
                distinct account_id, item_id,  sku, 
                online_price as `昨天调价前价格`, price as `昨天目标价格`, date_id
            FROM over_sea.tt_oversea_shopee_listing_all
            WHERE 
                date_id = (
                    SELECT max(date_id) 
                    FROM over_sea.tt_oversea_shopee_listing_all   
                    WHERE date_id < '{date_today}'
                    )
        )

        SELECT 
            a.*, b.price as `今天价格`,
            case 
                when `昨天调价前价格`= b.price then '失败'
                when abs(`昨天目标价格` - b.price) <= 0.5 then '成功'
                else '其他'
            end as `标签`
        FROM adjust_listing a
        LEFT JOIN (
            SELECT account_id, item_id, sku, price as price
            FROM over_sea.tt_oversea_shopee_listing_all  
        ) b
        ON a.account_id = b.account_id and a.item_id = b.item_id
    """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_temp = conn_ck.ck_select_to_df(sql)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_temp = conn.read_sql(sql)
    df_temp = df_temp.drop_duplicates(subset=['account_id', 'item_id'])
    print(df_temp.head(4))
    #
    df_temp['date_id'] = date_today
    table_name = 'tt_oversea_success_rate_shopee'
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = f"""
    # delete from {table_name} where date_id='{date_today}'
    # """
    # conn.execute(sql)
    conn.to_sql(df_temp, table_name, if_exists='append')
    conn.close()


def check_allegro_adjust():
    """
    lazada
    """
    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-02-19'
    # 取有运费链接
    sql = f"""
        SELECT
            sku, offer_id, account_id, account_name, country,  online_price, selling_mode_currency, 
            online_profit online_net_profit, date_id, best_warehouse_name, warehouse, new_price,
            total_cost, ship_name,  rate,available_stock,is_white_account,
            sales_status,price, target_profit_rate+platform_zero target_gross_profit,
            target_profit_rate, profit_diff
        FROM yibai_oversea.tt_oversea_allegro_listing_all
        WHERE date_id = '{date_today}' and total_cost > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_allegro = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT site country, pay_fee, paypal_fee, vat_fee, extra_fee,platform_zero,platform_must_percent
        FROM over_sea.tt_yibai_platform_fee
        WHERE platform = 'ALLEGRO'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform = conn.read_sql(sql)
    print(f'链接数量共{len(df_allegro)}条.')
    sql = f"""
        SELECT sku, warehouse, country, overage_level, shippingCost, firstCarrierCost, fixed_fee
        FROM yibai_oversea.tt_dwm_oversea_price
        WHERE date_id = '{date_today}' and platform = 'ALLEGRO'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_dtl = conn_ck.ck_select_to_df(sql)
    df = pd.merge(df_allegro, df_dtl, how='left', on=['sku','warehouse','country'])

    print(df.info())
    df['is_up'] = np.where(df['price'] > df['online_price'], 1, 0)
    df['price_up_range'] = (df['price']/df['online_price'] - 1).round(4)
    df = data_section(df, 'price_up_range')
    df = data_section(df, 'profit_diff')

    # 读表上传
    # df = pd.read_excel('F://Desktop//df_upload.xlsx')

    # 筛选预计调价数据
    # df = df[~((df['price_up_range'] <= 0.01) & (df['price_up_range'] >= -0.01))]
    # df = df[df['is_white_account'] != 1]
    # df = df[~df['best_warehouse_name'].str.contains('XYD')]
    #
    # df['date_id'] = time.strftime('%Y-%m-%d')

    print(df.info())
    # df.to_excel('F://Desktop//df_tt_allegro.xlsx', index=0)

    # df.drop('is_white_account', axis=1, inplace=True)
    # 预计调价数据备份
    write_to_sql(df, 'tt_oversea_allegro_upload')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'tt_oversea_allegro_upload', if_exists='replace')

    return None

def tt_allegro_listing_group():
    """ allegro上传调价数据时，多站点价格需放在一条链接下 """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT account_id, offer_id, country, sales_status, price
        FROM yibai_oversea.tt_oversea_allegro_listing_all
        WHERE date_id = '{date_today}' 
        and is_normal_cdt = 0 
        -- and is_small_diff = 0
        -- and sales_status != '负利润加快动销'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # 分批次调价
    # df_upload = pd.read_excel('F://yibai-price-strategy//data//2025-03-25//ALLEGRO调价数据检查_2025-03-25.xlsx', dtype={'offer_id':str})
    # print(df_upload.info())
    # df = df[df['offer_id'].isin(df_upload['offer_id'].unique())]
    print(df.info())
    df_base = df[df['country']=='PL']
    df_base.columns = ['account_id','item_id','country','adjustment_reason','start_price_target_pl']

    df_cz = df[df['country']=='CZ']
    df_cz.columns = ['account_id','item_id','country','adjustment_reason','start_price_target_cz']

    df_sk = df[df['country']=='SK']
    df_sk.columns = ['account_id','item_id','country','adjustment_reason','start_price_target_sk']

    df_base = pd.merge(df_base, df_cz[['account_id','item_id','start_price_target_cz']], how='left', on=['account_id','item_id'])
    df_base = pd.merge(df_base, df_sk[['account_id','item_id','start_price_target_sk']], how='left', on=['account_id','item_id'])
    col = ['start_price_target_cz','start_price_target_sk', 'start_price_target_pl']
    # 校验调价item_id, 是否实际存在cz和sk的价格
    sql = f"""
        SELECT account_id, offer_id as item_id, online_price price_site, country
        FROM tt_oversea.tt_ads_oversea_allegro_listing
        WHERE date_id = '{date_today}'
    """
    conn_ck_tt = pd_to_ck(database='tt_oversea', data_sys='通拓-新')
    df_listing_base = conn_ck_tt.ck_select_to_df(sql)
    df_base_cz = df_listing_base[df_listing_base['country']=='CZ']
    df_base_sk = df_listing_base[df_listing_base['country']=='SK']
    df_base = pd.merge(df_base, df_base_cz[['account_id','item_id','price_site']], how='left', on=['account_id','item_id'])
    df_base['start_price_target_cz'] = np.where(df_base['start_price_target_cz'].isna(),
                                                df_base['price_site'], df_base['start_price_target_cz'])
    df_base.drop(['price_site'], axis=1, inplace=True)
    df_base = pd.merge(df_base, df_base_sk[['account_id','item_id','price_site']], how='left', on=['account_id','item_id'])
    df_base['start_price_target_sk'] = np.where(df_base['start_price_target_sk'].isna(),
                                                df_base['price_site'], df_base['start_price_target_sk'])
    df_base.drop(['price_site'], axis=1, inplace=True)

    df_base[col] = df_base[col].fillna(0).astype(float).round(2)
    for i in col:
        df_base[i] = np.where(df_base[i]<=0.01, 0, df_base[i])
    df_base.drop(['country'], axis=1, inplace=True)
    df_base['date_id'] = date_today
    print(df_base.info())

    write_to_ck(df_base, 'tt_oversea_allegro_upload')
    # df_base.to_excel('F://Desktop//df_base_2.xlsx', index=0)

    # # 初始链接情况
    # sql = """
    #     SELECT *
    #     FROM yibai_oversea.yibai_oversea_allegro_all_site
    #     WHERE date_id = '2025-03-24'
    # """
    # df_base_all = conn_ck.ck_select_to_df(sql)
    # df_base_all.to_excel('F://Desktop//df_base_all.xlsx', index=0)

    return df_base

def tt_allegro_price_post():
    """ allegro调价程序 """
    time_today = time.strftime('%Y-%m-%d')

    df_data = tt_allegro_listing_group()

    # df_data = pd.read_excel('F://Desktop//tt_allegro_test.xlsx', dtype={'item_id':str})
    # df_data = df_data[df_data['item_id'].isin(['17431233838'])]
    # df_data = df_data[df_data['item_id'].isin(['16289570631', '16323942458', '13824309706'])]
    print("allegro调价数目: ", len(df_data))

    col = ['account_id','item_id','adjustment_reason', 'start_price_target_pl',
           'start_price_target_cz','start_price_target_sk']
    df = df_data[col]
    df[['item_id','account_id']] = df[['item_id','account_id']].astype('int64')
    col = ['start_price_target_pl', 'start_price_target_cz', 'start_price_target_sk']
    df[col] = df[col].astype(float).round(2)
    def format_float(value):
        if value == 0:
            return "{:.0f}".format(value)
        else:
            return "{:.2f}".format(value)

    for i in ['start_price_target_cz', 'start_price_target_sk']:
        df[i] = df[i].apply(format_float)
    # cz只支持0位小数、pl和sk支持2位小数
    df['start_price_target_cz'] = df['start_price_target_cz'].astype(float).round(0)
    # df[col] = df[col].astype(str)
    print(df)
    df = df.reset_index(drop=True)
    # 每次传10000个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 500))
    print(df.info())
    # df.to_excel('F://Desktop//df_allegro_upload_2.xlsx', index=0)
    #
    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(allegro_Importprice, group, key, org_code='org_00235')
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='allegro') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

def allegro_Importprice(group, item, org_code,environment='formal'):
    """ allegro调价数据上传 """
    if environment == 'formal':
        url0 = 'http://salescenter.yibainetwork.com:91/common/common_price_adjustment_api/data_upload'
        app_id = 1005
        app_secret_key = 'g7ydn2xitgxx39btqv5eq7bc27'
    elif environment == 'test':
        url0 = 'http://dp.yibai-it.com:11031/common/common_price_adjustment_api/data_upload'
        app_id = 1005
        app_secret_key = '96rlpzgt8k47mj0femgwjjawve'

    group['work_number'] = '209313'
    data_post = {'platform_code': 'ALLEGRO', 'data': json.dumps(group.to_dict(orient='records'), ensure_ascii=False)}

    # print(data_post)
    try:
        # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
        # while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
        #     time.sleep(30)
        timestamp = int(time.time())
        token_str = f'{app_id}{timestamp}{app_secret_key}'
        access_token = hashlib.md5(token_str.encode()).hexdigest()
        url = f'{url0}?timestamp={timestamp}&access_token={access_token}&org_code={org_code}&app_id={app_id}'
        res = requests.post(url, data=data_post, timeout=(180, 120)).json()
        # print(url)
        # print(res)
        if res['status'] == 1:
            if res['data_list']['error_count'] > 0:
                df_err = pd.DataFrame({'error_list': res['data_list']['error_list'], })
                df_err['date_id'] = time.strftime('%Y-%m-%d')
                df_err['item'] = item
                conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
                conn.to_sql(df_err, 'tt_oversea_allegro_upload_result', if_exists='append')
        else:
            print(f'allegro{item}:', res['data_list'])
            # self.adjust_post_error_log_to_ck(f"ebay{item}: {res['data_list']}")
    except:
        time.sleep(5)
        print(f"allegro{item}接口失败，重新上传: {traceback.format_exc()}")
        # self.adjust_post_error_log_to_ck(f"ebay{item}接口失败，重新上传: {traceback.format_exc()}")

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



def wait_check():
    try:
        print('TT 各平台链接利润率计算开始...')
        tt_listing_info()
        # print(is_check)
    except IOError as n:
        print('还未检查完成！')

def check_adjust_data():
    """ 调价数据检查 汇总 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    col = ['ebay', 'amazon', 'shopee', 'lazada', 'allegro']
    # col = ['allegro']
    for i in col:
        sql = f"""
            SELECT *
            FROM over_sea.tt_oversea_{i}_upload
            WHERE date_id = '{time.strftime('%Y-%m-%d')}'
        """
        df = conn.read_sql(sql)

        df.to_excel(f'F://Desktop//df_{i}.xlsx', index=0)

##
if __name__ == '__main__':
    # df = tt_get_stock()

    # check_adjust_data()

    # check_ebay_adjust()
    # check_amazon_adjust()
    # check_shopee_adjust()
    # check_lazada_adjust()
    # check_allegro_adjust()
    # tt_allegro_listing_group()

    upload_ebay()
    upload_amazon()
    upload_shopee()
    upload_lazada()
    tt_allegro_price_post()

    # df_sku = dwm_oversea_sku()
    # df_sku.to_excel('F:\Desktop\df_sku.xlsx', index=0)
    # tt_shopee_success_rate()
    # print(df.info())
