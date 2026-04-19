"""
TEMU平台相关需求

"""
##
import warnings
import datetime, time
import pandas as pd
import numpy as np
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_listing_detail_2023 import get_amazon_listing_data, get_price_data, get_platform_fee
from all_auto_task.oversea_price_adjust_2023 import get_stock, get_stock_age, get_rate, get_sku_sales_new
from utils.utils import save_df, make_path
from utils import utils
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, \
    sku_and_num_split
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account, get_freight_subsidy, \
    get_promotion_listing, get_transport_fee, get_line_new, get_stock_age
from all_auto_task.oversea_add_logic import get_temp_clear_sku
from all_auto_task.oversea_temu_shield import extract_correct_string, warehouse_mark, get_main_resp, get_listing_order
from all_auto_task.oversea_price_adjust_tt import tt_get_sku
import pymysql

warnings.filterwarnings('ignore')


##
# TEMU 海外仓定价
def mysql_escape(df, col='sku', d_type=None):
    # pymysql.escape_string
    # pymysql.converters.escape_string
    dfM = df[[col]].drop_duplicates()
    sku_list = list(dfM[col])
    if d_type:
        erp_list_str = ','.join(list(map(lambda m: f"{m}", sku_list)))
        return erp_list_str
    sku_list = list(map(lambda m: pymysql.converters.escape_string(m), sku_list))
    erp_list_str = ','.join(list(map(lambda m: f"'{m}'", sku_list)))
    return erp_list_str


def chicun_zhongliang(df, Organization, conn_mx, is_public=0):
    """
    :param df:
    :param Organization:
    :param conn_mx:
    :param is_public: 是否强制对公sku， 0：否； 1：是；  默认0
    :return:
    """
    if df.shape[0] > 0:
        df_sku = df[['sku', '数量']].drop_duplicates()
        df_sku['sku1'] = df_sku['sku']
        df_sku = df_sku.reset_index(drop=True)
        df_sku = df_sku.drop('sku1', axis=1).join(
            df_sku['sku1'].str.split('+', expand=True).stack().reset_index(level=1, drop=True).rename('sku1'))
        df_sku = df_sku.reset_index(drop=True)
        df_sku = df_sku.drop('sku1', axis=1).join(
            df_sku['sku1'].str.split(';', expand=True).stack().reset_index(level=1, drop=True).rename('sku1'))
        df_sku = df_sku.reset_index(drop=True)
        df_sku = df_sku.drop('sku1', axis=1).join(
            df_sku['sku1'].str.rsplit('*', n=1, expand=True).rename(columns={0: 'sku1', 1: 'num'}))
        if 'num' in df_sku.columns:
            df_sku['num'].fillna('1', inplace=True)
            df_sku.loc[~df_sku['num'].str.isnumeric(), 'num'] = 1
            df_sku['num'] = df_sku['num'].astype(int)
        else:
            df_sku['num'] = 1
        df_sku['num'] = df_sku['数量'] * df_sku['num']

        df_sku = df_sku.reset_index(drop=True)
        df_sku['index'] = df_sku.index
        df_sku['index'] = df_sku['index'].apply(lambda m: int(m / 3000))
        df_erp_sku = pd.DataFrame()
        for key, group in df_sku.groupby(['index']):
            sku_list = mysql_escape(group, 'sku1')
            sql = f"""
            SELECT a.sku AS sku1,
                multiIf( 
                    a.product_status=11 and toFloat64(a.avg_goods_price) > 0, toFloat64(a.avg_goods_price), 
                    a.product_status=11 and toFloat64(a.avg_goods_price) = 0 and toFloat64(a.new_price) > 0, toFloat64(a.new_price), 
                    a.product_status=11 and toFloat64(a.avg_goods_price) = 0 and toFloat64(a.new_price) = 0, toFloat64(a.product_cost), 
                    a.product_status!=11 and toFloat64(a.new_price) > 0, toFloat64(a.new_price),
                toFloat64(a.product_cost)) * if(e.is_corporate1=1 and {is_public}=1, 1.26, 1) as `成本`,
                toFloat64(pur_length_pack) as `长`,
                toFloat64(pur_width_pack) as `宽`,
                toFloat64(pur_height_pack) as `高`,
                case 
                    when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
                    else toFloat64(weight_out_storage) 
                end as `重量`,
                case 
                    when toFloat64(weight_out_storage)=0 then '毛重' 
                    else '出库平均重量' 
                end as `重量来源`
            FROM yibai_prod_base_sync.yibai_prod_sku a 
            left join (
                -- 20241107 XQ-2411070013 强制对公sku国内仓调价成本上调26%
                SELECT DISTINCT sku, 0 as is_corporate1
                from yibai_prod_base_sync.yibai_prod_sku a
            ) e
            on a.sku=e.sku 
            where a.sku in ({sku_list})
            """
            # print(sql)
            df_erp_sku0 = conn_mx.ck_select_to_df(sql)
            df_erp_sku = df_erp_sku.append(df_erp_sku0)
        if df_erp_sku.shape[0] == 0:
            df_erp_sku = pd.DataFrame(columns=['sku1', '长', '宽', '高', '重量', '成本', '重量来源'])
        for col in ['长', '宽', '高', '重量', '成本']:
            df_erp_sku[col] = df_erp_sku[col].astype(float)

        df_erp_sku['最长边'] = np.max(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku['次长边'] = np.median(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku['最短边'] = np.min(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku = df_erp_sku[['sku1', '最长边', '次长边', '最短边', '重量', '成本', '重量来源']]
        df_erp_sku.columns = ['sku1', '长', '宽', '高', '重量', '成本', '重量来源']
        df_erp_sku = df_erp_sku.drop_duplicates()

        df_erp_sku = df_erp_sku.merge(df_sku, on=['sku1'])
        df_erp_sku['重量'] = df_erp_sku['重量'] * df_erp_sku['num']
        df_erp_sku['成本'] = df_erp_sku['成本'] * df_erp_sku['num']
        df_erp_sku['高'] = df_erp_sku['高'] * df_erp_sku['num']
        df_erp_sku['最长边'] = np.max(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku['次长边'] = np.median(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku['最短边'] = np.min(df_erp_sku[['长', '宽', '高']], axis=1)
        df_erp_sku.drop(['长', '宽', '高'], axis=1, inplace=True)
        df_erp_sku.rename(columns={'最长边': '长', '次长边': '宽', '最短边': '高'}, inplace=True)

        if df_erp_sku.shape[0] > 0:
            df1 = df_erp_sku.groupby(['sku', '数量'])[['高', '重量', '成本']].sum().reset_index()

            df2 = df_erp_sku[['sku', '数量', '长', '重量来源']]
            df2 = df2.sort_values(['长'], ascending=False)
            df2 = df2.drop_duplicates(['sku', '数量'], 'first')

            df3 = df_erp_sku[['sku', '数量', '宽']]
            df3 = df3.sort_values(['宽'], ascending=False)
            df3 = df3.drop_duplicates(['sku', '数量'], 'first')

            df_erp_sku = df_erp_sku[['sku', '数量']].drop_duplicates()
            df_erp_sku = df_erp_sku.merge(df1, on=['sku', '数量'], how='left')
            df_erp_sku = df_erp_sku.merge(df2, on=['sku', '数量'], how='left')
            df_erp_sku = df_erp_sku.merge(df3, on=['sku', '数量'], how='left')
            df_erp_sku['最长边'] = np.max(df_erp_sku[['长', '宽', '高']], axis=1)
            df_erp_sku['次长边'] = np.median(df_erp_sku[['长', '宽', '高']], axis=1)
            df_erp_sku['最短边'] = np.min(df_erp_sku[['长', '宽', '高']], axis=1)
            df_erp_sku.drop(['长', '宽', '高'], axis=1, inplace=True)
            df_erp_sku.rename(columns={'最长边': '长', '次长边': '宽', '最短边': '高'}, inplace=True)

            df = df.merge(df_erp_sku, on=['sku', '数量'], how='left')
            # for col in ['长', '宽', '高', '重量', '成本']:
            #     df[col] = df[col].fillna(0)
            if Organization != 1:
                df['成本'] = df['成本'] * Organization
                df['成本'] = df['成本'] + 2
        else:
            for col in ['长', '宽', '高', '重量', '成本', '重量来源']:
                df[col] = None
    else:
        for col in ['长', '宽', '高', '重量', '成本', '重量来源']:
            df[col] = 0
    return df


##
def get_oversea_info():
    date_id = time.strftime('%Y-%m-%d')
    # 运费
    sql = """
        SELECT
            sku, warehouseId warehouse_id, totalCost total_cost,
            totalCost_origin as total_cost_base, shippingCost,
            firstCarrierCost, new_firstCarrierCost, dutyCost, country, lowest_price
        FROM over_sea.oversea_transport_fee_useful_temu
        -- WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_useful_1 = conn.read_sql(sql)
    df_useful_1['is_temu'] = 1
    sql = """
        SELECT
            sku, warehouseId warehouse_id, totalCost total_cost,
            totalCost_origin as total_cost_base, shippingCost,
            firstCarrierCost, new_firstCarrierCost, dutyCost, country, lowest_price
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_useful_2 = conn.read_sql(sql)
    df_useful_2['is_temu'] = 0

    df_useful = pd.concat([df_useful_1, df_useful_2])
    df_useful = df_useful.sort_values(by='is_temu', ascending=False)
    df_useful = df_useful.drop_duplicates(subset=['sku', 'warehouse_id', 'country'])
    print(df_useful.info())

    df_useful['lowest_price'] = 1.2 * df_useful['shippingCost'] / (1 - 0.04 - 0.08-0.03)
    df_useful.drop(['dutyCost'], axis=1, inplace=True)
    # # df_useful['国家'] = df_useful['country'].replace({'US':'美国','UK':'英国','DE':'德国','FR':'法国','MX':'墨西哥','AU':'澳洲'})
    # # 20240722 新增分摊头程对应的总运费
    # sql = """
    #     SELECT warehouse_id, include_tax
    #     FROM `yibai_toucheng_new`
    #     """
    # df_toucheng = conn.read_sql(sql)
    # df_useful = pd.merge(df_useful, df_toucheng, how='left', on=['warehouse_id'])
    # df_useful['total_cost_base'] = np.where(df_useful['include_tax']==1,
    #                                         df_useful['total_cost'] - df_useful['new_firstCarrierCost']+df_useful['dutyCost'] + df_useful['firstCarrierCost'],
    #                                         df_useful['total_cost'] - df_useful['new_firstCarrierCost'] + df_useful['firstCarrierCost'])

    # 销售状态
    sql = f"""
        SELECT 
            sku, warehouse, sale_status
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)

    # 平台佣金
    sql = """
    SELECT 
        site as country, pay_fee, paypal_fee, vat_fee, extra_fee, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    for i in ['CZ', 'HU', 'PT']:
        new_row = df_fee[df_fee['country'] == 'DE'].copy()
        new_row['country'] = i
        df_fee = pd.concat([df_fee, new_row])
    df_fee = df_fee.drop_duplicates()

    return df_useful, df_status, df_fee


##
# asin
# 1、先取日销高的asin
# 2、剩余的匹配链接表
def get_asin():
    sql = """
        SELECT sku, seller_sku, account_id, quantity, order_id
        FROM over_sea.ads_oversea_order
        WHERE paytime >= '2023-04-07' and platform_code = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku_order = conn.read_sql(sql)
    # 取链接表
    sku_tuple = tuple(df_sku_order['sku'].unique())
    account_tuple = tuple(df_sku_order['account_id'].unique())

    sql = f"""
        with listing_table as (
            select distinct account_id, seller_sku 
            from yibai_product_kd_sync.yibai_amazon_sku_map 
            where deliver_mode=2 and account_id in {account_tuple}
        )
    
        select account_id, seller_sku, asin1, country
        from yibai_product_kd_sync.yibai_amazon_listing_alls a
        left join (
            select erp_id, site_code as country
            from yibai_sale_center_system_sync.yibai_system_account
            where platform_code = 'AMAZON'
        ) b on a.account_id = b.erp_id
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_ama_listing = conn_ck.ck_select_to_df(sql)

    df_sku_order = pd.merge(df_sku_order, df_ama_listing, how='left', on=['account_id', 'seller_sku'])

    df_sku_order_cnt = df_sku_order.groupby(['sku', 'country', 'asin1']).agg({'quantity': 'sum'}).reset_index()
    df_sku_order_cnt = df_sku_order_cnt.sort_values(by=['quantity'], ascending=False)

    df_sku_order_cnt['new_sku'] = df_sku_order_cnt['sku'].str.replace(r'^(GB|DE|US|AU)-', '')

    return df_sku_order_cnt


def get_asin_all():
    """
    获取目标SKU的asin
    """
    sql = f"""
    WITH listing_table as (
        SELECT distinct account_id, seller_sku, sku
        FROM yibai_product_kd_sync.yibai_amazon_sku_map a
        INNER JOIN (
            SELECT DISTINCT sku
            FROM yibai_oversea.temp_oversea_sku
        ) b ON a.sku = b.sku
        where deliver_mode=2
    )

    SELECT a.account_id, a.seller_sku, a.asin1 asin, b.sku sku, c.country
    FROM yibai_product_kd_sync.yibai_amazon_listing_alls a
    INNER JOIN listing_table b
    ON a.account_id = b.account_id and a.seller_sku = b.seller_sku
    LEFT JOIN (
        select erp_id, site_code as country
        from yibai_sale_center_system_sync.yibai_system_account
        where platform_code = 'AMAZON'
    ) c
    ON a.account_id = c.erp_id
    WHERE fulfillment_channel='DEF' and status = 1
    ORDER BY open_date DESC
    LIMIT 1 by sku, country
    """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)

    # df_listing = df_listing.drop_duplicates(subset=['sku'])

    return df_temp


def get_sales():
    # sku近30天销量
    sql = """

    SELECT  
        SKU as sku,warehouse_name, SUM(3days_sales) as 3days_sales,SUM(7days_sales) as 7days_sales,
        SUM(30days_sales) as 30days_sales,SUM(90days_sales) as 90days_sales
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse_name 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        )A 
    GROUP BY SKU, warehouse_name
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    return df_sales


##
def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct 
        case when from_currency_code = 'HUF' then 'HU' else country end as country, 
        from_currency_code as charge_currency,erp_rate rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = conn_ck.ck_select_to_df(sql)

    return df_rate


#
# df_rate = get_rate()
# df_rate.to_excel('F://Desktop//df_rate.xlsx', index=0)
## TEMU&SHEIN海外仓可用库存， Amazon和TEMU定价
def get_temu_shein_price():
    """
    海外仓sku，在temu的目标定价
    """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()
    # 取海外仓sku信息
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            sku, warehouse, best_warehouse_name, best_warehouse_id, title, linest, last_linest, new_price, gross, 
            product_package_size, available_stock, available_stock_money, on_way_stock, age_180_plus, `30days_sales`
        FROM over_sea.dwm_sku_info_temu
        WHERE date_id = '{date_today}' and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓') 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku_info = conn.read_sql(sql)

    # 基础数据存表，方便后续获取链接信息
    table_name = 'temp_oversea_sku'
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_execute_sql(sql=f'truncate table yibai_oversea.{table_name}')
    df_temp = df_sku_info[['sku', 'best_warehouse_name', 'best_warehouse_id']]
    df_temp.columns = ['sku', 'warehouse_name', 'warehouse_id']
    conn_ck.ck_insert(df_temp[['sku', 'warehouse_name', 'warehouse_id']], table_name, if_exist='append')

    # 获取海外仓运费、销售状态、平台配置表
    df_useful, df_status, df_fee = get_oversea_info()

    # 独享仓需替换为非独享仓的运费
    sql = """
        SELECT warehouse_id best_warehouse_id, real_warehouse_id
        FROM over_sea.yibai_warehouse_oversea_temp
        WHERE warehouse_name like '%%独享%%'
    """
    df_warehouse = conn.read_sql(sql)

    # df_useful['warehouse_id'] = df_useful['best_warehouse_id']
    df_sku_info = pd.merge(df_sku_info, df_warehouse, how='left', on=['best_warehouse_id'])
    df_sku_info['best_warehouse_id'] = np.where(df_sku_info['real_warehouse_id'].isna(),
                                                df_sku_info['best_warehouse_id'], df_sku_info['real_warehouse_id'])

    df_useful = df_useful.rename(columns={'warehouse_id': 'best_warehouse_id'})
    df_price = pd.merge(df_sku_info, df_useful, how='left', on=['sku', 'best_warehouse_id'])
    df_price = pd.merge(df_price, df_status, how='left', on=['sku', 'warehouse'])
    df_price = pd.merge(df_price, df_fee, how='left', on=['country'])

    #
    c1 = (df_price['warehouse'] == '美国仓') & (df_price['country'] == 'US')
    c2 = (df_price['warehouse'] == '澳洲仓') & (df_price['country'] == 'AU')
    c3 = (df_price['warehouse'] == '英国仓') & (df_price['country'] == 'UK')
    c4 = (df_price['warehouse'] == '德国仓') & (
        df_price['country'].isin(['DE', 'FR', 'ES', 'IT', 'PL', 'CZ', 'HU', 'PT']))
    c5 = (df_price['warehouse'] == '加拿大仓') & (df_price['country'] == 'CA')
    c6 = (df_price['warehouse'] == '法国仓') & (df_price['country'].isin(['DE', 'FR', 'ES', 'IT', 'PL', 'CZ']))
    c7 = (df_price['country'].isna())
    df_price = df_price[c1 | c2 | c3 | c4 | c5 | c6 | c7]

    #
    df_asin = get_asin()
    # 剩余的取链接表asin
    df_asin_all = get_asin_all()
    #
    df_rate = get_rate()
    #
    df_asin['country'] = df_asin['country'].replace('GB', 'UK')
    df_asin_all.columns = [i.split('.')[-1] for i in df_asin_all.columns]
    df_asin_all['country'] = df_asin_all['country'].replace('GB', 'UK')
    df_asin_0 = df_asin.drop_duplicates(subset=['sku', 'country'])
    df_asin_new = df_asin.drop_duplicates(subset=['new_sku', 'country'])
    #
    df_asin_us = df_asin_all[df_asin_all['country'] == 'US']
    df_asin_us = df_asin_us.rename(columns={'asin': 'asin_us'})

    df_price_result = pd.merge(df_price, df_asin_0[['sku', 'country', 'asin1']], how='left', on=['sku', 'country'])
    df_price_result = pd.merge(df_price_result, df_asin_new[['new_sku', 'country', 'asin1']],
                               how='left', left_on=['sku', 'country'], right_on=['new_sku', 'country'])
    df_price_result = pd.merge(df_price_result, df_asin_all[['sku', 'country', 'asin']], how='left',
                               on=['sku', 'country'])
    df_price_result = pd.merge(df_price_result, df_asin_us[['sku', 'asin_us']], how='left', on=['sku'])
    df_price_result = pd.merge(df_price_result, df_rate[['country', 'rate']], how='left', on=['country'])

    # 字段处理
    df_price_result['asin_0'] = np.where(~df_price_result['asin1_x'].isna(), df_price_result['asin1_x'],
                                         df_price_result['asin1_y'])
    df_price_result['asin_0'] = np.where(df_price_result['asin_0'].isna(), df_price_result['asin'],
                                         df_price_result['asin_0'])
    df_price_result['asin_0'] = np.where(df_price_result['asin_0'].isna(), df_price_result['asin_us'],
                                         df_price_result['asin_0'])

    df_price_result['new_price'] = df_price_result['new_price'].astype(float)

    df_price_result['亚马逊目标毛利率'] = (df_price_result['new_price'] + df_price_result['total_cost']) / (
            1 - df_price_result['pay_fee'] - 0 - df_price_result['extra_fee'] - df_price_result['vat_fee'] -
            df_price_result['platform_zero'] - df_price_result['platform_must_percent']) / df_price_result['rate']
    df_price_result['temu目标毛利率_本币'] = (df_price_result['new_price'] + df_price_result['total_cost']) / (
            1 - 0 - df_price_result['extra_fee'] -
            df_price_result['platform_zero'] + 0.05 - df_price_result['platform_must_percent']) / df_price_result[
                                                 'rate']
    #
    df_price_result['亚马逊目标毛利率_分摊头程'] = (df_price_result['new_price'] + df_price_result[
        'total_cost_base']) / (
                                                           1 - df_price_result['pay_fee'] - 0 - df_price_result[
                                                       'extra_fee'] - df_price_result['vat_fee'] -
                                                           df_price_result['platform_zero'] - df_price_result[
                                                               'platform_must_percent']) / df_price_result['rate']
    df_price_result['temu目标毛利率_分摊头程_本币'] = (df_price_result['new_price'] + df_price_result[
        'total_cost_base']) / (
                                                              1 - 0 - df_price_result['extra_fee'] -
                                                              df_price_result['platform_zero'] + 0.05 - df_price_result[
                                                                  'platform_must_percent']) / df_price_result['rate']

    df_price_result.drop(['asin1_x', 'new_sku', 'asin1_y', 'asin', 'asin_us', 'paypal_fee'], axis=1, inplace=True)

    # 法、西、意保留一条价格
    # 20240805 取消筛选
    # df_price_result_1 = df_price_result[df_price_result['country'] != 'DE']
    # df_price_result_2 = df_price_result[df_price_result['country'] == 'DE']
    # df_price_result_1 = df_price_result_1.sort_values(by='temu目标毛利率_本币', ascending=False).drop_duplicates(subset=['sku','warehouse'])
    # df_price_result = pd.concat([df_price_result_2, df_price_result_1])
    #
    df_price_result['sale_status'] = df_price_result['sale_status'].fillna('正常')
    # df_price_result = df_price_result[df_price_result['available_stock']>=10]
    #
    df_price_result.insert(loc=df_price_result.columns.get_loc('platform_zero') + 1, column='temu差值',
                           value=df_price_result['platform_zero'] - 0.05)
    df_price_result.insert(loc=df_price_result.columns.get_loc('platform_must_percent') + 1, column='temu净利率',
                           value=df_price_result['platform_must_percent'])

    col = ['sku', 'warehouse', 'best_warehouse_name', 'best_warehouse_id', 'title', 'linest', 'last_linest',
           'new_price', 'gross', 'product_package_size',
           'available_stock', 'available_stock_money', 'on_way_stock', 'total_cost', 'total_cost_base',
           'new_firstCarrierCost', 'firstCarrierCost', 'country', 'shippingCost', 'lowest_price', 'pay_fee', 'vat_fee',
           'extra_fee', 'platform_zero', 'temu差值', 'platform_must_percent', 'temu净利率', 'rate', 'age_180_plus',
           'sale_status', 'asin_0', '亚马逊目标毛利率',
           'temu目标毛利率_本币', '亚马逊目标毛利率_分摊头程', 'temu目标毛利率_分摊头程_本币']
    df_price_result = df_price_result[col]
    df_price_result.columns = ['sku', 'warehouse', '最优子仓', '最优子仓ID', '标题', '产品线大类', '产品线子类', '成本',
                               '毛重', '尺寸', '可用库存',
                               '可用库存金额', '在途库存', '总运费_自算头程', '总运费_分摊头程', '自算头程', '分摊头程',
                               '国家', '尾程', '销毁价', '平台佣金', 'vat费率', '汇损费率', 'amazon差值',
                               'temu差值', 'amazon净利率', 'temu净利率', 'rate', '超180库龄库存', '销售状态', 'ASIN',
                               '亚马逊目标毛利率', 'temu目标毛利率_本币',
                               '亚马逊目标毛利率_分摊头程', 'temu目标毛利率_分摊头程_本币']
    # df_price_result.to_excel('df_price_result.xlsx', index=0)
    save_df(df_price_result, 'TEMU&SHEIN海外仓可用库存定价', file_type='xlsx')


##

def temp():
    """
    按TEMU销售要求，输出数据
    """

    df_result = get_temu_shein_price()

    df_fenxiao = pd.read_excel('F:\Desktop\日常任务\分销定价\分销sku新版本定价20240507.xlsx')
    #
    df_result_0 = df_result.drop_duplicates(subset=['sku', 'warehouse'])
    df_fenxiao = df_fenxiao.rename(columns={'大仓': 'warehouse'})
    df_temp = pd.merge(df_fenxiao, df_result_0[['sku', 'warehouse', 'asin_0', '30days_sales']], how='left',
                       on=['sku', 'warehouse'])
    df_temp['供货系数1.25时的供货价'] = (df_temp['成本'] + df_temp['国内采购运费'] + df_temp['头程'] + df_temp['关税'] +
                                         df_temp['尾程']
                                         + df_temp['海外仓处理费'] + df_temp['超尺寸附加费'] + df_temp[
                                             '偏远附加费']) * 1.25

    df_temp.drop(['尾程', '头程', '关税', '包装类型费（异型附加费）', '偏远附加费', '超尺寸附加费',
                  '海外仓处理费', '复合打包费', '国内采购运费', '是否开放分销', '不开放分销原因'], axis=1, inplace=True)

    df_temp.to_excel('df_temp.xlsx', index=0)


def temp_fuc():
    df_sku_temp = pd.read_excel('F:\Desktop\工业汽摩sku.xlsx')
    sku_tuple = tuple(df_sku_temp['sku'].unique())
    df_listing = get_asin_all(sku_tuple)
    sql = f"""
         with listing_table as (
             select account_id, seller_sku, sku
             from yibai_product_kd_sync.yibai_amazon_sku_map 
             where deliver_mode=2 and sku in {sku_tuple} and account_name like '%美国%'
         )
    
         select a.account_id, a.seller_sku, b.sku, asin1
         from (
             SELECT account_id, seller_sku, asin1
             FROM yibai_product_kd_sync.yibai_amazon_listing_alls
             where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
         ) a
         INNER JOIN listing_table b
         ON a.account_id = b.account_id and a.seller_sku = b.seller_sku        
    
     """
    conn_ck = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
    df_ama_listing_2 = conn_ck.ck_select_to_df(sql)
    df_ama_listing_2.to_excel('sku_listing.xlsx', index=0)


##
# 海外仓sku调价目标净利率
def get_oversea_target_price():
    """

    """
    date_id = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT 
            a.sku sku, a.warehouse warehouse, best_warehouse_name, best_warehouse_id, available_stock, sales_status, 
            overage_level, day_sales, estimated_sales_days, target_profit_rate, platform, country, 
            new_price, total_cost
        FROM (
            SELECT 
                sku, warehouse, best_warehouse_name, best_warehouse_id, available_stock, sales_status, 
                overage_level,day_sales, estimated_sales_days, target_profit_rate, platform, country, price_rmb, 
                new_price, total_cost
            FROM yibai_oversea.dwm_oversea_price_dtl
            WHERE 
            warehouse in ('美国仓','德国仓','法国仓','英国仓','澳洲仓') 
            and available_stock > 0 and platform = 'AMAZON'
            and date_id = '{date_id}'
        ) a
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_price_dtl = conn_ck.ck_select_to_df(sql)
    sql = f"""
        SELECT sku, warehouse, age_180_plus
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_id}' and available_stock > 0
    """
    df_sku_info = conn.read_sql(sql)
    df_price_dtl = pd.merge(df_price_dtl, df_sku_info, how='left', on=['sku', 'warehouse'])
    print('海外仓调价数据获取完成...')

    # 取平台费率
    df_platform_fee = get_platform_fee('AMAZON')
    df_platform_fee['platform'] = 'AMAZON'

    df_price_dtl = pd.merge(df_price_dtl, df_platform_fee, how='left', on=['platform', 'country'])
    # 暂时只取美国仓和澳洲仓数据
    # df_price_dtl = df_price_dtl[df_price_dtl['warehouse'].isin(['美国仓', '澳洲仓'])]

    # 处理目标净利率，计算目标定价
    c1 = (df_price_dtl['target_profit_rate'] < 0)
    df_price_dtl['target_profit_rate'] = np.where(c1, 0, df_price_dtl['target_profit_rate'])
    df_rate = get_rate()
    df_price_dtl = pd.merge(df_price_dtl, df_rate, how='left', on=['country'])
    df_price_dtl['目标定价_人民币'] = (df_price_dtl['new_price'] + df_price_dtl['total_cost']) / (
            1 - df_price_dtl['ppve'] - df_price_dtl['platform_zero'] - df_price_dtl['target_profit_rate'])
    df_price_dtl['目标定价_本币'] = df_price_dtl['目标定价_人民币'] / df_price_dtl['rate']

    return df_price_dtl


## 补充amazon当前最低价asin
def get_amazon_asin_price():
    """
    补充amazon当前最低价asin
    """
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT account_id, sku, upper(site) as country, seller_sku, asin, online_price
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing
        WHERE `date_id` = '{date_today}' and online_price > 0
        ORDER BY status ASC
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_amazon_listing = conn_ck.ck_select_to_df(sql)
    #
    df_amazon_listing = df_amazon_listing.sort_values(by='online_price', ascending=True).drop_duplicates(
        subset=['sku', 'country'])
    #
    df_rate = get_rate()
    df_amazon_listing = pd.merge(df_amazon_listing, df_rate[['country', 'rate']], how='left', on=['country'])
    df_amazon_listing['online_price_rmb'] = df_amazon_listing['online_price'] * df_amazon_listing['rate']

    return df_amazon_listing


##
def get_temu_oversea_price():
    """
    TEMU清仓定价。
    输出美、澳仓当前海外仓定价。及超120天库龄情况
    """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()
    print(1)
    df_price_dtl = get_oversea_target_price()
    df_amazon_listing = get_amazon_asin_price()
    df_result = pd.merge(df_price_dtl, df_amazon_listing[['sku', 'country', 'asin', 'online_price_rmb']], how='left',
                         on=['sku', 'country'])

    # 匹不到的asin用us站点的asin代替
    df_listing_temp = df_amazon_listing[df_amazon_listing['country'] == 'US']
    df_listing_temp = df_listing_temp.rename(columns={'asin': 'asin_temp', 'online_price_rmb': 'online_price_rmb_temp'})
    df_result = pd.merge(df_result, df_listing_temp[['sku', 'asin_temp', 'online_price_rmb_temp']], how='left',
                         on=['sku'])
    #
    # us_rate = df_rate.loc[df_rate['country']=='US','rate']
    df_rate = get_rate()
    us_rate = df_rate[df_rate['country'] == 'US'].iloc[0, 2]
    #
    df_result['asin'] = np.where(~df_result['asin'].isna(), df_result['asin'], df_result['asin_temp'])
    df_result['online_price_rmb'] = np.where(~df_result['asin'].isna(), df_result['online_price_rmb'],
                                             df_result['online_price_rmb_temp'])
    df_result['online_price_rmb'] = np.where(~df_result['online_price_rmb'].isna(), df_result['online_price_rmb'],
                                             df_result['目标定价_人民币'])
    df_result['Amazon当前最低定价USD'] = df_result['online_price_rmb'] / us_rate
    df_result['Amazon当前最低定价USD * 0.75'] = df_result['Amazon当前最低定价USD'] * 0.75
    df_result['0.75*Amazon售价下TEMU对应订单利润率'] = 1 - 0.04 - (df_result['new_price'] + df_result['total_cost']) / (
            df_result['Amazon当前最低定价USD * 0.75'] * us_rate)
    df_result['0.75*Amazon售价下TEMU对应净利率'] = 1 - 0.04 - 0.14 - (
                df_result['new_price'] + df_result['total_cost']) / (
                                                           df_result['Amazon当前最低定价USD * 0.75'] * us_rate)
    df_result['TEMU 6%净利对应售价'] = (df_result['new_price'] + df_result['total_cost']) / (
                1 - 0.04 - 0.14 - 0.06) / us_rate
    df_result['TEMU 0%净利对应售价'] = (df_result['new_price'] + df_result['total_cost']) / (
                1 - 0.04 - 0.14 - 0) / us_rate

    df_result.drop(['asin_temp', 'online_price_rmb_temp', 'refound_fee', 'asin', 'day_sales'], axis=1, inplace=True)
    # 暂时只需要澳洲仓、美国仓
    # df_result = df_result[(df_result['warehouse'].isin(['澳洲仓','美国仓'])) & (df_result['country'].isin(['US','AU']))]
    # df_result.to_excel('df_result.xlsx', index=0)
    save_df(df_result, 'TEMU海外仓sku当前调价目标定价数据', file_type='xlsx')


## 产品线
# 一级产品线
def get_sku_line(df):
    sql_line = f"""
            select distinct a.sku sku, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id) 
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    df = pd.merge(df, df_line, how='left', on=['sku'])

    return df


## 海外仓sku销量匹配
def get_oversea_sku_sales():
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT sku, warehouse, best_warehouse_name,new_price, available_stock, overage_level,`30days_sales`,
        `90days_sales`, title
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '{date_today}' and warehouse in ('美国仓', '澳洲仓','英国仓','德国仓','法国仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sku_info = conn.read_sql(sql)
    #
    sql = """
        SELECT sku ,warehouse ,sale_status 
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_status = conn.read_sql(sql)
    df_sku_info = pd.merge(df_sku_info, df_status, how='left', on=['sku', 'warehouse'])
    df_sku_info['sale_status'] = df_sku_info['sale_status'].fillna('正常')
    #
    sql = """

    SELECT  
        SKU as sku,SUM(3days_sales) as 3days_sales,SUM(7days_sales) as 7days_sales,SUM(30days_sales) as 30days_sales,
        SUM(60days_sales) as 60days_sales,
        SUM(90days_sales) as 90days_sales,
        SUM(3days_sales)/3*0.9+SUM(7days_sales)/7*0.1 AS 'recent_day_sales',
        SUM(7days_sales)/7*0.9+SUM(30days_sales)/30*0.1 AS 'day_sales', 
        warehouse
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,60days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        )A 
    GROUP BY SKU, warehouse

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)
    #
    df_result = pd.merge(df_sku_info, df_sales[['sku', 'warehouse', '60days_sales']], how='left',
                         on=['sku', 'warehouse'])
    df_result['60days_sales'] = df_result['60days_sales'].fillna(0)

    df_result = get_sku_line(df_result)
    # 近90天有到货产品
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    sql = """
        select distinct sku, warehouse_id
        from yb_datacenter.yb_stock 
        where create_time>=subtractDays(now(),90) and available_stock > 0  and cargo_owner_id = 8
        and warehouse_id in ( SELECT distinct id FROM yb_datacenter.yb_warehouse WHERE type IN ('third', 'overseas') )
    """
    df_new_sku = ck_client.ck_select_to_df(sql)
    sql = """
        SELECT warehouse_id, warehouse
        FROM yibai_warehouse_oversea_temp
    """
    df_warehouse = conn.read_sql(sql)
    df_new_sku = pd.merge(df_new_sku, df_warehouse, how='left', on=['warehouse_id'])
    #
    df_new_sku['is_90_new'] = 1
    df_result = pd.merge(df_result, df_new_sku[['sku', 'warehouse', 'is_90_new']], how='left', on=['sku', 'warehouse'])
    #
    # df_result.to_excel('df_result.xlsx', index=0)
    df_result = df_result.drop_duplicates()
    save_df(df_result, 'TEMU_海外仓sku销量数据', file_type='xlsx')

    return None


############################### TEMU目标sku定价
def temu_ym_price():
    df_sku = pd.read_excel('F://Desktop//TEMU补计算.xlsx', dtype={'sku': str})
    #
    df_sku = df_sku[['唯一标识', '公司SKU', '最贵发货站点']]
    #
    df_sku.columns = ['id', 'sku', 'country']
    df_sku['sku'] = df_sku['sku'].astype(str)
    #
    df_sku_info = df_sku.assign(column_to_split=df_sku['site'].str.split('、')).explode('column_to_split')
    #
    df_sku_info['sku'] = df_sku_info['sku'].astype(str)
    # 取成本
    df_sku_info['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku_info = chicun_zhongliang(df_sku_info, 1, conn_ck)
    #
    dic = {'意大利站': 'IT', '西班牙站': 'ES', '德国站': 'DE', '法国站': 'FR', '英国站': 'UK', '澳大利亚站': 'AU',
           '美国站': 'US',
           '法国': 'FR', '意大利': 'IT', '美国': 'US'}
    df_sku_info['country'] = df_sku_info['column_to_split'].replace(dic)
    #
    df_sku_info = df_sku[['id', 'sku', 'site']]

    # 取运费
    sku = tuple(df_sku['sku'].unique())
    step = 200
    df_fee_result = pd.DataFrame()
    for i in range(int(len(sku) / step) + 1):
        sku_tuple = sku[i * step:(i + 1) * step]
        sql = f"""
            SELECT 
                sku, warehouse, warehouseName as warehouse_name, country,  totalCost, firstCarrierCost `分摊头程`, 
                shippingCost `尾程`, dutyCost 关税, remoteExtraFee as 偏远附加费,
                extraSizeFee as 超尺寸附加费,overseasFee as 海外仓处理费
            FROM over_sea.oversea_transport_fee_useful
            WHERE warehouse in ('美国仓','德国仓','英国仓','澳洲仓') and platform = 'AMAZON' and sku in {sku_tuple}
            ORDER BY totalCost DESC
        """
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        df_fee = conn.read_sql(sql)
        df_fee_result = pd.concat([df_fee, df_fee_result])
    #
    c1 = (df_fee_result['warehouse'] == '英国仓') & (df_fee_result['country'] != 'UK')
    c2 = (df_fee_result['warehouse'].isin(['德国仓'])) & (df_fee_result['country'] == 'UK')
    c3 = df_fee_result['warehouse'] == '法国仓'
    df_fee_result = df_fee_result[(~c1) & (~c2) & (~c3)]
    #
    df_fee_result = df_fee_result[~df_fee_result['warehouse_name'].str.contains('波兰|递四方|亚马逊')]
    #
    df_result = pd.merge(df_sku_info, df_fee_result, how='left', on=['sku', 'country'])
    df_result = df_result.sort_values(by='totalCost', ascending=False).drop_duplicates(subset=['sku', 'id'])
    df_sku_result = pd.merge(df_sku, df_result[['id', '成本', 'country', 'warehouse_name', 'warehouse', 'totalCost']],
                             how='left', on=['id'])
    #
    # df_sku_result.to_excel('df_sku_result.xlsx', index=0)
    #
    df_fenxiao = pd.read_excel(
        'F://yibai-price-strategy//data//2024-07-09//分销sku新版本定价//分销sku新版本定价_增加目的国_0.xlsx',
        dtype={'sku': str})
    #
    df_fenxiao = df_fenxiao.rename(columns={'收货站点': 'country', '大仓': 'warehouse'})
    df_fenxiao_0 = df_fenxiao[df_fenxiao['warehouse'].isin(['美国仓', '德国仓', '英国仓', '法国仓', '澳洲仓'])]
    c1 = (df_fenxiao_0['warehouse'] == '英国仓') & (df_fenxiao_0['country'] != 'UK')
    c2 = (df_fenxiao_0['warehouse'].isin(['德国仓'])) & (df_fenxiao_0['country'] == 'UK')
    df_fenxiao_0 = df_fenxiao_0[(~c1) & (~c2)]
    #
    # df_fenxiao_0 = df_fenxiao_0[~df_fenxiao_0['warehouse_name'].str.contains('波兰|递四方|亚马逊')]
    df_result_0 = pd.merge(df_sku, df_fenxiao_0[['sku', 'country', 'warehouse', '最优发货子仓', '供货价格']],
                           how='left', on=['sku', 'country'])
    df_result_0 = df_result_0.sort_values(by='供货价格', ascending=False).drop_duplicates(subset=['sku', 'id'])
    df_result_0 = pd.merge(df_sku, df_result_0[['id', '成本', 'country', '最优发货子仓', 'warehouse', '供货价格']],
                           how='left', on=['id'])
    #
    df_result_0.to_excel('df_result_0.xlsx', index=0)



def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku new_sku, title_cn `产品名称`, b.category_path as `产品线路线`,develop_source
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
    df_line = df_line.drop_duplicates(subset='new_sku')
    c1 = df_line['develop_source'].isin([14, 15, 22])
    c2 = df_line['产品名称'].str.contains('通拓')
    df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)

    df = pd.merge(df, df_line[['new_sku', '一级产品线', '二级产品线', 'is_tt_sku']], how='left', on=['new_sku'])

    return df


def get_temu_account():
    # temu账号获取
    sql = """
    select 
        a.account_id `账号ID`,a.account_status `账号状态`,a.account_id `平台账号ID`, b.main_name `主体账号`,a.oa_department_name `大部`,
        a.oa_group_name `小组` ,a.account_name `账号全称`,a.account_s_name `账号简称`,a.account_num_name `账号店铺名`,a.account_operation_mode `账号运营模式` 
    from yibai_account_manage_sync.yibai_temu_account a
    left join yibai_account_manage_sync.yibai_account_main b
    on a.main_id=b.main_id 
    where a.account_type=1
    """
    conn_ck = pd_to_ck(database='yibai_account_manage_sync', data_sys='调价明细历史数据')
    df_temu_account = conn_ck.ck_select_to_df(sql)
    df_temu_account['账号状态'] = df_temu_account['账号状态'].map({10: '启用', 20: '停用', 30: '异常', })
    df_temu_account['账号运营模式'] = df_temu_account['账号运营模式'].map({1: '全托管', 2: '半托管'})
    df_temu_account.rename(columns={'账号ID': 'account_id'}, inplace=True)

    return df_temu_account


def check_temu_order():
    """
    temu订单利润明细核对
    """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()
    sql = f"""

        SELECT
            order_id, platform_code, sku, seller_sku, account_id, paytime, total_price,true_profit_new1, real_profit, 
            warehouse_name, warehouse, quantity, release_money, sales_status
        FROM over_sea.dashbord_new1
        WHERE 
            paytime >= '2024-05-01' and paytime <= '2024-08-01'
            and `total_price` > 0 
            and `sales_status` not in ('','nan','总计')
            and platform_code != 'TEMU'
            -- and warehouse = '英国仓'
            and platform_code = 'EB'
    """
    conn_ck = pd_to_ck(database='over_sea', data_sys='数据部服务器')
    df_order_info = conn_ck.ck_select_to_df(sql)
    print(df_order_info.info())
    df_order_info = df_order_info.sample(10000)
    # df_order_info.to_excel('df_order_info.xlsx', index=0)
    # temu订单
    step = 100
    account_tuple = tuple(df_order_info['account_id'].unique())
    df_order_profit = pd.DataFrame()
    for i in range(int(len(account_tuple) / step) + 1):
        accont_id = account_tuple[i * step:(i + 1) * step]
        sql = f"""
                WITH temp_order as (
                    SELECT order_id, ship_country
                    FROM yibai_oms_sync.yibai_oms_order
                    WHERE platform_code != 'TEMU' and account_id in {accont_id}
                )
                SELECT                     
                    order_id, ship_country country, total_price, product_price, shipping_price, commission_fees, pay_cost, escrow_tax, purchase_cost_new1,
                    shipping_cost, ship_cost_second,true_shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, 
                    extra_price,exceedprice,stock_price, exchange_price, true_profit2,true_profit_rate2,true_profit_new1,true_profit_rate2
                FROM temp_order a
                LEFT JOIN (
                    SELECT
                        order_id,  total_price, product_price,shipping_price, commission_fees, pay_cost, escrow_tax, purchase_cost_new1,
                        shipping_cost, ship_cost_second,true_shipping_fee, first_carrier_cost, duty_cost,  processing, package_cost, 
                        extra_price,exceedprice,stock_price, exchange_price, true_profit2,true_profit_rate2,true_profit_new1,true_profit_rate2
                    FROM yibai_oms_sync.yibai_oms_order_profit 
                    WHERE order_id in (SELECT order_id FROM temp_order)
                ) p ON p.order_id = a.order_id    
        """
        conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
        df_order_temp = conn_ck.ck_select_to_df(sql)
        df_order_profit = pd.concat([df_order_temp, df_order_profit])
    #
    df_order_profit = df_order_profit[['order_id', 'country', 'shipping_price']]
    df = pd.merge(df_order_info, df_order_profit, how='left', on=['order_id'])
    # 获取sku的类目
    df = get_line(df)
    # 获取账号信息
    df_temu_account = get_temu_account()
    df = pd.merge(df, df_temu_account[['account_id', '主体账号']], how='left', on=['account_id'])
    save_df(df, 'TEMU海外仓订单利润明细', file_type='xlsx')
    return df


##
def get_oversea_warehouse():
    sql = """
        SELECT 
            yw.id AS warehouse_id,   
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code,
            yw.country, ebay_category_id,
            ywc.name AS warehouse, yw.warehouse_type
        FROM yibai_logistics_tms_sync.yibai_warehouse yw
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        -- WHERE yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)
    df_warehouse.columns = [i.split('.')[-1] for i in df_warehouse.columns]

    return df_warehouse


def temp_temu_order():
    """
    temu订单利润明细核对
    """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()
    sql = f"""
    WITH temp_order as (
        SELECT distinct 
            order_id,platform_order_id, account_id, payment_time, refund_status,platform_code,
            warehouse_id, ship_country
        FROM yibai_oms_sync.yibai_oms_order
        WHERE payment_time >= '2024-05-01' 
        and payment_time < '2024-09-11'
        and platform_code = 'TEMU' 
        and payment_status=1 and order_status <> 80
        -- and warehouse_id not in (60,323)
        -- LIMIT 1000
    )
    
    SELECT distinct 
        A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku,B.account_id,
        B.payment_time as paytime, toFloat64OrZero(C.total_price) total_price, A.quantity,
        case 
            WHEN toInt64OrZero(C.true_shipping_fee) > 0 THEN C.true_profit_new1
            else C.profit_new1 
        end as true_profit_new1, C.shipping_price,
        CASE
            WHEN B.refund_status = 0 THEN '未退款'
            WHEN B.refund_status = 1 THEN '退款中'
            WHEN B.refund_status = 2 THEN '部分退款' 
            when B.refund_status=3 then '全部退款' 
            ELSE ''
        END AS refound_status,
        B.warehouse_id, B.ship_country country,
        F.sales_status
    FROM (
        SELECT id, order_id, quantity, seller_sku
        FROM yibai_oms_sync.yibai_oms_order_detail
        WHERE order_id in (SELECT order_id FROM temp_order)
    ) A
    LEFT JOIN temp_order B ON A.order_id=B.order_id
    LEFT JOIN (
        SELECT order_id, total_price, true_shipping_fee,true_profit_new1, profit_new1,shipping_price
        FROM yibai_oms_sync.yibai_oms_order_profit 
        WHERE order_id in (SELECT order_id FROM temp_order)
    ) C ON B.order_id=C.order_id
    LEFT JOIN (
        SELECT order_id, order_detail_id, sku, sales_status
        FROM yibai_oms_sync.yibai_oms_order_sku
        WHERE order_id in (SELECT order_id FROM temp_order)
    ) F ON F.order_detail_id=A.id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    df = df[(df['total_price'] > 0) & (df['total_price'] != '')]
    df_warehouse = get_oversea_warehouse()
    df = pd.merge(df, df_warehouse[['warehouse_id', 'warehouse_name',
                                    'warehouse', 'warehouse_type']], how='left', on=['warehouse_id'])
    df['sales_status'] = df['sales_status'].fillna('正常')
    #

    # 获取sku的类目
    df = get_line(df)
    # 获取账号信息
    df_temu_account = get_temu_account()
    df = pd.merge(df, df_temu_account[['account_id', '主体账号']], how='left', on=['account_id'])

    df = df.rename(columns={'total_price': '总收入', 'true_profit_new1': '毛利润', 'quantity': '销量'})
    df[['总收入', '毛利润']] = df[['总收入', '毛利润']].astype(float)
    df['毛利润率'] = df['毛利润'] / df['总收入']
    df = df[df['毛利润率'] > 0.15]
    df['country'] = df['country'].replace({'GB': 'UK'})
    df = df[df['country'] != 'CN']
    print(df.info())
    # save_df(df, 'TEMU海外仓订单利润明细', file_type='xlsx')
    # sku+country维度
    df_all = df.groupby(['主体账号', 'sku', 'country']).agg(
        {'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix('_5月至今').reset_index()
    df_60 = df[df['paytime'] > '2024-07-12'].groupby(['主体账号', 'sku', 'country']).agg(
        {'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix('_近60天').reset_index()
    df_30 = df[df['paytime'] > '2024-08-11'].groupby(['主体账号', 'sku', 'country']).agg(
        {'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix('_近30天').reset_index()
    #
    df_final = pd.merge(df_all, df_60, how='left', on=['主体账号', 'sku', 'country'])
    df_final = pd.merge(df_final, df_30, how='left', on=['主体账号', 'sku', 'country'])
    df_final = df_final.fillna(0)
    df_final['加权日销'] = 0.5 * df_final['销量_近30天'] / 30 + 0.3 * df_final['销量_近60天'] / 60 + 0.2 * df_final[
        '销量_5月至今'] / 133
    df_sku = df[['sku', '产品名称', '一级产品线', '二级产品线', '三级产品线']].drop_duplicates()
    df_final = pd.merge(df_final, df_sku, how='left', on=['sku'])
    #
    save_df(df_final, 'TEMU订单1', file_type='xlsx')
    # df_final.to_excel('df_final_country.xlsx', index=0)
    # sku维度
    df_all = df.groupby(['主体账号', 'sku']).agg({'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix(
        '_5月至今').reset_index()
    df_60 = df[df['paytime'] > '2024-07-12'].groupby(['主体账号', 'sku']).agg(
        {'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix('_近60天').reset_index()
    df_30 = df[df['paytime'] > '2024-08-11'].groupby(['主体账号', 'sku']).agg(
        {'总收入': 'sum', '毛利润': 'sum', '销量': 'sum'}).add_suffix('_近30天').reset_index()
    df_final = pd.merge(df_all, df_60, how='left', on=['主体账号', 'sku'])
    df_final = pd.merge(df_final, df_30, how='left', on=['主体账号', 'sku'])
    df_final = df_final.fillna(0)
    df_final['加权日销'] = 0.5 * df_final['销量_近30天'] / 30 + 0.3 * df_final['销量_近60天'] / 60 + 0.2 * df_final[
        '销量_5月至今'] / 133
    df_sku = df[['sku', '产品名称', '一级产品线', '二级产品线', '三级产品线']].drop_duplicates()
    df_final = pd.merge(df_final, df_sku, how='left', on=['sku'])
    #
    # df_final.to_excel('df_final_sku.xlsx', index=0)
    save_df(df_final, 'TEMU订单2', file_type='xlsx')

    return None


##
def ym_temu_price_check():
    ck_client = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    #
    df_sku_temp = pd.read_excel('F://Desktop//新大仓查询美仓此刻库存.xlsx', dtype={'sku': str})
    #
    df_sku_temp = df_sku_temp[['唯一标识', 'SKU', 'warehouse_name']]
    df_sku_temp['SKU'] = df_sku_temp['SKU'].fillna(method='ffill')
    df_sku_temp.columns = ['唯一标识', 'sku', 'warehouse_name']
    #
    sql = """
        SELECT 
            sku, warehouse, warehouseName as warehouse_name, country, shippingCost `尾程`, totalCost, firstCarrierCost `分摊头程`, 
            new_firstCarrierCost `自算头程`, dutyCost 关税, remoteExtraFee as 偏远附加费,
            extraSizeFee as 超尺寸附加费,overseasFee as 海外仓处理费
        FROM over_sea.oversea_transport_fee_useful
        WHERE warehouse in ('美国仓','德国仓','英国仓','法国仓','澳洲仓') and platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee = conn.read_sql(sql)
    #
    df_fee = df_fee.sort_values(by='totalCost', ascending=True)
    #
    sql = f"""
        SELECT
            sku, 
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `成本`
        FROM yibai_prod_base_sync.yibai_prod_sku
    """
    sku_info_temp = ck_client.ck_select_to_df(sql)
    #
    df = pd.merge(df_sku_temp, df_fee, how='left', on=['sku', 'warehouse_name'])
    df = pd.merge(df, sku_info_temp, how='left', on=['sku'])

    #
    df.to_excel('df.xlsx', index=0)

    #
    df_temp = pd.read_excel('F://Desktop//TEMU订单排查0703.xlsx', dtype={'sku': str})
    #
    df_temp = df_temp.rename(columns={'系统SKU': 'sku'})
    df_temp['country'] = df_temp['country'].replace('GB', 'UK')
    df_result = pd.merge(df_temp, df_fee, how='left', on=['sku', 'warehouse_name', 'country'])
    #
    df_result = pd.merge(df_result, sku_info_temp, how='left', on=['sku'])
    df_result['国内采购运费'] = df_result['成本'] * 0.013
    #
    sql = """
        SELECT a.sku sku, a.warehouse warehouse, overage_level,IF(b.sale_status IS NULL ,'正常',b.sale_status) as '销售状态'
        FROM over_sea.dwm_sku_temp_info a
        LEFT JOIN over_sea.oversea_sale_status b
        ON a.sku = b.SKU and a.warehouse = b.warehouse and b.end_time IS NULL 
        WHERE a.date_id = '2024-07-09' 
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_age = conn.read_sql(sql)
    #
    df_result = df_result.rename(columns={'warehouse_x': 'warehouse'})
    df_result = pd.merge(df_result, df_age, how='left', on=['sku', 'warehouse'])
    #
    c1 = (df_result['warehouse'].isin(['美国仓', '英国仓', '德国仓', '法国仓', '澳洲仓'])) & (
            df_result['overage_level'] >= 180)
    c2 = (df_result['warehouse'].isin(['美国仓', '英国仓', '德国仓', '法国仓', '澳洲仓'])) & (
            df_result['overage_level'] == 150)
    c3 = (df_result['warehouse'].isin(['美国仓', '英国仓', '德国仓', '法国仓', '澳洲仓'])) & (
            df_result['overage_level'] == 120)
    c4 = (df_result['warehouse'].isin(['美国仓', '英国仓', '德国仓', '法国仓', '澳洲仓'])) & (
            df_result['overage_level'] == 90)
    c5 = (df_result['warehouse'].isin(['美国仓', '英国仓', '德国仓', '法国仓', '澳洲仓'])) & (
            df_result['overage_level'] < 90)
    df_result['供货系数'] = np.select([c1, c2, c3, c4, c5], [1.1, 1.15, 1.2, 1.25, 1.3], 0)
    #
    df_result.to_excel('df_result.xlsx', index=0)


#############
# 未匹配到运费&非代销仓sku，调用接口运费
def get_interf_fee(df):
    df_bundle = df[(df['total_cost'].isna()) & (df['latest_quotation'].isna())]
    df_bundle = df_bundle[['sku', 'column_to_split']].drop_duplicates()
    #
    df_bundle['数量'] = 1
    df_bundle = df_bundle.reset_index(drop=True).reset_index()
    dic = {'US': '47,49,50,58', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
           'AU': '353,769', 'CZ': '325', 'PL': '325'}
    df_bundle['warehouse_id'] = df_bundle['column_to_split'].replace(dic)
    df_bundle['sku'] = df_bundle['sku'].replace(
        {'1*JY03556': 'JY03556', '1*JYA02556-01': 'JYA02556-01', '1*DS00500': 'DS00500', '1*DS01567': 'DS01567',
         '5591*6-': '5591*6', '5591*5-': '5591*5'})
    #
    # df_bundle = df_bundle.sample(1000)
    df_bundle = sku_and_num_split(df_bundle)
    df_bundle_fee = get_bundle_fee(df_bundle)
    df_bundle_fee = df_bundle_fee[['sku', 'shipCountry', 'warehouseName', 'shipName', 'totalCost']]

    return df_bundle_fee


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
        group3 = group3[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost', 'shippingCost', 'firstCarrierCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True).drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    return df_result


def get_useful_fee(df):
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
            and warehouseName not like '%%TT%%' and warehouseName not like '%%亚马逊%%'
            and warehouseName not in ('MBB波兰仓','递四方英国伦敦仓库（亚马逊FBA退货上架）')
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
            sku, warehouse, sale_status sales_status
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_status = conn.read_sql(sql)
    df = pd.merge(df, df_status, how='left', on=['sku', 'warehouse'])

    return df


def check_temu_online_price():
    """
    TEMU在线链接的毛利润率测算。
    因不确定链接模式，运费数据获取顺序：
        海外仓调价数据的运费 >> 海外仓接口运费 >> 剔除代销仓 >> 虚拟仓运费
    """
    utils.program_name = 'TEMU链接订单利润率核算'
    make_path()
    df_rate0 = get_rate()
    df_rate = df_rate0[df_rate0['country'] == 'US']
    # 测算temu申报价格的订单利润率
    print('获取TEMU链接信息...')
    # df_temu = pd.read_excel('F:\Desktop\Temu数据统计20240821.xlsx')
    df_temu_base = temu_listing()
    df_temu_base = df_temu_base.rename(
        columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格', 'main_name': '主体账号'})
    df_temu_base = df_temu_base[
        df_temu_base['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止'])]
    df_temu_base = df_temu_base[~df_temu_base['sku'].isna()]
    df_temu_base['sku'] = df_temu_base['sku'].astype(str)
    # sku拆分捆绑、仓标。责任类目匹配
    df_temu_base['nb_sku'] = df_temu_base['sku'].map(extract_correct_string)
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_temu_base['new_sku'] = df_temu_base['nb_sku']
    df_temu_base['new_sku'] = df_temu_base['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    df_temu_base = get_line(df_temu_base)
    df_temu_base = get_main_resp(df_temu_base)
    print(f'有效链接共{len(df_temu_base)}条.')
    # 处理站点
    df_sku = df_temu_base[['唯一标识', 'site_code', 'online_status', 'sku', '加入站点时间', '站点']]
    df_sku = df_sku.assign(column_to_split=df_sku['site_code'].str.split(',')).explode('column_to_split')
    df_sku['column_to_split'] = df_sku['column_to_split'].replace({'SP': 'ES', 'GB': 'UK', 'NZ': 'AU'})
    df_sku['site'] = df_sku['column_to_split'].replace(
        {'AU': '澳大利亚', 'US': '美国', 'DE': '德国', 'FR': '法国', 'IT': '意大利',
         'ES': '西班牙', 'UK': '英国', 'NZ': '新西兰'})
    df_sku['数量'] = 1
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_sku = chicun_zhongliang(df_sku, 1, conn_ck)
    df_sku.drop(['数量', '重量', '重量来源', '长', '宽', '高'], axis=1, inplace=True)
    # # 获取海外仓调价使用运费数据
    # df_sku_1 = df_sku[(df_sku['加入站点时间'] >= '2024-07-01') & (df_sku['加入站点时间'] < '2024-08-01')]
    # df_sku_2 = df_sku[(df_sku['加入站点时间'] >= '2024-06-01') & (df_sku['加入站点时间'] < '2024-07-01')]
    # df_sku_3 = df_sku[(df_sku['加入站点时间'] >= '2024-05-01') & (df_sku['加入站点时间'] < '2024-06-01')]
    # df_sku_4 = df_sku[(df_sku['加入站点时间'] >= '2024-04-01') & (df_sku['加入站点时间'] < '2024-05-01')]
    # df_sku_5 = df_sku[(df_sku['加入站点时间'] >= '2024-03-01') & (df_sku['加入站点时间'] < '2024-04-01')]
    # df_sku_6 = df_sku[(df_sku['加入站点时间'] >= '2024-08-01') & (df_sku['加入站点时间'] < '2024-09-01')]
    # df_sku_7 = df_sku[(df_sku['加入站点时间'] >= '2024-09-01') & (df_sku['加入站点时间'] < '2024-10-01')]
    # df_sku_8 = df_sku[(df_sku['加入站点时间'] >= '2024-10-01') | (df_sku['加入站点时间'] < '2024-01-01')]
    #
    # date_list = ['2024-07-22', '2024-06-11', '2024-05-10', '2024-04-10', '2024-03-11','2024-08-20','2024-09-10',
    #              '2024-10-08']
    # sku_list = [df_sku_1, df_sku_2, df_sku_3, df_sku_4, df_sku_5, df_sku_6, df_sku_7, df_sku_8]
    # print('获取海外仓调价运费数据...')
    # df_result = pd.DataFrame()
    # for df_sku_1, date in zip(sku_list, date_list):
    #     sql = f"""
    #         SELECT sku, new_price, total_cost, best_warehouse_name, country, warehouse, sales_status
    #         FROM over_sea.dwm_oversea_price_dtl
    #         WHERE
    #             date_id = '{date}' and platform = 'AMAZON'
    #             and warehouse in ('美国仓','澳洲仓', '德国仓', '英国仓','加拿大仓')
    #             and !(warehouse = '美国仓' and country in ('CA','MX'))
    #     """
    #     conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #     df_fee_7 = conn.read_sql(sql)
    #     c1 = (df_fee_7['warehouse'].isin(['英国仓'])) & (df_fee_7['country'] != 'UK')
    #     c2 = (~df_fee_7['warehouse'].isin(['英国仓'])) & (df_fee_7['country'] == 'UK')
    #     df_fee_7 = df_fee_7[~(c1 | c2)]
    #     #
    #     df_fee_7 = df_fee_7.rename(columns={'country':'column_to_split'})
    #     df_sku_1 = pd.merge(df_sku_1, df_fee_7, how='left', on=['sku','column_to_split'])
    #     df_sku_1['us_rate'] = df_rate.iloc[0,2]
    #     # df_sku_1['rate'] = 7.055489
    #     df_sku_1 = df_sku_1.sort_values(by='total_cost', ascending=False).drop_duplicates(subset=['唯一标识'])
    #     #
    #     # df_sku_1['订单利润率'] = 1 - 0.04 - (df_sku_1['new_price']+df_sku_1['total_cost'])/df_sku_1['rate']/df_sku_1['申报价格']
    #     # df_sku_1['订单利润率'] = np.where(df_sku_1['申报价格'] < 30, df_sku_1['订单利润率']+2.99/df_sku_1['申报价格'], df_sku_1['订单利润率'])
    #     # df_sku_1['加入站点时间'] = df_sku_1['加入站点时间'].fillna('2024-07-23')
    #     # #
    #     # c1 = (df_sku_1['站点'].isin(['美国','英国','欧洲'])) & (df_sku_1['加入站点时间'] < '2024-05-28') & (df_sku_1['订单利润率'] >= 0.2)
    #     # c2 = (df_sku_1['站点'].isin(['美国','英国','欧洲'])) & (df_sku_1['加入站点时间'] >= '2024-05-28') & (df_sku_1['订单利润率'] >= 0.25)
    #     # c3 = (df_sku_1['站点'].isin(['澳大利亚'])) & (df_sku_1['加入站点时间'] < '2024-05-28') & (df_sku_1['订单利润率'] >= 0.25)
    #     # c4 = (df_sku_1['站点'].isin(['澳大利亚'])) & (df_sku_1['加入站点时间'] >= '2024-05-28') & (df_sku_1['订单利润率'] >= 0.30)
    #     # df_sku_1['订单利润率是否达标'] = np.select([c1, c2, c3, c4], [1,1,1,1], 0)
    #     #
    #     df_result = pd.concat([df_sku_1, df_result])
    #     # df_sku_1.to_excel('df_result.xlsx', index=0)
    # 获取海外仓运费
    df_result = get_useful_fee(df_sku)
    df_result = df_result.sort_values(by='total_cost', ascending=False).drop_duplicates(
        subset=['product_sku_id', 'account_id', 'sku'], keep='first')
    df_result['运费类型'] = np.where(df_result['total_cost'].isna(), 0, '海外仓')
    print(f'海外仓调价运费共{len(df_result)}条')
    # 获取虚拟仓运费
    print('获取虚拟仓运费数据...')

    def get_virtual_fee(df_sku):
        df_yunfei = pd.DataFrame()
        conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器2')
        for key, group in df_sku.groupby(['site']):
            sql = f"""
                SELECT distinct sku,ship_name,total_cost as `虚拟仓总运费`,site
                from yibai_temp_hxx.old_freight_interface_temu
                where site='{key}' and warehouse_id = 481
                order by total_cost asc limit 1 by sku
                """
            df_yunfei0 = conn_ck.ck_select_to_df(sql)
            df_yunfei = df_yunfei.append(df_yunfei0)
        df_sku = pd.merge(df_sku, df_yunfei, how='left', on=['sku', 'site'])

        return df_sku

    #
    df_virtual_fee = get_virtual_fee(df_sku)
    df_virtual_fee = df_virtual_fee.sort_values(by='虚拟仓总运费', ascending=True).drop_duplicates(subset=['唯一标识'])
    print(f'虚拟仓运费数据共{len(df_virtual_fee)}条.')
    # 代销sku匹配
    print('获取代销仓SKU信息...')
    sql = """
        SELECT 
            sku, latest_quotation, 
            case
                when site = 1 then 'US'
                when site = 2 then 'UK'
                when site = 3 then 'FR'
                when site = 4 then 'IT'
                when site = 5 then 'ES'
                when site = 6 then 'DE'
                when site = 7 then 'AU'
                when site = 9 then 'CA'
            else '其他' end as column_to_split
        FROM yibai_prod_base_sync.yibai_prod_sku_consignment_inventory
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df_cons = conn_ck.ck_select_to_df(sql)
    df_cons = df_cons.drop_duplicates(subset='sku')
    #
    df = pd.merge(df_temu_base,
                  df_result[['唯一标识', 'column_to_split', 'total_cost', 'best_warehouse_name', 'sales_status']],
                  how='left', on=['唯一标识'])
    df = pd.merge(df, df_virtual_fee[['唯一标识', '成本', '虚拟仓总运费']], how='left', on=['唯一标识'])
    # df = pd.merge(df, df_cons, how='left', on=['sku'])
    df = pd.merge(df, df_cons, how='left', on=['sku', 'column_to_split'])
    df_rate0 = df_rate0.rename(columns={'country': 'column_to_split'})
    df['us_rate'] = df_rate.iloc[0, 2]
    df = pd.merge(df, df_rate0[['column_to_split', 'rate']], how='left', on='column_to_split')

    # df['收货国家2'] = df['站点'].replace({'新西兰':'澳大利亚'})
    # df['limit_price'] = df['收货国家2'].replace({'美国':27, '英国':28, '欧洲':24,'澳大利亚':35, '加拿大':31})
    # df['运费补贴'] = df['收货国家2'].replace({'美国':2.99, '英国':2, '欧洲':2.99,'澳大利亚':2.99, '加拿大':3.99})
    # df['申报价格'] = df['申报价格'].astype(float)
    # df['运费补贴'] = np.where((df['申报价格']*df['us_rate']/df['rate']) < df['limit_price'], df['运费补贴'] * df['rate'], 0)

    # df['column_to_split'] = df['column_to_split'].replace({'NZ': 'AU'})
    # df['limit_price'] = df['column_to_split'].replace({'US':27, 'UK':28, 'DE':24,'FR':24,'IT':24,'ES':24,'AU':35,
    #                                                          'CA':31,'CZ':592,'PL':50,'MX':200})
    # df['运费补贴'] = df['column_to_split'].replace({'US':2.99, 'UK':2, 'DE':2.99,'FR':2.99,'IT':2.99,'MX':0,
    #                                                              'ES':2.99,'CZ':75,'PL':12.99, 'AU':2.99, 'CA':3.99})
    # col = ['limit_price','申报价格','运费补贴']
    # df[col] = df[col].fillna(0).astype(float)
    # df['运费补贴'] = np.where((df['申报价格']*df['us_rate']/df['rate']) < df['limit_price'],
    #                                  df['运费补贴'] * df['rate'], 0)
    print('计算完成，开始存表...')
    save_df(df, 'TEMU链接订单利润率核算_不含接口', file_type='xlsx')
    #
    print('获取接口运费信息...')
    df_bundle_fee = get_interf_fee(df)
    df_bundle_fee = df_bundle_fee.rename(columns={'shipCountry': 'column_to_split'})
    df = pd.merge(df, df_bundle_fee, how='left', on=['sku', 'column_to_split'])

    save_df(df, 'TEMU链接订单利润率核算', file_type='xlsx')
    return df


def temu_listing():
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
    # left join yibai_sale_center_system.yibai_system_account as e on a.account_id=e.id
    # where e.platform_code='TEMU' and e.is_del=0 and b.is_del=0 and b.lazada_account_operation_mode = 2
    # """
    # conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    # listing_t = conn_ck.ck_select_to_df(sql)
    # listing_t.columns = [i.split('.')[-1] for i in listing_t.columns]
    # listing_t['运营模式'] = listing_t['lazada_account_operation_mode'].map({1: '全托管', 2: '半托管'})
    # del listing_t['lazada_account_operation_mode']
    # # 获取捆绑表的sku信息
    # sql = """
    # select erp_id as account_id,platform_sku as product_sku_id,company_sku as sku
    # from yibai_sale_center_listing_sync.yibai_temu_bind_sku
    # order by update_time desc
    # """
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # yibai_temu_bind_sku = conn_ck.ck_select_to_df(sql)
    # yibai_temu_bind_sku.drop_duplicates(subset=['account_id', 'product_sku_id'], inplace=True)
    # listing_t[['product_sku_id','sku']] = listing_t[['product_sku_id','sku']].astype('str')
    # listing_t = listing_t.merge(yibai_temu_bind_sku, on=['account_id', 'product_sku_id'], how='left',
    #                                   suffixes=['', '1'])
    # listing_t.loc[listing_t['sku']=='','sku']=np.nan
    # listing_t['sku'].fillna(listing_t['sku1'],inplace=True)
    #
    # listing_t.rename(columns={'added_to_site_time': '加入站点时间', 'supplier_price': '申报价格'}, inplace=True)
    # listing_t.loc[listing_t['online_status'] == '', 'online_status'] = '待申报'
    # listing_t.loc[listing_t['online_status'].isnull(), 'online_status'] = '未知'
    # listing_t['是否核价通过链接'] = np.where(
    #     listing_t['online_status'].isin(['未发布到站点', '已发布到站点', '已下架', '已终止']), 1, 0)
    # listing_t['product_sku_id'] = listing_t['product_sku_id'].astype(str)
    # listing_t['加入站点时间'] = pd.to_datetime(listing_t['加入站点时间'].dt.date, format='%Y-%m-%d')
    # country = {'AU': '澳大利亚', 'AU,NZ': '澳大利亚', 'DE': '欧洲', 'DE,ES,SP': '欧洲', 'DE,FR,IT,ES,SP': '欧洲',
    #            'DE,IT': '欧洲', 'ES,SP': '欧洲', 'FR': '欧洲', 'FR,ES,SP': '欧洲', 'FR,IT': '欧洲',
    #            'FR,IT,ES,SP': '欧洲',
    #            'IT': '欧洲', 'IT,ES,SP': '欧洲', 'DE,FR,IT': '欧洲', 'UK,GB': '英国', 'US': '美国', 'NZ': '新西兰',
    #            'CA': '加拿大'}
    # listing_t['站点'] = listing_t['site_code'].map(country)
    # #
    # listing_t['warehouse'] = listing_t['站点'].replace({'欧洲': '德国仓', '澳大利亚': '澳洲仓', '美国': '美国仓',
    #                                                     '英国': '英国仓', '新西兰': '澳洲仓', '加拿大': '加拿大仓'})
    # listing_t = listing_t.drop_duplicates()
    # # temu账号获取
    # df_temu_account = get_temu_account()
    #
    # listing_t = pd.merge(listing_t,
    #                          df_temu_account[['account_id', '主体账号', '大部', '小组', '账号运营模式']],
    #                          how='left', on=['account_id'])
    #
    # # 订单数据
    # sql_order = """
    # select
    #     b.platform_order_id,b.order_id as order_id,b.account_id as account_id,c.seller_sku as seller_sku,
    #     c.quantity,toDate(b.create_time)as create_time,c.item_id,b.order_status,c.total_price,b.currency
    # from (
    #     select *
    #     from yibai_oms_sync.yibai_oms_order
    #     where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4
    # ) b
    # left join (
    #     select *
    #     from yibai_oms_sync.yibai_oms_order_detail
    #     where order_id in (
    #         select order_id from yibai_oms_sync.yibai_oms_order
    #         where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4)
    # ) c on b.order_id=c.order_id
    # """
    # order = conn_ck.ck_select_to_df(sql_order)
    # order.columns = [i.split('.')[-1] for i in order.columns]
    # order = order[order['order_status'] != 80]
    # order['create_time'] = pd.to_datetime(order['create_time'], format='%Y-%m-%d')
    #
    # min_order_time = order.groupby(['account_id', 'seller_sku'])['create_time'].agg(['min', 'max']).reset_index()
    # min_order_time.columns = ['account_id', 'seller_sku', '最早出单时间', '最晚出单时间']
    # min_order_time = min_order_time[min_order_time['seller_sku'] != '']
    # min_order_time['seller_sku'] = min_order_time['seller_sku'].astype(str)
    #
    # listing_t = listing_t.merge(min_order_time.rename(columns={'seller_sku': 'product_sku_id'}),
    #                               on=['account_id', 'product_sku_id'], how='left')
    # # 1、有效链接
    # df_temu_listing = listing_t[
    #     (listing_t['online_status'] == '已发布到站点') & (~listing_t['sku'].isna()) & (listing_t['sku'] != '')]
    #
    # # 2、捆绑链接覆盖率
    # df_bind_listing = listing_t[(~listing_t['sku'].isna()) & (listing_t['sku'] != '') & (listing_t['是否核价通过链接'] == 1)]
    df_bind_listing = get_temu_listing()
    df_bind_listing = df_bind_listing[(~df_bind_listing['sku'].isna()) & ~(df_bind_listing['sku'] == '')]
    col = ['stock_number', 'lazada_account_operation_mode', 'create_time', 'account_status', 'account_operation_mode']
    df_bind_listing.drop(col, axis=1, inplace=True)
    # 订单数据
    sql_order = """
    select
        b.platform_order_id,b.order_id as order_id,b.account_id as account_id,c.seller_sku as seller_sku,
        c.quantity,toDate(b.create_time)as create_time,c.item_id,b.order_status,c.total_price,b.currency
    from (
        select *
        from yibai_oms_sync.yibai_oms_order
        where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4
    ) b
    left join (
        select *
        from yibai_oms_sync.yibai_oms_order_detail
        where order_id in (
            select order_id from yibai_oms_sync.yibai_oms_order
            where platform_code ='TEMU' and order_id not like '%RE%'and order_id not like '%CJ%' and total_price !=0 and order_type !=4)
    ) c on b.order_id=c.order_id
    """
    conn_ck = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    order = conn_ck.ck_select_to_df(sql_order)
    order.columns = [i.split('.')[-1] for i in order.columns]
    order = order[order['order_status'] != 80]
    order['create_time'] = pd.to_datetime(order['create_time'], format='%Y-%m-%d')

    min_order_time = order.groupby(['account_id', 'seller_sku'])['create_time'].agg(['min', 'max']).reset_index()
    min_order_time.columns = ['account_id', 'seller_sku', '最早出单时间', '最晚出单时间']
    min_order_time = min_order_time[min_order_time['seller_sku'] != '']
    min_order_time['seller_sku'] = min_order_time['seller_sku'].astype(str)

    df_bind_listing = df_bind_listing.merge(min_order_time.rename(columns={'seller_sku': 'product_sku_id'}),
                                            on=['account_id', 'product_sku_id'], how='left')
    df_bind_listing = df_bind_listing.reset_index(drop=True).reset_index()
    df_bind_listing = df_bind_listing.rename(columns={'index': '唯一标识'})
    print(df_bind_listing.info())
    return df_bind_listing



##
# concat_temp()
def temu_dx_stock():
    """ 1005\1006\TK独享库存 """
    sql = """
        SELECT sku, warehouse, warehouse_name, stock, available_stock, on_way_stock
        FROM yb_datacenter.v_oversea_stock
        WHERE (warehouse_name like '%1005%' or warehouse_name like '%1006%' or warehouse_name like '%TK%')
        -- and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    c1 = df['warehouse_name'].str.contains('1005')
    c2 = df['warehouse_name'].str.contains('1006')
    c3 = df['warehouse_name'].str.contains('TK')
    df['独享归属'] = np.select([c1, c2, c3], ['1005', '1006', 'TK'])
    df_group = pd.pivot_table(df, values='available_stock', index=['sku', 'warehouse'],
                              columns=['独享归属'], aggfunc='sum', fill_value=0, margins=True)
    df_group = df_group.reset_index()
    df_group.drop('All', axis=1, inplace=True)
    print(df_group.info())
    # df.to_excel('F://Desktop//temu_dx_stock3.xlsx', index=0)

    return df_group


def get_dx_sku_sales():
    sql = """

    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name not like '%%TT%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_all = conn.read_sql(sql)

    sql = """

    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_base'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name not like '%%1005%%'
        and b.warehouse_name not like '%%1006%%'
        and b.warehouse_name not like '%%TK%%'
        and b.warehouse_name not like '%%TT%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_base = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_1005'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%1005%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_1005 = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_1006'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%1006%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_1006 = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_tk'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%TK%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_tk = conn.read_sql(sql)

    df_sales = pd.merge(df_sales_all, df_sales_base, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_1005, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_1006, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_tk, how='left', on=['sku', 'warehouse'])

    print(df_sales.info())

    return df_sales

def get_dx_sku_sales_tt():
    # sql = """
    #
    # SELECT
    #     SKU as sku, warehouse,
    #     SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales'
    # FROM (
    #     SELECT
    #         SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse
    #     FROM `yibai_sku_sales_statistics` a
    #     INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
    #     WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
    #     )A
    # GROUP BY SKU, warehouse
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_sales_all = conn.read_sql(sql)

    df_sales_all = get_sku_sales_new()
    df_sales_all = df_sales_all[['sku','warehouse','day_sales']]

    sql = """

    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_base'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name not like '%%1005%%'
        and b.warehouse_name not like '%%1006%%'
        and b.warehouse_name not like '%%TK%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_base = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_1005'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%1005%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_1005 = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_1006'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%1006%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_1006 = conn.read_sql(sql)

    sql = """
    SELECT  
        SKU as sku, warehouse,
        SUM(7days_sales)/7*0.7+SUM(15days_sales)/15*0.2+SUM(30days_sales)/30*0.1 AS 'day_sales_tk'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales, warehouse 
        FROM `yibai_sku_sales_statistics` a 
        INNER JOIN yibai_warehouse_oversea_temp b on a.warehouse_id=b.warehouse_id
        WHERE platform_code not in ('DIS','WYLFX') and b.warehouse is not Null
        and b.warehouse_name like '%%TK%%'
        )A 
    GROUP BY SKU, warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales_tk = conn.read_sql(sql)

    df_sales = pd.merge(df_sales_all, df_sales_base, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_1005, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_1006, how='left', on=['sku', 'warehouse'])
    df_sales = pd.merge(df_sales, df_sales_tk, how='left', on=['sku', 'warehouse'])

    print(df_sales.info())

    return df_sales


def get_958_stock():
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
                where warehouse_id in (958)
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
    df_sku_stock = get_line_new(df_sku_stock)
    # 20250321 隔离精铺非转泛品的sku
    c1 = (df_sku_stock['best_warehouse'].str.contains('精铺')) & (~df_sku_stock['type'].str.contains('转泛品'))
    df_sku_stock = df_sku_stock[~c1]
    # df_sku_stock.to_excel('F://Desktop//df_sku_stock.xlsx', index=0)
    return df_sku_stock

def get_958_stock_age():
    """
    20240807
    获取YM墨西哥仓的库龄数据。
    由于saas系统库龄是YM墨西哥实仓数据，无法区分出虚拟子仓的明细。而易佰销售可使用的仅限子仓YM墨西哥2仓。
    为分离出虚拟子仓YM墨西哥2仓的库龄数据，采用【实体仓库龄库存】 * 【子仓库存】/【实体仓库存】的方式等比例计算子仓的库龄数据。
    """
    # 获取saas系统库龄数据
    sql = """
    SELECT 
        sku, cargo_owner_id, sum(saas_stock) saas_stock,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus
    FROM (
        SELECT
            sku, cargo_owner_id, saas_stock, stock_age,
            case
                when stock_age >= 30 and stock_age < 60 then 30
                when stock_age >= 60 and stock_age < 90 then 60
                when stock_age >= 90 and stock_age < 120 then 90
                when stock_age >= 120 and stock_age < 150 then 120
                when stock_age >= 150 and stock_age < 180 then 150
                when stock_age >= 180 and stock_age < 270 then 180
                when stock_age >= 270 and stock_age < 360 then 270
                when stock_age >= 360 then 360
            else 0 end as overage_level, 
            case when stock_age >= 30 then saas_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then saas_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then saas_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then saas_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then saas_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then saas_stock else 0 end as age_180_plus,
            case when stock_age >= 270 then saas_stock else 0 end as age_270_plus,
            case when stock_age >= 360 then saas_stock else 0 end as age_360_plus
        FROM (
            SELECT sku, cargo_owner_id, sum(saas_stock) as saas_stock, stock_age
            FROM (
                SELECT 
                    w, b.sku sku, b.client_sku client_sku, 
                    a.sku as oversea_sku, cargo_owner_id, 
                    instock_stock-out_stock as saas_stock, storage_age_date, 
                    toInt32(today() - toDate(storage_age_date)) as stock_age,
                    today()
                FROM yb_datacenter.yibai_stock_age_detail a
                LEFT JOIN yb_datacenter.yb_oversea_sku_mapping b
                ON a.sku = b.oversea_sku
                WHERE w = 'YM-MX-2'
            ) a
            GROUP BY sku, cargo_owner_id, stock_age
            HAVING saas_stock > 0 and cargo_owner_id = 8
        )
    ) a
    GROUP BY sku, cargo_owner_id
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_mx_stock = conn_ck.ck_select_to_df(sql)

    # 获取云仓库存数据
    date_today = time.strftime('%Y%m%d')
    sql = f"""
    SELECT 
        sku, warehouse, arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age, sum(available_stock) as available_stock,
        sum(on_way_stock) as on_way_stock, sum(wait_outbound) as wait_outbound, sum(frozen_stock) frozen_stock, max(new_price) new_price
    FROM (
        SELECT
            ps.sku sku, toString(toDate(toString(date_id))) date_id, yw.ebay_category_id AS category_id, yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, allot_on_way_count AS on_way_stock, wait_outbound, frozen_stock, yps.new_price as new_price, cargo_owner_id,
            concat(toString(warehouse_id), ':', toString(available_stock)) as stock_info
        FROM yb_datacenter.yb_stock AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        LEFT JOIN (
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
        ) yps ON ps.sku = yps.sku
        WHERE 
            ps.date_id = '{date_today}'          -- 根据需要取时间
            and ps.cargo_owner_id = 8         -- 筛选货主ID为8的
            -- and ps.available_stock > 0
            and yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and yw.id = 958  
            and ywc.name = '墨西哥仓'
        ORDER BY date_id DESC
    ) a
    GROUP BY sku, warehouse
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock_temp = conn_ck.ck_select_to_df(sql)

    df_mx = pd.merge(df_stock_temp, df_mx_stock, how='left', on=['sku'])
    # YM库龄库存按比例分解
    df_mx['占比'] = df_mx['available_stock'] / df_mx['saas_stock']
    df_mx['占比'] = np.where(df_mx['占比'] > 1, 1, df_mx['占比'])
    #
    col_list = ['age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus', 'age_150_plus', 'age_180_plus', 'age_270_plus',
                'age_360_plus']
    for i in col_list:
        df_mx[i] = df_mx[i] * df_mx['占比']
    #
    df_mx['warehouse_stock'] = np.where(df_mx['saas_stock'].isna(), df_mx['saas_stock'], df_mx['available_stock'])
    df_mx['best_warehouse_id'] = 958
    df_mx['best_warehouse_name'] = 'YM墨西哥toB代发仓'
    df_mx['charge_currency'] = 'MXN'
    df_mx['charge_total_price'] = 0

    #
    df_mx = df_mx[
        ['sku', 'warehouse', 'warehouse_stock', 'charge_total_price', 'age_30_plus','age_60_plus', 'age_90_plus', 'age_120_plus',
         'age_150_plus',
         'age_180_plus', 'age_270_plus', 'age_360_plus', 'warehouse_stock_age', 'charge_currency', 'overage_level',
         'best_warehouse_id', 'best_warehouse_name']]
    #
    df_mx = df_mx[df_mx['warehouse_stock'] > 0]

    return df_mx
# temu_dx_stock()
## YM墨西哥toB代发仓定价
def get_mx_tob_stock():
    """ YM墨西哥toB打发仓库存信息获取"""
    df_stock = get_958_stock()
    print('获取库龄信息...')
    # df_stock_age_id, df_stock_age_warehouse = get_stock_age()
    df_stock_age_warehouse = get_958_stock_age()
    dwm_sku = pd.merge(df_stock, df_stock_age_warehouse, how='left', on=['sku', 'warehouse'])

    dwm_sku = dwm_sku[dwm_sku['available_stock']>0]
    dwm_sku['best_warehouse_id'] = np.where(dwm_sku['best_warehouse_id'].isna(), dwm_sku['warehouse_id'],
                                            dwm_sku['best_warehouse_id'])
    dwm_sku['best_warehouse_name'] = np.where(dwm_sku['best_warehouse_name'].isna(), dwm_sku['best_warehouse'],
                                              dwm_sku['best_warehouse_name'])

    col = ['sku','title','new_price','gross','product_package_size','best_warehouse_id', 'best_warehouse_name',
            'warehouse', 'available_stock', 'on_way_stock', 'warehouse_stock_age', 'overage_level',
            'age_60_plus']
    dwm_sku = dwm_sku[col]
    # dwm_sku = dwm_sku.rename(columns={'age_60_plus':'age_60_plus_all'})

    # dwm_sku.to_excel('F://Desktop//dwm_sku_mx.xlsx', index=0)

    return dwm_sku

def replace_real_warehouse_id(df):
    """ 独享仓ID替换 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 独享仓需替换为非独享仓的运费
    sql = """
        SELECT warehouse_id best_warehouse_id, real_warehouse_id
        FROM over_sea.yibai_warehouse_oversea_temp
        WHERE warehouse_name like '%%独享%%'
    """
    df_warehouse = conn.read_sql(sql)

    # df['warehouse_id'] = df['best_warehouse_id']
    df = pd.merge(df, df_warehouse, how='left', on=['best_warehouse_id'])
    df['best_warehouse_id'] = np.where(df['real_warehouse_id'].isna(),
                                  df['best_warehouse_id'], df['real_warehouse_id'])
    df['best_warehouse_id'] = df['best_warehouse_id'].replace(958, 956)
    df.drop('real_warehouse_id', axis=1, inplace=True)

    return df
##
def temu_clear_sku():
    """ 清仓sku的库存信息 """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-04-28'
    # temusku的清仓状态
    sql = f"""
        SELECT 
            sku, title, new_price, gross, product_package_size, best_warehouse_id, best_warehouse_name,
            warehouse, available_stock, on_way_stock, warehouse_stock_age, overage_level,
            age_60_plus age_60_plus_all, age_180_plus age_180_plus_all, after_profit
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-08-01')
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓') 
        -- and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    col = ['age_60_plus_all', 'available_stock']
    df[col] = df[col].fillna(0).astype(int)
    # df['90_ratio'] = np.where(df['age_90_plus']/df['available_stock'] > 1, 1,
    #                               df['age_90_plus']/df['available_stock'])
    # 拼接独享仓库存
    # df_group = temu_dx_stock()
    # df = pd.merge(df, df_group, how='left', on=['sku', 'warehouse'])
    # col = ['1005', '1006', 'TK']
    # df[col] = df[col].fillna(0).astype(int)
    df['available_stock_all'] = df['available_stock']
    df = df[df['available_stock_all'] > 0]
    # 拼接独享仓日销
    df_sales = get_dx_sku_sales()
    # df_sales = get_dx_sku_sales_tt()
    df = pd.merge(df, df_sales[['sku','warehouse','day_sales']], how='left', on=['sku', 'warehouse'])
    col = ['day_sales']
    df[col] = df[col].fillna(0).astype(float)

    # 各独享仓超库龄可售天数
    df['oesd_temu'] = (df['age_60_plus_all'] / df['day_sales']).replace(np.inf, 9999).replace(np.nan, 0)
    df['oesd_180'] = ((df['age_180_plus_all']) / (df['day_sales'])).replace(np.inf, 9999).replace(np.nan, 0)

    # 各独享仓总库存的可售天数
    df['esd_temu'] = (df['available_stock_all'] / df['day_sales']).replace(np.inf, 9999).replace(np.nan, 0)
    # c1 = (df['overage_level'] > 30) & (df['estimated_sales_days'] > 30)
    # df['is_temu_clear'] = np.where(c1, 1, 0)

    # 获取责任人
    df['new_sku'] = df['sku']
    df = get_line(df)
    df['主体账号'] = ' '
    dic = {'德国仓': '欧洲', '澳洲仓': '澳大利亚', '美国仓': '美国', '英国仓': '英国',
           '加拿大仓': '加拿大', '墨西哥仓': '墨西哥', '法国仓': '欧洲'}
    df['站点'] = df['warehouse'].replace(dic)
    print(df['站点'].unique())
    df = get_main_resp(df)
    df.drop(['主体账号', 'is_same', '站点'], axis=1, inplace=True)
    # 20251208 补充责任人
    df_add = pd.read_excel('F://Ding_workspace//责任SKU以及账号分配3.12.xlsx', dtype={'sku':str})
    df = pd.merge(df, df_add[['sku','warehouse','责任团队']], how='left', on=['sku','warehouse'])
    # 获取开发来源.剔除精铺转泛品sku
    df_sku = tt_get_sku()
    df = pd.merge(df, df_sku[['sku', 'develop_source_name']], how='left', on='sku')
    print(df.info())
    df['develop_source_name'] = df['develop_source_name'].fillna('')
    c1 = ~(df['develop_source_name'].str.contains('转泛品'))
    c2 = df['develop_source_name'].str.contains('海兔')
    df = df[c1 | c2]
    # 20250321 隔离精铺非转泛品的sku
    c0 = df['develop_source_name'].str.contains('海兔|易通兔') | df['develop_source_name'].str.contains('转VC|转泛品')
    c1 = (df['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) & (~c0)
    df = df[~c1]
    # 判断是否需要清仓
    # 20250114 取销售状态表
    sql = """
        SELECT sku ,warehouse ,sale_status is_temu_clear
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_sale_status = conn.read_sql(sql)
    df = pd.merge(df, df_sale_status, how='left', on=['sku', 'warehouse'])
    # c1 = (~df['责任人'].isin(['控股公司项目：彭柳森','控股公司项目：子龙'])) & (
    #         df['age_60_plus_temu'] > 0) & (df['oesd_temu'] > 30)
    # c2 = (df['责任人'].isin(['控股公司项目：彭柳森'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1005']) > 0) & (df['oesd_1005'] > 30)
    # c3 = (df['责任人'].isin(['控股公司项目：子龙'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1006']) > 0) & (df['oesd_1006'] > 30)
    # df['is_temu_clear'] = np.select([c1, c2, c3], [1,1,1], 0)

    # 20241224 白名单sku修改清仓状态
    # 第一批截止时间2025-02-01
    sql = """
        SELECT sku, warehouse, is_clear
        FROM over_sea.sales_status_temp
    """
    df_temp_sku = conn.read_sql(sql)
    df = pd.merge(df, df_temp_sku, how='left', on=['sku', 'warehouse'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 0, 0, df['is_temu_clear'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 1, 1, df['is_temu_clear'])
    df.drop('is_clear', axis=1, inplace=True)
    stock_temp = df.groupby(['warehouse'])['available_stock'].sum().reset_index()
    stock_temp = stock_temp.sort_values(by='available_stock', ascending=False)
    print(stock_temp)
    print('清仓状态判断完成...')

    # 获取有效销量
    # df_point = mrp_oversea_sales()
    # df = pd.merge(df, df_point, how='left', on=['sku', 'warehouse'])
    df['effective_sales'] = 0
    df['effective_sales'] = df['effective_sales'].fillna(0).astype(float)
    c1 = (df['warehouse'] == '美国仓') & (df['effective_sales'] >= 10)
    c2 = (df['warehouse'] != '美国仓') & (df['effective_sales'] >= 4)
    df['非责任sku不可新增链接'] = np.where(c1 | c2, 1, 0)

    # 20250327 李总补充一批独享sku
    sku_list = ['1616240137811', '2013240106411', '2013240106411', '2013240106411', '2013240106411', '2013240106511',
                '2013240106511',
                '2013240106511', '2013240106511', '3113230312411', '3114230104311', '3116240011611', '3116240011611',
                '3116240011711',
                '3116240011911', '3116240013011', '3116240013611', '3116240013611', '3116240013611', '3116240013611',
                '3116240013711',
                '3116240013711', '3116240013712', '3116240013712', '3116240013911', '3116240013911', '3116240015911',
                '3116240015911',
                '3116240015911', '3116240015911', '3116240016211', '3116240016211', '3116240016211', '3116240016211',
                '3116240016311',
                '3116240016311', '3116240016311', '3116240016311', '3116240016711', '3116240016711', '3116240016711',
                '3116240018611',
                '3116240026511', '3116240026611', '3116240026711', '3117240210711', '3117240210911', '3117240211911',
                '3117240221111',
                '3117240222511', '3117240222511', '3117240222511', '3117240276111', '3117240276211', '3117240276311',
                '3118240152711',
                '3118240153711', '3118240169111', '3118240169111', '3118240169111']
    df['非责任sku不可新增链接'] = np.where(df['sku'].isin(sku_list), 'TEMU3组独享', df['非责任sku不可新增链接'])

    return df

def temu_clear_sku_new():
    """ 清仓sku的库存信息 """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-04-28'
    # temusku的清仓状态
    sql = f"""
        SELECT 
            sku, title, type, new_price, gross, product_package_size, best_warehouse_id, best_warehouse_name,
            warehouse, available_stock, on_way_stock, warehouse_stock_age, overage_level, day_sales,
            age_60_plus, age_180_plus, after_profit
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-08-01')
        -- date_id = '2026-02-13'
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓','日本仓') 
        and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    # 剔除通拓、转泛品、精铺仓数据
    # 剔除通拓、精品仓数据
    # c1 = (df['best_warehouse_name'].str.contains('精铺|精品|凯美晨')) | (df['type'].str.contains('通拓|转泛品|海兔|易通兔|转VC'))
    c1 = (df['best_warehouse_name'].str.contains('精品|凯美晨')) | (df['type'].str.contains('海兔|易通兔|VC'))
    df = df[~c1]
    col = ['age_60_plus', 'available_stock']
    df[col] = df[col].fillna(0).astype(int)
    col = ['day_sales']
    df[col] = df[col].fillna(0).astype(float)

    # 获取责任人
    df['new_sku'] = df['sku']
    df = get_line(df)
    df['主体账号'] = ' '
    dic = {'德国仓': '欧洲', '澳洲仓': '澳大利亚', '美国仓': '美国', '英国仓': '英国', '日本仓':'日本',
           '加拿大仓': '加拿大', '墨西哥仓': '墨西哥', '法国仓': '欧洲'}
    df['站点'] = df['warehouse'].replace(dic)
    print(df['站点'].unique())
    df = get_main_resp(df)
    df.drop(['主体账号', 'is_same', '站点'], axis=1, inplace=True)
    # 20251208 补充责任人
    df_add = pd.read_excel('F://Ding_workspace//责任SKU以及账号分配3.12.xlsx', dtype={'sku':str})
    df = pd.merge(df, df_add[['sku','warehouse','责任团队']], how='left', on=['sku','warehouse'])

    # 20250114 取销售状态表
    sql = """
        SELECT sku ,warehouse ,sale_status is_temu_clear
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_sale_status = conn.read_sql(sql)
    df = pd.merge(df, df_sale_status, how='left', on=['sku', 'warehouse'])

    # 独享仓id替换
    df = replace_real_warehouse_id(df)

    # 是否美国站日销>0.3
    c1 = (df['warehouse'] == '美国仓') & (df['day_sales'] > 0.3)
    df['是否美国站日销>0.3'] = np.where(c1, 1, 0)

    return df

def haitu_clear_sku():
    """ 清仓sku的库存信息 """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-04-28'
    # temusku的清仓状态
    sql = """
        SELECT sku, title, new_price, gross, product_package_size, warehouse_id best_warehouse_id, warehouse_name best_warehouse_name,
        warehouse, available_stock, on_way_stock, '' warehouse_stock_age, 0 overage_level,
        0 age_60_plus_all, 0 age_180_plus_all
        FROM over_sea.dwd_sku_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_info where date_id > '2025-08-01')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)


    sql = f"""
        SELECT sku, warehouse, after_profit
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-08-01')
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓')
        and available_stock > 0
    """
    df_after_profit = conn.read_sql(sql)
    df = pd.merge(df, df_after_profit, how='left', on=['sku','warehouse'])
    print(df.info())
    col = ['age_60_plus_all', 'available_stock']
    df[col] = df[col].fillna(0).astype(int)
    # df['90_ratio'] = np.where(df['age_90_plus']/df['available_stock'] > 1, 1,
    #                               df['age_90_plus']/df['available_stock'])
    # 拼接独享仓库存
    df_group = temu_dx_stock()
    df = pd.merge(df, df_group, how='left', on=['sku', 'warehouse'])
    col = ['1005', '1006', 'TK']
    df[col] = df[col].fillna(0).astype(int)
    df['available_stock_all'] = df['available_stock'] + df['1005'] + df['1006'] + df['TK']
    df['age_60_plus_all'] = np.where(df['age_60_plus_all'] > df['available_stock_all'],
                                     df['available_stock_all'], df['age_60_plus_all'])
    df['age_60_plus_temu'] = df['age_60_plus_all'] * df['available_stock'] / df['available_stock_all']
    df['age_60_plus_1005'] = df['age_60_plus_all'] * df['1005'] / df['available_stock_all']
    df['age_60_plus_1006'] = df['age_60_plus_all'] * df['1006'] / df['available_stock_all']
    df['age_60_plus_tk'] = df['age_60_plus_all'] * df['TK'] / df['available_stock_all']
    df['age_180_plus_all'] = np.where(df['age_180_plus_all'] > df['available_stock_all'],
                                     df['available_stock_all'], df['age_180_plus_all'])
    df = df[df['available_stock_all'] > 0]
    # 拼接独享仓日销
    df_sales = get_dx_sku_sales()
    # df_sales = get_dx_sku_sales_tt()
    df = pd.merge(df, df_sales, how='left', on=['sku', 'warehouse'])
    col = ['day_sales', 'day_sales_base', 'day_sales_1005', 'day_sales_1006', 'day_sales_tk']
    df[col] = df[col].fillna(0).astype(float)
    # 各独享仓超库龄可售天数
    df['oesd_temu'] = ((df['age_60_plus_temu'] + df['age_60_plus_1005']) / (
            df['day_sales_base'] + df['day_sales_1005'])).replace(np.inf, 9999).replace(np.nan, 0)
    df['oesd_1006'] = ((df['age_60_plus_1006'] + df['age_60_plus_temu']) / (
            df['day_sales_1006'] + df['day_sales_base'])).replace(np.inf, 9999).replace(np.nan, 0)

    df['oesd_180'] = ((df['age_180_plus_all']) / (df['day_sales'])).replace(np.inf, 9999).replace(np.nan, 0)

    # 各独享仓总库存的可售天数
    df['esd_temu'] = ((df['available_stock'] + df['1005']) / (
            df['day_sales_base'] + df['day_sales_1005'])).replace(np.inf, 9999).replace(np.nan, 0)
    df['esd_1006'] = ((df['1006'] + df['available_stock']) / (
            df['day_sales_1006'] + df['day_sales_base'])).replace(np.inf, 9999).replace(np.nan, 0)
    # c1 = (df['overage_level'] > 30) & (df['estimated_sales_days'] > 30)
    # df['is_temu_clear'] = np.where(c1, 1, 0)

    # 获取责任人
    df['new_sku'] = df['sku']
    df = get_line(df)
    df['主体账号'] = ' '
    dic = {'德国仓': '欧洲', '澳洲仓': '澳大利亚', '美国仓': '美国', '英国仓': '英国',
           '加拿大仓': '加拿大', '墨西哥仓': '墨西哥', '法国仓': '欧洲'}
    df['站点'] = df['warehouse'].replace(dic)
    print(df['站点'].unique())
    df = get_main_resp(df)
    df.drop(['主体账号', 'is_same', '站点'], axis=1, inplace=True)
    # 获取开发来源.剔除精铺转泛品sku
    df_sku = tt_get_sku()
    df = pd.merge(df, df_sku[['sku', 'develop_source_name']], how='left', on='sku')
    # c1 = (~df['develop_source_name'].str.contains('转泛品'))
    # c2 = df['develop_source_name'].str.contains('海兔')
    # df = df[c1 | c2]

    # 指定sku
    # 筛选转泛品、海兔、耳机专项sku
    earphones_list = ['2610220196211', '2610220275311', '2613240141011', '2613240141111', '2613240151411',
                      '2613240151412', '2613240136811', '2613240136812', '2613240142211', '2610230453411',
                      '2610230457211', '2613240136711', '2613240136712', '2613240144011', '2613240144012',
                      '2613240144111', '2613240144311', '2613240136411']
    df = df[df['sku'].isin(earphones_list) | df['develop_source_name'].str.contains('海兔') |
            df['develop_source_name'].str.contains('易通兔')]

    # 判断是否需要清仓
    # 20250114 取销售状态表
    sql = """
        SELECT sku ,warehouse ,sale_status is_temu_clear
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_sale_status = conn.read_sql(sql)
    df = pd.merge(df, df_sale_status, how='left', on=['sku', 'warehouse'])
    # c1 = (~df['责任人'].isin(['控股公司项目：彭柳森','控股公司项目：子龙'])) & (
    #         df['age_60_plus_temu'] > 0) & (df['oesd_temu'] > 30)
    # c2 = (df['责任人'].isin(['控股公司项目：彭柳森'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1005']) > 0) & (df['oesd_1005'] > 30)
    # c3 = (df['责任人'].isin(['控股公司项目：子龙'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1006']) > 0) & (df['oesd_1006'] > 30)
    # df['is_temu_clear'] = np.select([c1, c2, c3], [1,1,1], 0)

    # 20241224 白名单sku修改清仓状态
    # 第一批截止时间2025-02-01
    sql = """
        SELECT sku, warehouse, is_clear
        FROM over_sea.sales_status_temp
    """
    df_temp_sku = conn.read_sql(sql)
    df = pd.merge(df, df_temp_sku, how='left', on=['sku', 'warehouse'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 0, 0, df['is_temu_clear'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 1, 1, df['is_temu_clear'])
    df.drop('is_clear', axis=1, inplace=True)
    stock_temp = df.groupby(['warehouse'])['available_stock'].sum().reset_index()
    stock_temp = stock_temp.sort_values(by='available_stock', ascending=False)
    print(stock_temp)
    print('清仓状态判断完成...')

    # 获取有效销量
    # df_point = mrp_oversea_sales()
    # df = pd.merge(df, df_point, how='left', on=['sku', 'warehouse'])
    df['effective_sales'] = 0
    df['effective_sales'] = df['effective_sales'].fillna(0).astype(float)
    c1 = (df['warehouse'] == '美国仓') & (df['effective_sales'] >= 10)
    c2 = (df['warehouse'] != '美国仓') & (df['effective_sales'] >= 4)
    df['非责任sku不可新增链接'] = np.where(c1 | c2, 1, 0)

    # 20250327 李总补充一批独享sku
    sku_list = ['1616240137811', '2013240106411', '2013240106411', '2013240106411', '2013240106411', '2013240106511',
                '2013240106511',
                '2013240106511', '2013240106511', '3113230312411', '3114230104311', '3116240011611', '3116240011611',
                '3116240011711',
                '3116240011911', '3116240013011', '3116240013611', '3116240013611', '3116240013611', '3116240013611',
                '3116240013711',
                '3116240013711', '3116240013712', '3116240013712', '3116240013911', '3116240013911', '3116240015911',
                '3116240015911',
                '3116240015911', '3116240015911', '3116240016211', '3116240016211', '3116240016211', '3116240016211',
                '3116240016311',
                '3116240016311', '3116240016311', '3116240016311', '3116240016711', '3116240016711', '3116240016711',
                '3116240018611',
                '3116240026511', '3116240026611', '3116240026711', '3117240210711', '3117240210911', '3117240211911',
                '3117240221111',
                '3117240222511', '3117240222511', '3117240222511', '3117240276111', '3117240276211', '3117240276311',
                '3118240152711',
                '3118240153711', '3118240169111', '3118240169111', '3118240169111']
    df['非责任sku不可新增链接'] = np.where(df['sku'].isin(sku_list), 'TEMU3组独享', df['非责任sku不可新增链接'])

    # 20250806 李总补充申请不清仓sku
    # df_no_clear = pd.read_excel('F://Ding_workspace//申请不清仓SKU.xlsx', dtype={'sku':str})
    # df_no_clear = df_no_clear[['sku','warehouse']]
    # df_no_clear['is_no_clear'] = 1
    # df = pd.merge(df, df_no_clear, how='left', on=['sku','warehouse'])
    # df = df[df['is_no_clear']!=1]

    # # 临时
    # sku_list = ['2613240136711','2613240141011','2613240136712','2613240141111','2610230453411','2610220196211',
    #             '2610230457211','2613240144311','2610220275311','2613240144111','2613240142211','2613240151411',
    #             '2613240136411','2613240136811','2613240136812','2613240144011','2613240144012','2613240151412']
    # df = df[df['sku'].isin(sku_list)]
    # print(df.info())
    # # 判断是否独享sku
    # df_temu_dx = get_duxiang_stock()
    # df = pd.merge(df, df_temu_dx, how='left', on=['sku','warehouse'])
    # df['非责任sku不可新增链接'] = np.where(~df['是否库存独享'].isna(), 1, df['非责任sku不可新增链接'])

    return df

def temu_clear_sku_tt():
    """ 清仓sku的库存信息 """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    # date_today = '2025-11-25'
    # temusku的清仓状态
    sql = f"""
        SELECT 
            sku, title, new_price, gross, product_package_size, best_warehouse_id, best_warehouse_name,
            warehouse, available_stock, on_way_stock, warehouse_stock_age, overage_level,warehouse_stock,
            age_60_plus , age_180_plus, age_90_plus,age_120_plus,age_150_plus,age_210_plus,
            age_270_plus,age_360_plus, age_30_plus
        FROM over_sea.dwm_sku_info_temu_tt
        WHERE date_id = '{date_today}' 
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓','西班牙仓', '意大利仓','日本仓') 
        -- and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    sql = f"""
        SELECT sku, warehouse, after_profit, day_sales
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info where date_id > '2025-08-01')
        and warehouse in ('美国仓', '澳洲仓', '德国仓', '英国仓', '加拿大仓','法国仓','墨西哥仓')
        and available_stock > 0
    """
    df_after_profit = conn.read_sql(sql)
    df = pd.merge(df, df_after_profit, how='left', on=['sku','warehouse'])
    col = ['age_180_plus','age_60_plus', 'available_stock']
    df[col] = df[col].fillna(0).astype(int)
    print(df.info())
    # df['90_ratio'] = np.where(df['age_90_plus']/df['available_stock'] > 1, 1,
    #                               df['age_90_plus']/df['available_stock'])
    c1 = (df['warehouse_stock'] * 0.5 <= df['age_360_plus']) & (df['warehouse_stock'] > 0)
    c2 = (df['warehouse_stock'] * 0.5 <= df['age_270_plus']) & (df['warehouse_stock'] > 0)
    c3 = (df['warehouse_stock'] * 0.5 <= df['age_210_plus']) & (df['warehouse_stock'] > 0)
    c4 = (df['warehouse_stock'] * 0.5 <= df['age_180_plus']) & (df['warehouse_stock'] > 0)
    c5 = (df['warehouse_stock'] * 0.5 <= df['age_150_plus']) & (df['warehouse_stock'] > 0)
    c6 = (df['warehouse_stock'] * 0.5 <= df['age_120_plus']) & (df['warehouse_stock'] > 0)
    c7 = (df['warehouse_stock'] * 0.5 <= df['age_90_plus']) & (df['warehouse_stock'] > 0)
    c8 = (df['warehouse_stock'] * 0.5 <= df['age_60_plus']) & (df['warehouse_stock'] > 0)
    c9 = (df['warehouse_stock'] * 0.5 <= df['age_30_plus']) & (df['warehouse_stock'] > 0)
    df['overage_level'] = np.select([c1,c2,c3,c4,c5,c6,c7,c8,c9],[360,270,210,180,150,120,90,60,30],0)

    df.drop(['age_30_plus','age_90_plus','age_120_plus','age_150_plus','age_210_plus','age_270_plus',
             'warehouse_stock'], axis=1, inplace=True)

    df = df[df['available_stock'] > 0]
    # 拼接独享仓日销

    # 获取责任人
    df['new_sku'] = df['sku']
    df = get_line(df)

    df_resp = pd.read_excel('F://Ding_workspace//2025年TEMU责任SKU初始化-0527.xlsx', dtype={'SKU': str})
    df_resp = df_resp.rename(columns={'SKU':'sku'})
    print(df_resp.info())
    df = pd.merge(df, df_resp[['sku','责任人']], how='left', on=['sku'])
    # 获取开发来源.剔除精铺转泛品sku
    df_sku = tt_get_sku()
    df = pd.merge(df, df_sku[['sku', 'develop_source_name']], how='left', on='sku')
    df = df[~df['develop_source_name'].str.contains('转泛品')]

    # 判断是否需要清仓
    # 20250114 取销售状态表
    sql = """
        SELECT sku ,warehouse ,sale_status is_temu_clear
        FROM over_sea.oversea_sale_status 
        WHERE end_time is NULL
    """
    df_sale_status = conn.read_sql(sql)
    df = pd.merge(df, df_sale_status, how='left', on=['sku', 'warehouse'])
    # c1 = (~df['责任人'].isin(['控股公司项目：彭柳森','控股公司项目：子龙'])) & (
    #         df['age_60_plus_temu'] > 0) & (df['oesd_temu'] > 30)
    # c2 = (df['责任人'].isin(['控股公司项目：彭柳森'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1005']) > 0) & (df['oesd_1005'] > 30)
    # c3 = (df['责任人'].isin(['控股公司项目：子龙'])) & (
    #         (df['age_60_plus_temu']+df['age_60_plus_1006']) > 0) & (df['oesd_1006'] > 30)
    # df['is_temu_clear'] = np.select([c1, c2, c3], [1,1,1], 0)

    # 20241224 白名单sku修改清仓状态
    # 第一批截止时间2025-02-01
    sql = """
        SELECT sku, warehouse, is_clear
        FROM over_sea.sales_status_temp
    """
    df_temp_sku = conn.read_sql(sql)
    df = pd.merge(df, df_temp_sku, how='left', on=['sku', 'warehouse'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 0, 0, df['is_temu_clear'])
    df['is_temu_clear'] = np.where(df['is_clear'] == 1, 1, df['is_temu_clear'])
    df.drop('is_clear', axis=1, inplace=True)
    stock_temp = df.groupby(['warehouse'])['available_stock'].sum().reset_index()
    stock_temp = stock_temp.sort_values(by='available_stock', ascending=False)
    print(stock_temp)
    print('清仓状态判断完成...')


    return df

def get_season_sku():
    """ 季节性产品标签 """
    sql = """
    SELECT DISTINCT sku, 1 as `冬季产品_产品系统标记`
    from yibai_prod_base_sync.yibai_prod_sku 
    where (feature_val  like '%4%' and feature_val not like '%1%' and feature_val not like '%2%' 
    and feature_val not like '%3%')  or for_holiday = 1
    """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    # df.to_excel('F://Desktop//df_season_sku.xlsx', index=0)
    return df

def get_high_charge_sku():
    """ 获取高仓租sku """
    sql = """
        SELECT a.* ,b.sale_status as `销售状态` 
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info 
            WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info)
            -- and warehouse in ('美国仓','德国仓','西班牙仓','意大利仓', '法国仓', '英国仓','澳洲仓','加拿大仓','墨西哥仓')
            and warehouse in ('美国仓','德国仓','法国仓','西班牙仓','意大利仓','英国仓','澳洲仓','加拿大仓')
            and available_stock > 0 
            and type not in ('通拓精铺', '通拓', '通拓跟卖')
            ) a
        left join (
            select sku ,warehouse ,sale_status from over_sea.oversea_sale_status WHERE end_time is NULL) b
        on a.sku = b.sku AND a.warehouse = b.warehouse
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    print(df.info())

    df = df.sort_values(by='charge_total_price_rmb', ascending=False)

    df_head = df.head(int(len(df) * 0.05))
    df_head['is_high_charge'] = 1
    df_head = df_head[['sku', 'warehouse', 'is_high_charge']]
    print(df_head.info())

    return df_head

def get_flash_clearout_sku():
    """ 限时清仓sku """
    sql = """
        SELECT sku, warehouse, source, 1 as flash_clearout
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df


def temu_clear_price_new(org='YB'):
    """
    计算temu清仓定价：
    1、按temu标准，区分清仓品和正常品
    2、计算不同利润率的定价
    3、补充责任人的信息及销量
    """
    utils.program_name = 'TEMU海外仓定价数据'
    make_path()

    date_today = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # ------------------------- 配置项 ---------------------
    extra_fee = 0.04      # 库损汇损
    refound_fee = 0.08    # TEMU退款率
    tp = 0.08              # TEMU目标净利率
    tz = 0.2             # TEMU差值
    tgp = 0.28            # TEMU目标毛利率
    sp = 0.1            # 建议兜底利润率
    ca_upward = 0.12      # 加拿大利润率上浮12%
    ship_upward = 1.2     # 销毁价尾程上浮20%
    highest_p = 0.15      # 销毁价最高毛利率15%
    col_yb = [40, 35, 30, 28, 20, 15, 8, 0, -12]  # 易佰、海兔：需计算展示的不同毛利率定价
    col_tt = [40, 35, 30, 28, 20, 15, 8, 0, -12]  # 通拓：需计算展示的不同毛利率定价
    if org == 'TT':
        tp = 0.08  # TEMU目标净利率
        tz = 0.19  # TEMU目标毛利率

    # 1. 获取sku信息
    if org == 'YB':
        df = temu_clear_sku_new()
        df['type'] = df['type'].fillna('').astype(str)
        df = df[~df['type'].str.contains('通拓')]
        df_mx = get_mx_tob_stock()
        df = pd.concat([df, df_mx])
    elif org == 'TT':
        df = temu_clear_sku_new()
        df['type'] = df['type'].fillna('').astype(str)
        df = df[df['type'].str.contains('通拓')]
    elif org == 'haitu':
        df = haitu_clear_sku()

    # 2. 获取运费
    df_useful = get_transport_fee()
    df_useful = df_useful.rename(columns={'warehouse_id':'best_warehouse_id'})
    col = ['sku', 'best_warehouse_id', 'total_cost', 'ship_fee', 'ship_name', 'country']
    df = pd.merge(df, df_useful[col], how='left', on=['sku', 'best_warehouse_id'])
    # df.drop(['warehouse_id'], axis=1, inplace=True)
    df['new_price'] = df['new_price'].astype(float)
    # 汇率
    df_rate = get_rate()
    df['us_rate'] = df_rate[df_rate['country'] == 'US'].iloc[0, 2]

    # 3. 计算价格
    # 3.1 销毁价
    is_canada = df['warehouse'] == '加拿大仓'
    destroy_den = np.where(is_canada, 1-extra_fee-refound_fee-tp-ca_upward, 1-extra_fee-refound_fee-tp)
    df['销毁价'] = ship_upward * df['ship_fee'] / destroy_den / df['us_rate']
    # 销毁价最高值不超15%
    highest_den = np.where(is_canada, 1-extra_fee-highest_p-ca_upward, 1-extra_fee-highest_p)
    df['highest_price_15'] = (df['new_price'] + df['total_cost']) / highest_den / df['us_rate']
    df['销毁价'] = np.minimum(df['销毁价'], df['highest_price_15'])

    # 目标毛利率定价
    if org in ['YB', 'haitu']:
        col = col_yb
    elif org == 'TT':
        col = col_tt

    for i in col:
        df[f'毛利率{i}%_美元'] = (df['new_price'] + df['total_cost']) / (1 - extra_fee - i / 100) / df['us_rate']

    # 运费补贴及门槛
    df = get_freight_subsidy(df, dim='sku')
    df = df.rename(columns={'limit_price':'补贴门槛美元', 'freight_subsidy':'运费补贴美元'})
    df['补贴门槛美元'] = df['补贴门槛美元'] * df['rate'] / df['us_rate']
    df['运费补贴美元'] = df['运费补贴美元'] * df['rate'] / df['us_rate']

    for i in col:
        df[f'毛利率{i}%_扣除运费补贴价美元'] = np.where(df[f'毛利率{i}%_美元'] < df['补贴门槛美元'],
                                                        df[f'毛利率{i}%_美元'] - df['运费补贴美元'],
                                                        df[f'毛利率{i}%_美元'])
        df[f'毛利率{i}%_扣除运费补贴价美元'] = np.where(df['country'] == 'US',
                                                        df[f'毛利率{i}%_扣除运费补贴价美元'] - 0.3,
                                                        df[f'毛利率{i}%_扣除运费补贴价美元'])
        df[f'毛利率{i}%_扣除运费补贴价美元'] = np.where(df[f'毛利率{i}%_扣除运费补贴价美元'] < 0.01, 0.01,
                                                        df[f'毛利率{i}%_扣除运费补贴价美元'])

    df['销毁价(扣除运费补贴)'] = np.where(df['销毁价'] < df['补贴门槛美元'], df['销毁价'] - df['运费补贴美元'], df['销毁价'])
    df['销毁价(扣除运费补贴)'] = np.where(df['country'] == 'US', df['销毁价(扣除运费补贴)'] - 0.3, df['销毁价(扣除运费补贴)'])
    df['销毁价(扣除运费补贴)'] = np.where(df['销毁价(扣除运费补贴)'] < 0.01, 0.01, df['销毁价(扣除运费补贴)'])
    df['销毁价毛利率'] = 1 - extra_fee - (df['new_price'] + df['total_cost']) / (df['销毁价']) / df['us_rate']
    df['销毁价毛利率'] = df['销毁价毛利率'].fillna(0).astype(float)
    df['销毁价毛利润额美元'] = df['销毁价'] * df['销毁价毛利率']

    # 20250722 接入调价平台的限时清仓逻辑
    # 20260213 接入继续清仓sku
    # df_flash = get_flash_clearout_sku()
    df_flash = get_temp_clear_sku()
    df_flash = df_flash.rename(columns={'temp_clear_sku':'flash_clearout'})
    # if org == 'TT':
    #     df_flash = pd.read_excel('F://Ding_workspace//202512海外仓国内仓清仓SKU明细(1).xlsx', dtype={'sku': str})
    #     df_flash = df_flash[df_flash['是否放缓'] == '不可放缓']
    #     df_flash = df_flash[['sku', 'warehouse']]
    #     df_flash['flash_clearout'] = 1
    df = pd.merge(df, df_flash, how='left', on=['sku','warehouse'])
    # df['source'] = np.where(df['develop_source_name'].isin(['海兔转泛品','易通兔']), 'PartC', df['source'])
    # sku建议兜底毛利率 & sku建议定价
    # 20260108 非限时清仓兜底恢复之前的刻度（超180的销毁价兜底）
    # 20260213 非限时清仓兜底调整为-10%，限时清仓（继续清仓）用调价目标毛利率
    df['sku建议兜底毛利率'] = sp
    # df['sku建议兜底毛利率'] = df['after_profit'] + tgp
    df['sku调价目标毛利率'] = df['after_profit'] + tp + tz
    df['overage_level'] = df['overage_level'].fillna(0).astype(int)
    # # 20260104 限时清仓不同part指定清仓截止日期。（分批次降到销毁价）
    # d1 = (pd.to_datetime('2026-01-31') - datetime.datetime.now()).days
    # d2 = (pd.to_datetime('2026-02-28') - datetime.datetime.now()).days
    # d3 = (pd.to_datetime('2026-03-31') - datetime.datetime.now()).days
    # d4 = (pd.to_datetime('2026-04-30') - datetime.datetime.now()).days
    # c0 = (~df['flash_clearout'].isna()) & (df['sku调价目标毛利率'] > df['销毁价毛利率']) & (df['available_stock'] > 0)
    # c1 = c0 & (df['source']=='PartA')
    # c2 = c0 & (df['source']=='PartB')
    # c3 = c0 & (df['source']=='PartC')
    # c4 = c0 & (df['source']=='PartD')
    # r1 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d1/7 - 1, 1)
    # r2 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d2/7 - 1, 1)
    # r3 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d3/7 - 1, 1)
    # r4 = df['sku调价目标毛利率'] - (df['sku调价目标毛利率']-df['销毁价毛利率'])/max(d4/7 - 1, 1)
    # df['label'] = np.select([c1,c2,c3,c4], [1, 2, 3, 4], 0)
    # df['sku调价目标毛利率'] = np.select([c1,c2,c3,c4], [r1, r2, r3, r4], df['sku调价目标毛利率'])
    # df['sku调价目标毛利率'] = np.where(c1, df['销毁价毛利率'], df['sku调价目标毛利率'])

    # 最终建议兜底价毛利率。（20260205取消限时清仓逻辑）
    c1 = (df['flash_clearout'].isna()) & (df['sku调价目标毛利率'] <= (sp + 0.02))
    df['sku建议兜底毛利率'] = np.where(c1, df['sku建议兜底毛利率'], df['sku调价目标毛利率'])

    # 计算最终建议兜底价
    df['sku建议兜底价'] = (df['new_price'] + df['total_cost']) / (1 - extra_fee - df['sku建议兜底毛利率']) / df['us_rate']
    # df.to_excel('F://Desktop//temu_df_temp.xlsx', index=0)
    # 供应商货盘sku
    df = temu_supplier_sku_price(df, org=org)
    # 供应商sku补充门槛及补贴
    df_1 = df[df['是否使用货盘sku定价'] == 1]
    df_2 = df[df['是否使用货盘sku定价'] != 1]
    df_1.drop(['补贴门槛美元','运费补贴美元','rate'], axis=1, inplace=True)
    df_1 = get_freight_subsidy(df_1, dim='sku')
    df_1['us_rate'] = df_rate[df_rate['country'] == 'US'].iloc[0, 2]
    df_1 = df_1.rename(columns={'limit_price':'补贴门槛美元', 'freight_subsidy':'运费补贴美元'})
    df_1['补贴门槛美元'] = df_1['补贴门槛美元'] * df_1['rate'] / df_1['us_rate']
    df_1['运费补贴美元'] = df_1['运费补贴美元'] * df_1['rate'] / df_1['us_rate']
    df = pd.concat([df_2, df_1])

    df['sku建议兜底价'] = np.where(df['sku建议兜底价'] < df['补贴门槛美元'],
                                   df['sku建议兜底价'] - df['运费补贴美元'], df['sku建议兜底价'])
    df['sku建议兜底价'] = np.where(df['country'] == 'US', df['sku建议兜底价'] - 0.3, df['sku建议兜底价'])
    # 近30天日销=0的价格再降10%
    # df['近30天无销'] = np.where(df['day_sales']==0, 1, 0)
    # df['sku建议兜底价'] = np.where(df['day_sales']==0, df['sku建议兜底价']*0.9, df['sku建议兜底价'])

    df['sku建议兜底价'] = np.where(df['sku建议兜底价'] < 0.01, 0.01, df['sku建议兜底价'])
    df['sku建议兜底价'] = np.where(df['sku建议兜底价'] < df['销毁价(扣除运费补贴)'], df['销毁价(扣除运费补贴)'], df['sku建议兜底价'])

    # 正常品建议兜底毛利率置为空
    df['sku建议兜底价'] = df['sku建议兜底价'].replace('', np.nan).fillna(0).astype(float)
    df['sku建议兜底毛利率'] = np.where(abs(df['sku建议兜底价']-df['销毁价(扣除运费补贴)'])<=0.0001, df['销毁价毛利率'],df['sku建议兜底毛利率'])
    # df['sku建议兜底毛利率'] = np.where(df['is_temu_clear'].isna(), np.nan, df['sku建议兜底毛利率'])
    # df['sku建议兜底价'] = np.where(df['is_temu_clear'].isna(), np.nan, df['sku建议兜底价'])

    # # 海兔补充按正常品建议兜底
    # sku_list = ['RYE006','RYE007','MF007W','24EE005B10001','24EE005B10002','AM004B','24EE005G10001','GZ031','24EE005G10002','MF006W']
    # c1 = (df['sku'].isin(sku_list)) & (df['warehouse']=='美国仓')
    # # 指定sku按正常品清仓
    # df_no_clear = pd.read_excel('F://Ding_workspace//确定不清仓SKU1.6.xlsx', dtype={'sku': str})
    # df_no_clear['is_no_clear'] = 1
    # df = pd.merge(df, df_no_clear[['sku','warehouse','is_no_clear']], how='left', on=['sku','warehouse'])
    # c2 = ~df['is_no_clear'].isna()
    # df['sku建议兜底毛利率'] = np.where(c1, tgp, df['sku建议兜底毛利率'])
    # df['sku建议兜底价'] = np.where(c1, df[f'毛利率{int(tgp*100)}%_扣除运费补贴价美元'], df['sku建议兜底价'])
    # df['flash_clearout'] = np.where(c2, 0, df['flash_clearout'])
    # df['source'] = np.where(c2, 0, df['source'])

    # 字段整理
    df['is_temu_clear'] = df['is_temu_clear'].replace({1:'补充清仓sku', 0:''})
    col1 = df.pop('new_price')
    df.insert(df.columns.get_loc('total_cost') - 1, 'new_price', col1)
    df = df.drop_duplicates(subset=['sku', 'warehouse', 'country'])
    print(df.info())
    col = ['warehouse_stock_age','day_sales', 'new_sku',  'after_profit', 'is_tt_sku',
           'highest_price_15','销毁价','sku调价目标毛利率']
    df.drop(col, axis=1, inplace=True)

    if org in ['YB', 'haitu']:
        col = col_yb
    elif org == 'TT':
        col = col_tt
    col_2 = []
    for i in col:
        col_2.append(f'毛利率{i}%_美元')
    df.drop(col_2, axis=1, inplace=True)
    # df.to_excel('F://Desktop//temu_df_temp.xlsx', index=0)

    df = df[~df['total_cost'].isna()]

    if org == 'YB':
        table_name = 'TEMU海外仓清仓定价'
    elif org == 'TT':
        table_name = 'TEMU海外仓清仓定价_TT'
        df['available_stock'] = np.where(df['available_stock']>5, '5+', df['available_stock'])
        df['on_way_stock'] = np.where(df['on_way_stock'] > 5, '5+', df['on_way_stock'])
        df['age_180_plus'] = np.where(df['age_180_plus']>5, '5+', df['age_180_plus'])
    elif org == 'haitu':
        table_name = 'TEMU海外仓清仓定价_haitu'
        df = df[df['供应商sku'].isna()]

    # 屏蔽sku
    df_block = pd.read_excel('F://Ding_workspace//特定SKU明细-20250708.xlsx', dtype={'sku': str})
    df = df[~df['sku'].isin(df_block['sku'].unique())]

    dic = {'available_stock':'可用库存', 'on_way_stock':'在途库存', 'overage_level':'超库龄等级',
           'flash_clearout':'是否限时清仓sku', 'age_60_plus':'超60库龄库存', 'develop_source_name':'开发来源',
           'age_180_plus':'超180库龄库存','is_temu_clear':'是否temu清仓sku','new_price':'成本', 'source':'限时清仓sku分类',
           'total_cost':'总运费','ship_fee':'尾程','ship_name':'尾程渠道','country':'目的国'}
    df = df.rename(columns=dic)

    save_df(df, table_name, file_type='xlsx')


    # df.to_excel('F://Desktop//df_temu_clear_2.xlsx', index=0)

def temu_supplier_sku_price(df_price, org='YB'):
    """
    供应商货盘sku TEMU定价
    1、用供应商货盘sku的信息，匹配TEMU定价数据
    2、能匹配到的，用定价逻辑判断怎么定价
    3、不能匹配到的，用供应商sku定价
    """
    # 1. 供应商货盘sku
    if org in ['YB','haitu']:
        table_name = 'dwm_supplier_sku_price'
    elif org == 'TT':
        table_name = 'tt_dwm_supplier_sku_price'
    sql = f"""
         SELECT 
             YB_sku sku, YM_sku, warehouse_price, total_cost total_cost_s, shippingCost, ship_name ship_name_s, 
             warehouse_id warehouse_id_s, 
             warehouse_name, warehouse, available_stock sup_stock, country, platform,  
             sup_price, platform_must_percent+platform_zero target_profit_rate 
         FROM yibai_oversea.{table_name}
         WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name})
         and sup_price is not Null 
         and platform = 'TEMU'
         and sup_stock > 0
     """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)

    # 2. YB sku
    # df_price = pd.read_excel('F://Desktop//日常任务//TEMU定价需求//TEMU海外仓清仓定价1223.xlsx', dtype={'sku':'str'})
    # df_price = df_price.rename(columns={'目的国':'country'})
    df = pd.merge(df_price, df_sup_price, how='outer', on=['sku','country','warehouse'])
    df['available_stock'] = df['available_stock'].fillna(0).astype(int)
    us_rate = df['us_rate'].iloc[0]/0.97
    print(us_rate)
    df['sup_price'] = df['sup_price'].astype(float)
    df['sup_price'] = df['sup_price']/us_rate

    # 替换逻辑
    c1 = (df['available_stock'] > 0) & (df['sup_stock'] == 0)
    c2 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['is_temu_clear'].isin(['正常', '涨价缩销','']))
    c3 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (
        df['is_temu_clear'].isin(['正利润加快动销', '负利润加快动销', '清仓', '补充清仓sku']))
    c4 = (df['available_stock'] == 0 | df['available_stock'].isna()) & (df['sup_stock'] > 0)
    df['condition'] = np.select([c1, c2, c3, c4],
                                ['供应商sku无库存取建议兜底价', '都有库存且状态正常取小值', '动销品取建议兜底价', 'YBsku无库存'], 0)
    df['最终建议价'] = np.select([c1, c2, c3, c4], [
        df['sku建议兜底价'], np.minimum(df['sup_price'], df['sku建议兜底价'].fillna(np.inf)), df['sku建议兜底价'], df['sup_price']],
        df['sku建议兜底价'])
    df['最终建议兜底毛利率'] = np.where(df['最终建议价'] == df['sku建议兜底价'], df['sku建议兜底毛利率'], df['target_profit_rate'])
    df['是否供应商货盘sku'] = np.where(~df['YM_sku'].isna(), 1, 0)
    df['是否使用货盘sku定价'] = np.where(df['最终建议价']==df['sup_price'], 1, 0)

    # 替换字段
    dic = {'YM_sku':'供应商sku', 'warehouse_price':'供货价', 'total_cost_s':'供应商sku总运费', 'shippingCost':'供应商sku尾程运费',
           'ship_name_s':'供应商sku尾程渠道', 'warehouse_id_s':'供应商sku库存子仓id', 'warehouse_name':'供应商sku库存子仓',
           'sup_stock':'供应商sku可用库存', 'condition':'替换为货盘价的原因',
           'sup_price':'供应商供货价的定价', 'target_profit_rate':'供应商sku目标毛利率'}
    df = df.rename(columns=dic)
    print(df.info())
    # 定义列映射关系：
    col_mapping = {
        'new_price': '供货价',
        'total_cost': '供应商sku总运费',
        'ship_fee': '供应商sku总运费',
        'ship_name': '供应商sku尾程渠道',
        'best_warehouse_id': '供应商sku库存子仓id',
        'best_warehouse_name': '供应商sku库存子仓',
        'available_stock': '供应商sku可用库存',
        'sku建议兜底毛利率': '最终建议兜底毛利率',
        'sku建议兜底价': '最终建议价'
    }
    # ,
    # 'sku建议兜底毛利率': '最终建议兜底毛利率',
    # 'sku建议兜底价': '最终建议价'
    mask = df['是否使用货盘sku定价'] == 1
    for target_col, replace_col in col_mapping.items():
        df[target_col] = np.where(mask, df[replace_col], df[target_col])

    # 整理字段
    col = ['供货价', '供应商sku总运费', '供应商sku尾程运费', '供应商sku尾程渠道', '供应商sku库存子仓id', '供应商sku库存子仓',
           '供应商sku可用库存','供应商供货价的定价','供应商sku目标毛利率','platform','最终建议价','最终建议兜底毛利率']
    df.drop(col, axis=1, inplace=True)

    # df.to_excel('F://Desktop//temu_supplier_sku.xlsx', index=0)

    return df



## 有效销量
def mrp_oversea_sales():
    """ 计划采购需求有效销量数据获取 """
    date_today = time.strftime('%Y%m%d')
    # date_today = '20251128'
    sql = f"""
        SELECT `标识`, `区域大仓`, `海外仓总日销` effective_sales
        FROM yibai_mrp_oversea.yibai_order_point_temp_all_{date_today}
    """
    conn_ck = pd_to_ck(database='yibai_mrp_oversea', data_sys='易佰MRP')
    df_point_sales = conn_ck.ck_select_to_df(sql)
    print(df_point_sales.info())

    df_point_sales['sku'] = df_point_sales['标识'].str.split('$').str[0]
    df_point_sales['warehouse'] = np.where(df_point_sales['区域大仓'].isin(['美东仓', '美西仓']), '美国仓',
                                           df_point_sales['区域大仓'])
    df_point_sales['warehouse'] = np.where(df_point_sales['warehouse'].isin(['澳洲悉尼仓', '澳洲墨尔本仓']), '澳洲仓',
                                           df_point_sales['warehouse'])
    print(df_point_sales['warehouse'].unique())
    df_point_sales = df_point_sales.groupby(['sku', 'warehouse']).agg({'effective_sales': 'sum'})

    return df_point_sales
    # print(df_point_sales.head(5))


# mrp_oversea_sales()

def get_section():
    """ 海外仓调价逻辑 """
    sql = """
    SELECT *
    FROM profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    profit_rate_section = conn.read_sql(sql)

    profit_rate_section.to_excel('F://Desktop//profit_rate.xlsx', index=0)


def get_sku():
    df = pd.read_excel('F://Desktop//df_sku_price.xlsx', dtype={'sku': str})
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

    df = pd.merge(df, df_sku[['sku', 'new_price']], how='left', on='sku')
    df.to_excel('F://Desktop//df_sku_cost.xlsx', index=0)

    return df



##
if __name__ == '__main__':
    # main()
    # check_temu_order()
    # # 每周一
    # a, b, c = get_oversea_info()
    # get_temu_shein_price()

    # TEMU在线链接利润率核算
    # check_temu_online_price()

    temu_clear_price_new(org='YB')
    temu_clear_price_new(org='haitu')
    temu_clear_price_new(org='TT')
    # temu_supplier_sku_price()

    # temu_clear_sku()
    # mrp_oversea_sales()