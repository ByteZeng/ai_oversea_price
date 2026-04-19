"""
ALLEGRO海外仓链接获取
"""
##
import datetime
import traceback
import pandas as pd
import time
import warnings
from tqdm import tqdm
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import  chicun_zhongliang
warnings.filterwarnings("ignore")
##
def get_oversea_fee_temp(df):
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')

    #
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    print(df.info())
    #
    ship_type = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    warehouse_id = '325'
    df = df.reset_index(drop=True)

    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 200))
    df_result = pd.DataFrame()
    for key, group in tqdm(df.groupby(['index'])):
        print(key, group.shape)
        group1 = group[['sku', '数量', '成本','重量','长','宽','高']]
        yunfei_jisuan = get_trip_fee_oversea('ALLEGRO', 'PL', warehouse_id, ship_type)
        group2 = yunfei_jisuan.batch_df_order(group1)
        # print(group2.info())
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipName','shipCode',
             'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[
            ['sku', '成本','重量','长','宽','高', 'shipCountry', 'platform', 'warehouseId', 'warehouseName',
             'shipCode', 'shipName', 'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True)
        # group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])
    df_result['platform'] = 'ALLEGRO'
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_result, 'sku_fee_allegro', if_exists='replace')
    # conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn_mx2.ck_insert(df_result, table_name, if_exist='append')
    # 海运头程
    # group2[['长','宽','高']] = group2[['长','宽','高']].astype(float)
    # group2['weight_volume'] = group2['长'] * group2['宽'] * group2['高']
    # group2['new_first_sea'] = (group2['weight_volume'] / 1000000) * 1070
    # group2[['totalCost','firstCarrierCost']] = group2[['totalCost','firstCarrierCost']].astype('float')
    # group2['total_cost_sea'] = group2['totalCost'] - group2['firstCarrierCost'] + group2['new_first_sea']
    # group2.to_excel('group.xlsx', index=0)

    return df_result

def get_sku_info():
    """
    获取sku信息
    """
    sql = """
    SELECT distinct sku, warehouse
    FROM over_sea.dwm_sku_temp_info
    WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info )
    and warehouse = '德国仓'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # df = df.sample(1000)
    return df

def get_listing_info():
    sql = f"""
    SELECT 
        a.account_id as account_id, b.erp_id, b.short_name as account_name,  
        a.offer_id as offer_id, a.seller_sku, if(match(a.seller_sku, '\+|\*')=1, a.seller_sku, a.sku) as sku,
        a.product_id as product_id, toFloat64(a.selling_mode_price) as online_price,
        toFloat64(a.delivery_amount) as delivery_amount, a.location1 as location, b.group_name as group_name
    FROM (
        SELECT 
            *, replace(JSONExtractRaw(location, 'countryCode'), '"', '') as location1
        FROM yibai_sale_center_listing_sync.yibai_allegro_listing
        WHERE 
            selling_mode_price > 0 and status in (1,4)
            and location1 not in ('CN', 'GB')
        ) a 
    INNER JOIN (
        SELECT b.id, b.erp_id,  b.short_name, c.group_name
        FROM yibai_sale_center_system.yibai_system_account b
        inner join (
            select account_id,splitByString('->', oa_group_name)[-1] as group_name
            from yibai_system_kd_sync.yibai_allegro_account
            where account_type = 1 and is_platform_account != 2 and account_num_name != 'DMSAllegro'
        ) c
        on b.account_id=c.account_id
        WHERE platform_code  = 'ALLEGRO' 
    ) b 
    on a.account_id=b.id
    
    """
    conn_kd = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df = conn_kd.ck_select_to_df(sql)
    #
    # 获取海外仓sku信息
    sql = """
        SELECT 
            a.sku, a.warehouse, best_warehouse_name, new_price,  available_stock, overage_level,
            day_sales, b.total_cost, b.sales_status, b.price_rmb, b.price, platform, country
        FROM over_sea.dwm_sku_temp_info a
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT 
                    sku, warehouse, total_cost, sales_status, price_rmb, price, platform, country,
                    ROW_NUMBER() OVER (PARTITION BY sku, warehouse, country ORDER BY total_cost) AS row_num
                FROM yibai_oversea.dwm_oversea_price_dtl
                WHERE date_id = '2025-03-04' and country in ('CZ', 'DE', 'FR', 'PL') and warehouse in ('德国仓', '法国仓')
            ) a
            WHERE a.row_num = 1
        ) b
        ON a.sku = b.sku and a.warehouse = b.warehouse
        WHERE date_id = '2025-03-04' and a.warehouse in ('德国仓', '法国仓')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    #
    df_sku = df_sku.sort_values(by=['sku','available_stock','warehouse'], ascending=[True, False, True])
    df_sku = df_sku.drop_duplicates(subset=['sku', 'warehouse','country'])
    #
    sql = """
        SELECT distinct country, from_currency_code as charge_currency,rate 
        FROM domestic_warehouse_clear.erp_rate
        WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate) and site = '波兰'
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = conn_ck.ck_select_to_df(sql)
    #
    df_sku = df_sku.rename(columns={'country':'location'})
    df_result = pd.merge(df, df_sku, how='left', on=['sku', 'location'])

    return df_result
##
def main():
    # df_result = get_listing_info()
    # print(df_result.info())
    # # df_result.to_excel('df_result.xlsx', index=0)
    # df_result = df_result[~df_result['location'].isna()]
    df = get_sku_info()
    df['site'] = 'PL'
    df = df[['sku','site']]
    df = df.drop_duplicates()
    get_oversea_fee_temp(df)

# ##
# # 核对价格
# df_result = get_listing_info()
# ##
#
# ##
# # 获取运费数据
# sql = """
#     SELECT
#         sku, `成本`,`重量`,`长`,`宽`,`高`, shipCountry, warehouseName, shipCode, shipName, totalCost, shippingCost,
#         firstCarrierCost, dutyCost
#     FROM over_sea.sku_fee_allegro
# """
# conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
# df_fee = conn.read_sql(sql)
# ##
# df_fee[['长','宽','高']] = df_fee[['长','宽','高']].astype(float)
# df_fee['weight_volume'] = df_fee['长'] * df_fee['宽'] * df_fee['高']
# df_fee['new_first_sea'] = (df_fee['weight_volume'] / 1000000) * 1202
# df_fee[['totalCost','firstCarrierCost']] = df_fee[['totalCost','firstCarrierCost']].astype('float')
# df_fee['total_cost_sea'] = df_fee['totalCost'] - df_fee['firstCarrierCost'] + df_fee['new_first_sea']
# df_fee.drop(['weight_volume'], axis=1, inplace=True)
#
# df_fee = df_fee.sort_values(by=['sku','total_cost_sea'], ascending=[True, True])
# df_fee_useful = df_fee.drop_duplicates(subset=['sku'])
# ##
# df_fee_useful.to_excel('df_fee_useful.xlsx', index=0)
# ##
# df_final = pd.merge(df_result, df_fee_useful[['sku','成本','warehouseName','shipName','total_cost_sea']], how='left', on=['sku'])
# ##
# df_final.to_excel('df_final_all.xlsx', index=0)
# ##
# # allegro海外仓订单
# sql = """
#     SELECT
#         order_id, sku, account_id, paytime, total_price, true_profit_new1, warehouse_name, quantity,sales_status
#     FROM over_sea.ads_oversea_order
#     WHERE platform_code = 'ALLEGRO' and paytime >= '2024-01-01'
# """
# df_order  = conn.read_sql(sql)
# ##
# df_order.to_excel('df_order.xlsx', index=0)
# ##
# df_temp = df_final[df_final['total_cost_sea'].isna()][['sku','location']]
# df_temp['site'] = 'PL'
# df_temp = df_temp.drop_duplicates(subset=['sku'])
# ##
# df_temp_fee = get_oversea_fee_temp(df_temp)

def contrast_fee():
    """ 对比amazon 和 allegro 运费数据（尾程） """

    sql = """
        SELECT sku, warehouseId, warehouseName, shipName, totalCost_origin, shippingCost
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON' and country = 'PL' and warehouseId = 325
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT sku, warehouseId,  shipName shipName_a, totalCost totalCost_a, shippingCost shippingCost_a
        FROM over_sea.sku_fee_allegro
               
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_a = conn.read_sql(sql)

    df_a['warehouseId'] = df_a['warehouseId'].astype(int)
    df = pd.merge(df, df_a, how='left', on=['sku','warehouseId'])

    df.to_excel('F://Desktop//df_allegro.xlsx', index=0)




##
if __name__ == '__main__':
    # main()
    contrast_fee()

