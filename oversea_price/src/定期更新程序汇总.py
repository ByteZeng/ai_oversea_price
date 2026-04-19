"""

"""
##
import pandas as pd
import numpy as np
import time, datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck


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

    # sql = f"""
    # delete from {table_name} where date_id='{date_id}'
    # """
    # conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='replace')

    conn.close()

## 定价比运费配置表
def write_pricing_ratio_config():
    df_temp = pd.read_excel('F://Desktop//pricing_ratio_config_table.xlsx')
    print(df_temp.info())
    #
    write_to_sql(df_temp, 'pricing_ratio_config_table')
#
write_pricing_ratio_config()


## 定价比头程配置表自动更新
def update_pricing_ratio_config():
    """ 更新定价比头程单价配置表 """
    """`站点`,`对照表`, `头程价格`,`申报比率`,`关税率`,`计泡系数`,
            `敏感货单价`,`关税/kg（日本）`,`计费方式`,`关税计算方式`"""
    sql = f"""
        select *
        from over_sea.pricing_ratio_config_table    
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    # 备份原表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'pricing_ratio_config_table_backup', if_exists='replace')
    conn.close()

    df.loc[df['站点'] != '美国', '对照表'] = df['对照表'].replace('慢海', '海运')
    df.loc[df['站点'] == '美国', '对照表'] = df['对照表'].replace('快海', '海运')

    type = df['对照表'].unique()

    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = f"""
    select site as `站点`, unit_price, sensitive_price, tc_type as `对照表`
    from domestic_warehouse_clear.fba_fees_parameter_tc
    where  platform='FBA'
    """
    price_table = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, price_table, how='left', on=['站点','对照表'])

    df['头程价格'] = np.where(~df['unit_price'].isna(), df['unit_price'], df['头程价格'])
    df['敏感货单价'] = np.where(~df['sensitive_price'].isna(), df['sensitive_price'], df['敏感货单价'])

    df.loc[df['站点'] != '美国', '对照表'] = df['对照表'].replace('海运', '慢海')
    df.loc[df['站点'] == '美国', '对照表'] = df['对照表'].replace('海运', '快海')

    df.drop(['unit_price','sensitive_price'], axis=1, inplace=True)

    # 存表
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'pricing_ratio_config_table', if_exists='replace')
    conn.close()
    # df.to_excel('F://Desktop//df_price_temp.xlsx', index=0)

update_pricing_ratio_config()
## 海外仓头程单价配置表
def oversea_fees_parameter():
    df = pd.read_excel('F:\Desktop\海外仓配置表.xlsx')
    #
    # df.drop('关税/kg', axis=1, inplace=True)
    #
    # df['warehouse_id'] = df['warehouse_id'].fillna(1136).astype(int)
    df['是否主要渠道'] = df['是否主要渠道'].fillna(0).astype(int)
    #
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn_ck.execute('TRUNCATE TABLE amazon_sku_site_commission')
    conn_ck.ck_insert(df, 'oversea_fees_parameter', if_exist='replace')

oversea_fees_parameter()

##
def oversea_fees_parameter_new():
    df = pd.read_excel('F:\Desktop\海外仓头程配置表2026.xlsx')
    #
    # df.drop('关税/kg', axis=1, inplace=True)
    #
    # df['warehouse_id'] = df['warehouse_id'].fillna(1136).astype(int)
    df['是否主要渠道'] = df['是否主要渠道'].fillna(0).astype(int)
    #
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn_ck.execute('TRUNCATE TABLE amazon_sku_site_commission')
    conn_ck.ck_insert(df, 'oversea_fees_parameter_new', if_exist='replace')

oversea_fees_parameter_new()
##
def oversea_temu_para():
    df = pd.read_excel('F://Desktop//temu_profit_rate_section.xlsx')
    #
    # df.drop('关税/kg', axis=1, inplace=True)

    #
    write_to_sql(df, 'temu_profit_rate_section')

oversea_temu_para()
## 调价幅度表更新

def profit_section():

    df = pd.read_excel('F://Desktop//profit_rate_section.xlsx')
    print(df.info())
    #
    # write_to_sql(df, 'profit_rate_section')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'profit_rate_section', if_exists='replace')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'profit_rate_section', if_exist='replace')
profit_section()

##
def profit_section_upload():

    sql = """
    SELECT *
    FROM profit_rate_section
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    df.to_excel('F://Desktop//profit_rate_section.xlsx', index=0)

profit_section_upload()

## 限时清仓幅度表更新
def flash_clear_profit_section():

    df = pd.read_excel('F://Desktop//flash_clear_profit_rate.xlsx')
    print(df.info())
    #
    # write_to_sql(df, 'profit_rate_section')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'flash_clear_profit_rate', if_exists='append')

flash_clear_profit_section()

## 限时清仓sku更新
def flash_clear_sku():

    df = pd.read_excel('F://Desktop//df_flash.xlsx', dtype={'sku':str})
    col = ['date_id', 'start_time', 'end_time', 'limit_time']
    col = ['date_id']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    # df['date_id'] = time.strftime('%Y-%m-%d')
    # df = df[['sku', 'warehouse']]
    # df['date_id'] = '2025-12-02'
    print(df.info())
    print(df.head(4))
    #
    # write_to_sql(df, 'profit_rate_section')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_flash_clearout_sku', if_exists='append')

flash_clear_sku()

## 继续清仓sku存表
def temp_clear_sku():

    df = pd.read_excel('F://Desktop//df_temp_clear_sku_2.xlsx', dtype={'sku':str})
    # df['date_id'] = time.strftime('%Y-%m-%d')
    df['date_id'] = '2026-04-08'
    print(df.info())
    print(df.head(4))

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'oversea_temp_clear_sku', if_exist='append')

temp_clear_sku()

## 待销毁数据临时表
def wait_destroy_sku():

    df = pd.read_excel('F://Desktop//oversea_wait_destroy_sku.xlsx', dtype={'sku':str})
    col = ['date_id', 'start_time']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    # df['date_id'] = time.strftime('%Y-%m-%d')
    # df = df[['sku', 'warehouse']]
    # df['date_id'] = '2025-12-02'
    print(df.info())
    print(df.head(4))
    #
    # write_to_sql(df, 'profit_rate_section')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_wait_destroy_sku', if_exists='replace')

wait_destroy_sku()

##
def supplier_sku_fee():
    """  供应商货盘sku接口运费 """
    df = pd.read_excel('F://Desktop//df_result_fee.xlsx', dtype={'sku':str})
    df['date_id'] = time.strftime('%Y-%m-%d')
    df.drop('数量', axis=1, inplace=True)
    print(df.info())

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn_ck.execute('TRUNCATE TABLE amazon_sku_site_commission')
    conn_ck.ck_insert(df, 'oversea_transport_fee_supplier', if_exist='replace')
supplier_sku_fee()
##
def up_section():

    df = pd.read_excel('F://Desktop//df_up.xlsx')
    print(df.info())
    #
    # write_to_sql(df, 'profit_rate_section')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'up_rate_section', if_exists='replace')

up_section()

## 差值表更新
def update_platform_fee():
    sql = """
        SELECT *
        FROM over_sea.yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_pf = conn.read_sql(sql)
    df_pf = df_pf.drop_duplicates()
    # 原差值表备份
    conn.to_sql(df_pf, 'yibai_platform_fee_backup', if_exists='replace')
    # 中台差值表
    sql = """
        SELECT platform_code platform, site, toFloat32(net_profit2)/100 as net_profit2
        FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_pc = conn_ck.ck_select_to_df(sql)
    #
    df = pd.merge(df_pf, df_pc, how='left', on=['platform', 'site'])

    # 差值替换
    df.loc[df['platform']=='CDISCOUNT', 'net_profit2'] = df_pc.loc[df_pc['platform']=='CDISCOUNT', 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='EB') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='EB') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WALMART') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WALMART') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='AMAZON') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='AMAZON') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='ALI') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='ALI') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WISH') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WISH') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='TEMU') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='TEMU') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]

    #
    df[['platform_zero','net_profit2']] = df[['platform_zero','net_profit2']].round(4)
    #
    mismatched_rows = df[df['platform_zero'] != df['net_profit2']]
    print('差值更新的平台和国家：')
    mismatched_rows[['platform', 'site','platform_zero','net_profit2']].apply(lambda x: print(f"Platform: {x['platform']}, Site: {x['site']}, "
                                                   f"原差值：{x['platform_zero']},最新差值：{x['net_profit2']}"), axis=1)
    #
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df.drop('net_profit2', axis=1, inplace=True)

    # 更新
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_platform_fee', if_exists='replace')

update_platform_fee()
##
def update_platform_fee_old():
    """
    """
    df = pd.read_excel('F://Desktop//yibai_platform_fee.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_platform_fee', if_exists='replace')

update_platform_fee_old()

##
def tt_update_platform_fee_old():
    """
    """
    df = pd.read_excel('F://Desktop//tt_yibai_platform_fee.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_yibai_platform_fee', if_exists='replace')

tt_update_platform_fee_old()
## tt差值表更新
def tt_update_platform_fee():
    sql = """
        SELECT *
        FROM over_sea.tt_yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_pf = conn.read_sql(sql)
    # 原差值表备份
    conn.to_sql(df_pf, 'tt_yibai_platform_fee', if_exists='replace')
    #
    sql = """
        SELECT platform_code platform, site, toFloat32(net_profit2)/100 as net_profit2
        FROM tt_sale_center_listing_sync.tt_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    # conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_pc = conn_ck.ck_select_to_df(sql)
    #

    df = pd.merge(df_pf, df_pc, how='left', on=['platform', 'site'])

    # 差值替换
    df.loc[df['platform']=='CDISCOUNT', 'net_profit2'] = df_pc.loc[df_pc['platform']=='CDISCOUNT', 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='EB') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='EB') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WALMART') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WALMART') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='AMAZON') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='AMAZON') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='ALI') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='ALI') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='WISH') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='WISH') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='SHOPEE') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='SHOPEE') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform']=='LAZADA') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform']=='LAZADA') & (df_pc['site']=='other'), 'net_profit2'].iloc[0]

    #
    df[['platform_zero','net_profit2']] = df[['platform_zero','net_profit2']].round(4)
    #
    mismatched_rows = df[df['platform_zero'] != df['net_profit2']]
    print('差值更新的平台和国家：')
    mismatched_rows[['platform', 'site','platform_zero','net_profit2']].apply(lambda x: print(f"Platform: {x['platform']}, Site: {x['site']}, "
                                                   f"原差值：{x['platform_zero']},最新差值：{x['net_profit2']}"), axis=1)
    #
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df.drop('net_profit2', axis=1, inplace=True)

    # 更新
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_yibai_platform_fee', if_exists='replace')

tt_update_platform_fee()

## temu责任人归属更新
def temu_resp_update():
    """  temu责任人明细  """
    df_resp = pd.read_excel('F://Desktop//TEMU责任人明细.xlsx', dtype={'sku': str})
    #
    df_resp['date_id'] = time.strftime('%Y-%m-%d')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_resp, 'temu_responsible_name', if_exists='replace')

    # df_resp = df_resp.rename(columns={'主体账号':'责任人','sku':'new_sku'})
    # # df_resp['new_sku'] = df_resp['new_sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    # temu_listing_0 = pd.merge(temu_listing, df_resp[['new_sku','站点','责任人']], how='left', on=['new_sku','站点'])
    #
    df_resp_line = pd.read_excel('F://Desktop//TEMU责任人明细_类目.xlsx')
    df_resp_line['date_id'] = time.strftime('%Y-%m-%d')
    # df_resp_line = df_resp_line.rename(columns={'一级类目':'一级产品线','二级级类目':'二级产品线','主体账号':'责任账号'})
    conn.to_sql(df_resp_line, 'temu_responsible_line_name', if_exists='replace')

temu_resp_update()

## 分销定价异型附加费
def fenxiao_fee():
    """ 异型附加费 """
    df = pd.read_excel('F://Desktop//亿迈异性附加费.xlsx', dtype={'sku': str})

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'fenxiao_fee_temp', if_exists='replace')

fenxiao_fee()


## tt清仓sku状态表
def tt_sale_status():
    """ tt清仓sku """
    df = pd.read_excel('F://Desktop//TT清仓sku.xlsx', dtype={'sku': str})
    #
    df['sale_status'] = '清仓'
    df['start_time'] = '2025-11-30'
    df['end_time'] = np.nan

    print(df.info())
    print(df.head(5))
    #
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'tt_oversea_sale_status_temp', if_exists='append')
tt_sale_status()

## 销售状态白名单, temu补充清仓sku
def sales_status_temp():
    """ TEMU需手动修改销售状态的sku """
    df = pd.read_excel('F://Desktop//sales_status_temp.xlsx', dtype={'sku': str, 'end_time':str})

    print(df.head(5))

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'sales_status_temp', if_exists='replace')
sales_status_temp()

## temu运费补贴
def temu_freight_subsidy():
    """ temu运费补贴更新 """
    df = pd.read_excel('F://Desktop//运费补贴.xlsx', dtype={'sku': str})
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'temu_freight_subsidy', if_exists='replace')

temu_freight_subsidy()

##
sql = """
    SELECT *
    FROM over_sea.temu_freight_subsidy
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df = conn.read_sql(sql)

df.to_excel('F://Desktop//df_fre.xlsx', index=0)

## temu清仓sku备份
def temu_clear_sku():
    """
    每月提供给销售的temu清仓定价sku备份，用来匹配清仓订单提供给财务
    优先取靠前日期的记录
    """
    df = pd.read_excel('F://Desktop//TEMU清仓sku备份.xlsx', dtype={'sku': str})
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'temu_clear_sku_backup', if_exists='replace')

temu_clear_sku()

## 精铺转泛品sku库龄数据
def fine_sku_age():
    """ """
    df = pd.read_excel('F://Desktop//精铺转泛品sku库龄数据20251015.xlsx', dtype={'sku': str})
    col = ['0-30', '30-60', '60-90', '90-120', '120-150', '150-180', '180+']
    df[col] = df[col].fillna(0)
    df['age_30_plus'] = df['0-30'] + df['30-60'] + df['60-90'] +df['90-120'] +\
                        df['120-150'] + df['150-180'] + df['180+']
    df['age_60_plus'] = df['30-60'] + df['60-90'] +df['90-120'] +\
                        df['120-150'] + df['150-180'] + df['180+']
    df['age_90_plus'] = df['90-120'] + df['120-150'] + df['150-180'] + df['180+']
    df['age_120_plus'] = df['120-150'] + df['150-180'] + df['180+']
    df['age_150_plus'] = df['150-180'] + df['180+']
    df['age_180_plus'] = df['180+']
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'fine_sku_age', if_exists='replace')

fine_sku_age()

## 精铺子仓与TMS仓库映射关系
def jp_warehouse_name():
    """ 手动整理版 """
    df_warehouse = pd.read_excel('F://Desktop//精铺仓库与TMS仓库映射_手动整理.xlsx')
    print(df_warehouse.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_warehouse, 'jp_warehouse_name_temp', if_exists='replace')


jp_warehouse_name()

## 精铺转泛品sku, 精铺在线价信息
def get_fine_limit_price():
    """ 获取精铺产品当前售价。调价目标价不能高于当前售价 """
    sql = """
        SELECT erp_sku sku, sales_price
        FROM jp_saas_erp_base.yibai_amazon_overdue_inventory_detail_zt_his
        WHERE busi_date = (SELECT max(busi_date) FROM jp_saas_erp_base.yibai_amazon_overdue_inventory_detail_zt_his)
    """
    conn = connect_to_sql(database='', data_sys='新数仓')
    df = conn.read_sql(sql)
    df = df[(~df['sku'].isna()) & ~(df['sales_price'].isna())]
    df = df[~(df['sku']=='') & ~(df['sales_price']=='')]
    print(df.info())

    df = df.drop('sales_price', axis=1).join(
        df['sales_price'].str.split('<br/>', expand=True).stack().reset_index(level=1, drop=True).rename('country_price'))
    df[['country', 'fine_price']] = df['country_price'].str.split(' ', expand=True)
    df['fine_price'] = df['fine_price'].astype(float)
    df['country'] = df['country'].str.upper()
    df = df[df['fine_price'] > 0]
    df = df.sort_values(by='fine_price', ascending=True).drop_duplicates(subset=['sku','country'])
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'fine_sku_limit_price', if_exists='replace')
    # df.to_excel('F://Desktop//df_result.xlsx', index=0)

    return None
get_fine_limit_price()

## 海外仓运费下载站点
def oversea_fee_site():
    """ """
    df = pd.read_excel('F://Desktop//oversea_fee_site.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_fee_site', if_exists='replace')

oversea_fee_site()

## 海外仓头程单价更新
def oversea_toucheng():
    """ """
    df = pd.read_excel('F://Desktop//df_touc.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'yibai_toucheng_new', if_exists='replace')

oversea_toucheng()
## ebay不调价账号
def ebay_no_adjust_account():
    """ ebay海外仓不调价账号测试 """
    df = pd.read_excel('F://Desktop//ebay不调价账号0528.xlsx')
    # df.drop('account_id', axis=1, inplace=True)
    print(df.info())
    #
    # sql = """
    #     select distinct account_id, short_name
    #     from yibai_sale_center_system_sync.yibai_system_account
    #     where platform_code='EB'
    # """
    # conn_ck = pd_to_ck(database='yibai_sale_center_system_sync', data_sys='调价明细历史数据')
    # df_account = conn_ck.ck_select_to_df(sql)
    # df_account.to_excel('F://Desktop//df_ebay_account.xlsx', index=0)
    # df = pd.merge(df, df_account, how='left', on=['short_name'])
    # print(df.info())
    #
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_ebay_account_temp', if_exists='replace')

ebay_no_adjust_account()
##
def temp_temp():
    sql = """
        SELECT *
        FROM over_sea.oversea_ebay_account_temp
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_account_temp = conn.read_sql(sql)
    df_account_temp['is_white_account'] = 1
    print(df_account_temp.info())
temp_temp()

## 分销海外仓处理费标准
def fenxiao_fee():
    """ 分销定价中，海外仓处理费 """
    df = pd.read_excel('F://Desktop//【20250423】海外仓处理费的基础价格.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_fenxiao_fee', if_exists='replace')

fenxiao_fee()

## 国内仓滞销sku涨价测试
def dom_test():
    """ 存档备份 """
    df = pd.read_excel('F://Desktop//国内仓滞销品sku涨价测试0421.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'dom_price_test', if_exists='replace')
dom_test()

## fedex\ups尾程上浮
def fedex_sku():
    """ 20250822 临时
    清单内尾程上浮
    """
    df = pd.read_excel('F://Desktop//sku_fee_temp.xlsx',dtype={'sku':str})
    print(df.info())

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'fedex_sku_temp', if_exists='replace')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'fedex_sku_temp', if_exist='replace')

fedex_sku()

## fba转新品定价比参数
def fba_pricing_ratio():
    df = pd.read_excel('F://Desktop//fba发运建议参数表.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'fba_pricing_ratio_para', if_exists='replace')

fba_pricing_ratio()

##
sql = """
    SELECT *
    FROM yibai_oversea.temu_clear_sku_backup
    WHERE end_time = ''
"""
# conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
df = conn_ck.ck_select_to_df(sql)
df.to_excel('F://Desktop//df_temu_clear.xlsx', index=0)
## temu清仓定价表记录
def temu_clear_sku_backup():
    """ 每周给temu的清仓定价表数据备份 """

    df = pd.read_excel('F://Desktop//temu_clear_sku_backup.xlsx',dtype={'sku':str})
    col = ['date_id', 'start_time', 'end_time']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    print(df.info())

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temu_clear_sku_backup', if_exist='append')

temu_clear_sku_backup()


## temu申请不清仓定价表记录
def temu_no_clear_sku_backup():
    """ 每周给temu的清仓定价比数据备份 """

    df = pd.read_excel('F://Desktop//申请不清仓_拆分.xlsx',dtype={'sku':str})
    col = ['date_id', 'start_time', 'end_time']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    print(df.info())

    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'fedex_sku_temp', if_exists='replace')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temu_no_clear_sku', if_exist='replace')

temu_no_clear_sku_backup()

##
# tt lazada 3pf账号
def tt_lazada_3pf_account():
    """ """
    df_3pf = pd.read_excel('F://Ding_workspace//tt_lazada_海外仓账号-3PF补充20250729.xlsx')
    print(df_3pf.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_3pf, 'tt_lazada_3pf_account', if_exists='replace')

tt_lazada_3pf_account()

## 清仓任务目标表
def oversea_monthly_target():
    """ """
    df = pd.read_excel('F://Desktop//release_target.xlsx')
    print(df.info())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df, 'oversea_monthly_target', if_exists='replace')

oversea_monthly_target()

## 转泛品sku临时备注表
def temp_jp_to_fp_sku():

    df = pd.read_excel('F://Desktop//temp_oversea_jp_to_fp_sku.xlsx')
    # df['date_id'] = time.strftime('%Y-%m-%d')
    df['date_id'] = '2026-03-09'
    print(df.info())
    #
    # write_to_sql(df, 'profit_rate_section')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # conn.to_sql(df, 'oversea_temp_clear_sku', if_exists='append')

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temp_oversea_jp_to_fp_sku', if_exist='replace')

temp_jp_to_fp_sku()

## 需强制清仓sku临时表
def temp_oversea_clear_sku():

    df = pd.read_excel('F://Desktop//temp_oversea_clear_sku.xlsx')
    col = ['date_id', 'start_time', 'end_time']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    print(df.info())

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temp_oversea_clear_sku', if_exist='replace')

temp_oversea_clear_sku()

## 供应商sku计费重临时修改表
def temp_sup_sku_cw():
    """ """
    df = pd.read_excel('F://Desktop//供应商sku头程费用明细.xlsx')
    col = ['更新日期']
    for i in col:
        df[i] = df[i].dt.strftime('%Y-%m-%d')
    print(df.info())

    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temp_sup_sku_cw', if_exist='replace')

temp_sup_sku_cw()