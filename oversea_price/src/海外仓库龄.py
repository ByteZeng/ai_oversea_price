import yaml
import warnings
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime, timedelta
from excel_them import theme_title
from func import read_password, time_wrapper

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# 云仓库龄
@time_wrapper
def cloud_stock():
    sql_age = """SELECT
                    ysd.id `整体序列号`,
                    yw.name `虚拟仓_云仓`,
                    ifnull(yw2.name,
                    yw.name) `实体仓`,
                    ysd.cargo_owner_id `商户id`,
                    ysd.sku sku,
                    receipt_quantity - delivery_quantity `库存数量`,
                    date_diff('day',
                    date(receipt_time),
                    today()) `库龄`,
                    '云仓' `数据来源`
                FROM
                    yb_stock_center_sync.yb_stockage_detail ysd
                JOIN yb_datacenter.yb_warehouse yw ON
                    ysd.warehouse_id = yw.id
                left JOIN yb_datacenter.yb_warehouse yw2 on
                    yw.real_warehouse_id = yw2.id
                WHERE
                    yw.`type` IN ('overseas', 'third')
                    AND receipt_quantity - delivery_quantity>0
                    AND yw.name NOT LIKE '%%精铺%%';"""
    data_tra = read_password(15, sql_age)
    data_tra['库存数量'] = data_tra['库存数量'].apply(lambda x: x * [1, ])
    data_tra = data_tra.explode('库存数量')
    data_tra['库存序列号'] = data_tra.groupby(['sku', '实体仓'])['库龄'].rank(ascending=True, method='first')
    data_tra['库存序列号-商户id'] = data_tra.groupby(['sku', '实体仓', '商户id'])['库龄'].rank(ascending=True,
                                                                                               method='first')
    data_tra['标识-云仓'] = 'cloud_age'
    return data_tra


# 技术库龄表
@time_wrapper
def stock_age_table():
    sql_stock_age = f"""with age as (
                            select
                                id,
                                sku,
                                case
                                    when warehouse_code != '' then warehouse_code
                                    when order_warehouse_code != '' then order_warehouse_code
                                    else oversea_warehosue_code
                                end as code,
                                oversea_type,
                                warehouse_stock,
                                inventory_age + 3 as inventory_age
                            from
                                hwc_sync.yb_oversea_sku_age_hwc
                            where
                                sku != ''
                                and `date` = '{(datetime.today() - timedelta(3)).strftime('%Y-%m-%d')}')
                                                        select
                                age.sku sku,
                                if(yw.name = ''
                                    and age.oversea_type = 'WYT'
                                    and age.code = 'DEBR2',
                                    '万邑通德国仓-DE Warehouse',
                                    yw.name) `虚拟仓_库龄表`,
                                    yw2.name `实体仓`,
                                age.warehouse_stock `库存数量`,
                                age.inventory_age `库龄`,
                                '库龄表' `数据来源`
                            from
                                age
                            left join yb_datacenter.yb_warehouse yw on
                                age.code = yw.code
                            left join yb_datacenter.yb_warehouse yw2 on
                                yw.real_warehouse_id = yw2.id
                            where
                                yw.`type` in ('third', 'overseas');"""
    data_stock_age = read_password(15, sql_stock_age)
    data_stock_age['库存数量'] = data_stock_age['库存数量'].apply(lambda x: x * [1, ])
    data_stock_age = data_stock_age.explode('库存数量')
    data_stock_age['库存序列号'] = data_stock_age.groupby(['sku', '实体仓'])['库龄'].rank(ascending=True,
                                                                                          method='first')
    data_stock_age['标识-库龄表'] = 'age_table'
    return data_stock_age


# 墨西哥库龄
@time_wrapper
def stock_age_mx():
    sql_age_mx = """SELECT
                        yw.name `虚拟仓_墨西哥`,
                        yw2.name `实体仓`,
                        b.sku sku,
                        cargo_owner_id `商户id`,
                        instock_stock-out_stock as `库存数量`,
                        DATEDIFF( now() , storage_age_date) as `库龄`,
                        'SAAS系统' `数据来源`
                    FROM
                            yibai_saas_wms.yibai_stock_age_detail a
                    LEFT JOIN yb_stock_center.yb_oversea_sku_mapping b
                                                ON
                        a.sku = b.oversea_sku
                    left join yb_stock_center.yb_warehouse yw on
                        a.w = yw.code
                    left JOIN yb_stock_center.yb_warehouse yw2 on
                        yw.real_warehouse_id = yw2.id
                    where
                        instock_stock-out_stock>0
                        and yw.name like '%%YM墨西哥%%'
                        and b.sku != '';"""
    data_mx = read_password(5, sql_age_mx)
    data_mx['库存数量'] = data_mx['库存数量'].apply(lambda x: x * [1, ])
    data_mx = data_mx.explode('库存数量')
    data_mx['库存序列号-商户id'] = data_mx.groupby(['sku', '实体仓', '商户id'])['库龄'].rank(ascending=True,
                                                                                             method='first')
    data_mx['标识-墨西哥'] = 'mx-table'
    return data_mx


# 非墨西哥库龄表分析
# 云仓记录多余库龄表的部分，以云仓记录为主;库龄表的数据赋予商户id和虚拟仓，没有的其商户id默认8，虚拟仓默认实体仓
@time_wrapper
def alsy_abnormal():
    data_abnormal = pd.merge(data_age_cloud[~data_age_cloud['实体仓'].str.contains('墨西哥')],
                             data_age_table[['sku', '实体仓', '库存序列号', '标识-库龄表']], how='left',
                             on=['sku', '实体仓', '库存序列号'])
    data_abnormal = data_abnormal[data_abnormal['标识-库龄表'].isna()]
    data_share = pd.merge(data_age_table,
                          data_age_cloud[['sku', '实体仓', '虚拟仓_云仓', '库存序列号', '标识-云仓', '商户id']],
                          how='left', on=['sku', '实体仓', '库存序列号'])
    data_share.loc[~data_share['标识-云仓'].isna(), '虚拟仓_库龄表'] = data_share.loc[
        ~data_share['标识-云仓'].isna(), '虚拟仓_云仓']
    data_share['商户id'] = data_share['商户id'].fillna(8)
    return data_abnormal, data_share


# 墨西哥库龄表分析
# 云仓有库龄表没有的,以云仓为主;库龄表的数据赋予虚拟仓，没有的其虚拟仓默认实体仓
@time_wrapper
def alsy_mx():
    data_abnormal = pd.merge(data_age_cloud[data_age_cloud['实体仓'] == 'YM墨西哥仓'],
                             data_age_mx[['sku', '实体仓', '商户id', '库存序列号-商户id', '标识-墨西哥']], how='left',
                             on=['sku', '实体仓', '商户id', '库存序列号-商户id'])
    data_abnormal = data_abnormal[data_abnormal['标识-墨西哥'].isna()]
    data_share = pd.merge(data_age_mx,
                          data_age_cloud[['sku', '实体仓', '商户id', '虚拟仓_云仓', '库存序列号-商户id', '标识-云仓']],
                          how='left', on=['sku', '实体仓', '商户id', '库存序列号-商户id'])
    data_share.loc[~data_share['标识-云仓'].isna(), '虚拟仓_库龄表'] = data_share.loc[
        ~data_share['标识-云仓'].isna(), '虚拟仓_云仓']
    return data_abnormal, data_share


# 获取最新采购成本
@time_wrapper
def sku_price():
    sql_price = "select distinct sku,new_price from yb_datacenter.yb_product;"
    data_price = read_password(18, sql_price)
    data_price['sku'] = data_price['sku'].astype(str)
    return data_price


# sku归属
@time_wrapper
def dev_source(df):
    sql_source = """
                    SELECT
                        sku ,
                        case
                         when develop_source in(14,15,22) then '通拓'
                         when develop_source in (46,51) then '海兔'
                         else '易佰' end "开发来源"
                    from yibai_prod_base_sync.yibai_prod_sku;"""
    data_source = read_password(15, sql_source)
    data_source['sku'] = data_source['sku'].astype(str)
    df = pd.merge(df, data_source, how='left', on='sku')
    df['开发来源'] = df['开发来源'].fillna('易佰')
    return df


if __name__ == '__main__':
    with open(r'D:\pycharm\项目\oversea\transcode.yaml', 'r', encoding='utf-8') as stream:
        data_site = yaml.safe_load(stream)  # 只调用一次，并将结果存储在 data 变量中
    sites_to_names = data_site.get(
        'sites_to_names', None)  # 使用 get 方法，避免 KeyError
    sites_list = data_site.get('sites_to_names', [])  # 假设 sites_to_names 是一个列表
    sites_region = {site['code']: site['region'] for site in sites_list}
    data_age_table = stock_age_table()
    data_age_cloud = cloud_stock()
    data_age_mx = stock_age_mx()
    data_mx_add, data_mx_share = alsy_mx()
    data_age_add, data_age_share = alsy_abnormal()
    data_age_share.rename(columns={'虚拟仓_库龄表': '虚拟仓'}, inplace=True)
    data_age_add.rename(columns={'虚拟仓_云仓': '虚拟仓'}, inplace=True)
    data_mx_add.rename(columns={'虚拟仓_云仓': '虚拟仓'}, inplace=True)
    data_mx_share.rename(columns={'虚拟仓_墨西哥': '虚拟仓'}, inplace=True)
    data_age = pd.concat(
        [data_age_add[['sku', '实体仓', '虚拟仓', '商户id', '库存数量', '库龄', '数据来源']],
         data_age_share[['sku', '实体仓', '虚拟仓', '商户id', '库存数量', '库龄', '数据来源']],
         data_mx_add[['sku', '实体仓', '虚拟仓', '商户id', '库存数量', '库龄', '数据来源']],
         data_mx_share[['sku', '实体仓', '虚拟仓', '商户id', '库存数量', '库龄', '数据来源']]], ignore_index=True)
    # 数据聚合一下
    data_age = pd.pivot_table(data_age, index=['sku', '实体仓', '虚拟仓', '商户id', '库龄', '数据来源'],
                              values='库存数量', aggfunc='sum').reset_index()
    price = sku_price()
    data_age['sku'] = data_age['sku'].astype(str)
    data_age = pd.merge(data_age, price, how='left', on='sku')
    data_age['new_price'] = data_age['new_price'].fillna(25)
    data_age['库存金额'] = data_age['库存数量'].astype(float) * data_age['new_price'].astype(float)
    bins = [0, 15, 30, 45, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 365, float('inf')]
    labels = ['a.0~15天', 'b.15~30天', 'c.30~45天', 'd.45~60天', 'e.60~90天', 'f.90~120天', 'g.120~150天',
              'h.150~180天', 'i.180~210天', 'j.210~240天', 'k.240~270天', 'l.270~300天', 'm.300~330天', 'n.330~365天',
              'o.365天以上']
    data_age['库龄分段'] = pd.cut(data_age['库龄'], bins=bins, labels=labels, include_lowest=True, right=False)
    data_age['库龄分段'] = data_age['库龄分段'].astype(str)
    data_age = dev_source(data_age)
    out_table = pd.pivot_table(data_age[data_age['商户id'] == 8], index=['实体仓', '虚拟仓'], columns='库龄分段',
                               values='库存金额',
                               aggfunc='sum', margins_name='总计', margins=True, fill_value=0).reset_index()
    data_age.to_csv(f'C:\\Users\\Administrator\\Desktop\\海外汇总库龄明细表{datetime.today().strftime('%Y%m%d')}.csv',
                    index=False)
    excel_path = f'C:\\Users\\Administrator\\Desktop\\YB海外汇总库龄表{datetime.today().strftime('%Y%m%d')}.xlsx'
    out_table.to_excel(excel_path, startrow=1, index=False)
    wb = load_workbook(excel_path)
    theme_title(out_table, wb)
    wb.save(excel_path)
