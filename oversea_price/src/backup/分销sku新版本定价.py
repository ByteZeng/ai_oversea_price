##
import time
import warnings
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    make_path, get_path
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from utils import utils
import src.fetch_data as fd
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.price_adjust_web_service.daingjia_public import chicun_zhongliang
from all_auto_task.sku_to_anywhere_price import get_trip_fee_oversea2
warnings.filterwarnings('ignore')
##
def sql_to_pd2(database, host, port, username, password, sql):
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(username, password, host, port, database, 'utf8'))
    conn = engine.connect()  # 创建连接
    df = pd.read_sql(sql, conn)
    conn.close()
    return df
##

def main():
    """
    易佰海外仓分销对接
    jayden(刘楠)
    海外仓报价
    """
    utils.program_name = '分销sku新版本定价'
    make_path()
    cur_path, root_path = get_path()
    date_today = datetime.date.today()
    date_yesterday = datetime.date.today() - datetime.timedelta(days=1)
    # sql2 = """
    #     SELECT A.*,
    # -- case when 供货系数<>1.25 then  round((成本+国内采购运费+头程+关税)*供货系数+尾程+海外仓处理费+超尺寸附加费+偏远附加费,2)
    # -- else round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*1.25,2)
    # -- end as '供货价格',
    # case
    # when 标题 like '%跃星辉%' then round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*0.5,2)
    # when 标题 not like '%跃星辉%' and 供货系数<>1.25 then round((成本+国内采购运费+头程+关税)*供货系数+尾程+海外仓处理费+超尺寸附加费+偏远附加费,2)
    # else round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*1.25,2)
    # end as '供货价格',
    #  '是' AS  '是否开放分销',
    #  '' as '不开放分销原因'
    # -- (成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*0.5/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero-platform_must_percent) as '折扣成本定价'
    # FROM (
    # SELECT A.*,
    # case
    # when 标题 like '%跃星辉%' then 0.5
    # when 标题 not like '%跃星辉%' and 销售状态 in ('正常','涨价缩销') and 最优发货子仓 not like '%万邑通%' then 1.25
    # when 标题 not like '%跃星辉%' and 销售状态 in ('正利润加快动销')  and 最优发货子仓 not like '%万邑通%' then 1.0
    # when 标题 not like '%跃星辉%' and 销售状态 in ('负利润加快动销','清仓') and 最优发货子仓 not like '%万邑通%'  then 0.8
    # when 标题 not like '%跃星辉%' and 销售状态 in ('正常','涨价缩销','正利润加快动销') and 最优发货子仓 like '%万邑通%' then 1.0
    # when 标题 not like '%跃星辉%' and 销售状态 in ('负利润加快动销','清仓') and 最优发货子仓 like '%万邑通%' and 超180天库龄库存=0 then 0.8
    # when 标题 not like '%跃星辉%' and 销售状态 in ('负利润加快动销','清仓') and 最优发货子仓 like '%万邑通%' and 超180天库龄库存>0 then 0.6
    # else null end  as 供货系数
    # FROM
    # (
    # SELECT
    # A.SKU,
    # A.title AS 标题,
    # A.type AS 开发类型,
    # A.product_status AS 产品状态,
    # linest as 产品线,
    # A.warehouse as 大仓,
    # A.available_stock AS 大仓总库存,
    # A.best_warehouse AS 最优发货子仓,
    # inv_age_180_to_210_days+inv_age_210_plus_days as 超180天库龄库存,
    # product_package_size AS 包装尺寸,
    # B.shipCode,
    # B.shipName,
    # A.new_price AS 成本,
    # A.gross AS 毛重,
    # totalCost as 除成本外的各项支出,
    # B.shippingCost AS 尾程,
    # B.firstCarrierCost AS 头程,
    # dutyCost as 关税,
    # packTypeFee as 包装类型费（异型附加费）,
    # remoteExtraFee as 偏远附加费,
    # extraSizeFee as 超尺寸附加费,
    # overseasFee as 海外仓处理费,
    # packFee as 复合打包费,
    # A.new_price*0.013 as '国内采购运费',
    # B.country AS 收货站点,
    # C.pay_fee, C.paypal_fee, C.vat_fee, C.extra_fee, C.refound_fee, C.platform_zero, C.platform_must_percent,
    # CASE WHEN D.sale_status in ('负利润加快动销','清仓') THEN '打折款' ELSE '正常款' END AS '分销款式',
    # IF(D.sale_status IS NULL ,'正常',D.sale_status) as '销售状态'
    # FROM over_sea_age_new_date A
    # INNER JOIN oversea_transport_fee_useful B ON A.SKU=B.SKU AND A.warehouse_id=B.warehouseId AND B.platform='AMAZON'
    # INNER join yibai_platform_fee C on B.platform=C.platform AND B.country=C.site
    # LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
    # WHERE
    # -- DATE=DATE_FORMAT(now(),'%Y-%m-%d')
    # DATE = date_sub(curdate(), interval 0 day)
    # and A.available_stock>0
    # )A
    # )A
    # """

    # 新逻辑切换
    # 20240129 万邑通修改供货系数
    sql2 = """
        SELECT
            A.*,
            case
                when 标题 like '%%跃星辉%%' then round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*0.5,2)
                when 标题 not like '%%跃星辉%%' and 供货系数<>1.25 then round((成本+国内采购运费+头程+关税)*供货系数+尾程+海外仓处理费+超尺寸附加费+偏远附加费,2)
                else round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*1.25,2)
            end as '供货价格',
             '是' AS  '是否开放分销',
             '' as '不开放分销原因'
        FROM (
            SELECT A.*,
                case
                WHEN 大仓 = '俄罗斯仓' then 1.18
                when 标题 like '%%跃星辉%%' then 0.5
                when 标题 not like '%%跃星辉%%' and 销售状态 in ('正常','涨价缩销')  then 1.25
                when 标题 not like '%%跃星辉%%' and 销售状态 in ('正利润加快动销') then 1.15
                when 标题 not like '%%跃星辉%%' and 销售状态 in ('负利润加快动销','清仓')  then 0.8
                else null end  as 供货系数
            FROM (
                SELECT
                    A.sku,
                    A.title AS 标题,
                    A.type AS 开发类型,
                    A.product_status AS 产品状态,
                    linest as 产品线,
                    A.warehouse as 大仓,
                    A.available_stock AS 大仓总库存,
                    A.best_warehouse_name AS 最优发货子仓,
                    age_180_plus as 超180天库龄库存,
                    product_package_size AS 包装尺寸,
                    B.shipCode,
                    B.shipName,
                    A.new_price AS 成本,
                    A.gross AS 毛重,
                    totalCost as 除成本外的各项支出,
                    B.shippingCost AS 尾程,
                    B.new_firstCarrierCost AS 头程,
                    dutyCost as 关税,
                    packTypeFee as 包装类型费（异型附加费）,
                    remoteExtraFee as 偏远附加费,
                    extraSizeFee as 超尺寸附加费,
                    overseasFee as 海外仓处理费,
                    packFee as 复合打包费,
                    A.new_price*0.013 as '国内采购运费',
                    B.country AS 收货站点,
                    CASE WHEN D.sale_status in ('负利润加快动销','清仓') THEN '打折款' ELSE '正常款' END AS '分销款式',
                    IF(D.sale_status IS NULL ,'正常',D.sale_status) as '销售状态'
                FROM (
                    SELECT
                        sku, title, type, product_status, linest, warehouse, available_stock, best_warehouse_name, age_180_plus,product_package_size,
                        new_price, gross,best_warehouse_id
                    FROM dwm_sku_temp_info
                    WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) and (available_stock > 0 or warehouse in ('俄罗斯仓'))
                    ) A
                LEFT JOIN oversea_transport_fee_useful B ON A.SKU=B.SKU AND A.best_warehouse_id=B.warehouseId AND (B.platform='AMAZON' or B.platform='OZON')
                LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
            )A
        )A
    """
    # and available_stock > 0
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # con_212 = get_mysql_con(host='121.37.239.131', user='panxx', password='panxx#Mrp01', database='over_sea')
    # con_212 = get_mysql_con(host='121.37.248.212', user='zhenghongwei', password='Zhenghongwei#mrp01', database='over_sea')
    df = sql_to_pd2(database="over_sea", host="121.37.239.131", port="11950", username="wangj", password="Wangj#01", sql=sql2)
    # df = pd.read_sql(sql2, con_212)
    # df = conn.read_sql(sql2)
    # 20231206：俄罗斯仓运费数据取指定渠道，另外商品供货系数由1.18改为1

    # 匹配目的国的中文
    client = get_ck_client()
    sql_account = f"""
                SELECT
                    site1 as `目的国`,
                    site3 as `收货站点`
                FROM domestic_warehouse_clear.site_table
        """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df_country = read_sql_ck(sql_account, client)
    df_country = conn_ck.ck_select_to_df(sql_account)
    df_country.loc[df_country['目的国'] == '英国', '收货站点'] = 'UK'
    df = df.merge(df_country, on='收货站点', how='left')
    df['目的国'] = np.where(df['收货站点']=='RU', '俄罗斯', df['目的国'])
    col_name = df.columns.tolist()
    col_name.insert(col_name.index('最优发货子仓'), '目的国') # 在 '最优发货子仓' 列前面插入 '目的国'
    col_name.pop()
    df = df.reindex(columns=col_name)

    df = df.drop_duplicates()
    # save_df(df, '分销sku新版本定价_增加目的国', file_type='xlsx')
    # 重新匹配毛重数据
    sku_list = tuple(df['sku'].unique())
    step = 10000
    df_gross = pd.DataFrame()
    for i in range(int(len(sku_list)/step)+1):
        sku_list_temp = sku_list[i*step:(i+1)*step]
        sql = f"""
        SELECT 
            distinct sku,
            case 
                when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
                else toFloat64(weight_out_storage) 
            end as `重量`
        FROM yibai_prod_base_sync.yibai_prod_sku
        WHERE sku in {sku_list_temp}
        """
        # df_gross = read_sql_ck(sql, client)
        conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据2')
        df_gross_temp = conn_ck.ck_select_to_df(sql)
        df_gross = pd.concat([df_gross_temp, df_gross])
    df = pd.merge(df, df_gross, how='left', on=['sku'])
    print('done!')
    df_fenxiao = df.copy()

    # 俄罗斯仓运费数据替换
    # df_ru_fee = pd.read_excel('F:\yibai-price-strategy\data\DMSOzon-RU-运费.xlsx')
    date_num = pd.to_numeric(time.strftime('%Y%m%d'))
    sql = f"""
        SELECT sku, warehouseName as `仓库名称`, shipCode `渠道编码`, shipName `渠道名称`, totalCost `新总运费`, shippingCost `新尾程`,
        firstCarrierCost `新头程运费`, dutyCost `新关税`, overseasFee `新海外仓处理费`
        FROM yibai_oversea.oversea_transport_fee_daily
        WHERE date_id = {date_num} and platform = 'DMSOzon' and shipName = 'LD-OZON俄罗斯尾程派送（海外仓-驿站）计价' 
    """
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_ru_fee = conn_ck.ck_select_to_df(sql)
    # df_ru_fee = fd.fetch_ck(sql, 78, 'yibai_oversea')
    ##
    df_ru_fee['大仓'] = '俄罗斯仓'
    df_fenxiao_2 = pd.merge(df_fenxiao, df_ru_fee, how='left', on=['sku', '大仓'])
    c1 = df_fenxiao_2['大仓'] == '俄罗斯仓'
    df_fenxiao_2['shipCode'] = np.where(c1, df_fenxiao_2['渠道编码'], df_fenxiao_2['shipCode'])
    df_fenxiao_2['shipName'] = np.where(c1, df_fenxiao_2['渠道名称'], df_fenxiao_2['shipName'])
    df_fenxiao_2['除成本外的各项支出'] = np.where(c1, df_fenxiao_2['新总运费'], df_fenxiao_2['除成本外的各项支出'])
    df_fenxiao_2['尾程'] = np.where(c1, df_fenxiao_2['新尾程'], df_fenxiao_2['尾程'])
    df_fenxiao_2['头程'] = np.where(c1, df_fenxiao_2['新头程运费'], df_fenxiao_2['头程'])
    df_fenxiao_2['关税'] = np.where(c1, df_fenxiao_2['新关税'], df_fenxiao_2['关税'])
    df_fenxiao_2['海外仓处理费'] = np.where(c1, df_fenxiao_2['新海外仓处理费'], df_fenxiao_2['海外仓处理费'])
    df_fenxiao_2['国内采购运费'] = np.where(c1, 0, df_fenxiao_2['国内采购运费'])
    # (成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)
    df_fenxiao_2['供货系数'] = np.where(c1, 1.18, df_fenxiao_2['供货系数'])
    col_list = ['成本', '国内采购运费', '头程', '关税', '尾程', '海外仓处理费']
    df_fenxiao_2[col_list] = df_fenxiao_2[col_list].astype(float)
    c2 = (df_fenxiao_2['成本'] + df_fenxiao_2['国内采购运费'] + df_fenxiao_2['头程'] + df_fenxiao_2['关税'] + df_fenxiao_2['尾程'] + df_fenxiao_2['海外仓处理费']) * \
         df_fenxiao_2['供货系数']
    df_fenxiao_2['供货价格'] = np.where(c1, c2, df_fenxiao_2['供货价格'])
    df_fenxiao_2.drop(['渠道编码', '渠道名称','新总运费','新尾程','新头程运费','新关税','新海外仓处理费'], axis=1, inplace=True)
    ##
    # df_fenxiao_2.to_excel('df_fenxiao_2.xlsx', index=0)
    save_df(df_fenxiao_2, '分销sku新版本定价_增加目的国', file_type='xlsx')

    return None
##
# date_num = pd.to_numeric(time.strftime('%Y%m%d'))
# sql = f"""
#     SELECT sku, warehouseName as `仓库名称`, shipCode `渠道编码`, shipName `渠道名称`, totalCost `新总运费`, shippingCost `新尾程`,
#     firstCarrierCost `新头程运费`, dutyCost `新关税`, overseasFee `新海外仓处理费`
#     FROM yibai_oversea.oversea_transport_fee_daily
#     WHERE date_id = {date_num} and platform = 'DMSOzon' and shipName = 'LD-OZON俄罗斯尾程派送（海外仓-驿站）计价'
# """
# df_ru_fee = fd.fetch_ck(sql, 78 ,'yibai_oversea')
##
# df_sku = pd.read_excel('F://Desktop//需导运费.xlsx')
# ##
# df_sku['sku'] = df_sku['sku'].astype(str)
# sku_list = tuple(df_sku['sku'].unique())
# sql = f"""
#     SELECT *
#     FROM over_sea.oversea_transport_fee_useful
#     WHERE sku in {sku_list}
# """
# conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
# df_fee = conn.read_sql(sql)
# ##
# df_fee.to_excel('df_fee.xlsx', index=0)
##
def get_ru_fees():
    # sql = """
    #     SELECT sku, warehouse, warehouse_id, warehouse_name, available_stock
    #     FROM yb_datacenter.v_oversea_stock
    #     WHERE available_stock > 0 and warehouse_id = 1019
    # """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df_sku = conn_ck.ck_select_to_df(sql)
    ##
    df_sku = pd.read_excel('F://Desktop//需导运费.xlsx')
    # df_sku = pd.DataFrame({})
    df_sku['sku'] = df_sku['sku'].astype(str)
    df_sku['数量'] = 1
    df_sku = chicun_zhongliang(df_sku, 1, conn_ck)
    ##
    ship_type = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    warehouse_id = '1019'

    group1 = df_sku[['sku', '数量', '成本', '重量', '长', '宽', '高']]
    yunfei_jisuan = get_trip_fee_oversea2('DMSOzon', 'RU', warehouse_id, ship_type)
    group2 = yunfei_jisuan.batch_df_order(group1)
    group2 = pd.merge(group1, group2, how='left', on=['sku'])
    group2 = group2[
        ['sku',  '成本', '重量', '长', '宽', '高', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipName', 'shipCode',
         'totalCost', 'shippingCost', 'firstCarrierCost','overseasFee','dutyCost']]
    group2 = group2.sort_values(['totalCost'], ascending=True)
    group2.to_excel('group2.xlsx', index=0)


##
def get_us_fees():
    # sql = """
    #     SELECT sku, warehouse, warehouse_id, warehouse_name, available_stock
    #     FROM yb_datacenter.v_oversea_stock
    #     WHERE available_stock > 0 and warehouse_id = 1019
    # """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    # df_sku = conn_ck.ck_select_to_df(sql)
    # dic = {'sku':['DS04067-04','XD00790','2611220046312'],'warehouse_id':[50,50,50]}
    # df_sku = pd.DataFrame(dic)
    df_sku = pd.read_excel('F:\Desktop\sku0315.xlsx')
    df_sku = df_sku[['sku']]
    #
    df_sku['数量'] = 1
    df_sku = chicun_zhongliang(df_sku, 1, conn_ck)
    #
    ship_type = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    warehouse_id = '50'

    group1 = df_sku[['sku', '数量', '成本', '重量', '长', '宽', '高']]
    yunfei_jisuan = get_trip_fee_oversea2('EB', 'US', warehouse_id, ship_type)
    group2 = yunfei_jisuan.batch_df_order(group1)
    group2 = pd.merge(group1, group2, how='left', on=['sku'])
    group2 = group2[
        ['sku', '成本', '重量', '长', '宽', '高', 'shipCountry', 'platform', 'warehouseId', 'warehouseName',
         'shipName', 'shipCode', 'totalCost', 'shippingCost', 'firstCarrierCost','overseasFee','dutyCost']]
    group2 = group2.sort_values(['totalCost'], ascending=True)
    group2.to_excel('group.xlsx', index=0)

##
if __name__ == '__main__':
    main()
    # get_ru_fees()
