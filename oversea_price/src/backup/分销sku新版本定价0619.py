
##
import time
import warnings
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    make_path, get_path
import datetime
import pandas as pd
import numpy as np
from utils import utils
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.price_adjust_web_service.daingjia_public import chicun_zhongliang
from all_auto_task.oversea_price_adjust_2023 import get_platform_fee
from all_auto_task.sku_to_anywhere_price import get_trip_fee_oversea2
warnings.filterwarnings('ignore')
##
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
sql = """
    SELECT
        a.sku, a.title AS 标题, a.type AS 开发类型, a.product_status AS 产品状态, linest as 产品线,
        a.warehouse as 大仓, a.available_stock AS 大仓总库存, a.best_warehouse_name AS 最优发货子仓,
        age_180_plus as 超180天库龄库存, overage_level as 超库龄等级, product_package_size AS 包装尺寸, b.shipCode,
        b.shipName, a.new_price AS 成本, a.gross AS 毛重, totalCost as 总运费, b.shippingCost AS 尾程,
        b.firstCarrierCost AS 头程, dutyCost as 关税, packTypeFee as 包装类型费（异型附加费）, remoteExtraFee as 偏远附加费,
        extraSizeFee as 超尺寸附加费, overseasFee as 海外仓处理费, packFee as 复合打包费, overseaPackageFee 包装费,
        a.new_price*0.013 as `国内采购运费`, new_firstCarrierCost 自算头程,
        b.country AS country, IF(d.sale_status IS NULL ,'正常',d.sale_status) as `销售状态`
    FROM (
        SELECT 
            sku, title, type, product_status, linest, warehouse, available_stock, best_warehouse_name, age_180_plus,product_package_size,
            new_price, gross,best_warehouse_id, overage_level
        FROM over_sea.dwm_sku_temp_info
        WHERE
            date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
            and (available_stock > 0 or warehouse in ('俄罗斯仓'))
    ) a
    LEFT JOIN over_sea.oversea_transport_fee_useful b 
    ON a.sku=b.sku AND a.best_warehouse_id=b.warehouseId AND (b.platform in ('AMAZON','OZON','DMSozon'))
    LEFT JOIN over_sea.oversea_sale_status d 
    ON a.sku=d.SKU AND a.warehouse=d.warehouse AND d.end_time IS NULL
"""
df_sku = conn.read_sql(sql)
##
df_fee = get_platform_fee('AMAZON')
df_fee.drop('refound_fee', axis=1, inplace=True)
##
df_sku_info = pd.merge(df_sku, df_fee, how='left', on=['country'])
##
# 定价计算
df_sku_info['成本'] = df_sku_info['成本'].astype(float)
df_sku_info['YB净利润额'] = df_sku_info['platform_must_percent'] * (df_sku_info['成本']+df_sku_info['总运费'])/(
        1-df_sku_info['ppve']-df_sku_info['platform_zero']-df_sku_info['platform_must_percent'])
##
c1 = (df_sku_info['销售状态'] == '正常') & (df_sku_info['warehouse'].isin(['俄罗斯仓','乌拉圭仓']))
c2 = (df_sku_info['销售状态'] == '正利润加快动销') & (df_sku_info['warehouse'].isin(['俄罗斯仓','乌拉圭仓']))
##
df_sku.to_excel('df_sku.xlsx', index=0)
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
    sql2 = """
        SELECT
            A.*,
            round((成本+国内采购运费+头程+关税+尾程+海外仓处理费+超尺寸附加费+偏远附加费)*`供货系数`,2) as '供货价格',
            '是' AS  '是否开放分销',
            '' as '不开放分销原因'
        FROM (
            SELECT A.*,
                case
                when `大仓` = '俄罗斯仓' then 1.18
                when `大仓` in ('美国仓','英国仓','德国仓','法国仓','澳洲仓') and `超180天库龄库存` > 0 then 1
                when `大仓` in ('美国仓','英国仓','德国仓','法国仓','澳洲仓') and `超180天库龄库存` = 0 then 1.5
                when  `大仓` not in ('美国仓','英国仓','德国仓','法国仓','澳洲仓','俄罗斯仓') and 销售状态 in ('正常','涨价缩销')  then 1.25
                when `大仓` not in ('美国仓','英国仓','德国仓','法国仓','澳洲仓','俄罗斯仓') and 销售状态 in ('正利润加快动销') then 1.15
                when `大仓` not in ('美国仓','英国仓','德国仓','法国仓','澳洲仓','俄罗斯仓') and 销售状态 in ('负利润加快动销','清仓')  then 0.8
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
                    overage_level as 超库龄等级,
                    product_package_size AS 包装尺寸,
                    B.shipCode,
                    B.shipName,
                    A.new_price AS 成本,
                    A.gross AS 毛重,
                    totalCost as 除成本外的各项支出,
                    B.shippingCost AS 尾程,
                    B.firstCarrierCost AS 头程,
                    dutyCost as 关税,
                    packTypeFee as 包装类型费（异型附加费）,
                    remoteExtraFee as 偏远附加费,
                    extraSizeFee as 超尺寸附加费,
                    overseasFee as 海外仓处理费,
                    packFee as 复合打包费,
                    A.new_price*0.013 as `国内采购运费`,
                    B.country AS 收货站点,
                    IF(D.sale_status IS NULL ,'正常',D.sale_status) as '销售状态',
                    f.pay_fee, f.vat_fee, f.extra_fee, f.platform_zero, f.platform_must_percent
                FROM (
                    SELECT
                        sku, title, type, product_status, linest, warehouse, available_stock, best_warehouse_name, age_180_plus,product_package_size,
                        new_price, gross,best_warehouse_id, overage_level
                    FROM dwm_sku_temp_info
                    WHERE 
                        date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
                        and (available_stock > 0 or warehouse in ('俄罗斯仓'))
                        -- and (available_stock = 0 and on_way_stock > 0)
                        -- and title not like '%%跃星辉%%'
                    ) A
                LEFT JOIN oversea_transport_fee_useful B ON A.SKU=B.SKU AND A.best_warehouse_id=B.warehouseId AND (B.platform='AMAZON' or B.platform='OZON')
                LEFT JOIN oversea_sale_status D ON A.SKU=D.SKU AND A.warehouse=D.warehouse AND  end_time IS NULL
                LEFT JOIN yibai_platform_fee f ON A.country = f.country AND f.platform = 'AMAZON'
            )A
        )A
    """
    # and available_stock > 0
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    return None



##
if __name__ == '__main__':
    main()
    # get_ru_fees()
