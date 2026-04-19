"""
海外仓运费数据监控
"""

##
import pandas as pd
import numpy as np
import time,datetime
import warnings
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
warnings.filterwarnings("ignore")

##
def yunfei_data_check():
    """

    """
    n = 1
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""SELECT count() as data FROM yibai_oversea.oversea_transport_fee_daily otfd 
                    where date_id = toYYYYMMDD(today())"""
    df = conn_ck.ck_select_to_df(sql)
    today_data = df['data'][0]
    print(today_data)
    sql = f"""SELECT count() as data FROM  yibai_oversea.oversea_transport_fee_daily
                        where date_id = toYYYYMMDD(today()-{n})"""
    df = conn_ck.ck_select_to_df(sql)
    yesterday_data = df['data'][0]
    print(yesterday_data)
    a = abs((today_data - yesterday_data) / yesterday_data)
    b = '%.2f%%' % (a * 100)
    print(b)
    msg_str = f"""类型:海外仓运费拉取\n状态：数据量异常\n昨日运费总行数:{yesterday_data}\n今日运费总行数:{today_data}\n差异:{b}\n通知时间:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"""
    print(msg_str)

    # 运费数据监控
    date_today = time.strftime('%Y%m%d')
    # toYYYYMMDD(today())
    sql = f"""
        SELECT y.*
        FROM (
            SELECT 
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                toFloat64(firstCarrierCost) + toFloat64(dutyCost) as first_cost_y, 
                (toFloat64(overseasFee) + toFloat64(packFee) + toFloat64(overseaPackageFee)) as class2_y,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(antidumpFee)) as class3_y
            FROM yibai_oversea.oversea_transport_fee_daily
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily WHERE date_id <= toYYYYMMDD(today()-{n}))  
            and platform = 'AMAZON'
            -- and country = 'AU'
        ) y 
        settings max_memory_usage = 20000000000
    """
    df_fee = conn_ck.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    sql = """
        SELECT t.*
        FROM (
            SELECT 
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t, 
                toFloat64(firstCarrierCost) + toFloat64(dutyCost) as first_cost_t,
                (toFloat64(overseasFee) + toFloat64(packFee)+ toFloat64(overseaPackageFee)) as class2_t,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) +  toFloat64(antidumpFee)) as class3_t
            FROM yibai_oversea.oversea_transport_fee_daily
            WHERE date_id = toYYYYMMDD(today()) 
            and platform = 'AMAZON'
            -- and country = 'AU'
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = conn_ck.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    df_fee_all = pd.merge(df_fee, df_fee_2, how='left', on=['sku', 'warehouseName', 'shipName', 'country'])
    df_fee_all['尾程涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['头程涨降幅度'] = df_fee_all['first_cost_t'] / df_fee_all['first_cost_y'] - 1
    df_fee_all['仓内处理费涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 99)
    df_fee_all['附加费涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 99)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 99)
    df_fee_all['尾程涨价幅度分段'] = pd.cut(df_fee_all['尾程涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['头程涨价幅度分段'] = pd.cut(df_fee_all['头程涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['仓内处理费涨价幅度分段'] = pd.cut(df_fee_all['仓内处理费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['附加费涨价幅度分段'] = pd.cut(df_fee_all['附加费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.1)]) / len(df_fee_all)
    msg_str_2 = f"""类型:海外仓今日运费数据与最近运费数据对比：\n差异小于1%的数据量占比为：{'%.2f%%' % (c * 100)}\n"""
    print(msg_str_2)
    print(df_fee_all.groupby('头程涨价幅度分段')['sku'].count())
    # (df_fee_all['country'].isin(['US', 'DE', 'UK', 'FR', 'AU','CA'])) &
    # & (df_fee_all['头程涨降幅度'].abs() >= 0.01)
    df_fee_all = df_fee_all[(df_fee_all['country'].isin(['US', 'DE', 'UK', 'FR', 'AU','CA']))& (df_fee_all['总运费涨降幅度'].abs() >= 0.01)]
    print(df_fee_all.info())
    df_fee_all.to_excel('F://Desktop//df_fee_all.xlsx', index=0)
    return None

yunfei_data_check()

##
def temu_yunfei_data_check():
    """

    """
    n = 20
    p = 13
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""SELECT count() as data FROM yibai_oversea.oversea_transport_fee_daily_1 otfd 
                    where date_id = toYYYYMMDD(today()) - {p}"""
    df = conn_ck.ck_select_to_df(sql)
    today_data = df['data'][0]
    print(today_data)
    sql = f"""SELECT count() as data FROM  yibai_oversea.oversea_transport_fee_daily_1
                        where date_id = toYYYYMMDD(today()-{n})"""
    df = conn_ck.ck_select_to_df(sql)
    yesterday_data = df['data'][0]
    print(yesterday_data)
    a = abs((today_data - yesterday_data) / yesterday_data)
    b = '%.2f%%' % (a * 100)
    print(b)
    msg_str = f"""类型:海外仓运费拉取\n状态：数据量异常\n昨日运费总行数:{yesterday_data}\n今日运费总行数:{today_data}\n差异:{b}\n通知时间:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"""
    print(msg_str)

    # 运费数据监控
    date_today = time.strftime('%Y%m%d')
    # toYYYYMMDD(today())
    sql = f"""
        SELECT y.*
        FROM (
            SELECT 
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                toFloat64(firstCarrierCost) + toFloat64(dutyCost) as first_cost_y, 
                (toFloat64(overseasFee) + toFloat64(packFee) + toFloat64(overseaPackageFee)) as class2_y,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(antidumpFee)) as class3_y
            FROM yibai_oversea.oversea_transport_fee_daily_1
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily WHERE date_id <= toYYYYMMDD(today()-{n}))  
            -- and country = 'AU'
        ) y 
        settings max_memory_usage = 20000000000
    """
    df_fee = conn_ck.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    sql = f"""
        SELECT t.*
        FROM (
            SELECT 
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t, 
                toFloat64(firstCarrierCost) + toFloat64(dutyCost) as first_cost_t,
                (toFloat64(overseasFee) + toFloat64(packFee)+ toFloat64(overseaPackageFee)) as class2_t,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) +  toFloat64(antidumpFee)) as class3_t
            FROM yibai_oversea.oversea_transport_fee_daily_1
            WHERE date_id = toYYYYMMDD(today()) - {p}
            -- and country = 'AU'
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = conn_ck.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    df_fee_all = pd.merge(df_fee, df_fee_2, how='left', on=['sku', 'warehouseName', 'shipName', 'country'])
    df_fee_all['尾程涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['头程涨降幅度'] = df_fee_all['first_cost_t'] / df_fee_all['first_cost_y'] - 1
    df_fee_all['仓内处理费涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 99)
    df_fee_all['附加费涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 99)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 99)
    df_fee_all['尾程涨价幅度分段'] = pd.cut(df_fee_all['尾程涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['头程涨价幅度分段'] = pd.cut(df_fee_all['头程涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['仓内处理费涨价幅度分段'] = pd.cut(df_fee_all['仓内处理费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['附加费涨价幅度分段'] = pd.cut(df_fee_all['附加费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 100],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.1)]) / len(df_fee_all)
    msg_str_2 = f"""类型:海外仓今日运费数据与最近运费数据对比：\n差异小于1%的数据量占比为：{'%.2f%%' % (c * 100)}\n"""
    print(msg_str_2)
    print(df_fee_all.groupby('头程涨价幅度分段')['sku'].count())
    # (df_fee_all['country'].isin(['US', 'DE', 'UK', 'FR', 'AU','CA'])) &
    # & (df_fee_all['头程涨降幅度'].abs() >= 0.01)
    df_fee_all = df_fee_all[(df_fee_all['country'].isin(['US', 'DE', 'UK','GB', 'AU','CA']))]
    df_fee_all['warehouse'] = df_fee_all['country'].replace({'US':'美国仓', 'DE':'德国仓', 'GB':'英国仓','UK':'英国仓','AU':'澳洲仓','CA':'加拿大仓'})
    sql = """
        SELECT sku, warehouse, available_stock, `30days_sales`
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-09-22' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_rent = conn.read_sql(sql)
    df_fee_all = pd.merge(df_fee_all, df_rent, how='inner', on=['sku','warehouse'])
    print(df_fee_all.info())
    df_fee_all.to_excel('F://Desktop//df_fee_all_temu_2.xlsx', index=0)
    return None

temu_yunfei_data_check()
##
def yunfei_num_check():
    """

    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT platform, country, count() as data_t 
        FROM yibai_oversea.oversea_transport_fee_daily otfd 
        where date_id = toYYYYMMDD(today())
        GROUP BY platform, country
    """
    dft = conn_ck.ck_select_to_df(sql)

    sql = f"""        
        SELECT platform, country, count() as data_y
        FROM yibai_oversea.oversea_transport_fee_daily otfd 
        where date_id = toYYYYMMDD(today()-1)
        GROUP BY platform, country
    """
    dfy = conn_ck.ck_select_to_df(sql)

    df = pd.merge(dfy, dft, how='left', on=['platform', 'country'])

    df.to_excel('F://Desktop//df.xlsx', index=0)

yunfei_num_check()
##
def yunfei_num_check_2():
    """

    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT warehouseName, count() as data_t 
        FROM yibai_oversea.oversea_transport_fee_daily otfd 
        where date_id = toYYYYMMDD(today()) and country = 'UK' and platform = 'AMAZON'
        GROUP BY warehouseName
    """
    dft = conn_ck.ck_select_to_df(sql)

    sql = f"""        
        SELECT warehouseName, count() as data_y
        FROM yibai_oversea.oversea_transport_fee_daily otfd 
        where date_id = toYYYYMMDD(today()-1) and country = 'UK' and platform = 'AMAZON'
        GROUP BY warehouseName
    """
    dfy = conn_ck.ck_select_to_df(sql)

    df = pd.merge(dfy, dft, how='left', on=['warehouseName'])

    df.to_excel('F://Desktop//df_uk.xlsx', index=0)

yunfei_num_check_2()
## 通拓子仓运费，与同一仓库下易佰子仓运费的对比
def contrast_tt_yibai_fee():
    df_temp = pd.read_excel('F://Desktop//tt仓库对照表.xlsx')
    #
    warehouse_id = tuple(df_temp['warehouseId'].unique())
    warehouse_id_yibai = tuple(df_temp['warehouse_id_yibai'].unique())
    # 通拓子仓运费，与同一仓库下易佰子仓运费的对比
    sql = f"""
        SELECT sku, warehouseId, warehouseName, warehouse, shipName, totalCost_origin,shippingCost,firstCarrierCost,
        available_stock, country
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON' and warehouseId in {warehouse_id}
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_tt = conn.read_sql(sql)
    df_tt = df_tt.sort_values(by='totalCost_origin', ascending=True).drop_duplicates(subset=['sku','warehouseId','country'])
    sql = f"""
        SELECT sku, warehouseId warehouse_id_yibai, warehouseName, shipName, totalCost_origin totalCost_origin_y,
        shippingCost shippingCost_y, firstCarrierCost firstCarrierCost_y, country
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON' and warehouseId in {warehouse_id_yibai}
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_yibai = conn.read_sql(sql)
    df_yibai = df_yibai.sort_values(by='totalCost_origin_y', ascending=True).drop_duplicates(
        subset=['sku', 'warehouse_id_yibai', 'country'])
    df = pd.merge(df_tt, df_temp[['warehouseId','warehouse_id_yibai']], how='left', on=['warehouseId'])
    df = pd.merge(df, df_yibai, how='inner', on=['sku','warehouse_id_yibai', 'country'])

    df['差异率'] = round(df['shippingCost_y'] / df['shippingCost'] - 1, 4)
    df['差异率分段'] = pd.cut(df['差异率'],
                                      bins=[-2, -1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                      labels=['A:<-1', 'B:(-1,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                              'E:(-0.1, -0.05]', 'F:(-0.05, 0.05]', 'G:(0.05, 0.1]', 'H:(0.1, 0.2]',
                                              'I:(0.2, 0.5]', 'J:(0.5, 1]', 'K:(1, +]'])
    df['差异率分段'] = np.where(df['差异率'] < -1, 'A:<-1', df['差异率分段'])
    df['差异率分段'] = np.where(df['差异率'] > 1, 'K:(1, +]', df['差异率分段'])

    df.to_excel('F://Desktop//df_fee.xlsx', index=0)

contrast_tt_yibai_fee()
##

## IT头程与自算头程比较
def useful_fee_contrast():
    sql = """
        SELECT 
            distinct sku, warehouseName, warehouse, firstCarrierCost, dutyCost,
            firstCarrierCost as `IT计算总头程`, new_firstCarrierCost as `数据部自算头程`
        FROM over_sea.oversea_transport_fee_useful
        WHERE platform = 'AMAZON'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fee_cts = conn.read_sql(sql)
    #
    df_fee_cts[['IT计算总头程', '数据部自算头程']] = df_fee_cts[['IT计算总头程', '数据部自算头程']].astype(float)
    df_fee_cts['差异率'] = round(df_fee_cts['数据部自算头程'] / df_fee_cts['IT计算总头程'] - 1, 4)
    #
    df_fee_cts['差异率分段'] = pd.cut(df_fee_cts['差异率'],
                                              bins=[-2,-1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                              labels=['A:<-1', 'B:(-1,-0.5]','C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                                      'E:(-0.1, -0.05]','F:(-0.05, 0.05]','G:(0.05, 0.1]','H:(0.1, 0.2]',
                                                      'I:(0.2, 0.5]','J:(0.5, 1]','K:(1, +]'])
    df_fee_cts['差异率分段'] = np.where(df_fee_cts['差异率'] < -1, 'A:<-1', df_fee_cts['差异率分段'])
    df_fee_cts['差异率分段'] = np.where(df_fee_cts['差异率'] > 1, 'K:(1, +]', df_fee_cts['差异率分段'])

    df_fee_cts.to_excel('df_fee_cts.xlsx', index=0)

def get_au_fee():
    sql = """   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            round(toDecimal64(1.2,4)*(toDecimal64OrZero(a.shippingCost,4)+toDecimal64OrZero(a.extraSizeFee,4)) 
                    / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
            b.new_price as new_price, b.pur_weight_pack pur_weight_pack, 
            toFloat64(b.pur_length_pack )*toFloat64(b.pur_width_pack)*toFloat64(pur_height_pack) as weight_volume,
            multiIf(arrayExists(x->x.1='ALL', d.limit_arr), arrayFirst(x->x.1='ALL',d.limit_arr).3, 
                    arrayExists(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr), 
                        arrayFirst(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr).3,
                    arrayExists(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)), d.limit_arr),
                        arrayFirst(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)),d.limit_arr).3, 0) as limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            '澳洲仓' as warehouse,
            case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                 subString(a.sku, 4)
            when subString(a.sku, 1, 2) in ['DE', 'GB'] then 
                 subString(a.sku, 3)
            when subString(a.sku, -2) in ['DE', 'GB'] then 
                 subString(a.sku, 1, -2)
            else 
                 a.sku
            end as son_sku
            from 
            (
                select * except(date_id) 
                from yibai_oversea.oversea_transport_fee_daily
                where (not (platform in ['AMAZON', 'EB', 'WALMART']
                  and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                                             '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
                  and date_id =  toYYYYMMDD(today())) 
                  and warehouseId in (36, 352, 353, 769, 49, 50, 47, 58, 59)
                  and platform in ('AMAZON', 'EB')
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc 
                limit 1  by sku, platform, country, warehouseId
            ) a
            join
            (
             select 
                sku,toFloat64(new_price) new_price,pur_length_pack,pur_width_pack,pur_height_pack,pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and a.warehouseId = g.warehouseId)
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
            left join 
            (
               select 
                     sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
                 from 
                     yibai_oversea.sku_limit_price 
               group by sku
            ) d
            on (a.sku = d.sku)
        """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # 旧头程
    sql = """
        SELECT warehouse_id as warehouseId,price price_old
        FROM `yibai_toucheng` 
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_toucheng_old = conn.read_sql(sql)
    # 20240603取最新头程
    sql = """
        SELECT warehouse_id as warehouseId,price, weight_method, include_tax
        FROM `yibai_toucheng_new` 
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_toucheng = conn.read_sql(sql)
    df = df.merge(df_toucheng, on=['warehouseId'])
    df = df.merge(df_toucheng_old, on=['warehouseId'])
    df[['price','price_old']] = df[['price','price_old']].astype('float')
    df[['weight_volume', 'pur_weight_pack']] = df[['weight_volume', 'pur_weight_pack']].astype(float)
    df['计费重'] = np.where(df['weight_volume']/6000 > df['pur_weight_pack']/1000,
                           df['weight_volume']/6000, df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']==1,
                                          df['计费重'] * df['price'], df['weight_volume'] / 1000000 * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])

    df['new_firstCarrierCost_old'] = df['weight_volume'] / 1000000 * df['price_old']
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost','new_firstCarrierCost_old']
    df[col] = df[col].astype(float)

    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    df['totalCost_old'] = df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost_old']
    df['totalCost_new'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])

    df.drop(columns=['price', 'weight_volume', '计费重', 'pur_weight_pack'], inplace=True, axis=1)

    return df
##
from all_auto_task.scripts_ck_client import CkClient
def write_transport_fee_data():
    # sql = f"""
    #         select
    #         a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost,
    #         remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice,
    #         firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
    #         a.country as country,
    #         round(toDecimal64(if(a.platform=='EB',1.2, 1),4)*toDecimal64OrZero(a.shippingCost,4)
    #                 / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
    #         b.new_price as new_price,
    #         multiIf(arrayExists(x->x.1='ALL', d.limit_arr), arrayFirst(x->x.1='ALL',d.limit_arr).3,
    #                 arrayExists(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr),
    #                     arrayFirst(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr).3,
    #                 arrayExists(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)), d.limit_arr),
    #                     arrayFirst(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)),d.limit_arr).3, 0) as limit_price_rmb,
    #         null as zero_percent,
    #         null as five_percent,
    #         available_stock,
    #         dictGet('dic_warehouse', 'warehouse', toUInt64(a.warehouseId)) as warehouse,
    #         case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then
    #    			 subString(a.sku, 4)
    #         when subString(a.sku, 1, 2) in ['DE', 'GB'] then
    #         	 subString(a.sku, 3)
    #         when subString(a.sku, -2) in ['DE', 'GB'] then
    #         	 subString(a.sku, 1, -2)
    #         else
    #              a.sku
    #         end as son_sku
    #         from
    #         (
    #             select * except(date_id)
    #             from oversea_transport_fee_daily
    #             where not (platform in ['AMAZON', 'EB', 'WALMART']
    #               and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
    #                                          '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
    #               and date_id =  toYYYYMMDD(today())
    #             order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
    #             by sku, platform, country, warehouseId
    #         ) a
    #         join
    #         (
    #          select sku, if(product_status == '7', last_price, new_price) as new_price
    #          from yb_datacenter.yb_product
    #          where if(product_status == '7', last_price, new_price) > 0
    #         ) b
    #         on (a.sku = b.sku)
    #         join
    #         (
    #             select sku, warehouse_id as warehouseId, available_stock
    #               from yb_datacenter.v_oversea_stock
    #         ) g
    #         on (a.sku = g.sku and a.warehouseId = g.warehouseId)
    #         left join
    #         (
    #           select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee
    #           from yibai_wish.yibai_platform_fee
    #         ) c
    #         on (a.country = c.country and a.platform = c.platform)
    #         left join
    #         (
    #            select
    #                  sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
    #              from
    #                  yibai_oversea.sku_limit_price
    #            group by sku
    #         ) d
    #         on (a.sku = d.sku)
    # """

    # 2023-04-13 海外仓lowest_price计算的系数调整为不区分平台，全部设置为1.2，原来只有EB平台是1.2。
    sql = """   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            round(toDecimal64(1.2,4)*(toDecimal64OrZero(a.shippingCost,4)+toDecimal64OrZero(a.extraSizeFee,4)) 
                    / (1 - pay_fee - paypal_fee - refound_fee - extra_fee - vat_fee), 4) as lowest_price,
            b.new_price as new_price, b.pur_weight_pack pur_weight_pack, 
            toFloat64(b.pur_length_pack )*toFloat64(b.pur_width_pack)*toFloat64(pur_height_pack) as weight_volume,
            multiIf(arrayExists(x->x.1='ALL', d.limit_arr), arrayFirst(x->x.1='ALL',d.limit_arr).3, 
                    arrayExists(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr), 
                        arrayFirst(x->and(x.1 == a.platform, empty(x.2)),d.limit_arr).3,
                    arrayExists(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)), d.limit_arr),
                        arrayFirst(x->and(x.1 == a.platform, x.2 == a.country, notEmpty(x.2)),d.limit_arr).3, 0) as limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            0 warehouse,
            case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
	   			 subString(a.sku, 4)
	        when subString(a.sku, 1, 2) in ['DE', 'GB'] then 
	        	 subString(a.sku, 3)
	        when subString(a.sku, -2) in ['DE', 'GB'] then 
	        	 subString(a.sku, 1, -2)
	        else 
	             a.sku
	        end as son_sku
            from 
            (
                select * except(date_id) 
                from yibai_oversea.oversea_transport_fee_daily
                where not (platform in ['AMAZON', 'EB', 'WALMART']
                  and shipName in ['谷东-UPS_一票多箱_Multi', '谷东_FEDEX_OVERNIGHT[Fedex_标准次日达]', '谷东_FEDEX_2DAY',
                                             '谷西_FEDEX_2DAY', '谷西_FEDEX_OVERNIGHT[Fedex_标准次日达]'])
                  and date_id =  toYYYYMMDD(today())
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
                by sku, platform, country, warehouseId
            ) a
            join
            (
             select 
                sku,toFloat64(new_price) new_price,pur_length_pack,pur_width_pack,pur_height_pack,pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and a.warehouseId = g.warehouseId)
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
            left join 
            (
               select 
                     sku, groupArray((platform,country,limit_price_rmb)) as limit_arr
                 from 
                     yibai_oversea.sku_limit_price 
               group by sku
            ) d
            on (a.sku = d.sku)
            LIMIT 10000
        """
    # ck_client = CkClient(user='datax', password='datax#07231226', host='172.16.51.140', port='9001',
    #                      db_name='yibai_oversea')
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    # 20240603取最新头程
    sql = """
        SELECT warehouse_id as warehouseId,price, weight_method, include_tax
        FROM `yibai_toucheng_new` 
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_toucheng = conn.read_sql(sql)
    df = df.merge(df_toucheng, on=['warehouseId'])
    df['price'] = df['price'].astype('float')
    df[['weight_volume', 'pur_weight_pack']] = df[['weight_volume', 'pur_weight_pack']].astype(float)
    df['计费重'] = np.where(df['weight_volume']/6000 > df['pur_weight_pack']/1000,
                           df['weight_volume']/6000, df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']==1,
                                          df['计费重'] * df['price'], df['weight_volume'] / 1000000 * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    df['totalCost'] = df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    df.drop(columns=['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax'], inplace=True, axis=1)
    print(df.info())
    # conn.to_sql(df, table='oversea_transport_fee_useful', if_exists='append')
    print('mysql数据存储完成.')
    # # 20231228 同步一份数据到ck
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='yibai_oversea')
    # ck_client.ck_clear_table('oversea_transport_fee_useful')
    # n = 0
    # while n < 4:
    #     sql = """
    #         SELECT count(1)
    #         FROM yibai_oversea.oversea_transport_fee_useful
    #     """
    #     df_num = ck_client.ck_select_to_df(sql)
    #     if df_num.iloc[0,0] == 0:
    #         for i in df['platform'].unique():
    #             sql = f"""
    #                 SELECT *
    #                 FROM over_sea.oversea_transport_fee_useful
    #                 WHERE platform = '{i}'
    #             """
    #             conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #             df_useful = conn.read_sql(sql)
    #             ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                                  db_name='yibai_oversea')
    #             if len(df_useful) > 1000000:
    #                 ck_client.write_to_ck_json_type('oversea_transport_fee_useful', df_useful.iloc[0:800000, :])
    #                 print('数据1同步完成')
    #                 ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                                      db_name='yibai_oversea')
    #                 ck_client.write_to_ck_json_type('oversea_transport_fee_useful', df_useful.iloc[800000:, :])
    #             else:
    #                 ck_client.write_to_ck_json_type('oversea_transport_fee_useful', df_useful)
    #             print(f"{i}站点数据同步完成")
    #             break
    #     else:
    #         time.sleep(10)
    #         n += 1
    return df
##
useful_fee_contrast()
##
df_fee = yunfei_data_check()
##
df_fee[df_fee['总运费涨降幅度'].abs() > 0.01].to_excel('df_fee.xlsx', index=0)
##
df_new_fee = get_au_fee()

##
df_new_fee['差异率'] = round(df_new_fee['totalCost_new'] / df_new_fee['totalCost'] - 1, 4)
df_new_fee['差异率分段'] = pd.cut(df_new_fee['差异率'],
                                  bins=[-2, -1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                  labels=['A:<-1', 'B:(-1,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                          'E:(-0.1, -0.05]', 'F:(-0.05, 0.05]', 'G:(0.05, 0.1]', 'H:(0.1, 0.2]',
                                          'I:(0.2, 0.5]', 'J:(0.5, 1]', 'K:(1, +]'])
df_new_fee['差异率分段'] = np.where(df_new_fee['差异率'] < -1, 'A:<-1', df_new_fee['差异率分段'])
df_new_fee['差异率分段'] = np.where(df_new_fee['差异率'] > 1, 'K:(1, +]', df_new_fee['差异率分段'])
##
df_new_fee['差异率_old'] = round(df_new_fee['totalCost_new'] / df_new_fee['totalCost_old'] - 1, 4)
df_new_fee['差异率分段_old'] = pd.cut(df_new_fee['差异率_old'],
                                  bins=[-2, -1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                  labels=['A:<-1', 'B:(-1,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                          'E:(-0.1, -0.05]', 'F:(-0.05, 0.05]', 'G:(0.05, 0.1]', 'H:(0.1, 0.2]',
                                          'I:(0.2, 0.5]', 'J:(0.5, 1]', 'K:(1, +]'])
df_new_fee['差异率分段_old'] = np.where(df_new_fee['差异率_old'] < -1, 'A:<-1', df_new_fee['差异率分段_old'])
df_new_fee['差异率分段_old'] = np.where(df_new_fee['差异率_old'] > 1, 'K:(1, +]', df_new_fee['差异率分段_old'])
##
df_new_fee['差异率_2'] = round(df_new_fee['totalCost_old'] / df_new_fee['totalCost'] - 1, 4)
df_new_fee['差异率分段_2'] = pd.cut(df_new_fee['差异率_2'],
                                  bins=[-2, -1, -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.5, 1, 10],
                                  labels=['A:<-1', 'B:(-1,-0.5]', 'C:(-0.5,-0.2]', 'D:(-0.2,-0.1]',
                                          'E:(-0.1, -0.05]', 'F:(-0.05, 0.05]', 'G:(0.05, 0.1]', 'H:(0.1, 0.2]',
                                          'I:(0.2, 0.5]', 'J:(0.5, 1]', 'K:(1, +]'])
df_new_fee['差异率分段_2'] = np.where(df_new_fee['差异率_2'] < -1, 'A:<-1', df_new_fee['差异率分段_2'])
df_new_fee['差异率分段_2'] = np.where(df_new_fee['差异率_2'] > 1, 'K:(1, +]', df_new_fee['差异率分段_2'])
##
df_new_fee.groupby('差异率分段_2')['sku'].count()
##
df_new_fee.to_excel('df_new_fee.xlsx', index=0)
##  新头程单价表
sql = """
    SELECT *
    FROM over_sea.yibai_toucheng
"""
conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
df_price = conn.read_sql(sql)
##
wlist = ['谷仓英国仓', '谷仓捷克仓', '万邑通澳洲墨尔本仓', '谷仓美国东仓']
df_price['weight_method'] = np.where(df_price['warehouse_name'].isin(wlist), 1, 0)
df_price['weight_method'] = np.where(df_price['warehouse_name'].str.contains('墨西哥'), 1, 0)
df_price['include_tax'] = np.where(df_price['warehouse_name'].isin(['谷仓法国仓']), 1, 0)
##
B_column = df_price['B']
# 删除原来的B列
df_price = df_price.drop('B', axis=1)
df_price.insert(df_price.columns.get_loc('warehouse_name') + 1, 'warehouse_id', B_column)
##
conn.to_sql(df_price, 'yibai_toucheng_new', if_exists='replace')
##
if __name__ == '__main__':
    # # IT计算初始运费比较
    # df_fee_all = yunfei_data_check()
    # df_fee_all[df_fee_all['总运费涨降幅度'].abs() > 0.01].to_excel('df_fee_all.xlsx', index=0)
    #
    # a = df_fee_all.groupby(['country', '总运费涨降幅度分段'])['sku'].count().reset_index()
    # a.to_excel('a.xlsx', index=0)

    # IT头程与数据部头程比较
    useful_fee_contrast()