"""
供应商货盘相关需求:
1、建议供货价数据

"""

import numpy as np
import pandas as pd
import time, datetime
import tqdm
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
from warnings import filterwarnings
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang
from all_auto_task.oversea_add_logic import get_sup_yb_sku
filterwarnings('ignore')


def get_sup_dwm():
    """ 获取供应商sku信息表 """
    sql = """
        SELECT *
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and (platform = 'TEMU' or platform = '' or platform is Null)
        and country in ('US', 'DE', 'UK')
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    return df

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, develop_source, b.category_path as `产品线路线`, spu,
            CASE 
                when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                else toFloat64(product_cost) 
            END as `new_price`
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
    df_line = df_line.drop_duplicates(subset='sku')

    # 取开发来源
    sql = """
        SELECT distinct id as develop_source, develop_source_name
        FROM yibai_prod_base_sync.yibai_prod_develop_source
    """
    df_source = conn_ck.ck_select_to_df(sql)
    df_line = pd.merge(df_line, df_source, how='left', on=['develop_source'])
    # c1 = df_line['develop_source'].isin([14,15,22])
    # c2 = df_line['产品名称'].str.contains('通拓')
    # df_line['is_tt_sku'] = np.where(c1 | c2, 1, 0)
    df = pd.merge(df, df_line[['sku','develop_source_name','new_price']], how='left', on=['sku'])

    return df

def get_toucheng():
    """ 获取头程单价 """
    # 取头程单价
    sql = """
         SELECT DISTINCT warehouse, `计费方式` weight_method,
         `头程计泡系数` dim_weight, `是否包税` include_tax, `普货单价` price
         FROM yibai_oversea.oversea_fees_parameter_new
         WHERE `是否主要渠道` = 1 and `物流类型` = '慢海'
     """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tc = conn_ck.ck_select_to_df(sql)

    df_tc = df_tc.sort_values(by='price', ascending=False).drop_duplicates(subset=['warehouse'])

    return df_tc

def get_sup_useful_fee():
    """ 供应商sku运费数据 """
    # 取sku及子仓库存（用于取运费子仓）
    # df_sku = get_sup_sku()
    sql = """
        SELECT distinct YM_sku, warehouse_name best_warehouse_name, available_stock
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
        and warehouse_id is not Null
        -- and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    sku_list = tuple(df_sku['YM_sku'].unique())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id,  
        case when firstCarrierCost <= 0.01 then totalCost else totalCost_origin end as total_cost, 
        (totalCost_origin - firstCarrierCost - dutyCost) shippingCost,
        case when firstCarrierCost <= 0.01 then new_firstCarrierCost else firstCarrierCost end as firstCarrierCost, 
        dutyCost, shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH')  and country in ('US','DE','UK')
    UNION ALL
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id,  
        case when firstCarrierCost <= 0.01 then totalCost else totalCost_origin end as total_cost, 
        (totalCost_origin - firstCarrierCost - dutyCost) shippingCost,
        case when firstCarrierCost <= 0.01 then new_firstCarrierCost else firstCarrierCost end as firstCarrierCost, 
        dutyCost, shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_temu
    WHERE country in ('US','DE','UK')
    """
    df_transport_fee = conn.read_sql(sql)
    sql = f"""
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id, totalCost as total_cost, 
        (totalCost - firstCarrierCost - dutyCost) shippingCost,
        firstCarrierCost, dutyCost,
        shipName as ship_name,0 lowest_price, platform, shipCountry country, warehouse
    FROM yibai_oversea.oversea_transport_fee_supplier a
    LEFT JOIN (
        SELECT  warehouse_name, b.name warehouse
        FROM yibai_logistics_tms_sync.yibai_warehouse a
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
        WHERE warehouse_type IN (2,3,8)
    ) b ON a.warehouseName = b.warehouse_name
    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_supplier)
    """
    df_intf_fee = conn_ck.ck_select_to_df(sql)

    df_transport_fee['source'] = 1
    df_intf_fee['source'] = 2
    df_transport_fee = pd.concat([df_transport_fee, df_intf_fee])
    df_transport_fee = df_transport_fee.sort_values(by='source', ascending=True).\
        drop_duplicates(subset=['YM_sku','best_warehouse_name','platform','country'])
    df_transport_fee.drop('source', axis=1, inplace=True)

    df = pd.merge(df_transport_fee, df_sku, how='left', on=['YM_sku', 'best_warehouse_name'])
    df['available_stock'] = df['available_stock'].fillna(0).astype(int)
    # 取子仓下最便宜渠道
    df = df[~df['best_warehouse_name'].str.contains('精品')]
    df_1 = df[df['available_stock'] > 0]
    df_1 = df_1.sort_values(by='shippingCost', ascending=True)
    df_1 = df_1.drop_duplicates(subset=['YM_sku','warehouse','platform','country'], keep='first')
    df_2 = df[df['available_stock'] <= 0]
    df_2 = df_2.sort_values(by='shippingCost', ascending=True)
    df_2 = df_2.drop_duplicates(subset=['YM_sku','warehouse','platform','country'], keep='first')
    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['YM_sku','warehouse','platform','country'], keep='first')

    # df.to_excel('F://Desktop//df_sup_fee.xlsx', index=0)

    return df

def replace_sku_cw(df):
    """ 按亿迈指定头程计费重，替换头程费用 """
    sql = """
        SELECT YM_sku, warehouse, `亿迈指定计费重`, `亿迈指定头程方式`, `最终头程`
        FROM yibai_oversea.temp_sup_sku_cw
        WHERE `亿迈指定计费重` > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tc = conn_ck.ck_select_to_df(sql)
    print(df_tc.info())

    df = pd.merge(df, df_tc, how='left', on=['YM_sku','warehouse'])

    c1 = (~df['亿迈指定头程方式'].isna())
    df['计费重'] = np.where(c1, df['亿迈指定计费重'], df['计费重'])
    df['自算头程'] = np.where(c1, df['最终头程'], df['自算头程'])
    df['自算头程'] = df['自算头程'].astype(float)

    df.drop(['亿迈指定计费重','亿迈指定头程方式','最终头程'], axis=1, inplace=True)

    return df




def get_dwm_sku(df):
    """ 获取海外仓库存信息 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    sql = """
        SELECT sku, warehouse, available_stock, overage_level
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_sku_temp_info)
    """
    df_dwm = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT sku, warehouse, sale_status
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    df_status = conn.read_sql(sql)

    # df_fee = get_transport_fee()
    df_fee = get_sup_useful_fee()
    df_fee = df_fee.rename(columns={'YM_sku':'sku', 'shippingCost':'ship_fee', 'total_cost':'totalCost_origin'})
    col = ['sku','warehouse','best_warehouse_name','totalCost_origin','ship_fee','firstCarrierCost',
           'platform', 'country']
    df_fee = df_fee[col]
    # df_fee = df_fee[df_fee['platform']=='AMAZON']

    df = pd.merge(df, df_dwm, how='left', on=['sku','warehouse'])
    df = pd.merge(df, df_status, how='left', on=['sku','warehouse'])
    df = pd.merge(df, df_fee, how='left', on=['sku','warehouse','platform', 'country'])

    # 尺寸重量
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)

    # 头程单价
    df_tc = get_toucheng()
    df = pd.merge(df, df_tc[['warehouse','price']], how='left', on=['warehouse'])
    df['计费重'] = (df['重量']/1000).combine(df['长'] * df['宽'] * df['高'] / 6000, max)
    df['自算头程'] = df['长'] * df['宽'] * df['高'] * df['price'] / 1000000
    col = ['数量', '重量', '成本', '长', '宽', '高', '重量来源', 'price']
    col = ['数量','成本','重量来源']
    df.drop(col, axis=1, inplace=True)
    df = df.rename(columns={'price':'头程单价'})

    # 头程计费重替换
    df = replace_sku_cw(df)

    # 亚马逊最终定价
    sql = """
        SELECT sku, warehouse, is_supplier, is_supplier_price
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform = 'AMAZON' and country in ('US','DE')
    """
    # df_dtl = conn_ck.ck_select_to_df(sql)
    # df = pd.merge(df, df_dtl, how='left', on=['sku','warehouse'])

    return df


def get_days_sales():
    """ 获取日销信息 """
    # yibai
    sql = """
        SELECT sku, warehouse, `3days_sales`, `7days_sales`, `15days_sales`, 
        `30days_sales`, `60days_sales`, `90days_sales`
        FROM over_sea.dwd_sku_sales
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_sales)
        and org != 'YM'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sales = conn.read_sql(sql)

    # 聚合YM_sku + YB_sku销量数据
    df_sku = get_sup_yb_sku()

    sku_mapping = dict(zip(df_sku['YB_sku'], df_sku['YM_sku']))
    df_sales['sku'] = df_sales['sku'].map(sku_mapping).fillna(df_sales['sku'])

    # 按大仓聚合
    df = df_sales.groupby(['sku', 'warehouse']).agg({'3days_sales': 'sum', '7days_sales': 'sum', '15days_sales': 'sum',
                                                     '30days_sales': 'sum', '60days_sales': 'sum','90days_sales': 'sum'}).reset_index()
    df['day_sales'] = 0.7 * df['7days_sales'] / 7 + 0.2 * df['15days_sales'] / 15 + 0.1 * df['30days_sales'] / 30

    return df

def cut_bins(df):
    """ """
    df['建议供货价差异分段'] = pd.cut(
        df['建议供货价/实际供货价-1'], bins=[-50, -0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3, 1000],
        labels=['A:<-0.3', 'B:(-0.3,-0.2]', 'C:(-0.2,-0.1]', 'D:(-0.1,0]','E:(0, 0.1]',
                'F:(0.1,0.2]', 'G:(0.2,0.3]', 'H:>0.3'])

    df['折扣1分段'] = pd.cut(
        df['按调价逻辑_建议供货价折扣'], bins=[-1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 10],
        labels=['A:<0.2', 'B:(0.2,0.3]', 'C:(0.3,0.4]', 'D:(0.4,0.5]','E:(0.5, 0.6]',
                'F:(0.6,0.7]', 'G:(0.7,0.8]', 'H:(0.8,0.9]', 'I:>0.9'])

    df['折扣2分段'] = pd.cut(
        df['按调价逻辑_建议供货价/实际供货价'], bins=[-1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 10],
        labels=['A:<0.2', 'B:(0.2,0.3]', 'C:(0.3,0.4]', 'D:(0.4,0.5]','E:(0.5, 0.6]',
                'F:(0.6,0.7]', 'G:(0.7,0.8]', 'H:(0.8,0.9]', 'I:>0.9'])

    col = ['建议供货价差异分段','折扣1分段','折扣2分段']
    df[col] = df[col].astype(str)
    return df

def get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

# 海外仓建议供货价与实际供货价对比
def cal_sup_price_new():
    """
    1、取供应商sku, 及其【实际供货价】
    2、用YB的成本及运费，计算目标利润率定价，并将5%的利润 + 成本 +头程 做为【建议供货价】
    3、对比两者的差异
    """
    # 1、取供应商sku
    df = get_sup_dwm()
    # df = df[(df['platform']=='TEMU') & (df['YB_sku'] != '') & (df['country'].isin(['DE','US','UK']))]
    df = df[(df['platform'] == 'TEMU') & (df['country'].isin(['DE', 'US', 'UK']))]
    # 补充临时sku
    # dic = {'YM_sku':['1413200166712','GY04623-01','1011220266911','3112240034611',
    #                  '1011240363811','1618210438411','1618240042511','1618240190311','I4112','I4403-2'],
    #        'warehouse':['美国仓','美国仓','德国仓','德国仓','德国仓','德国仓','德国仓','德国仓','美国仓','美国仓'],
    #        'country':['US','US','DE','DE','DE','DE','DE','DE','US','US'],
    #        'YB_sku':['1413200166712','GY04623-01','1011220266911','3112240034611',
    #                  '1011240363811','1618210438411','1618240042511','1618240190311','I4112','I4403-2']}
    # df_temp_sku = pd.DataFrame(dic)
    # df_temp_sku['platform'] = 'TEMU'
    # df = pd.concat([df, df_temp_sku])
    # 20260317 没有映射关系的sku，也需要加入计算
    df['YB_sku'] = np.where(df['YB_sku']=='', df['YM_sku'], df['YB_sku'])
    col = ['YM_sku', 'warehouse', 'warehouse_price', 'cargo_owner_id', 'available_stock', 'overage_level', 'total_cost',
           'shippingCost','firstCarrierCost','ppve','platform_zero','platform_must_percent',
           'sup_price', 'platform', 'country','YB_sku']
    df = df[col]
    df['platform_zero'] = 0.13
    df['platform_must_percent'] = 0.05

    # df.to_excel('F://Desktop//df_sup_temp.xlsx', index=0)
    # 获取yb差值，及让利点数
    df_yb = get_platform_fee()
    yb_fee = df_yb[df_yb['platform']=='TEMU']['platform_zero'].iloc[0]
    ym_fee = df[df['platform'] == 'TEMU']['platform_zero'].iloc[0]
    diff = yb_fee - ym_fee
    # 2、取YBsku信息
    df = df.rename(columns={'YB_sku':'sku'})
    df = get_line(df)

    # 运费信息
    df = get_dwm_sku(df)
    col = ['total_cost','shippingCost','firstCarrierCost_x','ppve','platform_zero','available_stock_x',
           'platform_must_percent','warehouse_price']
    df[col] = df[col].astype(float)
    df[['available_stock_y','available_stock_x']] = df[['available_stock_y','available_stock_x']].fillna(0).astype(int)
    print(df.info())
    # 费用替换
    # df['运费来源'] = np.where(df['available_stock_y']>0, '调价运费', '接口运费')
    # 临时补充YBsku测算数据，运费只有YB数据
    df['total_cost'] = np.where(df['total_cost'].isna(), df['totalCost_origin'], df['total_cost'])
    df['shippingCost'] = np.where(df['shippingCost'].isna(), df['ship_fee'], df['shippingCost'])
    df['运费来源'] = '接口运费'
    df['总运费'] = np.where(df['available_stock_y']>0, df['totalCost_origin']-df['firstCarrierCost_y']+df['自算头程'],
                            df['total_cost']-df['firstCarrierCost_x']+df['自算头程'])
    df['尾程'] = np.where(df['available_stock_y'] > 0, df['ship_fee'], df['shippingCost'])
    col = ['total_cost','shippingCost','firstCarrierCost_x','sup_price','develop_source_name',
           'best_warehouse_name','totalCost_origin','ship_fee','firstCarrierCost_y']
    df.drop(col, axis=1, inplace=True)

    # 3、日销统计（YM_sku + YB_sku)的销量汇总
    df_sales = get_days_sales()
    df_sales = df_sales.rename(columns={'sku':'YM_sku'})

    df = pd.merge(df, df_sales[['YM_sku','warehouse','day_sales']], how='left', on=['YM_sku', 'warehouse'])
    df['day_sales'] = df['day_sales'].fillna(0).astype(float)
    # 4、计算
    df['预计可售天数'] = ((df['available_stock_x'] + df['available_stock_y'])/df['day_sales']).replace(np.inf, 9999)
    df['ppve'] = 0.02
    df['目标净利率定价'] = (df['new_price']+df['总运费'])/(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])
    df['建议供货价'] = df['new_price'] + df['自算头程'] + df['目标净利率定价']*0.07
    df['建议供货价/实际供货价-1'] = df['建议供货价'] / df['warehouse_price'] - 1
    df['建议供货价是否小于实际供货价'] = np.where(df['建议供货价']<df['warehouse_price'], 1, 0)
    dic = {60:0.02, 90:-0.05, 120:-0.1, 150:-0.2, 180:-0.4, 270:-0.5, 360:-0.8}
    df['调价逻辑目标净利率'] = df['overage_level_x'].map(dic).fillna(0.05)
    df['调价逻辑目标净利率定价'] = (df['建议供货价']+df['尾程'])/(1-df['ppve']-df['platform_zero']-df['调价逻辑目标净利率'])
    df['按调价逻辑降价后的建议供货价'] = df['调价逻辑目标净利率定价']*(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])-df['尾程']
    df['按调价逻辑_建议供货价折扣'] = df['按调价逻辑降价后的建议供货价'] / df['建议供货价']
    df['按调价逻辑_建议供货价/实际供货价'] = df['按调价逻辑降价后的建议供货价'] / df['warehouse_price']

    df = cut_bins(df)

    dic = {'warehouse_price':'实际供货价', 'cargo_owner_id':'商户ID', 'available_stock_x':'YMsku可用库存',
           'overage_level_x':'YMsku超库龄等级', 'ppve':'佣金+vat+库损汇损', 'platform_zero':'YM差值',
           'platform_must_percent':'YM目标净利率', 'platform':'平台', 'country':'国家','sku':'YB_sku',
           'new_price':'YBsku成本', 'available_stock_y':'YBsku可用库存', 'overage_level_y':'YBsku超库龄等级',
           'sale_status':'销售状态','day_sales':'日销(YBsku+YMsku)'}
    df = df.rename(columns=dic)
    df['date_id'] = time.strftime('%Y-%m-%d')

    # df.to_excel('F://Desktop//df_sup_info_all.xlsx', index=0)
    print(df.info())
    #
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'oversea_supplier_info_all', if_exist='replace')

# 测算专用
def temp_cal_sup_price():
    """
    1、取供应商sku, 及其【实际供货价】
    2、用YB的成本及运费，计算目标利润率定价，并将5%的利润 + 成本 +头程 做为【建议供货价】
    3、对比两者的差异
    """
    # 1、取供应商sku
    df = get_sup_dwm()
    # df = df[(df['platform']=='TEMU') & (df['YB_sku'] != '') & (df['country'].isin(['DE','US','UK']))]
    df = df[(df['platform'] == 'TEMU') & (df['country'].isin(['DE', 'US', 'UK']))]
    # 补充临时sku
    df_temp_sku = pd.read_excel('F://Desktop//测算建议供货价20260331.xlsx', dtype={'YB_sku':str, 'YM_sku':str})
    df_temp_sku = df_temp_sku[df_temp_sku['date_id']==20260407]
    df_temp_sku = df_temp_sku[['YB_sku','YM_sku','warehouse','country']]
    # df_temp_sku = pd.DataFrame(dic)
    df_temp_sku['platform'] = 'TEMU'
    df = pd.concat([df, df_temp_sku])
    # 20260317 没有映射关系的sku，也需要加入计算
    df['YB_sku'] = np.where(df['YB_sku']=='', df['YM_sku'], df['YB_sku'])
    col = ['YM_sku', 'warehouse', 'warehouse_price', 'cargo_owner_id', 'available_stock', 'overage_level', 'total_cost',
           'shippingCost','firstCarrierCost','ppve','platform_zero','platform_must_percent',
           'sup_price', 'platform', 'country','YB_sku']
    df = df[col]
    df['platform_zero'] = 0.13
    df['platform_must_percent'] = 0.05

    # df.to_excel('F://Desktop//df_sup_temp.xlsx', index=0)
    # 获取yb差值，及让利点数
    df_yb = get_platform_fee()
    yb_fee = df_yb[df_yb['platform']=='TEMU']['platform_zero'].iloc[0]
    ym_fee = df[df['platform'] == 'TEMU']['platform_zero'].iloc[0]
    diff = yb_fee - ym_fee
    # 2、取YBsku信息
    df = df.rename(columns={'YB_sku':'sku'})
    df = get_line(df)

    # 运费信息
    df = get_dwm_sku(df)
    col = ['total_cost','shippingCost','firstCarrierCost_x','ppve','platform_zero','available_stock_x',
           'platform_must_percent','warehouse_price']
    df[col] = df[col].astype(float)
    df[['available_stock_y','available_stock_x']] = df[['available_stock_y','available_stock_x']].fillna(0).astype(int)
    print(df.info())
    # 费用替换
    # df['运费来源'] = np.where(df['available_stock_y']>0, '调价运费', '接口运费')
    # 临时补充YBsku测算数据，运费只有YB数据
    df['total_cost'] = np.where(df['total_cost'].isna(), df['totalCost_origin'], df['total_cost'])
    df['shippingCost'] = np.where(df['shippingCost'].isna(), df['ship_fee'], df['shippingCost'])
    df['运费来源'] = '接口运费'
    df['总运费'] = np.where(df['available_stock_y']>0, df['totalCost_origin']-df['firstCarrierCost_y']+df['自算头程'],
                            df['total_cost']-df['firstCarrierCost_x']+df['自算头程'])
    df['尾程'] = np.where(df['available_stock_y'] > 0, df['ship_fee'], df['shippingCost'])
    col = ['total_cost','shippingCost','firstCarrierCost_x','sup_price','develop_source_name',
           'best_warehouse_name','totalCost_origin','ship_fee','firstCarrierCost_y']
    df.drop(col, axis=1, inplace=True)

    # 3、日销统计（YM_sku + YB_sku)的销量汇总
    df_sales = get_days_sales()
    df_sales = df_sales.rename(columns={'sku':'YM_sku'})

    df = pd.merge(df, df_sales[['YM_sku','warehouse','day_sales']], how='left', on=['YM_sku', 'warehouse'])
    df['day_sales'] = df['day_sales'].fillna(0).astype(float)
    # 4、计算
    df['预计可售天数'] = ((df['available_stock_x'] + df['available_stock_y'])/df['day_sales']).replace(np.inf, 9999)
    df['ppve'] = 0.02
    df['目标净利率定价'] = (df['new_price']+df['总运费'])/(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])
    df['建议供货价'] = df['new_price'] + df['自算头程'] + df['目标净利率定价']*0.07
    df['建议供货价/实际供货价-1'] = df['建议供货价'] / df['warehouse_price'] - 1
    df['建议供货价是否小于实际供货价'] = np.where(df['建议供货价']<df['warehouse_price'], 1, 0)
    dic = {60:0.02, 90:-0.05, 120:-0.1, 150:-0.2, 180:-0.4, 270:-0.5, 360:-0.8}
    df['调价逻辑目标净利率'] = df['overage_level_x'].map(dic).fillna(0.05)
    df['调价逻辑目标净利率定价'] = (df['建议供货价']+df['尾程'])/(1-df['ppve']-df['platform_zero']-df['调价逻辑目标净利率'])
    df['按调价逻辑降价后的建议供货价'] = df['调价逻辑目标净利率定价']*(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])-df['尾程']
    df['按调价逻辑_建议供货价折扣'] = df['按调价逻辑降价后的建议供货价'] / df['建议供货价']
    df['按调价逻辑_建议供货价/实际供货价'] = df['按调价逻辑降价后的建议供货价'] / df['warehouse_price']

    df = cut_bins(df)

    dic = {'warehouse_price':'实际供货价', 'cargo_owner_id':'商户ID', 'available_stock_x':'YMsku可用库存',
           'overage_level_x':'YMsku超库龄等级', 'ppve':'佣金+vat+库损汇损', 'platform_zero':'YM差值',
           'platform_must_percent':'YM目标净利率', 'platform':'平台', 'country':'国家','sku':'YB_sku',
           'new_price':'YBsku成本', 'available_stock_y':'YBsku可用库存', 'overage_level_y':'YBsku超库龄等级',
           'sale_status':'销售状态','day_sales':'日销(YBsku+YMsku)'}
    df = df.rename(columns=dic)
    df['date_id'] = time.strftime('%Y-%m-%d')

    df.to_excel('F://Desktop//df_sup_info_all.xlsx', index=0)
    print(df.info())
    #
    # conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn_ck.ck_insert(df, 'oversea_supplier_info_all', if_exist='replace')


def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')


    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()


# 给安琪拉数据
def get_sup_stock_info():
    """ 海外仓库存库龄信息"""
    date_today = time.strftime('%Y-%m-%d')
    # 取库存库龄
    sql = f"""
        SELECT YM_sku, warehouse_id, warehouse_name, warehouse, available_stock, cargo_owner_id, overage_level, date_id
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # 取映射关系
    df_sku = get_sup_yb_sku()
    df = pd.merge(df, df_sku[['YM_sku', 'YB_sku']], how='left', on='YM_sku')

    df.to_excel('F://Desktop//df_sup_stock.xlsx', index=0)


def temp():
    """ """
    sql = """
        SELECT *
        FROM yibai_oversea.oversea_supplier_info_all
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')

    df = conn_ck.ck_select_to_df(sql)

    df.to_excel('F://Desktop//df_sup_all.xlsx', index=0)


# 供应商sku链接的在线价利润率
def yb_get_sup_amazon_listing():
    """ """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_sku = get_sup_dwm()
    df_sup_sku_1 = df_sup_sku[['YB_sku', 'country']].drop_duplicates()
    df_sup_sku_1.columns=['sku','country']
    df_sup_sku_2 = df_sup_sku[['YM_sku', 'country']].drop_duplicates()
    df_sup_sku_2.columns = ['sku', 'country']
    df_sup_sku = pd.concat([df_sup_sku_1, df_sup_sku_2])
    sku_list = list(df_sup_sku['sku'].unique())

    sql = f"""
        SELECT account_id, short_name, 'AMAZON' as platform,
        upper(site) as country,
        status, sku, seller_sku, asin, online_price, deliver_mode
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all)
        and sku in {sku_list}
    """
    df = conn_ck.ck_select_to_df(sql)
    df['warehouse'] = df['country'].replace({'US':'美国仓','DE':'德国仓','UK':'英国仓'})
    df['is_supplier'] = 1
    df = pd.merge(df, df_sup_sku, how='inner', on=['sku','country'])
    df = df[(~df['sku'].isna()) & (df['sku']!='')]
    print(df.info())

    return df



def get_sup_listing_profit():
    """ 获取各平台供应商链接，并计算利润率数据 """
    # 1、获取链接
    sql = """
        SELECT account_id,short_name,platform,country,sku,seller_sku,item_id,product_id,asin,listing_status,
        online_price, freight_subsidy, warehouse, is_supplier 
        FROM yibai_oversea.oversea_listing_profit
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_listing_profit)
        and is_supplier != 0
        and platform != 'amazon'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # amazon链接单独处理
    df_amazon = yb_get_sup_amazon_listing()
    df = pd.concat([df, df_amazon])

    dic = {'amazon':'AMAZON', 'ebay':'EB', 'temu':'TEMU', 'ali':'ALI', 'allegro':'ALLEGRO',
           'walmart':'WALMART', 'cdiscount':'CDISCOUNT'}
    df['platform'] = df['platform'].replace(dic)
    df = df[df['country'].isin(['US', 'DE'])]
    # 2、获取供应商sku价格数据
    sql = """
        SELECT YM_sku, YB_sku, warehouse, available_stock, country, platform, warehouse_price, shippingCost, ppve, platform_zero
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
    """
    df_sup = conn_ck.ck_select_to_df(sql)

    df_sku_temp = df_sup[['YM_sku','YB_sku']].drop_duplicates()
    df_sku_temp = df_sku_temp.rename(columns={'YB_sku':'sku'})

    df = pd.merge(df, df_sku_temp, how='left', on=['sku'])
    df['YM_sku'] = np.where(df['YM_sku'].isna(), df['sku'], df['YM_sku'])

    df = pd.merge(df, df_sup, how='left', on=['YM_sku', 'warehouse', 'country', 'platform'])

    # 汇率
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='left', on=['country'])

    # 3、计算在线价利润率
    df.to_excel('F://Desktop//df_sup_listing_yb.xlsx', index=0)


def tt_get_sup_amazon_listing():
    conn_ck = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sup_sku = get_sup_dwm()
    df_sup_sku_1 = df_sup_sku[['YB_sku', 'country']].drop_duplicates()
    df_sup_sku_1.columns=['sku','country']
    df_sup_sku_2 = df_sup_sku[['YM_sku', 'country']].drop_duplicates()
    df_sup_sku_2.columns = ['sku', 'country']
    df_sup_sku = pd.concat([df_sup_sku_1, df_sup_sku_2])
    sku_list = list(df_sup_sku['sku'].unique())
    # sku_list = sku_list[0:100]
    sql = """
    with listing_table as (
        select distinct account_id, seller_sku
        from tt_product_kd_sync.tt_amazon_sku_map
        where sku in {}
        -- and deliver_mode=2
    )

    select b.account_id as account_id, b.account_name as short_name,
         'AMAZON' as platform,
        upper(if(b.site ='sp', 'es', b.site)) as country,
        status, e.sku as sku, a.seller_sku as seller_sku,
        if(trim(a.asin1) != '', a.asin1, t.asin1) as asin,
        a.price as online_price, deliver_mode
    from (
        select account_id, asin1, seller_sku, price, status, fulfillment_channel, open_date, create_time
        from tt_product_kd_sync.tt_amazon_listings_all_raw2
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) a
    inner join (
        select account_id, sku, seller_sku, deliver_mode
        from tt_product_kd_sync.tt_amazon_sku_map
        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
        -- where deliver_mode=2
    ) e
    on (a.account_id = e.account_id and a.seller_sku = e.seller_sku)
    left join (
        select account_id, seller_sku, asin1
        from tt_product_kd_sync.tt_amazon_listing_alls
        where fulfillment_channel='DEF' and (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) t
    on (t.account_id = a.account_id and t.seller_sku = a.seller_sku)
    inner join (
        select toInt32(b.id) as account_id, account_name, group_id, short_name, site
        from tt_system_kd_sync.tt_amazon_account b
        where account_num not in ('Gaorgas','Wocilnia','Heixwaio') or group_id != 163
    ) b
    on (a.account_id= b.account_id)
    inner join (
        select group_id, group_name
        from tt_system_kd_sync.tt_amazon_group
        where group_name not in ('武汉亚马逊分部一组', '武汉亚马逊分部二组','新项目小组（GXJ）','领创项目部','易蓝网络','深圳精品二部','极星项目部')
        or not hasAll(multiMatchAllIndices(group_name, ['深圳', '精品']), [1,2])
    ) c
    on (b.group_id=c.group_id)
    left join (
        select account_id, seller_sku, ListingPrice as sale_price
        from tt_product_kd_sync.tt_amazon_listing_price
        where (account_id, seller_sku) in (select account_id, seller_sku from listing_table)
    ) f
    on (a.account_id = f.account_id and a.seller_sku = f.seller_sku)
    order by a.create_time desc

    """.format(sku_list)
    # data = con.execute(sql)
    # columns1 = ['sku', 'platform', 'site', 'amazon_listing_num']
    # df_amazon_listing_x = pd.DataFrame(data=data, columns=columns1)
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())
    df['warehouse'] = df['country'].replace({'US':'美国仓','DE':'德国仓','UK':'英国仓'})
    df['is_supplier'] = 1
    print(df.head(4))
    df = pd.merge(df, df_sup_sku, how='inner', on=['sku','country'])
    df = df[(~df['sku'].isna()) & (df['sku']!='')]
    # df.to_excel('F://Desktop//df_tt_amazon.xlsx', index=0)
    print(df.info())

    return df

def tt_get_sup_listing_profit():
    """ 获取各平台供应商链接，并计算利润率数据 """
    # 1、获取链接
    sql = """
        SELECT account_id,short_name,platform,country,sku,seller_sku,item_id,product_id,asin,listing_status,
        online_price, freight_subsidy, warehouse, is_supplier 
        FROM yibai_oversea.tt_oversea_listing_profit
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_oversea_listing_profit)
        and is_supplier != 0
        and platform != 'amazon'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    # amazon链接单独处理
    df_amazon = tt_get_sup_amazon_listing()
    df = pd.concat([df, df_amazon])

    dic = {'amazon':'AMAZON', 'ebay':'EB', 'temu':'TEMU', 'ali':'ALI', 'allegro':'ALLEGRO', 'walmart':'WALMART'}
    df['platform'] = df['platform'].replace(dic)
    df = df[df['country'].isin(['US','DE'])]
    # 2、获取供应商sku价格数据
    sql = """
        SELECT YM_sku, YB_sku, warehouse,available_stock, country, platform, warehouse_price, 
        shippingCost, ppve, platform_zero
        FROM yibai_oversea.tt_dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_dwm_supplier_sku_price)
    """
    df_sup = conn_ck.ck_select_to_df(sql)

    df_sku_temp = df_sup[['YM_sku','YB_sku']].drop_duplicates()
    df_sku_temp = df_sku_temp.rename(columns={'YB_sku':'sku'})

    df = pd.merge(df, df_sku_temp, how='left', on=['sku'])
    df['YM_sku'] = np.where(df['YM_sku'].isna(), df['sku'], df['YM_sku'])

    df = pd.merge(df, df_sup, how='left', on=['YM_sku', 'warehouse', 'country', 'platform'])

    # 汇率
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='left', on=['country'])

    #
    # 3、计算在线价利润率
    df.to_excel('F://Desktop//df_sup_listing.xlsx', index=0)

def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate['country'] = np.where(df_rate['charge_currency']=='HUF', 'HU', df_rate['country'])
    # df_rate = df_rate.drop_duplicates(subset='charge_currency')
    return df_rate


def get_cd_listing():
    sql = """
        SELECT *
        FROM yibai_oversea.oversea_cdiscount_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_cdiscount_listing_all)
        
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # df_cd = conn_ck.ck_select_to_df(sql)

    sql = """
        SELECT *
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and available_stock > 0
        and platform = 'CDISCOUNT'
    """
    df_cd_sku = conn_ck.ck_select_to_df(sql)

    df_cd_sku.to_excel('F://Desktop//df_cd_sku.xlsx', index=0)

def sup_temp():
    """ """
    sql = """
        SELECT sku, seller_sku, account_id, asin, online_price, price, available_stock, is_normal_cdt, is_supplier_price
        FROM yibai_oversea.oversea_amazon_listing_all
        WHERE date_id = '2026-04-03' and sku in ['3116250007212','YM18733YCJ-101','YM18733YCJ-501','3116250007512',
        '3116250007312','3116250007511','YM18733YCJ-401','3116250007311',
        '3116250007211','YM18733YCJ-201','YM18733YCJ-601','YM18733YCJ-301']
    """
    sql = """
        SELECT sku, seller_sku, account_id, short_name, asin, online_price, deliver_mode
        FROM yibai_oversea.yibai_ads_oversea_amazon_listing_all
        WHERE date_id = '2026-04-03' and sku in ['3116250007212','YM18733YCJ-101','YM18733YCJ-501','3116250007512',
        '3116250007312','3116250007511','YM18733YCJ-401','3116250007311',
        '3116250007211','YM18733YCJ-201','YM18733YCJ-601','YM18733YCJ-301']
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_temp = conn_ck.ck_select_to_df(sql)

    df_sup_temp.to_excel('F://Desktop//df_sup_temp_all.xlsx', index=0)


if __name__ == '__main__':
    # main()
    # cal_sup_price_new()
    temp_cal_sup_price()
    # get_sup_dwm()

    # get_sup_stock_info()

    # tt_get_sup_listing_profit()
    # get_sup_listing_profit()

    # tt_get_sup_amazon_listing()
    # tt_get_sup_listing_profit()
    # yb_get_sup_amazon_listing()
    # get_sup_listing_profit()
    # sup_temp()