"""
供应商货盘相关需求:
1、定价数据
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
    df['自算头程'] = df['长'] * df['宽'] * df['高'] * df['price'] / 1000000
    col = ['数量', '重量', '成本', '长', '宽', '高', '重量来源', 'price']
    df.drop(col, axis=1, inplace=True)

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
    df['【建议供货价/实际供货价-1】分段'] = pd.cut(
        df['建议供货价/实际供货价-1'], bins=[-50, -0.3, -0.2, -0.1, 0, 0.1, 0.2, 0.3, 1000],
        labels=['A:<-0.3', 'B:(-0.3,-0.2]', 'C:(-0.2,-0.1]', 'D:(-0.1,0]','E:(0, 0.1]',
                'F:(0.1,0.2]', 'G:(0.2,0.3]', 'H:>0.3'])

    df['折扣分段'] = pd.cut(
        df['按调价逻辑_建议供货价折扣'], bins=[-1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 10],
        labels=['A:<0.2', 'B:(0.2,0.3]', 'C:(0.3,0.4]', 'D:(0.4,0.5]','E:(0.5, 0.6]',
                'F:(0.6,0.7]', 'G:(0.7,0.8]', 'H:(0.8,0.9]', 'I:>0.9'])

    return df

# 海外仓建议供货价与实际供货价测算
def cal_sup_price_new():
    """
    1、取供应商sku, 及其【实际供货价】
    2、用YB的成本及运费，计算目标利润率定价，并将5%的利润 + 成本 +头程 做为【建议供货价】
    3、对比两者的差异
    """
    # 1、取供应商sku
    df = get_sup_dwm()
    df = df[(df['platform']=='TEMU') & (df['YB_sku'] != '') & (df['country'].isin(['DE','US','UK']))]
    col = ['YM_sku', 'warehouse', 'warehouse_price', 'cargo_owner_id', 'available_stock', 'overage_level', 'total_cost',
           'shippingCost','firstCarrierCost','ppve','platform_zero','platform_must_percent',
           'sup_price', 'platform', 'country','YB_sku']
    df['platform_must_percent'] = 0.05
    df = df[col]

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
    df['运费来源'] = np.where(df['available_stock_y']>0, '调价运费', '接口运费')
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
    df['预计可售天数'] = ((df['available_stock_x'] + df['available_stock_y'])/df['day_sales'])
    df['目标净利率定价'] = (df['new_price']+df['总运费'])/(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])
    df['建议供货价'] = df['new_price'] + df['自算头程'] + df['目标净利率定价']*0.05
    df['建议供货价是否小于实际供货价'] = (df['建议供货价']<df['warehouse_price'])
    dic = {60:0.02, 90:-0.05, 120:-0.1, 150:-0.2, 180:-0.4, 270:-0.5, 360:-0.8}
    df['调价逻辑目标净利率'] = df['overage_level_x'].map(dic).fillna(0.05)
    df['调价逻辑目标净利率定价'] = (df['建议供货价']+df['尾程'])/(1-df['ppve']-df['platform_zero']-df['调价逻辑目标净利率'])
    df['按调价逻辑降价后的建议供货价'] = df['调价逻辑目标净利率定价']*(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])-df['尾程']
    df['按调价逻辑_建议供货价折扣'] = df['按调价逻辑降价后的建议供货价'] / df['建议供货价']
    df['建议供货价/实际供货价-1'] = df['建议供货价']/df['warehouse_price'] - 1
    df = cut_bins(df)

    dic = {'warehouse_price':'实际供货价', 'cargo_owner_id':'商户ID', 'available_stock_x':'YMsku可用库存',
           'overage_level_x':'YMsku超库龄等级', 'ppve':'佣金+vat+库损汇损', 'platform_zero':'YM差值',
           'platform_must_percent':'YM目标净利率', 'platform':'平台', 'country':'国家','sku':'YB_sku',
           'new_price':'YBsku成本', 'available_stock_y':'YBsku可用库存', 'overage_level_y':'YBsku超库龄等级',
           'sale_status':'销售状态','day_sales':'日销(YBsku+YMsku)'}
    df = df.rename(columns=dic)

    df.to_excel('F://Desktop//df_sup_info_all.xlsx', index=0)


# amazon链接deliver_mode=2
def check_amazon_listing():
    """ """
    sql = """
        SELECT account_id, seller_sku, deliver_mode, sku, `90days_sales1`, site
        FROM yibai_price_dom.yibai_domestic_warehouse_increase_fbm_20260304
        WHERE deliver_mode = 2
    """
    conn_ck = pd_to_ck(database='yibai_price_dom', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)
    dic = {'美国':'美国仓', '英国':'英国仓', '德国':'德国仓', '澳大利亚':'澳洲仓', '加拿大':'加拿大仓', '墨西哥':'墨西哥仓'}
    df_listing['warehouse'] = df_listing['site'].replace(dic)

    # sku
    sql = """
        SELECT sku, warehouse, best_warehouse_name, date_id
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_sku_temp_info)
    """
    df_sku = conn_ck.ck_select_to_df(sql)

    # dtl
    sql = """
        SELECT DISTINCT sku, warehouse, 1 as sku_dtl
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform = 'AMAZON'
    """
    df_dtl = conn_ck.ck_select_to_df(sql)

    df_sku = pd.merge(df_sku, df_dtl, how='left', on=['sku','warehouse'])

    df_listing = pd.merge(df_listing, df_sku, how='left', on=['sku','warehouse'])

    # order
    sql = """
        
    """
    df_listing.to_excel('F://Desktop//df_amazon_listing.xlsx', index=0)





if __name__ == '__main__':
    # main()
    # cal_sup_price_new()

    # get_sup_dwm()

    # get_days_sales()
    check_amazon_listing()