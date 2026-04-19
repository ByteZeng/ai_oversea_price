"""
国内仓SKU，在不同业务模式（国内仓、虚拟仓、海外仓、FBA转寄）下，同样利润率的定价
1、取国内仓在库+在途库存>0的SKU信息
2、计算国内仓、虚拟仓、海外仓空运、海外仓海运、FBA转寄运费数据
    * 国内仓、虚拟仓取现有运费表
    * 海外仓用接口拉取
    * FBA转寄运费用运费明细计算
3、汇总不同模式的运费数据，设置定价参数，计算定价

"""
##
import pandas as pd
import numpy as np
import datetime, time
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang, jisuanfenlei, get_oversea_ship_type_list
from pulic_func.price_adjust_web_service.AMAZON_FBA_dingjia import fba_ding_jia_biao, aut
from pulic_func.adjust_price_base_api.FBA_fee import tou_cheng_dingjiabi, fbafee_us,fbafee_us_us, fbafee_au, fbafee_eu
from pulic_func.base_api.base_function import get_token, mysql_escape
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from concurrent.futures._base import as_completed
##
def get_sku():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 产品表里取所有sku（在售在途库存大于0的）
    sql = f"""
    with sku_table as (
        select distinct sku from yibai_prod_base_sync.yibai_prod_sku_select_attr
        where attr_value_id = 67
    ),
    sku_table2 as (
        SELECT sku, sum(purchase_on_way_count + stock) as stock
        FROM yb_datacenter.yb_stock
        WHERE 
            date_id = (SELECT max(date_id) FROM yb_datacenter.yb_stock)
            and warehouse_id in (SELECT id FROM yibai_logistics_tms_sync.yibai_warehouse where warehouse_type=1)
        GROUP BY sku
        HAVING stock > 0
    ),
    sku_table3 as (
        select sku from sku_table2
        where stock > 0
    )
    select 
        sku sku
    from (
        select
            distinct sku sku,
            product_status product_status,
            toFloat64(product_cost) as cost,
            '1' as temp
        from yibai_prod_base_sync.yibai_prod_sku
        where sku in (select * from sku_table)
        and sku in (select * from sku_table3)
        and (sku not like '%%*%%' and sku not like '%%+%%')
    ) a
    """
    df = conn_mx.ck_select_to_df(sql)
    # df = df.sample(1000)
    print('获取sku完成！')
    return df
def merge_site(df):
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 欧洲只选德国 + 阿联酋没有海运，要剔除
    sql = f"""
        SELECT 
            site2 as site, site1 `站点`
        from domestic_warehouse_clear.yibai_site_table_amazon 
        where site2 in ('AU', 'DE', 'UK', 'US')
        """
    df_site = conn_mx.ck_select_to_df(sql)
    df_site['temp'] = 1
    df['temp'] = 1
    df = df.merge(df_site, on=['temp'], how='left')
    df.drop('temp', axis=1, inplace=True)
    print('匹配站点完成！')
    return df

def get_diff():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 四维差值 之后使用需要进行多次匹配来修正各个字段信息从而能够最终用所有字段进行匹配
    sql_diff = """
        select * from yibai_sale_center_listing_sync.yibai_listing_profit_config
        where is_del=0 and status=1 and platform_code='AMAZON'
    """
    df_diff = conn_mx.ck_select_to_df(sql_diff)
    sql_site_table = """
        select site2 as site_en, site1 as site from domestic_warehouse_clear.yibai_site_table_amazon
    """
    df_site_table = conn_mx.ck_select_to_df(sql_site_table)
    df_diff = df_diff.rename(columns={'site': 'site_en'}).merge(df_site_table, on='site_en', how='left')
    df_diff['site'] = df_diff.apply(lambda x: 'other' if x['site_en'] == 'other' else x['site'], axis=1)
    df_diff.dropna(inplace=True)
    return df_diff

def merge_first_product_line(df):
    print(f'匹配第一产品线...')
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 一级产品线
    sql_line = f"""
                    select a.sku sku1,
                    b.path_name as `产品线路线` from yb_datacenter.yb_product a
                    left join yb_datacenter.yb_product_linelist b
                    on toInt32(a.product_linelist_id) = toInt32(b.id) 
                """
    df_line = conn_mx.ck_select_to_df(sql_line)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]

    sql_line_2 = f"""
                select distinct id first_product_line, 
                linelist_cn_name as `一级产品线` from yb_datacenter.yb_product_linelist
                where linelist_cn_name = path_name
            """
    df_line_2 = conn_mx.ck_select_to_df(sql_line_2)
    df_line = df_line.merge(df_line_2, on='一级产品线', how='left')
    df_line = df_line[['sku1', 'first_product_line']]

    # 拆分捆绑sku
    df['sku1'] = df['sku'].str.split('+', expand=True)[0].str.split('*', expand=True)[0]

    df = df.merge(df_line, on='sku1', how='left')
    df['first_product_line'] = df['first_product_line'].fillna(-1)
    df.drop('sku1', axis=1, inplace=True)
    print('匹配第一产品线完成！')
    return df

def bendiyunfei(df, conn_ck, table=None, ship_list=None, warehouse_id=478):
    join_list = ['old_freight_interface_amazon', 'old_freight_interface_walmart',
                 'old_freight_interface_cd', 'old_freight_interface_allegro']
    if table in join_list:
        col = 'sku_join'
        df_other = df[df['数量_join'] != '1']
        df = df[df['数量_join'] == '1']
    else:
        col = 'sku'
        df_other = df[df['数量'] != 1]
        df = df[df['数量'] == 1]

    # 20230605 为加快行数较多文件的计算速度，删除不符合条件的数据
    df_copy = df[df.index >= 0]

    df_yunfei = pd.DataFrame(columns=['sku', 'ship_name', 'warehouse_id', '运费', '站点', 'ship_type'])
    for key, group in tqdm(df.groupby(['站点']), desc='获取运费'):
        # group = group[[col]].drop_duplicates()
        # group = group.reset_index(drop=True)
        # group['index'] = group.index
        # group['index'] = group['index'].apply(lambda m: int(m / 3000))
        # for key1, group1 in group.groupby(['index']):
        #     sku_list = mysql_escape(group1, col=col)
        sql = f"""
            SELECT distinct sku,ship_name,warehouse_id,total_cost as `运费`,site as `站点`,ship_type  
            from yibai_temp_hxx.{table} 
            where site='{key}' and warehouse_id = {warehouse_id}
            order by total_cost asc limit 1 by sku
            """
        df_yunfei0 = conn_ck.ck_select_to_df(sql)
        df_yunfei = df_yunfei.append(df_yunfei0)
        del df_yunfei0
    df_yunfei['数量'] = 1
    # df_yunfei['ship_type'] = df_yunfei['ship_type'].astype(int)
    # df_yunfei['warehouse_id'] = df_yunfei['warehouse_id'].astype(int)
    #
    if table in join_list:
        df_yunfei.rename(columns={'sku': 'sku_join', '数量': '数量_join'}, inplace=True)
        df_yunfei['数量_join'] = '1'
        df = df.merge(df_yunfei, on=['sku_join', '数量_join', '站点'], how='left')
    else:
        df = df.merge(df_yunfei, on=['sku', '数量', '站点'], how='left')
    del df_yunfei

    # 仓库、物流方式选择
    df['发货仓库1'] = df['warehouse_id']

    df['物流类型1'] = df['ship_type']

    # 20230605 为加快行数较多文件的计算速度，删除不符合条件的数据
    df1 = df[['index']].drop_duplicates()
    df1['运费选择'] = '是'
    df_copy = df_copy.merge(df1, on=['index'], how='left')
    df_copy = df_copy[df_copy['运费选择'] != '是']
    df_copy.drop(['运费选择'], axis=1, inplace=True)
    df = df.append([df_copy, df_other])
    del df_copy, df_other

    df['运费选择'] = '否'
    df.loc[(df['发货仓库1'] == df['warehouse_id']) & (df['物流类型1'] == df['ship_type']), '运费选择'] = '是'
    if ship_list:
        df.loc[(df['账号类型'] == '虚拟仓账号') & (~df['ship_name'].isin(ship_list)), '运费选择'] = '否'
    return df

class get_trip_fee_oversea2(object):
    def __init__(self, platform, shipCountry, warehouse, shipType):
        self.platform = platform
        self.shipCountry = shipCountry
        self.warehouse = warehouse
        self.shipType = shipType
        self.ship_list = []
        self.df = pd.DataFrame(
            columns=['shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipName', 'shipCode','totalCost',
                     'firstCarrierCost', 'shippingCost','overseasFee','dutyCost'])

    def batch_df_order_fu(self, group):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        # url0 = 'http://rest.java.yibainetwork.com/logistics/orderShippingFee/getProductBestLogistics?access_token='
        url0 = 'http://logistics-local.yibainetwork.com:33357/skuShippingFee/getProductBestLogistics?access_token='
        url = url0 + get_token()
        #
        for i in range(group.shape[0]):
            data = {
                'cpath': '1->8->9801',
                'shipType': self.shipType,
                'warehouseId': self.warehouse,
                'shipCountry': self.shipCountry,
                'platformCode': self.platform,
                # 'sku': group['sku'][i],
                'skuNum': '1',
                'weight': group['重量'][i],
                'cLength': group['长'][i],
                'cWidth': group['宽'][i],
                'cHeight': group['高'][i],
                'lastPrice': group['成本'][i],
                'responseLevel': 3,
                'feeType': 255,
                'checkWarehouse': 0
            }
            try:
                r = requests.post(url=url, data=data, headers=headers, timeout=5)
                if r.status_code == 200:
                    temp = pd.DataFrame(r.json().get('data'))
                    col_list = ['warehouseId', 'warehouseName', 'shipName', 'shipCode','totalCost','shippingCost', 'firstCarrierCost',
                                'overseasFee','dutyCost']
                    try:
                        temp = temp[col_list]
                    except:
                        temp = pd.DataFrame(columns=col_list)
                    temp['platformCode'] = self.platform
                    temp['shipCountry'] = self.shipCountry
                    # temp['group_id'] = group['group_id'][i]
                    temp['sku'] = group['sku'][i]
                    temp['数量'] = group['数量'][i]
                    self.df = self.df.append(temp)
            except:
                pass

    def batch_df_order(self, df):
        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 1000))
        threadPool = ThreadPoolExecutor(max_workers=10)
        thread_list = []
        for key, group in df.groupby(['index']):
            group = group.reset_index(drop=True)
            future = threadPool.submit(self.batch_df_order_fu, group)
            thread_list.append(future)

        # with tqdm(total=len(thread_list), desc=f'{self.platform}-{self.shipCountry}接口获取运费') as pbar:
        #     for future in as_completed(thread_list):
        #         data = future.result()
        #         pbar.update(1)
        threadPool.shutdown(wait=True)
        #
        return self.df

def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    conn.to_sql(df, table_name, if_exists='replace')
    conn.close()
##
def get_oversea_fee(df, table_name='sku_to_oversea_fee_20240403'):
    """
    调用接口，获取sku发海外仓运费数据。暂时默认取ebay平台对应的运费。
    输入：sku
    输出：sku+大仓+国家维度的运费数据，存入ck数据库yibai_oversea，需指定表名
    """
    # df = get_sku()
    # df = merge_site(df)
    # # 尺寸重量
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df['数量'] = 1
    # df = chicun_zhongliang(df, 1, conn_mx)
    #
    # 切换SKU
    # 海外仓SKU发四个主要国家的运费
    # sql = """
    #     SELECT sku, product_status, linest
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = '2024-02-26'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)
    # df = pd.read_excel('F:\Desktop\国内仓sku发海外仓定价_补充20240403.xlsx')
    #
    # df['数量'] = 1
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df = chicun_zhongliang(df, 1, conn_mx)
    # dic = {'temp': [1, 1, 1, 1],
    #        'site': ['AU', 'DE', 'US', 'UK']}
    # df_site = pd.DataFrame(dic)
    # df['temp'] = 1
    # df = pd.merge(df, df_site, how='left', on=['temp'])
    # df.drop('temp', axis=1, inplace=True)
    # print(df.info())
    #
    # 表准备
    conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    # table_name = f'oversea_sku_to_oversea_fee'
    # table_name = f'sku_to_oversea_fee_20240403'
    sql = f"""
        DROP TABLE IF EXISTS yibai_oversea.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
        CREATE TABLE IF NOT EXISTS yibai_oversea.{table_name}
        (
        `id` Int64,
        `date` String,
        `sku` String,
        `shipCountry` String COMMENT '站点',
        `platform` String COMMENT '平台',
        `warehouseId` Int64 COMMENT '海外仓仓库ID',
        `warehouseName` String COMMENT '海外仓仓库名称',
        `shipCode` String COMMENT '渠道CODE',
        `shipName` String COMMENT '渠道名称',
        `totalCost` Float64 COMMENT '总运费',
        `shippingCost` Float64 COMMENT '尾程运费',
        `firstCarrierCost` Float64 COMMENT '头程运费',
        `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `shipCountry`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE yibai_oversea.{table_name}
        DELETE where date = '{date_today}'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')
    #
    # 海外仓运费获取
    # 仓库ID:澳、德、美、英常用仓库
    warehouse_id = '353,769,325,35,47,50,88,680'
    ship_type = get_oversea_ship_type_list()
    #
    # 海外仓接口调取
    # df_temp = df.sample(1000)
    # df_result = pd.DataFrame()
    # df.to_excel('df_sku_to_oversea.xlsx',index=0)
    df = df.reset_index(drop=True)
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 12000))
    for key, df0 in df.groupby(['index']):
        df_result = pd.DataFrame()
        for key1, group in tqdm(df0.groupby(['site'])):
            print(key1, group.shape)
            group1 = group[['sku', '数量', '成本','重量','长','宽','高']]
            if key1 == 'AU':
                warehouse_id = '769'
            elif key1 == 'DE':
                warehouse_id = '325'
            elif key1 == 'US':
                warehouse_id = '50'
            elif key1 == 'UK':
                warehouse_id = '88'
            yunfei_jisuan = get_trip_fee_oversea2('EB', key1, warehouse_id, ship_type)
            group2 = yunfei_jisuan.batch_df_order(group1)
            group2 = group2[
                ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipName','shipCode',
                 'totalCost', 'shippingCost', 'firstCarrierCost']]
            group3 = group.merge(group2, on=['sku', '数量'])
            group3 = group3[
                ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
                 'totalCost', 'shippingCost', 'firstCarrierCost']]
            group3 = group3.sort_values(['totalCost'], ascending=True)
            group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
            df_result = pd.concat([group3, df_result])
            df_result.drop('数量',axis=1, inplace=True)
            df_result['platform'] = 'EB'
            df_result['date'] = date_today
        conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        conn_mx2.ck_insert(df_result, table_name, if_exist='append')

def get_oversea_totalcost(df, table_name='sku_to_oversea_fee_20240403'):
    """
    先通过接口，获取SKU的海外仓运费数据，
    然后用公告报价，自算头程海运、头程空运运费，
    最后替换头程费用。
    """
    sql = """
        SELECT warehouse_id as warehouseId,price, weight_method, include_tax
        FROM `yibai_toucheng_new` 
        """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_toucheng = conn.read_sql(sql)
    sql = f"""
        SELECT sku, shipCountry as site, warehouseId as warehouse_id, warehouseName as warehouse_name, totalCost total_cost,
        shippingCost shipping_cost, firstCarrierCost
        FROM yibai_oversea.{table_name}
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_oversea_fee = conn_mx.ck_select_to_df(sql)
    #
    df_oversea_fee = pd.merge(df_oversea_fee, df_toucheng, how='left',on=['warehouse_id'])
    #
    df_oversea = pd.merge(df, df_oversea_fee, how='left', on=['sku','site'])
    # 海运头程
    df_oversea = df_oversea.merge(df_toucheng, on=['warehouseId'])
    df_oversea['price'] = df_oversea['price'].astype('float')
    df_oversea[['weight_volume', 'pur_weight_pack']] = df_oversea[['weight_volume', 'pur_weight_pack']].astype(float)
    df_oversea['计费重'] = np.where(df_oversea['weight_volume']/6000 > df_oversea['pur_weight_pack']/1000,
                           df_oversea['weight_volume']/6000, df_oversea['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df_oversea['new_firstCarrierCost'] = np.where(df_oversea['weight_method']==1,
                                          df_oversea['计费重'] * df_oversea['price'], df_oversea['weight_volume'] / 1000000 * df_oversea['price'])
    df_oversea['new_firstCarrierCost'] = np.where(df_oversea['warehouseId'].isin([961,1019,1057,847]),
                                          df_oversea['firstCarrierCost'], df_oversea['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df_oversea[col] = df_oversea[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    df_oversea['totalCost'] = np.where(df_oversea['include_tax']==1,
                               df_oversea['totalCost'] - df_oversea['firstCarrierCost']-df_oversea['dutyCost'] + df_oversea['new_firstCarrierCost'],
                               df_oversea['totalCost'] - df_oversea['firstCarrierCost'] + df_oversea['new_firstCarrierCost'])
    df_oversea.drop(columns=['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax'], inplace=True, axis=1)
    #
    # 空运头程
    data_air = {'site':['UK','DE', 'AU','US'],
                'norm_price':[38.5, 35, 39, 47.5],
                'sens_price':[42.5, 40, 44, 51.5],
                'tax_rate':[0.27, 0, 0, 0]}
    oversea_air = pd.DataFrame(data=data_air)
    #
    df_oversea = pd.merge(df_oversea, oversea_air, how='left', on=['site'])
    df_oversea['体积重'] = df_oversea['长'] * df_oversea['宽'] * df_oversea['高'] / 6000
    df_oversea['计费重'] = df_oversea['重量'] / 1000
    df_oversea.loc[df_oversea['体积重'] > df_oversea['计费重'], '计费重'] = df_oversea.loc[df_oversea['体积重'] > df_oversea['计费重'], '体积重']
    df_oversea['体积'] = df_oversea['长'] * df_oversea['宽'] * df_oversea['高'] / (100 * 100 * 100)
    #
    # 运费、关税
    df_oversea['运费'] = df_oversea['计费重'] * df_oversea['norm_price']
    df_oversea['关税'] = (df_oversea['运费'] + df_oversea['成本'] * 0.3) * df_oversea['tax_rate']
    # 暂时不考虑关税
    df_oversea['new_first_air'] = df_oversea['运费']
    #
    df_oversea['total_cost_air'] = df_oversea['total_cost'] - df_oversea['firstCarrierCost'] + df_oversea['new_first_air']
    df_oversea.drop(['norm_price','sens_price','tax_rate','体积重','计费重','体积','运费','关税',
                     'total_cost','shipping_cost','firstCarrierCost','price','weight_volume',
                     'new_first_sea','new_first_air'], axis=1, inplace=True)
    return df_oversea

def get_fba_price(df, table_name='sku_to_fba_fee_20240403'):
    """
    计算国内仓发FBA转寄的定价
    """
    # df = get_sku()
    # df = merge_site(df)
    # ##
    # # 尺寸重量
    # conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # df['数量'] = 1
    # df = chicun_zhongliang(df, 1, conn_mx)
    # ##
    # df.drop(['cost', '重量来源'], axis=1, inplace=True)
    df['sku1'] = df['sku']
    df['country_code'] = df['site']
    #
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    result_ky = aut(df[df['site'].isin(['US','AU'])], conn_mx, f_type='空运')
    result_kh = aut(df[df['site'].isin(['UK','DE'])], conn_mx, f_type='卡航')
    result = pd.concat([result_kh, result_ky])
    #
    # df = merge_first_product_line(df)
    #
    df_fast = df.merge(result, on=["sku1", "country_code"], how='left')
    df_fast['fba头程属性'].fillna('普货', inplace=True)
    # df_fast.rename(columns={"sku1": "sku"}, inplace=True)
    df_fast['站点1'] = df_fast['站点']
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_fast = tou_cheng_dingjiabi(df_fast, conn, dingjia_type='快运')
    #
    df_fast.drop(['sku1','数量','country_code','站点1','计费方式'], axis=1, inplace=True)

    # 尾程
    df_fast['最长边'] = np.max(df_fast[['长', '宽', '高']], axis=1)
    df_fast['次长边'] = np.median(df_fast[['长', '宽', '高']], axis=1)
    df_fast['最短边'] = np.min(df_fast[['长', '宽', '高']], axis=1)
    df_fast['长度 + 周长'] = df_fast['最长边'] + (df_fast['次长边'] + df_fast['最短边']) * 2
    #
    conn = connect_to_sql(database='domestic_warehouse_clear', data_sys='数据部服务器')
    us = df_fast[df_fast['site']=='US']
    us = fbafee_us(us, conn)

    au = df_fast[df_fast['site']=='AU']
    au = fbafee_au(au, conn)

    eu = df_fast[df_fast['site'].isin(['DE','UK'])]
    eu = fbafee_eu(eu, conn)

    df_fba = pd.concat([us,au,eu])
    df_fba.drop(['最长边','次长边','最短边','长度 + 周长'], axis=1, inplace=True)
    df_fba = df_fba.rename(columns={'重量':'weight','尺寸分段':'size_segment'})
    #
    # 存表
    # 表准备
    conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_today = datetime.datetime.today().strftime('%Y-%m-%d')
    # table_name = f'sku_to_fba_fee_20240403'
    sql = f"""
        DROP TABLE IF EXISTS yibai_oversea.{table_name}
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表删除成功！')
    sql = f"""
        CREATE TABLE IF NOT EXISTS yibai_oversea.{table_name}
        (
        `id` Int64,
        `date` String,
        `sku` String,
        `site` String COMMENT 'site',
        `站点` String COMMENT '站点',
        `weight` Float64 COMMENT '重量',
        `成本` Float64 COMMENT '成本',
        `长` Float64 COMMENT '长',
        `宽` Float64 COMMENT '宽',
        `高` Float64 COMMENT '高',
        `fba头程属性` String COMMENT 'fba头程属性',
        `头程费_人民币` Float64 COMMENT '头程费_人民币',
        `size_segment` String COMMENT '尺寸分段',
        `update_time` String COMMENT '更新时间'
        )
        ENGINE = MergeTree
        ORDER BY (sku, `site`)
        SETTINGS index_granularity = 8192
    """
    conn_mx2.ck_create_table(sql)
    print('结果表建立成功！')
    sql = f"""
        ALTER TABLE yibai_oversea.{table_name}
        DELETE where date = '{date_today}'
    """
    conn_mx2.ck_execute_sql(sql)
    print('结果表今日数据删除成功！')

    df_fba = df_fba.fillna(0)
    df_fba['date'] = date_today
    for i in df_fba['site'].unique():
        conn_mx2 = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        conn_mx2.ck_insert(df_fba[df_fba['site']==i], table_name, if_exist='append')
    #
    # 取数计算
    today = datetime.date.today().strftime('%Y%m%d')
    is_high_season = int((today >= '20231015') & (today <= '20240114'))
    p_fee = 0.15
    gross_profit = 0.2
    sql = f"""
    select 
        a.*,
        round((toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb))/(1-{p_fee}-{gross_profit}-0.04-0.013),2) as  `sku_fba_price`    
    from (
        select  
            C.site site,
            C.`成本` product_cost,
            C.sku sku,
            C.`头程费_人民币` as `first_trip_fee_rmb`,
            e.rate rate,
            C.weight,
            C.weight/28.34 AS `盎司`,
            case
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '小号标准尺寸： 不超过 6 盎司'
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '小号标准尺寸： 6 至 12 盎司（不含 6 盎司）'
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '小号标准尺寸： 12 至 16 盎司（不含 12 盎司）'
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then '大号标准尺寸： 不超过 6 盎司'
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then '大号标准尺寸： 6 至 12 盎司（不含 6 盎司）'
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then '大号标准尺寸： 12 至 16 盎司（不含 12 盎司）'
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then '大号标准尺寸： 1 至 2 磅（不含 1 磅）'
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then '大号标准尺寸： 2 至 20 磅（不含 2 磅）'
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then '小号大件： 不超过 2 磅'
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then '小号大件： 2 至 30 磅（不含 2 磅）'
                when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then '小号大件： 超过 30 磅'
                when  C.size_segment like '%中号大件%' then '中号大件'
                when  C.size_segment like '%大号大件%' then '大号大件'
                when  C.size_segment like '%特殊大件%' then '特殊大件'
            else null end as `尺寸`,
            case
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16
                when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 32.88
                when  C.size_segment like '%中号大件%' then 25.25
                when  C.size_segment like '%大号大件%' then 118.8
                when  C.size_segment like '%特殊大件%' then 189.19
            else null end as `初始收费`,
            case
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 0
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 0
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 0
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 0
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.62
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 0
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 0.62
                when  C.size_segment like '%小号大件%' and C.weight/453.59 BETWEEN  30.00001 and 99  then 0.62
                when  C.size_segment like '%中号大件%' then 0.62
                when  C.size_segment like '%大号大件%' then 1.16
                when  C.size_segment like '%特殊大件%' then 1.21
            else null end as `初续重收费`,
            case
                when C.size_segment like '%小号标准尺寸%' then 0.2
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  0.00001 and 2 then 0.3
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 0.5
                when C.size_segment  like '%小号大件%' then 1
                when  C.size_segment like '%中号大件%' then 2.5
                when  C.size_segment like '%大号大件%' then 2.5
                when  C.size_segment like '%特殊大件%' then 2.5
            else null end as `旺季增长费用`,
            case
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.15 + 0.2 * {is_high_season}
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 7.8 + 0.2 * {is_high_season}
                when C.size_segment like '%小号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.25 + 0.2 * {is_high_season}
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  0 and 6 then 7.35 + 0.3 * {is_high_season}
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  6.00001 and 12 then 8.2 + 0.3 * {is_high_season}
                when C.size_segment  like '%大号标准尺寸%' and C.weight/28.34 BETWEEN  12.00001 and 16 then 8.5 + 0.3 * {is_high_season}
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  1.00001 and 2 then 9.5 + 0.3 * {is_high_season}
                when C.size_segment  like '%大号标准尺寸%' and C.weight/453.59 BETWEEN  2.00001 and 20 then 9.5+(C.weight/453.59-2)*0.62 + 0.5 * {is_high_season}
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 16 + 1 * {is_high_season}
                when C.size_segment  like '%小号大件%' and C.weight/453.59 BETWEEN  2.00001 and 30 then 16+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
                when  C.size_segment like '%小号大件%' and C.weight/453.59 > 30 then 32.88+(C.weight/453.59-2)*0.62 + 1 * {is_high_season}
                when  C.size_segment like '%中号大件%' and C.weight/453.59 BETWEEN  0 and 2 then 25.25 + 2.5 * {is_high_season}
                when  C.size_segment like '%中号大件%' and C.weight/453.59 > 2 then 25.25+(C.weight/453.59-2)*0.62 + 2.5 * {is_high_season}
                when  C.size_segment like '%大号大件%' and C.weight/453.59 BETWEEN 0 and 90 then 118.8 + 2.5 * {is_high_season}
                when  C.size_segment like '%大号大件%' and C.weight/453.59 > 90 then 118.8+(C.weight/453.59-90)*1.16 + 2.5 * {is_high_season}
                when  C.size_segment like '%特殊大件%' and C.weight/453.59 BETWEEN 0 and 90 then 189.19 + 2.5 * {is_high_season}
                when  C.size_segment like '%特殊大件%' and C.weight/453.59 > 90 then 189.19+(C.weight/453.59-90)*1.21 + 2.5 * {is_high_season}
            else null end as `fba_to_other_fees`,
            C.size_segment
        from yibai_oversea.{table_name} as C
        LEFT JOIN (
            SELECT distinct country ,rate 
            FROM domestic_warehouse_clear.erp_rate
            WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
        ) e ON C.site = e.country
        where C.site='US'
    ) a

    UNION ALL

    select 
        a.*,
        round((toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb))/(1-{p_fee}-{gross_profit}-0.04-0.013-0.1667),2) as  `sku_fba_price`    
    from (
        select  
            C.site site,
            C.`成本` product_cost,
            C.sku sku,
            C.`头程费_人民币` as `first_trip_fee_rmb`,
            e.rate rate,
            C.weight,
            C.weight/28.34 AS `盎司`,
            case
                when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'      
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'       
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'     
                when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克' 
                when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'         
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'  
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'      
                when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'     
                when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'              
                when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'  
                when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克' 
                when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克' 
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
            else null end as `尺寸`,
            case
                when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 3.35     
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 3.37      
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 3.39       
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 3.41       
                when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 3.43
                when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 3.43   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 3.71       
                when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 4.18      
                when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 4.91          
                when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 5.74           
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 6.56            
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 7.9             
                when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 9.39              
                when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 9.8               
                when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 10.2               
                when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 10.81                 
                when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 11.21                  
                when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 11.61                   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 12.02                   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 12.42                     
                when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 12.83
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 9.47     
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 11.18
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 11.7
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 12.2      
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 12.4                 
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 11
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 12.5      
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 13.12      
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 14.87      
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 14.99     
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 16.48     
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 16.88   
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 17.2     
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 18.12  
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 19.02     
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 20.22      
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 21.84      
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 25.71     
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 29.75
                when C.size_segment like '%标准大件%' and C.weight > 29760 then 0.99*(C.weight/1000)
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 16.31    
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 20.72      
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23      
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 24.2      
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 28      
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 33
                when C.size_segment like '%大号大件%' and C.weight > 31500 then 0.99*(C.weight/1000)
            else null end as "初始收费",
            0 as "初续重收费",
            0 as `旺季增长费用`,
            `初始收费`+`初续重收费` as "fba_to_other_fees", 
            C.size_segment
        from yibai_oversea.{table_name} as C
        LEFT JOIN (
            SELECT distinct country ,rate 
            FROM domestic_warehouse_clear.erp_rate
            WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
        ) e ON C.site = e.country
        where C.site='UK'
    ) a

    UNION ALL

    select 
        a.*,
        round((toFloat64(product_cost)+toFloat64(fba_to_other_fees)*rate+toFloat64(first_trip_fee_rmb))/(1-{p_fee}-{gross_profit}-0.04-0.013-0.1597),2) as  `sku_fba_price`    
    from (
        select  
            C.site site,
            C.`成本` product_cost,
            C.sku sku,
            C.`头程费_人民币` as `first_trip_fee_rmb`,
            e.rate rate,
            C.weight,
            C.weight/28.34 AS `盎司`,
            case
                when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then '小号信封：不超过 80 克'      
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then '标准信封：不超过 60 克'       
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then '标准信封：不超过 210 克'
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then '标准信封：不超过 460 克'
                when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'
                when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then '大号信封：不超过 960 克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then '标准包裹：不超过 150 克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then '标准包裹：不超过 400 克'     
                when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then '标准包裹：不超过 900 克' 
                when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then '标准包裹：不超过 1.4 千克'         
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then '标准包裹：不超过 1.9 千克'  
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then '标准包裹：不超过 2.9 千克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then '标准包裹：不超过 3.9 千克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then '标准包裹：不超过 4.9 千克'   
                when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then '标准包裹：不超过 5.9 千克'
                when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then '标准包裹：不超过 6.9 千克'      
                when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then '标准包裹：不超过 7.9 千克'     
                when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then '标准包裹：不超过 8.9 千克'              
                when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then '标准包裹：不超过 9.9 千克'  
                when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then '标准包裹：不超过 10.9 千克' 
                when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then '标准包裹：不超过 11.9 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then '小号大件：不超过 760 克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then '小号大件：不超过 1.01 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then '小号大件：不超过 1.26 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then '小号大件：不超过 1.51 千克'
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then '小号大件：不超过 1.76 千克' 
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then '标准大件：不超过 760 克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then '标准大件：不超过 1.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then '标准大件：不超过 2.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then '标准大件：不超过 3.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then '标准大件：不超过 4.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then '标准大件：不超过 5.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then '标准大件：不超过 6.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then '标准大件：不超过 7.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then '标准大件：不超过 8.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then '标准大件：不超过 9.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then '标准大件：不超过 14.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then '标准大件：不超过 19.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then '标准大件：不超过 24.76 千克'
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then '标准大件：不超过 29.76 千克'
                when C.size_segment like '%标准大件%' and C.weight > 29760 then '标准大件 > 29.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then '大号大件：不超过 4.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then '大号大件：不超过 9.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then '大号大件：不超过 14.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then '大号大件：不超过 19.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then '大号大件：不超过 24.76 千克'
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then '大号大件：不超过 31.5 千克'
                when C.size_segment like '%大号大件%' and C.weight > 31500 then '大号大件：> 31.5 千克'
            else null end as `尺寸`,
            case
                when C.size_segment like '%小号信封%' and C.weight BETWEEN  0 and 80 then 5.09
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  0 and 60 then 5.29
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  60.0001 and 210 then 5.45
                when C.size_segment like '%标准信封%' and C.weight BETWEEN  210.0001 and 460 then 5.55
                when C.size_segment like '%大号信封%' and C.weight BETWEEN  0 and 960 then 5.87
                when C.size_segment like '%超大号信封%' and C.weight BETWEEN  0 and 960 then 5.87
                when C.size_segment like '%包裹%' and C.weight BETWEEN  0 and 150 then 6.19
                when C.size_segment like '%包裹%' and C.weight BETWEEN  150.0001 and 400 then 6.76
                when C.size_segment like '%包裹%' and C.weight BETWEEN  400.0001 and 900 then 7.58
                when C.size_segment like '%包裹%' and C.weight BETWEEN  900.0001 and 1400 then 8.45
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1400.0001 and 1900 then 9.53
                when C.size_segment like '%包裹%' and C.weight BETWEEN  1900.0001 and 2900 then 10.55
                when C.size_segment like '%包裹%' and C.weight BETWEEN  2900.0001 and 3900 then 11.59
                when C.size_segment like '%包裹%' and C.weight BETWEEN  3900.0001 and 4900 then 12.02
                when C.size_segment like '%包裹%' and C.weight BETWEEN  4900.0001 and 5900 then 12.86
                when C.size_segment like '%包裹%' and C.weight BETWEEN  5900.0001 and 6900 then 13.6
                when C.size_segment like '%包裹%' and C.weight BETWEEN  6900.0001 and 7900 then 14.63
                when C.size_segment like '%包裹%' and C.weight BETWEEN  7900.0001 and 8900 then 15.37
                when C.size_segment like '%包裹%' and C.weight BETWEEN  8900.0001 and 9900 then 16.4
                when C.size_segment like '%包裹%' and C.weight BETWEEN  9900.0001 and 10900 then 17.44
                when C.size_segment like '%包裹%' and C.weight BETWEEN  10900.0001 and 11900 then 18.98
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  0 and 760 then 12.74
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  760.0001 and 1010 then 13.1
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1010.0001 and 1260 then 13.47
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1260.0001 and 1510 then 13.83
                when C.size_segment like '%小号大件%' and C.weight BETWEEN  1510.0001 and 1760 then 14.49
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  0 and 760 then 15.15
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  760.0001 and 1760 then 15.76
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  1760.0001 and 2760 then 16.37
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  2760.0001 and 3760 then 16.98
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  3760.0001 and 4760 then 17.62
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  4760.0001 and 5760 then 18.24
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  5760.0001 and 6760 then 18.85
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  6760.0001 and 7760 then 19.46
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  7760.0001 and 8760 then 20.07
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  8760.0001 and 9760 then 20.68
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  9760.0001 and 14760 then 23.74
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  14760.0001 and 19760 then 26.81
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  19760.0001 and 24760 then 29.87
                when C.size_segment like '%标准大件%' and C.weight BETWEEN  24760.0001 and 29760 then 33.33
                when C.size_segment like '%标准大件%' and C.weight > 29760 then 1.11*(C.weight/1000)
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  0 and 4760 then 30.39
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  4760.0001 and 9760 then 33.17
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  9760.0001 and 14760 then 35.95
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  14760.0001 and 19760 then 38.72
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  19760.0001 and 24760 then 41.5
                when C.size_segment like '%大号大件%' and C.weight BETWEEN  24760.0001 and 31500 then 45.78
                when C.size_segment like '%大号大件%' and C.weight > 31500 then 1.45*(C.weight/1000)
            else null end as "初始收费",
            0 as "初续重收费",
            0 as `旺季增长费用`,
            `初始收费`+`初续重收费` as "fba_to_other_fees", 
            C.size_segment
        from yibai_oversea.{table_name} as C
        LEFT JOIN (
            SELECT distinct country ,rate 
            FROM domestic_warehouse_clear.erp_rate
            WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
        ) e ON C.site = e.country
        where C.site='DE'
    ) a
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_fba_price = conn_mx.ck_select_to_df(sql)
    col = ['sku', 'site','product_cost','weight','尺寸','size_segment','fba_to_other_fees','first_trip_fee_rmb','rate','sku_fba_price']
    df_fba_price = df_fba_price[col]
    # write_to_sql(df_fba_price, 'sku_to_fba_price')

    return df_fba_price

def main(table_name):
    """
    汇总计算
    """
    # 获取目标sku。可以是国内仓全量sku，也可以是指定sku
    # df = get_sku()
    df = pd.read_excel('F:\Desktop\国内仓sku发海外仓定价_补充20240403.xlsx')

    # 获取站点（笛卡尔积）
    df = merge_site(df)
    # 尺寸重量
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_mx)
    df.drop(['重量来源'], axis=1, inplace=True)
    print(df.info())
    # 通过接口拉取海外仓运费数据，并存入ck
    get_oversea_fee(df, table_name='sku_to_oversea_fee_20240403')

    # 获取海外仓运费数据，需替换头程
    df_oversea = get_oversea_totalcost(df, table_name='sku_to_oversea_fee_20240403')

    # 国内仓及虚拟仓运费获取
    conn_ck = pd_to_ck(database='yibai_temp_hxx', data_sys='数据部服务器')
    df = df.reset_index(drop=False)
    df_domestic = bendiyunfei(df, conn_ck, 'old_freight_interface_ebay',warehouse_id=478)
    df_virtual = bendiyunfei(df, conn_ck, 'old_freight_interface_ebay',warehouse_id=481)
    df_domestic.rename(columns={'运费':'国内仓总运费','ship_name':'国内仓发货渠道'}, inplace=True)
    df_virtual.rename(columns={'运费':'虚拟仓总运费','ship_name':'虚拟仓发货渠道'}, inplace=True)

    # 获取FBA转寄运费。sku详细数据需提前存入CK, 方便计算运费
    df.drop('index', axis=1, inplace=True)
    df_fba_price = get_fba_price(df, table_name='sku_to_fba_fee_20240403')

    # 各业务仓定价数据汇总
    df_1 = pd.merge(df, df_domestic[['sku','site','国内仓总运费','国内仓发货渠道']], how='left', on=['sku','site'])
    df_1 = pd.merge(df_1, df_virtual[['sku','site','虚拟仓总运费','虚拟仓发货渠道']], how='left', on=['sku','site'])
    # FBA
    df_1 = pd.merge(df_1, df_fba_price[['sku','site','fba_to_other_fees','first_trip_fee_rmb','rate','sku_fba_price']],how='left', on=['sku','site'])
    # 海外仓
    df_1 = pd.merge(df_1, df_oversea[['sku','site','warehouse_id','warehouse_name','total_cost_air','total_cost_sea']],how='left', on=['sku','site'])
    #
    print('数据存表...')
    write_to_sql(df_1, table_name)

def result_to_excel(table_name):
    """
    计算结果存excel
    """
    # table_name = 'sku_to_anywhere_price_20240403'
    #
    sql = f"""
         SELECT
             sku, site, `重量`, `成本`, `长`, `宽`, `高`, `国内仓总运费`, `虚拟仓总运费`, (fba_to_other_fees*rate+first_trip_fee_rmb) `FBA转寄总运费`,
             warehouse_name, total_cost_air `海外仓总运费（头程空运）`, total_cost_sea `海外仓总运费（头程海运）`, 0.15 `平台佣金费率`,
             0.04 `汇兑&库损费率`,
             case
                 when site = 'AU' then 0.015
                 when site = 'DE' then 0.0175
                 when site = 'UK' then 0.0145
                 when site = 'US' then 0.013
             end as `paypal费率`,
             case
                 when site = 'DE' then 0.1597
                 when site = 'UK' then 0.1667
                 else 0
             end as `vat费率`,
             0.2 `国内仓_毛利率`, 0.18 `虚拟仓_毛利率1`, 0.25 `虚拟仓_毛利率2`, 0.25 `海外仓_毛利率`, 0.2 `FBA转寄_毛利率`,
             sku_fba_price
         FROM over_sea.{table_name}
     """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    sku_tuple = tuple(df['sku'].unique())
    sql = f"""
            WITH 
            [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
            ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
            '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
            '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
            '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr,
            [1,2,3,10] as resource_type_list, ['正常','停产','缺货','停产找货中'] as resource_list

            select 
                a.sku sku, c.product_status product_status, c.resource_type resource_type, d.sales_status sales_status,
                c.title_cn title_cn, b.path_name as `产品线路线` 
            from yb_datacenter.yb_product a
            left join yb_datacenter.yb_product_linelist b
            on toInt32(a.product_linelist_id) = toInt32(b.id)
            LEFT JOIN (
                select distinct sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack,
                transform(product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
                transform(resource_type, resource_type_list, resource_list, '正常') as resource_type
                from yibai_prod_base_sync.yibai_prod_sku
            ) c ON a.sku = c.sku
            LEFT JOIN (
                SELECT sku, status1 as sales_status
                FROM domestic_warehouse_clear.yibai_domestic_warehouse_clear_status
                WHERE end_time is Null
            ) d ON a.sku = d.sku
            WHERE a.sku in {sku_tuple}
        """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_line = conn_ck.ck_select_to_df(sql)
    # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #                      db_name='support_document')
    # df_line = ck_client.ck_select_to_df(sql)
    df_line['一级产品线'] = df_line['产品线路线'].str.split('>>', expand=True)[0]
    #
    df.iloc[:, 2:10] = df.iloc[:, 2:10].astype(float)
    df.iloc[:, 11:] = df.iloc[:, 11:].astype(float)
    df = pd.merge(df, df_line, how='left', on=['sku'])
    #
    df['国内仓定价_人民币'] = (df['成本'] + df['国内仓总运费']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['国内仓_毛利率'])
    df['虚拟仓18%定价_人民币'] = (df['成本'] + df['虚拟仓总运费']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['vat费率'] - df['虚拟仓_毛利率1'])
    df['虚拟仓25%定价_人民币'] = (df['成本'] + df['虚拟仓总运费']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['vat费率'] - df['虚拟仓_毛利率2'])
    df['海外仓空运定价_人民币'] = (df['成本'] + df['海外仓总运费（头程空运）']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['vat费率'] - df['海外仓_毛利率'])
    df['海外仓海运定价_人民币'] = (df['成本'] + df['海外仓总运费（头程海运）']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['vat费率'] - df['海外仓_毛利率'])
    df['FBA转寄定价_人民币'] = (df['成本'] + df['FBA转寄总运费']) / (
                1 - df['平台佣金费率'] - df['汇兑&库损费率'] - df['paypal费率'] - df['vat费率'] - df['FBA转寄_毛利率'])
    #
    df.drop(['sku_fba_price'], axis=1, inplace=True)
    df.to_excel(f'df_sku_to.xlsx', index=0)
    # for s in ['AU','UK','DE','US']:
    #     sql = f"""
    #         SELECT
    #             sku, site, `重量`, `成本`, `长`, `宽`, `高`, `国内仓总运费`, `虚拟仓总运费`, (fba_to_other_fees*rate+first_trip_fee_rmb) `FBA转寄总运费`,
    #             warehouse_name, total_cost_air `海外仓总运费（头程空运）`, total_cost_sea `海外仓总运费（头程海运）`, 0.15 `平台佣金费率`,
    #             0.04 `汇兑&库损费率`,
    #             case
    #                 when site = 'AU' then 0.015
    #                 when site = 'DE' then 0.0175
    #                 when site = 'UK' then 0.0145
    #                 when site = 'US' then 0.013
    #             end as `paypal费率`,
    #             case
    #                 when site = 'DE' then 0.1597
    #                 when site = 'UK' then 0.1667
    #                 else 0
    #             end as `vat费率`,
    #             0.18 `国内仓_毛利率`, 0.18 `虚拟仓_毛利率1`, 0.25 `虚拟仓_毛利率2`, 0.25 `海外仓_毛利率`, 0.2 `FBA转寄_毛利率`,
    #             sku_fba_price
    #         FROM over_sea.{table_name}
    #         WHERE site = '{s}'
    #     """
    #     conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #     df = conn.read_sql(sql)
    #     #
    #     df.iloc[:,2:10] = df.iloc[:,2:10].astype(float)
    #     df.iloc[:,11:] = df.iloc[:,11:].astype(float)
    #     df = pd.merge(df, df_line, how='left', on=['sku'])
    #     #
    #     df['国内仓定价_人民币'] = (df['成本']+df['国内仓总运费'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['国内仓_毛利率'])
    #     df['虚拟仓18%定价_人民币'] = (df['成本']+df['虚拟仓总运费'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['vat费率']-df['虚拟仓_毛利率1'])
    #     df['虚拟仓25%定价_人民币'] = (df['成本']+df['虚拟仓总运费'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['vat费率']-df['虚拟仓_毛利率2'])
    #     df['海外仓空运定价_人民币'] = (df['成本']+df['海外仓总运费（头程空运）'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['vat费率']-df['海外仓_毛利率'])
    #     df['海外仓海运定价_人民币'] = (df['成本']+df['海外仓总运费（头程海运）'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['vat费率']-df['海外仓_毛利率'])
    #     df['FBA转寄定价_人民币'] = (df['成本']+df['FBA转寄总运费'])/(1-df['平台佣金费率']-df['汇兑&库损费率']-df['paypal费率']-df['vat费率']-df['FBA转寄_毛利率'])
    #     #
    #     df.drop(['sku_fba_price'],axis=1, inplace=True)
    #     df.to_excel(f'df_{s}.xlsx', index=0)

##



##

if __name__ == '__main__':
    # get_oversea_fee()
    # get_fba_price()
    # get_trip_fee_oversea2
    table_name = 'sku_to_anywhere_price_20240403'
    # main(table_name)
    result_to_excel(table_name)