"""
Amazon 定价商城
定价比数据
0、取目标sku
1、先算运费
2、再算定价比
3、最后取字段
"""
##
import pandas as pd
import numpy as np
import datetime, time
import os
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.base_api.adjust_price_function_amazon import shuilv,fanou_fanmei
from pulic_func.price_adjust_web_service.daingjia_public import get_amazon_referral_fee, chicun_zhongliang
from all_auto_task.pricing_ratio_monitor import merge_first_product_line, merge_four_dim_diff, get_diff_new, \
    get_cost_range
##
def get_sku():
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    # 产品表里取sku
    t1 = time.time()
    # sku_list = get_temp_sku()
    sql = f"""
        select
            distinct sku, title_cn, product_status, `sku改在售时间`, new_price, `重量`, `长`, `宽`, `高`
        from (
            SELECT 
                distinct sku, title_cn, product_status, toDate(b.end_time) as `sku改在售时间`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`,
                case 
                    when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
                    else toFloat64(weight_out_storage) 
                end as `重量`,
                toFloat64(pur_length_pack) as `长`,
                toFloat64(pur_width_pack) as `宽`,
                toFloat64(pur_height_pack) as `高`
            FROM yibai_prod_base_sync.yibai_prod_sku a
            LEFT JOIN yibai_prod_base_sync.yibai_prod_spu b
            ON a.spu=b.spu
        ) a
        INNER JOIN (
            SELECT DISTINCT sku
            FROM yibai_prod_base_sync.yibai_prod_sku_select_attr
            where attr_value_id = 67
            ) b
        ON a.sku = b.sku
        WHERE 1 = 1
        and a.new_price < 50 
        and `重量` < 450 
        and `长` < 40 and `宽` < 40 and `高` < 40 
        and a.new_price > 0
        """
    df = conn_mx.ck_select_to_df(sql)
    t2 = time.time()
    # df = df.sample(10000)
    # df = df[df['sku'].isin(sku_list)]
    print(f'获取sku完成，共{len(df)}条！共耗时{t2 - t1:.2f}s')
    df.drop(['new_price','重量','长','宽','高'], axis=1, inplace=True)
    pro_dic = {1:'已创建', 2:'已开发', 3:'待买样', 4:'待品检', 5:'待编辑', 6:'待拍摄', 7:'待编辑待拍摄', 8:'待修图', 9:'在售中',
               10:'审核不通过', 11:'停售', 12:'待清仓', 13:'已滞销', 14:'待物流审核', 15:'待关务审核',  16:'ECN资料变更中', 17:'ECN资料变更驳回'}
    df['product_status'] = df['product_status'].replace(pro_dic)
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_mx, 0)

    c1 = (df['成本'] < 50) & (df['重量'] < 450) & (df['长'] < 35) & (df['宽'] < 20) & (df['高'] < 12)
    df = df[c1]
    df.drop(['数量','重量来源'], axis=1, inplace=True)
    OUNCE_FACTOR = 28.3495
    inches = 2.54
    df['盎司'] = df['重量'] / OUNCE_FACTOR
    df['体积重'] = (df['长'] / inches) * (df['宽'] / inches) * (df['高'] / inches) / 139
    df['体积重'] = 16 * df['体积重']
    df['计费重'] = np.where(df['体积重'] > df['盎司'], df['体积重'], df['盎司'])

    df['头程体积重'] = df['长'] * df['宽'] * df['高'] / 6000
    df['头程计费重'] = np.where(df['头程体积重'] > (df['重量']/1000), df['头程体积重'], df['重量']/1000)

    # # 筛选
    df = get_line(df)
    df = get_ban_info(df)
    df_attr = get_sku_attr()
    df = pd.merge(df, df_attr, how='left', on=['sku'])
    df = df[~df['产品形态属性'].str.contains('液体|膏体', na=False)]
    df = df[~df['产品电池属性'].isin(['内置电池','配套电池','纯电池及移动电源'])]

    print(df.info())

    # CK存表
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn_ck.ck_insert(df, 'temp_amazon_lowprice_sku', if_exist='replace')
    # df.to_excel('F://Desktop//df_sku_i.xlsx', index=0)
    return df


def get_temp_sku():
    """ """
    df = pd.read_excel('F://Ding_workspace//低价商城热销品汇总sku匹配0319.xlsx', dtype={'sku':str})
    print(df.info())
    sku_list = tuple(df['sku'].unique())

    return sku_list

# 侵权违禁
def get_ban_info(df):
    # 侵权违禁信息数据读取
    # 读取侵权信息数据表
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')

    sql_info = f"""
    SELECT distinct sku, risk_grade_type as `侵权等级` 
    FROM yibai_prod_base_sync.yibai_prod_inf_country_grade
    WHERE is_del = 0 and country_code = 'US' 
    -- and risk_grade_type in ('III','IV','V')
    """
    df_info = conn_ck.ck_select_to_df(sql_info)
    df_info['sku'] = df_info['sku'].astype('str')
    # 读取禁售信息数据表
    sql_js = f"""
    SELECT distinct sku ,risk_grade_type as `禁售等级` 
    FROM yibai_prod_base_sync.yibai_prod_forbidden_grade
    WHERE is_del = 0 and platform_code in ('Amazon-us','AMAZON') 
    -- and risk_grade_type in ('III','IV','V')
    """
    df_js = conn_ck.ck_select_to_df(sql_js)
    df_js['sku'] = df_js['sku'].astype('str')

    df = pd.merge(df, df_js, on='sku', how='left')
    df = df[~df['禁售等级'].isin(['III','IV','V'])]
    df = pd.merge(df, df_info, on='sku', how='left')
    df = df[~df['侵权等级'].isin(['III','IV','V'])]

    return df

def get_line(df):
    # 一级产品线
    sql_line = f"""
            select a.sku sku, title_cn `产品名称`, b.category_path as `产品线路线` 
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
    df = pd.merge(df, df_line[['sku','一级产品线','二级产品线']], how='left', on=['sku'])
    df = df[~df['一级产品线'].isin(['母婴用品','宠物用品','玩具产品'])]
    # df = df[~df['二级产品线'].str.contains('母婴|宠物|玩具')]

    return df
##
def get_sku_attr():
    """带电、材质"""

    dict={1:'产品基础属性',2:'产品特殊属性',5:'产品电池属性',25:'产品插头属性',32:'产品违禁属性',
          40:'产品形态属性',66:'订单属性',142:'产品电压属性',168:'产品包装属性'}
    attr_list=pd.DataFrame()
    for i in dict.keys():
        sql=f"""
        SELECT
            a.sku,
            CASE
                WHEN b.parent_id = {i} THEN
                b.attribute_name 
            END AS  `{dict[i]}`
        FROM yibai_prod_base_sync.yibai_prod_sku_select_attr a
        LEFT JOIN yibai_prod_base_sync.yibai_prod_attributes_tms b ON a.attr_value_id = b.id 
        WHERE
            b.parent_id = {i} and a.attr_type_id=1
            LIMIT 1000
        """
        conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
        df=df.groupby('sku').apply(lambda x:';'.join(set(x['{}'.format(dict[i])]))).rename('{}'.format(dict[i])).reset_index()
        df=df.drop_duplicates()
        # 属性_df.head(1)
        attr_list = pd.concat([df, attr_list])

    # print(attr_list.info())
    col = ['sku', '产品形态属性', '产品违禁属性','产品电池属性']
    attr_list = attr_list[col]

    return attr_list

# a = get_sku_attr()
# a.to_excel('F://Desktop//a.xlsx', index=0)
##
# 获取国内仓运费
def warehouse_mark(s, codes):
    for code in codes:
        # 替换前缀，包括连字符
        if s.startswith(f"{code}-"):
            s = s[len(code) + 1:]
        # 替换后缀，包括连字符
        elif s.endswith(f"-{code}"):
            s = s[:-len(code) - 1]
        elif s.endswith(f"{code}"):
            s = s[:-len(code)]
    return s
def get_fbm_fee(df_fbm):
    """
    获取国内仓运费
    """
    warehouse_code = ['US', 'AU', 'DE', 'GB', 'FR', 'IT', 'ES', 'CA']
    df_fbm['new_sku'] = df_fbm['sku'].apply(lambda x: warehouse_mark(x, warehouse_code))
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = pd.DataFrame()
    for i in df_fbm['站点'].unique():
        df_temp = df_fbm[df_fbm['站点'] == i]
        sql = f"""
            SELECT distinct sku new_sku, total_cost as `fbm运费`,site as `站点`  
            from domestic_warehouse_clear.freight_interface_amazon 
            WHERE site = '美国'
            order by update_date DESC limit 1 by sku
            """
        df_yunfei0 = conn_ck.ck_select_to_df(sql)
        df_temp = pd.merge(df_temp, df_yunfei0, how='left', on=['new_sku', '站点'])
        df_yunfei1 = df_yunfei0.copy()
        df_yunfei1 = df_yunfei1.rename(columns={'new_sku':'sku','fbm运费':'fbm运费_new'})
        df_temp = pd.merge(df_temp, df_yunfei1, how='left', on=['sku', '站点'])
        df_temp['fbm运费'] = np.where(df_temp['fbm运费'].isna(), df_temp['fbm运费_new'], df_temp['fbm运费'])
        df_temp.drop('fbm运费_new', axis=1, inplace=True)
        df = pd.concat([df_temp, df])
    print(f'获取运费后的数量共{len(df)}条.')
    return df


def get_low_price_fee(df, line1='非服装服饰'):
    """ 计算低价商城的运费 """
    # 重量单位：盎司， 价格单位：美元
    price_ranges = {
        (0, 4): 0.88,
        (4, 8): 1.77,
        (8, 12): 2.60,
        (12, 16): 3.22,
        (16, 20): 3.72,
        (20, 24): 4.42,
        (24, 28): 5.11,
        (28, 32): 5.81,
        (32, 40): 6.83,
        (40, 48): 8.19,
        (48, 56): 9.59,
        (56, 64): 11.05,
        (64, 9999): 12.51
    }
    if line1 == '服装服饰':
        price_ranges = {
            (0, 4): 0.99,
            (4, 8): 2.05,
            (8, 12): 2.84,
            (12, 16): 3.48,
            (16, 20): 4.14,
            (20, 24): 4.90,
            (24, 28): 5.60,
            (28, 32): 6.31,
            (32, 40): 7.27,
            (40, 48): 8.70,
            (48, 56): 10.07,
            (56, 64): 11.15,
            (64, 9999): 12.23
        }
    def calculate_shipping(weight):
        for (lower_bound, upper_bound), price_per_ounce in price_ranges.items():
            if lower_bound < weight <= upper_bound:
                return price_per_ounce
        return price_ranges[(64, 9999)]  # 默认使用最大区间的单价

    # 应用函数计算运费
    df['运费'] = df['计费重'].apply(calculate_shipping)

    return df

def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate 
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)

    return df_rate

def get_fbm_diff(df_temp):
    """

    """
    df_diff = get_diff_new()
    df_diff_fbm = df_diff[(df_diff['shipping_type'] == 1)][
        ['站点', 'first_product_line', 'cost_range', 'net_profit2', 'net_interest_rate_target']]
    df_temp['cost'] = df_temp['成本']
    df_fbm = get_cost_range(df_temp, df_diff_fbm)
    df_fbm = merge_four_dim_diff(df_fbm, df_diff_fbm, ['站点', 'first_product_line', 'cost_range'])
    df_fbm['net_profit2'] = (df_fbm['net_profit2'] / 100).astype('float')
    df_fbm['net_interest_rate_target'] = (df_fbm['net_interest_rate_target'] / 100).astype('float')
    # 目标净利率设置为10%
    df_fbm['net_interest_rate_target'] = 0.1
    df_fbm['FBM目标毛利润率'] = df_fbm['net_profit2'] + df_fbm['net_interest_rate_target']
    df_fbm['FBM差值'] = df_fbm['net_profit2']
    col = ['sku', '站点', 'FBM差值','net_interest_rate_target','FBM目标毛利润率']
    df_fbm = df_fbm[col]
    df_fbm.columns = ['sku', '站点', 'FBM差值','目标净利率','FBM目标毛利润率']
    df_temp = pd.merge(df_temp, df_fbm, how='left', on=['sku', '站点'])
    df_temp = df_temp.drop_duplicates()
    df_temp.drop(['first_product_line','cost'], axis=1, inplace=True)
    # 其他费率项
    df_temp['平台佣金'] = 0.15
    df_temp['冗余系数'] = 0.04
    return df_temp

# sku销量
def get_sku_sales(df):
    """
    获取sku近30天平均日销、今年123月平均日销
    """
    sql = """

    SELECT  
        SKU as sku,
        SUM(30days_sales)/30 AS '近30天平均日销'
    FROM (
        SELECT 
            SKU,3days_sales,7days_sales,15days_sales,30days_sales,60days_sales,90days_sales
        FROM `yibai_sku_sales_statistics` a 
        WHERE 
            platform_code not in ('DIS','WYLFX') and warehouse_id in (478, 481)
        )A 
    GROUP BY SKU

    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_30_sales = conn.read_sql(sql)

    sql = """
        SELECT sku, sum(`sku数量`)/91 as `今年123月平均日销`
        FROM domestic_warehouse_clear.monitor_dom_order
        WHERE created_time >= '2025-01-01' and created_time < '2025-04-01'
        GROUP BY sku
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_123 = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_30_sales, how='left', on=['sku'])
    df = df.sort_values(by='近30天平均日销', ascending=False)
    df = pd.merge(df, df_123, how='left', on=['sku'])

    return df

def sales_temp():
    sql = """
        SELECT sku, sum(`sku数量`)/91 as `今年123月平均日销`
        FROM domestic_warehouse_clear.monitor_dom_order
        WHERE created_time >= '2025-01-01' and created_time < '2025-04-01'
        GROUP BY sku
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_123 = conn_ck.ck_select_to_df(sql)

    df_123.to_excel('F://Desktop//df_123.xlsx', index=0)


def get_dom_stock(df):
    """ 国内仓库存 """
    sql = """
    select 
        sku, sum(stock) `在库库存`
    from domestic_warehouse_clear.domestic_warehouse_age
    group by sku
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_stock = conn_mx.ck_select_to_df(sql)

    df = pd.merge(df, df_stock, how='left', on=['sku'])

    return df
##
def get_refound(df):
    """ 获取国内仓sku退款率 """

    df_refound = pd.read_excel('F://Desktop//国内仓SKU真实退款率情况20250523.xlsx', dtype={'SKU':str})
    df_refound = df_refound[['SKU','平均真实退款率（近5个有销月份）']]
    df_refound.columns = ['sku','近5个月平均退款率']

    print(df_refound.info())
    df = pd.merge(df, df_refound, how='left', on=['sku'])

    return df

##
def main():
    """ 主函数 """
    # 指定的基文件夹路径
    base_folder = r"F:\yibai-price-strategy\data"

    # 获取当前日期并格式化
    current_date = time.strftime("%Y-%m-%d")
    new_folder_path = os.path.join(base_folder, current_date, '低价商城定价比')

    # 如果文件夹不存在，则创建文件夹
    if not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)
    print('文件夹创建成功...')

    df = get_sku()
    df['站点'] = '美国'
    df_rate = get_rate()
    df['us_rate'] = df_rate[df_rate['country']=='US'].iloc[0,2]
    df = get_low_price_fee(df)
    df = df.rename(columns={'运费':'非服装类运费'})
    df = get_low_price_fee(df, line1='服装服饰')
    df['低价商城运费'] = np.where(df['一级产品线']=='服装服饰', df['运费'], df['非服装类运费'])
    df['低价商城运费'] = df['低价商城运费'] * df['us_rate']
    # 头程单价
    toucheng_price = 1.32
    df['头程运费'] = df['头程计费重'] * toucheng_price
    df.drop(['盎司','体积重','运费','非服装类运费'], axis=1, inplace=True)
    df = get_fbm_fee(df)

    df = merge_first_product_line(df)
    df = get_fbm_diff(df)

    # 计算价格
    df['低价商城定价'] = (df['成本'] + df['头程运费'] + df['低价商城运费']) / (1-df['平台佣金']-df['冗余系数']-df['FBM目标毛利润率'])
    df['FBM定价'] = (df['成本'] + df['fbm运费']) / (1-df['平台佣金']-df['冗余系数']-df['FBM目标毛利润率'])
    df['定价比'] = df['低价商城定价']/df['FBM定价']
    print(df.info())

    # 获取日销数据
    print('获取日销数据...')
    df = get_sku_sales(df)

    # 获取库存数据
    print('获取库存数据...')
    df = get_dom_stock(df)

    print('获取退款率数据...')
    df = get_refound(df)

    print(df.info())
    # df.to_excel('F://Desktop//df_low_price.xlsx', index=0)

    # 筛选定价 < 20美金的
    df = df[(df['低价商城定价']/df['us_rate']) < 25]
    print(f'筛除后的数量共{len(df)}条...')
    #
    # Excel文件名
    excel_filename = "低价商城定价比.xlsx"
    # 完整的Excel文件路径
    excel_file_path = os.path.join(new_folder_path, excel_filename)
    # 将DataFrame保存为Excel文件
    df.to_excel(excel_file_path, index=False)

def temp_sku():
    """ """
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
    sql = f"""
        select
            distinct sku, title_cn, product_status, `sku改在售时间`, new_price, `重量`, `长`, `宽`, `高`
        from (
            SELECT 
                distinct sku, title_cn, product_status, toDate(b.end_time) as `sku改在售时间`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `new_price`,
                case 
                    when toFloat64(weight_out_storage)=0 then toFloat64(pur_weight_pack) 
                    else toFloat64(weight_out_storage) 
                end as `重量`,
                toFloat64(pur_length_pack) as `长`,
                toFloat64(pur_width_pack) as `宽`,
                toFloat64(pur_height_pack) as `高`
            FROM yibai_prod_base_sync.yibai_prod_sku a
            LEFT JOIN yibai_prod_base_sync.yibai_prod_spu b
            ON a.spu=b.spu
        ) a
        WHERE 
        `重量` > 2000 
        and a.new_price > 0
        """
    df = conn_ck.ck_select_to_df(sql)

    df = get_dom_stock(df)

    df = df[df['在库库存'] > 3]
    print(df.info())
    df.to_excel('F://Desktop//df_dom.xlsx', index=0)


##
if __name__ == '__main__':
    # get_sku()
    main()
    # df = get_sku()
    # sales_temp()

    # temp_sku()
