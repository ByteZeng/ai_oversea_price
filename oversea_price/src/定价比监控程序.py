"""

"""

##
import pandas as pd
import numpy as np
import datetime, time
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd,pd_to_ck
import warnings
warnings.filterwarnings('ignore')
import tqdm

##
def pr_monitor():
    ### sku维度数据监控
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    table_now = f"pricing_ratio_listing_{time.strftime('%Y%m%d')}"
    sql_table = f"""
        select distinct table from system.parts
        where database = 'support_document'
        and table like 'pricing_ratio_listing%'
        and table <= \'{table_now}\'
        order by table desc
        limit 10
    """
    df_table = conn_ck.ck_select_to_df(sql_table)
    #
    date_today = time.strftime('%Y%m%d')
    df_all = pd.DataFrame()
    for t in df_table['table'].unique():
        sql = f"""
            SELECT date, `站点`, `建议物流方式`, count(1) as sku_cnt
            FROM support_document.{t}
            GROUP BY date, `站点`, `建议物流方式`
        """
        conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
        df_all = pd.concat([df, df_all])
    #
    def map_values(s):
        # 分割字符串，根据包含的分隔符来决定使用哪种分割方式
        if ';' in s:
            parts = s.split(';')
        elif ':' in s:
            parts = s.split(':')
        else:
            parts = [s]  # 如果没有分隔符，直接将字符串作为单个元素处理
        # 映射到对应的值，并过滤掉不在字典中的键
        dic = {'11': '快海', '12': '慢海', '13': '铁路', '15': '快卡', '16': '慢卡', '6': '空运', '0': '不建议发FBA',
               '-1': '不建议发FBA'}
        mapped_parts = [dic.get(p, p) for p in parts]
        # 合并结果，用分号分隔
        return ';'.join(mapped_parts)

    df_all['建议物流方式2'] = df_all['建议物流方式'].map(map_values)
    #
    df_all['维度'] = 'sku'
    df_all = df_all.rename(columns={'sku_cnt':'数量'})

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df_all, 'ads_pricing_ratio', if_exists='replace')
    #####  链接维度数据
    date_today = time.strftime('%Y%m%d')
    df_2 = pd.DataFrame()
    for t in df_table['table'].unique():
        sql = f"""
            SELECT date, site as `站点`, proposed_transport as `建议物流方式`, count(1) as `数量`, 'listing' as `维度` 
            FROM support_document.{t}
            GROUP BY date, site, proposed_transport
        """
        conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
        df = conn_ck.ck_select_to_df(sql)
        df_2 = pd.concat([df, df_2])

    df_2['建议物流方式2'] = df_2['建议物流方式'].map(map_values)
    #
    conn.to_sql(df_2, 'ads_pricing_ratio', if_exists='append')

def contrast_listing_pr():
    """
    对比链接维度定价比数据
    """
    sql = """
        SELECT account_id, proposed_transport, count(1) as cnt
        FROM yibai_sale_center_amazon.yibai_amazon_price_link_rate 
        GROUP BY account_id, proposed_transport
    """
    conn = connect_to_sql(database='yibai_sale_center_amazon', data_sys='新数仓')

    df_temp = conn.read_sql(sql)

    #
    # 获取账号数据
    sql = """
        SELECT id as account_id, site_code
        FROM yibai_sale_center_system.yibai_system_account
        WHERE platform_code = 'AMAZON'
    """
    conn_ck = pd_to_ck(database='yibai_sale_center_amazon', data_sys='调价明细历史数据')
    df_site = conn_ck.ck_select_to_df(sql)
    #
    df_temp['account_id'] = df_temp['account_id'].astype(int)
    df = pd.merge(df_temp, df_site, how='left', on=['account_id'])
    df_final = df.groupby(['site_code','proposed_transport'])['cnt'].sum().reset_index()

    # 站点转义
    sql = """
        SELECT site1 site, site3 site_code
        FROM domestic_warehouse_clear.site_table
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_site_code = conn_ck.ck_select_to_df(sql)
    #
    df_final = pd.merge(df_final, df_site_code, how='left', on=['site_code'])
    # df_final = df_final.rename(columns={'site_code':'site'})
    # 获取计算的链接比数据
    date_today =  time.strftime('%Y%m%d')
    sql = f"""
        SELECT date, site , proposed_transport, count(1) as `数量`, 'listing' as `维度` 
        FROM support_document.pricing_ratio_listing_{date_today}
        GROUP BY date, site, proposed_transport
    """
    conn_ck = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    df_2 = conn_ck.ck_select_to_df(sql)
    df_result = pd.merge(df_final, df_2, how='left', on=['site','proposed_transport'])
    #
    df_result['cnt_org'] = df_result['cnt_org'].fillna(0).astype(int)
    #
    df_result.to_excel('df_result.xlsx', index=0)

def temp():
    sku_info = pd.read_excel('F:\Desktop\Fyndiq平台在售产品.xlsx')
    #
    sku_info = sku_info.rename(columns={'易佰云SKU':'sku'})
    sku_info['sku'] = sku_info['sku'].astype(str)
    sku_tuple = tuple(sku_info['sku'].unique())
    #
    step = 200
    df_result = pd.DataFrame()
    for i in tqdm.tqdm(range(int(len(sku_tuple)/step)+1)):
        sku_temp = sku_tuple[i*step:(i+1)*step]
        sql = f"""
            select distinct sku, title_cn, pur_length_pack,pur_width_pack,pur_height_pack,
            if(toFloat64(weight_out_storage)=0, toFloat64(pur_weight_pack), toFloat64(weight_out_storage)) as `gross`
            from yibai_prod_base_sync.yibai_prod_sku
            WHERE sku in {sku_temp}
        """
        conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')
        df_title = conn_ck.ck_select_to_df(sql)
        df_result = pd.concat([df_title, df_result])
    #
    df_result = df_result.drop_duplicates(subset=['sku'])
    df_final = pd.merge(sku_info, df_result, how='left', on='sku')

    df_final.to_excel('df_final.xlsx', index=0)
#####

import numpy as np


# 目标函数
def fitness_function(x):
    return x * np.sin(10 * np.pi * x)


# 编码
def encode(x, min_value, max_value, bit_length):
    return np.round((x - min_value) / (max_value - min_value) * (2 ** bit_length - 1))


# 解码
def decode(x, min_value, max_value, bit_length):
    return x * (max_value - min_value) / (2 ** bit_length - 1) + min_value


# 初始化种群
population_size = 200
bit_length = 10
population = np.random.randint(0, 2, (population_size, bit_length))

##
# 遗传算法主循环
max_iterations = 1000
for iteration in range(max_iterations):
    # 计算适应度
    decimal_population = np.sum(population * 2 ** np.arange(population.shape[1])[::-1], axis=1)
    fitness_values = np.array([fitness_function(decode(x, 0, 10, bit_length)) for x in decimal_population])
    #
    # 选择
    # 确保适应度值是一维的
    fitness_values = fitness_values.flatten()
    #
    # 归一化
    min_value = np.min(fitness_values)
    max_value = np.max(fitness_values)
    # 设置偏移量
    offset = 0.01
    normalized_array = (fitness_values - min_value) / (max_value - min_value)  * (1 - 2 * offset) + offset
    #
    selected_indices = np.random.choice(np.arange(population_size), size=population_size, replace=True,
                                        p=(normalized_array / np.sum(normalized_array)))
    selected_population = population[selected_indices]
    #
    # 交叉
    crossover_point = np.random.randint(0, bit_length)
    for i in range(0, population_size, 2):
        selected_population[i, crossover_point:] = selected_population[i + 1, crossover_point:]
        selected_population[i + 1, crossover_point:] = selected_population[i, crossover_point:]

    # 变异
    for i in range(population_size):
        for j in range(bit_length):
            if np.random.rand() < 0.01:  # 变异概率
                selected_population[i, j] = 1 - selected_population[i, j]
    #
    # 替代
    population = selected_population
#
# 解码最终种群中的最佳个体
best_individual = population[np.argmax(fitness_values)]
decimal_population = np.sum(best_individual * 2**np.arange(len(best_individual))[::-1])
best_x = decode(decimal_population, 0, 10, bit_length)
best_fitness = fitness_function(best_x)
print(f"最大值所在位置: {best_x}, 最大值为: {best_fitness}")
##### 测试
