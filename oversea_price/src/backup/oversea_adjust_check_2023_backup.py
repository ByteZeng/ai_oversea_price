##
import time
import pandas as pd
import numpy as np
import os
from pulic_func.base_api.mysql_connect import connect_to_sql
from requests.auth import HTTPBasicAuth
import requests
import pulic_func.base_api.upload_zip as uz
from all_auto_task.scripts_ck_client import CkClient

##
def zj_qujian(df, col='涨降幅度'):
    df['涨降幅区间'] = 'L.1~'
    df.loc[(df[col] < -1), '涨降幅区间'] = 'A.-1~'
    df.loc[(df[col] >= -1) & (df[col] < -0.5), '涨降幅区间'] = 'B.-1~-0.5'
    df.loc[(df[col] >= -0.5) & (df[col] < -0.2), '涨降幅区间'] = 'C.-0.5~-0.2'
    df.loc[(df[col] >= -0.2) & (df[col] < -0.1), '涨降幅区间'] = 'D.-0.2~-0.1'
    df.loc[(df[col] >= -0.1) & (df[col] < -0.05), '涨降幅区间'] = 'E.-0.1~-0.05'
    df.loc[(df[col] >= -0.05) & (df[col] < 0), '涨降幅区间'] = 'F.-0.05~0'

    df.loc[(df[col] >= 0) & (df[col] < 0.05), '涨降幅区间'] = 'G.0~0.05'
    df.loc[(df[col] > 0.05) & (df[col] <= 0.1), '涨降幅区间'] = 'H.0.05~0.1'
    df.loc[(df[col] > 0.1) & (df[col] <= 0.2), '涨降幅区间'] = 'I.0.1~0.2'
    df.loc[(df[col] > 0.2) & (df[col] <= 0.5), '涨降幅区间'] = 'J.0.2~0.5'
    df.loc[(df[col] > 0.5) & (df[col] <= 1), '涨降幅区间'] = 'K.0.5~1'
    return df

def lv_qujian(df, col='今天较上次定价利润率涨降', new_col='今天较上次定价利润率涨降分段'):

    df[new_col] = 'k.[0.3,∞)'
    df.loc[df[col] < -0.3, new_col] = 'a.(-∞，-0.3)'
    df.loc[(df[col] >= -0.3) & (df[col] < -0.151), new_col] = 'b.[-0.3,-0.15)'
    df.loc[(df[col] >= -0.151) & (df[col] < -0.1), new_col] = 'c.[-0.15,-0.1)'
    df.loc[(df[col] >= -0.1) & (df[col] < -0.05), new_col] = 'd.[-0.1,-0.05)'
    df.loc[(df[col] >= -0.05) & (df[col] < 0), new_col] = 'e.[-0.05,0)'
    df.loc[(df[col] == 0), new_col] = 'f.0'
    df.loc[(df[col] > 0) & (df[col] < 0.05), new_col] = 'g.(0,0.05)'
    df.loc[(df[col] >= 0.05) & (df[col] < 0.1), new_col] = 'h.[0.05,0.1)'
    df.loc[(df[col] >= 0.1) & (df[col] < 0.15), new_col] = 'i.[0.1,0.15)'
    df.loc[(df[col] >= 0.15) & (df[col] < 0.3), new_col] = 'j.[0.15,0.3)'
    return df

def yunfei_qujian(df, col='今天较上次运费涨降', new_col='今天较上次运费涨降分段'):
    df[new_col] = 'o.[200,∞)'
    df.loc[df[col] < -200, new_col] = 'a.(-∞，-200)'
    df.loc[(df[col] >= -200) & (df[col] < -150), new_col] = 'b.[-200,-150)'
    df.loc[(df[col] >= -150) & (df[col] < -100), new_col] = 'c.[-150,-100)'
    df.loc[(df[col] >= -100) & (df[col] < -50), new_col] = 'd.[-100,-50)'
    df.loc[(df[col] >= -50) & (df[col] < -10), new_col] = 'e.[-50,-10)'
    df.loc[(df[col] >= -10) & (df[col] < -5), new_col] = 'f.[-10,-5)'
    df.loc[(df[col] >= -5) & (df[col] < 0), new_col] = 'g.[-5,0)'
    df.loc[(df[col] == 0), new_col] = 'h.0'
    df.loc[(df[col] > 0) & (df[col] < 5), new_col] = 'i.(0,5)'
    df.loc[(df[col] >= 5) & (df[col] < 10), new_col] = 'j.(5,10)'
    df.loc[(df[col] >= 10) & (df[col] < 50), new_col] = 'k.[10,50)'
    df.loc[(df[col] >= 50) & (df[col] < 100), new_col] = 'l.[50,100)'
    df.loc[(df[col] >= 100) & (df[col] < 150), new_col] = 'm.[100,150)'
    df.loc[(df[col] >= 150) & (df[col] < 200), new_col] = 'n.[150,200)'
    return df

def get_new_last_date():
    # 获取当前日期、上一次调价日期
    time_today = time.strftime('%Y-%m-%d')
    sql = f"""
    SELECT max(date_id)
    FROM dwm_oversea_price_dtl
    WHERE date_id < '{time_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_max_date = conn.read_sql(sql=sql)
    time_last = df_max_date.iloc[0, 0]
    return time_today, time_last

## 调价数据检查，对比关键信息
def contrast_info(platform='AMAZON', table_name='oversea_amazon_listing_upload_temp'):
    # 获取当前日期、上一次调价日期
    time_today, time_last = get_new_last_date()

    # 获取待调价链接
    sql = f"""
    SELECT distinct * 
    FROM {table_name} 
    where date_id = (SELECT max(date_id) FROM {table_name})
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql=sql)

    # 价格涨降幅度
    df['涨降幅度'] = 999
    df['涨降幅度'] = np.where(df['online_price'] > 0, (df['price'] - df['online_price']) / df['online_price'], df['涨降幅度'])
    # df.loc[df['online_price'] > 0, '涨降幅度'] = (df['price'] - df['online_price']) / df['online_price']
    df = zj_qujian(df)

    # 计算在线价格的实际净利率
    df['new_price'] = df['new_price'].astype(float)
    # print(df.info())
    df['online_profit'] = 1 - df['ppve'] - (df['total_cost'] + df['new_price']) / (
                df['online_price'] * df['rate']) - df['platform_zero']
    df['今天较当前在线利润率涨降'] = df['target_profit_rate'] - df['online_profit']
    df = lv_qujian(df,'今天较当前在线利润率涨降', '今天较当前在线利润率涨降分段')

    # 获取今日与昨日调价数据对比：净利率、运费、最优子仓、渠道
    sql = f"""
    select
        a.sku,a.warehouse,a.country,a.target_profit_rate as target_profit_rate_t, a.total_cost total_cost_t,a.best_warehouse_name best_warehouse_name_t,
        a.ship_name ship_name_t,b.target_profit_rate target_profit_rate_y,b.total_cost total_cost_y,b.best_warehouse_name best_warehouse_name_y,b.ship_name ship_name_y
    from (
        select *,row_number() over(partition by sku,warehouse,country,platform order by total_cost) as flag
        from dwm_oversea_price_dtl
        where date_id='{time_today}' and platform='{platform}') a
    left join (
        select *,row_number() over(partition by sku,warehouse,country,platform order by total_cost) as flag
        from dwm_oversea_price_dtl
        where date_id='{time_last}' and platform='{platform}') b
    on a.sku=b.sku and a.warehouse=b.warehouse and a.country=b.country
    where a.flag=1 and b.flag=1
    """
    df_contrast = conn.read_sql(sql=sql)

    df_contrast['定价利润率变化'] = df_contrast['target_profit_rate_t'] - df_contrast['target_profit_rate_y']
    df_contrast['今天较上次运费涨降'] = df_contrast['total_cost_t'] - df_contrast['total_cost_y']
    # 渠道运费监控...
    df_contrast['是否换渠道'] = np.where(df_contrast['ship_name_t'] == df_contrast['ship_name_y'], 0, 1)
    df_contrast['是否换仓'] = np.where(df_contrast['best_warehouse_name_t'] == df_contrast['best_warehouse_name_y'], 0, 1)

    df_contrast = lv_qujian(df_contrast, '定价利润率变化', '定价利润率变化分段')
    df_contrast = yunfei_qujian(df_contrast)

    # 匹配上对比数据
    df = pd.merge(df, df_contrast, how='left', on=['sku', 'warehouse', 'country'])

    # 汇率变化对比
    sql = f"""
    select
        a.country, a.rate rate_t, b.rate rate_y
    from (
        select distinct country, rate
        from {table_name}
        where date_id='{time_today}' ) a
    left join (
        select distinct country, rate
        from {table_name}
        where date_id='{time_last}') b
    on a.country=b.country
    """
    df_rate_contrast = conn.read_sql(sql=sql)
    df_rate_contrast['汇率变化'] = (df_rate_contrast['rate_t'] - df_rate_contrast['rate_y']) / df_rate_contrast[
        'rate_y']
    df_rate_contrast['汇率变化分段'] = pd.cut(df_rate_contrast['汇率变化'],
                                              bins=[-1000, -0.01, -0.005, 0, 0.005, 0.01, 1000],
                                              labels=['A:<-1%', 'B:(-1%,-0.5%]', 'C:(-0.5%,0]', 'D:(0,0.5%]',
                                                      'E:(0.5%,1%]', 'F:>1%'])
    df = pd.merge(df, df_rate_contrast, how='left', on=['country'])
    print('platform:{}'.format(platform))
    print(df.groupby(['涨降幅区间'])['sku'].count().reset_index())
    print(df.groupby(['今天较当前在线利润率涨降分段', 'sales_status'])['sku'].count().unstack())
    file_path = os.path.join('/data', time_today)
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # # 排除测试数据
    # if platform == 'AMAZON':
    #     test_file_path = os.path.join('F:\\yibai-price-strategy\\data', 'amazon动销品测试数据1019.xlsx')
    #     df_test_amazon = pd.read_excel(test_file_path, engine='openpyxl')
    #     df_test_amazon['account_id'] = df_test_amazon['account_id'].astype(int)
    #     df = pd.merge(df, df_test_amazon[['account_id', 'seller_sku', 'sku','group']], how='left', on=['account_id','seller_sku','sku'])
    #     df = df[df['group'].isna()]
    #     df.drop('group', axis=1, inplace=True)
    # elif platform == 'EB':
    #     test_file_path = os.path.join('F:\\yibai-price-strategy\\data', 'ebay正常品测试数据1019.xlsx')
    #     df_test_ebay = pd.read_excel(test_file_path, engine='openpyxl')
    #     df_test_ebay['account_id'] = df_test_ebay['account_id'].astype(int)
    #     df['account_id'] = df['account_id'].astype(int)
    #     df_test_ebay['item_id'] = df_test_ebay['item_id'].astype(str)
    #     df['item_id'] = df['item_id'].astype(str)
    #     df = pd.merge(df, df_test_ebay[['account_id', 'item_id', 'sku','group']], how='left', on=['account_id','item_id','sku'])
    #     df = df[df['group'].isna()]
    #     df.drop('group', axis=1, inplace=True)

    with pd.ExcelWriter(os.path.join(file_path, '{}调价数据检查_{}.xlsx'.format(platform, time_today))) as writer:
        df.to_excel(writer, sheet_name='{}'.format(platform), index=0)

    return df

##
# 匹配group_name
def get_oa_dep():
    def create_attr_token():
        token_url = "http://oauth.java.yibainetwork.com/oauth/" \
                    "token?grant_type=client_credentials"
        # 获取token
        token_request = requests.post(token_url, auth=HTTPBasicAuth("prod_libo", "libo321"))
        token_dict_content = token_request.json(strict=True)
        now_token = token_dict_content['access_token']
        return now_token

    header = {'Content-Type': "application/json;charset=UTF-8"}
    base_url = f"http://rest.java.yibainetwork.com/oa/oaDepartment/getOaDepartment"
    # 合成url
    now_token = create_attr_token()
    url = f"{base_url}?access_token={now_token}"
    response = requests.post(url, json={'isDel': 0}, headers=header)
    response_result = response.json(strict=True)
    if "error_description" in response_result.keys():
        err_reason = response_result['error_description']
        if "Access token expired" in err_reason:
            now_token = create_attr_token()
            url = f"{base_url}?access_token={now_token}"
            response = requests.post(url, json={'isDel': 0}, headers=header)
            response_result = response.json(strict=True)
    return response_result
##
def get_top_dep():
    # 1085984-为最大层级, 54495400-为销售团队, 30046131-Amazon部
    total_data = (get_oa_dep())['data']
    # amazon_sale_dep_list = ["产品线团队", "海外仓团队", "精品团队", "自发货团队"]
    amazon_sale_dep_id = [1079248, 1079249, 1079250, 1079251]
    dep_child_list, dep_sale_df = pd.DataFrame(), pd.DataFrame()
    for one_top_dep in total_data:
        if one_top_dep["userNumberDetail"] != "" and one_top_dep['pid'] in amazon_sale_dep_id:
            now_dep = one_top_dep['name']
            short_dep = now_dep.split("部")[0] + "部" if "部" in now_dep \
                else now_dep.split("仓")[0] + "仓" if "仓" in now_dep else ""
            update_time = one_top_dep['updateTime']
            now_child_dep_list = list(map(
                lambda m_x: m_x.split("(")[0].strip() if "(" in m_x else m_x.split("（")[0].strip(),
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            # 当1个小组同时在两个大部时，优先取组中有括号的
            now_child_dep_list1 = list(map(
                lambda m_x: 1 if "(" in m_x or "（" in m_x else 2,
                [one_child['name'] for one_child in one_top_dep['children']]
            ))
            dep_child_list = pd.DataFrame({'dep_name': [short_dep] * len(now_child_dep_list), 'sale_group_name': now_child_dep_list,
                              'update_time': [update_time] * len(now_child_dep_list),
                              'sale_group_name1': now_child_dep_list1, })
            dep_sale_df = pd.concat([dep_sale_df, dep_child_list])
            # dep_sale_df = dep_sale_df.append(dep_child_list, sort=False)
    return dep_sale_df
# response_result = get_oa_dep()
def xiaozu_dabu():
    df = get_top_dep()
    df = df.sort_values(['sale_group_name1'], ascending=True)
    df = df.drop_duplicates(subset=['sale_group_name'], keep='first')
    df = df[['dep_name', 'sale_group_name']].drop_duplicates()
    df.rename(columns={'dep_name': '大部', 'sale_group_name': 'group_name'}, inplace=True)
    df = df.drop_duplicates()

    df1 = df[df['group_name'] == '深圳3组']
    if df1.shape[0] == 0:
        df1 = pd.DataFrame({'group_name': ['深圳3组'],
                            '大部': ['深圳产品线一部']})
        df = df.append(df1)
    return df

##
# 调价数据分发销售平台
def file_distrib(df, platform='AMAZON'):

    time_today = time.strftime('%Y%m%d')
    f_path = os.path.dirname(os.path.abspath('..'))  # 文件当前路径
    f_path1 = os.path.join(f_path, 'over_sea')
    f_path_ZIP = os.path.join(f_path1, platform, time_today, '海外仓调价数据')
    if not os.path.exists(f_path_ZIP):
        os.makedirs(f_path_ZIP)

    # 数据分发
    if platform == 'AMAZON':
        for item, group in df.groupby('group_name'):
            excel_name = os.path.join(f_path_ZIP, f'海外仓{platform}调价数据分发{item}_{time_today}.xlsx')
            # group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            if group.shape[0] < 1048576:
                group.to_excel(excel_name, sheet_name=f'{item}', index=False)
            else:
                group = group.reset_index(drop=True)
                group['index'] = group.index
                group['index'] = group['index'].apply(lambda m: int(m / 1000000))

                writer = pd.ExcelWriter(excel_name)
                for key, sub_group in group.groupby('index'):
                    sub_group.drop(['index'], axis=1, inplace=True)
                    sub_group.to_excel(writer, sheet_name=f'{item}{key}', index=False)
                writer.save()
                writer.close()
        excel_name_all = os.path.join(f_path_ZIP, f'海外仓{platform}调价数据分发全量_{time_today}.xlsx')
        df.to_excel(excel_name_all, sheet_name=f'AMAZON', index=False)
    else:
        # 非Amazon平台，按数量分文件
        df = df.reset_index(drop=True)
        df['index'] = df.index
        df['index'] = df['index'].apply(lambda m: int(m / 1000000))
        for item, group in df.groupby('index'):
            group.drop(['index'], axis=1, inplace=True)
            excel_name = os.path.join(f_path_ZIP, f'海外仓{platform}调价数据分发全量{item}_{time_today}.xlsx')
            group.to_excel(excel_name, sheet_name=f'{item}', index=False)

    # 分发销售平台
    uz.upload_zip(f_path1, platform, '209313')

##
# 定义检查断点函数，函数始终报错，需在airflow上手动mark
def wait_check():
    try:
        print(is_check)
    except IOError as n:
        print('还未检查完成！')

def write_to_ck():
    time_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM dwm_oversea_price_dtl
        WHERE date_id = '{time_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku_price_temp = conn.read_sql(sql)
    dwm_sku_price_temp['target_profit_y'] = dwm_sku_price_temp['target_profit_y'].fillna(2.0)
    print(dwm_sku_price_temp.info())
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',db_name='yibai_oversea')
    # ck_client.ck_execute_sql(sql='truncate table yibai_oversea.dwm_oversea_price_dtl_temp')
    n = 1
    while n < 5:
        print(n)
        sql = """
        SELECT count(1)
        FROM yibai_oversea.dwm_oversea_price_dtl_temp
        """
        df = ck_client.ck_select_to_df(sql)
        if df.iloc[0, 0] == 0:
            ck_client.write_to_ck_json_type('yibai_oversea.dwm_oversea_price_dtl_temp', dwm_sku_price_temp)
            break
        else:
            n += 1
            time.sleep(120)

def sku_write_to_ck():
    time_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT *
        FROM dwm_sku_temp_info
        WHERE date_id = '{time_today}'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    dwm_sku_info_temp = conn.read_sql(sql)
    print(dwm_sku_info_temp.info())
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',db_name='yibai_oversea')
    ck_client.ck_execute_sql(sql='truncate table yibai_oversea.dwm_oversea_price_dtl_temp')

    ck_client.write_to_ck_json_type('yibai_oversea.dwm_sku_temp_info_temp', dwm_sku_info_temp)
##
def add_listing():
    # 新增的海外仓链接刷价。（绕过【正常品且日销大于0.1】不降价逻辑）
    # 1、获取临时链接信息
    # 2、获取海外仓调价链接
    # 3、匹配数据
    # 4、上传数据

    df_data = pd.read_excel('F:\Desktop\武汉46组海外仓SKU.xlsx')
    #
    df_data = df_data[['short_name','seller_sku','sku','asin']]
    account_list = tuple(df_data['short_name'].unique())
    sql = f"""
        SELECT account_id, short_name, seller_sku, online_price, price,sales_status,is_uk_cdt,is_normal_cdt,is_small_diff,
        is_white_account,is_white_listing,is_fba_asin
        FROM over_sea.oversea_amazon_listing_all
        WHERE short_name in {account_list} and date_id = '2023-12-19'
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_price = conn.read_sql(sql=sql)
    #
    df = pd.merge(df_data, df_price, how='left', on=['seller_sku','short_name'])
    #
    df_final = df[(~df['online_price'].isna()) & (df['price'] < df['online_price']) & (df['is_uk_cdt']==0)&(df['is_fba_asin']==0)&
                  (df['is_small_diff']==0) & (df['is_normal_cdt']==1)]
    # df_final['price'] = (df_final['online_price'] + df_final['price']) / 2
    #
    time_today, time_last = get_new_last_date()
    file_path = os.path.join('/data', time_today)
    with pd.ExcelWriter(os.path.join(file_path, 'AMAZON补充调价数据20231219.xlsx')) as writer:
        df_final.to_excel(writer, sheet_name='{}'.format('AMAZON'), index=0)

##
# add_listing()
##
def main():
    """
    对各平台的调价数据进行对比检查，
    输出数据明细
    """
    # AMAZON、EB、WALMART、CDISCOUNT、WISH
    # table_name='oversea_amazon_listing_upload_temp'、'oversea_ebay_listing_upload_temp'

    df_amazon = contrast_info(platform='AMAZON', table_name='oversea_amazon_listing_upload_temp')
    file_distrib(df_amazon, 'AMAZON')
    print('Amazon数据分发完成')
    df_ebay = contrast_info(platform='EB', table_name='oversea_ebay_listing_upload_temp')
    file_distrib(df_ebay, platform='EB')
    print('EB数据分发完成')
    df_walmart = contrast_info(platform='WALMART', table_name='oversea_walmart_listing_upload_temp')
    file_distrib(df_walmart, platform='WALMART')
    print('Walmart数据分发完成')
    df_cd = contrast_info(platform='CDISCOUNT', table_name='oversea_cdiscount_listing_upload_temp')
    file_distrib(df_cd, platform='CDISCOUNT')
    print('CD数据分发完成')

    # 临时链接
    # add_listing()
    # print('临时调价链接输出完成')
    write_to_ck()
    print('调价数据CK备份完成。')

    # sku_write_to_ck()
##
if __name__ == "__main__":
    main()
    # sku_write_to_ck()
    print('done!')
