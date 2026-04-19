import datetime
import traceback
import pandas as pd
import numpy as np
import time
import warnings
from sqlalchemy import create_engine
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck

warnings.filterwarnings("ignore")



def get_date():
    qu_shu_date_str = datetime.date.today().isoformat()
    date_list = qu_shu_date_str.split("-")
    date_new = date_list[0] + date_list[1] + date_list[2]
    return date_new


def get_yesterday_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=1)).strftime("%Y%m%d")
    out_date_2 = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    return out_date, out_date_2

def print_err(df):
    """ 输出异常数量 """

    # 1. 筛选“差异>1%”的分段（排除 D 段）
    non_d_segments = df[df['总运费涨降幅度分段'] != 'D:(-0.01,0.01]']

    # 2. 按国家统计“差异>1%”的 SKU 总数
    # （国家列是 AE、AU、BE...US，所以先将数据转为长表方便分组）
    long_df = non_d_segments.melt(
        id_vars=['总运费涨降幅度分段'],
        var_name='country',
        value_name='sku_count'
    )
    # 统计每个国家的“差异>1%”的 SKU 总数
    country_diff_sum = long_df.groupby('country')['sku_count'].sum().reset_index()
    country_diff_sum.rename(columns={'sku_count': 'diff_gt_1pct_sku'}, inplace=True)

    # 3. 统计每个国家的总 SKU 数（含 D 段）
    total_sku_per_country = df.drop('总运费涨降幅度分段', axis=1).sum().reset_index()
    total_sku_per_country.columns = ['country', 'total_sku']

    # 4. 合并数据，计算比例
    country_stats = pd.merge(
        country_diff_sum,
        total_sku_per_country,
        on='country',
        how='left'
    )
    country_stats['diff_gt_1pct_ratio'] = (country_stats['diff_gt_1pct_sku'] / country_stats['total_sku']).fillna(0)

    # 5. 筛选触发报警的国家
    alert_countries = country_stats[
        (country_stats['diff_gt_1pct_sku'] > 5000) |
        (country_stats['diff_gt_1pct_ratio'] > 0.05)
        ]

    # 6. 输出结果
    if not alert_countries.empty:
        print("⚠️ 报警：以下国家差异>1%的SKU数量/比例超标：")
        print(alert_countries[['country', 'diff_gt_1pct_sku', 'diff_gt_1pct_ratio']])
    else:
        print("✅ 所有国家差异>1%的SKU数量/比例均未超标")


def yunfei_data_check(platform='AMAZON'):
    try:
        print("======= 海外仓运费数据检查 =======")
        print("1. 各国家数据量监控")
        a, msg_str = check_base_num(platform=platform)

        print("2. 各国家各子仓最便宜渠道运费涨降幅度监控")
        c, msg_str_2 = check_useful_fee(platform=platform)

    except:
        send_msg('动销组定时任务推送', '海外仓运费校验',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓运费校验出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-13922822326'], is_all=False,
                 status='失败')
        # '+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950',
        raise Exception(traceback.format_exc())
    else:
        if a > 0.05:
            send_msg('定价与订单物流运费获取一致性', '海外仓运费校验', msg_str,
                     mobiles=['+86-13922822326'], is_all=False)
            raise Exception(msg_str)
        elif c < 0.95:
            send_msg('定价与订单物流运费值校验', '海外仓运费校验', msg_str_2,
                     mobiles=['+86-13922822326'], is_all=False)
            raise Exception(msg_str_2)

def check_base_num(platform='AMAZON'):
    """ 检查各国家运费数量变化情况 """
    if platform == 'AMAZON':
        table_name = 'oversea_transport_fee_daily'
        n = 0
    elif platform == 'TEMU':
        table_name = 'oversea_transport_fee_daily_1'
        n = 1
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""SELECT count() as data FROM yibai_oversea.{table_name} otfd
                    where date_id = toYYYYMMDD(today()) - {n}"""
    df = ck_client.ck_select_to_df(sql)
    today_data = df['data'][0]
    print(today_data)
    sql = f"""SELECT count() as data FROM  yibai_oversea.{table_name}
    where date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < toYYYYMMDD(today())) - {n}"""
    df = ck_client.ck_select_to_df(sql)
    yesterday_data = df['data'][0]
    print(yesterday_data)
    a = abs((today_data - yesterday_data) / yesterday_data)
    b = '%.2f%%' % (a * 100)
    print(b)
    msg_str = f"""类型:{platform}海外仓运费拉取\n上一期运费总行数:{yesterday_data}\n今日运费总行数:{today_data}\n差异:{b}\n通知时间:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"""
    print(msg_str)
    # 国家维度的运费行数校验
    sql = f"""SELECT country, count() as num_t FROM yibai_oversea.{table_name} otfd
                    WHERE date_id = toYYYYMMDD(today()) - {n} GROUP BY country order by num_t DESC"""
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    sql = f"""
    SELECT country, count() as num_y FROM yibai_oversea.{table_name} otfd
    WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < toYYYYMMDD(today()) - {n})
    GROUP BY country """
    df_y = ck_client.ck_select_to_df(sql)
    df = pd.merge(df, df_y, how='left', on=['country'])
    df['num_y'] = df['num_y'].fillna(0)
    df['变化率'] = (df['num_t'] / df['num_y'] - 1).replace({np.inf, 1})
    df["变化率_显示"] = df["变化率"].map("{:.2%}".format)
    print(df)
    # print(f"运费国家共有{len(df['country'].unique())}, 具体为：\n{df['country'].value_counts()}")
    alert_countries = df[abs(df["变化率"]) > 0.05]["country"]

    # 1. 国家行数变化过大，打印提醒
    if not alert_countries.empty:
        print("\n" + f"⚠提醒：以下国家运费行数变化率超过5%：{', '.join(alert_countries)}")
    else:
        print("所有国家运费行数变化率均未超过5%")

    return a, msg_str

def check_all_fee(platform='AMAZON'):
    """ 检查所有渠道费用项变化 """
    if platform == 'AMAZON':
        table_name = 'oversea_transport_fee_daily'
        n = 0
    elif platform == 'TEMU':
        table_name = 'oversea_transport_fee_daily_1'
        n = 1
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT y.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                (toFloat64(overseasFee) + toFloat64(packFee)) as class2_y,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(firstCarrierCost) + toFloat64(dutyCost) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_y
            FROM yibai_oversea.{table_name}
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < toYYYYMMDD(today()) - {n}) 
            and platform = '{platform}'
        ) y
        settings max_memory_usage = 20000000000
    """
    df_fee = ck_client.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    sql = f"""
        SELECT t.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t,
                (toFloat64(overseasFee) + toFloat64(packFee)) as class2_t,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(firstCarrierCost) + toFloat64(dutyCost) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_t
            FROM yibai_oversea.{table_name}
            WHERE date_id = toYYYYMMDD(today() - {n}) 
            and platform = '{platform}'
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = ck_client.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    df_fee_all = pd.merge(df_fee, df_fee_2, how='left', on=['sku', 'warehouseName', 'shipName', 'country'])
    df_fee_all['第一类涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['第二类涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第三类涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 9999)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第一类涨价幅度分段'] = pd.cut(df_fee_all['第一类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第二类涨价幅度分段'] = pd.cut(df_fee_all['第二类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第三类涨价幅度分段'] = pd.cut(df_fee_all['第三类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.01)]) / len(df_fee_all)
    msg_str_2 = f"""类型:海外仓今日运费数据与最近运费数据对比：\n差异小于1%的数据量占比为：{'%.2f%%' % (c * 100)}\n"""
    print(msg_str_2)
    # d = df_fee_all.groupby(['总运费涨降幅度分段','country'])['sku'].count().reset_index()
    pivot_df = df_fee_all.pivot_table(index='总运费涨降幅度分段', columns='country', values='sku',
                                      aggfunc='count', fill_value=0).astype(int)
    long_df = pivot_df.reset_index()
    print(long_df.to_string(line_width=2000))
    print_err(long_df)

    return c, msg_str_2

def check_useful_fee(platform='AMAZON'):
    """ 检查最便宜渠道（调价使用）费用项变化 """
    if platform == 'AMAZON':
        table_name = 'oversea_transport_fee_daily'
        n = 0
    elif platform == 'TEMU':
        table_name = 'oversea_transport_fee_daily_1'
        n = 1
    # 显示所有列（解决列省略）
    pd.set_option('display.max_columns', None)  # 不限制列数
    pd.set_option('display.width', 2000)  # 增大输出宽度（避免换行导致列折叠）
    pd.set_option('display.expand_frame_repr', False)  # 禁止宽表格自动换行
    pd.set_option('display.unicode.ambiguous_as_wide', True)  # 把模糊字符视为全宽
    pd.set_option('display.unicode.east_asian_width', True)  # 按东亚字符宽度计算（适配中文/数字）

    # 1、各子仓最便宜渠道
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        SELECT y.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                (toFloat64(overseasFee) + toFloat64(packFee)) as class2_y,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(firstCarrierCost) + toFloat64(dutyCost) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_y
            FROM yibai_oversea.{table_name}
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < toYYYYMMDD(today()) - {n}) 
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName, country
        ) y
        settings max_memory_usage = 20000000000
    """
    df_fee = ck_client.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    sql = f"""
        SELECT t.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t,
                (toFloat64(overseasFee) + toFloat64(packFee)) as class2_t,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(firstCarrierCost) + toFloat64(dutyCost) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_t
            FROM yibai_oversea.{table_name}
            WHERE date_id = toYYYYMMDD(today()) - {n}
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName, country
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = ck_client.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    df_fee_all = pd.merge(df_fee, df_fee_2, how='outer', on=['sku', 'warehouseName', 'country'])
    df_fee_all['第一类涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['第二类涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第三类涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 9999)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第一类涨价幅度分段'] = pd.cut(df_fee_all['第一类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第二类涨价幅度分段'] = pd.cut(df_fee_all['第二类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第三类涨价幅度分段'] = pd.cut(df_fee_all['第三类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    b = df_fee_all[df_fee_all['total_cost_t'].isna()]
    print(f"今日运费缺失的sku+子仓+国家的数量共{len(b)}条，具体为：\n{b['country'].value_counts()}")
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.01)]) / len(df_fee_all)

    # df_temp = df_fee_all[df_fee_all['country']=='US']
    # df_temp.to_excel('F://Desktop//df_fee_temp.xlsx', index=0)

    msg_str_2 = f"""类型:{platform}海外仓今日最优渠道运费数据对比：\n差异小于1%的数据量占比为：{'%.2f%%' % (c * 100)}\n"""
    print(msg_str_2)
    # d = df_fee_all.groupby(['总运费涨降幅度分段','country'])['sku'].count().reset_index()
    pivot_df = df_fee_all.pivot_table(index='总运费涨降幅度分段', columns='country', values='sku',
                                      aggfunc='count', fill_value=0).astype(int)
    long_df = pivot_df.reset_index()
    print("\n" + long_df.to_string(line_width=2000))
    # print("\n" + long_df)
    print_err(long_df)

    return c, msg_str_2

def get_err_data_info_jp(platform='AMAZON'):
    """ 获取异常运费明细 """

    table_name = 'oversea_transport_fee_supplier'
    n = 0
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # n = 1
    sql = f"""
        SELECT y.*
        FROM (
            SELECT
                sku, warehouseName, shipName, shipCountry country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                (toFloat64(firstCarrierCost) + toFloat64(dutyCost)) as class2_y
            FROM yibai_oversea.{table_name}
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.{table_name} WHERE date_id < '2026-01-16')
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName,  shipCountry
        ) y
        settings max_memory_usage = 20000000000
    """
    df_fee = ck_client.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    print(len(df_fee))
    sql = f"""
        SELECT t.*
        FROM (
            SELECT
                sku, warehouseName, shipName, shipCountry country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t,
                (toFloat64(firstCarrierCost) + toFloat64(dutyCost)) as class2_t
            FROM yibai_oversea.{table_name}
            WHERE date_id = '2026-01-21'
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName, shipCountry
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = ck_client.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    print(len(df_fee_2))
    df_fee_all = pd.merge(df_fee, df_fee_2, how='outer', on=['sku', 'warehouseName', 'country'])
    df_fee_all['第一类涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['第二类涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 9999)
    # df_fee_all['第三类涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 9999)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第一类涨价幅度分段'] = pd.cut(df_fee_all['第一类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第二类涨价幅度分段'] = pd.cut(df_fee_all['第二类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    # df_fee_all['第三类涨价幅度分段'] = pd.cut(df_fee_all['第三类涨降幅度'],
    #                                           bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
    #                                           labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
    #                                                   'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    b = df_fee_all[df_fee_all['total_cost_t'].isna()]
    print(f"今日运费缺失的sku+子仓+国家的数量共{len(b)}条，具体为：\n{b['country'].value_counts()}")
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.01)]) / len(df_fee_all)

    # df_temp = df_fee_all[df_fee_all['country']=='DE']

    df_fee_all.to_excel('F://Desktop//df_fee_jp_temp.xlsx', index=0)



def get_err_data_info(platform='AMAZON'):
    """ 获取异常运费明细 """
    if platform == 'AMAZON':
        table_name = 'oversea_transport_fee_daily'
        n1 = 0
        n2 = 0
    elif platform == 'TEMU':
        table_name = 'oversea_transport_fee_daily_1'
        n1 = 1
        n2 = 1
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # n = 1
    sql = f"""
        SELECT y.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_y, toFloat64(shippingCost) as shipping_cost_y,
                (toFloat64(firstCarrierCost) + toFloat64(dutyCost)) as class2_y,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_y
            FROM yibai_oversea.{table_name}
            WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily WHERE date_id < toYYYYMMDD(today()) - {n1}) 
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            AND shipName not like '%%VC账号%%'
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName, country
        ) y
        settings max_memory_usage = 20000000000
    """
    df_fee = ck_client.ck_select_to_df(sql)
    df_fee = df_fee.drop_duplicates()
    sql = f"""
        SELECT t.*
        FROM (
            SELECT
                sku, warehouseName, shipName, country, toFloat64(totalCost) as total_cost_t, toFloat64(shippingCost) as shipping_cost_t,
                (toFloat64(firstCarrierCost) + toFloat64(dutyCost)) as class2_t,
                (toFloat64(remoteExtraFee) + toFloat64(extraSizeFee) + toFloat64(packTypeFee) + toFloat64(taxationFee) + toFloat64(drawPrice) + toFloat64(antidumpFee) + toFloat64(overseaPackageFee)) as class3_t
            FROM yibai_oversea.{table_name}
            WHERE date_id = toYYYYMMDD(today()) - {n2}
            and platform = '{platform}'
            and toFloat64(shippingCost) > 1  -- 去掉自提等尾程过小的渠道
            AND shipName not like '%%VC账号%%'
            ORDER BY totalCost ASC
            LIMIT 1 BY sku, warehouseName, country
        ) t
        settings max_memory_usage = 20000000000
    """
    df_fee_2 = ck_client.ck_select_to_df(sql)
    df_fee_2 = df_fee_2.drop_duplicates()
    df_fee_all = pd.merge(df_fee, df_fee_2, how='outer', on=['sku', 'warehouseName', 'country'])
    df_fee_all['第一类涨降幅度'] = df_fee_all['shipping_cost_t'] / df_fee_all['shipping_cost_y'] - 1
    df_fee_all['第二类涨降幅度'] = (df_fee_all['class2_t'] / df_fee_all['class2_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第三类涨降幅度'] = (df_fee_all['class3_t'] / df_fee_all['class3_y'] - 1).replace(np.inf, 9999)
    df_fee_all['总运费涨降幅度'] = (df_fee_all['total_cost_t'] / df_fee_all['total_cost_y'] - 1).replace(np.inf, 9999)
    df_fee_all['第一类涨价幅度分段'] = pd.cut(df_fee_all['第一类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第二类涨价幅度分段'] = pd.cut(df_fee_all['第二类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['第三类涨价幅度分段'] = pd.cut(df_fee_all['第三类涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    df_fee_all['总运费涨降幅度分段'] = pd.cut(df_fee_all['总运费涨降幅度'],
                                              bins=[-2, -0.4, -0.1, -0.01, 0.01, 0.1, 0.4, 3],
                                              labels=['A:<-0.4', 'B:(-0.4,-0.1]', 'C:(-0.1,-0.01]',
                                                      'D:(-0.01,0.01]', 'E:(0.01,0.1]', 'F:(0.1, 0.4]', 'G:0.4+'])
    b = df_fee_all[df_fee_all['total_cost_t'].isna()]
    print(f"今日运费缺失的sku+子仓+国家的数量共{len(b)}条，具体为：\n{b['country'].value_counts()}")
    c = len(df_fee_all[(df_fee_all['总运费涨降幅度'].abs() <= 0.01)]) / len(df_fee_all)

    df_temp = df_fee_all[df_fee_all['country'].isin(['US'])]

    df_temp.to_excel('F://Desktop//df_fee_temp.xlsx', index=0)


if __name__ == '__main__':
    # yunfei_data_check(platform='TEMU')

    # define_threshold()
    # check_useful_fee()

    get_err_data_info(platform='AMAZON')

    # get_err_data_info_jp(platform='AMAZON')
