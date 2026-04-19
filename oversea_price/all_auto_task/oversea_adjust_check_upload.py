import datetime,time
import pandas as pd
from pulic_func.base_api.mysql_connect import connect_to_sql
import warnings

warnings.filterwarnings('ignore')

def get_today_week():
    week_today = datetime.date.today().isoweekday()
    return week_today


def get_date():
    # today = datetime.date.today().isoformat()
    today = time.strftime('%Y-%m-%d')
    print(today)
    return today

def amazon():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_w = conn.read_sql(sql="""select * from qingcang_weekday""")
    w_df = df_w['qingcang_weekday'].astype('int').tolist()
    # w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='海外仓调价库')[
    #     'qingcang_weekday'].astype('int').tolist()
    print(w_df)
    print(get_today_week())
    if get_today_week() in w_df:
        sql = f"""SELECT distinct * FROM yibai_oversea_amazon_adjust_log where DATE='{get_date()}'"""

    else:
        sql = f"""SELECT distinct * FROM yibai_oversea_amazon_adjust_log where DATE='{get_date()}' and is_up='涨价' """

    print(sql)
    df = conn.read_sql(sql)
    # df = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')
    # 因为3RG账号限制，暂时不调3RG这个账号
    df = df[df['short_name'] != '3RG']

    print('AMAZON剔除前的数量', len(df))
    # df = tichu_all(df)
    # print('剔除第一种的数量',len(df))

    sql1 = f"""select sku as sku,warehouse,available_stock,after_profit from oversea_age_{time.strftime("%Y%m%d")}_dtl"""
    df_dtl = conn.read_sql(sql1)
    # df_dtl = sql_to_pd(sql=sql1, database='over_sea', data_sys='海外仓调价库')
    df = df.merge(df_dtl, on=['sku', 'warehouse'], how='left')
    # df = tichu1(df)
    # print('剔除第二种的数量', len(df))
    print(df.head())
    print('降价的数据', df[df['is_up'] == '降价'])

    # 亚马逊阶梯定价
    df.sort_values(by=['sku', 'warehouse', 'site', 'price'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['price'] = df['price'] + df.index.values % 30 * 0.01
    df = df[(df['price'] - df['online_price']).abs() >= 0.3]

    ##剔除利润率1%以内波动的数据
    df['涨降幅度'] = 999
    df.loc[df['online_price'] > 0, '涨降幅度'] = (df['price'] - df['online_price']) / df['online_price']
    df = df.loc[(df['涨降幅度'] >= 0.01) | (df['涨降幅度'] <= -0.01)]
    print(df)
    df = df.drop(['涨降幅度'], axis=1)

    ## Amazon账号异常不调1027[id in ('228','265','881','1349','6594','6614','6615','930','6851')]
    df = df.loc[
        (df['account_id'] != 228) & (df['account_id'] != 265) & (df['account_id'] != 881) & (df['account_id'] != 1349)
        & (df['account_id'] != 6594) & (df['account_id'] != 6614) & (df['account_id'] != 6615) & (
                df['account_id'] != 930) & (df['account_id'] != 6851)]

    # df.to_excel(
    #     rf"F:\pycharm_workspace\myproject\海外仓清仓加快动销log上传(王杰交接)\调价分表-AMAZON\AMAZON价格上传-{short_date}.xlsx")
    # df_yxh = df[df['group_name'].str.contains('YXH')]
    # df_yxh.to_excel(
    #     rf"F:\pycharm_workspace\myproject\海外仓清仓加快动销log上传(王杰交接)\调价分表-AMAZON\AMAZON价格上传-{short_date}-yxh.xlsx")
    # # print(df)
    # conn = connect_to_cql()
    try:
        conn.execute(sql=f"""delete from yibai_oversea_amazon_upload_log where date='{datetime.date.today()}'""")
        print('删除成功')
    except:
        pass

    print(df.info())
    write_to_sql(df, 'yibai_oversea_amazon_upload_log')
    # conn.to_sql(df, 'yibai_oversea_amazon_upload_log', if_exists='append', database='over_sea')
    # pd_to_sql(df, table='yibai_oversea_amazon_upload_log', if_exists='append', database='over_sea',
    #           data_sys='海外仓调价库')
    df['涨降幅度'] = 999
    df.loc[df['online_price'] > 0, '涨降幅度'] = (df['price'] - df['online_price']) / df['online_price']
    df = zj_qujian(df)

    # df_toushi = pd.pivot_table(data=df, index='涨降幅区间', values='seller_sku', margins=True, margins_name='合计',
    #                            aggfunc="count")
    # with pd.ExcelWriter(os.path.join(file_path, '亚马逊涨价幅检查123.xlsx')) as writer:
    #     # 昨天的运费
    #     number = str(get_newest_time(1))
    #     date_newest = f"{number[:4]}-{number[4:6]}-{number[6:]}"
    #     print(date_newest)
    #     sql = f"""
    #     select a.sku,a.warehouse,a.country as site,a.after_profit,a.totalCost,a.best_warehouse,a.shipName,b.shipName from
    #     (select *,row_number()
    #     over(partition by sku,warehouse,country,platform order by totalCost) as flag
    #     from oversea_adjust_platform_dtl
    #     where date='{date_newest}' and platform='AMAZON') a
    #     left join
    #     (select *,row_number()
    #     over(partition by sku,warehouse,country,platform order by totalCost) as flag
    #     from oversea_adjust_platform_dtl
    #     where date='{datetime.date.today()}' and platform='AMAZON') b on a.sku=b.sku and a.warehouse=b.warehouse
    #     and a.country=b.country
    #
    #     where a.flag=1 and b.flag=1"""
    #     df_y = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')
    #     df_y['site'] = df_y['site'].str.lower()
    #     df_y.columns = ['sku', 'warehouse', 'site', '上次定价利润率', "上次运费", "上次最优子仓", "上次shipName",
    #                     "本次shipName"]
    #     df = df.merge(df_y, on=['sku', 'warehouse', 'site'], how='left')
    #     df['今天较上次定价利润率涨降'] = df['after_profit'] - df['上次定价利润率']
    #     df['今天较上次运费涨降'] = df['totalCost'] - df['上次运费']
    #     df = lv_qujian(df)
    #     df = yunfei_qujian(df)
    #     df['是否换仓'] = '否'
    #     df.loc[df['上次最优子仓'] != df['best_warehouse'], '是否换仓'] = '是'
    #     # 定价利润率分段和运费涨降分段
    #
    #     # 昨天与今天的汇率
    #     sql2 = f"""select a.site,a.rate as 今天的汇率,b.rate 上次的汇率,(a.rate-b.rate)/b.rate 今天较上次汇率增长占比
    #         from
    #         (select distinct site,rate from amazon_listing_price_detail
    #         where date='{datetime.date.today()}') a
    #         left join
    #         (select distinct site,rate from amazon_listing_price_detail
    #         where date='{date_newest}') b on a.site=b.site"""
    #     df_rate = sql_to_pd(sql=sql2, database='over_sea', data_sys='海外仓调价库')
    #     df_rate['site'] = df_rate['site'].str.lower()
    #     df = df.merge(df_rate, on=['site'], how='left')
    #     df.loc[(df['今天较上次汇率增长占比'] < -0.01), '今天较上次汇率增长占比分段'] = 'a.-1%~'
    #     df.loc[(df['今天较上次汇率增长占比'] >= -0.01) & (
    #             df['今天较上次汇率增长占比'] < -0.005), '今天较上次汇率增长占比分段'] = 'b.-1%~-0.5%'
    #     df.loc[(df['今天较上次汇率增长占比'] >= -0.05) & (
    #             df['今天较上次汇率增长占比'] < 0), '今天较上次汇率增长占比分段'] = 'c.-0.5%~0'
    #     df.loc[(df['今天较上次汇率增长占比'] >= 0) & (
    #             df['今天较上次汇率增长占比'] < 0.005), '今天较上次汇率增长占比分段'] = 'd.0~0.5%'
    #     df.loc[(df['今天较上次汇率增长占比'] >= 0.005) & (
    #             df['今天较上次汇率增长占比'] < 0.1), '今天较上次汇率增长占比分段'] = 'e.0.5~1%'
    #     df.loc[(df['今天较上次汇率增长占比'] >= 0.01), '今天较上次汇率增长占比分段'] = 'f.1%~'
    #
    #     df.to_excel(writer, sheet_name='AMAZON')
    #     # df_toushi.to_excel(writer, sheet_name='透视结果')




def lv_qujian(df, col='今天较上次定价利润率涨降', new_col='今天较上次定价利润率涨降分段'):
    print(df.info())
    df[new_col] = 'k.[0.3,∞)'
    print(df.info())
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


def cd():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_w = conn.read_sql(sql="""select * from qingcang_weekday""")
    w_df = df_w['qingcang_weekday'].astype('int').tolist()
    # w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='海外仓调价库')[
    #     'qingcang_weekday'].astype('int').tolist()
    if get_today_week() in w_df:
        sql = f"""SELECT distinct * FROM `yibai_oversea_cd_adjust_log` where DATE='{get_date()}'"""
    else:
        sql = f"""SELECT distinct * FROM `yibai_oversea_cd_adjust_log` where DATE='{get_date()}' and is_up='涨价' """
    df = conn.read_sql(sql)
    # df = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')

    print('CD剔除前的数量', len(df))
    # df = tichu_all(df)
    # print('剔除第一种的数量', len(df))
    # df = tichu1(df)
    print('剔除第二种的数量', len(df))

    df = df[df['warehouse'] != '英国仓']
    df = df[(df['price'] - df['online_price']).abs() >= 0.3]

    # #剔除利润率1%以内波动的数据
    df['涨幅率'] = (df['post_price'] - df['online_price']) / df['online_price']
    df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01)]
    print(df)
    # 11.24~12.3
    # 只执行负利润加快动销和负利润加快动销的回调以及正利润加快动销的回调
    df = df.loc[(df['adjust_recent_clean'] != '正常') & (df['adjust_recent_clean'] != '正利润加快动销')]
    df = df.drop(['涨幅率', 'day_sales'], axis=1)

    if datetime.date.today() == datetime.date.fromisoformat("2022-07-04"):
        df = df[~(df['sku'].isin(['3113210034211', 'US-JM13541']))]

    # 海外仓冲业绩不调账号（2023-03-30）
    # SUP
    # BIN
    df = df.loc[
        (df['account_id'] != 271)
        & (df['account_id'] != 313)
        ]
    # short_date = get_date().replace("-", "")
    # df.to_excel(os.path.join(file_path, '调价分表-CD', f'CD价格上传-{short_date}.xlsx'))

    # 重新写一下调价数据
    try:
        conn.execute(sql=f"""delete from yibai_oversea_cd_upload_log where date='{datetime.date.today()}'""")
        print('删除成功')
    except:
        pass
    print(df.columns)
    write_to_sql(df, 'yibai_oversea_cd_upload_log')
    # conn.to_sql(df, 'yibai_oversea_cd_upload_log', if_exists='append', database='over_sea')
    # pd_to_sql(df, table='yibai_oversea_cd_upload_log', if_exists='append', database='over_sea', data_sys='海外仓调价库')

    # df_new = df.copy()
    # print(df_new.head())
    # print(len(df_new))
    # df_new['涨幅率'] = (df_new['post_price'] - df_new['online_price']) / df_new['online_price']
    # # print(df_new['涨幅率'])
    # df_new.sort_values(by='涨幅率', inplace=True, ignore_index=True, ascending=False)
    # # print(df_new.columns)
    # df_new = pd.concat([df_new.head(5), df_new.tail(5)])



def walmart():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_w = conn.read_sql(sql="""select * from qingcang_weekday""")
    w_df = df_w['qingcang_weekday'].astype('int').tolist()
    # w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='海外仓调价库')[
    #     'qingcang_weekday'].astype('int').tolist()
    print(w_df)
    if get_today_week() in w_df:
        sql = f"""SELECT distinct * FROM `yibai_oversea_walmart_adjust_log` where DATE='{get_date()}' and site!='ca'"""

    else:
        sql = f"""SELECT distinct * FROM `yibai_oversea_walmart_adjust_log` where DATE='{get_date()}'  and site!='ca' and is_up='涨价' """
    print(sql)
    df = conn.read_sql(sql)
    # df = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')
    # df = df[df['is_up'] == '涨价']
    df.drop('day_sales', axis=1, inplace=True)
    print('WALMART剔除前的数量', len(df))
    # df = tichu_all(df)
    # print('剔除第一种的数量', len(df))
    # df = tichu1(df)
    print('剔除第二种的数量', len(df))

    # walmart阶梯定价
    df.sort_values(by=['sku', 'warehouse', 'site', '上传调价'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['上传调价'] = df['上传调价'] + df.index.values % 30 * 0.01

    # US-JFLR US-GFD 截止2023.12.31不调价
    # walmart  US-XLGL(171) US-YHX(99) US-PSS(100) 不调价， 截止23年12月31日 原因是律所案件多，自己涨价，准备放弃链接
    # walmart 474 账号终止了（2023年3月3日）
    df = df.loc[(df['account_id'] != 369) & (df['account_id'] != 378) & (df['account_id'] != 99)
                & (df['account_id'] != 100) & (df['account_id'] != 171) & (df['account_id'] != 474)
                ]

    # walmart 账户 状态有问题的不调价
    conn2 = connect_to_sql(database='yibai_system', data_sys='销售运营分析系统')
    sql_account_status = '''
    SELECT id as account_id, status 
    FROM yibai_system.yibai_walmart_account
    WHERE status = 1
    '''
    df_status = conn2.read_sql(sql_account_status)
    df_status['account_id'] = df_status['account_id'].astype(int)
    df = df[df['account_id'].isin(df_status['account_id'].unique())]


    df = df[(df['上传调价'] - df['price']).abs() >= 0.3]
    # #剔除利润率1%以内波动的数据
    df['涨幅率'] = (df['上传调价'] - df['price']) / df['price']
    df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01)]
    # print(df)
    df = df.drop(['涨幅率'], axis=1)

    print("最终数据的数量", len(df))
    # if len(df) <= 1000000:
    #     df.to_excel(os.path.join(file_path, '调价分表-WALMART', f'WALMART价格上传-{short_date}.xlsx'), index=False)
    #
    # else:
    #     df.to_csv(os.path.join(file_path, '调价分表-WALMART', f'WALMART价格上传-{short_date}.csv'))

    try:
        conn.execute(sql=f"""delete from yibai_oversea_walmart_upload_log where date='{datetime.date.today()}'""")
        print('删除成功')
    except:
        pass
    print('walmart平台今日调价数量共{}条'.format(len(df)))
    print(df.columns)
    write_to_sql(df, 'yibai_oversea_walmart_upload_log')
    # conn.to_sql(df, 'yibai_oversea_walmart_upload_log', if_exists='append', database='over_sea')

    # pd_to_sql(df, table='yibai_oversea_walmart_upload_log', if_exists='append', database='over_sea',
    #           data_sys='海外仓调价库')
    # df_new = df.copy()
    # df_new['涨幅率'] = (df_new['上传调价'] - df_new['price']) / df_new['price']
    # df_new.sort_values(by='涨幅率', inplace=True, ignore_index=True, ascending=False)
    # df_new = pd.concat([df_new.head(5), df_new.tail(5)])



def ebay_price_listing_config(df):

    sql = """
    SELECT item_id,sku_all as sku,source_type 
    FROM yibai_ebay_modify_price_listing_config 
    WHERE status=1
    """
    conn = connect_to_sql(database='yibai_product', data_sys='ebay刊登库')
    df1 = conn.read_sql(sql)
    # df1 = sql_to_pd(database='yibai_product', sql=sql, data_sys='ebay刊登库')
    df1['source_type'].fillna('', inplace=True)
    df1 = df1[df1['source_type'].str.contains('-1|2')]
    df1 = df1[['item_id', 'sku']].drop_duplicates()
    df1['需要不调价'] = '是'

    df['item_id'] = df['item_id'].apply(lambda m: int(m))
    df1['item_id'] = df1['item_id'].apply(lambda m: int(m))
    # 分成两类
    # 如果sku有多个，则通过item_id去除
    df2 = df1[df1['sku'].str.contains(',')]
    df2 = df2[['item_id', '需要不调价']].drop_duplicates()
    df = df.merge(df2, on=['item_id'], how='left')
    df = df[df['需要不调价'] != '是']
    df.drop(['需要不调价'], axis=1, inplace=True)
    # 其他德则通过item_id和sku进行去除
    df = df.merge(df1, on=['item_id', 'sku'], how='left')
    df = df[df['需要不调价'] != '是']
    df.drop(['需要不调价'], axis=1, inplace=True)
    return df


def ebay():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_w = conn.read_sql(sql="""select * from qingcang_weekday""")
    w_df = df_w['qingcang_weekday'].astype('int').tolist()
    # w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='海外仓调价库')[
    #     'qingcang_weekday'].astype('int').tolist()
    if get_today_week() in w_df:
        sql = f"""SELECT distinct * FROM yibai_oversea_eb_adjust_log  where DATE='{datetime.date.today()}' """

    else:
        sql = f"""SELECT distinct * FROM yibai_oversea_eb_adjust_log  where DATE='{datetime.date.today()}' and is_up='涨价'"""
    df = conn.read_sql(sql)
    # df = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')
    df.rename(columns={"name": 'warehouse'}, inplace=True)
    df.drop('day_sales', axis=1, inplace=True)
    print('EBAY剔除前的数量', len(df))

    # df = tichu_all(df)
    # print('剔除第一种的数量', len(df))
    # df = ebay_price_listing_config(df)
    # print('剔除第二种的数量', len(df))
    # 20211221当前定价（售价）>150 且属于法国仓，暂不进行涨价操作
    df = df.loc[~((df['online_price'] > 150) & (df['warehouse'] == '法国仓') & (df['IS_UP'] == '涨价')), :]

    # 20221009-ebay英国BGS,GFL,GF0;德国AUP,DSP,GAO最低净利不能低于-10%进行兜底测试(广告费率加到10%)
    # engine = create_engine("mysql+pymysql://209313:vlEFa0WsjD@124.71.5.174:3306/over_sea?charset=utf8mb4")
    # con = engine.connect()  # 创建连接
    sql2 = f"""select  a.sku,a.warehouse,a.warehouseId as warehouse_id,
                        a.platform,a.country,totalCost,newPrice,pay_fee,paypal_fee,vat_fee,extra_fee,platform_zero,r.rate
                        from oversea_transport_fee_useful a
                        inner join yibai_platform_fee b on a.platform=b.platform and a.country=b.site
                        inner join domestic_warehouse_clear.erp_rate r on a.country=r.country COLLATE utf8mb4_general_ci
                        where a.platform<>'AMAZON'"""
    df2 = conn.read_sql(sql2)

    df4 = df.loc[(df['country'] == 'UK') | (df['adjust_recent_clean'] == '负利润加快动销')]
    df4 = df4.loc[(df4['account_id'] == 81) | (df4['account_id'] == 85) | (df4['account_id'] == 80)]
    df5 = df.loc[(df['country'] == 'DE') | (df['adjust_recent_clean'] == '负利润加快动销')]
    df5 = df5.loc[(df5['account_id'] == 331) | (df5['account_id'] == 278) | (df5['account_id'] == 446)]
    df_te = pd.concat([df4, df5])
    df_te = df_te[['sku', 'account_id', 'country', 'warehouse', 'warehouse_id', 'best_warehouse']]
    df_te = df_te.merge(df2, on=['sku', 'warehouse', 'warehouse_id', 'country'], how='left')
    df_te = df_te[df_te['platform'] == 'EB']
    df_te['-10%净利定价'] = (df_te['newPrice'] + df_te['totalCost']) / (
            1 - df_te['pay_fee'] - df_te['paypal_fee'] - df_te['vat_fee'] - df_te['extra_fee']
            - df_te['platform_zero'] + 0.1) / df_te['rate']
    df_te['-10%净利定价'] = df_te['-10%净利定价'].round(1) - 0.01
    df_te['毛利润率'] = df_te['platform_zero'] - 0.1
    print('EB净利-10%数量', len(df_te))
    # print(df_te)
    df_te = df_te[['sku', 'account_id', 'warehouse', 'country', '-10%净利定价', '毛利润率']]
    # print(df_te)
    df = df.merge(df_te, on=['sku', 'account_id', 'warehouse', 'country'], how='left')
    # print(df)
    df.loc[df['post_price'] < df['-10%净利定价'], 'acc_profit'] = df['毛利润率']
    df.loc[df['post_price'] < df['-10%净利定价'], 'post_price'] = df['-10%净利定价'] - df['shipping_fee']
    df = df.drop(['-10%净利定价', '毛利润率'], axis=1)

    # 20221009-ebay英国TPG,GHB;德国EGG,MOL,进行利润率33%测试(广告费率加到10%)
    df6 = df.loc[(df['country'] == 'UK')]
    df6 = df6.loc[(df6['account_id'] == 171) | (df6['account_id'] == 181) | (df6['account_id'] == 905) | (
            df6['account_id'] == 774)]
    df7 = df.loc[(df['country'] == 'DE')]
    df7 = df7.loc[(df7['account_id'] == 889) | (df7['account_id'] == 671) | (df7['account_id'] == 448) | (
            df7['account_id'] == 824)]
    df_te2 = pd.concat([df6, df7])
    df_te2 = df_te2[['sku', 'account_id', 'country', 'warehouse', 'warehouse_id', 'best_warehouse']]
    df_te2 = df_te2.merge(df2, on=['sku', 'warehouse', 'warehouse_id', 'country'], how='left')
    df_te2 = df_te2[df_te2['platform'] == 'EB']

    df_te2['净利率'] = 0.33 - df_te2['platform_zero']
    df_te2['对应净利定价'] = (df_te2['newPrice'] + df_te2['totalCost']) / (
            1 - df_te2['pay_fee'] - df_te2['paypal_fee'] - df_te2['vat_fee'] - df_te2['extra_fee']
            - df_te2['platform_zero'] - df_te2['净利率']) / df_te2['rate']
    df_te2['对应净利定价'] = df_te2['对应净利定价'].round(1) - 0.01
    df_te2['33%毛利润率'] = 0.33
    print('EB毛利33%数量', len(df_te2))

    df_te2 = df_te2[['sku', 'account_id', 'warehouse', 'country', '净利率', '对应净利定价', '33%毛利润率']]
    df = df.merge(df_te2, on=['sku', 'account_id', 'warehouse', 'country'], how='left')
    df = df.drop_duplicates()
    df.loc[df['33%毛利润率'].isna() == False, 'acc_profit'] = df['33%毛利润率']
    df.loc[df['对应净利定价'].isna() == False, 'post_price'] = df['对应净利定价'] - df['shipping_fee']
    df = df.drop(['净利率', '对应净利定价', '33%毛利润率'], axis=1)
    df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] >= 0), "IS_UP"] = "涨价"
    df.loc[(df['post_price'] - df['online_price'] + df['shipping_fee'] < 0), "IS_UP"] = "降价"

    # 20220317 GY6账号暂时不调价
    df = df.loc[df['account_id'] != 978]
    # 20220921 HSD/VAG/JUS/DIF/FOA/DRO/STE账号暂时不调价，澳仓提利润率测试高推广费效果，利润率都调至28%以上

    df = df.loc[
        (df['account_id'] != 923) & (df['account_id'] != 855) & (df['account_id'] != 393) & (df['account_id'] != 710)
        & (df['account_id'] != 357) & (df['account_id'] != 599) & (df['account_id'] != 602)]

    # 海外仓冲业绩不调账号（2023-03-30）FFS
    # CAB
    # DLF
    # HLF
    # CTT
    # HWK
    # FHS
    # HGP
    # SAP
    # TJS
    # FOD
    # QHN
    # LOS
    # SRS
    # BUU
    # FER
    # NC3
    # SH3
    # SUP
    # DPF
    # KKO
    # CAO
    # SUH
    # LVE
    # BIN
    # AM9
    # UNP
    # MSS
    # ISP
    # GOB
    # ONS
    # AOS
    # HSA
    # SGH
    # TOS
    # NDB
    # SN1
    # CUL
    # SSH
    # VIS
    # GIN
    # BJY
    # AGL
    # HOA
    # CAD
    df = df.loc[
        (df['account_id'] != 296)
        & (df['account_id'] != 42)
        & (df['account_id'] != 716)
        & (df['account_id'] != 930)
        & (df['account_id'] != 718)
        & (df['account_id'] != 773)
        & (df['account_id'] != 139)
        & (df['account_id'] != 138)
        & (df['account_id'] != 387)
        & (df['account_id'] != 775)
        & (df['account_id'] != 336)
        & (df['account_id'] != 776)
        & (df['account_id'] != 717)
        & (df['account_id'] != 774)
        & (df['account_id'] != 931)
        & (df['account_id'] != 293)
        & (df['account_id'] != 289)
        & (df['account_id'] != 487)
        & (df['account_id'] != 361)
        & (df['account_id'] != 245)
        & (df['account_id'] != 267)
        & (df['account_id'] != 801)
        & (df['account_id'] != 800)
        & (df['account_id'] != 437)
        & (df['account_id'] != 438)
        & (df['account_id'] != 504)
        & (df['account_id'] != 506)
        & (df['account_id'] != 505)
        & (df['account_id'] != 384)
        & (df['account_id'] != 422)
        & (df['account_id'] != 424)
        & (df['account_id'] != 453)
        & (df['account_id'] != 451)
        & (df['account_id'] != 164)
        & (df['account_id'] != 145)
        & (df['account_id'] != 144)
        & (df['account_id'] != 280)
        & (df['account_id'] != 274)
        & (df['account_id'] != 204)
        & (df['account_id'] != 386)
        & (df['account_id'] != 701)
        & (df['account_id'] != 543)
        & (df['account_id'] != 206)
        & (df['account_id'] != 423)
        & (df['account_id'] != 43)
        ]

    df = df[(df['post_price'] - df['online_price'] + df['shipping_fee']).abs() >= 0.3]
    # #剔除利润率1%以内波动的数据
    df['涨幅率'] = (df['post_price'] - df['online_price'] + df['shipping_fee']) / df['online_price']
    df = df.loc[(df['涨幅率'] >= 0.01) | (df['涨幅率'] <= -0.01)]
    df = df.drop(['涨幅率'], axis=1)

    # 去重
    df.drop_duplicates(subset=['item_id', 'sku'], inplace=True)
    # df.to_excel(os.path.join(file_path, '调价分表-EB', f'EB价格上传-{short_date}.xlsx'), index_label=False)

    try:
        conn.execute(sql=f"""delete from yibai_oversea_eb_upload_log where date='{datetime.date.today()}'""")
        # print('删除成功')
    except:
        pass
    print('ebay平台今日调价数量共{}条'.format(len(df)))
    print(df.columns)
    write_to_sql(df, 'yibai_oversea_eb_upload_log')
    # conn.to_sql(df, 'yibai_oversea_eb_upload_log', if_exists='append', database='over_sea')
    # pd_to_sql(df, table='yibai_oversea_eb_upload_log', if_exists='append', database='over_sea', data_sys='海外仓调价库')

    # df_new = df.copy()
    # df_new['涨幅率'] = (df_new['post_price'] - df_new['online_price'] + df_new['shipping_fee']) / df_new['online_price']
    # df_new.sort_values(by='涨幅率', inplace=True, ignore_index=True, ascending=False)
    # print(df_new.columns)
    # df_new = pd.concat([df_new.head(5), df_new.tail(5)])
    # df_new.drop_duplicates(subset=['item_id', 'sku'], inplace=True)



# def ali(file_path):
#     w_df = sql_to_pd(sql="""select * from qingcang_weekday""", database='over_sea', data_sys='海外仓调价库')[
#         'qingcang_weekday'].astype('int').tolist()
#     if get_today_week() in w_df:
#         sql = f"""SELECT distinct * FROM `yibai_oversea_ali_adjust_log` where DATE='{get_date()}' """
#
#     else:
#         sql = f"""SELECT distinct * FROM `yibai_oversea_ali_adjust_log` where DATE='{get_date()}' and is_up='涨价'"""
#     df = sql_to_pd(sql=sql, database='over_sea', data_sys='海外仓调价库')
#     # df = df[df['is_up'] == '涨价']
#     print('ALI剔除前的数量', len(df))
#     # df = tichu_all(df)
#     # print('剔除第一种的数量', len(df))
#     # df = tichu1(df)
#     print('剔除第二种的数量', len(df))
#
#     # if len(df) <= 1000000:
#     #     df.to_excel(os.path.join(file_path, '调价分表-ALI', f'ALI价格上传-{short_date}.xlsx'), index=False)
#     # else:
#     #     df.to_csv(os.path.join(file_path, '调价分表-ALI', f'ALI价格上传-{short_date}.csv'))
#
#     conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
#     try:
#         conn.execute(sql=f"""delete from yibai_oversea_ali_upload_log where date='{datetime.date.today()}'""")
#         print('删除成功')
#     except:
#         pass
#
#     pd_to_sql(df, table='yibai_oversea_ali_upload_log', if_exists='append', database='over_sea',
#               data_sys='海外仓调价库')
#
#     df_new = df.copy()
#     df_new['涨幅率'] = (df_new['price'] - df_new['当前实际售价']) / df_new['当前实际售价']
#     df_new.sort_values(by='涨幅率', inplace=True, ignore_index=True, ascending=False)
#     df_new = pd.concat([df_new.head(5), df_new.tail(5)])
#     return df_new
def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')
    # df10 = df[df.index < 10000]
    # conn.to_sql(df10, table_name, if_exists='append')

    # sql = f"""
    # delete from {table_name}
    # """
    # conn.execute(sql)

    sql = f"""
    delete from {table_name} where DATE='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()

## 上传相关程序

# from myself_public.public_function import *
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED, as_completed
import json
import os
import base64
import datetime
import hashlib
import hmac
from typing import List, Optional
from urllib.parse import quote_plus
import time
import requests
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')


# #####################################################################################################################

# 数据部服务器
# def connect_to_cql():
#     engine = create_engine("mysql+pymysql://209313:vlEFa0WsjD@124.71.5.174:3306/over_sea?charset=utf8mb4")
#     conn = engine.connect()  # 创建连接
#     return conn


# conn = connect_to_cql()


# def get_date():
#     qu_shu_date = datetime.date.today()
#     qu_shu_date_str = qu_shu_date.isoformat()
#     date_lsit = qu_shu_date_str.split("-")
#     date_new = date_lsit[0] + date_lsit[1] + date_lsit[2]
#     return date_new


# #####################################################################################################################
def ebay_Importprice(group, item):
    url = 'http://ebayapi.yibainetwork.com/services/ebay/ebayapi/modifypriceapproval'
    headers = {'Content-Type': 'application/json', }

    data_post0 = []
    # group.to_excel('rslj.xlsx')
    for i in range(len(group['sku'])):
        data_dict = {
            'sku': str(group['sku'][i]),
            'itemid': int(group['item_id'][i]),
            'start_price_target': float(group['销售价'][i]),
            'target_profit': float(group['目标利润率'][i]),
            'shipping_service_cost': str(group['运费(国内第一运费)'][i]),
            'shipping_service_additional_cost': str(group['额外每件加收运费'][i]),
            'work_number': '209313',
            'sale_status': str(group['sale_status'][i]),
            'warehouse_ids': str(group['warehouse_id'][i])

        }

        # print(data_dict)
        data_post0.append(data_dict)
    data_post = {'data': data_post0}
    # print(data_post)

    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(180, 120)).json()
            print(res)
            if res['code'] == 200:
                break
            else:
                print(f'eaby{item}:', res['message'])
        except:
            print(f'eaby{item}:接口失败，重新上传')
            time.sleep(30)


def ebay_price_post():
    # file_name=r"C:\Users\Administrator\Desktop\eb上传.xls"
    # file_name = os.path.join(f_path, '调价分表-EB', f'EB价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-EB', f'EB价格上传-{get_date()}.xlsx')
    # print(file_name)
    # df_data = pd.read_excel(file_name)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT *
    FROM yibai_oversea_eb_upload_log
    WHERE DATE = '{datetime.date.today()}'
    """
    df_data = conn.read_sql(sql)
    print(df_data.head(5))
    print("ebay调价数目", len(df_data))

    # #把当天的调价数目上传
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='EB' """
    # conn.execute(delete_sql)
    # insert_sql = f"""insert into yibai_oversea_adjust_number
    # values ('{datetime.date.today().isoformat()}','EB',{len(df_data)})"""
    # conn.execute(insert_sql)
    # print('eb调价数据上传成功')

    df = df_data[["sku", "item_id", "shipping_fee", "post_price", "acc_profit", "adjust_recent_clean", 'warehouse_id']]
    df['额外每件加收运费'] = ""
    df["销售价"] = df["post_price"]
    df["目标利润率"] = df["acc_profit"] * 100
    df["目标利润率"] = df["目标利润率"].round(2)
    df['运费(国内第一运费)'] = df["shipping_fee"]
    df['sale_status'] = df["adjust_recent_clean"]
    df = df.reset_index(drop=True)
    # 每次传10000个item_id
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 50000))
    df['warehouse_id'] = df['warehouse_id'].astype('int')
    print(df.info())

    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(ebay_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='ebay') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)


# #####################################################################################################################
def amazon_Importprice(df1, item):
    # 正式环境
    url0 = 'http://amazon.yibainetwork.com/services/products/Amazonlistingpriceadjustment/Importprice?token='
    # 测试环境
    # url0 = 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token='
    headers = {'Content-Type': 'application/json', }

    data0 = []
    for i in range(len(df1.index)):
        data1 = {'account_id': int(df1['account_id'][i]),
                 'seller_sku': df1['seller_sku'][i],
                 'price': df1['price'][i],
                 'sale_price': df1['sale_price'][i],
                 'business_price': df1['price'][i],
                 'lowest_price': df1['lowest_price'][i],
                 'start_time': df1['start_time'][i],
                 'end_time': df1['end_time'][i],
                 'reason': df1['adjust_recent_clean'][i], }
        data0.append(data1)
    data_post = {'user_id': '205532',
                 'price_data': data0}
    data_post0 = json.dumps(data_post)
    # print(data_post0)
    token_str = data_post0 + 'saagdfaz'
    token = hashlib.md5(token_str.encode()).hexdigest()
    # print(token)
    url = url0 + token
    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            if res['status'] == 1:
                break
            else:
                print(f'amazon{item}:', res['msg'])
        except:
            print(f'amazon{item}:接口失败，重新上传')
            time.sleep(30)

# yxh调价接口
def amazon_importprice_yxh(df1, item):
    # 正式环境
    url0 = 'http://yuexinghamazon.yibainetwork.com/services/products/Amazonlistingpriceadjustment/Importprice?token='
    # 测试环境
    # url0 = 'http://dp.yibai-it.com:10026/services/products/Amazonlistingpriceadjustment/Importprice?token='
    headers = {'Content-Type': 'application/json', }


    data0 = []
    for i in range(len(df1.index)):
        data1 = {'account_id': int(df1['account_id'][i]),
                 'seller_sku': str(df1['SellerSKU'][i]),
                 'price': float(df1['your price'][i]),
                 'sale_price': df1['sale price'][i],
                 'business_price': float(df1['Business Price'][i]),
                 'lowest_price': df1['lowest price'][i],
                 'start_time': df1['start time'][i],
                 'end_time': df1['end time'][i],
                 'reason': df1['调价原因'][i], }
        data0.append(data1)
    data_post = {'user_id':'Y202655',
                 'price_data': data0}
    data_post0 = json.dumps(data_post)
    # print(data_post0)
    token_str = data_post0 + 'saagdfaz'
    token = hashlib.md5(token_str.encode()).hexdigest()
    # print(token)
    url = url0 + token
    while True:
        try:
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            # print(res)
            # res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600))
            # print(res.content.decode("utf-8"))
            if res['status'] == 1:
                break
            else:
                print(f'amazon{item}:', res['msg'])
        except:
            print(f'amazon{item}接口失败，重新上传')
            time.sleep(30)

def amazon_price_post():
    # file_name=os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-{get_date()}.xlsx')
    # if os.path.exists(file_name):
    #     print(file_name)
    #     df = pd.read_excel(file_name)
    # else:
    #     file_name = file_name.replace('.xlsx', '.csv')
    #     print(file_name)
    #     df = pd.read_csv(file_name)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT *
    FROM yibai_oversea_amazon_upload_log
    WHERE DATE = '{datetime.date.today()}'
    """
    df = conn.read_sql(sql)
    print('AMAZON上传数量为:', len(df))

    # 上传amazon调价数目
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='AMAZON' """
    # conn.execute(delete_sql)
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today()}','AMAZON',{len(df)})"""
    # conn.execute(insert_sql)
    # print('AMAZON调价数目统计成功')
    df = df[["account_id", "seller_sku", "price", "adjust_recent_clean"]]
    df.loc[df['adjust_recent_clean'].isin(['回调', '正常']), 'price'] = df['price'] + 0.5
    df['sale_price'] = ''
    df['lowest_price'] = ''
    df['start_time'] = ''
    df['end_time'] = ''
    df = df.drop_duplicates()
    df = df.sort_values(['account_id'], ascending=True)

    df.fillna('', inplace=True)
    df = df.reset_index(drop=True)
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m_data: int(m_data / 1000))

    # 单线程测试使用
    # for key, group in tqdm(df.groupby(['index'])):
    #     group = group.reset_index(drop=True)
    #     amazon_Importprice(group, key)
    # exit()

    threadPool = ThreadPoolExecutor(max_workers=20)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(amazon_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='AMAZON') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)
    send_msg("海外仓及精品组日常数据处理", '动销组定时任务推送', 'AMAZON调价已上传,请检查数据')

    # # 上传yxh调价数据
    # file_name = os.path.join(f_path, '调价分表-AMAZON', f'AMAZON价格上传-{get_date()}-yxh.xlsx')
    # if os.path.exists(file_name):
    #     print(file_name)
    #     df_yxh = pd.read_excel(file_name)
    # else:
    #     file_name = file_name.replace('.xlsx', '.csv')
    #     print(file_name)
    #     df_yxh = pd.read_csv(file_name)
    # print('yxh_AMAZON上传数量为:', len(df_yxh))
    # df_yxh = df_yxh[["account_id", "seller_sku", "price", "adjust_recent_clean"]]
    # df_yxh.loc[df_yxh['adjust_recent_clean'].isin(['回调', '正常']), 'price'] = df['price'] + 0.5
    # df_yxh['sale_price'] = ''
    # df_yxh['lowest_price'] = ''
    # df_yxh['start_time'] = ''
    # df_yxh['end_time'] = ''
    # df_yxh = df_yxh.drop_duplicates()
    # df_yxh = df_yxh.sort_values(['account_id'], ascending=True)
    #
    # df_yxh.fillna('', inplace=True)
    # df_yxh = df_yxh.reset_index(drop=True)
    # df_yxh['index'] = df_yxh.index
    # df_yxh['index'] = df_yxh['index'].apply(lambda m_data: int(m_data / 1000))
    #
    # threadPool = ThreadPoolExecutor(max_workers=20)
    # thread_list = []
    # for key, group in df_yxh.groupby(['index']):
    #     group = group.reset_index(drop=True)
    #     future = threadPool.submit(amazon_importprice_yxh, group, key)
    #     thread_list.append(future)
    #
    # with tqdm(total=len(thread_list), desc='AMAZON') as pbar:
    #     for future in as_completed(thread_list):
    #         data = future.result()
    #         pbar.update(1)
    # threadPool.shutdown(wait=True)


def url(ding_type):
    secret = ''
    access_token = ''
    if ding_type == '动销组定时任务推送':
        secret = 'SEC2dbc10f676641297b6fd934a80d2041efa54bfe4ff1621b93aa0b6da9a72d905'
        access_token = 'c3f16626d9c431a60dc7b00154fd7c5f0ce47e76aa5a3bebc4f91250bd1c0d98'
    elif ding_type == 'daraz调价':
        secret = 'SECa16e30b19585e6a2e3583eb6a2140422595727b579a07192e197722dcb9564ba'
        access_token = '15c1293e4da188b17904478c4cba806f10bf6333cbb1d47f174025eb9aa4ec31'
    elif ding_type == 'lazada平台-erp调价':
        secret = 'SECbbe93a58a6d0e147c34e09b8184e40361e1ed1b0b8c4528ad8bb91fe4067f268'
        access_token = '939f6e2574249bf5a5be9cebbbcefa6c482030fcebe1cc296bbae287f32e0f76'

    elif ding_type == 'shopee平台-erp调价':
        secret = 'SEC897a406db159d7d4c23ff64a08d36f2635ad817655742ba91c99637e7bd4ee75'
        access_token = '8fb417e555d1c3051296e4d017c6239913fd8ce70986b356c4b381c280ed03d3'

    elif ding_type == '亚马逊调价成功率':
        secret = 'SEC64f8a376f8099fc457599955c53ccc0b1c276176df60faf03f09a715db9b7cab'
        access_token = 'd007ec0dc8ce2762959b914135070efb151d48f850542965d2123bec88717bb7'

    elif ding_type == 'walmart平台调价':
        secret = 'SEC832cbeb9efeac511f55106b49cecf654f4125fbe0f6cdbdc75ac17230ed09e1f'
        access_token = 'cef404c06828d9a631bb009941a8dabc848bfb9a7c687b5f8500d3779104602c'
    elif ding_type == 'EBAY调价LISTING问题沟通群':
        secret = 'SEC168a2eb35d58dbcec639124216299ca74c8fe13d2b27079cc1aeb224004edb57'
        access_token = '36bf0e59ba08c6f76659afc0ec680ec187f9a4a91dcda9faf231fbec6403631c'

    elif ding_type == 'CD-调价群':
        secret = 'SEC8fcb4aa051537e949ff3f3572042406ab14938d17a7afccae9720dfe0548e988'
        access_token = '388f264b2b2a0ea307b732237abed77c132d9ae2b2fb4beef5e9b3ec15fca43b'

    elif ding_type == "ebay海外仓调价群":
        secret = "SECfb65f1e919aced91956e8648b00b5fb1562eaef9d7cbfddf009c525029a51996"
        access_token = "c926786ee1ea0335fe7c6fcca5930b00e68e3aa157522358aabeb5ea88363215"


    elif ding_type == '定价与订单物流运费获取一致性':
        secret = 'SEC8190f5a1b93aae39b7852cd407b7e91cffac4b378d838d1462058c3cc39a15e9'
        access_token = '044761aa27153fb1c51929857633f2ba018efa5900e9ae56aee0364975db1b1f'

    elif ding_type == 'FBC调价跟进':
        secret = 'SEC458d57d140032c7e0df63de4db6c348582a89329aec6bb1082357e31b8f0d631'
        access_token = '52638fa774e929e243c355dafcb4a80fa62b17009d1a49163dcdac316d9d8b7f'

    elif ding_type == '速卖通-平台调价':
        secret = 'SECb25bcac46e8f7438afeedde3b5f0dfdf99420bb3f69a80ee646367fe5bb44863'
        access_token = '5a5f18ce5c6dbe8ce3f067d8239f7af65de3fa4a1822fbdccc784ba5e1bce300'

    elif ding_type == '全平台调价数据分享群':
        secret = 'SEC8ce5d783308e65c1a69b8cd3ebd531e799b6665905f29d6ed58ecba16014358e'
        access_token = '347347b57eeb7a978074b828be05273279612a46895e0ea8ba0b25c4125d4b59'

    elif ding_type == '海外仓及精品组日常数据处理':
        secret = 'SECcc44563c593e67724f6fc688ff8737755639b25fcc0679cf5ea1d9be5199feed'
        access_token = 'a02560731053a18bf07eae2ed04bc8f3a188c1c6d59c1bbc54f274f29a4eee6f'

    elif ding_type == 'wish平台调价':
        secret = 'SEC8dd2575c7541d38b9336464c159bb8f64d21079ebb62d476b18d3288c2c4cba0'
        access_token = '580e558b5dc7f8dec84ddddd0dea9de74adce5c8e2b8f51dbc3329525847a9f7'

    timestamp = round(time.time() * 1000)
    secret_enc = secret.encode("utf-8")
    string_to_sign = "{}\n{}".format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode("utf-8")
    hmac_code = hmac.new(
        secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
    ).digest()
    sign = quote_plus(base64.b64encode(hmac_code))
    url = f"https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}"
    return url


def send_msg(ding_type: str = None,
             task_type: str = None,
             text: str = None,
             mobiles: Optional[List[str]] = None,
             is_all: bool = True,
             status: str = '成功', ):
    res = requests.post(url=url(ding_type),
                        headers={"Content-Type": "application/json"},
                        json={
                            "msgtype": "text",
                            "at": {
                                "atMobiles": mobiles or [],
                                "isAtAll": is_all,
                            },
                            "text": {"content": text},
                        },
                        )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")

    if ding_type == '动销组定时任务推送':
        send_action(url(ding_type),
                    title="动销任务状态",
                    text='动销任务状态',
                    btn_s=[
                        {
                            'title': '查看详细进度',
                            'actionURL': f'http://sjfxapi.yibainetwork.com:5050/api/price/task/{datetime.date.today().strftime("%Y%m%d")}'
                        }
                    ]
                    )


def send_action(url, title: str = None, text: str = None, btn_s: List = None):
    res = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={
            "actionCard": {
                "title": title,
                "text": text,
                "btnOrientation": "1",
                "btns": btn_s
            },
            "msgtype": "actionCard"
        },
    )
    result = res.json()
    print(result)
    if result.get("errcode") == 0:
        print("发送成功！")


# #####################################################################################################################
def cd_Importprice(group, item):
    url = 'http://smallplatform.yibainetwork.com/services/api/cdiscount/editprice'
    headers = {'Content-Type': 'application/json', }

    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'short_name': group['店铺简称'][i],
            'seller_sku': group['刊登SKU'][i],
            'adjust': group['调整后的价格(支持百分比)'][i],
            'std_fee': float(group['STD运费'][i]),
            'std_add': float(group['STD每件加收'][i]),
            'trk_fee': float(group['TRK运费'][i]),
            'trk_add': float(group['TRK每件加收'][i]),
            'reg_fee': float(group['REG运费'][i]),
            'reg_add': float(group['REG每件加收'][i]),
            'fst_fee': float(group['FST运费'][i]),
            'fst_add': float(group['FST每件加收'][i]),
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'work_number': '205532',
                 'create_time': str(datetime.datetime.now()).split('.')[0]}
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            res = requests.post(url, json=data_post, headers=headers, timeout=(180, 120)).json()
            if res['result'] == "成功":
                break
            else:
                print(f'cd{item}:', res['message'])
        except:
            print(f'cd{item}:接口失败，重新上传')
            time.sleep(30)


def cd_price_post():
    # file_name=r"C:\Users\Administrator\Desktop\cd上传.xlsx"
    # file_name=r"E:\调价任务\海外仓清仓加快动销log上传(王杰交接)\调价分表-CD\CD价格上传前60万条-20211118.xlsx"
    # file_name = os.path.join(f_path, '调价分表-CD', f'CD价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-CD', f'CD价格上传-{get_date()}.xlsx')
    # # 如果写出来的是xlsx就读取xlsx的内容，否则读取csv的内容
    # if os.path.exists(file_name):
    #     print(file_name)
    #     df = pd.read_excel(file_name)
    # else:
    #     file_name = file_name.replace('.xlsx', '.csv')
    #     print(file_name)
    #     df = pd.read_csv(file_name)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT *
    FROM yibai_oversea_cd_upload_log
    WHERE DATE = '{datetime.date.today()}'
    """
    df = conn.read_sql(sql)
    df_data = df.copy()

    # df_data=df_data[df_data['is_up']=='涨价']
    print('CD上传数量为:', len(df_data))
    # 删除CD目前的调价数据
    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='CD' """
    # conn.execute(delete_sql)
    #
    # # 上传CD调价数目
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today().isoformat()}','CD',{len(df)})"""
    # conn.execute(insert_sql)
    # print('CD调价数目统计成功')

    df = df_data[["account_name", "seller_sku", "post_price"]]
    df = df.sort_values(['account_name'], ascending=True)
    df["调整后的价格(支持百分比)"] = df["post_price"]
    df["STD运费"] = 0
    df["STD每件加收"] = 0
    df["TRK运费"] = 0
    df["TRK每件加收"] = 0
    df["REG运费"] = 12.99
    df["REG每件加收"] = 12.99
    df["FST运费"] = 999
    df["FST每件加收"] = 999
    df = df.rename(columns={"account_name": "店铺简称", "seller_sku": "刊登SKU"})

    # 接口原因，价格需要传字符串
    df['调整后的价格(支持百分比)'] = df['调整后的价格(支持百分比)'].apply(lambda m: round(m, 2))
    df['调整后的价格(支持百分比)'] = df['调整后的价格(支持百分比)'].astype(str)
    #
    df = df.reset_index(drop=True)
    # 每次传10000
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 1000))

    threadPool = ThreadPoolExecutor(max_workers=2)
    thread_list = []
    for key, group in df.groupby(['index']):
        group = group.reset_index(drop=True)
        future = threadPool.submit(cd_Importprice, group, key)
        thread_list.append(future)

    with tqdm(total=len(thread_list), desc='cd') as pbar:
        for future in as_completed(thread_list):
            data = future.result()
            pbar.update(1)
    threadPool.shutdown(wait=True)

def time_and_sign(WALMART_API_KEY):
    # 验证
    # 1.time
    time0 = int(time.time())
    # 2.sign
    time1 = json.dumps({'time': time0}).replace(' ', '')
    s = f'{time1}{WALMART_API_KEY}'
    sign = hashlib.md5(s.encode('utf8')).hexdigest()
    return time0, sign

def data_walmart_zhekou(group):
    # 20221109从13小时1分钟改为14小时,1124改为20小时
    start_time = (datetime.datetime.today() + datetime.timedelta(hours=20, minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    end_time = (datetime.datetime.today() + datetime.timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S')
    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'account_id': int(group['account_id'][i]),
            'seller_sku': group['线上SKU'][i],
            'current_price': float(group['价格'][i]),
            'comparison_price': float(group['原价'][i]),
            'current_price_type': group['促销类型'][i],
            'effective_date': start_time,
            'expiration_date': end_time,
            'process_mode': group['processMode值'][i],
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'user_id': '8988', }
    return data_post

def data_walmart_price(group):
    data_post0 = []
    for i in range(len(group.index)):
        data_dict = {
            'account_id': int(group['account_id'][i]),
            'seller_sku': group['线上SKU'][i],
            'price': float(group['价格'][i]),
        }
        data_post0.append(data_dict)
    data_post = {'data': data_post0,
                 'user_id': '8988', }
    return data_post

def walmart_Importprice(group, item):
    # 线上
    url0 = 'http://smallplatformapi.yibainetwork.com/services/walmart/api/mutipromotions'
    url1 = 'http://smallplatformapi.yibainetwork.com/services/walmart/api/mutiprice'
    WALMART_API_KEY = 'YThlM2Y1NjI0ODQyYTQzMGI2MDllZWUzYmQ5NjVjYzY='
    # 测试
    # url0 = 'http://dp.yibai-it.com:30016/services/walmart/api/mutipromotions'
    # url1 = 'http://dp.yibai-it.com:30016/services/walmart/api/mutiprice'
    # WALMART_API_KEY = 'Njk4YzY3MmYzYzc3YjI3MzM2NDc1NjhjZGIyNDMyYzY='
    #
    headers = {'Content-Type': 'application/json', }

    # 修改价格
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            #
            time0, sign = time_and_sign(WALMART_API_KEY)
            #
            url = f'{url1}?sign={sign}&time={time0}'

            data_post = data_walmart_price(group)

            #
            res = requests.post(url, json=data_post, headers=headers, timeout=(3600, 3600)).json()
            # print(res)
            if res['ack'] == "success" and len(res['error_data']) == 0:
                break
            else:
                print(f'walmart价格{item}:', res['error_data'])
        except:
            print(f'walmart价格{item}:接口失败，重新上传')
            time.sleep(30)

    # 修改折扣
    while True:
        try:
            # 小平台刊登库每个整点开始有很多定时任务，传数据避开（58~10分）
            while datetime.datetime.now().minute < 10 or datetime.datetime.now().minute >= 58:
                time.sleep(30)
            #
            time0, sign = time_and_sign(WALMART_API_KEY)
            #
            url = f'{url0}?sign={sign}&time={time0}'
            data_post = data_walmart_zhekou(group)
            #
            res = requests.post(url, json=data_post, headers=headers, timeout=(600, 600)).json()
            print(res)
            if res['ack'] == "success" and len(res['error_data']) == 0:
                break
            else:
                print(f'walmart折扣{item}:', res['error_data'])
        except:
            print(f'walmart折扣{item}:接口失败，重新上传')
            time.sleep(30)


# def read_sql_ck(sql, client):
#     data, columns = client.execute(sql, columnar=True, with_column_types=True)
#     df = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
#     return df


# def get_ck_client(url='121.37.30.78', user='lumengyu', password='lu1Me4n6gyu_1hd', port='9001'):
#     client = Client(host=url, port=port, user=user, password=password)
#     return client


def walmart_price_post():
    # file_name=os.path.join(f_path, '调价分表-WALMART', f'WALMART价格上传-20220621.xlsx')
    # file_name = os.path.join(f_path, '调价分表-WALMART', f'WALMART价格上传-{get_date()}.xlsx')
    # print(file_name)
    # df_data = pd.read_excel(file_name)
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT *
    FROM yibai_oversea_walmart_upload_log
    WHERE DATE = '{datetime.date.today()}'
    """
    df_data = conn.read_sql(sql)

    # df_data=df_data[df_data['is_up']=='涨价']
    df_data = df_data.drop_duplicates()
    print('walmart', df_data.head())
    print("walmart调价数量:", len(df_data))

    # delete_sql = f"""delete from yibai_oversea_adjust_number where date='{datetime.date.today()}' and platform='WALMART' """
    # conn.execute(delete_sql)
    #
    # # 上传调价数目
    # insert_sql = f"""insert into yibai_oversea_adjust_number (date,platform,number)
    #    values ('{datetime.date.today().isoformat()}','WALMART',{len(df_data)})"""
    # conn.execute(insert_sql)
    # print('walmart调价数目统计成功')

    df = df_data[["account_id", "seller_sku", "上传调价"]]
    df = df.rename(columns={"上传调价": "价格", "seller_sku": "线上SKU"})
    df["原价"] = df["价格"] + 5
    df["促销类型"] = "REDUCED"
    df["processMode值"] = "UPSERT"
    df = df.reset_index(drop=True)
    df = df.sort_values(['account_id'], ascending=True)
    df = df.reset_index(drop=True)
    # 每次传100
    df['index'] = df.index
    df['index'] = df['index'].apply(lambda m: int(m / 100))

    # 单线程测试使用(walmart接口实现方式要求只能单线程)
    for key, group in tqdm(df.groupby(['index'])):
        group = group.reset_index(drop=True)
        walmart_Importprice(group, key)


# #####################################################################################################################


# #####################################################################################################################
# #####################################################################################################################
def platform_fu(platform, f_path):
    if platform == 'ebay':
        ebay_price_post(f_path)
        print('ebay上传完成')
    # elif platform == 'amazon':
    #     amazon_price_post(f_path)
    #     print('amazon上传完成')
    elif platform == 'cd':
        cd_price_post(f_path)
        print('cd上传完成')
    elif platform == 'walmart':
        walmart_price_post(f_path)
        print('walmart上传完成')


# def delete_adjust_number():
#     now_time = time.strftime('%Y-%m-%d')
#     print(now_time)
#     conn = connect_to_cql()
#     sql_delete_2 = f"""delete from yibai_oversea_adjust_number where date = '{now_time}'"""
#     print(sql_delete_2)
#     conn.execute(sql_delete_2)
#     print('删除当天插入数据')



if __name__ == "__main__":

    # file_path = os.path.dirname(__file__)
    # print(file_path)
    # print(get_today_week())
    #
    amazon = amazon()
    print('amazon更新完成')
    # cd = cd(file_path)
    # print('cd更新完成')
    # eb = ebay(file_path)
    # print('ebay更新完成')
    # wal = walmart(file_path)
    # print('walmart更新完成')
    # # # a=ali(file_path)
    # # print("ali更新完成")
    # with pd.ExcelWriter(os.path.join(file_path, '检查.xlsx')) as writer:
    #     amazon.to_excel(writer, sheet_name='AMAZON')
    #     cd.to_excel(writer, sheet_name='CD')
    #     eb.to_excel(writer, sheet_name='EB')
    #     wal.to_excel(writer, sheet_name='WALMART')
    #     # a.to_excel(writer,sheet_name='ALI',index=False)
    #
    # print('done!')
