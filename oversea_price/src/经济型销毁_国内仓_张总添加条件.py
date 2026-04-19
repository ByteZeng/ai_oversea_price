import warnings
import datetime
import pandas as pd
from tqdm import tqdm

from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd
from utils import utils
from utils.utils import make_path, save_df, read_df
import src.fetch_data as fd
warnings.filterwarnings('ignore')


def get_sku_by_age():
    sql = """
    select 
        sku, stock `在库库存`, age
    from domestic_warehouse_clear.domestic_warehouse_age
    """
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df = conn_mx.ck_select_to_df(sql)
    df['超420天库龄库存'] = (df['age'] > 420).astype('int') * df['在库库存']
    df1 = df.drop(['age'], axis=1).groupby('sku').sum().reset_index()
    df2 = df1[(df1['超420天库龄库存'] == df1['在库库存']) & (df1['超420天库龄库存'] > 0)]
    return df2


def get_listing_by_destroy_time():
    date = datetime.date.today() - pd.Timedelta(days=14)
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql = f"""
    select 
        sku, start_time `抵达销毁价开始时间`
        from domestic_warehouse_clear.domestic_warehouse_clear_destroy_time
        where start_time <= \'{date}\'
    """
    df = conn_mx.ck_select_to_df(sql)
    df['已达销毁价天数'] = (pd.to_datetime(datetime.date.today()) - pd.to_datetime(
        df['抵达销毁价开始时间'])).dt.days + 1
    return df


def main():
    """
    经济型销毁_国内仓清仓全平台
    国内仓暂无销毁场景，只用于做活动产品筛选
    筛选条件：
    1、库龄全超420天
    2、近30天销量为0
    3、已达到销毁价15天（来源表记录的是5个主要平台链接都达到销毁价的开始时间）
    4、不包含有冬季产品属性的产品
    5、近三个月没有正净利销量
    优先将被领用作用奖品的商品纳入销毁项目中，然后优先将销毁金额大的商品纳入销毁项目中。
    """
    utils.program_name = '经济型销毁_国内仓清仓全平台'
    make_path()
    conn_mx = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')

    date = datetime.date.today()
    date_31_before = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime('%Y-%m-%d')
    date_3month_before = (datetime.datetime.now() - datetime.timedelta(days=91)).strftime('%Y-%m-%d')
    df1 = get_sku_by_age()
    print(df1.info())
    df2 = get_listing_by_destroy_time()
    print(df2.info())
    df = df2.merge(df1, on=['sku'], how='inner')
    print(df.info())
    del df1
    del df2

    # # 筛选非冬季 - 冬季feature_val=4
    # sql = f"""
    #     SELECT
    #         sku
    #     FROM yibai_prod_base_sync.yibai_prod_sku
    #     WHERE feature_val like \'%4%\'
    # """
    # df4 = conn_mx.ck_select_to_df(sql)
    # df4['是否冬季'] = '是'
    # df = df.merge(df4, on=['sku'], how='left')
    # df['是否冬季'] = df['是否冬季'].fillna('否')
    # df = df[df['是否冬季'] == '否']
    # del df4

    # 国内仓仓库
    sql = """select id from yibai_logistics_tms_sync.yb_warehouse where warehouse_type=1"""
    df_warehouse = conn_mx.ck_select_to_df(sql)

    # 匹配成本 日均销量
    step = 5000
    df_distinct = df[['sku']].drop_duplicates().reset_index(drop=True)
    list_group = [df_distinct[i:i + step] for i in range(0, len(df_distinct), step)]
    df3 = pd.DataFrame()
    df5 = pd.DataFrame()
    df_dcm = pd.DataFrame()
    for df_member in tqdm(list_group, total=len(list_group)):
        sql = f"""
            SELECT
                sku, product_status `产品状态`, title_cn `产品名称`,
                CASE 
                    when product_status=11 and toFloat64(avg_goods_price) > 0 then toFloat64(avg_goods_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) > 0 then toFloat64(new_price) 
                    when product_status=11 and toFloat64(avg_goods_price) = 0 and toFloat64(new_price) = 0 then toFloat64(product_cost) 
                    when product_status!=11 and toFloat64(new_price) > 0 then toFloat64(new_price)
                    else toFloat64(product_cost) 
                END as `成本`, 
                infringement_remarks `侵权备注`
            FROM yibai_prod_base_sync.yibai_prod_sku
            where sku in {tuple(df_member['sku'].values)}
        """
        df_temp = conn_mx.ck_select_to_df(sql)
        df3 = df_temp if df3.empty else pd.concat([df3, df_temp], ignore_index=True)

        sql = f"""
        select sku, 30days_sales
        from over_sea.yibai_sku_sales_statistics
        where sku in {tuple(df_member['sku'].values)}
        and warehouse_id in {tuple(df_warehouse['id'].values)}
        """
        df_temp = sql_to_pd(database='over_sea', sql=sql, data_sys='数据部服务器')
        df5 = df_temp if df5.empty else pd.concat([df5, df_temp], ignore_index=True)

        sql = f"""
            WITH order_temp as (
                SELECT order_id, sku, quantity
                FROM yibai_dcm_order_sync.dcm_order_sku
                WHERE sku in {tuple(df_member['sku'].values)} and create_time > '{date_31_before}'
            )
            SELECT sku, sum(quantity) as 30days_sales_dcm
            FROM order_temp t
            INNER JOIN (
                SELECT order_id
                FROM yibai_dcm_order_sync.dcm_order
                WHERE order_status<>40 and is_abnormal<>1 and is_intercept <> 1 
                and order_id in (SELECT order_id FROM order_temp) and payment_status = 1
            ) d ON t.order_id = d.order_id
            GROUP BY sku
        """
        df_dcm_order = fd.fetch_ck(sql, 34, 'yibai_dcm_order_sync')
        df_dcm = df_dcm_order if df_dcm.empty else pd.concat([df_dcm, df_dcm_order], ignore_index=True)

    df3['产品状态'] = df3['产品状态'].astype('str').replace({
        '1': '已创建',
        '2': '已开发',
        '3': '待买样',
        '4': '待品检',
        '5': '待编辑',
        '6': '待拍摄',
        '7': '待编辑待拍摄',
        '8': '待修图',
        '9': '在售中',
        '10': '审核不通过',
        '11': '停售',
        '12': '待清仓',
        '13': '已滞销',
        '14': '待物流审核',
        '15': '待关务审核',
        '16': 'ECN资料变更中',
        '17': 'ECN资料变更驳回'
    })
    df = df.merge(df3, on=['sku'], how='left')
    df['销毁金额'] = df['成本'] * df['超420天库龄库存']
    df5 = df5.groupby('sku').sum().reset_index()
    df = df.merge(df5, on=['sku'], how='left')
    df = df.merge(df_dcm, on=['sku'], how='left')
    df[['30days_sales','30days_sales_dcm']] = df[['30days_sales','30days_sales_dcm']].fillna(0)
    df = df[(df['30days_sales'] == 0) & (df['30days_sales_dcm'] == 0)]
    del df5
    df1 = pd.read_excel('F://yibai-price-strategy//data//经济型销毁_国内仓清仓全平台20230921.xlsx')
    df1 = df1[['sku', '是否领用2']]
    df1['sku'] = df1['sku'].astype('str')
    df = df.merge(df1, on=['sku'], how='left')
    df['是否领用2'] = df['是否领用2'].fillna('否')
    df['是否库龄全超420天'] = (df['超420天库龄库存'] == df['在库库存']).replace({True: '是', False: '否'})
    df['是否已达到销毁价15天'] = (df['已达销毁价天数'] >= 15).replace({True: '是', False: '否'})

    # 近三个月没有正净利销量
    step = 5000
    df_distinct = df[['sku']].drop_duplicates().reset_index(drop=True)
    list_group = [df_distinct[i:i + step] for i in range(0, len(df_distinct), step)]
    df1 = pd.DataFrame()
    for df_member in tqdm(list_group, total=len(list_group)):
        sql = f"""
        select sku, max(`净利润`) `净利润`
        from domestic_warehouse_clear.monitor_dom_order
        where created_time >= '{date_3month_before}'
        and created_time < '{date}'
        and sku in {tuple(df_member['sku'].values)}
        group by sku
        """
        df_temp = conn_mx.ck_select_to_df(sql)
        df1 = df_temp if df1.empty else pd.concat([df1, df_temp], ignore_index=True)
    df1['近三个月是否有正净利销量'] = (df1['净利润'] > 0).replace({True: '是', False: '否'})
    df1 = df1.drop('净利润', axis=1)
    df = df.merge(df1, on=['sku'], how='left')
    df['近三个月是否有正净利销量'] = df['近三个月是否有正净利销量'].fillna('否')
    df = df.query("`近三个月是否有正净利销量` == '否'")

    # 选200万
    df = df.sort_values(['是否领用2', '销毁金额'], ascending=False).reset_index(drop=True)
    total = 0
    is_go = True
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        if is_go:
            if total + row['销毁金额'] < 2000000:
                total += row['销毁金额']
                df.loc[index, '建议提交报废'] = '是'
            else:
                df.loc[index, '建议提交报废'] = '否'
                is_go = False
        else:
            df.loc[index, '建议提交报废'] = '否'

    df_output = df.query("`建议提交报废` == '是'")
    print(f'{date.year:04d}-{date.month:02d}-{date.day:02d} 国内仓销毁数据')
    print(f"共计{df_output.shape[0]}个sku，{df_output['超420天库龄库存'].sum()}PCS，"
          f"销毁金额{round(df_output['销毁金额'].sum() / 10000, 2)}W")

    save_df(df, f'{utils.program_name}{date.year:04d}{date.month:02d}{date.day:02d}', file_type='xlsx')
    print('done!')


if __name__ == '__main__':
    main()
