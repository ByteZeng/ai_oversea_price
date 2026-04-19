# 给财务的订单数据
# 每月更新一次，数据包含近1个月海外仓清仓订单（销售状态是【正负利润加快动销】、【清仓】）
##
import time

import pandas as pd
import numpy as np
import datetime,os,calendar
import traceback
from pulic_func.robot import ding
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account
# warnings.filterwarnings("ignore")


##
# 获取订单
def Xindingdan(start_date, end_date):
    ck_client = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    sql='''
    WITH order_temp AS (
        SELECT DISTINCT order_id
        FROM yibai_oms_sync.yibai_oms_order
        WHERE payment_time >= '{}' and purchase_time < '{}' 
        and warehouse_id not in (478, 481, 323, 60) 
        and order_id not like '%%-RE%%' 
        -- and platform_code IN ('AMAZON','EB','WISH','ALI','WALMART','CDISCOUNT','TEMU')
        )

    select 
        a.order_id,a.platform_order_id,a.platform_code,a.account_id,b.seller_sku,c.sku,a.warehouse_id,
        toDate(a.payment_time)  AS `付款时间`,
        toDate(a.purchase_time)  AS `创建时间`,
        c.sales_status AS `销售状态`
    from (
        SELECT order_id, platform_order_id, platform_code, account_id, warehouse_id, payment_time, purchase_time
        FROM yibai_oms_sync.yibai_oms_order 
        WHERE order_id in order_temp
        ) a
    left join (
        SELECT seller_sku, order_id, id
        FROM yibai_oms_sync.yibai_oms_order_detail 
        WHERE order_id in order_temp
        ) b on a.order_id=b.order_id
    left join (
        SELECT sku, order_detail_id, order_id, sales_status
        FROM yibai_oms_sync.yibai_oms_order_sku
        WHERE order_id in order_temp
        ) c on b.id=c.order_detail_id 
    '''.format(start_date, end_date)

    df = ck_client.ck_select_to_df(sql)
    df.columns = [i.split('.')[-1] for i in df.columns]
    print(df.info())
    return df

# 获取仓库
def get_warehouse():
    ck_client = pd_to_ck(database='yibai_oms_sync', data_sys='调价明细历史数据')
    sql='''
    SELECT 
        a.id as warehouse_id, a.warehouse_name AS `发货仓库`, b.name AS `大仓`
    FROM yibai_logistics_tms_sync.yibai_warehouse a
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b 
    ON toInt64(a.ebay_category_id) = toInt64(b.id)
    WHERE 
        a.warehouse_type in (2, 3)
        AND a.warehouse_name NOT LIKE '亚马逊%%' 
        -- AND a.warehouse_name NOT LIKE '艾姆乐%%' 
        AND a.warehouse_name NOT LIKE '%%FBC%%'
        AND a.warehouse_name NOT LIKE '%%Deliverr%%' AND a.warehouse_name NOT LIKE 'deliverr%%' AND a.warehouse_name NOT LIKE '代销%%'
    '''
    df_warehouse=ck_client.ck_select_to_df(sql)
    
    return df_warehouse


# 存入 mysql
def write_to_sql(df):
    conn_ck = pd_to_ck(database='clear_order_cw', data_sys='调价明细历史数据')
    
    d_time1 = df['update_month'].str.replace('-','').astype(int).iloc[0]
    # d_time1 = 202508
    sql = f"""
        alter table clear_order_cw.oversea_clear_orders drop PARTITION {d_time1}
    """
    conn_ck.ck_execute_sql(sql)

    sql = """
        optimize table clear_order_cw.oversea_clear_orders final
    """
    conn_ck.ck_execute_sql(sql)

    conn_ck.write_to_ck_json_type(df, 'oversea_clear_orders')
    
    # 统计每月订单数量
    sql = """

    SELECT *
    FROM clear_order_cw.oversea_clear_orders

    """
    df_temp = conn_ck.ck_select_to_df(sql)
    df_temp.drop_duplicates(subset=['order_id','account_id','seller_sku','sku'], inplace=True)
    df_temp = df_temp.set_index('付款时间')
    df_temp.index = pd.to_datetime(df_temp.index)    
    print(df_temp.groupby([df_temp.index.year, df_temp.index.month]).agg({'order_id':'count'}))

##
def main():

    # 订单时间范围
    now = datetime.datetime.now()
    last_month = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)
    # 上个月开始日期。往前推三天，避免订单获取不全
    start_date = (datetime.datetime(last_month.year, last_month.month, 1) - datetime.timedelta(days=2)).strftime(
        '%Y-%m-%d')
    # 上个月结束日期。往后延两天
    end_date = (last_month + datetime.timedelta(days=3)).strftime('%Y-%m-%d')
    #
    # start_date = '2025-06-28'
    # end_date = '2025-09-04'
    # end_date = time.strftime('%Y-%m-%d')
    print(start_date, end_date)
    # last_3_month = (last_month - datetime.timedelta(days=93)).strftime('%Y-%m-%d')
    ding_robot1 = ding.DingRobot('清仓订单获取通知')
    # 获取订单
    try:
        order_oms = Xindingdan(start_date, end_date)
        order_oms.drop_duplicates(inplace=True)
        # 获取仓库
        df_warehouse = get_warehouse()
        # 匹配大仓和temu清仓sku
        data = order_oms.merge(df_warehouse, how='inner', on='warehouse_id')

        # 20250226 补充temu主体账号
        temu_account = get_temu_account()
        temu_account['platform_code'] = 'TEMU'
        data = pd.merge(data, temu_account[['account_id','platform_code', 'main_name']], how='left', on=['account_id','platform_code'])

        # 20250928：获取temu清仓sku，订单时间在清仓时间范围内时，标记为清仓订单
        data = get_temu_sku_status(data)
        # 筛选正负利润加快动销的订单
        data['update_month'] = datetime.datetime.now().strftime('%Y-%m')
        data = data[data['销售状态'].isin(['正利润加快动销','负利润加快动销','清仓'])]
        print(data.info())
        # data.to_excel('F://Desktop//oversea_clear_order.xlsx', index=0)
        write_to_sql(data)
    except:
        ding_robot1.send_msg(text=f"""{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 海外仓每月销库订单汇总失败，请检查.""",
                            mobiles=['+86-13922822326'], is_all=False)
        raise Exception(traceback.format_exc())
    else:
        ding_robot1.send_msg(text=f"""{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 海外仓每月销库订单汇总完成，数据已上传到oversea_clear_orders.""",
                            mobiles=['+86-13922822326'], is_all=False)

    return None


def get_temu_sku_status(df):
    df.reset_index(drop=True, inplace=True)
    df['唯一标识'] = df.index
    # 匹配销售状态
    sql = """
        SELECT sku, warehouse `大仓`, is_temu_clear, start_time, end_time 
        FROM yibai_oversea.temu_clear_sku_backup 
        where (end_time>='2025-08-01' and end_time <= '2025-09-10') 
        -- or end_time is null 
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    data_temu = conn_ck.ck_select_to_df(sql)
    data_temu['is_temu_clear'] = data_temu['is_temu_clear'].replace({'补充清仓sku':'负利润加快动销'})
    data_temu.fillna({'end_time': datetime.date.today().strftime("%Y-%m-%d")}, inplace=True)
    # data_fbm['end_time'] = data_fbm['end_time'].apply(lambda x:(pd.to_datetime(x)+pd.DateOffset(0) ).strftime("%Y-%m-%d"))
    df = df.merge(data_temu, on=['sku','大仓'], how='left')
    df['状态有效'] = 0
    print(df.info())
    df['start_time'] = pd.to_datetime(df['start_time']).dt.date
    df['end_time'] = pd.to_datetime(df['end_time']).dt.date
    c1 = (df['创建时间'] >= df['start_time']) & (df['创建时间'] <= df['end_time']) & (~df['销售状态'].isin(['正利润加快动销','负利润加快动销','清仓']))
    df.loc[c1, '状态有效'] = 1
    df.sort_values(by=['状态有效', 'start_time'], ascending=[False, True], inplace=True)
    df.drop_duplicates(['唯一标识'], keep='first', inplace=True)
    c1 = (df['状态有效']==1) & (~df['is_temu_clear'].isna()) & (df['platform_code']=='TEMU')
    df['销售状态'] = np.where(c1, df['is_temu_clear'], df['销售状态'])
    df.drop(['start_time', 'end_time', '状态有效','is_temu_clear'], axis=1, inplace=True)

    # temu标记不清仓sku
    sql = """
        SELECT sku, warehouse `大仓`, 1 as is_no_clear, start_time, end_time 
        FROM yibai_oversea.temu_no_clear_sku
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    no_clear = conn_ck.ck_select_to_df(sql)
    df = df.merge(no_clear, on=['sku','大仓'], how='left')
    df['状态有效'] = 0
    print(df.info())
    df['start_time'] = pd.to_datetime(df['start_time']).dt.date
    df['end_time'] = pd.to_datetime(df['end_time']).dt.date
    c1 = (df['创建时间'] >= df['start_time']) & (df['创建时间'] <= df['end_time']) & (
        df['销售状态'].isin(['正利润加快动销', '负利润加快动销', '清仓']))
    df.loc[c1, '状态有效'] = 1
    df.sort_values(by=['状态有效', 'start_time'], ascending=[False, True], inplace=True)
    df.drop_duplicates(['唯一标识'], keep='first', inplace=True)
    c1 = (df['状态有效'] == 1) & (~df['is_no_clear'].isna()) & (df['platform_code']=='TEMU')
    df['销售状态'] = np.where(c1, '正常', df['销售状态'])
    df.drop(['唯一标识', 'start_time', 'end_time', '状态有效', 'is_no_clear'], axis=1, inplace=True)

    return df


##
if __name__ == '__main__':
    main()
    # split_date_ranges()

    # main_temp()
    # check_order_status()







