import datetime
import traceback
import pandas as pd
import numpy as np
import time
import os
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.clickhouse_con import send_ck_cls
from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
warnings.filterwarnings("ignore")
os.environ['NUMEXPR_MAX_THREADS'] = '16'
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck


def insert_data(df, table):
    df["account_id"] = df["account_id"].astype(int)
    # 写入ck数据
    user, password, port, db_name = "zhenghw", "zhenghw#mrp03", 9003, "over_sea"
    from ctypes import cdll, c_char_p
    if os.path.exists('/data'):
        host = '127.0.0.1'
        PyWithGoWriteToCk = cdll.LoadLibrary('/opt/soft/gotool/bin/goclickhouse.so').PyWithGoWriteToCk
    else:
        host = '121.37.248.212'
        PyWithGoWriteToCk = cdll.LoadLibrary('C:/yibai_go_tool/goclickhouse.dll').PyWithGoWriteToCk
    PyWithGoWriteToCk.restype = c_char_p
    ip_port = f"{host}:{port}".encode()
    databaseStr = db_name.encode()
    tableStr = table.encode()
    usernameStr = user.encode()
    passwordStr = password.encode()
    i = 0
    while 1:
        col = f'index{i}'
        if col not in list(df.columns):
            break
        else:
            i = i + 1
    df[col] = df.index
    df[col] = df[col] / (262144 * 6)
    df[col] = df[col].astype(int)
    for _, group in df.groupby([col]):
        group.drop([col], axis=1, inplace=True)
        jsonDataStr = group.to_json(orient='records').encode()
        del group
        result_byte = PyWithGoWriteToCk(ip_port, databaseStr, tableStr, usernameStr, passwordStr, jsonDataStr)
        result = result_byte.decode()
        if result != '':
            raise Exception(result)
        del jsonDataStr


def delete_data(conn):
    sql = f"delete from dashbord_new_data where created_time>='{days_10_ago_date()}'"
    conn.execute(sql)


def delete_data4():
    conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
    sql = "truncate dashbord_new"
    conn.execute(sql)
    conn.close(sql)


def delete_data2(conn):
    sql = """delete from dashbord_new_data where order_id in (
        select order_id from (
        select order_id,count(order_id) as flag from dashbord_new_data group by order_id order by  flag desc)a where flag>10)"""
    conn.execute(sql)


def get_date():
    qu_shu_date = datetime.date.today()
    qu_shu_date_str = qu_shu_date.isoformat()
    date_lsit = qu_shu_date_str.split("-")
    date_new = date_lsit[0] + date_lsit[1] + date_lsit[2]
    return date_new


def str_to_day(time_str):
    time_str = time_str[0:10]
    return time_str


def days_10_ago_date():
    d = datetime.date.today() - datetime.timedelta(days=7)
    return d


def get_base_data2(conn_mx, conn):
    # 2022/01/24从订单状态=20或者45改成只要true_profit_new1>0就取

    sql_oms1 = f"""SELECT distinct A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as 'seller_sku',B.account_id,
                B.payment_time as paytime,
                B.purchase_time as created_time,
                -- H.total_price,
                C.total_price,
                -- case when H.true_shipping_fee>0 then H.true_profit_new1 else H.profit_new1 end as true_profit_new1,
                case 
                    WHEN C.true_shipping_fee > 0 THEN C.true_profit_new1
                    else C.profit_new1 
                end as true_profit_new1,
                CASE
                when order_status=1 then '下载'
                when order_status=10 then '待确认'
                when order_status=20 then '初始化'
                when order_status=30 then '正常'
                when order_status=40 then '待处理'
                when order_status=50 then '部分出库'
                when order_status=60 then '已出库'
                when order_status=70 then '已完结'
                when order_status=80 then '已取消'
                ELSE ''   END AS 'complete_status',
                case
                when B.order_type=1 then '常规线上平台客户订单'
                when B.order_type=2 then '线下客户订单'
                when B.order_type=3 then '线上客户补款单'
                when B.order_type=4 then '重寄单'
                when B.order_type=5 then '港前重寄'
                when B.order_type=6 then '虚拟发货单'
                ELSE '未知'END AS 'order_typr',
                CASE
                WHEN B.payment_status = 1 THEN
                '已付款' ELSE '未付款'
                END AS 'pay_status',
                CASE
                WHEN B.refund_status = 0 THEN
                '未退款'
                WHEN B.refund_status = 1 THEN
                '退款中'
                WHEN B.refund_status = 2 THEN
                '部分退款' 
                when B.refund_status=3 then '全部退款' ELSE ''
                END AS 'refound_status',
                "status" as status,
                "warehouse_name" as warehouse_name,
                B.warehouse_id,
                case when B.warehouse_id=434 then '意大利仓'
                when B.warehouse_id=529 then '意大利仓'
                when B.warehouse_id=349 then '加拿大仓'
                when B.warehouse_id=443 then '日本仓'
                end as  'warehouse',
                1 as new_price,
                A.quantity,
                A.quantity as release_money,
                -- G.sales_status as sales_status
                F.sales_status as sales_status
                FROM yibai_oms_order_detail A
                LEFT JOIN yibai_oms_order B ON A.order_id=B.order_id
                LEFT JOIN yibai_oms_order_profit C ON B.order_id=C.order_id
                left join yibai_oms_order_sku F on F.order_detail_id=A.id

                WHERE  
                B.purchase_time>='{days_10_ago_date()}'
                and 
                (B.allocation_status <> 1 and  B.order_status<>40 and  B.is_abnormal<>1 and B.is_intercept <> 1 and B.order_status <> 80)
                and 
                B.payment_status=1
                and B.warehouse_id not in (60,323)  AND  B.refund_status = 0  AND A.order_id not like '%%-RE%%'
            and B.platform_code in ('AMAZON','CDISCOUNT','WALMART','WISH','EB','ALI')"""

    sql_oms10 = f"""SELECT distinct A.order_id,B.platform_order_id,B.platform_code,F.sku,A.seller_sku as 'seller_sku',B.account_id,
                B.payment_time as paytime,
                B.purchase_time as created_time,
                -- H.total_price,
                C.total_price,
                -- case when H.true_shipping_fee>0 then H.true_profit_new1 else H.profit_new1 end as true_profit_new1,
                case 
                    WHEN C.true_shipping_fee > 0 THEN C.true_profit_new1
                    else C.profit_new1 
                end as true_profit_new1,
                CASE
                when order_status=1 then '下载'
                when order_status=10 then '待确认'
                when order_status=20 then '初始化'
                when order_status=30 then '正常'
                when order_status=40 then '待处理'
                when order_status=50 then '部分出库'
                when order_status=60 then '已出库'
                when order_status=70 then '已完结'
                when order_status=80 then '已取消'
                ELSE ''   END AS 'complete_status',
                case
                when B.order_type=1 then '常规线上平台客户订单'
                when B.order_type=2 then '线下客户订单'
                when B.order_type=3 then '线上客户补款单'
                when B.order_type=4 then '重寄单'
                when B.order_type=5 then '港前重寄'
                when B.order_type=6 then '虚拟发货单'
                ELSE '未知'END AS 'order_typr',
                CASE  
                WHEN B.payment_status = 1 THEN
                '已付款' ELSE '未付款'
                END AS 'pay_status',
                
                CASE              
                WHEN B.refund_status = 0 THEN
                '未退款'
                WHEN B.refund_status = 1 THEN
                '退款中'
                WHEN B.refund_status = 2 THEN
                '部分退款' 
                when B.refund_status=3 then '全部退款' ELSE ''
                END AS 'refound_status',
                "status" as status,
                "warehouse_name" as warehouse_name,
                B.warehouse_id,
                case when B.warehouse_id=434 then '意大利仓'
                when B.warehouse_id=529 then '意大利仓'
                when B.warehouse_id=349 then '加拿大仓'
                when B.warehouse_id=443 then '日本仓'
                end as  'warehouse',
                1 as new_price,              
                A.quantity,
                A.quantity as release_money,
                '总计' as sales_status
                FROM yibai_oms_order_detail A
                LEFT JOIN yibai_oms_order B ON A.order_id=B.order_id
                LEFT JOIN yibai_oms_order_profit C ON B.order_id=C.order_id
                left join yibai_oms_order_sku F on F.order_detail_id=A.id
                
                WHERE  
                B.purchase_time>='{days_10_ago_date()}'
                and 
                (B.allocation_status <> 1 and   B.order_status<>40 and  B.is_abnormal<>1 and 
                B.is_intercept <> 1 and B.order_status <> 80)
                and 
                B.payment_status=1
                and B.warehouse_id not in (60,323)  AND  B.refund_status = 0  AND A.order_id not like '%%-RE%%'
                and B.platform_code in ('WALMART','CDISCOUNT','AMAZON','EB','WISH','ALI')"""

    df = pd.DataFrame()
    df_1 = pd.DataFrame()
    conn_oms = connect_to_sql(database='yibai_oms_order', data_sys='新订单系统从库')
    for sql in [sql_oms1]:
        df_new = conn_oms.read_sql(sql)
        df = df.append(df_new)
    for sql in [sql_oms10]:
        df_new = conn_oms.read_sql(sql)
        df_1 = df_1.append(df_new)
    conn_oms.close()
    print(df.head())
    print(2)
    print(df_1.head())
    sql_product = """with 
                  [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
                  ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
                  '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
                  '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
                  '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr	 
                  select D.sku,transform(D.product_status, product_status_arr, product_status_desc_arr, '未知') 
                  as product_status_test
                  ,D.new_price as new_price_test
                  FROM yb_datacenter.yb_product as D"""
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_product = conn_ck.ck_select_to_df(sql_product)
    # 匹配上对应的成本，产品状态，并重新计算销库金额
    ######1
    df = df.merge(df_product, on=['sku'], how='left')
    df['new_price'] = df['new_price_test']
    df['status'] = df['product_status_test']
    df['release_money'] = df['quantity'] * df['new_price']
    df.drop(['new_price_test', 'product_status_test'], axis=1, inplace=True)
    ######2
    df_1 = df_1.merge(df_product, on=['sku'], how='left')
    df_1['new_price'] = df_1['new_price_test']
    df_1['release_money'] = df_1['quantity'] * df_1['new_price']
    df_1['status'] = df_1['product_status_test']
    df_1.drop(['new_price_test', 'product_status_test'], axis=1, inplace=True)

    sql_warehouse = """
        select E.id as warehouse_id,E.warehouse_type,E.warehouse_name as warehouse_name_1,F.name as warehouse_1
        from yibai_logistics_tms_sync.yibai_warehouse E
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category F 
        ON E.ebay_category_id = F.id
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql_warehouse)
    # df_warehouse = sql_to_pd(database='yibai_logistics', sql=sql_warehouse, data_sys='数据管理部同步服务器')
    print(df_warehouse.head())

    # sql_package_relation = f"""select package_id,order_id
    #                         from yibai_order.yibai_oms_order_package_relation
    #                         where is_delete=0 and order_id in {order_list}"""
    # df_pack = sql_to_pd3(sql=sql_package_relation, database='yibai_order')
    # package_list = tuple(set(df_pack['package_id']))
    #
    # sql_package_detail = f"""select package_id,sku,sales_status as sales_status_1
    #                     from yibai_order.yibai_oms_order_package_detail
    #                     where package_id in {package_list}"""
    # df_dt = sql_to_pd3(sql=sql_package_detail, database='yibai_order')

    # 对于df需要用到销售状态
    # 先匹配和筛选仓
    df = df.merge(df_warehouse, on=['warehouse_id'], how='left')
    df['warehouse_name'] = df['warehouse_name_1']
    df = df[(df['warehouse_type'] == 2)]  # 选取warehouse_type<>1的数据,即剔除国内仓

    df.loc[df['warehouse'].isnull(), 'warehouse'] = df['warehouse_1']
    df.loc[(df['warehouse'].isnull()) & (df['warehouse_name'].str.contains('FBA')), 'warehouse'] \
        = df['warehouse_name'].apply(lambda x: str(x).split('FBA')[-1])
    df.drop(columns=['warehouse_name_1', 'warehouse_1', 'warehouse_type', 'warehouse_id'], inplace=True)
    # 链接到销售状态
    # df=df.merge(df_pack,on=['order_id'],how='left')
    # df=df.merge(df_dt,on=['package_id','sku'],how='left')
    # df.loc[df['sales_status'].isnull(),'sales_status']=df['sales_status_1']
    # df.drop(['package_id','sales_status_1'],axis=1,inplace=True)
    # 对于销售状态为总计的只需要连接到warehouse就行
    df_1 = df_1.merge(df_warehouse, on=['warehouse_id'], how='left')
    df_1['warehouse_name'] = df_1['warehouse_name_1']
    df_1 = df_1[(df_1['warehouse_type'] == 2)]  # 选取warehouse<>1的数据
    df_1.loc[df_1['warehouse'].isnull(), 'warehouse'] = df_1['warehouse_1']
    df_1.loc[(df_1['warehouse'].isnull()) & (df_1['warehouse_name'].str.contains('FBA')), 'warehouse'] \
        = df_1['warehouse_name'].apply(lambda x: str(x).split('FBA')[-1])

    df_1.drop(columns=['warehouse_name_1', 'warehouse_1', 'warehouse_type', 'warehouse_id'], inplace=True)
    # 将表明销售状态的和销售状态为总计的拼接起来
    df = pd.concat([df, df_1])

    df = df[df["total_price"].notnull()]
    df = df[df["total_price"] != '']
    df["created_time"] = df["created_time"].astype(str)
    df["created_time"] = df["created_time"].apply(lambda x: str_to_day(x))
    df["paytime"] = df["paytime"].astype(str)
    df["paytime"] = df["paytime"].apply(lambda x: str_to_day(x))

    # 20251205 切换取中台差值表。先用平台+国家匹配，再用平台匹配，最后匹配不上的填充0.17
    sql = """

         SELECT platform_code, site, toFloat32(net_profit2)/100 as `差值`
         FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
         WHERE shipping_type = 2 and is_del = 0 and status = 1
     """
    conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
    df_fee1 = conn_ck.ck_select_to_df(sql)
    df_fee2 = df_fee1[df_fee1['site'] == 'other']
    df_fee2 = df_fee2.rename(columns={'差值': '差值2'})
    df_fee2 = df_fee2.drop_duplicates(subset='platform_code')
    df = pd.merge(df, df_fee1, how='left', on=['platform_code', 'site'])
    df = pd.merge(df, df_fee2[['platform_code', '差值2']], how='left', on=['platform_code'])
    df['差值'] = np.where(df['差值'].isna(), df['差值2'], df['差值'])
    df['差值'] = df['差值'].fillna(0.17).astype(float)

    df["real_profit"] = df["true_profit_new1"] - df["差值"] * df["total_price"]
    df["real_profit"] = df["real_profit"].astype(float)
    df["real_profit"] = df["real_profit"].round(4)
    df.drop(['差值2', '差值', 'site'], axis=1, inplace=True)
    df = df[(df['total_price'] > 0) | (df['warehouse'] != '墨西哥仓')]

    # 去重
    # df.drop_duplicates(inplace=True)

    df.insert(loc=20, column='real_profit', value=df.pop("real_profit"))

    # 删除package_id
    # del df['package_id']

    # conn.to_sql(df, table='dashbord_new_data', if_exists="append")
    # all_data = len(df)

    conn_mx.write_to_ck_json_type(df, 'dashbord_new_data')



def merge_data():
    conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
    sql = """select * from dashbord_new_data 
            union all
             select * from dashbord_new_copy"""
    df = conn.read_sql(sql)
    df.drop_duplicates(inplace=True)
    conn.to_sql(df, table='dashbord_new', if_exists="append")
    conn.close()


def run_listing_contorl():
    try:
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        conn_mx = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        print("清空dashbord_new_data")
        delete_data(conn)
        print("数据开始处理")
        get_base_data2(conn_mx, conn)
        # delete_data2(conn)
        # send_msg('动销组定时任务推送', '海外仓订单监控',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓订单监控数据处理完成,总量为:{all_data}",
        #          is_all=False)
        conn.close()
    except Exception as e:
        # send_msg('动销组定时任务推送', '海外仓订单监控',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓订单监控数据处理完成处理出现问题,请及时排查,失败原因详情请查看airflow日志",
        #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
        #          status='失败')
        raise Exception(traceback.format_exc())


def write_to_mysql_and_ck():
    # 不再写入mysql并清空mysql的数据
    try:
        # 清空ck数据库
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        conn_ck.ck_clear_table(sheet_name='dashbord_new', db_name='yibai_oversea')
        # send_ck_cls.ck_clear_table(sheet_name='dashbord_new')
        sql = """
        INSERT INTO yibai_oversea.dashbord_new
        SELECT
            DISTINCT *, now()
        FROM (
            SELECT `created_time`, `order_id`, `platform_order_id`, `platform_code`, `sku`,`seller_sku`,
                `account_id`,`paytime`,`total_price`,`true_profit_new1`,`complete_status`,`order_typr`,
                `pay_status`,`refound_status`,`status`,`warehouse_name`,`warehouse`,
                `new_price`,`quantity`,`release_money`,`real_profit`,`sales_status`
            FROM yibai_oversea.dashbord_new_data dnc
            
            UNION ALL
            
            SELECT `created_time`,`order_id`,`platform_order_id`,`platform_code`,`sku`,`seller_sku`,
                `account_id`,`paytime`,`total_price`,`true_profit_new1`,`complete_status`,`order_typr`,
                `pay_status`,`refound_status`,`status`,`warehouse_name`,`warehouse`,`new_price`,
                `quantity`,`release_money`,`real_profit`,`sales_status`
            FROM yibai_oversea.dashbord_new_copy dnd
        )
        """
        conn_ck.ck_execute_sql(sql)
        # send_ck_cls.client.execute(sql)
    except Exception as e:
        # send_msg('动销组定时任务推送', '海外仓订单监控',
        #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓订单监控数据处理完成处理出现问题,请及时排查,失败原因详情请查看airflow日志",
        #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
        #          status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    run_listing_contorl()
