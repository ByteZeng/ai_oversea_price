import datetime
import time
import traceback
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
# sku销量拉取
from all_auto_task.sales_calculation import sales_calculation_one
# 锁价表推送
from all_auto_task.yb_oversea_sku_age_lock_price import lock_price_table_push
# 海外仓oversea_age_new
from all_auto_task.over_sea_age import oversea_age_new
# 销量校验
from all_auto_task.days_sale_control_all import check_everyday_sales
# 调度完成后生成listing调价链接
from all_auto_task.change_price_adjustment_link import insert_oversea_adjust_platform_dtl, ali_link_price_adjustment, \
    amazon_link_price_adjustment, cd_link_price_adjustment, eb_link_price_adjustment, walmart_link_price_adjustment, \
    insert_yibai_oversea_adjust_number_data, judge_dtl_temp_table_count
# 销售状态补全
from all_auto_task.status_complate import insert_show_status, status_complete, delete_dtl_and_remake_data
# 海外仓清仓及加快动销与正常品提价数据
from all_auto_task.oversea_adjust_log_upload import cd_log_data_to_distribute_system, ali_log_data_to_distribute_system, \
    amazon_log_data_to_distribute_system, ebay_log_data_to_distribute_system, walmart_log_data_to_distribute_system
from pulic_func.dag_manage.dag_status import dag_status_of_price_one
from all_auto_task.oversea_amazon_listing_detail import insert_amazon_listing_price_detail
from all_auto_task.oversea_ali_listing_detail import insert_ali_listing_price_detail
from all_auto_task.oversea_cd_listing_detail import insert_cd_listing_price_detail
from all_auto_task.oversea_ebay_listing_detail import insert_ebay_listing_price_detail
from all_auto_task.oversea_walmart_listing_detail import insert_walmart_listing_price_detail
from pulic_func.base_api.mysql_connect import connect_to_sql


def send_dingding_info_to_mengqi():
    send_msg('动销组定时任务推送', '海外仓调价数据已执行完成消息发送',
             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓调价数据已生成，请检查数据，并执行",
             mobiles=['+86-13419546972', '+86-18202993981', '+86-13922822326'], is_all=False)


def wait_oversea_adjust_log():
    try:
        i = 1
        while True:
            oversea_adjust_log_status = dag_status_of_price_one('yibai_oversea_price_adjust', f'{time.strftime("%Y-%m-%d")}')
            if oversea_adjust_log_status != 'success':
                if i == 1:
                    print(f'等待调价数据生成{i}')
                    # send_msg('动销组定时任务推送', '等待运费获取',
                    #          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())},调价数据的状态为{oversea_adjust_log_status},等待运费和listing基础数据获取中,详情请查看airflow日志",
                    #          mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                    #          is_all=False)

                time.sleep(10)
                i += 1
            else:
                break
    except:
        send_msg('动销组定时任务推送', '等待调价数据生成',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}等待海外仓调价数据出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def wait_oversea_yunfei_data():
    try:
        i = 1
        while True:
            oversea_yunfei_status = dag_status_of_price_one('oversea_yunfei', f'{time.strftime("%Y-%m-%d")}')
            if oversea_yunfei_status != 'success':
                if i == 1:
                    send_msg('动销组定时任务推送', '等待运费获取',
                             f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())},oversea_yunfei的状态为{oversea_yunfei_status},等待运费和listing基础数据获取中,详情请查看airflow日志",
                             mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                             is_all=False)

                time.sleep(10)
                i += 1
            else:
                break
    except:
        send_msg('动销组定时任务推送', '等待运费获取',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}等待运费和listing基础数据出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def get_newest_time(conn):
    table_list = conn.show_table_list()
    r_list = [i for i in table_list if 'oversea_age_' in i and 'dtl' in i]
    print(2)
    if f"oversea_age_{time.strftime('%Y%m%d')}_dtl" in r_list:
        print('存在今天创建的表')
        sql1 = f'''drop table oversea_age_{time.strftime('%Y%m%d')}_dtl'''
        conn.execute(sql1)
        print(111)
        new_list = r_list.remove(f"oversea_age_{time.strftime('%Y%m%d')}_dtl")
    else:
        new_list = r_list
    print(new_list)
    res_list = [int(i[i.rindex('_') - 8:i.rindex('_')]) for i in new_list if
                '_' not in i[i.rindex('_') - 8:i.rindex('_')]]
    max_number = max(res_list)
    print(max_number)
    return max_number

def get_newest_adjust_time(conn):
    table_list = conn.show_table_list()
    r_list = [i for i in table_list if 'oversea_age_' in i and 'dtl' in i]
    #
    res_list = [int(i[i.rindex('_') - 8:i.rindex('_')]) for i in r_list if
                '_' not in i[i.rindex('_') - 8:i.rindex('_')]]
    res_list.sort(reverse=True)
    for adjust_date in res_list:
        # 20220117未执行降价逻辑跳过，当天也跳过
        if adjust_date in [int(time.strftime('%Y%m%d')), 20220117]:
            continue
        if datetime.datetime.strptime(str(adjust_date), "%Y%m%d").weekday() + 1 in [1, 4]:
            return adjust_date
    raise Exception('未找到最新一天的降价数据的表')


def truncate_table(conn):
    sql1 = f'''drop table  over_sea_age_new_date'''
    conn.execute(sql1)
    sql_create="""create table over_sea_age_new_date like over_sea_age_new"""
    conn.execute(sql_create)
    sql2 = f"""
        insert into over_sea_age_new_date
        select * from over_sea_age_new 
        where date='{time.strftime('%Y-%m-%d')}'
    """
    conn.execute(sql2)



def create_now_table(get_newest_time_str, conn):
    sql3 = f'''create table oversea_age_{time.strftime('%Y%m%d')}_dtl like oversea_age_{get_newest_time_str}_dtl'''
    conn.execute(sql3)


def get_price_datetime(conn):
    sql = f"""select execute_task from oversea_price_task_datetime where execute_date='{time.strftime('%Y%m%d')}'"""
    df = conn.read_sql(sql)
    execute_task = df.loc[0][0]
    print(execute_task)
    return execute_task


def base_table_insert_data():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn_datacenter = connect_to_sql(database='yb_datacenter', data_sys='数据中心1')
        #
        truncate_table(conn)
        print('a')
        get_newest_time_str = get_newest_time(conn)
        print('b')
        create_now_table(get_newest_time_str, conn)
        print('c')
        if get_price_datetime(conn) == "price_down_1":
            # sql = f'''insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
            #             SELECT
            #             SKU,
            #             title,
            #             new_price,
            #             gross,
            #             warehouse_id,
            #             type,
            #             product_status,
            #             last_linest,
            #             linest,
            #             available_stock,
            #             available_stock_money,
            #             on_way_stock,
            #             create_time,
            #             product_size,
            #             product_package_size,
            #             best_warehouse,
            #             warehouse,
            #             inv_age_0_to_30_days,
            #             inv_age_30_to_60_days,
            #             inv_age_60_to_90_days,
            #             inv_age_90_to_120_days,
            #             inv_age_120_to_150_days,
            #             inv_age_150_to_180_days,
            #             inv_age_180_to_210_days,
            #             inv_age_210_plus_days,
            #             inv_age_0_to_40_days,
            #             inv_age_40_to_70_days,
            #             inv_age_70_plus_days,
            #             `SUM(7days_sales)`,
            #             `SUM(15days_sales)`,
            #             `SUM(30days_sales)`,
            #             `SUM(90days_sales)`,
            #             day_sales,
            #             reduce_day_sales,
            #             estimated_sales_days,
            #             estimated_reduce_sales_days,
            #             a_value_section,
            #             day_sales_value_section,
            #             DATE,
            #             inv_age_150_plus_days as over_age150_stock,
            #             inv_age_120_plus_days as over_age120_stock,
            #             inv_age_90_plus_days as over_age90_stock,
            #             inv_age_60_plus_days as over_age60_stock,
            #             inv_age_30_plus_days as over_age30_stock,
            #             0 as estimated_all_sales_days,
            #             0 as over_sale_stock,
            #             0 as over_sale_section,
            #             gradient,
            #             is_chargo,
            #             begin_profit,
            #             section,
            #             lowest_profit,
            #             first_come_profit,
            #             CASE WHEN after_profit<0 THEN
            #             after_profit+IFNULL(单次涨幅,0)  ELSE after_profit END as after_profit,
            #             adjust_recent_clean,
            #             case when begin_profit is null and after_profit<=0.06  and adjust_recent_clean<>'正常' then '降价'
            #             when if(after_profit<0,after_profit+IFNULL(单次涨幅,0),after_profit)-begin_profit>0 then '涨价'
            #             when after_profit -begin_profit<0 then '降价'
            #             else '保持'  end as is_adjust
            #
            #             FROM (
            #             SELECT A.*,
            #             CASE
            #              # when gradient is null  and begin_profit>=0 then 0.06
            #              # when gradient is null  and begin_profit<0 then 0
            #             WHEN A.begin_profit IS NOT NULL  AND available_stock=0 THEN 0.06
            #             WHEN A.begin_profit IS NULL AND section is null then 0.06
            #             WHEN A.begin_profit IS NULL AND section >= 0 then 0.06
            #             WHEN  product_status<>'已停售'   and  gradient='在库库存可售天数，N∈(0,15)'  and product_status<>'已停售' then 0.06
            #             WHEN  if(`SUM(3days_sales)`=0,0.001,`SUM(3days_sales)`)/if(`SUM(7days_sales)`=0,0.01,`SUM(7days_sales)`)>=0.6 THEN begin_profit
            #             WHEN A.begin_profit IS NOT NULL AND over_x_stock=0 and gradient is null and product_status<>'已停售'   then first_come_profit
            #             WHEN A.begin_profit IS NULL AND section <0 and first_come_profit is not null then first_come_profit
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section>=0.06 then 0.06
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section>=0 then begin_profit+section
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section<0   and lowest_profit is not null then GREATEST(begin_profit+section,lowest_profit)
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section<0   and lowest_profit is  null then begin_profit
            #             WHEN A.begin_profit IS NOT NULL AND section is null then begin_profit
            #             else null end as 'after_profit',
            #             CASE WHEN section>0 AND A.begin_profit<0 THEN GREATEST((0-A.begin_profit)/5,section) ELSE NULL END AS  单次涨幅,
            #             CASE
            #              # when gradient is null and begin_profit>=0 then '回调'
            #              # when gradient is null  and begin_profit<0 then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL  AND available_stock=0 THEN '回调'
            #             WHEN A.begin_profit IS NULL AND section is null then '正常'
            #             WHEN A.begin_profit IS NULL AND section >= 0 then '正常'
            #             WHEN  product_status<>'已停售'   and  gradient='在库库存可售天数，N∈(0,15)' and  product_status<>'已停售' then '回调'
            #             WHEN A.begin_profit IS NOT NULL AND over_x_stock=0 and gradient is null and product_status<>'已停售' then '回调'
            #             WHEN A.begin_profit IS NULL AND section <0 then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section>=0.06  then '回调'
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section>=0  then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL  AND GREATEST(begin_profit+section,lowest_profit)>=0  then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL  AND begin_profit+section<0  then '负利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL AND section is null  AND begin_profit>=0 then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL AND section is null AND  GREATEST(begin_profit+section,lowest_profit)>=0 then '正利润加快动销'
            #             WHEN A.begin_profit IS NOT NULL AND section is null  AND begin_profit<0 then '负利润加快动销'
            #             else null end as 'adjust_recent_clean'
            #             FROM (
            #             select A.*,
            #             B.section,
            #             B.lowest_profit,
            #             case
            #             when warehouse='美国仓' then 0.05
            #             when warehouse in ('英国仓','德国仓','意大利仓','法国仓','西班牙仓') then 0.04
            #             when warehouse in ('英国仓','德国仓','意大利仓','法国仓','西班牙仓') then 0.04
            #             else 0.06 end as 'first_come_profit'
            #             from (
            #             select a.*,
            #             case when charge_days=60 then inv_age_60_plus_days
            #             when charge_days=90 then inv_age_90_plus_days
            #             when charge_days=120 then inv_age_120_plus_days
            #             else null end as over_x_stock,
            #              c.after_profit as 'begin_profit' from over_sea_age_new_date a
            #              LEFT JOIN (select sku,warehouse,after_profit from oversea_age_{get_newest_time_str}_dtl where adjust_recent_clean in ('正利润加快动销','负利润加快动销','清仓')) c on a.sku=c.sku  AND a.warehouse = c.warehouse
            #              WHERE DATE='{time.strftime('%Y-%m-%d')}'
            #               and available_stock<>0
            #             union all
            #             select a.*,
            #                 0 as over_x_stock,
            #                 c.after_profit as 'begin_profit' from over_sea_age_new_date a
            #                 INNER JOIN oversea_sale_status b ON a.sku = b.sku AND a.warehouse = b.warehouse  AND b.end_time IS NULL
            #                 INNER JOIN (select sku,warehouse,after_profit from oversea_age_{get_newest_time_str}_dtl where adjust_recent_clean in ('正利润加快动销','负利润加快动销','清仓')) c on a.sku=c.sku  AND a.warehouse = c.warehouse
            #                 WHERE a.DATE='{time.strftime('%Y-%m-%d')}'
            #                 and a.available_stock=0)A
            #             LEFT JOIN yibai_oversea_adjust_section B ON A.gradient=B.gradient AND A.over_section_detail=B.over_section_detail)A)A '''
            insert_oversea_date_week1(get_newest_time_str, conn, conn_datacenter)
        elif get_price_datetime(conn) == "price_down_2":
            # sql = f"""insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
            #             SELECT
            #                 SKU,
            #                 title,
            #                 new_price,
            #                 gross,
            #                 warehouse_id,
            #                 type,
            #                 product_status,
            #                 last_linest,
            #                 linest,
            #                 available_stock,
            #                 available_stock_money,
            #                 on_way_stock,
            #                 create_time,
            #                 product_size,
            #                 product_package_size,
            #                 best_warehouse,
            #                 warehouse,
            #                 inv_age_0_to_30_days,
            #                 inv_age_30_to_60_days,
            #                 inv_age_60_to_90_days,
            #                 inv_age_90_to_120_days,
            #                 inv_age_120_to_150_days,
            #                 inv_age_150_to_180_days,
            #                 inv_age_180_to_210_days,
            #                 inv_age_210_plus_days,
            #                 inv_age_0_to_40_days,
            #                 inv_age_40_to_70_days,
            #                 inv_age_70_plus_days,
            #                 `SUM(7days_sales)`,
            #                 `SUM(15days_sales)`,
            #                 `SUM(30days_sales)`,
            #                 `SUM(90days_sales)`,
            #                 day_sales,
            #                 reduce_day_sales,
            #                 estimated_sales_days,
            #                 estimated_reduce_sales_days,
            #                 a_value_section,
            #                 day_sales_value_section,
            #                 DATE,
            #                 inv_age_150_plus_days as over_age150_stock,
            #                 inv_age_120_plus_days as over_age120_stock,
            #                 inv_age_90_plus_days as over_age90_stock,
            #                 inv_age_60_plus_days as over_age60_stock,
            #                 inv_age_30_plus_days as over_age30_stock,
            #                 0 as estimated_all_sales_days,
            #                 0 as over_sale_stock,
            #                 0 as over_sale_section,
            #                 gradient,
            #                 is_chargo,
            #                 begin_profit,
            #                 section,
            #                 lowest_profit,
            #                 first_come_profit,
            #                 CASE
            #                     WHEN after_profit<0 THEN after_profit + IFNULL(单次涨幅,
            #                     0)
            #                     ELSE after_profit
            #                 END as after_profit,
            #                 adjust_recent_clean,
            #                 case
            #                     when begin_profit is null
            #                     and after_profit <= 0.08
            #                     and adjust_recent_clean <> '正常' then '降价'
            #                     when if(after_profit<0,
            #                     after_profit + IFNULL(单次涨幅,
            #                     0),
            #                     after_profit)-begin_profit>0 then '涨价'
            #                     when after_profit -begin_profit<0 then '降价'
            #                     else '保持'
            #                 end as is_adjust
            #             FROM
            #                 (
            #                 SELECT
            #                     A.*,
            #                     CASE
            #                         # when gradient is null
            #                         # and begin_profit >= 0 then 0.06
            #                         # when gradient is null
            #                         # and begin_profit<0 then 0
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND available_stock = 0 THEN 0.06
            #                         WHEN A.begin_profit IS NULL
            #                         AND section is null then 0.06
            #                         WHEN A.begin_profit IS NULL
            #                         AND section >= 0 then 0.06
            #                         WHEN product_status <> '已停售'
            #                         and gradient = '在库库存可售天数，N∈(0,15)'
            #                         and product_status <> '已停售' then 0.06
            #                         WHEN if(`SUM(3days_sales)`=0,0.001,`SUM(3days_sales)`)/if(`SUM(7days_sales)`=0,0.01,`SUM(7days_sales)`) >= 0.7 THEN begin_profit
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND over_x_stock = 0
            #                         and gradient is null
            #                         and product_status <> '已停售' then first_come_profit
            #                         WHEN A.begin_profit IS NULL
            #                         AND section <0
            #                         and first_come_profit is not null then first_come_profit
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section >= 0.06 then 0.06
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section >= 0 then begin_profit + section
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section<0
            #                         and lowest_profit is not null then GREATEST(begin_profit + section,
            #                         lowest_profit)
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section<0
            #                         and lowest_profit is null then begin_profit
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND section is null then begin_profit
            #                         else null
            #                     end as 'after_profit',
            #                     CASE
            #                         WHEN section>0
            #                         AND A.begin_profit<0 THEN GREATEST((0-A.begin_profit)/ 5,
            #                         section)
            #                         ELSE NULL
            #                     END AS 单次涨幅,
            #                     CASE
            #                         # when gradient is null
            #                         # and begin_profit >= 0 then '回调'
            #                         # when gradient is null
            #                         # and begin_profit<0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND available_stock = 0 THEN '回调'
            #                         WHEN A.begin_profit IS NULL
            #                         AND section is null then '正常'
            #                         WHEN A.begin_profit IS NULL
            #                         AND section >= 0 then '正常'
            #                         WHEN product_status <> '已停售'
            #                         and gradient = '在库库存可售天数，N∈(0,15)'
            #                         and product_status <> '已停售' then '回调'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND over_x_stock = 0
            #                         and gradient is null
            #                         and product_status <> '已停售' then '回调'
            #                         WHEN A.begin_profit IS NULL
            #                         AND section <0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section >= 0.06 then '回调'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section >= 0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND GREATEST(begin_profit + section,
            #                         lowest_profit)>= 0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND begin_profit + section<0 then '负利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND section is null
            #                         AND begin_profit >= 0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND section is null
            #                         AND GREATEST(begin_profit + section,
            #                         lowest_profit)>= 0 then '正利润加快动销'
            #                         WHEN A.begin_profit IS NOT NULL
            #                         AND section is null
            #                         AND begin_profit<0 then '负利润加快动销'
            #                         else null
            #                     end as 'adjust_recent_clean'
            #                 FROM
            #                     (
            #                     select
            #                         A.*,
            #                         B.section,
            #                         B.lowest_profit,
            #                         case
            #                             when warehouse = '美国仓' then 0.05
            #                             when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
            #                             when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
            #                             else 0.06
            #                         end as 'first_come_profit'
            #                     from
            #                         (
            #                         select
            #                             a.*,
            #                             case
            #                                 when charge_days = 60 then inv_age_60_plus_days
            #                                 when charge_days = 90 then inv_age_90_plus_days
            #                                 when charge_days = 120 then inv_age_120_plus_days
            #                                 else null
            #                             end as over_x_stock,
            #                             c.after_profit as 'begin_profit'
            #                         from
            #                             over_sea_age_new_date a
            #                         LEFT JOIN (
            #                             select
            #                                 sku,
            #                                 warehouse,
            #                                 after_profit
            #                             from
            #                                 oversea_age_{get_newest_time_str}_dtl
            #                             where
            #                                 adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
            #                             a.sku = c.sku
            #                             AND a.warehouse = c.warehouse
            #                         WHERE
            #                             DATE = '{time.strftime('%Y-%m-%d')}'
            #                             and available_stock <> 0
            #                     union all
            #                         select
            #                             a.*,
            #                             0 as over_x_stock,
            #                             c.after_profit as 'begin_profit'
            #                         from
            #                             over_sea_age_new_date a
            #                         INNER JOIN oversea_sale_status b ON
            #                             a.sku = b.sku
            #                             AND a.warehouse = b.warehouse
            #                             AND b.end_time IS NULL
            #                         INNER JOIN (
            #                             select
            #                                 sku,
            #                                 warehouse,
            #                                 after_profit
            #                             from
            #                                 oversea_age_{get_newest_time_str}_dtl
            #                             where
            #                                 adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
            #                             a.sku = c.sku
            #                             AND a.warehouse = c.warehouse
            #                         WHERE
            #                             a.DATE = '{time.strftime('%Y-%m-%d')}'
            #                             and a.available_stock = 0)A
            #                     LEFT JOIN yibai_oversea_adjust_section B ON
            #                         A.gradient = B.gradient
            #                         AND A.over_section_detail = B.over_section_detail)A)A"""
            insert_oversea_date_week4(get_newest_time_str, conn, conn_datacenter)
        elif get_price_datetime(conn) == "price_up":
            print('c0')
            #超300天库龄的sku
            sql = f"""SELECT distinct sku as sku, case WHEN country='US' THEN '美国仓'

                           WHEN country IN ('UK','GB') THEN '英国仓'

                           WHEN country IN ('CZ','CS','DE') THEN '德国仓'

                           WHEN country='FR' THEN '法国仓'

                           WHEN country='IT' THEN '意大利仓'

                           WHEN country='AU' THEN '澳洲仓'

                           WHEN country IN ('ES','SP') THEN '西班牙仓'

                           WHEN country='CA' THEN '加拿大仓'

                           WHEN country='JP' THEN '日本仓'

                           WHEN country='PL' THEN '德国仓'

                           ELSE '美国仓' END AS warehouse

                            FROM `yb_oversea_sku_age`
                            where oversea_type='WYT' and `status` in (0,1)
                            and date='{datetime.date.today() - datetime.timedelta(days=2)}' 
                            and inventory_age>=300 and warehouse_stock>0"""
            # df_age=sql_to_pd3(sql=sql,database='yb_datacenter')
            df_age = conn_datacenter.read_sql(sql)
            # pd_to_sql(df_age,if_exists='replace',table='oversea_age_300_plus_days')
            conn.to_sql(df_age, 'oversea_age_300_plus_days')
            print('c1')
            sql1='truncate sku_this_week_acost'
            conn.execute(sql1)
            sql1 = f"""insert into sku_this_week_acost
            select sku,warehouse,this_week_acost from oversea_predict_acost
            where date='{datetime.date.today()}'"""
            conn.execute(sql1)
            print('c2')
            sql = f"""
                insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
                SELECT 
                distinct A.SKU,
                title,
                new_price,
                gross,
                warehouse_id,
                type,
                product_status,
                last_linest,
                linest,
                available_stock,
                available_stock_money,
                on_way_stock,
                create_time,
                product_size,
                product_package_size,
                best_warehouse,
                A.warehouse,
                inv_age_0_to_30_days,
                inv_age_30_to_60_days,
                inv_age_60_to_90_days,
                inv_age_90_to_120_days,
                inv_age_120_to_150_days,
                inv_age_150_to_180_days,
                inv_age_180_to_210_days,
                inv_age_210_plus_days,
                inv_age_0_to_40_days,
                inv_age_40_to_70_days,
                inv_age_70_plus_days,
                `SUM(7days_sales)`,
                `SUM(15days_sales)`,
                `SUM(30days_sales)`,
                `SUM(90days_sales)`,
                day_sales,
                reduce_day_sales,
                estimated_sales_days,
                estimated_reduce_sales_days,
                a_value_section,
                day_sales_value_section,
                DATE,
                inv_age_150_plus_days as over_age150_stock,
                inv_age_120_plus_days as over_age120_stock,
                inv_age_90_plus_days as over_age90_stock,
                inv_age_60_plus_days as over_age60_stock,
                inv_age_30_plus_days as over_age30_stock,
                0 as estimated_all_sales_days,
                0 as over_sale_stock,
                0 as over_sale_section,
                gradient,
                is_chargo,
                acost,
                begin_profit,
                section_temp as section,
                lowest_profit,
                first_come_profit,
                CASE 
--                 when best_warehouse like "%%万邑通%%" and (estimated_sales_days>90 or old_estimated_sales_days>90)
--                 and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
--                 and after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
--                 then ifnull(begin_profit,0)
-- 
--                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 
--                 and t.this_week_acost is not null and t.this_week_acost<0.05 and begin_profit<-0.2
--                 and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
--                 then round(least(ifnull(begin_profit,0)+0.1,-0.2),2)
--                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90
--                 and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days) 
--                 and t.this_week_acost is not null and t.this_week_acost<0.1 and begin_profit<-0.15
--                 then round(least(ifnull(begin_profit,0)+0.05,-0.2),2)
                when if(after_profit<0,after_profit+IFNULL(单次涨幅,0),after_profit)-begin_profit>0  and available_stock > 0 and day_sales < 0.5 then
                     ifnull(begin_profit,0)
                WHEN after_profit<0 THEN 
                     after_profit+IFNULL(单次涨幅,0)  
                ELSE after_profit END as after_profit,
                case when adjust_recent_clean='正常' and s.sale_status in ('正利润加快动销','负利润加快动销') 
                then '回调' else adjust_recent_clean end adjust_recent_clean,
                case
--                 when best_warehouse like "%%万邑通%%"  and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
--                 and (estimated_sales_days>90 or old_estimated_sales_days>90) and after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
--                 then '保持'
--                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 
--                 and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
--                 and t.this_week_acost is not null and t.this_week_acost<0.05 and begin_profit<-0.2
--                 then "涨价"
--                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 
--                 and concat(A.sku,A.warehouse) not in 
--                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
--                 and t.this_week_acost is not null and t.this_week_acost<0.1 and begin_profit<-0.2
--                 then "涨价"
                when begin_profit is null and after_profit<=0.06  and adjust_recent_clean<>'正常' then '降价'
                when if(after_profit<0,after_profit+IFNULL(单次涨幅,0),after_profit)-begin_profit>0 and not(available_stock > 0 and day_sales < 0.5) then '涨价'
                when after_profit -begin_profit<0 then '降价'
                else '保持'  end as is_adjust
                
                FROM (
                SELECT A.*,
                CASE
                
                 # when gradient is null  and begin_profit>=0 then 0.06
                 # when gradient is null  and begin_profit<0 then 0
                WHEN A.begin_profit IS NOT NULL  AND available_stock=0 THEN 0.06
                WHEN A.begin_profit IS NULL AND section_temp is null then 0.06
                WHEN A.begin_profit IS NULL AND section_temp >= 0 then 0.06
                # WHEN  product_status<>'已停售'   and  gradient='在库库存可售天数，N∈(0,15)'  and product_status<>'已停售' then 0.06
                WHEN  if(`SUM(3days_sales)`=0,0.001,`SUM(3days_sales)`)/if(`SUM(7days_sales)`=0,0.01,`SUM(7days_sales)`)>=0.7 THEN begin_profit
                WHEN A.begin_profit IS NOT NULL AND over_x_stock=0 and gradient is null and product_status<>'已停售'   then first_come_profit
                WHEN A.begin_profit IS NULL AND section_temp <0 and first_come_profit is not null then first_come_profit
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp>0.06 then 0.06
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp>=0 then begin_profit+section_temp
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp<0   and lowest_profit is not null then GREATEST(begin_profit+section_temp,lowest_profit)
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp<0   and lowest_profit is  null then begin_profit
                WHEN A.begin_profit IS NOT NULL AND section_temp is null then begin_profit
                else null end as 'after_profit',
                CASE WHEN section_temp>0 AND A.begin_profit<0 THEN GREATEST((0-A.begin_profit)/5,section_temp) ELSE NULL END AS  单次涨幅,
                
                
                
                CASE
                 # when gradient is null and begin_profit>=0 then '回调' 
                 # when gradient is null  and begin_profit<0 then '正利润加快动销'
                WHEN A.begin_profit IS NOT NULL  AND available_stock=0 THEN '回调'
                WHEN A.begin_profit IS NULL AND section_temp is null then '正常'
                WHEN A.begin_profit IS NULL AND section_temp >= 0 then '正常'
                # WHEN  product_status<>'已停售'   and  gradient='在库库存可售天数，N∈(0,15)' and  product_status<>'已停售' then '回调'
                WHEN A.begin_profit IS NOT NULL AND section_temp is null  AND begin_profit<0 then '负利润加快动销'
                WHEN A.begin_profit IS NOT NULL AND over_x_stock=0 and gradient is null and product_status<>'已停售' then '回调'
                WHEN A.begin_profit IS NULL AND section_temp <0 then '正利润加快动销'
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp>0.06  then '回调'
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp>=0  then '正利润加快动销'
                WHEN A.begin_profit IS NOT NULL  AND GREATEST(begin_profit+section_temp,lowest_profit)>=0  then '正利润加快动销'
                WHEN A.begin_profit IS NOT NULL  AND begin_profit+section_temp<0  then '负利润加快动销'
                
                WHEN A.begin_profit IS NOT NULL AND section_temp is null  AND begin_profit>=0 then '正利润加快动销'
                WHEN A.begin_profit IS NOT NULL AND section_temp is null AND  GREATEST(begin_profit+section_temp,lowest_profit)>=0 then '正利润加快动销'
                
                else null end as 'adjust_recent_clean'
                
                FROM (
                
                select A.*,
                B.section_temp,
                B.lowest_profit,
                case 
                when warehouse='美国仓' then 0.05
                when warehouse in ('英国仓','德国仓','意大利仓','法国仓','西班牙仓') then 0.04
                when warehouse in ('英国仓','德国仓','意大利仓','法国仓','西班牙仓') then 0.04
                else 0.06 end as 'first_come_profit'
                
                from (
                select a.*,
                case when charge_days=60 then inv_age_60_plus_days
                when charge_days=90 then inv_age_90_plus_days
                when charge_days=120 then inv_age_120_plus_days
                else null end as over_x_stock,
                 c.after_profit as 'begin_profit' from over_sea_age_new_date a
                 LEFT JOIN (select sku,warehouse,after_profit,estimated_sales_days as old_estimated_sales_days
                from oversea_age_{get_newest_time_str}_dtl where adjust_recent_clean in ('正利润加快动销','负利润加快动销','清仓')) c on a.sku=c.sku  AND a.warehouse = c.warehouse 
                 WHERE DATE='{time.strftime('%Y-%m-%d')}'
                  and available_stock<>0
                union all 
                  select a.*,
                    0 as over_x_stock,
                    c.after_profit as 'begin_profit' from over_sea_age_new_date a
                    INNER JOIN oversea_sale_status b ON a.sku = b.sku AND a.warehouse = b.warehouse  
                    -- AND b.end_time IS NULL 
                    LEFT JOIN (select sku,warehouse,after_profit,
                    estimated_sales_days as old_estimated_sales_days from oversea_age_{get_newest_time_str}_dtl where adjust_recent_clean in ('正利润加快动销','负利润加快动销','清仓')) c on a.sku=c.sku  AND a.warehouse = c.warehouse 
                    WHERE a.DATE='{time.strftime('%Y-%m-%d')}'
                  and a.available_stock=0)A
                LEFT JOIN yibai_oversea_adjust_section B ON A.gradient=B.gradient AND A.over_section_detail=B.over_section_detail AND A.acost = B.acost )A)A
                left join sku_this_week_acost  t on t.sku=A.sku and t.warehouse=A.warehouse
                left join (select sku,warehouse,estimated_sales_days as old_estimated_sales_days 
                from oversea_age_{get_newest_time_str}_dtl) d on A.sku=d.sku and A.warehouse=d.warehouse
                left join oversea_sale_status s on A.sku=s.sku and A.warehouse=s.warehouse
								and s.end_time is null"""

            conn.execute(sql)
            print('c3')
            # df=sql_to_pd(sql=sql,database='over_sea')
            # sql1 = f"""select opa.*,case when BB.is_gc='仅谷仓有库存' then "是" else '否' end as is_gc
            # ,case when opa.available_stock>=5 and BB.is_gc is not null then "库存大于5"
            # when opa.available_stock<5 and BB.is_gc is not null then "库存小于5"
            # else "否" end as gc_warehouse_stock
            # from oversea_predict_acost opa
            # inner join (select gc.sku,gc.warehouse,
            # case when wyt.warehosue_stock is null then '仅谷仓有库存' else '万邑通有库存'  end as is_gc
            # from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            # from warehouse_stock_charges
            # where date="{datetime.date.today()}" and oversea_type="GC" and warehouse_stock>5
            # group by sku,warehouse) gc
            # left join
            # (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            # from warehouse_stock_charges
            # where date="{datetime.date.today()}" and oversea_type="WYT" and warehouse_stock>0
			#
            # group by sku,warehouse)wyt on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
			# 			having is_gc='仅谷仓有库存'
			#
            # )BB on opa.sku=BB.sku and opa.warehouse=BB.warehouse  and BB.is_gc='仅谷仓有库存'
            # where  opa.date='{datetime.date.today()}'"""
            # df1 = sql_to_pd(sql=sql1, database='over_sea')
            # sql2 = f"""select wyt.sku,wyt.warehouse,
            # 						case when gc.warehosue_stock is null
            # 						then '是'
            # 						else '否'
            # 						end as wyt_kucun
            # 						from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            # 						from warehouse_stock_charges
            # 						where date="{datetime.date.today()}" and oversea_type="WYT" and warehouse_stock>0
            # 						group by sku,warehouse) wyt
            # 						left join
            # 						(select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            # 						from warehouse_stock_charges
            # 						where date="{datetime.date.today()}" and oversea_type="GC" and warehouse_stock>0
            # 						group by sku,warehouse)gc on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse"""
            # df2 = sql_to_pd(sql=sql2, database='over_sea')

            # df3 = sql_to_pd(sql=f"""select sku,warehouse,adjust_recent_clean from oversea_age_{get_newest_time_str}_dtl""",
            #                 database='over_sea')
            # df2 = df2.merge(df3, on=['sku', 'warehouse'], how='left')
            # df2.columns = ['SKU', 'warehouse', 'wyt_kucun', '销售状态']
            #
            #
            # #仅万邑通库存判断
            # df = df.merge(df2, on=['SKU', 'warehouse'], how='left')
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 150)
            #        & (df['after_profit'] - df['begin_profit'] < 0) & (df['begin_profit'].notnull()), 'after_profit'] = \
            # df['begin_profit']
            #
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 60) & (df['day_sales'] > 0.3)
            #        & (df['begin_profit'].notnull()), 'after_profit'] = df['begin_profit']
            #
            # # 销售状态
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 150)
            #        & (df['after_profit'] - df['begin_profit'] < 0) & (
            #            df['begin_profit'].notnull()), 'adjust_recent_clean'] = df['销售状态']
            #
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 60) & (df['day_sales'] > 0.3)
            #        & (df['begin_profit'].notnull()), 'adjust_recent_clean'] = df['销售状态']
            #
            # # 涨价降价判断
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 150)
            #        & (df['after_profit'] - df['begin_profit'] < 0) & (df['begin_profit'].notnull()), 'is_adjust'] = '保持'
            #
            # df.loc[(df['wyt_kucun'] == '是') & (df['inv_age_180_to_210_days'] + df['inv_age_210_plus_days'] == 0) & (
            #             df['available_stock'] > 5) & (df['estimated_sales_days'] < 60) & (df['day_sales'] > 0.3)
            #        & (df['begin_profit'].notnull()), 'is_adjust'] = '保持'
            # df.drop(columns=['wyt_kucun', '销售状态'], inplace=True)
            #
            # #仅谷仓有库存
            # # 谷仓
            # df11 = df1[['sku', 'warehouse', '昨日日销', 'this_week_acost', 'is_gc']]
            # df11.columns = ['SKU', 'warehouse', '昨日日销', 'this_week_acost', 'is_gc']
            # df = df.merge(df11, on=['SKU', 'warehouse'], how='left')
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #             df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.5), 'after_profit'] = -0.5
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #             df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.4) & (df['begin_profit'] > -0.5)
            # , 'after_profit'] = df['begin_profit'].apply(lambda x: min(x + 0.15, -0.2))
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #             df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.3) & (df['begin_profit'] >= -0.4)
            # , 'after_profit'] = df['begin_profit'].apply(lambda x: min(x + 0.1, -0.2))
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #             df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.2) & (df['begin_profit'] >= -0.3)
            # , 'after_profit'] = df['begin_profit'].apply(lambda x: min(x + 0.05, -0.2))
            #
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #             df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.2), 'is_adjust'] = '涨价'
            #
            # df.loc[(df['is_gc'] == '是') & (df['available_stock'] > 5) & (df['昨日日销'] >= 0.3) & (
            #         df['this_week_acost'] <= 0.1) & (df['begin_profit'] < -0.2), 'adjust_recent_clean'] = '负利润加快动销'
            #
            # df.drop(columns=['昨日日销', 'this_week_acost', 'is_gc'], inplace=True)
            #
            # pd_to_sql(df=df,table=f"oversea_age_{time.strftime('%Y%m%d')}_dtl",if_exists='append')
            #
            #
            #
        else:
            raise Exception('不执行调价')
        print('d')
        send_msg('动销组定时任务推送', '调价基础表调度', f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓调价基础表调度更新完成",
                 is_all=False)
        conn.close()
        conn_datacenter.close()
    except:
        send_msg('动销组定时任务推送', '调价基础表调度',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓调价基础表调度处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def insert_oversea_date_week4(get_newest_time_str, conn, conn_datacenter):
    try:
        now_time = time.strftime('%Y-%m-%d')
        print(now_time)
        print(get_newest_adjust_time(conn))
        sql = """truncate table oversea_age_date_dtl_temp"""
        conn.execute(sql)
        sql = f"""
            insert into oversea_age_date_dtl_temp
            SELECT
                distinct SKU,
                title,
                new_price,
                gross,
                warehouse_id,
                type,
                product_status,
                last_linest,
                linest,
                available_stock,
                available_stock_money,
                on_way_stock,
                create_time,
                product_size,
                product_package_size,
                best_warehouse,
                warehouse,
                inv_age_0_to_30_days,
                inv_age_30_to_60_days,
                inv_age_60_to_90_days,
                inv_age_90_to_120_days,
                inv_age_120_to_150_days,
                inv_age_150_to_180_days,
                inv_age_180_to_210_days,
                inv_age_210_plus_days,
                inv_age_0_to_40_days,
                inv_age_40_to_70_days,
                inv_age_70_plus_days,
                `SUM(7days_sales)`,
                `SUM(15days_sales)`,
                `SUM(30days_sales)`,
                `SUM(90days_sales)`,
                day_sales,
                reduce_day_sales,
                estimated_sales_days,
                estimated_reduce_sales_days,
                a_value_section,
                day_sales_value_section,
                DATE,
                inv_age_150_plus_days as over_age150_stock,
                inv_age_120_plus_days as over_age120_stock,
                inv_age_90_plus_days as over_age90_stock,
                inv_age_60_plus_days as over_age60_stock,
                inv_age_30_plus_days as over_age30_stock,
                0 as estimated_all_sales_days,
                0 as over_sale_stock,
                0 as over_sale_section,
                gradient,
                is_chargo,
                acost,
                begin_profit,
                section,
                lowest_profit,
                first_come_profit,
                CASE
                    WHEN after_profit<0 THEN after_profit + IFNULL(单次涨幅,0)
                    ELSE ifnull(after_profit,0)
                END as after_profit,    
                adjust_recent_clean,
                case
                    when begin_profit is null
                    and after_profit <= 0.08
                    and adjust_recent_clean <> '正常' then '降价'
                    when if(after_profit<0,
                    after_profit + IFNULL(单次涨幅, 0),
                    after_profit)-begin_profit>0 then '涨价'
                    when after_profit -begin_profit<0 then '降价'
                    else '保持'
                end as is_adjust,
                adjust_flag,
                last_begin_profit 
            FROM
                (
                SELECT
                    A.*,
                    CASE
                        WHEN A.begin_profit IS NOT NULL
                        AND A.available_stock = 0 THEN 0.06
                        WHEN A.begin_profit IS NULL
                        AND section is null then 0.06
                        WHEN A.begin_profit IS NULL
                        AND section >= 0 then 0.06
                        # WHEN product_status <> '已停售' and gradient = '在库库存可售天数，N∈(0,15)' then 0.06
                        WHEN if(`SUM(3days_sales)`=0,0.001,`SUM(3days_sales)`)/if(`SUM(7days_sales)`=0,0.01,`SUM(7days_sales)`) >= 0.7 THEN begin_profit
                        WHEN A.begin_profit IS NOT NULL

                        and gradient is null
                        and product_status <> '已停售' then first_come_profit
                        WHEN A.begin_profit IS NULL
                        AND section <0
                        and first_come_profit is not null then first_come_profit
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section > 0.06 then 0.06
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section >= 0 then begin_profit + section
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0
                        and lowest_profit is not null then GREATEST(begin_profit + section,
                        lowest_profit)
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0
                        and lowest_profit is null then begin_profit
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null then begin_profit
                        else null
                    end as 'after_profit',
                    CASE
                        WHEN section>0
                        AND A.begin_profit<0 THEN GREATEST((0-A.begin_profit)/ 5,
                        section)
                        ELSE NULL
                    END AS 单次涨幅,
                    CASE
                        WHEN A.begin_profit IS NOT NULL
                        AND A.available_stock = 0 THEN '回调'
                        WHEN A.begin_profit IS NULL
                        AND section is null then '正常'
                        WHEN A.begin_profit IS NULL
                        AND section >= 0 then '正常'
                        # WHEN product_status <> '已停售' and gradient = '在库库存可售天数，N∈(0,15)' then '回调'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND begin_profit<0 then '负利润加快动销'

                        WHEN A.begin_profit IS NOT NULL
                        AND over_x_stock = 0
                        and gradient is null
                        and product_status <> '已停售' then '回调'
                        WHEN A.begin_profit IS NULL
                        AND section <0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section > 0.06 then '回调'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section >= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND GREATEST(begin_profit + section,
                        lowest_profit)>= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0 then '负利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND begin_profit >= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND GREATEST(begin_profit + section,
                        lowest_profit)>= 0 then '正利润加快动销'
                        else null
                    end as 'adjust_recent_clean',
                    get_price_adjust_flag(B.predict_acost_after_this_week, A.day_sales, `净利增益`, 
                        A.last_is_adjust, B.predict_acost_after_last_week, A.begin_profit) as adjust_flag
                FROM
                    (
                    select
                        A.*,
                        B.section,
                        B.lowest_profit,
                        case
                            when warehouse = '美国仓' then 0.05
                            when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
                            when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
                            else 0.06
                        end as 'first_come_profit'
                    from
                        (
                        select
                            a.*,
                            case
                                when charge_days = 60 then inv_age_60_plus_days
                                when charge_days = 90 then inv_age_90_plus_days
                                when charge_days = 120 then inv_age_120_plus_days
                                else null
                            end as over_x_stock,
                            c.after_profit as 'begin_profit',
                            c.is_adjust as last_is_adjust,
                            c.last_begin_profit
                        from
                            over_sea_age_new_date a
                        LEFT JOIN (
                            select
                                sku,
                                warehouse,
                                after_profit,
                                is_adjust,
                                begin_profit as last_begin_profit
                            from
                                oversea_age_{get_newest_time_str}_dtl
                            where
                                adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
                            a.sku = c.sku
                            AND a.warehouse = c.warehouse
                        WHERE
                            DATE = '{now_time}'
                            and available_stock <> 0
                    union all
                        select
                            a.*,
                            0 as over_x_stock,
                            c.after_profit as 'begin_profit',
                            c.is_adjust as last_is_adjust,
                            last_begin_profit
                        from
                            over_sea_age_new_date a
                        INNER JOIN oversea_sale_status b ON
                            a.sku = b.sku
                            AND a.warehouse = b.warehouse
                        --    AND b.end_time IS NULL
                        LEFT JOIN (
                            select
                                sku,
                                warehouse,
                                after_profit,
                                is_adjust,
                                begin_profit as last_begin_profit
                            from
                                oversea_age_{get_newest_time_str}_dtl
                            where
                                adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
                            a.sku = c.sku
                            AND a.warehouse = c.warehouse
                        WHERE
                            a.DATE = '{now_time}'
                            and a.available_stock = 0)A
                    LEFT JOIN yibai_oversea_adjust_section B ON
                        A.gradient = B.gradient
                        AND A.over_section_detail = B.over_section_detail
                        AND A.acost = B.acost        
               )A
               left join  
                    oversea_predict_acost B 
                    on (A.sku = B.sku and A.warehouse = B.warehouse and B.date = '{now_time}')
               )A  
        """
        conn.execute(sql)
        # sql = f"""insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
        #             select SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, linest,
        #             available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
        #             best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
        #             inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
        #             inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
        #             `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
        #             estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
        #             over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
        #             over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
        #             case when after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
        #                 ifnull(begin_profit,0)
        #             else
        #                 after_profit
        #             end as after_profit,
        #             case when after_profit>=0 then
        #                  if(adjust_recent_clean='负利润加快动销', '正利润加快动销', adjust_recent_clean)
        #                  when after_profit < 0 then
        #                  if(adjust_recent_clean='正利润加快动销', '负利润加快动销', adjust_recent_clean)
        #                  else adjust_recent_clean
        #                  end,
        #             case when after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
        #                 '保持'
        #             else
        #                 is_adjust
        #             end as is_adjust
        #
        #             from
        #             (
        #                 SELECT SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, concat(ifnull(linest,''), adjust_flag) as linest,
        #                 available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
        #                 best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
        #                 inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
        #                 inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
        #                 `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
        #                 estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
        #                 over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
        #                 over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
        #                 case when begin_profit is null or available_stock = 0
        #                             then after_profit
        #                      when adjust_flag = 1 -- 降价
        #                             then if(is_adjust in ('保持','涨价'), ifnull(begin_profit, 0)-0.02, after_profit)
        #                      when adjust_flag in (0, 2) -- 保持
        #                             then ifnull(begin_profit, 0)
        #                      when adjust_flag = 3 -- 涨价
        #                             then if(begin_profit <= -0.2, begin_profit + 0.05, begin_profit + 0.03)
        #                      when adjust_flag = 4 -- 降价至前一次利润率
        #                             then ifnull(last_begin_profit, 0)
        #                      when adjust_flag = 5 -- 涨价至前一次利润率, 前一次的取不到则保持当前的
        #                             then  ifnull(
        #                                             (select
        #                                                 b.begin_profit
        #                                             from
        #                                                 oversea_age_{get_newest_adjust_time()}_dtl b
        #                                             where
        #                                                 b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
        #                                             and a.sku = b.sku
        #                                             and a.warehouse = b.warehouse
        #                                             and b.is_adjust = '降价'), ifnull(a.begin_profit,0))
        #                     else ifnull(a.begin_profit, 0)
        #                 end as after_profit,
        #                 adjust_recent_clean,
        #                 case when begin_profit is null or available_stock = 0
        #                            then is_adjust
        #                      when adjust_flag in (1, 4)
        #                             then '降价'
        #                      when adjust_flag in (0, 2)
        #                             then '保持'
        #                      when adjust_flag = 3 or (adjust_flag = 5 and not isnull((select
        #                                                                                 b.begin_profit
        #                                                                             from
        #                                                                                 oversea_age_{get_newest_adjust_time()}_dtl b
        #                                                                             where
        #                                                                                 b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
        #                                                                             and a.sku = b.sku
        #                                                                             and a.warehouse = b.warehouse
        #                                                                             and b.is_adjust = '降价')))
        #                             then '涨价'
        #                      else
        #                      '保持'
        #                 end is_adjust
        #                 FROM over_sea.oversea_age_date_dtl_temp a
        #             ) t
        #       """

        # 超300天库龄的sku
        sql = f"""SELECT distinct sku as sku, case WHEN country='US' THEN '美国仓'

                                   WHEN country IN ('UK','GB') THEN '英国仓'

                                   WHEN country IN ('CZ','CS','DE') THEN '德国仓'

                                   WHEN country='FR' THEN '法国仓'

                                   WHEN country='IT' THEN '意大利仓'

                                   WHEN country='AU' THEN '澳洲仓'

                                   WHEN country IN ('ES','SP') THEN '西班牙仓'

                                   WHEN country='CA' THEN '加拿大仓'

                                   WHEN country='JP' THEN '日本仓'

                                   WHEN country='PL' THEN '德国仓'

                                   ELSE '美国仓' END AS warehouse

                                    FROM `yb_oversea_sku_age`
                                    where oversea_type='WYT' and `status` in (0,1)
                                    and date='{datetime.date.today() - datetime.timedelta(days=2)}' 
                                    and inventory_age>=300 and warehouse_stock>0"""
        # df_age = sql_to_pd3(sql=sql, database='yb_datacenter')
        df_age = conn_datacenter.read_sql(sql)
        # pd_to_sql(df_age, if_exists='replace', table='oversea_age_300_plus_days')
        conn.to_sql(df_age, 'oversea_age_300_plus_days')

        # 超240天库龄的sku
        sql = f"""SELECT distinct sku as sku, case WHEN country='US' THEN '美国仓'

                                   WHEN country IN ('UK','GB') THEN '英国仓'

                                   WHEN country IN ('CZ','CS','DE') THEN '德国仓'

                                   WHEN country='FR' THEN '法国仓'

                                   WHEN country='IT' THEN '意大利仓'

                                   WHEN country='AU' THEN '澳洲仓'

                                   WHEN country IN ('ES','SP') THEN '西班牙仓'

                                   WHEN country='CA' THEN '加拿大仓'

                                   WHEN country='JP' THEN '日本仓'

                                   WHEN country='PL' THEN '德国仓'

                                   ELSE '美国仓' END AS warehouse

                                    FROM `yb_oversea_sku_age`
                                    where oversea_type='WYT' and `status` in (0,1)
                                    and date='{datetime.date.today() - datetime.timedelta(days=2)}' 
                                    and inventory_age>=240 and warehouse_stock>0"""
        # df_age_240 = sql_to_pd3(sql=sql, database='yb_datacenter')
        df_age_240 = conn_datacenter.read_sql(sql)
        # pd_to_sql(df_age_240, if_exists='replace', table='oversea_age_240_plus_days')
        conn.to_sql(df_age_240, 'oversea_age_240_plus_days')

        sql1 = 'truncate sku_this_week_acost'
        conn.execute(sql1)
        sql1 = f"""insert into sku_this_week_acost
        select sku,warehouse,this_week_acost from oversea_predict_acost
        where date='{datetime.date.today()}'"""
        conn.execute(sql1)

        #设谷仓兜底值
        # 20220725谷仓兜底值要求改为-0.25（20220809 改为-0.3,20221021 改为-0.4,20221026改为-0.5，2022115改为-0.4，20221207改为-0.45,1209改-0.55,1226改-0.8
        a=-0.6
        sql= f"""insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
               select t.SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, linest,
                available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
                best_warehouse, t.warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
                inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
                inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
                `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
                estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
                over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
                over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
                
                case when adjust_recent_clean in ('正利润加快动销','负利润加快动销') and after_profit>0.04 then 0.04
                --                 when best_warehouse like "%%万邑通%%" and (estimated_sales_days>90 or old_estimated_sales_days>90) and 
                --                 after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then ifnull(begin_profit,0)
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.01 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.15,-0.2),2)
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.02 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.14,-0.2),2)
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.03 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.13,-0.2),2)
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.04 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.12,-0.2),2)
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.05 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.11,-0.2),2)
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.08 and  begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.1,-0.2),2)
                --             
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.1 and begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then round(least(ifnull(begin_profit,0)+0.08,-0.2),2)
                
                --                 202301海外仓开始循环降价，2022最后一轮降价动销差的sku直接降至之前最低兜底-0.8
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < 0 and begin_profit = -0.8
                then -0.8        

                when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<{a} and 
                after_profit - ifnull(begin_profit, 0) >0
                then round(after_profit,4)



                --                 存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(1,∞)部分-0.4兜底；(0.3,1]部分-0.5兜底
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and begin_profit = -0.4 and
                gradient is null
                then -0.4
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and begin_profit = -0.5 and gradient is null
                then -0.5
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and begin_profit > -0.4 and gradient is null and
                day_sales > 1
                then -0.4
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and begin_profit > -0.5 and gradient is null and
                day_sales <= 1
                then -0.5
                
                
                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and gradient in 
                ('存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(5,∞);N∈(40,∞);M∈(100,∞)',
                '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(3,5];N∈(40,∞);M∈(100,∞)',
                '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(1,3];N∈(40,∞);M∈(90,∞)')
                then -0.4

                when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and gradient in 
                ('存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)',
                '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)')
                then -0.5
                

                --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.6
                --                 then {a}
                --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.4 
                --                 then least(begin_profit+0.15,{a})
                --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.3 
                --                 then least(begin_profit+0.1,{a})  
                --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<{a}
                --                 then least(begin_profit+0.05,{a})                           
                
                when best_warehouse like "%%谷仓%%" and available_stock>0  and begin_profit>={a} and after_profit  <{a}
                then {a}
                when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
                ifnull(begin_profit,0)

                else  round(after_profit,4)
                end as after_profit,
                case when after_profit>=0 then
                if(adjust_recent_clean='负利润加快动销', '正利润加快动销', adjust_recent_clean)               
             
                when after_profit < 0 then
                if(adjust_recent_clean='正利润加快动销', '负利润加快动销', adjust_recent_clean)
                when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5
                then if(ifnull(begin_profit,0)>=0,'正利润加快动销','负利润加快动销')
                when adjust_recent_clean ='正常' and ss.sale_status in ('正利润加快动销','负利润加快动销')
                then '回调'
                else adjust_recent_clean
                end as adjsut_recent_clean,
                case when adjust_recent_clean in ('正利润加快动销','负利润加快动销') and after_profit>0.04 then '降价'
                --                 when best_warehouse like "%%万邑通%%" and (estimated_sales_days>90 or old_estimated_sales_days>90)
                --                 and after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then "保持"
                --                 
                --                 when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
                --                 s.this_week_acost<0.1 and begin_profit<-0.2
                --                 and concat(t.sku,t.warehouse) not in 
                --                 (select concat(sku,warehouse) from oversea_age_300_plus_days)
                --                 then "涨价"
                when best_warehouse like "%%谷仓%%"  and available_stock>0 and begin_profit<{a} then '涨价'
                
                when best_warehouse like "%%谷仓%%" and available_stock>0 and  begin_profit={a} and after_profit <={a}
                then '保持'
                when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
                '保持'
                else
                is_adjust
                end as is_adjust
                
                from
                (
                SELECT a.SKU, title, a.new_price, a.gross, a.warehouse_id,a. `type`, product_status, last_linest, concat(ifnull(linest,''), adjust_flag) as linest, 
                a.available_stock, a.available_stock_money, on_way_stock, create_time, product_size, product_package_size, 
                best_warehouse, a.warehouse, a.inv_age_0_to_30_days, a.inv_age_30_to_60_days, a.inv_age_60_to_90_days, 
                a.inv_age_90_to_120_days, a.inv_age_120_to_150_days, a.inv_age_150_to_180_days, a.inv_age_180_to_210_days, 
                a.inv_age_210_plus_days, a.inv_age_0_to_40_days, a.inv_age_40_to_70_days, a.inv_age_70_plus_days, a.`SUM(7days_sales)`, 
                a.`SUM(15days_sales)`, a.`SUM(30days_sales)`, a.`SUM(90days_sales)`, a.day_sales, reduce_day_sales, estimated_sales_days, 
                estimated_reduce_sales_days, a_value_section, day_sales_value_section, a.`DATE`, over_age150_stock, 
                over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, a.estimated_all_sales_days, over_sale_stock, 
                over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
                case when begin_profit is null or a.available_stock = 0
                then after_profit
                --                 when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and a.available_stock>5 
                --                 and a.estimated_sales_days<150 and a.adjust_flag in (1,4)  and begin_profit<0 
                --                 then  ifnull(begin_profit, 0)
                --                 when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and a.available_stock>5 
                --                 and a.estimated_sales_days<60 and a.day_sales>0.3 and begin_profit<0 then  round(ifnull(begin_profit, 0)+0.05,2)
                -- 若存在库龄超240天，且超出240天库龄部分库存超出30天可售 则降0.15, 若存在库龄超210天，且超出210天库龄部分库存超出60天可售,则降价0.1
                when  best_warehouse like '%%万邑通%%' and a.available_stock>0  and concat(a.SKU,a.warehouse) in
                (select concat(sku,warehouse) from oversea_age_240_plus_days) and a.estimated_sales_days>30
                then ifnull(begin_profit,0)-0.15

                when best_warehouse like '%%万邑通%%' and a.available_stock>0   and inv_age_210_plus_days>0
                and a.estimated_sales_days>60
                then ifnull(begin_profit,0)-0.1
                
                -- 20220428 若存在库龄超180天，超出90天可售，则降价0.05
                when best_warehouse like '%%万邑通%%' and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`>0 
                and a.available_stock>0 and a.estimated_sales_days>90 then ifnull(begin_profit,0)-0.05
                
                when best_warehouse like "%%谷仓%%"  and a.available_stock>0  and begin_profit<-0.6 and 
                adjust_recent_clean='负利润加快动销' 
                then -0.6
                when best_warehouse like "%%谷仓%%" and a.available_stock>0 and begin_profit<-0.4  
                and adjust_recent_clean='负利润加快动销'
                then  round(least(ifnull(begin_profit, 0)+0.15,{a}),2)
                when best_warehouse like "%%谷仓%%"  and a.available_stock>0  and begin_profit<-0.3 
                and adjust_recent_clean='负利润加快动销'
                
                then round(least(ifnull(begin_profit, 0)+0.1,{a}),2)
                when best_warehouse like "%%谷仓%%" and a.available_stock>0  and begin_profit<{a}
                and adjust_recent_clean='负利润加快动销'
                then  round(least(ifnull(begin_profit, 0)+0.05 ,{a}),2)
                when adjust_flag = 1 -- 降价
                then if(is_adjust in ('保持','涨价'), ifnull(begin_profit, 0)-0.02, after_profit)
                when adjust_flag in (0, 2) -- 保持
                then ifnull(begin_profit, 0)
                when adjust_flag = 3 -- 涨价
                then if(begin_profit <= -0.2, begin_profit + 0.05, begin_profit + 0.03)
                when adjust_flag = 4 -- 降价至前一次利润率
                then ifnull(last_begin_profit, 0)
                when adjust_flag = 5 -- 涨价至前一次利润率, 前一次的取不到则保持当前的
                then  ifnull(
                (select
                b.begin_profit
                from
                oversea_age_{get_newest_time_str}_dtl b
                where
                b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
                and a.sku = b.sku
                and a.warehouse = b.warehouse
                and b.is_adjust = '降价'), ifnull(a.begin_profit,0))
                else ifnull(a.begin_profit, 0)
                end as after_profit,
                adjust_recent_clean,
                case when begin_profit is null or a.available_stock = 0
                then is_adjust
                --                 when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and a.available_stock>5 
                --                 and a.estimated_sales_days<150 and a.adjust_flag in (1,4) and begin_profit<0 then '保持'
                --                 when cc.wyt_kucun="是" and a.available_stock>5 and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and a.available_stock>5 
                --                 and a.estimated_sales_days<60 and a.day_sales>0.3  and begin_profit<0
                --                 then  '涨价'
                
                -- 20220428 若存在库龄超180天，超出90天可售，则降价0.05
                when best_warehouse like '%%万邑通%%' and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`>0 and a.available_stock>0 
                and a.estimated_sales_days>90 then '降价'
                -- 若存在库龄超240天，且超出240天库龄部分库存超出30天可售降0.15， 若存在库龄超210天，且超出210天库龄部分库存超出60天可售,则降价0.1
                
                when  best_warehouse like '%%万邑通%%' and a.available_stock>0  and concat(a.SKU,a.warehouse) in
                (select concat(sku,warehouse) from oversea_age_240_plus_days) and a.estimated_sales_days>30
                then '降价'
                when best_warehouse like '%%万邑通%%' and a.available_stock>0   and inv_age_210_plus_days>0
                and a.estimated_sales_days>60
                then '降价'
                
                
                
                when best_warehouse like "%%谷仓%%"  and a.available_stock>0 and begin_profit<{a}
                and adjust_recent_clean='负利润加快动销'
                then '涨价'
                when adjust_flag in (1, 4)
                then '降价'
                when adjust_flag in (0, 2)
                then '保持'
                when adjust_flag = 3 or (adjust_flag = 5 and not isnull((select
                b.begin_profit
                from
                oversea_age_{get_newest_time_str}_dtl b
                where
                b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
                and a.sku = b.sku
                and a.warehouse = b.warehouse
                and b.is_adjust = '降价')))
                then '涨价'
                else
                '保持'
                end is_adjust
                FROM over_sea.oversea_age_date_dtl_temp a                 
                --                 left join (select a.*,case when '不涨价' is null then '可降价' else '不涨价' end is_increase
                --                 from (select wyt.sku,wyt.warehouse,
                --                 case when gc.warehosue_stock is null 
                --                 then '是' 
                --                 else '否'
                --                 end as wyt_kucun
                --                 from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
                --                 from warehouse_stock_charges
                --                 where date="{now_time}" and oversea_type="WYT" and warehouse_stock>0
                --                 group by sku,warehouse) wyt
                --                 left join
                --                 (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
                --                 from warehouse_stock_charges
                --                 where date="{now_time}" and oversea_type="GC" and warehouse_stock>0
                --                 group by sku,warehouse)gc on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
                --                 HAVING   wyt_kucun='是') a
                --                 left join 
                --                 (select distinct sku,warehouse,'不涨价' from wyt_no_price_increase_details) c
                --                 
                --                 on a.sku=c.sku and a.warehouse=c.warehouse
                --                 having is_increase='可降价')cc on cc.sku=a.SKU and cc.warehouse=a.warehouse
                ) t 
                left join sku_this_week_acost s on t.sku=s.sku and t.warehouse=s.warehouse
                left join (select sku,warehouse,estimated_sales_days as old_estimated_sales_days 
                from oversea_age_{get_newest_time_str}_dtl) d on t.sku=d.sku and t.warehouse=d.warehouse  
                left join oversea_sale_status ss on t.sku=ss.sku and t.warehouse=ss.warehouse  and ss.end_time is null   
                """
        conn.execute(sql)

    # #写入仅万邑通有库存有降价的明细数据
    #     sql1=f"""insert into wyt_no_price_increase_details
    #         select a.sku,b.warehouse,a.begin_profit,a.after_profit,b.wyt_kucun,a.is_adjust,a.DATE
    #         from oversea_age_{time.strftime('%Y%m%d')}_dtl a
    #         inner join (select wyt.sku,wyt.warehouse,
    #         case when gc.warehosue_stock is null
    #         then '是'
    #         else '否'
    #         end as wyt_kucun
    #         from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
    #         from warehouse_stock_charges
    #         where date="{datetime.date.today()}" and oversea_type="WYT" and warehouse_stock>0
    #         group by sku,warehouse) wyt
    #         left join
    #         (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
    #         from warehouse_stock_charges
    #         where date="{datetime.date.today()}" and oversea_type="GC" and warehouse_stock>0
    #         group by sku,warehouse)gc on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
    #         having wyt_kucun='是') b on a.sku=b.sku and a.warehouse=b.warehouse
    #         where a.after_profit-a.begin_profit<0"""
    #     conn.execute(sql1)
    except:
        raise Exception(traceback.format_exc())


def insert_oversea_date_week1(get_newest_time_str, conn, conn_datacenter):
    try:
        now_time = time.strftime('%Y-%m-%d')
        print(now_time)
        print(get_newest_adjust_time(conn))
        sql = """truncate table oversea_age_date_dtl_temp"""
        conn.execute(sql)
        sql = f"""
            insert into oversea_age_date_dtl_temp
            SELECT
                distinct
                SKU,
                title,
                new_price,
                gross,
                warehouse_id,
                type,
                product_status,
                last_linest,
                linest,
                available_stock,
                available_stock_money,
                on_way_stock,
                create_time,
                product_size,
                product_package_size,
                best_warehouse,
                warehouse,
                inv_age_0_to_30_days,
                inv_age_30_to_60_days,
                inv_age_60_to_90_days,
                inv_age_90_to_120_days,
                inv_age_120_to_150_days,
                inv_age_150_to_180_days,
                inv_age_180_to_210_days,
                inv_age_210_plus_days,
                inv_age_0_to_40_days,
                inv_age_40_to_70_days,
                inv_age_70_plus_days,
                `SUM(7days_sales)`,
                `SUM(15days_sales)`,
                `SUM(30days_sales)`,
                `SUM(90days_sales)`,
                day_sales,
                reduce_day_sales,
                estimated_sales_days,
                estimated_reduce_sales_days,
                a_value_section,
                day_sales_value_section,
                DATE,
                inv_age_150_plus_days as over_age150_stock,
                inv_age_120_plus_days as over_age120_stock,
                inv_age_90_plus_days as over_age90_stock,
                inv_age_60_plus_days as over_age60_stock,
                inv_age_30_plus_days as over_age30_stock,
                0 as estimated_all_sales_days,
                0 as over_sale_stock,
                0 as over_sale_section,
                gradient,
                is_chargo,
                acost,
                begin_profit,
                section,
                lowest_profit,
                first_come_profit,
                CASE
                    WHEN after_profit<0 THEN after_profit + IFNULL(单次涨幅,0)
                    ELSE ifnull(after_profit,0)
                END as after_profit,    
                adjust_recent_clean,
                case
                    when begin_profit is null
                    and after_profit <= 0.08
                    and adjust_recent_clean <> '正常' then '降价'
                    when if(after_profit<0,
                    after_profit + IFNULL(单次涨幅, 0),
                    after_profit)-begin_profit>0 then '涨价'
                    when after_profit -begin_profit<0 then '降价'
                    else '保持'
                end as is_adjust,
                adjust_flag,
                last_begin_profit 
            FROM
                (
                SELECT
                    A.*,
                    CASE
                        WHEN A.begin_profit IS NOT NULL
                        AND A.available_stock = 0 THEN 0.06
                        WHEN A.begin_profit IS NULL
                        AND section is null then 0.06
                        WHEN A.begin_profit IS NULL
                        AND section >= 0 then 0.06
                        # WHEN product_status <> '已停售' and gradient = '在库库存可售天数，N∈(0,15)' then 0.06
                        WHEN if(`SUM(3days_sales)`=0,0.001,`SUM(3days_sales)`)/if(`SUM(7days_sales)`=0,0.01,`SUM(7days_sales)`) >= 0.6 THEN begin_profit
                        WHEN A.begin_profit IS NOT NULL
                        AND over_x_stock = 0
                        and gradient is null
                        and product_status <> '已停售' then first_come_profit
                        WHEN A.begin_profit IS NULL
                        AND section <0
                        and first_come_profit is not null then first_come_profit
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section > 0.06 then 0.06
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section >= 0 then begin_profit + section
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0
                        and lowest_profit is not null then GREATEST(begin_profit + section,
                        lowest_profit)
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0
                        and lowest_profit is null then begin_profit
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null then begin_profit
                        else null
                    end as 'after_profit',
                    CASE
                        WHEN section>0
                        AND A.begin_profit<0 THEN GREATEST((0-A.begin_profit)/ 5,
                        section)
                        ELSE NULL
                    END AS 单次涨幅,
                    CASE
                        WHEN A.begin_profit IS NOT NULL
                        AND A.available_stock = 0 THEN '回调'
                        WHEN A.begin_profit IS NULL
                        AND section is null then '正常'
                        WHEN A.begin_profit IS NULL
                        AND section >= 0 then '正常'
                        # WHEN product_status <> '已停售' and gradient = '在库库存可售天数，N∈(0,15)' then '回调'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND begin_profit<0 then '负利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND over_x_stock = 0
                        and gradient is null
                        and product_status <> '已停售' then '回调'
                        WHEN A.begin_profit IS NULL
                        AND section <0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section > 0.06 then '回调'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section >= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND GREATEST(begin_profit + section,
                        lowest_profit)>= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND begin_profit + section<0 then '负利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND begin_profit >= 0 then '正利润加快动销'
                        WHEN A.begin_profit IS NOT NULL
                        AND section is null
                        AND GREATEST(begin_profit + section,
                        lowest_profit)>= 0 then '正利润加快动销'
                        else null
                    end as 'adjust_recent_clean',
                    get_price_adjust_flag(B.predict_acost_after_this_week, A.day_sales, `净利增益`, 
                        A.last_is_adjust, B.predict_acost_after_last_week, A.begin_profit) as adjust_flag
                FROM
                    (
                    select
                        A.*,
                        B.section,
                        B.lowest_profit,
                        case
                            when warehouse = '美国仓' then 0.05
                            when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
                            when warehouse in ('英国仓', '德国仓', '意大利仓', '法国仓', '西班牙仓') then 0.04
                            else 0.06
                        end as 'first_come_profit'
                    from
                        (
                        select
                            a.*,
                            case
                                when charge_days = 60 then inv_age_60_plus_days
                                when charge_days = 90 then inv_age_90_plus_days
                                when charge_days = 120 then inv_age_120_plus_days
                                else null
                            end as over_x_stock,
                            c.after_profit as 'begin_profit',
                            c.is_adjust as last_is_adjust,
                            c.last_begin_profit
                        from
                            over_sea_age_new_date a
                        LEFT JOIN (
                            select
                                sku,
                                warehouse,
                                after_profit,
                                is_adjust,
                                begin_profit as last_begin_profit
                            from
                                oversea_age_{get_newest_time_str}_dtl
                            where
                                adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
                            a.sku = c.sku
                            AND a.warehouse = c.warehouse
                        WHERE
                            DATE = '{now_time}'
                            and available_stock <> 0
                    union all
                        select
                            a.*,
                            0 as over_x_stock,
                            c.after_profit as 'begin_profit',
                            c.is_adjust as last_is_adjust,
                            last_begin_profit
                        from
                            over_sea_age_new_date a
                        INNER JOIN oversea_sale_status b ON
                            a.sku = b.sku
                            AND a.warehouse = b.warehouse
                            -- AND b.end_time IS NULL
                        LEFT JOIN (
                            select
                                sku,
                                warehouse,
                                after_profit,
                                is_adjust,
                                begin_profit as last_begin_profit
                            from
                                oversea_age_{get_newest_time_str}_dtl
                            where
                                adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')) c on
                            a.sku = c.sku
                            AND a.warehouse = c.warehouse
                        WHERE
                            a.DATE = '{now_time}'
                            and a.available_stock = 0)A
                    LEFT JOIN yibai_oversea_adjust_section B ON
                        A.gradient = B.gradient
                        AND A.over_section_detail = B.over_section_detail
                        AND A.acost = B.acost        
               )A
               left join  
                    oversea_predict_acost B 
                    on (A.sku = B.sku and A.warehouse = B.warehouse and B.date = '{now_time}')
               )A  
        """
        conn.execute(sql)
        # sql = f"""insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
        #             select SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, linest,
        #             available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
        #             best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
        #             inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
        #             inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
        #             `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
        #             estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
        #             over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
        #             over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
        #             case when after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
        #                 ifnull(begin_profit,0)
        #             else
        #                 after_profit
        #             end as after_profit,
        #             case when after_profit>=0 then
        #                  if(adjust_recent_clean='负利润加快动销', '正利润加快动销', adjust_recent_clean)
        #                  when after_profit < 0 then
        #                  if(adjust_recent_clean='正利润加快动销', '负利润加快动销', adjust_recent_clean)
        #                  else adjust_recent_clean
        #                  end,
        #             case when after_profit - ifnull(begin_profit, 0) > 0 and available_stock > 0 and day_sales < 0.5 then
        #                 '保持'
        #             else
        #                 is_adjust
        #             end as is_adjust
        #
        #             from
        #             (
        #                 SELECT SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, concat(ifnull(linest,''), adjust_flag) as linest,
        #                 available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
        #                 best_warehouse, warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
        #                 inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
        #                 inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
        #                 `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
        #                 estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
        #                 over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
        #                 over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
        #                 case when begin_profit is null or available_stock = 0
        #                             then after_profit
        #                      when adjust_flag = 1 -- 降价
        #                             then if(is_adjust in ('保持','涨价'), ifnull(begin_profit, 0)-0.02, after_profit)
        #                      when adjust_flag in (0, 2) -- 保持
        #                             then ifnull(begin_profit, 0)
        #                      when adjust_flag = 3 -- 涨价
        #                             then if(begin_profit <= -0.2, begin_profit + 0.05, begin_profit + 0.03)
        #                      when adjust_flag = 4 -- 降价至前一次利润率
        #                             then ifnull(last_begin_profit, 0)
        #                      when adjust_flag = 5 -- 涨价至前一次利润率, 前一次的取不到则保持当前的
        #                             then  ifnull(
        #                                             (select
        #                                                 b.begin_profit
        #                                             from
        #                                                 oversea_age_{get_newest_adjust_time()}_dtl b
        #                                             where
        #                                                 b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
        #                                             and a.sku = b.sku
        #                                             and a.warehouse = b.warehouse
        #                                             and b.is_adjust = '降价'), ifnull(a.begin_profit,0))
        #                     else ifnull(a.begin_profit, 0)
        #                 end as after_profit,
        #                 adjust_recent_clean,
        #                 case when begin_profit is null or available_stock = 0
        #                            then is_adjust
        #                      when adjust_flag in (1, 4)
        #                             then '降价'
        #                      when adjust_flag in (0, 2)
        #                             then '保持'
        #                      when adjust_flag = 3 or (adjust_flag = 5 and not isnull((select
        #                                                                                 b.begin_profit
        #                                                                             from
        #                                                                                 oversea_age_{get_newest_adjust_time()}_dtl b
        #                                                                             where
        #                                                                                 b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
        #                                                                             and a.sku = b.sku
        #                                                                             and a.warehouse = b.warehouse
        #                                                                             and b.is_adjust = '降价')))
        #                             then '涨价'
        #                      else
        #                      '保持'
        #                 end is_adjust
        #                 FROM over_sea.oversea_age_date_dtl_temp a
        #             ) t
        #       """

        # 超300天库龄的sku
        sql = f"""SELECT distinct sku as sku, case WHEN country='US' THEN '美国仓'

                                   WHEN country IN ('UK','GB') THEN '英国仓'

                                   WHEN country IN ('CZ','CS','DE') THEN '德国仓'

                                   WHEN country='FR' THEN '法国仓'

                                   WHEN country='IT' THEN '意大利仓'

                                   WHEN country='AU' THEN '澳洲仓'

                                   WHEN country IN ('ES','SP') THEN '西班牙仓'

                                   WHEN country='CA' THEN '加拿大仓'

                                   WHEN country='JP' THEN '日本仓'

                                   WHEN country='PL' THEN '德国仓'

                                   ELSE '美国仓' END AS warehouse

                                    FROM `yb_oversea_sku_age`
                                    where oversea_type='WYT' and `status` in (0,1)
                                    and date='{datetime.date.today() - datetime.timedelta(days=2)}' 
                                    and inventory_age>=300 and warehouse_stock>0"""
        # df_age = sql_to_pd3(sql=sql, database='yb_datacenter')
        df_age = conn_datacenter.read_sql(sql)
        # pd_to_sql(df_age, if_exists='replace', table='oversea_age_300_plus_days')
        conn.to_sql(df_age, 'oversea_age_300_plus_days')

        # 超240天库龄的sku
        sql = f"""SELECT distinct sku as sku, case WHEN country='US' THEN '美国仓'

                                   WHEN country IN ('UK','GB') THEN '英国仓'

                                   WHEN country IN ('CZ','CS','DE') THEN '德国仓'

                                   WHEN country='FR' THEN '法国仓'

                                   WHEN country='IT' THEN '意大利仓'

                                   WHEN country='AU' THEN '澳洲仓'

                                   WHEN country IN ('ES','SP') THEN '西班牙仓'

                                   WHEN country='CA' THEN '加拿大仓'

                                   WHEN country='JP' THEN '日本仓'

                                   WHEN country='PL' THEN '德国仓'

                                   ELSE '美国仓' END AS warehouse

                                    FROM `yb_oversea_sku_age`
                                    where oversea_type='WYT' and `status` in (0,1)
                                    and date='{datetime.date.today() - datetime.timedelta(days=2)}' 
                                    and inventory_age>=240 and warehouse_stock>0"""
        # df_age_240 = sql_to_pd3(sql=sql, database='yb_datacenter')
        df_age_240 = conn_datacenter.read_sql(sql)
        # pd_to_sql(df_age_240, if_exists='replace', table='oversea_age_240_plus_days')
        conn.to_sql(df_age_240, 'oversea_age_240_plus_days')

        sql1 = 'truncate sku_this_week_acost'
        conn.execute(sql1)
        sql1 = f"""insert into sku_this_week_acost
        select sku,warehouse,this_week_acost from oversea_predict_acost
        where date='{datetime.date.today()}'"""
        conn.execute(sql1)

        #谷仓兜底值参数
        # 20220725谷仓要求兜底值改为-0.25（20220809 改为-0.3，20221021 改为-0.4,20221026改为-0.5,20221115改为-0.4 20221207改为-0.45,20221209改-0.55
        # 20221212改为-0.6,1226改为-0.8）
        a=-0.6
        sql= f"""insert into oversea_age_{time.strftime('%Y%m%d')}_dtl
            select distinct t.SKU, title, new_price, gross, warehouse_id, `type`, product_status, last_linest, linest,
            t.available_stock, available_stock_money, on_way_stock, create_time, product_size, product_package_size,
            best_warehouse, t.warehouse, inv_age_0_to_30_days, inv_age_30_to_60_days, inv_age_60_to_90_days,
            inv_age_90_to_120_days, inv_age_120_to_150_days, inv_age_150_to_180_days, inv_age_180_to_210_days,
            inv_age_210_plus_days, inv_age_0_to_40_days, inv_age_40_to_70_days, inv_age_70_plus_days, `SUM(7days_sales)`,
            `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, day_sales, reduce_day_sales, estimated_sales_days,
            estimated_reduce_sales_days, a_value_section, day_sales_value_section, `DATE`, over_age150_stock,
            over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
            over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
            
            case 
            when adjust_recent_clean in ('正利润加快动销','负利润加快动销') and after_profit>0.04 then 0.04
            --             when best_warehouse like "%%万邑通%%" and (estimated_sales_days>90 or d.old_estimated_sales_days>90) 
            --             and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             and after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
            --             then ifnull(begin_profit,0)
            -- 
            --             when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null
            --             and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             and s.this_week_acost<0.05 and begin_profit<-0.2
            --             then round(least(ifnull(begin_profit,0)+0.1,-0.2),2)
            --             when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and 
            --             s.this_week_acost<0.1 and begin_profit<-0.2
            --             and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             then round(least(ifnull(begin_profit,0)+0.05,-0.2),2)


            --                 202301海外仓开始循环降价，2022最后一轮降价动销差的sku直接降至之前最低兜底-0.8
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < 0 and begin_profit = -0.8
            then -0.8        

            when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<{a} and 
            after_profit - ifnull(begin_profit, 0) >0
            then round(after_profit,4)
            
            --                 存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(1,∞)部分-0.4兜底；(0.3,1]部分-0.5兜底
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and begin_profit = -0.4 and gradient is null
            then -0.4
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and begin_profit = -0.5 and gradient is null
            then -0.5
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and begin_profit > -0.4 and gradient is null and
            day_sales > 1
            then -0.4
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and begin_profit > -0.5 and gradient is null and
            day_sales <= 1
            then -0.5
                
                
            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.4 and gradient in 
            ('存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(5,∞);N∈(40,∞);M∈(100,∞)',
            '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(3,5];N∈(40,∞);M∈(100,∞)',
            '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(1,3];N∈(40,∞);M∈(90,∞)')
            then -0.4

            when best_warehouse like "%%谷仓%%" and available_stock>0 and after_profit < -0.5 and gradient in 
            ('存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)',
            '存在对应收仓租天数库龄存在X+60的库存，且超出数量可售天数>30天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)')
            then -0.5

  
            --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.6
            --                 then -0.6
            --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.4 
            --                 then least(begin_profit+0.15,{a})
            --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<-0.3 
            --                 then least(begin_profit+0.1,{a})  
            --                 when best_warehouse like "%%谷仓%%" and available_stock>0 and begin_profit<{a}
            --                 then least(begin_profit+0.05,{a})                           
            --                 when best_warehouse like "%%谷仓%%" and t.available_stock>0 and begin_profit>={a} and after_profit<={a}
            --                 then {a}
            
            when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 and t.available_stock > 0 and day_sales < 0.5 then
            ifnull(begin_profit,0)

            else round(after_profit,4)
            end as after_profit,
             case 
            when after_profit>=0 then
            if(adjust_recent_clean='负利润加快动销', '正利润加快动销', adjust_recent_clean)
            when after_profit < 0 then
            if(adjust_recent_clean='正利润加快动销', '负利润加快动销', adjust_recent_clean)
            when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 
            and available_stock > 0 and day_sales < 0.5
            then  if(ifnull(begin_profit,0)>=0,'正利润加快动销','负利润加快动销')
            when adjust_recent_clean='正常' and ss.sale_status in ('正利润加快动销','负利润加快动销')
            then '回调'
            
            else adjust_recent_clean
            end,
            case when adjust_recent_clean in ('正利润加快动销','负利润加快动销') and after_profit>0.04 then  if(0.04-ifnull(begin_profit,0)>0,'涨价','降价')
            --             when best_warehouse like "%%万邑通%%" and (estimated_sales_days>90 or d.old_estimated_sales_days>90) and 
            --             after_profit-ifnull(begin_profit,0)>0 and after_profit<=-0.2
            --             and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             then '保持'
            -- 
            --             when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and s.this_week_acost<0.05  
            --             and begin_profit<-0.2 and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             then "涨价"
            --             when best_warehouse like "%%万邑通%%" and day_sales>1 and estimated_sales_days<=90 and s.this_week_acost is not null and s.this_week_acost<0.1 
            --             and begin_profit<-0.2 and concat(t.sku,t.warehouse) not in 
            --             (select concat(sku,warehouse) from oversea_age_300_plus_days)
            --             then "涨价"
            when best_warehouse like "%%谷仓%%" and  t.available_stock>0 and  begin_profit<{a} then '涨价'
            when  best_warehouse like "%%谷仓%%" and t.available_stock>0 and begin_profit={a} and after_profit<={a}
            then '保持'
            when adjust_recent_clean in ("正利润加快动销","负利润加快动销") and after_profit - ifnull(begin_profit, 0) > 0 and t.available_stock > 0 and day_sales < 0.5 then
            '保持' 
            else
            is_adjust
            end as is_adjust
            
            from
            (
            SELECT a.SKU, a.title, a.new_price, a.gross, a.warehouse_id, a.`type`, a.product_status, last_linest, concat(ifnull(linest,''), adjust_flag) as linest,
            a.available_stock, a.available_stock_money, a.on_way_stock, a.create_time, product_size, product_package_size,
            best_warehouse, a.warehouse, a.inv_age_0_to_30_days, a.inv_age_30_to_60_days, a.inv_age_60_to_90_days,
            a.inv_age_90_to_120_days, a.inv_age_120_to_150_days, a.inv_age_150_to_180_days, a.inv_age_180_to_210_days,
            a.inv_age_210_plus_days, a.inv_age_0_to_40_days, a.inv_age_40_to_70_days, a.inv_age_70_plus_days, a.`SUM(7days_sales)`,
            `SUM(15days_sales)`, `SUM(30days_sales)`, `SUM(90days_sales)`, a.day_sales, reduce_day_sales, estimated_sales_days,
            estimated_reduce_sales_days, a_value_section, day_sales_value_section, a.`DATE`, over_age150_stock,
            over_age120_stock, over_age90_stock, over_age60_stock, over_age30_stock, estimated_all_sales_days, over_sale_stock,
            over_sale_section, gradient, is_chargo, acost, begin_profit, `section`, lowest_profit, first_come_profit,
            case when begin_profit is null or a.available_stock = 0
            then after_profit
            -- 2022/03/8 满足仅万邑通有库存新加条件,begin_profit<0
            --             when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 
            --             and a.available_stock>5
            --             
            --             and a.estimated_sales_days<150 and a.adjust_flag in (1,4)  and begin_profit<0 then  ifnull(begin_profit, 0)
            --             
            --             
            --             when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and 
            --             a.available_stock>5 and a.estimated_sales_days<60 and a.day_sales>0.3  and begin_profit<0
            
            --             then  round(ifnull(begin_profit, 0)+0.05,2)
                    -- 若存在库龄超240天，且超出240天库龄部分库存超出30天可售降0.15，若存在库龄超210天，且超出210天库龄部分库存超出60天可售,则降价0.1
        
                    when best_warehouse like '%%万邑通%%' and a.available_stock>0 and concat(a.SKU,a.warehouse) in
                    (select concat(sku,warehouse) from oversea_age_240_plus_days) and a.estimated_sales_days>30 
                    then ifnull(begin_profit,0)-0.15
                    when
                    best_warehouse like '%%万邑通%%'  and a.available_stock>0 and inv_age_210_plus_days>0 
                    and a.estimated_sales_days>60
                    then ifnull(begin_profit,0)-0.1
                    -- 20220428 若存在库龄超180天，超出90天可售，则降价0.05
                    when best_warehouse like '%%万邑通%%' and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`>0 
                    and a.available_stock>0 and a.estimated_sales_days>90 then ifnull(begin_profit,0)-0.05

            
            
            when best_warehouse like "%%谷仓%%" and a.available_stock>0 and begin_profit<-0.6
            and adjust_recent_clean='负利润加快动销'
            then -0.6
            
            when best_warehouse like "%%谷仓%%"  and a.available_stock>0 and begin_profit<-0.4 
            and adjust_recent_clean='负利润加快动销' 
            
            then  round(least(ifnull(begin_profit, 0)+0.15,{a}),2)
            
            when best_warehouse like "%%谷仓%%"  and a.available_stock>0 and begin_profit<-0.3
            and adjust_recent_clean='负利润加快动销'  
            
            then round(least(ifnull(begin_profit, 0)+0.1,{a}),2)
            
            when best_warehouse like "%%谷仓%%"  and a.available_stock>0  and begin_profit<{a}
            and adjust_recent_clean='负利润加快动销'
            
            then  round(least(ifnull(begin_profit, 0)+0.05 ,{a}),2)
            
            when adjust_flag = 1 -- 降价
            then if(is_adjust in ('保持','涨价'), ifnull(begin_profit, 0)-0.02, after_profit)
            when adjust_flag in (0, 2) -- 保持
            then ifnull(begin_profit, 0)
            when adjust_flag = 3 -- 涨价
            then if(begin_profit <= -0.2, begin_profit + 0.05, begin_profit + 0.03)
            when adjust_flag = 4 -- 降价至前一次利润率
            then ifnull(last_begin_profit, 0)
            when adjust_flag = 5 -- 涨价至前一次利润率, 前一次的取不到则保持当前的
            then  ifnull(
            (select
            b.begin_profit
            from
            oversea_age_{get_newest_time_str}_dtl b
            where
            b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
            and a.sku = b.sku
            and a.warehouse = b.warehouse
            and b.is_adjust = '降价'), ifnull(a.begin_profit,0))
            else ifnull(a.begin_profit, 0)
            end as after_profit,
            adjust_recent_clean,
            case when begin_profit is null or a.available_stock = 0
            then is_adjust
            --             when cc.wyt_kucun="是" and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 and a.available_stock>5 
            --             
            --             and a.estimated_sales_days<150 and a.adjust_flag in (1,4)  and begin_profit<0 then '保持'
            --             
            --             
            --             
            --             when cc.wyt_kucun="是" and a.available_stock>5 and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`=0 
            --             
            --             and a.estimated_sales_days<60 and a.day_sales>0.3  and begin_profit<0
            --             
            --             then  '涨价'
            --             
                    -- 20220428 若存在库龄超180天，超出90天可售，则降价0.05
                    when best_warehouse like '%%万邑通%%' and a.`inv_age_180_to_210_days`+a.`inv_age_210_plus_days`>0 
                    and a.available_stock>0 and a.estimated_sales_days>90 then '降价'
                    -- 若存在库龄超240天，且超出240天库龄部分库存超出30天可售 OR 若存在库龄超210天，且超出210天库龄部分库存超出60天可售,则降价0.1
                    
                    when best_warehouse like '%%万邑通%%' and a.available_stock>0 and concat(a.SKU,a.warehouse) in
                    (select concat(sku,warehouse) from oversea_age_240_plus_days) and a.estimated_sales_days>30 
                    then '降价'
                    when
                    best_warehouse like '%%万邑通%%'  and a.available_stock>0 and inv_age_210_plus_days>0 
                    and a.estimated_sales_days>60
                    then '降价'
            
            
            
            when best_warehouse like "%%谷仓%%"   and a.available_stock>0 and begin_profit<{a} then '涨价'
            when adjust_flag in (1, 4)
            then '降价'
            when adjust_flag in (0, 2)
            then '保持'
            when adjust_flag = 3 or (adjust_flag = 5 and not isnull((select
                                                                                                                                            b.begin_profit
            from
            oversea_age_{get_newest_time_str}_dtl b
            where
            b.adjust_recent_clean in ('正利润加快动销', '负利润加快动销', '清仓')
            and a.sku = b.sku
            and a.warehouse = b.warehouse
            and b.is_adjust = '降价')))
            then '涨价'
            else
            '保持'
            end is_adjust
            FROM over_sea.oversea_age_date_dtl_temp a
            
            -- left join (select a.*,case when '不涨价' is null then '可降价' else '不涨价' end is_increase
            -- from (select wyt.sku,wyt.warehouse,
            -- case when gc.warehosue_stock is null 
            -- then '是' 
            -- else '否'
            -- end as wyt_kucun
            -- from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            -- from warehouse_stock_charges
            -- where date="{now_time}" and oversea_type="WYT" and warehouse_stock>0
            -- group by sku,warehouse) wyt
            -- left join
            -- (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
            -- from warehouse_stock_charges
            -- where date="{now_time}" and oversea_type="GC" and warehouse_stock>0
            -- group by sku,warehouse)gc on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
            -- HAVING   wyt_kucun='是') a
            -- left join 
            -- (select distinct sku,warehouse,'不涨价' from wyt_no_price_increase_details) c
            -- 
            -- on a.sku=c.sku and a.warehouse=c.warehouse
            -- having is_increase='可降价')cc on cc.sku=a.SKU and cc.warehouse=a.warehouse
            ) t
            left join sku_this_week_acost s on t.sku=s.sku and t.warehouse=s.warehouse
            left join (select sku,warehouse,estimated_sales_days as old_estimated_sales_days 
            from oversea_age_{get_newest_time_str}_dtl) 
            d on t.sku=d.sku and t.warehouse=d.warehouse
            left join oversea_sale_status ss on t.sku=ss.sku and t.warehouse=ss.warehouse and ss.end_time is null
           
				"""
        conn.execute(sql)
        # sql1=f"""insert into wyt_no_price_increase_details
        #     select a.sku,b.warehouse,a.begin_profit,a.after_profit,b.wyt_kucun,a.is_adjust,a.DATE
        #     from oversea_age_{time.strftime('%Y%m%d')}_dtl a
        #     inner join (select wyt.sku,wyt.warehouse,
        #     case when gc.warehosue_stock is null
        #     then '是'
        #     else '否'
        #     end as wyt_kucun
        #     from (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
        #     from warehouse_stock_charges
        #     where date="{datetime.date.today()}" and oversea_type="WYT" and warehouse_stock>0
        #     group by sku,warehouse) wyt
        #     left join
        #     (select sku,warehouse,sum(warehouse_stock) as warehosue_stock
        #     from warehouse_stock_charges
        #     where date="{datetime.date.today()}" and oversea_type="GC" and warehouse_stock>0
        #     group by sku,warehouse)gc on gc.sku=wyt.sku and gc.warehouse=wyt.warehouse
        #     having wyt_kucun='是') b on a.sku=b.sku and a.warehouse=b.warehouse
        #     where a.after_profit-a.begin_profit<0"""
        # conn.execute(sql1)
    except:
        raise Exception(traceback.format_exc())

if __name__ == '__main__':
    base_table_insert_data()
    # get_price_datetime()
