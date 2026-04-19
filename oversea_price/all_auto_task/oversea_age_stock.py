import traceback
import pandas as pd
import time
import warnings
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql
warnings.filterwarnings("ignore")


def delete_data(conn):
    sql = "DELETE FROM  oversea_age_stock WHERE date >= date_format(date_sub(current_date(), interval 6 day), '%%Y-%%m-%%d')"
    conn.execute(sql)


def str_to_day(time_str):
    time_str = time_str[0:10]
    return time_str


def get_data(conn):
    sql = """
                with 
            [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
            ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
         '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
         '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
         '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr
        select 
            a.sku as sku, date, charge_currency, round(charge_total_price, 2) as charge_total_price,
            splitByString('>>', pl.path_name)[1] AS product_line, p.new_price as new_price,
            transform(product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
            available_stock,
            available_stock*p.new_price as available_stock_money,
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
            warehouse    
        from
            (
             select
                date,
                sku as sku,
                charge_currency,
                cargo_type,
                warehouse_code,
               case when inventory_age between 0 and 30 then 'inv_age_0_to_30_days'
                    when inventory_age between 30 and 60 then 'inv_age_30_to_60_days'
                    when inventory_age between 60 and 90 then 'inv_age_60_to_90_days'
                    when inventory_age between 90 and 120 then 'inv_age_90_to_120_days'
                    when inventory_age between 120 and 150 then 'inv_age_120_to_150_days'
                    when inventory_age between 150 and 180 then 'inv_age_150_to_180_days'
                    when inventory_age between 180 and 210 then 'inv_age_180_to_210_days'
                    when inventory_age between 210 and 9999999 then 'inv_age_210_plus_days'
                    else Null
                end as stock_age_section,
                case
                    when inventory_age between 0 and 40 then 'inv_age_0_to_40_days'
                    when inventory_age between 40 and 70 then 'inv_age_40_to_70_days'
                    when inventory_age between 70 and 100000000 then 'inv_age_70_plus_days'
                    else Null
                end as stock_age_section_new,
                case
                    when country = 'US' then '美国仓'
                    when country in ('UK', 'GB') then '英国仓'
                    when country in ('CZ', 'CS', 'DE') then '德国仓'
                    when country = 'FR' then '法国仓'
                    when country = 'IT' then '意大利仓'
                    when country = 'AU' then '澳洲仓'
                    when country in ('ES', 'SP') then '西班牙仓'
                    when country = 'CA' then '加拿大仓'
                    when country = 'JP' then '日本仓'
                    when country = 'PL' then '德国仓'
                    else '美国仓'
                end as warehouse,
                warehouse_stock,
                inventory_age,
                sum(charge_total_price) over w as charge_total_price,
                sum(warehouse_stock) over w as available_stock,
                sum(case when stock_age_section = 'inv_age_0_to_30_days' then warehouse_stock else 0 end) over w as inv_age_0_to_30_days, 
                sum(case when stock_age_section  = 'inv_age_30_to_60_days' then warehouse_stock else 0 end) over w as inv_age_30_to_60_days, 
                sum(case when stock_age_section  = 'inv_age_60_to_90_days' then warehouse_stock else 0 end) over w as inv_age_60_to_90_days, 
                sum(case when stock_age_section  = 'inv_age_90_to_120_days' then warehouse_stock else 0 end) over w as inv_age_90_to_120_days, 
                sum(case when stock_age_section  = 'inv_age_120_to_150_days' then warehouse_stock else 0 end) over w as inv_age_120_to_150_days,
                sum(case when stock_age_section  = 'inv_age_150_to_180_days' then warehouse_stock else 0 end) over w as inv_age_150_to_180_days, 
                sum(case when stock_age_section  = 'inv_age_180_to_210_days' then warehouse_stock else 0 end) over w as inv_age_180_to_210_days, 
                sum(case when stock_age_section  = 'inv_age_210_plus_days' then warehouse_stock else 0 end) over w as inv_age_210_plus_days, 
                sum(case when stock_age_section_new = 'inv_age_0_to_40_days' then warehouse_stock else 0 end) over w as inv_age_0_to_40_days, 
                sum(case when stock_age_section_new = 'inv_age_40_to_70_days' then warehouse_stock else 0 end) over w as inv_age_40_to_70_days, 
                sum(case when stock_age_section_new = 'inv_age_70_plus_days' then warehouse_stock else 0 end) over w as inv_age_70_plus_days
             from
                yb_oversea_sku_age 
             where
                date between toString(today()-6) and toString(today()) 
               and status in (0, 1) 
               and warehouse_stock > 0
            window w as (partition by date, sku, warehouse)
            ) a
            inner join yb_product p on a.sku = p.sku
            left join yb_product_linelist pl on	pl.id = toUInt64(p.product_linelist_id)
            left join yb_warehouse_erp p2 on a.warehouse_code = p2.warehouse_code 
        where
            p.product_status between 0 and 35
            and p.state_type <> '9'
            order by intDivOrZero(charge_total_price*100,warehouse_stock)/100 desc,inventory_age desc limit 1
            by a.date, a.sku, warehouse
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df = ck_client.ck_select_to_df(sql)

    conn.to_sql(df, table='oversea_age_stock', if_exists="append")
    all_data = len(df)
    return all_data


def run_oversea_stock():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        delete_data(conn)
        all_data = get_data(conn)
        send_msg('动销组定时任务推送', '海外仓库存监控',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓库存监控数据处理完成,总量为:{all_data}",
                 is_all=False)
        conn.close()
    except Exception as e:
        send_msg('动销组定时任务推送', '海外仓库存监控',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓库存监控数据,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False, status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    run_oversea_stock()
