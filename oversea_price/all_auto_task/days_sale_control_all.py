import datetime
import pandas as pd
import time
from sqlalchemy import create_engine
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql

def get_date(n):
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=n)).strftime("%Y-%m-%d")
    return out_date


def get_new_data(conn):
    sql = f"""
        select distinct platform_code,warehouse_id,sku,7days_sales 
        from yibai_sku_sales_statistics_all 
        where statistics_date='{datetime.date.today().isoformat()}'
    """
    print(sql)
    new_data = conn.read_sql(sql)
    print(new_data)
    return new_data['7days_sales'].sum()


def get_old_data(conn):
    sql = f"""
        select distinct platform_code, warehouse_id, sku, 7days_sales 
        from yibai_sku_sales_statistics_all 
        where statistics_date='{get_date(1)}'
    """
    print(sql)
    old_data = conn.read_sql(sql)
    print(old_data)
    return old_data['7days_sales'].sum()


def check_everyday_sales():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    #
    new_data = get_new_data(conn)
    old_data = get_old_data(conn)
    conn.close()
    if new_data == 0:
        send_msg('动销组定时任务推送', '销量校验', '销量数据存在严重异常，请停止调价!!!',
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
        time.sleep(15)
        send_msg('动销组定时任务推送', '销量校验', '销量数据存在严重异常，请停止调价!!!',
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
        time.sleep(15)
        send_msg('动销组定时任务推送', '销量校验', '销量数据存在严重异常，请停止调价!!!',
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
        raise Exception('销量数据存在严重异常，请停止调价!!!')
    else:
        a = abs(new_data - old_data)
        print(a)
        str_data = f"""类型:sku销量检查\n状态：7天销量异常\n{get_date(1)}:sku7天总销量:{int(old_data)}\n{datetime.date.today().isoformat()}:sku7天总销量:{int(new_data)}\n差异:{int(a)}\n通知时间:{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}"""
        print(str_data)
        if a > 40000:
            send_msg('动销组定时任务推送', '销量校验', str(str_data),
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                     status='失败')
            raise Exception('7天销量异常，请停止调价!!!')


if __name__ == '__main__':
    check_everyday_sales()
