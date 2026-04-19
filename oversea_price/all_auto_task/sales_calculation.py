import datetime
import os
import time
import traceback
from pulic_func.base_api.mysql_connect import connect_to_sql
import numpy as np
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
import json
import requests
from all_auto_task.dingding import send_msg
from all_auto_task.over_sea_age import oversea_age_new
from all_auto_task.days_sale_control_all import check_everyday_sales


def get_yesterday_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    return out_date


def get_token():
    url = 'http://oauth.java.yibainetwork.com/oauth/token?grant_type=client_credentials'
    resp = requests.post(url, auth=HTTPBasicAuth('prod_data_mgn', 'mgnkk7cytdsD'))
    token = json.loads(resp.text)['access_token']
    print(token)
    return token


def get_all_need_number(conn):
    sql = """
    select statistics_date,count(1) as num,sum(`7days_sales`) as `7days_sales`,
        sum(`30days_sales`) as `30days_sales`
    from yibai_sku_sales_statistics_all
    group by statistics_date
    order by statistics_date desc 
    limit 2
    """
    df = conn.read_sql(sql)
    # df = df.fillna(0)
    print(df)
    # df = df.astype(int)
    # all_number = df.values.tolist()

    today_count = df['num'][0]
    yesterday_count = df['num'][1]
    d_value_count = abs(yesterday_count - today_count) / yesterday_count

    today_7days = df['7days_sales'][0]
    yesterday_7days = df['7days_sales'][1]
    d_value_7days = abs(today_7days - yesterday_7days) / yesterday_7days

    today_30days = df['30days_sales'][0]
    yesterday_30days = df['30days_sales'][1]
    d_value_30days = abs(today_30days - yesterday_30days) / yesterday_30days

    yesterday = df['statistics_date'][1]
    return d_value_count, yesterday_count, today_count, d_value_7days, yesterday_7days, today_7days, d_value_30days, yesterday_30days, today_30days, yesterday


def delete_data(conn):
    sql = "truncate yibai_sku_sales_statistics"
    conn.execute(sql)


def delete_data2(conn):
    sql = f"""delete from yibai_sku_sales_statistics_all where statistics_date='{datetime.date.today().isoformat()}'"""
    print(sql)
    conn.execute(sql)


def get_data(date_total, conn):
    url = f'http://rest.java.yibainetwork.com/data/yibaiSkuSalesStatistics/getSkuSalesStatistics?access_token={get_token()}'
    print(url)
    pageSize = 100000
    n = int(np.ceil(date_total / pageSize))
    #
    df_all = pd.DataFrame()
    for i in range(1, n + 1):
        print(i)
        data = {
            "pageNumber": i,
            "pageSize": pageSize
        }
        while 1:
            try:
                res = requests.post(url=url, json=data)
                if res.status_code != 200:
                    with open(os.path.join(os.getcwd(), 'err_status.txt'), 'a', encoding='utf-8') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{res.content.decode('utf-8')}")
                res_data = res.json()
                # print(res_data)
                data_list = res_data["data"]["records"]
                df = pd.DataFrame(data_list)
                df_all = df_all.append(df)
                break
            except:
                print(traceback.format_exc())
                time.sleep(60)
                if datetime.datetime.now().hour >= 12:
                    raise Exception('销量表获取异常，到12点未完成')

    df_all.drop_duplicates(inplace=True)
    df_all = df_all.rename(
        columns={"platformCode": "platform_code", "warehouseId": "warehouse_id", "sku3daysSales": "3days_sales",
                 "sku7daysSales": "7days_sales", "sku15daysSales": "15days_sales", "sku30daysSales": "30days_sales",
                 "sku60daysSales": "60days_sales", "sku90daysSales": "90days_sales",
                 "statisticsDate": "statistics_date"})
    df_all.drop(['id'], axis=1, inplace=True)
    # pd_to_sql(df_all, 'yibai_sku_sales_statistics')
    # pd_to_sql(df_all, 'yibai_sku_sales_statistics_all')
    conn.to_sql(df_all, 'yibai_sku_sales_statistics', 'append')
    conn.to_sql(df_all, 'yibai_sku_sales_statistics_all', 'append')

    df_count = len(df_all)
    print(df_count)


def sales_calculation(date_total):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    try:
        delete_data(conn)
        delete_data2(conn)
        get_data(date_total, conn)
        d_value_count, yesterday_count, today_count, d_value_7days, yesterday_7days, today_7days, d_value_30days, yesterday_30days, today_30days, yesterday = get_all_need_number(
            conn)
        conn.close()
    except:
        send_msg('动销组定时任务推送', 'sku销量拉取',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 销量计算出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                 is_all=False, status='失败')
        raise Exception(traceback.format_exc())
    else:
        d_value_count = round(d_value_count, 3)
        d_value_7days = round(d_value_7days, 3)
        d_value_30days = round(d_value_30days, 3)
        if d_value_count > 0.03 or d_value_7days > 0.03 or d_value_30days > 0.04:
            send_message = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 销量异常,请检查!\n\r销量计算总行数为{today_count},和前一天{yesterday}的销量计算总行数为{yesterday_count}相比,销量计算总行数两天的差异为{'%.2f%%' % (d_value_count * 100)};\n\r7天销量总数为{today_7days},和前一天{yesterday}的7天销量总数为{yesterday_7days}相比,7天销量总数两天的差异为{'%.2f%%' % (d_value_7days * 100)};\n\r30天销量总数为{today_30days},和前一天{yesterday}的30天销量总数为{yesterday_30days}相比,30天销量总数两天的差异为{'%.2f%%' % (d_value_30days * 100)}"
            send_msg('动销组定时任务推送', 'sku销量拉取', send_message,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950',
                              '+86-18202993981', '+86-13922822326'], is_all=False)
            raise Exception(send_message)
        else:
            send_msg('动销组定时任务推送', 'sku销量拉取',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 销量计算处理完成\n\r销量计算总行数为{today_count}\n\r7天销量总数为{today_7days}\n\r30天销量总数为{today_30days}；\n\r昨天{get_yesterday_date()}的销量:\n\r销量计算总行数为{today_count}\n\r7天销量总数为{yesterday_7days}\n\r30天销量总数为{yesterday_30days}\n\r三种数据差异量比较的绝对值均小于3%,正常",
                     is_all=False)


def sales_calculation_one():
    url = f'http://rest.java.yibainetwork.com/data/yibaiSkuSalesStatistics/getSkuSalesStatistics?access_token={get_token()}'
    print(url)
    data = {
        "pageNumber": 1,
        "pageSize": 1
    }
    res = requests.post(url=url, json=data)
    res_data = json.loads(res.content.decode())
    print(res_data)
    date_now = res_data["data"]["records"][0]['statisticsDate']
    date_total = int(res_data["data"]["total"])
    print(date_now)
    print(date_total)
    if str(date_now) == f'{datetime.date.today().isoformat()}':
        sales_calculation(date_total)
    else:
        send_msg('动销组定时任务推送', 'sku销量拉取',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}销量数据没有更新",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
        raise Exception('销量数据没有更新')


if __name__ == '__main__':
    sales_calculation_one()
