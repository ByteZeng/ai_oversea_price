import time,datetime
import traceback

import pandas as pd
from sqlalchemy import create_engine
import pymysql
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_sql, sql_to_pd,pd_to_ck

def lock_price_table_push():
    try:
        date_today = datetime.datetime.today().strftime('%Y-%m-%d')
        sql = f"""
            select * 
            from yibai_product_kd_sync.yibai_amazon_adjustprice_filter_sku 
            where start_time<='{date_today}' and end_time>='{date_today}' and `status`=1
        """
        conn_mx = pd_to_ck(database='yibai_product_kd_sync', data_sys='调价明细历史数据')
        df1 = conn_mx.ck_select_to_df(sql)
        a = len(df1)
        # b = len(df2)
        df1.drop('sync_time', axis=1, inplace=True)
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        conn.execute("truncate table yibai_amazon_adjustprice_filter_sku")
        conn.to_sql(df1, table="yibai_amazon_adjustprice_filter_sku", if_exists="append")
        n = 90000
        if a > n:
            send_msg('动销组定时任务推送', '锁价表推送',
                     f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓锁价表推送成功,AMAZON锁价表{a},
                    数据量正常""",
                     is_all=False)
        else:

            send_msg('动销组定时任务推送', '锁价表推送', f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}
                 海外仓锁价表拉取量异常,AMAZON锁价表为{a},低于{n}请检查数据！""",
                     is_all=False, mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
                     status='失败')
            raise Exception(f'海外仓锁价表拉取量异常,AMAZON锁价表为{a},低于{n},请检查数据！')
        conn.close()
    except:
        send_msg('动销组定时任务推送', '锁价表推送',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓锁价表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())

    #     if a > 100000 and b > 15000:
    #         send_msg('动销组定时任务推送', '锁价表推送',
    #                  f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓锁价表推送成功,AMAZON锁价表{a},
    #                 WALMART锁价表{b},数据量正常""",
    #                  is_all=False)
    #     else:
    #
    #         send_msg('动销组定时任务推送', '锁价表推送', f"""{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}
    #              海外仓锁价表拉取量异常,AMAZON锁价表为{a},WALMART锁价表为{b},分别低于100000,15000,请检查数据！""",
    #                  is_all=False, mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'],
    #                  status='失败')
    #         raise Exception(f'海外仓锁价表拉取量异常,AMAZON锁价表为{a},WALMART锁价表为{b},分别低于100000,15000,请检查数据！')
    # except:
    #     send_msg('动销组定时任务推送', '锁价表推送',
    #              f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓锁价表处理出现问题,请及时排查,失败原因详情请查看airflow日志",
    #              mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
    #              status='失败')
    #     raise Exception(traceback.format_exc())


if __name__ == "__main__":
    lock_price_table_push()
