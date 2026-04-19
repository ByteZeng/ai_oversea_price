import datetime
import time
import traceback
import shutil
import os
import pandas as pd
from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.yibai_obs import obs_file_upload
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from sqlalchemy import create_engine
from all_auto_task.nacos_api import get_user
from pulic_func.base_api.mysql_connect import sql_to_pd

tempdir = "/data/yunfei/compare"


def qushu_date(days=0):
    day = datetime.date.today() - datetime.timedelta(days=days)
    day = day.isoformat()
    qushu = day.replace("-", "")
    return qushu


def write_to_ck():
    """
    useful运费备份ck
    """
    # 20231228 同步一份数据到ck
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    table_name = 'oversea_transport_fee_useful'
    sql = f'TRUNCATE TABLE IF EXISTS yibai_oversea.{table_name}'
    conn_ck.ck_execute_sql(sql)
    print('当日数据删除完成.')
    # 取数
    sql = """
        SELECT distinct platform
        FROM over_sea.oversea_transport_fee_useful
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform = conn.read_sql(sql)
    n = 0
    while n < 4:
        sql = """
            SELECT count(1)
            FROM yibai_oversea.oversea_transport_fee_useful
        """
        df_num = conn_ck.ck_select_to_df(sql)
        if df_num.iloc[0, 0] == 0:
            print('开始备份数据到ck...')
            for i in df_platform['platform'].unique():
                sql = f"""
                    SELECT *
                    FROM over_sea.oversea_transport_fee_useful
                    WHERE platform = '{i}'
                """
                conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
                df_useful = conn.read_sql(sql)
                print(df_useful.info())
                col = ['zero_percent','five_percent','must_percent','lowest_price']
                df_useful[col] = df_useful[col].fillna(0).astype(float)
                conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
                if len(df_useful) > 1000000:
                    conn_ck.ck_insert(df_useful.iloc[0:800000, :], table_name, if_exist='append')
                    conn_ck.ck_insert(df_useful.iloc[800000:, :], table_name, if_exist='append')
                else:
                    conn_ck.ck_insert(df_useful, table_name, if_exist='append')
                print(f"{i}站点数据同步完成")
                # break
        else:
            time.sleep(10)
            n += 1

def transport_fee_compare_upload():
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)
    if not os.path.exists(tempdir):
        os.makedirs(tempdir)
    # try:
    #     sql = """select
    #                   a.sku as sku,
    #                   a.warehouseId as warehouseId,
    #                   a.country as country,
    #                   a.shipName as "今天物流渠道",
    #                   a.totalcost as "今天运费",
    #                   b.shipName as "昨日物流渠道",
    #                   b.totalcost as "昨日运费"
    #                 from
    #                     (
    #                     select
    #                         sku,
    #                         warehouseId,
    #                         shipName,
    #                         country,
    #                         toDecimal64(totalCost,4) as totalcost,
    #                         row_number() over(partition by sku, warehouseId, country
    #                     order by
    #                         totalcost asc ) as rn
    #                     from
    #                         yibai_oversea.oversea_transport_fee_daily otfd
    #                     where
    #                         platform = 'AMAZON'
    #                       and date_id = toYYYYMMDD(today())
    #                     ) a
    #                     inner join
    #                     (
    #                         select
    #                             sku,
    #                             warehouseId,
    #                             shipName,
    #                             country,
    #                             toDecimal64(totalCost,4) as totalcost,
    #                             row_number() over(partition by sku, warehouseId, country
    #                         order by
    #                             totalcost asc ) as rn
    #                         from
    #                             yibai_oversea.oversea_transport_fee_daily
    #                         where
    #                             platform = 'AMAZON'
    #                           and date_id = toYYYYMMDD(today()-1)
    #                     ) b
    #                     on (a.sku = b.sku and a.warehouseId = b.warehouseId and a.country = b.country)
    #                 where a.rn = 1
    #                   and b.rn = 1
    #                   and abs((a.totalcost - b.totalcost)/b.totalcost) > toDecimal64(0.1, 4)"""
    #     # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
    #     #                      db_name='yb_datacenter')
    #     conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    #     df = conn_ck.ck_select_to_df(sql)
    #
    #     if len(df) < 1:
    #         print("海外仓运费核对:无涨幅超过10%的数据")
    #         df = pd.DataFrame({'海外仓运费核对': ['无涨幅超过10%的数据']})
    #         filename = f"{tempdir}/海外仓运费核对{time.strftime('%Y%m%d', time.localtime())}.xlsx"
    #         with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
    #             df.to_excel(writer, sheet_name="海外仓运费核对", index=False)
    #         obs_file_upload(f"transport_fee_compare/{filename.split('/')[-1]}", filename)
    #         return
    #
    #     df['比率(今天/昨天)'] = df['今天运费'] / df[f'昨日运费']
    #     df['涨幅比较'] = (df['今天运费'] - df['昨日运费']) / df['昨日运费']
    #
    #     print(len(df))
    #     if len(df) > 0:
    #         df['涨幅'] = None
    #         df.loc[(df['涨幅比较']) < -0.5, '涨幅'] = '下降50%以上'
    #         df.loc[(df['涨幅比较'] <= -0.1) & (df['涨幅比较'] >= -0.5), '涨幅'] = '下降10%-50%'
    #         df.loc[(df['涨幅比较'] >= 0.1) & (df['涨幅比较'] <= 0.2), '涨幅'] = '上涨10%-20%'
    #         df.loc[(df['涨幅比较'] > 0.2) & (df['涨幅比较'] <= 0.3), '涨幅'] = '上涨20%-30%'
    #         df.loc[(df['涨幅比较'] > 0.3) & (df['涨幅比较'] <= 0.5), '涨幅'] = '上涨30%-50%'
    #         df.loc[(df['涨幅比较'] > 0.5), '涨幅'] = '上涨50%以上'
    #         df.loc[(df['涨幅比较'] >= 0) & (df['涨幅比较'] < 0.1), '涨幅'] = '上涨10%以内'
    #         df.loc[(df['涨幅比较'] > -0.1) & (df['涨幅比较'] < 0), '涨幅'] = '下降10%以内'
    #         # 删除涨幅比较
    #         # del df['涨幅比较']
    #         # 取毛重和出库平均重量做对比
    #         sku_set = set(df['sku'])
    #         sku_set.add('for_one_sku_bug')
    #         sku_list = tuple(sku_set)
    #         print(len(sku_list) - 1)
    #         # sql3 = f"""select distinct sku,pur_weight_pack as '毛重',weight_out_storage as '出库平均重量'
    #         #     from yibai_prod_base.yibai_prod_sku where sku in {sku_list} order by create_time desc
    #         #     """
    #         # df3 = sql_to_pd(sql=sql3, database='yibai_prod_base', data_sys='新产品系统')
    #         # df = df.merge(df3, on=['sku'], how='left')
    #         # print(len(df))
    #         df4 = df.groupby(['涨幅'])['sku'].count().reset_index()
    #         filename = f"{tempdir}/海外仓运费核对{time.strftime('%Y%m%d', time.localtime())}.xlsx"
    #         with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
    #             df.to_excel(writer, sheet_name="有差异的数据", index=False)
    #             df4.to_excel(writer, sheet_name='各涨幅差异情况', index=False)
    #         print("开始上传文件到obs")
    #         obs_file_upload(f"transport_fee_compare/{filename.split('/')[-1]}", filename)
    #         send_msg('动销组定时任务推送', '海外仓运费核对',
    #                  f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓运费核对上传成功",
    #                  is_all=False)
    #     else:
    #         print("无运费差异超过10%的数据")
    # except:
    #     send_msg('动销组定时任务推送', '海外仓运费核对',
    #              f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓运费核对上传出现问题,请及时排查,失败原因详情请查看airflow",
    #              mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
    #              status='失败')
    #     raise Exception(traceback.format_exc())
    # # 海外仓运费数据备份
    try:
        print('海外仓运费useful备份CK...')
        write_to_ck()
    except:
        print('海外仓运费备份CK失败.')
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)


if __name__ == "__main__":
    transport_fee_compare_upload()
