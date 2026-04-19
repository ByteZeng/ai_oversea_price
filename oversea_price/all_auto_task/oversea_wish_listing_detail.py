import time
import traceback
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger

from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql



def delete_data(conn):
    sql_3 = f"TRUNCATE yibai_wish_over_sea_listing"
    conn.execute(sql_3)


def get_database_data(conn):
    conn_kd = connect_to_sql(database='yibai_wish', data_sys='小平台刊登库wish')
    i = 1
    for account_id in range(601):
        print(i)
        sql = f'''SELECT DISTINCT A.*,
             CASE WHEN  B.product_id IS NULL THEN '修改WE及直发运费' ELSE '只修改WE运费' end as '是否修改运费'
            FROM (
            SELECT
              a.account_id,
              a.parent_seller_sku,
              seller_sku,
              c.account_name,
              a.product_id,
              parent_sku as '父体SKU',
              a.sku,
               CASE 

                WHEN a.sku LIKE 'GB-%%' THEN REPLACE(a.sku,'GB-','') 

                WHEN a.sku LIKE 'DE-%%' THEN REPLACE(a.sku,'DE-','') 

                WHEN a.sku LIKE 'FR-%%' THEN REPLACE(a.sku,'FR-','') 

                WHEN a.sku LIKE 'ES-%%' THEN REPLACE(a.sku,'ES-','') 

                WHEN a.sku LIKE 'IT-%%' THEN REPLACE(a.sku,'IT-','') 

                WHEN a.sku LIKE 'AU-%%' THEN REPLACE(a.sku,'AU-','') 

                WHEN a.sku LIKE 'CA-%%' THEN REPLACE(a.sku,'CA-','') 

                WHEN a.sku LIKE 'JP-%%' THEN REPLACE(a.sku,'JP-','') 

                WHEN a.sku LIKE 'US-%%' THEN REPLACE(a.sku,'US-','') 

                WHEN a.sku LIKE '%%DE' THEN REPLACE(a.sku,'DE','')

                WHEN a.sku LIKE '%%GB' THEN REPLACE(a.sku,'GB','')

                ELSE a.sku END AS '子SKU',
              case when b.we_status=1 then '海外仓发货' ELSE '国内仓发货' end as '是否海外发货',
              a.price as '线上价格',
              b.standard_fee  as '直发运费',
              a.price+convert(b.standard_fee,DECIMAL(10,2)) as '直发当前价格',
              CASE WHEN b.standard_fee='' then 9999 else 
              GREATEST(70,convert(b.standard_fee,DECIMAL(10,2))) end  as '直发运费最高涨价值',
              CASE WHEN b.country_code='GB' THEN 'UK' ELSE b.country_code END AS country_code ,
              CASE WHEN b.we_fee='' THEN 0.01 ELSE  b.we_fee END  AS 'WE发货运费',
              a.price+convert(b.we_fee,DECIMAL(10,2)) as 'WE当前价格',
                CASE WHEN b.we_fee='' then 9999 else 
              GREATEST(70,convert(b.we_fee,DECIMAL(10,2))) end  as 'WE运费最高涨价值',
            CONCAT('Express-',b.country_code) AS 'country_code_post' 
            FROM
              yibai_wish.yibai_wish_listing a
              LEFT JOIN yibai_wish.yibai_wish_express_fee b ON a.product_id = b.product_id
              LEFT JOIN yibai_system.yibai_wish_account c ON a.account_id = c.wish_id 
              
            WHERE
              a.status = 1 AND b.we_status=1
            and sku<>'' and price<>0  and b.we_fee<>'0' 
           and  c.wish_id={account_id})A
            LEFT  JOIN 
            (
             SELECT
              a.account_id,
              a.parent_seller_sku,
              seller_sku,
              c.account_name,
              a.product_id,
              parent_sku as '父体SKU',
              a.sku,
               CASE 

                WHEN a.sku LIKE 'GB-%%' THEN REPLACE(a.sku,'GB-','') 

                WHEN a.sku LIKE 'DE-%%' THEN REPLACE(a.sku,'DE-','') 

                WHEN a.sku LIKE 'FR-%%' THEN REPLACE(a.sku,'FR-','') 

                WHEN a.sku LIKE 'ES-%%' THEN REPLACE(a.sku,'ES-','') 

                WHEN a.sku LIKE 'IT-%%' THEN REPLACE(a.sku,'IT-','') 

                WHEN a.sku LIKE 'AU-%%' THEN REPLACE(a.sku,'AU-','') 

                WHEN a.sku LIKE 'CA-%%' THEN REPLACE(a.sku,'CA-','') 

                WHEN a.sku LIKE 'JP-%%' THEN REPLACE(a.sku,'JP-','') 

                WHEN a.sku LIKE 'US-%%' THEN REPLACE(a.sku,'US-','') 

                WHEN a.sku LIKE '%%DE' THEN REPLACE(a.sku,'DE','')

                WHEN a.sku LIKE '%%GB' THEN REPLACE(a.sku,'GB','')

                ELSE a.sku END AS '子SKU',
              case when b.we_status=1 then '海外仓发货' ELSE '国内仓发货' end as '是否海外发货',
              a.price as '线上价格',
              b.standard_fee  as '直发运费',
              a.price+convert(b.standard_fee,DECIMAL(10,2)) as '直发当前价格',
              CASE WHEN b.standard_fee='' then 9999 else 
              GREATEST(70,convert(b.standard_fee,DECIMAL(10,2))) end  as '直发运费最高涨价值',
              CASE WHEN b.country_code='GB' THEN 'UK' ELSE b.country_code END AS country_code ,
              CASE WHEN b.we_fee='' THEN 0.01 ELSE  b.we_fee END  AS 'WE发货运费',
              a.price+convert(b.we_fee,DECIMAL(10,2)) as 'WE当前价格',
                CASE WHEN b.we_fee='' then 9999 else 
              GREATEST(70,convert(b.we_fee,DECIMAL(10,2))) end  as 'WE运费最高涨价值',
            CONCAT('Express-',b.country_code) AS 'country_code_post' 
            FROM
              yibai_wish.yibai_wish_listing a
              LEFT JOIN yibai_wish.yibai_wish_express_fee b ON a.product_id = b.product_id
              LEFT JOIN yibai_system.yibai_wish_account c ON a.account_id = c.wish_id 
                    WHERE
                       a.status = 1   AND c.status=1  and b.we_status<>1 and b.`standard_status`=1
                    and sku<>'' and price<>0  AND b.country_code='KR'  and c.wish_id={account_id}) B  ON A.account_id=B.account_id and A.product_id=B.product_id'''
        df = conn_kd.read_sql(sql)
        conn.to_sql(df, 'yibai_wish_over_sea_listing', if_exists='append')
        i += 1
    conn_kd.close()


def filter_data_number(conn):
    sql = """select count(1) from yibai_wish_over_sea_listing"""
    df = conn.read_sql(sql)
    df_number = df.loc[0][0]
    return df_number


def wish_listing():
    try:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        print(1)
        delete_data(conn)
        print(2)
        get_database_data(conn)
        all_data_number = filter_data_number(conn)
        print(all_data_number)
        conn.close()
    except:
        send_msg('动销组定时任务推送', '海外仓wish全量定价',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓wish平台listing表数据数据处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if all_data_number:
            send_msg('动销组定时任务推送', '海外仓wish全量定价',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓wish平台listing表数据数据条数为{all_data_number}",
                     is_all=False)
        else:
            send_message = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓wish平台listing表数据数据条数为{all_data_number}，数据量异常，请检查！"
            send_msg('动销组定时任务推送', '海外仓wish全量定价', send_message,
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(send_message)
