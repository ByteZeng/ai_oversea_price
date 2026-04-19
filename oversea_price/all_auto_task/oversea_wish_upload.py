import datetime
import json
import time

import pandas as pd
from retry import retry
from tqdm import tqdm
import pika
import traceback

from all_auto_task.dingding import send_msg
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.robot import ding

def get_channel():
    #测试环境
    # USERNAME = 'zhangyilan'
    # PASSWORD = '123'
    # VIRTUAL_HOST = '/'
    # HOST = '172.16.10.186'
    # PORT = 5672
    # credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    # parameters = pika.ConnectionParameters(HOST, PORT, VIRTUAL_HOST, credentials, heartbeat=0)
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()

    #生产环境
    USERNAME = 'appuser'
    PASSWORD = 'Ybmqapp2019'
    VIRTUAL_HOST = '/'
    # HOST = '121.37.212.130'
    HOST = '172.16.50.166'
    PORT = 5672
    credentials = pika.PlainCredentials(USERNAME, PASSWORD)
    parameters = pika.ConnectionParameters(HOST, PORT, VIRTUAL_HOST, credentials, heartbeat=0)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    return channel

g_channel = None
g_err_count = 0

# wish平台接口传价
@retry(tries=7, delay=2, backoff=2)
def wish_put_data(json_data):
    global g_channel
    global g_err_count
    try:
        g_channel.basic_publish(exchange='WISH_PRICE_ADJUST', routing_key='wish_price_adjust', body=json_data)
    except:
        print(f'wish:{json_data}失败，失败原因：', traceback.format_exc())
        g_err_count = g_err_count + 1
        time.sleep(5)
        g_channel = get_channel()
        raise Exception(traceback.format_exc())

def wish_price_post(df):
    global g_channel
    if len(df) < 1 :
        return
    with tqdm(total=len(df), desc='wish') as pbar:
        for row in df.to_dict(orient='records'):
            json_data = json.dumps(row)
            wish_put_data(json_data)
            pbar.update(1)

def oversea_wish_price_post():
    global g_channel
    global g_err_count
    ck_sql1 = """
                with groupArrayIf((country_code,direct_shipping), direct_shipping is not null) as data_arr
                select 
                    account_id, product_id ,
                    arrayMap(x->map('code', x.1, 'new_shipping', toString(x.2)), data_arr) as change
                 from
                (
                with 
                
                    groupArray((we_trans_fee_adjust, "WE发货运费最终")) as we_send_fee_final_arr,
                    groupArray(("是否修改运费", line_trans_fee_adjust, "直发运费最终")) as standard_trans_fee_final_arr,
                    arrayMap(x->if(x.1==-1, Null, x.2), we_send_fee_final_arr) as res_arr,
                    arrayReduce('max', res_arr) as trans_price,
                    arrayMap(x->multiIf(x.2==-1, Null,and(x.1=='修改WE及直发运费',x.2<>-1), x.3, Null), standard_trans_fee_final_arr) as direct_trans_fee_arr,
                    arrayReduce('max', direct_trans_fee_arr) as direct_trans_fee
                    select 
                        account_id ,
                        product_id,
                        splitByChar('-', country_code_post)[2] as country_code,
                        cast(multiIf(trans_price>1, round(trans_price,2), isNull(trans_price), Null, 1) as Nullable(Float64))-1 + rand()%100/100 as oversea_shipping,
                        cast(multiIf(direct_trans_fee>1, round(direct_trans_fee,2), isNull(direct_trans_fee), Null, 1) as Nullable(Float64))-1 + rand()%100/100 as direct_shipping
                    from 
                    (
                    select
                        account_id,
                        product_id,
                        "是否修改运费",
                        if("是否修改运费"='只修改WE运费', -1, cast("直发运费调整" as Int32)) as line_trans_fee_adjust,
                        country_code_post,
                        cast("WE运费调整" as Int32) as we_trans_fee_adjust,
                        toDecimal32OrZero("WE发货运费",4) as we_send_fee_1,
    					cast(we_send_fee_1 as Int32) as we_send_fee,
                        toDecimal32OrZero("直发运费", 4) as line_trans_fee_1,
    					cast(line_trans_fee_1 as Int32) as line_trans_fee,
                        case when line_trans_fee_adjust = -1 or line_trans_fee = 0 then NULL 
                             when line_trans_fee_adjust <> -1 and  line_trans_fee <> 0 then line_trans_fee + line_trans_fee_adjust
                         end as "直发运费最终",
                        case when we_send_fee = 0 then NULL 
                             when we_send_fee <> 0 then we_send_fee + we_trans_fee_adjust
                         end as "WE发货运费最终"
                    from
                        yibai_oversea.yibai_wish_oversea   
                    where mod(account_id, 7) = arg_filter
                    )
                    group by 
                        account_id ,
                        product_id,
                        country_code_post 
                )   
                group by account_id, product_id
                having length(data_arr) > 0
                """

    ck_sql2 = """
                  with groupArrayIf((country_code,oversea_shipping), oversea_shipping is not null) as data_arr
                select 
                    account_id, product_id ,
                    arrayMap(x->map('code', x.1, 'new_shipping', toString(x.2)), data_arr) as change
                 from
                (
                with 
                    groupArray((we_trans_fee_adjust, "WE发货运费最终")) as we_send_fee_final_arr,
                    groupArray(("是否修改运费", line_trans_fee_adjust, "直发运费最终")) as standard_trans_fee_final_arr,
                    arrayMap(x->if(x.1==-1, Null, x.2), we_send_fee_final_arr) as res_arr,
                    arrayReduce('max', res_arr) as trans_price,
                    arrayMap(x->multiIf(x.2==-1, Null,and(x.1=='修改WE及直发运费',x.2<>-1), x.3, Null), standard_trans_fee_final_arr) as direct_trans_fee_arr,
                    arrayReduce('max', direct_trans_fee_arr) as direct_trans_fee
                    select 
                        account_id ,
                        product_id,
                        splitByChar('-', country_code_post)[2] as country_code,
                        cast(multiIf(trans_price>1, round(trans_price,2), isNull(trans_price), Null, 1) as Nullable(Float64))-1  + rand()%100/100 as oversea_shipping,
                        cast(multiIf(direct_trans_fee>1, round(direct_trans_fee,2), isNull(direct_trans_fee), Null, 1) as Nullable(Float64))-1  + rand()%100/100 as direct_shipping
                    from 
                    (
                    select
                        account_id,
                        product_id,
                        "是否修改运费",
                        if("是否修改运费"='只修改WE运费', -1, cast("直发运费调整" as Int32)) as line_trans_fee_adjust,
                        country_code_post,
                        cast("WE运费调整" as Int32) as we_trans_fee_adjust,
                        toDecimal32OrZero("WE发货运费",4) as we_send_fee_1,
    					cast(we_send_fee_1 as Int32) as we_send_fee,
                        toDecimal32OrZero("直发运费", 4) as line_trans_fee_1,
    					cast(line_trans_fee_1 as Int32) as line_trans_fee,
                        case when line_trans_fee_adjust = -1 or line_trans_fee = 0 then NULL 
                             when line_trans_fee_adjust <> -1 and  line_trans_fee <> 0 then line_trans_fee + line_trans_fee_adjust
                         end as "直发运费最终",
                        case when we_send_fee = 0 then NULL 
                             when we_send_fee <> 0 then we_send_fee + we_trans_fee_adjust
                         end as "WE发货运费最终"
                    from
                        yibai_oversea.yibai_wish_oversea   
                    where mod(account_id, 7) = arg_filter
                    )
                    group by 
                        account_id ,
                        product_id,
                        country_code_post 
                )   
                group by account_id, product_id
                having length(data_arr) > 0
            """
    try:
        ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                             db_name='yibai_oversea')
        g_channel = get_channel()
        direct_count = 0
        oversea_count = 0
        # 调价数记录
        num = 0
        for i in range(7):
            print(i)
            print("upload oversea direct_shipping! ")
            df = ck_client.ck_select_to_df(ck_sql1.replace('arg_filter', str(i)))
            direct_count = len(df) + direct_count
            df1 = df[['product_id']].drop_duplicates()

            df['type'] = 1
            df['user_id'] = '8988'
            wish_price_post(df)

            print("upload oversea oversea_shipping! ")
            df = ck_client.ck_select_to_df(ck_sql2.replace('arg_filter', str(i)))
            oversea_count = len(df) + oversea_count
            df2 = df[['product_id']].drop_duplicates()

            df['type'] = 2
            df['user_id'] = '8988'
            wish_price_post(df)
            print(f"g_err_count:{g_err_count}")
            df3 = df1.append(df2)
            df3 = df3[['product_id']].drop_duplicates()
            num = num + df3.shape[0]
            del df1, df2, df3

        g_channel.close()
        ding_robot1 = ding.DingRobot('WISH平台调价通知')
        ding_robot1.send_msg(
            text=f"""{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 海外仓WISH调价上传成功, 其中直发数据量:{direct_count}, 二级仓数据量:{oversea_count}""",
            mobiles=['+86-15827466691', '+86-13296513752'],
            is_all=False)

        send_msg('动销组定时任务推送', '海外仓WISH调价上传',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓WISH调价上传成功, 其中直发数据量:{direct_count}, 二级仓数据量:{oversea_count}",
                 is_all=False)
        from pulic_func.base_api.adjust_price_function import adjust_post
        # num = ck_client.ck_select_to_df('select count(distinct account_id,product_id,country_code) as num from yibai_oversea.yibai_wish_oversea')
        adjust_post(platform='WISH', item='WISH海外仓调价', number=num, type=2)
    except:
        send_msg('动销组定时任务推送', '海外仓WISH调价上传',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓WISH调价上传出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise(traceback.format_exc())


if __name__ == '__main__':
    oversea_wish_price_post()


