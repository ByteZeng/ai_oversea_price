import collections
import time
import traceback
from pandas import DataFrame
from sqlalchemy import create_engine, VARCHAR, DECIMAL, Integer, BigInteger
import pandas as pd
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck


def get_newest_time():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    table_list = conn.show_table_list()
    conn.close()
    #
    new_list = [i for i in table_list if 'oversea_age_' in i and 'dtl' in i]
    res_list = [int(i[i.rindex('_') - 8:i.rindex('_')]) for i in new_list if
                '_' not in i[i.rindex('_') - 8:i.rindex('_')]]
    max_number = max(res_list)
    print(max_number)
    return max_number


# 回滚函数
def delete_dtl_and_remake_data():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql1 = f"""drop table oversea_age_{time.strftime('%Y%m%d')}_dtl"""
    conn.execute(sql1)
    print(sql1)
    sql2 = f"""delete from oversea_sale_status_remake where date='{time.strftime('%Y-%m-%d')}'"""
    conn.execute(sql2)
    print(sql2)
    conn.close()


def get_up_data(conn):
    sql = """
    SELECT distinct A.sku,A.name,group_concat( distinct A.price_up_day,'#','涨价缩销' order by A.price_up_day) as up_price 
    from (
        select sku,name,price_up_day from oversea_explosives_data.amazon_up_data
        union all
        select sku,name,price_up_day from oversea_explosives_data.cdiscount_up_data 
        union all
        select sku,name,price_up_day from oversea_explosives_data.ebay_up_data 
        union all
        select sku,name,price_up_day from oversea_explosives_data.walmart_up_data
    ) A 
    group by A.sku,A.name
    """
    df_1 = conn.read_sql(sql)
    return df_1


def get_down_data(conn):
    sql = """
    SELECT distinct A.sku,A.name,group_concat(distinct A.price_down_day,'#','回调' order by A.price_down_day) as down_price 
    from (
        select sku,name,price_down_day from oversea_explosives_data.amazon_down_data
        union all
        select sku,name,price_down_day from oversea_explosives_data.cd_down_data 
        union all
        select sku,name,price_down_day from oversea_explosives_data.ebay_down_data 
        union all
        select sku,name,price_down_day from oversea_explosives_data.walmart_down_data
    ) A 
    group by A.sku,A.name
    """
    df_2 = conn.read_sql(sql)
    return df_2


def get_data(conn):
    sql = """
    select A.SKU as sku,A.warehouse,group_concat(A.DATE,'#',A.adjust_recent_clean order by A.DATE) as status_list  
    from (
        select * from oversea_sale_status_remake
    )A
	group by A.SKU,A.warehouse 
	HAVING A.SKU IS NOT NULL 
	"""
    df = conn.read_sql(sql)
    return df


def merge_data():
    conn_mx = pd_to_ck(database='support_document', data_sys='调价明细历史数据')
    sql = """
    with (
        select CAST(date_diff(day, toDate(min(`DATE`)), today())+1, 'UInt64') as n
        from yibai_oversea.oversea_sale_status_remake 
    ) as n_day, 
    all_date_table as (
        select toString(date_add(day, n, min_date)) as all_date
        from (
            select min_date,arrayJoin(range(ifNull(n_day, 1000))) as n
            from (
                select toDate(min(`DATE`)) as min_date from yibai_oversea.oversea_sale_status_remake 
            )
        )
        where all_date not in (select distinct `DATE` from yibai_oversea.oversea_sale_status_remake)
        order by all_date asc
    ),
    listing_temp0 as (
        -- 历史记录(第一次记录为回调的， 状态改为正常)
        select distinct SKU AS sku,warehouse,adjust_recent_clean,`DATE` from yibai_oversea.oversea_sale_status_remake 
        
        union all 
        -- 当天有其他sku记录， 但是没有这个sku记录的按照正常处理
        select a.SKU as sku,a.warehouse as warehouse,'正常' as adjust_recent_clean,b.`DATE` as `DATE` 
        from ( 
            select distinct SKU,warehouse 
            from yibai_oversea.oversea_sale_status_remake 
        ) a 
        cross join (select distinct `DATE` from yibai_oversea.oversea_sale_status_remake) b 
        where (a.SKU,a.warehouse,b.`DATE`) not in (
            select distinct SKU,warehouse,`DATE` from yibai_oversea.oversea_sale_status_remake
        )
        union all 
        -- 当天没有任何记录的数据
        select a.SKU AS sku,a.warehouse,'未知' as adjust_recent_clean,b.all_date as `DATE`
        from (
            select distinct SKU,warehouse 
            from yibai_oversea.oversea_sale_status_remake 
        ) a 
        cross join all_date_table b 
    )
    ,
    listing_temp as (
        -- 第一天为 '回调', '未知' 的， 改为正常
        select sku,warehouse,adjust_recent_clean1 as adjust_recent_clean,`DATE`
        from (
            select sku,warehouse,
            if(`DATE` = (select min(`DATE`) from yibai_oversea.oversea_sale_status_remake) and adjust_recent_clean in ('回调', '未知'), '正常', adjust_recent_clean) as adjust_recent_clean1,`DATE` 
            from listing_temp0 
        )
    )
    ,
    adjust_temp1 as (
        -- 1. 今天状态和昨天一样的，把今天的数据删掉
        select sku,warehouse,adjust_recent_clean,`DATE`,adjust_recent_clean_yes 
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`,
                any(adjust_recent_clean) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 preceding and 1 preceding) as adjust_recent_clean_yes
            from listing_temp 
        ) 
        where adjust_recent_clean != adjust_recent_clean_yes
    )
    ,
    adjust_temp2 as (
        -- 2. 如果当天状态未知 且 前一天和后一天的状态相同的， 把当天改成和前一天的状态相同
        select sku,warehouse,adjust_recent_clean,
            if(adjust_recent_clean_yes=adjust_recent_clean_tomorrow and adjust_recent_clean='未知', adjust_recent_clean_yes, adjust_recent_clean) as adjust_recent_clean1,
            `DATE`,adjust_recent_clean_yes,adjust_recent_clean_tomorrow 
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`,adjust_recent_clean_yes,
                any(adjust_recent_clean) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 following and 1 following) as adjust_recent_clean_tomorrow
            from adjust_temp1
        )
    )
    ,
    adjust_temp3 as (
        -- 3. 再次：今天状态和昨天一样的，把今天的数据删掉
        select sku,warehouse,adjust_recent_clean,`DATE`,adjust_recent_clean_yes 
        from (
            select sku,warehouse,adjust_recent_clean1 as adjust_recent_clean,`DATE`,
                any(adjust_recent_clean1) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 preceding and 1 preceding) as adjust_recent_clean_yes
            from adjust_temp2
        )
        where adjust_recent_clean != adjust_recent_clean_yes
    )
    ,
    adjust_temp4 as (
        -- 4. 把 未知 改为前一个状态
        select sku,warehouse,adjust_recent_clean1 as adjust_recent_clean,`DATE` 
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`,adjust_recent_clean_yes,
                if(adjust_recent_clean='未知', adjust_recent_clean_yes, adjust_recent_clean) as adjust_recent_clean1
            from adjust_temp3
        )
    )
    ,
    adjust_temp5 as (
        --5. 把回调改为前一个状态
        select sku,warehouse,if(adjust_recent_clean='回调', adjust_recent_clean_yes, adjust_recent_clean) as adjust_recent_clean,`DATE` 
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`,
                any(adjust_recent_clean) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 preceding and 1 preceding) as adjust_recent_clean_yes 
            from (
                select sku,warehouse,adjust_recent_clean,`DATE` 
                from (
                    select sku,warehouse,adjust_recent_clean,`DATE`,
                        any(adjust_recent_clean) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 preceding and 1 preceding) as adjust_recent_clean_yes
                    from adjust_temp4
                ) 
                where adjust_recent_clean != adjust_recent_clean_yes
            )
        )
    )
    ,
    adjust_temp6 as (
        --6. 今天状态和昨天一样的，把今天的数据删掉
        select sku,warehouse,adjust_recent_clean,`DATE` 
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`,
                any(adjust_recent_clean) over(partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 preceding and 1 preceding) as adjust_recent_clean_yes
            from adjust_temp5
        )
        where adjust_recent_clean != adjust_recent_clean_yes
    )
    
    select sku,warehouse,adjust_recent_clean as sale_status,start_time,end_time 
    from (
        select sku,warehouse,adjust_recent_clean,`DATE` as start_time, DATE_tomorrow, 
            toString(date_add(day, -1, toDate(if(DATE_tomorrow='', null, DATE_tomorrow)))) as end_time
        from (
            select sku,warehouse,adjust_recent_clean,`DATE`, any(`DATE`) over w as DATE_tomorrow
            from adjust_temp6 
            window w as (partition by sku,warehouse order by `DATE` asc ROWS BETWEEN 1 following and 1 following)
        )
    )
    where adjust_recent_clean != '正常' 
    settings max_memory_usage = 20000000000
    """
    data = conn_mx.ck_select_to_df(sql)
    print('销售状态计算完成')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # #
    # df_1 = get_up_data(conn)
    # print('df_1', len(df_1), df_1.columns)
    # df_1["new_sku"] = df_1["sku"] + '#' + df_1["name"]
    # dict_data1 = dict(collections.OrderedDict(zip(df_1.iloc[:, 3], df_1.iloc[:, 2])))
    # df_2 = get_down_data(conn)
    # df_2["new_sku"] = df_2["sku"] + '#' + df_2["name"]
    # dict_data2 = dict(collections.OrderedDict(zip(df_2.iloc[:, 3], df_2.iloc[:, 2])))
    # print('dict_data2', len(dict_data2))
    # df = get_data(conn)
    # print('df', len(df), df.columns)
    # df["new_sku"] = df["sku"] + '#' + df["warehouse"]
    # dict_data = dict(collections.OrderedDict(zip(df.iloc[:, 3], df.iloc[:, 2])))
    # print(len(dict_data))
    # new_dict = {}
    # for k, v in dict_data1.items():
    #     for key, value in dict_data2.items():
    #         if k == key:
    #             v = v + ',' + value
    #         new_dict[k] = v
    #
    # new_dict2 = {}
    # for k, v in dict_data.items():
    #     for key, value in new_dict.items():
    #         if k == key:
    #             v = v + ',' + value
    #         new_dict2[k] = v
    # temp_list = []
    # for k, v in new_dict2.items():
    #     v_list = v.split(",")
    #     v_list.sort()
    #     # print(v_list)
    #     if len(v_list) == 0:
    #         continue
    #     while True:
    #         if len(v_list) == 0:
    #             break
    #         base_status = v_list[0].split("#")[1]
    #         start_time = v_list[0].split("#")[0]
    #         if len(v_list) == 1:
    #             end_time = None
    #             temp_list.append([k.split("#")[0], k.split("#")[1], base_status, start_time, end_time])
    #             break
    #         # print(v_list)
    #         for i in range(len(v_list)):
    #             if v_list[1].split("#")[1] == base_status:
    #                 v_list.remove(v_list[1])
    #                 if len(v_list) == 1:
    #                     end_time = None
    #                     temp_list.append([k.split("#")[0], k.split("#")[1], base_status, start_time, end_time])
    #                     break
    #                 if v_list[1].split("#")[1] != base_status:
    #                     end_time = v_list[1].split("#")[0]
    #                     temp_list.append([k.split("#")[0], k.split("#")[1], base_status, start_time, end_time])
    #                     v_list.remove(v_list[0])
    #                     break
    #             if v_list[1].split("#")[1] != base_status:
    #                 end_time = v_list[1].split("#")[0]
    #                 temp_list.append([k.split("#")[0], k.split("#")[1], base_status, start_time, end_time])
    #                 v_list.remove(v_list[0])
    #                 break
    # print(5)
    # data = DataFrame(temp_list)
    # data.columns = ["sku", "warehouse", "sale_status", "start_time", "end_time"]
    # data.drop_duplicates(inplace=True)
    # data = data[data["sale_status"] != '回调']
    #
    print(data.info())

    print('销售状态开始写入')
    sql = f"""
    delete from over_sea.oversea_sale_status
    """
    conn.execute(sql)
    conn.close()
    n = 0
    while n < 5:
        conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        sql = f"""
            SELECT *
            FROM over_sea.oversea_sale_status
        """
        df_temp = conn.read_sql(sql)
        if len(df_temp) == 0:
            conn.to_sql(data, table='oversea_sale_status', if_exists='append')
            conn.close()
            print('销售状态写入完成')
            break
        n = n + 1
        time.sleep(10)

    # 销售状态备份ck
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = f"""
        TRUNCATE TABLE yibai_oversea.oversea_sale_status
    """
    conn_ck.ck_execute_sql(sql)
    n = 0
    while n < 5:
        sql = f"""
            SELECT *
            FROM yibai_oversea.oversea_sale_status
        """
        df_temp = conn_ck.ck_select_to_df(sql)
        if len(df_temp) == 0:
            conn_ck.ck_insert(data, 'oversea_sale_status', if_exist='append')
            print('销售状态写入完成')
            break
        n = n + 1
        time.sleep(10)


def get_slm_sku():
    """
    SLM美国仓主要用来冲销量，其sku需手动置为负利润加快动销
    """
    sql = f"""

    SELECT sku, warehouse, warehouse_name, warehouse_id, available_stock
    FROM yb_datacenter.v_oversea_stock

    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_oversea = conn_ck.ck_select_to_df(sql)
    #
    df_slm = df_oversea[df_oversea['warehouse_id'] == 921]
    df_other = df_oversea[(df_oversea['warehouse_id'] != 921) & (df_oversea['warehouse'] == '美国仓')]
    df_slm_new = df_slm[~df_slm['sku'].isin(df_other['sku'].unique())]
    #
    df_slm_new = df_slm_new[['sku', 'warehouse']]
    df_slm_new['adjust_recent_clean'] = '负利润加快动销'
    df_slm_new['DATE'] = time.strftime('%Y-%m-%d')
    df_slm_new = df_slm_new.rename(columns={'sku': 'SKU'})

    return df_slm_new

def insert_show_status():
    try:
        conn2 = connect_to_sql(database='over_sea', data_sys='数据部服务器')
        #
        # sql1 = f"""delete from oversea_sale_status_remake where date='{time.strftime('%Y-%m-%d')}'"""
        # conn2.execute(sql1)
        # 原逻辑
        # sql2 = f"""
        #     insert into oversea_sale_status_remake
        #     SELECT SKU,warehouse,adjust_recent_clean,DATE
        #     FROM oversea_age_{get_newest_time()}_dtl
        #     WHERE adjust_recent_clean IN ('正利润加快动销','负利润加快动销','回调','清仓')
        # """
        # 2023-10-09切换新逻辑
        # sql2 = f"""
        #     insert into oversea_sale_status_remake
        #     SELECT distinct sku, warehouse, sales_status as adjust_recent_clean, date_id as DATE
        #     FROM dwm_oversea_price_dtl
        #     WHERE date_id = '{time.strftime('%Y-%m-%d')}' and sales_status IN ('正利润加快动销','负利润加快动销','回调','清仓')
        # """
        # conn2.execute(sql2)

        # 2023-10-20 将remake表复制到CK中，后续销售状态在CK中处理。
        sql2 = f"""
             SELECT distinct sku as SKU, warehouse, sales_status as adjust_recent_clean, date_id as DATE
             FROM yibai_oversea.dwm_oversea_price_dtl
             WHERE date_id = '{time.strftime('%Y-%m-%d')}' and sales_status IN ('正利润加快动销','负利润加快动销','回调','清仓')     
         """
        conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        df_sku_status = conn_ck.ck_select_to_df(sql2)
        # 20240325：SLM美国仓sku手动置为负利润加快动销
        df_slm = get_slm_sku()
        df_slm = df_slm[~df_slm['SKU'].isin(df_sku_status['SKU'].unique())]
        df_sku_status = pd.concat([df_sku_status, df_slm])
        # 20241224: sku白名单数据手动加入清仓
        sql = """
            SELECT sku SKU, warehouse, is_clear
            FROM over_sea.sales_status_temp
            -- WHERE is_clear = 1
        """
        df_temp_sku = conn2.read_sql(sql)
        df_temp_sku1 = df_temp_sku[df_temp_sku['is_clear'] == 1]
        # 20250529 转泛品sku需打标清仓
        sql = """
            SELECT sku SKU, warehouse, 1 as is_clear
            FROM over_sea.dwm_sku_temp_info
            WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
            and available_stock > 0
            and  (type like '%%转泛品%%' or type like '%%VC%%')
        """
        df_fine_sku = conn2.read_sql(sql)

        df_temp_sku1 = pd.concat([df_temp_sku1, df_fine_sku])
        df_temp_sku1['adjust_recent_clean'] = '负利润加快动销'
        df_temp_sku1['DATE'] = time.strftime('%Y-%m-%d')
        df_temp_sku1.drop('is_clear', axis=1, inplace=True)
        df_sku_status = pd.concat([df_sku_status, df_temp_sku1])

        df_temp_sku2 = df_temp_sku[df_temp_sku['is_clear'] == 0]
        df_sku_status = pd.merge(df_sku_status, df_temp_sku2, how='left', on=['SKU', 'warehouse'])
        df_sku_status = df_sku_status[df_sku_status['is_clear'].isna()]
        df_sku_status.drop('is_clear', axis=1, inplace=True)

        df_sku_status = df_sku_status.sort_values(by='adjust_recent_clean', ascending=False).drop_duplicates(
            subset=['SKU', 'warehouse'])
        # ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
        #                      db_name='yibai_oversea')
        ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
        # 如果有今天的数据，先删除今天的
        sql = f"""ALTER TABLE yibai_oversea.oversea_sale_status_remake DELETE where DATE='{time.strftime('%Y-%m-%d')}'"""
        ck_client.ck_execute_sql(sql=sql)
        # 查看是否刪除完
        sql = f"""
        SELECT *
        FROM yibai_oversea.oversea_sale_status_remake  
        where DATE='{time.strftime("%Y-%m-%d")}'
        """
        df_num = ck_client.ck_select_to_df(sql)
        if len(df_num) == 0:
            # 数据存表
            # ck_client.write_to_ck_json_type(df_sku_status, 'yibai_oversea.oversea_sale_status_remake')
            ck_client.ck_insert(df_sku_status, 'oversea_sale_status_remake', if_exist='append')
        else:
            raise IOError('数据未清理完！')
        send_msg('动销组定时任务推送', '销售状态插入状态执行',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓调价销售状态插入已完成",is_all=False)

        conn2.close()
    except:
        send_msg('动销组定时任务推送', '销售状态插入状态执行',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓调价销售状态插入处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


def status_complete():
    try:
        merge_data()
        send_msg('动销组定时任务推送', '销售状态补全',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} 海外仓销售状态已补全",
                 is_all=False)
    except:
        send_msg('动销组定时任务推送', '销售状态补全',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓调价销售状态补全处理出现问题,请及时排查,失败原因详情请查看airflow日志",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())


if __name__ == '__main__':
    # status_complete()
    # merge_data()
    insert_show_status()
