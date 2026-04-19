import datetime
import os
import time
from sqlalchemy import create_engine
import pandas as pd
from all_auto_task.dingding import send_msg
from pulic_func.base_api.mysql_connect import connect_to_sql, pd_to_ck


def adjust_post_success_monitor(conn_ck, out_date, plat, item, all_num, all_data, success_num, to_be_updated_num,
                                error_num, failed_num, tocancel_num, success_lpl, audit_data, fail_pass_data, updating_data, ti, org_group='yibai'):
    a = 0
    for x in [all_num, all_data, success_num, to_be_updated_num,  error_num, failed_num, tocancel_num, success_lpl,
              audit_data, fail_pass_data, updating_data]:
        a = a + x
    if a > 0:
        sql = f"""
                insert into fba_inventory.adjust_post_success_monitor{ti} 
                (date, plat, item, all_num, all_data, success_num, to_be_updated_num, 
                error_num, success_lpl, failed_num, tocancel_num, audit_data, fail_pass_data, updating_data, org_group) 
                VALUES ('{out_date}', '{plat}', '{item}', {all_num}, {all_data}, {success_num}, {to_be_updated_num}, 
                {error_num}, {success_lpl}, {failed_num}, {tocancel_num}, {audit_data}, {fail_pass_data}, {updating_data}, '{org_group}')
                """
        conn_ck.ck_execute_sql(sql)

def get_com_time(out_date, conn_ck, yesterday_time, plat, item, type=1, org_group='yibai'):
    sql = f"""
        select Complete_time,ifNull(start_time,  Complete_time) as start_time
        from fba_inventory.adjust_Complete_time 
        where record='{out_date}' and item like '%%{plat}%%' and item like '%%{item}%%' and org_group='{org_group}'
        order by start_time asc 
        """
    df = conn_ck.ck_select_to_df(sql, columns=['Complete_time', 'start_time'])
    # df = df.sort_values(['start_time'], ascending=True)
    # df = df.reset_index(drop=True)
    if df.shape[0] == 0:
        yesterday_time = yesterday_time
    else:
        yesterday_time = df['start_time'][0]
        if type == 2:
            yesterday_time = time.mktime(time.strptime(yesterday_time, '%Y-%m-%d %H:%M:%S'))
    return yesterday_time


def get_yesterday_time():
    out_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    yesterday_time = out_date + ' ' + '12:00:00'

    today = datetime.date.today().isoformat()
    today_time = today + ' ' + '12:00:00'

    yesterday_time_array = time.strptime(yesterday_time, "%Y-%m-%d %H:%M:%S")
    yesterday_time_stamp = int(time.mktime(yesterday_time_array))

    today_time_array = time.strptime(today_time, "%Y-%m-%d %H:%M:%S")
    today_time_stamp = int(time.mktime(today_time_array))

    return out_date, yesterday_time, today, today_time, yesterday_time_stamp, today_time_stamp


def price_adjust_data_ding(conn_ck, ding_text, platform, item, org_group='yibai'):
    if org_group == 'tt':
        conn_ck = pd_to_ck(database='fba_inventory', data_sys='通拓-ck')
    else:
        conn_ck = pd_to_ck(database='fba_inventory', data_sys='调价明细历史数据')
    sql = f"""
        select record_time,platform,item,
            sum(if(status='成功', num, 0)) as `成功`,
            sum(if(status='失败', num, 0)) as `失败`,
            sum(if(status='其他', num, 0)) as `其他`,
            `成功` + `失败` + `其他` as `总数量`,
            round(`成功`/`总数量` * 100, 2) as `成功率`
        from fba_inventory.{org_group}_price_adjust_monitor 
        where record_time=toString(today()-1) and platform='{platform}' and item like '%%{item}%%'
        group by record_time,platform,item
        order by record_time,platform,item
    """
    df = conn_ck.ck_select_to_df(sql)
    if df.shape[0] > 0:
        for i in range(df.shape[0]):
            record_time = df['record_time'][i]
            platform = df['platform'][i]
            item = df['item'][i]
            a = df['成功'][i]
            b = df['失败'][i]
            c = df['其他'][i]
            d = df['总数量'][i]
            e = df['成功率'][i]
            ding_text1 = f'{item}总调价链接{d},成功{a}条,失败{b}条,其他{c}条,调价成功率{e}%\n'
            ding_text = ding_text + ding_text1
    return ding_text



def get_ebay_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'ebay', '国内仓')

    sql = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by ='W01596'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql1 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}'
        and create_by ='W01596' and sync_status=3
    """
    df = conn_mx.ck_select_to_df(sql1)
    success_num = df['data'][0]


    sql2 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='W01596' and sync_status=1
    """
    df = conn_mx.ck_select_to_df(sql2)
    fail_pass_data = df['data'][0]

    sql3 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='W01596' and sync_status=4
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]

    sql4 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='W01596' and sync_status=0
    """
    df = conn_mx.ck_select_to_df(sql4)
    audit_data = df['data'][0]

    sql4 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='W01596' and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql4)
    num2 = df['data'][0]

    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='ebay' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_5 = conn_ck.ck_select_to_df(sql5)

    if df_5.shape[0] > 0:
        all_num = df_5["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'ebay', '国内仓', all_num, all_data, success_num, 0,
                                error_num, 0, 0, success_lpl1, audit_data, fail_pass_data, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_5.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}:ebay国内仓\n上传调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接{success_num},\n待审核链接{audit_data},\n执行中链接{num2},\n执行失败链接{error_num},\n审核不通过链接{fail_pass_data},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ebay', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('EBAY调价LISTING问题沟通群', '国内仓ebay调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓ebay调价监控', ding_text)
                dd_jk = 1
    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ebay', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('EBAY调价LISTING问题沟通群', '国内仓ebay调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓ebay调价监控', ding_text)


def get_cd_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'cd', '国内仓', 1)

    out_date1 = out_date.replace('-', '')

    sql = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by='W01596' and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')            
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=2 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')   
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by='W01596' and status=3 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')   
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=1 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')   
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    updating_data = df['data'][0]

    sql5 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by='W01596' and status=0 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')   
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql5)
    to_be_updated_num = df['data'][0]

    sql5 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by='W01596' and status=4 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all
        select distinct account_id,product_id,sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_cd_merge 
        where toDate(move_date)=toDate('{out_date}')   
    ) b 
    on c.account_id=b.account_id and a.item_id=b.product_id and a.sku=b.sku
     """
    df = conn_mx.ck_select_to_df(sql5)
    num4 = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='cd' AND item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)
    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'cd', '国内仓', all_num, all_data, success_num, to_be_updated_num,
                                error_num, 0, 0, success_lpl1, 0, 0, updating_data, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and all_data > 0:
        a = success_num / all_data
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}cd国内仓\n上传调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接{success_num},\n执行失败链接{error_num},\n上传中{updating_data},\n待上传{to_be_updated_num},\n手动取消{num4},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'cd', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('CD-调价群', '国内仓cd调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓cd调价监控', ding_text)
                dd_jk = 1


    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'cd', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('CD-调价群', '国内仓cd调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓cd调价监控', ding_text)


def get_cd_FBC(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'cd', 'FBC', 1)

    out_date1 = out_date.replace('-', '')

    # table_list = conn_mx.ck_show_tables(db_name='fba_ag_over_180_days_price_adjustment')
    # table1 = f'fbc_fbd_price_adjust_{out_date1}'
    # table2 = f'fbc_price_adjust_{out_date1}'
    # table_list1 = [table1, table2]
    # #
    # base_sql = """
    #         select 0 as account_id, '' as itemid, '' as sku
    #         """
    # for table in table_list1:
    #     if table in table_list:
    #         base_sql = f"""
    #                     {base_sql} 
    #                     union all 
    #                     select distinct account_id,itemid,sku 
    #                     from fba_ag_over_180_days_price_adjustment.{table} 
    #                     """

    sql = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_fbd_price_adjust_merge 
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_price_adjust_merge 
        where adjust_date = '{out_date}'        
    ) b 
    on c.account_id=b.account_id and a.item_id=b.itemid and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=2 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_fbd_price_adjust_merge 
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_price_adjust_merge 
        where adjust_date = '{out_date}'      
    ) b 
    on c.account_id=b.account_id and a.item_id=b.itemid and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=3 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_fbd_price_adjust_merge 
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_price_adjust_merge 
        where adjust_date = '{out_date}'      
    ) b 
    on c.account_id=b.account_id and a.item_id=b.itemid and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=1 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_fbd_price_adjust_merge 
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_price_adjust_merge 
        where adjust_date = '{out_date}'      
    ) b 
    on c.account_id=b.account_id and a.item_id=b.itemid and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    updating_data = df['data'][0]

    sql5 = f"""
    select count(distinct a.account_id,a.item_id,a.sku) as data
    from (
        select distinct account_id,item_id,sku 
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by='W01596' and status=0 and type=2
    ) a 
    inner join (
        select id,account_id from {org_group}_sale_center_system_sync.{org_group}_system_account
        where platform_code='CDISCOUNT'
    ) c
    on a.account_id=c.id
    inner join (
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_fbd_price_adjust_merge 
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,itemid,sku 
        from fba_ag_over_180_days_price_adjustment.fbc_price_adjust_merge 
        where adjust_date = '{out_date}'      
    ) b 
    on c.account_id=b.account_id and a.item_id=b.itemid and a.sku=b.sku
    """
    df = conn_mx.ck_select_to_df(sql5)
    to_be_updated_num = df['data'][0]


    sql5 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=4 and type=2
    """
    df = conn_mx.ck_select_to_df(sql5)
    num4 = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='cd' AND item like '%%FBC%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)
    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'cd', 'FBC', all_num, all_data, success_num, to_be_updated_num,
                                error_num, 0, 0, success_lpl1, 0, 0, updating_data, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and all_data > 0:
        a = success_num / all_data
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}FBC\n上传调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接{success_num},\n执行失败链接{error_num},\n上传中{updating_data},\n待上传{to_be_updated_num},\n手动取消{num4},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'cd', 'FBC', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('CD-调价群', 'FBC调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓cd调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'cd', 'FBC', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('CD-调价群', 'FBC调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓cd调价监控', ding_text)



def get_walmart_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'walmart', '国内仓')

    sql = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku 
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596'
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')            
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=3
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=2
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]
    #print(error_num)

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=1
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    failed_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=0
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    not_implement_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=4
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    num4 = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=5
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_walmart_merge 
        where toDate(move_date)=toDate('{out_date}')      
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    num5 = df['data'][0]


    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='walmart' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'walmart', '国内仓', all_num, all_data, success_num, 0,
                                error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}国内仓walmart,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n未执行{not_implement_num},\n执行失败链接{error_num},\n执行中链接{failed_num},\n需要再次上传{num4},\n不需要上传{num5},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'walmart', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('walmart平台调价', '国内仓walmart调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓walmart调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'walmart', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('walmart平台调价', '国内仓walmart调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓walmart调价监控', ding_text)


def get_walmart_wfs(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'walmart', '国内仓')

    sql = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku 
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596'
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=3
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=2
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]
    #print(error_num)

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=1
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    failed_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=0
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    not_implement_num = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=4
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    num4 = df['data'][0]

    sql4 = f"""
    select count(distinct a.erp_id, a.seller_sku) as data
    from (
        select distinct erp_id, seller_sku
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = 'W01596' and upload_status=5
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_price_adjust_merge
        where adjust_date = '{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_wfs.{org_group}_wfs_fbd_price_adjust_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.erp_id=b.account_id and a.seller_sku=b.seller_sku
    """
    df = conn_mx.ck_select_to_df(sql4)
    num5 = df['data'][0]


    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='walmart' and item like '%%WFS%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'walmart', 'WFS', all_num, all_data, success_num, 0,
                                error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}walmart-WFS,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n未执行{not_implement_num},\n执行失败链接{error_num},\n执行中链接{failed_num},\n需要再次上传{num4},\n不需要上传{num5},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'walmart', 'WFS', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('walmart平台调价', 'walmart-WFS调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', 'walmart-WFS调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'walmart', 'WFS', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('walmart平台调价', 'walmart-WFS调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', 'walmart-WFS调价监控', ding_text)

def get_amazon_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time1 = get_com_time(out_date, conn_ck, yesterday_time, 'amazon', '国内仓', org_group=org_group)
        yesterday_time2 = get_com_time(out_date, conn_ck, yesterday_time, 'FBA', 'FBA', org_group=org_group)
        yesterday_time = min(yesterday_time1, yesterday_time2)

    out_date1 = out_date.replace('-', '')
    # 国内仓
    sql = f"""
    select count(distinct a.account_id,a.seller_sku) as num
    from (
        select account_id,seller_sku 
        from {org_group}_product_kd_sync.{org_group}_price_adjust_log_month 
        where creator in ('黄星星', '宋展昭', '流程测试') and created_at >'{yesterday_time}' and created_at <='{today_time}'
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_fbm_merge
        where adjust_date='{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_fbm_merge
        where adjust_date='{out_date}'
    ) b 
    on a.account_id=b.account_id and a.seller_sku=b.seller_sku
    settings max_memory_usage = 30000000000
    """
    df_FBM = conn_mx.ck_select_to_df(sql)

    #
    sql = f"""
    select count(distinct a.account_id,a.seller_sku) as num
    from (
        select account_id,seller_sku 
        from {org_group}_product_kd_sync.{org_group}_price_adjust_log_month 
        where creator in ('黄星星', '宋展昭', '流程测试') and created_at >'{yesterday_time}' and created_at <='{today_time}'
    ) a 
    inner join (
        select distinct account_id,seller_sku 
        from {org_group}_price_fba.{org_group}_fba_price_adjustment_fba_merge
        where adjust_date='{out_date}'
        union all 
        select distinct account_id,seller_sku 
        from {org_group}_price_fba.{org_group}_fba_price_adjustment_fbm_merge
        where toDate(move_date)=toDate('{out_date}')
    ) b 
    on a.account_id=b.account_id and a.seller_sku=b.seller_sku
    settings max_memory_usage = 30000000000
    """
    df_FBA = conn_mx.ck_select_to_df(sql)
    #
    success_num1 = df_FBA['num'][0]
    success_num2 = df_FBM['num'][0]


    # fba
    sql2 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='AMAZON' and item like '%%FBA%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql2)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0

    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num1 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'AMAZON', 'FBA', all_num, 0, success_num1, 0,
                                0, 0, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num1 >= 0:
        a = success_num1 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}: FBA总上传链接{all_num},调价执行链接数{success_num1},调价执行率{success_lpl};\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'amazon', 'FBA', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('亚马逊调价成功率', 'fba调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', 'fba调价监控', ding_text)
                dd_jk = 1
    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'amazon', 'FBA', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('亚马逊调价成功率', 'fba调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', 'fba调价监控', ding_text)


    # 国内仓
    sql2 = f"""
            select platform,sum(number) as number
            from fba_inventory.adjust_post_number
            where record='{out_date}' and platform='AMAZON' and item like '%%国内仓%%' and org_group='{org_group}'
            group by platform
        """
    df_2 = conn_ck.ck_select_to_df(sql2)
    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0

    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num2 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'AMAZON', '国内仓', all_num, 0, success_num2, 0,
                                0, 0, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num2 >= 0:
        a = success_num2 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}: 国内仓AMAZON总调价链接{all_num},调价执行链接数{success_num2},调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'amazon', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('亚马逊调价成功率', '国内仓AMAZON调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓AMAZON调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'amazon', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('亚马逊调价成功率', '国内仓AMAZON调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓AMAZON调价监控', ding_text)


def get_wish_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'wish', '国内仓')

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596' and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql)
    success_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596' and sync_status in (0)
    """
    df = conn_mx.ck_select_to_df(sql)
    to_be_updated_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596' and sync_status in (1)
    """
    df = conn_mx.ck_select_to_df(sql)
    num1 = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596' and sync_status in (3)
    """
    df = conn_mx.ck_select_to_df(sql)
    error_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = 'W01596' and sync_status in (4)
    """
    df = conn_mx.ck_select_to_df(sql)
    failed_num = df['data'][0]

    sql2 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='wish' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql2)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'wish', '国内仓', all_num, all_data, success_num,
                                to_be_updated_num, error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}国内仓wish总调价链接{all_num},\n上传成功{all_data},\n待执行{to_be_updated_num},\n执行中{num1},\n取消{failed_num},\n失败{error_num},\n执行成功链接数{success_num},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'wish', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('wish平台调价', '国内仓wish调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓wish调价监控', ding_text)
                dd_jk = 1
    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'wish', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('wish平台调价', '国内仓wish调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓wish调价监控', ding_text)


def get_wish_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'wish', '海外仓')
    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313' and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql)
    success_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313' and sync_status in (0)
    """
    df = conn_mx.ck_select_to_df(sql)
    to_be_updated_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313' and sync_status in (1)
    """
    df = conn_mx.ck_select_to_df(sql)
    num1 = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313' and sync_status in (3)
    """
    df = conn_mx.ck_select_to_df(sql)
    error_num = df['data'][0]

    sql = f"""
    select count(distinct product_id) as data from {org_group}_sale_center_listing_sync.{org_group}_wish_price_adjustment
    where warehouse_type in (1, 2) AND create_at > '{yesterday_time}' AND create_at <= '{today_time}' 
    AND create_by = '209313' and sync_status in (4)
    """
    df = conn_mx.ck_select_to_df(sql)
    failed_num = df['data'][0]

    sql2 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}'and platform='WISH' and item like '%%海外仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql2)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'wish', '海外仓', all_num, all_data, success_num,
                                to_be_updated_num, error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}国内仓wish总调价链接{all_num},\n上传成功{all_data},\n待执行{to_be_updated_num},\n执行中{num1},\n取消{failed_num},\n失败{error_num},\n执行成功链接数{success_num},\n调价执行率{success_lpl}\n'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('wish平台调价', '海外仓wish调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓wish调价监控', ding_text)


def get_daraz_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'daraz', '国内仓', type=2)

    sql = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_daraz_auto_modify_price
        where update_time>{yesterday_time} and update_time <={today_time}
        and create_by = 'W01596'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql1 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_daraz_auto_modify_price
        where update_time>{yesterday_time} and update_time <={today_time}
        and modify_state=1 and create_by = 'W01596'
    """
    df = conn_mx.ck_select_to_df(sql1)
    success_num = df['data'][0]
    #print(success_num)

    sql2 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_daraz_auto_modify_price
        where update_time>{yesterday_time} and update_time <={today_time} 
        and modify_state=0 and create_by = 'W01596'
    """
    df = conn_mx.ck_select_to_df(sql2)
    to_be_updated_num = df['data'][0]
    #print(to_be_updated_num)

    sql3 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='daraz' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql3)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    error_num = max(all_num - success_num, 0)
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'daraz', '国内仓', all_num, all_data, success_num, to_be_updated_num,
                                error_num, 0, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}daraz国内仓\n总上传链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n待更新链接{to_be_updated_num},\n执行失败链接{error_num},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'darae', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('daraz调价-new', '国内仓daraz调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓daraz调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'darae', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('daraz调价-new', '国内仓daraz调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓daraz调价监控', ding_text)



def get_lazada_china(out_date, yesterday_time_stamp, today, today_time_stamp, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time_stamp = get_com_time(out_date, conn_ck, yesterday_time_stamp, 'lazada', '国内仓', 2)

    sql = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_lazada_auto_modify_price
        where upload_time>{yesterday_time_stamp} and upload_time <= {today_time_stamp} and create_by in ('W01596', 'TT202655')
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    # `is_upload` Int64 COMMENT '是否上传价格到平台,默认0是没有，1是上传到平台', 2 上传失败  3上传中
    sql0 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_lazada_auto_modify_price
        where upload_time>{yesterday_time_stamp} and upload_time <= {today_time_stamp} and is_upload=0 and create_by in ('W01596', 'TT202655')
        """
    df = conn_mx.ck_select_to_df(sql0)
    failed_num = df['data'][0]

    sql1 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_lazada_auto_modify_price
        where upload_time>{yesterday_time_stamp} and upload_time <= {today_time_stamp} and is_upload=1 and create_by in ('W01596', 'TT202655')
    """
    df = conn_mx.ck_select_to_df(sql1)
    success_num = df['data'][0]
    #print(success_num)

    sql2 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_lazada_auto_modify_price
        where upload_time>{yesterday_time_stamp} and upload_time <= {today_time_stamp} and is_upload=2 and create_by in ('W01596', 'TT202655')
    """
    df = conn_mx.ck_select_to_df(sql2)
    error_num = df['data'][0]
    #print(to_be_updated_num)

    sql3 = f"""
        select count(distinct account_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_lazada_auto_modify_price
        where upload_time>{yesterday_time_stamp} and upload_time <= {today_time_stamp} and is_upload=3 and create_by in ('W01596', 'TT202655')
        """
    df = conn_mx.ck_select_to_df(sql3)
    to_be_updated_num = df['data'][0]

    sql3 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='lazada' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql3)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    # error_num = max(all_num - success_num, 0)
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)

    # 记录
    adjust_post_success_monitor(conn_ck, out_date, 'LAZADA', '国内仓', all_num, all_data, success_num, to_be_updated_num,
                                error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)
    print(all_num, all_data, success_num, to_be_updated_num, error_num, failed_num, success_lpl1)
    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}lazada国内仓\n总上传链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n未上传链接{failed_num},\n执行失败链接{error_num},\n执行中{to_be_updated_num},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'lazada', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('lazada平台-erp调价', '国内仓lazada调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓lazada调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'lazada', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('lazada平台-erp调价', '国内仓lazada调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓lazada调价监控', ding_text)


def get_shopee_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'shopee', '国内仓')

    out_date_str = out_date.replace('-', '')
    # table_list = conn_mx.ck_show_tables(db_name='support_document')
    # table1 = f'shopee_clear_post_data_{out_date_str}'
    # table2 = f'shopee_increase_post_data_{out_date_str}'
    # table_list1 = [table1, table2]
    # #
    # sql_wal = """
    #         select 0 as account_id, 0 as item_id, '' as `系统SKU`
    #         """
    # for table in table_list1:
    #     if table in table_list:
    #         sql_wal = f"""
    #                 {sql_wal} 
    #                 union all 
    #                 select distinct account_id,item_id,`系统SKU` 
    #                 from support_document.{table} 
    #                 """

    sql = f"""
    select count(distinct account_id, item_id, seller_sku) as data
    from (
        select * from {org_group}_sale_center_listing_sync.{org_group}_shopee_update_discount 
        where source = 2 and is_mulit in (0,1,2)  and 
        create_time>'{yesterday_time}' and create_time <= '{today_time}' and create_by in ('W01596', 'TT202655')
    ) a 
    inner join (
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}') 
        union all 
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}')         
    ) b 
    on a.account_id=b.account_id and a.item_id=b.item_id and a.sku=b.`系统SKU`
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
    select count(distinct account_id, item_id, seller_sku) as data
    from (
        select * from {org_group}_sale_center_listing_sync.{org_group}_shopee_update_discount 
        where status=4 and source = 2 and check_status in(0,1,2) and is_mulit in (0,2)  and
        create_time>'{yesterday_time}' and create_time <= '{today_time}' and create_by in ('W01596', 'TT202655')
    ) a 
    inner join (
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}') 
        union all 
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.account_id=b.account_id and a.item_id=b.item_id and a.sku=b.`系统SKU`
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
    select count(distinct account_id, item_id, seller_sku) as data
    from (
        select * from {org_group}_sale_center_listing_sync.{org_group}_shopee_update_discount 
        where status=0 and source = 2  and check_status in(0,1,2) and is_mulit in (0,2)  and 
        create_time>'{yesterday_time}' and create_time <= '{today_time}' and create_by in ('W01596', 'TT202655')
    ) a 
    inner join (
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}') 
        union all 
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.account_id=b.account_id and a.item_id=b.item_id and a.sku=b.`系统SKU`
    """
    df = conn_mx.ck_select_to_df(sql3)
    failed_num = df['data'][0]
    #print(failed_num)

    sql4 = f"""
    select count(distinct account_id, item_id, seller_sku) as data
    from (
        select * from {org_group}_sale_center_listing_sync.{org_group}_shopee_update_discount 
        where status=1 and source = 2  and check_status in(0,1,2) and is_mulit in (0,2)  and 
        create_time>'{yesterday_time}' and create_time <= '{today_time}' and create_by in ('W01596', 'TT202655')
    ) a 
    inner join (
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}') 
        union all 
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.account_id=b.account_id and a.item_id=b.item_id and a.sku=b.`系统SKU`
    """
    df = conn_mx.ck_select_to_df(sql4)
    tocancel_num = df['data'][0]
    #print(tocancel_num)

    sql5 = f"""
    select count(distinct account_id, item_id, seller_sku) as data
    from (
        select * from {org_group}_sale_center_listing_sync.{org_group}_shopee_update_discount 
        where status=5 and source = 2 and check_status in(0,1,2) and is_mulit in (0,2) and 
        create_time>'{yesterday_time}' and create_time <= '{today_time}' and create_by in ('W01596', 'TT202655')
    ) a 
    inner join (
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}') 
        union all 
        select distinct account_id,item_id,sku as `系统SKU` 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_shopee_merge
        where toDate(move_date)=toDate('{out_date}')       
    ) b 
    on a.account_id=b.account_id and a.item_id=b.item_id and a.sku=b.`系统SKU`
    """
    df = conn_mx.ck_select_to_df(sql5)
    error_num = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='shopee' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'shopee', '国内仓', all_num, all_data, success_num, 0,
                                error_num, failed_num, tocancel_num, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}shopee国内仓,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n执行失败链接{error_num},\n未处理调价链接{failed_num},\n待取消调价链接{tocancel_num},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'shopee', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('shopee平台-erp调价', '国内仓shopee调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓shopee调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'shopee', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('shopee平台-erp调价', '国内仓shopee调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓shopee调价监控', ding_text)


def get_ali_china(out_date, yesterday_time, today, today_time, conn_ck, conn_mx, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'ali', '国内仓')

    sql = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    # print(all_data)

    sql2 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=0
    """
    df = conn_mx.ck_select_to_df(sql2)
    num0 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=1
    """
    df = conn_mx.ck_select_to_df(sql3)
    num1 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql3)
    num2 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=3
    """
    df = conn_mx.ck_select_to_df(sql3)
    num3 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=4
    """
    df = conn_mx.ck_select_to_df(sql3)
    num4 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=5
    """
    df = conn_mx.ck_select_to_df(sql3)
    num5 = df['data'][0]


    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='W01596'
        and sync_status=6
    """
    df = conn_mx.ck_select_to_df(sql3)
    num6 = df['data'][0]


    sql6 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='ali' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = num5 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'ali', '国内仓', all_num, all_data, num5, 0,
                                num3, num6, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and num5 > 0:
        a = num5 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}速卖通国内仓,\n总调价链接{all_num},\n上传成功链接{all_data},\n待处理{num0},\n处理中{num1},\n处理成功{num2},\n处理失败{num3},\n同步中{num4},\n同步成功{num5},\n同步失败{num6},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ali', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('速卖通-平台调价', '国内仓速卖通调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓速卖通调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ali', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('速卖通-平台调价', '国内仓速卖通调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓速卖通调价监控', ding_text)

def get_ali_oversea_wj(out_date, yesterday_time, today, today_time, conn_ck, conn_mx, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'ali', '海外仓')
    sql = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    # print(all_data)

    sql2 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=0
    """
    df = conn_mx.ck_select_to_df(sql2)
    num0 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=1
    """
    df = conn_mx.ck_select_to_df(sql3)
    num1 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql3)
    num2 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=3
    """
    df = conn_mx.ck_select_to_df(sql3)
    num3 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=4
    """
    df = conn_mx.ck_select_to_df(sql3)
    num4 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=5
    """
    df = conn_mx.ck_select_to_df(sql3)
    num5 = df['data'][0]

    sql3 = f"""
        select count(distinct product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_aliexpress_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by='209313'
        and sync_status=6
    """
    df = conn_mx.ck_select_to_df(sql3)
    num6 = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='ALI' and item like '%%海外仓%%' and org_group='{org_group}'
       	group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = num5 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'ali', '国内仓', all_num, all_data, num5, 0,
                                num3, num6, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)
    if df_2.shape[0] > 0 and num5 > 0:
        a = num5 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}速卖通海外仓,\n总调价链接{all_num},\n上传成功链接{all_data},\n待处理{num0},\n处理中{num1},\n处理成功{num2},\n处理失败{num3},\n同步中{num4},\n同步成功{num5},\n同步失败{num6},\n调价执行率{success_lpl}\n'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('速卖通-平台调价', '海外仓速卖通调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓速卖通调价监控', ding_text)


def get_ebay_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'ebay', '海外仓')
    sql = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='209313'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql2 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='209313' and sync_status=3
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]

    sql3 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by ='209313' and sync_status=1
    """
    df = conn_mx.ck_select_to_df(sql3)
    fail_pass_data = df['data'][0]

    sql4 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
            and create_by ='209313' and sync_status=4
    """
    df = conn_mx.ck_select_to_df(sql4)
    error_num = df['data'][0]

    sql5 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
            and create_by ='209313' and sync_status=0
    """
    df = conn_mx.ck_select_to_df(sql5)
    audit_data = df['data'][0]

    sql5 = f"""
        select count(distinct sku, item_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ebay_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
            and create_by ='209313' and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql5)
    num2 = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='EB' and item like '%%海外仓%%' and org_group='{org_group}'
       	group by platform
    """
    df_5 = conn_ck.ck_select_to_df(sql6)

    if df_5.shape[0] > 0:
        all_num = df_5["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'ebay', '海外仓', all_num, all_data, success_num, 0,
                                error_num, 0, 0, success_lpl1, audit_data, fail_pass_data, 0, ti, org_group=org_group)

    if df_5.shape[0] > 0 and success_num > 0:
        a = success_num / all_data
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}ebay海外仓\n上传调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接{success_num},\n待审核链接{audit_data},\n执行中链接{num2},\n执行失败链接{error_num},\n审核不通过链接{fail_pass_data},\n调价执行率{success_lpl}'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('EBAY调价LISTING问题沟通群', '海外仓ebay调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓ebay调价监控', ding_text)


def get_cd_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'cd', '海外仓', 1)
    sql = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and type=2
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql2 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=2 and type=2
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]

    sql3 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=3 and type=2
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]

    sql4 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=1 and type=2
    """
    df = conn_mx.ck_select_to_df(sql4)
    updating_data = df['data'][0]

    sql5 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=0 and type=2
    """
    df = conn_mx.ck_select_to_df(sql5)
    to_be_updated_num = df['data'][0]

    sql5 = f"""
        select count(distinct account_id,item_id,sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_cdiscount_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}'  
        and create_by in ('209313') and status=4 and type=2
    """
    df = conn_mx.ck_select_to_df(sql5)
    num4 = df['data'][0]

    sql6 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='CD' and item like '%%海外仓%%' and org_group='{org_group}'
       	group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql6)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'cd', '海外仓', all_num, all_data, success_num, to_be_updated_num,
                                error_num, 0, 0, success_lpl1, 0, 0, updating_data, ti, org_group=org_group)

    if df_2.shape[0] > 0 and all_data > 0:
        a = success_num / all_data
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}cd海外仓清仓加快动销调价\n上传调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接{success_num},\n执行失败链接{error_num},\n上传中{updating_data},\n待上传{to_be_updated_num},\n手动取消{num4},\n调价执行率{success_lpl}\n'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('CD-调价群', '海外仓cd调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓cd调价监控', ding_text)


def get_walmart_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'walmart', '海外仓')
    sql = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313'
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    #print(all_data)

    sql2 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=3
    """
    df = conn_mx.ck_select_to_df(sql2)
    success_num = df['data'][0]
    #print(success_num)

    sql3 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=2
    """
    df = conn_mx.ck_select_to_df(sql3)
    error_num = df['data'][0]
    #print(error_num)

    sql4 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=1
    """
    df = conn_mx.ck_select_to_df(sql4)
    failed_num = df['data'][0]

    sql4 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=0
    """
    df = conn_mx.ck_select_to_df(sql4)
    not_implement_num = df['data'][0]

    sql4 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=4
     """
    df = conn_mx.ck_select_to_df(sql4)
    num4 = df['data'][0]

    sql4 = f"""
        select count(distinct erp_id, seller_sku) as data
        from {org_group}_sale_center_listing_sync.{org_group}_walmart_promotion_details 
        where create_time>'{yesterday_time}' and create_time<='{today_time}' 
        and create_user_id = '209313' and upload_status=5
     """
    df = conn_mx.ck_select_to_df(sql4)
    num5 = df['data'][0]


    sql5 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='WALMART' and item like '%%海外仓%%' and org_group='{org_group}'
       	group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)
    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'walmart', '海外仓', all_num, all_data, success_num, 0,
                                error_num, failed_num, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    if df_2.shape[0] > 0 and success_num > 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}海外仓walmart加快动销调价,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{success_num},\n未执行{not_implement_num},\n执行失败链接{error_num},\n执行中链接{failed_num},\n需要再次上传{num4},\n不需要上传{num5},\n调价执行率{success_lpl}\n'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('walmart平台调价', '海外仓walmart调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓walmart调价监控', ding_text)


def get_amazon_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'amazon', '海外仓')
    sql = f"""
        select count(distinct account_id,seller_sku) as num 
        from {org_group}_product_kd_sync.{org_group}_price_adjust_log_month 
        where creator in ('白果', '王杰W02735') and created_at >'{yesterday_time}' and created_at <='{today_time}'
        """
    df = conn_mx.ck_select_to_df(sql)
    success_num = df['num'][0]

    sql2 = f"""
        select platform,sum(number) as number 
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='AMAZON' and item like '%%海外仓%%' and org_group='{org_group}'
       	group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql2)
    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = success_num / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'AMAZON', '海外仓', all_num, 0, success_num, 0,
                                0, 0, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    if df_2.shape[0] > 0 and success_num >= 0:
        a = success_num / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}海外仓AMAZON清仓加快动销调价链接{all_num},执行成功链接数{success_num},调价执行率{success_lpl}'
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('亚马逊调价成功率', '海外仓amazon调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '海外仓amazon调价监控', ding_text)

def get_allegro_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'allegro', '国内仓')

    out_date1 = out_date.replace('-', '')


    sql = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601')
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')               
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    # print(all_data)


    sql2 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601') and sync_status=0
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql2)
    num0 = df['data'][0]
    # print(num_0)

    sql3 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601') and sync_status=1
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql3)
    num1 = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601') and sync_status=2
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql4)
    num2 = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601') and sync_status=3
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql4)
    num3 = df['data'][0]

    sql5 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT13601') and sync_status=4
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_clear_adjustment_allegro_merge  
        where toDate(move_date)=toDate('{out_date}')   
        union all 
        select distinct account_id,offer_id 
        from {org_group}_price_dom.{org_group}_domestic_warehouse_increase_adjustment_allegro_merge 
        where toDate(move_date)=toDate('{out_date}')     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql5)
    num4 = df['data'][0]


    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='allegro' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = num2 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'allegro', '国内仓', all_num, all_data, num2, 0,
                                num3, num4, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and num2 > 0:
        a = num2 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}国内仓allegro,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{num2},\n待执行{num0},\n执行中{num1},\n执行失败链接{num3},\n取消{num4},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'allegro', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('ALLEGRO平台调价通知', '国内仓allegro调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓allegro调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'allegro', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('ALLEGRO平台调价通知', '国内仓allegro调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓allegro调价监控', ding_text)

def get_allegro_fbal(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'allegro', 'FBAL')

    out_date1 = out_date.replace('-', '')

    sql = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596'
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'        
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]
    # print(all_data)


    sql2 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596' and sync_status=0
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql2)
    num0 = df['data'][0]
    # print(num_0)

    sql3 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596' and sync_status=1
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql3)
    num1 = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596' and sync_status=2
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql4)
    num2 = df['data'][0]

    sql4 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596' and sync_status=3
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql4)
    num3 = df['data'][0]

    sql5 = f"""
    select count(distinct a.account_id, a.item_id) as data
    from (
        select distinct account_id, item_id 
        from {org_group}_sale_center_listing_sync.{org_group}_allegro_price_adjustment 
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by = 'W01596' and sync_status=4
    ) a 
    inner join (
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_price_adjust_merge
        where adjust_date='{out_date}'
        union all
        select distinct account_id,offer_id 
        from fba_ag_over_180_days_price_adjustment.fbal_fbd_price_adjust_merge
        where adjust_date='{out_date}'     
    ) b 
    on a.account_id=b.account_id and a.item_id=b.offer_id
    """
    df = conn_mx.ck_select_to_df(sql5)
    num4 = df['data'][0]

    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='allegro' and item like '%%FBAL%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = num2 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'allegro', 'FBAL', all_num, all_data, num2, 0,
                                num3, num4, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and num2 > 0:
        a = num2 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}allegro-FBAL,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{num2},\n待执行{num0},\n执行中{num1},\n执行失败链接{num3},\n取消{num4},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'allegro', 'FBAL', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('ALLEGRO平台调价通知', 'allegro-FBAL调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', 'allegro-FBAL调价监控', ding_text)
                dd_jk = 1


    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'allegro', 'FBAL', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('ALLEGRO平台调价通知', 'allegro-FBAL调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', 'allegro-FBAL调价监控', ding_text)

def get_ozon_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='yibai'):
    if ti == '':
        yesterday_time = get_com_time(out_date, conn_ck, yesterday_time, 'ozon', '国内仓')

    sql = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' and create_by in ('W01596', 'TT202655')
    """
    df = conn_mx.ck_select_to_df(sql)
    all_data = df['data'][0]

    sql1 = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}'
        and create_by in ('W01596', 'TT202655') and sync_status=0
    """
    df = conn_mx.ck_select_to_df(sql1)
    num0 = df['data'][0]


    sql2 = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT202655') and sync_status=1
    """
    df = conn_mx.ck_select_to_df(sql2)
    num1 = df['data'][0]

    sql3 = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT202655') and sync_status=2
    """
    df = conn_mx.ck_select_to_df(sql3)
    num2 = df['data'][0]

    sql4 = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT202655') and sync_status=3
    """
    df = conn_mx.ck_select_to_df(sql4)
    num3 = df['data'][0]

    sql4 = f"""
        select count(distinct account_id, product_id) as data
        from {org_group}_sale_center_listing_sync.{org_group}_ozon_price_adjustment
        where create_at>'{yesterday_time}' and create_at<='{today_time}' 
        and create_by in ('W01596', 'TT202655') and sync_status=4
    """
    df = conn_mx.ck_select_to_df(sql4)
    num4 = df['data'][0]

    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='ozon' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    sql5 = f"""
        select platform,sum(number) as number
        from fba_inventory.adjust_post_number
        where record='{out_date}' and platform='ozon' and item like '%%国内仓%%' and org_group='{org_group}'
        group by platform
    """
    df_2 = conn_ck.ck_select_to_df(sql5)

    if df_2.shape[0] > 0:
        all_num = df_2["number"][0]
    else:
        all_num = 0
    if all_num == 0:
        success_lpl1 = 0
    else:
        success_lpl1 = num2 / all_num
    success_lpl = '%.2f%%' % (success_lpl1 * 100)
    adjust_post_success_monitor(conn_ck, out_date, 'ozon', '国内仓', all_num, all_data, num2, 0,
                                num3, num4, 0, success_lpl1, 0, 0, 0, ti, org_group=org_group)

    dd_jk = 0
    if df_2.shape[0] > 0 and num2 > 0:
        a = num2 / all_num
        if a > 1 or a < 0:
            print('a: ', a)
        else:
            if i == 0 and ti == '':
                ding_text = f'{org_group}:{out_date}国内仓ozon,\n总调价链接{all_num},\n上传成功链接{all_data},\n执行成功链接数{num2},\n待执行{num0},\n执行中{num1},\n执行失败链接{num3},\n取消{num4},\n调价执行率{success_lpl}\n'
                ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ozon', '国内仓', org_group=org_group)
                if org_group == 'yibai' and len(ding_text) >= 10:
                    send_msg('OZON平台调价通知', '国内仓ozon调价监控', ding_text)
                if len(ding_text) >= 10:
                    send_msg('动销组定时任务推送', '国内仓ozon调价监控', ding_text)
                dd_jk = 1

    if i == 0 and ti == '' and dd_jk == 0:
        ding_text = f'{org_group}:'
        ding_text = price_adjust_data_ding(conn_ck, ding_text, 'ozon', '国内仓', org_group=org_group)
        if org_group == 'yibai' and len(ding_text) >= 10:
            send_msg('OZON平台调价通知', '国内仓ozon调价监控', ding_text)
        if len(ding_text) >= 10:
            send_msg('动销组定时任务推送', '国内仓ozon调价监控', ding_text)


def run_success_lpl(n=7):
    conn_mx = pd_to_ck(database='yibai_price_dom', data_sys='调价明细历史数据')
    conn_tt = pd_to_ck(database='tt_price_dom', data_sys='通拓-ck')
    conn_ck = pd_to_ck(database='fba_inventory', data_sys='调价明细历史数据')
    #
    # out_date, yesterday_time, today, today_time, yesterday_time_stamp, today_time_stamp = get_yesterday_time()

    #
    print('n:', n)
    #
    if n == 7:
        m = 0
    else:
        m = 1

    for i in range(m, n):
        print('i:', i)
        out_date = (datetime.date.today() - datetime.timedelta(days=i+1)).isoformat()
        yesterday_time = out_date + ' ' + '09:00:00'
        yesterday_time_array = time.strptime(yesterday_time, "%Y-%m-%d %H:%M:%S")
        yesterday_time_stamp = int(time.mktime(yesterday_time_array))

        today = (datetime.date.today() - datetime.timedelta(days=i)).isoformat()
        today_time = today + ' ' + '09:00:00'
        today_time_array = time.strptime(today_time, "%Y-%m-%d %H:%M:%S")
        today_time_stamp = int(time.mktime(today_time_array))

        for ti in ['', '1']:
            # 20260128 钉钉每分钟发的消息数有限制
            if i == 0 and ti == '':
                sleep_time = 30
            else:
                sleep_time = 1
            get_amazon_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_amazon_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_ebay_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_ebay_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_ali_china(out_date, yesterday_time, today, today_time, conn_ck, conn_mx, i, ti)
            get_ali_oversea_wj(out_date, yesterday_time, today, today_time, conn_ck, conn_mx, i, ti)
            time.sleep(sleep_time)
            get_wish_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_wish_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_cd_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_cd_FBC(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_cd_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            time.sleep(sleep_time)
            get_walmart_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_walmart_wfs(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_walmart_oversea_wj(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_lazada_china(out_date, yesterday_time_stamp, today, today_time_stamp, conn_mx, conn_ck, i, ti)
            get_shopee_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            time.sleep(sleep_time)
            get_daraz_china(out_date, yesterday_time_stamp, today, today_time_stamp, conn_mx, conn_ck, i, ti)
            get_allegro_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_allegro_fbal(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            get_ozon_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti)
            time.sleep(sleep_time)
            # tt 
            get_amazon_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            get_ebay_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            get_ali_china(out_date, yesterday_time, today, today_time, conn_ck, conn_tt, i, ti, org_group='tt')
            get_walmart_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            get_lazada_china(out_date, yesterday_time_stamp, today, today_time_stamp, conn_tt, conn_ck, i, ti, org_group='tt')
            time.sleep(sleep_time)
            get_shopee_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            get_allegro_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            get_ozon_china(out_date, yesterday_time, today, today_time, conn_tt, conn_ck, i, ti, org_group='tt')
            # mz
            get_amazon_china(out_date, yesterday_time, today, today_time, conn_mx, conn_ck, i, ti, org_group='mz')
    # 把整行为0的数据删掉
    for ti in ['', '1']:
        sql = f"SELECT * except(insert_time) FROM fba_inventory.adjust_post_success_monitor{ti}"
        df = conn_ck.ck_select_to_df(sql)
        col_list = list(df.columns)
        df['汇总'] = 0
        for col in col_list:
            if col not in ['date', 'plat', 'item', 'org_group']:
                df['汇总'] = df['汇总'] + df[col]
        df = df[df['汇总'] == 0]
        df = df[['date', 'plat', 'item', 'org_group']].drop_duplicates()
        if df.shape[0] > 0:
            df = df.reset_index(drop=True)
            for i in range(df.shape[0]):
                date1 = df['date'][i]
                plat = df['plat'][i]
                item = df['item'][i]
                org_group = df['org_group'][i]
                sql = f"""
                alter table fba_inventory.adjust_post_success_monitor{ti} delete
                where date='{date1}' and plat='{plat}' and item='{item}' and org_group='{org_group}'
                """
                conn_ck.ck_execute_sql(sql)


if __name__ == '__main__':
    run_success_lpl()
