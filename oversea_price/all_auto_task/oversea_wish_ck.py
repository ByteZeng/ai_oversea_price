import datetime
import traceback
from pulic_func.robot import ding
from all_auto_task.oversea_wish_upload import oversea_wish_price_post
from all_auto_task.scripts_ck_client import CkClient
import pandas as pd
from sqlalchemy import create_engine
import requests
from tqdm import tqdm
import json
import time
import os
import shutil
import numpy as np
# from pulic_func.base_api.table_info_ck_205 import dag_trigger_monitor
from pulic_func.base_api.adjust_price_function import ck_tongbu_jiankong
from pulic_func.base_api.upload_zip import upload_zip
from pulic_func.base_api.mysql_connect import sql_to_pd, pd_to_ck
from all_auto_task.oversea_listing_detail_2023 import write_to_ck

def get_wish_listing(ck_client):
    sql = """
    insert into yibai_oversea.yibai_wish_over_sea_listing
    with wish_express_fee as (
        select distinct a.country_code as country_code,a.product_id as product_id,
            b.standard_fee as standard_fee,b.standard_status as standard_status,
            c.we_fee as we_fee,c.we_status as we_status
        from (
            SELECT distinct country_code,product_id 
            from yibai_sale_center_listing_sync.yibai_wish_listing_warehouse_shippings
        ) a 
        left join (
            SELECT distinct country_code,product_id,toFloat64(price) as standard_fee,status as standard_status
            from yibai_sale_center_listing_sync.yibai_wish_listing_warehouse_shippings
            where warehouse_id in (
                select distinct warehouse_id from yibai_sale_center_listing_sync.yibai_wish_account_warehouse 
                where shipping_type IN ('STANDARD')
            )
        ) b 
        on a.country_code=b.country_code and a.product_id=b.product_id
        left join (
            SELECT distinct country_code,product_id,toFloat64(price) as we_fee,status as we_status
            from yibai_sale_center_listing_sync.yibai_wish_listing_warehouse_shippings
            where warehouse_id in (
                select distinct warehouse_id from yibai_sale_center_listing_sync.yibai_wish_account_warehouse 
                where shipping_type not IN ('STANDARD')
            )
        ) c 
        on a.country_code=c.country_code and a.product_id=c.product_id
    )  
    
    SELECT DISTINCT A.*,if(empty(B.product_id), '修改WE及直发运费', '只修改WE运费') as `是否修改运费`
    FROM (
        SELECT c.account_id as account_id,a.parent_seller_sku as parent_seller_sku, seller_sku, 
            c.account_name as account_name, a.product_id as product_id, parent_sku as `父体SKU`, a.sku as sku,
            multiIf(
                subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'], subString(a.sku, 4),
                subString(a.sku, 1, 2) in ['DE', 'GB'], subString(a.sku, 3),
                subString(a.sku, -2) in ['DE', 'GB'],  subString(a.sku, 1, -2),
            a.sku) AS `子SKU`,
            if(b.we_status = 1, '海外仓发货', '国内仓发货') as `是否海外发货`,
            a.price as `线上价格`, b.standard_fee as `直发运费`, 
            a.price + b.standard_fee as `直发当前价格`,
            if(b.standard_fee = 0, 9999, GREATEST(70, b.standard_fee)) as `直发运费最高涨价值`,
            if(b.country_code = 'GB', 'UK', b.country_code) AS country_code,
            if(b.we_fee = 0, 0.01, b.we_fee) AS `WE发货运费`, 
            a.price + b.we_fee as `WE当前价格`,
            if(b.we_fee = 0, 9999, GREATEST(70, b.we_fee)) as `WE运费最高涨价值`, 
            concat('Express-', b.country_code) AS country_code_post, a.shipping_warehouse as warehouse_id
        FROM (
            select * from wish_express_fee
            where we_status = 1 and we_fee <> 0
        ) b
        JOIN (
            select account_id,product_id,sku,parent_sku,seller_sku,
            seller_parent_sku as parent_seller_sku,toFloat64(seller_price) as price,
            product_is_multi,seller_created_at as upload_time,upload_type,shipping_warehouse
            from yibai_sale_center_listing_sync.yibai_wish_listing 
            where status = 1 and sku <> '' and price <> 0
        ) a 
        ON a.product_id = b.product_id
        JOIN (
            select id,account_id,account_name 
            from yibai_sale_center_system_sync.yibai_system_account
            where platform_code = 'WISH' and is_del=0 and `status`=1
        ) c 
        ON a.account_id = c.id
    ) A
    LEFT JOIN (
        SELECT c.account_id as account_id, a.parent_seller_sku as parent_seller_sku, seller_sku, 
            c.account_name as account_name, a.product_id as product_id, parent_sku as `父体SKU`, 
            a.sku as sku,
            multiIf(
                subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'], subString(a.sku, 4),
                subString(a.sku, 1, 2) in ['DE', 'GB'], subString(a.sku, 3),
                subString(a.sku, -2) in ['DE', 'GB'], subString(a.sku, 1, -2),
            a.sku) AS `子SKU`,
            if(b.we_status = 1, '海外仓发货', '国内仓发货') as `是否海外发货`, 
            a.price as `线上价格`, b.standard_fee as `直发运费`, 
            a.price + b.standard_fee as `直发当前价格`,
            if(b.standard_fee = 0, 9999, GREATEST(70, b.standard_fee)) as `直发运费最高涨价值`,
            if(b.country_code = 'GB', 'UK', b.country_code) AS country_code,
            if(b.we_fee = 0, 0.01, b.we_fee) AS `WE发货运费`, 
            a.price + b.we_fee as `WE当前价格`,
            if(b.we_fee = 0, 9999, GREATEST(70, b.we_fee)) as `WE运费最高涨价值`, 
            CONCAT('Express-', b.country_code) AS country_code_post
        FROM (
            select * from wish_express_fee
            where we_status <> 1 and standard_status = 1 and country_code = 'KR'
        ) b
        JOIN (
            select account_id,product_id,sku,parent_sku,seller_sku,
            seller_parent_sku as parent_seller_sku,toFloat64(seller_price) as price,
            product_is_multi,seller_created_at as upload_time,upload_type
            from yibai_sale_center_listing_sync.yibai_wish_listing 
            where status = 1 and sku <> '' and price <> 0
        ) a 
        ON a.product_id = b.product_id
        JOIN (
            select id,account_id,account_name 
            from yibai_sale_center_system_sync.yibai_system_account
            where platform_code = 'WISH' and is_del=0 and `status`=1
        ) c 
        ON a.account_id = c.id
    ) B 
    ON A.account_id = B.account_id and A.product_id = B.product_id 
    settings max_memory_usage = 200000000000 
    """
    ck_client.ck_execute_sql(sql='truncate table yibai_oversea.yibai_wish_over_sea_listing')
    ck_client.ck_execute_sql(sql)
    res_cnt = ck_client.ck_get_table_data_numbers(sheet_name='yibai_wish_over_sea_listing', database_name='yibai_oversea')
    # if res_cnt < 30000000:
    # 20231208 wish平台近期大量下架listing中
    if res_cnt < 100:
        print(f'yibai_wish_over_sea_listing表数据量为{res_cnt}, 小于预期,请检查！')
        return -1
    return 1


def over_sea_wish_result(ck_client):
    sql = """insert into yibai_oversea.yibai_wish_oversea
             select
                a.*,
                b.best_warehouse_name,
                b.ship_name,
                total_cost,
                0 as shippingCost,
                0 as firstCarrierCost,
                b.country as country,
                GREATEST(cast((b.new_price + total_cost) as decimal(22,4))/(1-wish_standed-paypal_fee-vat_fee-extra_fee-platform_zero-ifnull(target_profit_rate,0)), b.lowest_price) as "直发要求价格",
                b.price as "WE要求价格",
                case when a.warehouse_id in ('3', '6') and (b.sku, b.country) in 
                          (select sku,country_code from yibai_oversea.yibai_wish_virtual_warehouse_sku)  
                     then 0
                     when (a.sku, a.product_id) in (select sku, product_id from yibai_wish.wish_listing_white_list)
                     then 0
                     else 
                     LEAST(`WE运费最高涨价值`, b.price-`WE当前价格`) 
                 end AS "WE运费调整",
                LEAST(`直发运费最高涨价值`, GREATEST(cast((b.new_price + total_cost) as decimal(22,4)) /(1-wish_standed-paypal_fee-vat_fee-extra_fee-platform_zero-ifnull(target_profit_rate,0)), lowest_price)-`直发当前价格`) AS "直发运费调整"
            from
                yibai_oversea.yibai_wish_over_sea_listing a
            inner join (
                select
                    a.* ,
                     case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                         subString(a.sku, 4)
                    when subString(a.sku, 1, 2) in ['DE', 'GB'] then 
                         subString(a.sku, 3)
                    when subString(a.sku, -2) in ['DE', 'GB'] then 
                         subString(a.sku, 1, -2)
                    else 
                         a.sku
                    end AS son_sku
                from
                    yibai_oversea.dwm_oversea_price_dtl_temp a
                where
                    platform = 'WISH'
                    and available_stock >= 5
                    and (country, best_warehouse_id) in
                          (select
                          		if(country_code='GB', 'UK', country_code),
                          		warehouse_id
                             from
                             yibai_wish.yibai_erp_warehouse_conf
                          )
                order by
                    total_cost asc
                limit 1 by son_sku, country ) b ON
                a.country_code = b.country
                and a.`子SKU` = b.son_sku
              inner join
              (select * from yibai_platform_fee where platform = 'WISH') c on
              b.country = c.site
              where `WE运费调整`>0
              settings max_memory_usage = 200000000000"""
    ck_client.ck_execute_sql(sql='truncate table yibai_oversea.yibai_wish_oversea')
    ck_client.ck_execute_sql(sql)
    # 20230517 对白名单链接单独进行调价，定价公式采用非动销的公式（不看库存、平台最低净利率的定价公式）
    sql = """
        insert into yibai_oversea.yibai_wish_oversea
        select
            a.*, b.best_warehouse_name, b.ship_name,total_cost, 0 as shippingCost, 0 as firstCarrierCost, b.country as country,
            GREATEST(cast((b.new_price + total_cost) as decimal(22,4))/(1-wish_standed-paypal_fee-vat_fee-extra_fee-platform_zero - platform_must_percent), b.lowest_price) as "直发要求价格",
            GREATEST(cast((b.new_price + total_cost) as decimal(22,4))/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero - platform_must_percent), b.lowest_price) as "WE要求价格",
            case when a.warehouse_id in ('3', '6') and (b.sku, b.country) in (select sku,country_code from yibai_oversea.yibai_wish_virtual_warehouse_sku)
            then 0
            else
            LEAST(`WE运费最高涨价值`, GREATEST(cast((b.new_price + total_cost) as decimal(22,4))/(1-pay_fee-paypal_fee-vat_fee-extra_fee-platform_zero - platform_must_percent), b.lowest_price)-`WE当前价格`)
            end AS "WE运费调整",
            LEAST(`直发运费最高涨价值`, GREATEST(cast((b.new_price + total_cost) as decimal(22,4)) /(1-wish_standed-paypal_fee-vat_fee-extra_fee-platform_zero-ifnull(target_profit_rate,0)), lowest_price)-`直发当前价格`) AS "直发运费调整"
        from
            yibai_oversea.yibai_wish_over_sea_listing a
        LEFT join (
            select
                a.* ,
                case when subString(a.sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then subString(a.sku, 4)
                when subString(a.sku, 1, 2) in ['DE', 'GB'] then subString(a.sku, 3)
                when subString(a.sku, -2) in ['DE', 'GB'] then subString(a.sku, 1, -2)
                else a.sku end AS son_sku
            from
                yibai_oversea.dwm_oversea_price_dtl_temp a
            where
                platform = 'WISH'
                and (country, best_warehouse_id) in
                (select
                    if(country_code='GB', 'UK', country_code),  warehouse_id
                 from yibai_wish.yibai_erp_warehouse_conf)
            order by total_cost asc
            limit 1 by son_sku, country
            ) b ON a.country_code = b.country and a.`子SKU` = b.son_sku
        inner join (select * from yibai_wish.yibai_platform_fee where platform = 'WISH') c on b.country = c.site
        inner join yibai_wish.wish_listing_white_list w ON a.sku = w.sku and a.product_id = w.product_id
        where `WE运费调整`>0
        settings max_memory_usage = 200000000000
    """
    # ck_client.ck_execute_sql(sql='truncate table yibai_oversea.yibai_wish_oversea')
    ck_client.ck_execute_sql(sql)
    # 20240425 历史数据备份
    sql = """
        SELECT
            *
        FROM yibai_oversea.yibai_wish_oversea
    """
    df = ck_client.ck_select_to_df(sql)
    df['date_id'] = time.strftime('%Y-%m-%d')
    write_to_ck(df, 'oversea_wish_listing_upload_temp')
    res_cnt = ck_client.ck_get_table_data_numbers(sheet_name='yibai_wish_oversea', database_name='yibai_oversea')
    # if res_cnt < 11000000:
    if res_cnt < 10:
        print(f'yibai_wish_oversea表数据量为{res_cnt}, 小于预期,请检查！')
        return -1
    print('yibai_wish_oversea表已更新')
    return 1


def over_sea_wish_pivot_result(ck_client):
    sql1 = """insert into yibai_wish_pivot_result
                select 
                a.account_id,
                account_name,
                parent_seller_sku as "Parent Unique ID",
                'CNY' as "Local Currency Code",
                country_code_post as "Warehouse Name",
                arrayMap(x->splitByChar('-',x)[2], b.arr) as column_arr,
                arrayMap(x->if(x==country_code_post, price, NULL), b.arr) as val_arr,
                direct_trans_fee_final
                from 
                (
                    with 
                    max("WE发货运费最终") as we_send_fee_final_mean,
                    groupArray((we_trans_fee_adjust, "WE发货运费最终")) as we_send_fee_final_arr,
                    groupArray(("是否修改运费", line_trans_fee_adjust, "直发运费最终")) as standard_trans_fee_final_arr,
                    arrayMap(x->if(x.1==-1, NULL, we_send_fee_final_mean), we_send_fee_final_arr) as res_arr,
                    arrayReduce('max', res_arr) as trans_price,
                    arrayMap(x->multiIf(x.2==-1, NULL,and(x.1=='修改WE及直发运费',x.2<>-1), x.3, we_send_fee_final_mean), standard_trans_fee_final_arr) as direct_trans_fee_arr,
                    arrayReduce('max', direct_trans_fee_arr) as direct_trans_fee
                    select 
                        account_id ,
                        parent_seller_sku,
                        account_name,
                        country_code_post,
                        cast(multiIf(trans_price>1, round(trans_price,2), isNull(trans_price), NULL, 1) as Nullable(Float64)) as price,
                        cast(multiIf(direct_trans_fee>1, round(direct_trans_fee,2), isNull(direct_trans_fee), NULL, 1) as Nullable(Float64)) as direct_trans_fee_final
                    from 
                    (
                    select
                        account_id,
                        parent_seller_sku,
                        account_name,
                        "是否修改运费",
                        if("是否修改运费"='只修改WE运费', -1, "直发运费调整") as line_trans_fee_adjust,
                        country_code_post,
                        "WE运费调整" as we_trans_fee_adjust,
                        toDecimal32OrZero("WE发货运费",4) as we_send_fee,
                        toDecimal32OrZero("直发运费", 4) as line_trans_fee,
                        case when line_trans_fee_adjust = -1 or line_trans_fee = 0 then NULL 
                             when line_trans_fee_adjust <> -1 and  line_trans_fee <> 0 then line_trans_fee + line_trans_fee_adjust
                         end as "直发运费最终",
                        case when we_send_fee = 0 then NULL 
                             when we_send_fee <> 0 then we_send_fee + we_trans_fee_adjust
                         end as "WE发货运费最终"
                    from
                        yibai_wish_oversea
                    )
                    group by 
                        account_id ,
                        parent_seller_sku,
                        account_name,
                        country_code_post
                ) a
                join
                (select
                    account_id,
                    arraySort(arrayDistinct(groupArray(country_code_post))) as arr
                from
                    yibai_wish_oversea
                group by
                    account_id) b 	
                on (a.account_id = b.account_id)
                settings max_memory_usage = 200000000000 """
    sql2 = """insert into yibai_wish_pivot_result
                with 
                arrayMap(x->if(indexOf(res_arr.1, x) > 0, (res_arr.2)[indexOf(res_arr.1, x)], NULL),  b.arr) as tmp_res
                select 
                    account_id,
                    account_name,
                    `Parent Unique ID`,
                    'CNY',
                    'STANDARD',
                    arrayMap(x->splitByChar('-', x)[2], b.arr),
                    tmp_res,
                    null
                from 
                (
                    select
                        account_id,
                        account_name,
                        `Parent Unique ID`,
                        arraySort(x->x.1, groupArray((`Warehouse Name`, direct_trans_fee))) as res_arr
                    from
                     yibai_wish_pivot_result
                     group by
                        account_id,
                        account_name,
                        `Parent Unique ID`
                ) a 
                inner join 
                (
                    select
                        account_id, 
                        arraySort(arrayDistinct(groupArray(country_code_post))) as arr
                    from
                        yibai_wish_oversea
                    group by
                        account_id 
                ) b 
                on (a.account_id = b.account_id) """
    ck_client.ck_execute_sql(sql='truncate table yibai_wish_pivot_result')
    ck_client.ck_execute_sql(sql1)
    ck_client.ck_execute_sql(sql2)
    res_cnt = ck_client.ck_get_table_data_numbers('yibai_wish_pivot_result')
    # if res_cnt < 14000000:
    #     print(f'yibai_wish_pivot_result表数据量为{res_cnt}, 小于预期,请检查！')
    #     return -1
    return 1


def get_now_weekday():
    day_of_week = datetime.datetime.now().isoweekday()
    return day_of_week




def get_sku_stock_ods(ck_client):
    ck_client.ck_execute_sql(sql='truncate table yibai_wish.yibai_warehouse_sku_stock_ods')
    sql="""
            select  
          a.sku as sku,
          cast(sum(a.available_qty) as Int) as available_stock 
        from
          yibai_plan_stock.yibai_warehouse_sku_stock_ods a
        where
              update_date >= concat(CURRENT_DATE(), " 00:00:00")  
          and available_qty >0
          and exists ( select 1 from yibai_warehouse.yibai_warehouse b 
                        where a.warehouse_code = b.warehouse_code and b.warehouse_type = 1 )
        group by a.sku  
      --  having available_stock > 3  
    """
    df = sql_to_pd(database='yibai_plan_stock', sql=sql, data_sys='数据管理部同步服务器')
    print(df.head(100))
    ck_client.write_to_ck_json_type('yibai_wish.yibai_warehouse_sku_stock_ods', df)
    res_cnt = ck_client.ck_get_table_data_numbers(sheet_name='yibai_warehouse_sku_stock_ods',
                                                  database_name='yibai_wish')
    if res_cnt < 100000:
        print(f'yibai_warehouse_sku_stock_ods表数据量为{res_cnt}, 小于预期,请检查！')
        return -1
    return 1


def get_erp_warehouse_conf():
    url = f'http://tmsservice.yibainetwork.com:94/ordersys/api/logisticsOrderRule/getWarehouseRulesList'
    print(url)
    data = {'sale_platforms': 'WISH'}
    res = requests.post(url=url, data=data)
    # res = requests.post(url=url, json=json.dumps(data))
    if res.status_code != 200:
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}:{res.content.decode('utf-8')}, 发货仓对应配置接口获取数据失败")
    res_data = res.json()

    insert_df = pd.DataFrame(res_data["data"]["list"].values())

    print(insert_df['sale_platforms'])

    cols = ','.join(map(lambda m_x: f"`{m_x}`", list(insert_df.columns)))
    data_dict_list = insert_df.to_dict('records')
    data_list = [['' if col is None else col if type(col) != dict else json.dumps(col, ensure_ascii=False) for col in
                  list(one_item.values())] for one_item in data_dict_list]

    group_limit = 1000
    insert_num = int(len(insert_df.index) / group_limit) + 1
    for one_num in range(insert_num):
        now_data_list = data_list[(one_num * group_limit): (one_num * group_limit + group_limit)]
        if now_data_list:
            try:
                ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78',
                                     port='9001',
                                     db_name='yibai_wish')
                ck_client.ck_execute_sql("truncate table yibai_wish.yibai_country_warehouse_rule_sync")
                sql = f"""INSERT INTO yibai_wish.yibai_country_warehouse_rule_sync ({cols}) VALUES {','.join([str(tuple(one_item)) for one_item in now_data_list])}"""
                print(sql)
                ck_client.ck_execute_sql(sql)
                ck_client.ck_execute_sql("truncate table yibai_wish.yibai_erp_warehouse_conf_new")
                sql = """insert into yibai_wish.yibai_erp_warehouse_conf_new
                            select 
                                country_code,
                                warehouse_type,
                                warehouse_tuple.1 as warehouse_id,
                                warehouse_tuple.2 as warehouse_name
                            from 
                            (
                                select arrayJoin(splitByChar(',', country_codes)) as country_code,
                                       '一级仓' as warehouse_type,
                                       arrayJoin(arrayMap(x->JSONExtract(x, 'Tuple(Int32, String)')
                                        ,JSONExtractKeysAndValuesRaw(first_warehouse).2)) as warehouse_tuple
                                from yibai_wish.yibai_country_warehouse_rule_sync 
                                where sale_platforms = 'WISH'
                                  and country_codes <> ''
                                  and use_status = '1'
                                union all 
                                select arrayJoin(splitByChar(',', country_codes)),
                                       '二级仓' as warehouse_type,
                                       arrayJoin(arrayMap(x->JSONExtract(x, 'Tuple(Int32, String)')
                                        ,JSONExtractKeysAndValuesRaw(second_warehouse).2)) as warehouse_tuple
                                from yibai_wish.yibai_country_warehouse_rule_sync 
                                where sale_platforms = 'WISH'
                                  and country_codes <> ''
                                  and use_status = '1'
                                union all 
                                select arrayJoin(splitByChar(',', country_codes)),
                                       '三级仓' as warehouse_type,
                                       arrayJoin(arrayMap(x->JSONExtract(x, 'Tuple(Int32, String)')
                                        ,JSONExtractKeysAndValuesRaw(third_warehouse).2)) as warehouse_tuple
                                from yibai_wish.yibai_country_warehouse_rule_sync 
                                where sale_platforms = 'WISH'
                                  and country_codes <> ''  
                                  and use_status = '1'
                            ) t 

                """
                ck_client.ck_execute_sql(sql)
            except:
                raise Exception(traceback.format_exc())

def wish_export_file(ck_client):
    if os.path.exists('/data'):
        f_path = '/data'
    else:
        f_path = os.path.dirname(os.path.abspath(__file__))  # 文件当前路径
    qu_shu_date_str = datetime.date.today().isoformat()
    f_path1 = os.path.join(f_path, f'oversea_wish{qu_shu_date_str}')
    wenjianjia = 'wish海外仓调价'
    today = qu_shu_date_str.replace('-', '')
    f_path_ZIP = os.path.join(f_path1, 'WISH', today, wenjianjia)
    if not os.path.exists(f_path_ZIP):
        os.makedirs(f_path_ZIP)

    df = ck_client.ck_select_to_df('select distinct account_id  from yibai_oversea.yibai_wish_oversea order by account_id')
    for account_id in tqdm(list(df['account_id']), desc='拆分明细数据'):
        # 直发
        sql1 = f"""
        with groupArrayIf((country_code,direct_shipping), direct_shipping is not null) as data_arr
                select
                    account_id, product_id ,
                    toString(arrayMap(x->map('code', x.1, 'new_shipping', toString(x.2)), data_arr)) as change
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
                        cast(multiIf(trans_price>1, round(trans_price,2), isNull(trans_price), Null, 1) as Nullable(Float64)) + rand()%100/100 as oversea_shipping,
                        cast(multiIf(direct_trans_fee>1, round(direct_trans_fee,2), isNull(direct_trans_fee), Null, 1) as Nullable(Float64)) + rand()%100/100 as direct_shipping
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
                    where account_id = {account_id}
                    )
                    group by
                        account_id ,
                        product_id,
                        country_code_post
                )
                group by account_id, product_id
                having length(data_arr) > 0
        """
        df1 = ck_client.ck_select_to_df(sql1)
        # 二级仓
        sql2 = f"""
                with groupArrayIf((country_code,oversea_shipping), oversea_shipping is not null) as data_arr
                select
                    account_id, product_id ,
                    toString(arrayMap(x->map('code', x.1, 'new_shipping', toString(x.2)), data_arr)) as change
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
                        cast(multiIf(trans_price>1, round(trans_price,2), isNull(trans_price), Null, 1) as Nullable(Float64))  + rand()%100/100 as oversea_shipping,
                        cast(multiIf(direct_trans_fee>1, round(direct_trans_fee,2), isNull(direct_trans_fee), Null, 1) as Nullable(Float64))  + rand()%100/100 as direct_shipping
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
                    where account_id = {account_id}
                    )
                    group by
                        account_id ,
                        product_id,
                        country_code_post
                )
                group by account_id, product_id
                having length(data_arr) > 0
                """
        df2 = ck_client.ck_select_to_df(sql2)
        # 明细
        sql3 = f"""
                select * from  yibai_oversea.yibai_wish_oversea
                where account_id = {account_id} order by product_id , country
                """
        df3 = ck_client.ck_select_to_df(sql3)
        #
        file_name = os.path.join(f_path_ZIP, f'wish海外仓调价明细-账号id{account_id}.xlsx')
        writer = pd.ExcelWriter(file_name)
        df1.to_excel(writer, sheet_name='直发', index=False)
        df2.to_excel(writer, sheet_name='二级仓', index=False)
        df3.to_excel(writer, sheet_name='明细', index=False)
        writer.save()
        writer.close()
    # 压缩上传到销售工作台
    upload_zip(f_path1, 'WISH')
    shutil.rmtree(f_path1)
    ding_robot1 = ding.DingRobot('WISH平台调价通知')
    ding_robot1.send_msg(text=f"""{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} wish海外仓计算完成，数据已上传到销售中台，请检查。""",
                        mobiles=['+86-15827466691', '+86-13296513752'],
                        is_all=False)

def yibai_wish_virtual_warehouse_sku():
    # 虚拟仓sku配置表获取，sku+country_code不调价
    sql = """
    select distinct sku,country_code from yibai_wish_virtual_warehouse_sku where status=1
    """
    df = sql_to_pd(database='yibai_wish', sql=sql, data_sys='小平台刊登库wish')
    df = df.drop('country_code', axis=1).join(
        df['country_code'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('country_code'))
    df.loc[df['country_code'] == 'GB', 'country_code'] = 'UK'
    df = df[['sku', 'country_code']].drop_duplicates()
    conn = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    conn.ck_clear_table('yibai_wish_virtual_warehouse_sku')
    conn.write_to_ck_json_type(df, 'yibai_wish_virtual_warehouse_sku')

# 2023-05-16 修改wish平台佣金率：可以都用海外仓直发佣金率
def update_wish_payfee():
    """
    更新wish的平台佣金率
    1、先获取yibai_platform_fee、yibai_wish_country的数据
    2、对数据进行替换、更新(wish_standed,pay_fee都替换为we_standard_rate)
    3、重新存入yibai_platform_fee
    """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_wish')
    sql = '''
    SELECT *
    FROM yibai_wish.yibai_platform_fee
    WHERE platform = 'WISH'
    '''
    df_old = ck_client.ck_select_to_df(sql)

    sql = '''
    SELECT
        'WISH' as platform,
        CASE WHEN country_code='GB' THEN 'UK'
        ELSE country_code END as site,
        toFloat64(sea_commission_rate/100) as we_standard_rate
    from yibai_sale_center_listing_sync.yibai_wish_commission_rate_config
    where status = 1 and is_del=0
    '''
    df_new = ck_client.ck_select_to_df(sql)

    # 更新替换
    df = pd.merge(df_old, df_new, how='left', on=['platform', 'site'])
    df['wish_standed'] = np.where(df['we_standard_rate'].isna(), df['wish_standed'], df['we_standard_rate'])
    df['pay_fee'] = df['wish_standed']
    df[['pay_fee', 'paypal_fee','vat_fee', 'extra_fee', 'refound_fee', 'platform_zero', 'platform_must_percent','wish_standed']] = \
        df[['pay_fee', 'paypal_fee','vat_fee', 'extra_fee', 'refound_fee', 'platform_zero', 'platform_must_percent','wish_standed']].astype(float)
    df = df[df['platform']=='WISH']
    df.drop(['we_standard_rate','sync_time'], axis=1, inplace=True)

    # 重新写入
    sql = '''
    ALTER TABLE yibai_wish.yibai_platform_fee DELETE WHERE platform = 'WISH'
    '''
    ck_client.ck_execute_sql(sql)
    ck_client.write_to_ck_json_type('yibai_wish.yibai_platform_fee', df)

    # # 备份
    # sql = '''
    # SELECT *
    # FROM over_sea.yibai_platform_fee
    # WHERE platform = 'WISH'
    # '''
    # df = sql_to_pd('over_sea', sql, '数据部服务器')
    # ck_client.write_to_ck_json_type('yibai_wish.yibai_platform_fee', df)
    return None

def wish_price_adjust_update():

    # if get_now_weekday() != 4:
    #     return

    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yibai_wish')
    # 查看ck同步数据是否完整，完整则使用ck同步数据
    ck_tongbu_jiankong('yibai_other_listing_sync')
    # 20220428
    yibai_wish_virtual_warehouse_sku()
    # 2023-05-16 修改wish平台佣金率：可以都用海外仓直发佣金率
    update_wish_payfee()
    try:
        # if get_sku_stock_ods(ck_client) < 0:
        #     return
        # get_erp_warehouse_conf()
        if get_wish_listing(ck_client) < 0:
            return
        if over_sea_wish_result(ck_client) < 0:
            return
        # 导出明细数据给销售检查
        wish_export_file(ck_client)
        # oversea_wish_price_post()
        # csv方式导出时使用
        # if over_sea_wish_pivot_result(ck_client) < 0:
        #     return
        # gen_wish_csv()
    except:
        raise Exception(traceback.format_exc())


if __name__ == "__main__":
    # wish_price_adjust_update()
    ck_client = CkClient(user='zengzhijie', password='ze65nG_zHij5ie', host='121.37.30.78', port='9001',
                         db_name='yibai_wish')
    over_sea_wish_result(ck_client)