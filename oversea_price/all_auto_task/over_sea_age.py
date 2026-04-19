import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import traceback
from all_auto_task.dingding import send_msg
from all_auto_task.nacos_api import get_user
from all_auto_task.scripts_ck_client import CkClient
from pulic_func.base_api.mysql_connect import sql_to_pd
from pulic_func.base_api.mysql_connect import connect_to_sql





def get_date():
    in_date = datetime.date.today().isoformat()
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    out_date = (dt - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    return out_date



def over_seas(conn):

    select_sql1=f"""with 
                    [invage_360_plus_days, invage_300_to_360_days, invage_270_to_300_days, invage_240_to_270_days, 
                    invage_210_to_240_days, invage_180_to_210_days, invage_150_to_180_days, invage_120_to_150_days, 
                    invage_90_to_120_days, invage_60_to_90_days, invage_30_to_60_days, invage_0_to_30_days] as inv_stock_age_arr,
                    arrayMap(y,z->if(available_stock-z>=0, y, greatest(available_stock-z+y,0)),
                    inv_stock_age_arr, arrayCumSum(inv_stock_age_arr)) as res_arr		
                    select
                    a.sku as SKU, a.* except(sku, warehouse_id, best_warehouse,warehouse),
                    if(empty(b.best_warehouse), a.warehouse_id, b.id) as warehouse_id,
                    if(empty(b.best_warehouse), a.best_warehouse, b.best_warehouse) as best_warehouse,
                    if(best_warehouse='MBB波兰仓','波兰仓',a.warehouse) as warehouse,
                    available_stock - arraySum(res_arr) + invage_0_to_30_days as inv_age_0_to_30_days,
                    res_arr[11] as inv_age_30_to_60_days,
                    res_arr[10] as inv_age_60_to_90_days,
                    res_arr[9] as inv_age_90_to_120_days,
                    res_arr[8] as inv_age_120_to_150_days,
                    res_arr[7] as inv_age_150_to_180_days,
                    res_arr[6] as inv_age_180_to_210_days,
                    res_arr[5] as inv_age_210_to_240_days,
                    res_arr[4] as inv_age_240_to_270_days,
                    res_arr[3] as inv_age_270_to_300_days,
                    res_arr[2] as inv_age_300_to_360_days,

                    case when if(empty(b.best_warehouse), a.best_warehouse, b.best_warehouse)='MBB波兰仓' 
                    then a.available_stock  
                    else res_arr[1] end as inv_age_360_plus_days,
                    
                    case when if(empty(b.best_warehouse), a.best_warehouse, b.best_warehouse)='MBB波兰仓' then a.available_stock  
                    else inv_age_210_to_240_days+inv_age_240_to_270_days+inv_age_270_to_300_days+inv_age_300_to_360_days+inv_age_360_plus_days 
                    end as inv_age_210_plus_days,

                    case
                    when a.available_stock-b.inv_age_0_to_40_days >= 0 then b.inv_age_0_to_40_days
                    else a.available_stock
                    end as inv_age_0_to_40_days,

                    case
                    when a.available_stock-b.inv_age_0_to_40_days-b.inv_age_40_to_70_days >= 0 then b.inv_age_40_to_70_days
                    else GREATEST(a.available_stock-b.inv_age_0_to_40_days, 0)
                    end as inv_age_40_to_70_days,

                    case
                    when if(empty(b.best_warehouse), a.best_warehouse, b.best_warehouse)='MBB波兰仓' then a.available_stock
                    when a.available_stock-b.inv_age_0_to_40_days-b.inv_age_40_to_70_days-inv_age_70_plus_days >= 0 then b.inv_age_70_plus_days
                    else GREATEST(a.available_stock-b.inv_age_0_to_40_days-inv_age_40_to_70_days, 0)
                    end as inv_age_70_plus_days,

                    b.cargo_type
                    from
                    (
                    select
                    sku, title, new_price, gross, warehouse_id, 
                    dev_type as type, product_status, last_linest, linest, 
                    sum_available_stock as available_stock, 
                    sum_available_stock_money as available_stock_money, 
                    sum_on_way_stock as on_way_stock, 
                    sku_create_time as create_time, 
                    product_size, product_package_size, best_warehouse, 
                    warehouse
                    from
                    (
                    with 
                    [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
                    ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
                    '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
                    '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
                    '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr	 
                    select
                    ps.sku as sku,
                    pd.title as title,
                    ps.new_price as new_price, ps.product_weight_pross as gross, 
                    ps.warehouse_code as warehouse_code, p2.warehouse_id as warehouse_id,
                    case
                    when p.state_type = '1' then '常规产品'
                    when p.state_type = '2' then '试卖产品'
                    when p.state_type = '3' then '亚马逊产品'
                    when p.state_type = '4' then '通途产品'
                    when p.state_type = '5' then '亚马逊服装'
                    when p.state_type = '6' then '国内仓转海外仓'
                    when p.state_type = '9' then '代销产品'
                    else '未知类型'
                    end as dev_type,
                    transform(p.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status,
                    pl.linelist_cn_name as last_linest, splitByString('>>', pl.path_name)[1] as linest,
                    ps.available_stock as available_stock, ps.available_stock*toFloat64(ps.new_price) as available_stock_money,  
                    ps.on_way_stock as on_way_stock, ps.create_time as sku_create_time, 
                    concat(toString(ps.product_length), '*', toString(ps.product_width), '*', toString(ps.product_height)) as product_size, 
                    concat(toString(ps.pur_lenght_pack), '*',toString(ps.pur_width_pack), '*', toString(ps.pur_height_pack) ) as product_package_size, 
                    p2.warehouse_name as best_warehouse,
                    case
                    when p2.warehouse_name = '出口易意大利仓' then '意大利仓'
                    when p2.warehouse_name = '谷仓意大利仓' then '意大利仓'
                    when p2.warehouse_name = '递四方加拿大仓' then '加拿大仓'
                    when p2.warehouse_name = '递四方日本大阪仓' then '日本仓'
                    when p2.warehouse_name = '旺集俄罗斯3号仓' then '俄罗斯仓'
                    when p2.warehouse_name = 'MBB波兰仓' then '德国仓'
                    when p2.warehouse_name = 'DHS乌拉圭仓' then '乌拉圭仓'
                    when p2.warehouse_name LIKE '%墨西哥%' then '墨西哥仓'
                    else p2.warehouse 
                    end as warehouse,
                    sum(ps.available_stock) over w as sum_available_stock, 
                    sum(available_stock_money) over w as sum_available_stock_money, 
                    sum(ps.on_way_stock) over w as sum_on_way_stock
                    from
                    (select ps.sku sku,ps.warehouse warehouse,ps.warehouse_name as warehouse_name 
                    ,ps.warehouse_code as warehouse_code,ps.warehouse_id as warehouse_id,ps.on_way_stock on_way_stock 
                    ,ps.type as type,ps.available_stock available_stock ,if(empty(yps.new_price),yps1.new_price,yps.new_price) as new_price
                    ,if(empty(yps.product_weight_gross),yps1.product_weight_gross,yps.product_weight_gross) as product_weight_pross
                    ,if(empty(yps.product_length),yps1.product_length ,yps.product_length )  as product_length
                    ,if(empty(yps.product_width),yps1.product_width ,yps.product_width ) as product_width
                    ,if(empty(yps.product_height),yps1.product_height ,yps.product_height ) as product_height
                    ,if(empty(yps.pur_length_pack),yps1.pur_length_pack ,yps.pur_length_pack ) as pur_lenght_pack
                    ,if(empty(yps.pur_width_pack),yps1.pur_width_pack ,yps.pur_width_pack ) pur_width_pack
                    ,if(empty(yps.pur_height_pack),yps1.pur_height_pack ,yps.pur_height_pack ) pur_height_pack
                    ,if(empty(toString(yps.create_time)),yps1.create_time,yps.create_time) as create_time
                    from 
                    (select * except (available_stock),case WHEN sku LIKE 'GB-%' THEN REPLACE(sku,'GB-','') 

                    WHEN sku LIKE 'DE-%' THEN REPLACE(sku,'DE-','') 
                    
                    WHEN sku LIKE 'FR-%' THEN REPLACE(sku,'FR-','') 
                    
                    WHEN sku LIKE 'ES-%' THEN REPLACE(sku,'ES-','') 
                    
                    WHEN sku LIKE 'IT-%' THEN REPLACE(sku,'IT-','') 
                    
                    WHEN sku LIKE 'AU-%' THEN REPLACE(sku,'AU-','') 
                    
                    WHEN sku LIKE 'CA-%' THEN REPLACE(sku,'CA-','') 
                    
                    WHEN sku LIKE 'JP-%' THEN REPLACE(sku,'JP-','') 
                    
                    WHEN sku LIKE 'US-%' THEN REPLACE(sku,'US-','') 
                    
                    WHEN sku LIKE '%DE' THEN REPLACE(sku,'DE','') else sku end as skuu,

                    -- 2023-04-26 非澳洲仓下万邑通仓库可用库存不加干预（原来为全部为设置为0），进入调价逻辑
                    available_stock
                    from yb_datacenter.v_oversea_stock order by available_stock desc) ps
                    left join yibai_prod_base_sync.yibai_prod_sku yps on ps.sku=yps.sku
                    left join yibai_prod_base_sync.yibai_prod_sku yps1 on ps.skuu=yps1.sku
                    having new_price<>'') ps
                    
                    left join yb_product p on
                    ps.sku = p.sku
                    left join yb_product_description pd on
                    pd.sku = p.sku and pd.language_code = 'Chinese'
                    left join yb_product_linelist pl on
                    pl.id = toUInt64(p.product_linelist_id)
                    join v_warehouse_erp p2 on
                    ps.warehouse_code = p2.warehouse_code
                    -- 2023-04-26 剔除原有的基于 warehouse_id, state_type, product_status 的筛选
                    window w as (partition by sku, warehouse)
                    order by available_stock desc
                    ) a
                    limit 1 
                    by sku, warehouse
                    ) a
                    left join 
                    (
                    select aa.* except(id, best_warehouse),
                    if(empty(bb.best_warehouse), aa.id, bb.id) as id,
                    if(empty(bb.best_warehouse), aa.best_warehouse, bb.best_warehouse) as best_warehouse
                    from 
                    (with 
                    [0,1,2,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20,21,22,27,28,29,30,31,32,33,35] as product_status_arr,
                    ['审核不通过', '刚开发', '编辑中', '预上线', '在售中', '已滞销', '待清仓', '已停售', '待买样', '待品检', '拍摄中', '产品信息确认', 
                    '修图中', '设计审核中', '文案审核中', '文案主管终审中', '试卖编辑中', '试卖在售中', '试卖文案终审中', '预上线拍摄中', 
                    '物流审核中', '缺货中', '作图审核中', '关务审核中', '开发审核中', '拍摄中,编辑中', '编辑中,拍摄中',
                    '已编辑,拍摄中', '编辑中,已拍摄', '新系统开发中'] as product_status_desc_arr
                    select
                    a.sku as sku, 
                    charge_currency, 
                    charge_total_price, 
                    splitByString('>>', pl.path_name)[1] as product_line, 
                    
                    transform(p.product_status, product_status_arr, product_status_desc_arr, '未知') as product_status, 
                    available_stock, 
                    
                    invage_0_to_30_days, 
                    invage_30_to_60_days, 
                    invage_60_to_90_days, 
                    invage_90_to_120_days, 
                    invage_120_to_150_days,
                    invage_150_to_180_days, 
                    invage_180_to_210_days,
                    invage_210_to_240_days,
                    invage_240_to_270_days,
                    invage_270_to_300_days,
                    invage_300_to_360_days,
                    invage_360_plus_days,
                    
                    invage_210_plus_days, 
                    inv_age_0_to_40_days, 
                    inv_age_40_to_70_days, 
                    inv_age_70_plus_days,
                    p2.id as id,
                    p2.warehouse_name as best_warehouse,
                    warehouse, cargo_type
                    from
                    (
                    select
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
                    when inventory_age between 210 and 240 then 'inv_age_210_to_240_days'
                    when inventory_age between 240 and 270 then 'inv_age_240_to_270_days'
                    when inventory_age between 270 and 300 then 'inv_age_270_to_300_days'
                    when inventory_age between 300 and 360 then 'inv_age_300_to_360_days'
                    when inventory_age between 360 and 9999999 then 'inv_age_360_plus_days'
                  
                    else Null
                    end as stock_age_section,
                    case when inventory_age between 210 and 9999999 then 'inv_age_210_plus_days' end as stock_age_section210,
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
                    when country = 'MX' then '墨西哥仓'
                    else '美国仓'
                    end as warehouse,
                    warehouse_stock,
                    inventory_age,
                    sum(charge_total_price) over w as charge_total_price,
                    sum(warehouse_stock) over w as available_stock,
                    sum(case when stock_age_section = 'inv_age_0_to_30_days' then warehouse_stock else 0 end) over w as invage_0_to_30_days, 
                    sum(case when stock_age_section  = 'inv_age_30_to_60_days' then warehouse_stock else 0 end) over w as invage_30_to_60_days, 
                    sum(case when stock_age_section  = 'inv_age_60_to_90_days' then warehouse_stock else 0 end) over w as invage_60_to_90_days, 
                    sum(case when stock_age_section  = 'inv_age_90_to_120_days' then warehouse_stock else 0 end) over w as invage_90_to_120_days, 
                    sum(case when stock_age_section  = 'inv_age_120_to_150_days' then warehouse_stock else 0 end) over w as invage_120_to_150_days,
                    sum(case when stock_age_section  = 'inv_age_150_to_180_days' then warehouse_stock else 0 end) over w as invage_150_to_180_days, 
                    sum(case when stock_age_section  = 'inv_age_180_to_210_days' then warehouse_stock else 0 end) over w as invage_180_to_210_days, 
                   
                    sum(case when stock_age_section  = 'inv_age_210_to_240_days' then warehouse_stock else 0 end) over w as invage_210_to_240_days,
                    sum(case when stock_age_section  = 'inv_age_240_to_270_days' then warehouse_stock else 0 end) over w as invage_240_to_270_days,
                    sum(case when stock_age_section  = 'inv_age_270_to_300_days' then warehouse_stock else 0 end) over w as invage_270_to_300_days,
                    sum(case when stock_age_section  = 'inv_age_300_to_360_days' then warehouse_stock else 0 end) over w as invage_300_to_360_days,
                    sum(case when stock_age_section  = 'inv_age_360_plus_days' then warehouse_stock else 0 end) over w as invage_360_plus_days,
                    sum(case when stock_age_section210  = 'inv_age_210_plus_days' then warehouse_stock else 0 end) over w as invage_210_plus_days,
                    
                    sum(case when stock_age_section_new = 'inv_age_0_to_40_days' then warehouse_stock else 0 end) over w as inv_age_0_to_40_days, 
                    sum(case when stock_age_section_new = 'inv_age_40_to_70_days' then warehouse_stock else 0 end) over w as inv_age_40_to_70_days, 
                    sum(case when stock_age_section_new = 'inv_age_70_plus_days' then warehouse_stock else 0 end) over w as inv_age_70_plus_days
                    from
                    ({yb_oversea_sku_age()}) a
                    inner join (
                        select distinct sku,warehouse_code, available_stock as astock 
                        -- 2023-04-26 非澳洲仓下万邑通仓库可用库存不加干预（原来为全部为设置为0），进入调价逻辑
                        from yb_datacenter.v_oversea_stock 
                        having astock>0
                    ) b on a.sku=b.sku and a.warehouse_code =b.warehouse_code
                    where
                    date = '{get_date()}'
                    and status in (0, 1) 
                    and warehouse_stock > 0
                    window w as (partition by sku, warehouse)
                    ) a
                   
                    left join yb_product p on a.sku = p.sku
                    left join yb_product_linelist pl on	pl.id = toUInt64(p.product_linelist_id)
                    left join yb_warehouse_erp p2 on a.warehouse_code = p2.warehouse_code 
                    -- 2023-04-26 剔除原有的基于 state_type, product_status 的筛选
                    order by charge_total_price/warehouse_stock desc,inventory_age desc limit 1
                    by a.sku, warehouse) aa
                    left join (
                        select
                            a.sku as sku, 
                            p2.id as id,
                            p2.warehouse_name as best_warehouse,
                            warehouse
                    from
                    (
                    select
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
                    when country = 'MX' then '墨西哥仓'
                    else '美国仓'
                    end as warehouse,
                    warehouse_stock,
                    inventory_age,
                    sum(charge_total_price) over w as charge_total_price,
                    sum(warehouse_stock) over w as available_stock,
                    sum(case when stock_age_section = 'inv_age_0_to_30_days' then warehouse_stock else 0 end) over w as invage_0_to_30_days, 
                    sum(case when stock_age_section  = 'inv_age_30_to_60_days' then warehouse_stock else 0 end) over w as invage_30_to_60_days, 
                    sum(case when stock_age_section  = 'inv_age_60_to_90_days' then warehouse_stock else 0 end) over w as invage_60_to_90_days, 
                    sum(case when stock_age_section  = 'inv_age_90_to_120_days' then warehouse_stock else 0 end) over w as invage_90_to_120_days, 
                    sum(case when stock_age_section  = 'inv_age_120_to_150_days' then warehouse_stock else 0 end) over w as invage_120_to_150_days,
                    sum(case when stock_age_section  = 'inv_age_150_to_180_days' then warehouse_stock else 0 end) over w as invage_150_to_180_days, 
                    sum(case when stock_age_section  = 'inv_age_180_to_210_days' then warehouse_stock else 0 end) over w as invage_180_to_210_days, 
                    sum(case when stock_age_section  = 'inv_age_210_plus_days' then warehouse_stock else 0 end) over w as invage_210_plus_days, 
                    sum(case when stock_age_section_new = 'inv_age_0_to_40_days' then warehouse_stock else 0 end) over w as inv_age_0_to_40_days, 
                    sum(case when stock_age_section_new = 'inv_age_40_to_70_days' then warehouse_stock else 0 end) over w as inv_age_40_to_70_days, 
                    sum(case when stock_age_section_new = 'inv_age_70_plus_days' then warehouse_stock else 0 end) over w as inv_age_70_plus_days
                    from ({yb_oversea_sku_age()}) a 
                    inner join (select distinct sku,warehouse_code from yb_datacenter.v_oversea_stock 
                    where available_stock>0 and warehouse='澳洲仓' and warehouse_name like '%%万邑通%%')b on a.sku=b.sku and a.warehouse_code =b.warehouse_code
                    where  a.date = '{get_date()}'
                    and status in (0, 1) 
                    and warehouse_stock > 0
                    window w as (partition by sku, warehouse)
                    ) a
                    left join yb_product p on a.sku = p.sku
                    left join yb_product_linelist pl on	pl.id = toUInt64(p.product_linelist_id)
                    left join yb_warehouse_erp p2 on a.warehouse_code = p2.warehouse_code 
                    -- 2023-04-26 剔除原有的基于 state_type, product_status 的筛选
                    order by charge_total_price/warehouse_stock desc,inventory_age desc limit 1
                    by a.sku, warehouse) bb on aa.sku=bb.sku and aa.warehouse=bb.warehouse  ) b on	a.sku = b.sku and a.warehouse = b.warehouse
                    """

    print(select_sql1)
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001', db_name='yb_datacenter')
    df = ck_client.ck_select_to_df(select_sql1)

    # df_cangku.drop(["best_warehouse","warehouse_id","warehouse"],axis=1, inplace=True)
    sql4 = """select type_house,chargo_type,chargo_days from yibai_ovrsea_chargo_day"""
    df4 = conn.read_sql(sql4)

    df['仓位'] = "德法西意大利及其他"
    df.loc[(df["warehouse"] == "美国仓"), "仓位"] = "美国"
    df.loc[(df["warehouse"] == "英国仓"), "仓位"] = "英国"
    df.loc[(df["warehouse"] == "德国仓"), "仓位"] = "德国"
    df.loc[(df["warehouse"] == "澳洲仓"), "仓位"] = "澳大利亚"
    df.loc[(df["warehouse"] == "墨西哥仓"), "仓位"] = "墨西哥"
    print(df.columns)
    print(df4.columns)
    df = df.merge(df4, left_on=["仓位", "cargo_type"], right_on=["type_house", "chargo_type"], how="left")
    print(df.head())
    df.drop(columns=["chargo_type"], axis=1, inplace=True)
    df["chargo_days"].fillna(90, inplace=True)
    df = kuling(df)

    return df


def kuling(df):
    df["chargo_days"] = df["chargo_days"].astype("int")
    df["超30天库龄"] = None
    df["超60天库龄"] = None
    df["超90天库龄"] = None
    df["超120天库龄"] = None
    df["超150天库龄"] = None
    df["超180天库龄"] = None
    df.loc[:, "超30天库龄"] = df["inv_age_30_to_60_days"] + df["inv_age_60_to_90_days"] + \
                          df["inv_age_90_to_120_days"] + df["inv_age_120_to_150_days"] + \
                          df["inv_age_150_to_180_days"] + df["inv_age_180_to_210_days"] + \
                          df["inv_age_210_plus_days"]

    df.loc[:, "超60天库龄"] = df["inv_age_60_to_90_days"] + \
                          df["inv_age_90_to_120_days"] + df["inv_age_120_to_150_days"] + \
                          df["inv_age_150_to_180_days"] + df["inv_age_180_to_210_days"] + \
                          df["inv_age_210_plus_days"]
    df.loc[:, "超90天库龄"] = df["inv_age_90_to_120_days"] + df["inv_age_120_to_150_days"] + \
                          df["inv_age_150_to_180_days"] + df["inv_age_180_to_210_days"] + \
                          df["inv_age_210_plus_days"]
    df.loc[:, "超120天库龄"] = df["inv_age_120_to_150_days"] + df["inv_age_150_to_180_days"] + df[
        "inv_age_180_to_210_days"] + df["inv_age_210_plus_days"]
    df.loc[:, "超150天库龄"] = df["inv_age_150_to_180_days"] + df["inv_age_180_to_210_days"] + df["inv_age_210_plus_days"]
    df.loc[:, "超180天库龄"] = df["inv_age_180_to_210_days"] + df["inv_age_210_plus_days"]

    import re

    for i in df.columns:
        if re.findall("^超.+(天库龄$)", i):
            df[i].fillna(0, inplace=True)
            df[i] = df[i].astype("int")

    df["over_section"] = "无"
    df["over_section_stock"] = 0

    # chargo_days进行去重分组
    day_list = df['chargo_days'].unique().tolist()
    for i in day_list:

        df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] > 0), "over_section"] = "x+60"
        df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] > 0), "over_section_stock"] = df[f"超{i + 60}天库龄"]
        df.loc[
            (df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] > 0), "over_section"] = "x+30"
        df.loc[
            (df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] > 0), "over_section_stock"] = \
            df[f"超{i + 30}天库龄"]
        if i > 30:
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] > 0), "over_section"] = "x"
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] > 0), "over_section_stock"] = df[f"超{i}天库龄"]
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] == 0) & (df[f"超{i - 30}天库龄"] > 0), "over_section"] = "x-30"
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] == 0) & (df[f"超{i - 30}天库龄"] > 0), "over_section_stock"] = df[f"超{i - 30}天库龄"]

        elif i == 30:
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] > 0), "over_section"] = "x"
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] > 0), "over_section_stock"] = df[f"超{i}天库龄"]
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df[f"超{i}天库龄"] == 0) & (df["available_stock"] > 0), "over_section"] = "x-30"

        else:
            df.loc[(df["chargo_days"] == i) & (df[f"超{i + 60}天库龄"] == 0) & (df[f"超{i + 30}天库龄"] == 0) & (
                    df["available_stock"] > 0), "over_section"] = "x"

    a = len(df.columns) - 7
    df.insert(a, "over_section", df.pop("over_section"))
    df.insert(a + 1, "over_section_stock", df.pop("over_section_stock"))
    # df.rename(columns={"best_warehouse": "warehouse_name_es"}, inplace=True)

    return df


def get_best_warehouse():
    sql = f"""SELECT distinct sku as SKU,
                        warehouse_code,
                        warehouse_stock,
                        charge_total_price,
                        inventory_age,
                        charge_total_price/warehouse_stock as per_charge_price  
                        FROM ({yb_oversea_sku_age()}) a
                        where date>='{get_date()}'
                        order by charge_total_price/warehouse_stock desc,inventory_age desc """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    df=ck_client.ck_select_to_df(ck_sql=sql)
    # df = sql_to_pd(database='yb_datacenter', sql=sql)
    # df = df.sort_values('per_charge_price',ascending=False)
    df = df.drop_duplicates(['SKU'], 'first')
    # sql2 = """select id,warehouse_name,warehouse_code from yb_warehouse_erp"""
    sql2 = """select a.id as warehouse_id,a.warehouse_name,a.warehouse_code,b.name as warehouse from yb_warehouse_erp a left join yb_warehouse_category b on a.ebay_category_id = b.id"""
    df2=ck_client.ck_select_to_df(ck_sql=sql2)
    # df2 = sql_to_pd(database='yb_datacenter', sql=sql2)
    df2.loc[df2["warehouse_id"].isin([434, 529]), 'warehouse'] = '意大利仓'
    df2.loc[df2["warehouse_id"] == 349, 'warehouse'] = '加拿大仓'
    df2.loc[df2["warehouse_id"] == 443, 'warehouse'] = '日本仓'
    df2.loc[df2["warehouse_id"] == 577, 'warehouse'] = '德国仓'
    df = df.merge(df2, on="warehouse_code")
    df.drop(["warehouse_code", "warehouse_stock", "charge_total_price", "inventory_age", "per_charge_price"], axis=1,
            inplace=True)
    # df = df.rename(columns={"warehouse_name": "best_warehouse",""})
    print(df.head(5))
    return df


def fun(conn):
    sql = """select  SKU,SUM(3days_sales),SUM(7days_sales),SUM(15days_sales),SUM(30days_sales),SUM(90days_sales),
    (SUM(7days_sales)/7*0.9+SUM(30days_sales)/30*0.1) AS 'day_sales', (SUM(7days_sales)/7*0.9+SUM(30days_sales)/30*0.1)*0.9 as 'reduce_day_sales',warehouse

    from (SELECT SKU,3days_sales,7days_sales,15days_sales,30days_sales,90days_sales,CASE 

        WHEN country='US' THEN '美国仓'

        WHEN country='UK' THEN '英国仓'

        WHEN country='GB' THEN '英国仓'

        WHEN country='CS' THEN '德国仓'

        WHEN country='FR' THEN '法国仓'

        WHEN country='IT' THEN '意大利仓'

        WHEN country='AU' THEN '澳洲仓'

        WHEN country='ES' THEN '西班牙仓'

        WHEN country='CA' THEN '加拿大仓'

        WHEN country='DE' THEN '德国仓'

        WHEN country='JP' THEN '日本仓'

        WHEN country='PL' THEN '德国仓'
        
        WHEN country='MX' THEN '墨西哥仓'
        WHEN country='BR' THEN '乌拉圭仓'
        ELSE '美国仓' END AS 'warehouse' FROM `yibai_sku_sales_statistics` a 

    left join yibai_warehouse b on a.warehouse_id=b.id

    where b.warehouse_type in (2, 3) and b.id not in (60,323) and platform_code not in ('DIS','WYLFX')

    )A GROUP BY SKU,warehouse"""
    df = conn.read_sql(sql)
    return df


def oversea_age_run(conn):
    df_cangku = over_seas(conn)
    # df = get_best_warehouse()
    # df_cangku = df_cangku.merge(df, on=["SKU", "warehouse", "warehouse_id"], how="left")
    # df_cangku["best_warehouse"] = df_cangku["warehouse_name"]
    # df_cangku.loc[df_cangku["best_warehouse"].isnull(), "best_warehouse"] = df_cangku["warehouse_name_es"]
    # df_cangku.drop(["warehouse_name", "warehouse_name_es"], axis=1, inplace=True)

    df_cangku = df_cangku[

        ['SKU', 'title', 'new_price', 'gross', 'warehouse_id', 'type',

         'product_status', 'last_linest', 'linest', 'available_stock', 'available_stock_money',

         'on_way_stock', 'create_time', 'product_size', 'product_package_size',

         'best_warehouse', 'warehouse', 'inv_age_0_to_30_days', 'inv_age_30_to_60_days', 'inv_age_60_to_90_days',
         'inv_age_90_to_120_days', 'inv_age_120_to_150_days', 'inv_age_150_to_180_days', 'inv_age_180_to_210_days',
         'inv_age_210_to_240_days','inv_age_240_to_270_days','inv_age_270_to_300_days','inv_age_300_to_360_days',
         'inv_age_360_plus_days',
         'inv_age_210_plus_days', 'inv_age_0_to_40_days', 'inv_age_40_to_70_days', 'inv_age_70_plus_days',
         "cargo_type", "chargo_days", "over_section", "over_section_stock", "超30天库龄", "超60天库龄", "超90天库龄",
         "超120天库龄", "超150天库龄", "超180天库龄"]]

    fun1 = fun(conn)

    data = pd.merge(df_cangku, fun1, on=['SKU', 'warehouse'], how='left')
    data['SUM(3days_sales)'] = data['SUM(3days_sales)'].fillna(0)

    data['SUM(7days_sales)'] = data['SUM(7days_sales)'].fillna(0)

    data['SUM(15days_sales)'] = data['SUM(15days_sales)'].fillna(0)

    data['SUM(30days_sales)'] = data['SUM(30days_sales)'].fillna(0)

    data['SUM(90days_sales)'] = data['SUM(90days_sales)'].fillna(0)

    data['day_sales'] = data['day_sales'].fillna(0)
    data.loc[data['available_stock'] == 0, 'day_sales'] = 0
    data['reduce_day_sales'] = data['reduce_day_sales'].fillna(0)

    data['inv_age_30_to_60_days'] = data['inv_age_30_to_60_days'].fillna(0)

    data['inv_age_60_to_90_days'] = data['inv_age_60_to_90_days'].fillna(0)

    data['inv_age_90_to_120_days'] = data['inv_age_90_to_120_days'].fillna(0)

    data['inv_age_120_to_150_days'] = data['inv_age_120_to_150_days'].fillna(0)

    data['inv_age_150_to_180_days'] = data['inv_age_150_to_180_days'].fillna(0)

    data['inv_age_180_to_210_days'] = data['inv_age_180_to_210_days'].fillna(0)

    data['inv_age_210_plus_days'] = data['inv_age_210_plus_days'].fillna(0)

    data['inv_age_40_to_70_days'] = data['inv_age_40_to_70_days'].fillna(0)

    data['inv_age_70_plus_days'] = data['inv_age_70_plus_days'].fillna(0)

    data['estimated_sales_days'] = data.apply(

        lambda x: x['available_stock'] / x['day_sales'] if x['day_sales'] != 0 else 999, axis=1).round(2)

    data['estimated_reduce_sales_days'] = data.apply(

        lambda x: x['available_stock'] / x['reduce_day_sales'] if x['day_sales'] != 0 else 999, axis=1).round(2)

    def fun2(x):

        if x['estimated_sales_days'] <= 30:

            return 'A<=1'

        elif 30 < x['estimated_sales_days'] <= 60:

            return '1<A<=2'

        elif 60 < x['estimated_sales_days'] <= 90:

            return '2<A<=3'

        elif 90 < x['estimated_sales_days'] <= 150:

            return '3<A<=5'

        elif 150 < x['estimated_sales_days'] <= 300:

            return '5<A<=10'

        elif 300 < x['estimated_sales_days'] <= 99999999999:

            return 'A>10'

    data['a_value_section'] = data.apply(lambda x: fun2(x), axis=1)
    data["onway_available_sales_days"] = round((data["available_stock"] + data['on_way_stock']) / data['day_sales'], 2)
    data.loc[data["day_sales"] == 0, 'onway_available_sales_days'] = 999

    print(data)

    def fun3(x):
        if x['day_sales'] == 0:
            return 'S=0'
        if 0 < x['day_sales'] <= 0.1:
            return 'S∈(0,0.1]'
        elif 0.1 < x['day_sales'] <= 0.3:
            return 'S∈(0.1,0.3]'

        elif 0.3 < x['day_sales'] <= 0.6:
            return 'S∈(0.3,0.6]'
        elif 0.6 < x["day_sales"] <= 1:
            return "S∈(0.6,1]"
        elif 1 < x['day_sales'] <= 3:
            return 'S∈(1,3]'

        elif 3 < x['day_sales'] <= 5:
            return 'S∈(3,5]'
        elif 5 < x['day_sales'] <= 99999999999:
            return 'S∈(5,∞)'

    data['day_sales_value_section'] = data.apply(lambda x: fun3(x), axis=1)

    data["DATE"] = datetime.date.today().isoformat()
    data['over_estimated_message'] = data.apply(lambda x: fun4(x), axis=1)
    data['over_estimated_message_section'] = data['over_estimated_message'].apply(lambda x: fun5(x))
    data["estimated_message"] = ''
    data.loc[(data["day_sales"] >= 0) & (data["day_sales"] <= 0.1) & (
            data["estimated_sales_days"] > 30), "estimated_message"] = 'S∈[0,0.1];N∈(30,+∞)'
    data.loc[(data["day_sales"] > 0.1) & (data["day_sales"] <= 0.3) & (
            data["estimated_sales_days"] > 30) & (data[
                                                      'onway_available_sales_days'] > 60), "estimated_message"] = 'S∈(0.1,0.3];N∈(30,+∞);M∈(60,+∞)'

    data.loc[(data["day_sales"] > 0.3) & (data["day_sales"] <= 1) & (
            data["estimated_sales_days"] > 30) & (data[
                                                      'onway_available_sales_days'] > 70), "estimated_message"] = 'S∈(0.3,1];N∈(30,+∞);M∈(70,+∞)'
    data.loc[(data["day_sales"] > 1) & (data["day_sales"] <= 3) & (
            data["estimated_sales_days"] > 40) & (data[
                                                      'onway_available_sales_days'] > 80), "estimated_message"] = 'S∈(1,3];N∈(40,+∞);M∈(80,+∞)'

    data.loc[(data["day_sales"] > 3) & (data["day_sales"] <= 5) & (
            data["estimated_sales_days"] > 50) & (data[
                                                      'onway_available_sales_days'] > 90), "estimated_message"] = 'S∈(3,5];N∈(50,+∞);M∈(90,+∞)'

    data.loc[(data["day_sales"] > 5) & (
            data["estimated_sales_days"] > 60) & (data[
                                                      'onway_available_sales_days'] > 100), "estimated_message"] = 'S∈(5,+∞);N∈(60,+∞);M∈(100,+∞)'

    data["inventory_stock_message"] = 'D'

    data["over_150_inventory_stock"] = data['inv_age_150_to_180_days'] + data['inv_age_180_to_210_days'] + data[
        "inv_age_210_plus_days"]
    data['over_120_inventory_stock'] = data["inv_age_120_to_150_days"] + data['inv_age_150_to_180_days'] + data[
        'inv_age_180_to_210_days'] + data["inv_age_210_plus_days"]
    data['over_90_inventory_stock'] = data["inv_age_90_to_120_days"] + data["inv_age_120_to_150_days"] + data[
        'inv_age_150_to_180_days'] + data['inv_age_180_to_210_days'] + data["inv_age_210_plus_days"]
    data.loc[
        (data['over_estimated_message'] > 0) & (data['over_150_inventory_stock'] >= 2), "inventory_stock_message"] = 'A'
    data.loc[(data['over_estimated_message'] > 0) & (data['over_150_inventory_stock'] < 2) & (
            data['over_120_inventory_stock'] >= 3), "inventory_stock_message"] = 'B'
    data.loc[(data['over_estimated_message'] > 0) & (data['over_150_inventory_stock'] < 2) & (
            data['over_120_inventory_stock'] < 3) & (
                     data['over_90_inventory_stock'] >= 5), "inventory_stock_message"] = 'C'

    data.drop(["over_150_inventory_stock", "over_120_inventory_stock", "over_90_inventory_stock"], axis=1, inplace=True)

    # data.to_excel('oversea_age测试.xlsx',index=False)
    #
    # data=pd.read_excel("oversea_age测试.xlsx")
    data["over_section_stock"] = data["over_section_stock"].astype("int")
    data["day_sales"] = data["day_sales"].astype("float")
    data["over_section_stock_days"] = data["over_section_stock"] / data["day_sales"]
    data.loc[(data["over_section_stock"] == 0) & (data["day_sales"] == 0), "over_section_stock_days"] = 0
    data.loc[(data["over_section_stock"] > 0) & (data["day_sales"] == 0), "over_section_stock_days"] = 999
    # data.loc[(data["over_section_stock"] > 0) & (data["day_sales"] != 0), "over_section_stock_days"] = data["over_section_stock"] / data["day_sales"]
    data.insert(32, "over_section_stock_days", data.pop("over_section_stock_days"))
    data.rename(columns={"chargo_days": "charge_days",
                         "超30天库龄": "inv_age_30_plus_days",
                         "超60天库龄": "inv_age_60_plus_days",
                         "超90天库龄": "inv_age_90_plus_days",
                         "超120天库龄": "inv_age_120_plus_days",
                         "超150天库龄": "inv_age_150_plus_days",
                         "超180天库龄": "inv_age_180_plus_days"}, inplace=True)

    # 添加新老品
    data = age_new(data)
    data["over_section_stock_days"] = data["over_section_stock_days"].astype("float")
    data["day_sales_g"] = data["day_sales"]
    data.loc[data["day_sales_g"] == 0, "day_sales_g"] = 0.0001
    data["test"] = data.apply(lambda x: fun6(x), axis=1)
    data["gradient"] = None
    data["A值"] = None
    print("-------------------------------")
    print((data[data["test"].notnull()]["test"]).apply(lambda x: x))
    gradient = (data[data["test"].notnull()]["test"]).apply(lambda x: x[-1])
    A = (data[data["test"].notnull()]["test"]).apply(lambda x: x[0])
    data.loc[data["test"].notnull(), "gradient"] = gradient
    data.loc[data["test"].notnull(), "A值"] = A
    data["A值"].fillna(0, inplace=True)

    data = gradient_also(data)

    # del data["day_sales_g"]

    data["available_stock"] = data["available_stock"].astype("int")
    data = predict_cangzu(data)
    data.drop(
        columns=["inv_age_0_to_15_days", "inv_age_15_to_30_days", "inv_age_30_to_45_days", "inv_age_45_to_60_days",
                 "inv_age_60_to_75_days", "inv_age_75_to_90_days", "inv_age_90_to_105_days", "inv_age_105_to_120_days"],
        inplace=True, axis=1)

    data["over_section_detail"] = data.apply(lambda x: fun8(x), axis=1)
    data.drop(columns=["day_sales_g", "A值", "test"], inplace=True)
    # pd_to_sql(data, database="yibai", data_sys="my_databases", table="test", if_exists="append")

    # 匹配上当天的仓租费用
    sql = f"""select a.sku as SKU,a.charge_currency,sum(charge_total_price) as "仓租",warehouse,country from (SELECT sku,charge_currency,
     case when charge_total_price is null then 0 else charge_total_price  end as charge_total_price,country,

        CASE 

        WHEN country='US' THEN '美国仓'

        WHEN country IN ('UK','GB') THEN '英国仓'

        WHEN country IN ('CZ','CS','DE') THEN '德国仓'

        WHEN country='FR' THEN '法国仓'

        WHEN country='IT' THEN '意大利仓'

        WHEN country='AU' THEN '澳洲仓'

        WHEN country IN ('ES','SP') THEN '西班牙仓'

        WHEN country='CA' THEN '加拿大仓'

        WHEN country='JP' THEN '日本仓'

        WHEN country='PL' THEN '德国仓'
        WHEN country='MX' THEN '墨西哥仓'

        ELSE '美国仓' END AS warehouse


        FROM ({yb_oversea_sku_age()}) a

        WHERE 

         date= '{get_date()}' and status in (0,1)  and oversea_type<>'4PX') a
        group by a.sku,warehouse,charge_currency,country """
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    zu=ck_client.ck_select_to_df(ck_sql=sql)
    # zu = sql_to_pd(sql=sql, database='yb_datacenter')

    sql2 = 'select distinct from_currency_code as charge_currency,rate from domestic_warehouse_clear.erp_rate'
    rate = conn.read_sql(sql2)

    print(rate.head())
    zu.loc[zu['country'] == 'GB', 'country'] = 'UK'
    zu = zu.merge(rate, on=['charge_currency'], how='left')
    zu['仓租']=zu['仓租'].astype('float')
    zu['rate']=zu['rate'].astype('float')
    zu['仓租'] = zu['仓租'] * zu['rate']
    zu = zu[['SKU', 'warehouse', '仓租']]
    zu.drop_duplicates(inplace=True)
    # 开始匹配仓租费用
    data = data.merge(zu, on=['SKU', 'warehouse'], how='left')
    data.rename(columns={'仓租': 'charge_total_price'}, inplace=True)
    data.insert(loc=34, column='charge_total_price', value=data.pop('charge_total_price'))
    # 写到数据库
    now_time = time.strftime('%Y-%m-%d')
    sql_acost = f"""
            select sku as SKU, warehouse,
                    case when predict_acost_after_this_week is null then 
                              null
                         when predict_acost_after_this_week > 0 and predict_acost_after_this_week <= 0.05 then 
                            'Q∈(0,0.05]'
                         when predict_acost_after_this_week > 0.05 and predict_acost_after_this_week <= 0.1 then 
                            'Q∈(0.05,0.1]'
                         when predict_acost_after_this_week > 0.1 and predict_acost_after_this_week <= 0.2 then
                            'Q∈(0.1,0.2]'
                         when predict_acost_after_this_week > 0.2 and predict_acost_after_this_week <= 0.5 then 
                            'Q∈(0.2,0.5]'
                         when predict_acost_after_this_week > 0.5 and predict_acost_after_this_week <= 1 then 
                            'Q∈(0.5,1]'
                         when predict_acost_after_this_week > 1 then 
                            'Q∈(1,∞]'
                         else 
                            null
                    end as acost 
                    from oversea_predict_acost 
                    where date = '{now_time}'
    """
    df_acost = conn.read_sql(sql_acost)
    data = data.merge(df_acost, on=['SKU', 'warehouse'], how='left')

    #2022/05/07加上近万邑通有库存字段
    sql_wyt="""select distinct a.sku as SKU,a.warehouse
        from
        (SELECT sku, warehouse, warehouse_name, warehouse_code, warehouse_id, available_stock, on_way_stock, `type`
        FROM yb_datacenter.v_oversea_stock
        where warehouse_name  like '%万邑通%'and available_stock>0) a
        left join 
        (SELECT sku, warehouse, warehouse_name, warehouse_code, warehouse_id, available_stock, on_way_stock, `type`
        FROM yb_datacenter.v_oversea_stock
        where warehouse_name  not like '%万邑通%' and available_stock>0) b on a.sku=b.sku and a.warehouse=b.warehouse
        having b.warehouse is null
        """
    df_wyt=  ck_client.ck_select_to_df(sql_wyt)

    df_wyt['only_wyt']=1
    data=data.merge(df_wyt,on=['SKU','warehouse'],how='left')
    data['only_wyt'].fillna(0,inplace=True)

    # 20220601把MBB波兰仓从以前的德国大仓改成波兰大仓
    data.loc[(data['best_warehouse']=='MBB波兰仓'),'warehouse']='波兰仓'
    data.drop_duplicates(inplace=True)

    conn.to_sql(data, 'over_sea_age_new', if_exists='append')
    # data.to_sql('over_sea_age_new_CESHI', con=engine1, if_exists='append', index=False)

    # get_engine = GetFromSql(database="yibai_process_control", host="139.9.206.7", port="3306",
    #                         username="whpro", password="ki&#qXxzkRdgz")
    # engine = get_engine.engine
    # data.to_sql('over_sea_age_new', con=engine, if_exists='append', index=False)
    # data.to_sql('over_sea_age_new_CESHI', con=engine, if_exists='append', index=False)
    all_data = len(data)
    return all_data


def fun8(x):
    import re
    if 0 < x['A值'] < 30:
        return "A∈(0,30)"
    elif 30 <= x['A值'] < 60:
        return "A∈[30,60)"
    elif 60 <= x["A值"] < 90:
        return "A∈[60,90)"
    elif 90 <= x["A值"] < 150:
        return "A∈[90,150)"
    elif 150 <= x["A值"] < 300:
        return "A∈[150,300)"
    elif x["A值"] >= 300:
        return "A∈[300，∞)"
    elif re.findall("^在库库存可售天数.*?", str(x["gradient"])):
        return "A=0"


def yb_oversea_sku_age():
    # sql = f"""select distinct *
    # from 

    # (select CASE 

    # WHEN a.origin_sku LIKE 'GB-%%' THEN REPLACE(a.origin_sku,'GB-','') 

    # WHEN a.origin_sku LIKE 'DE-%%' THEN REPLACE(a.origin_sku,'DE-','') 

    # WHEN a.origin_sku LIKE 'FR-%%' THEN REPLACE(a.origin_sku,'FR-','') 

    # WHEN a.origin_sku LIKE 'ES-%%' THEN REPLACE(a.origin_sku,'ES-','') 

    # WHEN a.origin_sku LIKE 'IT-%%' THEN REPLACE(a.origin_sku,'IT-','') 

    # WHEN a.origin_sku LIKE 'AU-%%' THEN REPLACE(a.origin_sku,'AU-','') 

    # WHEN a.origin_sku LIKE 'CA-%%' THEN REPLACE(a.origin_sku,'CA-','') 

    # WHEN a.origin_sku LIKE 'JP-%%' THEN REPLACE(a.origin_sku,'JP-','') 

    # WHEN a.origin_sku LIKE 'US-%%' THEN REPLACE(a.origin_sku,'US-','') 

    # WHEN a.origin_sku LIKE '%%DE' THEN REPLACE(a.origin_sku,'DE','')

    # ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    # from yb_oversea_sku_age a
    # where date='{datetime.date.today() - datetime.timedelta(days=2)}'
    # and status in (0,1)
    # union ALL 
    # select  CASE 

    # WHEN  a.country in ('GB','UK') and a.origin_sku not LIKE 'GB-%%' THEN concat('GB-',a.origin_sku)
    # WHEN  a.country IN ('CZ','CS','DE') and a.origin_sku not LIKE '%DE%%'   THEN concat('DE-',a.origin_sku)

    # when a.country='FR' and a.origin_sku not like 'FR-%%' then concat('FR-',a.origin_sku)

    # when a.country in ('ES','SP')  and a.origin_sku not like 'ES-%%' then concat('ES-',a.origin_sku)

    # when a.country='IT' and a.origin_sku not like 'IT-%%' then concat('IT-',a.origin_sku)

    # when a.country='AU' and a.origin_sku not like '%%AU%%' then concat('AU-',a.origin_sku)

    # when a.country='CA' and a.origin_sku not like 'CA-%%' then concat('CA-',a.origin_sku)

    # when a.country='JP' and a.origin_sku not like 'JP-%%' then concat('JP-',a.origin_sku)

    # when a.country='US' and a.origin_sku not like 'US-%%' then concat('US-',a.origin_sku)

    # ELSE a.origin_sku END AS origin_sku ,a.*except(origin_sku,id)
    # from yb_oversea_sku_age a
    # where date='{datetime.date.today() - datetime.timedelta(days= 2)}'
    # and status in (0,1)) asku"""

    sql = f"""select a.*except(id)
    from yb_oversea_sku_age a
    where date='{datetime.date.today() - datetime.timedelta(days=2)}'
    and status in (0,1)"""

    return sql

def predict_cangzu(df):
    sql = f"""
   select 

    A.sku as SKU,A.warehouse as warehouse,

    SUM(CASE when A.kulingduan = 'inv_age_0_to_15_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_0_to_15_days",

    SUM(CASE when A.kulingduan = 'inv_age_15_to_30_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_15_to_30_days",

    SUM(CASE WHEN A.kulingduan=  'inv_age_30_to_45_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_30_to_45_days", 

    SUM(CASE WHEN A.kulingduan=  'inv_age_45_to_60_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_45_to_60_days", 

    SUM(CASE WHEN A.kulingduan= 'inv_age_60_to_75_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_60_to_75_days", 

    SUM(CASE WHEN A.kulingduan= 'inv_age_75_to_90_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_75_to_90_days", 

    SUM(CASE WHEN A.kulingduan= 'inv_age_90_to_105_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_90_to_105_days", 

    SUM(CASE WHEN A.kulingduan= 'inv_age_105_to_120_days' THEN toInt64(A.warehouse_stock) ELSE 0 END) "inv_age_105_to_120_days"
    


    from (
  SELECT sku as sku,warehouse_stock,case

    when inventory_age between 0 and 15 then  'inv_age_0_to_15_days'

    when inventory_age between 15 and 30 then  'inv_age_15_to_30_days'

    when inventory_age between 30 and 45 then  'inv_age_30_to_45_days'

    when inventory_age between 45 and 60 then  'inv_age_45_to_60_days'

    when inventory_age between 60 and 75 then  'inv_age_60_to_75_days'

    when inventory_age between 75 and 90 then  'inv_age_75_to_90_days'

    when inventory_age between 90 and 105 then  'inv_age_90_to_105_days'

    when inventory_age between 105 and 120 then  'inv_age_105_to_120_days'

    ELSE NULL END AS kulingduan,


    CASE 

    WHEN country='US' THEN '美国仓'

    WHEN country IN ('UK','GB') THEN '英国仓'

    WHEN country IN ('CZ','CS','DE') THEN '德国仓'

    WHEN country='FR' THEN '法国仓'

    WHEN country='IT' THEN '意大利仓'

    WHEN country='AU' THEN '澳洲仓'

    WHEN country IN ('ES','SP') THEN '西班牙仓'

    WHEN country='CA' THEN '加拿大仓'

    WHEN country='JP' THEN '日本仓'

    WHEN country='PL' THEN '德国仓'
    WHEN country='MX' THEN '墨西哥仓'

    ELSE '美国仓' END AS warehouse


    FROM ({yb_oversea_sku_age()}) a

    WHERE 

     date= '{get_date()}' and status in (0,1)
     and inventory_age<=120
    ) A 

    LEFT JOIN yb_product p ON A.sku = p.sku

    LEFT JOIN yb_product_linelist pl ON toInt64(pl.id) = toInt64(p.product_linelist_id)
   

    where p.product_status between 0 and 35 and p.state_type <> '9'

    group by A.sku,A.warehouse"""
    ck_client = CkClient(user='zhangyilan', password='zhangyilan2109221544', host='121.37.30.78', port='9001',
                         db_name='yb_datacenter')
    predict = ck_client.ck_select_to_df(ck_sql=sql)
    for i in predict.columns:
        if i.__contains__("inv_age"):
            predict[i].fillna(0, inplace=True)
    df = df.merge(predict, on=["SKU", "warehouse"], how="left")
    df.loc[df["available_stock"] - df["inv_age_0_to_15_days"] <= 0, "inv_age_0_to_15_days"] = df["available_stock"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df[
        "inv_age_15_to_30_days"]) <= 0, "inv_age_15_to_30_days"] = df["available_stock"] - df["inv_age_0_to_15_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"]) <= 0, "inv_age_30_to_45_days"] = df["available_stock"] - df["inv_age_0_to_15_days"] - \
                                                                   df["inv_age_15_to_30_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"]) <= 0, "inv_age_45_to_60_days"] \
        = df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df["inv_age_30_to_45_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] - df[
                "inv_age_60_to_75_days"]) <= 0, "inv_age_60_to_75_days"] \
        = df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] -
            df["inv_age_60_to_75_days"] - df["inv_age_75_to_90_days"]) <= 0, "inv_age_75_to_90_days"] = df[
                                                                                                            "available_stock"] - \
                                                                                                        df[
                                                                                                            "inv_age_0_to_15_days"] - \
                                                                                                        df[
                                                                                                            "inv_age_15_to_30_days"] - \
                                                                                                        df[
                                                                                                            "inv_age_30_to_45_days"] - \
                                                                                                        df[
                                                                                                            "inv_age_45_to_60_days"] - \
                                                                                                        df[
                                                                                                            "inv_age_60_to_75_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] -
            df["inv_age_60_to_75_days"] - df["inv_age_75_to_90_days"] - df[
                "inv_age_90_to_105_days"]) <= 0, "inv_age_90_to_105_days"] \
        = df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] - df["inv_age_60_to_75_days"] - df[
              "inv_age_75_to_90_days"]

    df.loc[(df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] -
            df["inv_age_60_to_75_days"] - df["inv_age_75_to_90_days"] - df["inv_age_90_to_105_days"] - df[
                "inv_age_105_to_120_days"]) <= 0, "inv_age_105_to_120_days"] \
        = df["available_stock"] - df["inv_age_0_to_15_days"] - df["inv_age_15_to_30_days"] - df[
        "inv_age_30_to_45_days"] - df["inv_age_45_to_60_days"] - df["inv_age_60_to_75_days"] \
          - df["inv_age_75_to_90_days"] - df["inv_age_90_to_105_days"]

    for i in df.columns:
        if i.__contains__("inv_age"):
            df.loc[df[i] < 0, i] = 0
    df["day_sales_1"] = df["day_sales"]
    df.loc[df["day_sales_1"] == 0, "day_sales_1"] = 0.0001
    df["预计是否会收仓租"] = df.apply(lambda x: fun7(x), axis=1)
    df["预计是否会收仓租"].fillna("是", inplace=True)
    del df["day_sales_1"]
    df.rename(columns={"预计是否会收仓租": "is_chargo"}, inplace=True)
    return df


def fun7(x):
    for i in [0, 30, 60, 90, 120]:
        if x["charge_days"] == i:
            if x["over_section"] == "无" or x["over_section"] == "x-30":
                if (i - 15) > 0 and x[f"inv_age_{i - 15}_to_{i}_days"] / x["day_sales_1"] > 15:
                    return "是"
                elif (i - 30) > 0 and x[f"inv_age_{i - 30}_to_{i - 15}_days"] / x["day_sales_1"] > 30:
                    return "是"
                elif (i - 45) > 0 and x[f"inv_age_{i - 45}_to_{i - 30}_days"] / x["day_sales_1"] > 45:
                    return "是"
                elif (i - 60) > 0 and x[f"inv_age_{i - 60}_to_{i - 45}_days"] / x["day_sales_1"] > 60:
                    return "是"
                elif (i - 75) >= 0 and x[f"inv_age_{i - 75}_to_{i - 60}_days"] / x["day_sales_1"] > 75:
                    return "是"
                elif (i - 90) >= 0 and x[f"inv_age_{i - 90}_to_{i - 75}_days"] / x["day_sales_1"] > 90:
                    return "是"
                elif (i - 105) >= 0 and x[f"inv_age_{i - 105}_to_{i - 90}_days"] / x["day_sales_1"] > 105:
                    return "是"
                elif (i - 120) >= 0 and x[f"inv_age_{i - 120}_to_{i - 105}_days"] / x["day_sales_1"] > 120:
                    return "是"
                else:
                    return "否"


def fun6(x):
    i = x["charge_days"]
    for j, k in [(60, 30), (30, 30), (0, 60), (-30, 90)]:
        if (i + j) > 0:
            a = x[f"inv_age_{i + j}_plus_days"] / x["day_sales_g"]

            if x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and (a > 40) and x["onway_available_sales_days"] > 100 and \
                    x[
                        "day_sales_value_section"] == "S∈(5,∞)":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                return A, b

            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 40 and x["onway_available_sales_days"] > 100 and \
                    x[
                        "day_sales_value_section"] == "S∈(3,5]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                return A, b

            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 40 and x["onway_available_sales_days"] > 90 and \
                    x[
                        "day_sales_value_section"] == "S∈(1,3]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                return A, b
            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 40 and x['onway_available_sales_days'] > 80 and \
                    x[
                        'day_sales_value_section'] == "S∈(0.6,1]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                return A, b
            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 80 and \
                    x[
                        "day_sales_value_section"] == "S∈(0.3,0.6]":
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                return A, b
            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 70 and \
                    x[
                        "day_sales_value_section"] == "S∈(0.1,0.3]":
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                return A, b

            elif x[f"inv_age_{i + j}_plus_days"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 70 and (
                    x["day_sales_value_section"] == "S∈(0,0.1]" or x["day_sales_value_section"] == "S=0"):
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"

                return A, b
        elif i + j == 0:
            a = x[f"available_stock"] / x["day_sales_g"]

            if x[f"available_stock"] > 0 and a > k and (a > 40) and x["onway_available_sales_days"] > 100 and \
                    x["day_sales_value_section"] == "S∈(5,∞)":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(5,∞);N∈(40,∞);M∈(100,∞)"
                return A, b

            elif x[f"available_stock"] > 0 and a > k and a > 40 and x["onway_available_sales_days"] > 100 and \
                    x[
                        "day_sales_value_section"] == "S∈(3,5]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(3,5];N∈(40,∞);M∈(100,∞)"
                return A, b

            elif x[f"available_stock"] > 0 and a > k and a > 40 and x["onway_available_sales_days"] > 90 and \
                    x[
                        "day_sales_value_section"] == "S∈(1,3]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(1,3];N∈(40,∞);M∈(90,∞)"
                return A, b
            elif x[f"available_stock"] > 0 and a > k and a > 40 and x['onway_available_sales_days'] > 80 and \
                    x[
                        'day_sales_value_section'] == "S∈(0.6,1]":
                A = a - 40
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.6,1];N∈(40,∞);M∈(80,∞)"
                return A, b
            elif x[f"available_stock"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 80 and \
                    x["day_sales_value_section"] == "S∈(0.3,0.6]":
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.3,0.6];N∈(30,∞);M∈(80,∞)"
                return A, b
            elif x[f"available_stock"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 70 and \
                    x[
                        "day_sales_value_section"] == "S∈(0.1,0.3]":
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈(0.1,0.3];N∈(30,∞);M∈(70,∞)"
                return A, b

            elif x[f"available_stock"] > 0 and a > k and a > 30 and x["onway_available_sales_days"] > 70 and (
                    x["day_sales_value_section"] == "S∈(0,0.1]" or x["day_sales_value_section"] == "S=0"):
                A = a - 30
                if j == 0:
                    b = f"存在对应收仓租天数库龄存在X的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"
                elif j == -30:
                    b = f"存在对应收仓租天数库龄存在X-30的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"
                else:
                    b = f"存在对应收仓租天数库龄存在X+{j}的库存，且超出数量可售天数>{k}天;S∈[0,0.1];N∈(30,∞);M∈(70,∞)"

                return A, b


def gradient_also(df):
    for i in [60, 90, 120]:
        # 2021/11/03对于回调部分增加要求销量>=1
        df.loc[(df['day_sales'] >= 1) & (
                    (df["charge_days"] == i) & (df["gradient"].isnull()) & (df[f"inv_age_{i}_plus_days"] == 0)) &
               (((df["estimated_sales_days"] > 15) & (df["estimated_sales_days"] < 25)) | (
                       (df["onway_available_sales_days"] > 25) &
                       (df[
                            "onway_available_sales_days"] < 35))), "gradient"] = "在库库存可售天数，N∈(15,25) or 在库库存+在途库存可售天数，M∈(25,35)"
        df.loc[(df['day_sales'] >= 1) & (
                    (df["charge_days"] == i) & (df["gradient"].isnull()) & (df[f"inv_age_{i}_plus_days"] == 0)) &
               (((df["estimated_sales_days"] > 15) & (df["estimated_sales_days"] < 25)) | (
                       (df["onway_available_sales_days"] > 15) &
                       (df[
                            "onway_available_sales_days"] < 25))), "gradient"] = "在库库存可售天数，N∈(15,25) or 在库库存+在途库存可售天数，M∈(15,25)"
        df.loc[(df['day_sales'] >= 1) & (
                    (df["charge_days"] == i) & (df["gradient"].isnull()) & (df[f"inv_age_{i}_plus_days"] == 0)) &
               (((df["estimated_sales_days"] > 0) & (
                       df["estimated_sales_days"] < 15))), "gradient"] = "在库库存可售天数，N∈(0,15)"

        # 20230703 针对 GB-JY37362-01 先回调观察效果
        df.loc[(df['day_sales'] >= 1) & 
               ((df["charge_days"] == i) & (df["gradient"].isnull()) & (df[f"inv_age_{i+60}_plus_days"] == 0)) &
               (((df["estimated_sales_days"] > 0) & (df["estimated_sales_days"] < 15))),
               "gradient"] = "在库库存可售天数，N∈(0,15)"
        # 20230703 存在逻辑遗漏：  N∈(15,30)时希望section=0.04
        df.loc[(df['day_sales'] >= 1) & 
               ((df["charge_days"] == i) & (df["gradient"].isnull()) & (df[f"inv_age_{i}_plus_days"] == 0)) &
               (((df["estimated_sales_days"] >= 15) & (df["estimated_sales_days"] < 30))),
               "gradient"] = "在库库存可售天数，N∈[15,30)"
        # 20230731 对高销的SKU补充回调逻辑：只看日销数据和可售天数
        df.loc[(df['day_sales'] >= 15) & (((df["estimated_sales_days"] >= 15) & (df["estimated_sales_days"] < 30))),
               "gradient"] = "在库库存可售天数，N∈[15,30)"
        df.loc[(df['day_sales'] >= 15) & (((df["estimated_sales_days"] > 0) & (df["estimated_sales_days"] < 15))),
               "gradient"] = "在库库存可售天数，N∈(0,15)"
    return df


def is_old_sku():
    # sql_pd2 = GetFromSql(database="yibai_product", host="139.9.206.7", port="3306", username="whpro",
    #                      password="ki&#qXxzkRdgz")
    sql1 = """select aa.sku,gg.name,'老品'as old
            from yibai_product.yb_product_stock_log aa
            inner join yibai_logistics.yibai_warehouse bb on aa.warehouse_code=bb.warehouse_code
            inner join yibai_logistics.yibai_warehouse_category  gg
            on bb.ebay_category_id=gg.id
            where aa.created_at<=date_sub(CURDATE(),interval 180 day)
            and type=8
            """
    old = sql_to_pd(database="yibai_product", data_sys='数据管理部同步服务器', sql=sql1)
    sql2 = """select distinct a.sku,g.name,a.created_at
            from yibai_product.yb_product_stock_log a
            inner join yibai_logistics.yibai_warehouse b on a.warehouse_code=b.warehouse_code
            inner join yibai_logistics.yibai_warehouse_category g
            on g.id=b.ebay_category_id
            where a.created_at>=date_sub(CURDATE(),interval 180 day)
            and type=8
        """
    daohuo = sql_to_pd(database="yibai_product", data_sys='数据管理部同步服务器', sql=sql2)
    daohuo = daohuo.merge(old, how="left", on=["sku", "name"])
    daohuo["old"].fillna("新品", inplace=True)
    daohuo.drop_duplicates(subset=["sku", "name"], inplace=True)
    daohuo.rename(columns={"sku": "SKU", "name": "warehouse"}, inplace=True)
    return daohuo


def age_new(df):
    # sql=f"""select * from over_sea.over_sea_age_new where DATE='{datetime.date.today()}'"""
    # df=sql_to_pd(sql=sql,database="over_sea",data_sys="海外仓调价库")
    df_new = is_old_sku()
    df = df.merge(df_new, on=["SKU", "warehouse"], how="left")
    df["is_new"] = None
    df.loc[df['old'] == "新品", "is_new"] = "是"
    df["is_new"].fillna("否", inplace=True)
    df.drop(["created_at", "old"], axis=1, inplace=True)
    print(df.head())
    return df


def fun4(x):
    A = 0
    if x["day_sales"] >= 0 and x["day_sales"] <= 0.1 and x["estimated_sales_days"] > 30:
        A = x["estimated_sales_days"] - 30
    elif x["day_sales"] > 0.1 and x["day_sales"] <= 0.3 and x["estimated_sales_days"] > 30 and x[
        'onway_available_sales_days'] > 60:
        A = x["estimated_sales_days"] - 30

    elif x["day_sales"] > 0.3 and x["day_sales"] <= 1 and x["estimated_sales_days"] > 30 and x[
        'onway_available_sales_days'] > 70:
        A = x["estimated_sales_days"] - 30
    elif x["day_sales"] > 1 and x["day_sales"] <= 3 and x["estimated_sales_days"] > 40 and x[
        'onway_available_sales_days'] > 80:
        A = x["estimated_sales_days"] - 40

    elif x["day_sales"] > 3 and x["day_sales"] <= 5 and x["estimated_sales_days"] > 50 and x[
        'onway_available_sales_days'] > 90:
        A = x["estimated_sales_days"] - 50
    elif x["day_sales"] > 5 and x["estimated_sales_days"] > 60 and x['onway_available_sales_days'] > 100:
        A = x["estimated_sales_days"] - 60

    return A


def fun5(x):
    if x > 0 and x < 30:
        return 'A∈(0,30)'
    elif x >= 30 and x < 60:
        return 'A∈[30,60)'
    elif x >= 60 and x < 90:
        return 'A∈[60,90)'
    elif x >= 90 and x < 150:
        return 'A∈[90,150)'
    elif x >= 150 and x < 300:
        return 'A∈[150,300)'
    elif x >= 300:
        return 'A∈[300,+∞)'


def delete_data(conn):
    sql = f"delete from over_sea.over_sea_age_new  where DATE='{datetime.date.today().isoformat()}'"
    conn.execute(sql)


def oversea_age_new():
    try:
        conn = connect_to_sql(database="over_sea", data_sys='数据部服务器')
        delete_data(conn)
        all_data = oversea_age_run(conn)
        conn.close()
    except Exception as e:
        send_msg('动销组定时任务推送', '海外仓oversea_age_new',
                 f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}计算海外仓库龄出现问题,请及时排查,失败原因详情请查看airflow",
                 mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False,
                 status='失败')
        raise Exception(traceback.format_exc())
    else:
        if all_data < 80000:
            send_msg('动销组定时任务推送', '海外仓oversea_age_new',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓库龄总数量为{all_data},数据量异常,请检查!",
                     mobiles=['+86-15872366806', '+86-13667213671', '+86-13419546972', '+86-18986845950'], is_all=False)
            raise Exception(f'海外仓库龄总数量为{all_data},数据量异常,请检查!')
        else:
            send_msg('动销组定时任务推送', '海外仓oversea_age_new',
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}海外仓库龄计算完成,总量为{all_data}",
                     is_all=False)


if __name__ == "__main__":
    oversea_age_new()
