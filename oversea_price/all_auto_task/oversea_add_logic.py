"""
海外仓调价补充逻辑汇总程序：
1、高PCS降价逻辑
2、供应商货盘sku

"""
import time, datetime
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list,chicun_zhongliang
import warnings
warnings.filterwarnings("ignore")
# ======================== 1. 高pcs降价逻辑 配置规则 ========================
PRICING_RULES = {
    (0.5, 1): {(0, 60): 5, (60, 80): 3, (80, 100): 2, (100, 150): 0, (150, 200): -1, (200, 300): -2, (300, float('inf')): -3},
    (0.3, 0.5): {(60, 80): 0, (80, 100): 0, (100, 150): -1, (150, 200): -1, (200, 300): -2, (300, float('inf')): -3},
    (0.1, 0.3): {(100, 200): -1.5, (200, 300): -2.5, (300, float('inf')): -3},
    (0, 0.1): {(300, float('inf')): -4}
}

BASIC_CONFIG = {
    "first_entry_stock_qty": 50,
    "first_entry_stock_age": 30,
    "price_adjust_interval": 6,
    "max_single_increase": 5,
    "first_entry_sales": 0.15
}
# ========================  ========================
def preprocess_data(df):
    """
    数据预处理：生成分箱标签、调价间隔校验、首次进入条件校验
    """

    df = df.copy()  # 避免修改原数据

    # 2.1 计算上次调价天数差
    df["最近降价日期"] = pd.to_datetime(df["最近降价日期"], errors="coerce")
    today = pd.Timestamp.now()
    df["调价天数差"] = (today - df["最近降价日期"]).dt.days
    df["调价天数差"] = df["调价天数差"].fillna(-1)  # 空值视为-1（允许调价）
    df['最近降价日期'] = df['最近降价日期'].dt.strftime('%Y-%m-%d')
    # 2.2 校验调价间隔
    df["满足调价间隔"] = (df["调价天数差"] >= BASIC_CONFIG["price_adjust_interval"]) | (df["调价天数差"] == -1)

    # 2.3 校验首次进入条件
    c1 = (df["available_stock"] >= BASIC_CONFIG["first_entry_stock_qty"]) & \
         (df["overage_level"] >= BASIC_CONFIG["first_entry_stock_age"]) & \
         (df["day_sales"] <= BASIC_CONFIG["first_entry_sales"]) & (df["after_profit"]==0)
    df['首次进入满足条件'] = np.where(c1, 1, 0)
    c1 = (df['首次进入满足条件']==1) & (df['最近降价日期'].isna())
    df['是否首次进入'] = np.where(c1, 1, 0)
    df['首次进入时间'] = np.where(df['是否首次进入']==1, time.strftime('%Y-%m-%d'), df['首次进入时间'])

    # 2.4 校验不降价规则
    df["15days_sales"] = df["15days_sales"].fillna(0)
    df["不降价标记"] = (df["7days_sales"] / df["15days_sales"] >= 0.6).where(df["15days_sales"] != 0, False)

    # 2.5 分箱（对应PRICING_RULES的区间，左开右闭）
    sales_bins = [0, 0.1, 0.3, 0.5, 1.0, float('inf')]
    sales_labels = ["[0,0.1]", "[0.1,0.3]", "[0.3,0.5]", "[0.5,1]", "[1,inf)"]
    days_bins = [0, 60, 80, 100, 150, 200, 300, float('inf')]
    days_labels = ["(0,60]", "(60,80]", "(80,100]", "(100,150]", "(150,200]", "(200,300]", "(300,inf)"]

    df["销量区间"] = pd.cut(
        df["day_sales"], bins=sales_bins, labels=sales_labels, right=False, include_lowest=False
    )

    df["可售天数区间"] = pd.cut(
        df["estimated_sales_days"], bins=days_bins, labels=days_labels, right=True, include_lowest=False
    )

    return df

def build_rule_map():
    """
    构建规则映射字典：(销量区间标签, 可售天数区间标签) → 涨跌幅/特殊标记
    从PRICING_RULES自动生成，无需手动维护
    """
    rule_map = {}
    # 销量区间映射：(min, max) → 区间标签（和分箱标签一致）
    sales_range_to_label = {
        (0, 0.1): "[0,0.1]",
        (0.1, 0.3): "[0.1,0.3]",
        (0.3, 0.5): "[0.3,0.5]",
        (0.5, 1): "[0.5,1]",
        (1, float('inf')): "[1,inf)"
    }
    # 可售天数区间映射：(min, max) → 区间标签
    days_range_to_label = {
        (0, 60): "(0,60]",
        (60, 80): "(60,80]",
        (80, 100): "(80,100]",
        (60, 100): "(60,100]",
        (100, 150): "(100,150]",
        (100, 200): "(100,200]",
        (150, 200): "(150,200]",
        (200, 300): "(200,300]",
        (300, float('inf')): "(300,inf)"
    }

    # 遍历配置规则，生成映射字典
    for sales_range, days_rules in PRICING_RULES.items():
        sales_label = sales_range_to_label[sales_range]
        for days_range, adjust_val in days_rules.items():
            days_label = days_range_to_label[days_range]
            rule_map[(sales_label, days_label)] = adjust_val

    # 补充默认规则：未匹配到的情况（退出动销，涨5%）
    rule_map["default"] = 0

    return rule_map


def get_high_pcs_logic(df):
    """ 真实数据验证 """
    # sql = """
    #     SELECT *
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = '2025-11-21' and available_stock > 0
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)
    # df['15days_sales'] = 0
    # df['7days_sales'] = 0
    # 获取上一期的数据：最近降价日期、是否首次进入
    date_today = time.strftime('%Y-%m-%d')
    sql = f"""
        SELECT sku, warehouse, `首次进入时间`, `最近降价日期`
        FROM over_sea.dwm_sku_add_logic
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_add_logic WHERE date_id < '{date_today}')
        and `是否退出高pcs降价逻辑` != 1
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_history = conn.read_sql(sql)
    if len(df_history) == 0:
        df_history = pd.DataFrame(columns=['sku', 'warehouse', '首次进入时间', '最近降价日期'])
    df = pd.merge(df, df_history, how='left', on=['sku','warehouse'])
    # df['最近降价日期'] = '2025-11-01'
    # df['是否首次进入'] = False
    # df = df.rename(columns={'15days_sales':'近15天销量','7days_sales':'近7天销量'})
    print(f"数据量：{len(df):,} 行")
    print("开始计算...")
    start_time = datetime.now()

    df_result = high_pcs_rule(df)

    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"计算完成！耗时：{elapsed_time:.2f} 秒")

    # # 存表
    df_result = df_result[(~df_result['最近降价日期'].isna()) & (df_result['最近降价日期'] != 'nan')]
    df_result['date_id'] = time.strftime('%Y-%m-%d')
    # print(df_result.info())
    write_to_sql(df_result, 'dwm_sku_add_logic')
    # conn.to_sql(df_result, 'dwm_sku_add_logic', if_exists='replace')
    # df_result.to_excel('F://Desktop//df_result.xlsx', index=0)

    # 返回
    df.drop(['首次进入时间', '最近降价日期'], axis=1, inplace=True)
    df = pd.merge(df, df_result[['sku','warehouse', 'after_profit_new', 'lowest_profit_new']],
                  how='left', on=['sku','warehouse'])
    df['after_profit'] = np.where(df['after_profit_new'].isna(), df['after_profit'], df['after_profit_new'])
    df['lowest_profit'] = np.where(df['lowest_profit_new'].isna(), df['lowest_profit'], df['lowest_profit_new'])
    # 20251117 暂时用is_new = 2字段筛选哪些是高pcs降价
    df['is_new'] = np.where(df['after_profit_new'].isna(), df['is_new'], 2)

    return df

def write_to_sql(df, table_name):
    """
    将中间表数据写入mysql
    """

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_id = time.strftime('%Y-%m-%d')
    # df10 = df[df.index < 10000]
    # conn.to_sql(df10, table_name, if_exists='append')

    # sql = f"""
    # delete from {table_name}
    # """
    # conn.execute(sql)

    sql = f"""
    delete from {table_name} where date_id='{date_id}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

    conn.close()


def write_to_ck(df, table_name):
    """
    将中间表数据写入ck
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    date_id = time.strftime('%Y-%m-%d')

    sql = f"""
    ALTER TABLE yibai_oversea.{table_name} DELETE where date_id = '{date_id}'
    """
    conn_ck.ck_execute_sql(sql)
    # 确认当天日期数据已删除
    n = 1
    while n < 5:
        print(f'删除当前表里的数据，第{n}次测试...')
        sql = f"""
            SELECT count()
            FROM yibai_oversea.{table_name}
            where date_id = '{date_id}'
        """
        df_cnt = conn_ck.ck_select_to_df(sql)
        if df_cnt.iloc[0,0] == 0:
            print('结果表删除成功！')
            conn_ck.ck_insert(df, table_name, if_exist='append')
            break
        else:
            n += 1
            time.sleep(60)
    if n == 5:
        print('备份CK失败，当天数据未删除完成，CK未备份')

def get_sale_status():
    """ 海外仓sku销售状态 """
    sql = """
        SELECT sku, warehouse, sale_status
        FROM over_sea.oversea_sale_status
        WHERE end_time is Null
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df


def high_pcs_rule(df):
    """ 海外仓高pcs降价逻辑 """
    # 1. 处理数据
    # 补充销售状态
    df_sale_status = get_sale_status()
    df = pd.merge(df, df_sale_status, how='left', on=['sku','warehouse'])
    df['sale_status'] = df['sale_status'].fillna('正常')
    df = preprocess_data(df)
    # 2. 匹配规则
    rule_map = build_rule_map()
    df["匹配键"] = list(zip(df["销量区间"], df["可售天数区间"]))
    df["涨跌幅"] = rule_map["default"]  # 全局默认值
    condition = (df["day_sales"] < 1) & (df["available_stock"] > 30)
    df.loc[condition, "涨跌幅"] = df.loc[condition, "匹配键"].map(
        lambda x: rule_map.get(x, rule_map["default"])  # 满足条件时匹配规则，未匹配到仍用默认
    )
    # 3. 处理额外逻辑
    df["after_profit_new"] = df["after_profit"] + df["涨跌幅"]/100

    # 3.1 条件过滤：不满足调价间隔/首次进入条件 → 保持原利润率
    keep_original_mask = ~df["满足调价间隔"] | (df["首次进入满足条件"] == 0)
    df.loc[keep_original_mask, "after_profit_new"] = df.loc[
        keep_original_mask, "after_profit"
    ]
    # 3.2 条件过滤：降价时若触发不降价规则 → 保持原利润率
    price_down_mask = (df["涨跌幅"] < 0) & df["不降价标记"]
    df.loc[price_down_mask, "after_profit_new"] = df.loc[
        price_down_mask, "after_profit"
    ]

    # 4. 最近降价日期
    c1 = (~df['不降价标记']) & (df['满足调价间隔']) & (df['首次进入满足条件']==1)
    c2 = (df['是否首次进入'])
    df['本次是否调价'] = np.where(c1|c2, 1, 0)
    df['最近降价日期'] = np.where(df['本次是否调价']==1, time.strftime('%Y-%m-%d'), df['最近降价日期'])
    df['最近降价日期'] = df['最近降价日期'].replace('nan', np.nan)
    c1 = (df['day_sales']>=1) | (df['available_stock']<=30)
    c2 = ~df['最近降价日期'].isna()
    df['是否退出高pcs降价逻辑'] = np.where(c1 & c2, 1, 0)
    df['after_profit_new'] = np.where(df['是否退出高pcs降价逻辑']==1, 0, df['after_profit_new'])

    # 4. 返回结果
    df['lowest_profit_new'] = 0.02
    result_cols = [
        "sku", "warehouse", "available_stock", "overage_level", "sale_status", "day_sales", "estimated_sales_days",
        "是否首次进入", "首次进入满足条件", '首次进入时间', "销量区间","可售天数区间", "涨跌幅", "不降价标记", "after_profit", "after_profit_new",
        "满足调价间隔","最近降价日期","本次是否调价","是否退出高pcs降价逻辑","lowest_profit_new"
    ]
    df_result = df[result_cols].copy()

    # df_result.to_excel('F://Desktop//df_result.xlsx', index=0)

    return df_result

def oversea_add_logic():
    """ 海外仓补充逻辑汇总 """
    pass

def get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    return df

def tt_get_platform_fee():
    """
    获取配置表：平台费率、差值等
    """
    sql = f"""
    SELECT 
        platform, site as country, pay_fee+paypal_fee+vat_fee+extra_fee ppve,refound_fee,
        platform_zero, platform_must_percent
    FROM over_sea.tt_yibai_platform_fee
    -- WHERE platform in ('AMAZON', 'EB','ALI', 'SHOPEE', 'LAZADA','ALLEGRO')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    sql = """
        SELECT platform_code platform, site country, toFloat32(net_profit2)/100 as net_profit2,
        toFloat32(net_interest_rate_target)/100 as net_interest_rate_target
        FROM tt_sale_center_listing_sync.tt_listing_profit_config
        WHERE shipping_type = 2 and is_del = 0 and status = 1
    """
    conn_ck = pd_to_ck(database='tt_prod_base_sync', data_sys='通拓-新')
    df_pc = conn_ck.ck_select_to_df(sql)

    # amazon按站点更新
    df = pd.merge(df, df_pc[df_pc['platform']=='AMAZON'], how='left', on=['platform', 'country'])
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df['platform_must_percent'] = np.where(df['net_interest_rate_target'].isna(), df['platform_must_percent'], df['net_interest_rate_target'])
    df.drop(['net_profit2','net_interest_rate_target'], axis=1, inplace=True)

    # eb\ali按平台更新
    col = ['platform','net_profit2','net_interest_rate_target']
    df = pd.merge(df, df_pc[df_pc['platform'].isin(['EB','ALI','ALLEGRO', 'SHOPEE', 'LAZADA','TEMU'])][col], how='left', on=['platform'])
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df['platform_must_percent'] = np.where(df['net_interest_rate_target'].isna(), df['platform_must_percent'], df['net_interest_rate_target'])
    df.drop(['net_profit2','net_interest_rate_target'], axis=1, inplace=True)

    # df.to_excel('F://Desktop//df_tt.xlsx', index=0)

    return df


##################### 2. 供应商货盘sku补充逻辑 ##############################

def cloud_stock_age():
    """ 云仓库存库龄数据 """
    # 云仓库龄
    sql = """
    SELECT
        sku, cargo_owner_id, warehouse_name, real_warehouse_name,warehouse_id, 
        sum(age_stock) warehouse_stock, max(stock_age) max_age,
        max(overage_level) overage_level, sum(age_30_plus) age_30_plus, sum(age_60_plus) age_60_plus, sum(age_90_plus) age_90_plus,
        sum(age_120_plus) age_120_plus, sum(age_150_plus) age_150_plus, sum(age_180_plus) age_180_plus,
        sum(age_270_plus) age_270_plus, sum(age_360_plus) age_360_plus,
        arrayStringConcat(groupArray(stock_info), ',') AS warehouse_stock_age
    FROM (
        SELECT
            sku, warehouse_name, real_warehouse_name, warehouse_id, cargo_owner_id,  age_stock, stock_age,
            concat(toString(age_stock), ':', toString(stock_age)) as stock_info,
            case
                when stock_age >= 30 and stock_age < 60 then 30
                when stock_age >= 60 and stock_age < 90 then 60
                when stock_age >= 90 and stock_age < 120 then 90
                when stock_age >= 120 and stock_age < 150 then 120
                when stock_age >= 150 and stock_age < 180 then 150
                when stock_age >= 180 and stock_age < 270 then 180
                when stock_age >= 270 and stock_age < 360 then 270
                when stock_age >= 360 then 360
            else 0 end as overage_level, 
            case when stock_age >= 30 then age_stock else 0 end as age_30_plus,       
            case when stock_age >= 60 then age_stock else 0 end as age_60_plus,
            case when stock_age >= 90 then age_stock else 0 end as age_90_plus,
            case when stock_age >= 120 then age_stock else 0 end as age_120_plus,
            case when stock_age >= 150 then age_stock else 0 end as age_150_plus,
            case when stock_age >= 180 then age_stock else 0 end as age_180_plus,
            case when stock_age >= 270 then age_stock else 0 end as age_270_plus,
            case when stock_age >= 360 then age_stock else 0 end as age_360_plus
        FROM (
            SELECT
                yw0.warehouse_name `warehouse_name`,
                yw0.id `warehouse_id`,
                yw2.warehouse_name `real_warehouse_name`,
                -- ifnull(yw2.name, yw.name) `real_warehouse_name`,
                ysd.cargo_owner_id `cargo_owner_id`,
                ysd.sku sku,
                receipt_quantity - delivery_quantity `age_stock`,
                dateDiff('day', date(receipt_time), today()) `stock_age`,
                '云仓' `data_source`
            FROM yb_stock_center_sync.yb_stockage_detail ysd
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse yw0 ON ysd.warehouse_id = yw0.id
            LEFT JOIN yb_datacenter.yb_warehouse yw ON ysd.warehouse_id = yw.id
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse yw2 on yw.real_warehouse_id = yw2.id
            WHERE
                yw.`type` IN ('overseas', 'third')
                AND receipt_quantity - delivery_quantity>0
        ) a
    ) b
    GROUP BY sku, cargo_owner_id, real_warehouse_name, warehouse_name, warehouse_id
    """
    conn_ck = pd_to_ck(database='yb_stock_center_sync', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df['warehouse_id'] = df['warehouse_id'].astype(int)
    df['warehouse_stock_age'] = (
            df['warehouse_id'].astype(str) + ":{'" + df['warehouse_stock_age'] + "'}")
    # df_cloud_age.to_excel('F://Desktop//df_cloud_age.xlsx', index=0)

    return df

def get_sup_yb_sku():
    """ 获取供应商sku映射yb的sku """
    # 1、取供应商货盘sku映射表
    # df_sku = pd.read_excel('F://Desktop//dcm_product_map_relation_management_202510301114.xlsx')
    sql = """
        SELECT distinct merchant_sku, similar_sku, similar_merchant_id
        FROM yibai_dcm_base_sync.dcm_product_map_relation_management
        WHERE is_del = 0
    """
    conn_ck = pd_to_ck(database='yibai_dcm_base_sync', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    df_sku = df_sku[['merchant_sku','similar_sku','similar_merchant_id']]
    df_sku.columns = ['YB_sku', 'YM_sku', 'similar_merchant_id']

    return df_sku

def get_sup_sku():
    """ 获取供应商sku信息 """
    # 取供应商货盘sku
    conn_ck = pd_to_ck(database='yibai_prod_base_sync', data_sys='调价明细历史数据')

    sql = """
        SELECT sku YM_sku, country_code country, warehouse_price
        FROM yibai_prod_base_sync.yibai_prod_sku_pallet_price
        WHERE is_del = 0 and country_code != 'CN' and sku like '%YM%'
    """
    df_sup_cost = conn_ck.ck_select_to_df(sql)
    df_sup_cost['country'] = df_sup_cost['country'].replace('CZ', 'DE')
    df_sup_cost = df_sup_cost.sort_values(by='warehouse_price', ascending=False).drop_duplicates(subset=['YM_sku', 'country'])
    df_sup_cost['warehouse'] = df_sup_cost['country'].replace({'CZ':'德国仓','DE':'德国仓','US':'美国仓','GB':'英国仓'})

    # df_sup_cost.to_excel('F://Desktop//df_sup_sku.xlsx', index=0)

    return df_sup_cost

# sku+子仓维度。同时存表备份
def get_sup_stock_id():
    """ 获取供应商sku库存信息 """
    # 0、取sku
    df_sku = get_sup_sku()
    df_sku = df_sku[['YM_sku','country','warehouse_price','warehouse']]
    sku_list = tuple(df_sku['YM_sku'].unique())

    # 1、取库存数据
    date_today = time.strftime('%Y%m%d')
    sql = f"""
        SELECT
            ps.sku YM_sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
            yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
            available_stock, warehouse_stock, cargo_owner_id
        FROM (
            SELECT 
                trim(sku) sku, warehouse_id, date_id, cargo_owner_id, redundancy_stock available_stock,
                concat(toString(warehouse_id),':',toString(redundancy_stock)) as warehouse_stock
            FROM yb_datacenter.yb_stock
            WHERE 
                date_id = '{date_today}' -- 根据需要取时间
        ) AS ps
        INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
        WHERE         
            yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
            and sku in {sku_list}
            and available_stock > 0

    """
    sql = f"""
            SELECT
                ps.sku YM_sku, toString(toDate(toString(date_id))) date_id,  yw.id AS warehouse_id,
                yw.warehouse_name AS warehouse_name, yw.warehouse_code AS warehouse_code, ywc.name AS warehouse,
                available_stock, warehouse_stock, cargo_owner_id
            FROM (
                SELECT 
                    trim(sku) sku, warehouse_id, formatDateTime(now(), '%Y-%m-%d') date_id, cargo_owner_id, 
                    redundancy_stock available_stock,
                    concat(toString(warehouse_id),':',toString(redundancy_stock)) as warehouse_stock
                FROM yb_stock_center_sync.yb_mlcode_stock
            ) AS ps
            INNER JOIN yibai_logistics_tms_sync.yibai_warehouse yw ON ps.warehouse_id = yw.id
            LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category ywc ON yw.ebay_category_id = ywc.id
            WHERE         
                yw.warehouse_type in (2,3)    -- 筛选海外仓仓库
                and sku in {sku_list}
                and available_stock > 0

        """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_stock = conn_ck.ck_select_to_df(sql)
    df_stock = df_stock[~df_stock['warehouse_name'].str.contains('独享')]

    # 2、取库龄数据
    df_age = cloud_stock_age()
    df_age.drop(['warehouse_id', 'warehouse_stock'], axis=1, inplace=True)
    df_age = df_age.rename(columns={'sku':'YM_sku'})
    # 3、合并数据（取有共享库存的数据）
    df = pd.merge(df_stock, df_age, how='inner', on=['YM_sku', 'warehouse_name', 'cargo_owner_id'])

    df = pd.merge(df_sku, df, how='left', on=['YM_sku', 'warehouse'])
    df['date_id'] = time.strftime('%Y-%m-%d')
    df['available_stock'] = df['available_stock'].fillna(0).astype(int)
    # df.to_excel('F://Desktop//df_sup_detail_id.xlsx', index=0)

    # 4、子仓维度库存数据存表备份
    # write_to_sql(df, 'dwd_supplier_sku_stock_id')

    col = ['cargo_owner_id','max_age','overage_level','age_30_plus','age_60_plus',
           'age_90_plus','age_120_plus','age_150_plus','age_180_plus','age_270_plus','age_360_plus']
    df[col] = df[col].fillna(0).astype(int)
    print(df.info())
    # 存ck
    write_to_ck(df, 'dwd_supplier_sku_stock_id')


    return df

def parse_stock_age(series):
    stock_age_dict = {}
    for item in series:
        if pd.isna(item) or not isinstance(item, str) or item.strip() == "":
            continue
        try:
            parts = item.split("|")[-1].strip()
            wh_id, sa_str = parts.split(":", 1)
            wh_id = wh_id.strip()
            sa_str = sa_str.strip().replace("{", "").replace("}", "").replace("'", "").replace('"', "").strip()
            sa_key, sa_val = sa_str.split(":", 1)
            sa_key, sa_val = sa_key.strip(), sa_val.strip()
            stock_age_dict[wh_id] = {sa_key: sa_val}
        except Exception as e:
            # 调试用，正式环境可注释
            # print(f"跳过异常数据：{item}，错误：{e}")
            continue
    return stock_age_dict

# 聚合为sku+warehouse维度
def get_sup_dwm():
    """ 供应商sku信息汇总 dwm """
    # 0、取sku
    df_sku = get_sup_sku()
    df_sku = df_sku[['YM_sku', 'warehouse_price','warehouse']]
    # 1、取库存库龄
    sql = """
        SELECT *
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
        and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # 2、按大仓聚合
    df = df.sort_values(
        by=["YM_sku", "warehouse", "available_stock"],  # 按sku、warehouse分组，再按库存降序
        ascending=[True, True, False]
    )

    result = df.groupby(["YM_sku", "warehouse", "date_id"], as_index=False).agg(
        warehouse_id=("warehouse_id", "first"),  # 取库存最多的warehouse_id
        warehouse_name=("warehouse_name", "first"),  # 取库存最多的warehouse_id
        warehouse_code=("warehouse_code", "first"),  # 取库存最多的warehouse_id
        available_stock=("available_stock", "sum"),  # 求和
        max_age=("max_age", "max"),  # 取最大值
        overage_level=("overage_level", "max"),  # 取最大值
        cargo_owner_id=("cargo_owner_id", "max"),  # 取最大值
        age_30_plus=("age_30_plus", "sum"),
        age_60_plus=("age_60_plus", "sum"),
        age_90_plus=("age_90_plus", "sum"),
        age_120_plus=("age_120_plus", "sum"),
        age_150_plus=("age_150_plus", "sum"),
        age_180_plus=("age_180_plus", "sum"),
        age_270_plus=("age_270_plus", "sum"),
        age_360_plus=("age_360_plus", "sum"),
        warehouse_stock_age=("warehouse_stock_age", parse_stock_age)  # 自定义解析为字典
    )

    df = pd.merge(df_sku, result, how='left', on=['YM_sku', 'warehouse'])

    # 4、数据处理
    df['date_id'] = time.strftime('%Y-%m-%d')
    col = ['available_stock', 'max_age', 'overage_level', 'age_30_plus', 'age_60_plus', 'age_90_plus', 'age_120_plus',
           'age_150_plus', 'age_180_plus', 'age_270_plus', 'age_360_plus']
    df[col] = df[col].fillna(0)
    print(df.info())
    # df.to_excel('F://Desktop//df_sup_detail.xlsx', index=0)

    return df

def get_sup_useful_fee():
    """ 供应商sku运费数据 """
    # 取sku及子仓库存（用于取运费子仓）
    # df_sku = get_sup_sku()
    sql = """
        SELECT distinct YM_sku, warehouse_name best_warehouse_name, available_stock
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
        and warehouse_id is not Null
        -- and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    sku_list = tuple(df_sku['YM_sku'].unique())

    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = f"""
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id,  
        case when firstCarrierCost <= 0.01 then totalCost else totalCost_origin end as total_cost, 
        (totalCost_origin - firstCarrierCost - dutyCost) shippingCost,
        case when firstCarrierCost <= 0.01 then new_firstCarrierCost else firstCarrierCost end as firstCarrierCost, 
        dutyCost, shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful
    WHERE platform not in ('WISH') and sku in {sku_list}
    UNION ALL
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id,  
        case when firstCarrierCost <= 0.01 then totalCost else totalCost_origin end as total_cost, 
        (totalCost_origin - firstCarrierCost - dutyCost) shippingCost,
        case when firstCarrierCost <= 0.01 then new_firstCarrierCost else firstCarrierCost end as firstCarrierCost, 
        dutyCost, shipName as ship_name,lowest_price, platform, country, warehouse
    FROM oversea_transport_fee_useful_temu
    WHERE sku in {sku_list}
    """
    df_transport_fee = conn.read_sql(sql)
    sql = f"""
    SELECT
        sku YM_sku, warehouseName as best_warehouse_name, warehouseId as best_warehouse_id, totalCost as total_cost, 
        (totalCost - firstCarrierCost - dutyCost) shippingCost,
        firstCarrierCost, dutyCost,
        shipName as ship_name,0 lowest_price, platform, shipCountry country, warehouse
    FROM yibai_oversea.oversea_transport_fee_supplier a
    LEFT JOIN (
        SELECT  warehouse_name, b.name warehouse
        FROM yibai_logistics_tms_sync.yibai_warehouse a
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category b ON a.ebay_category_id = b.id
        WHERE warehouse_type IN (2,3,8)
    ) b ON a.warehouseName = b.warehouse_name
    WHERE sku in {sku_list} and date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_supplier)
    """
    df_intf_fee = conn_ck.ck_select_to_df(sql)

    df_transport_fee['source'] = 1
    df_intf_fee['source'] = 2
    df_transport_fee = pd.concat([df_transport_fee, df_intf_fee])
    df_transport_fee = df_transport_fee.sort_values(by='source', ascending=True).\
        drop_duplicates(subset=['YM_sku','best_warehouse_name','platform','country'])
    df_transport_fee.drop('source', axis=1, inplace=True)

    df = pd.merge(df_transport_fee, df_sku, how='left', on=['YM_sku', 'best_warehouse_name'])
    df['available_stock'] = df['available_stock'].fillna(0).astype(int)
    # 取子仓下最便宜渠道
    df = df[~df['best_warehouse_name'].str.contains('精品')]
    df_1 = df[df['available_stock'] > 0]
    df_1 = df_1.sort_values(by='shippingCost', ascending=True)
    df_1 = df_1.drop_duplicates(subset=['YM_sku','warehouse','platform','country'], keep='first')
    df_2 = df[df['available_stock'] <= 0]
    df_2 = df_2.sort_values(by='shippingCost', ascending=True)
    df_2 = df_2.drop_duplicates(subset=['YM_sku','warehouse','platform','country'], keep='first')
    df = pd.concat([df_1, df_2])
    df = df.sort_values(by='available_stock', ascending=False).drop_duplicates(
        subset=['YM_sku','warehouse','platform','country'], keep='first')

    # 20260317 供应商sku计费重替换
    df = replace_sku_cw(df)

    # df.to_excel('F://Desktop//df_sup_fee.xlsx', index=0)

    return df

def replace_sku_cw(df):
    """ 按亿迈指定头程计费重，替换头程费用 """
    sql = """
        SELECT YM_sku, warehouse, `亿迈指定计费重`, `亿迈指定头程方式`, `最终头程`
        FROM yibai_oversea.temp_sup_sku_cw
        WHERE `亿迈指定计费重` > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tc = conn_ck.ck_select_to_df(sql)
    print(df_tc.info())

    df = pd.merge(df, df_tc, how='left', on=['YM_sku','warehouse'])

    c1 = (~df['亿迈指定头程方式'].isna())
    df['total_cost'] = np.where(c1, df['total_cost']-df['firstCarrierCost']+df['最终头程'], df['total_cost'])
    df['firstCarrierCost'] = np.where(c1, df['最终头程'], df['firstCarrierCost'])
    df['firstCarrierCost'] = df['firstCarrierCost'].astype(float)


    df.drop(['亿迈指定计费重','亿迈指定头程方式','最终头程'], axis=1, inplace=True)

    return df

# 供应商sku差值替换
def update_sup_platform_fee(df, org='YB'):
    """ """
    # sql = """
    #     SELECT *
    #     FROM over_sea.yibai_platform_fee
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)

    if org == 'YB':
        sql = f"""
              SELECT platform_code platform, site country, toFloat32(net_profit2)/100 as `net_profit2`
              FROM yibai_sale_center_listing_sync.yibai_listing_profit_config
              WHERE shipping_type = 5 and is_del = 0 and status = 1
              -- and platform_code == 'TEMU'
          """
        conn_ck = pd_to_ck(database='yibai_sale_center_listing_sync', data_sys='调价明细历史数据')
        df_pc = conn_ck.ck_select_to_df(sql)
    elif org == 'TT':
        sql = f"""
              SELECT platform_code platform, site country, toFloat32(net_profit2)/100 as `net_profit2`
              FROM tt_sale_center_listing_sync.tt_listing_profit_config
              WHERE shipping_type = 5 and is_del = 0 and status = 1
              -- and platform_code = 'TEMU'
          """
        conn_ck = pd_to_ck(database='tt_sale_center_listing_sync', data_sys='通拓-新')
        df_pc = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_pc, how='left', on=['platform', 'country'])

    # 差值替换
    # df.loc[df['platform'] == 'CDISCOUNT', 'net_profit2'] = df_pc.loc[df_pc['platform'] == 'CDISCOUNT', 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'EB') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'EB') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'WALMART') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'WALMART') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'AMAZON') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'AMAZON') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'ALI') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'ALI') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'WISH') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'WISH') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]
    df.loc[(df['platform'] == 'TEMU') & (df['net_profit2'].isna()), 'net_profit2'] = df_pc.loc[
        (df_pc['platform'] == 'TEMU') & (df_pc['country'] == 'other'), 'net_profit2'].iloc[0]

    #
    df[['platform_zero', 'net_profit2']] = df[['platform_zero', 'net_profit2']].round(4)
    #
    mismatched_rows = df[df['platform_zero'] != df['net_profit2']]
    print('差值更新的平台和国家：')
    mismatched_rows[['platform', 'country', 'platform_zero', 'net_profit2']].apply(
        lambda x: print(f"Platform: {x['platform']}, country: {x['country']}, "
                        f"原差值：{x['platform_zero']},最新差值：{x['net_profit2']}"), axis=1)

    # 差值替换
    # df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'] - 0.05, df['net_profit2'])
    df['platform_zero'] = np.where(df['net_profit2'].isna(), df['platform_zero'], df['net_profit2'])
    df.drop('net_profit2', axis=1, inplace=True)

    # df.to_excel("F://Desktop//df_sup_pf.xlsx", index = 0)

    return df

def supplier_sku_info():
    """ 供应商sku信息 """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')

    # 1、取库存及库龄（子仓维度）
    get_sup_stock_id()

    # 2、取sku+warehouse维度数据
    df_sku = get_sup_dwm()
    df_yb_sku = get_sup_yb_sku()
    df_sku = pd.merge(df_sku, df_yb_sku[['YM_sku', 'YB_sku']], how='left', on=['YM_sku'])

    # 3、供应商货盘sku接口运费
    get_supplier_fee()

    # 4、匹配调价程序的运费、参数表。计算供应商供货价下的目标净利率定价
    df_transport_fee = get_sup_useful_fee()
    col = ['YM_sku','best_warehouse_name','best_warehouse_id','warehouse','total_cost',
           'shippingCost','firstCarrierCost','dutyCost',
           'ship_name','lowest_price','platform','country']
    df = pd.merge(df_sku, df_transport_fee[col], how='left', on=['YM_sku', 'warehouse'])
    df['warehouse_id'] = np.where(df['best_warehouse_id'].isna(), df['warehouse_id'], df['best_warehouse_id'])
    df['warehouse_name'] = np.where(df['best_warehouse_name'].isna(), df['warehouse_name'], df['best_warehouse_name'])
    col = ['warehouse_price']
    df[col] = df[col].astype(float)
    df.drop(['warehouse_code','best_warehouse_id','best_warehouse_name'], axis=1, inplace=True)
    # df = df.rename(columns={'cargo_owner_id':'similar_merchant_id'})
    # 20260212 库龄补充逻辑（不同库龄的目标净利率不同）
    dic = {360:0, 270:0, 180:0, 150:0.01, 120:0.02, 90:0.03, 60:0.04, 30:0.05, 0:0.05}
    df['target_net_profit'] = df['overage_level'].map(dic).fillna(0.05)
    # 20260227 非二次备货的供应商sku按0净利率定价
    df_sec = get_sup_sku_second_stocking()
    df = pd.merge(df, df_sec[['YM_sku','cargo_owner_id','is_second_stocking']], how='left', on=['YM_sku','cargo_owner_id'])
    df['is_second_stocking'] = df['is_second_stocking'].fillna(0).astype(int)
    c1 = (df['is_second_stocking']==0) & (df['available_stock']>0)
    df['target_net_profit'] = np.where(c1, 0, df['target_net_profit'])
    # df.drop('is_second_stocking', axis=1, inplace=True)
    # df.to_excel('F://Desktop//df_sec_stocking.xlsx', index=0)
    org_col = ['YB','TT']
    for org in org_col:
        if org == 'YB':
            df_platform_fee = get_platform_fee()
            col = ['TEMU', 'TTS']
        elif org == 'TT':
            df_platform_fee = tt_get_platform_fee()
            col = ['TEMU']
        # 更新sup差值
        df_platform_fee = update_sup_platform_fee(df_platform_fee, org=org)
        # 计算定价
        df_yb = pd.merge(df, df_platform_fee, how='left', on=['platform','country'])
        # 覆盖 TEMU 组的匹配结果（双字段匹配失败的行，用 TEMU 单字段数据填充）
        for i in col:
            temu_fee = df_platform_fee[df_platform_fee['platform'] == i].iloc[0]  # 取第一条 TEMU 平台费
            fee_columns = ['ppve', 'refound_fee', 'platform_zero', 'platform_must_percent']
            temu_mask = (df_yb['platform'] == i) & (df_yb['ppve'].isna())
            df_yb.loc[temu_mask, fee_columns] = temu_fee[fee_columns].values

        # df_yb['platform_zero'] = df_yb['platform_zero'] - 0.05
        df_yb['platform_must_percent'] = df_yb['target_net_profit']
        print(df_yb.info())
        # 汇损折旧由 4% 修改为 2%。汇率不打折
        df_yb['ppve'] = df_yb['ppve'] - 0.02
        df_yb['sup_price'] = (df_yb['warehouse_price'] + df_yb['shippingCost'])/(1 - df_yb['ppve'] - df_yb['platform_zero']- df_yb['platform_must_percent'])
        df_yb['date_id'] = time.strftime('%Y-%m-%d')
        col = ['warehouse_id','cargo_owner_id','max_age','overage_level','age_30_plus','age_60_plus',
               'age_90_plus','age_120_plus','age_150_plus','age_180_plus','age_270_plus','age_360_plus']
        df_yb[col] = df_yb[col].fillna(0).astype(int)
        print(df_yb.info())
        # df_yb.to_excel('F://Desktop//df_sup_info.xlsx', index=0)
        df_yb.drop('target_net_profit', axis=1, inplace=True)
        # 7、存表
        if org == 'YB':
            table_name = 'dwm_supplier_sku_price'
        elif org == 'TT':
            table_name = 'tt_dwm_supplier_sku_price'
        write_to_ck(df_yb, table_name)


def get_supplier_price_temp():
    """
    供应商货盘sku定价处理
    返回字段：是否供应商sku、是否使用供应商报价的定价
    """
    sql = """
        SELECT YB_sku sku, YM_sku, warehouse, warehouse_name, country, platform, available_stock sup_stock, warehouse_price, sup_price
        FROM over_sea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_supplier_sku_price)
        -- and sup_price is not Null
        and platform = 'AMAZON' and country in ('US','DE')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_sup_price = conn.read_sql(sql)

    df_sup_price.to_excel('F://Desktop//df_sup_price.xlsx', index=0)

def get_supplier_price(df):
    """
    供应商货盘sku定价处理
    返回字段：是否供应商sku、是否使用供应商报价的定价
    """
    # 1. 获取供应商sku定价信息
    sql = """
        SELECT YB_sku sku, warehouse, country, platform, available_stock sup_stock, warehouse_price new_price_s, 
        total_cost total_cost_s, shippingCost shippingCost_s, firstCarrierCost firstCarrierCost_s,
        ppve ppve_s, refound_fee refound_fee_s, platform_zero platform_zero_s,
        platform_must_percent platform_must_percent_s, sup_price
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and sup_price > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_sup_price, how='left', on=['sku','warehouse', 'country','platform'])
    df[['sup_stock', 'sup_price']] = df[['sup_stock', 'sup_price']].fillna(0)
    # 2. 替换逻辑
    df['is_supplier'] = np.where(df['sup_price'] > 0, 1, 0)
    c1 = (df['available_stock'] > 0) & (df['sup_stock'] == 0)
    c2 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['sales_status'].isin(['正常', '涨价缩销'])) & (df['price_rmb']>df['sup_price'])
    c3 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['sales_status'].isin(['正利润加快动销', '负利润加快动销','清仓']))
    c4 = (df['available_stock'] == 0) & (df['sup_stock'] > 0)
    df['supplier_logic'] = np.select([c1, c2, c3, c4], [1, 2, 3, 4], 0)
    # 定义列映射关系：
    col_mapping = {
        'new_price': 'new_price_s',
        'total_cost': 'shippingCost_s',
        'ppve': 'ppve_s',
        'platform_zero': 'platform_zero_s',
        'platform_must_percent': 'platform_must_percent_s',
        'target_profit_rate':'platform_must_percent_s',
        'available_stock': 'sup_stock'
    }
    mask = df['supplier_logic'].isin([2, 4])
    for target_col, replace_col in col_mapping.items():
        df[target_col] = np.where(mask, df[replace_col], df[target_col])
    df['price_rmb_new'] = np.where(mask, df['sup_price'], df['price_rmb'])
    # 是否使用供应商供货价的定价
    df['is_supplier_price'] = np.where(df['sup_price']==df['price_rmb_new'], 1, 0)


    # 3. 供应商sku中间表存表
    # write_to_sql(df[df['is_supplier']==1], 'dwm_supplier_dtl')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df[df['is_supplier']==1], 'dwm_supplier_dtl', if_exists='replace')

    df['price_rmb'] = df['price_rmb_new']
    col = ['ppve','platform_zero','new_price', 'total_cost','price_rmb']
    df[col] = df[col].astype(float)
    df.drop(['sup_stock', 'sup_price', 'supplier_logic','price_rmb_new'], axis=1, inplace=True)
    col = ['new_price_s','total_cost_s','shippingCost_s','firstCarrierCost_s','ppve_s','refound_fee_s',
           'platform_zero_s','platform_must_percent_s']
    df.drop(col, axis=1, inplace=True)
    # df.to_excel('F://Desktop//dwm_sku_price_new.xlsx', index=0)
    print(df.info())

    return df

def tt_get_supplier_price(df):
    """
    供应商货盘sku定价处理
    返回字段：是否供应商sku、是否使用供应商报价的定价
    """

    # 1. 获取供应商sku定价信息
    sql = """
        SELECT YB_sku sku, warehouse, country, platform, available_stock sup_stock, warehouse_price new_price_s, 
        total_cost total_cost_s, shippingCost shippingCost_s, firstCarrierCost firstCarrierCost_s,
        ppve ppve_s, refound_fee refound_fee_s, platform_zero platform_zero_s,
        platform_must_percent platform_must_percent_s, sup_price
        FROM yibai_oversea.tt_dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_dwm_supplier_sku_price)
        and sup_price > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_sup_price, how='left', on=['sku','warehouse', 'country','platform'])
    df[['sup_stock', 'sup_price']] = df[['sup_stock', 'sup_price']].fillna(0)
    # 2. 替换逻辑
    df['is_supplier'] = np.where(df['sup_price'] > 0, 1, 0)
    c1 = (df['available_stock'] > 0) & (df['sup_stock'] == 0)
    c2 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['sales_status'].isin(['正常', '涨价缩销'])) & (df['price_rmb']>df['sup_price'])
    c3 = (df['available_stock'] > 0) & (df['sup_stock'] > 0) & (df['sales_status'].isin(['正利润加快动销', '负利润加快动销','清仓']))
    c4 = (df['available_stock'] == 0) & (df['sup_stock'] > 0)
    df['supplier_logic'] = np.select([c1, c2, c3, c4], [1, 2, 3, 4], 0)
    # 定义列映射关系：
    col_mapping = {
        'new_price': 'new_price_s',
        'total_cost': 'shippingCost_s',
        'shippingCost': 'shippingCost_s',
        'firstCarrierCost': 'firstCarrierCost_s',
        'ppve': 'ppve_s',
        'refound_fee': 'refound_fee_s',
        'platform_zero': 'platform_zero_s',
        'platform_must_percent': 'platform_must_percent_s',
        'target_profit_rate':'platform_must_percent_s',
        'available_stock': 'sup_stock'
    }
    mask = df['supplier_logic'].isin([2, 4])
    for target_col, replace_col in col_mapping.items():
        df[target_col] = np.where(mask, df[replace_col], df[target_col])
    df['price_rmb_new'] = np.where(mask, df['sup_price'], df['price_rmb'])
    # 是否使用供应商供货价的定价
    df['is_supplier_price'] = np.where(df['sup_price']==df['price_rmb_new'], 1, 0)

    # 3. 供应商sku中间表存表
    # write_to_sql(df[df['is_supplier']==1], 'dwm_supplier_dtl')
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn.to_sql(df[df['is_supplier']==1], 'tt_dwm_supplier_dtl', if_exists='replace')

    df['price_rmb'] = df['price_rmb_new']
    col = ['ppve','platform_zero','new_price', 'total_cost','price_rmb']
    df[col] = df[col].astype(float)
    df.drop(['sup_stock', 'sup_price', 'supplier_logic','price_rmb_new'], axis=1, inplace=True)
    col = ['new_price_s','total_cost_s','shippingCost_s','firstCarrierCost_s','ppve_s','refound_fee_s',
           'platform_zero_s','platform_must_percent_s']
    df.drop(col, axis=1, inplace=True)
    # df.to_excel('F://Desktop//dwm_sku_price_new.xlsx', index=0)
    print(df.info())

    return df


def get_toucheng_price(df):
    """
    获取头程单价：
    1、优先用子仓匹配，筛选主要渠道的单价（普货）
    2、其次用分摊头程的头程
    """
    # 尺寸重量
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    # 取头程单价
    sql = """
         SELECT DISTINCT warehouse, `计费方式` weight_method,
         `头程计泡系数` dim_weight, `是否包税` include_tax, `普货单价` price
         FROM yibai_oversea.oversea_fees_parameter_new
         WHERE `是否主要渠道` = 1 
     """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_tc = conn_ck.ck_select_to_df(sql)
    df_tc = df_tc.sort_values(by='price', ascending=False).drop_duplicates(subset=['warehouse'])

    df = pd.merge(df, df_tc[['warehouse','price','weight_method']], how='left', on=['warehouse'])
    col = ['长','宽','高','price','totalCost','firstCarrierCost']
    df[col] = df[col].fillna(0).astype(float)
    df['计费重'] = (df['重量']/1000).combine(df['长'] * df['宽'] * df['高'] / 6000, max)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']=='计费重',
                                          df['计费重'] * df['price'],
                                          df['长'] * df['宽'] * df['高'] * df['price'] / 1000000)

    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['totalCost'] = np.where(df['new_firstCarrierCost']==0, df['totalCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    df['firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])


    col = ['数量', '重量', '成本', '长', '宽', '高', '重量来源', 'price','weight_method','计费重','new_firstCarrierCost']
    df.drop(col, axis=1, inplace=True)

    return df


def get_supplier_fee():
    """
    供应商货盘SKU的运费数据。
    调用运费接口获取
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 1、取sku和子仓
    sql = """
        SELECT distinct YM_sku sku, warehouse_id, warehouse
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
        -- and warehouse_id is not Null
        -- and available_stock > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku = conn_ck.ck_select_to_df(sql)
    # 没有库存的数据，warehouse_id填充为常用子仓
    df_sku['warehouse_id'] = df_sku['warehouse_id'].fillna(0).astype(int)
    dic = {'美国仓':'1739,1760,1741,1108,1740', '德国仓':'325,653', '英国仓':'88'}
    mask = (df_sku['warehouse_id'] == 0) | (df_sku['warehouse_id']=='')
    df_sku.loc[mask, 'warehouse_id'] = df_sku.loc[mask, 'warehouse'].map(dic).fillna('')
    df_sku['warehouse_id'] = df_sku['warehouse_id'].astype(str)
    # df_sku = df_sku[df_sku['sku']=='YM2612250000311']
    # df_sku = df_sku.sample(10)
    # 2、取运费的平台国家
    sql = """
        SELECT distinct platform, shipCountry country
        FROM over_sea.oversea_fee_site
        WHERE group_code = 'YB' and is_del = 0 and platform in ('AMAZON', 'EB','ALI', 'WALMART','TTS')
    """
    df_site = conn.read_sql(sql)
    df_site_temu = df_site[df_site['platform']=='AMAZON']
    df_site_temu['platform'] = 'TEMU'
    df_site = pd.concat([df_site, df_site_temu])
    warehouse_dict = {'US': '美国仓','CA': '加拿大仓','MX': '墨西哥仓','JP': '日本仓','AU': '澳洲仓','GB': '英国仓',
                      'DE': '德国仓','FR': '德国仓','ES': '德国仓','IT': '德国仓','NL': '德国仓','SE': '德国仓',
                      'PL': '德国仓','TR': '德国仓','BE': '德国仓','BR': '巴西仓','SK': '德国仓','CZ': '德国仓',
                      'DK': '德国仓','FI': '德国仓','HU': '德国仓','PT': '德国仓','AE': '阿联酋仓','IE': '德国仓'}
    df_site['warehouse_country'] = df_site['country'].map(warehouse_dict)
    df_sku['is_key'] = 1
    df_site['is_key'] = 1
    df_data = pd.merge(df_sku, df_site, how='outer', on=['is_key'])
    df_data = df_data[df_data['warehouse']==df_data['warehouse_country']]
    df_data.drop(['is_key', 'warehouse_country'], axis=1, inplace=True)
    df_data = df_data.drop_duplicates()
    df_data['数量'] = 1
    # df_data = df_data.sample(1)
    # df_data = df_data[df_data['country']=='US']
    # df_data.to_excel('F://Desktop//df_data.xlsx', index=0)
    print(df_data)
    # w_list1 = get_oversea_ship_type_list()
    w_list1 = '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30'
    df_result = pd.DataFrame()
    for (key0, key1, key2), group in df_data.groupby(['platform', 'country', 'warehouse_id']):
        print(key0, key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea(key0, key1, key2, w_list1, '')
        # print(group1.info())
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost','shippingCost', 'firstCarrierCost', 'dutyCost']]
        # print(group2.info())
        group3 = group.merge(group2, on=['sku', '数量'])
        group3 = group3[
            ['sku', '数量', 'shipCountry', 'platform','warehouse', 'warehouseId', 'warehouseName', 'shipCode',
             'shipName','totalCost','shippingCost', 'firstCarrierCost', 'dutyCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True)
        # group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])

    df_result['date_id'] = time.strftime('%Y-%m-%d')
    df_result.drop('数量', axis=1, inplace=True)

    # 自算头程
    df_result = get_toucheng_price(df_result)

    print(df_result.info())
    write_to_ck(df_result, 'oversea_transport_fee_supplier')
    # df_result.to_excel('F://Desktop//df_result_fee.xlsx', index=0)

    return None

def get_rate():
    """获取各国家的汇率"""

    sql = """
    SELECT distinct country, from_currency_code as charge_currency,rate , erp_rate
    FROM domestic_warehouse_clear.erp_rate
    WHERE date_archive = (SELECT max(date_archive) FROM domestic_warehouse_clear.erp_rate)
    """
    ck_client = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_rate = ck_client.ck_select_to_df(sql)
    df_rate['country'] = np.where(df_rate['charge_currency']=='HUF', 'HU', df_rate['country'])
    # df_rate = df_rate.drop_duplicates(subset='charge_currency')
    return df_rate

# 供应商子仓库存临时数据
def get_sup_stock_temp():
    """ """
    date_today = time.strftime('%Y-%m-%d')
    # 取库存库龄
    sql = f"""
        SELECT YM_sku, warehouse_id, warehouse_name, warehouse, available_stock, cargo_owner_id, overage_level, date_id
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    # 取映射关系
    df_sku = get_sup_yb_sku()
    df = pd.merge(df, df_sku[['YM_sku', 'YB_sku']], how='left', on='YM_sku')

    df.to_excel('F://Desktop//df_sup_stock.xlsx', index=0)


# 供应商sku有库存，海外仓sku无到货记录的sku处理。
def only_supplier_price():
    """
    1、筛选出哪些sku是海外仓sku无到货记录的sku
    2、计算这部分sku在各个平台的定价
    3、和易佰sku定价信息合并。合并到：dwm_oversea_price_dtl
    """
    sql = """
        SELECT 
            YB_sku sku, warehouse_price new_price, shippingCost total_cost, shippingCost, firstCarrierCost,warehouse_id best_warehouse_id, 
            warehouse_name best_warehouse_name, warehouse, available_stock,
            0 is_new, 2 is_supplier, 1 is_supplier_price, '正常' sales_status, country, platform,  ppve, refound_fee,
            platform_zero, platform_must_percent, ship_name,  
            sup_price price_rmb, '' is_adjust, platform_must_percent target_profit_rate 
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and sup_price > 0
        UNION ALL
        SELECT 
            YM_sku sku, warehouse_price new_price, shippingCost total_cost, shippingCost, firstCarrierCost,warehouse_id best_warehouse_id, 
            warehouse_name best_warehouse_name, warehouse, available_stock,
            0 is_new, 2 is_supplier, 1 is_supplier_price, '正常' sales_status, country, platform,  ppve, refound_fee,
            platform_zero, platform_must_percent, ship_name,  
            sup_price price_rmb, '' is_adjust, platform_must_percent target_profit_rate 
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price)
        and sup_price > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)
    col = ['overage_level', 'day_sales', 'recent_day_sales','estimated_sales_days','section','up_profit_rate',
           'after_profit', 'lowest_profit', 'is_distory','lowest_price','target_profit_y']
    df_sup_price = df_sup_price.assign(**{c: 0 for c in col})

    # 海外仓有到货sku
    sql = """
        SELECT sku, warehouse, 1 as is_normal_sku
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and type != '供应商货盘'
    """
    df_sku = conn.read_sql(sql)

    df = pd.merge(df_sup_price, df_sku, how='left', on=['sku','warehouse'])
    df = df[df['is_normal_sku'].isna()]
    df.drop('is_normal_sku', axis=1, inplace=True)
    date_id = time.strftime('%Y-%m-%d')

    df_rate = get_rate()
    df_rate = df_rate.drop_duplicates(subset=['country', 'rate'])
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])

    col = ['price_rmb', 'rate']
    df[col] = df[col].astype(float)
    df['price'] = df['price_rmb'] / df['rate']
    df['price'] = df['price'].round(1) - 0.01

    df['date_id'] = date_id
    print(df.info())
    # df.to_excel('F://Desktop//df_only_supplier.xlsx', index=0)

    # # 存表
    # table_name = 'dwm_oversea_price_dtl'
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = f"""
    # delete from {table_name} where date_id='{date_id}' and is_supplier = 2
    # """
    # conn.execute(sql)
    # conn.to_sql(df, table_name, if_exists='append')

    return df

def get_flash_sku():
    """  限时清仓sku """
    sql = """
        SELECT *
        FROM over_sea.oversea_flash_clearout_sku
        WHERE date_id = (SELECT max(date_id) FROM over_sea.oversea_flash_clearout_sku)
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_flash_sku = conn.read_sql(sql)
    print(df_flash_sku.info())

    return df_flash_sku

def get_destroy_profit():
    """
    计算amazon、ebay销毁价利润率。
    将低值回写到dwm_sku_temp_info。
    用于判断【加快动销补充逻辑】：销毁价过低时，是否需要增大降价幅度
    """
    sql = """

        SELECT sku, warehouse, new_price, total_cost, lowest_price, country, platform , target_profit_rate, is_distory, date_id
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_oversea_price_dtl)
        and platform in ('AMAZON', 'EB')
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    dic = {'美国仓':'US','德国仓':'DE','英国仓':'UK','法国仓':'FR','澳洲仓':'AU','加拿大仓':'CA','墨西哥仓':'MX',
           '西班牙仓':'ES','意大利仓':'IT','乌拉圭仓':'BR','日本仓':'JP'}
    df['warehouse_country'] = df['warehouse'].replace(dic)
    # 利润率、是否销毁价，可以只取一个国家（仓库所在国）
    df = df[df['warehouse_country'] == df['country']]

    # 取配置表
    df_platform_fee = get_platform_fee()
    df = pd.merge(df, df_platform_fee[['platform','country','ppve','platform_zero', 'platform_must_percent']],
                  how='left', on=['platform','country'])
    df['lowest_price_profit'] = 1 - df['ppve'] - df['platform_zero'] - df['platform_must_percent'] - \
                                (df['new_price']+df['total_cost'])/df['lowest_price']

    df = df.sort_values(by='lowest_price_profit', ascending=True).drop_duplicates(subset=['sku','warehouse'])

    df = df[['sku','warehouse','target_profit_rate','lowest_price_profit', 'is_distory']]

    # df.to_excel('F://Desktop//df_destroy.xlsx', index=0)

    return df

def tt_only_supplier_price():
    """
    1、筛选出哪些sku是海外仓sku无到货记录的sku
    2、计算这部分sku在各个平台的定价
    3、和易佰sku定价信息合并。合并到：dwm_oversea_price_dtl
    """
    sql = """
        SELECT 
            YB_sku sku, warehouse_price new_price, shippingCost total_cost, shippingCost, firstCarrierCost,warehouse_id best_warehouse_id, 
            warehouse_name best_warehouse_name, warehouse, available_stock,
            0 is_new, 2 is_supplier, 1 is_supplier_price, '正常' sales_status, country, platform,  ppve, refound_fee,
            platform_zero, platform_must_percent, ship_name,  
            sup_price price_rmb, '' is_adjust, platform_must_percent target_profit_rate 
        FROM yibai_oversea.tt_dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_dwm_supplier_sku_price)
        and sup_price > 0
        UNION ALL
        SELECT 
            YM_sku sku, warehouse_price new_price, shippingCost total_cost, shippingCost, firstCarrierCost,warehouse_id best_warehouse_id, 
            warehouse_name best_warehouse_name, warehouse, available_stock,
            0 is_new, 2 is_supplier, 1 is_supplier_price, '正常' sales_status, country, platform,  ppve, refound_fee,
            platform_zero, platform_must_percent, ship_name,  
            sup_price price_rmb, '' is_adjust, platform_must_percent target_profit_rate 
        FROM yibai_oversea.tt_dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.tt_dwm_supplier_sku_price)
        and sup_price > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup_price = conn_ck.ck_select_to_df(sql)
    col = ['overage_level', 'day_sales', 'recent_day_sales','estimated_sales_days','section','up_profit_rate',
           'after_profit', 'lowest_profit', 'is_distory','lowest_price','target_profit_y']
    df_sup_price = df_sup_price.assign(**{c: 0 for c in col})

    # 海外仓有到货sku
    sql = """
        SELECT sku, warehouse, 1 as is_normal_sku
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwm_sku_temp_info)
        and type != '供应商货盘'
    """
    df_sku = conn.read_sql(sql)

    df = pd.merge(df_sup_price, df_sku, how='left', on=['sku','warehouse'])
    df = df[df['is_normal_sku'].isna()]
    df.drop('is_normal_sku', axis=1, inplace=True)
    date_id = time.strftime('%Y-%m-%d')

    df_rate = get_rate()
    df_rate = df_rate.drop_duplicates(subset=['country', 'rate'])
    df = pd.merge(df, df_rate[['country', 'rate']], how='left', on=['country'])

    col = ['price_rmb', 'rate']
    df[col] = df[col].astype(float)
    df['price'] = df['price_rmb'] / df['rate']
    df['price'] = df['price'].round(1) - 0.01

    df['date_id'] = date_id
    print(df.info())
    # df.to_excel('F://Desktop//df_only_supplier.xlsx', index=0)

    # # 存表
    # table_name = 'dwm_oversea_price_dtl'
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = f"""
    # delete from {table_name} where date_id='{date_id}' and is_supplier = 2
    # """
    # conn.execute(sql)
    # conn.to_sql(df, table_name, if_exists='append')

    return df

# 供应商二次备货sku，调价目标净利率设置为0
def get_sup_sku_second_stocking():
    """ 获取二次备货sku信息 """
    sql = """
        WITH 
        order_base_info AS (
            SELECT
                dofi.distributor_id `商户id`,
                sd.sku,
                dofi.order_id `订单号`,
                IF(dofi.ship_time >= '1971-01-01', dofi.ship_time, NULL) AS `订单发货时间`,
                IF(dofi.complete_time >= '1971-01-01', dofi.complete_time, NULL) AS `订单完成时间`,
                -- 标记订单是否有效（已发货且有完成时间）
                CASE 
                    WHEN dofi.ship_time >= '1971-01-01' AND dofi.complete_time >= '1971-01-01' 
                    THEN 1 ELSE 0 
                END AS `有效订单标识`
            FROM yibai_dcm_order_sync.dcm_order_fba_inbound dofi
            LEFT JOIN yibai_dcm_order_sync.dcm_order_fba_inbound_detail as sd 
                ON dofi.order_id = sd.order_id
                LEFT JOIN yibai_dcm_base_sync.dcm_distributor ddv on ddv.id=dofi.distributor_id 
            where 
                  (ddv.member_type=21
                  or left(dofi.order_id,9)= 'FBAOS-GYS'
                  or dofi.order_remark='供应商备货')
                and dofi.distributor_id not in ('14548')
            and sd.sku IS NOT NULL  -- 过滤无SKU的记录
        ),
        
        -- 步骤2：获取每个（商户+SKU）的历史订单完成时间（按时间排序）
        sku_order_history AS (
            SELECT
                `商户id`,
                sku,
                `订单号`,
                `订单发货时间`,
                `订单完成时间`,
                -- 上一个订单的完成时间（用于判断是否在完成后发货）
                anyLast(`订单完成时间`) OVER (
                    PARTITION BY `商户id`, sku 
                    ORDER BY `订单发货时间`
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS `上一订单完成时间`
            FROM order_base_info
            WHERE `有效订单标识` = 1  -- 只保留有效订单
        ),
        
        -- 步骤3：判断每个订单是否为二次备货（在同一商户同一SKU的上一订单完成后发货）
        second_stockup_judge AS (
            SELECT
                `商户id`,
                sku,
                `订单号`,
                `订单发货时间`,
                `上一订单完成时间`,
                -- 若当前订单发货时间晚于上一订单完成时间，则为二次备货
                CASE 
                    WHEN `上一订单完成时间` IS NOT NULL 
                        AND `订单发货时间` > `上一订单完成时间` 
                    THEN 1 ELSE 0 
                END AS `是否二次备货标识`
            FROM sku_order_history
        )
        
        -- 最终结果：按（商户+SKU）汇总，判断是否存在二次备货
        SELECT
            `商户id` cargo_owner_id,
            sku YM_sku,
            -- 只要存在一次二次备货，就判定为有
            CASE 
                WHEN MAX(`是否二次备货标识`) = 1 THEN 1 
                ELSE 0 
            END AS `is_second_stocking`,
            COUNT(DISTINCT `订单号`) AS `stocking_num`,
            SUM(`是否二次备货标识`) AS `second_stocking_num`,
            MIN(CASE WHEN `是否二次备货标识` = 1 THEN formatDateTime(`订单发货时间`, '%Y-%m-%d') END) AS `second_stocking_time`
        FROM second_stockup_judge
        GROUP BY `商户id`, sku
        ORDER BY `商户id`, sku
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    print(df.info())

    # df.to_excel('F://Desktop//df_sup_sec.xlsx', index=0)

    return df



############################## 3. 20251215限时清仓sku，分次降为销毁价。（有效期至2025年12月30日）
def flash_sku_to_lowest(df):
    """ 限时清仓sku分次降到销毁价 """
    # #
    # sql = """
    #     SELECT *
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = '2025-12-15' and available_stock > 0 and warehouse = '美国仓'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)
    # df_destroy = get_destroy_profit()
    # col = ['sku','warehouse','target_profit_rate','lowest_price_profit']
    # df = pd.merge(df, df_destroy[col], how='left', on=['sku','warehouse'])
    # #

    df_flash = get_flash_sku()
    df_flash = df_flash[['sku', 'warehouse']]
    df_flash['is_flash'] = 1

    df = pd.merge(df, df_flash, how='left', on=['sku', 'warehouse'])
    df['is_flash'] = df['is_flash'].fillna(0).astype(int)
    print(df.info())
    c1 = (df['is_flash']==1) & (df['overage_level'] >= 60) & \
         (df['target_profit_rate'] > df['lowest_price_profit']) & (df['section'] < 0)
    df['section'] = np.where(c1,
                             np.minimum(
                                 df['section'] - (df['target_profit_rate'] - df['lowest_price_profit']) / 2,
                                 df['section']
                             ),
                             df['section'])

    df.drop('is_flash', axis=1, inplace=True)
    # df.to_excel('F://Desktop//df_sku_flash.xlsx', index=0)

    return df


############################## 4. 挑选出的部分sku继续降价。（20260204：指定sku继续降价，其他清仓品保持）
def get_temp_clear_sku():
    """
    获取挑选sku信息
    """
    sql = """ 
        SELECT distinct sku, warehouse , 1 as temp_clear_sku
        FROM yibai_oversea.oversea_temp_clear_sku
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_temp_clear_sku )
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df = conn.read_sql(sql)
    df = conn_ck.ck_select_to_df(sql)

    return df


############################## 5、海外仓美国仓三仓都有库存的sku，运费的默认邮编使用5区
def get_target_sku():
    """  获取三仓都有库存的sku """
    sql = """
        SELECT sku, warehouse, warehouse_name, warehouse_code, warehouse_id, stock, available_stock, 0 is_sup_sku
        FROM yb_datacenter.v_oversea_stock
        WHERE available_stock > 0 and warehouse = '美国仓'
        and warehouse_name not like '%精品%'
        and warehouse_name not in ('云仓美西仓')
    """
    conn = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df = conn.ck_select_to_df(sql)

    # 合并供应商库存
    sql = """
        SELECT YM_sku sku, warehouse, warehouse_name, warehouse_code, warehouse_id, available_stock stock, 
        available_stock, 1 is_sup_sku
        FROM yibai_oversea.dwd_supplier_sku_stock_id
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwd_supplier_sku_stock_id)
        and available_stock > 0
        and cargo_owner_id != 8
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sup = conn_ck.ck_select_to_df(sql)
    df = pd.concat([df, df_sup])

    c1 = df['warehouse_name'].str.contains('东')
    c2 = df['warehouse_name'].str.contains('西')
    c3 = df['warehouse_name'].str.contains('南')
    df['warehouse_area'] = np.select([c1, c2, c3], ['东仓', '西仓', '南仓'], np.nan)

    # 判断三仓都有库存
    target_areas = {'东仓', '西仓', '南仓'}
    sku_areas = df.groupby('sku')['warehouse_area'].apply(set)
    sku_full_coverage = sku_areas.apply(lambda x: x.issuperset(target_areas))
    df['all_three_areas'] = df['sku'].map(sku_full_coverage).fillna(False)

    # 5区默认邮编
    # 美东5区：65065 美东6区：65689 美东7区：57769
    # 美西5区：97701 美西6区：74066 美西7区：71270
    # 美南5区：73071 美南6区：82609 美南7区：84098
    ship_zip = {'东仓':65065, '西仓':97701, '南仓':73071}
    df['ship_zip'] = df['warehouse_area'].map(ship_zip)
    df['warehouse_id'] = df['warehouse_id'].fillna(0).astype(int)

    df = df[df['all_three_areas']]
    df = df.drop_duplicates(subset=['sku', 'warehouse_name'], keep='first')
    # df.to_excel('F://Desktop//df_three.xlsx', index=0)

    return df

def get_fee_platform():
    """ 获取运费计算的平台 """
    sql = """
        SELECT DISTINCT platform
        FROM over_sea.oversea_fee_site
        WHERE shipCountry = 'US' and group_code = 'YB' and is_del = 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)
    p_list = list(df['platform'].unique())
    p_list.append('TEMU')
    print(p_list)
    return p_list


def get_target_sku_fee():
    """ 获取目标sku的运费数据 """
    # 1、获取sku
    # df = pd.read_excel('F://Desktop//df_fee_temp.xlsx', dtype={'sku':str})
    df = get_target_sku()
    df['country'] = 'US'
    col = ['warehouse_id', 'ship_zip']
    df['ship_zip'] = df['ship_zip'].fillna(0).astype(int)
    df = df[['sku', 'country','warehouse_id','ship_zip']].drop_duplicates()
    df['数量'] = 1
    # df = df.sample(2)
    # 控制country对应的子仓
    dic = {'US': '49,50', 'UK': '88,680', 'DE': '325', 'FR': '325', 'ES': '325', 'IT': '325', 'CA': '1139',
           'AU': '353,769','CZ': '325','PL': '325','HU':'325','PT':'325','NL':'325','MX':'956','BR':'1467'}
    print(df.info())
    # 2、获取运费
    p_list = get_fee_platform()
    # p_list = ['AMAZON']
    df_result = pd.DataFrame()
    for p in p_list:
        df_fee = load_interface(df, p)
        df_result = pd.concat([df_fee, df_result])

    col = ['sku', 'shipCountry', 'ship_zip', 'platform', 'warehouseId', 'warehouseName',
           'shipCode', 'shipName', 'totalCost','shippingCost', 'firstCarrierCost','dutyCost']
    df_result = df_result[col]
    df_result['date_id'] = time.strftime('%Y-%m-%d')
    print(df_result.info())
    # df_result.to_excel('F://Desktop//df_three_fee.xlsx', index=0)

    # 3、存表ck
    write_to_ck(df_result, 'oversea_transport_fee_three_area')

def check_target_sku():
    """ 三仓都有库存运费校验 """
    df_sku = get_target_sku()
    df_sku = df_sku.drop_duplicates(subset=['sku', 'warehouse'])
    # sku_list = tuple(df_sku['sku'].unique())
    sql = """
        SELECT sku, warehouse, best_warehouse_name, available_stock, total_cost, 
        is_supplier, ship_name, price_rmb, is_supplier_price
        FROM yibai_oversea.dwm_oversea_price_dtl
        WHERE date_id = '2026-02-10' and warehouse = '美国仓' and platform = 'AMAZON'
        and country = 'US'
    """
    conn_ck = pd_to_ck(database='yibai_ovesea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df_sku[['sku', 'warehouse', 'warehouse_name']], df, how='left', on=['sku','warehouse'])

    sql = """
         SELECT sku, warehouse, total_cost 
         FROM yibai_oversea.dwm_oversea_price_dtl
         WHERE date_id = '2026-02-09' and warehouse = '美国仓' and platform = 'AMAZON'
         and country = 'US'
     """
    conn_ck = pd_to_ck(database='yibai_ovesea', data_sys='调价明细历史数据')
    df_2 = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_2, how='left', on=['sku','warehouse'])

    df['运费差异'] = (df['total_cost_x']/df['total_cost_y'] - 1).replace(np.nan, 0)
    bins = [float('-inf'), -0.5, -0.2, -0.1, -0.05, 0.05, 0.1, float('inf')]
    labels = ['A:<-0.5','B:[-0.5, -0.2)','C:[-0.2, -0.1)','D:[-0.1, -0.05)','E:[-0.05, 0.05)','F:[0.05, 0.1)',
              'G:[0.1, +)']
    # 分段（right=False确保左闭右开）
    df['运费差异分段'] = pd.cut(df['运费差异'],bins=bins,labels=labels,right=False)  # 区间为[左, 右)，匹配标签格式

    # df.to_excel('F://Desktop//df_three_sku.xlsx', index=0)


############################# 6、临时需清仓sku逻辑。（库龄手动置为超*天）
def temp_clear_sku_logic(df):
    """  """
    date_today = time.strftime('%Y-%m-%d')
    # 1、临时sku清单
    sql = f""" 
        SELECT sku, warehouse, 1 as is_temp_clear
        FROM yibai_oversea.temp_oversea_clear_sku
        WHERE 
        end_time > '{date_today}'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_temp = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_temp, how='left', on=['sku','warehouse'])
    df['is_temp_clear'] = df['is_temp_clear'].fillna(0).astype(int)

    c1 = (df['is_temp_clear']==1) & (df['overage_level']<90)
    df['overage_level'] = np.where(c1, 90, df['overage_level'])
    for i in ['age_30_plus','age_60_plus','age_90_plus']:
        df[i] = np.where(c1, df['available_stock'], df[i])

    df.drop('is_temp_clear', axis=1, inplace=True)

    # 2、20250115 乌拉圭仓手动置为清仓状态
    df['overage_level'] = np.where((df['warehouse'] == '乌拉圭仓') & (df['available_stock']>0),
                                        360, df['overage_level'])
    df['age_360_plus'] = np.where(df['warehouse'] == '乌拉圭仓', df['available_stock'], df['age_360_plus'])

    # 3、20250717 海兔sku手动置为清仓状态
    c1 = (df['type'].str.contains('海兔|易通兔')) & (df['available_stock']>0) & (df['overage_level']<180)
    df['overage_level'] = np.where(c1, 180, df['overage_level'])
    for i in ['age_30_plus','age_60_plus','age_90_plus','age_120_plus','age_150_plus','age_180_plus']:
        df[i] = np.where(c1, df['available_stock'], df[i])


    return df


############################# 7、walmart 云仓美西仓库存优化逻辑（dtl表的库存数量会直接影响海外仓是否会对链接进行调价）
def walmart_dtl_stock_logic(df):
    """ """
    # 1、获取云仓美西仓库存
    sql = """
    SELECT DISTINCT 
        trimBoth(ps.sku) AS sku,ywc.name AS warehouse,yw.warehouse_name AS warehouse_name,
        yw.warehouse_code AS warehouse_code,yw.id AS warehouse_id,
        yw.warehouse_other_type AS warehouse_other_type,stock,available_stock available_stock_cloud,
        allot_on_way_count AS on_way_stock,wait_outbound,frozen_stock
    FROM yb_datacenter.yb_stock AS ps
    INNER JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ps.warehouse_id = yw.id
    LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
    WHERE (ps.date_id = toYYYYMMDD(now())) 
    AND (ps.cargo_owner_id = 8) 
    AND (yw.warehouse_type IN (2,3)) AND yw.warehouse_name IN ('云仓美西仓')
    AND available_stock > 0
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_stock = conn_ck.ck_select_to_df(sql)
    df_stock = df_stock.groupby(['sku','warehouse']).agg({'available_stock_cloud':'sum'}).reset_index()
    df = pd.merge(df, df_stock[['sku','warehouse','available_stock_cloud']], how='left', on=['sku','warehouse'])
    df['available_stock_cloud'] = df['available_stock_cloud'].fillna(0).astype(int)

    # 2、合并云仓库存
    c1 = (df['platform']=='WALMART')
    df['available_stock'] = np.where(c1, df['available_stock']+df['available_stock_cloud'], df['available_stock'])
    df.drop('available_stock_cloud', axis=1, inplace=True)

    # df[df['available_stock']>0].to_excel('F://Desktop//df_stock.xlsx', index=0)

    return df


############################# 8、捆绑sku补充逻辑
"""
1、抓取各平台捆绑链接的sku信息。
2、汇总sku信息，并统一抓取运费信息。
3、不同平台调价链接处理
"""
def parse_sku_qty(sku_str):
    """
    内部工具函数：解析单个sku片段，返回 (标准sku, 数量)
    例如：GS04995*2 → (GS04995, 2)
         GS07324 → (GS07324, 1)
    """
    # 第一步：先清理 = 及其后面的所有内容（核心修复）
    sku_str = sku_str.split("=")[0].strip()

    # 第二步：解析 * 数量
    if "*" in sku_str:
        sku, qty = sku_str.split("*", 1)
        # 容错：转不了数字就默认 1
        try:
            qty = int(qty)
        except:
            qty = 1
        return sku.strip(), qty
    else:
        return sku_str.strip(), 1


def split_bundle_sku(df):
    """
    终极版：支持所有sku1格式
    1. GS04995*2 → sku=GS04995, 数量=2
    2. A+B → sku=A,B, 数量=1,1
    3. GS04995*2+GS07324 → sku=GS04995,GS07324, 数量=2,1
    4. 普通SKU → 数量=1
    """
    result = []

    for _, row in df.iterrows():
        sku1 = str(row["sku1"]).strip()
        country = row["country"]

        # 按 + 拆分所有子SKU（核心步骤）
        sku_parts = sku1.split("+")

        sku_list = []
        qty_list = []

        # 逐个解析每个子SKU（带*或不带*）
        for part in sku_parts:
            sku, qty = parse_sku_qty(part)
            sku_list.append(sku)
            qty_list.append(str(qty))

        # 拼接成最终格式
        sku_final = ",".join(sku_list)
        qty_final = ",".join(qty_list)

        result.append({
            "sku1": sku1,
            "country": country,
            "sku": sku_final,
            "数量": qty_final
        })

    return pd.DataFrame(result)
def get_bundle_sku():
    """ 获取各平台捆绑链接的sku信息 """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    # 1、获取平台捆绑链接信息
    # 1.1 AMAZON
    sql = """
    with sku_map as (
        select sku,
            arrayStringConcat(groupArray(sku1), ',') as `sku1`,  
            arrayStringConcat(groupArray(`数量`), ',') as `数量`
        from (
            select sku,sku1,sum(`数量`) as `数量`
            from (
                select distinct sku,
                    arrayJoin(splitByString('+', sku)) as sku2,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[1]) as sku1,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[2]) as num1,
                    if(num1 = '', 1, toInt32(num1)) as `数量`
                from (
                    select distinct sku from (
                        select distinct sku from yibai_product_kd_sync.yibai_amazon_sku_map 
                        where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2 
                    )
                )
            )
            group by sku,sku1
        )
        group by sku
    ),
    sku_site_table as (
        select distinct site1,site3,sku 
        from (
            select distinct c.site1 as site1,c.site3 as site3,a.sku as sku
            from (
                select distinct account_id,sku 
                from yibai_product_kd_sync.yibai_amazon_sku_map 
                where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2
            ) a 
            left join (
                select toInt64(id) as account_id,site from yibai_system_kd_sync.yibai_amazon_account
            ) b 
            on a.account_id=b.account_id
            left join domestic_warehouse_clear.yibai_site_table_amazon c 
            on b.site=c.site
        )
    )

    select distinct 
        a.site3 as shipCountry,a.sku as sku1,b.sku1 as sku,b.`数量` as `数量`, 'AMAZON' platform
    from sku_site_table a 
    left join sku_map b 
    on a.sku=b.sku 
    """
    df_amazon_yb = conn_ck.ck_select_to_df(sql)
    # print(df_amazon.info())
    # print(df_amazon.head(4))
    sql = """
    with sku_map as (
        select sku,
            arrayStringConcat(groupArray(sku1), ',') as `sku1`,  
            arrayStringConcat(groupArray(`数量`), ',') as `数量`
        from (
            select sku,sku1,sum(`数量`) as `数量`
            from (
                select distinct sku,
                    arrayJoin(splitByString('+', sku)) as sku2,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[1]) as sku1,
                    trim(BOTH ' ' FROM splitByString('*', sku2)[2]) as num1,
                    if(num1 = '', 1, toInt32(num1)) as `数量`
                from (
                    select distinct sku from (
                        select distinct sku from tt_product_kd_sync.tt_amazon_sku_map 
                        where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2 
                    )
                )
            )
            group by sku,sku1
        )
        group by sku
    ),
    sku_site_table as (
        select distinct site1,site3,sku 
        from (
            select distinct c.site1 as site1,c.site3 as site3,a.sku as sku
            from (
                select distinct account_id,sku 
                from tt_product_kd_sync.tt_amazon_sku_map 
                where (sku like '%%*%%' or sku like '%%+%%') and deliver_mode=2
            ) a 
            left join (
                select toInt64(id) as account_id,site from tt_system_kd_sync.tt_amazon_account
            ) b 
            on a.account_id=b.account_id
            left join domestic_warehouse_clear.tt_site_table_amazon c 
            on b.site=c.site
        )
    )

    select distinct 
        a.site3 as shipCountry,a.sku as sku1,b.sku1 as sku,b.`数量` as `数量`, 'AMAZON' platform
    from sku_site_table a 
    left join sku_map b 
    on a.sku=b.sku 
    """
    conn_ck_tt = pd_to_ck(database='tt_product_kd_sync', data_sys='通拓-新')
    df_amazon_tt = conn_ck_tt.ck_select_to_df(sql)
    df_amazon_tt['org'] = 'TT'

    # 1.2 TEMU
    sql = """
    SELECT
        sku1, country shipCountry, 'TEMU' platform,
        arrayStringConcat(groupArray(sku), ',') as `sku`,  
        arrayStringConcat(groupArray(`数量`), ',') as `数量`
    FROM (
        SELECT sku1, country, sku,sum(`数量`) as `数量`
        FROM (
            SELECT distinct 
                sku1,
                country,
                arrayJoin(splitByString('+', sku1)) as sku2,
                trim(BOTH ' ' FROM splitByString('*', sku2)[1]) as sku,
                trim(BOTH ' ' FROM splitByString('*', sku2)[2]) as num1,
                if(num1 = '', 1, toInt32(num1)) as `数量`
            FROM (
                SELECT distinct sku sku1, country
                FROM tt_oversea.tt_ads_oversea_temu_listing 
                WHERE (sku like '%%*%%' or sku like '%%+%%')
                and date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_temu_listing)
            )
        )
        group by sku1,country, sku
    )
    group by sku1, country
    """
    df_temu_tt = conn_ck_tt.ck_select_to_df(sql)
    print(f'TT temu捆绑链接数量为{len(df_temu_tt)}')
    df_temu_tt['org'] = 'TT'

    sql = """
    SELECT
        sku1, country shipCountry, 'TEMU' platform,
        arrayStringConcat(groupArray(sku), ',') as `sku`,  
        arrayStringConcat(groupArray(`数量`), ',') as `数量`
    FROM (
        SELECT sku1, country, sku,sum(`数量`) as `数量`
        FROM (
            SELECT distinct 
                sku1,
                country,
                arrayJoin(splitByString('+', sku1)) as sku2,
                trim(BOTH ' ' FROM splitByString('*', sku2)[1]) as sku,
                trim(BOTH ' ' FROM splitByString('*', sku2)[2]) as num1,
                if(num1 = '', 1, toInt32(num1)) as `数量`
            FROM (
                SELECT distinct sku sku1, country
                FROM yibai_oversea.yibai_ads_oversea_temu_listing 
                WHERE (sku like '%%*%%' or sku like '%%+%%')
                and date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_temu_listing)
            )
        )
        group by sku1,country, sku
    )
    group by sku1, country
    """
    df_temu = conn_ck.ck_select_to_df(sql)

    # 1.3 EB
    sql = """
        SELECT distinct sku sku1, country 
        FROM yibai_oversea.yibai_ads_oversea_ebay_listing 
        WHERE (sku like '%%*%%' or sku like '%%+%%')
        and date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_ebay_listing)
    """
    df_ebay = conn_ck.ck_select_to_df(sql)
    sql = """
        SELECT distinct sku sku1, country 
        FROM tt_oversea.tt_ads_oversea_ebay_listing 
        WHERE (sku like '%%*%%' or sku like '%%+%%')
        and date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_ebay_listing)
    """
    df_ebay_tt = conn_ck_tt.ck_select_to_df(sql)
    print(f'TT ebay捆绑链接数量为{len(df_ebay_tt)}')
    df_ebay_tt['org'] = 'TT'
    df_ebay = pd.concat([df_ebay, df_ebay_tt])
    df_ebay = split_bundle_sku(df_ebay)
    df_ebay['platform'] = 'EB'
    df_ebay = df_ebay.rename(columns={'country':'shipCountry'})

    # 1.4 ali
    sql = """
        SELECT distinct sku sku1, country 
        FROM yibai_oversea.yibai_ads_oversea_ali_listing
        WHERE (sku like '%%*%%' or sku like '%%+%%')
        and date_id = (SELECT max(date_id) FROM yibai_oversea.yibai_ads_oversea_ali_listing)
        and country not in ('GLO')
    """
    df_ali = conn_ck.ck_select_to_df(sql)
    sql = """
        SELECT distinct sku sku1, country 
        FROM tt_oversea.tt_ads_oversea_ali_listing
        WHERE (sku like '%%*%%' or sku like '%%+%%')
        and date_id = (SELECT max(date_id) FROM tt_oversea.tt_ads_oversea_ali_listing)
        and country not in ('GLO')
    """
    df_ali_tt = conn_ck_tt.ck_select_to_df(sql)
    print(f'TT ali捆绑链接数量为{len(df_ali_tt)}')
    df_ali_tt['org'] = 'TT'
    df_ali = pd.concat([df_ali, df_ali_tt])
    df_ali = split_bundle_sku(df_ali)
    df_ali['platform'] = 'ALI'
    df_ali = df_ali.rename(columns={'country':'shipCountry'})

    # 1.5
    # 合并sku基础信息：sku1, shipCountry, platform, sku, 数量
    df = pd.concat([df_temu, df_temu_tt, df_amazon_yb, df_amazon_tt, df_ebay, df_ali])
    df['shipCountry'] = df['shipCountry'].replace('UK','GB')
    df = df.reset_index()
    df.drop(['org', 'index'], axis=1, inplace=True)
    # 拆分捆绑SKU信息
    warehouse_map = {
        '德国仓': ['PT', 'UK', 'FI', 'EE', 'ES', 'PL', 'FR', 'DK', 'DE', 'HR', 'RO', 'GR', 'IE', 'IT', 'AT', 'CZ', 'SE',
                   'LU', 'SI', 'SK', 'HU', 'MT', 'BG', 'BE', 'NL', 'LV', 'LT', 'CY', 'GB','TR'],
        '美国仓': ['US', 'MX'], '澳洲仓': ['AU'], '加拿大仓': ['CA'], '阿联酋仓': ['AE'], '墨西哥仓':['MX'],
        '英国仓': ['GB', 'UK']
    }
    # 反向构建国家→仓库的映射字典
    country_to_warehouse = {}
    for warehouse, countries in warehouse_map.items():
        for country in countries:
            country_to_warehouse[country] = warehouse
    # 赋值（未匹配的国家默认'其他仓'）
    df['warehouse'] = df['shipCountry'].map(country_to_warehouse).fillna('其他仓')
    df = df[(~df['sku'].isna()) & (df['sku']!='')]

    # # 2、获取sku仓库、库存、库龄信息
    df_sku_info = calculate_bundle_summary(df)

    df = pd.merge(df, df_sku_info, how='inner', on=['sku1', 'warehouse','shipCountry'])

    # 3、获取运费信息
    df_fee = get_bundle_fee(df)
    df = pd.merge(df, df_fee, how='left', on=['sku', '数量','platform','shipCountry'])
    col = ['sku1','shipCountry','platform','warehouse','available_stock','overage_level',
           'warehouseId', 'warehouseName', 'shipCode', 'shipName','totalCost',
           'shippingCost', 'firstCarrierCost','dutyCost']
    # col = ['sku1','shipCountry','platform','warehouse','new_price','gross']
    df = df[col]
    df = df.rename(columns={'sku1':'sku'})
    # 3.1 尺寸重量
    df['数量'] = 1
    df = chicun_zhongliang(df, 1, conn_ck)
    col = ['数量','重量来源']
    df.drop(col, axis=1, inplace=True)
    df = df.rename(columns={'重量':'gross','成本':'new_price', '长':'length', '宽':'width', '高':'high'})

    # 筛选剔除
    df = df[~df['new_price'].isna()]
    col = ['new_price','gross','length','width','high']
    df[col] = df[col].fillna(0).astype(float)
    col = ['available_stock','overage_level','warehouseId']
    df[col] = df[col].fillna(0).astype(int)

    df['date_id'] = time.strftime('%Y-%m-%d')
    print(df.info())
    # 4、存表
    # df.to_excel('F://Desktop//df_temu_4.xlsx', index=0)

    table_name = 'oversea_transport_fee_bundle'
    # conn_ck.ck_insert(df, table_name, if_exist='append')
    write_to_ck(df, table_name)


def get_bundle_sku_price():
    """ 捆绑sku各平台价格计算 """

    # 1、 获取捆绑sku信息
    sql = """
        SELECT sku, new_price, totalCost, shippingCost, firstCarrierCost, dutyCost,
        warehouseId best_warehouse_id, warehouseName best_warehouse_name, warehouse, available_stock,
        0 is_new, 0 is_supplier, 0 is_supplier_price, '正常' sales_status, overage_level, platform, 
        shipCountry country, shipName ship_name, '保持' is_adjust
        FROM yibai_oversea.oversea_transport_fee_bundle
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_bundle)
        and totalCost > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)
    df['country'] = df['country'].replace('GB', 'UK')

    # 运费渠道选择
    c1 = df['ship_name'].str.contains('GOFO|自提UNI|VC|UNI本地派送')
    df = df[~c1]
    df = df.sort_values(by='shippingCost', ascending=True).drop_duplicates(subset=['sku','warehouse','platform','country'])
    # 1.2 自算头程替换
    df = get_toucheng_price(df)
    # 字段补齐（合并进dtl
    col = ['day_sales','recent_day_sales','estimated_sales_days','section','up_profit_rate','after_profit',
           'lowest_profit','target_profit_rate','target_profit_y', 'is_distory']
    df[col] = 0

    # 1.3 供应商货盘sku额外处理：供货价及运费直接相加
    c1 = df['sku'].str.contains('YM')
    df['is_supplier'] = np.where(c1, 1, 0)
    df = df[~c1]

    # 2、获取费率项
    sql = """
    SELECT 
        platform, site as country, pay_fee + paypal_fee + vat_fee + extra_fee as ppve, refound_fee, 
        platform_zero, platform_must_percent
    FROM yibai_platform_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_platform_fee = conn.read_sql(sql)

    # 筛选other行并取第一行（确保other数据存在）
    df = pd.merge(df, df_platform_fee, how='left', on=['platform', 'country'])

    fill_cols = ['ppve', 'refound_fee', 'platform_zero', 'platform_must_percent']
    fill_platforms = ['AMAZON', 'TEMU']

    for platform in fill_platforms:
        platform_other = df_platform_fee[
            (df_platform_fee['platform'] == platform) &
            (df_platform_fee['country'] == 'other')]

        if platform_other.empty:
            raise ValueError(f"费率表中未找到 {platform} 平台的 other 国家数据，无法填充！")

        # 提取第一行数据
        other_vals = platform_other.iloc[0][fill_cols]
        fill_mask = (df['platform'] == platform) & (df['ppve'].isna())
        df.loc[fill_mask, fill_cols] = other_vals.values

    # 未匹配到汇率的国家直接剔除
    df_rate = get_rate()
    df = pd.merge(df, df_rate[['country','rate']], how='inner', on=['country'])

    # 3、计算价格
    df['price_rmb'] = (df['new_price']+df['totalCost'])/(1-df['ppve']-df['platform_zero']-df['platform_must_percent'])
    df['lowest_price'] = 1.2 * (df['totalCost'] - df['firstCarrierCost'] - df['dutyCost']) / (1 - df['ppve'] - df['refound_fee'])
    df['price_rmb'] = np.where(df['price_rmb'] < df['lowest_price'], df['lowest_price'], df['price_rmb'])

    df['price'] = df['price_rmb']/df['rate']
    df['date_id'] = time.strftime('%Y-%m-%d')

    col = ['shippingCost','firstCarrierCost','dutyCost','refound_fee','rate']
    df.drop(col, axis=1, inplace=True)
    df = df.rename(columns={'totalCost':'total_cost'})
    print(df.info())
    # 4、存入dtl
    # table_name = 'dwm_oversea_price_dtl'
    # conn_ck.ck_insert(df, table_name, if_exist='append')

    return df



def get_bundle_fee(df_bundle_sku):
    """
    捆绑SKU的运费数据。
    调用运费接口获取
    """
    # df_bundle_sku = df_bundle_sku.sample(10)  # 抽样测试
    w_list1 = get_oversea_ship_type_list()
    # print(w_list1)
    df_result = pd.DataFrame()
    for (key0, key1, key2), group in df_bundle_sku.groupby(['platform','shipCountry', 'warehouse_id']):
        # print(key0, key1, key2, group.shape)
        group1 = group[['sku', '数量']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea(key0, key1, key2, w_list1, '')
        group2 = yunfei_jisuan.batch_df_order(group1)
        # print(group2.info())
        group2 = group2[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group2['platform'] = key0
        group3 = group.merge(group2, on=['sku', '数量','platform','shipCountry'])
        group3 = group3[
            ['sku', '数量', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group3 = group3.sort_values(['totalCost'], ascending=True)
        # group3 = group3.drop_duplicates(['shipCountry', 'sku'], 'first')
        df_result = pd.concat([group3, df_result])
    # print(df_result.info())
    return df_result

def get_toucheng_price_new(df):
    """
    获取头程单价：
    1、优先用子仓匹配，筛选主要渠道的单价（普货）
    2、其次用分摊头程的头程
    """
    # 取子仓
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    sql = """
        SELECT distinct warehouse_id warehouseId, area
        FROM yibai_oversea.yibai_warehouse_oversea_temp
    """
    df_area = conn_ck.ck_select_to_df(sql)

    # 取头程单价
    sql = """
        SELECT DISTINCT area, `计费方式` weight_method,
        `头程计泡系数` dim_weight, `是否包税` include_tax, `普货单价` price, warehouse
        FROM yibai_oversea.oversea_fees_parameter_new
        WHERE `是否主要渠道` = 1
    """
    df_tc = conn_ck.ck_select_to_df(sql)
    df_tc = pd.merge(df_area, df_tc, how='left', on='area')
    df_tc = df_tc[~df_tc['price'].isna()]
    # df_tc = df_tc.drop_duplicates(subset=['warehouseId'])
    # df_tc.to_excel('F://Desktop//df_tc_temp.xlsx', index=0)
    col = ['warehouseId', 'price', 'weight_method', 'dim_weight', 'include_tax']
    df_tc['warehouseId'] = df_tc['warehouseId'].fillna(0).astype(int)
    df = pd.merge(df, df_tc[col], how='left', on=['warehouseId'])
    print(df.info())
    df['weight_volume'] = df['length'] * df['width'] * df['high']
    col = ['weight_volume', 'gross','price']
    df[col] = df[col].fillna(0).astype(float)
    df['dim_weight'] = df['dim_weight'].fillna(6000).astype(int)
    df['计费重'] = np.where(df['weight_volume']/df['dim_weight'] > df['gross']/1000,
                           df['weight_volume']/df['dim_weight'], df['gross']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']=='计费重',
                                          df['计费重'] * df['price'], df['weight_volume'] / df['dim_weight'] * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,1057,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['new_firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    # 20241010 保留分摊头程的总运费
    df['totalCost_origin'] = df['totalCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    col = ['price', 'weight_volume', '计费重', 'gross','weight_method','include_tax','dim_weight',
           'length','width','high']
    df.drop(columns=col, inplace=True, axis=1)


    return df


def expand_bundle_rows(df_bundle):
    # 1. 处理空值，避免 split 报错
    df_bundle['sku'] = df_bundle['sku'].fillna('')
    df_bundle['数量'] = df_bundle['数量'].fillna('')

    # 2. 按逗号分割（兼容 逗号、逗号+空格）
    df_bundle['sku_list'] = df_bundle['sku'].str.split(',\s*')
    df_bundle['qty_list'] = df_bundle['数量'].str.split(',\s*')

    # 3. 安全 explode：自动对齐长度，短的用最后一个值补齐（最稳）
    rows = []
    for _, row in df_bundle.iterrows():
        skus = row['sku_list']
        qtys = row['qty_list']

        # 自动补齐长度，以 sku 长度为准
        max_len = len(skus)
        qtys = qtys + [qtys[-1]] * (max_len - len(qtys)) if qtys else [''] * max_len

        # 逐行展开
        for s, q in zip(skus, qtys):
            new_row = row.copy()
            new_row['sku'] = s.strip()
            new_row['数量'] = q.strip()
            rows.append(new_row)

    # 生成新表
    df_expanded = pd.DataFrame(rows)
    df_expanded = df_expanded.drop(columns=['sku_list', 'qty_list'], errors='ignore')

    return df_expanded

def calculate_bundle_summary(df_bundle: pd.DataFrame) -> pd.DataFrame:
    """
    计算捆绑sku1的汇总成本、重量、库存、库龄
    :param df_bundle: 捆绑sku明细，字段: shipCountry,warehouse, sku1, sku, 数量
    :param df_sku_info: 子sku信息表，字段: sku, 成本, 重量, 库存, 库龄
    :return: 按warehouse+sku1汇总后的结果
    """
    # 1. 拆分sku1为子sku列表（处理 * 和 + 两种格式）
    # df_bundle = df_bundle.reset_index()
    # df_bundle_expand = df_bundle.set_index(['index', 'shipCountry', 'warehouse', 'platform','sku1']
    #                                        ).apply(lambda x: x.str.split(',').explode())

    df_bundle['sku_list'] = df_bundle['sku'].str.split(',')  # 兼容逗号+空格
    df_bundle['qty_list'] = df_bundle['数量'].str.split(',')
    # 这里的关键：先 explode 这两列，再处理其他字段，避免索引冲突
    # df_bundle_expand = df_bundle.explode(['sku_list', 'qty_list'], ignore_index=True)
    df_bundle_expand = expand_bundle_rows(df_bundle)
    # 可选：重置 index 让它从 0 开始
    df_bundle_expand = df_bundle_expand.reset_index(drop=True)
    df_bundle_expand = df_bundle_expand[(~df_bundle_expand['sku'].isna()) & (df_bundle_expand['sku']!='')]

    # df_bundle_expand.to_excel('F://Desktop//df_bundle_sku.xlsx', index=0)
    # df_bundle_expand = df_bundle_expand.reset_index()
    # 2. 获取海外仓sku信息
    sql = """
        SELECT sku, warehouse, best_warehouse_id warehouse_id, new_price, gross, available_stock, overage_level
        FROM yibai_oversea.dwm_sku_temp_info
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_sku_temp_info)
        and best_warehouse_name not like '%精品%'
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_sku_info = conn_ck.ck_select_to_df(sql)

    # 3. 关联子sku的基础信息
    df_merge = pd.merge(df_bundle_expand, df_sku_info, on=['sku','warehouse'], how='left')
    df_merge['sku_match'] = df_merge['new_price'].notna()

    # 3.1 只保留所有子sku都匹配成功的主sku数据
    group_check = df_merge.groupby(['warehouse', 'sku1'])['sku_match'].all().reset_index()
    group_check.columns = ['warehouse', 'sku1', 'all_sku_matched']
    df_merge = pd.merge(df_merge, group_check, on=['warehouse', 'sku1'], how='left')
    df_merge = df_merge[df_merge['all_sku_matched'] == True]  # 剔除有缺失的主sku

    # 4. 计算子sku的贡献值（数量 * 单sku成本/重量）
    print(df_merge.info())
    df_merge['数量'] = df_merge['数量'].astype(int)
    df_merge['new_price'] = df_merge['数量'] * df_merge['new_price']
    df_merge['gross'] = df_merge['数量'] * df_merge['gross']

    def concat_warehouse_id(series):
        # 步骤1：剔除空值 → 去重 → 转为整数（确保是数字）→ 排序（可选，保证1,3而非3,1）
        unique_ids = (
            series.dropna()          # 剔除空值
                  .astype(int)       # 强制转为整数（避免浮点型如1.0）
                  .unique()          # 去重
        )
        # 步骤2：按逗号拼接为"1,3"格式
        return ','.join(map(str, unique_ids))
    # 5. 按 warehouse + sku1 汇总
    agg_rules = {
        'new_price': 'sum',          # 成本相加
        'gross': 'sum',          # 重量相加
        'available_stock': 'min',            # 库存取最小值
        'overage_level': 'max',             # 库龄取最大值
        'warehouse_id': concat_warehouse_id
    }
    df_merge = df_merge.groupby(['warehouse', 'sku1', 'shipCountry'], as_index=False).agg(agg_rules)

    # 6. 重命名列（还原语义）
    # df_summary.rename(columns={'子成本': '汇总成本','子重量': '汇总重量'}, inplace=True)

    return df_merge


def load_interface(df, platform='TEMU'):
    """
    调用运费接口获取sku海外仓运费
    """
    df_result = pd.DataFrame()
    # key2 = '49;769;325;88;50;353;47;680;58;1139'
    for (key1, key2), group in df.groupby(['country', 'warehouse_id']):
        # print(key1, key2, group.shape)
        group1 = group[['sku', '数量', 'ship_zip']]
        # # for warehouse in df_oversea_warehouse['warehouse_id'].unique():
        yunfei_jisuan = get_trip_fee_oversea(platform, key1, key2, '1,2,3,4,5,6,8,12,16,17,18,26,27,28,29,30','')
        group2 = yunfei_jisuan.batch_df_order(group1)
        group2 = group2[
            ['sku', '数量', 'ship_zip', 'shipCountry', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
             'totalCost', 'shippingCost', 'firstCarrierCost','dutyCost']]
        group2['platform'] = platform
        group3 = group.merge(group2, on=['sku', '数量', 'ship_zip'])
        # group3 = group3[['sku', '数量', 'ship_zip', 'shipCountry', 'platform', 'warehouseId', 'warehouseName', 'shipCode', 'shipName',
        #      'totalCost','shippingCost', 'firstCarrierCost','dutyCost']]
        df_result = pd.concat([group3, df_result])

    return df_result


def temp():
    # 限时清仓sku
    sql = """
        SELECT YM_sku, warehouse, available_stock
        FROM yibai_oversea.dwm_supplier_sku_price
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.dwm_supplier_sku_price )
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = conn_ck.ck_select_to_df(sql)

    df.to_excel('F://Desktop//df_ym_stock.xlsx', index=0)

def get_oversea_rent():
    # date_today = datetime.datetime.now().strftime('%Y-%m-%d')
    # date_today = datetime.date.today() - datetime.timedelta(days=100)
    date_today = '2026-03-01'
    date_end = '2026-04-01'
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    sql = f"""
    SELECT date_id, warehouse_id, warehouse_name, warehouse, country, sum(charge_total_price) charge_total_price
    FROM (
        SELECT  
            date as date_id, sku, yw.id warehouse_id, warehouse_name,
            case 
                when ya.country = 'GB' then 'UK' 
                when ya.country = 'CS' then 'DE'
                when ya.warehouse_code = 'WYT_CATO' then 'CA' 
                when ya.country = 'USEA' then 'US'
                when ya.country = 'USWE' then 'US'
                else ya.country 
            end as country, 
            ywc.name warehouse,
            warehouse_stock, inventory_age, charge_total_price,
            case
                when inventory_age <= 60 then 0
                when inventory_age <= 90 and inventory_age > 60 then 60
                when inventory_age <= 120 and inventory_age > 90 then 90
                when inventory_age <= 150 and inventory_age > 120 then 120
                when inventory_age <= 180 and inventory_age > 150 then 150
                when inventory_age <= 270 and inventory_age > 180 then 180
                when inventory_age <= 360 and inventory_age > 270 then 270
                when inventory_age > 360 then 360
            end as overage_level
        FROM yb_datacenter.yb_oversea_sku_age ya
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse AS yw ON ya.order_warehouse_code = yw.warehouse_code
        LEFT JOIN yibai_logistics_tms_sync.yibai_warehouse_category AS ywc ON yw.ebay_category_id = ywc.id
        WHERE 
            date >= '{date_today}' 
            and date < '{date_end}'
            and status in (0,1) 
            and warehouse_stock > 0
            and warehouse != '中国仓'
            and warehouse_id != 0
            -- and yw.warehouse_name not like '%独享%' 
            -- and yw.warehouse_name not like '%TT%'
            -- and ya.order_warehouse_code not like '%TT%'      -- 库龄表TT仓库code使用字段order_warehouse_code
    ) a
    GROUP BY date_id, warehouse_id, warehouse_name, warehouse, country
    """
    conn_ck = pd_to_ck(database='yb_datacenter', data_sys='调价明细历史数据')
    df_age = conn_ck.ck_select_to_df(sql)
    print(df_age.info())
    #
    df_rate = get_rate()
    #
    df_age = pd.merge(df_age, df_rate[['country', 'erp_rate']], how='left', on=['country'])
    df_age = df_age.drop_duplicates()
    #
    df_age['charge_total_price'] = df_age['charge_total_price'].astype(float)
    df_age['charge_total_price_rmb'] = df_age['charge_total_price'] * df_age['erp_rate']
    # df_age.drop('rate', axis=1, inplace=True)
    #
    df_age.to_excel('F://Desktop//df_rent_info.xlsx', index=0)

    # write_to_sql(df_age, 'ads_oversea_rent')
    # conn.to_sql(df_age, 'ads_oversea_rent', if_exists='append')


def get_sku_line():
    df = pd.read_excel('F://Desktop//sku_temp.xlsx', dtype={'sku':str})


if __name__ == "__main__":
    # 1. 高pcs降价逻辑
    # get_high_pcs_logic()

    # 2. 供应商货盘逻辑
    # get_sup_stock_id()
    # get_supplier_price(df)
    # get_supplier_fee()
    # get_sup_useful_fee()
    # supplier_sku_info()

    # 3. 限时清仓sku, 分次降到销毁价
    # flash_sku_to_lowest()

    # 4. 可继续清仓sku、
    # get_temp_clear_sku()

    # 5、三仓都有库存sku运费获取
    # get_target_sku()
    # get_target_sku_fee()
    # check_target_sku()

    # 7、walmart云仓美西仓库存设置
    # walmart_dtl_stock_logic()

    # 8、捆绑sku
    # get_bundle_sku()
    # get_bundle_sku_price()

    # get_sup_sku_second_stocking()
    # update_sup_platform_fee()
    # temp()
    get_oversea_rent()
