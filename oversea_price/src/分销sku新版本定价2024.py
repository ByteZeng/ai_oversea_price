##
import time
import warnings
from utils.utils import read_sql_ck, get_ck_client, save_df, get_mysql_con, \
    make_path, get_path
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from utils import utils
import src.fetch_data as fd
from pulic_func.base_api.mysql_connect import pd_to_ck, sql_to_pd,connect_to_sql
from pulic_func.price_adjust_web_service.daingjia_public import chicun_zhongliang
from all_auto_task.sku_to_anywhere_price import get_trip_fee_oversea2
from all_auto_task.oversea_price_adjust_2023 import get_rate
from all_auto_task.oversea_price_adjust_tt import tt_get_sku
from all_auto_task.oversea_temu_price import get_temu_listing, get_temu_account, get_line
from all_auto_task.oversea_temu_shield import extract_correct_string, warehouse_mark, get_main_resp
warnings.filterwarnings('ignore')
##

def write_to_sql(df, table_name):
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    date_today = datetime.date.today()
    sql = f"""
    delete from over_sea.{table_name} where date_id >='{date_today}'
    """
    conn.execute(sql)

    conn.to_sql(df, table_name, if_exists='append')

def get_overseas_fee(df):
    """  分销的海外仓处理费按标准自算 """
    sql = """
        SELECT *
        FROM over_sea.oversea_fenxiao_fee
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df_rules = conn.read_sql(sql)
    columns = df_rules.columns
    rule_melt = df_rules.melt(
        id_vars=['min', 'max'],
        value_vars=columns[2:],
        var_name='大仓',
        value_name='fee'
    ).sort_values(by=['大仓', 'min'])
    rule_melt['min'] = np.where(rule_melt['min'] != 0, rule_melt['min'] + 0.00001, rule_melt['min'])

    print(rule_melt)
    df['毛重'] = df['毛重'].astype(float)
    df['毛重'] = df['毛重']/1000
    print(df.info())

    def calculate_fee(input_df):
        """
        核心计算逻辑：
        1. 处理重量区间匹配
        2. 处理仓库类型匹配
        3. 异常值处理
        """
        # 生成副本避免修改原始数据
        df = input_df.copy()

        # 转换数据类型
        df['毛重'] = pd.to_numeric(df['毛重'], errors='coerce')

        # 合并规则数据（使用merge_asof处理区间匹配）
        result = pd.merge_asof(
            df.sort_values('毛重'),
            rule_melt.sort_values('min'),
            left_by='大仓',
            right_by='大仓',
            left_on='毛重',
            right_on='min',
            direction='backward'
        )

        # 处理未匹配到仓库的情况（使用其他仓）
        missing_mask = result['fee'].isnull()
        if missing_mask.any():
            other_rules = rule_melt[rule_melt['大仓'] == '其它仓']
            other_result = pd.merge_asof(
                df.sort_values('毛重'),
                other_rules,
                left_on='毛重',
                right_on='min',
                direction='backward'
            )
            other_result = other_result.rename(columns={'fee':'other_fee'})
            print(other_result.info())

        # # 处理超过最大重量区间的情况
        # overflow_mask = result['毛重'] > result['max']
        # if overflow_mask.any():
        #     max_rules = rule_melt.groupby('大仓').last().reset_index()
        #     result.loc[overflow_mask] = pd.merge(
        #         df[overflow_mask],
        #         max_rules,
        #         on='大仓',
        #         how='left'
        #     )
        result = result.drop_duplicates(subset=['sku','大仓'])
        df = pd.merge(df, result[['sku','大仓','min','max','fee']], how='left', on=['sku','大仓'])
        df = pd.merge(df, other_result[['sku','other_fee']].drop_duplicates(), how='left', on=['sku'])
        df['fee'] = np.where(df['fee'].isna(), df['other_fee'], df['fee'])

        df.drop(['min','max','other_fee'], axis=1, inplace=True)
        return df
        # 合并规则数据（使用merge_asof处理区间匹配）

    df = calculate_fee(df)

    return df

def main_new():
    """
    易佰海外仓分销对接

    """
    utils.program_name = '分销sku新版本定价'
    make_path()

    # 定义常用仓库列表，减少代码重复
    WH_NA = ['美国仓', '澳洲仓', '加拿大仓']
    WH_EU = ['英国仓', '德国仓', '法国仓', '西班牙仓', '意大利仓', '俄罗斯仓']
    WH_ALL_MAIN = WH_NA + WH_EU

    # ======================= 1. 数据获取 =======================
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')

    # 1.1 获取SKU基础信息
    sql_sku = """
        SELECT
            a.sku, title, type, product_status, linest, a.warehouse warehouse, available_stock, 
            best_warehouse_name, age_180_plus, product_package_size, estimated_sales_days,
            new_price, gross, best_warehouse_id, overage_level, `30days_sales`,
            new_price * 0.013 as `国内采购运费`
        FROM (
            SELECT *
            FROM over_sea.dwm_sku_temp_info
            WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info) 
            AND available_stock > 0
        ) a
    """
    df_sku = conn.read_sql(sql_sku)
    sql = """
        SELECT sku, warehouse, sale_status `销售状态`
        FROM oversea_sale_status
        WHERE end_time IS NULL
    """
    df_status = conn.read_sql(sql)
    df_sku = pd.merge(df_sku, df_status, how='left', on=['sku','warehouse'])
    df_sku['销售状态'] = df_sku['销售状态'].fillna('正常')
    print(df_sku.info())
    # 1.2 获取运费信息
    sql_fee = """
        SELECT 
            sku, warehouseName as `运费最优子仓`, warehouse `大仓`, shipCode, shipName, shippingCost `尾程`, 
            CASE WHEN firstCarrierCost <= 0 THEN new_firstCarrierCost ELSE firstCarrierCost END as `头程`, 
            dutyCost `关税`, remoteExtraFee `偏远附加费`, extraSizeFee `超尺寸附加费`,
            overseasFee `海外仓处理费`, country `收货站点`, 0 as is_temu
        FROM oversea_transport_fee_useful
        WHERE (platform='AMAZON' or platform in ('DMSOzon') or (platform = 'WISH' and country = 'AT'))
    """
    df_useful = conn.read_sql(sql_fee)

    # 运费数据预处理：排序并去重，保留大仓下最便宜渠道
    df_useful = df_useful.sort_values(by=['is_temu', '尾程'], ascending=[False, True])
    df_useful = df_useful.drop_duplicates(subset=['sku', '大仓', '收货站点'])

    # 1.3 获取MRP点位信息并计算is_shield
    df_mrp = mrp_sku_point()
    df_sku = pd.merge(df_sku, df_mrp[['sku', 'warehouse', 'is_stock']], how='left', on=['sku', 'warehouse'])

    # 计算屏蔽状态
    shield_cond = (df_sku['销售状态'] == '正常') & (df_sku['estimated_sales_days'] < 60) & (df_sku['is_stock'] == 1)
    df_sku['is_shield'] = np.where(shield_cond, 1, 0)

    # SKU数据列筛选与重命名
    cols_map = {
        'sku': 'sku', 'title': '标题', 'type': '开发来源', 'product_status': '产品状态',
        'linest': '产品线', 'warehouse': '大仓', 'available_stock': '大仓总库存',
        'best_warehouse_name': '最优发货子仓', 'overage_level': '超库龄等级',
        'age_180_plus': '超180天库龄库存', '30days_sales': '临时列',
        'product_package_size': '包装尺寸', 'gross': '毛重', 'new_price': '成本',
        'best_warehouse_id': 'best_warehouse_id', '销售状态': '销售状态',
        '国内采购运费': '国内采购运费', 'is_shield': 'is_shield'
    }
    df_sku = df_sku[list(cols_map.keys())].rename(columns=cols_map)

    # 1.4 合并SKU信息与运费信息
    df = pd.merge(df_sku, df_useful, how='left', on=['sku', '大仓'])

    # ======================= 2. 定价系数计算 =======================
    # 计算供货系数
    # 定义条件列表
    cond_north_america = df['大仓'].isin(WH_NA)
    cond_europe = df['大仓'].isin(WH_EU)
    cond_other = ~df['大仓'].isin(WH_ALL_MAIN)

    # 构建np.select所需的条件与值列表
    conditions = [
        cond_north_america & (df['超库龄等级'] >= 180),
        cond_north_america & (df['超库龄等级'] >= 150),
        cond_north_america & (df['超库龄等级'] >= 120),
        cond_north_america & (df['超库龄等级'] >= 90),
        cond_north_america & (df['超库龄等级'].isin([0, 30, 60])),

        cond_europe & (df['超库龄等级'] >= 180),
        cond_europe & (df['超库龄等级'] >= 150),
        cond_europe & (df['超库龄等级'] >= 120),
        cond_europe & (df['超库龄等级'] >= 90),
        cond_europe & (df['超库龄等级'].isin([0, 30, 60])),

        cond_other & (df['销售状态'].isin(['正常', '涨价缩销'])),
        cond_other & (df['销售状态'].isin(['正利润加快动销'])),
        cond_other & (df['销售状态'].isin(['负利润加快动销', '清仓']))
    ]
    choices = [1.05, 1.08, 1.12, 1.16, 1.2, 1.05, 1.10, 1.15, 1.2, 1.25, 1.25, 1.15, 0.8]

    df['供货系数'] = np.select(conditions, choices, default=np.nan)

    # 俄罗斯仓特殊上浮
    df['供货系数'] = np.where(df['大仓'] == '俄罗斯仓', df['供货系数'] + 0.05, df['供货系数'])

    # 计算原供货价格
    cost_cols = ['成本', '国内采购运费', '尾程', '头程', '关税', '超尺寸附加费', '海外仓处理费', '偏远附加费']
    df[cost_cols] = df[cost_cols].fillna(0).astype(float)
    df['原供货价格'] = df['供货系数'] * df[cost_cols].sum(axis=1)

    # ======================= 3. 数据补充与修正 =======================
    # 3.1 匹配目的国中文名
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    sql_country = "SELECT site1 as `目的国`, site3 as `收货站点` FROM domestic_warehouse_clear.site_table"
    df_country = conn_ck.ck_select_to_df(sql_country)
    df_country.loc[df_country['目的国'] == '英国', '收货站点'] = 'UK'

    df = df.merge(df_country, on='收货站点', how='left')

    # 国家编码映射
    country_dict = {'RU': '俄罗斯', 'CZ': '捷克', 'FI': '芬兰', 'SK': '斯洛伐克', 'DK': '丹麦',
                    'HU': '匈牙利', 'PT': '葡萄牙', 'GB': '英国', 'AT': '奥地利', 'PH': '菲律宾',
                    'VN': '越南', 'TH': '泰国', 'ID': '印度尼西亚', 'MY': '马来西亚'}
    df['目的国'] = np.where(df['目的国'].isna(), df['收货站点'].replace(country_dict), df['目的国'])

    # 调整列顺序 (使用insert替代reindex操作，更直观)
    df.insert(df.columns.get_loc('最优发货子仓'), '目的国', df.pop('目的国'))
    df = df.drop_duplicates()

    # 3.2 运费替换逻辑
    df_fee_temp = write_transport_fee_data_temp()
    cols_fee_rename = {
        'sku':'sku', 'warehouseName': '最优发货子仓', 'country': '收货站点', 'shipCode': 'shipCode_new',
        'shipName': 'shipName_new', 'shippingCost': 'shippingCost_new',
        'firstCarrierCost': 'firstCarrierCost_new', 'dutyCost': 'dutyCost_new',
        'remoteExtraFee': 'remoteExtraFee_new', 'extraSizeFee': 'extraSizeFee_new',
        'overseasFee': 'overseasFee_new'
    }
    df_fee_temp = df_fee_temp[cols_fee_rename.keys()].rename(columns=cols_fee_rename)

    df = pd.merge(df, df_fee_temp, how='left', on=['sku', '最优发货子仓', '收货站点'])

    # 批量替换运费相关列
    update_cols = ['shipCode', 'shipName', '尾程', '头程', '关税', '偏远附加费', '超尺寸附加费', '海外仓处理费']
    source_cols = ['shipCode_new', 'shipName_new', 'shippingCost_new', 'firstCarrierCost_new',
                   'dutyCost_new', 'remoteExtraFee_new', 'extraSizeFee_new', 'overseasFee_new']

    for u_col, s_col in zip(update_cols, source_cols):
        df[u_col] = np.where(df[s_col].isna(), df[u_col], df[s_col])

    df.drop(columns=source_cols, inplace=True)

    # 3.3 海外仓处理费替换 & 异型附加费
    df = get_overseas_fee(df)
    df['海外仓处理费'] = np.where(df['fee'].isna(), df['海外仓处理费'], df['fee'])
    df.drop('fee', axis=1, inplace=True, errors='ignore')

    df = df.drop_duplicates(subset=['sku', '大仓', '目的国'])

    # 获取异型附加费
    sql_extra = "SELECT sku, warehouse_name `大仓`, extraSizeFee as `异型附加费` FROM over_sea.fenxiao_fee_temp"
    df_extra = conn.read_sql(sql_extra)
    df = pd.merge(df, df_extra, how='left', on=['sku', '大仓'])
    df['异型附加费'] = df['异型附加费'].fillna(0).astype(float)

    # 3.4 偏远附加费计算 (需先计算中间成本)
    cost_basis_cols = ['成本', '国内采购运费', '尾程', '头程', '关税', '超尺寸附加费', '海外仓处理费', '异型附加费']
    df['cost_sum'] = df[cost_basis_cols].sum(axis=1)
    df['偏远附加费'] = np.where(df['大仓'] == '加拿大仓', df['cost_sum'] * 0.25, df['cost_sum'] * 0.02)

    # ======================= 4. TEMU热销品调整 =======================
    df_listing = sku_lower_price().rename(columns={'warehouse': '大仓'})
    df_sales = mrp_oversea_sales().rename(columns={'warehouse': '大仓'})
    df_dev_source = tt_get_sku()[['sku', 'develop_source_name']]

    df = pd.merge(df, df_listing, how='left', on=['sku', '大仓'])
    df = pd.merge(df, df_sales, how='left', on=['sku', '大仓'])
    df = pd.merge(df, df_dev_source, how='left', on='sku')

    df['online_price_rate'] = df['online_price_rate'].fillna(0).astype(float)

    # TEMU调价条件
    temu_cond1 = (df['大仓'].isin(WH_NA)) & (df['effective_sales'] > 2) & (df['online_price_rate'] >= 0.28)
    temu_cond2 = (df['大仓'].isin(['英国仓', '德国仓', '法国仓', '西班牙仓', '意大利仓'])) & (
                df['effective_sales'] > 1) & (df['online_price_rate'] >= 0.28)

    # 更新供货系数
    df['供货系数'] = np.select([temu_cond1, temu_cond2], [1.25, 1.3], default=df['供货系数'])

    # 最终供货价格计算
    final_cost_cols = ['成本', '国内采购运费', '尾程', '头程', '关税', '超尺寸附加费', '海外仓处理费', '异型附加费',
                       '偏远附加费']
    df[final_cost_cols] = df[final_cost_cols].astype(float)
    df['供货价格'] = df['供货系数'] * df[final_cost_cols].sum(axis=1)

    # ======================= 5. 过滤与清洗 =======================
    # 剔除不可发渠道
    filter_uk = (df['大仓'] == '英国仓') & (df['目的国'] != '英国')
    filter_non_uk = (~df['大仓'].isin(['英国仓'])) & (df['目的国'] == '英国')
    df = df[~(filter_uk | filter_non_uk)]

    df = df[~df['尾程'].isna()]
    df['头程'] = df['头程'].fillna(0).astype(float)
    df = df[df['头程'] > 0]
    df = df[df['成本'] > 0]

    # 剔除品牌SKU
    sku_ban = pd.read_excel('F://Desktop//TT云仓库存数据0927--开放易佰销售.xlsx', dtype={'sku': str})
    df = pd.merge(df, sku_ban, how='left', on='sku')
    df['是否开放分销'] = np.where(~df['来源'].isna(), '否', '是')
    df['不开放分销原因'] = np.where(df['是否开放分销'] == '否', 'TT不开放sku', '')
    df = df[df['是否开放分销'] == '是']

    # 剔除不开放的开发来源
    line_ban = pd.read_excel('F://Desktop//开发来源分类.xlsx')
    df = pd.merge(df, line_ban[['开发来源', '是否开放给分销']], how='left', on='开发来源')
    df['是否开放分销'] = np.where(df['是否开放给分销'] == '否', '否', df['是否开放分销'])
    df['不开放分销原因'] = np.where(df['是否开放给分销'] == '否', '不开放的开发来源', df['不开放分销原因'])

    # 库存与屏蔽判断
    shield_logic = (df['is_shield'] == 1) & (df['大仓'] != '墨西哥仓')
    df['是否开放分销'] = np.where(shield_logic, '否', df['是否开放分销'])
    df['不开放分销原因'] = np.where(shield_logic, '正常品、计划判断满足备货条件的、总可售天数<60', df['不开放分销原因'])

    # 最终列选择与去重
    final_cols = ['sku', '标题', '开发来源', '产品状态', '产品线', '大仓', '大仓总库存', '目的国', '最优发货子仓',
                  '运费最优子仓', '包装尺寸', '毛重', '超库龄等级', '超180天库龄库存', '销售状态', 'shipCode',
                  'shipName', '收货站点', '成本', '国内采购运费', '头程', '关税', '尾程', '偏远附加费',
                  '超尺寸附加费', '海外仓处理费', '异型附加费', '供货系数', '供货价格', '是否开放分销',
                  '不开放分销原因', 'develop_source_name', 'is_shield', 'effective_sales', 'online_price_rate']
    df = df[final_cols]
    df = limit_price(df)
    df = df.drop_duplicates(subset=['sku', '大仓', '目的国'])

    # 库存显示格式化
    df['大仓总库存'] = df['大仓总库存'].astype(float)
    df['大仓总库存'] = np.where(df['大仓总库存'] > 5, '5+', df['大仓总库存'])
    df['超180天库龄库存'] = df['超180天库龄库存'].astype(float)
    df['超180天库龄库存'] = np.where(df['超180天库龄库存'] > 5, '5+', df['超180天库龄库存'])

    # 汇率匹配
    df_rate = get_rate().rename(columns={'country': '收货站点'})
    df = pd.merge(df, df_rate[['收货站点', 'rate']], how='left', on='收货站点')

    # ======================= 6. 历史数据对比 =======================
    last_data = pd.read_excel('F://Desktop//日常任务//分销定价//分销sku新版本定价0323.xlsx')
    last_cols = ['sku', '大仓', '目的国', '是否开放分销', '不开放分销原因']
    last_data = last_data[last_cols].drop_duplicates(subset=['sku', '大仓'])
    last_data = last_data.rename(
        columns={'是否开放分销': '上一期是否开放分销', '不开放分销原因': '上一期不开放分销原因'})

    df = pd.merge(df, last_data, how='left', on=['sku', '大仓'])

    # 新增不开放判断
    new_close_cond = (df['是否开放分销'] == '否') & (
                (df['上一期是否开放分销'] == '是') | df['上一期是否开放分销'].isna())
    df['新增不开放'] = np.where(new_close_cond, '是', '')

    # 新增开放判断
    new_open_cond = (df['是否开放分销'] == '是') & (
                (df['上一期是否开放分销'] == '否') | df['上一期是否开放分销'].isna())
    df['新增开放'] = np.where(new_open_cond, '是', '')

    return df


## 分销测算
def mrp_oversea_sales():
    """ 计划采购需求有效销量数据获取 """
    date_today = time.strftime('%Y%m%d')
    # date_today = '20251128'
    sql = f"""
        SELECT `标识`, 
        	multiIf(
			lower(splitByChar('$', `标识`)[2]) = 'ru', '俄罗斯仓',
			splitByChar('$', `标识`)[2] = 'ae', '阿联酋仓',
			splitByChar('$', `标识`)[2] = 'ar', '阿根廷仓',
			splitByChar('$', `标识`)[2] = 'br', '巴西仓',
			splitByChar('$', `标识`)[2] = 'ca', '加拿大仓',
			splitByChar('$', `标识`)[2] = 'cl', '智利仓',
			splitByChar('$', `标识`)[2] = 'co', '哥伦比亚仓',
			splitByChar('$', `标识`)[2] = 'de', '德国仓',
			splitByChar('$', `标识`)[2] = 'east', '美国仓',
			splitByChar('$', `标识`)[2] = 'es', '西班牙仓',
			splitByChar('$', `标识`)[2] = 'fr', '法国仓',
			splitByChar('$', `标识`)[2] = 'gb', '英国仓',
			splitByChar('$', `标识`)[2] = 'id', '印度尼西亚仓',
			splitByChar('$', `标识`)[2] = 'it', '意大利仓',
			splitByChar('$', `标识`)[2] = 'jp', '日本仓',
			splitByChar('$', `标识`)[2] = 'mel', '澳洲仓',
			splitByChar('$', `标识`)[2] = 'mx', '墨西哥仓',
			splitByChar('$', `标识`)[2] = 'my', '马来西亚仓',
			splitByChar('$', `标识`)[2] = 'pe', '秘鲁仓',
			splitByChar('$', `标识`)[2] = 'ph', '菲律宾仓',
			splitByChar('$', `标识`)[2] = 'vn', '越南仓',
			splitByChar('$', `标识`)[2] = 'pl', '波兰仓',
			splitByChar('$', `标识`)[2] = 'syd', '澳洲仓',
			splitByChar('$', `标识`)[2] = 'th', '泰国仓',
			splitByChar('$', `标识`)[2] = 'uy', '乌拉圭仓',
			splitByChar('$', `标识`)[2] = 'west', '美国仓',
			splitByChar('$', `标识`)[2] = 'sa', '沙特仓',
			splitByChar('$', `标识`)[2] = 'south', '美国仓',
			''
		) as `区域大仓`, `加权销量` effective_sales
        FROM yibai_mrp_oversea.yibai_oversea_sale_mean_supplier_dcm_{date_today}
    """
    conn_ck = pd_to_ck(database='yibai_mrp_oversea', data_sys='易佰MRP')
    df_point_sales = conn_ck.ck_select_to_df(sql)
    print(df_point_sales.info())

    df_point_sales['sku'] = df_point_sales['标识'].str.split('$').str[0]
    df_point_sales['warehouse'] = np.where(df_point_sales['区域大仓'].isin(['美东仓','美西仓']), '美国仓', df_point_sales['区域大仓'])
    df_point_sales['warehouse'] = np.where(df_point_sales['warehouse'].isin(['澳洲悉尼仓', '澳洲墨尔本仓']), '澳洲仓',
                                           df_point_sales['warehouse'])
    print(df_point_sales['warehouse'].unique())
    df_point_sales = df_point_sales.groupby(['sku','warehouse']).agg({'effective_sales':'sum'}).reset_index()

    #
    # sql = """
    #     SELECT sku, warehouse, available_stock, best_warehouse_name, day_sales, `30days_sales`, overage_level
    #     FROM over_sea.dwm_sku_temp_info
    #     WHERE date_id = '2025-01-14'
    # """
    # conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # df_temp = conn.read_sql(sql)
    #
    # df_temp = pd.merge(df_temp, df_point_sales, how='left', on=['sku','warehouse'])
    #
    # df_temp.to_excel('F://Desktop//df_temp.xlsx', index=0)

    return df_point_sales

def mrp_sku_point():
    """ 获取海外仓有采购需求的sku """
    # 20250811 正常品、计划判断满足备货条件的、总可售天数<60，则不开放并通知分销屏蔽
    df_temp = pd.read_excel('F://Ding_workspace//海外仓备货建议表20260413.xlsx', dtype={'sku':str})
    df_temp = df_temp[(df_temp['是否满足海外备货条件']=='是') | (df_temp['同期季节日销是否满足备货条件']=='是')]
    df_temp = df_temp[['sku', '国家']]
    df_temp['is_stock'] = 1
    dic = {'美国':'美国仓','英国':'英国仓','澳洲':'澳洲仓','加拿大':'加拿大仓','欧洲':'德国仓','俄罗斯':'俄罗斯仓',
           '泰国':'泰国仓','马来西亚':'马来西亚仓','菲律宾':'菲律宾仓','墨西哥':'墨西哥仓'}
    df_temp['warehouse'] = df_temp['国家'].replace(dic)
    print(df_temp.info())

    return df_temp

def fine_sku():
    """ 获取精品转泛品且开放给亿迈的sku """
    df = pd.read_excel('F:\Desktop\精铺大部转泛品-释放给亿迈的SKU20250213.xlsx')
    print(df.info())
    df = df[['sku','国家']]
    df['精品是否释放给分销'] = 1
    df = df[['sku','精品是否释放给分销','国家']]

    return df

def sku_lower_price():
    """ sku在TEMU链接最低价（责任人链接）"""
    sql = """
        SELECT account_id, country, product_sku_id, sku, sku new_sku, online_status, warehouse, 
        supplier_price, online_profit_rate
        FROM yibai_oversea.oversea_temu_listing_all
        WHERE date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_temu_listing_all) 
        and online_status in ('已发布到站点','已加入站点') and total_cost > 0
    """
    conn_ck = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df_listing = conn_ck.ck_select_to_df(sql)

    df_listing = df_listing.sort_values(
        by=['supplier_price','online_profit_rate'], ascending=[True,True]).drop_duplicates(subset=['sku','warehouse'])

    temu_account = get_temu_account()
    df_listing = pd.merge(df_listing, temu_account[['account_id', 'main_name']], how='left', on=['account_id'])
    df_listing = df_listing.rename(columns={'main_name':'主体账号','online_profit_rate':'online_price_rate'})
    dic = {'DE': '欧洲', 'FR': '欧洲', 'IT': '欧洲', 'ES': '欧洲', 'SP': '欧洲', 'PL': '欧洲', 'CZ': '欧洲',
           'PT': '欧洲', 'HU': '欧洲', 'NL': '欧洲','SE': '欧洲', 'BE':'欧洲',
           'AU': '澳大利亚', 'NZ': '澳大利亚', 'US': '美国', 'UK': '英国', 'GB': '英国', 'CA': '加拿大', 'MX': '墨西哥'}
    df_listing['站点'] = df_listing['country'].apply(lambda x: next((dic[code] for code in x.split(',') if code in dic), None))
    df_listing = get_line(df_listing)
    df_listing = get_main_resp(df_listing)

    # df_listing = df_listing[df_listing['is_same']==1]

    col = ['sku','warehouse','product_sku_id','supplier_price','online_price_rate']
    df_listing = df_listing[col]
    print(df_listing.info())
    return df_listing

def limit_price(df):
    """ 限价数据 """
    # df = pd.read_excel('F:\Desktop\日常任务\分销定价\分销sku新版本定价汇总0106.xlsx')
    print(df.info())
    # 获取ttsku
    tt_sku = tt_get_sku()
    tt_sku = tt_sku[tt_sku['is_tt_sku']==1]
    df = pd.merge(df, tt_sku[['sku','develop_source']], how='left', on=['sku'])
    # 获取限价信息
    sql = """
        SELECT plat, site as `目的国`, sku, price
        FROM domestic_warehouse_clear.plat_sku_limit_price
    """
    # conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='通拓-新')
    conn_ck = pd_to_ck(database='domestic_warehouse_clear', data_sys='调价明细历史数据')
    df_limit = conn_ck.ck_select_to_df(sql)
    dic = {'AMAZON':1, 'ALL':2, 'EB':3}
    df_limit['rank'] = df_limit['plat'].map(dic).fillna(5)
    df_limit = df_limit.sort_values(by='rank', ascending=True).drop_duplicates(subset=['sku','目的国'])

    df = pd.merge(df, df_limit[['sku','目的国', 'plat','price']], how='left', on=['sku','目的国'])

    return df


# 分销临时运费。指定渠道
def write_transport_fee_data_temp():
    """

    """
    n = 8
    sql = f"""   select 
            a.sku as sku, a.warehouseId as warehouseId, warehouseName, logisticsId, shipCode, shipName, totalCost, shippingCost, 
            remoteExtraFee, extraSizeFee, packTypeFee, overseasFee, packFee, taxationFee, drawPrice, 
            firstCarrierCost, dutyCost, antidumpFee, overseaPackageFee, newPrice, createTime, a.platform as platform,
            a.country as country,
            0 as lowest_price,
            if(isNull(b.new_price),bb.new_price,b.new_price) as new_price,
            if (isNull(b.pur_weight_pack), bb.pur_weight_pack, b.pur_weight_pack) as pur_weight_pack,
            toFloat64( if (isNull(b.pur_length_pack), bb.pur_length_pack, b.pur_length_pack)) *
            toFloat64( if (isNull(b.pur_width_pack), bb.pur_width_pack, b.pur_width_pack)) *
            toFloat64( if (isNull(b.pur_height_pack), bb.pur_height_pack, b.pur_height_pack)) as weight_volume,
            0 limit_price_rmb,
            null as zero_percent,
            null as five_percent,
            available_stock,
            w.warehouse as warehouse,
            son_sku
            from 
            (
                select 
                    * except(date_id),
                    case when subString(sku, 1, 3) in ['GB-', 'DE-', 'FR-', 'ES-', 'IT-', 'AU-', 'CA-', 'JP-', 'US-'] then 
                         subString(sku, 4)
                    when subString(sku, 1, 2) in ['DE', 'GB'] then 
                         subString(sku, 3)
                    when subString(sku, -2) in ['DE', 'GB'] then 
                         subString(sku, 1, -2)
                    else sku
                    end as son_sku 
                from yibai_oversea.oversea_transport_fee_daily
                where warehouseName in  ('谷仓澳洲悉尼仓', '谷仓美国东仓', '谷仓英国仓')
                and shipCode in ('GCHWC_220312003','USPS- LWPARCEL','GCHWC_HERMES_DOMESTIC_PC')
                -- and date_id =  toYYYYMMDD(today()) - {n}
                and date_id = (SELECT max(date_id) FROM yibai_oversea.oversea_transport_fee_daily)
                order by toDate(createTime) desc,toDecimal64(totalCost,4) asc limit 1
                by sku, platform, country, warehouseId
            ) a
            LEFT JOIN (
                SELECT
                    id as warehouse_id, warehouse_name, warehouse_code,warehouse_type type, country,
                    CASE 
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
                        WHEN country='UY' THEN '乌拉圭仓'
                        WHEN country='BR' THEN '巴西仓'
                        WHEN country='RU' THEN '俄罗斯仓'
                        ELSE NULL 
                    END AS warehouse
                FROM yibai_logistics_tms_sync.yibai_warehouse
                WHERE warehouse_type IN (2,3,8)
            ) w ON toInt64(a.warehouseId) = toInt64(w.warehouse_id)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) b
            on (a.sku = b.sku)
            left join
            (
             select 
                sku,toFloat64(new_price) new_price,toFloat64(pur_length_pack) pur_length_pack,
                toFloat64(pur_width_pack) pur_width_pack,toFloat64(pur_height_pack) pur_height_pack,toFloat64(pur_weight_pack) pur_weight_pack 
             from yibai_prod_base_sync.yibai_prod_sku 
             where toFloat64(new_price)> 0 and (toFloat64(pur_length_pack)>1 or toFloat64(pur_width_pack)>1 or toFloat64(pur_height_pack)>1)
            ) bb
            on (a.son_sku = bb.sku)
            join 
            (
                select sku, warehouse_id as warehouseId, available_stock
                  from yb_datacenter.v_oversea_stock 
            ) g
            on (a.sku = g.sku and toInt64(a.warehouseId) = toInt64(g.warehouseId))
            left join 
            (
              select site as country, platform, pay_fee, paypal_fee, refound_fee, extra_fee, vat_fee 
              from yibai_wish.yibai_platform_fee
            ) c
            on (a.country = c.country and a.platform = c.platform)
        WHERE 
            (NOT (warehouse = '英国仓' and country not in ('UK','GB')))
            AND (NOT (warehouse != '英国仓' and country in ('UK','GB')))
            -- AND (NOT (warehouse = '美国仓' and country != 'US'))
        """
    # ck_client = CkClient(user='datax', password='datax#07231226', host='172.16.51.140', port='9001',
    #                      db_name='yibai_oversea')
    ck_client = pd_to_ck(database='yibai_oversea', data_sys='调价明细历史数据')
    df = ck_client.ck_select_to_df(sql)
    print(df.info())
    df['country'] = df['country'].replace('GB','UK')
    df = df.drop_duplicates()
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # 20240603取最新头程
    sql = """
        SELECT warehouse_id as warehouseId,price, weight_method, include_tax
        FROM `yibai_toucheng_new` 
        """
    df_toucheng = conn.read_sql(sql)
    df = pd.merge(df, df_toucheng, how='left', on=['warehouseId'])
    # df = df.merge(df_toucheng, on=['warehouseId'])
    col = ['weight_volume', 'pur_weight_pack','price']
    df[col] = df[col].fillna(0).astype(float)
    df['计费重'] = np.where(df['weight_volume']/6000 > df['pur_weight_pack']/1000,
                           df['weight_volume']/6000, df['pur_weight_pack']/1000)
    # 巴西、乌拉圭、俄罗斯头程暂不替换，采用物流提供的分摊法头程
    df['new_firstCarrierCost'] = np.where(df['weight_method']==1,
                                          df['计费重'] * df['price'], df['weight_volume'] / 1000000 * df['price'])
    df['new_firstCarrierCost'] = np.where(df['warehouseId'].isin([961,1019,1057,847]),
                                          df['firstCarrierCost'], df['new_firstCarrierCost'])
    col = ['totalCost','firstCarrierCost','new_firstCarrierCost','dutyCost']
    df[col] = df[col].astype(float)
    # 20241022未匹配到头程报价的子仓，头程取分摊
    df['new_firstCarrierCost'] = np.where(df['new_firstCarrierCost']==0, df['firstCarrierCost'], df['new_firstCarrierCost'])
    df['firstCarrierCost'] = np.where(df['firstCarrierCost'] == 0, df['new_firstCarrierCost'], df['firstCarrierCost'])

    # 2023-04-13 数据管理部自算头程去矫正totalCost（暂时还是调回来）
    # 20241010 保留分摊头程的总运费
    df['totalCost_origin'] = df['totalCost']
    df['totalCost'] = np.where(df['include_tax']==1,
                               df['totalCost'] - df['firstCarrierCost']-df['dutyCost'] + df['new_firstCarrierCost'],
                               df['totalCost'] - df['firstCarrierCost'] + df['new_firstCarrierCost'])
    df.drop(columns=['price', 'weight_volume', '计费重', 'pur_weight_pack','weight_method','include_tax'], inplace=True, axis=1)
    # print(df.info())
    # conn.to_sql(df, table='oversea_transport_fee_useful_temp', if_exists='append')
    print(df.info())
    print('指定渠道运费计算完成.')


    return df


# 不开放的sku，上传云仓系统
def upload_stock_center():
    """ 分销不开放的sku，需上传到云仓系统【禁止共享列表】 。按子仓维度 """
    df = pd.read_excel('F://Desktop//日常任务//分销定价//分销sku新版本定价0303.xlsx')
    col = ['sku', '大仓', '是否开放分销', '不开放分销原因']
    df = df[col].drop_duplicates(subset=['sku', '大仓'])
    df.columns = ['sku', 'warehouse', '是否开放分销', '不开放分销原因']
    df = df[df['是否开放分销']=='否']

    # 2、取子仓库存
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    sql = """
        SELECT Distinct sku, type, warehouse, warehouse_name, warehouse_other_type, available_stock, available_stock_money
        FROM over_sea.dwd_sku_info
        WHERE date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_info)
    """
    df_stock = conn.read_sql(sql)
    print(df_stock.info())
    #
    sql = """
        SELECT distinct warehouse_name, warehouse_code
        FROM yibai_logistics_tms_sync.yibai_warehouse
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)

    df = pd.merge(df, df_stock, how='inner', on=['sku', 'warehouse'])
    df = pd.merge(df, df_warehouse, how='left', on=['warehouse_name'])

    df = df[df['available_stock']>0]
    df.to_excel('F://Desktop//分销不开放sku.xlsx', index=0)

def temp_temp():
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    # sql = """
    #     SELECT Distinct sku, type, warehouse, warehouse_name, warehouse_other_type, available_stock, available_stock_money
    #     FROM over_sea.dwd_sku_info
    #     WHERE date_id = (SELECT max(date_id) FROM over_sea.dwd_sku_info)
    #     and available_stock > 0
    # """
    # df_stock = conn.read_sql(sql)

    sql = """
        SELECT distinct warehouse_name, warehouse_code
        FROM yibai_logistics_tms_sync.yibai_warehouse
    """
    conn_ck = pd_to_ck(database='yibai_logistics_tms_sync', data_sys='调价明细历史数据')
    df_warehouse = conn_ck.ck_select_to_df(sql)

    # df_stock = pd.merge(df_stock, df_warehouse, how='left', on=['warehouse_name'])

    df_warehouse.to_excel('F://Desktop//df_dwd_warehouse.xlsx', index=0)

##
if __name__ == '__main__':
    utils.program_name = '分销sku新版本定价'
    make_path()

    df_yibai = main_new()    # 需要更新：海外仓备货建议表、上一期数据
    save_df(df_yibai, '分销sku新版本定价_new', file_type='xlsx')
    # get_pj_line()

    # upload_stock_center()
    # mrp_oversea_sales()
    # supplier_sku()  # 供应商货盘sku
    # write_transport_fee_data_temp()
    # temp_temp()

