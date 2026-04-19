import pandas as pd
import time, datetime
import warnings
from pulic_func.base_api.mysql_connect import connect_to_sql, sql_to_pd, pd_to_ck
from pulic_func.price_adjust_web_service.yibaiAPI_domestic import get_trip_fee_oversea
from pulic_func.price_adjust_web_service.daingjia_public import get_oversea_ship_type_list
from all_auto_task.oversea_price_adjust_tt import tt_get_warehouse, tt_get_oversea_order
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ======================== 1. 配置规则（和之前一致，无需修改）========================
PRICING_RULES = {
    (0.5, 1): {
        (0, 60): "回调至目标净利",
        (60, 80): 3,
        (80, 100): 2,
        (100, 150): 0,
        (150, 200): -1,
        (200, 300): -2,
        (300, float('inf')): -3,
    },
    (0.3, 0.5): {
        (60, 100): 0,
        (100, 200): -1,
        (200, 300): -2,
        (300, float('inf')): -3,
    },
    (0.1, 0.3): {
        (100, 200): -1.5,
        (200, 300): -2.5,
        (300, float('inf')): -3,
    },
    (0, 0.1): {
        (300, float('inf')): -4,
    }
}

BASIC_CONFIG = {
    "first_entry_stock_qty": 50,
    "first_entry_stock_age": 30,
    "price_adjust_interval": 6,
    "max_single_increase": 5,
}

# ======================== 2. 向量化工具函数（无循环）========================
def preprocess_data(df):
    """
    数据预处理：生成分箱标签、调价间隔校验、首次进入条件校验（全向量化）
    """
    df = df.copy()  # 避免修改原数据

    # 2.1 计算上次调价天数差（向量化，无需逐行计算datetime）
    df["上次调价日期"] = pd.to_datetime(df["上次调价日期"], errors="coerce")  # 统一转为datetime
    today = pd.Timestamp.now()
    df["调价天数差"] = (today - df["上次调价日期"]).dt.days  # 向量化计算天数差
    df["调价天数差"] = df["调价天数差"].fillna(-1)  # 空值视为-1（允许调价）

    # 2.2 校验调价间隔（布尔索引，向量化）
    df["满足调价间隔"] = df["调价天数差"] >= BASIC_CONFIG["price_adjust_interval"] | (df["调价天数差"] == -1)

    # 2.3 校验首次进入条件（布尔索引，向量化）
    df["首次进入满足条件"] = ~df["是否首次进入"]  # 非首次进入默认满足
    df.loc[df["是否首次进入"], "首次进入满足条件"] = (
        (df.loc[df["是否首次进入"], "available_stock"] >= BASIC_CONFIG["first_entry_stock_qty"])
        & (df.loc[df["是否首次进入"], "overage_level"] >= BASIC_CONFIG["first_entry_stock_age"])
    )

    # 2.4 校验不降价规则（向量化）
    df["近15天销量"] = df["近15天销量"].fillna(0)
    df["不降价标记"] = (df["近7天销量"] / df["近15天销量"] >= 0.6).where(df["近15天销量"] != 0, False)

    # 2.5 定义分箱边界（对应PRICING_RULES的区间，左开右闭）
    # day_sales分箱边界：0 → 0.1 → 0.3 → 0.5 → 1 → ∞
    sales_bins = [0, 0.1, 0.3, 0.5, 1.0, float('inf')]
    sales_labels = ["(0,0.1]", "(0.1,0.3]", "(0.3,0.5]", "(0.5,1]", "(1,inf)"]
    # 可售天数分箱边界：0 → 60 → 80 → 100 → 150 → 200 → 300 → ∞
    days_bins = [0, 60, 80, 100, 150, 200, 300, float('inf')]
    days_labels = ["(0,60]", "(60,80]", "(80,100]", "(100,150]", "(150,200]", "(200,300]", "(300,inf)"]

    # 2.6 向量化分箱（给每行打区间标签）
    df["销量区间"] = pd.cut(
        df["day_sales"], bins=sales_bins, labels=sales_labels, right=True, include_lowest=False
    )
    df["可售天数区间"] = pd.cut(
        df["可售天数"], bins=days_bins, labels=days_labels, right=True, include_lowest=False
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
        (0, 0.1): "(0,0.1]",
        (0.1, 0.3): "(0.1,0.3]",
        (0.3, 0.5): "(0.3,0.5]",
        (0.5, 1): "(0.5,1]",
        (1, float('inf')): "(1,inf)"
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
    rule_map["default"] = BASIC_CONFIG["max_single_increase"]
    return rule_map

# ======================== 3. 核心向量化计算（无循环）========================
def vectorized_pricing_calc(df):
    """
    全向量化定价计算
    """
    # 步骤1：数据预处理（分箱、条件校验）
    df_processed = preprocess_data(df)

    # 步骤2：构建规则映射字典
    rule_map = build_rule_map()

    # 步骤3：批量匹配涨跌幅（向量化map，无循环）
    df_processed["匹配键"] = list(zip(df_processed["销量区间"], df_processed["可售天数区间"]))
    df_processed["涨跌幅"] = df_processed["匹配键"].map(lambda x: rule_map.get(x, rule_map["default"]))

    # 步骤4：处理特殊逻辑和条件过滤（全向量化）
    # 4.1 分离特殊标记和数值涨跌幅
    # 新增：标记特殊逻辑行
    df_processed["是否特殊逻辑"] = df_processed["涨跌幅"].apply(lambda x: isinstance(x, str))
    # 数值涨跌幅列（特殊逻辑行填充为0，不影响计算）
    df_processed["涨跌幅_数值"] = df_processed["涨跌幅"].where(
        ~df_processed["是否特殊逻辑"], 0
    ).astype(float)  # 强制转为float类型

    # 4.2 初始化新利润率
    # 数值型新利润率 = 当前利润率 + 数值涨跌幅
    df_processed["新利润率_数值"] = df_processed["after_profit"] + df_processed["涨跌幅_数值"]
    # 初始化结果列（默认用数值结果）
    df_processed["新利润率/状态"] = df_processed["新利润率_数值"]

    # 4.3 替换特殊逻辑行的结果
    df_processed.loc[df_processed["是否特殊逻辑"], "新利润率/状态"] = df_processed.loc[
        df_processed["是否特殊逻辑"], "涨跌幅"
    ]

    # 4.4 条件过滤：不满足调价间隔/首次进入条件 → 保持原利润率
    keep_original_mask = ~df_processed["满足调价间隔"] | ~df_processed["首次进入满足条件"]
    df_processed.loc[keep_original_mask, "新利润率/状态"] = df_processed.loc[
        keep_original_mask, "after_profit"
    ]

    # 4.5 条件过滤：降价时若触发不降价规则 → 保持原利润率（仅对数值型涨跌幅生效）
    # 关键修复：用「涨跌幅_数值 < 0」替代「涨跌幅 < 0」，避免字符串比较
    price_down_mask = (df_processed["涨跌幅_数值"] < 0) & df_processed["不降价标记"]
    df_processed.loc[price_down_mask, "新利润率/状态"] = df_processed.loc[
        price_down_mask, "after_profit"
    ]

    # 步骤5：清理临时列，返回结果
    result_cols = [
        "sku", "available_stock", "overage_level", "day_sales", "estimated_sales_days",
        "after_profit", "是否首次进入", "新利润率/状态"
    ]
    df_result = df_processed[result_cols].copy()

    # 优化：保持结果类型一致性（数值为float，特殊标记为str）
    return df_result

# ======================== 4. Pandas 大数据量测试示例 ========================

def test_temp():
    np.random.seed(42)  # 固定随机种子，结果可复现

    n_rows = 50000  # 50万行数据
    today = pd.Timestamp.now()

    df = pd.DataFrame({
        "sku": [f"A{str(i).zfill(6)}" for i in range(n_rows)],
        "available_stock": np.random.randint(10, 200, size=n_rows),  # 库存10~200
        "overage_level": np.random.randint(10, 100, size=n_rows),   # 库龄10~100天
        "day_sales": np.random.choice(
            [np.random.uniform(0, 0.1), np.random.uniform(0.1, 0.3),
             np.random.uniform(0.3, 0.5), np.random.uniform(0.5, 1),
             np.random.uniform(1, 3)],
            size=n_rows,
            p=[0.2, 0.2, 0.2, 0.2, 0.2]  # 各区间概率均等
        ),
        "estimated_sales_days": np.random.choice(
            [np.random.uniform(0, 60), np.random.uniform(60, 80),
             np.random.uniform(80, 100), np.random.uniform(100, 150),
             np.random.uniform(150, 200), np.random.uniform(200, 300),
             np.random.uniform(300, 1000)],
            size=n_rows,
            p=[0.15, 0.15, 0.15, 0.15, 0.15, 0.15, 0.1]
        ),
        "after_profit": np.random.uniform(15, 30, size=n_rows),  # 利润率15%~30%
        "是否首次进入": np.random.choice([True, False], size=n_rows, p=[0.3, 0.7]),
        "上次调价日期": np.random.choice(
            [today - timedelta(days=d) for d in range(10)] + [pd.NA],  # 10个日期 + 1个NA（共11个候选）
            size=n_rows,
            p=[0.09] * 10 + [0.1]  # 前10个日期各0.09概率，NA占0.1概率，总和=1.0
        ),
        "近7天销量": np.random.uniform(0, 10, size=n_rows),
        "近15天销量": np.random.uniform(0, 20, size=n_rows)
    })

    # 步骤2：执行向量化计算（计时）
    print(f"数据量：{n_rows:,} 行")
    print("开始计算...")
    start_time = datetime.now()

    df_result = vectorized_pricing_calc(df)

    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"计算完成！耗时：{elapsed_time:.2f} 秒")

    # 步骤3：查看结果（前10行）
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    print("\n前10行结果：")
    print(df_result.head(10))

    # 步骤4：验证规则匹配正确性（抽样检查）
    print("\n规则匹配验证：")
    sample = df_result[df_result["新利润率/状态"] != df_result["after_profit"]].head(5)
    print("有调价的抽样数据：")
    print(sample[["sku", "day_sales", "estimated_sales_days", "after_profit", "新利润率/状态"]])


def main():
    """ 真实数据验证 """
    sql = """
        SELECT *
        FROM over_sea.dwm_sku_temp_info
        WHERE date_id = '2025-11-11' and available_stock > 0
    """
    conn = connect_to_sql(database='over_sea', data_sys='数据部服务器')
    df = conn.read_sql(sql)

    print(f"数据量：{len(df):,} 行")
    print("开始计算...")
    start_time = datetime.now()

    df_result = vectorized_pricing_calc(df)

    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"计算完成！耗时：{elapsed_time:.2f} 秒")

    df_result.to_excel('F://Desktop//df_result.xlsx', index=0)


if __name__ == "__main__":
    # 步骤1：构造50万行测试数据（模拟真实业务数据）
    # test()
    main()


