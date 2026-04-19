# main.py：主函数dwm_oversea_sku + 单独执行入口
from datetime import datetime
import pandas as pd
import numpy as np
from datetime import datetime


def load_base_config():
    """加载基础配置（替代硬编码，可迁移至YAML）"""
    return {
        "stock_age_segments": [30, 60, 90, 180],
        "max_adjust_rate": -1.0,
        "min_profit_rate": -0.5,
        "overstock_esd_threshold": 30,
        "shortage_esd_threshold": 3,
        "target_date": "2025-10-30",
        "charge_ratio_segments": [2, 4, 6, 12]
    }

def load_special_rules():
    """加载特殊仓库/SKU规则（从数据库查询，替代硬编码np.where）"""
    sql = """
        SELECT rule_type, apply_value, adjust_type, adjust_param
        FROM over_sea.special_adjust_rule
        WHERE status = 1
    """
    df = query_sql(sql)
    if df.empty:
        return {"warehouse_rule": [], "sku_rule": [], "category_rule": []}
    # 格式化apply_value为列表（如"乌拉圭仓,东南亚仓"→["乌拉圭仓","东南亚仓"]）
    df["apply_value"] = df["apply_value"].str.split(",")
    return {
        "warehouse_rule": df[df["rule_type"] == "WAREHOUSE"].to_dict("records"),
        "sku_rule": df[df["rule_type"] == "SKU"].to_dict("records"),
        "category_rule": df[df["rule_type"] == "CATEGORY"].to_dict("records")
    }

def load_all_config():
    """整合所有配置（对外提供统一入口）"""
    return {
        "base": load_base_config(),
        "special_rules": load_special_rules()
    }

def data_integration() -> pd.DataFrame:
    """数据整合：聚合库存、库龄、销量、成本（可单独执行）"""

    # 1. 查询各维度数据
    stock_sql = """SELECT sku, title, type, product_status, warehouse, warehouse_id,
                          available_stock, on_way_stock, available_stock_money
                   FROM dwd_warehouse_stock WHERE available_stock >= 0"""
    stock_age_sql = """SELECT sku, warehouse, overage_level, age_30_plus, age_60_plus, age_90_plus, age_180_plus
                       FROM dwm_stock_age WHERE date_id = (SELECT max(date_id) FROM dwm_stock_age)"""
    sales_sql = """SELECT sku, warehouse, 3days_sales, 7days_sales, 30days_sales, 90days_sales, day_sales, recent_day_sales
                   FROM dwm_sku_sales WHERE date_id = (SELECT max(date_id) FROM dwm_sku_sales)"""
    charge_sql = """SELECT sku, warehouse, charge_total_price_rmb FROM dwm_warehouse_charge"""

    # 2. 关联数据
    stock_df = query_sql(stock_sql)
    stock_age_df = query_sql(stock_age_sql)
    sales_df = query_sql(sales_sql)
    charge_df = query_sql(charge_sql)

    merged_df = pd.merge(stock_df, stock_age_df, on=["sku", "warehouse"], how="left")
    merged_df = pd.merge(merged_df, sales_df, on=["sku", "warehouse"], how="left")
    merged_df = pd.merge(merged_df, charge_df, on=["sku", "warehouse"], how="left")

    print(f"数据整合完成：共 {len(merged_df)} 条SKU+仓库数据")
    return merged_df

def data_clean(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """数据清洗：缺失值填充、异常值校正（可单独执行，传入df和config）"""
    cleaned_df = df.copy()
    stock_age_segments = config["base"]["stock_age_segments"]

    # 1. 缺失值填充
    sales_cols = ["3days_sales", "7days_sales", "day_sales", "recent_day_sales"]
    cleaned_df[sales_cols] = cleaned_df[sales_cols].fillna(0)
    cleaned_df["overage_level"] = cleaned_df["overage_level"].fillna(0).astype(int)
    cleaned_df["charge_total_price_rmb"] = cleaned_df["charge_total_price_rmb"].fillna(0)

    # 2. 库龄异常校正（分段库存≤可用库存）
    age_cols = [f"age_{seg}_plus" for seg in stock_age_segments if f"age_{seg}_plus" in cleaned_df.columns]
    for col in age_cols:
        mask = cleaned_df[col] > cleaned_df["available_stock"]
        cleaned_df.loc[mask, col] = cleaned_df.loc[mask, "available_stock"]

    # 3. 特殊仓库库龄标记（从配置读取规则）
    warehouse_rules = config["special_rules"]["warehouse_rule"]
    for rule in warehouse_rules:
        if rule["adjust_type"] == "OVERAGE_LEVEL":
            mask = cleaned_df["warehouse"].isin(rule["apply_value"]) & (cleaned_df["available_stock"] > 0)
            cleaned_df.loc[mask, "overage_level"] = rule["adjust_param"]

    print(f"数据清洗完成：有效数据 {len(cleaned_df)} 条")
    return cleaned_df

def calculate_indicators(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """衍生指标计算（可单独执行，传入df和config）"""
    indicator_df = df.copy()
    base_config = config["base"]

    # 1. 可售天数
    indicator_df["estimated_sales_days"] = np.where(
        indicator_df["day_sales"] > 0,
        indicator_df["available_stock"] / indicator_df["day_sales"],
        9999
    ).replace(np.inf, 9999)

    # 2. 含在途库存可售天数
    d1 = (pd.to_datetime(base_config["target_date"]) - datetime.now()).days
    indicator_df["all_esd"] = np.where(
        indicator_df["recent_day_sales"] > 0,
        (indicator_df["available_stock"] + indicator_df["on_way_stock"]) / indicator_df["recent_day_sales"] - d1,
        9999
    )

    # 3. 库存状态（积压/紧张）
    indicator_df["is_overstock"] = (indicator_df["estimated_sales_days"] > base_config["overstock_esd_threshold"])
    indicator_df["is_shortage"] = (indicator_df["estimated_sales_days"] < base_config["shortage_esd_threshold"])

    # 4. 仓租成本占比
    indicator_df["charge_ratio"] = np.where(
        (indicator_df["charge_total_price_rmb"] > 0) & (indicator_df["available_stock_money"] > 0),
        indicator_df["available_stock_money"] / (indicator_df["charge_total_price_rmb"] * 30),
        0
    )

    print("衍生指标计算完成")
    return indicator_df

def calculate_adjust_rate(df: pd.DataFrame, config: dict, current_date: str) -> pd.DataFrame:
    """计算调价幅度（可单独执行，传入df、config、日期）"""
    from db_utils import query_sql

    adjust_df = df.copy()
    base_config = config["base"]
    special_rules = config["special_rules"]

    # 1. 加载利润率规则
    profit_rules = query_sql("SELECT * FROM profit_rate_section")
    up_rules = query_sql("SELECT * FROM up_rate_section")

    # 2. 分箱匹配基础规则
    adjust_df["overage_esd_bins"] = pd.cut(adjust_df["overage_esd"], bins=[0,30,60,90,np.inf], labels=[1,2,3,4], right=False).fillna(1)
    adjust_df["day_sales_bins"] = pd.cut(adjust_df["day_sales"], bins=[0,1,5,10,np.inf], labels=[1,2,3,4], right=False).fillna(1)

    adjust_df = pd.merge(adjust_df, profit_rules, on=["overage_level", "overage_esd_bins", "day_sales_bins"], how="left")
    adjust_df = pd.merge(adjust_df, up_rules, on=["overage_level", "esd_bins"], how="left")
    adjust_df["section"] = adjust_df["section"].fillna(0)
    adjust_df["up_profit_rate"] = adjust_df["up_profit_rate"].fillna(0)
    adjust_df["lowest_profit"] = adjust_df["lowest_profit"].fillna(base_config["min_profit_rate"])

    # 3. 应用特殊规则（仓库/SKU）
    # 特殊仓库规则
    for rule in special_rules["warehouse_rule"]:
        if rule["adjust_type"] == "MIN_PROFIT":
            mask = adjust_df["warehouse"].isin(rule["apply_value"]) & (adjust_df["available_stock"] > 0)
            adjust_df.loc[mask, "lowest_profit"] = rule["adjust_param"]
    # 特殊SKU规则
    for rule in special_rules["sku_rule"]:
        mask = adjust_df["sku"].isin(rule["apply_value"]) & (adjust_df["available_stock"] > 0)
        if rule["adjust_type"] == "SECTION":
            adjust_df.loc[mask, "section"] = rule["adjust_param"]

    # 4. 关联历史利润率
    history_sql = f"""
        SELECT sku, warehouse, after_profit AS after_profit_yest
        FROM dwm_sku_temp_info WHERE date_id = (SELECT max(date_id) FROM dwm_sku_temp_info WHERE date_id < '{current_date}')
    """
    history_df = query_sql(history_sql)
    adjust_df = pd.merge(adjust_df, history_df, on=["sku", "warehouse"], how="left")
    adjust_df["begin_profit"] = adjust_df["after_profit_yest"].fillna(0)

    # 5. 计算目标利润率
    adjust_df["after_profit"] = np.where(
        adjust_df["up_profit_rate"] > 0,
        adjust_df["begin_profit"] + adjust_df["up_profit_rate"],
        adjust_df["begin_profit"] + adjust_df["section"]
    )

    # 6. 安全限制（最大降幅、保底利润率）
    adjust_df["after_profit"] = np.maximum(adjust_df["after_profit"], base_config["max_adjust_rate"])
    adjust_df["after_profit"] = np.maximum(adjust_df["after_profit"], adjust_df["lowest_profit"])
    adjust_df.loc[adjust_df["available_stock"] <= 0, "after_profit"] = 0  # 无库存不调价

    print("调价幅度计算完成")
    return adjust_df

def dwm_oversea_sku():
    """
    主函数：串联所有流程（Airflow直接调用此函数）
    执行逻辑：配置加载→数据整合→清洗→指标计算→规则匹配→写入+备份
    """
    start_time = datetime.now()
    print("="*50)
    print(f"开始执行dwm_oversea_sku，时间：{start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    try:
        # 1. 加载配置
        config = load_all_config()
        current_date = start_time.strftime("%Y-%m-%d")

        # 2. 数据整合（可单独执行：data_integration()）
        core_df = data_integration()

        # 3. 数据清洗（可单独执行：data_clean(core_df, config)）
        cleaned_df = data_clean(core_df, config)

        # 4. 指标计算（可单独执行：calculate_indicators(cleaned_df, config)）
        indicator_df = calculate_indicators(cleaned_df, config)

        # 5. 规则匹配（可单独执行：calculate_adjust_rate(indicator_df, config, current_date)）
        result_df = calculate_adjust_rate(indicator_df, config, current_date)

        # 6. 数据写入+备份+报表
        write_success, fail_skus = write_adjust_result(result_df)
        if write_success:
            backup_to_ck(result_df)
            generate_report(result_df)

        # 7. 输出结果
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds() / 60
        print("="*50)
        print(f"执行完成！总耗时：{total_time:.2f} 分钟")
        print(f"处理数据：{len(result_df)} 条，失败：{len(fail_skus)} 条")
        print("="*50)

        return result_df  # 返回结果，方便单独执行时查看

    except Exception as e:
        print(f"执行失败：{str(e)}")
        raise

# 单独执行入口（直接运行main.py即可，或在Jupyter中调用）
if __name__ == "__main__":
    # 单独执行整个流程
    result = dwm_oversea_sku()

    # 如需单独测试某个函数，解开注释即可
    # config = load_all_config()
    # core_df = data_integration()  # 单独测试数据整合
    # cleaned_df = data_clean(core_df, config)  # 单独测试清洗
    # result = calculate_adjust_rate(cleaned_df, config, "2025-11-27")  # 单独测试规则