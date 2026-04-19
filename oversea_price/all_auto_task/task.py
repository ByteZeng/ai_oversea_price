from apscheduler.schedulers.blocking import  BlockingScheduler
from all_auto_task.yunfei_auto import run
from all_auto_task.status import run_status
from all_auto_task.over_sea_age import run_oversea_age
from oversea_amazon_listing_detail import amazon_listing
from oversea_walmart_listing_detail import walmart_listing
from all_auto_task.listing_success_lpl2 import run_success_lpl
from oversea_ebay_listing_detail import ebay_listing
from oversea_cd_listing_detail import cd_listing
from all_auto_task.listing_control3 import run_listing_contorl
from yunfei_ckeck import yunfei_data_ckeck
from all_auto_task.oversea_age_stock import run_oversea_stock
from all_auto_task.oversea_amazon_asin_fba import amazon_fba_listing
from all_auto_task.status_repair import repair_status
from all_auto_task.sales_calculation import sales_calculation_one
from all_auto_task.mexico_account_post import run_mexico_account
from datetime import datetime
from all_auto_task.fbc_listing_lpl import run_fbc_lpl
from all_auto_task.wide_table import run_wide_table



# 输出时间
# def job():

# BlockingScheduler
scheduler = BlockingScheduler()
#海外仓运费计算
# scheduler.add_job(run,'cron',day_of_week='mon',hour=7,minute=45)
scheduler.add_job(run,'cron',hour=5,minute=45)
# scheduler.add_job(run_es,'cron',day_of_week='1-6',hour=7,minute=55)
#fba,国内仓，海外仓销售状态更新
scheduler.add_job(run_status,'cron', hour=23,minute=5)
scheduler.add_job(run_status,'cron', hour=21,minute=5)
#海外仓库龄计算
# scheduler.add_job(run_oversea_age,'cron', hour=9,minute=15)
scheduler.add_job(run_oversea_age,'cron', hour=13,minute=25)
#海外仓amazon链接
# scheduler.add_job(amazon_listing,'cron', hour=9,minute=45)
#海外仓walmart链接
# scheduler.add_job(walmart_listing,'cron', hour=10,minute=45)
#海外仓ebay链接
# scheduler.add_job(ebay_listing,'cron', hour=11,minute=30)
#海外仓cd链接
# scheduler.add_job(cd_listing,'cron', hour=12,minute=22)
#调价生效率监控
scheduler.add_job(run_success_lpl,'cron', hour=13,minute=30)
#海外仓价格监控
scheduler.add_job(run_listing_contorl,'cron', hour=14,minute=15)
# scheduler.add_job(run_listing_contorl,'cron', hour=18,minute=15)
#海外仓运费总数校验
# scheduler.add_job(yunfei_data_ckeck,'cron', hour=9,minute=10)
#海外仓库存监控
scheduler.add_job(run_oversea_stock,'cron',day_of_week='mon,wed,fri',hour=13,minute=5)
#海外仓同asin fba数据抽取
scheduler.add_job(amazon_fba_listing,'cron',day_of_week='mon',hour=12,minute=50)
#销量自动计算
scheduler.add_job(sales_calculation_one,'cron', hour=8,minute=50)
# scheduler.add_job(sales_calculation,'cron', hour=13,minute=30)
# scheduler.add_job(sales_calculation,'cron', hour=18,minute=30)
#修复国内仓无在库无在途sku销售状态
scheduler.add_job(repair_status,'cron', hour=14,minute=15)
scheduler.add_job(repair_status,'cron', hour=23,minute=55)
#墨西哥税号推送
scheduler.add_job(run_mexico_account,'cron', hour=14,minute=15)
#销量数据监控
# scheduler.add_job(day_sales_data,'cron', hour=9,minute=20)
#fbc调价监控
scheduler.add_job(run_fbc_lpl,'cron', hour=13,minute=40)
#大宽表接口
scheduler.add_job(run_wide_table,'cron', hour=8,minute=30)


scheduler.start()