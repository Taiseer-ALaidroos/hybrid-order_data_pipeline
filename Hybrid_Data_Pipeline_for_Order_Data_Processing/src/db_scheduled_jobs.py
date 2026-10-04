import pymongo
import sys
import os
import time
import json
from datetime import datetime
import schedule # تأكد من تثبيتها عبر: pip install schedule

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config.settings import MONGO_URI, DB_NAME

# إعداد الاتصال بقاعدة البيانات
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
valid_collection = db['orders_validated']
log_collection = db['jobs_log'] # جدول جديد لتسجيل حركات المهام

# ---------------------------------------------------------
# دالة التسجيل (Logging) لتسجيل نجاح/فشل المهام وأوقاتها
# ---------------------------------------------------------
def log_job_execution(job_name, status, start_time, end_time, details=""):
    log_entry = {
        "job_name": job_name,
        "status": status, # "Success" or "Failed"
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": round((end_time - start_time).total_seconds(), 2),
        "details": details
    }
    log_collection.insert_one(log_entry)
    print(f"\n📝 تم تسجيل حالة المهمة ({status}) في جدول jobs_log.")

# ---------------------------------------------------------
# المهمة الأولى: تحديث العروض المادية (Materialized Views)
# ---------------------------------------------------------
def job_update_views():
    job_name = "Update Materialized Views"
    print(f"\n⚙️ [بدء المهمة] {job_name} ...")
    start_t = datetime.now()
    
    try:
        # تحديث العرض الأول
        pipeline_1 = [
            {"\x24project": {"day": {"\x24substr": ["\x24order_date", 0, 10]}}},
            {"\x24group": {"_id": "\x24day", "total_orders": {"\x24sum": 1}}},
            {"\x24merge": {"into": "daily_sales_mv", "whenMatched": "replace", "whenNotMatched": "insert"}}
        ]
        valid_collection.aggregate(pipeline_1)
        
        end_t = datetime.now()
        log_job_execution(job_name, "Success", start_t, end_t, "تم تحديث daily_sales_mv بنجاح.")
        print(f"✅ [نجاح] اكتملت المهمة في {round((end_t - start_t).total_seconds(), 2)} ثانية.")
        
    except Exception as e:
        end_t = datetime.now()
        log_job_execution(job_name, "Failed", start_t, end_t, str(e))
        print(f"❌ [فشل] حدث خطأ: {e}")

# ---------------------------------------------------------
# المهمة الثانية: إنشاء تقرير دوري (حفظ JSON) لأداء اليوم
# ---------------------------------------------------------
def job_generate_report():
    job_name = "Generate Daily JSON Report"
    print(f"\n⚙️ [بدء المهمة] {job_name} ...")
    start_t = datetime.now()
    
    try:
        # إنشاء مجلد للتقارير إذا لم يكن موجوداً
        reports_dir = os.path.join(PARENT_DIR, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        # استخراج أفضل 3 طرق دفع وتصديرها لملف
        pipeline = [
            {"\x24group": {"_id": "\x24payment_method", "usage_count": {"\x24sum": 1}}},
            {"\x24sort": {"usage_count": -1}},
            {"\x24limit": 3}
        ]
        results = list(valid_collection.aggregate(pipeline))
        
        # حفظ الملف
        report_path = os.path.join(reports_dir, "latest_payment_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
            
        end_t = datetime.now()
        log_job_execution(job_name, "Success", start_t, end_t, f"تم إنشاء التقرير في {report_path}")
        print(f"✅ [نجاح] تم حفظ التقرير في مجلد reports في {round((end_t - start_t).total_seconds(), 2)} ثانية.")
        
    except Exception as e:
        end_t = datetime.now()
        log_job_execution(job_name, "Failed", start_t, end_t, str(e))
        print(f"❌ [فشل] حدث خطأ: {e}")

# ---------------------------------------------------------
# نظام الجدولة (يعمل تلقائياً)
# ---------------------------------------------------------
def start_scheduler():
    print("\n⏰ تم تشغيل المجدول التلقائي (المهام تعمل في الخلفية). اضغط Ctrl+C للإيقاف.")
    # إعداد جدول افتراضي (كل 10 ثواني للتجربة، في الواقع يمكنك جعلها كل يوم)
    schedule.every(10).seconds.do(job_update_views)
    schedule.every(15).seconds.do(job_generate_report)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# ---------------------------------------------------------
# القائمة التفاعلية للمناقشة والاختبار
# ---------------------------------------------------------
def main_menu():
    while True:
        print("\n" + "="*50)
        print("🤖 نظام إدارة المهام المجدولة (Scheduled Jobs)")
        print("="*50)
        print("1. تشغيل مهمة [تحديث العروض المادية] يدوياً 🔄")
        print("2. تشغيل مهمة [إنشاء تقرير JSON] يدوياً 📊")
        print("3. تشغيل [المجدول التلقائي] ⏰")
        print("4. خروج ❌")
        
        choice = input("\n👉 اختر رقم العملية (1-4): ")
        
        if choice == '1':
            job_update_views()
        elif choice == '2':
            job_generate_report()
        elif choice == '3':
            start_scheduler()
        elif choice == '4':
            print("👋 وداعاً!")
            break
        else:
            print("⚠️ اختيار غير صحيح، يرجى المحاولة مجدداً.")

if __name__ == "__main__":
    main_menu()