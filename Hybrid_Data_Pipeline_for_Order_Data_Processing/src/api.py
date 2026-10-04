from fastapi import FastAPI, BackgroundTasks, HTTPException
import sys
import os
import pymongo
import subprocess

# إعداد المسارات للوصول لبقية الملفات
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

# استدعاء ملفات المشروع
import db_indexes
import db_materialized_views
import db_scheduled_jobs
from config.settings import MONGO_URI, DB_NAME

app = FastAPI(
    title="Big Data Project API", 
    description="واجهة موحدة لتشغيل واختبار وظائف المشروع النهائي مطابقة لمتطلبات الدكتور 100%",
    version="1.0.0"
)

# الاتصال بقاعدة البيانات
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
valid_collection = db['orders_validated']
quarantine_collection = db['orders_quarantine']

# ---------------------------------------------------------
# 1. الأساسيات
# ---------------------------------------------------------
@app.get("/health", tags=["الأساسيات"])
def health_check():
    return {"status": "ok", "message": "API is running successfully!"}

@app.post("/ingest", tags=["الأساسيات"])
def ingest_data(background_tasks: BackgroundTasks):
    script_path = os.path.join(CURRENT_DIR, "main.py")
    background_tasks.add_task(subprocess.run, [sys.executable, script_path])
    return {"status": "success", "message": "تم بدء تشغيل خط البيانات (Pipeline) في الخلفية."}

@app.post("/indexes", tags=["الأساسيات"])
def create_indexes():
    db_indexes.create_mongodb_indexes()
    return {"status": "success", "message": "تم إنشاء الفهارس (Unique, Single, Compound) بنجاح."}

# ---------------------------------------------------------
# 2. الاستعلامات (Queries) - 5 استعلامات عملية
# ---------------------------------------------------------
@app.get("/queries", tags=["الاستعلامات (Queries)"])
def get_available_queries():
    return {
        "status": "success",
        "available_queries": [
            "q1_search_by_id", 
            "q2_date_and_status", 
            "q3_quarantine_errors", 
            "q4_wallet_payments", 
            "q5_city_search"
        ]
    }

@app.get("/queries/{name}", tags=["الاستعلامات (Queries)"])
def run_specific_query(name: str):
    if name == "q1_search_by_id":
        query = {"order_id": "100000-طلب"}
        # جلب الـ explain لبيان أثر الفهرس الفريد
        stats = db.command("explain", {"find": "orders_validated", "filter": query})["executionStats"]
        return {"query_name": name, "execution_time_ms": stats["executionTimeMillis"], "docs_examined": stats["totalDocsExamined"], "details": "يستخدم الفهرس الفريد"}
    
    elif name == "q2_date_and_status":
        query = {"order_date": {"$regex": "^2025-02-24"}, "status": "returned"}
        stats = db.command("explain", {"find": "orders_validated", "filter": query})["executionStats"]
        return {"query_name": name, "execution_time_ms": stats["executionTimeMillis"], "docs_examined": stats["totalDocsExamined"], "details": "يستخدم الفهرس المركب"}
    
    elif name == "q3_quarantine_errors":
        query = {"record_status": {"$regex": "Quarantined"}}
        stats = db.command("explain", {"find": "orders_quarantine", "filter": query})["executionStats"]
        return {"query_name": name, "execution_time_ms": stats["executionTimeMillis"], "docs_examined": stats["totalDocsExamined"], "details": "يستخدم الفهرس العادي"}
    
    elif name == "q4_wallet_payments":
        results = list(valid_collection.find({"payment_method": "wallet"}, {"_id": 0}).limit(3))
        return {"query_name": name, "results": results}
    
    elif name == "q5_city_search":
        results = list(valid_collection.find({"city": "تعز"}, {"_id": 0}).limit(3))
        return {"query_name": name, "results": results}
    
    raise HTTPException(status_code=404, detail="الاستعلام غير موجود. الرجاء اختيار اسم من قائمة /queries")

# ---------------------------------------------------------
# 3. التجميعات (Aggregations) - 5 تقارير
# ---------------------------------------------------------
@app.get("/aggregations", tags=["التجميعات (Aggregations)"])
def get_available_aggregations():
    return {
        "status": "success",
        "available_aggregations": ["top_cities", "order_status", "payment_methods", "busy_days", "top_customers"]
    }

@app.get("/aggregations/{name}", tags=["التجميعات (Aggregations)"])
def run_aggregation_report(name: str):
    pipeline = []
    if name == "top_cities":
        pipeline = [{"\(group": {"_id": "\)city", "total_orders": {"\(sum": 1}}}, {"\)sort": {"total_orders": -1}}, {"$limit": 5}]
    elif name == "order_status":
        pipeline = [{"\(group": {"_id": "\)status", "count": {"\(sum": 1}}}, {"\)sort": {"count": -1}}]
    elif name == "payment_methods":
        pipeline = [{"\(group": {"_id": "\)payment_method", "usage": {"\(sum": 1}}}, {"\)sort": {"usage": -1}}]
    elif name == "busy_days":
        pipeline = [{"\(project": {"day": {"\)substr": ["\(order_date", 0, 10]}}}, {"\)group": {"_id": "\(day", "orders": {"\)sum": 1}}}, {"\(sort": {"orders": -1}}, {"\)limit": 5}]
    elif name == "top_customers":
        pipeline = [{"\(group": {"_id": "\)customer_name", "orders": {"\(sum": 1}}}, {"\)sort": {"orders": -1}}, {"$limit": 5}]
    else:
        raise HTTPException(status_code=404, detail="التقرير غير موجود.")
    
    results = list(valid_collection.aggregate(pipeline))
    return {"report_name": name, "data": results}

# ---------------------------------------------------------
# 4. العروض المادية (Materialized Views)
# ---------------------------------------------------------
@app.post("/refresh-mv", tags=["العروض المادية"])
def refresh_materialized_views():
    db_materialized_views.create_materialized_views()
    
    # جلب عينة للتأكيد على التحديث التزايدي
    sample_daily = list(db['daily_sales_mv'].find({}, {"_id": 1, "total_orders": 1}).sort("total_orders", -1).limit(2))
    sample_city = list(db['city_sales_mv'].find({}, {"_id": 1, "total_orders": 1}).sort("total_orders", -1).limit(2))
    
    return {
        "status": "success", 
        "message": "تم تحديث العروض المادية (آلية التحديث التزايدي) بنجاح.",
        "daily_sales_sample": sample_daily,
        "city_sales_sample": sample_city
    }

# ---------------------------------------------------------
# 5. المهام المجدولة (Scheduled Jobs)
# ---------------------------------------------------------
@app.get("/jobs", tags=["المهام المجدولة (Jobs)"])
def get_jobs_log():
    # استرجاع آخر 10 مهام مع وقت البداية والنهاية والحالة
    logs = list(db['jobs_log'].find({}, {"_id": 0}).sort("start_time", -1).limit(10))
    return {"status": "success", "total_logs": len(logs), "latest_jobs": logs}

@app.post("/jobs/{name}/run", tags=["المهام المجدولة (Jobs)"])
def run_specific_job(name: str):
    if name == "update_views":
        db_scheduled_jobs.job_update_views()
        return {"status": "success", "message": "تم تشغيل مهمة تحديث العروض المادية يدوياً وتوثيقها في jobs_log."}
    elif name == "generate_report":
        db_scheduled_jobs.job_generate_report()
        return {"status": "success", "message": "تم تشغيل مهمة التقرير اليومي يدوياً وتوثيقها في jobs_log."}
    else:
        raise HTTPException(status_code=404, detail="المهمة غير معروفة. جرب update_views أو generate_report")