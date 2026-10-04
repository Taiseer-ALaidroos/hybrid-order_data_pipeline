import csv
import time
import json
import os
from datetime import datetime
from config import settings
from src.db_loader import get_mongo_client

import os
import sys

# إضافة المسار الرئيسي للمشروع لكي يتمكن من رؤية مجلد config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config import settings
# ... باقي الاستدعاءات والكود


def run_batch_processing(input_file, run_id="batch_run_raw_01"):

    print("\n" + "="*60)
    print(f"🚀 بدء معالجة الباتش (Python Batch) لطبقة RAW")
    print(f"📁 الملف: {input_file}")
    print(f"🔑 رقم التشغيل: {run_id}")
    print("="*60)
    
    client = get_mongo_client()
    db = client[settings.DB_NAME]
    raw_collection = db[settings.RAW_COLLECTION]
    
    start_time = time.time()
    
    # أسماء المقاييس مطابقة تماماً لوثيقة الدكتور (القسم 6.12)
    metrics = {
        "run_id": run_id,
        "file_name": os.path.basename(input_file),
        "engine_used": "python_batch",
        "rows_read": 0,
        "raw_loaded": 0,
        "elapsed_seconds": 0,
        "throughput": 0,
        "batch_size": settings.BATCH_SIZE
    }
    
    batch = []
    batch_number = 1
    
    try:
        # قراءة الملف بصورة Streaming باستخدام csv.DictReader (كما طلب الدكتور)
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_number, row in enumerate(reader, start=1):
                metrics["rows_read"] += 1
                
                # بناء السجل الخام وفق معمارية ELT
                raw_record = {
                    "run_id": run_id,
                    "source_file": os.path.basename(input_file),
                    "source_row_number": row_number,
                    "ingested_at": datetime.now().isoformat(),
                    "engine_used": "python_batch",
                    "raw_record": row
                }
                
                batch.append(raw_record)
                
                # عندما تصل الدفعة للحجم المحدد في الإعدادات، نقوم برفعها
                if len(batch) == settings.BATCH_SIZE:
                    try:
                        batch_start = time.time()
                        # استخدام insert_many بدلاً من Upsert/update_one
                        raw_collection.insert_many(batch)
                        batch_time = time.time() - batch_start
                        
                        # حساب معدل الإدخال للدفعة
                        throughput = len(batch) / batch_time if batch_time > 0 else len(batch)
                        metrics["raw_loaded"] += len(batch)
                        
                        # طباعة بيانات الدفعة كما اشترط الدكتور
                        print(f"📦 دفعة {batch_number}: تم إدخال {len(batch)} سجل | الزمن: {batch_time:.2f} ث | المعدل: {throughput:.2f} سجل/ث")
                    except Exception as e:
                        # معالجة خطأ الدفعة دون إخفاء السبب
                        print(f"❌ خطأ في إدخال الدفعة {batch_number}: {e}")
                    
                    # تفريغ الدفعة استعداداً للدفعة التالية
                    batch.clear()
                    batch_number += 1
            
            # إدخال أي سجلات متبقية في الدفعة الأخيرة (إن وجدت)
            if batch:
                try:
                    batch_start = time.time()
                    raw_collection.insert_many(batch)
                    batch_time = time.time() - batch_start
                    throughput = len(batch) / batch_time if batch_time > 0 else len(batch)
                    metrics["raw_loaded"] += len(batch)
                    print(f"📦 دفعة {batch_number} (أخيرة): تم إدخال {len(batch)} سجل | الزمن: {batch_time:.2f} ث | المعدل: {throughput:.2f} سجل/ث")
                except Exception as e:
                    print(f"❌ خطأ في إدخال الدفعة الأخيرة {batch_number}: {e}")
                    
        # حساب المقاييس النهائية
        elapsed = time.time() - start_time
        metrics["elapsed_seconds"] = round(elapsed, 2)
        metrics["throughput"] = round(metrics["raw_loaded"] / elapsed if elapsed > 0 else 0, 2)
        
        # حفظ التقرير في مجلد reports
    # حفظ التقرير في مجلد reports في ملف واحد (results.json)
        os.makedirs(settings.REPORTS_DIR, exist_ok=True)
        report_path = os.path.join(settings.REPORTS_DIR, "results.json")
        
        existing_data = []
        # قراءة البيانات القديمة إن وجدت
        if os.path.exists(report_path) and os.path.getsize(report_path) > 0:
            try:
                with open(report_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_data = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                existing_data = []

        # إضافة التقرير الجديد للمصفوفة
        existing_data.append(metrics)
        
        # حفظ الملف بالكامل
        with open(report_path, 'w', encoding='utf-8') as rep_file:
            json.dump(existing_data, rep_file, ensure_ascii=False, indent=4)
            
        print("="*60)
        print(f"✅ تم الانتهاء بنجاح تام!")
        print(f"📊 إجمالي السجلات المقروءة: {metrics['rows_read']}")
        print(f"📥 إجمالي السجلات المحملة لـ RAW: {metrics['raw_loaded']}")
        print(f"⏱️ الوقت المستغرق الكلي: {metrics['elapsed_seconds']} ثانية")
        print(f"🚀 معدل الإدخال الكلي: {metrics['throughput']} سجل/ثانية")
        print("="*60)
        
    except Exception as e:
        print(f"❌ حدث خطأ رئيسي أثناء قراءة الملف: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    # استقبال مسار الملف من المتغيرات أو من المسار الديناميكي الجديد
    input_file = os.environ.get("PIPELINE_INPUT_FILE", getattr(settings, 'DYNAMIC_INPUT_FILE', ''))
    
    if not input_file:
        print("❌ خطأ: لم يتم تمرير مسار الملف. الرجاء التشغيل عبر main.py")
        sys.exit(1)
    
    # التعديل: استقبال run_id من المشغل المركزي إذا تم تمريره
    if len(sys.argv) > 1:
        run_id = sys.argv[1]
    else:
        run_id = f"run_batch_{int(time.time())}"
        
    run_batch_processing(input_file, run_id)
