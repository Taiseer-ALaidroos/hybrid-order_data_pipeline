import pymongo
import sys
import os
import json

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config.settings import MONGO_URI, DB_NAME

def run_queries_and_explain():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        valid_collection = db['orders_validated']
        quarantine_collection = db['orders_quarantine']
        
        print("🚀 بدء تنفيذ الاستعلامات وتحليل الأداء (Explain)...\n")
        print("="*60)

        # -------------------------------------------------------------------
        # 1. الاستعلام الأول (مع التحليل): البحث عن طلب معين بواسطة order_id
        # -------------------------------------------------------------------
        print("📌 الاستعلام 1: البحث عن طلب محدد (يستخدم الفهرس الفريد Unique Index)")
        query_1 = {"order_id": "100000-طلب"}
        
        # قبل الفهرس (إجبار قاعدة البيانات على تجاهل الفهرس)
        explain_before_1 = db.command("explain", {"find": "orders_validated", "filter": query_1, "hint": {"$natural": 1}})["executionStats"]
        print(f"   ⏱️ قبل الفهرس (بطيء) -> وقت التنفيذ: {explain_before_1['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_before_1['totalDocsExamined']}")
        
        # بعد الفهرس (الوضع الطبيعي السريع)
        explain_after_1 = db.command("explain", {"find": "orders_validated", "filter": query_1})["executionStats"]
        print(f"   ⚡ بعد الفهرس (سريع) -> وقت التنفيذ: {explain_after_1['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_after_1['totalDocsExamined']}")
        print("   📝 السبب: الفهرس الفريد يمنع المسح الشامل (Full Scan) ويصل للوثيقة المطلوبة مباشرة بـ O(1).")
        print("-" * 60)

        # -------------------------------------------------------------------
        # 2. الاستعلام الثاني (مع التحليل): البحث عن سجلات بناءً على التاريخ والحالة
        # -------------------------------------------------------------------
        print("📌 الاستعلام 2: البحث عن الطلبات (المرتجعة) في تاريخ معين (يستخدم الفهرس المركب Compound Index)")
        # تم تعديل الحالة إلى 'returned' لتطابق ما تم تنظيفه في quality_rules
        query_2 = {"order_date": {"$regex": "^2025-02-24"}, "status": "returned"}
        
        explain_before_2 = db.command("explain", {"find": "orders_validated", "filter": query_2, "hint": {"$natural": 1}})["executionStats"]
        print(f"   ⏱️ قبل الفهرس (بطيء) -> وقت التنفيذ: {explain_before_2['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_before_2['totalDocsExamined']}")
        
        explain_after_2 = db.command("explain", {"find": "orders_validated", "filter": query_2})["executionStats"]
        print(f"   ⚡ بعد الفهرس (سريع) -> وقت التنفيذ: {explain_after_2['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_after_2['totalDocsExamined']}")
        print("   📝 السبب: الفهرس المركب ممتاز للاستعلامات التي تجمع بين أكثر من شرط معاً (AND logic)، مما يقلل نطاق البحث بشكل هائل.")
        print("-" * 60)

        # -------------------------------------------------------------------
        # 3. الاستعلام الثالث (مع التحليل): البحث في سجلات العزل
        # -------------------------------------------------------------------
        print("📌 الاستعلام 3: البحث عن نوع خطأ معين في البيانات المعزولة (يستخدم الفهرس العادي Single Index)")
        # تم تعديل الحقل إلى 'record_status' ليتطابق مع قاعدة البيانات
        query_3 = {"record_status": {"$regex": "Quarantined"}} 
        
        explain_before_3 = db.command("explain", {"find": "orders_quarantine", "filter": query_3, "hint": {"$natural": 1}})["executionStats"]
        print(f"   ⏱️ قبل الفهرس (بطيء) -> وقت التنفيذ: {explain_before_3['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_before_3['totalDocsExamined']}")
        
        explain_after_3 = db.command("explain", {"find": "orders_quarantine", "filter": query_3})["executionStats"]
        print(f"   ⚡ بعد الفهرس (سريع) -> وقت التنفيذ: {explain_after_3['executionTimeMillis']} مللي ثانية | الوثائق التي تم فحصها: {explain_after_3['totalDocsExamined']}")
        print("   📝 السبب: الفهرس يفيد في سرعة استخراج السجلات التي تحتوي على نفس نوع الخطأ بدلاً من فحص كل سجلات العزل.")
        print("-" * 60)

        # -------------------------------------------------------------------
        # 4. الاستعلام الرابع (استعلام عملي إضافي بدون تحليل)
        # -------------------------------------------------------------------
        print("📌 الاستعلام 4: جلب 3 طلبات تم دفعها عبر (محفظة إلكترونية)")
        # تم التعديل إلى 'wallet' لتطابق ما تم تنظيفه في quality_rules
        query_4 = {"payment_method": "wallet"}
        results_4 = list(valid_collection.find(query_4).limit(3))
        print(f"   ✅ تم العثور على {len(results_4)} طلبات كمثال. (منها الطلب: {results_4[0]['order_id'] if results_4 else 'لا يوجد'})")
        print("-" * 60)

        # -------------------------------------------------------------------
        # 5. الاستعلام الخامس (استعلام عملي إضافي بدون تحليل)
        # -------------------------------------------------------------------
        print("📌 الاستعلام 5: جلب 3 طلبات لمدينة معينة (مثال: تعز)")
        query_5 = {"city": "تعز"}
        results_5 = list(valid_collection.find(query_5).limit(3))
        print(f"   ✅ تم العثور على {len(results_5)} طلبات كمثال. (منها الطلب: {results_5[0]['order_id'] if results_5 else 'لا يوجد'})")
        print("="*60)
        
        print("🎉 اكتملت مرحلة الاستعلامات والفهارس بنجاح تام!")

    except Exception as e:
        print(f"❌ حدث خطأ: {e}")

if __name__ == "__main__":
    run_queries_and_explain()