import pymongo
import sys
import os

# إضافة المسار الرئيسي للتأكد من قدرة الملف على قراءة مجلد config
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config.settings import MONGO_URI, DB_NAME

def create_mongodb_indexes():
    try:
        print("⏳ جاري الاتصال بقاعدة البيانات لإنشاء الفهارس...")
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # تم تعديل أسماء المجموعات لتتطابق مع قاعدة البيانات
        valid_collection = db['orders_validated']
        quarantine_collection = db['orders_quarantine']

        # 1. فهرس فريد (Unique Index) لمجموعة البيانات الصحيحة
        valid_collection.create_index([("order_id", pymongo.ASCENDING)], unique=True)
        print("✅ تم إنشاء فهرس فريد لحقل 'order_id' في مجموعة orders_validated")

        # 2. فهرس عادي (Single Index) لمجموعة البيانات المعزولة
        # تم التعديل إلى 'record_status' ليتطابق مع ما تم حفظه في كود المعالجة
        quarantine_collection.create_index([("record_status", pymongo.ASCENDING)])
        print("✅ تم إنشاء فهرس لحقل 'record_status' في مجموعة orders_quarantine")

        # 3. فهرس مركب (Compound Index) - "مطلوب إجباري في المشروع"
        # استخدمنا الحقول الموجودة فعلياً في بياناتك
        valid_collection.create_index([
            ("order_date", pymongo.DESCENDING),
            ("status", pymongo.ASCENDING)
        ])
        print("✅ تم إنشاء فهرس مركب (Compound Index) لحقلي 'order_date' و 'status'")

        print("🎉 اكتمل إعداد الفهارس بنجاح ومطابقة لمتطلبات المشروع!")
        
    except Exception as e:
        print(f"❌ حدث خطأ أثناء إنشاء الفهارس: {e}")

if __name__ == "__main__":
    create_mongodb_indexes()