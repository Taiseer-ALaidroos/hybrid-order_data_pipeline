import pymongo
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config.settings import MONGO_URI, DB_NAME

def create_materialized_views():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        valid_collection = db['orders_validated']

        print("🏗️ بدء إنشاء العروض المادية (Materialized Views)...\n")
        print("="*60)

        # ---------------------------------------------------------
        # العرض الأول: ملخص المبيعات اليومية
        # ---------------------------------------------------------
        print("📅 1. جاري إنشاء/تحديث العرض المادي الأول: ملخص المبيعات اليومية (daily_sales_mv)")
        pipeline_1 = [
            {"\x24project": {"day": {"\x24substr": ["\x24order_date", 0, 10]}}},
            {"\x24group": {"_id": "\x24day", "total_orders": {"\x24sum": 1}}},
            # دالة merge هي التي تقوم بالتحديث التزايدي (إضافة الجديد وتحديث القديم)
            {"\x24merge": {
                "into": "daily_sales_mv",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }}
        ]
        valid_collection.aggregate(pipeline_1)
        print("   ✅ تم إنشاء/تحديث daily_sales_mv بنجاح!")

        # ---------------------------------------------------------
        # العرض الثاني: ملخص الطلبات حسب المدينة
        # ---------------------------------------------------------
        print("🏙️ 2. جاري إنشاء/تحديث العرض المادي الثاني: ملخص الطلبات حسب المدينة (city_sales_mv)")
        pipeline_2 = [
            {"\x24group": {"_id": "\x24city", "total_orders": {"\x24sum": 1}}},
            {"\x24merge": {
                "into": "city_sales_mv",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }}
        ]
        valid_collection.aggregate(pipeline_2)
        print("   ✅ تم إنشاء/تحديث city_sales_mv بنجاح!")

        print("-" * 60)
        print("🔍 جاري قراءة البيانات من العروض المادية مباشرة للتحقق من نجاحها:")

        # قراءة سريعة من الجداول الجديدة لإثبات أن البيانات تم حفظها فعلاً
        print("\n   -> أعلى 3 أيام من العرض (daily_sales_mv):")
        for r in db['daily_sales_mv'].find().sort("total_orders", -1).limit(3):
            print(f"      📆 التاريخ: {r['_id']} | 📦 الطلبات: {r['total_orders']}")

        print("\n   -> أعلى 3 مدن من العرض (city_sales_mv):")
        for r in db['city_sales_mv'].find().sort("total_orders", -1).limit(3):
            print(f"      🏙️ المدينة: {r['_id']} | 📦 الطلبات: {r['total_orders']}")

        print("="*60)
        print("🎉 اكتملت مرحلة العروض المادية بنجاح وتتوافق مع آلية التحديث التزايدي!")

    except Exception as e:
        print(f"❌ حدث خطأ: {e}")

if __name__ == "__main__":
    create_materialized_views()