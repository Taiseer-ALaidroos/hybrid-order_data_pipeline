import pymongo
import sys
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from config.settings import MONGO_URI, DB_NAME

def run_aggregations():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        valid_collection = db['orders_validated']

        print("📊 بدء تشغيل تقارير التجميعات (النسخة السحرية)...\n")
        print("="*60)

        # ---------------------------------------------------------
        # التقرير 1: أعلى 5 مدن من حيث عدد الطلبات
        # ---------------------------------------------------------
        print("📈 التقرير 1: أعلى 5 مدن من حيث عدد الطلبات")
        pipeline_1 = [
            {"\x24group": {"_id": "\x24city", "total_orders": {"\x24sum": 1}}},
            {"\x24sort": {"total_orders": -1}},
            {"\x24limit": 5}
        ]
        for r in valid_collection.aggregate(pipeline_1):
            print(f"   🏙️ المدينة: {r['_id']} | 📦 إجمالي الطلبات: {r['total_orders']}")
        print("-" * 60)

        # ---------------------------------------------------------
        # التقرير 2: توزيع الطلبات حسب حالة الطلب (Status)
        # ---------------------------------------------------------
        print("📋 التقرير 2: توزيع الطلبات حسب الحالة")
        pipeline_2 = [
            {"\x24group": {"_id": "\x24status", "count": {"\x24sum": 1}}},
            {"\x24sort": {"count": -1}}
        ]
        for r in valid_collection.aggregate(pipeline_2):
            print(f"   📌 الحالة: {r['_id']} | 🔢 العدد: {r['count']}")
        print("-" * 60)

        # ---------------------------------------------------------
        # التقرير 3: طرق الدفع الأكثر استخداماً
        # ---------------------------------------------------------
        print("💳 التقرير 3: طرق الدفع الأكثر استخداماً")
        pipeline_3 = [
            {"\x24group": {"_id": "\x24payment_method", "usage_count": {"\x24sum": 1}}},
            {"\x24sort": {"usage_count": -1}}
        ]
        for r in valid_collection.aggregate(pipeline_3):
            print(f"   💵 الطريقة: {r['_id']} | 🔄 مرات الاستخدام: {r['usage_count']}")
        print("-" * 60)

        # ---------------------------------------------------------
        # التقرير 4: أكثر الأيام استقبالاً للطلبات
        # ---------------------------------------------------------
        print("📅 التقرير 4: أكثر الأيام استقبالاً للطلبات")
        pipeline_4 = [
            {"\x24project": {"day": {"\x24substr": ["\x24order_date", 0, 10]}}},
            {"\x24group": {"_id": "\x24day", "orders_count": {"\x24sum": 1}}},
            {"\x24sort": {"orders_count": -1}},
            {"\x24limit": 5}
        ]
        for r in valid_collection.aggregate(pipeline_4):
            print(f"   📆 التاريخ: {r['_id']} | 📦 الطلبات: {r['orders_count']}")
        print("-" * 60)


# ---------------------------------------------------------
        # التقرير 5: العملاء الأكثر نشاطاً وربحية
        # ---------------------------------------------------------
        print("⭐ التقرير 5: أفضل 5 عملاء (حسب إجمالي المبالغ المدفوعة وعدد الطلبات)")
        pipeline_5 = [
            {"$group": {
                "_id": "$customer_name", 
                "orders_count": {"$sum": 1},
                "total_spent": {"\(sum": "\)total_amount"} # جمع المبالغ المالية
            }},
            {"$sort": {"total_spent": -1}}, # الترتيب حسب أكثر من دفع مالاً
            {"$limit": 5}
        ]
        for r in valid_collection.aggregate(pipeline_5):
            print(f"   👤 العميل: {r['_id']} | 🛍️ الطلبات: {r['orders_count']} | 💰 إجمالي الدفع: {r['total_spent']:.2f}")
        print("="*60)
        
        print("🎉 اكتملت مرحلة التجميعات بنجاح!")

    except Exception as e:
        print(f"❌ حدث خطأ: {e}")

if __name__ == "__main__":
    run_aggregations()