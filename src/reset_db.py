from pymongo import MongoClient

# الاتصال بقاعدة البيانات
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_store"]

# حذف المجموعات القديمة بالكامل
db.drop_collection("orders_raw")
db.drop_collection("orders_validated")
db.drop_collection("orders_quarantine")

print("🧹 تم تنظيف قاعدة البيانات القديمة بنجاح! الساحة الآن جاهزة.")