import os

# تحديد المسار الرئيسي للمشروع
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# مسارات مجلدات البيانات والتقارير
DATA_DIR = os.path.join(BASE_DIR, 'data')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

# ---------------------------------------------------------
# التعديل هنا: تم توقيف المسارات الثابتة القديمة
# ---------------------------------------------------------
# SAMPLE_DATA_FILE = os.path.join(DATA_DIR, 'sample_orders.csv')
# HUGE_DATA_FILE = os.path.join(DATA_DIR, 'orders_huge_mixed_quality.csv')

# ---------------------------------------------------------
# الإضافة الجديدة: استقبال أي مسار ديناميكياً
# الكود الآن سيبحث عن المسار الذي أدخلته في التيرمينال (عبر متغيرات البيئة).
# ---------------------------------------------------------
DYNAMIC_INPUT_FILE = os.environ.get("PIPELINE_INPUT_FILE", "")


# مسار ملف التقارير (مطلوب لحفظ المقاييس)
RESULTS_FILE = os.path.join(REPORTS_DIR, 'results.json')

# التأكد من وجود مجلد التقارير
os.makedirs(REPORTS_DIR, exist_ok=True)

# 1. إعدادات الموجه (File Router) - (تم إصلاح التعليق هنا)
SMALL_FILE_THRESHOLD_MB = 200.0

# 2. إعدادات قاعدة البيانات MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "تيسير_العيدروس"

# المجموعات (Collections) كما طلبها الدكتور بالضبط
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# 3. إعدادات المعالجة
BATCH_SIZE = 5000  # حجم الدفعة لمعالجة Python Batch









# import os

# # تحديد المسار الرئيسي للمشروع
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# # مسارات مجلدات البيانات والتقارير
# DATA_DIR = os.path.join(BASE_DIR, 'data')
# REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

# SAMPLE_DATA_FILE = os.path.join(DATA_DIR, 'sample_orders.csv')
# HUGE_DATA_FILE = os.path.join(DATA_DIR, 'orders_huge_mixed_quality.csv')

# # مسار ملف التقارير (مطلوب لحفظ المقاييس)
# RESULTS_FILE = os.path.join(REPORTS_DIR, 'results.json')

# # التأكد من وجود مجلد التقارير
# os.makedirs(REPORTS_DIR, exist_ok=True)

# # 1. إعدادات الموجه (File Router)
# SMALL_FILE_THRESHOLD_MB = 200.0

# # 2. إعدادات قاعدة البيانات MongoDB
# MONGO_URI = "mongodb://localhost:27017/"
# DB_NAME = "sample_orders2"

# # المجموعات (Collections) كما طلبها الدكتور بالضبط
# RAW_COLLECTION = "orders_raw"
# VALIDATED_COLLECTION = "orders_validated"
# QUARANTINE_COLLECTION = "orders_quarantine"

# # 3. إعدادات المعالجة
# BATCH_SIZE = 5000  # حجم الدفعة لمعالجة Python Batch
