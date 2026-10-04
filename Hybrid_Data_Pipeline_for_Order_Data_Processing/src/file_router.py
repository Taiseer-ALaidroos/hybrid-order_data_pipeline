import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

def route_file(file_path):
    """
    يفحص حجم الملف ويقرر المحرك المناسب بناءً على الحد الفاصل في الإعدادات
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")
        
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    
    print("-" * 60)
    print(f"📁 حجم الملف: {file_size_mb:.2f} MB")
    print(f"⚖️ الحد الفاصل (Threshold): {settings.SMALL_FILE_THRESHOLD_MB} MB")
    
    if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
        print("🔀 القرار: استخدام محرك [Python Batch] لأن الملف صغير.")
        return 'python_batch', file_size_mb
    else:
        print("🔀 القرار: استخدام محرك [PySpark] لأن الملف ضخم.")
        return 'pyspark', file_size_mb