import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import settings
from pymongo import MongoClient

def get_mongo_client():
    """إنشاء وإرجاع اتصال مع قاعدة بيانات MongoDB باستخدام الرابط من ملف الإعدادات"""
    client = MongoClient(settings.MONGO_URI)
    return client

def get_database():
    """الحصول على كائن قاعدة البيانات مباشرة بناءً على الإعدادات الصحيحة"""
    client = get_mongo_client()
    return client[settings.DB_NAME]




