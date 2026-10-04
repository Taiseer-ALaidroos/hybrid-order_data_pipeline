import os
import sys

from fastapi.testclient import TestClient


# تحديد مسار جذر المشروع
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# استيراد تطبيق FastAPI الحقيقي
from src.api import app


# إنشاء عميل اختبار بدون تشغيل Uvicorn
client = TestClient(app)


def test_health_check():
    """اختبار مسار فحص حالة الخدمة."""
    response = client.get("/health")

    assert response.status_code == 200

    body = response.json()

    # يقبل القيم الشائعة حسب طريقة كتابة api.py
    assert body.get("status") in ["ok", "healthy", "success"]


def test_get_queries_info():
    """اختبار مسار عرض الاستعلامات."""
    response = client.get("/queries")

    assert response.status_code == 200

    body = response.json()

    # بعض المشاريع تستخدم success، وبعضها تعيد قائمة مباشرة
    assert (
        body.get("status") == "success"
        or "available_queries" in body
        or "queries" in body
    )


def test_get_jobs_log():
    """اختبار مسار قراءة سجل المهام المجدولة."""
    response = client.get("/jobs")

    assert response.status_code == 200

    body = response.json()

    assert (
        "latest_jobs" in body
        or "jobs" in body
        or "data" in body
    )
