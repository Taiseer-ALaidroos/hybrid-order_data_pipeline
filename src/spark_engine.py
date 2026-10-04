import os
import sys
import json
import time
from datetime import datetime
from config import settings
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, current_timestamp, monotonically_increasing_id, struct, col

# ⬅️ توجيه Hadoop مباشرة إلى مساره الحقيقي على جهازك
os.environ['HADOOP_USER_NAME'] = 'root'
os.environ['HADOOP_HOME'] = r"C:\hadoop"


def run_spark_processing(input_file, run_id="run_spark_huge_001"):
    print("\n" + "="*60)
    print(f"🚀 بدء معالجة الملف الضخم عبر [Apache Spark] لطبقة RAW")
    print(f"📁 الملف: {input_file}")
    print(f"🔑 رقم التشغيل: {run_id}")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("MidtermDataPipelineSpark") \
        .config("spark.mongodb.input.uri", settings.MONGO_URI) \
        .config("spark.mongodb.output.uri", settings.MONGO_URI) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
    
    start_time = time.time()
    metrics = {
        "run_id": run_id,
        "file_name": os.path.basename(input_file),
        "engine_used": "pyspark",
        "rows_read": 0,
        "raw_loaded": 0,
        "elapsed_seconds": 0,
        "throughput": 0,
        "input_partitions": 0
    }
    
    try:
        # 1. تعريف Schema ثابتة (Fixed Schema) بجميع الحقول كـ String بناءً على ملفك
        field_names = [
            "order_id", "order_date", "status", "customer_id", 
            "customer_name", "customer_phone", "customer_email", 
            "city", "district", "delivery_type", "delivery_cost", 
            "payment_method", "payment_status", "payment_amount", 
            "currency", "total_amount", "items_json"
        ]
        
        schema = StructType([StructField(field_name, StringType(), True) for field_name in field_names])
        
        # 2. قراءة الملف باستخدام الـ Schema الثابتة
        df = spark.read.option("header", "true").schema(schema).csv(input_file)
        
        # تسجيل عدد الـ Partitions
        metrics["input_partitions"] = df.rdd.getNumPartitions()
        
        total_rows = df.count()
        metrics["rows_read"] = total_rows
        metrics["raw_loaded"] = total_rows
        
        # 3. بناء الهيكلية المطلوبة لطبقة RAW وإضافة الميتا داتا
        # نجمع كل أعمدة البيانات داخل حقل raw_record
        df_processed = df.withColumn("raw_record", struct([col(c) for c in df.columns]))
        
        df_final = df_processed.withColumn("run_id", lit(run_id)) \
                                   .withColumn("source_file", lit(os.path.basename(input_file))) \
                                   .withColumn("ingested_at", current_timestamp()) \
                                   .withColumn("engine_used", lit("pyspark")) \
                                   .withColumn("source_row_number", monotonically_increasing_id()) \
                                   .select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record")
        
        # 4. الكتابة إلى MongoDB (Append فقط دون Upsert لطبقة RAW)
        df_final.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", settings.MONGO_URI) \
            .option("database", settings.DB_NAME) \
            .option("collection", settings.RAW_COLLECTION) \
            .save()
            
        elapsed = time.time() - start_time
        metrics["elapsed_seconds"] = round(elapsed, 2)
        metrics["throughput"] = round(total_rows / elapsed if elapsed > 0 else 0, 2)
        
        # 5. حفظ التقرير في ملف واحد (results.json)
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
            
        print(f"✅ تم الانتهاء بنجاح وحفظ التقرير في reports/results.json!")
        
        print(f"📊 إجمالي السجلات المحملة: {metrics['raw_loaded']}")
        print(f"🧩 عدد التقسيمات (Partitions): {metrics['input_partitions']}")
        print(f"⏱️ الوقت المستغرق: {metrics['elapsed_seconds']} ثانية")
        print(f"🚀 معدل الإدخال الكلي: {metrics['throughput']} سجل/ثانية")
        print("="*60)
        
    except Exception as e:
        print(f"❌ خطأ: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    input_file = os.environ.get("PIPELINE_INPUT_FILE", getattr(settings, 'DYNAMIC_INPUT_FILE', ''))
    
    if not input_file:
        print("❌ خطأ: لم يتم تمرير مسار الملف. الرجاء التشغيل عبر main.py")
        sys.exit(1)
        
    run_spark_processing(input_file, f"run_spark_huge_{int(time.time())}")


