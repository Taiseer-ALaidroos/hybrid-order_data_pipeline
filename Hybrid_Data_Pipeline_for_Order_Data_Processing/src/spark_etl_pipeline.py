import time
import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_json, from_json, get_json_object, expr, md5, concat_ws, coalesce, trim, udf, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)

# =========================================================================
# 🛑 إعدادات بيئة ويندوز الحيوية (لمنع خطأ انقطاع اتصال Python Worker)
# =========================================================================
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from config.settings import MONGO_URI, DB_NAME, RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION, REPORTS_DIR

def save_metrics_to_json(report_data, output_path=None):
    if output_path is None:
        output_path = os.path.join(REPORTS_DIR, "results.json")
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    existing_data = []
    
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                existing_data = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            existing_data = []

    existing_data.append(report_data)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)

# =====================================================================
# 1. تعريف المخططات (Schemas)
# =====================================================================
item_schema = StructType([
    StructField("sku", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("subtotal", DoubleType(), True)
])

correction_schema = StructType([
    StructField("field", StringType(), True),
    StructField("original_value", StringType(), True),
    StructField("corrected_value", StringType(), True),
    StructField("rule_code", StringType(), True)
])

final_record_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("quality_status", StringType(), True),
    StructField("corrections", ArrayType(correction_schema), True)
])

# =====================================================================
# 2. إنشاء الجسر (UDF) للاتصال بملف quality_rules.py
# =====================================================================
@udf(StringType())
def apply_nader_engine_udf(raw_record_json):
    if not raw_record_json: return "{}"
    try:
        # استدعاء كود نادر من ملف quality_rules.py
        from quality_rules import clean_order
    except:
        from src.quality_rules import clean_order
        
    try:
        raw_dict = json.loads(raw_record_json)
        cleaned_dict, corrections, quarantine_reasons = clean_order(raw_dict)
        
        # تنسيق المصفوفة لمنع مشاكل التحويل
        safe_corrections = [{"field": str(c.get("field","")), "original_value": str(c.get("original_value","")), "corrected_value": str(c.get("corrected_value","")), "rule_code": str(c.get("rule_code",""))} for c in corrections]
        cleaned_dict["corrections"] = safe_corrections
        
        # تصنيف السجل ليقرأه السبارك
        if len(quarantine_reasons) > 0:
            cleaned_dict["spark_record_status"] = f"Quarantined: {quarantine_reasons[0]}"
        elif len(corrections) > 0:
            cleaned_dict["spark_record_status"] = "Corrected"
        else:
            cleaned_dict["spark_record_status"] = "Valid"
            
        return json.dumps(cleaned_dict, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"spark_record_status": f"Quarantined: ERROR_{str(e)}"}, ensure_ascii=False)

def apply_quality_rules_and_classify(df_raw):
    df_json = df_raw.withColumn("raw_record_json", to_json(col("raw_record")))
    df_processed_str = df_json.withColumn("nader_json_str", apply_nader_engine_udf(col("raw_record_json")))
    df_with_status = df_processed_str.withColumn("record_status", get_json_object(col("nader_json_str"), "$.spark_record_status"))
    df_final = df_with_status.withColumn("cleaned_data", from_json(col("nader_json_str"), final_record_schema))
    
    df_classified = df_final.withColumn("order_id", col("cleaned_data.order_id")) \
                            .withColumn("customer_id", col("cleaned_data.customer_id")) \
                            .withColumn("order_date", col("cleaned_data.order_date")) \
                            .drop("raw_record_json", "nader_json_str")
    return df_classified

# =====================================================================
# 3. خط الأنابيب الرئيسي
# =====================================================================
def run_elt_pipeline(spark, run_id):
    print(f"\n--- Starting Advanced ELT Pipeline (Run ID: {run_id}) ---")
    start_time = time.time()

    raw_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{RAW_COLLECTION}"
    df_raw = spark.read.format("mongo").option("uri", raw_uri).load()
    
    # فلترة البيانات لمنع التراكم
    if "run_id" in df_raw.columns:
        df_raw = df_raw.filter(col("run_id") == run_id)
        
    rows_read = df_raw.count()

    if rows_read == 0:
        print("⚠️ No records found in Raw Collection.")
        return

    try:
        file_name = df_raw.select("source_file").first()[0] if "source_file" in df_raw.columns else "unknown_file.csv"
    except:
        file_name = "unknown_file.csv"

    # تطبيق التنظيف عبر الجسر
    df_processed = apply_quality_rules_and_classify(df_raw).cache()

    df_quarantine = df_processed.filter(col("record_status").startswith("Quarantined"))
    df_valid = df_processed.filter(col("record_status").isin("Valid", "Corrected"))

    valid_count = df_processed.filter(col("record_status") == "Valid").count()
    corrected_count = df_processed.filter(col("record_status") == "Corrected").count()
    quarantine_count = df_quarantine.count()

    error_counts_df = df_quarantine.groupBy("record_status").count().collect()
    error_case_counts = {row["record_status"]: row["count"] for row in error_counts_df}
    
    # 1. الرفع إلى الحجر الصحي
    quarantine_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{QUARANTINE_COLLECTION}"
    print(f"⚠️ Saving quarantined records to '{QUARANTINE_COLLECTION}'...")
    if quarantine_count > 0:
        df_quarantine_upsert = df_quarantine.select(
            col("run_id"), col("source_file"), col("source_row_number"), col("ingested_at"), col("engine_used"),
            col("record_status"), col("raw_record"), col("cleaned_data.*")
        ).withColumn(
            "_id", 
            when(col("order_id").isNotNull() & (trim(col("order_id")) != ""), col("order_id"))
            .otherwise(expr("concat('q_', md5(concat_ws('_', coalesce(customer_id, 'unknown'), coalesce(order_date, 'unknown'))))"))
        )
        
        df_quarantine_upsert.write.format("mongo") \
            .option("uri", quarantine_uri) \
            .option("replaceDocument", "true") \
            .mode("append") \
            .save()

    # 2. الدمج والرفع النهائي بـ Idempotent Upsert مع حساب العدادات
    valid_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}.{VALIDATED_COLLECTION}"
    print(f"🔄 Executing Idempotent Upsert to '{VALIDATED_COLLECTION}'...")
    
    df_valid_flat = df_valid.select(
        col("run_id"), col("source_file"), col("source_row_number"), col("ingested_at"), col("engine_used"),
        col("record_status"), col("cleaned_data.*")
    )
    
    # إنشاء البصمة لاكتشاف التغيرات وتحديد السجلات غير المعدلة بدقة
    df_valid_upsert = df_valid_flat.withColumn("_id", col("order_id")) \
        .withColumn("row_hash", md5(to_json(struct([col(c) for c in df_valid_flat.columns if c not in ["record_status", "corrections"]]))))

    inserted_count = valid_count + corrected_count
    updated_count = 0
    unchanged_count = 0

    try:
        df_existing = spark.read.format("mongo").option("uri", valid_uri).load()
        if "_id" in df_existing.columns:
            if "row_hash" in df_existing.columns:
                existing_data = df_existing.select(col("_id").alias("existing_id"), col("row_hash").alias("old_hash"))
            else:
                existing_data = df_existing.select(col("_id").alias("existing_id")).withColumn("old_hash", lit("none"))
            
            df_compare = df_valid_upsert.join(existing_data, df_valid_upsert["_id"] == existing_data["existing_id"], "left")
            
            inserted_count = df_compare.filter(col("existing_id").isNull()).count()
            unchanged_count = df_compare.filter((col("existing_id").isNotNull()) & (col("row_hash") == col("old_hash"))).count()
            updated_count = df_compare.filter((col("existing_id").isNotNull()) & (col("row_hash") != col("old_hash"))).count()
    except Exception as e:
        pass 

    df_valid_upsert.write.format("mongo") \
        .option("uri", valid_uri) \
        .option("replaceDocument", "true") \
        .mode("append") \
        .save()

    elapsed_seconds = time.time() - start_time

    # التقرير النهائي بالعدادات المطلوبة
    final_report = {
        "run_id": run_id,
        "file_name": file_name,
        "file_size_mb": 0.0,
        "engine_used": "pyspark_elt",
        "rows_read": rows_read,
        "raw_loaded": rows_read,
        "valid_count": valid_count,
        "corrected_count": corrected_count,
        "quarantine_count": quarantine_count,
        "elapsed_seconds": round(elapsed_seconds, 2),
        "throughput": round(rows_read / elapsed_seconds, 2) if elapsed_seconds > 0 else 0,
        "partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "error_case_counts": error_case_counts,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count
    }

    save_metrics_to_json(final_report)

    df_processed.unpersist()
    print(f"\n--- ELT Summary ---")
    print(f"Total: {rows_read} | Valid: {valid_count} | Corrected: {corrected_count} | Quarantine: {quarantine_count}")
    print(f"DB Actions -> Inserted: {inserted_count} | Updated: {updated_count} | Unchanged: {unchanged_count}")
    print(f"✅ Pipeline Completed in {round(elapsed_seconds, 2)}s!")

if __name__ == "__main__":
    # تم تقليل عدد الأنوية إلى 2 لضمان استقرار الشبكة وتقليل الضغط على الويندوز
    spark = SparkSession.builder \
        .appName("Hybrid_ELT_Pipeline") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        if len(sys.argv) > 1:
            passed_run_id = sys.argv[1]
        else:
            passed_run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
            
        run_elt_pipeline(spark, passed_run_id)
    finally:
        spark.stop() # لضمان إغلاق الجلسة حتى لو حدث خطأ
