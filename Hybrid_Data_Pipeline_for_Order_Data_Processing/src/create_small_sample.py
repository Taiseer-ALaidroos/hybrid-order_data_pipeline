import argparse
import os
import sys

# إضافة المجلد الرئيسي لمسار النظام للتمكن من استيراد الإعدادات بنجاح
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

def create_sample(input_file, output_file, num_rows):
    # التحقق من وجود الملف الأصلي
    if not os.path.exists(input_file):
        print(f"❌ خطأ: الملف الأصلي غير موجود في المسار: {input_file}")
        print("الرجاء التأكد من وضع ملف البيانات في مجلد 'data'.")
        return

    print(f"⏳ جاري إنشاء عينة بحجم {num_rows} صف من '{input_file}'...")
    
    try:
        # استخدام القراءة السطرية (Streaming) لتجنب تحميل الملف الضخم في الذاكرة
        # إضافة errors='replace' لتجنب مشاكل ترميز الحروف التالفة في الملفات الضخمة
        with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
             
            # قراءة وكتابة الترويسة (Header) أولاً
            header = infile.readline()
            if not header:
                print("❌ خطأ: الملف الأصلي فارغ أو لا يحتوي على ترويسة (Header).")
                return
            outfile.write(header)
            
            count = 0
            for line in infile:
                # تجاهل الأسطر الفارغة تماماً لضمان دقة عدد الصفوف الفعلية
                if not line.strip():
                    continue
                    
                if count >= num_rows:
                    break
                    
                outfile.write(line)
                count += 1
                
        print(f"✅ تم إنشاء العينة بنجاح بحجم {count} صف، وحفظها في: {output_file}")
        
    except Exception as e:
        print(f"❌ حدث خطأ أثناء إنشاء العينة: {e}")

if __name__ == "__main__":
    # إعداد مدخلات سطر الأوامر كما طلب المحاضر
    parser = argparse.ArgumentParser(description="إنشاء عينة صغيرة من ملف بيانات ضخم.")
    parser.add_argument("--input", type=str, default=getattr(settings, 'HUGE_DATA_FILE', 'data/orders_huge_mixed_quality.csv'), help="مسار ملف البيانات الأصلي")
    parser.add_argument("--output", type=str, default=getattr(settings, 'SAMPLE_DATA_FILE', 'data/sample_orders.csv'), help="مسار ملف العينة الناتج")
    parser.add_argument("--rows", type=int, default=100000, help="عدد الصفوف المطلوبة في العينة")
    
    args = parser.parse_args()
    create_sample(args.input, args.output, args.rows)