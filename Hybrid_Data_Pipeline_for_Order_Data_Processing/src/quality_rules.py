# src/quality_rules.py
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

ARABIC_DIGITS_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
ARABIC_DECIMAL_MAP = str.maketrans({"٫": ".", "٬": ","})

KNOWN_WORD_NUMBERS = {
    "ثلاثة آلاف": 3000.0, "ثلاثة الاف": 3000.0,
    "أربعة آلاف": 4000.0, "اربعة الاف": 4000.0,
    "خمسة آلاف": 5000.0, "خمسة الاف": 5000.0,
    "عشرة آلاف": 10000.0, "عشرة الاف": 10000.0,
    "ألفان": 2000.0, "الفان": 2000.0, "ألفين": 2000.0, "الفين": 2000.0,
    "ألف": 1000.0, "الف": 1000.0,
}

STATUS_MAPPING = {
    "قيد الانتظار": "pending", "معلق": "pending", "created": "pending", "pending": "pending",
    "مؤكد": "confirmed", "مدفوع": "confirmed", "paid": "confirmed", "confirmed": "confirmed",
    "قيد الشحن": "shipped", "قيد التوصيل": "shipped", "shipped": "shipped", "in_transit": "shipped",
    "تم التسليم": "delivered", "تم التوصيل": "delivered", "delivered": "delivered", "done": "delivered",
    "مرتجع": "returned", "returned": "returned",
    "ملغي": "cancelled", "ملغى": "cancelled", "cancelled": "cancelled", "canceled": "cancelled",
}

PAYMENT_METHOD_MAPPING = {
    "نقداً عند التسليم": "cash_on_delivery", "نقدًا عند التسليم": "cash_on_delivery",
    "نقد": "cash_on_delivery", "كاش": "cash_on_delivery",
    "cash": "cash_on_delivery", "cash on delivery": "cash_on_delivery", "cod": "cash_on_delivery",
    "بطاقة": "card", "بطاقة ائتمان": "card", "card": "card", "credit card": "card",
    "محفظة إلكترونية": "wallet", "محفظة": "wallet", "wallet": "wallet", "e-wallet": "wallet",
}

PAYMENT_STATUS_MAPPING = {
    "بانتظار الدفع": "unpaid", "غير مدفوع": "unpaid", "معلق": "unpaid",
    "pending": "unpaid", "unpaid": "unpaid", "waiting": "unpaid",
    "تم الدفع": "paid", "مدفوع": "paid", "paid": "paid", "approved": "paid",
    "مرفوض": "failed", "فشل": "failed", "failed": "failed", "rejected": "failed",
    "مسترد": "refunded", "refunded": "refunded",
}

DELIVERY_MAPPING = {
    "عادي": "standard", "قياسي": "standard", "normal": "standard", "standard": "standard",
    "سريع": "express", "مستعجل": "express", "fast": "express", "express": "express",
}

CURRENCY_SYNONYMS = {
    "ريال يمني": "YER", "ريال": "YER", "ريالات": "YER", "ر.ي": "YER", "yer": "YER",
    "ريال سعودي": "SAR", "sar": "SAR",
    "دولار": "USD", "usd": "USD",
}

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
ISO_DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$")

def normalize_arabic_digits(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return value.translate(ARABIC_DIGITS_MAP).translate(ARABIC_DECIMAL_MAP)

def parse_number_field(val: Any) -> Tuple[Optional[float], bool]:
    if val is None or isinstance(val, bool):
        return None, False
    if isinstance(val, (int, float)):
        return float(val), False

    val_str = str(val).strip()
    if not val_str or val_str.lower() in ["none", "null", "nan", ""]:
        return None, False

    if re.match(r"^-?\d+(\.\d+)?$", val_str) and val_str.isascii():
        return float(val_str), False

    text = normalize_arabic_digits(val_str)
    for word, num in KNOWN_WORD_NUMBERS.items():
        if word in text:
            return num, True
    for word in ["ريال يمني", "ريال", "ريالات", "لاير", "لاير يمني", "YER", "yer"]:
        text = text.replace(word, "")

    text = text.replace(",", "").strip()
    cleaned = re.sub(r"[^\d.-]", "", text)
    if cleaned in {"", "-", ".", "-."}:
        return None, False
    try:
        return float(cleaned), True
    except ValueError:
        return None, False

def clean_order(raw_record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[str]]:
    # كود نادر الشوكي لتنظيف البيانات كاملاً هنا
    record = {k.strip().replace("\ufeff", ""): v for k, v in raw_record.items()}
    corrections: List[Dict[str, Any]] = []
    quarantine_reasons: List[str] = []

    # 1. order_id
    raw_oid = record.get("order_id")
    if raw_oid is None or str(raw_oid).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("MISSING_ORDER_ID")
    else:
        oid_str = str(raw_oid)
        if oid_str != oid_str.strip():
            corrections.append({"field": "order_id", "original_value": raw_oid, "corrected_value": oid_str.strip(), "rule_code": "TRIM_WHITESPACE"})
        record["order_id"] = oid_str.strip()

    # 2. customer_id
    raw_cid = record.get("customer_id")
    if raw_cid is None or str(raw_cid).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("MISSING_CUSTOMER_ID")
    else:
        cid_str = str(raw_cid)
        if cid_str != cid_str.strip():
            corrections.append({"field": "customer_id", "original_value": raw_cid, "corrected_value": cid_str.strip(), "rule_code": "TRIM_WHITESPACE"})
        record["customer_id"] = cid_str.strip()

    # 3. order_date
    raw_date = record.get("order_date")
    if raw_date is None or str(raw_date).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("INVALID_IMPOSSIBLE_DATE")
    else:
        date_str = str(raw_date).strip()
        is_standard_iso = bool(ISO_DATE_REGEX.match(date_str)) and date_str.isascii() and (str(raw_date) == date_str)
        text = normalize_arabic_digits(date_str).replace("/", "-").replace(".", "-")
        parsed = None
        for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y"]:
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue

        if not parsed or parsed.year < 2015 or parsed.year > 2030:
            quarantine_reasons.append("INVALID_IMPOSSIBLE_DATE")
        else:
            normalized_date = parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
            record["order_date"] = normalized_date
            if not is_standard_iso:
                corrections.append({"field": "order_date", "original_value": raw_date, "corrected_value": normalized_date, "rule_code": "DATE_FORMAT_NORMALIZED"})

    # 4. status & categorical mappings
    raw_status = record.get("status")
    if raw_status is None or str(raw_status).strip() == "":
        quarantine_reasons.append("INVALID_UNKNOWN_STATUS")
    else:
        st_raw_str = str(raw_status)
        st_clean = st_raw_str.strip()
        mapped_status = STATUS_MAPPING.get(st_clean.lower(), STATUS_MAPPING.get(st_clean))
        if not mapped_status:
            quarantine_reasons.append("INVALID_UNKNOWN_STATUS")
        else:
            record["status"] = mapped_status
            if st_raw_str != st_clean:
                corrections.append({"field": "status", "original_value": raw_status, "corrected_value": mapped_status, "rule_code": "STATUS_WHITESPACE_TRIMMED"})

    for field, mapping in [("payment_status", PAYMENT_STATUS_MAPPING), ("payment_method", PAYMENT_METHOD_MAPPING), ("delivery_type", DELIVERY_MAPPING)]:
        val = record.get(field)
        if val is not None:
            raw_v = str(val)
            clean_v = raw_v.strip()
            mapped_v = mapping.get(clean_v.lower(), mapping.get(clean_v, clean_v))
            record[field] = mapped_v
            if raw_v != clean_v:
                corrections.append({"field": field, "original_value": val, "corrected_value": mapped_v, "rule_code": f"{field.upper()}_WHITESPACE_TRIMMED"})

    # 5. customer_phone
    raw_phone = record.get("customer_phone")
    if raw_phone is None or str(raw_phone).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("INVALID_PHONE_NUMBER")
    else:
        ph_raw_str = str(raw_phone)
        ph_str = ph_raw_str.strip()
        is_clean_phone = bool(re.match(r"^(?:\+967)?7\d{8}$", ph_str)) and ph_str.isascii() and (ph_raw_str == ph_str)
        digits = re.sub(r"\D", "", normalize_arabic_digits(ph_str))
        if digits.startswith("00967"): digits = digits[5:]
        elif digits.startswith("967") and len(digits) == 12: digits = digits[3:]
        elif digits.startswith("07") and len(digits) == 10: digits = digits[1:]

        if len(digits) == 9 and digits.startswith("7"):
            record["customer_phone"] = f"+967{digits}"
            if not is_clean_phone:
                corrections.append({"field": "customer_phone", "original_value": raw_phone, "corrected_value": record["customer_phone"], "rule_code": "PHONE_FORMAT_NORMALIZED"})
        else:
            quarantine_reasons.append("INVALID_PHONE_NUMBER")

    # 6. customer_email
    raw_email = record.get("customer_email")
    if raw_email is None or str(raw_email).strip().lower() in ["", "null", "none", "nan"]:
        quarantine_reasons.append("INVALID_EMAIL_ADDRESS")
    else:
        em_raw_str = str(raw_email)
        em_str = em_raw_str.strip()
        if EMAIL_REGEX.match(em_str) and (em_raw_str == em_str):
            record["customer_email"] = em_str
        else:
            cleaned_em = em_str.lower().replace(" ", "")
            cleaned_em = re.sub(r"@{2,}", "@", cleaned_em)
            cleaned_em = re.sub(r"\.{2,}", ".", cleaned_em)
            if EMAIL_REGEX.match(cleaned_em):
                record["customer_email"] = cleaned_em
                corrections.append({"field": "customer_email", "original_value": raw_email, "corrected_value": cleaned_em, "rule_code": "EMAIL_TYPO_REPAIRED"})
            else:
                quarantine_reasons.append("INVALID_EMAIL_ADDRESS")

    # 7. Numeric fields
    for field, err_code in [("delivery_cost", "INVALID_DELIVERY_COST"), ("payment_amount", "INVALID_PAYMENT_AMOUNT"), ("total_amount", "INVALID_TOTAL_AMOUNT")]:
        raw_val = record.get(field)
        parsed_val, is_changed = parse_number_field(raw_val)
        if parsed_val is None:
            quarantine_reasons.append(err_code)
            record[field] = 0.0
        else:
            record[field] = parsed_val
            if is_changed:
                corrections.append({"field": field, "original_value": raw_val, "corrected_value": parsed_val, "rule_code": "NUMERIC_FORMAT_NORMALIZED"})

    if (record["delivery_cost"] < 0) or (record["payment_amount"] < 0) or (record["total_amount"] < 0):
        quarantine_reasons.append("AMBIGUOUS_NEGATIVE_VALUE")

    # 8. Items Extraction
    items_raw = record.get("items_json")
    items_sum = 0.0
    parsed_items = None
    if items_raw is None or str(items_raw).strip() in ["", "[]", "null", "None", "nan"]:
        quarantine_reasons.append("EMPTY_ITEMS")
    else:
        try:
            data = json.loads(str(items_raw).strip())
            if isinstance(data, dict): data = [data]
            if isinstance(data, list) and len(data) > 0:
                parsed_items = []
                item_string_num_repaired = False
                for it in data:
                    raw_sku = it.get("sku")
                    raw_name = it.get("name", it.get("item_name"))
                    if not raw_sku or not str(raw_sku).strip() or not raw_name or not str(raw_name).strip():
                        quarantine_reasons.append("MISSING_ITEM_SKU_OR_NAME")

                    q_val, q_chg = parse_number_field(it.get("qty", it.get("quantity")))
                    p_val, p_chg = parse_number_field(it.get("unit_price", it.get("price")))
                    
                    if q_chg or p_chg or isinstance(it.get("qty"), str) or isinstance(it.get("unit_price"), str):
                        item_string_num_repaired = True

                    if q_val is None or p_val is None or q_val <= 0 or p_val <= 0:
                        quarantine_reasons.append("INVALID_ITEM_QUANTITY_OR_PRICE")
                        q, p = int(q_val or 0), float(p_val or 0.0)
                    else:
                        q, p = int(q_val), float(p_val)

                    sub = round(q * p, 2)
                    items_sum = round(items_sum + sub, 2)
                    parsed_items.append({"sku": str(raw_sku).strip() if raw_sku else "UNKNOWN", "item_name": str(raw_name).strip() if raw_name else "Product_Item", "quantity": q, "unit_price": p, "subtotal": sub})
                
                record["items"] = parsed_items
                if item_string_num_repaired and "INVALID_ITEM_QUANTITY_OR_PRICE" not in quarantine_reasons:
                    corrections.append({"field": "items_json", "original_value": items_raw, "corrected_value": parsed_items, "rule_code": "ITEM_NUMERIC_TYPE_CAST"})
            else:
                quarantine_reasons.append("EMPTY_ITEMS")
        except Exception:
            quarantine_reasons.append("CORRUPTED_ITEMS_JSON")

    # 9. Reconcile total_amount
    if not quarantine_reasons and parsed_items:
        expected_total = round(items_sum + record["delivery_cost"], 2)
        if round(record["total_amount"], 2) != expected_total:
            old_total = record["total_amount"]
            record["total_amount"] = expected_total
            corrections.append({"field": "total_amount", "original_value": old_total, "corrected_value": expected_total, "rule_code": "TOTAL_AMOUNT_RECALCULATED"})

    # 10. Currency
    curr = record.get("currency")
    if curr is None or str(curr).strip() == "":
        quarantine_reasons.append("INVALID_CURRENCY")
    else:
        c_strip = str(curr).strip()
        c_up = c_strip.upper()
        if c_up in ["YER", "SAR", "USD"]:
            record["currency"] = c_up
            if str(curr) != c_up:
                corrections.append({"field": "currency", "original_value": curr, "corrected_value": c_up, "rule_code": "CURRENCY_NORMALIZED"})
        elif c_strip in CURRENCY_SYNONYMS or c_strip.lower() in CURRENCY_SYNONYMS:
            mapped_curr = CURRENCY_SYNONYMS.get(c_strip, CURRENCY_SYNONYMS.get(c_strip.lower(), "YER"))
            record["currency"] = mapped_curr
            corrections.append({"field": "currency", "original_value": curr, "corrected_value": mapped_curr, "rule_code": "CURRENCY_SYNONYM_NORMALIZED"})
        else:
            quarantine_reasons.append("INVALID_CURRENCY")

    quarantine_reasons = list(dict.fromkeys(quarantine_reasons))

    # تطبيق شرط الدكتور: إذا كان السجل يحتوي على أكثر من خطأ، نصنفه كخطأ متعدد
    if len(quarantine_reasons) > 1:
        quarantine_reasons = ["MULTIPLE_CONFLICTING_ERRORS"]

    if quarantine_reasons:
        record["quality_status"] = "quarantined"
    elif len(corrections) > 0:
        record["quality_status"] = "corrected"
        record["corrections"] = corrections
    else:
        record["quality_status"] = "valid"
        record["corrections"] = []

    return record, corrections, quarantine_reasons


