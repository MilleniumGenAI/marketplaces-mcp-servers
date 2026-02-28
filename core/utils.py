import re
from typing import Any, Optional

def coerce_float(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        out = float(v)
        return out if out > 0 else None
    text = str(v).strip().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if not m:
        return None
    try:
        out = float(m.group(1))
        return out if out > 0 else None
    except Exception:
        return None

def coerce_int(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        out = int(v)
        return out if out > 0 else None
    text = re.sub(r"[^\d]", "", str(v))
    if not text:
        return None
    try:
        out = int(text)
        return out if out > 0 else None
    except Exception:
        return None

def coerce_price_rub(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        parsed = float(v)
        if parsed >= 1_000_000:
            parsed = parsed / 100.0
        out = int(round(parsed))
        return out if out > 0 else None

    text = str(v).strip().replace("\xa0", " ")
    if not text:
        return None

    if re.search(r"\d", text):
        if re.search(r"[.,]\d{1,2}\b", text):
            compact = re.sub(r"[^\d,.\-]", "", text).replace(",", ".")
            try:
                out = int(round(float(compact)))
                return out if out > 0 else None
            except Exception:
                pass

        digits = re.findall(r"\d+", text)
        if digits:
            try:
                out = int("".join(digits))
                if out >= 1_000_000_000:
                    out = int(round(out / 100.0))
                return out if out > 0 else None
            except Exception:
                return None
    return None

def parse_price_rub(v: Any) -> Optional[int]:
    return coerce_price_rub(v)

def parse_rating(v: Any) -> Optional[float]:
    return coerce_float(v)

def parse_feedbacks(v: Any) -> int:
    val = coerce_int(v)
    return val if val is not None else 0
