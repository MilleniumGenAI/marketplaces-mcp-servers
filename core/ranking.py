from typing import Any, Tuple, List, Dict
from core.models import ProductItem
from core.utils import parse_price_rub, parse_rating, parse_feedbacks

def select_top_products(products: List[ProductItem], top_n: int = 3) -> Tuple[List[ProductItem], Dict[str, Any]]:
    if top_n <= 0 or not products:
        return [], {
            "sort_policy": "price_asc_then_rating_desc_then_feedbacks_desc_then_stable",
            "filters_applied": ["price>0", "rating>0", "fallback_fill_to_top_n"],
            "eligible_count": 0,
            "dropped_count": len(products),
            "top_n": max(0, top_n),
            "selected_count": 0,
        }

    eligible: List[Tuple[int, int, float, int, str, str]] = []
    rest: List[Tuple[int, int, float, int, str, str]] = []
    for idx, item in enumerate(products):
        price = parse_price_rub(item.get("price"))
        rating = parse_rating(item.get("rating"))
        feedbacks = parse_feedbacks(item.get("feedbacks"))
        stable_id = str(item.get("id") or "")
        stable_link = str(item.get("link") or "")
        rec = (idx, price or 10 ** 9, rating or 0.0, feedbacks, stable_id, stable_link)
        if price is not None and rating is not None:
            eligible.append(rec)
        else:
            rest.append(rec)

    eligible_sorted = sorted(eligible, key=lambda x: (x[1], -x[2], -x[3], x[4], x[5], x[0]))
    selected_idx = [x[0] for x in eligible_sorted[:top_n]]

    if len(selected_idx) < top_n:
        rest_sorted = sorted(rest, key=lambda x: (x[1], -x[2], -x[3], x[4], x[5], x[0]))
        for rec in rest_sorted:
            if rec[0] in selected_idx:
                continue
            selected_idx.append(rec[0])
            if len(selected_idx) >= top_n:
                break

    selected = [dict(products[i]) for i in selected_idx]
    meta = {
        "sort_policy": "price_asc_then_rating_desc_then_feedbacks_desc_then_stable",
        "filters_applied": ["price>0", "rating>0", "fallback_fill_to_top_n"],
        "eligible_count": len(eligible),
        "dropped_count": max(0, len(products) - len(eligible)),
        "top_n": top_n,
        "selected_count": len(selected),
    }
    return selected, meta
