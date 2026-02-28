import asyncio
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from core.apify_client import ApifyClient
from core.cache import SearchCache
from core.models import ProductItem, FetchMeta
from core.utils import coerce_int, coerce_float, coerce_price_rub
from providers.base import BaseMarketplaceProvider

class OzonProvider(BaseMarketplaceProvider):
    def __init__(self):
        self.provider_id = "apify"
        self.actor_id = os.environ.get("OZON_APIFY_ACTOR_ID", "zen-studio/ozon-scraper-pro").strip().replace("/", "~")
        
        self.proxy_use = os.environ.get("OZON_APIFY_PROXY_USE", "1").strip().lower() in {"1", "true", "yes"}
        self.proxy_group = os.environ.get("OZON_APIFY_PROXY_GROUP", "").strip()
        self.proxy_country = os.environ.get("OZON_APIFY_PROXY_COUNTRY", "").strip().upper()
        
        self.client = ApifyClient(
            token=os.environ.get("APIFY_TOKEN", "").strip(),
            base_url=os.environ.get("OZON_APIFY_BASE_URL", "https://api.apify.com/v2"),
            timeout_sec=float(os.environ.get("OZON_APIFY_TIMEOUT_SEC", "120")),
            retries=max(1, int(os.environ.get("OZON_APIFY_RETRIES", "3"))),
        )
        self.cache = SearchCache(
            ttl_sec=float(os.environ.get("SEARCH_CACHE_TTL_SEC", "900")),
            max_size=100
        )
        self.search_semaphore = asyncio.Semaphore(int(os.environ.get("MAX_CONCURRENT_SEARCHES", "2")))
        self.max_products = 10

    @property
    def provider_name(self) -> str:
        return "ozon"

    def get_provider_status(self) -> Dict[str, Any]:
        st = self.client.get_status()
        st["actor_id"] = self.actor_id
        return st

    def get_runtime_config(self) -> Dict[str, Any]:
        return {
            "PROVIDER": self.provider_id,
            "ACTOR_ID": self.actor_id,
            "PROXY_USE": self.proxy_use,
            "PROXY_GROUP": self.proxy_group,
            "PROXY_COUNTRY": self.proxy_country,
        }

    def _build_proxy_config(self) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {"useApifyProxy": bool(self.proxy_use)}
        if self.proxy_group:
            cfg["apifyProxyGroups"] = [self.proxy_group]
        if self.proxy_country:
            cfg["apifyProxyCountry"] = self.proxy_country
        return cfg

    def _build_actor_payload(self, query: str, limit: int) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "query": query,
            "search": query,
            "searchQuery": query,
            "text": query,
            "queries": [query],
            "searchQueries": [query],
            "maxItems": limit,
            "limit": limit,
            "resultsLimit": limit,
            "maxResults": limit,
            "count": limit,
            "page": 1,
            "pages": 1,
            "startPage": 1,
            "endPage": 1,
            "skipDetails": True,
            "includeSellerDetails": False,
            "sorting": "score",
            "language": "ru",
            "currency": "RUB",
        }
        proxy = self._build_proxy_config()
        payload["proxyConfiguration"] = proxy
        payload["proxyServer"] = proxy
        return payload

    def _candidate_objects(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = [row]
        for key in ("result", "product", "item", "offer", "data"):
            val = row.get(key)
            if isinstance(val, dict):
                candidates.append(val)
        return candidates

    def _pick_first(self, raws: List[Dict[str, Any]], keys: Tuple[str, ...]) -> Any:
        for raw in raws:
            for key in keys:
                if key in raw and raw.get(key) not in (None, ""):
                    return raw.get(key)
        return None

    def _safe_link(self, v: Any) -> str:
        text = str(v or "").strip()
        if not text:
            return ""
        if text.startswith("//"):
            return f"https:{text}"
        if text.startswith("/"):
            return urljoin("https://www.ozon.ru", text)
        return text

    def _extract_image_url(self, raws: List[Dict[str, Any]]) -> str:
        for raw in raws:
            # Check explicit image arrays first
            for list_key in ("images", "photos", "gallery", "pictureList"):
                items = raw.get(list_key)
                if isinstance(items, list) and items:
                    for item in items:
                        if isinstance(item, str) and not "?at=" in item and not "/product/" in item:
                            return self._safe_link(item)
            
            # Check direct string fields
            for direct_key in ("image", "imageUrl", "thumbnail", "photo", "picture", "src"):
                val = raw.get(direct_key)
                if isinstance(val, str) and val.strip():
                    # Reject values that are actually webpage links, not images
                    if "/product/" in val or "?at=" in val:
                        continue
                    return self._safe_link(val)
        return ""

    def _normalize_row(self, row: Dict[str, Any]) -> Optional[ProductItem]:
        raws = self._candidate_objects(row)

        name = str(self._pick_first(raws, ("name", "title", "productName", "offerName")) or "").strip()
        link = self._safe_link(self._pick_first(raws, ("link", "url", "productUrl", "offerUrl", "itemUrl")))
        if not name and not link:
            return None

        raw_price = self._pick_first(raws, ("price", "priceValue", "currentPrice", "finalPrice", "discountedPrice", "priceRub"))
        if isinstance(raw_price, dict):
            raw_price = raw_price.get("value") or raw_price.get("amount") or raw_price.get("price")
        price_rub = coerce_price_rub(raw_price)
        price = f"{price_rub} rub" if price_rub else "0 rub"

        rating_raw = self._pick_first(raws, ("rating", "reviewRating", "averageRating", "stars"))
        feedbacks_raw = self._pick_first(raws, ("feedbacks", "reviewCount", "reviewsCount", "opinions", "commentsCount"))
        brand = str(self._pick_first(raws, ("brand", "vendor", "manufacturer", "seller")) or "").strip()
        
        image = self._extract_image_url(raws)

        item_id_val = self._pick_first(raws, ("id", "productId", "modelId", "sku", "offerId"))
        item_id = coerce_int(item_id_val)
        if item_id is None and link:
            m = re.search(r"(\d{5,})", link)
            if m:
                item_id_val = m.group(1)
        
        item_id_text = str(item_id if item_id is not None else item_id_val or "").strip()
        if not item_id_text:
            stable = hashlib.sha1(f"{name}|{link}".encode("utf-8", errors="ignore")).hexdigest()[:12]
            item_id_text = stable

        rating = coerce_float(rating_raw) or 0.0
        feedbacks = coerce_int(feedbacks_raw) or 0

        return {
            "id": item_id_text,
            "name": name or "unknown",
            "brand": brand,
            "price": price,
            "rating": rating,
            "feedbacks": feedbacks,
            "link": link,
            "image": image,
        }

    async def fetch_products(self, query: str, limit: Optional[int] = None, fresh_only: bool = False) -> Tuple[List[ProductItem], FetchMeta]:
        target_limit = min(limit or self.max_products, self.max_products)
        payload = self._build_actor_payload(query, target_limit)

        async with self.search_semaphore:
            rows, error = await self.client.run_sync_actor_items(self.actor_id, payload, target_limit)
            if error:
                if not fresh_only:
                    cached = self.cache.get(query)
                    if cached:
                        return cached[0], {"price_status": "cached", "cache_age_sec": round(cached[1], 3), "source_total": len(cached[0]), "source": "cache"}
                
                text = error.lower()
                if "429" in text or ("rate" in text and "limit" in text):
                    return [], {"price_status": "empty_rate_limited", "cache_age_sec": None, "source_total": 0, "source": "none"}
                raise Exception(error)

            if not rows:
                if not fresh_only:
                    cached = self.cache.get(query)
                    if cached:
                        return cached[0], {"price_status": "cached", "cache_age_sec": round(cached[1], 3), "source_total": len(cached[0]), "source": "cache"}
                return [], {"price_status": "empty_upstream", "cache_age_sec": None, "source_total": 0, "source": "managed"}

            products: List[ProductItem] = []
            for row in rows:
                item = self._normalize_row(row)
                if item:
                    products.append(item)
                    if len(products) >= target_limit:
                        break

            if products:
                self.cache.put(query, products)
                priced_count = sum(1 for item in products if coerce_price_rub(item.get("price")) is not None)
                status = "live" if priced_count >= max(1, len(products) // 2) else "partial"
                return products, {"price_status": status, "cache_age_sec": None, "source_total": len(rows), "source": "managed"}

            if not fresh_only:
                cached = self.cache.get(query)
                if cached:
                    return cached[0], {"price_status": "cached", "cache_age_sec": round(cached[1], 3), "source_total": len(cached[0]), "source": "cache"}

            return [], {"price_status": "empty_upstream", "cache_age_sec": None, "source_total": len(rows), "source": "managed"}
