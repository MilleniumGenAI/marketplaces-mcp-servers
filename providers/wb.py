import asyncio
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

class WbProvider(BaseMarketplaceProvider):
    def __init__(self):
        self.provider_id = "apify"
        self.card_actor_id = os.environ.get("WB_APIFY_CARD_ACTOR_ID", "akoinc~wb-card-parser").strip()
        self.search_actor_id = os.environ.get("WB_APIFY_SEARCH_ACTOR_ID", "apify~google-search-scraper").strip()
        
        self.search_max_pages = max(1, int(os.environ.get("WB_SEARCH_MAX_PAGES", "1")))
        self.proxy_use = os.environ.get("WB_APIFY_PROXY_USE", "1").strip().lower() in {"1", "true", "yes"}
        self.proxy_group = os.environ.get("WB_APIFY_PROXY_GROUP", "BUYPROXIES94952").strip()
        self.proxy_country = os.environ.get("WB_APIFY_PROXY_COUNTRY", "").strip().upper()
        
        self.client = ApifyClient(
            token=os.environ.get("APIFY_TOKEN", "").strip(),
            base_url=os.environ.get("WB_APIFY_BASE_URL", "https://api.apify.com/v2"),
            timeout_sec=float(os.environ.get("WB_APIFY_TIMEOUT_SEC", "120")),
            retries=max(1, int(os.environ.get("WB_APIFY_RETRIES", "3"))),
        )
        self.cache = SearchCache(
            ttl_sec=float(os.environ.get("SEARCH_CACHE_TTL_SEC", "900")),
            max_size=100
        )
        self.search_semaphore = asyncio.Semaphore(int(os.environ.get("MAX_CONCURRENT_SEARCHES", "2")))
        self.max_products = 10

    @property
    def provider_name(self) -> str:
        return "wb"

    def get_provider_status(self) -> Dict[str, Any]:
        st = self.client.get_status()
        st["actor_id"] = self.card_actor_id
        st["search_actor_id"] = self.search_actor_id
        return st

    def get_runtime_config(self) -> Dict[str, Any]:
        return {
            "PROVIDER": self.provider_id,
            "CARD_ACTOR_ID": self.card_actor_id,
            "SEARCH_ACTOR_ID": self.search_actor_id,
            "PROXY_USE": self.proxy_use,
            "PROXY_GROUP": self.proxy_group,
            "PROXY_COUNTRY": self.proxy_country,
        }

    def _build_proxy_server(self) -> Dict[str, Any]:
        proxy: Dict[str, Any] = {"useApifyProxy": bool(self.proxy_use)}
        if self.proxy_group:
            proxy["apifyProxyGroups"] = [self.proxy_group]
        if self.proxy_country:
            proxy["apifyProxyCountry"] = self.proxy_country
        return proxy

    def _extract_article_ids(self, query: str, cap: int) -> List[str]:
        out: List[str] = []
        seen = set()
        for m in re.finditer(r"wildberries\.ru/catalog/(\d+)/detail\.aspx", query, flags=re.IGNORECASE):
            aid = m.group(1)
            if aid not in seen:
                seen.add(aid)
                out.append(aid)
        for m in re.finditer(r"\b(\d{6,12})\b", query):
            aid = m.group(1)
            if aid not in seen:
                seen.add(aid)
                out.append(aid)
        return out[:cap]

    def _build_search_query(self, query: str) -> str:
        base = query.strip().lower()
        replacements = [
            ("айфона", "iphone"), ("айфоне", "iphone"), ("айфон", "iphone"),
            ("про макс", "pro max"), ("про", "pro"), ("макс", "max"),
            ("тб", "tb"), ("сим", "sim"),
        ]
        for src, dst in replacements:
            base = base.replace(src, dst)
        base = re.sub(r"\s+", " ", base).strip()
        
        if "iphone 17 pro max" in base and not re.search(r"\b\d+\s*(tb|gb|тб|гб)\b", base):
            base = f"{base} 1 tb"
        return f'site:wildberries.ru/catalog "detail.aspx" {base}'.strip()

    async def _discover_article_ids(self, query: str, cap: int) -> Tuple[List[str], Optional[str]]:
        direct_ids = self._extract_article_ids(query, cap)
        if direct_ids:
            return direct_ids, None

        payload = {
            "queries": self._build_search_query(query),
            "maxPagesPerQuery": self.search_max_pages,
            "countryCode": "ru",
            "languageCode": "ru",
        }
        rows, error = await self.client.run_sync_actor_items(self.search_actor_id, payload, 1)
        if error:
            return [], error

        out: List[str] = []
        seen = set()
        for row in (rows or []):
            for key in ("organicResults", "paidResults", "paidProducts", "suggestedResults"):
                items = row.get(key)
                if not isinstance(items, list):
                    continue
                for item in items:
                    val = item.get("url") or item.get("productUrl") or item.get("displayedUrl") or ""
                    m = re.search(r"/catalog/(\d+)/detail\.aspx", val, flags=re.IGNORECASE)
                    if m:
                        aid = m.group(1)
                        if aid not in seen:
                            seen.add(aid)
                            out.append(aid)
                            if len(out) >= cap:
                                return out, None
        return out[:cap], None

    def _find_deep(self, obj: Any, keys: List[str]) -> Any:
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and obj[k] not in (None, ""):
                    return obj[k]
            for v in obj.values():
                res = self._find_deep(v, keys)
                if res is not None:
                    return res
        elif isinstance(obj, list):
            for item in obj:
                res = self._find_deep(item, keys)
                if res is not None:
                    return res
        return None

    def _normalize_row(self, row: Dict[str, Any], fallback_id: str) -> Optional[ProductItem]:
        item_id = coerce_int(self._find_deep(row, ["nm_id", "id", "nmId", "article"])) or coerce_int(fallback_id)
        if not item_id:
            return None

        name = str(self._find_deep(row, ["imt_name", "name", "title", "goodsName"]) or f"WB {item_id}").strip()
        brand = str(self._find_deep(row, ["brand_name", "brand", "brandName", "seller"]) or "").strip()

        raw_price = self._find_deep(row, ["salePriceU", "priceU", "salePrice", "priceWithDiscount", "price"])
        price_rub = coerce_price_rub(raw_price) or 0

        rating = coerce_float(self._find_deep(row, ["reviewRating", "rating", "supplierRating"])) or 0.0
        feedbacks = coerce_int(self._find_deep(row, ["feedbacks", "nmFeedbacks", "reviewsCount"])) or 0
        
        image = self._find_deep(row, ["image", "imageUrl", "photo", "picture"])
        if not image:
            image = f"https://images.wbstatic.net/c516x688/new/{item_id}-1.jpg"
            
        if isinstance(image, str) and image.startswith("//"):
            image = "https:" + image

        return {
            "id": item_id,
            "name": name,
            "brand": brand,
            "price": f"{int(price_rub)} rub" if price_rub else "0 rub",
            "rating": rating,
            "feedbacks": feedbacks,
            "link": f"https://www.wildberries.ru/catalog/{item_id}/detail.aspx",
            "image": str(image)
        }

    async def fetch_products(self, query: str, limit: Optional[int] = None, fresh_only: bool = False) -> Tuple[List[ProductItem], FetchMeta]:
        target_limit = min(limit or self.max_products, self.max_products)
        discovery_cap = target_limit * 3

        async with self.search_semaphore:
            article_ids, discovery_error = await self._discover_article_ids(query, discovery_cap)
            if discovery_error:
                if not fresh_only:
                    cached = self.cache.get(query)
                    if cached:
                        return cached[0], {"price_status": "cached", "cache_age_sec": round(cached[1], 3), "source_total": len(cached[0]), "source": "cache"}
                raise Exception(f"Discovery error: {discovery_error}")

            if not article_ids:
                return [], {"price_status": "empty_upstream", "cache_age_sec": None, "source_total": 0, "source": "apify"}

            products: List[ProductItem] = []
            errors = []
            
            # Temporary debug logic
            debug_rows = []

            for aid in article_ids:
                payload = {"articleId": str(aid), "proxyServer": self._build_proxy_server()}
                rows, err = await self.client.run_sync_actor_items(self.card_actor_id, payload, 1)
                if err:
                    errors.append(err)
                    continue
                if rows:
                    debug_rows.append(rows[0])
                    item = self._normalize_row(rows[0], aid)
                    if item:
                        products.append(item)
                        if len(products) >= target_limit:
                            break

            # Dump debug
            with open("wb_debug_dump.json", "w", encoding="utf-8") as f:
                json.dump(debug_rows, f, indent=2, ensure_ascii=False)

            if products:
                self.cache.put(query, products)
                status = "live" if not errors else "partial"
                return products, {"price_status": status, "cache_age_sec": None, "source_total": len(article_ids), "source": "apify"}

            if not fresh_only:
                cached = self.cache.get(query)
                if cached:
                    return cached[0], {"price_status": "cached", "cache_age_sec": round(cached[1], 3), "source_total": len(cached[0]), "source": "cache"}

            if errors:
                raise Exception(f"Errors fetching items: {errors[0]}")
            return [], {"price_status": "empty_upstream", "cache_age_sec": None, "source_total": len(article_ids), "source": "apify"}
