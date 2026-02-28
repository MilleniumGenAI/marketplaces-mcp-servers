from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
import json
from core.models import ProductItem, FetchMeta
from core.ranking import select_top_products

class BaseMarketplaceProvider(ABC):
    @property
    @abstractmethod
    def provider_name(self) -> str:
        pass

    @abstractmethod
    async def fetch_products(self, query: str, limit: Optional[int] = None, fresh_only: bool = False) -> Tuple[List[ProductItem], FetchMeta]:
        pass

    @abstractmethod
    def get_provider_status(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_runtime_config(self) -> Dict[str, Any]:
        pass

    def build_tool_response(
        self,
        query: str,
        products: List[ProductItem],
        error: Optional[str],
        price_status: str = "live",
        cache_age_sec: Optional[float] = None,
        source_total: Optional[int] = None,
        compact: bool = True,
    ) -> str:
        if source_total is None:
            source_total = len(products)
        
        image_ready = [p for p in products if str(p.get("image") or "").strip()]
        ranking_source = image_ready if len(image_ready) >= min(3, len(products)) else products
        top_products, selection_meta = select_top_products(ranking_source, 3)

        payload: Dict[str, Any] = {
            "query": query,
            "total": len(top_products) if compact else len(products),
            "source_total": source_total,
            "products": [] if compact else products,
            "top_products": top_products,
            "selection_meta": selection_meta,
            "error": error,
            "price_status": price_status,
            "cache_age_sec": cache_age_sec,
            "response_mode": "compact" if compact else "full",
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)
