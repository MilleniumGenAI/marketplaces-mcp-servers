import time
from typing import Dict, List, Tuple, Optional
from collections import OrderedDict
from core.models import ProductItem

class SearchCache:
    def __init__(self, ttl_sec: float = 900.0, max_size: int = 100):
        self.ttl_sec = ttl_sec
        self.max_size = max_size
        self._cache: OrderedDict[str, Tuple[float, List[ProductItem]]] = OrderedDict()

    def get(self, query: str) -> Optional[Tuple[List[ProductItem], float]]:
        key = query.strip().lower()
        if key not in self._cache:
            return None
        
        ts, products = self._cache[key]
        age_sec = time.time() - ts
        if age_sec > self.ttl_sec:
            del self._cache[key]
            return None
        
        # Move to end (LRU)
        self._cache.move_to_end(key)
        return [dict(item) for item in products], age_sec

    def put(self, query: str, products: List[ProductItem]) -> None:
        key = query.strip().lower()
        if key in self._cache:
            del self._cache[key]
        elif len(self._cache) >= self.max_size:
            # Pop first item (least recently used)
            self._cache.popitem(last=False)
            
        self._cache[key] = (time.time(), [dict(item) for item in products])
