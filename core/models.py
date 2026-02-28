from typing import Any, Dict, TypedDict, List, Optional

class ProductItem(TypedDict, total=False):
    id: str | int
    name: str
    brand: str
    price: str
    rating: float
    feedbacks: int
    link: str
    image: str

class FetchMeta(TypedDict, total=False):
    price_status: str
    cache_age_sec: Optional[float]
    source_total: int
    source: str
