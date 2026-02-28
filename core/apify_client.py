import asyncio
import time
import random
from typing import Any, Dict, Optional, Tuple, List
import httpx
from urllib.parse import quote

class ApifyClient:
    def __init__(self, token: str, base_url: str = "https://api.apify.com/v2", timeout_sec: float = 120.0,
                 retries: int = 3, retry_base_delay: float = 2.0, min_interval_sec: float = 1.2):
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.retries = max(1, retries)
        self.retry_base_delay = retry_base_delay
        self.min_interval_sec = min_interval_sec
        
        self._request_lock = asyncio.Lock()
        self._last_request_ts = 0.0
        
        # Track provider status
        self.last_http_status: Optional[int] = None
        self.last_error: Optional[str] = None
        self.last_latency_ms: Optional[int] = None
        self.last_rows_count: Optional[int] = None
        self.last_request_at_unix: Optional[int] = None
        self.success_count: int = 0
        self.error_count: int = 0
        self.fail_streak: int = 0

    async def _throttle_requests(self) -> None:
        async with self._request_lock:
            now = time.monotonic()
            delta = now - self._last_request_ts
            if delta < self.min_interval_sec:
                await asyncio.sleep(self.min_interval_sec - delta)
            self._last_request_ts = time.monotonic()

    def _mark_success(self, status_code: Optional[int], latency_ms: Optional[int], rows_count: int) -> None:
        self.last_http_status = status_code
        self.last_latency_ms = latency_ms
        self.last_rows_count = rows_count
        self.last_error = None
        self.last_request_at_unix = int(time.time())
        self.success_count += 1
        self.fail_streak = 0

    def _mark_failure(self, status_code: Optional[int], error_text: str) -> None:
        self.last_http_status = status_code
        self.last_error = error_text
        self.last_request_at_unix = int(time.time())
        self.error_count += 1
        self.fail_streak += 1

    def _parse_error(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                err = payload.get("error")
                if isinstance(err, dict):
                    message = str(err.get("message") or "").strip()
                    err_type = str(err.get("type") or "").strip()
                    if err_type and message:
                        return f"{err_type}: {message}"
                    if message:
                        return message
                message = str(payload.get("message") or "").strip()
                if message:
                    return message
        except Exception:
            pass
        text = response.text.strip()
        return text[:300] if text else "Unknown upstream error"

    def get_status(self) -> Dict[str, Any]:
        return {
            "token_set": bool(self.token),
            "last_http_status": self.last_http_status,
            "last_error": self.last_error,
            "last_latency_ms": self.last_latency_ms,
            "last_rows_count": self.last_rows_count,
            "last_request_at_unix": self.last_request_at_unix,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "fail_streak": self.fail_streak,
        }

    async def run_sync_actor_items(self, actor_id: str, payload: Dict[str, Any], limit: int) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        if not self.token:
            return None, "APIFY_TOKEN is not configured"

        safe_actor_id = quote(actor_id.replace("/", "~"), safe="~")
        url = f"{self.base_url}/acts/{safe_actor_id}/run-sync-get-dataset-items"
        params = {
            "timeout": int(self.timeout_sec),
            "format": "json",
            "clean": "1",
            "limit": max(1, limit),
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        last_status: Optional[int] = None
        last_error = "Unknown error"

        async with httpx.AsyncClient(timeout=self.timeout_sec, follow_redirects=True) as client:
            for attempt in range(self.retries):
                try:
                    await self._throttle_requests()
                    started = time.monotonic()
                    response = await client.post(url, params=params, headers=headers, json=payload)
                    latency_ms = int((time.monotonic() - started) * 1000)
                    last_status = response.status_code

                    if response.status_code in (429, 500, 502, 503, 504):
                        last_error = self._parse_error(response)
                        self._mark_failure(response.status_code, f"{actor_id}: {last_error}")
                        if attempt < self.retries - 1:
                            await asyncio.sleep(self.retry_base_delay * (2**attempt) + random.uniform(0.0, 0.4))
                            continue
                        return None, f"Apify HTTP {response.status_code} ({actor_id}): {last_error}"

                    if response.status_code == 408:
                        last_error = self._parse_error(response)
                        self._mark_failure(response.status_code, f"{actor_id}: {last_error}")
                        return None, f"Apify sync timeout (408) ({actor_id}): {last_error}"

                    if response.status_code in (400, 401, 403, 404):
                        last_error = self._parse_error(response)
                        self._mark_failure(response.status_code, f"{actor_id}: {last_error}")
                        return None, f"Apify HTTP {response.status_code} ({actor_id}): {last_error}"

                    response.raise_for_status()
                    body = response.json()
                    rows = body if isinstance(body, list) else []
                    valid_rows = [x for x in rows if isinstance(x, dict)]
                    self._mark_success(response.status_code, latency_ms, len(valid_rows))
                    return valid_rows, None

                except httpx.RequestError as exc:
                    last_error = str(exc)
                    self._mark_failure(last_status, f"{actor_id}: {last_error}")
                    if attempt < self.retries - 1:
                        await asyncio.sleep(self.retry_base_delay * (2**attempt) + random.uniform(0.0, 0.4))
                        continue
                    return None, f"Apify request failed ({actor_id}): {last_error}"
                except Exception as exc:
                    last_error = str(exc)
                    self._mark_failure(last_status, f"{actor_id}: {last_error}")
                    return None, f"Apify unexpected error ({actor_id}): {last_error}"

        return None, last_error
