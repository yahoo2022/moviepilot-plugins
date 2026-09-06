"""OpenList 严格只读客户端。

阶段1网络能力被代码级限制为 POST /api/fs/list。这里不提供通用请求方法，
也不包含 fs/get、rename、remove、move、copy、upload 或 download 能力。
"""
from __future__ import annotations

import ipaddress
import json
import random
import time
import urllib.parse
import urllib.request
from typing import Any, Callable, Optional

from .models import PaginatedListing

LIST_ENDPOINT = "/api/fs/list"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024


class RequestBudgetExceeded(RuntimeError):
    """本轮网络 attempt 预算已经耗尽。"""


class PaginationIncomplete(RuntimeError):
    """目录分页未能得到可安全发布的完整结果。"""


class RejectRedirectHandler(urllib.request.HTTPRedirectHandler):
    """拒绝重定向，避免 Authorization 被带离固定 origin/endpoint。"""

    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _is_controlled_http_host(hostname: str) -> bool:
    """HTTP 只允许 Docker/本机名、localhost、.local 或私网 IP。"""

    host = str(hostname or "").casefold()
    if host == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return "." not in host or host.endswith(".local")
    return address.is_private or address.is_loopback or address.is_link_local


def validate_origin(base_url: str) -> str:
    """只接受不带 userinfo/path/query/fragment 的 http(s) origin。"""

    origin = str(base_url or "").strip().rstrip("/")
    parsed = urllib.parse.urlsplit(origin)
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("OpenList 地址必须是无 userinfo/path/query/fragment 的 http(s) origin")
    if parsed.scheme.casefold() == "http" and not _is_controlled_http_host(parsed.hostname):
        raise ValueError("公网或远程域名必须使用 HTTPS，HTTP 仅允许受控 Docker/LAN 地址")
    return origin


def validate_absolute_path(path: str) -> str:
    """规范化并校验 OpenList 绝对路径。"""

    value = str(path or "").strip()
    if not value.startswith("/") or "\\" in value or "\x00" in value:
        raise ValueError("OpenList 路径必须是使用正斜杠的绝对路径")
    parts = [part for part in value.split("/") if part]
    if any(part in {".", ".."} for part in parts):
        raise ValueError("OpenList 路径不能包含 . 或 ..")
    return "/" + "/".join(parts) if parts else "/"


class ReadOnlyOpenListClient:
    """带分页、重试 attempt 预算、限速和同源校验的只读客户端。"""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout_seconds: int = 30,
        retries: int = 1,
        min_interval_seconds: float = 1.5,
        max_interval_seconds: float = 3.0,
        batch_requests: int = 20,
        batch_pause_min_seconds: float = 30.0,
        batch_pause_max_seconds: float = 90.0,
        max_requests: int = 60,
        per_page: int = 1000,
        max_pages_per_directory: int = 20,
        opener: Optional[Callable[..., Any]] = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        uniform: Callable[[float, float], float] = random.uniform,
    ):
        self.base_url = validate_origin(base_url)
        self.endpoint = f"{self.base_url}{LIST_ENDPOINT}"
        self._token = str(token or "").strip()
        if not self._token:
            raise ValueError("OpenList Token 未配置")
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.retries = min(2, max(0, int(retries)))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.max_interval_seconds = max(
            self.min_interval_seconds, float(max_interval_seconds)
        )
        self.batch_requests = max(0, int(batch_requests))
        self.batch_pause_min_seconds = max(0.0, float(batch_pause_min_seconds))
        self.batch_pause_max_seconds = max(
            self.batch_pause_min_seconds, float(batch_pause_max_seconds)
        )
        self.max_requests = int(max_requests)
        self.per_page = min(1000, max(1, int(per_page)))
        self.max_pages_per_directory = min(100, max(1, int(max_pages_per_directory)))
        if self.max_requests <= 0:
            raise ValueError("max_requests 必须大于 0")

        self.request_count = 0
        self.retry_count = 0
        self.batch_pause_count = 0
        self.throttle_sleep_seconds = 0.0
        self._last_attempt_at: Optional[float] = None
        self._opener = opener or urllib.request.build_opener(RejectRedirectHandler()).open
        self._sleep = sleep
        self._monotonic = monotonic
        self._uniform = uniform

    @property
    def remaining_requests(self) -> int:
        """本轮尚可使用的网络 attempt 数，retry 同样占用。"""

        return max(0, self.max_requests - self.request_count)

    def _sleep_for(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self.throttle_sleep_seconds += seconds
        self._sleep(seconds)

    def _wait_before_attempt(self) -> None:
        if self._last_attempt_at is None:
            return
        if self.batch_requests > 0 and self.request_count % self.batch_requests == 0:
            pause = self._uniform(
                self.batch_pause_min_seconds,
                self.batch_pause_max_seconds,
            )
            self.batch_pause_count += 1
            self._sleep_for(pause)
        target_interval = self._uniform(
            self.min_interval_seconds,
            self.max_interval_seconds,
        )
        remaining = target_interval - (self._monotonic() - self._last_attempt_at)
        self._sleep_for(remaining)

    def _safe_error(self, error: BaseException) -> str:
        text = str(error).replace(self._token, "[REDACTED]")
        quoted = urllib.parse.quote(self._token, safe="")
        if quoted:
            text = text.replace(quoted, "[REDACTED]")
        return text.replace(self.base_url, "[OPENLIST]")[:500]

    def _request_page(self, path: str, page: int, refresh: bool) -> dict[str, Any]:
        last_error = "未知错误"
        for attempt in range(self.retries + 1):
            if self.request_count >= self.max_requests:
                raise RequestBudgetExceeded(f"OpenList 请求达到本轮上限 {self.max_requests}")
            self._wait_before_attempt()
            self.request_count += 1
            if attempt > 0:
                self.retry_count += 1
            self._last_attempt_at = self._monotonic()

            payload = json.dumps(
                {
                    "path": path,
                    "page": page,
                    "per_page": self.per_page,
                    "refresh": bool(refresh),
                },
                ensure_ascii=False,
            ).encode("utf-8")
            request = urllib.request.Request(
                self.endpoint,
                data=payload,
                method="POST",
                headers={
                    "Authorization": self._token,
                    "Content-Type": "application/json",
                    "User-Agent": "moviepilot-sourcepipeline/0.2",
                },
            )
            try:
                with self._opener(request, timeout=self.timeout_seconds) as response:
                    if response.geturl() != self.endpoint:
                        raise RuntimeError("OpenList 响应偏离固定 /api/fs/list endpoint")
                    raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise RuntimeError("OpenList 响应超过安全大小上限")
                result = json.loads(raw.decode("utf-8"))
                if not isinstance(result, dict) or result.get("code") != 200:
                    code = result.get("code") if isinstance(result, dict) else "invalid"
                    raise RuntimeError(f"OpenList 返回 code={code}")
                data = result.get("data") or {}
                if not isinstance(data, dict):
                    raise RuntimeError("OpenList data 不是对象")
                return data
            except RequestBudgetExceeded:
                raise
            except Exception as error:
                last_error = self._safe_error(error)
                if attempt < self.retries:
                    continue
        raise RuntimeError(f"OpenList list 请求失败: {last_error}")

    def list_directory(self, path: str, *, refresh: bool) -> PaginatedListing:
        """完整列出一个目录；任一页失败时不返回可发布结果。"""

        safe_path = validate_absolute_path(path)
        collected: list[dict[str, Any]] = []
        expected_total: Optional[int] = None

        for page in range(1, self.max_pages_per_directory + 1):
            data = self._request_page(safe_path, page, refresh)
            content = data.get("content") or []
            if not isinstance(content, list) or any(not isinstance(item, dict) for item in content):
                raise PaginationIncomplete("OpenList content 不是对象列表")

            raw_total = data.get("total")
            if raw_total not in (None, ""):
                try:
                    page_total = int(raw_total)
                except (TypeError, ValueError) as error:
                    raise PaginationIncomplete("OpenList total 不是整数") from error
                if page_total < 0:
                    raise PaginationIncomplete("OpenList total 不能为负数")
                if expected_total is None:
                    expected_total = page_total
                elif expected_total != page_total:
                    raise PaginationIncomplete("OpenList 分页期间 total 发生变化")

            collected.extend(content)
            total_satisfied = expected_total is not None and len(collected) >= expected_total
            terminal_page = not content or len(content) < self.per_page
            if total_satisfied or terminal_page:
                if expected_total is not None and len(collected) < expected_total:
                    raise PaginationIncomplete(
                        f"OpenList 提前结束分页：已取 {len(collected)}，total={expected_total}"
                    )
                if expected_total is not None and len(collected) > expected_total:
                    raise PaginationIncomplete(
                        f"OpenList 返回条目数 {len(collected)} 超过 total={expected_total}"
                    )
                return PaginatedListing(
                    path=safe_path,
                    items=tuple(collected),
                    pages=page,
                    total=expected_total,
                )

        raise PaginationIncomplete(
            f"目录超过 max_pages_per_directory={self.max_pages_per_directory}，结果未发布"
        )
