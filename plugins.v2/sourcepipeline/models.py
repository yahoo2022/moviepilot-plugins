"""SourcePipeline 阶段1的数据模型。"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass(frozen=True, slots=True)
class ProfileConfig:
    """一个独立来源实例及其可复用处理逻辑。"""

    name: str
    root: str
    logic: str = ""
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class SourceObject:
    """从 OpenList 目录项提取的最小库存对象，不保存原始响应。"""

    profile: str
    path: str
    parent_path: str
    basename: str
    is_dir: bool
    size: int = 0
    modified: str = ""
    object_id: str = ""
    fingerprint: str = ""


@dataclass(frozen=True, slots=True)
class PaginatedListing:
    """一个目录的完整分页结果。"""

    path: str
    items: tuple[dict[str, Any], ...]
    pages: int
    total: Optional[int]


@dataclass(slots=True)
class ProfileRunSummary:
    """单个来源实例的一次预算扫描汇总。"""

    profile: str
    logic: str
    root: str
    mode: str
    generation: int = 0
    request_budget: int = 0
    requests_remaining: int = 0
    directories_attempted: int = 0
    directories_completed: int = 0
    directories_partial: int = 0
    objects_seen: int = 0
    requests: int = 0
    retries: int = 0
    batch_pauses: int = 0
    throttle_sleep_seconds: float = 0.0
    queue_remaining: int = 0
    frontier_depth: Optional[int] = None
    stop_reason: str = ""
    converged: bool = False
    complete: bool = False
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class InventoryRunSummary:
    """一次插件运行汇总。"""

    run_id: str
    mode: str
    started_at: str
    finished_at: str = ""
    profiles: list[ProfileRunSummary] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """只表示本轮无 partial；是否整棵树收敛由 converged 单独表示。"""

        return bool(self.profiles) and all(item.directories_partial == 0 for item in self.profiles)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "success": self.success,
            "converged": bool(self.profiles) and all(item.converged for item in self.profiles),
            "profiles": [item.to_dict() for item in self.profiles],
        }
