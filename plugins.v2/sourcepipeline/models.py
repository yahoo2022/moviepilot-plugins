"""SourcePipeline 数据模型。

阶段1（库存）与阶段2/3（规划、清单、执行）共用同一批模型；
本模块只依赖标准库，可脱离 MoviePilot 独立导入用于离线验证。
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------- 库存（阶段1）


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


# ------------------------------------------------------------ 番号解析（阶段2）


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    """一次 profile 解析产生的可审计候选证据。"""

    profile: str
    source: str
    raw_text: str
    unwrapped_text: str
    canonical: str
    rule: str
    confidence: float = 1.0
    span: tuple[int, int] | None = None
    suffix: str = ""
    multipart: bool = False
    observations: tuple[str, ...] = ()

    def to_record(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "source": self.source,
            "raw_text": self.raw_text,
            "unwrapped_text": self.unwrapped_text,
            "canonical": self.canonical,
            "rule": self.rule,
            "confidence": self.confidence,
            "span": list(self.span) if self.span is not None else None,
            "suffix": self.suffix,
            "multipart": self.multipart,
            "observations": list(self.observations),
        }


@dataclass(frozen=True, slots=True)
class ParseResult:
    """profile 对单段候选文本的解析结果。"""

    profile: str
    evidence: tuple[CandidateEvidence, ...] = ()
    observations: tuple[str, ...] = ()

    @property
    def canonicals(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(item.canonical for item in self.evidence))

    @property
    def canonical(self) -> str | None:
        values = self.canonicals
        return values[0] if len(values) == 1 else None

    @property
    def conflicted(self) -> bool:
        return len(self.canonicals) > 1

    @property
    def multipart(self) -> bool:
        return any(item.multipart for item in self.evidence)


# ---------------------------------------------------------------- 计划（阶段2）


class PlanAction(str, Enum):
    """计划项要对 A 层执行的动作类型。"""

    NOOP = "NOOP"
    RENAME = "RENAME"
    REMOVE = "REMOVE"
    REVIEW = "REVIEW"


class PlanState(str, Enum):
    """规划裁决结果。只有 *_READY 才可能进入 manifest。"""

    NOOP = "NOOP"
    RENAME_READY = "RENAME_READY"
    GARBAGE_READY = "GARBAGE_READY"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    CONFLICT = "CONFLICT"
    PROTECTED = "PROTECTED"
    UNSUPPORTED = "UNSUPPORTED"


class PlanLifecycle(str, Enum):
    """计划项的执行生命周期，与规划裁决正交。"""

    PLANNED = "PLANNED"
    MANIFESTED = "MANIFESTED"
    APPLIED = "APPLIED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    STALE = "STALE"


READY_STATES = frozenset({PlanState.RENAME_READY, PlanState.GARBAGE_READY})

ACTION_FOR_STATE = {
    PlanState.RENAME_READY: PlanAction.RENAME,
    PlanState.GARBAGE_READY: PlanAction.REMOVE,
}


@dataclass(slots=True)
class PlanRecord:
    """一条 A 层整理计划。所有字段都可直接落 SQLite 或写报告。"""

    profile: str
    logic: str
    source_key: str
    path: str
    parent_path: str
    basename: str
    fingerprint: str
    size: int = 0
    modified: str = ""
    extension: str = ""
    state: PlanState = PlanState.REVIEW_REQUIRED
    action: PlanAction = PlanAction.REVIEW
    lifecycle: PlanLifecycle = PlanLifecycle.PLANNED
    canonical: str = ""
    target_name: str = ""
    confidence: float = 0.0
    reason: str = ""
    rules: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    conflict_with: list[str] = field(default_factory=list)
    ruleset_version: str = ""
    manifest_id: str = ""
    plan_id: str = ""

    @property
    def target_path(self) -> str:
        """RENAME 的目标绝对路径；其它动作为空。"""

        if self.action is not PlanAction.RENAME or not self.target_name:
            return ""
        return f"{self.parent_path.rstrip('/')}/{self.target_name}"

    def to_record(self) -> dict[str, Any]:
        """JSON/TSV 可序列化记录；不含 Token、URL 或原始响应。"""

        return {
            "plan_id": self.plan_id,
            "profile": self.profile,
            "logic": self.logic,
            "source_key": self.source_key,
            "path": self.path,
            "parent_path": self.parent_path,
            "basename": self.basename,
            "extension": self.extension,
            "size": self.size,
            "modified": self.modified,
            "fingerprint": self.fingerprint,
            "state": self.state.value,
            "action": self.action.value,
            "lifecycle": self.lifecycle.value,
            "canonical": self.canonical,
            "target_name": self.target_name,
            "target_path": self.target_path,
            "confidence": self.confidence,
            "reason": self.reason,
            "rules": list(self.rules),
            "observations": list(self.observations),
            "conflict_with": list(self.conflict_with),
            "ruleset_version": self.ruleset_version,
            "manifest_id": self.manifest_id,
        }


@dataclass(slots=True)
class PlanRunSummary:
    """一次规划运行的汇总。规划全程只读 SQLite，网络请求恒为 0。"""

    run_id: str
    started_at: str
    finished_at: str = ""
    ruleset_version: str = ""
    profiles: list[dict[str, Any]] = field(default_factory=list)
    report: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def totals(self) -> dict[str, int]:
        merged: dict[str, int] = {}
        for profile in self.profiles:
            for key, value in (profile.get("states") or {}).items():
                merged[key] = merged.get(key, 0) + int(value)
        return dict(sorted(merged.items()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "ruleset_version": self.ruleset_version,
            "profiles": list(self.profiles),
            "totals": self.totals,
            "report": self.report,
            "errors": list(self.errors),
        }


# 说明：清单冻结与执行相关的模型（ManifestSummary / ApplyRunSummary）刻意**不在**
# 本模块。它们连同写客户端一起放在仓库根的 `hub-seed/`，由未来的独立服务承担，
# 这样「插件目录只读」可以被静态检查证明。原因见 hub-seed/README.md。
