"""MDC STRM 预处理核心数据模型。

本模块仅依赖 Python 3.11 标准库，可脱离 MoviePilot 独立导入。
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Iterator


class ItemState(str, Enum):
    """扫描和规划项的生命周期状态。"""

    DISCOVERED = "DISCOVERED"
    READY = "READY"
    STAGED = "STAGED"
    UNCHANGED = "UNCHANGED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    PROFILE_MISMATCH = "PROFILE_MISMATCH"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    JUNK_CANDIDATE = "JUNK_CANDIDATE"
    INVALID_STRM = "INVALID_STRM"
    FAILED = "FAILED"


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
        """转换为 JSON 可序列化字典。"""

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
        """按确定顺序返回去重后的 canonical。"""

        return tuple(dict.fromkeys(item.canonical for item in self.evidence))

    @property
    def canonical(self) -> str | None:
        """仅在结果唯一时返回 canonical。"""

        values = self.canonicals
        return values[0] if len(values) == 1 else None

    @property
    def conflicted(self) -> bool:
        """同一文本是否出现多个不同 canonical。"""

        return len(self.canonicals) > 1

    @property
    def multipart(self) -> bool:
        """是否含疑似文件分段后缀。"""

        return any(item.multipart for item in self.evidence)


@dataclass(frozen=True, slots=True)
class SourceItem:
    """scanner 只读采集的一条 STRM 源记录。"""

    profile: str
    root: Path
    path: Path
    relative_path: str
    local_stem: str
    parent_names: tuple[str, ...] = ()
    url_leaf: str = ""
    raw_url: str = field(default="", repr=False)
    content_sha256: str = ""
    size: int = 0
    mtime_ns: int = 0
    state: ItemState = ItemState.DISCOVERED
    error: str = ""


@dataclass(slots=True)
class PlanItem:
    """planner 的逐项事实记录，也是 manifest 的唯一数据来源。"""

    profile: str
    source_path: str
    relative_path: str
    local_stem: str
    url_leaf: str
    content_sha256: str
    state: ItemState
    canonical: str = ""
    target_name: str = ""
    suffix: str = ""
    confidence: float = 0.0
    evidence: list[CandidateEvidence] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    conflict_with: list[str] = field(default_factory=list)
    reason: str = ""
    size: int = 0
    mtime_ns: int = 0

    def to_record(self) -> dict[str, Any]:
        """返回不含 raw URL/STRM 内容的 JSON 可序列化记录。"""

        return {
            "state": self.state.value,
            "profile": self.profile,
            "source_path": self.source_path,
            "relative_path": self.relative_path,
            "local_stem": self.local_stem,
            "url_leaf": self.url_leaf,
            "canonical": self.canonical,
            "target_name": self.target_name,
            "suffix": self.suffix,
            "confidence": self.confidence,
            "content_sha256": self.content_sha256,
            "size": self.size,
            "mtime_ns": self.mtime_ns,
            "rules": list(self.rules),
            "observations": list(self.observations),
            "conflict_with": list(self.conflict_with),
            "reason": self.reason,
            "evidence": [item.to_record() for item in self.evidence],
        }

    @property
    def record(self) -> dict[str, Any]:
        """兼容属性式访问的 JSON 记录。"""

        return self.to_record()


@dataclass(slots=True)
class ScanStats:
    """一次目录扫描的计数。"""

    candidates: int = 0
    discovered: int = 0
    invalid: int = 0
    failed: int = 0
    filtered_old: int = 0
    filtered_unstable: int = 0
    limited: int = 0

    def to_record(self) -> dict[str, int]:
        """转换为摘要字典。"""

        return {
            "candidates": self.candidates,
            "discovered": self.discovered,
            "invalid": self.invalid,
            "failed": self.failed,
            "filtered_old": self.filtered_old,
            "filtered_unstable": self.filtered_unstable,
            "limited": self.limited,
        }


@dataclass(frozen=True, slots=True)
class ScanResult:
    """scanner 返回的发现项、问题项和统计。"""

    items: tuple[SourceItem, ...]
    issues: tuple[SourceItem, ...]
    stats: ScanStats

    def __iter__(
        self,
    ) -> Iterator[tuple[SourceItem, ...] | ScanStats]:
        """允许 ``items, issues, stats = scan(...)`` 解包。"""

        yield self.items
        yield self.issues
        yield self.stats


@dataclass(slots=True)
class RunSummary:
    """一轮规划/报告的汇总。"""

    run_id: str
    total: int
    counts: dict[str, int]
    scan_stats: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_items(
        cls,
        run_id: str,
        items: Iterable[PlanItem],
        scan_stats: ScanStats | None = None,
    ) -> "RunSummary":
        """由规划项稳定生成状态计数。"""

        item_list = list(items)
        counts = Counter(item.state.value for item in item_list)
        return cls(
            run_id=run_id,
            total=len(item_list),
            counts=dict(sorted(counts.items())),
            scan_stats=scan_stats.to_record() if scan_stats else {},
        )

    def to_record(self) -> dict[str, Any]:
        """转换为 JSON 可序列化字典。"""

        return {
            "run_id": self.run_id,
            "total": self.total,
            "counts": dict(self.counts),
            "scan_stats": dict(self.scan_stats),
        }
