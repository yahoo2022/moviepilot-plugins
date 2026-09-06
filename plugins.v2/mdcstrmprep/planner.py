"""基于多来源证据的保守规划器。"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

try:
    from .models import CandidateEvidence, ItemState, PlanItem, SourceItem
    from .profiles import Fc2Profile, JavProfile, Profile
except ImportError:  # 允许把核心目录直接加入 sys.path 调试
    from models import CandidateEvidence, ItemState, PlanItem, SourceItem
    from profiles import Fc2Profile, JavProfile, Profile


def build_plan(
    items: Iterable[SourceItem],
    *,
    profiles: Mapping[str, Profile] | None = None,
    jav_prefix_allowlist: Iterable[str] | None = None,
) -> list[PlanItem]:
    """对扫描项取证、裁决并执行全局 hash 判重。

    本函数没有任何文件写操作。相同 canonical 的不同 hash 会使该组所有
    ``READY`` 候选变为 ``CONFLICT``；相同 hash 只把确定排序后的后续项标重。
    """

    engines: dict[str, Profile]
    if profiles is None:
        engines = {
            "fc2": Fc2Profile(),
            "jav": JavProfile(jav_prefix_allowlist),
        }
    else:
        engines = {name.casefold(): engine for name, engine in profiles.items()}

    plans = [_plan_one(item, engines) for item in sorted(items, key=_source_sort_key)]
    _apply_global_duplicates(plans)
    return plans


def plan_items(*args: Any, **kwargs: Any) -> list[PlanItem]:
    """``build_plan`` 的兼容别名。"""

    return build_plan(*args, **kwargs)


def _plan_one(item: SourceItem, engines: Mapping[str, Profile]) -> PlanItem:
    """规划单个源项，不在此阶段做跨项判重。"""

    base = PlanItem(
        profile=item.profile.casefold(),
        source_path=item.path.as_posix(),
        relative_path=item.relative_path,
        local_stem=item.local_stem,
        url_leaf=item.url_leaf,
        content_sha256=item.content_sha256,
        state=item.state,
        reason=item.error,
        size=item.size,
        mtime_ns=item.mtime_ns,
    )
    if item.state in {ItemState.INVALID_STRM, ItemState.FAILED}:
        return base

    source_profile = item.profile.casefold()
    engine = engines.get(source_profile)
    if engine is None:
        base.state = ItemState.FAILED
        base.reason = f"未知 source profile: {item.profile}"
        return base

    texts = _candidate_texts(item)
    evidence: list[CandidateEvidence] = []
    observations: list[str] = []
    for source, text in texts:
        result = engine.parse(text, source)
        evidence.extend(result.evidence)
        observations.extend(result.observations)
    base.evidence = evidence
    base.rules = list(dict.fromkeys(value.rule for value in evidence))
    base.observations = list(
        dict.fromkeys((*observations, *(note for value in evidence for note in value.observations)))
    )

    canonicals = list(dict.fromkeys(value.canonical for value in evidence))
    local_ids = {value.canonical for value in evidence if value.source == "local_stem"}
    url_ids = {value.canonical for value in evidence if value.source == "url_leaf"}
    if len(canonicals) > 1 or (local_ids and url_ids and local_ids != url_ids):
        base.state = ItemState.CONFLICT
        base.reason = "同一 profile 的候选证据得到多个 canonical"
        base.conflict_with = canonicals
        return base

    if not canonicals:
        mismatch = _find_mismatch(source_profile, texts, engines)
        if mismatch:
            base.state = ItemState.PROFILE_MISMATCH
            base.canonical = mismatch[0].canonical
            base.evidence = mismatch
            base.rules = list(dict.fromkeys(value.rule for value in mismatch))
            base.observations = list(
                dict.fromkeys((*base.observations, *(note for value in mismatch for note in value.observations)))
            )
            base.confidence = max(value.confidence for value in mismatch)
            base.reason = f"{source_profile} 源仅匹配 {mismatch[0].profile} profile"
            return base
        base.state = ItemState.REVIEW_REQUIRED
        base.reason = "未取得 allowlist/明确标记支持的可靠番号"
        return base

    base.canonical = canonicals[0]
    base.confidence = max(value.confidence for value in evidence)
    suffixes = list(dict.fromkeys(value.suffix for value in evidence if value.suffix))
    base.suffix = " | ".join(suffixes)
    if any(value.multipart for value in evidence):
        base.state = ItemState.REVIEW_REQUIRED
        base.reason = "检测到疑似分段后缀，必须人工审核"
        return base
    if suffixes:
        base.state = ItemState.REVIEW_REQUIRED
        base.reason = "检测到未知后缀，必须人工审核"
        return base
    base.state = ItemState.READY
    base.target_name = f"{base.canonical}.strm"
    base.reason = "单一高置信番号且无未知后缀"
    return base


def _candidate_texts(item: SourceItem) -> list[tuple[str, str]]:
    """按本地、URL、近到远父目录的确定顺序形成证据输入。"""

    values: list[tuple[str, str]] = [("local_stem", item.local_stem)]
    if item.url_leaf:
        values.append(("url_leaf", Path(item.url_leaf).stem))
    values.extend((f"parent_{index}", name) for index, name in enumerate(item.parent_names[:3], 1))
    return values


def _find_mismatch(
    source_profile: str,
    texts: list[tuple[str, str]],
    engines: Mapping[str, Profile],
) -> list[CandidateEvidence]:
    """仅当源 profile 无结果时，寻找另一 profile 的唯一可靠结果。"""

    expected_other = "jav" if source_profile == "fc2" else "fc2" if source_profile == "jav" else ""
    other = engines.get(expected_other)
    if other is None:
        return []
    evidence: list[CandidateEvidence] = []
    for source, text in texts:
        evidence.extend(other.parse(text, source).evidence)
    canonicals = {value.canonical for value in evidence}
    if len(canonicals) != 1 or any(value.suffix for value in evidence):
        return []
    return evidence


def _apply_global_duplicates(plans: list[PlanItem]) -> None:
    """仅对可投递项执行 canonical + 原始内容 hash 的全局裁决。"""

    groups: dict[tuple[str, str], list[PlanItem]] = defaultdict(list)
    for plan in plans:
        if plan.state is ItemState.READY and plan.canonical:
            groups[(plan.profile, plan.canonical)].append(plan)

    for grouped in groups.values():
        hashes = {plan.content_sha256 for plan in grouped}
        if len(hashes) > 1:
            paths = [plan.relative_path for plan in grouped]
            for plan in grouped:
                plan.state = ItemState.CONFLICT
                plan.target_name = ""
                plan.reason = "相同 canonical 对应不同 STRM 内容 SHA-256"
                plan.conflict_with = [path for path in paths if path != plan.relative_path]
            continue
        for duplicate in grouped[1:]:
            duplicate.state = ItemState.DUPLICATE
            duplicate.target_name = ""
            duplicate.reason = "相同 canonical 且 STRM 内容 SHA-256 相同"
            duplicate.conflict_with = [grouped[0].relative_path]


def _source_sort_key(item: SourceItem) -> tuple[str, str]:
    """跨平台确定性排序键。"""

    return item.profile.casefold(), item.relative_path.casefold()
