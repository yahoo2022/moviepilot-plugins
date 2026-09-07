"""基于 A 层 SQLite 库存的只读整理规划器。

规划过程**没有任何网络访问**：候选、父目录证据、体积和 fingerprint 全部来自
`source_objects`，且只读取分页完整（`directories.complete=1`）的目录。
规则或 allowlist 变化后重新规划即可，不需要再访问 115。
"""
from __future__ import annotations

import hashlib
import json
import posixpath
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional, Sequence

from .db import InventoryDatabase
from .models import (
    ACTION_FOR_STATE,
    CandidateEvidence,
    PlanAction,
    PlanLifecycle,
    PlanRecord,
    PlanRunSummary,
    PlanState,
    ProfileConfig,
    SourceObject,
)
from .profiles import Fc2Profile, JavProfile, Profile
from .rules import (
    VIDEO_EXTENSIONS,
    GarbageRules,
    extension_of,
    is_safe_basename,
    safe_target_name,
)

# 支持改名的处理逻辑；其它 logic 只做垃圾判定。
RENAME_LOGICS = ("jav", "fc2")

# 默认允许保留并写进目标名的已知版本后缀（大写比较）。
DEFAULT_KNOWN_SUFFIXES = "C,U,UC,4K"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_known_suffixes(value: str) -> tuple[str, ...]:
    """解析「已知版本后缀」清单：换行/逗号分隔，统一大写。"""

    text = str(value or "")
    for separator in (",", "，", "\n", "\t", ";", "；", "|", " "):
        text = text.replace(separator, "\n")
    values: list[str] = []
    for line in text.splitlines():
        candidate = line.strip().strip("-_.").upper()
        if candidate and candidate.isalnum() and candidate not in values:
            values.append(candidate)
    return tuple(values)


def _stem(basename: str) -> str:
    extension = extension_of(basename)
    if not extension:
        return basename
    return basename[: -(len(extension) + 1)]


def _normalized_suffix(suffix: str) -> str:
    return str(suffix or "").strip().strip("-_. ").upper()


class PlanningService:
    """把 A 层库存翻译成 NOOP / RENAME / REMOVE / REVIEW 计划。"""

    def __init__(
        self,
        database: InventoryDatabase,
        profiles: Iterable[ProfileConfig],
        rules: GarbageRules,
        *,
        jav_prefixes: Optional[Sequence[str]] = None,
        known_suffixes: Sequence[str] = (),
        enable_rename: bool = True,
        enable_garbage: bool = True,
        clock: Callable[[], str] = utc_now,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.database = database
        self.profiles = [profile for profile in profiles if profile.enabled]
        self.rules = rules
        self.enable_rename = bool(enable_rename)
        self.enable_garbage = bool(enable_garbage)
        self.known_suffixes = frozenset(
            _normalized_suffix(value) for value in known_suffixes if str(value).strip()
        )
        self.clock = clock
        # 日志用注入回调而不是直接 import MoviePilot logger：本模块要保持可脱离 MP 迁移。
        self.log = log if log is not None else (lambda _message: None)
        self._engines: dict[str, Profile] = {
            "jav": JavProfile(list(jav_prefixes) if jav_prefixes else None),
            "fc2": Fc2Profile(),
        }

    @property
    def ruleset_version(self) -> str:
        """规则 + allowlist + 开关的联合版本，用于识别需要重算的计划。"""

        jav_engine = self._engines["jav"]
        allowlist = ",".join(sorted(getattr(jav_engine, "prefix_allowlist", ())))
        payload = "|".join(
            (
                self.rules.version,
                f"rename={int(self.enable_rename)}",
                f"garbage={int(self.enable_garbage)}",
                "suffixes=" + ",".join(sorted(self.known_suffixes)),
                f"jav={len(allowlist)}:{hash_text(allowlist)}",
            )
        )
        return hash_text(payload)

    # ---------------------------------------------------------------- 运行入口

    def run(self) -> PlanRunSummary:
        if not self.profiles:
            raise ValueError("没有启用任何 source profile")
        started_at = self.clock()
        summary = PlanRunSummary(
            run_id=uuid.uuid4().hex,
            started_at=started_at,
            ruleset_version=self.ruleset_version,
        )
        for profile in self.profiles:
            try:
                self.log(f"── profile {profile.name}({profile.logic}) 规划中：{profile.root}")
                result = self._run_profile(profile)
                summary.profiles.append(result)
                self.log(
                    f"── profile {profile.name} 完成："
                    f"计划 {result['planned']}，"
                    f"改名 {result['rename_ready']}，垃圾 {result['garbage_ready']}，"
                    f"新增 {result['new']}，变化 {result['changed']}，未变 {result['unchanged']}"
                )
            except Exception as error:  # 单个 profile 失败不影响其它 profile
                message = f"{profile.name}: {type(error).__name__}: {str(error)[:300]}"
                summary.errors.append(message)
                self.log(f"── profile {profile.name} 规划失败：{message}")
        summary.finished_at = self.clock()
        return summary

    def all_plans(self, profile: ProfileConfig) -> list[PlanRecord]:
        """只生成计划对象而不落库，供报告和离线验证使用。"""

        plans: list[PlanRecord] = []
        canonical_counter: Counter[str] = Counter()
        for parent_path, objects in self.database.iter_planning_batches(
            profile.name, profile.root
        ):
            plans.extend(self._plan_directory(profile, parent_path, objects, canonical_counter))
        self._annotate_shared_canonicals(plans, canonical_counter)
        return plans

    # ---------------------------------------------------------------- 单 profile

    def _run_profile(self, profile: ProfileConfig) -> dict:
        previous = self.database.previous_plan_index(profile.name)
        plans = self.all_plans(profile)
        ruleset_version = self.ruleset_version
        updated_at = self.clock()

        created = changed = unchanged = 0
        for plan in plans:
            plan.ruleset_version = ruleset_version
            plan.plan_id = self.database.plan_id(profile.name, plan.path)
            history = previous.get(plan.path)
            if history is None:
                created += 1
            elif history[0] != plan.fingerprint or history[1] != ruleset_version:
                changed += 1
            elif history[2] != plan.state.value:
                changed += 1
            else:
                unchanged += 1

        self.database.replace_plans(
            profile.name, (_plan_row(plan) for plan in plans), updated_at=updated_at
        )
        states = Counter(plan.state.value for plan in plans)
        return {
            "profile": profile.name,
            "logic": profile.logic,
            "root": profile.root,
            "planned": len(plans),
            "new": created,
            "changed": changed,
            "unchanged": unchanged,
            "removed": max(0, len(previous) - unchanged - changed),
            "states": dict(sorted(states.items())),
            "rename_ready": states.get(PlanState.RENAME_READY.value, 0),
            "garbage_ready": states.get(PlanState.GARBAGE_READY.value, 0),
        }

    # ---------------------------------------------------------------- 单个目录

    def _plan_directory(
        self,
        profile: ProfileConfig,
        parent_path: str,
        objects: list[SourceObject],
        canonical_counter: Counter[str],
    ) -> list[PlanRecord]:
        files = [item for item in objects if not item.is_dir]
        video_count = sum(
            1 for item in files if extension_of(item.basename) in VIDEO_EXTENSIONS
        )
        occupied = {item.basename.casefold() for item in objects}
        parent_names = _parent_names(parent_path)

        plans: list[PlanRecord] = []
        for item in objects:
            plan = self._plan_object(
                profile,
                item,
                parent_names=parent_names,
                video_count=video_count,
                canonical_counter=canonical_counter,
            )
            plans.append(plan)

        self._resolve_target_collisions(plans, occupied)
        for plan in plans:
            _apply_self_check(plan, profile.root)
        return plans

    def _plan_object(
        self,
        profile: ProfileConfig,
        item: SourceObject,
        *,
        parent_names: tuple[str, ...],
        video_count: int,
        canonical_counter: Counter[str],
    ) -> PlanRecord:
        extension = extension_of(item.basename)
        plan = PlanRecord(
            profile=profile.name,
            logic=profile.logic,
            source_key=item.object_id or f"{profile.name}:{item.path}",
            path=item.path,
            parent_path=item.parent_path,
            basename=item.basename,
            fingerprint=item.fingerprint,
            size=item.size,
            modified=item.modified,
            extension=extension,
        )

        if item.is_dir:
            plan.state = PlanState.UNSUPPORTED
            plan.action = PlanAction.REVIEW
            plan.reason = "目录不参与自动改名或删除"
            return plan

        parse = self._parse_object(profile.logic, item, parent_names)
        plan.rules = parse["rules"]
        plan.observations = parse["observations"]
        if parse["canonical"]:
            canonical_counter[parse["canonical"]] += 1

        # 垃圾判定：只有**文件名自身**解析出番号才享受铁律保护。
        # 父目录番号不能保护文件：JAV 源常见布局是「一个目录一部片」，目录里混着
        # sample/预告/广告小片段。如果把父目录番号也算作保护，这些垃圾会全部免疫，
        # 恰好在最需要清理的布局下失效。大体积视频仍由体积铁律单独保护。
        identified_by_basename = bool(parse["canonical"]) and not parse["only_from_parent"]
        if self.enable_garbage:
            verdict = self.rules.classify(
                basename=item.basename,
                parent_path=item.parent_path,
                size=item.size,
                is_dir=False,
                has_canonical=identified_by_basename,
            )
            plan.observations = list(
                dict.fromkeys((*plan.observations, *verdict.observations))
            )
            if verdict.garbage:
                plan.state = PlanState.GARBAGE_READY
                plan.action = PlanAction.REMOVE
                plan.reason = verdict.reason
                plan.confidence = 1.0
                return plan
            if verdict.protected:
                plan.observations = list(dict.fromkeys((*plan.observations, verdict.reason)))

        if not self.enable_rename:
            plan.state = PlanState.NOOP
            plan.action = PlanAction.NOOP
            plan.reason = "改名已关闭，仅执行垃圾判定"
            return plan
        if profile.logic not in RENAME_LOGICS:
            plan.state = PlanState.UNSUPPORTED
            plan.action = PlanAction.REVIEW
            plan.reason = f"logic={profile.logic} 暂无改名规则"
            return plan
        if not extension:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = "无扩展名，不改名"
            return plan
        if extension not in VIDEO_EXTENSIONS:
            plan.state = PlanState.NOOP
            plan.action = PlanAction.NOOP
            plan.reason = "非视频文件，保留不改名"
            return plan

        return self._decide_rename(plan, parse, video_count=video_count, extension=extension)

    def _decide_rename(
        self, plan: PlanRecord, parse: dict, *, video_count: int, extension: str
    ) -> PlanRecord:
        if parse["conflicted"]:
            plan.state = PlanState.CONFLICT
            plan.action = PlanAction.REVIEW
            plan.reason = "同一对象的候选证据得到多个 canonical"
            plan.conflict_with = list(parse["canonicals"])
            return plan
        canonical = parse["canonical"]
        if not canonical:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = "未取得 allowlist/明确标记支持的可靠番号"
            return plan

        plan.canonical = canonical
        plan.confidence = parse["confidence"]
        if parse["multipart"]:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = f"检测到疑似分段后缀 {parse['suffix']}，必须人工审核"
            return plan
        suffix = _normalized_suffix(parse["suffix"])
        if suffix and suffix not in self.known_suffixes:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = f"未知后缀 {parse['suffix']}，必须人工审核"
            return plan
        if parse["only_from_parent"] and video_count != 1:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = f"番号只来自父目录，但该目录有 {video_count} 个视频"
            return plan

        stem = f"{canonical}-{suffix}" if suffix else canonical
        target = safe_target_name(stem, extension)
        if not target:
            plan.state = PlanState.REVIEW_REQUIRED
            plan.action = PlanAction.REVIEW
            plan.reason = "目标名不安全"
            return plan
        if target.casefold() == plan.basename.casefold():
            plan.state = PlanState.NOOP
            plan.action = PlanAction.NOOP
            plan.target_name = ""
            plan.reason = "源名已规范"
            return plan
        plan.target_name = target
        plan.state = PlanState.RENAME_READY
        plan.action = PlanAction.RENAME
        plan.reason = "单一高置信番号且无未知后缀"
        return plan

    # ---------------------------------------------------------------- 证据解析

    def _parse_object(
        self, logic: str, item: SourceObject, parent_names: tuple[str, ...]
    ) -> dict:
        engine = self._engines.get(logic)
        empty = {
            "canonical": "",
            "canonicals": (),
            "conflicted": False,
            "confidence": 0.0,
            "suffix": "",
            "multipart": False,
            "rules": [],
            "observations": [],
            "only_from_parent": False,
        }
        if engine is None:
            return empty

        texts: list[tuple[str, str]] = [("basename", _stem(item.basename))]
        texts.extend(
            (f"parent_{index}", name) for index, name in enumerate(parent_names, start=1)
        )
        evidence: list[CandidateEvidence] = []
        observations: list[str] = []
        for source, text in texts:
            result = engine.parse(text, source)
            evidence.extend(result.evidence)
            observations.extend(result.observations)
        if not evidence:
            return {**empty, "observations": list(dict.fromkeys(observations))}

        canonicals = tuple(dict.fromkeys(value.canonical for value in evidence))
        basename_evidence = [value for value in evidence if value.source == "basename"]
        # 只有 basename 证据参与后缀判定：父目录名里的版本后缀不代表文件本身。
        primary = basename_evidence or evidence
        suffix = next((value.suffix for value in primary if value.suffix), "")
        return {
            "canonical": canonicals[0] if len(canonicals) == 1 else "",
            "canonicals": canonicals,
            "conflicted": len(canonicals) > 1,
            "confidence": max(value.confidence for value in evidence),
            "suffix": suffix,
            "multipart": any(value.multipart for value in primary),
            "rules": list(dict.fromkeys(value.rule for value in evidence)),
            "observations": list(
                dict.fromkeys(
                    (
                        *observations,
                        *(note for value in evidence for note in value.observations),
                    )
                )
            ),
            "only_from_parent": not basename_evidence,
        }

    # ---------------------------------------------------------------- 冲突裁决

    @staticmethod
    def _resolve_target_collisions(plans: list[PlanRecord], occupied: set[str]) -> None:
        """同目录内目标名冲突一律降级为 CONFLICT，绝不覆盖已有对象。"""

        planned: dict[str, list[PlanRecord]] = {}
        for plan in plans:
            if plan.action is not PlanAction.RENAME or not plan.target_name:
                continue
            key = plan.target_name.casefold()
            if key in occupied and key != plan.basename.casefold():
                plan.state = PlanState.CONFLICT
                plan.action = PlanAction.REVIEW
                plan.reason = f"目标名已被同目录对象占用: {plan.target_name}"
                plan.conflict_with = [plan.target_name]
                plan.target_name = ""
                continue
            planned.setdefault(key, []).append(plan)

        for grouped in planned.values():
            if len(grouped) < 2:
                continue
            paths = [plan.path for plan in grouped]
            for plan in grouped:
                plan.state = PlanState.CONFLICT
                plan.action = PlanAction.REVIEW
                plan.reason = "同目录多个对象改名到同一目标"
                plan.conflict_with = [value for value in paths if value != plan.path]
                plan.target_name = ""

    @staticmethod
    def _annotate_shared_canonicals(
        plans: list[PlanRecord], canonical_counter: Counter[str]
    ) -> None:
        """同一番号出现在多个对象上时只加观察标签，绝不自动判重删除。

        没有媒体内容 hash，无法证明两个同番号对象是重复内容。
        """

        for plan in plans:
            if plan.canonical and canonical_counter.get(plan.canonical, 0) > 1:
                plan.observations = list(
                    dict.fromkeys((*plan.observations, "canonical_seen_elsewhere"))
                )


def _parent_names(parent_path: str, depth: int = 3) -> tuple[str, ...]:
    """由近到远返回父目录名，最多 depth 层。"""

    parts = [part for part in str(parent_path or "").split("/") if part]
    return tuple(reversed(parts[-depth:])) if parts else ()


def _plan_row(plan: PlanRecord) -> dict:
    """把计划转换为 SQLite 行；payload 保存完整可审计记录。"""

    return {
        "plan_id": plan.plan_id,
        "profile": plan.profile,
        "logic": plan.logic,
        "source_key": plan.source_key,
        "path": plan.path,
        "parent_path": plan.parent_path,
        "basename": plan.basename,
        "extension": plan.extension,
        "size": int(plan.size or 0),
        "modified": plan.modified,
        "source_fingerprint": plan.fingerprint,
        "ruleset_version": plan.ruleset_version,
        "state": plan.state.value,
        "action": plan.action.value,
        "lifecycle": PlanLifecycle.PLANNED.value,
        "canonical": plan.canonical,
        "target_name": plan.target_name,
        "confidence": float(plan.confidence or 0.0),
        "reason": plan.reason[:500],
        "payload_json": json.dumps(plan.to_record(), ensure_ascii=False, sort_keys=True),
    }


def hash_text(value: str) -> str:
    """短摘要工具，仅用于规则版本标识。"""

    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def _self_check(plan: PlanRecord, root: str) -> tuple[bool, str]:
    """READY 计划的纯本地自检。

    这一步不是「执行前防线」——本版本没有执行——而是让不合法的计划**在报告里就
    暴露出来**：越界路径、路径与父目录不一致、不安全的目标名都不该出现在
    「可执行」清单里。将来 hub 执行前仍必须再做一次 live CAS 复检。
    """

    path = plan.path
    normalized_root = str(root or "").rstrip("/")
    if not normalized_root or not path.startswith(f"{normalized_root}/"):
        return False, "路径不在 profile allowlist 根内"
    if ".." in path.split("/") or "\\" in path or "\x00" in path:
        return False, "路径包含非法段"
    if not plan.parent_path or posixpath.dirname(path) != plan.parent_path.rstrip("/"):
        return False, "父目录与路径不一致"
    if not is_safe_basename(plan.basename):
        return False, "源 basename 不安全"
    if plan.action is PlanAction.RENAME:
        if not is_safe_basename(plan.target_name):
            return False, "目标 basename 不安全"
        if plan.target_name.casefold() == plan.basename.casefold():
            return False, "目标名与源名相同"
    elif plan.action is PlanAction.REMOVE and plan.target_name:
        return False, "删除计划不应携带目标名"
    return True, ""


def _apply_self_check(plan: PlanRecord, root: str) -> None:
    """自检不通过的 READY 计划降级为 REVIEW_REQUIRED，保留原因便于排查。"""

    if plan.state not in {PlanState.RENAME_READY, PlanState.GARBAGE_READY}:
        return
    passed, message = _self_check(plan, root)
    if passed:
        return
    plan.state = PlanState.REVIEW_REQUIRED
    plan.action = PlanAction.REVIEW
    plan.target_name = ""
    plan.reason = f"自检不通过: {message}"


__all__ = [
    "ACTION_FOR_STATE",
    "DEFAULT_KNOWN_SUFFIXES",
    "PlanningService",
    "RENAME_LOGICS",
    "parse_known_suffixes",
    "utc_now",
]
