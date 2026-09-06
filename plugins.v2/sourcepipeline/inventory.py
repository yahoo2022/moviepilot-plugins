"""115 原生源的低频、可续跑库存扫描器。"""
from __future__ import annotations

import hashlib
import posixpath
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

from .db import InventoryDatabase
from .models import InventoryRunSummary, ProfileConfig, ProfileRunSummary, SourceObject
from .openlist_read import ReadOnlyOpenListClient, RequestBudgetExceeded, validate_absolute_path

LIVE_CONFIRMATION = "READ_115_SOURCE_SLOWLY"
_VALID_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_-")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _normalize_id(value: object, label: str) -> str:
    normalized = str(value or "").strip().casefold()
    if not normalized or any(character not in _VALID_ID_CHARS for character in normalized):
        raise ValueError(f"{label}只能包含小写字母、数字、下划线或连字符")
    return normalized


def normalize_profile(profile: ProfileConfig) -> ProfileConfig:
    name = _normalize_id(profile.name, "profile 名称")
    logic = _normalize_id(profile.logic or name, "profile logic")
    root = validate_absolute_path(profile.root)
    if root == "/":
        raise ValueError("profile 根目录不能是 OpenList 根路径")
    return ProfileConfig(name=name, root=root, logic=logic, enabled=bool(profile.enabled))


def parse_profiles(value: str, *, max_profiles: int = 20) -> list[ProfileConfig]:
    """解析页面配置。

    推荐三列 ``name|logic|/absolute/path``；兼容旧两列 ``name|/absolute/path``，
    旧格式自动令 ``logic=name``。
    """

    profiles: list[ProfileConfig] = []
    seen_names: set[str] = set()
    seen_roots: set[str] = set()
    for line_number, raw_line in enumerate(str(value or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) == 2:
            raw_name, raw_root = parts
            raw_logic = raw_name
        elif len(parts) == 3:
            raw_name, raw_logic, raw_root = parts
        else:
            raise ValueError(
                f"source profile 第 {line_number} 行必须是 name|logic|/absolute/path"
            )
        profile = normalize_profile(
            ProfileConfig(name=raw_name, logic=raw_logic, root=raw_root)
        )
        if profile.name in seen_names:
            raise ValueError(f"source profile 名称重复: {profile.name}")
        if profile.root in seen_roots:
            raise ValueError(f"source profile 根目录重复: {profile.root}")
        seen_names.add(profile.name)
        seen_roots.add(profile.root)
        profiles.append(profile)
        if len(profiles) > max_profiles:
            raise ValueError(f"source profile 最多允许 {max_profiles} 个")
    return profiles


def _safe_basename(raw_name: object) -> str:
    name = str(raw_name or "")
    if not name or name in {".", ".."} or "/" in name or "\\" in name or "\x00" in name:
        raise ValueError("OpenList 返回了不安全的目录项名称")
    return name


def _as_non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _hours_before(timestamp: str, hours: int) -> str:
    current = datetime.fromisoformat(timestamp)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current - timedelta(hours=max(1, int(hours)))).isoformat(timespec="seconds")


def object_from_entry(profile: str, parent_path: str, entry: dict) -> SourceObject:
    """提取稳定字段并计算元数据指纹，不保存原始 JSON。"""

    basename = _safe_basename(entry.get("name"))
    path = posixpath.join(parent_path.rstrip("/") or "/", basename)
    is_dir = bool(entry.get("is_dir", False))
    size = _as_non_negative_int(entry.get("size"))
    modified = str(entry.get("modified") or "")[:100]
    object_id = str(
        entry.get("id")
        or entry.get("object_id")
        or entry.get("provider_id")
        or ""
    )[:500]
    identity = object_id or f"{path}\0{int(is_dir)}\0{size}\0{modified}"
    fingerprint = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return SourceObject(
        profile=profile,
        path=path,
        parent_path=parent_path,
        basename=basename,
        is_dir=is_dir,
        size=size,
        modified=modified,
        object_id=object_id,
        fingerprint=fingerprint,
    )


class InventoryService:
    """按来源实例独立预算，以持久广度优先队列发布完整目录。"""

    def __init__(
        self,
        database: InventoryDatabase,
        profiles: Iterable[ProfileConfig],
        client_factory: Callable[[ProfileConfig], ReadOnlyOpenListClient],
        *,
        root_refresh_hours: int = 24,
        directory_refresh_hours: int = 168,
        clock: Callable[[], str] = utc_now,
    ):
        self.database = database
        self.profiles = [normalize_profile(profile) for profile in profiles if profile.enabled]
        self.client_factory = client_factory
        self.root_refresh_hours = max(1, int(root_refresh_hours))
        self.directory_refresh_hours = max(1, int(directory_refresh_hours))
        self.clock = clock

    def run(self, mode: str = "cache", *, confirmation: str = "") -> InventoryRunSummary:
        mode = str(mode).casefold()
        if mode not in {"cache", "live"}:
            raise ValueError("扫描模式只能是 cache 或 live")
        if mode == "live" and confirmation != LIVE_CONFIRMATION:
            raise PermissionError(f"live 扫描必须输入确认短语 {LIVE_CONFIRMATION}")
        if not self.profiles:
            raise ValueError("没有启用任何 source profile")

        started_at = self.clock()
        run = InventoryRunSummary(
            run_id=uuid.uuid4().hex,
            mode=mode,
            started_at=started_at,
        )
        self.database.sync_profiles(self.profiles, updated_at=started_at)
        for profile in self.profiles:
            run.profiles.append(self._run_profile(run.run_id, profile, mode))
        run.finished_at = self.clock()
        return run

    def _run_profile(self, run_id: str, profile: ProfileConfig, mode: str) -> ProfileRunSummary:
        generation = self.database.next_generation()
        started_at = self.clock()
        root_cutoff = _hours_before(started_at, self.root_refresh_hours)
        directory_cutoff = _hours_before(started_at, self.directory_refresh_hours)
        summary = ProfileRunSummary(
            profile=profile.name,
            logic=profile.logic,
            root=profile.root,
            mode=mode,
            generation=generation,
        )
        self.database.ensure_root(profile.name, profile.root)
        self.database.begin_run(
            run_id,
            profile,
            mode,
            generation,
            started_at,
        )

        client: ReadOnlyOpenListClient | None = None
        refresh = mode == "live"
        try:
            client = self.client_factory(profile)
            summary.request_budget = client.max_requests
            while client.remaining_requests > 0:
                directory = self.database.next_directory(
                    profile.name,
                    profile.root,
                    run_id,
                    mode=mode,
                    root_cutoff=root_cutoff,
                    directory_cutoff=directory_cutoff,
                )
                if directory is None:
                    summary.stop_reason = "queue_empty"
                    break

                summary.directories_attempted += 1
                attempted_at = self.clock()
                try:
                    listing = client.list_directory(directory, refresh=refresh)
                    objects = [
                        object_from_entry(profile.name, directory, entry)
                        for entry in listing.items
                    ]
                    self.database.publish_directory(
                        profile.name,
                        directory,
                        objects,
                        generation=generation,
                        mode=mode,
                        scanned_at=attempted_at,
                        run_id=run_id,
                    )
                    summary.directories_completed += 1
                    summary.objects_seen += len(objects)
                except Exception as error:
                    message = f"{directory}: {str(error)[:500]}"
                    self.database.mark_directory_partial(
                        profile.name,
                        directory,
                        attempted_at=attempted_at,
                        error=message,
                        run_id=run_id,
                    )
                    summary.directories_partial += 1
                    summary.errors.append(message)
                    if isinstance(error, RequestBudgetExceeded):
                        summary.stop_reason = "request_budget"
                        break

            if not summary.stop_reason:
                summary.stop_reason = "request_budget" if client.remaining_requests == 0 else "batch_complete"
        except Exception as error:
            summary.directories_partial += 1
            summary.errors.append(f"客户端初始化失败: {type(error).__name__}")
            summary.stop_reason = "client_error"
        finally:
            summary.requests = client.request_count if client else 0
            summary.requests_remaining = client.remaining_requests if client else 0
            summary.retries = client.retry_count if client else 0
            summary.batch_pauses = getattr(client, "batch_pause_count", 0) if client else 0
            summary.throttle_sleep_seconds = round(
                getattr(client, "throttle_sleep_seconds", 0.0) if client else 0.0,
                3,
            )
            pending = self.database.pending_summary(
                profile.name,
                profile.root,
                mode=mode,
                root_cutoff=root_cutoff,
                directory_cutoff=directory_cutoff,
            )
            summary.queue_remaining = pending["count"]
            summary.frontier_depth = pending["frontier_depth"]
            summary.converged = summary.queue_remaining == 0
            summary.complete = summary.directories_partial == 0 and summary.converged
            if summary.stop_reason != "client_error":
                if summary.converged:
                    summary.stop_reason = "queue_empty"
                elif summary.directories_partial > 0:
                    summary.stop_reason = (
                        "request_budget"
                        if summary.requests_remaining == 0
                        else "errors_deferred"
                    )
                elif summary.requests_remaining == 0:
                    summary.stop_reason = "request_budget"
                else:
                    summary.stop_reason = "batch_complete"
            self.database.finish_run(
                run_id,
                profile.name,
                finished_at=self.clock(),
                complete=summary.complete,
                converged=summary.converged,
                request_budget=summary.request_budget,
                stop_reason=summary.stop_reason,
                requests=summary.requests,
                retries=summary.retries,
                directories_completed=summary.directories_completed,
                directories_partial=summary.directories_partial,
                objects_seen=summary.objects_seen,
                errors=summary.errors,
            )
        return summary
