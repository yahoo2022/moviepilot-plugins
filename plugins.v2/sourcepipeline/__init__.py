"""SourcePipeline - 以 115 原生目录为事实源的 MoviePilot V2 插件。

v0.3.0 提供两件事，全部只读：

1. **A 层库存**：固定 ``POST /api/fs/list``，按请求预算广度优先建立 115 原生目录的
   SQLite 缓存，来源实例（name）与处理逻辑（logic）分离。
2. **整理规划**：在缓存上跑垃圾判定和番号改名规则，产出「垃圾清单 + 改名预演」
   报告（JSONL / TSV / summary），供人工审阅。

**本插件没有任何 115 写入能力**：没有 rename、remove、move、upload，也没有 STRM
生成或本地文件写入。唯一的网络 endpoint 是 ``/api/fs/list``。改名与删除的执行交给
部署在服务器上的独立服务承担，写客户端与不可变清单代码放在仓库根的 ``hub-seed/``，
原因见 ``hub-seed/README.md``。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

from .db import InventoryDatabase
from .inventory import LIVE_CONFIRMATION, InventoryService, parse_profiles
from .models import PlanState, ProfileConfig
from .openlist_read import ReadOnlyOpenListClient
from .planner import DEFAULT_KNOWN_SUFFIXES, PlanningService, parse_known_suffixes
from .profiles import parse_prefix_allowlist
from .report import write_plan_report
from .rules import (
    DEFAULT_JUNK_DIRECTORIES,
    DEFAULT_JUNK_EXTENSIONS,
    DEFAULT_JUNK_KEYWORDS,
    DEFAULT_KEEP_EXTENSIONS,
    MEGABYTE,
    GarbageRules,
    describe,
)

# 报告可以只收「需要人看的」计划，避免海量 NOOP 淹没 TSV。
ACTIONABLE_STATES = (
    PlanState.GARBAGE_READY.value,
    PlanState.RENAME_READY.value,
    PlanState.CONFLICT.value,
    PlanState.REVIEW_REQUIRED.value,
)


class SourcePipeline(_PluginBase):
    """115 原生源只读库存 + 整理规划；代码级不具备任何写能力。"""

    plugin_name = "115 源流水线"
    plugin_desc = "只读读取 115 原生目录建 SQLite 缓存，并在缓存上产出垃圾清单与改名预演报告（无任何写入）"
    plugin_icon = "workflow.png"
    plugin_version = "0.3.1"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "sourcepipeline_"
    plugin_order = 24
    auth_level = 1

    _enabled: bool = False
    _notify: bool = True
    _cache_once: bool = False
    _live_once: bool = False
    _live_confirmation: str = ""
    _cron: str = ""
    _openlist_url: str = ""
    _openlist_token: str = ""
    _timeout_seconds: int = 30
    _retries: int = 1
    _min_interval_seconds: float = 1.5
    _max_interval_seconds: float = 3.0
    _batch_requests: int = 20
    _batch_pause_min_seconds: float = 30.0
    _batch_pause_max_seconds: float = 90.0
    _max_requests_per_profile: int = 60
    _root_refresh_hours: int = 24
    _directory_refresh_hours: int = 168
    _per_page: int = 1000
    _max_pages_per_directory: int = 20
    _source_profiles: str = ""

    # ---- 规划（阶段2，纯本地、无网络） ----
    _plan_once: bool = False
    _plan_cron: str = ""
    _enable_garbage: bool = True
    _enable_rename: bool = True
    _garbage_mode: str = "blacklist"
    _keep_extensions: str = ""
    _junk_extensions: str = ""
    _junk_keywords: str = ""
    _junk_directories: str = ""
    _protect_min_mb: int = 400
    _small_video_mb: int = 0
    _no_extension_is_junk: bool = False
    _jav_prefixes: str = ""
    _known_suffixes: str = DEFAULT_KNOWN_SUFFIXES
    _report_scope: str = "actionable"
    _keep_reports: int = 20
    _container: str = "moviepilot-v2"

    _scheduler: Optional[BackgroundScheduler] = None
    _run_lock = threading.Lock()

    @staticmethod
    def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value) if value not in (None, "") else default
        except (TypeError, ValueError):
            parsed = default
        return min(maximum, max(minimum, parsed))

    @staticmethod
    def _as_float(value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value) if value not in (None, "") else default
        except (TypeError, ValueError):
            parsed = default
        return min(maximum, max(minimum, parsed))

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", True))
        self._cache_once = bool(config.get("cache_once", False))
        self._live_once = bool(config.get("live_once", False))
        self._live_confirmation = str(config.get("live_confirmation") or "").strip()
        self._cron = str(config.get("cron") or "").strip()
        self._openlist_url = str(config.get("openlist_url") or "").strip()
        self._openlist_token = str(config.get("openlist_token") or "").strip()
        self._timeout_seconds = self._as_int(config.get("timeout_seconds"), 30, 5, 120)
        self._retries = self._as_int(config.get("retries"), 1, 0, 2)
        self._min_interval_seconds = self._as_float(
            config.get("min_interval_seconds"), 1.5, 1.5, 30.0
        )
        self._max_interval_seconds = max(
            self._min_interval_seconds,
            self._as_float(config.get("max_interval_seconds"), 3.0, 1.5, 60.0),
        )
        self._batch_requests = self._as_int(
            config.get("batch_requests"), 20, 5, 100
        )
        self._batch_pause_min_seconds = self._as_float(
            config.get("batch_pause_min_seconds"), 30.0, 10.0, 600.0
        )
        self._batch_pause_max_seconds = max(
            self._batch_pause_min_seconds,
            self._as_float(
                config.get("batch_pause_max_seconds"), 90.0, 10.0, 900.0
            ),
        )
        self._max_requests_per_profile = self._as_int(
            config.get("max_requests_per_profile"), 60, 1, 100
        )
        self._root_refresh_hours = self._as_int(
            config.get("root_refresh_hours"), 24, 1, 720
        )
        self._directory_refresh_hours = self._as_int(
            config.get("directory_refresh_hours"), 168, 6, 8760
        )
        self._per_page = self._as_int(config.get("per_page"), 1000, 1, 1000)
        self._max_pages_per_directory = self._as_int(
            config.get("max_pages_per_directory"), 20, 1, 100
        )
        self._source_profiles = str(config.get("source_profiles") or "").strip()

        self._plan_once = bool(config.get("plan_once", False))
        self._plan_cron = str(config.get("plan_cron") or "").strip()
        self._enable_garbage = bool(config.get("enable_garbage", True))
        self._enable_rename = bool(config.get("enable_rename", True))
        self._garbage_mode = str(config.get("garbage_mode") or "blacklist").strip()
        self._keep_extensions = str(config.get("keep_extensions") or "").strip()
        self._junk_extensions = str(config.get("junk_extensions") or "").strip()
        self._junk_keywords = str(config.get("junk_keywords") or "").strip()
        self._junk_directories = str(config.get("junk_directories") or "").strip()
        self._protect_min_mb = self._as_int(config.get("protect_min_mb"), 400, 0, 1024 * 64)
        self._small_video_mb = self._as_int(config.get("small_video_mb"), 0, 0, 1024 * 8)
        self._no_extension_is_junk = bool(config.get("no_extension_is_junk", False))
        self._jav_prefixes = str(config.get("jav_prefixes") or "").strip()
        self._known_suffixes = str(
            config.get("known_suffixes")
            if config.get("known_suffixes") is not None
            else DEFAULT_KNOWN_SUFFIXES
        ).strip()
        self._report_scope = str(config.get("report_scope") or "actionable").strip()
        self._keep_reports = self._as_int(config.get("keep_reports"), 20, 0, 200)
        self._container = str(config.get("container") or "moviepilot-v2").strip()

        # 初始化 schema、同步 profile registry、校验规则都只操作本地内存/SQLite，
        # 不产生任何网络访问；配置错误必须在触发前 fail-closed。
        database = self._database()
        try:
            database.sync_profiles(
                self._profiles(),
                updated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            self._build_rules()
            parse_prefix_allowlist(self._jav_prefixes)
        except ValueError as error:
            message = f"配置错误: {str(error)[:500]}"
            self._cache_once = False
            self._live_once = False
            self._plan_once = False
            self._live_confirmation = ""
            self.update_config(self._current_config())
            logger.error(f"[{self.plugin_name}] {message}")
            self._set_status(state="config_error", mode="", message=message)
            return

        pending = [
            name
            for name, flag in (
                ("cache", self._cache_once),
                ("live", self._live_once),
                ("plan", self._plan_once),
            )
            if flag
        ]
        if not pending:
            return

        confirmation = self._live_confirmation
        mode = pending[0]
        error = ""
        if len(pending) > 1:
            error = "一次只能触发一个动作（cache / live / plan），本次未执行"
        elif mode == "live" and confirmation != LIVE_CONFIRMATION:
            error = f"live 未执行：确认短语必须为 {LIVE_CONFIRMATION}"

        self._cache_once = False
        self._live_once = False
        self._plan_once = False
        self._live_confirmation = ""
        self.update_config(self._current_config())
        if error:
            logger.error(f"[{self.plugin_name}] {error}")
            self._set_status(state="rejected", mode=mode, message=error)
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        run_at = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3)
        if mode == "plan":
            self._scheduler.add_job(self._run_plan, "date", run_date=run_at)
        else:
            self._scheduler.add_job(
                self._run_inventory,
                "date",
                run_date=run_at,
                kwargs={
                    "mode": mode,
                    "confirmation": LIVE_CONFIRMATION if mode == "live" else "",
                },
            )
        self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "cache_once": False,
            "live_once": False,
            "live_confirmation": "",
            "cron": self._cron,
            "openlist_url": self._openlist_url,
            "openlist_token": self._openlist_token,
            "timeout_seconds": self._timeout_seconds,
            "retries": self._retries,
            "min_interval_seconds": self._min_interval_seconds,
            "max_interval_seconds": self._max_interval_seconds,
            "batch_requests": self._batch_requests,
            "batch_pause_min_seconds": self._batch_pause_min_seconds,
            "batch_pause_max_seconds": self._batch_pause_max_seconds,
            "max_requests_per_profile": self._max_requests_per_profile,
            "root_refresh_hours": self._root_refresh_hours,
            "directory_refresh_hours": self._directory_refresh_hours,
            "per_page": self._per_page,
            "max_pages_per_directory": self._max_pages_per_directory,
            "source_profiles": self._source_profiles,
            "plan_once": False,
            "plan_cron": self._plan_cron,
            "enable_garbage": self._enable_garbage,
            "enable_rename": self._enable_rename,
            "garbage_mode": self._garbage_mode,
            "keep_extensions": self._keep_extensions,
            "junk_extensions": self._junk_extensions,
            "junk_keywords": self._junk_keywords,
            "junk_directories": self._junk_directories,
            "protect_min_mb": self._protect_min_mb,
            "small_video_mb": self._small_video_mb,
            "no_extension_is_junk": self._no_extension_is_junk,
            "jav_prefixes": self._jav_prefixes,
            "known_suffixes": self._known_suffixes,
            "report_scope": self._report_scope,
            "keep_reports": self._keep_reports,
            "container": self._container,
        }

    def _database(self) -> InventoryDatabase:
        data_path = Path(self.get_data_path())
        return InventoryDatabase(data_path / "source-inventory.sqlite3")

    def _profiles(self) -> list[ProfileConfig]:
        return parse_profiles(self._source_profiles)

    def _build_rules(self) -> GarbageRules:
        """构造垃圾判定规则；非法配置直接抛 ValueError 由调用方 fail-closed。"""

        return GarbageRules.build(
            mode=self._garbage_mode,
            keep_extensions=self._keep_extensions,
            junk_extensions=self._junk_extensions,
            junk_keywords=self._junk_keywords,
            junk_directories=self._junk_directories,
            protect_min_mb=self._protect_min_mb,
            small_video_mb=self._small_video_mb,
            no_extension_is_junk=self._no_extension_is_junk,
        )

    def _planning_service(self, profiles: list[ProfileConfig]) -> PlanningService:
        """规划器只读 SQLite；构造过程和运行过程都不接触网络。"""

        return PlanningService(
            self._database(),
            profiles,
            self._build_rules(),
            jav_prefixes=parse_prefix_allowlist(self._jav_prefixes) or None,
            known_suffixes=parse_known_suffixes(self._known_suffixes),
            enable_rename=self._enable_rename,
            enable_garbage=self._enable_garbage,
            log=self._log,
        )

    def _log(self, message: str) -> None:
        """核心模块的日志出口。它们不 import MP logger，由这里注入。"""

        logger.info(f"[{self.plugin_name}] {message}")

    def _client_factory(self, profile: ProfileConfig) -> ReadOnlyOpenListClient:
        return ReadOnlyOpenListClient(
            self._openlist_url,
            self._openlist_token,
            log=lambda message: self._log(f"[{profile.name}] {message}"),
            timeout_seconds=self._timeout_seconds,
            retries=self._retries,
            min_interval_seconds=self._min_interval_seconds,
            max_interval_seconds=self._max_interval_seconds,
            batch_requests=self._batch_requests,
            batch_pause_min_seconds=self._batch_pause_min_seconds,
            batch_pause_max_seconds=self._batch_pause_max_seconds,
            max_requests=self._max_requests_per_profile,
            per_page=self._per_page,
            max_pages_per_directory=self._max_pages_per_directory,
        )

    def _run_inventory(self, mode: str = "cache", confirmation: str = ""):
        if not self._run_lock.acquire(blocking=False):
            logger.warning(f"[{self.plugin_name}] 已有库存任务运行，本次 {mode} 跳过")
            return
        started = time.monotonic()
        try:
            self._set_status(state="running", mode=mode, message="库存扫描运行中")
            profiles = self._profiles()
            self._log(
                f"▶ 开始 {mode} 同步：{len(profiles)} 个 profile"
                f"（{', '.join(item.name for item in profiles)}），"
                f"每 profile 预算 {self._max_requests_per_profile} 请求"
            )
            service = InventoryService(
                self._database(),
                profiles,
                self._client_factory,
                root_refresh_hours=self._root_refresh_hours,
                directory_refresh_hours=self._directory_refresh_hours,
                log=self._log,
            )
            result = service.run(mode, confirmation=confirmation)
            data = result.to_dict()
            state = "success" if result.success else "partial"
            if result.success and not data.get("converged"):
                message = "本轮扫描正常结束，持久队列尚未收敛"
            elif result.success:
                message = "当前到期队列已收敛"
            else:
                message = "库存扫描部分完成，旧 generation 已保留"
            self._set_status(
                state=state,
                mode=mode,
                message=message,
                last_run=data,
                database=self._database().status(),
            )
            self._log(
                f"■ {mode} 同步结束（耗时 {int(time.monotonic() - started)} 秒）："
                f"{message}。{self._aggregate_text(data)}"
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"{self.plugin_name} · {mode}",
                    text=self._aggregate_text(data),
                )
        except Exception as error:
            message = str(error)[:500]
            logger.error(
                f"[{self.plugin_name}] ■ {mode} 同步失败"
                f"（耗时 {int(time.monotonic() - started)} 秒）: {message}"
            )
            self._set_status(state="failed", mode=mode, message=message)
        finally:
            self._run_lock.release()

    def _run_plan(self):
        """在 A 层库存缓存上重算整理计划并落报告。

        全程 **0 次网络请求**：输入是 SQLite 里已发布的完整目录，输出是本地报告。
        规则或 allowlist 调整后重跑即可，不需要再访问 115。
        """

        if not self._run_lock.acquire(blocking=False):
            logger.warning(f"[{self.plugin_name}] 已有任务运行，本次 plan 跳过")
            return
        started = time.monotonic()
        try:
            self._set_status(state="running", mode="plan", message="整理规划运行中")
            profiles = self._profiles()
            service = self._planning_service(profiles)
            self._log(
                f"▶ 开始整理规划（本地，0 次网络请求）：{len(profiles)} 个 profile。"
                f"{describe(service.rules)}"
            )
            result = service.run()
            database = self._database()
            aggregate = database.plan_status()

            scope_states = None if self._report_scope == "all" else ACTIONABLE_STATES
            records = database.plan_records(states=scope_states)
            data = result.to_dict()
            data["rules"] = describe(service.rules)
            data["report_scope"] = self._report_scope
            data["report_records"] = len(records)
            data["aggregate"] = aggregate
            report = write_plan_report(
                self.get_data_path(),
                result.run_id,
                records,
                data,
                keep_reports=self._keep_reports,
            )
            data["report"] = str(report)

            state = "success" if not result.errors else "partial"
            message = "规划完成" if state == "success" else "规划部分完成，存在 profile 级错误"
            self._set_status(
                state=state,
                mode="plan",
                message=message,
                last_plan=data,
                plans=aggregate,
            )
            text = self._plan_text(data, aggregate, str(report))
            self._log(
                f"■ 整理规划结束（耗时 {int(time.monotonic() - started)} 秒）："
                f"{text.splitlines()[0]}"
            )
            self._log(f"■ 报告（{len(records)} 条）：{report}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"{self.plugin_name} · 整理规划（预演，未写入）",
                    text=text,
                )
        except Exception as error:
            message = str(error)[:500]
            logger.error(
                f"[{self.plugin_name}] ■ 整理规划失败"
                f"（耗时 {int(time.monotonic() - started)} 秒）: {message}"
            )
            self._set_status(state="failed", mode="plan", message=message)
        finally:
            self._run_lock.release()

    def _plan_text(self, data: dict, aggregate: dict, report: str) -> str:
        """规划通知正文。明确标注「预演」，避免被误读为已经动过 115。"""

        states = data.get("totals") or {}
        garbage = int(states.get(PlanState.GARBAGE_READY.value, 0))
        rename = int(states.get(PlanState.RENAME_READY.value, 0))
        review = int(states.get(PlanState.REVIEW_REQUIRED.value, 0))
        conflict = int(states.get(PlanState.CONFLICT.value, 0))
        noop = int(states.get(PlanState.NOOP.value, 0))
        unsupported = int(states.get(PlanState.UNSUPPORTED.value, 0))
        garbage_gb = int(aggregate.get("garbage_bytes") or 0) / (1024 * MEGABYTE)
        created = sum(int(item.get("new") or 0) for item in data.get("profiles") or [])
        changed = sum(int(item.get("changed") or 0) for item in data.get("profiles") or [])
        unchanged = sum(
            int(item.get("unchanged") or 0) for item in data.get("profiles") or []
        )

        lines = [
            f"预演结果（本插件不会改动 115）：垃圾 {garbage} 个 / {garbage_gb:.2f} GB，"
            f"改名 {rename}，待审核 {review}，冲突 {conflict}，已规范 {noop}，不支持 {unsupported}",
            f"增量：新增 {created}，变化 {changed}，未变 {unchanged}",
            data.get("rules") or "",
        ]
        reasons = aggregate.get("garbage_reasons") or {}
        if reasons:
            top = "，".join(f"{key} {value}" for key, value in list(reasons.items())[:8])
            lines.append(f"垃圾原因 Top：{top}")
        for profile in data.get("profiles") or []:
            lines.append(
                f"{profile.get('profile')}({profile.get('logic')}): "
                f"计划 {profile.get('planned', 0)}，改名 {profile.get('rename_ready', 0)}，"
                f"垃圾 {profile.get('garbage_ready', 0)}"
            )
        for error in data.get("errors") or []:
            lines.append(f"⚠️ {error}")
        scope_text = "全部计划" if self._report_scope == "all" else "仅需人工处理的计划"
        lines.append(f"报告（{scope_text}，{data.get('report_records', 0)} 条）：{report}")
        lines.append(f"下载：docker cp {self._container}:{report} ./")
        lines.append(
            "改名与删除由独立服务执行，本插件不提供写入入口；确认清单后再走那条链路。"
        )
        return "\n".join(line for line in lines if line)

    @staticmethod
    def _aggregate_text(data: dict) -> str:
        parts = []
        for profile in data.get("profiles") or []:
            frontier = profile.get("frontier_depth")
            frontier_text = "无" if frontier is None else str(frontier)
            parts.append(
                f"{profile.get('profile')}({profile.get('logic')}): "
                f"目录完成 {profile.get('directories_completed', 0)}/"
                f"尝试 {profile.get('directories_attempted', 0)}，对象 {profile.get('objects_seen', 0)}，"
                f"请求 {profile.get('requests', 0)}/{profile.get('request_budget', 0)}，"
                f"批次长停 {profile.get('batch_pauses', 0)}，"
                f"节流 {profile.get('throttle_sleep_seconds', 0)} 秒，"
                f"待处理 {profile.get('queue_remaining', 0)}，前沿层级 {frontier_text}，"
                f"停止原因 {profile.get('stop_reason', '')}"
            )
        return "；".join(parts) or "没有启用的 profile"

    def _get_status(self) -> dict:
        return self.get_data("status") or {
            "state": "idle",
            "mode": "",
            "message": "尚未运行",
            "last_run": {},
            "database": self._database().status(),
        }

    def _set_status(self, **updates: Any):
        status = self._get_status()
        status.update(updates)
        self.save_data("status", status)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/source_pipeline_sync",
                "event": EventType.PluginAction,
                "desc": "执行一次 115 源 cache 库存同步",
                "category": "工具",
                "data": {"action": "source_pipeline_cache"},
            },
            {
                "cmd": "/source_pipeline_plan",
                "event": EventType.PluginAction,
                "desc": "在库存缓存上重算整理计划（不联网、不写入）",
                "category": "工具",
                "data": {"action": "source_pipeline_plan"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run",
                "endpoint": self._api_run,
                "methods": ["POST"],
                "summary": "触发 cache 库存同步",
                "description": "固定 refresh=false；API 不允许触发 live。",
            },
            {
                "path": "/plan",
                "endpoint": self._api_plan,
                "methods": ["POST"],
                "summary": "重算整理计划",
                "description": "只读 SQLite 生成垃圾清单与改名预演报告，网络请求为 0。",
            },
            {
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "summary": "查询库存与规划状态",
                "description": "只读本地状态与 SQLite 聚合，不访问 OpenList。",
            },
            {
                "path": "/plans",
                "endpoint": self._api_plans,
                "methods": ["GET"],
                "summary": "查询计划聚合与样例",
                "description": "返回状态计数、垃圾原因分布和少量样例，不访问 OpenList。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services: List[Dict[str, Any]] = []
        if not self._enabled:
            return services
        if self._cron:
            try:
                services.append({
                    "id": "SourcePipelineCacheCron",
                    "name": "115 源 cache 库存同步",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._run_inventory,
                    "kwargs": {"mode": "cache", "confirmation": ""},
                })
            except Exception as error:
                logger.error(f"[{self.plugin_name}] cache Cron 表达式错误: {error}")
        if self._plan_cron:
            try:
                services.append({
                    "id": "SourcePipelinePlanCron",
                    "name": "115 源整理规划（本地预演）",
                    "trigger": CronTrigger.from_crontab(self._plan_cron),
                    "func": self._run_plan,
                    "kwargs": {},
                })
            except Exception as error:
                logger.error(f"[{self.plugin_name}] plan Cron 表达式错误: {error}")
        return services

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        action = data.get("action")
        if action == "source_pipeline_cache":
            threading.Thread(
                target=self._run_inventory,
                kwargs={"mode": "cache", "confirmation": ""},
                name="sourcepipeline-command",
                daemon=True,
            ).start()
        elif action == "source_pipeline_plan":
            threading.Thread(
                target=self._run_plan,
                name="sourcepipeline-plan-command",
                daemon=True,
            ).start()

    def _api_run(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(
            target=self._run_inventory,
            kwargs={"mode": "cache", "confirmation": ""},
            name="sourcepipeline-api",
            daemon=True,
        ).start()
        return {"success": True, "message": "已触发 cache 同步（refresh=false）"}

    def _api_plan(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(
            target=self._run_plan, name="sourcepipeline-plan-api", daemon=True
        ).start()
        return {"success": True, "message": "已触发整理规划（只读，不访问 OpenList）"}

    def _api_status(self, *args, **kwargs):
        status = self._get_status()
        database = self._database()
        status["database"] = database.status()
        status["plans"] = database.plan_status()
        return {"success": True, "data": status}

    def _api_plans(self, *args, **kwargs):
        database = self._database()
        aggregate = database.plan_status()
        return {
            "success": True,
            "data": {
                "aggregate": aggregate,
                "samples": {
                    PlanState.GARBAGE_READY.value: database.sample_plans(
                        PlanState.GARBAGE_READY.value, 20
                    ),
                    PlanState.RENAME_READY.value: database.sample_plans(
                        PlanState.RENAME_READY.value, 20
                    ),
                    PlanState.CONFLICT.value: database.sample_plans(
                        PlanState.CONFLICT.value, 20
                    ),
                },
            },
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [{
            "component": "VForm",
            "content": [
                self._alert(
                    "warning",
                    "代码级只读：唯一网络接口为 POST /api/fs/list；没有改名、删除、移动、"
                    "STRM 生成或本地文件写入。整理只出「垃圾清单 + 改名预演」报告，"
                    "真正执行由服务器上的独立服务负责。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VSwitch", "enabled", "启用插件（Cron/命令/API）"),
                        self._col(3, "VSwitch", "notify", "发送摘要通知"),
                        self._col(3, "VSwitch", "cache_once", "立即 cache 同步（读 115）"),
                        self._col(3, "VSwitch", "live_once", "立即 live 同步（强制刷新，慢）"),
                    ],
                },
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            3, "VSwitch", "plan_once", "立即重算整理计划（不联网）"
                        ),
                        self._col(3, "VTextField", "cron", "cache Cron", placeholder="0 */2 * * *"),
                        self._col(
                            3, "VTextField", "plan_cron", "规划 Cron", placeholder="30 6 * * *"
                        ),
                        self._col(3, "VTextField", "container", "MP 容器名（生成下载命令）"),
                    ],
                },
                self._alert(
                    "info",
                    f"三个一次性开关每次只能开一个。cache/live 会访问 115；plan 只读本地 SQLite，"
                    f"网络请求为 0，规则调完随便重跑。Cron、远程命令和 POST /run 永远使用 "
                    f"refresh=false；live 只能在本页一次性触发，并输入 {LIVE_CONFIRMATION}。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(12, "VTextField", "live_confirmation", "live 确认短语（执行后清空）", placeholder=LIVE_CONFIRMATION),
                    ],
                },
                self._subtitle("OpenList 与读取风控预算"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(6, "VTextField", "openlist_url", "OpenList 地址", placeholder="http://openlist:5244"),
                        self._col(6, "VTextField", "openlist_token", "OpenList Token", type="password"),
                        self._col(3, "VTextField", "min_interval_seconds", "随机间隔最小（秒，≥1.5）"),
                        self._col(3, "VTextField", "max_interval_seconds", "随机间隔最大（秒）"),
                        self._col(3, "VTextField", "batch_requests", "每 N 个请求长停"),
                        self._col(3, "VTextField", "batch_pause_min_seconds", "长停最小（秒，≥10）"),
                        self._col(3, "VTextField", "batch_pause_max_seconds", "长停最大（秒）"),
                        self._col(3, "VTextField", "max_requests_per_profile", "每 profile 单轮请求上限（≤100）"),
                        self._col(3, "VTextField", "root_refresh_hours", "根目录刷新间隔（小时）"),
                        self._col(3, "VTextField", "directory_refresh_hours", "普通目录刷新间隔（小时）"),
                        self._col(3, "VTextField", "timeout_seconds", "请求超时（秒）"),
                        self._col(3, "VTextField", "per_page", "每页条目（≤1000）"),
                        self._col(3, "VTextField", "max_pages_per_directory", "每目录分页上限"),
                        self._col(3, "VTextField", "retries", "重试次数（0~2，计预算）"),
                    ],
                },
                self._alert(
                    "info",
                    "读取不是 rename/remove 写入：不采用每轮 300 次写额度。每 profile 单轮默认 60 请求、可配置 1~100，"
                    "每次随机等待 1.5~3 秒，每 20 请求长停 30~90 秒；目录按持久队列逐个取出，"
                    "未完成目录按层级广度优先，队列规模不再被固定 20 个目录截断。"
                    "OpenList 使用 HTTP 时只允许受控 Docker/LAN 地址；公网或远程域名必须使用 HTTPS。",
                ),
                self._subtitle("115 原生源 profiles（来源实例与处理逻辑分开保存）"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            12,
                            "VTextarea",
                            "source_profiles",
                            "源目录（每行：name|logic|OpenList绝对路径；旧 name|路径 仍兼容）",
                            placeholder=(
                                "jav|jav|/115/path/to/jav\n"
                                "fc2|fc2|/115/path/to/fc2\n"
                                "leak|jav|/115/path/to/leak"
                            ),
                            rows=5,
                            autoGrow=True,
                        ),
                    ],
                },
                self._subtitle("整理规划（纯本地预演，产出垃圾清单与改名清单）"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VSwitch", "enable_garbage", "启用垃圾判定"),
                        self._col(3, "VSwitch", "enable_rename", "启用番号改名"),
                        self._select(
                            3,
                            "garbage_mode",
                            "垃圾判定模式",
                            [
                                ("关闭", "off"),
                                ("黑名单：只判垃圾后缀", "blacklist"),
                                ("白名单：不在保留清单即垃圾", "whitelist"),
                            ],
                        ),
                        self._select(
                            3,
                            "report_scope",
                            "报告范围",
                            [
                                ("仅需人工处理的计划", "actionable"),
                                ("全部计划（可能很大）", "all"),
                            ],
                        ),
                    ],
                },
                self._alert(
                    "warning",
                    "想要「除了视频和图片全删」就选**白名单**模式，并把不需要的后缀从"
                    "「保留后缀」里删掉（默认保留清单含字幕和 nfo）。"
                    "正片铁律优先于一切规则：视频且体积 ≥ 保护阈值、或已解析出可靠番号，"
                    "永远不会进垃圾清单。目录永不进删除清单。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            3, "VTextField", "protect_min_mb", "大视频保护阈值（MB，0=关闭）"
                        ),
                        self._col(
                            3, "VTextField", "small_video_mb", "小视频判垃圾（MB，0=关闭）"
                        ),
                        self._col(
                            3, "VSwitch", "no_extension_is_junk", "无扩展名即垃圾"
                        ),
                        self._col(3, "VTextField", "keep_reports", "保留报告轮数（0=不清）"),
                    ],
                },
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            6,
                            "VTextarea",
                            "keep_extensions",
                            "保留后缀（留空=内置：视频+图片+字幕+nfo）",
                            placeholder=DEFAULT_KEEP_EXTENSIONS,
                            rows=2,
                            autoGrow=True,
                        ),
                        self._col(
                            6,
                            "VTextarea",
                            "junk_extensions",
                            "垃圾后缀（留空=内置：网页/文本/可执行/校验/种子/临时；默认不含压缩包）",
                            placeholder=DEFAULT_JUNK_EXTENSIONS,
                            rows=2,
                            autoGrow=True,
                        ),
                        self._col(
                            6,
                            "VTextarea",
                            "junk_keywords",
                            "垃圾关键词（整段短语，一行一条；留空=内置）",
                            placeholder=DEFAULT_JUNK_KEYWORDS.replace("\n", "、")[:120],
                            rows=2,
                            autoGrow=True,
                        ),
                        self._col(
                            6,
                            "VTextarea",
                            "junk_directories",
                            "附属子目录（其中的小文件判垃圾；留空=内置，不含 Specials）",
                            placeholder=DEFAULT_JUNK_DIRECTORIES,
                            rows=2,
                            autoGrow=True,
                        ),
                    ],
                },
                {
                    "component": "VRow",
                    "content": [
                        self._col(
                            8,
                            "VTextarea",
                            "jav_prefixes",
                            "JAV 厂商前缀 allowlist（留空=内置常见厂标；不在清单里的一律不改名）",
                            placeholder="ABW MIDV SSIS STARS FSDSS JUQ ...",
                            rows=2,
                            autoGrow=True,
                        ),
                        self._col(
                            4,
                            "VTextField",
                            "known_suffixes",
                            "已知版本后缀（保留进目标名）",
                            placeholder=DEFAULT_KNOWN_SUFFIXES,
                        ),
                    ],
                },
                self._alert(
                    "info",
                    "改名只作用于 logic=jav / fc2 的 profile，且只改视频文件：目标名为 "
                    "「番号[-已知后缀].原扩展名」。分段后缀（CD1/A/B）、未知后缀、多个 canonical、"
                    "同目录目标冲突一律标 REVIEW_REQUIRED 或 CONFLICT，不会出现在可执行清单里。"
                    "文件名没有番号但父目录有、且该目录只有一个视频时，才会按父目录番号改名。",
                ),
            ],
        }], self._default_config()

    @staticmethod
    def _default_config() -> dict:
        return {
            "enabled": False,
            "notify": True,
            "cache_once": False,
            "live_once": False,
            "live_confirmation": "",
            "cron": "",
            "openlist_url": "",
            "openlist_token": "",
            "timeout_seconds": 30,
            "retries": 1,
            "min_interval_seconds": 1.5,
            "max_interval_seconds": 3.0,
            "batch_requests": 20,
            "batch_pause_min_seconds": 30.0,
            "batch_pause_max_seconds": 90.0,
            "max_requests_per_profile": 60,
            "root_refresh_hours": 24,
            "directory_refresh_hours": 168,
            "per_page": 1000,
            "max_pages_per_directory": 20,
            "source_profiles": "",
            "plan_once": False,
            "plan_cron": "",
            "enable_garbage": True,
            "enable_rename": True,
            "garbage_mode": "blacklist",
            "keep_extensions": "",
            "junk_extensions": "",
            "junk_keywords": "",
            "junk_directories": "",
            "protect_min_mb": 400,
            "small_video_mb": 0,
            "no_extension_is_junk": False,
            "jav_prefixes": "",
            "known_suffixes": DEFAULT_KNOWN_SUFFIXES,
            "report_scope": "actionable",
            "keep_reports": 20,
            "container": "moviepilot-v2",
        }

    @staticmethod
    def _col(cols: int, component: str, model: str, label: str, **props: Any) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{"component": component, "props": {"model": model, "label": label, **props}}],
        }

    @staticmethod
    def _select(cols: int, model: str, label: str, options: List[Tuple[str, str]]) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{
                "component": "VSelect",
                "props": {
                    "model": model,
                    "label": label,
                    "items": [{"title": title, "value": value} for title, value in options],
                },
            }],
        }

    @staticmethod
    def _alert(kind: str, text: str) -> dict:
        return {
            "component": "VRow",
            "content": [{
                "component": "VCol",
                "props": {"cols": 12},
                "content": [{
                    "component": "VAlert",
                    "props": {"type": kind, "variant": "tonal", "density": "compact", "text": text},
                }],
            }],
        }

    @classmethod
    def _subtitle(cls, text: str) -> dict:
        return cls._alert("success", text)

    def get_page(self) -> List[dict]:
        return None

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as error:
            logger.error(f"[{self.plugin_name}] 退出插件失败: {error}")
