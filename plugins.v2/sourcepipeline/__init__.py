"""SourcePipeline - 以 115 原生目录为事实源的 MoviePilot V2 插件。

v0.2.0 实现 A 层只读分页 inventory、SQLite 持久缓存、来源实例/处理逻辑分离，
以及请求预算驱动的广度优先持久队列。所有网络入口都固定为 POST /api/fs/list；
本版本没有源写入、STRM 生成或本地文件操作。
"""
from __future__ import annotations

import threading
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
from .models import ProfileConfig
from .openlist_read import ReadOnlyOpenListClient


class SourcePipeline(_PluginBase):
    """115 原生源只读库存缓存；阶段1不具备任何写能力。"""

    plugin_name = "115 源流水线"
    plugin_desc = "来源实例与处理逻辑分离，按请求预算广度优先建立 115 原生目录 SQLite 缓存（阶段1只读）"
    plugin_icon = "workflow.png"
    plugin_version = "0.2.0"
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

        # 初始化 schema 和同步 profile registry 都只操作本地 SQLite，不产生网络访问。
        database = self._database()
        try:
            database.sync_profiles(
                self._profiles(),
                updated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
        except ValueError as error:
            message = f"source profiles 配置错误: {str(error)[:500]}"
            self._cache_once = False
            self._live_once = False
            self._live_confirmation = ""
            self.update_config(self._current_config())
            logger.error(f"[{self.plugin_name}] {message}")
            self._set_status(state="config_error", mode="", message=message)
            return

        if not (self._cache_once or self._live_once):
            return

        mode = "cache"
        error = ""
        if self._cache_once and self._live_once:
            error = "cache 与 live 一次性开关不能同时开启，本次未执行"
        elif self._live_once:
            mode = "live"
            if self._live_confirmation != LIVE_CONFIRMATION:
                error = f"live 未执行：确认短语必须为 {LIVE_CONFIRMATION}"

        self._cache_once = False
        self._live_once = False
        self._live_confirmation = ""
        self.update_config(self._current_config())
        if error:
            logger.error(f"[{self.plugin_name}] {error}")
            self._set_status(state="rejected", mode=mode, message=error)
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        run_at = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3)
        self._scheduler.add_job(
            self._run_inventory,
            "date",
            run_date=run_at,
            kwargs={"mode": mode, "confirmation": LIVE_CONFIRMATION if mode == "live" else ""},
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
        }

    def _database(self) -> InventoryDatabase:
        data_path = Path(self.get_data_path())
        return InventoryDatabase(data_path / "source-inventory.sqlite3")

    def _profiles(self) -> list[ProfileConfig]:
        return parse_profiles(self._source_profiles)

    def _client_factory(self, profile: ProfileConfig) -> ReadOnlyOpenListClient:
        return ReadOnlyOpenListClient(
            self._openlist_url,
            self._openlist_token,
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
        try:
            self._set_status(state="running", mode=mode, message="库存扫描运行中")
            service = InventoryService(
                self._database(),
                self._profiles(),
                self._client_factory,
                root_refresh_hours=self._root_refresh_hours,
                directory_refresh_hours=self._directory_refresh_hours,
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
            logger.info(f"[{self.plugin_name}] {mode} 完成: {self._aggregate_text(data)}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"{self.plugin_name} · {mode}",
                    text=self._aggregate_text(data),
                )
        except Exception as error:
            message = str(error)[:500]
            logger.error(f"[{self.plugin_name}] {mode} 失败: {message}")
            self._set_status(state="failed", mode=mode, message=message)
        finally:
            self._run_lock.release()

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
        return [{
            "cmd": "/source_pipeline_sync",
            "event": EventType.PluginAction,
            "desc": "执行一次 115 源 cache 库存同步",
            "category": "工具",
            "data": {"action": "source_pipeline_cache"},
        }]

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
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "summary": "查询库存状态",
                "description": "只读本地状态与 SQLite 聚合，不访问 OpenList。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "SourcePipelineCacheCron",
                    "name": "115 源 cache 库存同步",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._run_inventory,
                    "kwargs": {"mode": "cache", "confirmation": ""},
                }]
            except Exception as error:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {error}")
        return []

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "source_pipeline_cache":
            return
        threading.Thread(
            target=self._run_inventory,
            kwargs={"mode": "cache", "confirmation": ""},
            name="sourcepipeline-command",
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

    def _api_status(self, *args, **kwargs):
        status = self._get_status()
        status["database"] = self._database().status()
        return {"success": True, "data": status}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [{
            "component": "VForm",
            "content": [
                self._alert(
                    "warning",
                    "阶段1代码级只读：唯一网络接口为 POST /api/fs/list；没有改名、删除、移动、STRM 生成或本地文件写入。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VSwitch", "enabled", "启用插件（Cron/命令/API）"),
                        self._col(3, "VSwitch", "notify", "发送摘要通知"),
                        self._col(3, "VSwitch", "cache_once", "立即 cache 同步"),
                        self._col(3, "VSwitch", "live_once", "立即 live 同步（高风险入口）"),
                    ],
                },
                self._alert(
                    "info",
                    f"Cron、远程命令和 POST /run 永远使用 refresh=false。live 只能在本页一次性触发，并输入 {LIVE_CONFIRMATION}。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(4, "VTextField", "cron", "cache Cron", placeholder="0 */2 * * *"),
                        self._col(8, "VTextField", "live_confirmation", "live 确认短语（执行后清空）", placeholder=LIVE_CONFIRMATION),
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
        }

    @staticmethod
    def _col(cols: int, component: str, model: str, label: str, **props: Any) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{"component": component, "props": {"model": model, "label": label, **props}}],
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
