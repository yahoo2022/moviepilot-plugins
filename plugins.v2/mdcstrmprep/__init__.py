"""
MDC STRM 预处理 (MdcStrmPrep) - MoviePilot V2 插件。

v0.1 仅实现强制预演：只读日本厂商/FC2 的本地 STRM，按独立 profile 解析番号，
生成 JSONL/TSV/摘要报告。当前版本没有 staging、改名、删除、OpenList 写入或 MDC 调用。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta
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

from .manifest import write_run
from .models import ItemState, ScanStats
from .planner import build_plan
from .scanner import scan_strm


class MdcStrmPrep(_PluginBase):
    """日本厂商/FC2 STRM 的只读预处理规划器。"""

    plugin_name = "MDC STRM 预处理"
    plugin_desc = "日本厂商/FC2 番号规范化、冲突审计与强制预演报告（只读源，不写 inbox）"
    plugin_icon = "edit.png"
    plugin_version = "0.1.0"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "mdcstrmprep_"
    plugin_order = 25
    auth_level = 1

    _enabled: bool = False
    _notify: bool = True
    _notify_type: str = "Plugin"
    _push_target: str = "mp"
    _dingtalk_webhook: str = ""
    _dingtalk_keyword: str = ""
    _dingtalk_secret: str = ""
    _test_push_now: bool = False
    _run_once: bool = False
    _cron: str = ""

    # v0.1 的代码级安全门，配置无法关闭。
    _dry_run: bool = True
    _recursive: bool = True
    _recent_days: int = 3
    _stable_seconds: int = 300
    _max_items: int = 500
    _max_strm_bytes: int = 1048576
    _keep_reports: int = 20
    _container: str = "moviepilot-v2"

    _jav_enabled: bool = True
    _jav_source: str = "/media/日本厂商"
    _jav_inbox: str = "/media/mdc-inbox/jav"
    _jav_prefix_allowlist: str = "ABW,MIFD,HEY,HEYZO"
    _fc2_enabled: bool = True
    _fc2_source: str = "/media/fc2"
    _fc2_inbox: str = "/media/mdc-inbox/fc2"

    _scheduler: Optional[BackgroundScheduler] = None
    _run_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}

        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", True))
        self._notify_type = str(config.get("notify_type") or "Plugin")
        self._push_target = str(config.get("push_target") or "mp")
        self._dingtalk_webhook = str(config.get("dingtalk_webhook") or "").strip()
        self._dingtalk_keyword = str(config.get("dingtalk_keyword") or "").strip()
        self._dingtalk_secret = str(config.get("dingtalk_secret") or "").strip()
        self._test_push_now = bool(config.get("test_push_now", False))
        self._run_once = bool(config.get("run_once", False))
        self._cron = str(config.get("cron") or "").strip()

        if config.get("dry_run") is False:
            logger.warning(f"[{self.plugin_name}] v0.1 强制预演，已忽略 dry_run=false")
        self._dry_run = True
        self._recursive = bool(config.get("recursive", True))
        self._recent_days = self._as_int(config.get("recent_days"), 3, minimum=0)
        self._stable_seconds = self._as_int(config.get("stable_seconds"), 300, minimum=0)
        self._max_items = self._as_int(config.get("max_items"), 500, minimum=0)
        self._max_strm_bytes = self._as_int(
            config.get("max_strm_bytes"), 1048576, minimum=1024
        )
        self._keep_reports = self._as_int(config.get("keep_reports"), 20, minimum=0)
        self._container = str(config.get("container") or "moviepilot-v2").strip()

        self._jav_enabled = bool(config.get("jav_enabled", True))
        self._jav_source = str(config.get("jav_source") or "/media/日本厂商").strip()
        self._jav_inbox = str(config.get("jav_inbox") or "/media/mdc-inbox/jav").strip()
        self._jav_prefix_allowlist = str(
            config.get("jav_prefix_allowlist") or "ABW,MIFD,HEY,HEYZO"
        ).strip()
        self._fc2_enabled = bool(config.get("fc2_enabled", True))
        self._fc2_source = str(config.get("fc2_source") or "/media/fc2").strip()
        self._fc2_inbox = str(config.get("fc2_inbox") or "/media/mdc-inbox/fc2").strip()

        if self._test_push_now or self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            run_at = datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3)
            if self._test_push_now:
                self._scheduler.add_job(self._run_test_push, "date", run_date=run_at)
            if self._run_once:
                self._scheduler.add_job(self._run_task, "date", run_date=run_at)
            self._test_push_now = False
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    @staticmethod
    def _as_int(value: Any, default: int, *, minimum: int = 0) -> int:
        try:
            parsed = int(value) if value not in (None, "") else default
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, parsed)

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "notify_type": self._notify_type,
            "push_target": self._push_target,
            "dingtalk_webhook": self._dingtalk_webhook,
            "dingtalk_keyword": self._dingtalk_keyword,
            "dingtalk_secret": self._dingtalk_secret,
            "test_push_now": self._test_push_now,
            "run_once": self._run_once,
            "cron": self._cron,
            "dry_run": True,
            "recursive": self._recursive,
            "recent_days": self._recent_days,
            "stable_seconds": self._stable_seconds,
            "max_items": self._max_items,
            "max_strm_bytes": self._max_strm_bytes,
            "keep_reports": self._keep_reports,
            "container": self._container,
            "jav_enabled": self._jav_enabled,
            "jav_source": self._jav_source,
            "jav_inbox": self._jav_inbox,
            "jav_prefix_allowlist": self._jav_prefix_allowlist,
            "fc2_enabled": self._fc2_enabled,
            "fc2_source": self._fc2_source,
            "fc2_inbox": self._fc2_inbox,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/mdc_strm_prep",
            "event": EventType.PluginAction,
            "desc": "执行一次 MDC STRM 强制预演",
            "category": "整理",
            "data": {"action": "mdc_strm_prep"},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run",
                "endpoint": self._api_run,
                "methods": ["POST"],
                "summary": "触发 MDC STRM 强制预演",
                "description": "只读扫描日本厂商/FC2 STRM 并生成规划报告。",
            },
            {
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "summary": "查询最近运行状态",
                "description": "只读取插件持久化状态，不触发扫描。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "MdcStrmPrepCron",
                    "name": "MDC STRM 预处理定时预演",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._run_task,
                    "kwargs": {},
                }]
            except Exception as error:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {error}")
        return []

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "mdc_strm_prep":
            return
        threading.Thread(target=self._run_task, name="mdcstrmprep-command", daemon=True).start()

    def _api_run(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, name="mdcstrmprep-api", daemon=True).start()
        return {"success": True, "message": "已触发强制预演，详情见状态和报告"}

    def _api_status(self, *args, **kwargs):
        return {"success": True, "data": self._get_status()}

    def _get_status(self) -> dict:
        return self.get_data("status") or {
            "state": "idle",
            "message": "尚未运行",
            "run_id": "",
            "start_time": "",
            "end_time": "",
            "counts": {},
            "scan_stats": {},
            "reports": {},
            "last_error": "",
        }

    def _set_status(self, **updates: Any):
        status = self._get_status()
        status.update(updates)
        status["updated"] = self._now_text()
        self.save_data("status", status)

    @staticmethod
    def _now_text() -> str:
        return datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _merge_stats(target: ScanStats, source: ScanStats):
        for name in (
            "candidates", "discovered", "invalid", "failed",
            "filtered_old", "filtered_unstable", "limited",
        ):
            setattr(target, name, getattr(target, name) + getattr(source, name))

    def _enabled_profiles(self) -> list[tuple[str, str]]:
        profiles: list[tuple[str, str]] = []
        if self._jav_enabled:
            profiles.append(("jav", self._jav_source))
        if self._fc2_enabled:
            profiles.append(("fc2", self._fc2_source))
        return profiles

    def _allowlist(self) -> list[str]:
        return [
            value.upper()
            for value in re.split(r"[,，\s]+", self._jav_prefix_allowlist)
            if value.strip()
        ]

    def _run_task(self):
        if not self._run_lock.acquire(blocking=False):
            logger.warning(f"[{self.plugin_name}] 上一轮仍在执行，本次触发已跳过")
            self._send_notify("预演跳过", "上一轮仍在执行，本次触发已跳过。")
            return

        started = self._now_text()
        run_id = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y%m%d_%H%M%S_%f")
        self._set_status(
            state="running", message="正在只读扫描并生成预演报告", run_id=run_id,
            start_time=started, end_time="", counts={}, scan_stats={}, reports={}, last_error="",
        )
        logger.info(f"[{self.plugin_name}] ▶ 强制预演开始，run_id={run_id}")
        try:
            enabled_profiles = self._enabled_profiles()
            if not enabled_profiles:
                raise RuntimeError("JAV 与 FC2 profile 均未启用")

            sources = []
            aggregate = ScanStats()
            # max_items 对每个 profile 独立生效，避免 FC2 大目录挤占日本厂商根散文件。
            for profile, root in enabled_profiles:
                if not root:
                    raise RuntimeError(f"{profile} 只读源目录为空")
                result = scan_strm(
                    root,
                    profile,
                    recursive=self._recursive,
                    recent_days=self._recent_days,
                    stable_seconds=self._stable_seconds,
                    max_items=self._max_items,
                    max_bytes=self._max_strm_bytes,
                )
                sources.extend(result.items)
                sources.extend(result.issues)
                self._merge_stats(aggregate, result.stats)
                logger.info(
                    f"[{self.plugin_name}] {profile} 扫描：候选 {result.stats.candidates}，"
                    f"发现 {result.stats.discovered}，无效 {result.stats.invalid}，"
                    f"失败 {result.stats.failed}，旧文件跳过 {result.stats.filtered_old}，"
                    f"不稳定跳过 {result.stats.filtered_unstable}，超限 {result.stats.limited}"
                )

            plans = build_plan(sources, jav_prefix_allowlist=self._allowlist())
            report_paths = write_run(
                Path(self.get_data_path()),
                plans,
                run_id=run_id,
                scan_stats=aggregate,
                keep_reports=self._keep_reports,
            )
            counts = dict(sorted(Counter(item.state.value for item in plans).items()))
            reports = {name: str(path) for name, path in report_paths.items()}
            summary = self._summary_text(counts, aggregate, reports)
            self._set_status(
                state="completed",
                message="强制预演完成",
                end_time=self._now_text(),
                counts=counts,
                scan_stats=aggregate.to_record(),
                reports=reports,
                last_error="",
            )
            logger.info(f"[{self.plugin_name}] ■ 强制预演完成\n{summary}")
            self._send_notify("强制预演完成", summary)
        except Exception as error:
            message = str(error)
            logger.exception(f"[{self.plugin_name}] 强制预演失败: {message}")
            self._set_status(
                state="failed", message="强制预演失败", end_time=self._now_text(),
                last_error=message,
            )
            self._send_notify("强制预演失败", message)
        finally:
            self._run_lock.release()

    def _summary_text(self, counts: dict[str, int], stats: ScanStats, reports: dict[str, str]) -> str:
        ordered = [state.value for state in ItemState]
        count_text = " / ".join(f"{name} {counts[name]}" for name in ordered if counts.get(name)) or "无规划项"
        jsonl = reports.get("jsonl", "")
        text = (
            f"模式：强制预演（未写源目录/inbox）\n"
            f"候选 {stats.candidates}，有效 {stats.discovered}，无效 {stats.invalid}，失败 {stats.failed}\n"
            f"旧文件跳过 {stats.filtered_old}，稳定窗口跳过 {stats.filtered_unstable}，超限 {stats.limited}\n"
            f"{count_text}\n报告：{jsonl}"
        )
        if jsonl and self._container:
            text += f"\n下载：docker cp {self._container}:{jsonl} ./"
        return text

    def _notify_type_enum(self):
        try:
            from app.schemas.types import NotificationType as NT
            return getattr(NT, self._notify_type, NT.Plugin)
        except Exception:
            return NotificationType.Plugin

    def _send_notify(self, title: str, text: str):
        if not self._notify:
            return
        if self._push_target == "dingtalk" and self._dingtalk_webhook:
            self._send_dingtalk(title, text)
            return
        try:
            self.post_message(
                mtype=self._notify_type_enum(),
                title=f"【{self.plugin_name}】{title}",
                text=text,
            )
        except Exception as error:
            logger.warning(f"[{self.plugin_name}] 发送通知失败: {error}")

    def _send_dingtalk(self, title: str, text: str):
        url = self._dingtalk_webhook
        content = f"【{self.plugin_name}】{title}\n{text}"
        if self._dingtalk_keyword and self._dingtalk_keyword not in content:
            content = f"{self._dingtalk_keyword} {content}"
        if self._dingtalk_secret:
            timestamp = str(round(time.time() * 1000))
            signature = hmac.new(
                self._dingtalk_secret.encode("utf-8"),
                f"{timestamp}\n{self._dingtalk_secret}".encode("utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(signature))
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}timestamp={timestamp}&sign={sign}"
        payload = json.dumps(
            {"msgtype": "text", "text": {"content": content}},
            ensure_ascii=False,
        ).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "MdcStrmPrep/0.1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                result = json.loads(response.read().decode("utf-8"))
            if result.get("errcode") != 0:
                logger.warning(f"[{self.plugin_name}] 钉钉推送失败: {result}")
        except Exception as error:
            logger.warning(f"[{self.plugin_name}] 钉钉推送异常: {error}")

    def _run_test_push(self):
        self._send_notify(
            "测试推送",
            "这是一条 MdcStrmPrep 测试通知。当前版本始终强制预演，不写源目录或 inbox。",
        )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [{
            "component": "VForm",
            "content": [
                self._alert(
                    "warning",
                    "v0.1 安全门：当前版本强制预演，只读扫描并生成报告；代码中没有 staging、改名、删除、OpenList 写入或 MDC 调用。",
                ),
                {
                    "component": "VRow",
                    "content": [
                        self._col(4, "VSwitch", "enabled", "启用插件（控制 Cron/命令/API）"),
                        self._col(4, "VSwitch", "notify", "发送摘要通知"),
                        self._col(4, "VSwitch", "run_once", "立即预演一次（保存后自动关闭）"),
                        self._col(4, "VSwitch", "dry_run", "强制预演（不可关闭）", disabled=True),
                        self._col(4, "VSwitch", "recursive", "递归扫描子目录"),
                        self._col(4, "VTextField", "cron", "Cron（验证后再启用）", placeholder=""),
                    ],
                },
                self._subtitle("扫描预算与报告"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VTextField", "recent_days", "最近 N 天（0=全量）", placeholder="3"),
                        self._col(3, "VTextField", "stable_seconds", "稳定窗口（秒）", placeholder="300"),
                        self._col(3, "VTextField", "max_items", "每 profile 单次上限（0=不限）", placeholder="500"),
                        self._col(3, "VTextField", "max_strm_bytes", "STRM 最大字节", placeholder="1048576"),
                        self._col(3, "VTextField", "keep_reports", "保留报告轮数（0=不清理）", placeholder="20"),
                        self._col(3, "VTextField", "container", "MP 容器名（下载命令）", placeholder="moviepilot-v2"),
                    ],
                },
                self._subtitle("JAV profile（厂商 allowlist 驱动）"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VSwitch", "jav_enabled", "启用 JAV profile"),
                        self._col(5, "VTextField", "jav_source", "只读源目录", placeholder="/media/日本厂商"),
                        self._col(4, "VTextField", "jav_inbox", "未来 inbox（当前不写）", readonly=True),
                        self._col(12, "VTextarea", "jav_prefix_allowlist", "厂商前缀 allowlist（逗号/空格/换行）", rows=2, autoGrow=True),
                    ],
                },
                self._subtitle("FC2 profile（固定 canonical：FC2-<digits>）"),
                {
                    "component": "VRow",
                    "content": [
                        self._col(3, "VSwitch", "fc2_enabled", "启用 FC2 profile"),
                        self._col(5, "VTextField", "fc2_source", "只读源目录", placeholder="/media/fc2"),
                        self._col(4, "VTextField", "fc2_inbox", "未来 inbox（当前不写）", readonly=True),
                    ],
                },
                self._subtitle("通知"),
                {
                    "component": "VRow",
                    "content": [
                        self._select(4, "push_target", "推送方式", [("MP 站内通知", "mp"), ("钉钉直推", "dingtalk")]),
                        self._select(4, "notify_type", "MP 通知类型", [("插件", "Plugin"), ("整理入库", "Organize"), ("其它", "Other")]),
                        self._col(4, "VSwitch", "test_push_now", "发送测试推送（保存后自动关闭）"),
                        self._col(12, "VTextField", "dingtalk_webhook", "钉钉 Webhook", placeholder="https://oapi.dingtalk.com/robot/send?access_token=..."),
                        self._col(6, "VTextField", "dingtalk_keyword", "钉钉自定义关键词（可选）"),
                        self._col(6, "VTextField", "dingtalk_secret", "钉钉加签密钥（可选）"),
                    ],
                },
                self._alert(
                    "info",
                    "READY 仅表示 parser 高置信且无未知后缀；multipart、同番号不同内容、profile mismatch、gc2048/kcf9 均不会自动投递。请先下载 JSONL/TSV 人工核对。",
                ),
            ],
        }], {
            "enabled": False,
            "notify": True,
            "notify_type": "Plugin",
            "push_target": "mp",
            "dingtalk_webhook": "",
            "dingtalk_keyword": "",
            "dingtalk_secret": "",
            "test_push_now": False,
            "run_once": False,
            "cron": "",
            "dry_run": True,
            "recursive": True,
            "recent_days": 3,
            "stable_seconds": 300,
            "max_items": 500,
            "max_strm_bytes": 1048576,
            "keep_reports": 20,
            "container": "moviepilot-v2",
            "jav_enabled": True,
            "jav_source": "/media/日本厂商",
            "jav_inbox": "/media/mdc-inbox/jav",
            "jav_prefix_allowlist": "ABW,MIFD,HEY,HEYZO",
            "fc2_enabled": True,
            "fc2_source": "/media/fc2",
            "fc2_inbox": "/media/mdc-inbox/fc2",
        }

    @staticmethod
    def _col(cols: int, component: str, model: str, label: str, **props: Any) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{
                "component": component,
                "props": {"model": model, "label": label, **props},
            }],
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
