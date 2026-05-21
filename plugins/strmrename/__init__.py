"""
STRM 剧集重命名助手 - MoviePilot V2 插件

功能：扫描指定目录下的 .strm 文件，把纯数字/集数型文件按上级剧集目录名
重命名为 MoviePilot 更容易识别的电视剧格式。

示例：
  /media/TV/庆余年/001.strm          -> /media/TV/庆余年/庆余年.S01E01.strm
  /media/TV/庆余年/S02/001.strm      -> /media/TV/庆余年/S02/庆余年.S02E01.strm
  /media/TV/三体/1.1080p.strm        -> /media/TV/三体/三体.S01E01.1080p.strm
"""
import re
import threading
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


class StrmRename(_PluginBase):
    plugin_name = "STRM 剧集重命名助手"
    plugin_desc = "按上级目录名把纯数字 STRM 重命名为电视剧友好的 SxxExx 格式"
    plugin_icon = "edit.png"
    plugin_version = "1.0.0"
    plugin_author = "ahnuchen"
    author_url = "https://github.com/ahnuchen"
    plugin_config_prefix = "strmrename_"
    plugin_order = 21
    auth_level = 1

    _enabled: bool = False
    _notify: bool = True
    _run_once: bool = False
    _root_path: str = "/media/TV"
    _recursive: bool = True
    _dry_run: bool = True
    _default_season: int = 1
    _max_episode: int = 500
    _preserve_tail: bool = True
    _skip_existing_named: bool = True
    _template: str = "{title}.S{season:02d}E{episode:02d}{tail}.strm"
    _cron: str = ""
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._run_once = config.get("run_once", False)
            self._root_path = config.get("root_path") or "/media/TV"
            self._recursive = config.get("recursive", True)
            self._dry_run = config.get("dry_run", True)
            self._default_season = int(config.get("default_season") or 1)
            self._max_episode = int(config.get("max_episode") or 500)
            self._preserve_tail = config.get("preserve_tail", True)
            self._skip_existing_named = config.get("skip_existing_named", True)
            self._template = (
                config.get("template")
                or "{title}.S{season:02d}E{episode:02d}{tail}.strm"
            )
            self._cron = config.get("cron", "")

        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次重命名")
            self._scheduler.add_job(
                self._run_task,
                "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
            )
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "run_once": self._run_once,
            "root_path": self._root_path,
            "recursive": self._recursive,
            "dry_run": self._dry_run,
            "default_season": self._default_season,
            "max_episode": self._max_episode,
            "preserve_tail": self._preserve_tail,
            "skip_existing_named": self._skip_existing_named,
            "template": self._template,
            "cron": self._cron,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/strm_rename",
                "event": EventType.PluginAction,
                "desc": "执行一次 STRM 剧集重命名",
                "category": "OpenList",
                "data": {"action": "strm_rename"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/rename",
                "endpoint": self._api_rename,
                "methods": ["GET", "POST"],
                "summary": "执行 STRM 剧集重命名",
                "description": "扫描指定目录，把纯数字 STRM 按上级目录名重命名。",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [
                    {
                        "id": "StrmRenameCron",
                        "name": "STRM 定时重命名",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self._run_task,
                        "kwargs": {},
                    }
                ]
            except Exception as e:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {e}")
        return []

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "strm_rename":
            return
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_rename(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发重命名，详情见 MP 日志"}

    def _run_task(self):
        root = Path(self._root_path)
        if not root.exists():
            msg = f"目录不存在: {root}"
            logger.error(f"[{self.plugin_name}] {msg}")
            self._send_notify("执行失败", msg)
            return
        if not root.is_dir():
            msg = f"不是目录: {root}"
            logger.error(f"[{self.plugin_name}] {msg}")
            self._send_notify("执行失败", msg)
            return

        files = root.rglob("*.strm") if self._recursive else root.glob("*.strm")
        scanned = renamed = skipped = conflicts = failed = 0

        for file_path in files:
            scanned += 1
            try:
                ok, reason = self._rename_one(file_path)
                if ok:
                    renamed += 1
                elif reason == "conflict":
                    conflicts += 1
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                logger.error(f"[{self.plugin_name}] 处理失败: {file_path} - {e}")

        mode = "预演" if self._dry_run else "实际执行"
        msg = (
            f"{mode}完成：扫描 {scanned}，重命名 {renamed}，"
            f"跳过 {skipped}，冲突 {conflicts}，失败 {failed}"
        )
        logger.info(f"[{self.plugin_name}] {msg}")
        self._send_notify("执行完成", msg)

    def _rename_one(self, file_path: Path) -> Tuple[bool, str]:
        if file_path.suffix.lower() != ".strm":
            return False, "not_strm"

        if self._skip_existing_named and self._looks_named(file_path.stem):
            return False, "already_named"

        parsed = self._parse_episode(file_path.stem)
        if not parsed:
            return False, "not_episode"
        episode, tail = parsed

        title, season = self._title_and_season(file_path.parent)
        if not title:
            return False, "no_title"

        if not self._preserve_tail:
            tail = ""
        new_name = self._template.format(
            title=title,
            season=season,
            episode=episode,
            tail=tail,
        )
        new_name = self._safe_name(new_name)
        target = file_path.with_name(new_name)

        if target == file_path:
            return False, "same"
        if target.exists():
            logger.warning(
                f"[{self.plugin_name}] 目标已存在，跳过: {file_path} -> {target}"
            )
            return False, "conflict"

        if self._dry_run:
            logger.info(f"[{self.plugin_name}] [预演] {file_path} -> {target}")
        else:
            logger.info(f"[{self.plugin_name}] 重命名: {file_path} -> {target}")
            file_path.rename(target)
        return True, "renamed"

    @staticmethod
    def _looks_named(stem: str) -> bool:
        return bool(re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", stem))

    def _parse_episode(self, stem: str) -> Optional[Tuple[int, str]]:
        text = stem.strip()
        patterns = [
            r"^(?P<ep>\d{1,4})(?P<tail>(?:\.[A-Za-z0-9][A-Za-z0-9_-]*)*)$",
            r"^[Ee](?P<ep>\d{1,4})(?P<tail>(?:\.[A-Za-z0-9][A-Za-z0-9_-]*)*)$",
            r"^第(?P<ep>\d{1,4})[集话話](?P<tail>.*)$",
        ]
        for pattern in patterns:
            match = re.match(pattern, text)
            if not match:
                continue
            episode = int(match.group("ep"))
            if episode <= 0 or episode > self._max_episode:
                return None
            tail = match.groupdict().get("tail") or ""
            return episode, tail
        return None

    def _title_and_season(self, parent: Path) -> Tuple[str, int]:
        season = self._default_season
        title = parent.name

        season_match = re.match(
            r"(?i)^(?:S|Season\s*|第)(\d{1,2})(?:季)?$",
            parent.name.strip(),
        )
        if season_match and parent.parent.name:
            season = int(season_match.group(1))
            title = parent.parent.name

        return self._clean_title(title), season

    @staticmethod
    def _clean_title(title: str) -> str:
        return re.sub(r"\s+", " ", title).strip(" ._-")

    @staticmethod
    def _safe_name(name: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", name).strip()

    def _send_notify(self, title: str, text: str):
        if not self._notify:
            return
        try:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【{self.plugin_name}】{title}",
                text=text,
            )
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 发送通知失败: {e}")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VSwitch", "enabled", "启用插件"),
                            self._col(6, "VSwitch", "notify", "发送通知"),
                            self._col(6, "VSwitch", "run_once",
                                      "立即执行一次 (保存后生效，随后自动关闭)"),
                            self._col(6, "VSwitch", "dry_run",
                                      "预演模式 (只打印日志，不改名)"),
                            self._col(6, "VSwitch", "recursive", "递归扫描子目录"),
                            self._col(6, "VSwitch", "preserve_tail",
                                      "保留清晰度等后缀"),
                            self._col(6, "VSwitch", "skip_existing_named",
                                      "跳过已含 SxxExx 的文件"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "root_path",
                                      "扫描目录 (MP 容器内路径)",
                                      placeholder="/media/TV"),
                            self._col(3, "VTextField", "default_season",
                                      "默认季数",
                                      placeholder="1"),
                            self._col(3, "VTextField", "max_episode",
                                      "最大集数",
                                      placeholder="500"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextField", "template",
                                      "重命名模板",
                                      placeholder="{title}.S{season:02d}E{episode:02d}{tail}.strm"),
                            self._col(4, "VTextField", "cron",
                                      "Cron 定时 (可选)",
                                      placeholder=""),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "建议先开启预演模式执行一次，"
                                            "确认日志里的改名结果正确后，再关闭预演模式。"
                                            "支持 001.strm、E01.strm、1.1080p.strm；"
                                            "如果父目录是 S01/Season 1，会用上一级目录作为剧名。"
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "run_once": False,
            "root_path": "/media/TV",
            "recursive": True,
            "dry_run": True,
            "default_season": 1,
            "max_episode": 500,
            "preserve_tail": True,
            "skip_existing_named": True,
            "template": "{title}.S{season:02d}E{episode:02d}{tail}.strm",
            "cron": "",
        }

    @staticmethod
    def _col(cols: int, comp: str, model: str, label: str, **props) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [
                {
                    "component": comp,
                    "props": {"model": model, "label": label, **props},
                }
            ],
        }

    def get_page(self) -> List[dict]:
        return None

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"[{self.plugin_name}] 退出插件失败: {e}")
