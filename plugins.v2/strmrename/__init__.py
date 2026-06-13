"""
STRM 剧集重命名助手 - MoviePilot V2 插件

功能：扫描指定目录下的 .strm 文件，把纯数字/集数型文件按上级剧集目录名
重命名为 MoviePilot 更容易识别的电视剧格式。

示例：
  /media/TV/庆余年/001.strm          -> /media/TV/庆余年/庆余年.S01E01.strm
  /media/TV/庆余年/S02/001.strm      -> /media/TV/庆余年/S02/庆余年.S02E01.strm
  /media/TV/三体/1.1080p.strm        -> /media/TV/三体/三体.S01E01.1080p.strm
"""
import os
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
    plugin_version = "1.1.0"
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
    _force_parent_title: bool = False   # 已含 SxxExx 但用上级目录名重写标题（救拼音名）
    _touch_mtime: bool = True           # 改名后刷新 mtime，便于增量整理捡到
    _clean_junk: bool = False           # 删除垃圾 .strm（更多原盘请访问 / 首发广告等）
    _junk_keywords: str = ""            # 垃圾关键字，换行/逗号分隔
    _template: str = "{title}.S{season:02d}E{episode:02d}{tail}.strm"
    _cron: str = ""
    _scheduler: Optional[BackgroundScheduler] = None

    # 内置垃圾关键字（广告/引流型文件名片段）
    _DEFAULT_JUNK = (
        "更多原盘请访问,更多高清,120帧全球首发,全球首发,请访问,扫码,关注公众号,"
        "请关注,免费观看,在线观看,最新电影,高清影视,样片,测试文件,广告,sample,"
        "www.,http,.com,.net,.cc,.me,.tv,.xyz"
    )

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
            self._force_parent_title = config.get("force_parent_title", False)
            self._touch_mtime = config.get("touch_mtime", True)
            self._clean_junk = config.get("clean_junk", False)
            self._junk_keywords = config.get("junk_keywords") or ""
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
            "force_parent_title": self._force_parent_title,
            "touch_mtime": self._touch_mtime,
            "clean_junk": self._clean_junk,
            "junk_keywords": self._junk_keywords,
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
        scanned = renamed = skipped = conflicts = failed = junked = 0

        for file_path in files:
            scanned += 1
            try:
                # 垃圾文件优先处理
                if self._clean_junk and self._is_junk(file_path):
                    if self._delete_junk(file_path):
                        junked += 1
                    continue
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
            f"清理垃圾 {junked}，跳过 {skipped}，冲突 {conflicts}，失败 {failed}"
        )
        logger.info(f"[{self.plugin_name}] {msg}")
        self._send_notify("执行完成", msg)

    def _rename_one(self, file_path: Path) -> Tuple[bool, str]:
        if file_path.suffix.lower() != ".strm":
            return False, "not_strm"

        already_named = self._looks_named(file_path.stem)

        # 已含 SxxExx：默认跳过；开启 force_parent_title 时用上级目录名重写标题
        # （专治 Yi.Wu.Zhi.S01E22 这类拼音命名，把标题换成父目录中文名）
        if already_named and not self._force_parent_title:
            if self._skip_existing_named:
                return False, "already_named"

        if already_named:
            parsed = self._parse_named(file_path.stem)
        else:
            parsed = self._parse_episode(file_path.stem)
        if not parsed:
            return False, "not_episode"
        episode, tail, parsed_season = parsed

        title, season = self._title_and_season(file_path.parent)
        if not title:
            return False, "no_title"
        # 从原文件名解析到的季优先（如 S02E05 保留 S02）
        if parsed_season is not None:
            season = parsed_season

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
            if self._touch_mtime:
                try:
                    os.utime(target, None)
                except OSError as e:
                    logger.warning(f"[{self.plugin_name}] 刷新 mtime 失败 {target}: {e}")
        return True, "renamed"

    # 清晰度/来源等“干净”后缀标记，只有这些会被保留进文件名 tail
    _QUALITY_RE = re.compile(
        r"(?i)^(2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|sdr|dv|"
        r"web-?dl|webrip|bluray|blu-ray|remux|hdtv|"
        r"h\.?264|h\.?265|x264|x265|hevc|avc|10bit|aac|dts|ddp?5\.?1|"
        r"国语|粤语|中字|双语)$"
    )

    @staticmethod
    def _looks_named(stem: str) -> bool:
        return bool(re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", stem))

    def _parse_episode(self, stem: str) -> Optional[Tuple[int, str, Optional[int]]]:
        text = stem.strip()

        # 1) 中文“第N集/话” 优先
        match = re.match(r"^第\s*(?P<ep>\d{1,4})\s*[集话話]", text)
        if match:
            episode = int(match.group("ep"))
            rest = text[match.end():]
        else:
            # 2) 必须以（可选 E）数字开头，且数字后紧跟分隔符或结束，
            #    避免把“21点”“2021某电影”这类标题当成集数。
            match = re.match(r"^[Ee]?(?P<ep>\d{1,4})(?=$|[.\s_\-\[\]【】()])", text)
            if not match:
                return None
            episode = int(match.group("ep"))
            rest = text[match.end():]

        if episode <= 0 or episode > self._max_episode:
            return None

        return episode, self._extract_tail(rest), None

    def _parse_named(self, stem: str) -> Optional[Tuple[int, str, Optional[int]]]:
        """从已含 SxxExx 的文件名里提取季、集，并把 SxxExx 之后的串当作 tail。

        例：Yi.Wu.Zhi.2022.S01E22.1080p.WEB-DL.H265 -> (22, '.1080p.WEB-DL.H265', 1)
        """
        match = re.search(r"(?i)\bS(?P<season>\d{1,2})E(?P<ep>\d{1,4})\b", stem)
        if not match:
            return None
        season = int(match.group("season"))
        episode = int(match.group("ep"))
        if episode <= 0 or episode > self._max_episode:
            return None
        rest = stem[match.end():]
        return episode, self._extract_tail(rest), season

    def _extract_tail(self, rest: str) -> str:
        """从集数后面的剩余串里只挑出清晰度/来源等可识别标记，丢弃中文水印、站点等垃圾。"""
        if not self._preserve_tail or not rest:
            return ""
        tokens: List[str] = []
        for tok in re.split(r"[.\s_\-\[\]【】()]+", rest):
            tok = tok.strip()
            if tok and self._QUALITY_RE.match(tok):
                tokens.append(tok)
        return ("." + ".".join(tokens)) if tokens else ""

    def _junk_kw_list(self) -> List[str]:
        raw = self._junk_keywords.strip() or self._DEFAULT_JUNK
        kws: List[str] = []
        for part in raw.replace("，", "\n").replace(",", "\n").splitlines():
            kw = part.strip()
            if kw and kw not in kws:
                kws.append(kw)
        return kws

    def _is_junk(self, file_path: Path) -> bool:
        """文件名（含后缀）命中任一垃圾关键字即视为广告/引流垃圾。"""
        name = file_path.name.lower()
        for kw in self._junk_kw_list():
            if kw.lower() in name:
                return True
        return False

    def _delete_junk(self, file_path: Path) -> bool:
        if self._dry_run:
            logger.info(f"[{self.plugin_name}] [预演] 删除垃圾: {file_path}")
            return True
        try:
            file_path.unlink()
            logger.info(f"[{self.plugin_name}] 删除垃圾: {file_path}")
            return True
        except OSError as e:
            logger.error(f"[{self.plugin_name}] 删除垃圾失败 {file_path}: {e}")
            return False

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
                            self._col(6, "VSwitch", "force_parent_title",
                                      "用上级目录名重写标题 (救拼音名，如 Yi.Wu.Zhi)"),
                            self._col(6, "VSwitch", "touch_mtime",
                                      "改名后刷新修改时间 (便于增量整理捡到)"),
                            self._col(6, "VSwitch", "clean_junk",
                                      "删除垃圾 STRM (广告/引流文件)"),
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
                            self._col(12, "VTextarea", "junk_keywords",
                                      "垃圾关键字 (换行或逗号分隔，留空用内置默认)",
                                      placeholder="更多原盘请访问\n120帧全球首发\n全球首发\nwww."),
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
                                            "「用上级目录名重写标题」可把 Yi.Wu.Zhi.S01E22 "
                                            "改成父目录中文名（保留 S01E22）；"
                                            "「删除垃圾 STRM」会按关键字删掉广告引流文件，"
                                            "请务必先预演确认再实删。"
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
            "force_parent_title": False,
            "touch_mtime": True,
            "clean_junk": False,
            "junk_keywords": "",
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
