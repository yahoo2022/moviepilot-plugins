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
    plugin_version = "1.6.0"
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
    _junk_only: bool = False            # 仅清理垃圾：只删垃圾，完全不做重命名
    _junk_keywords: str = ""            # 垃圾关键字，换行/逗号分隔
    _recent_days: int = 0               # 只处理最近 N 天内改动的文件，0=全量
    _after_date: str = ""               # 只处理此日期(含)之后改动的文件，YYYY-MM-DD，优先于 recent_days
    _keep_reports: int = 10             # 保留最近 N 份执行报告，0=不清理
    _template: str = "{title}.S{season:02d}E{episode:02d}{tail}.strm"
    _cron: str = ""
    _scheduler: Optional[BackgroundScheduler] = None

    # 内置垃圾关键字（只保留“整段广告短语 / 明确花絮标记”，绝不用裸 TLD/短词，
    # 避免 Shine.on.Me、Always.Meet、The.Studio 这类正片名被子串误命中）。
    # 关键安全保障：含 SxxExx / EPxx / [NN] 集号或 第N集 的文件一律不当垃圾（见 _is_junk）。
    _DEFAULT_JUNK = (
        # 广告/引流——用完整短语，不用裸域名
        "更多原盘请访问,更多高清电影请访问,更多电视剧集下载请访问,更多剧集打包下载请访问,"
        "更多高清剧集下载请访问,更多无水印,120帧全球首发,全球首发,地址发布页,收藏不迷路,"
        "扫码关注,关注公众号,免费公益影视,公益影视站,全站无广告,样片,测试文件,"
        "mp4kan.com,dygangs.me,dygang.me,5266ys.com,6v123.net,6v123.com,butailing.com,"
        # 花絮/菜单/片头片尾/字幕附属（带括号或分隔符，避免误命中正片）
        "[menu],映像特典,音乐特典,花絮,预告片,creditless,"
        ".ncop.,.nced.,ending ver,review ver,opening ver,preview ver,[sp],[pv],"
        "[trailer],[logo],[scans],[fonts]"
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
            self._junk_only = config.get("junk_only", False)
            # 仅清理垃圾模式下，强制开启垃圾清理，否则什么都不做
            if self._junk_only:
                self._clean_junk = True
            self._junk_keywords = config.get("junk_keywords") or ""
            self._recent_days = int(config.get("recent_days") or 0)
            self._after_date = (config.get("after_date") or "").strip()
            self._keep_reports = int(config.get("keep_reports") if config.get("keep_reports") is not None else 10)
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
            "junk_only": self._junk_only,
            "junk_keywords": self._junk_keywords,
            "recent_days": self._recent_days,
            "after_date": self._after_date,
            "keep_reports": self._keep_reports,
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

    def _cutoff_ts(self) -> Optional[float]:
        """计算日期增量的 mtime 下限。after_date 优先于 recent_days；都没设则 None=全量。"""
        if self._after_date:
            try:
                dt = datetime.strptime(self._after_date, "%Y-%m-%d")
                dt = pytz.timezone(settings.TZ).localize(dt)
                return dt.timestamp()
            except Exception as e:
                logger.warning(f"[{self.plugin_name}] after_date 格式错误({self._after_date})，忽略: {e}")
        if self._recent_days and self._recent_days > 0:
            return (datetime.now() - timedelta(days=self._recent_days)).timestamp()
        return None

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
        cutoff_ts = self._cutoff_ts()
        scanned = renamed = skipped = conflicts = failed = junked = 0
        date_skipped = 0
        # 明细：(动作, 原因, 原路径, 目标/说明)
        details: List[Tuple[str, str, str, str]] = []
        reason_count: Dict[str, int] = {}
        deleted_junk: List[str] = []  # 本次实际删除的垃圾，写入持久日志

        for file_path in files:
            scanned += 1
            try:
                # 日期增量过滤：只处理 cutoff 之后改动的文件，保护老的正确内容
                if cutoff_ts is not None:
                    try:
                        if file_path.stat().st_mtime < cutoff_ts:
                            date_skipped += 1
                            continue
                    except OSError:
                        date_skipped += 1
                        continue
                # 垃圾文件优先处理
                if self._clean_junk and self._is_junk(file_path):
                    if self._delete_junk(file_path):
                        junked += 1
                        details.append(("JUNK", "junk", str(file_path), "已删除"))
                        if not self._dry_run:
                            deleted_junk.append(str(file_path))
                    else:
                        failed += 1
                        details.append(("JUNK", "junk", str(file_path), "删除失败"))
                    continue
                # 仅清理垃圾模式：不做任何重命名
                if self._junk_only:
                    skipped += 1
                    reason_count["junk_only_skip"] = reason_count.get("junk_only_skip", 0) + 1
                    continue
                ok, reason, extra = self._rename_one(file_path)
                if ok:
                    renamed += 1
                    details.append(("RENAME", reason, str(file_path), extra))
                elif reason == "conflict":
                    conflicts += 1
                    details.append(("SKIP", reason, str(file_path), extra))
                else:
                    skipped += 1
                    reason_count[reason] = reason_count.get(reason, 0) + 1
                    details.append(("SKIP", reason, str(file_path), extra))
            except Exception as e:
                failed += 1
                details.append(("ERROR", "exception", str(file_path), str(e)))
                logger.error(f"[{self.plugin_name}] 处理失败: {file_path} - {e}")

        mode = "预演" if self._dry_run else "实际执行"
        # 跳过原因分布，帮助判断“为什么没扫到垃圾/没改名”
        skip_brief = "，".join(f"{k}:{v}" for k, v in sorted(
            reason_count.items(), key=lambda x: -x[1])) or "无"
        date_info = ""
        if self._after_date:
            date_info = f"\n日期过滤：仅处理 {self._after_date} 之后，跳过旧文件 {date_skipped}"
        elif self._recent_days > 0:
            date_info = f"\n日期过滤：仅最近 {self._recent_days} 天，跳过旧文件 {date_skipped}"
        msg = (
            f"{mode}完成：扫描 {scanned}，重命名 {renamed}，"
            f"清理垃圾 {junked}，跳过 {skipped}，冲突 {conflicts}，失败 {failed}"
            f"{date_info}"
            f"\n跳过原因分布：{skip_brief}"
        )
        report_path = self._write_report(mode, msg, details)
        if report_path:
            msg += f"\n明细已写入：{report_path}"
        self._log_deletions(deleted_junk)
        if deleted_junk:
            msg += f"\n已删除 {len(deleted_junk)} 个垃圾，记录于 deleted_junk.log"
        logger.info(f"[{self.plugin_name}] {msg}")
        self._send_notify("执行完成", msg)

    def _write_report(self, mode: str, summary: str,
                       details: List[Tuple[str, str, str, str]]) -> str:
        """把本次明细写到插件数据目录，便于 docker cp 下载查看。"""
        try:
            data_dir = self.get_data_path()
            ts = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y%m%d_%H%M%S")
            report = Path(data_dir) / f"rename_report_{ts}.txt"
            lines = [
                f"# STRM 重命名报告 ({mode})",
                f"# 时间: {ts}",
                f"# 扫描目录: {self._root_path}",
                f"# {summary.splitlines()[0]}",
                "",
                "动作\t原因\t原路径\t目标/说明",
            ]
            for action, reason, src, extra in details:
                lines.append(f"{action}\t{reason}\t{src}\t{extra}")
            report.write_text("\n".join(lines), encoding="utf-8")
            self._rotate_reports(Path(data_dir))
            return str(report)
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 写明细报告失败: {e}")
            return ""

    def _rotate_reports(self, data_dir: Path):
        """只保留最近 N 份 rename_report_*.txt，避免越积越多。0=不清理。"""
        if not self._keep_reports or self._keep_reports <= 0:
            return
        try:
            reports = sorted(
                data_dir.glob("rename_report_*.txt"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old in reports[self._keep_reports:]:
                try:
                    old.unlink()
                except OSError:
                    pass
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 清理旧报告失败: {e}")

    def _log_deletions(self, deleted: List[str]):
        """把本次实际删除的垃圾文件追加写入持久日志（不随报告轮转丢失）。"""
        if not deleted:
            return
        try:
            log_file = Path(self.get_data_path()) / "deleted_junk.log"
            ts = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S")
            with log_file.open("a", encoding="utf-8") as f:
                for path in deleted:
                    f.write(f"{ts}\t{path}\n")
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 写删除日志失败: {e}")

    def _rename_one(self, file_path: Path) -> Tuple[bool, str, str]:
        if file_path.suffix.lower() != ".strm":
            return False, "not_strm", ""

        stem = file_path.stem
        already_named = self._looks_named(stem)

        if already_named:
            # 文件名已含 SxxExx —— 这类 MP 基本都能识别，默认不动。
            # 仅当：标题部分没有中文（纯拼音/英文，如 Yi.Wu.Zhi / Sniper.Butterfly）
            #       且开启了 force_parent_title 时，才用「清洗后的父目录中文名」补救。
            if not self._force_parent_title:
                return False, "already_named", "已含SxxExx，跳过"
            if self._title_has_cjk(stem):
                # 文件名里已经有中文剧名（如 落日.Sunset.S01E04），无需补救
                return False, "named_has_cjk", "文件名已含中文标题，跳过"
            parsed = self._parse_named(stem)
            if not parsed:
                return False, "not_episode", "未识别出季集"
            episode, tail, parsed_season = parsed
            title, season = self._title_and_season(file_path.parent)
            if not title or not self._has_cjk(title):
                # 父目录清洗后不是中文剧名，补救没意义，避免改脏
                return False, "no_clean_title", "父目录无干净中文名，跳过"
            if parsed_season is not None:
                season = parsed_season
        else:
            # 裸集号 / 第N集：MP 必然认错，必须补标题
            parsed = self._parse_episode(stem)
            if not parsed:
                return False, "not_episode", "未识别出集数"
            episode, tail, parsed_season = parsed
            title, season = self._title_and_season(file_path.parent)
            if not title:
                return False, "no_title", "上级目录名为空"
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
            return False, "same", "新旧文件名一致"
        if target.exists():
            logger.warning(
                f"[{self.plugin_name}] 目标已存在，跳过: {file_path} -> {target}"
            )
            return False, "conflict", new_name

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
        return True, "renamed", new_name

    @staticmethod
    def _has_cjk(text: str) -> bool:
        return bool(re.search(r"[\u4e00-\u9fff]", text))

    def _title_has_cjk(self, stem: str) -> bool:
        """判断文件名 SxxExx 之前的“标题部分”是否含中文。"""
        match = re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", stem)
        head = stem[:match.start()] if match else stem
        return self._has_cjk(head)

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

    @staticmethod
    def _cn_num(s: str) -> int:
        """简单中文数字转阿拉伯（支持 一~二十）。"""
        digits = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
                  "六": 6, "七": 7, "八": 8, "九": 9}
        if s == "十":
            return 10
        if s.startswith("十"):
            return 10 + digits.get(s[1:], 0)
        if s.endswith("十"):
            return digits.get(s[:-1], 0) * 10
        if "十" in s:
            a, b = s.split("十", 1)
            return digits.get(a, 0) * 10 + digits.get(b, 0)
        return digits.get(s, 1)

    def _parse_episode(self, stem: str) -> Optional[Tuple[int, str, Optional[int]]]:
        text = stem.strip()
        season: Optional[int] = None

        # 1) 中文“第N集/话” 优先
        match = re.match(r"^第\s*(?P<ep>\d{1,4})\s*[集话話]", text)
        if match:
            episode = int(match.group("ep"))
            rest = text[match.end():]
        else:
            # 2) 必须以（可选 E）数字开头，且数字后紧跟分隔符或结束，
            #    避免把“21点”“2021某电影”这类标题当成集数。
            match = re.match(r"^[Ee]?(?P<ep>\d{1,4})(?=$|[.\s_\-\[\]【】()])", text)
            if match:
                episode = int(match.group("ep"))
                rest = text[match.end():]
            else:
                # 3) 标题前缀 + EPxx（如 Under.the.Moonlight.2025.EP14 / 黑帮领地.第一季.EP05）
                #    同时尝试从文本里取“第N季”作为季号。
                ep_match = re.search(r"(?i)\bEP\.?(?P<ep>\d{1,4})\b", text)
                if not ep_match:
                    # 4) 番剧方括号集号：[DBD-Raws][Ao no Hako][19][1080P]... -> 19
                    #    取第一个“纯数字方括号”作为集号（[1080P] 含字母不算）。
                    br = re.search(r"\[(\d{1,3})\]", text)
                    if not br:
                        return None
                    episode = int(br.group(1))
                    rest = text[br.end():]
                else:
                    episode = int(ep_match.group("ep"))
                    rest = text[ep_match.end():]
                    smatch = re.search(r"第\s*(\d{1,2})\s*季", text) or \
                        re.search(r"(?i)\bS(?:eason)?\.?(\d{1,2})\b", text)
                    if smatch:
                        season = int(smatch.group(1))
                    else:
                        # 中文数字季：第一季 / 第二季 ...
                        cn = re.search(r"第\s*([一二三四五六七八九十]+)\s*季", text)
                        if cn:
                            season = self._cn_num(cn.group(1))

        if episode <= 0 or episode > self._max_episode:
            return None

        return episode, self._extract_tail(rest), season

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

    def _has_episode_marker(self, stem: str) -> bool:
        """文件名里是否带可识别的剧集/集号标记。带的一律不当垃圾，避免误删真内容。"""
        if self._looks_named(stem):                       # SxxExx
            return True
        if re.search(r"(?i)\bEP\.?\d{1,4}\b", stem):       # EPxx
            return True
        if re.search(r"第\s*\d{1,4}\s*[集话話]", stem):     # 第N集/话
            return True
        if re.search(r"\[\d{1,4}\]", stem):                # 番剧 [19]
            return True
        return False

    def _is_junk(self, file_path: Path) -> bool:
        """是否广告/引流/花絮垃圾。

        安全第一：只要文件名带集号标记（SxxExx / EPxx / 第N集 / [NN]），
        就认定它是真内容、绝不当垃圾——哪怕名字里还带广告词（那种交给改名去清广告）。
        只有“纯广告/纯花絮、没有任何集号”的文件才会被当垃圾删。
        """
        stem = file_path.stem
        if self._has_episode_marker(stem):
            return False
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
        raw = parent.name

        season_match = re.match(
            r"(?i)^(?:S|Season\s*|第)(\d{1,2})(?:季)?$",
            parent.name.strip(),
        )
        if season_match and parent.parent.name:
            season = int(season_match.group(1))
            raw = parent.parent.name

        return self._clean_title(raw), season

    @staticmethod
    def _clean_title(title: str) -> str:
        """把脏目录名清洗成干净剧名。

        例：【高清剧集网发布 www.TTHDTT.com】狙击蝴蝶[全30集][国语配音+中文字幕].Sniper.Butterfly.S01.1080p...
            -> 狙击蝴蝶
        番剧：[DBD-Raws][青之箱][01-25TV全集+特典映像][1080P]... -> 青之箱
        """
        # 番剧式：整名由多个 [..] 块组成，剧名也在某个方括号里。
        # 先尝试从方括号块里挑一个“含中文、且不是发布组/技术标记”的作为剧名。
        if title.lstrip().startswith("["):
            blocks = re.findall(r"\[([^\]]*)\]", title)
            for b in blocks:
                b = b.strip()
                # 跳过发布组、纯英文、含明显技术/集数标记的块
                if not re.search(r"[\u4e00-\u9fff]", b):
                    continue
                if re.search(r"(?i)(raws|rip|x26|hevc|flac|aac|bit|TV全集|"
                             r"全集|特典|字幕|外挂|\d{3,4}p|menu)", b):
                    continue
                # 去掉块内可能的“第N季”等季信息后返回
                cand = re.sub(r"\s+", " ", b).strip(" .-_·")
                if cand:
                    return cand

        t = title
        # 1) 去掉【...】【...】整块（发布组/站点广告）
        t = re.sub(r"【[^】]*】", " ", t)
        # 2) 去掉 [全30集] [中文字幕] [国语配音] 等方括号块
        t = re.sub(r"\[[^\]]*\]", " ", t)
        # 3) 去掉域名 www.xxx.com / xxx.net 等
        t = re.sub(r"(?i)\b(?:www\.)?[a-z0-9-]+\.(?:com|net|cc|me|tv|xyz|org|cn)\b", " ", t)
        # 4) 在第一个“技术标记”处截断：季号/年份/清晰度/来源/编码
        cut = re.split(
            r"(?i)(?:\.|\s|_)(?:S\d{1,2}(?:E\d+)?|Season\b|(?:19|20)\d{2}\b|"
            r"2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|web-?dl|webrip|"
            r"bluray|blu-ray|remux|hdtv|x264|x265|h\.?264|h\.?265|hevc|"
            r"60fps|10bit)",
            t,
            maxsplit=1,
        )
        t = cut[0] if cut else t
        t = t.strip(" .-_·")
        # 5) 若同时含中文和英文（如“狙击蝴蝶.Sniper.Butterfly”），优先取前导中文段
        cjk = re.match(r"^([\u4e00-\u9fff0-9：·\s]+)", t)
        if cjk and re.search(r"[\u4e00-\u9fff]", cjk.group(1)):
            t = cjk.group(1)
        return re.sub(r"\s+", " ", t).strip(" .-_·")

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
                            self._col(6, "VSwitch", "junk_only",
                                      "仅清理垃圾 (只删垃圾，不做任何重命名)"),
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
                            self._col(6, "VTextField", "after_date",
                                      "只处理此日期之后改动的文件 (YYYY-MM-DD，优先)",
                                      placeholder="2026-06-12"),
                            self._col(6, "VTextField", "recent_days",
                                      "只处理最近 N 天改动 (0=全量)",
                                      placeholder="0"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "keep_reports",
                                      "保留最近 N 份执行报告 (0=不清理)",
                                      placeholder="10"),
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
                                            "text": "默认只重命名 MP 无法识别的「裸集号」文件"
                                            "（如 04.strm、E01.strm、第1集.strm），"
                                            "用清洗后的上级目录名补成 剧名.S01E04。"
                                            "已含 SxxExx 的规范文件默认不动。"
                                            "「用上级目录名重写标题」仅对纯拼音/英文标题"
                                            "（如 Yi.Wu.Zhi.S01E22）生效，且父目录需为干净中文名，"
                                            "已含中文标题的文件会跳过，避免改脏。"
                                            "「删除垃圾 STRM」按关键字删广告引流文件。"
                                            "「仅清理垃圾」只删垃圾、完全不改名，适合单独清库。"
                                            "可用「日期过滤」只处理最近改动的文件，保护老内容。"
                                            "强烈建议先开预演、看明细报告确认无误，再关预演执行。"
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
            "junk_only": False,
            "junk_keywords": "",
            "recent_days": 0,
            "after_date": "",
            "keep_reports": 10,
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
