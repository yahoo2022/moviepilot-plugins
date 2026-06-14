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
    plugin_version = "2.2.0"
    plugin_author = "ahnuchen"
    author_url = "https://github.com/ahnuchen"
    plugin_config_prefix = "strmrename_"
    plugin_order = 21
    auth_level = 1

    _enabled: bool = False
    _notify: bool = True
    _run_once: bool = False
    _tv_paths: str = "/media/TV"        # 电视剧目录（含数字→按一级目录名重命名 SxxExx；无数字→垃圾）
    _movie_paths: str = "/media/Movie"  # 电影目录（只清垃圾，不改名）
    _recursive: bool = True
    _dry_run: bool = True
    _default_season: int = 1
    _max_episode: int = 500
    _preserve_tail: bool = True
    _touch_mtime: bool = True           # 改名后刷新 mtime，便于增量整理捡到
    _clean_junk: bool = True            # 删除垃圾 .strm
    _no_number_is_junk: bool = True     # 不含任何数字(集号)的 .strm 视为垃圾
    _junk_keywords: str = ""            # 额外垃圾关键字，换行/逗号分隔
    _recent_days: int = 0               # 只处理最近 N 天内改动的文件，0=全量
    _after_date: str = ""               # 只处理此日期(含)之后改动的文件，YYYY-MM-DD，优先于 recent_days
    _keep_reports: int = 10             # 保留最近 N 份执行报告，0=不清理
    _container: str = "moviepilot-v2"   # MP 容器名，用于生成 docker cp 拷贝命令
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
            self._tv_paths = config.get("tv_paths") if config.get("tv_paths") is not None else "/media/TV"
            self._movie_paths = config.get("movie_paths") if config.get("movie_paths") is not None else "/media/Movie"
            self._recursive = config.get("recursive", True)
            self._dry_run = config.get("dry_run", True)
            self._default_season = int(config.get("default_season") or 1)
            self._max_episode = int(config.get("max_episode") or 500)
            self._preserve_tail = config.get("preserve_tail", True)
            self._touch_mtime = config.get("touch_mtime", True)
            self._clean_junk = config.get("clean_junk", True)
            self._no_number_is_junk = config.get("no_number_is_junk", True)
            self._junk_keywords = config.get("junk_keywords") or ""
            self._recent_days = int(config.get("recent_days") or 0)
            self._after_date = (config.get("after_date") or "").strip()
            self._keep_reports = int(config.get("keep_reports") if config.get("keep_reports") is not None else 10)
            self._container = (config.get("container") or "moviepilot-v2").strip()
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
            "tv_paths": self._tv_paths,
            "movie_paths": self._movie_paths,
            "recursive": self._recursive,
            "dry_run": self._dry_run,
            "default_season": self._default_season,
            "max_episode": self._max_episode,
            "preserve_tail": self._preserve_tail,
            "touch_mtime": self._touch_mtime,
            "clean_junk": self._clean_junk,
            "no_number_is_junk": self._no_number_is_junk,
            "junk_keywords": self._junk_keywords,
            "recent_days": self._recent_days,
            "after_date": self._after_date,
            "keep_reports": self._keep_reports,
            "container": self._container,
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

    @staticmethod
    def _split_paths(raw: str) -> List[str]:
        if not raw:
            return []
        out: List[str] = []
        for line in raw.replace(",", "\n").replace("，", "\n").splitlines():
            p = line.strip()
            if p and p not in out:
                out.append(p)
        return out

    def _run_task(self):
        tv_paths = self._split_paths(self._tv_paths)
        movie_paths = self._split_paths(self._movie_paths)
        if not tv_paths and not movie_paths:
            self._send_notify("执行失败", "电视剧目录和电影目录都未配置")
            return

        cutoff_ts = self._cutoff_ts()
        # 累计统计
        stat = {"scanned": 0, "renamed": 0, "junked": 0, "skipped": 0,
                "conflicts": 0, "failed": 0, "date_skipped": 0}
        details: List[Tuple[str, str, str, str]] = []
        reason_count: Dict[str, int] = {}
        deleted_junk: List[str] = []

        for kind, paths in (("tv", tv_paths), ("movie", movie_paths)):
            for p in paths:
                root = Path(p)
                if not root.exists() or not root.is_dir():
                    details.append(("ERROR", "bad_root", str(root), "目录不存在或不是目录"))
                    stat["failed"] += 1
                    continue
                self._scan_dir(root, kind, cutoff_ts, stat, details,
                               reason_count, deleted_junk)

        mode = "预演" if self._dry_run else "实际执行"
        skip_brief = "，".join(f"{k}:{v}" for k, v in sorted(
            reason_count.items(), key=lambda x: -x[1])) or "无"
        date_info = ""
        if self._after_date:
            date_info = f"\n日期过滤：仅处理 {self._after_date} 之后，跳过旧文件 {stat['date_skipped']}"
        elif self._recent_days > 0:
            date_info = f"\n日期过滤：仅最近 {self._recent_days} 天，跳过旧文件 {stat['date_skipped']}"
        msg = (
            f"{mode}完成：扫描 {stat['scanned']}，重命名 {stat['renamed']}，"
            f"清理垃圾 {stat['junked']}，跳过 {stat['skipped']}，"
            f"冲突 {stat['conflicts']}，失败 {stat['failed']}"
            f"{date_info}"
            f"\n跳过原因分布：{skip_brief}"
        )
        report_path = self._write_report(mode, msg, details)
        if report_path:
            msg += f"\n明细已写入：{report_path}"
        self._log_deletions(deleted_junk)
        if deleted_junk:
            msg += f"\n已删除 {len(deleted_junk)} 个垃圾，记录于 deleted_junk.log"
        # 末尾附上现成的下载命令，跑完直接复制即可
        if report_path:
            cp_cmd = f"docker cp {self._container}:{report_path} ./"
            msg += f"\n\n下载本次报告（直接复制）：\n{cp_cmd}"
            if deleted_junk:
                log_path = str(Path(self.get_data_path()) / "deleted_junk.log")
                msg += f"\ndocker cp {self._container}:{log_path} ./"
        logger.info(f"[{self.plugin_name}] {msg}")
        self._send_notify("执行完成", msg)

    def _scan_dir(self, root: Path, kind: str, cutoff_ts: Optional[float],
                  stat: dict, details: list, reason_count: dict,
                  deleted_junk: list):
        """扫描单个根目录。kind='tv' 改名+清垃圾；kind='movie' 只清垃圾。"""
        files = root.rglob("*.strm") if self._recursive else root.glob("*.strm")
        for file_path in files:
            stat["scanned"] += 1
            try:
                if cutoff_ts is not None:
                    try:
                        if file_path.stat().st_mtime < cutoff_ts:
                            stat["date_skipped"] += 1
                            continue
                    except OSError:
                        stat["date_skipped"] += 1
                        continue
                # 垃圾判定（含集号铁律保护）
                if self._clean_junk and self._is_junk(file_path):
                    if self._delete_junk(file_path):
                        stat["junked"] += 1
                        details.append(("JUNK", "junk", str(file_path), "已删除"))
                        if not self._dry_run:
                            deleted_junk.append(str(file_path))
                    else:
                        stat["failed"] += 1
                        details.append(("JUNK", "junk", str(file_path), "删除失败"))
                    continue
                # 电影目录：只清垃圾，不改名
                if kind == "movie":
                    stat["skipped"] += 1
                    reason_count["movie_keep"] = reason_count.get("movie_keep", 0) + 1
                    continue
                # 电视剧目录：按一级目录名重命名
                ok, reason, extra = self._rename_one(file_path, root)
                if ok:
                    stat["renamed"] += 1
                    details.append(("RENAME", reason, str(file_path), extra))
                elif reason == "conflict":
                    stat["conflicts"] += 1
                    details.append(("SKIP", reason, str(file_path), extra))
                else:
                    stat["skipped"] += 1
                    reason_count[reason] = reason_count.get(reason, 0) + 1
                    details.append(("SKIP", reason, str(file_path), extra))
            except Exception as e:
                stat["failed"] += 1
                details.append(("ERROR", "exception", str(file_path), str(e)))
                logger.error(f"[{self.plugin_name}] 处理失败: {file_path} - {e}")

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
                f"# 电视剧目录: {self._tv_paths} | 电影目录: {self._movie_paths}",
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

    def _rename_one(self, file_path: Path, root: Path) -> Tuple[bool, str, str]:
        """电视剧统一策略：剧名 = 相对扫描根的「一级目录名」(清洗后)，
        集号从文件名提取。不管文件名原来是中文/拼音/番剧方括号，一律重命名为
        剧名.SxxExx.tail，标题来源完全可预测。"""
        if file_path.suffix.lower() != ".strm":
            return False, "not_strm", ""

        stem = file_path.stem
        parsed = self._parse_any_episode(stem)
        if not parsed:
            return False, "not_episode", "未识别出集数"
        episode, tail, parsed_season = parsed

        title, season = self._top_title_and_season(file_path, root)
        if not title:
            return False, "no_title", "一级目录名为空/无法清洗"
        if parsed_season is not None:
            season = parsed_season

        if not self._preserve_tail:
            tail = ""
        new_name = self._safe_name(self._template.format(
            title=title, season=season, episode=episode, tail=tail))
        target = file_path.with_name(new_name)

        if target == file_path:
            return False, "same", "新旧文件名一致"
        if target.exists():
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

    def _parse_any_episode(self, stem: str) -> Optional[Tuple[int, str, Optional[int]]]:
        """按优先级从文件名提取集号(+可选季号)：
        SxxExx 的 E > EPxx > 第N集 > 番剧[NN] > 开头裸数字。"""
        # 1) SxxExx —— 最高优先，季集都取文件名里的
        m = re.search(r"(?i)\bS(?P<s>\d{1,2})E(?P<e>\d{1,4})\b", stem)
        if m:
            ep = int(m.group("e"))
            if 0 < ep <= self._max_episode:
                return ep, self._extract_tail(stem[m.end():]), int(m.group("s"))
        # 2) 其余格式走通用解析（EPxx / 第N集 / [NN] / 开头数字）
        return self._parse_episode(stem)

    def _top_title_and_season(self, file_path: Path, root: Path) -> Tuple[str, int]:
        """剧名取「相对扫描根的一级目录」；若一级目录下还有 S01/第二季 这类季目录，
        则季号取之，剧名仍用一级目录名。"""
        try:
            rel_parts = file_path.relative_to(root).parts
        except ValueError:
            return self._clean_title(file_path.parent.name), self._default_season
        # rel_parts[-1] 是文件名；一级目录是 rel_parts[0]
        if len(rel_parts) < 2:
            # 文件直接躺在扫描根下，没有剧集目录，标题无从取
            return "", self._default_season
        top = rel_parts[0]
        season = self._default_season
        # 在中间目录里找季号（S02 / 第二季 / Season 2）
        for seg in rel_parts[1:-1]:
            sm = re.match(r"(?i)^(?:S|Season\s*)(\d{1,2})$", seg.strip())
            if sm:
                season = int(sm.group(1))
                break
            cm = re.match(r"^第\s*([0-9一二三四五六七八九十]+)\s*季$", seg.strip())
            if cm:
                g = cm.group(1)
                season = int(g) if g.isdigit() else self._cn_num(g)
                break
        return self._clean_title(top), season

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
        season: Optional[int] = self._guess_season(text)

        # 1) 中文“第N集/话” 优先
        match = re.match(r"^第\s*(?P<ep>\d{1,4})\s*[集话話]", text)
        if match:
            episode = int(match.group("ep"))
            rest = text[match.end():]
            return self._finish(episode, rest, season)

        # 2) 以（可选 E）数字开头：001 / E01 / 1.1080p
        match = re.match(r"^[Ee]?(?P<ep>\d{1,4})(?=$|[.\s_\-\[\]【】()])", text)
        if match:
            episode = int(match.group("ep"))
            return self._finish(episode, text[match.end():], season)

        # 3) 各种“标题 + 集号”格式，按优先级取第一个命中
        #    注意：EPxx / Exx 要排除年份(如 .2022.)，故 E 后≤3位且词边界
        patterns = [
            r"(?i)\bEP\.?(?P<ep>\d{1,4})\b",                 # EP14
            r"(?i)(?<![A-Za-z])E(?P<ep>\d{1,3})(?![0-9A-Za-z])",  # .E02. / E03（无S）
            r"(?i)\bEpisode\s*(?P<ep>\d{1,4})\b",            # Episode 56
            r"\[(?P<ep>\d{1,3})\]",                          # 番剧 [19]
            r"(?:\s|^)-\s*(?P<ep>\d{1,3})(?=\s|\[|$)",       # - 11  /  - 04 [
            r"\s(?P<ep>\d{1,3})(?=\s*[\[(])",                # 坂本日常 16 [1080P]
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                episode = int(m.group("ep"))
                return self._finish(episode, text[m.end():], season)
        return None

    def _guess_season(self, text: str) -> Optional[int]:
        """从文件名里猜季号：SxxExx 的 S / Sxx / 第N季 / 第二季 / 2nd Season。"""
        m = re.search(r"(?i)\bS(\d{1,2})E\d{1,4}\b", text)
        if m:
            return int(m.group(1))
        m = re.search(r"第\s*(\d{1,2})\s*季", text)
        if m:
            return int(m.group(1))
        m = re.search(r"第\s*([一二三四五六七八九十]+)\s*季", text)
        if m:
            return self._cn_num(m.group(1))
        m = re.search(r"(?i)\bS(?:eason\s*)?(\d{1,2})\b", text)
        if m:
            return int(m.group(1))
        m = re.search(r"(?i)\b(\d{1,2})(?:nd|rd|th|st)\s+Season\b", text)
        if m:
            return int(m.group(1))
        return None

    def _finish(self, episode: int, rest: str,
                season: Optional[int]) -> Optional[Tuple[int, str, Optional[int]]]:
        if episode <= 0 or episode > self._max_episode:
            return None
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
        return self._parse_episode(stem) is not None

    @staticmethod
    def _has_real_content(stem: str) -> bool:
        """是否“看起来是正片”（电影也算）：含年份(19xx/20xx)或清晰度(1080p/2160p/4K)。
        用于电影目录——带年份/清晰度的即便文件名里有站点水印也是真电影，不删。"""
        if re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", stem):
            return True
        if re.search(r"(?i)\b(?:2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd)\b", stem):
            return True
        return False

    def _is_junk(self, file_path: Path) -> bool:
        """是否广告/引流/花絮垃圾。

        安全铁律（任一成立即“真内容”，绝不删）：
          - 带集号标记（SxxExx / EPxx / Exx / 第N集 / 番剧[NN] / Episode N / - NN ...）；
          - 带年份或清晰度（电影正片，即便带站点水印，如 大室家.1080p...dygangs.me）。

        判垃圾的两个独立条件（满足其一即垃圾）：
          1. 命中广告/花絮关键字，且不是上面的真内容；
          2. 开启「无数字即垃圾」时，文件名完全不含任何数字。
        花絮例外：含 [menu]/特典/花絮/Ver. 等花絮标记的，即便有年份也按垃圾删。
        """
        stem = file_path.stem
        name = file_path.name.lower()

        # 花絮/菜单/片头片尾：明确的非正片，优先判垃圾（即便带年份/清晰度）
        extras = ("[menu]", "映像特典", "音乐特典", "花絮", "预告片", "creditless",
                  ".ncop.", ".nced.", "ending ver", "review ver", "opening ver",
                  "preview ver", "[sp]", "[pv]", "[trailer]", "[logo]")
        for ex in extras:
            if ex in name:
                return True

        # 真内容保护
        if self._has_episode_marker(stem) or self._has_real_content(stem):
            return False

        # 剩下的：无集号、无年份/清晰度
        for kw in self._junk_kw_list():
            if kw.lower() in name:
                return True
        if self._no_number_is_junk and not re.search(r"\d", stem):
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

    @staticmethod
    def _clean_title(title: str) -> str:
        """把脏目录名清洗成干净剧名。

        例：【高清剧集网发布 www.TTHDTT.com】狙击蝴蝶[全30集][国语配音+中文字幕].Sniper.Butterfly.S01.1080p...
            -> 狙击蝴蝶
        番剧：[DBD-Raws][青之箱][01-25TV全集+特典映像][1080P]... -> 青之箱
        """
        # 番剧式：整名由多个 [..] 块组成，剧名也在某个方括号里。
        # 从方括号块里挑第一个“不是发布组/技术/集数范围”的块作为剧名（中/英都可）。
        if title.lstrip().startswith("["):
            blocks = re.findall(r"\[([^\]]*)\]", title)
            # 已知发布组/字幕组名（整块等于这些则跳过）
            groups = {"dbd-raws", "vcb-studio", "nekomoe kissaten", "milks",
                      "lolihouse", "fyy raws", "bonobosubs", "toc", "ани",
                      "ohys-raws", "lilith-raws", "skymoon-raws", "ave"}
            for b in blocks:
                b = b.strip()
                low = b.lower()
                if not b:
                    continue
                if low in groups:
                    continue
                # 跳过含技术/集数范围/花絮标记的块
                if re.search(r"(?i)(raws|rip|studio|subs|\d{3,4}p|x26|hevc|avc|"
                             r"flac|aac|ac-3|e-ac|10bit|8bit|web-?dl|bdrip|"
                             r"全集|特典|字幕|外挂|双语|menu|mkv|mp4|hi10p|ma10p|"
                             r"\d{1,3}\s*-\s*\d{1,3}|bilibili|webrip|hdr|dovi)", b):
                    continue
                # 跳过纯数字块（那是集号 [19]）
                if re.fullmatch(r"\d{1,4}", b):
                    continue
                cand = re.sub(r"\s+", " ", b).strip(" .-_·")
                # 去掉块尾的季标记，如 "Ranma ½ (2024) S1" -> 截断在 S1/年份前
                cand = re.split(r"(?i)\s+S\d{1,2}\b|\s*\((?:19|20)\d{2}\)", cand)[0].strip()
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
                            self._col(6, "VSwitch", "touch_mtime",
                                      "改名后刷新修改时间 (便于增量整理捡到)"),
                            self._col(6, "VSwitch", "clean_junk",
                                      "删除垃圾 STRM (广告/引流/花絮)"),
                            self._col(6, "VSwitch", "no_number_is_junk",
                                      "无数字即垃圾 (文件名不含任何数字→删)"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextarea", "tv_paths",
                                      "电视剧目录 (含数字→按一级目录名重命名 SxxExx；多个换行)",
                                      placeholder="/media/TV"),
                            self._col(6, "VTextarea", "movie_paths",
                                      "电影目录 (只清垃圾，不改名；多个换行)",
                                      placeholder="/media/Movie"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "default_season",
                                      "默认季数",
                                      placeholder="1"),
                            self._col(6, "VTextField", "max_episode",
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
                            self._col(6, "VTextField", "container",
                                      "MP 容器名 (用于生成下载命令)",
                                      placeholder="moviepilot-v2"),
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
                                            "text": "两类目录分开处理："
                                            "【电视剧目录】文件名含集号(SxxExx/EPxx/第N集/番剧[NN]/开头数字)的，"
                                            "一律用「相对扫描根的一级目录名」(清洗广告后)重命名为 剧名.S01E04，"
                                            "标题来源稳定、不受文件名格式影响；多季放在 剧名/S02 子目录里会识别季号。"
                                            "【电影目录】只清垃圾、不改名(电影靠目录名+年份识别)。"
                                            "【垃圾判定】铁律：含集号的文件永不删；命中广告/花絮关键字、"
                                            "或开「无数字即垃圾」且文件名无任何数字 → 删。"
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
            "tv_paths": "/media/TV",
            "movie_paths": "/media/Movie",
            "recursive": True,
            "dry_run": True,
            "default_season": 1,
            "max_episode": 500,
            "preserve_tail": True,
            "touch_mtime": True,
            "clean_junk": True,
            "no_number_is_junk": True,
            "junk_keywords": "",
            "recent_days": 0,
            "after_date": "",
            "keep_reports": 10,
            "container": "moviepilot-v2",
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
