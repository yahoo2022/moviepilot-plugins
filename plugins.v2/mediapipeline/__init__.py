"""
媒体入库流水线 (MediaPipeline) - MoviePilot V2 插件

把「OpenList 扫描 + 网盘改名清洗 + 增量整理刮削 + Emby 全库扫描」合并成一条
四步流水线，一次触发顺序执行，各步独立开关：

  步骤1 OpenList 扫描      递归遍历挂载路径(/api/fs/list refresh=true)触发 Strm 懒生成
  步骤2 网盘改名清洗        读本地 strm 解析 115 源路径 → 调 OpenList /api/fs/rename 改【115 源名】
                          + /api/fs/remove 清垃圾；同时本地同步改 strm 名+内容URL，免再扫描。
                          对 115 的写操作做拟人化限速防风控。
  步骤3 增量整理刮削        按 mtime 只挑最近 N 天媒体，调 MP 手动整理+刮削(可指定媒体类型)
  步骤4 Emby 全库扫描      POST /Library/Refresh 刷新媒体库

为什么步骤2 改「115 源名」而不是改本地 strm：
  OpenList 的 /TV /Movie 存储是 insert 模式(只增不删)，改本地 strm 会被下次扫描按原始
  源名复活，导致每集堆两份(实测 742 冲突)。改 115 源名后源头就干净，strm 天生规范、不复活。
  改完顺手把本地 strm 也同步改名+改内容URL，这样无需再触发一次 OpenList 扫描。

设计文档：docs/strm-pipeline-redesign.md
预留步骤0(未来)：磁力下载 / gying-collector 采集 / PanSou 搜索联动，接在步骤1 之前。

注意：
  - 扫描路径填 OpenList 挂载路径(虚拟路径，如 /TV)，不是 115 源路径。
  - 改名/整理的目录填 MP 容器内路径(如 /media/TV)；本插件运行在 moviepilot-v2 容器内。
  - 步骤2 强烈建议先开「预演」，看明细报告确认无误，再关预演实际改 115。
"""
import os
import re
import time
import random
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

# 第三方解析库（requirements.txt 已声明，MP 启用插件时自动安装）
try:
    import anitopy
except Exception:
    anitopy = None  # type: ignore
try:
    from guessit import guessit
except Exception:
    guessit = None  # type: ignore


class MediaPipeline(_PluginBase):
    # 插件元数据
    plugin_name = "媒体入库流水线"
    plugin_desc = "四步合一：OpenList扫描→网盘改名清洗(改115源防复活)→MP增量整理刮削→Emby全库扫描，各步独立开关"
    plugin_icon = "workflow.png"
    plugin_version = "1.0.0"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "mediapipeline_"
    plugin_order = 24
    auth_level = 1

    # 默认媒体扩展名（整理步骤用，含 strm）
    _DEFAULT_EXTS = "strm,mkv,mp4,ts,m2ts,iso,avi,mov,wmv,rmvb,flv,m4v,mpg,mpeg"

    # 步骤2 内置垃圾关键字（整段广告短语/明确花絮标记，绝不用裸 TLD/短词，
    # 避免正片名被子串误命中；含集号/年份/清晰度的一律不当垃圾，见 _is_junk）。
    _DEFAULT_JUNK = (
        "更多原盘请访问,更多高清电影请访问,更多电视剧集下载请访问,更多剧集打包下载请访问,"
        "更多高清剧集下载请访问,更多无水印,120帧全球首发,全球首发,地址发布页,收藏不迷路,"
        "扫码关注,关注公众号,免费公益影视,公益影视站,全站无广告,样片,测试文件,"
        "mp4kan.com,dygangs.me,dygang.me,5266ys.com,6v123.net,6v123.com,butailing.com,"
        "[menu],映像特典,音乐特典,花絮,预告片,creditless,"
        ".ncop.,.nced.,ending ver,review ver,opening ver,preview ver,[sp],[pv],"
        "[trailer],[logo],[scans],[fonts]"
    )

    # ---- 总开关 ----
    _enabled: bool = False
    _notify: bool = True
    _notify_type: str = "Plugin"
    _run_once: bool = False
    _cron: str = ""
    _step_timeout_min: int = 0
    _stop_on_error: bool = True

    # ---- 步骤开关 ----
    _do_scan: bool = True
    _do_rename: bool = False       # 步骤2 默认关，需先预演确认
    _do_transfer: bool = True
    _do_emby: bool = False

    # ---- OpenList（步骤1/2 共用）----
    _openlist_url: str = ""
    _openlist_token: str = ""

    # ---- 步骤1：OpenList 扫描 ----
    _scan_path: str = ""
    _scan_limit: int = 20
    _scan_timeout: int = 0
    _scan_recent_days: int = 0

    # ---- 步骤2：网盘改名清洗 ----
    _rn_tv_paths: str = "/media/TV"       # 电视剧 strm 目录（改名+清垃圾）
    _rn_movie_paths: str = "/media/Movie" # 电影 strm 目录（只清垃圾）
    _rn_recursive: bool = True
    _rn_dry_run: bool = True
    _rn_clean_dirs: bool = False          # 2a：清洗一级目录名（去广告前缀）
    _rn_default_season: int = 1
    _rn_max_episode: int = 500
    _rn_preserve_tail: bool = True
    _rn_clean_junk: bool = True
    _rn_no_number_is_junk: bool = True
    _rn_junk_keywords: str = ""
    _rn_recent_days: int = 0
    _rn_after_date: str = ""
    _rn_template: str = "{title}.S{season:02d}E{episode:02d}{tail}"  # 不含扩展名，末尾补真实后缀
    _keep_reports: int = 10
    _container: str = "moviepilot-v2"
    # 防风控节奏（只作用于对 115 的写操作：rename/remove）
    _rl_min: float = 2.0
    _rl_max: float = 5.0
    _rl_batch: int = 30           # 每 N 个写操作长停一次
    _rl_pause_min: float = 60.0
    _rl_pause_max: float = 120.0
    _rl_max_ops: int = 300        # 单次运行 115 写操作上限，到顶即停
    _rl_shuffle: bool = True      # 打乱处理顺序

    # ---- 步骤3：增量整理刮削 ----
    _src_paths: str = ""
    _recent_days: int = 3
    _transfer_unit: str = "folder"
    _transfer_type: str = ""
    _mtype: str = ""
    _target_path: str = ""
    _scrape: str = "on"
    _type_folder: str = "default"
    _category_folder: str = "default"
    _min_filesize: int = 0
    _force: bool = False
    _fast_prune: bool = False
    _media_exts: str = ""

    # ---- 步骤4：Emby 扫描 ----
    _emby_host: str = ""
    _emby_apikey: str = ""

    _scheduler: Optional[BackgroundScheduler] = None

    # 运行期状态（步骤2 防风控计数，每次运行重置）
    _op_count: int = 0
    _consecutive_fail: int = 0

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._notify_type = config.get("notify_type") or "Plugin"
            self._run_once = config.get("run_once", False)
            self._cron = config.get("cron", "")
            self._step_timeout_min = int(config.get("step_timeout_min") or 0)
            self._stop_on_error = config.get("stop_on_error", True)

            self._do_scan = config.get("do_scan", True)
            self._do_rename = config.get("do_rename", False)
            self._do_transfer = config.get("do_transfer", True)
            self._do_emby = config.get("do_emby", False)

            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token", "")

            self._scan_path = config.get("scan_path") or ""
            self._scan_limit = int(config.get("scan_limit") or 20)
            _to = config.get("scan_timeout")
            self._scan_timeout = int(_to) if str(_to).strip() not in ("", "None") else 0
            self._scan_recent_days = int(config.get("scan_recent_days") or 0)

            self._rn_tv_paths = config.get("rn_tv_paths") if config.get("rn_tv_paths") is not None else "/media/TV"
            self._rn_movie_paths = config.get("rn_movie_paths") if config.get("rn_movie_paths") is not None else "/media/Movie"
            self._rn_recursive = config.get("rn_recursive", True)
            self._rn_dry_run = config.get("rn_dry_run", True)
            self._rn_clean_dirs = config.get("rn_clean_dirs", False)
            self._rn_default_season = int(config.get("rn_default_season") or 1)
            self._rn_max_episode = int(config.get("rn_max_episode") or 500)
            self._rn_preserve_tail = config.get("rn_preserve_tail", True)
            self._rn_clean_junk = config.get("rn_clean_junk", True)
            self._rn_no_number_is_junk = config.get("rn_no_number_is_junk", True)
            self._rn_junk_keywords = config.get("rn_junk_keywords") or ""
            self._rn_recent_days = int(config.get("rn_recent_days") or 0)
            self._rn_after_date = (config.get("rn_after_date") or "").strip()
            self._rn_template = (config.get("rn_template")
                                 or "{title}.S{season:02d}E{episode:02d}{tail}")
            self._keep_reports = int(config.get("keep_reports") if config.get("keep_reports") is not None else 10)
            self._container = (config.get("container") or "moviepilot-v2").strip()
            self._rl_min = float(config.get("rl_min") or 2.0)
            self._rl_max = float(config.get("rl_max") or 5.0)
            self._rl_batch = int(config.get("rl_batch") or 30)
            self._rl_pause_min = float(config.get("rl_pause_min") or 60.0)
            self._rl_pause_max = float(config.get("rl_pause_max") or 120.0)
            self._rl_max_ops = int(config.get("rl_max_ops") or 300)
            self._rl_shuffle = config.get("rl_shuffle", True)

            self._src_paths = config.get("src_paths") or ""
            self._recent_days = int(config.get("recent_days") or 0)
            self._transfer_unit = config.get("transfer_unit") or "folder"
            self._transfer_type = config.get("transfer_type") or ""
            self._mtype = config.get("mtype") or ""
            self._target_path = (config.get("target_path") or "").strip()
            self._scrape = config.get("scrape") or "on"
            self._type_folder = config.get("type_folder") or "default"
            self._category_folder = config.get("category_folder") or "default"
            self._min_filesize = int(config.get("min_filesize") or 0)
            self._force = config.get("force", False)
            self._fast_prune = config.get("fast_prune", False)
            self._media_exts = config.get("media_exts") or ""

            self._emby_host = (config.get("emby_host") or "").rstrip("/")
            self._emby_apikey = config.get("emby_apikey", "")

        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次流水线")
            self._scheduler.add_job(
                self._run_task, "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            )
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled, "notify": self._notify,
            "notify_type": self._notify_type, "run_once": self._run_once,
            "cron": self._cron, "step_timeout_min": self._step_timeout_min,
            "stop_on_error": self._stop_on_error,
            "do_scan": self._do_scan, "do_rename": self._do_rename,
            "do_transfer": self._do_transfer, "do_emby": self._do_emby,
            "openlist_url": self._openlist_url, "openlist_token": self._openlist_token,
            "scan_path": self._scan_path, "scan_limit": self._scan_limit,
            "scan_timeout": self._scan_timeout, "scan_recent_days": self._scan_recent_days,
            "rn_tv_paths": self._rn_tv_paths, "rn_movie_paths": self._rn_movie_paths,
            "rn_recursive": self._rn_recursive, "rn_dry_run": self._rn_dry_run,
            "rn_clean_dirs": self._rn_clean_dirs,
            "rn_default_season": self._rn_default_season, "rn_max_episode": self._rn_max_episode,
            "rn_preserve_tail": self._rn_preserve_tail, "rn_clean_junk": self._rn_clean_junk,
            "rn_no_number_is_junk": self._rn_no_number_is_junk,
            "rn_junk_keywords": self._rn_junk_keywords, "rn_recent_days": self._rn_recent_days,
            "rn_after_date": self._rn_after_date, "rn_template": self._rn_template,
            "keep_reports": self._keep_reports, "container": self._container,
            "rl_min": self._rl_min, "rl_max": self._rl_max, "rl_batch": self._rl_batch,
            "rl_pause_min": self._rl_pause_min, "rl_pause_max": self._rl_pause_max,
            "rl_max_ops": self._rl_max_ops, "rl_shuffle": self._rl_shuffle,
            "src_paths": self._src_paths, "recent_days": self._recent_days,
            "transfer_unit": self._transfer_unit, "transfer_type": self._transfer_type,
            "mtype": self._mtype, "target_path": self._target_path, "scrape": self._scrape,
            "type_folder": self._type_folder, "category_folder": self._category_folder,
            "min_filesize": self._min_filesize, "force": self._force,
            "fast_prune": self._fast_prune, "media_exts": self._media_exts,
            "emby_host": self._emby_host, "emby_apikey": self._emby_apikey,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/media_pipeline",
            "event": EventType.PluginAction,
            "desc": "执行一次媒体入库流水线",
            "category": "整理",
            "data": {"action": "media_pipeline"},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/run",
            "endpoint": self._api_run,
            "methods": ["GET", "POST"],
            "summary": "执行媒体入库流水线",
            "description": "顺序执行 OpenList 扫描 + 网盘改名清洗 + 增量整理刮削 + Emby 扫描（按步骤开关）。",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "MediaPipelineCron",
                    "name": "媒体入库流水线定时任务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._run_task,
                    "kwargs": {},
                }]
            except Exception as e:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {e}")
        return []

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "media_pipeline":
            return
        logger.info(f"[{self.plugin_name}] 收到远程命令，开始执行")
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_run(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发流水线，详情见 MP 日志"}

    @staticmethod
    def _split_paths(raw: str) -> List[str]:
        if not raw:
            return []
        parts: List[str] = []
        for line in raw.replace(",", "\n").replace("，", "\n").splitlines():
            p = line.strip()
            if p and p not in parts:
                parts.append(p)
        return parts

    # ---------- 主流程编排 ----------

    def _run_step(self, name: str, func) -> Tuple[bool, str]:
        """在子线程执行单步，套「单步超时」兜底。func 返回 (ok, summary)。"""
        result: Dict[str, Any] = {}

        def _worker():
            try:
                ok, summary = func()
                result["ok"] = ok
                result["summary"] = summary
            except Exception as e:
                logger.error(f"[{self.plugin_name}] {name} 执行异常: {e}")
                result["ok"] = False
                result["summary"] = f"执行异常: {e}"

        t = threading.Thread(target=_worker, name=f"mediapipeline-{name}", daemon=True)
        start = time.time()
        t.start()
        timeout = self._step_timeout_min * 60 if self._step_timeout_min > 0 else None
        t.join(timeout)
        if t.is_alive():
            msg = (f"超时：超过 {self._step_timeout_min} 分钟仍未完成，已停止等待"
                   f"（该步骤后台可能仍在跑，但流水线不再阻塞）")
            logger.warning(f"[{self.plugin_name}] {name} {msg}")
            return False, msg
        elapsed = int(time.time() - start)
        ok = result.get("ok", False)
        summary = result.get("summary", "无返回")
        return ok, f"{summary}（耗时 {elapsed} 秒）"

    def _run_task(self):
        """四步串行：扫描 → 网盘改名清洗 → 增量整理刮削 → Emby 扫描。各步独立开关。"""
        if not any([self._do_scan, self._do_rename, self._do_transfer, self._do_emby]):
            self._send_notify("流水线未执行", "四个步骤开关都关闭了，没有可执行的步骤")
            return

        summary_parts: List[str] = []
        aborted = False

        # 步骤1 OpenList 扫描
        if self._do_scan:
            ok, s = self._run_step("步骤1 OpenList 扫描", self._run_scan)
            summary_parts.append(f"【步骤1 OpenList 扫描】{'✅' if ok else '❌'}\n{s}")
            if not ok and self._stop_on_error:
                summary_parts.append("⚠️ 步骤1未成功，已按「出错即中止」停止后续")
                aborted = True
        else:
            summary_parts.append("【步骤1 OpenList 扫描】已跳过（开关关闭）")

        # 步骤2 网盘改名清洗
        if not aborted and self._do_rename:
            ok, s = self._run_step("步骤2 网盘改名清洗", self._run_rename)
            summary_parts.append(f"【步骤2 网盘改名清洗】{'✅' if ok else '❌'}\n{s}")
            if not ok and self._stop_on_error:
                summary_parts.append("⚠️ 步骤2未成功，已按「出错即中止」停止后续")
                aborted = True
        elif not self._do_rename:
            summary_parts.append("【步骤2 网盘改名清洗】已跳过（开关关闭）")

        # 步骤3 增量整理刮削
        if not aborted and self._do_transfer:
            ok, s = self._run_step("步骤3 增量整理刮削", self._run_transfer)
            summary_parts.append(f"【步骤3 增量整理刮削】{'✅' if ok else '❌'}\n{s}")
            if not ok and self._stop_on_error:
                summary_parts.append("⚠️ 步骤3未成功，已按「出错即中止」停止后续")
                aborted = True
        elif not self._do_transfer:
            summary_parts.append("【步骤3 增量整理刮削】已跳过（开关关闭）")

        # 步骤4 Emby 全库扫描
        if not aborted and self._do_emby:
            ok, s = self._run_step("步骤4 Emby 媒体库扫描", self._run_emby_scan)
            summary_parts.append(f"【步骤4 Emby 媒体库扫描】{'✅' if ok else '❌'}\n{s}")
        elif not self._do_emby:
            summary_parts.append("【步骤4 Emby 媒体库扫描】已跳过（开关关闭）")

        title = "媒体入库流水线完成" if not aborted else "媒体入库流水线中止（含失败）"
        self._send_notify(title, "\n\n".join(summary_parts))

    # ==================== 步骤1：OpenList 扫描 ====================

    def _run_scan(self) -> Tuple[bool, str]:
        if not self._openlist_url or not self._openlist_token:
            msg = "OpenList 地址或 token 未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg
        scan_paths = self._split_paths(self._scan_path)
        if not scan_paths:
            msg = "扫描路径未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg
        results: List[str] = []
        any_ok = False
        for sp in scan_paths:
            ok, msg = self._recursive_scan(sp)
            results.append((f"✓ {msg}") if ok else (f"✗ {sp}: {msg}"))
            any_ok = any_ok or ok
        summary = "\n".join(results)
        logger.info(f"[{self.plugin_name}] 扫描结束：\n{summary}")
        return any_ok, summary

    def _recursive_scan(self, scan_path: str) -> Tuple[bool, str]:
        start = time.time()
        dir_count = file_count = skipped = 0
        failed: List[str] = []
        cutoff = None
        if self._scan_recent_days and self._scan_recent_days > 0:
            cutoff = datetime.now(tz=pytz.utc) - timedelta(days=self._scan_recent_days)
        stack: List[str] = [scan_path]
        throttle = max(0.0, float(self._scan_limit) * 0.1)
        no_timeout = self._scan_timeout <= 0
        timed_out = False
        while stack:
            if not no_timeout and time.time() - start > self._scan_timeout:
                timed_out = True
                break
            cur = stack.pop()
            self._get_path(cur)
            ok, entries, err = self._list_dir(cur)
            if not ok:
                logger.warning(f"[{self.plugin_name}] 列目录失败（已重试）{cur}: {err}")
                failed.append(cur)
                continue
            dir_count += 1
            sub_dirs = 0
            for ent in entries:
                name = ent.get("name")
                if not name:
                    continue
                child = f"{cur.rstrip('/')}/{name}"
                if ent.get("is_dir"):
                    if cutoff is not None:
                        mtime = self._parse_time(ent.get("modified"))
                        if mtime is not None and mtime < cutoff:
                            skipped += 1
                            continue
                    sub_dirs += 1
                    stack.append(child)
                else:
                    file_count += 1
            logger.info(f"[{self.plugin_name}] [{dir_count}] 列目录 {cur}"
                        f"：子目录 {sub_dirs}，文件累计 {file_count}，待扫 {len(stack)}")
            if throttle:
                time.sleep(throttle)
        elapsed = int(time.time() - start)
        extra = f"，跳过旧目录 {skipped} 个" if cutoff is not None else ""
        mode = f"增量({self._scan_recent_days}天)" if cutoff is not None else "全量"
        if timed_out:
            msg = (f"路径: {scan_path}（{mode}）扫描中断：超过 {self._scan_timeout} 秒，"
                   f"已遍历目录 {dir_count}、文件 {file_count}{extra}，还剩 {len(stack)} 个目录未扫")
            logger.warning(f"[{self.plugin_name}] {msg}")
            return False, msg
        if failed:
            msg = (f"路径: {scan_path}（{mode}）完成但有遗漏：目录 {dir_count}、文件 {file_count}{extra}，"
                   f"{len(failed)} 个目录列举失败，耗时 {elapsed} 秒。失败示例：{failed[0]}")
            logger.warning(f"[{self.plugin_name}] {msg}")
            return False, msg
        msg = (f"路径: {scan_path}（{mode}），已遍历目录 {dir_count}、文件 {file_count}{extra}，"
               f"耗时 {elapsed} 秒")
        logger.info(f"[{self.plugin_name}] 递归扫描完成，{msg}")
        return True, msg

    @staticmethod
    def _parse_time(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
            return dt.astimezone(pytz.utc)
        except Exception:
            return None

    def _get_path(self, path: str) -> bool:
        """调 /api/fs/get 触发 OpenList Strm 驱动为该路径生成 strm。"""
        try:
            resp = requests.post(
                f"{self._openlist_url}/api/fs/get",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json={"path": path, "refresh": False}, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            if data.get("code") != 200:
                logger.warning(f"[{self.plugin_name}] get {path} 返回 {data.get('code')}: {data.get('message')}")
                return False
            return True
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] get {path} 异常: {e}")
            return False

    def _list_dir(self, path: str) -> Tuple[bool, List[dict], str]:
        url = f"{self._openlist_url}/api/fs/list"
        headers = {"Authorization": self._openlist_token, "Content-Type": "application/json"}
        payload = {"path": path, "page": 1, "per_page": 10000, "refresh": True}
        last_err = ""
        for attempt in range(1, 4):
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json() or {}
                if data.get("code") != 200:
                    last_err = f"OpenList 返回 {data.get('code')}: {data.get('message')}"
                    if attempt < 3:
                        time.sleep(attempt * 2)
                        continue
                    return False, [], last_err
                content = (data.get("data") or {}).get("content") or []
                return True, content, ""
            except Exception as e:
                last_err = str(e)
                if attempt < 3:
                    logger.warning(f"[{self.plugin_name}] 列目录 {path} 第 {attempt} 次失败，{attempt*2}s 后重试：{last_err}")
                    time.sleep(attempt * 2)
                    continue
        return False, [], last_err

    # ==================== 步骤4：Emby 全库扫描 ====================

    def _run_emby_scan(self) -> Tuple[bool, str]:
        if not self._emby_host or not self._emby_apikey:
            msg = "Emby 地址或 API Key 未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg
        try:
            resp = requests.post(f"{self._emby_host}/Library/Refresh",
                                 params={"api_key": self._emby_apikey}, timeout=30)
            if resp.status_code in (200, 204):
                msg = "已触发 Emby 全库扫描（Emby 后台异步执行）"
                logger.info(f"[{self.plugin_name}] {msg}")
                return True, msg
            msg = f"Emby 返回 HTTP {resp.status_code}: {resp.text[:200]}"
            logger.error(f"[{self.plugin_name}] 触发 Emby 扫描失败：{msg}")
            return False, msg
        except Exception as e:
            msg = f"触发 Emby 扫描异常：{e}"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg

    # ==================== 步骤3：增量整理刮削 ====================

    def _ext_set(self) -> set:
        raw = self._media_exts.strip() or self._DEFAULT_EXTS
        exts = set()
        for e in raw.replace(",", " ").replace("，", " ").split():
            e = e.strip().lstrip(".").lower()
            if e:
                exts.add(e)
        return exts

    def _mtype_enum(self):
        if not self._mtype:
            return None
        try:
            from app.schemas.types import MediaType
            if self._mtype == "电影":
                return MediaType.MOVIE
            if self._mtype == "电视剧":
                return MediaType.TV
        except Exception:
            return None
        return None

    def _scrape_val(self) -> Optional[bool]:
        if self._scrape == "on":
            return True
        if self._scrape == "off":
            return False
        return None

    @staticmethod
    def _tri_val(v: str) -> Optional[bool]:
        if v == "on":
            return True
        if v == "off":
            return False
        return None

    def _run_transfer(self) -> Tuple[bool, str]:
        src_paths = self._split_paths(self._src_paths)
        if not src_paths:
            return False, "源目录未配置，跳过整理"
        cutoff_ts: Optional[float] = None
        if self._recent_days and self._recent_days > 0:
            cutoff_ts = (datetime.now() - timedelta(days=self._recent_days)).timestamp()
        mode = f"增量({self._recent_days}天)" if cutoff_ts is not None else "全量"

        all_targets: List[Tuple[str, str]] = []
        seen = set()
        scan_errs: List[str] = []
        for sp in src_paths:
            root = Path(sp)
            if not root.exists():
                scan_errs.append(f"源目录不存在: {sp}")
                logger.error(f"[{self.plugin_name}] 源目录不存在: {sp}")
                continue
            try:
                targets = self._collect_targets(root, cutoff_ts)
            except Exception as e:
                scan_errs.append(f"{sp}: 扫描异常 {e}")
                logger.error(f"[{self.plugin_name}] 扫描 {sp} 异常: {e}")
                continue
            for path, ftype in targets:
                if path not in seen:
                    seen.add(path)
                    all_targets.append((path, ftype))

        logger.info(f"[{self.plugin_name}] {mode} 共找到 {len(all_targets)} 个待整理项")
        if not all_targets:
            text = f"模式: {mode}，没有找到需要整理的内容"
            if cutoff_ts is not None:
                text += f"（最近 {self._recent_days} 天无新增/改动媒体）"
            if scan_errs:
                text += "\n" + "\n".join(scan_errs)
            return (not scan_errs), text

        ok_list, fail_list = self._do_transfer_targets(all_targets)
        parts = [f"模式: {mode}，待整理 {len(all_targets)} 项"]
        if ok_list:
            parts.append(f"已完成 {len(ok_list)} 项:\n" + "\n".join(ok_list[:20])
                         + ("\n..." if len(ok_list) > 20 else ""))
        if fail_list:
            parts.append(f"失败 {len(fail_list)} 项:\n" + "\n".join(fail_list[:20])
                         + ("\n..." if len(fail_list) > 20 else ""))
        if scan_errs:
            parts.append("扫描告警:\n" + "\n".join(scan_errs))
        ok = (not fail_list) and (not scan_errs)
        return ok, "\n".join(parts)

    def _collect_targets(self, root: Path, cutoff_ts: Optional[float]) -> List[Tuple[str, str]]:
        if cutoff_ts is None:
            return [(str(root), "dir")]
        exts = self._ext_set()
        targets: "Dict[str, str]" = {}
        for dirpath, dirnames, filenames in os.walk(root):
            if self._fast_prune:
                kept = []
                for d in dirnames:
                    if d.startswith(".") or d in ("@Recycle", "#recycle", "@eaDir"):
                        continue
                    full = os.path.join(dirpath, d)
                    try:
                        if os.stat(full).st_mtime < cutoff_ts:
                            continue
                    except OSError:
                        continue
                    kept.append(d)
                dirnames[:] = kept
            else:
                dirnames[:] = [d for d in dirnames
                               if not d.startswith(".") and d not in ("@Recycle", "#recycle", "@eaDir")]
            for fn in filenames:
                if fn.startswith("."):
                    continue
                ext = fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
                if ext not in exts:
                    continue
                fp = Path(dirpath) / fn
                try:
                    if fp.stat().st_mtime < cutoff_ts:
                        continue
                except OSError:
                    continue
                if self._transfer_unit == "file":
                    targets[str(fp)] = "file"
                    continue
                try:
                    rel_parts = fp.relative_to(root).parts
                except ValueError:
                    rel_parts = (fn,)
                if len(rel_parts) <= 1:
                    targets[str(fp)] = "file"
                else:
                    targets[str(root / rel_parts[0])] = "dir"
        return list(targets.items())

    def _do_transfer_targets(self, targets: List[Tuple[str, str]]) -> Tuple[List[str], List[str]]:
        try:
            from app.chain.transfer import TransferChain
            from app.schemas import FileItem
        except Exception as e:
            logger.error(f"[{self.plugin_name}] 导入 MP 内部模块失败: {e}")
            return [], [f"导入模块异常: {e}"]
        chain = TransferChain()
        mtype = self._mtype_enum()
        scrape = self._scrape_val()
        type_folder = self._tri_val(self._type_folder)
        category_folder = self._tri_val(self._category_folder)
        ttype = self._transfer_type or None
        target = Path(self._target_path) if self._target_path else None
        ok_list: List[str] = []
        fail_list: List[str] = []
        for path_str, ftype in targets:
            p = Path(path_str)
            if not p.exists():
                fail_list.append(f"{path_str}: 路径不存在")
                continue
            try:
                fileitem = FileItem(
                    storage="local", type=ftype, path=str(p), name=p.name,
                    basename=p.stem if ftype == "file" else p.name,
                    extension=(p.suffix.lstrip(".") if ftype == "file" else ""), size=0)
                logger.info(f"[{self.plugin_name}] 整理 [{ftype}] {p}")
                ok, msg = chain.manual_transfer(
                    fileitem=fileitem, target_path=target, mtype=mtype,
                    transfer_type=ttype, scrape=scrape,
                    library_type_folder=type_folder, library_category_folder=category_folder,
                    min_filesize=self._min_filesize, force=self._force, background=False)
                if ok:
                    ok_list.append(str(p))
                else:
                    logger.error(f"[{self.plugin_name}] 整理失败 {p}: {msg}")
                    fail_list.append(f"{p.name}: {msg}")
            except Exception as e:
                logger.error(f"[{self.plugin_name}] 整理异常 {p}: {e}")
                fail_list.append(f"{p.name}: {e}")
        return ok_list, fail_list

    # ==================== 通知 ====================

    def _notify_type_enum(self):
        try:
            from app.schemas.types import NotificationType as NT
            return getattr(NT, self._notify_type, NT.Plugin)
        except Exception:
            return NotificationType.Plugin

    def _send_notify(self, title: str, text: str):
        if not self._notify:
            return
        try:
            self.post_message(mtype=self._notify_type_enum(),
                              title=f"【{self.plugin_name}】{title}", text=text)
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 发送通知失败: {e}")

    # ==================== 步骤2：网盘改名清洗 ====================

    def _rn_cutoff_ts(self) -> Optional[float]:
        """步骤2 日期增量下限。after_date 优先于 recent_days；都没设=全量。"""
        if self._rn_after_date:
            try:
                dt = datetime.strptime(self._rn_after_date, "%Y-%m-%d")
                dt = pytz.timezone(settings.TZ).localize(dt)
                return dt.timestamp()
            except Exception as e:
                logger.warning(f"[{self.plugin_name}] after_date 格式错误({self._rn_after_date})，忽略: {e}")
        if self._rn_recent_days and self._rn_recent_days > 0:
            return (datetime.now() - timedelta(days=self._rn_recent_days)).timestamp()
        return None

    # ---- OpenList 写操作 + 防风控 ----

    def _ol_rename(self, path_115: str, new_base: str) -> Tuple[bool, str]:
        """调 /api/fs/rename 改 115 源(文件或目录)名。path=完整旧路径，name=新的最后一段名。"""
        try:
            resp = requests.post(
                f"{self._openlist_url}/api/fs/rename",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json={"path": path_115, "name": new_base}, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            if data.get("code") == 200:
                return True, ""
            return False, f"code={data.get('code')} {data.get('message')}"
        except Exception as e:
            return False, str(e)

    def _ol_remove(self, dir_115: str, name: str) -> Tuple[bool, str]:
        """调 /api/fs/remove 删 115 源。dir=父目录，names=[文件名]。"""
        try:
            resp = requests.post(
                f"{self._openlist_url}/api/fs/remove",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json={"dir": dir_115, "names": [name]}, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            if data.get("code") == 200:
                return True, ""
            return False, f"code={data.get('code')} {data.get('message')}"
        except Exception as e:
            return False, str(e)

    def _write_gate(self) -> bool:
        """非预演时检查是否还能继续写(未到单次上限、未触发退避中止)。到上限置 _capped。"""
        if self._aborted_backoff:
            return False
        if self._rl_max_ops > 0 and self._op_count >= self._rl_max_ops:
            self._capped = True
            return False
        return True

    def _after_write(self, ok: bool):
        """一次 115 写操作后的节奏：计数 + 限速 sleep + 批次长停 + 错误退避。"""
        self._op_count += 1
        if ok:
            self._consecutive_fail = 0
        else:
            self._consecutive_fail += 1
            backoff = [5, 15, 45][min(self._consecutive_fail - 1, 2)]
            logger.warning(f"[{self.plugin_name}] 115 写操作第 {self._consecutive_fail} 次失败，退避 {backoff}s")
            time.sleep(backoff)
            if self._consecutive_fail >= 3:
                self._aborted_backoff = True
                logger.error(f"[{self.plugin_name}] 连续 3 次失败，中止本轮网盘写操作（防风控）")
                return
        time.sleep(random.uniform(self._rl_min, self._rl_max))
        if self._rl_batch > 0 and self._op_count % self._rl_batch == 0:
            pause = random.uniform(self._rl_pause_min, self._rl_pause_max)
            logger.info(f"[{self.plugin_name}] 已写 {self._op_count} 个，长停 {int(pause)}s（防风控）")
            time.sleep(pause)

    # ---- strm 内容 <-> 115 源路径 ----

    @staticmethod
    def _read_strm(strm: Path) -> str:
        try:
            return strm.read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            return ""

    @staticmethod
    def _head_d(content: str) -> str:
        """取 strm URL 里到 /d 为止的前缀，如 http://192.168.1.111:5244/d。"""
        c = content.split("?", 1)[0]
        i = c.find("/d/")
        return (c[:i] + "/d") if i >= 0 else ""

    @staticmethod
    def _strm_source_path(content: str) -> Optional[str]:
        """从 strm 内容(URL)解析 115 源路径(URL 解码后)，如 /115/自动追踪影视/.../x.mkv。"""
        if not content:
            return None
        c = content.split("?", 1)[0].strip()
        i = c.find("/d/")
        if i >= 0:
            return unquote(c[i + 2:])   # 保留开头的 /
        if c.startswith("/"):
            return unquote(c)
        return None

    @staticmethod
    def _build_url(head_d: str, src_decoded: str) -> str:
        """由「到 /d 的前缀」+「解码后的 115 路径」重建 strm URL(encodePath 逐段编码)。"""
        segs = src_decoded.split("/")
        return head_d + "/".join(quote(s, safe="") for s in segs)

    def _source_ext(self, src_115: Optional[str], content: str) -> str:
        """取 115 源文件真实后缀(.mkv/.mp4...)，解析不到兜底 .mkv。"""
        name = None
        if src_115:
            name = src_115.rstrip("/").split("/")[-1]
        if not name and content:
            name = unquote(content.split("?", 1)[0].rstrip("/").split("/")[-1])
        if name and "." in name:
            return "." + name.rsplit(".", 1)[-1].lower()
        return ".mkv"

    # ---- 主流程 ----

    def _run_rename(self) -> Tuple[bool, str]:
        if not self._openlist_url or not self._openlist_token:
            return False, "OpenList 地址或 token 未配置（步骤2 需调 OpenList API 改 115 源）"
        tv_paths = self._split_paths(self._rn_tv_paths)
        movie_paths = self._split_paths(self._rn_movie_paths)
        if not tv_paths and not movie_paths:
            return False, "电视剧目录和电影目录都未配置"

        # 重置本轮防风控状态
        self._op_count = 0
        self._consecutive_fail = 0
        self._capped = False
        self._aborted_backoff = False

        cutoff_ts = self._rn_cutoff_ts()
        stat = {"scanned": 0, "renamed": 0, "junked": 0, "skipped": 0,
                "conflicts": 0, "failed": 0, "date_skipped": 0, "dirs_renamed": 0}
        details: List[Tuple[str, str, str, str]] = []
        reason_count: Dict[str, int] = {}

        # 2a：目录名清洗（去广告前缀，仅电视剧目录；先做，避免和文件改名交叉）
        if self._rn_clean_dirs and not self._aborted_backoff:
            for p in tv_paths:
                root = Path(p)
                if root.exists() and root.is_dir():
                    self._clean_dir_names(root, cutoff_ts, stat, details)
                if self._capped or self._aborted_backoff:
                    break

        # 2b：文件改名(电视剧) + 清垃圾(电视剧/电影)
        stop = self._capped or self._aborted_backoff
        for kind, paths in (("tv", tv_paths), ("movie", movie_paths)):
            if stop:
                break
            for p in paths:
                root = Path(p)
                if not root.exists() or not root.is_dir():
                    details.append(("ERROR", "bad_root", str(root), "目录不存在或不是目录"))
                    stat["failed"] += 1
                    continue
                self._scan_dir_rename(root, kind, cutoff_ts, stat, details, reason_count)
                if self._capped or self._aborted_backoff:
                    stop = True
                    break

        mode = "预演" if self._rn_dry_run else "实际执行"
        skip_brief = "，".join(f"{k}:{v}" for k, v in sorted(
            reason_count.items(), key=lambda x: -x[1])) or "无"
        date_info = ""
        if self._rn_after_date:
            date_info = f"\n日期过滤：仅 {self._rn_after_date} 之后，跳过旧文件 {stat['date_skipped']}"
        elif self._rn_recent_days > 0:
            date_info = f"\n日期过滤：仅最近 {self._rn_recent_days} 天，跳过旧文件 {stat['date_skipped']}"
        msg = (f"{mode}完成：扫描 {stat['scanned']}，目录改名 {stat['dirs_renamed']}，"
               f"文件改名 {stat['renamed']}，清垃圾 {stat['junked']}，跳过 {stat['skipped']}，"
               f"冲突 {stat['conflicts']}，失败 {stat['failed']}"
               f"{date_info}\n跳过原因分布：{skip_brief}")
        if not self._rn_dry_run:
            msg += f"\n115 写操作：{self._op_count} 次"
        if self._capped:
            msg += f"\n⚠️ 已达单次写操作上限 {self._rl_max_ops}，剩余留待下次运行（防风控）"
        if self._aborted_backoff:
            msg += "\n⚠️ 连续多次失败已中止本轮网盘写操作（疑似风控/限流），请稍后再试"

        report_path = self._write_report(mode, msg, details)
        if report_path:
            msg += f"\n明细已写入：{report_path}"
            msg += f"\n\n下载本次报告（直接复制）：\ndocker cp {self._container}:{report_path} ./"
        logger.info(f"[{self.plugin_name}] {msg}")

        ok = (stat["failed"] == 0) and (not self._aborted_backoff)
        return ok, msg

    def _clean_dir_names(self, root: Path, cutoff_ts: Optional[float],
                         stat: dict, details: list):
        """把 root 下一级子目录中名字含广告前缀的重命名：改 115 源目录 + 本地同步(改文件夹名+子 strm URL)。"""
        try:
            subs = [d for d in root.iterdir() if d.is_dir()]
        except OSError as e:
            details.append(("ERROR", "list_root_fail", str(root), str(e)))
            stat["failed"] += 1
            return
        if self._rl_shuffle:
            random.shuffle(subs)
        for d in subs:
            if self._capped or self._aborted_backoff:
                return
            old = d.name
            new = self._clean_dir_name(old)
            if not new or new == old:
                continue
            if (root / new).exists():
                details.append(("SKIP", "dir_conflict", str(d), new))
                continue
            if cutoff_ts is not None:
                try:
                    if d.stat().st_mtime < cutoff_ts:
                        stat["date_skipped"] += 1
                        continue
                except OSError:
                    stat["date_skipped"] += 1
                    continue
            # 找目录下任一 strm，解析出该目录的 115 路径
            sample = next(iter(d.rglob("*.strm")), None)
            if sample is None:
                details.append(("SKIP", "dir_no_strm", str(d), "目录内无 strm，无法定位 115 源"))
                continue
            sample_src = self._strm_source_path(self._read_strm(sample))
            if not sample_src:
                details.append(("SKIP", "dir_no_source", str(d), "strm 解析不到 115 源"))
                continue
            try:
                rel_parts = sample.relative_to(d).parts   # (mid..., file.strm)
            except ValueError:
                continue
            strip = len(rel_parts)                        # 去掉 old 之后的所有段(mid...,file)
            src_segs = sample_src.split("/")
            if strip >= len(src_segs):
                continue
            dir_115 = "/".join(src_segs[:-strip])          # /115/.../TV/old
            if dir_115.split("/")[-1] != old:
                # 目录名和 115 段不一致(可能之前改过)，保守跳过
                details.append(("SKIP", "dir_mismatch", str(d), f"115段={dir_115.split('/')[-1]}"))
                continue
            new_dir_115 = dir_115.rsplit("/", 1)[0] + "/" + new

            if self._rn_dry_run:
                stat["dirs_renamed"] += 1
                details.append(("DIR", "would", str(d), f"115: {old} -> {new}"))
                continue
            if not self._write_gate():
                details.append(("SKIP", "capped", str(d), "达上限，留待下次"))
                return
            ok, err = self._ol_rename(dir_115, new)
            self._after_write(ok)
            if not ok:
                stat["failed"] += 1
                details.append(("ERROR", "ol_dir_rename_fail", str(d), err))
                continue
            # 本地同步：先算好每个子 strm 的新内容，再移动文件夹，再回写
            try:
                plan: List[Tuple[Path, str]] = []
                for c in d.rglob("*.strm"):
                    cc = self._read_strm(c)
                    csrc = self._strm_source_path(cc)
                    if not csrc:
                        continue
                    ncsrc = csrc.replace(dir_115, new_dir_115, 1)
                    plan.append((c.relative_to(d), self._build_url(self._head_d(cc), ncsrc)))
                d.rename(root / new)
                for rel, ncontent in plan:
                    try:
                        (root / new / rel).write_text(ncontent, encoding="utf-8")
                    except Exception as e:
                        details.append(("WARN", "child_sync_fail", str(root / new / rel), str(e)))
                stat["dirs_renamed"] += 1
                details.append(("DIR", "renamed", str(d), f"-> {new}（子 strm {len(plan)} 个已同步）"))
            except Exception as e:
                details.append(("WARN", "dir_local_sync_fail", str(d),
                                f"115 已改名但本地同步失败: {e}"))
                stat["dirs_renamed"] += 1

    def _scan_dir_rename(self, root: Path, kind: str, cutoff_ts: Optional[float],
                         stat: dict, details: list, reason_count: dict):
        files = list(root.rglob("*.strm") if self._rn_recursive else root.glob("*.strm"))
        if self._rl_shuffle:
            random.shuffle(files)
        for strm in files:
            if self._capped or self._aborted_backoff:
                return
            stat["scanned"] += 1
            try:
                if cutoff_ts is not None:
                    try:
                        if strm.stat().st_mtime < cutoff_ts:
                            stat["date_skipped"] += 1
                            continue
                    except OSError:
                        stat["date_skipped"] += 1
                        continue
                content = self._read_strm(strm)
                src_115 = self._strm_source_path(content)
                # 垃圾判定（含集号/年份/清晰度铁律保护，见 _is_junk）
                if self._rn_clean_junk and self._is_junk(strm):
                    self._handle_junk(strm, content, src_115, stat, details)
                    continue
                if kind == "movie":
                    stat["skipped"] += 1
                    reason_count["movie_keep"] = reason_count.get("movie_keep", 0) + 1
                    continue
                self._handle_rename(strm, content, src_115, root, stat, details, reason_count)
            except Exception as e:
                stat["failed"] += 1
                details.append(("ERROR", "exception", str(strm), str(e)))
                logger.error(f"[{self.plugin_name}] 处理失败 {strm}: {e}")

    def _handle_rename(self, strm: Path, content: str, src_115: Optional[str],
                       root: Path, stat: dict, details: list, reason_count: dict):
        stem = strm.stem
        parsed = self._parse_any_episode(stem)
        if not parsed:
            stat["skipped"] += 1
            reason_count["not_episode"] = reason_count.get("not_episode", 0) + 1
            details.append(("SKIP", "not_episode", str(strm), "未识别出集数"))
            return
        episode, tail, parsed_season = parsed
        title, season = self._top_title_and_season(strm, root)
        if not title:
            stat["skipped"] += 1
            reason_count["no_title"] = reason_count.get("no_title", 0) + 1
            details.append(("SKIP", "no_title", str(strm), "一级目录名为空/无法清洗"))
            return
        if parsed_season is not None:
            season = parsed_season
        if not self._rn_preserve_tail:
            tail = ""
        base = self._safe_name(self._rn_template.format(
            title=title, season=season, episode=episode, tail=tail))
        ext = self._source_ext(src_115, content)
        new_media = base + ext
        new_strm_name = base + ".strm"

        # 幂等：115 源已是规范名
        if src_115 and src_115.rstrip("/").split("/")[-1] == new_media:
            stat["skipped"] += 1
            reason_count["same"] = reason_count.get("same", 0) + 1
            details.append(("SKIP", "same", str(strm), "115 源已是规范名"))
            return
        target_strm = strm.with_name(new_strm_name)
        if target_strm.exists() and target_strm != strm:
            stat["conflicts"] += 1
            details.append(("SKIP", "conflict", str(strm), new_strm_name))
            return

        if self._rn_dry_run:
            old115 = src_115.rstrip("/").split("/")[-1] if src_115 else "?"
            stat["renamed"] += 1
            details.append(("RENAME", "would", str(strm), f"115: {old115} -> {new_media}"))
            return
        if not src_115:
            stat["failed"] += 1
            details.append(("ERROR", "no_source", str(strm), "strm 内容解析不到 115 源路径"))
            return
        if not self._write_gate():
            details.append(("SKIP", "capped", str(strm), "达上限，留待下次"))
            return
        ok, err = self._ol_rename(src_115, new_media)
        self._after_write(ok)
        if not ok:
            stat["failed"] += 1
            details.append(("ERROR", "ol_rename_fail", str(strm), err))
            return
        # 本地同步：改 strm 内容 URL(指向新 115 文件名) + 改本地 strm 文件名
        try:
            new_src = src_115.rsplit("/", 1)[0] + "/" + new_media
            new_content = self._build_url(self._head_d(content), new_src)
            strm.write_text(new_content, encoding="utf-8")
            if target_strm != strm:
                strm.rename(target_strm)
        except Exception as e:
            details.append(("WARN", "local_sync_fail", str(strm), f"115 已改名但本地同步失败: {e}"))
        stat["renamed"] += 1
        details.append(("RENAME", "renamed", str(strm), new_media))

    def _handle_junk(self, strm: Path, content: str, src_115: Optional[str],
                     stat: dict, details: list):
        if self._rn_dry_run:
            stat["junked"] += 1
            details.append(("JUNK", "would", str(strm), f"删 115: {src_115 or '?'}"))
            return
        if not self._write_gate():
            details.append(("SKIP", "capped", str(strm), "达上限，留待下次"))
            return
        ok, err = True, ""
        if src_115:
            parent = src_115.rsplit("/", 1)[0]
            name = src_115.rstrip("/").split("/")[-1]
            ok, err = self._ol_remove(parent, name)
            self._after_write(ok)
        if ok:
            try:
                strm.unlink()
            except OSError:
                pass
            stat["junked"] += 1
            details.append(("JUNK", "removed", str(strm), f"已删 115+本地: {src_115 or ''}"))
        else:
            stat["failed"] += 1
            details.append(("ERROR", "ol_remove_fail", str(strm), err))

    # ---- 目录名清洗规则（保守：去广告块/域名/发布站关键字）----

    def _clean_dir_name(self, name: str) -> str:
        t = name
        t = re.sub(r"【[^】]*】", "", t)                       # 去【广告块】
        t = re.sub(r"\[[^\]]*(?:\.(?:com|net|cc|me|tv|xyz|org|cn))[^\]]*\]", "", t, flags=re.I)  # 去[站点块]
        t = re.sub(r"(?i)(?:www\.)?[a-z0-9-]+\.(?:com|net|cc|me|tv|xyz|org|cn)", "", t)  # 去裸域名
        # 发布站中文短语（据 2026-08-21 失败快照统计，纯文本剥除，绝不会是真标题的一部分）
        for kw in ("地址发布页", "收藏不迷路", "最新电影", "电影港",
                   "高清剧集网发布", "高清剧集网", "高清影视之家发布", "高清影视之家",
                   "更多电视剧集下载访问", "更多剧集打包下载访问", "更多电视剧集下载请访问",
                   "更多剧集打包下载请访问", "4K时光",
                   "6v电影", "阳光电影", "电影天堂", "電影天堂", "BT天堂", "不太灵影视"):
            t = t.replace(kw, "")
        # 注意：strip 字符集不含圆括号，避免把「三体 (2023)」削成「三体 (2023」破坏年份
        t = re.sub(r"\s+", " ", t).strip(" .-_·【】[]")
        if not t or t == name or len(t) < 2:
            return ""
        return self._safe_name(t)

    # ==================== 步骤2：集号/标题/垃圾解析（移植自 strmrename）====================

    _QUALITY_RE = re.compile(
        r"(?i)^(2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|sdr|dv|"
        r"web-?dl|webrip|bluray|blu-ray|remux|hdtv|"
        r"h\.?264|h\.?265|x264|x265|hevc|avc|10bit|aac|dts|ddp?5\.?1|"
        r"国语|粤语|中字|双语)$")

    _ANIME_SKIP_RE = re.compile(
        r"(?i)(raws|rip\b|studio|subs?\b|fansub|\d{3,4}p|x26[45]|hevc|avc|"
        r"flac|aac|ac-3|e-ac|opus|10bit|8bit|web-?dl|webrip|bdrip|bluray|"
        r"全集|特典|字幕|外挂|双语|menu|\bmkv\b|\bmp4\b|hi10p|ma10p|"
        r"bilibili|baha|b-global|\bcr\b|hdr|dovi|remux|\bMAX\b|\bNF\b|"
        r"\beng\b|\bchs\b|\bcht\b|\bjpn\b|\bjp\b|\bgb\b|\bbig5\b|"
        r"\bSP\b|\bOVA\b|\bCM\b)")

    def _parse_any_episode(self, stem: str) -> Optional[Tuple[int, str, Optional[int]]]:
        m = re.search(r"(?i)\bS(?P<s>\d{1,2})EP?(?P<e>\d{1,4})\b", stem)
        if m:
            ep = int(m.group("e"))
            if 0 < ep <= self._rn_max_episode:
                return ep, self._extract_tail(stem[m.end():]), int(m.group("s"))
        result = self._parse_episode(stem)
        if result:
            return result
        if guessit is not None:
            try:
                g = dict(guessit(stem, {"type": "episode", "single_value": True}))
                ep = g.get("episode")
                if isinstance(ep, list):
                    ep = ep[0] if ep else None
                if isinstance(ep, int) and 0 < ep <= self._rn_max_episode:
                    season = g.get("season")
                    if isinstance(season, list):
                        season = season[0] if season else None
                    if isinstance(season, int) and season > 50:
                        season = None
                    return ep, self._extract_tail_after(stem, ep), \
                        season if isinstance(season, int) else None
            except Exception as e:
                logger.debug(f"[{self.plugin_name}] guessit 解析失败: {e}")
        return None

    def _extract_tail_after(self, stem: str, ep: int) -> str:
        ep_str = str(ep)
        ep2 = f"{ep:02d}"
        for pat in (rf"(?i)S\d{{1,2}}EP?{ep2}\b", rf"(?i)\bEP?\.?{ep2}\b",
                    rf"(?i)\bEpisode\s*{ep_str}\b", rf"\[{ep_str}\]", rf"\[{ep2}\]"):
            m = re.search(pat, stem)
            if m:
                return self._extract_tail(stem[m.end():])
        return ""

    def _top_title_and_season(self, file_path: Path, root: Path) -> Tuple[str, int]:
        try:
            rel_parts = file_path.relative_to(root).parts
        except ValueError:
            return self._clean_title(file_path.parent.name), self._rn_default_season
        if len(rel_parts) < 2:
            return "", self._rn_default_season
        top = rel_parts[0]
        season = self._rn_default_season
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

    @staticmethod
    def _cn_num(s: str) -> int:
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
        match = re.match(r"^第\s*(?P<ep>\d{1,4})\s*[集话話]", text)
        if match:
            return self._finish(int(match.group("ep")), text[match.end():], season)
        match = re.match(r"^[Ee]?(?P<ep>\d{1,4})(?=$|[.\s_\-\[\]【】()])", text)
        if match:
            return self._finish(int(match.group("ep")), text[match.end():], season)
        patterns = [
            r"(?i)\bS\d{1,2}EP?(?P<ep>\d{1,4})\b",
            r"(?i)\bEP\.?(?P<ep>\d{1,4})\b",
            r"(?i)(?<![A-Za-z])E(?P<ep>\d{1,3})(?![0-9A-Za-z])",
            r"(?i)\bEpisode\s*(?P<ep>\d{1,4})\b",
            r"\[(?P<ep>\d{1,3})\]",
            r"(?:\s|^)-\s*(?P<ep>\d{1,3})(?=\s|\[|$)",
            r"\s(?P<ep>\d{1,3})(?=\s*[\[(])",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                return self._finish(int(m.group("ep")), text[m.end():], season)
        return None

    def _guess_season(self, text: str) -> Optional[int]:
        m = re.search(r"(?i)\bS(\d{1,2})EP?\d{1,4}\b", text)
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
        if episode <= 0 or episode > self._rn_max_episode:
            return None
        return episode, self._extract_tail(rest), season

    def _extract_tail(self, rest: str) -> str:
        if not self._rn_preserve_tail or not rest:
            return ""
        tokens: List[str] = []
        for tok in re.split(r"[.\s_\-\[\]【】()]+", rest):
            tok = tok.strip()
            if tok and self._QUALITY_RE.match(tok):
                tokens.append(tok)
        return ("." + ".".join(tokens)) if tokens else ""

    def _junk_kw_list(self) -> List[str]:
        raw = self._rn_junk_keywords.strip() or self._DEFAULT_JUNK
        kws: List[str] = []
        for part in raw.replace("，", "\n").replace(",", "\n").splitlines():
            kw = part.strip()
            if kw and kw not in kws:
                kws.append(kw)
        return kws

    def _has_episode_marker(self, stem: str) -> bool:
        return self._parse_episode(stem) is not None

    @staticmethod
    def _has_real_content(stem: str) -> bool:
        if re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", stem):
            return True
        if re.search(r"(?i)\b(?:2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd)\b", stem):
            return True
        return False

    def _is_junk(self, file_path: Path) -> bool:
        stem = file_path.stem
        name = file_path.name.lower()
        extras = ("[menu]", "映像特典", "音乐特典", "花絮", "预告片", "creditless",
                  ".ncop.", ".nced.", "ending ver", "review ver", "opening ver",
                  "preview ver", "[sp]", "[pv]", "[trailer]", "[logo]")
        for ex in extras:
            if ex in name:
                return True
        if self._has_episode_marker(stem) or self._has_real_content(stem):
            return False
        for kw in self._junk_kw_list():
            if kw.lower() in name:
                return True
        if self._rn_no_number_is_junk and not re.search(r"\d", stem):
            return True
        return False

    @staticmethod
    def _pick_anime_title(title: str) -> str:
        blocks = re.findall(r"\[([^\]]*)\]", title)
        outside = re.sub(r"\[[^\]]*\]", "|", title)
        outside_parts = [p.strip() for p in outside.split("|") if p.strip()]
        candidates = [b.strip() for b in blocks] + outside_parts
        best = ""
        best_score = (-1, -1, -1)
        for c in candidates:
            if not c:
                continue
            if re.fullmatch(r"\d{1,4}", c):
                continue
            if re.fullmatch(r"[\d.\-_]+", c):
                continue
            if MediaPipeline._ANIME_SKIP_RE.search(c):
                continue
            c2 = re.split(r"(?i)\s+S\d{1,2}\b|\s*\((?:19|20)\d{2}\)|\s+\d{1,3}-\d{1,3}\b", c)[0]
            c2 = re.sub(r"\s+", " ", c2).strip(" .-_·!&")
            if not c2 or len(c2) < 2:
                continue
            has_cjk = 1 if re.search(r"[\u4e00-\u9fff]", c2) else 0
            multiword = 1 if (" " in c2 or has_cjk) else 0
            score = (has_cjk, multiword, len(c2))
            if score > best_score:
                best_score = score
                best = c2
        return best

    @staticmethod
    def _clean_title(title: str) -> str:
        t = MediaPipeline._heuristic_clean_title(title)
        if t:
            return t
        if anitopy is not None:
            try:
                a = anitopy.parse(title) or {}
                cand = a.get("anime_title")
                if cand:
                    cand = MediaPipeline._post_clean_title(cand)
                    if cand:
                        return cand
            except Exception:
                pass
        if guessit is not None:
            try:
                g = dict(guessit(title))
                cand = g.get("title")
                if cand:
                    cand = MediaPipeline._post_clean_title(cand)
                    if cand:
                        return cand
            except Exception:
                pass
        return ""

    @staticmethod
    def _post_clean_title(t: str) -> str:
        t = re.sub(r"\[[^\]]*\]", " ", t)
        t = re.sub(r"【[^】]*】", " ", t)
        t = re.sub(r"\s+", " ", t).strip(" .-_·!&")
        t = re.sub(r"\s+(?:19|20)\d{2}$", "", t).strip()
        t = re.sub(r"\s*第\s*[0-9一二三四五六七八九十]+\s*季\s*$", "", t).strip()
        t = re.sub(r"(?i)\s*Season\s*\d+\s*$", "", t).strip()
        cjk = re.match(r"^([\u4e00-\u9fff0-9：·\s]+?)\s+[A-Za-z]", t)
        if cjk and re.search(r"[\u4e00-\u9fff]", cjk.group(1)):
            t = cjk.group(1).strip()
        return re.sub(r"\s+", " ", t).strip(" .-_·!&")

    @staticmethod
    def _heuristic_clean_title(title: str) -> str:
        if title.lstrip().startswith("["):
            cand = MediaPipeline._pick_anime_title(title)
            if cand:
                return cand
        t = title
        t = re.sub(r"【[^】]*】", " ", t)
        t = re.sub(r"\[[^\]]*\]", " ", t)
        t = re.sub(r"(?i)\b(?:www\.)?[a-z0-9-]+\.(?:com|net|cc|me|tv|xyz|org|cn)\b", " ", t)
        t = re.sub(r"\s+", " ", t).strip(" .-_·")
        cut = re.split(
            r"(?i)(?<=.)(?:\.|\s|_)(?:S\d{1,2}(?:EP?\d+)?|Season\b|(?:19|20)\d{2}\b|"
            r"2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|web-?dl|webrip|"
            r"bluray|blu-ray|remux|hdtv|x264|x265|h\.?264|h\.?265|hevc|"
            r"60fps|10bit)",
            t, maxsplit=1)
        t = cut[0] if cut else t
        t = t.strip(" .-_·")
        cjk = re.match(r"^([\u4e00-\u9fff0-9：·\s]+)", t)
        if cjk and re.search(r"[\u4e00-\u9fff]", cjk.group(1)):
            t = cjk.group(1)
        return re.sub(r"\s+", " ", t).strip(" .-_·")

    @staticmethod
    def _safe_name(name: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", name).strip()

    # ---- 报告 ----

    def _write_report(self, mode: str, summary: str,
                      details: List[Tuple[str, str, str, str]]) -> str:
        try:
            data_dir = self.get_data_path()
            ts = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y%m%d_%H%M%S")
            report = Path(data_dir) / f"rename_report_{ts}.txt"
            lines = [
                f"# 媒体入库流水线·步骤2 网盘改名清洗报告 ({mode})",
                f"# 时间: {ts}",
                f"# 电视剧目录: {self._rn_tv_paths} | 电影目录: {self._rn_movie_paths}",
                f"# {summary.splitlines()[0]}",
                "",
                "动作\t原因\t本地strm路径\t目标/说明",
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
        if not self._keep_reports or self._keep_reports <= 0:
            return
        try:
            reports = sorted(data_dir.glob("rename_report_*.txt"),
                             key=lambda p: p.stat().st_mtime, reverse=True)
            for old in reports[self._keep_reports:]:
                try:
                    old.unlink()
                except OSError:
                    pass
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 清理旧报告失败: {e}")

    # ==================== 配置界面 ====================

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    # 总开关
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VSwitch", "enabled", "启用插件"),
                            self._col(6, "VSwitch", "notify", "发送通知"),
                            self._col(6, "VSwitch", "run_once",
                                      "立即执行一次 (保存后生效，随后自动关闭)"),
                            self._col(6, "VTextField", "cron",
                                      "Cron 定时 (可选)", placeholder="0 */6 * * *"),
                        ],
                    },
                    # 兜底 / 通知路由
                    {
                        "component": "VRow",
                        "content": [
                            self._col(4, "VSwitch", "stop_on_error", "出错/超时即中止后续步骤"),
                            self._col(4, "VTextField", "step_timeout_min",
                                      "单步超时(分钟,0=不限)", placeholder="0"),
                            self._select(4, "notify_type", "通知类型(对应 MP 渠道)",
                                         [("插件", "Plugin"), ("整理入库", "Organize"),
                                          ("媒体服务器", "MediaServer"),
                                          ("站点", "SiteMessage"), ("其它", "Other")]),
                        ],
                    },
                    # 步骤开关
                    {
                        "component": "VRow",
                        "content": [
                            self._col(3, "VSwitch", "do_scan", "步骤1 OpenList 扫描"),
                            self._col(3, "VSwitch", "do_rename", "步骤2 网盘改名清洗"),
                            self._col(3, "VSwitch", "do_transfer", "步骤3 增量整理刮削"),
                            self._col(3, "VSwitch", "do_emby", "步骤4 Emby 全库扫描"),
                        ],
                    },
                    # OpenList（步骤1/2 共用）
                    self._subtitle("OpenList 连接（步骤1 扫描 / 步骤2 改名 共用）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "openlist_url",
                                      "OpenList 地址", placeholder="http://192.168.1.111:5244"),
                            self._col(6, "VTextField", "openlist_token",
                                      "OpenList Token", placeholder="openlist-xxxxxx"),
                        ],
                    },
                    # === 步骤1 ===
                    self._subtitle("步骤1 · OpenList 扫描（生成 STRM）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(12, "VTextarea", "scan_path",
                                      "扫描路径 (OpenList 挂载路径，多个换行)",
                                      placeholder="/TV\n/Movie", rows=2, autoGrow=True),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(4, "VTextField", "scan_limit",
                                      "节流强度 (越大越慢)", placeholder="20"),
                            self._col(4, "VTextField", "scan_recent_days",
                                      "扫描增量天数 (0=全量)", placeholder="0"),
                            self._col(4, "VTextField", "scan_timeout",
                                      "超时秒数 (0=不限)", placeholder="0"),
                        ],
                    },
                    # === 步骤2 ===
                    self._subtitle("步骤2 · 网盘改名清洗（改 115 源名，防 insert 复活；强烈建议先预演）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextarea", "rn_tv_paths",
                                      "电视剧 strm 目录 (含集号→按一级目录名改 SxxExx；多个换行)",
                                      placeholder="/media/TV", rows=2, autoGrow=True),
                            self._col(6, "VTextarea", "rn_movie_paths",
                                      "电影 strm 目录 (只清垃圾，不改名；多个换行)",
                                      placeholder="/media/Movie", rows=2, autoGrow=True),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(4, "VSwitch", "rn_dry_run", "预演模式 (只出报告，不改 115)"),
                            self._col(4, "VSwitch", "rn_recursive", "递归子目录"),
                            self._col(4, "VSwitch", "rn_clean_dirs",
                                      "清洗一级目录名 (去广告前缀，先做)"),
                            self._col(4, "VSwitch", "rn_preserve_tail", "保留清晰度等后缀"),
                            self._col(4, "VSwitch", "rn_clean_junk", "清垃圾 (广告/引流/花絮)"),
                            self._col(4, "VSwitch", "rn_no_number_is_junk",
                                      "无数字即垃圾 (文件名无任何数字→删)"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(3, "VTextField", "rn_default_season", "默认季数", placeholder="1"),
                            self._col(3, "VTextField", "rn_max_episode", "最大集数", placeholder="500"),
                            self._col(6, "VTextField", "rn_template", "重命名模板 (不含扩展名)",
                                      placeholder="{title}.S{season:02d}E{episode:02d}{tail}"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(3, "VTextField", "rn_after_date",
                                      "仅此日期后 (YYYY-MM-DD，优先)", placeholder="2026-06-12"),
                            self._col(3, "VTextField", "rn_recent_days",
                                      "仅最近 N 天 (0=全量)", placeholder="0"),
                            self._col(3, "VTextField", "keep_reports",
                                      "保留报告份数 (0=不清)", placeholder="10"),
                            self._col(3, "VTextField", "container",
                                      "MP 容器名 (生成下载命令)", placeholder="moviepilot-v2"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(12, "VTextarea", "rn_junk_keywords",
                                      "垃圾关键字 (换行/逗号分隔，留空用内置默认)",
                                      placeholder="更多原盘请访问\n全球首发\nmp4kan.com", rows=2, autoGrow=True),
                        ],
                    },
                    self._subtitle("步骤2 · 115 防风控节奏（只作用于 rename/remove 写操作；预演不触发）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(3, "VTextField", "rl_min", "写间隔最小(秒)", placeholder="2"),
                            self._col(3, "VTextField", "rl_max", "写间隔最大(秒)", placeholder="5"),
                            self._col(3, "VTextField", "rl_batch", "每N个长停一次", placeholder="30"),
                            self._col(3, "VSwitch", "rl_shuffle", "打乱处理顺序"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(4, "VTextField", "rl_pause_min", "长停最小(秒)", placeholder="60"),
                            self._col(4, "VTextField", "rl_pause_max", "长停最大(秒)", placeholder="120"),
                            self._col(4, "VTextField", "rl_max_ops",
                                      "单次写操作上限 (到顶即停)", placeholder="300"),
                        ],
                    },
                    # === 步骤3 ===
                    self._subtitle("步骤3 · 增量整理刮削"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextarea", "src_paths",
                                      "源目录 (MP 容器内路径，多个换行)",
                                      placeholder="/media/TV\n/media/Movie", rows=2, autoGrow=True),
                            self._col(4, "VTextField", "recent_days",
                                      "整理增量天数 (0=全量)", placeholder="3"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._select(4, "mtype", "媒体类型",
                                         [("自动", ""), ("电影", "电影"), ("电视剧", "电视剧")]),
                            self._select(4, "transfer_type", "整理方式",
                                         [("自动(默认)", ""), ("复制", "copy"), ("移动", "move"),
                                          ("硬链接", "link"), ("软链接", "softlink")]),
                            self._select(4, "transfer_unit", "整理单元",
                                         [("按一级目录", "folder"), ("按单个文件", "file")]),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._select(4, "scrape", "刮削元数据",
                                         [("强制刮削", "on"), ("不刮削", "off"),
                                          ("跟随 MP 设置", "default")]),
                            self._col(4, "VTextField", "min_filesize", "最小文件(MB)", placeholder="0"),
                            self._col(4, "VTextField", "target_path",
                                      "目标路径 (留空=媒体库默认)", placeholder="/media/moviepilot/TV"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._select(6, "type_folder", "按类型分类 (电影/电视剧 子目录)",
                                         [("跟随 MP 设置", "default"), ("开启", "on"), ("关闭", "off")]),
                            self._select(6, "category_folder", "按类别分类 (动画/纪录片 等)",
                                         [("跟随 MP 设置", "default"), ("开启", "on"), ("关闭", "off")]),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextField", "media_exts",
                                      "媒体扩展名 (逗号分隔，留空用默认含 strm)",
                                      placeholder=self._DEFAULT_EXTS),
                            self._col(2, "VSwitch", "force", "强制整理"),
                            self._col(2, "VSwitch", "fast_prune", "快速模式"),
                        ],
                    },
                    # === 步骤4 ===
                    self._subtitle("步骤4 · Emby 全库扫描（整理完通知 Emby 刷新入库）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "emby_host",
                                      "Emby 地址", placeholder="http://192.168.1.126:8096"),
                            self._col(6, "VTextField", "emby_apikey",
                                      "Emby API Key", placeholder="Emby 后台生成的 API Key"),
                        ],
                    },
                    # 说明
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
                                            "type": "info", "variant": "tonal",
                                            "text": "四步顺序执行，各步独立开关。"
                                            "步骤2 是核心：读本地 strm 解析 115 源路径，调 OpenList "
                                            "/api/fs/rename 改【115 源名】(而非本地 strm，避免 insert 复活)，"
                                            "并调 /api/fs/remove 清垃圾；改完自动把本地 strm 同步改名+改内容URL，"
                                            "无需再触发一次扫描。对 115 的写操作有拟人化限速(随机间隔+批次长停+"
                                            "单次上限+失败退避)防风控，预演模式不触发写操作。"
                                            "强烈建议：步骤2 先只开预演跑一遍，docker cp 下载报告核对，"
                                            "确认无误再关预演小批量实跑。"
                                            "扫描路径填 OpenList 挂载路径(如 /TV)；改名/整理目录填 MP 容器内路径"
                                            "(如 /media/TV)。发布站前缀等可配合 MP 自定义识别词兜底。"
                                            "详见 docs/strm-pipeline-redesign.md。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False, "notify": True, "notify_type": "Plugin",
            "run_once": False, "cron": "", "stop_on_error": True, "step_timeout_min": 0,
            "do_scan": True, "do_rename": False, "do_transfer": True, "do_emby": False,
            "openlist_url": "", "openlist_token": "",
            "scan_path": "", "scan_limit": 20, "scan_recent_days": 0, "scan_timeout": 0,
            "rn_tv_paths": "/media/TV", "rn_movie_paths": "/media/Movie",
            "rn_recursive": True, "rn_dry_run": True, "rn_clean_dirs": False,
            "rn_default_season": 1, "rn_max_episode": 500, "rn_preserve_tail": True,
            "rn_clean_junk": True, "rn_no_number_is_junk": True, "rn_junk_keywords": "",
            "rn_recent_days": 0, "rn_after_date": "",
            "rn_template": "{title}.S{season:02d}E{episode:02d}{tail}",
            "keep_reports": 10, "container": "moviepilot-v2",
            "rl_min": 2.0, "rl_max": 5.0, "rl_batch": 30,
            "rl_pause_min": 60.0, "rl_pause_max": 120.0, "rl_max_ops": 300, "rl_shuffle": True,
            "src_paths": "", "recent_days": 3, "transfer_unit": "folder",
            "transfer_type": "", "mtype": "", "target_path": "", "scrape": "on",
            "type_folder": "default", "category_folder": "default", "min_filesize": 0,
            "force": False, "fast_prune": False, "media_exts": "",
            "emby_host": "", "emby_apikey": "",
        }

    @staticmethod
    def _col(cols: int, comp: str, model: str, label: str, **props) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{"component": comp, "props": {"model": model, "label": label, **props}}],
        }

    @staticmethod
    def _select(cols: int, model: str, label: str, options: List[Tuple[str, str]]) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [{
                "component": "VSelect",
                "props": {"model": model, "label": label,
                          "items": [{"title": t, "value": v} for t, v in options]},
            }],
        }

    @staticmethod
    def _subtitle(text: str) -> dict:
        return {
            "component": "VRow",
            "content": [{
                "component": "VCol",
                "props": {"cols": 12},
                "content": [{
                    "component": "VAlert",
                    "props": {"type": "success", "variant": "tonal",
                              "density": "compact", "text": text},
                }],
            }],
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
