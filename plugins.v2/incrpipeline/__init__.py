"""
增量入库流水线 - MoviePilot V2 插件

把「OpenList 扫描触发器」+「增量整理刮削」两个插件合并成一条流水线，一次触发顺序执行：

  第一步（可选）：递归遍历 OpenList 挂载路径（/api/fs/list，refresh=true），
                 触发 Strm 懒生成，等价于把每个文件夹自动点开一遍。
  第二步（可选）：按文件 mtime 只挑最近 N 天新增/改动的媒体，调 MP 手动整理 + 刮削。

为什么合并：
  实际使用里几乎总是「先增量生成 STRM，再增量整理刮削」。分成两个插件要点两次，
  还得手动等第一步跑完再点第二步。合并后一次点击/一条命令/一个 Cron 顺序跑完，
  第一步是同步阻塞的（列目录完才算 STRM 落地），结束后自动接第二步，无需人工等待。

两步各有独立开关（执行OpenList扫描 / 执行整理刮削），可以只开其中一个，
等价于单独使用原来的某一个插件。

配置会被 MP 自动保存，下次打开插件配置页即显示上次的设置，改一点保存即可。

注意：
  - 扫描路径填 OpenList 挂载路径（虚拟路径，如 /云下载），不是 115 源路径。
  - 源目录/目标路径填 MP 容器内路径（如 /media/云下载），不是宿主机路径。
"""
import os
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


class IncrPipeline(_PluginBase):
    # 插件元数据
    plugin_name = "增量入库流水线"
    plugin_desc = "一次触发顺序执行 OpenList 扫描生成 STRM + 增量整理刮削，两步各有独立开关"
    plugin_icon = "workflow.png"
    plugin_version = "1.2.0"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "incrpipeline_"
    plugin_order = 23
    auth_level = 1

    # 默认媒体扩展名（含 strm）
    _DEFAULT_EXTS = "strm,mkv,mp4,ts,m2ts,iso,avi,mov,wmv,rmvb,flv,m4v,mpg,mpeg"

    # ---- 总开关 ----
    _enabled: bool = False
    _notify: bool = True
    _notify_type: str = "Plugin"   # 通知类型，对应 MP 通知渠道路由
    _run_once: bool = False
    _cron: str = ""

    # ---- 兜底 ----
    # 单步超时（分钟）：任一步骤超过此时长视为超时；0=不限（适合刮削/生成STRM要1-2小时）
    _step_timeout_min: int = 0
    # 出错/超时即中止后续步骤（兜底）
    _stop_on_error: bool = True

    # ---- 步骤开关 ----
    _do_scan: bool = True       # 第一步：OpenList 扫描
    _do_transfer: bool = True   # 第二步：增量整理刮削
    _do_emby: bool = False      # 第三步：触发 Emby 媒体库扫描（全量）

    # ---- 第三步：Emby 媒体库扫描参数 ----
    _emby_host: str = ""        # 如 http://192.168.1.126:8096
    _emby_apikey: str = ""

    # ---- 第一步：OpenList 扫描参数 ----
    _openlist_url: str = ""
    _openlist_token: str = ""
    _scan_path: str = ""          # OpenList 挂载路径，多个换行
    _scan_limit: int = 20         # 节流：每列一层后 sleep = scan_limit * 0.1 秒
    _scan_timeout: int = 0        # 总超时秒；0=不限
    _scan_recent_days: int = 0    # 扫描增量天数：0=全量遍历

    # ---- 第二步：整理刮削参数 ----
    _src_paths: str = ""          # MP 容器内源目录，多个换行
    _recent_days: int = 3         # 整理增量天数：0=全量
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

    _scheduler: Optional[BackgroundScheduler] = None

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
            self._do_transfer = config.get("do_transfer", True)
            self._do_emby = config.get("do_emby", False)
            self._emby_host = (config.get("emby_host") or "").rstrip("/")
            self._emby_apikey = config.get("emby_apikey", "")

            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token", "")
            self._scan_path = config.get("scan_path") or ""
            self._scan_limit = int(config.get("scan_limit") or 20)
            _to = config.get("scan_timeout")
            self._scan_timeout = int(_to) if str(_to).strip() not in ("", "None") else 0
            self._scan_recent_days = int(config.get("scan_recent_days") or 0)

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

        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次流水线")
            self._scheduler.add_job(
                self._run_task,
                "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
            )
            # 执行后关掉开关，避免重启又触发；其余配置原样保存
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "notify_type": self._notify_type,
            "run_once": self._run_once,
            "cron": self._cron,
            "step_timeout_min": self._step_timeout_min,
            "stop_on_error": self._stop_on_error,
            "do_scan": self._do_scan,
            "do_transfer": self._do_transfer,
            "do_emby": self._do_emby,
            "emby_host": self._emby_host,
            "emby_apikey": self._emby_apikey,
            "openlist_url": self._openlist_url,
            "openlist_token": self._openlist_token,
            "scan_path": self._scan_path,
            "scan_limit": self._scan_limit,
            "scan_timeout": self._scan_timeout,
            "scan_recent_days": self._scan_recent_days,
            "src_paths": self._src_paths,
            "recent_days": self._recent_days,
            "transfer_unit": self._transfer_unit,
            "transfer_type": self._transfer_type,
            "mtype": self._mtype,
            "target_path": self._target_path,
            "scrape": self._scrape,
            "type_folder": self._type_folder,
            "category_folder": self._category_folder,
            "min_filesize": self._min_filesize,
            "force": self._force,
            "fast_prune": self._fast_prune,
            "media_exts": self._media_exts,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/incr_pipeline",
                "event": EventType.PluginAction,
                "desc": "执行一次增量入库流水线 (扫描+整理刮削)",
                "category": "整理",
                "data": {"action": "incr_pipeline"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/run",
                "endpoint": self._api_run,
                "methods": ["GET", "POST"],
                "summary": "执行增量入库流水线",
                "description": "顺序执行 OpenList 扫描 + 增量整理刮削（按步骤开关）。",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [
                    {
                        "id": "IncrPipelineCron",
                        "name": "增量入库流水线定时任务",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self._run_task,
                        "kwargs": {},
                    }
                ]
            except Exception as e:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {e}")
        return []

    # ---------- 事件 / HTTP 入口 ----------

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "incr_pipeline":
            return
        logger.info(f"[{self.plugin_name}] 收到远程命令，开始执行")
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_run(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发流水线，详情见 MP 日志"}

    # ---------- 工具 ----------

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

    # ---------- 主流程 ----------

    def _run_step(self, name: str, func) -> Tuple[bool, str]:
        """
        在子线程里执行单个步骤函数，套上「单步超时」兜底。
        func 返回 (ok: bool, summary: str)。
        返回同样的 (ok, summary)；超时则 ok=False 且 summary 标注超时。
        注意：超时只是「不再等待、判失败、中止后续」，已经在跑的后台线程
        （如 MP 队列、OpenList 列目录）无法强杀，但不会再阻塞流水线。
        """
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

        t = threading.Thread(target=_worker, name=f"incrpipeline-{name}", daemon=True)
        start = time.time()
        t.start()
        timeout = self._step_timeout_min * 60 if self._step_timeout_min > 0 else None
        t.join(timeout)
        if t.is_alive():
            # 超时：不再等待
            mins = self._step_timeout_min
            msg = (f"超时：超过 {mins} 分钟仍未完成，已停止等待"
                   f"（该步骤后台可能仍在跑，但流水线不再阻塞）")
            logger.warning(f"[{self.plugin_name}] {name} {msg}")
            return False, msg
        elapsed = int(time.time() - start)
        ok = result.get("ok", False)
        summary = result.get("summary", "无返回")
        return ok, f"{summary}（耗时 {elapsed} 秒）"

    def _run_task(self):
        """
        流水线：第一步 OpenList 扫描 → 第二步增量整理刮削（同步等待完成）→ 第三步 Emby 扫描。
        - 每步串行，前一步真正结束才进入下一步（方案A，整理同步等待入库完成）。
        - 兜底：单步超时 / 出错时，按「出错即中止」开关决定是否继续后续步骤。
        """
        if not self._do_scan and not self._do_transfer and not self._do_emby:
            self._send_notify("流水线未执行", "三个步骤开关都关闭了，没有可执行的步骤")
            return

        summary_parts: List[str] = []
        aborted = False

        # ===== 第一步：OpenList 扫描 =====
        if self._do_scan:
            ok, summary = self._run_step("OpenList 扫描", self._run_scan)
            summary_parts.append(f"【第一步 OpenList 扫描】{'✅' if ok else '❌'}\n{summary}")
            if not ok and self._stop_on_error:
                summary_parts.append("⚠️ 第一步未成功，已按「出错即中止」停止后续步骤")
                aborted = True
        else:
            summary_parts.append("【第一步 OpenList 扫描】已跳过（开关关闭）")

        # ===== 第二步：增量整理刮削（同步等待整理完成）=====
        if not aborted and self._do_transfer:
            ok, summary = self._run_step("增量整理刮削", self._run_transfer)
            summary_parts.append(f"【第二步 增量整理刮削】{'✅' if ok else '❌'}\n{summary}")
            if not ok and self._stop_on_error:
                summary_parts.append("⚠️ 第二步未成功，已按「出错即中止」停止后续步骤")
                aborted = True
        elif not self._do_transfer:
            summary_parts.append("【第二步 增量整理刮削】已跳过（开关关闭）")

        # ===== 第三步：触发 Emby 媒体库全量扫描 =====
        if not aborted and self._do_emby:
            ok, summary = self._run_step("Emby 媒体库扫描", self._run_emby_scan)
            summary_parts.append(f"【第三步 Emby 媒体库扫描】{'✅' if ok else '❌'}\n{summary}")
        elif not self._do_emby:
            summary_parts.append("【第三步 Emby 媒体库扫描】已跳过（开关关闭）")

        title = "增量入库流水线完成" if not aborted else "增量入库流水线中止（含失败）"
        self._send_notify(title, "\n\n".join(summary_parts))

    # ---------- 第一步：OpenList 扫描 ----------

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
            if ok:
                any_ok = True
                results.append(f"✓ {msg}")
            else:
                results.append(f"✗ {sp}: {msg}")
        summary = "\n".join(results)
        logger.info(f"[{self.plugin_name}] 扫描结束：\n{summary}")
        return any_ok, summary

    def _recursive_scan(self, scan_path: str) -> Tuple[bool, str]:
        start = time.time()
        dir_count = 0
        file_count = 0
        skipped = 0
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
            # 先 GET 当前目录，触发 Strm 驱动为该目录生成 strm 文件
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
                        f"：子目录 {sub_dirs} 个，文件 {file_count} 个，待扫 {len(stack)} 个，本层共 {len(entries)} 条")
            if throttle:
                time.sleep(throttle)

        elapsed = int(time.time() - start)
        extra = f"，跳过旧目录 {skipped} 个" if cutoff is not None else ""
        mode = f"增量({self._scan_recent_days}天)" if cutoff is not None else "全量"

        if timed_out:
            remain = len(stack)
            msg = (f"路径: {scan_path}（{mode}）扫描中断：超过 {self._scan_timeout} 秒，"
                   f"已遍历目录 {dir_count} 个、文件 {file_count} 个{extra}，"
                   f"还剩 {remain} 个目录未扫。建议调大「超时秒数」或设为 0（不限时）。")
            logger.warning(f"[{self.plugin_name}] {msg}")
            return False, msg
        if failed:
            msg = (f"路径: {scan_path}（{mode}）扫描完成但有遗漏：已遍历目录 "
                   f"{dir_count} 个、文件 {file_count} 个{extra}，"
                   f"{len(failed)} 个目录列举失败，耗时 {elapsed} 秒。失败示例：{failed[0]}")
            logger.warning(f"[{self.plugin_name}] {msg}")
            return False, msg

        msg = (f"路径: {scan_path}（{mode}），已遍历目录 {dir_count} 个、"
               f"文件 {file_count} 个{extra}，耗时 {elapsed} 秒")
        logger.info(f"[{self.plugin_name}] 递归扫描完成，{msg}")
        return True, msg

    @staticmethod
    def _parse_time(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            txt = s.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
            return dt.astimezone(pytz.utc)
        except Exception:
            return None

    def _get_path(self, path: str) -> bool:
        """调用 /api/fs/get 触发 OpenList Strm 驱动为该路径生成 strm 文件。"""
        url = f"{self._openlist_url}/api/fs/get"
        headers = {
            "Authorization": self._openlist_token,
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(url, headers=headers,
                                 json={"path": path, "refresh": False},
                                 timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            ok = data.get("code") == 200
            if not ok:
                logger.warning(f"[{self.plugin_name}] get {path} 返回 {data.get('code')}: {data.get('message')}")
            return ok
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] get {path} 异常: {e}")
            return False

    def _list_dir(self, path: str) -> Tuple[bool, List[dict], str]:
        url = f"{self._openlist_url}/api/fs/list"
        headers = {
            "Authorization": self._openlist_token,
            "Content-Type": "application/json",
        }
        payload = {"path": path, "page": 1, "per_page": 10000, "refresh": True}
        last_err = ""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json() or {}
                code = data.get("code")
                if code != 200:
                    last_err = f"OpenList 返回 {code}: {data.get('message')}"
                    if attempt < max_attempts:
                        time.sleep(attempt * 2)
                        continue
                    return False, [], last_err
                raw_data = data.get("data") or {}
                content = raw_data.get("content") or []
                total = raw_data.get("total", "?")
                logger.info(f"[{self.plugin_name}] 列目录 {path} 返回 total={total} content={len(content)} 条")
                return True, content, ""
            except Exception as e:
                last_err = str(e)
                if attempt < max_attempts:
                    logger.warning(
                        f"[{self.plugin_name}] 列目录 {path} 第 {attempt} 次失败，"
                        f"{attempt * 2}s 后重试：{last_err}")
                    time.sleep(attempt * 2)
                    continue
        return False, [], last_err


    # ---------- 第三步：Emby 媒体库扫描 ----------

    def _run_emby_scan(self) -> Tuple[bool, str]:
        """触发 Emby 全库扫描：POST /Library/Refresh（Emby 会异步扫描所有媒体库）。"""
        if not self._emby_host or not self._emby_apikey:
            msg = "Emby 地址或 API Key 未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg
        url = f"{self._emby_host}/Library/Refresh"
        try:
            resp = requests.post(url, params={"api_key": self._emby_apikey}, timeout=30)
            # Emby 成功返回 204 No Content
            if resp.status_code in (200, 204):
                msg = "已触发 Emby 全库扫描（Emby 后台异步执行，稍后入库）"
                logger.info(f"[{self.plugin_name}] {msg}")
                return True, msg
            msg = f"Emby 返回 HTTP {resp.status_code}: {resp.text[:200]}"
            logger.error(f"[{self.plugin_name}] 触发 Emby 扫描失败：{msg}")
            return False, msg
        except Exception as e:
            msg = f"触发 Emby 扫描异常：{e}"
            logger.error(f"[{self.plugin_name}] {msg}")
            return False, msg

    # ---------- 第二步：增量整理刮削 ----------

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
            # 没有新内容不算失败，源目录不存在才算告警
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
        # 有失败项或扫描告警则视为未完全成功
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
                dirnames[:] = [
                    d for d in dirnames
                    if not d.startswith(".") and d not in ("@Recycle", "#recycle", "@eaDir")
                ]

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
                    top = root / rel_parts[0]
                    targets[str(top)] = "dir"

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
                    storage="local",
                    type=ftype,
                    path=str(p),
                    name=p.name,
                    basename=p.stem if ftype == "file" else p.name,
                    extension=(p.suffix.lstrip(".") if ftype == "file" else ""),
                    size=0,
                )
                logger.info(f"[{self.plugin_name}] 整理 [{ftype}] {p}")
                ok, msg = chain.manual_transfer(
                    fileitem=fileitem,
                    target_path=target,
                    mtype=mtype,
                    transfer_type=ttype,
                    scrape=scrape,
                    library_type_folder=type_folder,
                    library_category_folder=category_folder,
                    min_filesize=self._min_filesize,
                    force=self._force,
                    background=False,  # 方案A：同步等待整理+刮削真正完成再返回
                )
                if ok:
                    ok_list.append(str(p))
                else:
                    logger.error(f"[{self.plugin_name}] 整理失败 {p}: {msg}")
                    fail_list.append(f"{p.name}: {msg}")
            except Exception as e:
                logger.error(f"[{self.plugin_name}] 整理异常 {p}: {e}")
                fail_list.append(f"{p.name}: {e}")

        return ok_list, fail_list

    def _notify_type_enum(self):
        """把配置的通知类型字符串映射为 MP 的 NotificationType 枚举。"""
        try:
            from app.schemas.types import NotificationType as NT
            return getattr(NT, self._notify_type, NT.Plugin)
        except Exception:
            return NotificationType.Plugin

    def _send_notify(self, title: str, text: str):
        if not self._notify:
            return
        try:
            self.post_message(
                mtype=self._notify_type_enum(),
                title=f"【{self.plugin_name}】{title}",
                text=text,
            )
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 发送通知失败: {e}")

    # ---------- 配置界面 ----------

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
                            self._col(4, "VSwitch", "stop_on_error",
                                      "出错/超时即中止后续步骤"),
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
                            self._col(4, "VSwitch", "do_scan",
                                      "第一步：OpenList 扫描"),
                            self._col(4, "VSwitch", "do_transfer",
                                      "第二步：增量整理刮削"),
                            self._col(4, "VSwitch", "do_emby",
                                      "第三步：Emby 全库扫描"),
                        ],
                    },
                    # === 第一步标题 ===
                    self._subtitle("第一步 · OpenList 扫描（生成 STRM）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "openlist_url",
                                      "OpenList 地址",
                                      placeholder="http://192.168.1.111:5244"),
                            self._col(6, "VTextField", "openlist_token",
                                      "OpenList Token", placeholder="openlist-xxxxxx"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(12, "VTextarea", "scan_path",
                                      "扫描路径 (OpenList 挂载路径，多个换行)",
                                      placeholder="/云下载\n/TV", rows=2, autoGrow=True),
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
                    # === 第二步标题 ===
                    self._subtitle("第二步 · 增量整理刮削"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextarea", "src_paths",
                                      "源目录 (MP 容器内路径，多个换行)",
                                      placeholder="/media/云下载\n/media/TV",
                                      rows=2, autoGrow=True),
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
                                         [("自动(默认)", ""), ("复制", "copy"),
                                          ("移动", "move"), ("硬链接", "link"),
                                          ("软链接", "softlink")]),
                            self._select(4, "transfer_unit", "整理单元",
                                         [("按一级目录", "folder"), ("按单个文件", "file")]),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._select(4, "scrape", "刮削元数据",
                                         [("强制刮削", "on"),
                                          ("不刮削", "off"),
                                          ("跟随 MP 设置(填了目标路径=不刮削)", "default")]),
                            self._col(4, "VTextField", "min_filesize",
                                      "最小文件大小(MB)", placeholder="0"),
                            self._col(4, "VTextField", "target_path",
                                      "目标路径 (留空=媒体库默认)",
                                      placeholder="/media/整理后"),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._select(6, "type_folder", "按类型分类 (目标路径下建 电影/电视剧 子目录)",
                                         [("跟随 MP 设置", "default"),
                                          ("开启", "on"), ("关闭", "off")]),
                            self._select(6, "category_folder", "按类别分类 (目标路径下建 动画/纪录片 等子目录)",
                                         [("跟随 MP 设置", "default"),
                                          ("开启", "on"), ("关闭", "off")]),
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
                    # === 第三步标题 ===
                    self._subtitle("第三步 · Emby 媒体库扫描（全量，整理完通知 Emby 刷新入库）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "emby_host",
                                      "Emby 地址",
                                      placeholder="http://192.168.1.126:8096"),
                            self._col(6, "VTextField", "emby_apikey",
                                      "Emby API Key",
                                      placeholder="Emby 后台生成的 API Key"),
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
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "流水线顺序执行：先 OpenList 扫描"
                                            "（递归列目录触发 STRM 懒生成），完成后增量整理"
                                            "刮削（同步等待真正整理+刮削入库完成再继续），"
                                            "最后可选触发 Emby 全库扫描。三步各有开关。"
                                            "兜底：单步超时(分钟)到了不再等待、判失败；"
                                            "开「出错/超时即中止」时任一步失败就停止后续；"
                                            "超时填 0=不限（刮削/生成STRM 要 1-2 小时也不会被掐）。"
                                            "通知走 MP 自身通知系统，按「通知类型」路由到你在"
                                            "MP 后台配置的渠道，不用在插件里另配 webhook。"
                                            "扫描路径填 OpenList 挂载路径（如 /云下载）；"
                                            "源目录/目标路径填 MP 容器内路径（如 /media/云下载）。"
                                            "填了「目标路径」时刮削必须选「强制刮削」才会下"
                                            "元数据/图片（MP 的行为）。「按类型/按类别分类」"
                                            "建议和 MP 手动整理保持一致。配置会自动保存，"
                                            "下次打开即显示上次设置。"
                                            "可通过 Web 按钮、远程命令 /incr_pipeline、"
                                            "Webhook 或 Cron 触发。",
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
            "notify_type": "Plugin",
            "run_once": False,
            "cron": "",
            "stop_on_error": True,
            "step_timeout_min": 0,
            "do_scan": True,
            "do_transfer": True,
            "do_emby": False,
            "emby_host": "",
            "emby_apikey": "",
            "openlist_url": "",
            "openlist_token": "",
            "scan_path": "",
            "scan_limit": 20,
            "scan_recent_days": 0,
            "scan_timeout": 0,
            "src_paths": "",
            "recent_days": 3,
            "mtype": "",
            "transfer_type": "",
            "transfer_unit": "folder",
            "scrape": "on",
            "type_folder": "default",
            "category_folder": "default",
            "min_filesize": 0,
            "target_path": "",
            "media_exts": "",
            "force": False,
            "fast_prune": False,
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

    @staticmethod
    def _select(cols: int, model: str, label: str,
                options: List[Tuple[str, str]]) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [
                {
                    "component": "VSelect",
                    "props": {
                        "model": model,
                        "label": label,
                        "items": [{"title": t, "value": v} for t, v in options],
                    },
                }
            ],
        }

    @staticmethod
    def _subtitle(text: str) -> dict:
        """分隔小标题行"""
        return {
            "component": "VRow",
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12},
                    "content": [
                        {
                            "component": "VAlert",
                            "props": {
                                "type": "success",
                                "variant": "tonal",
                                "density": "compact",
                                "text": text,
                            },
                        }
                    ],
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
