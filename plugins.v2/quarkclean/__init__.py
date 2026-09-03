"""
夸克改名清洗 (QuarkClean) - MoviePilot V2 插件

给「夸克网盘 → OpenList Strm 存储」这条链路补上与 115 相同的改名清洗能力，
让 MoviePilot 能正确识别刮削。逻辑与 mediapipeline 步骤2 同源，但独立成插件、
只作用于夸克的 strm 目录，与 115 主流程互不干扰：

  1. 读本地 strm（/media/<夸克strm目录>），从 strm 内容 URL 解析夸克源路径
     （即 /d/ 之后那段，如 /quark/电视剧/某剧/001.mkv）。
  2. 调 OpenList /api/fs/rename 改【夸克源名】：裸集号 → 剧名.SxxExx（标题取
     相对扫描根的一级目录名）；调 /api/fs/remove 删广告/花絮垃圾；
     可选清洗一级目录名（去广告前缀 → 剧名 (年份)）。
  3. 同步把本地 strm 改名 + 重写内容 URL（逐段编码），无需再触发 OpenList 扫描。

为什么改「夸克源名」而不是只改本地 strm：
  OpenList 的 Strm 存储是 insert 模式（只增不删），改本地 strm 会被下次扫描按原始
  源名复活，导致每集堆两份。改夸克源名后源头就干净，strm 天生规范、不复活。

注意：
  - 目录填 MP 容器内路径（如 /media/quark）；本插件运行在 moviepilot-v2 容器内。
    ⚠️ 本地目录名取自 Strm 存储「源 paths 的末段」，不是 mount_path：
    夸克存储 mount_path=/QuarkStrm 但 paths=/quark，所以本地目录是 /home/115strm/quark
    → 容器内 /media/quark（115 各存储只是恰好两者同名才没暴露这个规则）。
  - OpenList Token 需要管理员令牌（/api/fs/rename、/api/fs/remove 是管理接口）。
  - 夸克网盘若用「夸克TV」驱动（扫码登录的那种）不支持改名/删除，本插件写操作会
    全部失败；必须用普通「夸克网盘 (Quark)」cookie 驱动。
  - 强烈建议先开「预演」，看明细报告确认无误，再关预演小批量实跑。
  - 混放内容（电视剧+电影同目录）：填进「电视剧目录」即可——含集号的文件改名，
    电影文件（无集号）自动跳过，留给 MP 自己识别。
  - 对夸克的写操作有拟人化限速（随机间隔+批次长停+单次上限+失败退避）防风控。
"""
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


class QuarkClean(_PluginBase):
    # 插件元数据
    plugin_name = "夸克改名清洗"
    plugin_desc = "读本地夸克strm→OpenList改夸克源名(裸集号补SxxExx)+清垃圾+目录名清洗，防insert复活，含预演与防风控"
    plugin_icon = "edit.png"
    plugin_version = "1.0.1"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "quarkclean_"
    plugin_order = 26
    auth_level = 1

    # 内置垃圾关键字（整段广告短语/明确花絮标记，绝不用裸 TLD/短词，
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

    # 附属子目录：位于这些子目录里的文件视为附属垃圾，即使有年份/清晰度也删。
    # 注意：不含 Specials/Season 0（Emby 正规特别篇，默认保留）。可在配置里增删。
    _DEFAULT_JUNK_SUBDIRS = ("SPs,Extras,Scans,Fonts,Music,CDs,Trailers,Sample,Samples,"
                             "NCOP,NCED,特典,映像特典,音乐特典,花絮,预告,预告片,menu")

    # 文件名附属标记（命中即垃圾，即使有年份/清晰度）
    _EXTRAS_MARKERS = ("[menu]", "映像特典", "音乐特典", "花絮", "预告片", "creditless",
                       ".ncop.", ".nced.", "ending ver", "review ver", "opening ver",
                       "preview ver", "[sp]", "[pv]", "[trailer]", "[logo]",
                       "[scans]", "[fonts]", "[cm]", ".sample.", "-sample.")

    # ---- 总开关 ----
    _enabled: bool = False
    _notify: bool = True
    _notify_type: str = "Plugin"
    _run_once: bool = False
    _cron: str = ""
    _step_timeout_min: int = 0

    # ---- OpenList ----
    _openlist_url: str = ""
    _openlist_token: str = ""

    # ---- 改名清洗 ----
    _rn_tv_paths: str = "/media/quark"      # 电视剧 strm 目录（改名+清垃圾）；混放内容也填这里
    _rn_movie_paths: str = ""               # 电影 strm 目录（只清垃圾）；可留空
    _rn_recursive: bool = True
    _rn_dry_run: bool = True
    _rn_clean_dirs: bool = False            # 清洗一级目录名（去广告前缀）
    _rn_default_season: int = 1
    _rn_max_episode: int = 500
    _rn_preserve_tail: bool = True
    _rn_clean_junk: bool = True
    _rn_no_number_is_junk: bool = True
    _rn_junk_keywords: str = ""
    _rn_junk_subdirs: str = ""              # 附属子目录清单，空=用 _DEFAULT_JUNK_SUBDIRS
    _rn_recent_days: int = 0
    _rn_after_date: str = ""
    _rn_template: str = "{title}.S{season:02d}E{episode:02d}{tail}"  # 不含扩展名，末尾补真实后缀
    _keep_reports: int = 10
    _container: str = "moviepilot-v2"
    # 防风控节奏（只作用于对夸克的写操作：rename/remove）
    _rl_min: float = 2.0
    _rl_max: float = 5.0
    _rl_batch: int = 30           # 每 N 个写操作长停一次
    _rl_pause_min: float = 60.0
    _rl_pause_max: float = 120.0
    _rl_max_ops: int = 300        # 单次运行写操作上限，到顶即停
    _rl_shuffle: bool = True      # 打乱处理顺序
    _rn_fail_ratio: float = 0.2   # 「真失败」占比超过此值才判整体失败(幽灵不计)，默认 20%

    _scheduler: Optional[BackgroundScheduler] = None
    # 重入锁：正在跑时再次触发（双击保存 / Cron 撞手动 / Webhook）直接跳过，
    # 避免两轮并发写夸克（写频率翻倍触发风控）+ 共享防风控计数器互相踩。
    _run_lock = threading.Lock()

    # 运行期状态（防风控计数，每次运行重置）
    _op_count: int = 0
    _consecutive_fail: int = 0
    _capped: bool = False
    _aborted_backoff: bool = False

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._notify_type = config.get("notify_type") or "Plugin"
            self._run_once = config.get("run_once", False)
            self._cron = config.get("cron", "")
            self._step_timeout_min = int(config.get("step_timeout_min") or 0)

            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token", "")

            self._rn_tv_paths = (config.get("rn_tv_paths")
                                 if config.get("rn_tv_paths") is not None else "/media/quark")
            # v1.0.1 迁移：早期默认填的 /media/QuarkStrm 是错的。Strm 驱动落盘目录名取自
            # 「源 paths 的末段」而非 mount_path——夸克存储 paths=/quark，实际落地在
            # /home/115strm/quark（容器内 /media/quark）。自动把已保存的旧默认纠正过来。
            if self._rn_tv_paths:
                self._rn_tv_paths = self._rn_tv_paths.replace("/media/QuarkStrm", "/media/quark")
            self._rn_movie_paths = (config.get("rn_movie_paths")
                                    if config.get("rn_movie_paths") is not None else "")
            if self._rn_movie_paths:
                self._rn_movie_paths = self._rn_movie_paths.replace("/media/QuarkStrm", "/media/quark")
            self._rn_recursive = config.get("rn_recursive", True)
            self._rn_dry_run = config.get("rn_dry_run", True)
            self._rn_clean_dirs = config.get("rn_clean_dirs", False)
            self._rn_default_season = int(config.get("rn_default_season") or 1)
            self._rn_max_episode = int(config.get("rn_max_episode") or 500)
            self._rn_preserve_tail = config.get("rn_preserve_tail", True)
            self._rn_clean_junk = config.get("rn_clean_junk", True)
            self._rn_no_number_is_junk = config.get("rn_no_number_is_junk", True)
            self._rn_junk_keywords = config.get("rn_junk_keywords") or ""
            self._rn_junk_subdirs = (config.get("rn_junk_subdirs")
                                     if config.get("rn_junk_subdirs") is not None
                                     else self._DEFAULT_JUNK_SUBDIRS)
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
            self._rn_fail_ratio = float(config.get("rn_fail_ratio") if config.get("rn_fail_ratio") is not None else 0.2)

        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次")
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
            "openlist_url": self._openlist_url, "openlist_token": self._openlist_token,
            "rn_tv_paths": self._rn_tv_paths, "rn_movie_paths": self._rn_movie_paths,
            "rn_recursive": self._rn_recursive, "rn_dry_run": self._rn_dry_run,
            "rn_clean_dirs": self._rn_clean_dirs,
            "rn_default_season": self._rn_default_season, "rn_max_episode": self._rn_max_episode,
            "rn_preserve_tail": self._rn_preserve_tail, "rn_clean_junk": self._rn_clean_junk,
            "rn_no_number_is_junk": self._rn_no_number_is_junk,
            "rn_junk_keywords": self._rn_junk_keywords, "rn_junk_subdirs": self._rn_junk_subdirs,
            "rn_recent_days": self._rn_recent_days,
            "rn_after_date": self._rn_after_date, "rn_template": self._rn_template,
            "keep_reports": self._keep_reports, "container": self._container,
            "rl_min": self._rl_min, "rl_max": self._rl_max, "rl_batch": self._rl_batch,
            "rl_pause_min": self._rl_pause_min, "rl_pause_max": self._rl_pause_max,
            "rl_max_ops": self._rl_max_ops, "rl_shuffle": self._rl_shuffle,
            "rn_fail_ratio": self._rn_fail_ratio,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/quark_clean",
            "event": EventType.PluginAction,
            "desc": "执行一次夸克改名清洗",
            "category": "整理",
            "data": {"action": "quark_clean"},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/run",
            "endpoint": self._api_run,
            "methods": ["GET", "POST"],
            "summary": "执行夸克改名清洗",
            "description": "读本地夸克 strm → 调 OpenList 改夸克源名/清垃圾 → 本地 strm 同步（按配置开关）。",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "QuarkCleanCron",
                    "name": "夸克改名清洗定时任务",
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
        if data.get("action") != "quark_clean":
            return
        logger.info(f"[{self.plugin_name}] 收到远程命令，开始执行")
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_run(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发夸克改名清洗，详情见 MP 日志"}

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

    # ---------- 主流程 ----------

    def _run_step(self, name: str, func) -> Tuple[bool, str]:
        """在子线程执行，套「单步超时」兜底。func 返回 (ok, summary)。"""
        logger.info(f"[{self.plugin_name}] ── 开始：{name}")
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

        t = threading.Thread(target=_worker, name=f"quarkclean-{name}", daemon=True)
        start = time.time()
        t.start()
        timeout = self._step_timeout_min * 60 if self._step_timeout_min > 0 else None
        t.join(timeout)
        if t.is_alive():
            msg = (f"超时：超过 {self._step_timeout_min} 分钟仍未完成，已停止等待"
                   f"（后台可能仍在跑，但不再阻塞等待）")
            logger.warning(f"[{self.plugin_name}] {name} {msg}")
            return False, msg
        elapsed = int(time.time() - start)
        ok = result.get("ok", False)
        summary = result.get("summary", "无返回")
        return ok, f"{summary}（耗时 {elapsed} 秒）"

    def _run_task(self):
        """重入锁保护：正在执行时再次触发（双击保存 / Cron 撞手动 / Webhook）直接跳过，
        避免两轮并发写夸克（写频率翻倍触发风控）与共享计数器互相踩。"""
        if not self._run_lock.acquire(blocking=False):
            logger.warning(f"[{self.plugin_name}] ⏸ 上一轮仍在执行，本次触发已跳过（防并发写夸克）")
            self._send_notify("夸克改名清洗跳过（上一轮还在跑）",
                              "检测到上一轮仍在执行，本次触发已跳过，避免并发操作夸克网盘。\n"
                              "等上一轮结束后再手动触发即可。")
            return
        try:
            mode = "预演" if self._rn_dry_run else "实际执行"
            logger.info(f"[{self.plugin_name}] ▶ 开始执行（{mode}）")
            ok, s = self._run_step("夸克改名清洗", self._run_rename)
            title = f"夸克改名清洗{'✅' if ok else '❌'}（{mode}）"
            logger.info(f"[{self.plugin_name}] ■ 结束：{title}")
            self._send_notify(title, s)
        finally:
            self._run_lock.release()

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

    # ==================== 改名清洗核心 ====================

    def _rn_cutoff_ts(self) -> Optional[float]:
        """日期增量下限。after_date 优先于 recent_days；都没设=全量。"""
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

    def _ol_rename(self, path_src: str, new_base: str) -> Tuple[bool, str]:
        """调 /api/fs/rename 改网盘源(文件或目录)名。path=完整旧路径，name=新的最后一段名。"""
        try:
            resp = requests.post(
                f"{self._openlist_url}/api/fs/rename",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json={"path": path_src, "name": new_base}, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            if data.get("code") == 200:
                return True, ""
            return False, f"code={data.get('code')} {data.get('message')}"
        except Exception as e:
            return False, str(e)

    def _ol_remove(self, dir_src: str, name: str) -> Tuple[bool, str]:
        """调 /api/fs/remove 删网盘源。dir=父目录，names=[文件名]。"""
        try:
            resp = requests.post(
                f"{self._openlist_url}/api/fs/remove",
                headers={"Authorization": self._openlist_token, "Content-Type": "application/json"},
                json={"dir": dir_src, "names": [name]}, timeout=30)
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

    def _after_write(self, ok: bool, ghost: bool = False):
        """一次写操作后的节奏：计数 + 限速 sleep + 批次长停 + 错误退避。
        ghost=True：失败原因是「网盘源已不存在」(object not found)，不是风控/限流，
        按中性处理——不退避、不累加连续失败计数，只照常限速，避免幽灵把整轮拖进硬中止。"""
        self._op_count += 1
        if ok:
            self._consecutive_fail = 0
        elif ghost:
            pass  # 幽灵：源不存在，不是风控信号，连续失败计数保持不动、不退避
        else:
            self._consecutive_fail += 1
            backoff = [5, 15, 45][min(self._consecutive_fail - 1, 2)]
            logger.warning(f"[{self.plugin_name}] 夸克写操作第 {self._consecutive_fail} 次失败，退避 {backoff}s")
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

    @staticmethod
    def _is_source_gone(err: str) -> bool:
        """判断 OpenList 写失败是否因「网盘源不存在」(幽灵)，以区别于风控/限流/网络等真失败。
        典型：code=500 failed to get src object: object not found。
        注意只匹配"不存在"类，不匹配 429/限流/timeout(那些应正常退避重试)。"""
        e = (err or "").lower()
        return ("object not found" in e
                or "not found" in e
                or "no such file" in e
                or "does not exist" in e)

    # ---- strm 内容 <-> 网盘源路径 ----

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
        """从 strm 内容(URL)解析网盘源路径(URL 解码后)，如 /quark/电视剧/某剧/x.mkv。"""
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
        """由「到 /d 的前缀」+「解码后的网盘路径」重建 strm URL(encodePath 逐段编码)。"""
        segs = src_decoded.split("/")
        return head_d + "/".join(quote(s, safe="") for s in segs)

    def _source_ext(self, src: Optional[str], content: str) -> str:
        """取网盘源文件真实后缀(.mkv/.mp4...)，解析不到兜底 .mkv。"""
        name = None
        if src:
            name = src.rstrip("/").split("/")[-1]
        if not name and content:
            name = unquote(content.split("?", 1)[0].rstrip("/").split("/")[-1])
        if name and "." in name:
            return "." + name.rsplit(".", 1)[-1].lower()
        return ".mkv"

    # ---- 主流程 ----

    def _run_rename(self) -> Tuple[bool, str]:
        if not self._openlist_url or not self._openlist_token:
            return False, "OpenList 地址或 token 未配置（需调 OpenList API 改夸克源）"
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
                "conflicts": 0, "failed": 0, "date_skipped": 0, "dirs_renamed": 0,
                "ghost": 0}
        details: List[Tuple[str, str, str, str]] = []
        reason_count: Dict[str, int] = {}

        # 1) 目录名清洗（去广告前缀 → 剧名(年份)）。电视剧+电影都做——电影没集号，
        # 目录名是唯一识别源，更需要清洗；先做，避免和文件改名交叉。
        if self._rn_clean_dirs and not self._aborted_backoff:
            for p in (tv_paths + movie_paths):
                root = Path(p)
                if root.exists() and root.is_dir():
                    self._clean_dir_names(root, cutoff_ts, stat, details)
                if self._capped or self._aborted_backoff:
                    break

        # 2) 文件改名(电视剧) + 清垃圾(电视剧/电影)
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
               f"冲突 {stat['conflicts']}，幽灵 {stat['ghost']}，失败 {stat['failed']}"
               f"{date_info}\n跳过原因分布：{skip_brief}")
        if not self._rn_dry_run:
            msg += f"\n夸克写操作：{self._op_count} 次"
        if self._capped:
            msg += f"\n⚠️ 已达单次写操作上限 {self._rl_max_ops}，剩余留待下次运行（防风控）"
        if self._aborted_backoff:
            msg += "\n⚠️ 连续多次失败已中止本轮网盘写操作（疑似风控/限流），请稍后再试"
        if stat["ghost"]:
            msg += (f"\nℹ️ 幽灵 {stat['ghost']} 个：夸克源已不存在、本地 strm 是死指针，不计失败；"
                    f"可在服务器跑 security/scripts/strm-ghost-clean.py --mount <挂载路径> 清理本地残留")

        report_path = self._write_report(mode, msg, details)
        if report_path:
            msg += f"\n明细已写入：{report_path}"
            msg += f"\n\n下载本次报告（直接复制）：\ndocker cp {self._container}:{report_path} ./"
        logger.info(f"[{self.plugin_name}] {msg}")

        # 成功判定：硬中止(连续失败/风控)必判失败；否则只有「真失败」占比超过阈值才判失败。
        # 幽灵(源不存在)不计入 failed；个别瞬时失败不让整体 ❌。
        real_writes = stat["renamed"] + stat["junked"] + stat["dirs_renamed"] + stat["failed"]
        fail_ratio = (stat["failed"] / real_writes) if real_writes else 0.0
        ok = (not self._aborted_backoff) and (fail_ratio <= self._rn_fail_ratio)
        return ok, msg

    def _clean_dir_names(self, root: Path, cutoff_ts: Optional[float],
                         stat: dict, details: list):
        """把 root 下一级子目录中名字含广告前缀的重命名：改夸克源目录 + 本地同步(改文件夹名+子 strm URL)。"""
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
            # 找目录下任一 strm，解析出该目录的网盘源路径
            sample = next(iter(d.rglob("*.strm")), None)
            if sample is None:
                details.append(("SKIP", "dir_no_strm", str(d), "目录内无 strm，无法定位网盘源"))
                continue
            sample_src = self._strm_source_path(self._read_strm(sample))
            if not sample_src:
                details.append(("SKIP", "dir_no_source", str(d), "strm 解析不到网盘源"))
                continue
            try:
                rel_parts = sample.relative_to(d).parts   # (mid..., file.strm)
            except ValueError:
                continue
            strip = len(rel_parts)                        # 去掉 old 之后的所有段(mid...,file)
            src_segs = sample_src.split("/")
            if strip >= len(src_segs):
                continue
            dir_src = "/".join(src_segs[:-strip])          # /quark/.../old
            if dir_src.split("/")[-1] != old:
                # 目录名和网盘段不一致(可能之前改过)，保守跳过
                details.append(("SKIP", "dir_mismatch", str(d), f"网盘段={dir_src.split('/')[-1]}"))
                continue
            new_dir_src = dir_src.rsplit("/", 1)[0] + "/" + new

            if self._rn_dry_run:
                stat["dirs_renamed"] += 1
                details.append(("DIR", "would", str(d), f"夸克: {old} -> {new}"))
                continue
            if not self._write_gate():
                details.append(("SKIP", "capped", str(d), "达上限，留待下次"))
                return
            ok, err = self._ol_rename(dir_src, new)
            if not ok and self._is_source_gone(err):
                self._after_write(False, ghost=True)
                stat["ghost"] += 1
                details.append(("GHOST", "dir_source_gone", str(d),
                                f"夸克源目录不存在，本地幽灵(建议 strm-ghost-clean.py 清理): {dir_src}"))
                continue
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
                    ncsrc = csrc.replace(dir_src, new_dir_src, 1)
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
                                f"夸克已改名但本地同步失败: {e}"))
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
                src = self._strm_source_path(content)
                # 垃圾判定（含集号/年份/清晰度铁律保护，见 _is_junk）
                if self._rn_clean_junk and self._is_junk(strm):
                    self._handle_junk(strm, content, src, stat, details)
                    continue
                if kind == "movie":
                    stat["skipped"] += 1
                    reason_count["movie_keep"] = reason_count.get("movie_keep", 0) + 1
                    continue
                self._handle_rename(strm, content, src, root, stat, details, reason_count)
            except Exception as e:
                stat["failed"] += 1
                details.append(("ERROR", "exception", str(strm), str(e)))
                logger.error(f"[{self.plugin_name}] 处理失败 {strm}: {e}")

    def _handle_rename(self, strm: Path, content: str, src: Optional[str],
                       root: Path, stat: dict, details: list, reason_count: dict):
        # 合集包（[共N部合集]，一个文件夹塞多部）：自动拆错误率高，跳过交人工
        if "部合集" in str(strm):
            stat["skipped"] += 1
            reason_count["collection_manual"] = reason_count.get("collection_manual", 0) + 1
            details.append(("SKIP", "collection", str(strm), "合集包，交人工"))
            return
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
        ext = self._source_ext(src, content)
        new_media = base + ext
        new_strm_name = base + ".strm"

        # 幂等：网盘源已是规范名
        if src and src.rstrip("/").split("/")[-1] == new_media:
            stat["skipped"] += 1
            reason_count["same"] = reason_count.get("same", 0) + 1
            details.append(("SKIP", "same", str(strm), "夸克源已是规范名"))
            return
        target_strm = strm.with_name(new_strm_name)
        if target_strm.exists() and target_strm != strm:
            stat["conflicts"] += 1
            details.append(("SKIP", "conflict", str(strm), new_strm_name))
            return

        if self._rn_dry_run:
            old_src = src.rstrip("/").split("/")[-1] if src else "?"
            stat["renamed"] += 1
            details.append(("RENAME", "would", str(strm), f"夸克: {old_src} -> {new_media}"))
            return
        if not src:
            stat["failed"] += 1
            details.append(("ERROR", "no_source", str(strm), "strm 内容解析不到网盘源路径"))
            return
        if not self._write_gate():
            details.append(("SKIP", "capped", str(strm), "达上限，留待下次"))
            return
        ok, err = self._ol_rename(src, new_media)
        if not ok and self._is_source_gone(err):
            self._after_write(False, ghost=True)
            stat["ghost"] += 1
            details.append(("GHOST", "source_gone", str(strm),
                            f"夸克源不存在，本地幽灵(建议 strm-ghost-clean.py 清理): {src}"))
            return
        self._after_write(ok)
        if not ok:
            stat["failed"] += 1
            details.append(("ERROR", "ol_rename_fail", str(strm), err))
            return
        # 本地同步：改 strm 内容 URL(指向新文件名) + 改本地 strm 文件名
        try:
            new_src = src.rsplit("/", 1)[0] + "/" + new_media
            new_content = self._build_url(self._head_d(content), new_src)
            strm.write_text(new_content, encoding="utf-8")
            if target_strm != strm:
                strm.rename(target_strm)
        except Exception as e:
            details.append(("WARN", "local_sync_fail", str(strm), f"夸克已改名但本地同步失败: {e}"))
        stat["renamed"] += 1
        details.append(("RENAME", "renamed", str(strm), new_media))

    def _handle_junk(self, strm: Path, content: str, src: Optional[str],
                     stat: dict, details: list):
        if self._rn_dry_run:
            stat["junked"] += 1
            details.append(("JUNK", "would", str(strm), f"删夸克: {src or '?'}"))
            return
        if not self._write_gate():
            details.append(("SKIP", "capped", str(strm), "达上限，留待下次"))
            return
        ok, err = True, ""
        if src:
            parent = src.rsplit("/", 1)[0]
            name = src.rstrip("/").split("/")[-1]
            ok, err = self._ol_remove(parent, name)
            if not ok and self._is_source_gone(err):
                # 网盘源已不存在，等于垃圾已删；本地 strm 直接清掉，不计失败
                self._after_write(False, ghost=True)
                try:
                    strm.unlink()
                except OSError:
                    pass
                stat["ghost"] += 1
                details.append(("GHOST", "source_gone", str(strm),
                                f"夸克源已不在(视为已删)，删本地幽灵: {src}"))
                return
            self._after_write(ok)
        if ok:
            try:
                strm.unlink()
            except OSError:
                pass
            stat["junked"] += 1
            details.append(("JUNK", "removed", str(strm), f"已删夸克+本地: {src or ''}"))
        else:
            stat["failed"] += 1
            details.append(("ERROR", "ol_remove_fail", str(strm), err))

    # ---- 目录名清洗规则（保守：去广告块/域名/发布站关键字）----

    def _clean_dir_name(self, name: str) -> str:
        """把一级目录名规整为「剧名 (年份)」（电影、剧集都留年份）。
        先剥发布站中文短语（_clean_title 不认识这些），再用 _clean_title 提取干净标题
        （内部会剥【】/[站点]/域名/发布组/技术标签、中英混排取中文），再补年份。
        提取不到可靠标题、或结果与原名相同 → 返回 "" 表示跳过不改。"""
        pre = name
        for kw in ("地址发布页", "收藏不迷路", "最新电影", "电影港",
                   "高清剧集网发布", "高清剧集网", "高清影视之家发布", "高清影视之家",
                   "更多电视剧集下载访问", "更多剧集打包下载访问", "更多电视剧集下载请访问",
                   "更多剧集打包下载请访问", "4K时光",
                   "6v电影", "阳光电影", "电影天堂", "電影天堂", "BT天堂", "不太灵影视"):
            pre = pre.replace(kw, " ")
        title = self._clean_title(pre)
        if not title or len(title) < 2:
            return ""
        year = self._extract_year(name)
        new = f"{title} ({year})" if (year and str(year) not in title) else title
        # 保留清晰度后缀（如 2160p/1080p）：便于人眼看画质，也让不同画质的重复目录区分开
        res = self._extract_res(name)
        if res:
            new = f"{new} {res}"
        new = self._safe_name(new).strip()
        if not new or new == name.strip() or len(new) < 2:
            return ""
        return new

    @staticmethod
    def _extract_res(name: str) -> str:
        """从原名提取清晰度后缀(2160p/1080p/720p/4k/8k 等)，保留到清洗后的目录名。
        取首个匹配、小写；4k/uhd 归一为 2160p。MP 识别时会忽略结尾清晰度，不影响识别。"""
        m = re.search(r"(?i)\b(2160p|1440p|1080p|1080i|720p|576p|480p|4k|8k|uhd)\b", name)
        if not m:
            return ""
        r = m.group(1).lower()
        return "2160p" if r in ("4k", "uhd") else r

    @staticmethod
    def _extract_year(name: str) -> Optional[int]:
        """取目录名里首个 19xx/20xx 年份(1900-2099)，用于「剧名 (年份)」。分辨率(2160p等)不会误命中。"""
        for m in re.finditer(r"(?<!\d)(?:19|20)\d{2}(?!\d)", name):
            y = int(m.group(0))
            if 1900 <= y <= 2099:
                return y
        return None

    # ==================== 集号/标题/垃圾解析（移植自 strmrename / mediapipeline）====================

    _QUALITY_RE = re.compile(
        r"(?i)^(2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|sdr|dv|"
        r"web-?dl|webrip|bluray|blu-ray|remux|hdtv|"
        r"h\.?264|h\.?265|x264|x265|hevc|avc|10bit|aac|dts|ddp?5\.?1|"
        r"国语|粤语|中字|双语)$")

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

    def _junk_subdir_set(self) -> set:
        raw = self._rn_junk_subdirs.strip() or self._DEFAULT_JUNK_SUBDIRS
        return {p.strip().lower() for p in raw.replace("，", "\n").replace(",", "\n").splitlines()
                if p.strip()}

    def _is_junk(self, file_path: Path) -> bool:
        """分层判垃圾（API 删垃圾为主，铁律保护正片）：
          1) 位于附属子目录(SPs/Extras/特典/花絮/Music/CDs...) → 垃圾，即使有年份/清晰度；
          2) 文件名带附属标记([SP]/[PV]/NCOP/预告片/映像特典/menu...) → 垃圾，即使有年份/清晰度；
          3) 正片保护：有集号(SxxExx/Exx/第N集/[NN]) 或 有年份/清晰度 → 不删；
          4) 无集号无年份但命中广告词/域名 → 垃圾；
          5) 开「无数字即垃圾」且文件名无数字 → 垃圾。
        """
        stem = file_path.stem
        name = file_path.name.lower()
        # 1) 附属子目录（Specials/Season 0 默认不在清单里，正规特别篇保留）
        junk_dirs = self._junk_subdir_set()
        for part in file_path.parts[:-1]:
            if part.strip().lower() in junk_dirs:
                return True
        # 2) 文件名附属标记
        for ex in self._EXTRAS_MARKERS:
            if ex in name:
                return True
        # 3) 正片保护
        if self._has_episode_marker(stem) or self._has_real_content(stem):
            return False
        # 4) 广告/域名
        for kw in self._junk_kw_list():
            if kw.lower() in name:
                return True
        # 5) 无数字
        if self._rn_no_number_is_junk and not re.search(r"\d", stem):
            return True
        return False

    # 全角标题块只拒绝“整块可确认是元数据”的内容。严禁用“广告/Studio”等裸子串判断，
    # 否则《广告狂人》《The Studio》这类正式片名会被不可逆地截断。
    _DOMAIN_TAG_RE = re.compile(
        r"(?i)(?:https?://|www\.|(?:^|[^a-z0-9])(?:[a-z0-9-]+\.)+(?:com|net|cc|me|tv|org|cn|xyz)\b)"
    )
    _DIRTY_TAG_EXACT_RE = re.compile(
        r"(?i)^(?:发布(?:站|组)?|字幕组|压制组|广告|网址|官网|资源|下载|影视站|"
        r"电影港|电影天堂|陽光电影|阳光电影|6v电影|高清影视之家|高清剧集网|不太灵影视)$"
    )
    _METADATA_TAG_RE = re.compile(
        r"(?i)^(?:(?:内封|内嵌|外挂|字幕|中字|配音|音轨|双语|简繁|无字|国语|粤语|"
        r"raws?|rip|subs?|fansub|全集|特典|menu|bilibili|baha|b-global|"
        r"chs|cht|jpn|eng|big5|hi10p|ma10p|sp|ova|cm)"
        r"(?:[\s+&/·、，,._-]*))*$"
    )
    _FULLWIDTH_BLOCK_RE = re.compile(r"【([^】]*)】|『([^』]*)』|「([^」]*)」")
    _PURE_TECH_TOKEN_RE = re.compile(
        r"(?i)^(?:tv|movie|bd|bdrip|blu-?ray|web|web-?dl|webrip|remux|hdtv|uhd|"
        r"4k|8k|2160p|1440p|1080p|1080i|720p|576p|480p|hdr|sdr|dv|dovi|"
        r"h\.?26[45]|x26[45]|hevc|avc|10bit|8bit|60fps|aac|flac|opus|ac-?3|e-?ac-?3|"
        r"dts(?:-?hd)?|ddp?5\.?1|atmos|ma|mkv|mp4|raws?|rip)$"
    )

    @staticmethod
    def _is_rejected_title_block(content: str) -> bool:
        """仅拒绝可结构化确认的站点、发布标签、字幕音轨或纯技术块。"""
        text = re.sub(r"\s+", " ", (content or "")).strip(" .-_·!&")
        if not text:
            return True
        if QuarkClean._DOMAIN_TAG_RE.search(text):
            return True
        if QuarkClean._DIRTY_TAG_EXACT_RE.fullmatch(text):
            return True
        if QuarkClean._METADATA_TAG_RE.fullmatch(text):
            return True
        tech_text = re.sub(r"(?i)h\.(?=26[45])", "h", text)
        tech_text = re.sub(r"(?i)(ddp?5)\.1", r"\g<1>1", tech_text)
        tech_parts = [p for p in re.split(r"[\s._+/]+", tech_text) if p]
        return bool(tech_parts) and all(
            QuarkClean._PURE_TECH_TOKEN_RE.fullmatch(p) for p in tech_parts)

    @staticmethod
    def _is_low_confidence_title(title: str) -> bool:
        """拒绝明显只是媒体标签/季号/单字的结果，宁可跳过也不执行不可逆改名。"""
        t = re.sub(r"\s+", " ", (title or "")).strip(" .-_·!&")
        if not t:
            return True
        if re.fullmatch(r"[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]", t):
            return True
        if re.fullmatch(r"(?i)(?:tv|bd|web|movie)", t):
            return True
        if re.fullmatch(
                r"(?i)(?:第\s*[0-9一二三四五六七八九十]+(?:\s*季)?|Season\s*\d+|S\d{1,2})", t):
            return True
        tech_text = re.sub(r"(?i)h\.(?=26[45])", "h", t)
        tech_text = re.sub(r"(?i)(ddp?5)\.1", r"\g<1>1", tech_text)
        tech_parts = [p for p in re.split(r"[\s._+/]+", tech_text) if p]
        if tech_parts and all(QuarkClean._PURE_TECH_TOKEN_RE.fullmatch(p) for p in tech_parts):
            return True
        return False

    @staticmethod
    def _has_explicit_season_suffix(suffix: str) -> bool:
        """季号后必须结束或跟明确分隔/年份/技术信息，避免误认 Season 24 Hours。"""
        match = re.match(
            r"^\s*(?:第\s*[0-9一二三四五六七八九十]+\s*季|(?i:Season\s*\d+))",
            suffix or "")
        if not match:
            return False
        remainder = (suffix or "")[match.end():]
        stripped = remainder.strip()
        if not stripped:
            return True
        # 分隔符本身不是充分证据；去掉后仍必须是年份或技术元数据。
        payload = re.sub(r"^[._\-·(（\[【『「\s]+", "", remainder)
        if not payload.strip():
            return True
        return bool(re.match(
            r"^(?:\(?(?:19|20)\d{2}\)?\b|2160p\b|1440p\b|1080[pi]?\b|720p\b|"
            r"576p\b|480p\b|4k\b|8k\b|uhd\b|hdr\b|web-?dl\b|webrip\b|"
            r"blu-?ray\b|bdrip\b|remux\b|hdtv\b)",
            payload.strip(), re.IGNORECASE))

    @staticmethod
    def _explicit_fullwidth_title(title: str) -> str:
        """只在「正式标题块后紧跟明确季标记」时采用块内标题，避免把后续别名一并写入。"""
        for match in QuarkClean._FULLWIDTH_BLOCK_RE.finditer(title or ""):
            content = next((g for g in match.groups() if g is not None), "").strip()
            if QuarkClean._is_rejected_title_block(content):
                continue
            suffix = (title or "")[match.end():]
            if QuarkClean._has_explicit_season_suffix(suffix):
                return QuarkClean._finalize_title(content)
        return ""

    @staticmethod
    def _replace_fullwidth_blocks(title: str) -> str:
        """全角块只删除明确脏元数据；无法确认时仅去括号并保留内容。"""
        def _replace(match: re.Match) -> str:
            content = next((g for g in match.groups() if g is not None), "").strip()
            if QuarkClean._is_rejected_title_block(content):
                return " "
            return f" {content} "

        return QuarkClean._FULLWIDTH_BLOCK_RE.sub(_replace, title or "")

    @staticmethod
    def _finalize_title(title: str) -> str:
        t = re.sub(r"\s+", " ", title or "").strip(" .-_·!&")
        # 只剥明确且位于末尾的中文/英文季标记；绝不把裸尾数当季。
        t = re.sub(r"\s*第\s*[0-9一二三四五六七八九十]+\s*季\s*$", "", t).strip(" .-_·!&")
        t = re.sub(r"(?i)\s*Season\s*\d+\s*$", "", t).strip(" .-_·!&")
        t = re.sub(r"\s+", " ", t).strip(" .-_·!&")
        return "" if QuarkClean._is_low_confidence_title(t) else t

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
            if QuarkClean._is_rejected_title_block(c):
                continue
            c2 = re.split(r"(?i)\s+S\d{1,2}\b|\s*\((?:19|20)\d{2}\)|\s+\d{1,3}-\d{1,3}\b", c)[0]
            c2 = QuarkClean._finalize_title(c2)
            if not c2:
                continue
            has_cjk = 1 if re.search(r"[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]", c2) else 0
            multiword = 1 if (" " in c2 or has_cjk) else 0
            score = (has_cjk, multiword, len(c2))
            if score > best_score:
                best_score = score
                best = c2
        return best

    @staticmethod
    def _clean_title(title: str) -> str:
        t = QuarkClean._heuristic_clean_title(title)
        if t:
            return t
        if anitopy is not None:
            try:
                a = anitopy.parse(title) or {}
                cand = a.get("anime_title")
                if cand:
                    cand = QuarkClean._post_clean_title(cand)
                    if cand:
                        return cand
            except Exception:
                pass
        if guessit is not None:
            try:
                g = dict(guessit(title))
                cand = g.get("title")
                if cand:
                    cand = QuarkClean._post_clean_title(cand)
                    if cand:
                        return cand
            except Exception:
                pass
        return ""

    @staticmethod
    def _post_clean_title(t: str) -> str:
        t = QuarkClean._replace_fullwidth_blocks(t)
        # 半角动画方括号仍按候选规则处理；解析库回传到这里时仅移除其技术块。
        t = re.sub(r"\[[^\]]*\]", " ", t)
        t = re.sub(r"\s+", " ", t).strip(" .-_·!&")
        t = re.sub(r"\s+(?:19|20)\d{2}$", "", t).strip()
        return QuarkClean._finalize_title(t)

    @staticmethod
    def _heuristic_clean_title(title: str) -> str:
        explicit_title = QuarkClean._explicit_fullwidth_title(title)
        if explicit_title:
            return explicit_title
        title = QuarkClean._replace_fullwidth_blocks(title)
        # 先剥明确站点/域名半角方括号前缀；其余半角方括号继续走动画候选逻辑。
        title = re.sub(
            r"^\s*\[[^\]]*(?:\.(?:com|net|cc|me|tv|org|cn)|影视|公益|发布|下载|资源)[^\]]*\]\s*",
            "", title)
        if title.lstrip().startswith("["):
            cand = QuarkClean._pick_anime_title(title)
            if cand:
                return cand
        t = re.sub(r"\[[^\]]*\]", " ", title)
        t = re.sub(r"(?i)\b(?:www\.)?[a-z0-9-]+\.(?:com|net|cc|me|tv|xyz|org|cn)\b", " ", t)
        t = re.sub(r"\s+", " ", t).strip(" .-_·")
        cut = re.split(
            r"(?i)(?<=.)(?:\.|\s|_)(?:S\d{1,2}(?:EP?\d+)?|Season\b|(?:19|20)\d{2}\b|"
            r"2160p|1080p|1080i|720p|576p|480p|4k|8k|uhd|hdr|web-?dl|webrip|"
            r"bluray|blu-ray|remux|hdtv|x264|x265|h\.?264|h\.?265|hevc|"
            r"60fps|10bit)",
            t, maxsplit=1)
        t = cut[0] if cut else t
        # 不再按“中文前缀 + 任意 Latin 尾巴”猜测并截断；混排内容宁可保留给 MP 识别。
        return QuarkClean._finalize_title(t)

    @staticmethod
    def _safe_name(name: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", name).strip()

    # ---- 报告 ----

    def _write_report(self, mode: str, summary: str,
                      details: List[Tuple[str, str, str, str]]) -> str:
        try:
            data_dir = self.get_data_path()
            ts = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y%m%d_%H%M%S")
            report = Path(data_dir) / f"quarkclean_report_{ts}.txt"
            lines = [
                f"# 夸克改名清洗报告 ({mode})",
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
            reports = sorted(data_dir.glob("quarkclean_report_*.txt"),
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
                            self._col(4, "VTextField", "rn_fail_ratio",
                                      "失败率阈值(0-1,超过判失败)", placeholder="0.2"),
                            self._col(4, "VTextField", "step_timeout_min",
                                      "超时(分钟,0=不限)", placeholder="0"),
                            self._select(4, "notify_type", "通知类型(对应MP渠道)",
                                         [("插件", "Plugin"), ("整理入库", "Organize"),
                                          ("媒体服务器", "MediaServer"),
                                          ("站点", "SiteMessage"), ("其它", "Other")]),
                        ],
                    },
                    # OpenList
                    self._subtitle("OpenList 连接（需管理员令牌；写接口 /api/fs/rename、/api/fs/remove）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "openlist_url",
                                      "OpenList 地址", placeholder="http://192.168.1.111:5244"),
                            self._col(6, "VTextField", "openlist_token",
                                      "OpenList Token", placeholder="openlist-xxxxxx"),
                        ],
                    },
                    # 目录
                    self._subtitle("目录配置（填 MP 容器内路径；混放内容全填「电视剧目录」即可）"),
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextarea", "rn_tv_paths",
                                      "电视剧 strm 目录 (含集号→按一级目录名改 SxxExx；多个换行)",
                                      placeholder="/media/quark", rows=2, autoGrow=True),
                            self._col(6, "VTextarea", "rn_movie_paths",
                                      "电影 strm 目录 (只清垃圾，不改名；可留空)",
                                      placeholder="", rows=2, autoGrow=True),
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            self._col(4, "VSwitch", "rn_dry_run", "预演模式 (只出报告，不改夸克)"),
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
                                      "仅此日期后 (YYYY-MM-DD，优先)", placeholder="2026-08-30"),
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
                            self._col(6, "VTextarea", "rn_junk_keywords",
                                      "垃圾关键字 (换行/逗号分隔，留空用内置默认)",
                                      placeholder="更多原盘请访问\n全球首发\nmp4kan.com", rows=2, autoGrow=True),
                            self._col(6, "VTextarea", "rn_junk_subdirs",
                                      "附属子目录 (在这些子目录里的即使有年份也删；不含Specials)",
                                      placeholder="SPs,Extras,Scans,Fonts,Music,CDs,特典,花絮,预告",
                                      rows=2, autoGrow=True),
                        ],
                    },
                    self._subtitle("防风控节奏（只作用于 rename/remove 写操作；预演不触发）"),
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
                                            "text": "读本地夸克 strm → 解析夸克源路径 → 调 OpenList "
                                            "/api/fs/rename 改【夸克源名】(而非只改本地 strm，避免 insert 复活)，"
                                            "并调 /api/fs/remove 清垃圾；改完自动把本地 strm 同步改名+改内容URL，"
                                            "无需再触发一次扫描。对夸克的写操作有拟人化限速(随机间隔+批次长停+"
                                            "单次上限+失败退避)防风控，预演模式不触发写操作。"
                                            "强烈建议：先只开预演跑一遍，docker cp 下载报告核对，"
                                            "确认无误再关预演小批量实跑。"
                                            "目录填 MP 容器内路径(如 /media/quark)。"
                                            "⚠️ 本地目录名取自 Strm 存储「源paths末段」而非 mount_path："
                                            "夸克存储 mount_path=/QuarkStrm 但 paths=/quark，"
                                            "实际落地 /home/115strm/quark → 容器内 /media/quark。"
                                            "注意：夸克TV(扫码)驱动不支持改名/删除，需用普通夸克 cookie 驱动。",
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
            "run_once": False, "cron": "", "step_timeout_min": 0,
            "openlist_url": "", "openlist_token": "",
            "rn_tv_paths": "/media/quark", "rn_movie_paths": "",
            "rn_recursive": True, "rn_dry_run": True, "rn_clean_dirs": False,
            "rn_default_season": 1, "rn_max_episode": 500, "rn_preserve_tail": True,
            "rn_clean_junk": True, "rn_no_number_is_junk": True, "rn_junk_keywords": "",
            "rn_junk_subdirs": self._DEFAULT_JUNK_SUBDIRS,
            "rn_recent_days": 0, "rn_after_date": "",
            "rn_template": "{title}.S{season:02d}E{episode:02d}{tail}",
            "keep_reports": 10, "container": "moviepilot-v2",
            "rl_min": 2.0, "rl_max": 5.0, "rl_batch": 30,
            "rl_pause_min": 60.0, "rl_pause_max": 120.0, "rl_max_ops": 300, "rl_shuffle": True,
            "rn_fail_ratio": 0.2,
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
