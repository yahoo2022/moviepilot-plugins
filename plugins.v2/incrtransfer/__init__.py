"""
增量整理刮削 - MoviePilot V2 插件

功能：对指定源目录做「只整理最近 N 天新增/改动」的增量整理 + 刮削，
     可选 电影/电视剧、复制/移动/硬链接/软链接/自动、目标路径、是否刮削等，
     等价于把 MP「手动整理」的能力做成「只挑最近新增的那部分」并支持定时/一键。

为什么要这个插件：
  MP 自带的「手动整理目录」只能对某个目录做**全量**整理——每次都要把整棵目录树
  重新遍历、识别一遍（已整理过的会跳过，但遍历/识别开销仍在）。对于 115 离线下载
  这种「在 MP 之外往网盘里丢文件、再由 OpenList 生成 STRM」的场景，没有「只整理
  最近 3 天新增内容」的增量入口。本插件按文件 mtime 过滤，只把最近 N 天有改动的
  媒体挑出来交给 MP 整理，省去每次全量遍历。

  全量整理仍可用（增量天数填 0），此时退化为对整个源目录做一次 MP 手动整理。

原理：
  1. 遍历源目录，按媒体扩展名（默认含 .strm）找出 mtime 在最近 N 天内的文件
  2. 按「整理单元」把这些文件归并成若干待整理项（一级目录 / 单文件）
  3. 逐个调用 MP 内部 TransferChain().manual_transfer(...) 执行整理 + 刮削

注意：
  - 源目录/目标目录都填 MP 容器内可见的路径（如 /media/云下载），不是宿主机路径。
  - 增量依赖「父目录 mtime 随子项增删而更新」。115 离线下载落盘、新影视作为
    新文件夹出现时该前提成立。深层嵌套改动若漏扫，把天数调大或填 0 全量跑一次。
"""
import os
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


class IncrTransfer(_PluginBase):
    # 插件元数据
    plugin_name = "增量整理刮削"
    plugin_desc = "只整理最近 N 天新增/改动的媒体，支持电影/电视剧、复制/移动/链接/自动、目标路径与刮削"
    plugin_icon = "directory.png"
    plugin_version = "1.1.0"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "incrtransfer_"
    plugin_order = 22
    auth_level = 1

    # 默认媒体扩展名（含 strm，覆盖本仓库 strm 媒体库场景）
    _DEFAULT_EXTS = "strm,mkv,mp4,ts,m2ts,iso,avi,mov,wmv,rmvb,flv,m4v,mpg,mpeg"

    # 私有属性
    _enabled: bool = False
    _notify: bool = True
    _run_once: bool = False
    # 源目录（MP 容器内路径），多个用换行/逗号分隔
    _src_paths: str = ""
    # 增量天数：0=全量，N=只整理最近 N 天有改动的媒体
    _recent_days: int = 3
    # 整理单元：folder=按一级目录整理（整部电影/整部剧），file=按单个文件整理
    _transfer_unit: str = "folder"
    # 整理方式：""=自动(用 MP 目录默认), copy/move/link/softlink
    _transfer_type: str = ""
    # 媒体类型提示：""=自动, 电影, 电视剧
    _mtype: str = ""
    # 目标路径（MP 容器内路径），留空=用 MP 媒体库默认目录
    _target_path: str = ""
    # 刮削：default=跟随 MP 设置, on=强制刮削, off=不刮削
    _scrape: str = "default"
    # 按类型分类（目标路径下按媒体类型建子目录，如 电影/电视剧）：default/on/off
    _type_folder: str = "default"
    # 按类别分类（目标路径下按二级分类建子目录，如 动画/纪录片）：default/on/off
    _category_folder: str = "default"
    # 最小文件大小(MB)，过滤小样片
    _min_filesize: int = 0
    # 强制整理：忽略「已整理过」历史记录，重新整理
    _force: bool = False
    # 快速模式：按目录 mtime 剪枝，跳过明显很旧的目录不深入（可能漏扫深层嵌套）
    _fast_prune: bool = False
    # 自定义媒体扩展名
    _media_exts: str = ""
    _cron: str = ""
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._run_once = config.get("run_once", False)
            self._src_paths = config.get("src_paths") or ""
            self._recent_days = int(config.get("recent_days") or 0)
            self._transfer_unit = config.get("transfer_unit") or "folder"
            self._transfer_type = config.get("transfer_type") or ""
            self._mtype = config.get("mtype") or ""
            self._target_path = (config.get("target_path") or "").strip()
            self._scrape = config.get("scrape") or "default"
            self._type_folder = config.get("type_folder") or "default"
            self._category_folder = config.get("category_folder") or "default"
            self._min_filesize = int(config.get("min_filesize") or 0)
            self._force = config.get("force", False)
            self._fast_prune = config.get("fast_prune", False)
            self._media_exts = config.get("media_exts") or ""
            self._cron = config.get("cron", "")

        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次增量整理")
            self._scheduler.add_job(
                self._run_task,
                "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
            )
            # 执行后关掉开关，避免重启又触发
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "run_once": self._run_once,
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
            "cron": self._cron,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """远程命令：消息渠道里发送 /incr_transfer 触发一次增量整理"""
        return [
            {
                "cmd": "/incr_transfer",
                "event": EventType.PluginAction,
                "desc": "执行一次增量整理刮削",
                "category": "整理",
                "data": {"action": "incr_transfer"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/transfer",
                "endpoint": self._api_transfer,
                "methods": ["GET", "POST"],
                "summary": "执行增量整理刮削",
                "description": "只整理最近 N 天新增/改动的媒体，调用 MP 手动整理。",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [
                    {
                        "id": "IncrTransferCron",
                        "name": "增量整理定时任务",
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
        if data.get("action") != "incr_transfer":
            return
        logger.info(f"[{self.plugin_name}] 收到远程命令，开始执行")
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_transfer(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发增量整理，详情见 MP 日志"}

    # ---------- 工具 ----------

    @staticmethod
    def _split_paths(raw: str) -> List[str]:
        """多行/逗号分隔 -> 列表，去空白、去重、保序。"""
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
        """把中文媒体类型映射为 MediaType；自动返回 None。"""
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
        return None  # default：跟随 MP 设置

    @staticmethod
    def _tri_val(v: str) -> Optional[bool]:
        """三态开关：on=True, off=False, 其它(default)=None(跟随 MP 设置)。"""
        if v == "on":
            return True
        if v == "off":
            return False
        return None

    # ---------- 核心 ----------

    def _run_task(self):
        src_paths = self._split_paths(self._src_paths)
        if not src_paths:
            msg = "源目录未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            self._send_notify("配置错误", msg)
            return

        cutoff_ts: Optional[float] = None
        if self._recent_days and self._recent_days > 0:
            cutoff_ts = (datetime.now() - timedelta(days=self._recent_days)).timestamp()
        mode = f"增量({self._recent_days}天)" if cutoff_ts is not None else "全量"

        # 收集所有待整理项
        all_targets: List[Tuple[str, str]] = []  # (path, "dir"/"file")
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
            text = "没有找到需要整理的内容"
            if cutoff_ts is not None:
                text += f"（最近 {self._recent_days} 天无新增/改动媒体）"
            if scan_errs:
                text += "\n" + "\n".join(scan_errs)
            self._send_notify(f"{mode}整理完成", text)
            return

        # 逐个整理
        ok_list, fail_list = self._do_transfer_targets(all_targets)

        # 汇总
        parts = [f"模式: {mode}，待整理 {len(all_targets)} 项"]
        if ok_list:
            parts.append(f"已提交 {len(ok_list)} 项:\n" + "\n".join(ok_list[:20])
                         + ("\n..." if len(ok_list) > 20 else ""))
        if fail_list:
            parts.append(f"失败 {len(fail_list)} 项:\n" + "\n".join(fail_list[:20])
                         + ("\n..." if len(fail_list) > 20 else ""))
        if scan_errs:
            parts.append("扫描告警:\n" + "\n".join(scan_errs))
        title = "增量整理已提交" if ok_list and not fail_list else "增量整理完成（含失败）"
        if not ok_list and fail_list:
            title = "增量整理失败"
        self._send_notify(title, "\n\n".join(parts))

    def _collect_targets(self, root: Path, cutoff_ts: Optional[float]) -> List[Tuple[str, str]]:
        """
        遍历 root，挑出最近改动的媒体，归并成待整理项。
        - cutoff_ts 为 None（全量）：直接返回整个 root 作为一个目录整理项。
        - 否则：找 mtime >= cutoff 的媒体文件，按整理单元归并：
            * transfer_unit=folder：归并到该文件在 root 下的「一级子目录」；
              若文件就在 root 下，则作为单文件整理项。
            * transfer_unit=file：每个文件单独作为整理项。
        """
        if cutoff_ts is None:
            return [(str(root), "dir")]

        exts = self._ext_set()
        targets: "Dict[str, str]" = {}

        for dirpath, dirnames, filenames in os.walk(root):
            # 快速模式：剪掉明显很旧、且名字像隐藏/回收站的目录；并按 mtime 剪枝
            if self._fast_prune:
                kept = []
                for d in dirnames:
                    if d.startswith(".") or d in ("@Recycle", "#recycle", "@eaDir"):
                        continue
                    full = os.path.join(dirpath, d)
                    try:
                        if os.stat(full).st_mtime < cutoff_ts:
                            # 目录本身很旧，跳过深入（可能漏深层嵌套，已在文档中说明）
                            continue
                    except OSError:
                        continue
                    kept.append(d)
                dirnames[:] = kept
            else:
                # 常规模式只跳过隐藏/回收站目录
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

                # folder 模式：归并到 root 下的一级子目录
                try:
                    rel_parts = fp.relative_to(root).parts
                except ValueError:
                    rel_parts = (fn,)
                if len(rel_parts) <= 1:
                    # 文件直接位于 root 下，按单文件整理
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
            self._send_notify("整理失败", f"导入模块异常: {e}")
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
                    background=True,
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

    # ---------- 配置界面 ----------

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    # 开关
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VSwitch", "enabled", "启用插件"),
                            self._col(6, "VSwitch", "notify", "发送通知"),
                            self._col(6, "VSwitch", "run_once",
                                      "立即执行一次 (保存后生效，随后自动关闭)"),
                            self._col(6, "VSwitch", "force",
                                      "强制整理 (忽略已整理历史)"),
                            self._col(6, "VSwitch", "fast_prune",
                                      "快速模式 (按目录时间剪枝，可能漏深层嵌套)"),
                        ],
                    },
                    # 源目录 + 增量天数
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextarea", "src_paths",
                                      "源目录 (MP 容器内路径，多个换行)",
                                      placeholder="/media/云下载\n/media/TV",
                                      rows=2, autoGrow=True),
                            self._col(4, "VTextField", "recent_days",
                                      "增量天数 (0=全量)",
                                      placeholder="3"),
                        ],
                    },
                    # 媒体类型 / 整理方式 / 整理单元
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
                    # 刮削 / 最小大小 / 目标路径
                    {
                        "component": "VRow",
                        "content": [
                            self._select(4, "scrape", "刮削元数据",
                                         [("跟随 MP 设置", "default"),
                                          ("强制刮削", "on"), ("不刮削", "off")]),
                            self._col(4, "VTextField", "min_filesize",
                                      "最小文件大小(MB)", placeholder="0"),
                            self._col(4, "VTextField", "target_path",
                                      "目标路径 (留空=媒体库默认)",
                                      placeholder="/media/整理后"),
                        ],
                    },
                    # 按类型分类 / 按类别分类（对齐 MP 手动整理的两个开关，避免刮削路径不一致）
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
                    # 扩展名 / Cron
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextField", "media_exts",
                                      "媒体扩展名 (逗号分隔，留空用默认含 strm)",
                                      placeholder=self._DEFAULT_EXTS),
                            self._col(4, "VTextField", "cron",
                                      "Cron 定时 (可选)",
                                      placeholder="0 */6 * * *"),
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
                                            "text": "增量天数=N 时，只把最近 N 天内有改动的"
                                            "媒体文件挑出来整理（按文件 mtime 过滤），"
                                            "填 0 则对整个源目录全量整理。"
                                            "「整理单元」按一级目录=把新文件所在的一级子"
                                            "目录整体交给 MP（适合整部电影/整部剧）；"
                                            "按单个文件=只整理那个文件。"
                                            "源目录/目标路径都填 MP 容器内路径"
                                            "（如 /media/云下载）。"
                                            "「按类型/按类别分类」建议和你 MP 手动整理时"
                                            "的两个开关保持一致（一般都开），否则刮削入库"
                                            "路径会和手动整理不一样。"
                                            "可通过 Web 按钮、远程命令 /incr_transfer、"
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
            "run_once": False,
            "force": False,
            "fast_prune": False,
            "src_paths": "",
            "recent_days": 3,
            "mtype": "",
            "transfer_type": "",
            "transfer_unit": "folder",
            "scrape": "default",
            "type_folder": "default",
            "category_folder": "default",
            "min_filesize": 0,
            "target_path": "",
            "media_exts": "",
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

    @staticmethod
    def _select(cols: int, model: str, label: str,
                options: List[Tuple[str, str]]) -> dict:
        """下拉选择框：options 为 [(显示文案, 值), ...]"""
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
