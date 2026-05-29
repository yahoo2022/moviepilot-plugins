"""
OpenList 扫描触发器 - MoviePilot V2 插件

功能：在 MP Web 后台一键触发 OpenList 目录刷新 + MP 目录整理。
场景：115 离线下载完成后，通过 MP 后台一键生成新文件的 STRM 并整理入库，
     避免每次手动在 OpenList 里逐层点开目录 + 再到 MP 点整理。

原理：OpenList 的 Strm 驱动（SaveStrmToLocal + update 模式）是“懒生成”——
     只有目录被列出（访问）时才把该层 .strm 落到本地。本插件用
     /api/fs/list 递归遍历挂载路径，等价于把每个文件夹都自动点开一遍，
     从而让所有新文件的 STRM 落地。比建索引快，也不需要索引权限。

用法：
  1. 在插件配置页填 OpenList 地址、token、扫描路径（挂载路径如 /云下载）、
     MP 整理源目录等
  2. 开启"立即执行一次"开关后保存，插件会：
     - 用 /api/fs/list 递归遍历扫描路径下的所有目录（refresh=true 强制刷新）
     - 遍历完成后调用 MP 内部 TransferChain 对指定源目录做一次整理
  3. 也支持 Cron 定时执行，或通过远程命令 /openlist_scan 触发
"""
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


class OpenListScan(_PluginBase):
    # 插件元数据
    plugin_name = "OpenList 扫描触发器"
    plugin_desc = "一键触发 OpenList 目录扫描，并在扫描完成后自动执行 MP 目录整理"
    plugin_icon = "refresh2.png"
    plugin_version = "1.1.0"
    plugin_author = "ahnuchen"
    author_url = "https://github.com/ahnuchen"
    plugin_config_prefix = "openlistscan_"
    plugin_order = 20
    auth_level = 1

    # 私有属性
    _enabled: bool = False
    _notify: bool = True
    _run_once: bool = False
    _openlist_url: str = ""
    _openlist_token: str = ""
    _scan_path: str = "/云下载"  # OpenList 挂载路径（虚拟路径），不是 115 源路径
    _scan_limit: int = 2  # 递归并发/节流：每列一层目录后 sleep 的毫秒数基数
    _scan_timeout: int = 1800  # 秒，整个递归遍历的总超时
    _trigger_mp_transfer: bool = True
    _mp_source_path: str = ""  # MP 内可见的源路径，如 /media/云下载
    _cron: str = ""
    _scheduler: Optional[BackgroundScheduler] = None

    def init_plugin(self, config: dict = None):
        # 先停掉旧任务
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._run_once = config.get("run_once", False)
            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token", "")
            self._scan_path = config.get("scan_path") or "/云下载"
            self._scan_limit = int(config.get("scan_limit") or 2)
            self._scan_timeout = int(config.get("scan_timeout") or 1800)
            self._trigger_mp_transfer = config.get("trigger_mp_transfer", True)
            self._mp_source_path = config.get("mp_source_path", "")
            self._cron = config.get("cron", "")

        # 立即执行一次
        if self._run_once:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"[{self.plugin_name}] 立即执行一次扫描")
            self._scheduler.add_job(
                self._run_task,
                "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
            )
            # 执行后关掉开关，避免重启后又触发
            self._run_once = False
            self.update_config(self._current_config())
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "run_once": self._run_once,
            "openlist_url": self._openlist_url,
            "openlist_token": self._openlist_token,
            "scan_path": self._scan_path,
            "scan_limit": self._scan_limit,
            "scan_timeout": self._scan_timeout,
            "trigger_mp_transfer": self._trigger_mp_transfer,
            "mp_source_path": self._mp_source_path,
            "cron": self._cron,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """定义远程命令：在 Telegram/微信/WebHook 等消息渠道里发送 /openlist_scan 即可触发"""
        return [
            {
                "cmd": "/openlist_scan",
                "event": EventType.PluginAction,
                "desc": "触发一次 OpenList 扫描并 MP 整理",
                "category": "OpenList",
                "data": {"action": "openlist_scan"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """注册一个 HTTP 接口，方便 webhook / 第三方工具调用"""
        return [
            {
                "path": "/scan",
                "endpoint": self._api_scan,
                "methods": ["GET", "POST"],
                "summary": "触发 OpenList 扫描并整理",
                "description": "递归遍历 OpenList 挂载路径（/api/fs/list 触发 "
                "Strm 懒生成），完成后调用 MP 目录整理。",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """注册 Cron 定时服务"""
        if self._enabled and self._cron:
            try:
                return [
                    {
                        "id": "OpenListScanCron",
                        "name": "OpenList 定时扫描",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self._run_task,
                        "kwargs": {},
                    }
                ]
            except Exception as e:
                logger.error(f"[{self.plugin_name}] Cron 表达式错误: {e}")
        return []

    # ---------- 事件处理 ----------

    @eventmanager.register(EventType.PluginAction)
    def handle_action(self, event: Event):
        if not self._enabled:
            return
        data = event.event_data or {}
        if data.get("action") != "openlist_scan":
            return
        logger.info(f"[{self.plugin_name}] 收到远程命令，开始执行")
        # 异步，避免阻塞消息循环
        threading.Thread(target=self._run_task, daemon=True).start()

    def _api_scan(self, *args, **kwargs):
        """HTTP 触发入口"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_task, daemon=True).start()
        return {"success": True, "message": "已触发扫描，详情见 MP 日志"}

    # ---------- 核心逻辑 ----------

    def _run_task(self):
        """完整流程：递归遍历 OpenList 目录（触发 STRM 懒生成）→ MP 目录整理"""
        if not self._openlist_url or not self._openlist_token:
            msg = "OpenList 地址或 token 未配置"
            logger.error(f"[{self.plugin_name}] {msg}")
            self._send_notify("配置错误", msg)
            return

        # Step 1 + 2: 递归列目录，每访问一层即触发该层 STRM 落地
        ok, msg = self._recursive_scan()
        if not ok:
            self._send_notify("扫描失败", msg)
            return

        self._send_notify("OpenList 扫描完成", msg)

        # Step 3: 触发 MP 整理
        if self._trigger_mp_transfer and self._mp_source_path:
            self._trigger_transfer()

    def _recursive_scan(self) -> Tuple[bool, str]:
        """
        递归遍历 OpenList 挂载路径下的所有目录。

        原理：Strm 驱动（SaveStrmToLocal + update 模式）是“懒生成”——
        只有当某个目录被列出（访问）时，OpenList 才会把该层的 .strm 落到本地。
        因此这里用 /api/fs/list 逐层 DFS，每列一层就相当于“点开了那个文件夹”，
        遍历完成后所有层级的 STRM 就都生成好了。比建索引快，也不需要索引权限。
        """
        start = time.time()
        dir_count = 0
        file_count = 0
        # 用栈做 DFS，初始为配置的扫描根路径
        stack: List[str] = [self._scan_path]
        # 每列一层之间的节流间隔（秒），scan_limit 越小越保守
        throttle = max(0.0, float(self._scan_limit) * 0.1)

        while stack:
            if time.time() - start > self._scan_timeout:
                return False, (f"遍历超过 {self._scan_timeout} 秒未完成，"
                               f"已处理目录 {dir_count} 个")
            cur = stack.pop()
            ok, entries, err = self._list_dir(cur)
            if not ok:
                logger.warning(f"[{self.plugin_name}] 列目录失败 {cur}: {err}")
                continue
            dir_count += 1
            for ent in entries:
                name = ent.get("name")
                if not name:
                    continue
                child = f"{cur.rstrip('/')}/{name}"
                if ent.get("is_dir"):
                    stack.append(child)
                else:
                    file_count += 1
            if throttle:
                time.sleep(throttle)

        elapsed = int(time.time() - start)
        msg = (f"路径: {self._scan_path}，已遍历目录 {dir_count} 个、"
               f"文件 {file_count} 个，耗时 {elapsed} 秒")
        logger.info(f"[{self.plugin_name}] 递归扫描完成，{msg}")
        return True, msg

    def _list_dir(self, path: str) -> Tuple[bool, List[dict], str]:
        """
        调 OpenList /api/fs/list 列出一层目录。
        refresh=true 强制跳过缓存、重新拉取，从而触发该层 STRM 落地。
        返回 (是否成功, 目录项列表, 错误信息)。
        """
        url = f"{self._openlist_url}/api/fs/list"
        headers = {
            "Authorization": self._openlist_token,
            "Content-Type": "application/json",
        }
        payload = {
            "path": path,
            "page": 1,
            "per_page": 0,   # 0 = 不分页，返回全部
            "refresh": True,  # 强制刷新，触发 Strm 懒生成
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
            code = data.get("code")
            if code != 200:
                return False, [], f"OpenList 返回 {code}: {data.get('message')}"
            content = (data.get("data") or {}).get("content") or []
            return True, content, ""
        except Exception as e:
            return False, [], str(e)

    def _trigger_transfer(self):
        """调用 MP TransferChain 对指定目录执行整理"""
        try:
            from app.chain.transfer import TransferChain
            from app.chain.storage import StorageChain
            from app.schemas import FileItem
        except Exception as e:
            logger.error(f"[{self.plugin_name}] 导入 MP 内部模块失败: {e}")
            self._send_notify("MP 整理失败", f"导入模块异常: {e}")
            return

        src = Path(self._mp_source_path)
        if not src.exists():
            msg = f"源路径不存在: {self._mp_source_path}"
            logger.error(f"[{self.plugin_name}] {msg}")
            self._send_notify("MP 整理失败", msg)
            return

        try:
            # 构造 FileItem
            fileitem = FileItem(
                storage="local",
                type="dir",
                path=str(src),
                name=src.name,
                basename=src.name,
                extension="",
                size=0,
            )
            # 调 MP 的手动整理接口
            chain = TransferChain()
            logger.info(f"[{self.plugin_name}] 触发 MP 整理: {src}")
            ok, msg = chain.manual_transfer(
                fileitem=fileitem,
                background=True,  # 后台执行，不阻塞
            )
            if ok:
                logger.info(f"[{self.plugin_name}] MP 整理已提交: {msg}")
                self._send_notify("MP 整理已提交", f"目录: {self._mp_source_path}")
            else:
                logger.error(f"[{self.plugin_name}] MP 整理提交失败: {msg}")
                self._send_notify("MP 整理失败", str(msg))
        except Exception as e:
            logger.error(f"[{self.plugin_name}] 调 MP 整理异常: {e}")
            self._send_notify("MP 整理异常", str(e))

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
                    # 开关行
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VSwitch", "enabled", "启用插件"),
                            self._col(6, "VSwitch", "notify", "发送通知"),
                            self._col(6, "VSwitch", "run_once",
                                      "立即执行一次 (保存后生效，随后自动关闭)"),
                            self._col(6, "VSwitch", "trigger_mp_transfer",
                                      "扫描后自动 MP 整理"),
                        ],
                    },
                    # OpenList 配置
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "openlist_url",
                                      "OpenList 地址",
                                      placeholder="http://your-host:5244"),
                            self._col(6, "VTextField", "openlist_token",
                                      "OpenList Token",
                                      placeholder="openlist-xxxxxx"),
                        ],
                    },
                    # 扫描参数
                    {
                        "component": "VRow",
                        "content": [
                            self._col(6, "VTextField", "scan_path",
                                      "扫描路径 (OpenList 挂载路径)",
                                      placeholder="/云下载"),
                            self._col(3, "VTextField", "scan_limit",
                                      "节流强度 (越大越慢越稳)",
                                      placeholder="2"),
                            self._col(3, "VTextField", "scan_timeout",
                                      "超时秒数",
                                      placeholder="1800"),
                        ],
                    },
                    # MP 整理参数
                    {
                        "component": "VRow",
                        "content": [
                            self._col(8, "VTextField", "mp_source_path",
                                      "MP 整理源目录 (MP 容器内可见)",
                                      placeholder="/media/云下载"),
                            self._col(4, "VTextField", "cron",
                                      "Cron 定时 (可选)",
                                      placeholder="0 */2 * * *"),
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
                                            "text": "流程：递归遍历 OpenList 挂载路径"
                                            "（逐层列目录，触发 Strm 懒生成）→ "
                                            "调 MP 整理源目录。"
                                            "扫描路径请填 OpenList 的挂载路径"
                                            "（如 /云下载），不是 115 源路径。"
                                            "Cron 填了会按周期执行，"
                                            "也可以通过远程命令 /openlist_scan 触发。",
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
            "trigger_mp_transfer": True,
            "openlist_url": "",
            "openlist_token": "",
            "scan_path": "/云下载",
            "scan_limit": 2,
            "scan_timeout": 1800,
            "mp_source_path": "/media/云下载",
            "cron": "",
        }

    @staticmethod
    def _col(cols: int, comp: str, model: str, label: str, **props) -> dict:
        """简化 Vuetify 配置嵌套"""
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
