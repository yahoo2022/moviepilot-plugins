"""
115 Cookie 同步 - MoviePilot V2 插件

功能：定时通过 OpenList 校验 115 cookie 是否有效；失效时用「扫码登录」获取新 cookie，
     自动写回 OpenList 的 115 存储，并通过 MP 通知渠道（钉钉等）提醒。

设计要点（防风控）：
  1. 校验走 OpenList 自身的 /api/fs/list(refresh) + 可选 /api/fs/get，而不是直接打 115
     用户 API。这样 115 看到的永远是「OpenList 从同一 IP、同一请求方式」在活动，
     混进正常流量里；且校验的正是「OpenList 还能不能真的访问 115」这条真实链路。
  2. 只在确认失效后才触发重新登录，并带冷静期，避免频繁登录触发 115 风控。
  3. 校验用一个「探针目录」（只有 1~2 个小文件），refresh 只打这个小目录，开销极小
     又能强制回源，避免缓存假阳性、也不给 115 造成压力。

获取 cookie 的两种方式：
  - 扫码登录（推荐，默认）：纯 HTTP，无验证码、无滑块。二维码显示在本插件页面上，
    用 115 手机 App 扫一下即可。同一「设备类型(app)」会互相踢下线，默认用 alipaymini
    独占一个设备槽，避免和你自己浏览器的 115 web 会话互踢（互踢正是 cookie 频繁失效的
    元凶之一）。
  - 手动粘贴 cookie（兜底）：把浏览器里的 115 cookie 串粘到插件页面，插件校验后写回
    OpenList。适合扫码不便或临时救急。

关于账号密码自动登录：115 官方登录做了加密混淆 + 滑块验证码，成熟的开源 115 工具
（p115、alist 等）都只支持扫码登录，账号密码自动登录既不可靠也无法自动过滑块，
因此本插件不实现该方式。需要「页面输入」的诉求由「手动粘贴 cookie」覆盖。
"""
import base64
import hashlib
import hmac
import json
import threading
import time
import urllib.parse
from datetime import datetime, timedelta
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


class CookieSync115(_PluginBase):
    # 插件元数据
    plugin_name = "115 Cookie 同步"
    plugin_desc = "定时通过 OpenList 校验 115 cookie，失效时扫码登录获取新 cookie 并写回 OpenList"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/main/src/assets/images/misc/u115.png"
    plugin_version = "1.1.0"
    plugin_author = "yahoo2022"
    author_url = "https://github.com/yahoo2022"
    plugin_config_prefix = "cookiesync115_"
    plugin_order = 22
    auth_level = 1

    # 115 扫码登录 API
    _QR_TOKEN_URL = "https://qrcodeapi.115.com/api/1.0/web/1.0/token/"
    _QR_IMAGE_URL = "https://qrcodeapi.115.com/api/1.0/mac/1.0/qrcode?uid={uid}"
    _QR_STATUS_URL = "https://qrcodeapi.115.com/get/status/"
    _QR_RESULT_URL = "https://passportapi.115.com/app/1.0/{app}/1.0/login/qrcode/"
    _APP_CHOICES = ["web", "android", "ios", "linux", "mac", "windows",
                    "tv", "alipaymini", "wechatmini", "qandroid"]
    _UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

    # ---- 总开关 ----
    _enabled: bool = False
    _notify: bool = True
    _notify_type: str = "Plugin"
    # 推送方式：mp=走 MP 通知渠道；dingtalk=直推钉钉 webhook（不依赖 MP 通知配置）
    _push_target: str = "mp"
    _dingtalk_webhook: str = ""     # 钉钉机器人 webhook 地址
    _dingtalk_keyword: str = ""     # 钉钉「自定义关键词」安全设置时，消息需含此词
    _dingtalk_secret: str = ""      # 钉钉「加签」安全设置时的密钥（SEC 开头）

    # ---- 一次性动作开关 ----
    _check_now: bool = False       # 立即校验一次
    _login_now: bool = False       # 立即发起扫码登录
    _apply_cookie_now: bool = False  # 立即把手动粘贴的 cookie 写回 OpenList
    _test_push_now: bool = False   # 立即发一条测试通知

    # ---- OpenList ----
    _openlist_url: str = ""
    _openlist_token: str = ""
    _probe_path: str = ""          # 探针目录（OpenList 挂载路径下的小目录）
    _deep_check: bool = False      # 深度校验：额外 fs/get 拿直链
    _driver_keyword: str = "115"   # 匹配 115 存储的 driver 关键字

    # ---- 登录 ----
    _login_app: str = "alipaymini"  # 115 设备类型
    _manual_cookie: str = ""        # 手动粘贴的 cookie 串

    # ---- 调度与风控 ----
    _cron: str = ""                 # 定时校验
    _relogin_cooldown_min: int = 30  # 重登录冷静期（分钟）
    _auto_relogin: bool = True      # 校验失效后自动发起扫码登录（仍需人扫码）

    _scheduler: Optional[BackgroundScheduler] = None
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._notify_type = config.get("notify_type") or "Plugin"
            self._push_target = config.get("push_target") or "mp"
            self._dingtalk_webhook = (config.get("dingtalk_webhook") or "").strip()
            self._dingtalk_keyword = (config.get("dingtalk_keyword") or "").strip()
            self._dingtalk_secret = (config.get("dingtalk_secret") or "").strip()

            self._check_now = config.get("check_now", False)
            self._login_now = config.get("login_now", False)
            self._apply_cookie_now = config.get("apply_cookie_now", False)
            self._test_push_now = config.get("test_push_now", False)

            self._openlist_url = (config.get("openlist_url") or "").rstrip("/")
            self._openlist_token = config.get("openlist_token", "")
            self._probe_path = config.get("probe_path") or ""
            self._deep_check = config.get("deep_check", False)
            self._driver_keyword = config.get("driver_keyword") or "115"

            self._login_app = config.get("login_app") or "alipaymini"
            self._manual_cookie = (config.get("manual_cookie") or "").strip()

            self._cron = config.get("cron", "")
            self._relogin_cooldown_min = int(config.get("relogin_cooldown_min") or 30)
            self._auto_relogin = config.get("auto_relogin", True)

        # 处理一次性动作（保存后触发，随即关闭开关）
        one_shot = None
        if self._check_now:
            one_shot = self._run_check
            self._check_now = False
        elif self._login_now:
            one_shot = self._run_qr_login
            self._login_now = False
        elif self._apply_cookie_now:
            one_shot = self._apply_manual_cookie
            self._apply_cookie_now = False
        elif self._test_push_now:
            one_shot = self._run_test_push
            self._test_push_now = False

        if one_shot is not None:
            self.update_config(self._current_config())
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                one_shot, "date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
            )
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def _current_config(self) -> dict:
        return {
            "enabled": self._enabled,
            "notify": self._notify,
            "notify_type": self._notify_type,
            "push_target": self._push_target,
            "dingtalk_webhook": self._dingtalk_webhook,
            "dingtalk_keyword": self._dingtalk_keyword,
            "dingtalk_secret": self._dingtalk_secret,
            "check_now": self._check_now,
            "login_now": self._login_now,
            "apply_cookie_now": self._apply_cookie_now,
            "test_push_now": self._test_push_now,
            "openlist_url": self._openlist_url,
            "openlist_token": self._openlist_token,
            "probe_path": self._probe_path,
            "deep_check": self._deep_check,
            "driver_keyword": self._driver_keyword,
            "login_app": self._login_app,
            "manual_cookie": self._manual_cookie,
            "cron": self._cron,
            "relogin_cooldown_min": self._relogin_cooldown_min,
            "auto_relogin": self._auto_relogin,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/sync115_check",
                "event": EventType.PluginAction,
                "desc": "校验 115 cookie 是否有效",
                "category": "115",
                "data": {"action": "sync115_check"},
            },
            {
                "cmd": "/sync115_login",
                "event": EventType.PluginAction,
                "desc": "发起 115 扫码登录并同步 cookie",
                "category": "115",
                "data": {"action": "sync115_login"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/check",
                "endpoint": self._api_check,
                "methods": ["GET", "POST"],
                "summary": "校验 115 cookie 是否有效",
            },
            {
                "path": "/login",
                "endpoint": self._api_login,
                "methods": ["GET", "POST"],
                "summary": "发起 115 扫码登录",
            },
            {
                "path": "/status",
                "endpoint": self._api_status,
                "methods": ["GET"],
                "summary": "查询当前状态",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            try:
                return [
                    {
                        "id": "CookieSync115Check",
                        "name": "115 Cookie 定时校验",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self._run_check,
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
        action = data.get("action")
        if action == "sync115_check":
            threading.Thread(target=self._run_check, daemon=True).start()
        elif action == "sync115_login":
            threading.Thread(target=self._run_qr_login, daemon=True).start()

    def _api_check(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_check, daemon=True).start()
        return {"success": True, "message": "已触发校验，详情见状态或 MP 日志"}

    def _api_login(self, *args, **kwargs):
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        threading.Thread(target=self._run_qr_login, daemon=True).start()
        return {"success": True, "message": "已发起扫码登录，请到插件页扫码"}

    def _api_status(self, *args, **kwargs):
        return {"success": True, "data": self._get_status()}

    # ---------- 状态存取 ----------

    def _get_status(self) -> dict:
        return self.get_data("status") or {
            "state": "idle", "message": "尚未运行", "updated": "",
            "qr_image": "", "qr_tip": "",
        }

    def _set_status(self, state: str, message: str = "", qr_image: str = "", qr_tip: str = ""):
        now = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S")
        st = {"state": state, "message": message, "updated": now,
              "qr_image": qr_image, "qr_tip": qr_tip}
        self.save_data("status", st)

    def _send_notify(self, title: str, text: str):
        if not self._notify:
            return
        # 钉钉直推：不依赖 MP 通知渠道配置
        if self._push_target == "dingtalk" and self._dingtalk_webhook:
            self._send_dingtalk(title, text)
            return
        # 默认：走 MP 通知渠道（需在 MP 设定→通知里配好，并放行对应消息类型）
        try:
            from app.schemas.types import NotificationType as NT
            mtype = getattr(NT, self._notify_type, NT.Plugin)
        except Exception:
            mtype = NotificationType.Plugin
        try:
            self.post_message(mtype=mtype, title=f"【{self.plugin_name}】{title}", text=text)
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 发送通知失败: {e}")

    def _send_dingtalk(self, title: str, text: str):
        """直推钉钉群机器人（text 类型）。支持自定义关键词 / 加签两种安全设置。"""
        url = self._dingtalk_webhook
        content = f"【{self.plugin_name}】{title}\n{text}"
        # 自定义关键词：消息内容必须包含关键词，否则被钉钉拒收
        if self._dingtalk_keyword and self._dingtalk_keyword not in content:
            content = f"{self._dingtalk_keyword} {content}"
        # 加签：URL 追加 timestamp + sign
        if self._dingtalk_secret:
            try:
                ts = str(round(time.time() * 1000))
                string_to_sign = f"{ts}\n{self._dingtalk_secret}"
                hmac_code = hmac.new(self._dingtalk_secret.encode("utf-8"),
                                     string_to_sign.encode("utf-8"),
                                     digestmod=hashlib.sha256).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}timestamp={ts}&sign={sign}"
            except Exception as e:
                logger.warning(f"[{self.plugin_name}] 钉钉加签失败: {e}")
        payload = {"msgtype": "text", "text": {"content": content}}
        try:
            resp = requests.post(url, json=payload, timeout=10).json()
            if resp.get("errcode") == 0:
                logger.info(f"[{self.plugin_name}] 钉钉推送成功")
            else:
                logger.warning(f"[{self.plugin_name}] 钉钉推送失败: {resp}")
        except Exception as e:
            logger.warning(f"[{self.plugin_name}] 钉钉推送异常: {e}")

    def _run_test_push(self):
        """发一条测试通知，用于验证推送链路（MP 通知渠道 或 钉钉直推）。"""
        way = "钉钉直推" if (self._push_target == "dingtalk" and self._dingtalk_webhook) else "MP 通知渠道"
        self._send_notify("测试推送", f"这是一条来自「{self.plugin_name}」的测试通知。\n"
                                      f"当前推送方式：{way}。收到即说明链路正常。")
        logger.info(f"[{self.plugin_name}] 已发送测试通知（{way}）")

    # ---------- 校验：走 OpenList，防风控 ----------

    def _run_check(self) -> bool:
        """通过 OpenList 校验 115 cookie 是否有效。返回 True=有效。"""
        if not self._precheck_openlist():
            return False
        if not self._probe_path:
            self._set_status("error", "未配置探针目录")
            logger.error(f"[{self.plugin_name}] 未配置探针目录")
            return False

        self._set_status("checking", f"正在校验探针目录 {self._probe_path}")
        ok, detail = self._probe_via_openlist()
        if ok:
            self._set_status("valid", f"cookie 有效（探针 {self._probe_path} 可访问）")
            logger.info(f"[{self.plugin_name}] 校验通过：{detail}")
            return True

        # 失效
        self._set_status("invalid", f"cookie 疑似失效：{detail}")
        logger.warning(f"[{self.plugin_name}] 校验失败：{detail}")
        self._send_notify("115 cookie 失效", f"探针目录 {self._probe_path} 访问失败：{detail}\n"
                                              f"{'即将自动发起扫码登录，请到插件页扫码。' if self._auto_relogin else '请手动发起扫码登录。'}")
        if self._auto_relogin and self._cooldown_ok():
            self._run_qr_login()
        return False

    def _precheck_openlist(self) -> bool:
        if not self._openlist_url or not self._openlist_token:
            self._set_status("error", "OpenList 地址或 token 未配置")
            logger.error(f"[{self.plugin_name}] OpenList 地址或 token 未配置")
            return False
        return True

    def _cooldown_ok(self) -> bool:
        """重登录冷静期：避免频繁登录触发风控。"""
        last = self.get_data("last_login_ts") or 0
        if time.time() - float(last) < self._relogin_cooldown_min * 60:
            logger.info(f"[{self.plugin_name}] 处于重登录冷静期，跳过本次自动登录")
            return False
        return True

    def _probe_via_openlist(self) -> Tuple[bool, str]:
        """
        用 OpenList /api/fs/list(refresh) 打探针目录。
        - code==200 -> cookie 有效
        - 服务器有响应但 code!=200（driver 报错，多为 cookie 失效） -> 失效
        - 网络异常 -> 判定为 unknown（不触发重登录，返回失效但带 network 标记由上层决定）
        为降低误判：网络类异常重试 2 次；仍失败按「无法确认」处理，不当作 cookie 失效。
        """
        list_url = f"{self._openlist_url}/api/fs/list"
        headers = {"Authorization": self._openlist_token, "Content-Type": "application/json"}
        payload = {"path": self._probe_path, "page": 1, "per_page": 100, "refresh": True}
        server_resp = None
        for attempt in range(1, 4):
            try:
                resp = requests.post(list_url, headers=headers, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json() or {}
                server_resp = data
                break
            except Exception as e:
                logger.warning(f"[{self.plugin_name}] 探针请求第 {attempt} 次异常：{e}")
                if attempt < 3:
                    time.sleep(attempt * 2)
        if server_resp is None:
            # 网络层始终失败：无法确认 cookie 死活，保守起见不判失效
            return False, "无法连接 OpenList（网络异常，未判定 cookie 失效）"

        code = server_resp.get("code")
        if code == 200:
            if self._deep_check:
                dok, dmsg = self._deep_probe()
                if not dok:
                    return False, f"列目录成功但取直链失败：{dmsg}"
            return True, "列目录成功"
        return False, f"OpenList 返回 {code}: {server_resp.get('message')}"

    def _deep_probe(self) -> Tuple[bool, str]:
        """深度校验：取探针目录里第一个文件的直链并做 1 字节 Range 请求。"""
        get_url = f"{self._openlist_url}/api/fs/get"
        headers = {"Authorization": self._openlist_token, "Content-Type": "application/json"}
        list_url = f"{self._openlist_url}/api/fs/list"
        try:
            lr = requests.post(list_url, headers=headers,
                               json={"path": self._probe_path, "page": 1, "per_page": 100,
                                     "refresh": False}, timeout=30).json()
            content = (lr.get("data") or {}).get("content") or []
            first_file = next((x for x in content if not x.get("is_dir")), None)
            if not first_file:
                return True, "探针目录无文件，跳过深度校验"
            fpath = f"{self._probe_path.rstrip('/')}/{first_file.get('name')}"
            gr = requests.post(get_url, headers=headers,
                               json={"path": fpath, "refresh": False}, timeout=30).json()
            if gr.get("code") != 200:
                return False, f"fs/get 返回 {gr.get('code')}: {gr.get('message')}"
            raw_url = (gr.get("data") or {}).get("raw_url")
            if not raw_url:
                return False, "未取到直链 raw_url"
            rr = requests.get(raw_url, headers={"Range": "bytes=0-1", "User-Agent": self._UA},
                              timeout=30, stream=True)
            if rr.status_code in (200, 206):
                return True, "直链可访问"
            return False, f"直链返回 HTTP {rr.status_code}"
        except Exception as e:
            return False, f"深度校验异常：{e}"

    # ---------- 扫码登录 ----------

    def _run_qr_login(self):
        """完整扫码登录流程：取 token -> 显示二维码 -> 轮询状态 -> 取 cookie -> 写回 OpenList。"""
        if not self._lock.acquire(blocking=False):
            logger.info(f"[{self.plugin_name}] 已有登录流程在进行，跳过")
            return
        try:
            self.save_data("last_login_ts", time.time())
            app = self._login_app if self._login_app in self._APP_CHOICES else "alipaymini"
            sess = requests.Session()
            sess.headers.update({"User-Agent": self._UA})

            # 1. 取二维码 token
            try:
                token_resp = sess.get(self._QR_TOKEN_URL, timeout=30).json()
            except Exception as e:
                self._set_status("error", f"获取二维码失败：{e}")
                self._send_notify("扫码登录失败", f"获取二维码 token 异常：{e}")
                return
            if token_resp.get("state") != 1 and not token_resp.get("data"):
                self._set_status("error", f"获取二维码失败：{token_resp}")
                self._send_notify("扫码登录失败", f"获取二维码 token 返回异常：{token_resp}")
                return
            qr_data = token_resp["data"]
            uid = qr_data.get("uid")
            sign = qr_data.get("sign")
            tm = qr_data.get("time")

            # 2. 取二维码图片，转 base64 显示在插件页
            qr_b64 = ""
            try:
                img = sess.get(self._QR_IMAGE_URL.format(uid=uid), timeout=30)
                if img.status_code == 200 and img.content:
                    qr_b64 = "data:image/png;base64," + base64.b64encode(img.content).decode()
            except Exception as e:
                logger.warning(f"[{self.plugin_name}] 取二维码图片失败：{e}")
            self._set_status(
                "qr_waiting",
                f"二维码已生成（设备类型 {app}），请用 115 手机 App 扫码，扫码后在手机点确认",
                qr_image=qr_b64,
                qr_tip="打开 MP → 本插件配置页即可看到二维码；用 115 生活 App 扫一扫",
            )
            self._send_notify("115 扫码登录已就绪",
                              f"请打开 MoviePilot →「{self.plugin_name}」插件配置页，用 115 手机 App 扫码登录。\n"
                              f"设备类型：{app}")

            # 3. 轮询扫码状态（长轮询，最多 ~4 分钟）
            deadline = time.time() + 240
            scanned_notified = False
            while time.time() < deadline:
                try:
                    status_resp = sess.get(
                        self._QR_STATUS_URL,
                        params={"uid": uid, "time": tm, "sign": sign, "_": int(time.time() * 1000)},
                        timeout=45,
                    ).json()
                except requests.exceptions.Timeout:
                    continue
                except Exception as e:
                    logger.warning(f"[{self.plugin_name}] 轮询状态异常：{e}")
                    time.sleep(2)
                    continue
                status = (status_resp.get("data") or {}).get("status")
                if status == 1 and not scanned_notified:
                    scanned_notified = True
                    self._set_status("qr_waiting", "扫描成功，请在手机上点确认登录", qr_image=qr_b64)
                elif status == 2:
                    break
                elif status == -1:
                    self._set_status("error", "二维码已过期，请重新发起登录")
                    self._send_notify("扫码登录失败", "二维码已过期，请重新发起扫码登录")
                    return
                elif status == -2:
                    self._set_status("error", "已取消登录")
                    self._send_notify("扫码登录失败", "已在手机上取消登录")
                    return
            else:
                self._set_status("error", "扫码超时（4 分钟未完成），请重新发起")
                self._send_notify("扫码登录失败", "扫码超时，请重新发起扫码登录")
                return

            # 4. 取 cookie
            try:
                result = sess.post(
                    self._QR_RESULT_URL.format(app=app),
                    data={"app": app, "account": uid},
                    timeout=30,
                ).json()
            except Exception as e:
                self._set_status("error", f"获取 cookie 失败：{e}")
                self._send_notify("扫码登录失败", f"换取 cookie 异常：{e}")
                return
            cookie_dict = (result.get("data") or {}).get("cookie") or {}
            if not cookie_dict:
                self._set_status("error", f"未取到 cookie：{result}")
                self._send_notify("扫码登录失败", f"登录结果未包含 cookie：{result}")
                return
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_dict.items())

            # 5. 写回 OpenList
            ok, msg = self._push_cookie_to_openlist(cookie_str)
            if ok:
                self._set_status("valid", f"扫码登录成功并已写回 OpenList：{msg}")
                self._send_notify("115 cookie 已更新", f"扫码登录成功，已写回 OpenList 存储：{msg}")
            else:
                self._set_status("error", f"扫码成功但写回 OpenList 失败：{msg}")
                self._send_notify("115 cookie 写回失败", f"已拿到新 cookie，但写回 OpenList 失败：{msg}\n"
                                                          f"可稍后重试或手动更新。")
        finally:
            self._lock.release()

    # ---------- 手动粘贴 cookie ----------

    def _apply_manual_cookie(self):
        if not self._precheck_openlist():
            return
        cookie_str = (self._manual_cookie or "").strip()
        if not cookie_str or "=" not in cookie_str:
            self._set_status("error", "手动 cookie 为空或格式不正确")
            self._send_notify("手动更新失败", "粘贴的 cookie 为空或格式不正确（应形如 UID=..; CID=..; SEID=..）")
            return
        ok, msg = self._push_cookie_to_openlist(cookie_str)
        if ok:
            self._set_status("valid", f"手动 cookie 已写回 OpenList：{msg}")
            self._send_notify("115 cookie 已更新", f"手动 cookie 已写回 OpenList 存储：{msg}")
        else:
            self._set_status("error", f"手动 cookie 写回失败：{msg}")
            self._send_notify("手动更新失败", f"写回 OpenList 失败：{msg}")

    # ---------- 写回 OpenList 存储 ----------

    def _push_cookie_to_openlist(self, cookie_str: str) -> Tuple[bool, str]:
        """找到 OpenList 里 driver 含关键字(默认115)且 addition 有 cookie 字段的存储，更新其 cookie。"""
        headers = {"Authorization": self._openlist_token, "Content-Type": "application/json"}
        list_url = f"{self._openlist_url}/api/admin/storage/list"
        update_url = f"{self._openlist_url}/api/admin/storage/update"
        try:
            resp = requests.get(list_url, headers=headers,
                                params={"page": 1, "per_page": 1000}, timeout=30)
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception as e:
            return False, f"读取存储列表异常：{e}"
        if data.get("code") != 200:
            return False, f"读取存储列表返回 {data.get('code')}: {data.get('message')}（token 是否为管理员令牌？）"

        storages = (data.get("data") or {}).get("content") or []
        kw = (self._driver_keyword or "115").lower()
        targets = []
        for st in storages:
            driver = str(st.get("driver") or "")
            try:
                addition = json.loads(st.get("addition") or "{}")
            except Exception:
                addition = {}
            if kw in driver.lower() and "cookie" in addition:
                targets.append((st, addition))
        if not targets:
            return False, f"未找到匹配的 115 存储（driver 含「{self._driver_keyword}」且含 cookie 字段）"

        updated, failed = [], []
        for st, addition in targets:
            addition["cookie"] = cookie_str
            st["addition"] = json.dumps(addition, ensure_ascii=False)
            try:
                ur = requests.post(update_url, headers=headers, json=st, timeout=30).json()
                if ur.get("code") == 200:
                    updated.append(st.get("mount_path") or str(st.get("id")))
                else:
                    failed.append(f"{st.get('mount_path')}: {ur.get('message')}")
            except Exception as e:
                failed.append(f"{st.get('mount_path')}: {e}")

        if updated and not failed:
            return True, f"已更新 {len(updated)} 个存储：{', '.join(updated)}"
        if updated and failed:
            return True, f"部分成功，更新 {', '.join(updated)}；失败 {', '.join(failed)}"
        return False, f"全部失败：{', '.join(failed)}"

    # ---------- 配置界面 ----------

    @staticmethod
    def _col(cols: int, comp: str, model: str, label: str, **props) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": cols},
            "content": [
                {"component": comp, "props": {"model": model, "label": label, **props}}
            ],
        }

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        status = self._get_status()
        # 动态状态区：状态提示 + （若在等待扫码）二维码图片
        status_blocks = [
            {
                "component": "VAlert",
                "props": {
                    "type": self._alert_type(status.get("state")),
                    "variant": "tonal",
                    "text": f"当前状态：{status.get('state')} | {status.get('message')}"
                            f"（更新于 {status.get('updated') or 'N/A'}）",
                },
            }
        ]
        if status.get("state") == "qr_waiting" and status.get("qr_image"):
            status_blocks.append({
                "component": "VImg",
                "props": {
                    "src": status.get("qr_image"),
                    "width": 220, "height": 220,
                    "class": "mx-auto my-2",
                },
            })
            status_blocks.append({
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal",
                          "text": status.get("qr_tip") or "用 115 手机 App 扫码，扫码后在手机点确认。"
                                                          "扫码完成后重新打开本页可看到最新状态。"},
            })

        form = [
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": status_blocks},
                    ]},
                    # 开关
                    {"component": "VRow", "content": [
                        self._col(4, "VSwitch", "enabled", "启用插件"),
                        self._col(4, "VSwitch", "notify", "发送通知"),
                        self._col(4, "VSwitch", "auto_relogin", "失效后自动发起扫码登录"),
                    ]},
                    # 一次性动作（相当于按钮：打开开关并「保存」即执行一次，随后自动关闭）
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal", "density": "compact",
                                "text": "下面三个开关相当于「按钮」：打开对应开关后点页面底部「保存」即执行一次，"
                                        "执行完自动关闭。执行结果稍后在上方状态栏显示（重新打开本页可刷新状态）。"
                                        "想一键即时触发，也可用远程命令 /sync115_check、/sync115_login。",
                            }},
                        ]},
                    ]},
                    {"component": "VRow", "content": [
                        self._col(4, "VSwitch", "check_now", "▶ 检测状态（开+保存执行）"),
                        self._col(4, "VSwitch", "login_now", "▶ 扫码登录（开+保存执行）"),
                        self._col(4, "VSwitch", "apply_cookie_now", "▶ 写回手动 cookie（开+保存执行）"),
                    ]},
                    # OpenList
                    {"component": "VRow", "content": [
                        self._col(6, "VTextField", "openlist_url", "OpenList 地址",
                                  placeholder="http://192.168.1.111:5244"),
                        self._col(6, "VTextField", "openlist_token", "OpenList 管理员 Token",
                                  placeholder="openlist-xxxxxx（需管理员令牌）"),
                    ]},
                    {"component": "VRow", "content": [
                        self._col(6, "VTextField", "probe_path", "探针目录 (OpenList 挂载路径)",
                                  placeholder="/115/_healthcheck"),
                        self._col(3, "VTextField", "driver_keyword", "115 存储 driver 关键字",
                                  placeholder="115"),
                        self._col(3, "VSwitch", "deep_check", "深度校验 (取直链)"),
                    ]},
                    # 登录参数
                    {"component": "VRow", "content": [
                        self._col(4, "VSelect", "login_app", "扫码设备类型 (app)",
                                  items=self._APP_CHOICES),
                        self._col(4, "VTextField", "cron", "定时校验 Cron",
                                  placeholder="*/30 * * * *"),
                        self._col(4, "VTextField", "relogin_cooldown_min", "重登录冷静期(分钟)",
                                  placeholder="30"),
                    ]},
                    # 推送方式
                    {"component": "VRow", "content": [
                        self._col(4, "VSelect", "push_target", "推送方式",
                                  items=[
                                      {"title": "钉钉直推(不依赖MP通知)", "value": "dingtalk"},
                                      {"title": "MP 通知渠道", "value": "mp"},
                                  ]),
                        self._col(4, "VSelect", "notify_type", "MP通知类型(仅push=mp时用)",
                                  items=["Plugin", "SiteMessage", "Manual", "MediaServer", "Organize"]),
                        self._col(4, "VSwitch", "test_push_now", "▶ 发送测试推送（开+保存）"),
                    ]},
                    # 钉钉直推参数
                    {"component": "VRow", "content": [
                        self._col(12, "VTextField", "dingtalk_webhook", "钉钉机器人 Webhook 地址",
                                  placeholder="https://oapi.dingtalk.com/robot/send?access_token=xxx"),
                    ]},
                    {"component": "VRow", "content": [
                        self._col(6, "VTextField", "dingtalk_keyword", "钉钉安全设置-自定义关键词(可选)",
                                  placeholder="机器人设了关键词就填，消息会带上它"),
                        self._col(6, "VTextField", "dingtalk_secret", "钉钉安全设置-加签密钥(可选)",
                                  placeholder="SEC 开头，设了加签才填"),
                    ]},
                    # 手动 cookie
                    {"component": "VRow", "content": [
                        self._col(12, "VTextarea", "manual_cookie",
                                  "手动 cookie (兜底，形如 UID=..; CID=..; SEID=..)",
                                  placeholder="扫码不便时，把浏览器里的 115 cookie 粘到这里，"
                                              "再打开「写回手动 cookie」保存",
                                  rows=2, autoGrow=True),
                    ]},
                    # 说明
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VAlert", "props": {
                                "type": "info", "variant": "tonal",
                                "text": "校验走 OpenList 的 fs/list(refresh) 打「探针目录」判断 cookie 死活，"
                                        "防风控且贴近真实访问；建议在 115 里建一个只有 1~2 个小文件的目录作探针。"
                                        "失效后走扫码登录：二维码显示在本页，用 115 手机 App 扫一扫。"
                                        "设备类型默认 alipaymini，独占一个设备槽，避免和你浏览器的 115 网页登录互踢。"
                                        "拿到新 cookie 后自动写回 OpenList 中 driver 含「115」的存储。",
                            }},
                        ]},
                    ]},
                ],
            }
        ]
        defaults = {
            "enabled": False,
            "notify": True,
            "notify_type": "Plugin",
            "push_target": "dingtalk",
            "dingtalk_webhook": "",
            "dingtalk_keyword": "",
            "dingtalk_secret": "",
            "auto_relogin": True,
            "check_now": False,
            "login_now": False,
            "apply_cookie_now": False,
            "test_push_now": False,
            "openlist_url": "",
            "openlist_token": "",
            "probe_path": "",
            "driver_keyword": "115",
            "deep_check": False,
            "login_app": "alipaymini",
            "cron": "",
            "relogin_cooldown_min": 30,
            "manual_cookie": "",
        }
        return form, defaults

    @staticmethod
    def _alert_type(state: Optional[str]) -> str:
        return {
            "valid": "success",
            "checking": "info",
            "qr_waiting": "warning",
            "invalid": "error",
            "error": "error",
        }.get(state or "idle", "info")

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
