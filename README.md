# MoviePilot 自用插件

把本目录（`mp-plugins/`）推到你自己的 GitHub 仓库后，
在 MoviePilot 的「插件」→「插件仓库」里加上仓库地址（例如
`https://github.com/ahnuchen/your-mp-plugins`），MP 会自动从
`plugins.v2/` 目录读取插件列表。

> ⚠️ 如果想走 MP 官方插件市场约定的加载路径，需要把本目录内容放到
> 仓库根目录的 `plugins.v2/` 下（或在仓库里建软链接），并同时提供
> `package.v2.json` 索引文件。最简单的做法是直接把 `mp-plugins/` 重命名
> 为 `plugins.v2/` 放到仓库根。

## 插件清单

| 插件目录 | 名称 | 说明 |
|---|---|---|
| `openlistscan/` | OpenList 扫描触发器 | 一键触发 OpenList 扫描 + MP 目录整理，替代每次手动 curl |

## 安装方式

### 方式 A：通过 MP 第三方插件仓库（推荐）

1. 在 GitHub 创建一个独立仓库（如 `yahoo2022/moviepilot-plugins`）
2. 仓库结构如下：
   ```
   moviepilot-plugins/
   └── plugins/
       └── openlistscan/
           ├── __init__.py
           ├── requirements.txt
           └── package.json
   ```
3. 把本目录下 `openlistscan/` 的内容推到那个仓库的 `plugins/openlistscan/` 下
4. 在 MP 后台：「设定」→「系统」→ 找到「PLUGIN_MARKET」配置项
5. 在已有的仓库地址后面加上你的仓库地址（逗号分隔）：
   ```
   https://github.com/jxxghp/MoviePilot-Plugins,https://github.com/yahoo2022/moviepilot-plugins
   ```
6. 重启 MP：`cd /opt && docker compose restart moviepilot`
7. 在 MP 后台「插件」页面搜索"OpenList"即可安装

### 方式 B：通过环境变量指定仓库

在 `docker-compose.yml` 的 moviepilot 服务里加环境变量：

```yaml
environment:
  - PLUGIN_MARKET=https://github.com/jxxghp/MoviePilot-Plugins,https://github.com/yahoo2022/moviepilot-plugins
```

然后 `cd /opt && docker compose up -d`。

## openlistscan 使用说明

### 功能

把家庭媒体服务器流程里的三步操作（手动 curl OpenList 扫描 → 等扫描完 → 到 MP 点整理）
压缩成 MP 后台的一次点击，或一次 Cron 自动触发，或一次 webhook 调用。

### 安装

**方式 A：本地调试（推荐先走这条）**

```bash
# 把插件拷进 MP 容器的本地插件目录
sudo cp -r mp-plugins/openlistscan /opt/MoviePilot/config/plugins/

# 重启 MP 让它加载
cd /opt && docker compose restart moviepilot
```

**方式 B：通过 GitHub 插件仓库**

1. 把本目录推到 GitHub，整理成 `plugins.v2/openlistscan/` 结构
2. 在根目录加 `package.v2.json`（参考 MP 官方插件市场格式）
3. MP 后台「设定」→「插件」→「插件仓库」→「添加第三方仓库」

### 配置字段

| 字段 | 说明 | 建议值 |
|---|---|---|
| 启用插件 | 总开关 | 开启 |
| 发送通知 | 扫描/整理结果发到 MP 消息通道 | 开启 |
| 立即执行一次 | 保存后立刻跑一次，完成后自动关闭这个开关 | 需要时开启 |
| 扫描后自动 MP 整理 | 扫描完成后是否继续调 MP 整理 | 开启 |
| OpenList 地址 | | `http://192.168.1.111:5244` |
| OpenList Token | | `openlist-xxx...` |
| 扫描路径 | OpenList 中的源目录 | `/115/云下载` |
| 扫描速率 limit | 递归速率（越小越保守，1-5 均可） | `2` |
| 超时秒数 | 最多等这么久还没结束就放弃等 | `1800` |
| MP 整理源目录 | **MP 容器里看到的路径**，不是宿主机路径 | `/media/云下载` |
| Cron 定时 | 可选，填了会按周期跑 | 空 / `0 */2 * * *` |

> 注意：你的 `docker-compose.yml` 里 MP 挂的是 `/home/115strm:/media`，
> 所以 MP 容器内看到的路径是 `/media/云下载`，不是 `/home/115strm/云下载`。

### 三种触发方式

1. **Web UI**：插件配置页把「立即执行一次」打开 → 保存。执行完开关自动关。
2. **远程命令**：在 MP 绑定的 Telegram/微信/WebPush 等消息渠道发 `/openlist_scan`。
3. **HTTP Webhook**：
   ```
   POST http://192.168.1.111:3001/api/v1/plugin/OpenListScan/scan
   Header: Authorization: Bearer <MP API Token>
   ```
   可以把这个 URL 做成手机桌面快捷方式（用 iOS 快捷指令或安卓 HTTP Shortcuts）。
4. **定时**：填 Cron 表达式，启用插件即可。

### 流程

```
按钮 / 命令 / Webhook / Cron
       │
       ▼
POST /api/admin/scan/start   ←  OpenList
       │
       ▼
轮询 /api/admin/scan/progress 直到 is_running=false
       │
       ▼
TransferChain().manual_transfer(FileItem(/media/云下载))  ←  MP
       │
       ▼
MP 自动识别 / 刮削 / 入库
```

MDC 刮削依然手动：你文档里说用得少，不值得为它单独做自动化。

### 常见问题

- **扫描启动后接口立刻返回，但 OpenList 其实还在扫**：正常，`scan/start` 是异步的，
  插件会轮询 `scan/progress` 等它真正结束。
- **MP 整理失败说路径找不到**：检查「MP 整理源目录」填的是不是 MP 容器里的路径
  （`/media/xxx`），不是宿主机的 `/home/115strm/xxx`。
- **想分子目录扫更快**：在 115 离线下载时分 `/115/云下载/电影`、`/115/云下载/剧集`，
  然后把插件的「扫描路径」改成对应子目录即可。不需要新增 OpenList 存储。
