# MoviePilot 自用插件

## 插件清单

| 插件目录 | 名称 | 说明 |
|---|---|---|
| `plugins.v2/mediapipeline/` | 媒体入库流水线 | 四步合一：OpenList 扫描 + 网盘改名清洗(改 115 源防 insert 复活) + 增量整理刮削 + Emby 扫描；各步独立开关，含 115 防风控 + 预演。**主线推荐** |
| `plugins.v2/sourcepipeline/` | 115 源流水线 | 新方向阶段1只读实现：来源实例与可复用 logic 分开配置，按请求预算广度优先遍历多层 `/115/...`，SQLite v2 断点续扫；Cron/命令/API 只跑 cache，当前无改名、删除或 STRM 生成能力 |
| `plugins.v2/mdcstrmprep/` | MDC STRM 预处理 | 日本厂商/FC2 独立番号 profile；v0.1 强制预演，只读源并输出冲突/重复/审核 manifest，不写 inbox |
| `plugins.v2/incrpipeline/` | 增量入库流水线 | 一次触发顺序执行 OpenList 扫描生成 STRM + 增量整理刮削 + Emby 全库扫描，三步各有独立开关（mediapipeline 前身，无改名步骤） |
| `plugins.v2/openlistscan/` | OpenList 扫描触发器 | 一键触发 OpenList 扫描 + MP 目录整理 |
| `plugins.v2/strmrename/` | STRM 剧集重命名助手 | 电视剧按一级目录名统一命名 SxxExx；电影只清垃圾；含集号铁律防误删 |
| `plugins.v2/incrtransfer/` | 增量整理刮削 | 只整理最近 N 天新增/改动的媒体，支持电影/电视剧、复制/移动/链接/自动、目标路径与刮削 |
| `plugins.v2/cookiesync115/` | 115 Cookie 同步 | 定时经 OpenList 探针校验 115 cookie，失效时扫码登录取新 cookie 并写回 OpenList；含手动粘贴 cookie 兜底 |
| `plugins.v2/quarkclean/` | 夸克改名清洗 | 夸克网盘版「步骤2」：读本地夸克 strm → OpenList 改夸克源名(裸集号补 SxxExx) + 清垃圾 + 目录名清洗，含预演与防风控 |

> 仓库只面向 MoviePilot V2：索引为 `package.v2.json`，代码在 `plugins.v2/`。

## 安装

在 MoviePilot 的 `PLUGIN_MARKET` 环境变量中添加本仓库地址（逗号分隔）：

```
https://github.com/jxxghp/MoviePilot-Plugins,https://github.com/yahoo2022/moviepilot-plugins
```

重启 MoviePilot 后在「插件」页面搜索安装即可。

## 本地调试

```bash
cp -r plugins.v2/mediapipeline /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/sourcepipeline /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/mdcstrmprep /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/openlistscan /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/strmrename /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/incrtransfer /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/incrpipeline /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/cookiesync115 /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/quarkclean /path/to/MoviePilot/config/plugins/
docker compose restart moviepilot
```
