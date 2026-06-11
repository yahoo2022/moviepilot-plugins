# MoviePilot 自用插件

## 插件清单

| 插件目录 | 名称 | 说明 |
|---|---|---|
| `plugins.v2/openlistscan/` | OpenList 扫描触发器 | 一键触发 OpenList 扫描 + MP 目录整理 |
| `plugins.v2/strmrename/` | STRM 剧集重命名助手 | 按上级目录名把纯数字 STRM 重命名为 SxxExx |
| `plugins.v2/incrtransfer/` | 增量整理刮削 | 只整理最近 N 天新增/改动的媒体，支持电影/电视剧、复制/移动/链接/自动、目标路径与刮削 |

> 仓库只面向 MoviePilot V2：索引为 `package.v2.json`，代码在 `plugins.v2/`。

## 安装

在 MoviePilot 的 `PLUGIN_MARKET` 环境变量中添加本仓库地址（逗号分隔）：

```
https://github.com/jxxghp/MoviePilot-Plugins,https://github.com/yahoo2022/moviepilot-plugins
```

重启 MoviePilot 后在「插件」页面搜索安装即可。

## 本地调试

```bash
cp -r plugins.v2/openlistscan /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/strmrename /path/to/MoviePilot/config/plugins/
cp -r plugins.v2/incrtransfer /path/to/MoviePilot/config/plugins/
docker compose restart moviepilot
```
