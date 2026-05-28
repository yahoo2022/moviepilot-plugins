# MoviePilot 自用插件

## 插件清单

| 插件目录 | 名称 | 说明 |
|---|---|---|
| `plugins.v2/openlistscan/` | OpenList 扫描触发器 | 一键触发 OpenList 扫描 + MP 目录整理 |
| `plugins.v2/strmrename/` | STRM 剧集重命名助手 | 按上级目录名把纯数字 STRM 重命名为 SxxExx |

> `plugins/` 是 MoviePilot V1 的副本,实际生效的是 `plugins.v2/`(MP V2)。

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
docker compose restart moviepilot
```
