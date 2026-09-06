"""只读 STRM 文件扫描器。"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

try:
    from .models import ItemState, ScanResult, ScanStats, SourceItem
except ImportError:  # 允许把核心目录直接加入 sys.path 调试
    from models import ItemState, ScanResult, ScanStats, SourceItem

DEFAULT_MAX_BYTES = 1024 * 1024


def cutoff_from_recent_days(recent_days: float, now_ts: float | None = None) -> float | None:
    """把最近天数转换为 mtime 下限；非正数表示全量。"""

    if recent_days <= 0:
        return None
    current = time.time() if now_ts is None else now_ts
    return current - recent_days * 86400


def scan_strm(
    root: str | Path,
    profile: str,
    *,
    recursive: bool = True,
    recent_days: float = 0,
    cutoff_ts: float | None = None,
    stable_seconds: float = 0,
    max_items: int = 0,
    max_bytes: int = DEFAULT_MAX_BYTES,
    now_ts: float | None = None,
) -> ScanResult:
    """扫描根散或递归 STRM，返回发现项、问题项和统计。

    文件系统始终只读。``max_items <= 0`` 表示不限制；路径先排序再处理，
    以保证相同输入得到确定结果。
    """

    root_path = Path(root)
    stats = ScanStats()
    found: list[SourceItem] = []
    issues: list[SourceItem] = []
    current = time.time() if now_ts is None else now_ts
    effective_cutoff = cutoff_ts
    if effective_cutoff is None:
        effective_cutoff = cutoff_from_recent_days(recent_days, current)

    if not root_path.exists() or not root_path.is_dir():
        stats.failed = 1
        issues.append(
            _issue_item(root_path, root_path, profile, ItemState.FAILED, "扫描根目录不存在或不是目录")
        )
        return ScanResult(tuple(found), tuple(issues), stats)

    iterator = root_path.rglob("*") if recursive else root_path.glob("*")
    paths = sorted(
        (path for path in iterator if path.is_file() and path.suffix.casefold() == ".strm"),
        key=lambda path: path.relative_to(root_path).as_posix().casefold(),
    )
    stats.candidates = len(paths)

    for index, path in enumerate(paths):
        if max_items > 0 and len(found) + len(issues) >= max_items:
            stats.limited = len(paths) - index
            break
        try:
            stat = path.stat()
        except OSError as exc:
            stats.failed += 1
            issues.append(_issue_item(root_path, path, profile, ItemState.FAILED, f"读取文件元数据失败: {exc}"))
            continue
        if effective_cutoff is not None and stat.st_mtime < effective_cutoff:
            stats.filtered_old += 1
            continue
        if stable_seconds > 0 and stat.st_mtime > current - stable_seconds:
            stats.filtered_unstable += 1
            continue
        if stat.st_size <= 0:
            stats.invalid += 1
            issues.append(_issue_item(root_path, path, profile, ItemState.INVALID_STRM, "STRM 为空", stat))
            continue
        if max_bytes <= 0 or stat.st_size > max_bytes:
            stats.invalid += 1
            issues.append(
                _issue_item(
                    root_path,
                    path,
                    profile,
                    ItemState.INVALID_STRM,
                    f"STRM 超过安全大小上限 {max_bytes} bytes",
                    stat,
                )
            )
            continue
        try:
            raw = path.read_bytes()
        except OSError as exc:
            stats.failed += 1
            issues.append(_issue_item(root_path, path, profile, ItemState.FAILED, f"读取 STRM 失败: {exc}", stat))
            continue

        digest = hashlib.sha256(raw).hexdigest()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                stats.invalid += 1
                issues.append(
                    _issue_item(
                        root_path,
                        path,
                        profile,
                        ItemState.INVALID_STRM,
                        f"STRM 不是有效 UTF-8 文本: {exc}",
                        stat,
                        digest,
                    )
                )
                continue
        url = next((line.strip() for line in text.splitlines() if line.strip()), "")
        parsed = urlsplit(url)
        if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
            stats.invalid += 1
            issues.append(
                _issue_item(
                    root_path,
                    path,
                    profile,
                    ItemState.INVALID_STRM,
                    "第一条非空行不是有效 HTTP(S) URL",
                    stat,
                    digest,
                )
            )
            continue
        leaf = PurePosixPath(unquote(parsed.path)).name
        if not leaf:
            stats.invalid += 1
            issues.append(
                _issue_item(root_path, path, profile, ItemState.INVALID_STRM, "URL 不含叶子文件名", stat, digest)
            )
            continue
        stats.discovered += 1
        found.append(
            SourceItem(
                profile=profile.casefold(),
                root=root_path,
                path=path,
                relative_path=path.relative_to(root_path).as_posix(),
                local_stem=path.stem,
                parent_names=_parents(path, root_path),
                url_leaf=leaf,
                raw_url=url,
                content_sha256=digest,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
            )
        )
    return ScanResult(tuple(found), tuple(issues), stats)


def scan_directory(*args: Any, **kwargs: Any) -> ScanResult:
    """``scan_strm`` 的语义化别名。"""

    return scan_strm(*args, **kwargs)


def _parents(path: Path, root: Path) -> tuple[str, ...]:
    """返回离文件最近的至多三层父目录，不包含扫描根自身。"""

    values: list[str] = []
    current = path.parent
    while current != root and len(values) < 3:
        values.append(current.name)
        current = current.parent
    return tuple(values)


def _issue_item(
    root: Path,
    path: Path,
    profile: str,
    state: ItemState,
    error: str,
    stat: object | None = None,
    digest: str = "",
) -> SourceItem:
    """构造不泄露 STRM 内容的问题记录。"""

    try:
        relative = path.relative_to(root).as_posix()
    except ValueError:
        relative = path.as_posix()
    size = int(getattr(stat, "st_size", 0))
    mtime_ns = int(getattr(stat, "st_mtime_ns", 0))
    return SourceItem(
        profile=profile.casefold(),
        root=root,
        path=path,
        relative_path=relative,
        local_stem=path.stem,
        parent_names=_parents(path, root) if path != root else (),
        content_sha256=digest,
        size=size,
        mtime_ns=mtime_ns,
        state=state,
        error=error,
    )
