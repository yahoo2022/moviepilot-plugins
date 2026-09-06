"""规划 manifest 和摘要报告的原子写入。"""

from __future__ import annotations

import csv
import json
import os
import tempfile
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TextIO

try:
    from .models import PlanItem, RunSummary, ScanStats
except ImportError:  # 允许把核心目录直接加入 sys.path 调试
    from models import PlanItem, RunSummary, ScanStats

_TSV_FIELDS = (
    "state",
    "profile",
    "relative_path",
    "local_stem",
    "url_leaf",
    "canonical",
    "target_name",
    "suffix",
    "confidence",
    "content_sha256",
    "rules",
    "observations",
    "conflict_with",
    "reason",
)


def write_run(
    data_dir: str | Path,
    items: Iterable[PlanItem],
    *,
    run_id: str | None = None,
    scan_stats: ScanStats | None = None,
    keep_reports: int = 20,
) -> dict[str, Path]:
    """在 ``data_dir/runs`` 原子发布 JSONL、TSV 和 summary。

    报告只序列化 ``PlanItem.to_record``，该记录明确排除了 raw URL 和
    STRM 原始内容。``keep_reports=0`` 表示不轮转。
    """

    item_list = list(items)
    identifier = _safe_run_id(run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ"))
    runs_dir = Path(data_dir) / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "jsonl": runs_dir / f"{identifier}.jsonl",
        "tsv": runs_dir / f"{identifier}.tsv",
        "summary": runs_dir / f"{identifier}.summary.json",
    }
    summary = RunSummary.from_items(identifier, item_list, scan_stats)

    _atomic_text(paths["jsonl"], lambda handle: _write_jsonl(handle, item_list))
    _atomic_text(paths["tsv"], lambda handle: _write_tsv(handle, item_list))
    _atomic_text(
        paths["summary"],
        lambda handle: json.dump(summary.to_record(), handle, ensure_ascii=False, indent=2, sort_keys=True),
    )
    if keep_reports > 0:
        _rotate(runs_dir, keep_reports, identifier)
    return paths


def write_reports(*args: Any, **kwargs: Any) -> dict[str, Path]:
    """``write_run`` 的兼容别名。"""

    return write_run(*args, **kwargs)


def _write_jsonl(handle: TextIO, items: list[PlanItem]) -> None:
    """写逐行无损记录。"""

    for item in items:
        handle.write(json.dumps(item.to_record(), ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def _write_tsv(handle: TextIO, items: list[PlanItem]) -> None:
    """写便于人工审阅的 TSV。"""

    writer = csv.DictWriter(handle, fieldnames=_TSV_FIELDS, dialect="excel-tab", extrasaction="ignore")
    writer.writeheader()
    for item in items:
        record = item.to_record()
        for key in ("rules", "observations", "conflict_with"):
            record[key] = " | ".join(record[key])
        writer.writerow(record)


def _atomic_text(path: Path, writer: Callable[[TextIO], Any]) -> None:
    """在目标同目录写临时文件并以 ``os.replace`` 原子发布。"""

    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            writer(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            temporary.unlink(missing_ok=True)
        finally:
            raise


def _rotate(runs_dir: Path, keep_reports: int, current_run_id: str) -> None:
    """按整轮最近修改时间保留 N 轮，而不是按单个文件轮转。"""

    grouped: dict[str, list[Path]] = {}
    for path in runs_dir.iterdir():
        if not path.is_file() or not (
            path.name.endswith(".jsonl")
            or path.name.endswith(".tsv")
            or path.name.endswith(".summary.json")
        ):
            continue
        run_id = path.name.removesuffix(".summary.json").removesuffix(".jsonl").removesuffix(".tsv")
        grouped.setdefault(run_id, []).append(path)
    ordered = sorted(
        grouped,
        key=lambda run_id: max(path.stat().st_mtime_ns for path in grouped[run_id]),
        reverse=True,
    )
    # 当前轮刚刚原子发布；显式放到首位可抵御极低精度文件系统的时间并列。
    if current_run_id in ordered:
        ordered.remove(current_run_id)
        ordered.insert(0, current_run_id)
    for old_id in ordered[keep_reports:]:
        for path in grouped[old_id]:
            path.unlink(missing_ok=True)


def _safe_run_id(run_id: str) -> str:
    """限制 run id 为安全文件名，避免调用方逃逸 runs 目录。"""

    safe = "".join(character for character in run_id if character.isalnum() or character in "-_.")
    safe = safe.strip(".")
    if not safe:
        raise ValueError("run_id 不能为空或只含不安全字符")
    return safe
