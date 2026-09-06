"""整理计划报告的原子写出与轮转。

每轮规划发布三个文件到 ``<data_path>/runs/``：

* ``plan-<run_id>.jsonl``       逐条无损记录，便于脚本二次处理
* ``plan-<run_id>.tsv``         人工审阅用，列固定、可直接丢进表格
* ``plan-<run_id>.summary.json`` 本轮统计与规则摘要

只序列化 :meth:`PlanRecord.to_record` 的内容，因此永不包含 OpenList Token、
Authorization、完整下载 URL、签名 query 或原始 API 响应。
"""
from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, TextIO

# TSV 列顺序：先看结论，再看证据。审阅时通常只需要前六列。
PLAN_FIELDS = (
    "state",
    "action",
    "profile",
    "path",
    "basename",
    "target_name",
    "reason",
    "canonical",
    "size_mb",
    "extension",
    "logic",
    "confidence",
    "rules",
    "observations",
    "conflict_with",
)

MEGABYTE = 1024 * 1024


def _safe_name(value: str) -> str:
    """限制文件名片段，避免调用方逃逸 runs 目录。"""

    safe = "".join(
        character for character in str(value) if character.isalnum() or character in "-_."
    ).strip(".")
    if not safe:
        raise ValueError("报告名片段不能为空或只含不安全字符")
    return safe


def _atomic_text(path: Path, writer: Callable[[TextIO], Any]) -> None:
    """在目标同目录写临时文件并以 ``os.replace`` 原子发布。"""

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
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


def _tsv_row(record: Mapping[str, Any]) -> dict[str, Any]:
    """把嵌套字段压平成单元格，并补一个人类可读的体积列。"""

    row: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, (list, tuple)):
            row[key] = " | ".join(str(item) for item in value)
        else:
            row[key] = value
    size = int(record.get("size") or 0)
    row["size_mb"] = round(size / MEGABYTE, 1) if size else 0
    return row


def write_plan_report(
    data_dir: str | Path,
    run_id: str,
    records: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    *,
    kind: str = "plan",
    fields: Sequence[str] = PLAN_FIELDS,
    keep_reports: int = 20,
) -> Path:
    """原子发布一轮报告，返回 TSV 路径（通知里给用户的就是它）。"""

    identifier = f"{_safe_name(kind)}-{_safe_name(run_id)}"
    runs_dir = Path(data_dir) / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    def _write_jsonl(handle: TextIO) -> None:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    def _write_tsv(handle: TextIO) -> None:
        writer = csv.DictWriter(
            handle, fieldnames=list(fields), dialect="excel-tab", extrasaction="ignore"
        )
        writer.writeheader()
        for record in records:
            writer.writerow(_tsv_row(record))

    _atomic_text(runs_dir / f"{identifier}.jsonl", _write_jsonl)
    _atomic_text(runs_dir / f"{identifier}.tsv", _write_tsv)
    _atomic_text(
        runs_dir / f"{identifier}.summary.json",
        lambda handle: json.dump(
            dict(summary), handle, ensure_ascii=False, indent=2, sort_keys=True
        ),
    )
    if keep_reports > 0:
        rotate_reports(runs_dir, keep_reports, identifier)
    return runs_dir / f"{identifier}.tsv"


def rotate_reports(runs_dir: Path, keep_reports: int, current_id: str) -> None:
    """按「整轮」最近修改时间保留 N 轮，而不是按单个文件轮转。"""

    grouped: dict[str, list[Path]] = {}
    for path in runs_dir.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if not (
            name.endswith(".jsonl") or name.endswith(".tsv") or name.endswith(".summary.json")
        ):
            continue
        run_id = name.removesuffix(".summary.json").removesuffix(".jsonl").removesuffix(".tsv")
        grouped.setdefault(run_id, []).append(path)
    ordered = sorted(
        grouped,
        key=lambda run_id: max(path.stat().st_mtime_ns for path in grouped[run_id]),
        reverse=True,
    )
    # 当前轮刚刚原子发布；显式放到首位可抵御低精度文件系统的时间并列。
    if current_id in ordered:
        ordered.remove(current_id)
        ordered.insert(0, current_id)
    for old_id in ordered[keep_reports:]:
        for path in grouped[old_id]:
            path.unlink(missing_ok=True)


__all__ = ["PLAN_FIELDS", "rotate_reports", "write_plan_report"]
