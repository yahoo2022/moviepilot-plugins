"""番号 profile 的标准库基础协议与公共帮助函数。"""

from __future__ import annotations

import re
from typing import Protocol

try:
    from ..models import ParseResult
except ImportError:  # 允许把 mdcstrmprep 目录直接加入 sys.path 调试
    from models import ParseResult


class Profile(Protocol):
    """番号解析器的最小协议。"""

    name: str

    def parse(self, text: str, source: str = "unknown") -> ParseResult:
        """解析一段候选文本并返回全部可靠证据。"""


_BRACKET_PREFIX_RE = re.compile(r"^\s*(?:\[[^\]]+\]\s*)+")
_DOMAIN_PREFIX_RE = re.compile(
    r"^\s*(?:\d+\s+)?(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:@|\s+)",
    re.IGNORECASE,
)


def unwrap_reversible(text: str) -> tuple[str, tuple[str, ...]]:
    """可逆剥离前置方括号/域名包装，并返回观察标签。

    原文始终由 evidence 的 ``raw_text`` 保留；这里不删除候选后的内容。
    """

    value = text.strip()
    observations: list[str] = []
    bracket = _BRACKET_PREFIX_RE.match(value)
    if bracket:
        value = value[bracket.end() :].lstrip()
        observations.append("bracket_wrapper")
    domain = _DOMAIN_PREFIX_RE.match(value)
    if domain:
        value = value[domain.end() :].lstrip()
        observations.append("domain_wrapper")
    return value, tuple(observations)


def common_observations(text: str) -> tuple[str, ...]:
    """提取只用于观察、绝不直接驱动垃圾状态的信号。"""

    lowered = text.casefold()
    values: list[str] = []
    if "gc2048" in lowered:
        values.append("gc2048")
    if "kcf9" in lowered:
        values.append("kcf9")
    if re.search(r"(?i)(?:^|[-_.\s])(?:1080|2160)p?(?:$|[-_.\s])", text):
        values.append("resolution_token")
    return tuple(values)


_COMPACT_SUFFIX_RE = re.compile(
    r"(?i)^\s*(?P<suffix>"
    r"(?:[_\-.](?:[A-Z0-9][A-Z0-9_.-]{0,31}))|"
    r"(?:CD|PART)[_\-.\s]*[1-9]\d*|SP|EP|A|B|A[_\-.]B"
    r")(?=$|\s)"
)


def trailing_suffix(text: str, end: int) -> str:
    """提取紧邻番号的短技术后缀，不把后续标题/描述误当版本。

    例如 ``ABW-158_2``、``FC2-123-CD2`` 会保留后缀；
    ``ABW-158 中文标题`` 的普通描述不会形成 suffix。
    """

    tail = text[end:]
    tail = re.sub(r"(?i)\.(?:strm|mp4|mkv|avi|mov|wmv)$", "", tail).rstrip()
    match = _COMPACT_SUFFIX_RE.match(tail)
    return match.group("suffix") if match else ""


_MULTIPART_RE = re.compile(
    r"(?i)^(?:[_\-.\s]*(?:[1-7]|SP|EP|CD(?:[_\-.\s]*[1-9]\d*)?|"
    r"PART(?:[_\-.\s]*[1-9]\d*)?|[AB]|A[_\-.]B))$"
)


def is_multipart_suffix(suffix: str) -> bool:
    """判断是否为已知疑似分段后缀，不改变其原始文本。"""

    return bool(suffix and _MULTIPART_RE.fullmatch(suffix.strip()))
