"""日本厂商番号的 allowlist 驱动解析器。"""

from __future__ import annotations

import re
from collections.abc import Iterable

try:
    from ..models import CandidateEvidence, ParseResult
except ImportError:  # 允许核心模块脱离 MoviePilot 独立调试
    from models import CandidateEvidence, ParseResult

from .base import common_observations, is_multipart_suffix, trailing_suffix, unwrap_reversible


class JavProfile:
    """仅由调用方 allowlist 或内置保守 allowlist 驱动可靠解析。"""

    name = "jav"
    DEFAULT_PREFIXES = frozenset({"ABW", "MIFD", "HEY", "HEYZO"})

    def __init__(self, prefix_allowlist: Iterable[str] | None = None) -> None:
        values = prefix_allowlist if prefix_allowlist is not None else self.DEFAULT_PREFIXES
        self.prefix_allowlist = frozenset(
            value.strip().upper() for value in values if value and value.strip()
        )
        escaped = sorted((re.escape(value) for value in self.prefix_allowlist), key=len, reverse=True)
        self._standard_pattern = (
            re.compile(
                rf"(?i)(?<![A-Z0-9])(?P<prefix>{'|'.join(escaped)})[\s_.-]*(?P<number>\d+)(?!\d)"
            )
            if escaped
            else None
        )
        self._hey_three = re.compile(
            r"(?i)(?<![A-Z0-9])HEY[\s_.-]+(?P<first>\d+)[\s_.-]+(?P<second>\d+)(?!\d)"
        )

    def parse(self, text: str, source: str = "unknown") -> ParseResult:
        """解析 allowlist 内番号，保留数字宽度并优先处理 HEY 三段式。"""

        unwrapped, wrapper_notes = unwrap_reversible(text)
        observations = tuple(dict.fromkeys((*wrapper_notes, *common_observations(text))))
        evidence: list[CandidateEvidence] = []
        occupied: list[tuple[int, int]] = []

        if "HEY" in self.prefix_allowlist:
            for match in self._hey_three.finditer(unwrapped):
                suffix = trailing_suffix(unwrapped, match.end())
                multipart = is_multipart_suffix(suffix)
                notes = self._suffix_notes(observations, suffix, multipart)
                evidence.append(
                    CandidateEvidence(
                        profile=self.name,
                        source=source,
                        raw_text=text,
                        unwrapped_text=unwrapped,
                        canonical=f"HEY-{match.group('first')}-{match.group('second')}",
                        rule="jav_hey_three_part",
                        confidence=1.0,
                        span=match.span(),
                        suffix=suffix,
                        multipart=multipart,
                        observations=notes,
                    )
                )
                occupied.append(match.span())

        if self._standard_pattern is not None:
            for match in self._standard_pattern.finditer(unwrapped):
                if any(start <= match.start() < end for start, end in occupied):
                    continue
                prefix = match.group("prefix").upper()
                suffix = trailing_suffix(unwrapped, match.end())
                multipart = is_multipart_suffix(suffix)
                notes = self._suffix_notes(observations, suffix, multipart)
                evidence.append(
                    CandidateEvidence(
                        profile=self.name,
                        source=source,
                        raw_text=text,
                        unwrapped_text=unwrapped,
                        canonical=f"{prefix}-{match.group('number')}",
                        rule="jav_allowlist",
                        confidence=1.0,
                        span=match.span(),
                        suffix=suffix,
                        multipart=multipart,
                        observations=notes,
                    )
                )
        evidence.sort(key=lambda item: item.span or (0, 0))
        return ParseResult(profile=self.name, evidence=tuple(evidence), observations=observations)

    @staticmethod
    def _suffix_notes(
        observations: tuple[str, ...], suffix: str, multipart: bool
    ) -> tuple[str, ...]:
        """追加后缀观察，不把未知后缀解释为可投递内容。"""

        values = list(observations)
        if suffix:
            values.append("multipart_suffix" if multipart else "unknown_suffix")
        return tuple(dict.fromkeys(values))
