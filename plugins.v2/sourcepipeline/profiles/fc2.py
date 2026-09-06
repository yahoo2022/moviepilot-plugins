"""FC2 PPV 番号的保守解析器。"""

from __future__ import annotations

import re

try:
    from ..models import CandidateEvidence, ParseResult
except ImportError:  # 允许核心模块脱离包入口调试
    from models import CandidateEvidence, ParseResult

from .base import common_observations, is_multipart_suffix, trailing_suffix, unwrap_reversible


class Fc2Profile:
    """只识别带明确 FC2/FC2PPV/fc 短写标记的数字番号。"""

    name = "fc2"

    # ``fc`` 短写要求数字紧随其后或只隔常见分隔符；不扫描孤立长数字。
    _PATTERN = re.compile(
        r"(?i)(?<![A-Z0-9])(?:"
        r"FC2(?:[\s_.-]*PPV)?[\s_.-]*(?P<fc2>\d{4,})"
        r"|FC[\s_.-]*(?P<fc>\d{5,})"
        r")(?!\d)"
    )

    def parse(self, text: str, source: str = "unknown") -> ParseResult:
        """返回文本中全部明确 FC2 候选；域名包装仅作为证据。"""

        unwrapped, wrapper_notes = unwrap_reversible(text)
        observations = tuple(dict.fromkeys((*wrapper_notes, *common_observations(text))))
        evidence: list[CandidateEvidence] = []
        for match in self._PATTERN.finditer(unwrapped):
            digits = match.group("fc2") or match.group("fc")
            suffix = trailing_suffix(unwrapped, match.end())
            multipart = is_multipart_suffix(suffix)
            local_notes = list(observations)
            if suffix:
                local_notes.append("multipart_suffix" if multipart else "unknown_suffix")
            rule = "fc2_explicit" if match.group("fc2") else "fc_short"
            evidence.append(
                CandidateEvidence(
                    profile=self.name,
                    source=source,
                    raw_text=text,
                    unwrapped_text=unwrapped,
                    canonical=f"FC2-{digits}",
                    rule=rule,
                    confidence=1.0,
                    span=match.span(),
                    suffix=suffix,
                    multipart=multipart,
                    observations=tuple(dict.fromkeys(local_notes)),
                )
            )
        return ParseResult(profile=self.name, evidence=tuple(evidence), observations=observations)
