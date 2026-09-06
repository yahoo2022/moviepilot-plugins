"""日本厂商番号的 allowlist 驱动解析器。

只有前缀出现在 allowlist 里才会产生可靠证据。这样即使 115 源名里混着
分辨率、日期、编码参数等长数字，也不会被当成番号乱改名。
"""

from __future__ import annotations

import re
from collections.abc import Iterable

try:
    from ..models import CandidateEvidence, ParseResult
except ImportError:  # 允许核心模块脱离 MoviePilot 独立调试
    from models import CandidateEvidence, ParseResult

from .base import common_observations, is_multipart_suffix, trailing_suffix, unwrap_reversible


class JavProfile:
    """仅由调用方 allowlist 或内置常见厂商 allowlist 驱动可靠解析。"""

    name = "jav"

    # 内置常见厂商前缀。故意不含「数字开头」的素人系列（如 200GANA / 259LUXU），
    # 因为番号规则要求前缀前不能紧跟数字；这类需要单独规则，先走 REVIEW_REQUIRED。
    DEFAULT_PREFIXES = frozenset(
        {
            # S1 / Prestige / Idea Pocket / Moodyz 等主流厂标
            "SSIS", "SSNI", "SNIS", "SIVR", "OFJE", "SOE", "SPS",
            "IPX", "IPZ", "IPZZ", "IPIT", "IPVR", "IDBD",
            "MIDE", "MIDV", "MIDA", "MIAA", "MIAB", "MIAE", "MIFD", "MIMK", "MIDD",
            "MIRD", "MIAD", "MIGD", "MIBD",
            "ABW", "ABP", "ABF", "PRED", "PPPD", "PPPE", "PRTD",
            "STAR", "STARS", "SDDE", "SDMU", "SDNM", "SDAB", "SDJS", "SDMM", "SDSI",
            "SDNT", "STARS",
            "JUL", "JUQ", "JUY", "JUFE", "JUFD", "JUX", "JUC",
            "ADN", "ATID", "SHKD", "RBD", "RBK", "JBD", "SAME", "ADND",
            "MEYD", "MIZD", "MVSD", "MDTM", "MDBK", "MDVR",
            "HND", "HMN", "HNDS", "HUNTA", "HUNT", "HUNBL", "HZGD", "HODV",
            "KAWD", "CAWD", "KMHR", "KIRE", "KTRA", "KSBJ",
            "EBOD", "EBWH", "EYAN", "MKMP", "MKON", "MDON",
            "DASD", "DASS", "DLDSS", "DOCP", "DVDMS", "DVAJ", "DANDY",
            "NHDTB", "NHDTA", "NNPJ", "NPJB", "NACR", "NSPS", "NKKD",
            "FSDSS", "FTHT", "FPRE", "FSET",
            "GVH", "GVG", "GANA", "GENM", "GDRD",
            "SW", "TPPN", "TEK", "XVSR", "WANZ", "WAAA", "MIST", "RCTD", "SVDVD",
            "URE", "MMND", "AVOP", "BF", "BAZX", "BDA", "CJOD", "CHN", "CEMD",
            "TOEN", "TIKB", "TYOD", "SAN", "SORA", "SUPA", "SIRO", "LUXU", "MIUM",
            "GS", "ROYD", "SAKA", "START",
            # 无码/流出常见标
            "HEY", "HEYZO", "CARIB", "CARIBPR", "PACO", "MUGON", "TOKYOHOT",
        }
    )

    def __init__(self, prefix_allowlist: Iterable[str] | None = None) -> None:
        values = prefix_allowlist if prefix_allowlist is not None else self.DEFAULT_PREFIXES
        self.prefix_allowlist = frozenset(
            value.strip().upper() for value in values if value and value.strip()
        )
        escaped = sorted(
            (re.escape(value) for value in self.prefix_allowlist), key=len, reverse=True
        )
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
                        unwrapped_text=unwrapped,
                        raw_text=text,
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


def parse_prefix_allowlist(value: str) -> list[str]:
    """解析页面配置的 JAV 前缀清单（换行/逗号/空格分隔）。

    留空返回空列表，调用方据此回落到 :attr:`JavProfile.DEFAULT_PREFIXES`。
    """

    text = str(value or "")
    for separator in (",", "，", "\n", "\t", ";", "；"):
        text = text.replace(separator, " ")
    prefixes: list[str] = []
    for token in text.split():
        candidate = token.strip().upper()
        if not candidate or candidate in prefixes:
            continue
        if not re.fullmatch(r"[A-Z][A-Z0-9]{0,15}", candidate):
            raise ValueError(f"JAV 前缀只能是字母开头的字母数字组合: {token[:32]}")
        prefixes.append(candidate)
    return prefixes
