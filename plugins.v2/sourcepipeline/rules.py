"""A 层垃圾判定规则。

设计原则（针对「115 原生源上的不可逆删除」）：

1. **正片铁律最优先**：视频扩展名且体积达到保护阈值，或**文件名自身**已解析出
   可靠番号，一律 PROTECTED，任何关键词/目录/白名单规则都不能把它变成删除候选。
   这比 QuarkClean（只作用于本地 STRM 投影）更保守，因为这里删的是源文件。
   注意番号必须来自文件名：父目录番号不算，否则「一个目录一部片」布局里混放的
   sample/预告小片段会全部免疫垃圾判定。
2. **扩展名是最硬的证据**：`.txt/.url/.exe` 这类不可能是媒体，直接判垃圾；
   想要「除了视频和图片全删」时切到 whitelist 模式即可。
3. **关键词只是辅助**：只在文件通不过铁律保护时才参与判定，避免正片名被子串误命中。
4. **目录永不自动删除**：v0.3.0 只对文件生成 REMOVE 计划。
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Optional

# 判定「是不是视频」的固定集合，与用户可配置的「保留后缀」相互独立：
# 保留清单可以被用户改小，但铁律保护始终按这个集合识别视频。
VIDEO_EXTENSIONS = frozenset(
    {
        "mp4", "mkv", "avi", "wmv", "mov", "m4v", "mpg", "mpeg", "mpe", "mpv",
        "ts", "m2ts", "mts", "tp", "iso", "rmvb", "rm", "flv", "f4v", "vob",
        "asf", "3gp", "3g2", "webm", "ogm", "ogv", "divx", "mxf", "dat",
    }
)

IMAGE_EXTENSIONS = frozenset(
    {"jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif", "avif", "heic", "jfif"}
)

SUBTITLE_EXTENSIONS = frozenset({"srt", "ass", "ssa", "sub", "idx", "vtt", "smi", "sup", "lrc"})

# 默认保留清单 = 视频 + 图片 + 字幕 + nfo。
# whitelist 模式下不在此清单里的一律判垃圾，所以这里默认放宽一点，
# 想要「只留视频和图片」把字幕/nfo 从配置里删掉即可。
DEFAULT_KEEP_EXTENSIONS = ",".join(
    sorted(VIDEO_EXTENSIONS | IMAGE_EXTENSIONS | SUBTITLE_EXTENSIONS | {"nfo"})
)

# 默认垃圾后缀：网页、文本、可执行、脚本、系统、校验、种子、临时下载。
# 刻意不含压缩包（zip/rar/7z）——可能是分卷原盘；确实想删请自行追加。
DEFAULT_JUNK_EXTENSIONS = ",".join(
    (
        "txt", "url", "website", "html", "htm", "xhtml", "shtml", "mht", "mhtml",
        "lnk", "exe", "msi", "bat", "cmd", "com", "scr", "vbs", "ps1", "sh",
        "apk", "jar", "swf", "chm", "hlp",
        "db", "ini", "inf", "cfg", "conf", "log", "bak", "tmp", "temp",
        "sfv", "md5", "sha1", "crc", "par2", "nzb", "torrent", "magnet",
        "part", "downloading", "!qb", "aria2", "crdownload", "ut",
        "ds_store", "thumbs", "desktop", "lock", "xltd",
    )
)

# 默认垃圾关键词：整段广告短语与明确附属标记，绝不使用裸 TLD 或短词，
# 避免正片名被子串误命中（参照 QuarkClean 的教训）。
DEFAULT_JUNK_KEYWORDS = "\n".join(
    (
        "更多原盘请访问", "更多高清电影请访问", "更多电视剧集下载请访问",
        "更多剧集打包下载请访问", "更多高清剧集下载请访问", "更多无水印",
        "地址发布页", "收藏不迷路", "扫码关注", "关注公众号", "免费公益影视",
        "公益影视站", "全站无广告", "最新地址", "永久地址", "备用地址",
        "看片指南", "번호", "新片首发", "全球首发",
        "样片", "测试文件", "预告片", "花絮", "映像特典", "音乐特典",
        "无码破解说明", "使用说明", "必看", "推荐", "免费下载", "点击进入",
        "sample", "trailer", "creditless", "readme", "how to", "password",
    )
)

# 默认附属子目录：位于其中的小文件视为附属垃圾。
# 不含 Specials / Season 0，避免误删正规特别篇。
DEFAULT_JUNK_DIRECTORIES = ",".join(
    (
        "sample", "samples", "sps", "extras", "scans", "fonts", "music", "cds",
        "trailers", "trailer", "ncop", "nced", "menu", "bonus", "ads", "ad",
        "特典", "映像特典", "音乐特典", "花絮", "预告", "预告片", "广告", "样片",
    )
)

# 文件名附属标记：命中即视为附属内容（仍受正片铁律保护）。
DEFAULT_EXTRAS_MARKERS = (
    "[menu]", "[sp]", "[pv]", "[cm]", "[trailer]", "[logo]", "[scans]", "[fonts]",
    ".ncop.", ".nced.", ".sample.", "-sample.", "_sample.",
    "ending ver", "opening ver", "preview ver", "review ver",
    "映像特典", "音乐特典", "花絮", "预告片",
)

MEGABYTE = 1024 * 1024


def parse_extension_list(value: str) -> frozenset[str]:
    """解析扩展名清单：换行/逗号/空格分隔，去掉前导点，统一小写。"""

    text = str(value or "")
    for separator in (",", "，", "\n", "\t", ";", "；", "|"):
        text = text.replace(separator, " ")
    values: set[str] = set()
    for token in text.split():
        candidate = token.strip().lower().lstrip(".")
        if candidate:
            values.add(candidate)
    return frozenset(values)


def parse_keyword_list(value: str) -> tuple[str, ...]:
    """解析关键词清单：只按换行和半角/全角逗号切分，保留词内空格。"""

    text = str(value or "").replace("，", "\n").replace(",", "\n")
    keywords: list[str] = []
    for line in text.splitlines():
        candidate = line.strip().casefold()
        if candidate and candidate not in keywords:
            keywords.append(candidate)
    return tuple(keywords)


def extension_of(basename: str) -> str:
    """取小写扩展名（不含点）。无扩展名返回空串。"""

    name = str(basename or "")
    if "." not in name.strip("."):
        return ""
    return name.rsplit(".", 1)[-1].strip().lower()


@dataclass(frozen=True, slots=True)
class GarbageVerdict:
    """一次垃圾判定的结果。"""

    garbage: bool
    reason: str
    protected: bool = False
    observations: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GarbageRules:
    """可配置的垃圾判定规则集，附带用于增量重算的版本摘要。"""

    mode: str = "blacklist"
    keep_extensions: frozenset[str] = field(default_factory=frozenset)
    junk_extensions: frozenset[str] = field(default_factory=frozenset)
    junk_keywords: tuple[str, ...] = ()
    junk_directories: frozenset[str] = field(default_factory=frozenset)
    extras_markers: tuple[str, ...] = DEFAULT_EXTRAS_MARKERS
    protect_min_bytes: int = 400 * MEGABYTE
    small_video_bytes: int = 0
    no_extension_is_junk: bool = False

    VALID_MODES = ("off", "blacklist", "whitelist")

    @classmethod
    def build(
        cls,
        *,
        mode: str = "blacklist",
        keep_extensions: str = "",
        junk_extensions: str = "",
        junk_keywords: str = "",
        junk_directories: str = "",
        protect_min_mb: int = 400,
        small_video_mb: int = 0,
        no_extension_is_junk: bool = False,
    ) -> "GarbageRules":
        """从页面配置构造规则；留空回落到内置默认清单。"""

        normalized_mode = str(mode or "blacklist").strip().casefold()
        if normalized_mode not in cls.VALID_MODES:
            raise ValueError(f"垃圾判定模式只能是 {'/'.join(cls.VALID_MODES)}")
        keep = parse_extension_list(keep_extensions) or parse_extension_list(
            DEFAULT_KEEP_EXTENSIONS
        )
        junk = parse_extension_list(junk_extensions) or parse_extension_list(
            DEFAULT_JUNK_EXTENSIONS
        )
        overlap = keep & junk
        if overlap:
            raise ValueError(
                "同一扩展名不能同时出现在保留清单和垃圾清单: "
                + ",".join(sorted(overlap))
            )
        keywords = parse_keyword_list(junk_keywords) or parse_keyword_list(
            DEFAULT_JUNK_KEYWORDS
        )
        directories = parse_extension_list(junk_directories) or parse_extension_list(
            DEFAULT_JUNK_DIRECTORIES
        )
        return cls(
            mode=normalized_mode,
            keep_extensions=keep,
            junk_extensions=junk,
            junk_keywords=keywords,
            junk_directories=directories,
            protect_min_bytes=max(0, int(protect_min_mb or 0)) * MEGABYTE,
            small_video_bytes=max(0, int(small_video_mb or 0)) * MEGABYTE,
            no_extension_is_junk=bool(no_extension_is_junk),
        )

    @property
    def version(self) -> str:
        """规则内容摘要。规则变化后 planner 会重算全部计划，无需重新访问 115。"""

        payload = "\n".join(
            (
                f"mode={self.mode}",
                "keep=" + ",".join(sorted(self.keep_extensions)),
                "junk=" + ",".join(sorted(self.junk_extensions)),
                "keywords=" + "\u0000".join(self.junk_keywords),
                "dirs=" + ",".join(sorted(self.junk_directories)),
                "markers=" + "\u0000".join(self.extras_markers),
                f"protect={self.protect_min_bytes}",
                f"small={self.small_video_bytes}",
                f"noext={int(self.no_extension_is_junk)}",
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    # ------------------------------------------------------------------ 判定

    def classify(
        self,
        *,
        basename: str,
        parent_path: str,
        size: int,
        is_dir: bool,
        has_canonical: bool = False,
    ) -> GarbageVerdict:
        """判定单个 A 层对象是否为删除候选。

        ``has_canonical`` 表示 planner 已从**该文件自己的名字**解析出可靠番号
        （父目录番号不算，见模块文档）；这类视频永不判垃圾，避免把正片当广告删掉。
        """

        if self.mode == "off":
            return GarbageVerdict(False, "garbage_disabled")
        if is_dir:
            return GarbageVerdict(False, "directory_not_removable")

        name = str(basename or "")
        lowered = name.casefold()
        extension = extension_of(name)
        is_video = extension in VIDEO_EXTENSIONS
        observations: list[str] = []

        # 铁律 1：达到保护体积的视频永不删除。
        if is_video and self.protect_min_bytes > 0 and int(size or 0) >= self.protect_min_bytes:
            return GarbageVerdict(False, "protected_large_video", protected=True)
        # 铁律 2：已解析出可靠番号的视频永不删除。
        if is_video and has_canonical:
            return GarbageVerdict(False, "protected_identified_video", protected=True)

        if extension and extension in self.junk_extensions:
            return GarbageVerdict(True, f"junk_extension:{extension}")
        if self.mode == "whitelist" and extension and extension not in self.keep_extensions:
            return GarbageVerdict(True, f"not_kept_extension:{extension}")
        if not extension:
            if self.no_extension_is_junk:
                return GarbageVerdict(True, "no_extension")
            observations.append("no_extension")

        matched_directory = self._matched_directory(parent_path)
        if matched_directory:
            return GarbageVerdict(
                True, f"extras_directory:{matched_directory}", observations=tuple(observations)
            )
        marker = next((value for value in self.extras_markers if value in lowered), "")
        if marker:
            return GarbageVerdict(
                True, f"extras_marker:{marker}", observations=tuple(observations)
            )
        keyword = next((value for value in self.junk_keywords if value in lowered), "")
        if keyword:
            return GarbageVerdict(
                True, f"junk_keyword:{keyword[:40]}", observations=tuple(observations)
            )
        if (
            is_video
            and self.small_video_bytes > 0
            and 0 < int(size or 0) < self.small_video_bytes
        ):
            return GarbageVerdict(True, "small_video", observations=tuple(observations))
        return GarbageVerdict(False, "keep", observations=tuple(observations))

    def _matched_directory(self, parent_path: str) -> str:
        """命中附属子目录清单时返回该目录名。"""

        for segment in str(parent_path or "").split("/"):
            candidate = segment.strip().casefold()
            if candidate and candidate in self.junk_directories:
                return candidate
        return ""


_SAFE_BASENAME_RE = re.compile(r"^[^/\\\x00]+$")
_UNSAFE_NAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def is_safe_basename(name: str) -> bool:
    """校验目标 basename：不含路径分隔符和控制字符，且不是 . 或 ..。"""

    value = str(name or "")
    if value in {"", ".", ".."}:
        return False
    if value != value.strip():
        return False
    return bool(_SAFE_BASENAME_RE.fullmatch(value))


def safe_target_name(stem: str, extension: str) -> str:
    """由番号和原扩展名组装安全目标名；不安全时返回空串。"""

    cleaned = _UNSAFE_NAME_CHARS_RE.sub("", str(stem or "")).strip(" .")
    if not cleaned:
        return ""
    suffix = str(extension or "").strip().lower().lstrip(".")
    suffix = _UNSAFE_NAME_CHARS_RE.sub("", suffix)
    candidate = f"{cleaned}.{suffix}" if suffix else cleaned
    return candidate if is_safe_basename(candidate) else ""


def optional_int(value: object, default: int = 0) -> Optional[int]:
    """把页面输入转成非负整数；无法解析时返回默认值。"""

    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def describe(rules: GarbageRules) -> str:
    """给通知/报告用的一行规则摘要。"""

    mode_text = {
        "off": "关闭",
        "blacklist": "黑名单(只删垃圾后缀)",
        "whitelist": "白名单(不在保留清单即删)",
    }[rules.mode]
    protect_mb = rules.protect_min_bytes // MEGABYTE
    small_mb = rules.small_video_bytes // MEGABYTE
    return (
        f"模式={mode_text}，规则版本={rules.version}，"
        f"保留后缀 {len(rules.keep_extensions)} 个，垃圾后缀 {len(rules.junk_extensions)} 个，"
        f"关键词 {len(rules.junk_keywords)} 条，附属目录 {len(rules.junk_directories)} 个，"
        f"大视频保护 ≥{protect_mb}MB，"
        + (f"小视频判垃圾 <{small_mb}MB" if small_mb else "小视频判垃圾=关闭")
    )
