"""SourcePipeline SQLite 持久库存。

阶段1只发布完整目录分页；partial 保留旧 generation，live 缺失只累计观察次数，
不产生 tombstone，也不删除任何库存记录。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Optional

from .models import ProfileConfig, SourceObject


SCHEMA_VERSION = 3


class InventoryDatabase:
    """每次操作使用短连接的 SQLite WAL 数据库。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """提交/回滚普通短事务并始终显式关闭连接。"""

        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }

    @classmethod
    def _ensure_column(
        cls,
        connection: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        if column not in cls._columns(connection, table):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS source_profiles (
                name TEXT PRIMARY KEY,
                logic TEXT NOT NULL,
                root TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                config_format_version INTEGER NOT NULL DEFAULT 2,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scan_runs (
                run_id TEXT NOT NULL,
                profile TEXT NOT NULL,
                logic TEXT NOT NULL DEFAULT '',
                root TEXT NOT NULL DEFAULT '',
                mode TEXT NOT NULL CHECK(mode IN ('cache', 'live')),
                generation INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                complete INTEGER NOT NULL DEFAULT 0,
                converged INTEGER NOT NULL DEFAULT 0,
                request_budget INTEGER NOT NULL DEFAULT 0,
                stop_reason TEXT NOT NULL DEFAULT '',
                requests INTEGER NOT NULL DEFAULT 0,
                retries INTEGER NOT NULL DEFAULT 0,
                directories_completed INTEGER NOT NULL DEFAULT 0,
                directories_partial INTEGER NOT NULL DEFAULT 0,
                objects_seen INTEGER NOT NULL DEFAULT 0,
                error_summary TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (run_id, profile)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS directories (
                profile TEXT NOT NULL,
                path TEXT NOT NULL,
                parent_path TEXT NOT NULL DEFAULT '',
                basename TEXT NOT NULL DEFAULT '',
                depth INTEGER NOT NULL DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                complete INTEGER NOT NULL DEFAULT 0,
                generation INTEGER NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                children_digest TEXT NOT NULL DEFAULT '',
                last_cache_success TEXT,
                last_live_success TEXT,
                last_attempt TEXT,
                last_attempt_run_id TEXT NOT NULL DEFAULT '',
                error_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (profile, path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_objects (
                profile TEXT NOT NULL,
                source_key TEXT NOT NULL,
                path TEXT NOT NULL,
                parent_path TEXT NOT NULL,
                basename TEXT NOT NULL,
                is_dir INTEGER NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                modified TEXT NOT NULL DEFAULT '',
                object_id TEXT NOT NULL DEFAULT '',
                fingerprint TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                last_seen_generation INTEGER NOT NULL,
                missing_observations INTEGER NOT NULL DEFAULT 0,
                tombstone INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (profile, path)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS plans (
                plan_id TEXT PRIMARY KEY,
                profile TEXT NOT NULL,
                logic TEXT NOT NULL DEFAULT '',
                source_key TEXT NOT NULL,
                path TEXT NOT NULL,
                parent_path TEXT NOT NULL DEFAULT '',
                basename TEXT NOT NULL DEFAULT '',
                extension TEXT NOT NULL DEFAULT '',
                size INTEGER NOT NULL DEFAULT 0,
                modified TEXT NOT NULL DEFAULT '',
                source_fingerprint TEXT NOT NULL DEFAULT '',
                ruleset_version TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'REVIEW_REQUIRED',
                action TEXT NOT NULL DEFAULT 'REVIEW',
                lifecycle TEXT NOT NULL DEFAULT 'PLANNED',
                canonical TEXT NOT NULL DEFAULT '',
                target_name TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                manifest_id TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS manifests (
                manifest_id TEXT PRIMARY KEY,
                digest TEXT NOT NULL,
                action TEXT NOT NULL DEFAULT '',
                item_count INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL DEFAULT 'FROZEN',
                ruleset_version TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS actions (
                action_id TEXT PRIMARY KEY,
                manifest_id TEXT NOT NULL,
                profile TEXT NOT NULL DEFAULT '',
                source_key TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL DEFAULT '',
                target_name TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                finished_at TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS projection_status (
                profile TEXT NOT NULL,
                source_key TEXT NOT NULL,
                state TEXT NOT NULL,
                expected_path TEXT NOT NULL,
                checked_at TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (profile, source_key)
            )
            """,
        )
        for statement in statements:
            connection.execute(statement)

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            row = connection.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            current_version = int(row["value"]) if row else 0
            if current_version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"SQLite schema v{current_version} 高于当前代码 v{SCHEMA_VERSION}"
                )

            if current_version < 3:
                # 阶段1从不写 plans/manifests/actions（实现记录第三节第 10 条），
                # 因此升级到 v3 时直接重建这三张表，不会丢任何历史事实；
                # 库存表 source_objects/directories/scan_runs 一律保留。
                connection.execute("DROP TABLE IF EXISTS plans")
                connection.execute("DROP TABLE IF EXISTS manifests")
                connection.execute("DROP TABLE IF EXISTS actions")

            self._create_schema(connection)
            self._ensure_column(connection, "scan_runs", "logic", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scan_runs", "root", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "scan_runs", "converged", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(
                connection, "scan_runs", "request_budget", "INTEGER NOT NULL DEFAULT 0"
            )
            self._ensure_column(
                connection, "scan_runs", "stop_reason", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(
                connection, "directories", "active", "INTEGER NOT NULL DEFAULT 1"
            )
            self._ensure_column(
                connection, "directories", "last_attempt_run_id", "TEXT NOT NULL DEFAULT ''"
            )
            self._ensure_column(connection, "plans", "logic", "TEXT NOT NULL DEFAULT ''")

            connection.execute("UPDATE scan_runs SET logic=profile WHERE logic='' ")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_plans_object ON plans(profile, path)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_plans_ready "
                "ON plans(profile, state, lifecycle, path)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_actions_manifest ON actions(manifest_id, created_at)"
            )
            connection.execute("DROP INDEX IF EXISTS idx_directories_queue")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_directories_queue_v2 "
                "ON directories(profile, active, complete, depth, last_cache_success, last_live_success, path)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_parent "
                "ON source_objects(profile, parent_path, basename)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('next_generation', '1')"
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def sync_profiles(self, profiles: Iterable[ProfileConfig], *, updated_at: str) -> None:
        """把当前页面配置同步为权威来源定义；历史库存仍保留。"""

        configured = list(profiles)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE source_profiles SET enabled=0, updated_at=?",
                (updated_at,),
            )
            for profile in configured:
                connection.execute(
                    """
                    INSERT INTO source_profiles(
                        name, logic, root, enabled, config_format_version, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, 2, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        logic=excluded.logic,
                        root=excluded.root,
                        enabled=excluded.enabled,
                        config_format_version=2,
                        updated_at=excluded.updated_at
                    """,
                    (
                        profile.name,
                        profile.logic,
                        profile.root,
                        int(profile.enabled),
                        updated_at,
                        updated_at,
                    ),
                )

    def next_generation(self) -> int:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key='next_generation'"
            ).fetchone()
            generation = int(row["value"] if row else 1)
            connection.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('next_generation', ?)",
                (str(generation + 1),),
            )
            return generation

    def ensure_root(self, profile: str, root: str) -> None:
        parent, _, basename = root.rstrip("/").rpartition("/")
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO directories(profile, path, parent_path, basename, depth, active)
                VALUES(?, ?, ?, ?, ?, 1)
                ON CONFLICT(profile, path) DO UPDATE SET
                    parent_path=excluded.parent_path,
                    basename=excluded.basename,
                    depth=excluded.depth,
                    active=1
                """,
                (profile, root, parent or "/", basename, self._depth(root)),
            )

    @staticmethod
    def _success_column(mode: str) -> str:
        if mode == "cache":
            return "last_cache_success"
        if mode == "live":
            return "last_live_success"
        raise ValueError("扫描模式只能是 cache 或 live")

    def next_directory(
        self,
        profile: str,
        root: str,
        run_id: str,
        *,
        mode: str,
        root_cutoff: str,
        directory_cutoff: str,
    ) -> Optional[str]:
        """动态取一个候选：到期根优先，未完成目录按 depth 广度优先。"""

        success_column = self._success_column(mode)
        with self._connection() as connection:
            row = connection.execute(
                f"""
                SELECT path
                FROM directories
                WHERE profile=?
                  AND active=1
                  AND (path=? OR substr(path, 1, ?)=?)
                  AND COALESCE(last_attempt_run_id, '')<>?
                  AND (
                      complete=0
                      OR (path=? AND ({success_column} IS NULL OR {success_column}<=?))
                      OR (path<>? AND ({success_column} IS NULL OR {success_column}<=?))
                  )
                ORDER BY
                    CASE
                        WHEN path=? AND ({success_column} IS NULL OR {success_column}<=?) THEN 0
                        WHEN complete=0 THEN 1
                        ELSE 2
                    END,
                    CASE WHEN complete=0 THEN depth END ASC,
                    CASE WHEN complete=0 THEN path END ASC,
                    CASE WHEN complete<>0 AND {success_column} IS NULL THEN 0 ELSE 1 END ASC,
                    CASE WHEN complete<>0 THEN {success_column} END ASC,
                    path ASC
                LIMIT 1
                """,
                (
                    profile,
                    root,
                    len(root) + 1,
                    f"{root}/",
                    run_id,
                    root,
                    root_cutoff,
                    root,
                    directory_cutoff,
                    root,
                    root_cutoff,
                ),
            ).fetchone()
        return str(row["path"]) if row else None

    def pending_summary(
        self,
        profile: str,
        root: str,
        *,
        mode: str,
        root_cutoff: str,
        directory_cutoff: str,
    ) -> dict:
        """统计当前模式真正待处理的队列，并按相对层级汇总。"""

        success_column = self._success_column(mode)
        root_depth = self._depth(root)
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT depth, COUNT(*) AS count
                FROM directories
                WHERE profile=?
                  AND active=1
                  AND (path=? OR substr(path, 1, ?)=?)
                  AND (
                      complete=0
                      OR (path=? AND ({success_column} IS NULL OR {success_column}<=?))
                      OR (path<>? AND ({success_column} IS NULL OR {success_column}<=?))
                  )
                GROUP BY depth
                ORDER BY depth
                """,
                (
                    profile,
                    root,
                    len(root) + 1,
                    f"{root}/",
                    root,
                    root_cutoff,
                    root,
                    directory_cutoff,
                ),
            ).fetchall()
        by_depth = {
            max(0, int(row["depth"]) - root_depth): int(row["count"])
            for row in rows
        }
        return {
            "count": sum(by_depth.values()),
            "frontier_depth": min(by_depth) if by_depth else None,
            "by_depth": by_depth,
        }

    def begin_run(
        self,
        run_id: str,
        profile: ProfileConfig,
        mode: str,
        generation: int,
        started_at: str,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO scan_runs(
                    run_id, profile, logic, root, mode, generation, started_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    profile.name,
                    profile.logic,
                    profile.root,
                    mode,
                    generation,
                    started_at,
                ),
            )

    def finish_run(
        self,
        run_id: str,
        profile: str,
        *,
        finished_at: str,
        complete: bool,
        converged: bool,
        request_budget: int,
        stop_reason: str,
        requests: int,
        retries: int,
        directories_completed: int,
        directories_partial: int,
        objects_seen: int,
        errors: Iterable[str],
    ) -> None:
        error_summary = json.dumps(list(errors), ensure_ascii=False)[:4000]
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE scan_runs
                SET finished_at=?, complete=?, converged=?, request_budget=?, stop_reason=?,
                    requests=?, retries=?, directories_completed=?, directories_partial=?,
                    objects_seen=?, error_summary=?
                WHERE run_id=? AND profile=?
                """,
                (
                    finished_at,
                    int(complete),
                    int(converged),
                    request_budget,
                    stop_reason,
                    requests,
                    retries,
                    directories_completed,
                    directories_partial,
                    objects_seen,
                    error_summary,
                    run_id,
                    profile,
                ),
            )

    def publish_directory(
        self,
        profile: str,
        path: str,
        objects: Iterable[SourceObject],
        *,
        generation: int,
        mode: str,
        scanned_at: str,
        run_id: str = "",
    ) -> None:
        """仅在调用方取得完整分页后，以一个事务发布目录 generation。"""

        items = list(objects)
        digest_input = [
            f"{item.basename}\0{int(item.is_dir)}\0{item.size}\0{item.modified}\0{item.object_id}"
            for item in sorted(items, key=lambda value: (value.basename.casefold(), value.basename))
        ]
        children_digest = hashlib.sha256("\n".join(digest_input).encode("utf-8")).hexdigest()

        with self._transaction() as connection:
            previous = {
                str(row["path"])
                for row in connection.execute(
                    "SELECT path FROM source_objects WHERE profile=? AND parent_path=?",
                    (profile, path),
                ).fetchall()
            }
            incoming = {item.path for item in items}
            missing = sorted(previous - incoming)

            for item in items:
                source_key = item.object_id or f"{profile}:{item.path}"
                connection.execute(
                    """
                    INSERT INTO source_objects(
                        profile, source_key, path, parent_path, basename, is_dir,
                        size, modified, object_id, fingerprint, first_seen, last_seen,
                        last_seen_generation, missing_observations, tombstone
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
                    ON CONFLICT(profile, path) DO UPDATE SET
                        source_key=excluded.source_key,
                        parent_path=excluded.parent_path,
                        basename=excluded.basename,
                        is_dir=excluded.is_dir,
                        size=excluded.size,
                        modified=excluded.modified,
                        object_id=excluded.object_id,
                        fingerprint=excluded.fingerprint,
                        last_seen=excluded.last_seen,
                        last_seen_generation=excluded.last_seen_generation,
                        missing_observations=CASE
                            WHEN ?='live' THEN 0
                            ELSE source_objects.missing_observations
                        END,
                        tombstone=CASE
                            WHEN ?='live' THEN 0
                            ELSE source_objects.tombstone
                        END
                    """,
                    (
                        item.profile,
                        source_key,
                        item.path,
                        item.parent_path,
                        item.basename,
                        int(item.is_dir),
                        item.size,
                        item.modified,
                        item.object_id,
                        item.fingerprint,
                        scanned_at,
                        scanned_at,
                        generation,
                        mode,
                        mode,
                    ),
                )
                if item.is_dir:
                    connection.execute(
                        """
                        INSERT INTO directories(
                            profile, path, parent_path, basename, depth, active
                        ) VALUES(?, ?, ?, ?, ?, 1)
                        ON CONFLICT(profile, path) DO UPDATE SET
                            parent_path=excluded.parent_path,
                            basename=excluded.basename,
                            depth=excluded.depth,
                            active=CASE WHEN ?='live' THEN 1 ELSE directories.active END
                        """,
                        (
                            profile,
                            item.path,
                            item.parent_path,
                            item.basename,
                            self._depth(item.path),
                            mode,
                        ),
                    )

            if mode == "live":
                if missing:
                    connection.executemany(
                        """
                        UPDATE source_objects
                        SET missing_observations=missing_observations + 1
                        WHERE profile=? AND path=?
                        """,
                        [(profile, missing_path) for missing_path in missing],
                    )
                inactive_roots = sorted(
                    set(missing).union(item.path for item in items if not item.is_dir)
                )
                for inactive_root in inactive_roots:
                    connection.execute(
                        """
                        UPDATE directories
                        SET active=0
                        WHERE profile=?
                          AND (path=? OR substr(path, 1, ?)=?)
                        """,
                        (
                            profile,
                            inactive_root,
                            len(inactive_root) + 1,
                            f"{inactive_root}/",
                        ),
                    )

            success_column = self._success_column(mode)
            connection.execute(
                f"""
                UPDATE directories
                SET complete=1, generation=?, item_count=?, children_digest=?,
                    {success_column}=?, last_attempt=?, last_attempt_run_id=?,
                    error_count=0, last_error=''
                WHERE profile=? AND path=?
                """,
                (
                    generation,
                    len(items),
                    children_digest,
                    scanned_at,
                    scanned_at,
                    run_id,
                    profile,
                    path,
                ),
            )

    def mark_directory_partial(
        self,
        profile: str,
        path: str,
        *,
        attempted_at: str,
        error: str,
        run_id: str = "",
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE directories
                SET complete=0, last_attempt=?, last_attempt_run_id=?,
                    error_count=error_count + 1, last_error=?
                WHERE profile=? AND path=?
                """,
                (attempted_at, run_id, str(error)[:500], profile, path),
            )

    def queue_remaining(self, profile: str, root: str) -> int:
        """兼容接口：只统计当前根内尚未完成过的目录。"""

        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM directories
                WHERE profile=?
                  AND active=1
                  AND complete=0
                  AND (path=? OR substr(path, 1, ?)=?)
                """,
                (profile, root, len(root) + 1, f"{root}/"),
            ).fetchone()
        return int(row["count"] if row else 0)

    def status(self) -> dict:
        """只返回当前 profile 定义范围内的聚合，旧根库存保留但不混入。"""

        with self._connection() as connection:
            profile_rows = connection.execute(
                """
                SELECT name, logic, root, enabled, created_at, updated_at
                FROM source_profiles
                ORDER BY name
                """
            ).fetchall()
            schema_row = connection.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()

            profiles = []
            for profile_row in profile_rows:
                name = str(profile_row["name"])
                root = str(profile_row["root"])
                scope = (name, root, len(root) + 1, f"{root}/")
                directory_row = connection.execute(
                    """
                    SELECT COUNT(*) AS directories,
                           SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS active_directories,
                           SUM(CASE WHEN active=0 THEN 1 ELSE 0 END) AS inactive_directories,
                           SUM(CASE WHEN active=1 AND complete=0 THEN 1 ELSE 0 END) AS pending_directories,
                           MAX(CASE WHEN active=1 THEN last_cache_success END) AS last_cache_success,
                           MAX(CASE WHEN active=1 THEN last_live_success END) AS last_live_success
                    FROM directories
                    WHERE profile=? AND (path=? OR substr(path, 1, ?)=?)
                    """,
                    scope,
                ).fetchone()
                object_row = connection.execute(
                    """
                    SELECT COUNT(*) AS objects,
                           SUM(CASE WHEN missing_observations>0 THEN 1 ELSE 0 END) AS missing_observed
                    FROM source_objects
                    WHERE profile=? AND (path=? OR substr(path, 1, ?)=?)
                    """,
                    scope,
                ).fetchone()
                depth_rows = connection.execute(
                    """
                    SELECT depth, COUNT(*) AS count
                    FROM directories
                    WHERE profile=? AND active=1 AND complete=0
                      AND (path=? OR substr(path, 1, ?)=?)
                    GROUP BY depth ORDER BY depth
                    """,
                    scope,
                ).fetchall()
                root_depth = self._depth(root)
                profiles.append(
                    {
                        "profile": name,
                        "logic": str(profile_row["logic"]),
                        "root": root,
                        "enabled": bool(profile_row["enabled"]),
                        "directories": int(directory_row["directories"] or 0),
                        "active_directories": int(directory_row["active_directories"] or 0),
                        "inactive_directories": int(directory_row["inactive_directories"] or 0),
                        "pending_directories": int(directory_row["pending_directories"] or 0),
                        "pending_by_depth": {
                            str(max(0, int(row["depth"]) - root_depth)): int(row["count"])
                            for row in depth_rows
                        },
                        "objects": int(object_row["objects"] or 0),
                        "missing_observed": int(object_row["missing_observed"] or 0),
                        "last_cache_success": directory_row["last_cache_success"] or "",
                        "last_live_success": directory_row["last_live_success"] or "",
                        "updated_at": profile_row["updated_at"],
                    }
                )

        return {
            "schema_version": int(schema_row["value"] if schema_row else 0),
            "profiles": profiles,
        }

    def count_objects(self, profile: Optional[str] = None) -> int:
        with self._connection() as connection:
            if profile:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM source_objects WHERE profile=?", (profile,)
                ).fetchone()
            else:
                row = connection.execute("SELECT COUNT(*) AS count FROM source_objects").fetchone()
        return int(row["count"] if row else 0)

    # ------------------------------------------------------------ 阶段2：规划

    @staticmethod
    def plan_id(profile: str, path: str) -> str:
        """稳定 plan 主键：profile + A 绝对路径的 SHA-256。"""

        return hashlib.sha256(f"{profile}\0{path}".encode("utf-8")).hexdigest()

    def iter_planning_batches(
        self, profile: str, root: str
    ) -> Iterator[tuple[str, list[SourceObject]]]:
        """按目录产出**分页完整且仍 active** 的库存对象。

        只发布 ``directories.complete=1`` 的目录，保证 planner 拿到的是整个目录的
        兄弟节点列表，可以直接做目标占用冲突判定；不完整目录一律不参与规划，
        对应设计原则「不完整就不推导删除」。
        """

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT o.source_key, o.path, o.parent_path, o.basename, o.is_dir,
                       o.size, o.modified, o.object_id, o.fingerprint
                FROM source_objects AS o
                JOIN directories AS d
                  ON d.profile = o.profile AND d.path = o.parent_path
                WHERE o.profile=?
                  AND o.tombstone=0
                  AND d.active=1
                  AND d.complete=1
                  AND (o.parent_path=? OR substr(o.parent_path, 1, ?)=?)
                ORDER BY o.parent_path, o.basename
                """,
                (profile, root, len(root) + 1, f"{root}/"),
            ).fetchall()

        current_parent = ""
        batch: list[SourceObject] = []
        for row in rows:
            parent = str(row["parent_path"])
            if parent != current_parent:
                if batch:
                    yield current_parent, batch
                current_parent = parent
                batch = []
            batch.append(
                SourceObject(
                    profile=profile,
                    path=str(row["path"]),
                    parent_path=parent,
                    basename=str(row["basename"]),
                    is_dir=bool(row["is_dir"]),
                    size=int(row["size"] or 0),
                    modified=str(row["modified"] or ""),
                    object_id=str(row["object_id"] or ""),
                    fingerprint=str(row["fingerprint"] or ""),
                )
            )
        if batch:
            yield current_parent, batch

    def previous_plan_index(self, profile: str) -> dict[str, tuple[str, str, str]]:
        """返回 path -> (fingerprint, ruleset_version, state)，用于增量差异统计。"""

        with self._connection() as connection:
            rows = connection.execute(
                "SELECT path, source_fingerprint, ruleset_version, state FROM plans WHERE profile=?",
                (profile,),
            ).fetchall()
        return {
            str(row["path"]): (
                str(row["source_fingerprint"] or ""),
                str(row["ruleset_version"] or ""),
                str(row["state"] or ""),
            )
            for row in rows
        }

    def replace_plans(self, profile: str, plans: Iterable[dict], *, updated_at: str) -> int:
        """整体替换某个 profile 的计划快照。

        plans 是纯派生状态：输入是库存 + 规则，两者都可追溯，所以整体替换不会
        丢失事实，也让规则升级后的重算保持幂等。当前版本没有执行链路，
        计划的唯一消费者是报告和状态页。
        """

        items = list(plans)
        with self._transaction() as connection:
            connection.execute("DELETE FROM plans WHERE profile=?", (profile,))
            connection.executemany(
                """
                INSERT INTO plans(
                    plan_id, profile, logic, source_key, path, parent_path, basename,
                    extension, size, modified, source_fingerprint, ruleset_version,
                    state, action, lifecycle, canonical, target_name, confidence,
                    reason, payload_json, manifest_id, created_at, updated_at
                ) VALUES(
                    :plan_id, :profile, :logic, :source_key, :path, :parent_path, :basename,
                    :extension, :size, :modified, :source_fingerprint, :ruleset_version,
                    :state, :action, :lifecycle, :canonical, :target_name, :confidence,
                    :reason, :payload_json, '', :updated_at, :updated_at
                )
                """,
                [{**item, "updated_at": updated_at} for item in items],
            )
        return len(items)

    def plan_state_counts(self, profile: Optional[str] = None) -> dict[str, int]:
        """按 state 聚合计划数量。"""

        with self._connection() as connection:
            if profile:
                rows = connection.execute(
                    "SELECT state, COUNT(*) AS count FROM plans WHERE profile=? GROUP BY state",
                    (profile,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT state, COUNT(*) AS count FROM plans GROUP BY state"
                ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}

    def plan_records(self, *, states: Optional[Iterable[str]] = None) -> list[dict]:
        """按确定顺序取出计划的完整可审计记录，供报告序列化。

        ``states`` 为空表示全部；传入状态清单可以只导出「需要人看的」计划，
        避免海量 NOOP 淹没 TSV。
        """

        wanted = [str(value) for value in (states or ()) if str(value).strip()]
        query = "SELECT payload_json FROM plans"
        parameters: tuple = ()
        if wanted:
            placeholders = ",".join("?" for _ in wanted)
            query += f" WHERE state IN ({placeholders})"
            parameters = tuple(wanted)
        query += " ORDER BY profile, path"
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        records: list[dict] = []
        for row in rows:
            try:
                records.append(json.loads(str(row["payload_json"])))
            except (TypeError, ValueError):
                continue
        return records

    def sample_plans(
        self, state: str, limit: int, *, profile: Optional[str] = None
    ) -> list[dict]:
        """按确定顺序抽取某个 state 的计划，用于通知正文里展示样例。

        这是只读抽样，不改变任何计划状态；完整清单永远看报告文件。
        """

        capped = max(0, int(limit))
        if not capped:
            return []
        query = (
            "SELECT profile, path, basename, target_name, canonical, size, reason, state "
            "FROM plans WHERE state=?"
        )
        parameters: list = [str(state)]
        if profile:
            query += " AND profile=?"
            parameters.append(profile)
        query += " ORDER BY profile, path LIMIT ?"
        parameters.append(capped)
        with self._connection() as connection:
            rows = connection.execute(query, tuple(parameters)).fetchall()
        return [dict(row) for row in rows]

    def plan_status(self) -> dict:
        """给状态页/通知用的规划聚合。

        ``manifests`` 与 ``actions`` 仍是空表：当前版本没有执行链路，
        115 的改名/删除由独立服务承担（原因和安全约束见 hub-seed/README.md）。
        这两张表保留只为将来迁移时不再动 schema。
        """

        with self._connection() as connection:
            state_rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM plans GROUP BY state ORDER BY state"
            ).fetchall()
            profile_rows = connection.execute(
                """
                SELECT profile,
                       COUNT(*) AS planned,
                       SUM(CASE WHEN state='RENAME_READY' THEN 1 ELSE 0 END) AS rename_ready,
                       SUM(CASE WHEN state='GARBAGE_READY' THEN 1 ELSE 0 END) AS garbage_ready,
                       MAX(ruleset_version) AS ruleset_version,
                       MAX(updated_at) AS updated_at
                FROM plans GROUP BY profile ORDER BY profile
                """
            ).fetchall()
            garbage_rows = connection.execute(
                """
                SELECT reason, COUNT(*) AS count
                FROM plans WHERE state='GARBAGE_READY'
                GROUP BY reason ORDER BY count DESC, reason LIMIT 20
                """
            ).fetchall()
            size_row = connection.execute(
                "SELECT COALESCE(SUM(size), 0) AS bytes FROM plans WHERE state='GARBAGE_READY'"
            ).fetchone()
        return {
            "plan_states": {str(row["state"]): int(row["count"]) for row in state_rows},
            "profiles": [dict(row) for row in profile_rows],
            "garbage_reasons": {
                str(row["reason"]): int(row["count"]) for row in garbage_rows
            },
            "garbage_bytes": int(size_row["bytes"] if size_row else 0),
        }

    @staticmethod
    def _depth(path: str) -> int:
        return len([part for part in path.split("/") if part])
