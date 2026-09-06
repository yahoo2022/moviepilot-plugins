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


SCHEMA_VERSION = 2


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
                source_fingerprint TEXT NOT NULL,
                ruleset_version TEXT NOT NULL,
                state TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS manifests (
                manifest_id TEXT PRIMARY KEY,
                digest TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS actions (
                action_id TEXT PRIMARY KEY,
                manifest_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                result_json TEXT NOT NULL
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
            connection.execute("UPDATE plans SET logic=profile WHERE logic='' ")
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

    @staticmethod
    def _depth(path: str) -> int:
        return len([part for part in path.split("/") if part])
