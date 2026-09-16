"""本地（自建）Zep 后端。

当 ``ZEP_BACKEND=local`` 时，``get_zep_client()`` 返回本模块的 :class:`LocalZepClient`，
它在本地 SQLite 里维护图谱（实体 / 关系 / 事件），实体与关系的抽取由 ``Config`` 中配置的
OpenAI 兼容 LLM 完成，检索用字符级 TF-IDF，不依赖 Zep Cloud、不需要 ``ZEP_API_KEY``。

对外暴露的接口与调用形态和 zep_cloud SDK 对齐，包括：

* ``client.graph.create / get / delete / set_ontology / add / search``
* ``client.graph.episode.get``
* ``client.graph.node.get`` / ``client.graph.node.get_edges``
* ``client.graph.node.with_raw_response.get_by_graph_id``（分页，游标放在 ``zep-next-cursor`` 头）
* ``client.graph.edge.with_raw_response.get_by_graph_id``
* ``client.batch.create / add / process / get / list / list_items``
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
import uuid as _uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Iterable

from ..config import Config

try:  # 与云端 SDK 共用 NotFoundError，保证调用方的 except 分支能命中
    from zep_cloud import NotFoundError as _CloudNotFoundError
except Exception:  # pragma: no cover - SDK 缺失时退化为普通异常
    _CloudNotFoundError = Exception


class NotFoundError(_CloudNotFoundError):  # type: ignore[misc,valid-type]
    """本地图谱中对象不存在。"""


_NEXT_CURSOR_HEADER = "zep-next-cursor"
_MAX_EXTRACT_CHARS = 8000
_EXTRACT_MAX_TOKENS = 8192
_EXTRACT_WORKERS = 3
_EXTRACT_MAX_ATTEMPTS = 3
_EXTRACT_RETRY_DELAY_SECONDS = 3.0

_NODE_PAGE_SIZE = 100


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _loads(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


class _Obj:
    """轻量对象容器：字段名与 zep_cloud 返回对象保持一致。"""

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        head = ", ".join(f"{k}={v!r}" for k, v in list(self.__dict__.items())[:3])
        return f"<{type(self).__name__} {head}>"

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class _RawResponse:
    """模拟 SDK 的 ``with_raw_response`` 返回体（``.data`` + ``.headers``）。"""

    def __init__(self, data: Iterable[Any] | None = None, next_cursor: str | None = None) -> None:
        self.data = list(data or [])
        self.headers: dict[str, str] = {}
        if next_cursor is not None:
            self.headers[_NEXT_CURSOR_HEADER] = str(next_cursor)


# ---------------------------------------------------------------------------
# SQLite 存储
# ---------------------------------------------------------------------------


class _Store:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._init_schema()

    # -- schema ------------------------------------------------------------
    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS graphs (
                graph_id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                created_at TEXT,
                ontology_json TEXT
            );
            CREATE TABLE IF NOT EXISTS nodes (
                uuid_ TEXT PRIMARY KEY,
                graph_id TEXT NOT NULL,
                name TEXT,
                labels_json TEXT,
                summary TEXT,
                attributes_json TEXT,
                created_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_nodes_graph ON nodes(graph_id);
            CREATE TABLE IF NOT EXISTS edges (
                uuid_ TEXT PRIMARY KEY,
                graph_id TEXT NOT NULL,
                name TEXT,
                fact TEXT,
                source_node_uuid TEXT,
                target_node_uuid TEXT,
                attributes_json TEXT,
                created_at TEXT,
                valid_at TEXT,
                invalid_at TEXT,
                expired_at TEXT,
                episodes_json TEXT,
                scope TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_edges_graph ON edges(graph_id);
            CREATE TABLE IF NOT EXISTS episodes (
                uuid_ TEXT PRIMARY KEY,
                graph_id TEXT NOT NULL,
                content TEXT,
                created_at TEXT,
                source TEXT,
                source_description TEXT,
                metadata_json TEXT,
                processed INTEGER DEFAULT 0,
                role TEXT,
                role_type TEXT,
                task_id TEXT,
                thread_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_episodes_graph ON episodes(graph_id);
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY,
                metadata_json TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT,
                total_items INTEGER DEFAULT 0,
                succeeded_items INTEGER DEFAULT 0,
                failed_items INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS batch_items (
                batch_id TEXT,
                sequence_index INTEGER,
                type TEXT,
                data TEXT,
                metadata_json TEXT,
                episode_uuid TEXT,
                status TEXT,
                error TEXT,
                PRIMARY KEY (batch_id, sequence_index)
            );
            """
        )
        self._conn.commit()

    # -- helpers -----------------------------------------------------------
    def _execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            cursor = self._conn.execute(sql, tuple(params))
            self._conn.commit()
            return cursor

    def _query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, tuple(params)).fetchall())

    def _query_one(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        rows = self._query(sql, params)
        return rows[0] if rows else None

    # -- graphs ------------------------------------------------------------
    def create_graph(self, graph_id: str, name: str | None, description: str | None) -> None:
        self._execute(
            "INSERT INTO graphs (graph_id, name, description, created_at, ontology_json)"
            " VALUES (?, ?, ?, ?, ?)",
            (graph_id, name, description, _now_iso(), None),
        )

    def graph_exists(self, graph_id: str) -> bool:
        return self._query_one("SELECT 1 FROM graphs WHERE graph_id = ?", (graph_id,)) is not None

    def get_graph_row(self, graph_id: str) -> dict[str, Any] | None:
        row = self._query_one("SELECT * FROM graphs WHERE graph_id = ?", (graph_id,))
        return dict(row) if row else None

    def delete_graph(self, graph_id: str) -> None:
        self._execute("DELETE FROM edges WHERE graph_id = ?", (graph_id,))
        self._execute("DELETE FROM nodes WHERE graph_id = ?", (graph_id,))
        self._execute("DELETE FROM episodes WHERE graph_id = ?", (graph_id,))
        self._execute("DELETE FROM graphs WHERE graph_id = ?", (graph_id,))

    def set_ontology(self, graph_id: str, ontology: dict[str, Any]) -> None:
        self._execute(
            "UPDATE graphs SET ontology_json = ? WHERE graph_id = ?",
            (json.dumps(ontology, ensure_ascii=False), graph_id),
        )

    def get_ontology(self, graph_id: str) -> dict[str, Any]:
        row = self._query_one("SELECT ontology_json FROM graphs WHERE graph_id = ?", (graph_id,))
        if row is None:
            return {}
        return _loads(row["ontology_json"], {}) or {}

    # -- nodes / edges -----------------------------------------------------
    def find_node_by_name(self, graph_id: str, name: str) -> sqlite3.Row | None:
        return self._query_one(
            "SELECT * FROM nodes WHERE graph_id = ? AND lower(name) = ?",
            (graph_id, name.strip().lower()),
        )

    def insert_node(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO nodes (uuid_, graph_id, name, labels_json, summary, attributes_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["uuid_"],
                row["graph_id"],
                row["name"],
                json.dumps(row.get("labels") or [], ensure_ascii=False),
                row.get("summary"),
                json.dumps(row.get("attributes") or {}, ensure_ascii=False),
                row.get("created_at") or _now_iso(),
            ),
        )

    def update_node(self, node_uuid: str, summary: str | None, labels: list[str], attributes: dict[str, Any]) -> None:
        self._execute(
            "UPDATE nodes SET summary = ?, labels_json = ?, attributes_json = ? WHERE uuid_ = ?",
            (
                summary,
                json.dumps(labels, ensure_ascii=False),
                json.dumps(attributes, ensure_ascii=False),
                node_uuid,
            ),
        )

    def list_nodes(self, graph_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._query("SELECT * FROM nodes WHERE graph_id = ?", (graph_id,))]

    def list_edges(self, graph_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._query("SELECT * FROM edges WHERE graph_id = ?", (graph_id,))]

    def get_node(self, node_uuid: str) -> dict[str, Any] | None:
        row = self._query_one("SELECT * FROM nodes WHERE uuid_ = ?", (node_uuid,))
        return dict(row) if row else None

    def get_edge(self, edge_uuid: str) -> dict[str, Any] | None:
        row = self._query_one("SELECT * FROM edges WHERE uuid_ = ?", (edge_uuid,))
        return dict(row) if row else None

    def find_edge(self, graph_id: str, source: str, target: str, name: str) -> dict[str, Any] | None:
        row = self._query_one(
            "SELECT * FROM edges WHERE graph_id = ? AND source_node_uuid = ? AND target_node_uuid = ? AND name = ?",
            (graph_id, source, target, name),
        )
        return dict(row) if row else None

    def insert_edge(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO edges (uuid_, graph_id, name, fact, source_node_uuid, target_node_uuid,"
            " attributes_json, created_at, valid_at, invalid_at, expired_at, episodes_json, scope)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["uuid_"],
                row["graph_id"],
                row.get("name"),
                row.get("fact"),
                row.get("source_node_uuid"),
                row.get("target_node_uuid"),
                json.dumps(row.get("attributes") or {}, ensure_ascii=False),
                row.get("created_at") or _now_iso(),
                row.get("valid_at"),
                row.get("invalid_at"),
                row.get("expired_at"),
                json.dumps(row.get("episodes") or [], ensure_ascii=False),
                row.get("scope") or "global",
            ),
        )

    def edges_of_node(self, node_uuid: str) -> list[dict[str, Any]]:
        rows = self._query(
            "SELECT * FROM edges WHERE source_node_uuid = ? OR target_node_uuid = ?",
            (node_uuid, node_uuid),
        )
        return [dict(row) for row in rows]

    # -- episodes ----------------------------------------------------------
    def insert_episode(self, row: dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO episodes (uuid_, graph_id, content, created_at, source, source_description,"
            " metadata_json, processed, role, role_type, task_id, thread_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
            (
                row["uuid_"],
                row["graph_id"],
                row.get("content"),
                row.get("created_at") or _now_iso(),
                row.get("source") or "api",
                row.get("source_description"),
                json.dumps(row.get("metadata") or {}, ensure_ascii=False),
                row.get("role"),
                row.get("role_type"),
                row.get("task_id"),
                row.get("thread_id"),
            ),
        )

    def get_episode(self, episode_uuid: str) -> dict[str, Any] | None:
        row = self._query_one("SELECT * FROM episodes WHERE uuid_ = ?", (episode_uuid,))
        return dict(row) if row else None

    def mark_episode_processed(self, episode_uuid: str) -> None:
        self._execute("UPDATE episodes SET processed = 1 WHERE uuid_ = ?", (episode_uuid,))

    def list_episodes(self, graph_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._query("SELECT * FROM episodes WHERE graph_id = ?", (graph_id,))]

    # -- batches -----------------------------------------------------------
    def insert_batch(self, batch_id: str, metadata: dict[str, Any], total_items: int) -> None:
        now = _now_iso()
        self._execute(
            "INSERT INTO batches (batch_id, metadata_json, status, created_at, updated_at, total_items,"
            " succeeded_items, failed_items) VALUES (?, ?, 'draft', ?, ?, ?, 0, 0)",
            (batch_id, json.dumps(metadata or {}, ensure_ascii=False), now, now, total_items),
        )

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        row = self._query_one("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))
        return dict(row) if row else None

    def list_batches(self, limit: int, offset: int) -> list[dict[str, Any]]:
        rows = self._query(
            "SELECT * FROM batches ORDER BY created_at DESC, batch_id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [dict(row) for row in rows]

    def count_batches(self) -> int:
        row = self._query_one("SELECT COUNT(*) AS c FROM batches")
        return int(row["c"]) if row else 0

    def set_batch_total(self, batch_id: str, total_items: int) -> None:
        self._execute(
            "UPDATE batches SET total_items = ?, updated_at = ? WHERE batch_id = ?",
            (total_items, _now_iso(), batch_id),
        )

    def update_batch_status(self, batch_id: str, status: str) -> None:
        self._execute(
            "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
            (status, _now_iso(), batch_id),
        )

    def update_batch_progress(self, batch_id: str, succeeded: int, failed: int) -> None:
        self._execute(
            "UPDATE batches SET succeeded_items = ?, failed_items = ?, updated_at = ? WHERE batch_id = ?",
            (succeeded, failed, _now_iso(), batch_id),
        )

    def insert_batch_item(self, item: dict[str, Any]) -> None:
        self._execute(
            "INSERT OR REPLACE INTO batch_items (batch_id, sequence_index, type, data, metadata_json,"
            " episode_uuid, status, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item["batch_id"],
                item["sequence_index"],
                item.get("type"),
                item.get("data"),
                json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                item.get("episode_uuid"),
                item.get("status") or "pending",
                item.get("error"),
            ),
        )

    def update_batch_item_status(self, batch_id: str, sequence_index: int, status: str, error: str | None = None) -> None:
        self._execute(
            "UPDATE batch_items SET status = ?, error = ? WHERE batch_id = ? AND sequence_index = ?",
            (status, error, batch_id, sequence_index),
        )

    def list_batch_items(self, batch_id: str, limit: int, offset: int) -> list[dict[str, Any]]:
        rows = self._query(
            "SELECT * FROM batch_items WHERE batch_id = ? ORDER BY sequence_index LIMIT ? OFFSET ?",
            (batch_id, limit, offset),
        )
        return [dict(row) for row in rows]

    def count_batch_items(self, batch_id: str) -> int:
        row = self._query_one("SELECT COUNT(*) AS c FROM batch_items WHERE batch_id = ?", (batch_id,))
        return int(row["c"]) if row else 0


# ---------------------------------------------------------------------------
# LLM 抽取
# ---------------------------------------------------------------------------

_EXTRACTION_SYSTEM_PROMPT = (
    "你是知识图谱抽取引擎。请从用户给出的文本片段中抽取实体和实体之间的关系，"
    "并严格输出一个 JSON 对象，不要输出任何解释或 Markdown 代码块。"
)

_EXTRACTION_SCHEMA_HINT = (
    '输出格式：{"entities":[{"name":"实体名称","type":"实体类型","summary":"一句话说明"}],'
    '"edges":[{"source":"源实体名称","target":"目标实体名称","type":"关系类型","fact":"关系事实陈述"}]}\n'
    "要求：\n"
    "1. name 用最简洁的专名（人名、机构、产品、地点、事件、概念等），不要带修饰语；\n"
    "2. summary 用中文一句话概括该实体在文本中的关键信息；\n"
    "3. fact 用中文陈述句描述关系，句中要出现源实体与目标实体的名称；\n"
    "4. source / target 必须是上面 entities 里出现过的 name；\n"
    '5. 没有可抽取内容时返回 {"entities":[],"edges":[]}。'
)


_BATCH_ITEM_FIELDS = ("type", "graph_id", "data", "data_type", "source_description", "metadata")


def _coerce_batch_item(raw_item: Any) -> dict[str, Any]:
    """把 SDK 的 BatchAddItem 或普通 dict 归一化成字典。"""

    if isinstance(raw_item, dict):
        return raw_item
    fields = {
        name: getattr(raw_item, name)
        for name in _BATCH_ITEM_FIELDS
        if getattr(raw_item, name, None) is not None
    }
    if not fields:
        fields = {"data": raw_item if isinstance(raw_item, str) else str(raw_item)}
    return fields


def _extract_json_object(text: str) -> dict[str, Any]:
    if not text:
        return {}
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
        cleaned = re.sub(r"```\s*$", "", cleaned).strip()
    try:
        return json.loads(cleaned)
    except (TypeError, ValueError):
        pass
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(cleaned[start : end + 1])
        except (TypeError, ValueError):
            return {}
    return {}


class _Extractor:
    """用 LLM 把文本片段转成实体 / 关系，并写入本地图谱。"""

    def __init__(self, store: _Store) -> None:
        self._store = store
        self._client_lock = threading.Lock()
        self._client = None

    def _llm(self):
        with self._client_lock:
            if self._client is None:
                from openai import OpenAI

                if not Config.LLM_API_KEY:
                    raise RuntimeError("LLM_API_KEY 未配置，无法在本地模式下抽取实体")
                self._client = OpenAI(
                    api_key=Config.LLM_API_KEY,
                    base_url=Config.LLM_BASE_URL,
                    timeout=120.0,
                    max_retries=1,
                )
            return self._client

    @staticmethod
    def _ontology_hint(ontology: dict[str, Any]) -> str:
        entities = (ontology or {}).get("entities") or {}
        edges = (ontology or {}).get("edges") or {}
        lines: list[str] = []
        if entities:
            lines.append("允许的实体类型：" + "、".join(list(entities.keys())[:40]))
        if edges:
            lines.append("允许的关系类型：" + "、".join(list(edges.keys())[:40]))
        return "\n".join(lines)

    def _chat(self, text: str, ontology: dict[str, Any]) -> dict[str, Any]:
        hint = self._ontology_hint(ontology)
        user_prompt = f"{_EXTRACTION_SCHEMA_HINT}\n{hint}\n\n文本片段：\n{text}"
        last_error: Exception | None = None
        disable_thinking = True
        for attempt in range(1, _EXTRACT_MAX_ATTEMPTS + 1):
            request_kwargs: dict[str, Any] = {}
            if disable_thinking:
                # 推理型模型（如 deepseek-v4.1-flash）默认带思考，抽取任务无需推理链
                request_kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
            try:
                response = self._llm().chat.completions.create(
                    model=Config.LLM_MODEL_NAME,
                    messages=[
                        {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.1,
                    max_tokens=_EXTRACT_MAX_TOKENS,
                    **request_kwargs,
                )
                content = ""
                if response and getattr(response, "choices", None):
                    message = response.choices[0].message
                    content = getattr(message, "content", None) or ""
                    if not content:
                        content = getattr(message, "reasoning_content", None) or ""
                payload = _extract_json_object(content)
                if payload:
                    return payload
                last_error = ValueError("LLM 未返回可解析的 JSON")
            except Exception as error:  # noqa: BLE001 - 渠道可能限流或参数不被支持，退避重试
                message = str(error).lower()
                if disable_thinking and any(
                    token in message for token in ("thinking", "extra_body", "unknown", "unsupported", "invalid_request")
                ):
                    disable_thinking = False
                last_error = error
            if attempt < _EXTRACT_MAX_ATTEMPTS:
                time.sleep(_EXTRACT_RETRY_DELAY_SECONDS * attempt)
        raise RuntimeError(f"LLM 抽取失败：{last_error}")

    def process_episode(self, graph_id: str, episode_uuid: str, content: str) -> None:
        """抽取单个 episode 并在结束后标记 processed。"""

        try:
            text = (content or "").strip()
            if not text:
                return
            text = text[:_MAX_EXTRACT_CHARS]
            ontology = self._store.get_ontology(graph_id)
            payload = self._chat(text, ontology)
            self._apply(graph_id, episode_uuid, payload)
        finally:
            self._store.mark_episode_processed(episode_uuid)

    def _apply(self, graph_id: str, episode_uuid: str, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        raw_entities = payload.get("entities") or []
        raw_edges = payload.get("edges") or []

        name_to_uuid: dict[str, str] = {}
        for entity in raw_entities:
            if not isinstance(entity, dict):
                continue
            name = str(entity.get("name") or "").strip()
            if not name:
                continue
            entity_type = str(entity.get("type") or "Entity").strip() or "Entity"
            summary = str(entity.get("summary") or "").strip()
            attributes = entity.get("attributes") if isinstance(entity.get("attributes"), dict) else {}
            labels = [entity_type] + (["Entity"] if entity_type != "Entity" else [])

            existing = self._store.find_node_by_name(graph_id, name)
            if existing:
                merged_summary = existing["summary"] or ""
                if summary and summary not in merged_summary:
                    merged_summary = f"{merged_summary}\n{summary}".strip()
                merged_labels = sorted(set(_loads(existing["labels_json"], []) or []) | set(labels))
                merged_attributes = {
                    **(_loads(existing["attributes_json"], {}) or {}),
                    **(attributes or {}),
                }
                self._store.update_node(existing["uuid_"], merged_summary, merged_labels, merged_attributes)
                name_to_uuid[name.lower()] = existing["uuid_"]
            else:
                node_uuid = str(_uuid.uuid4())
                self._store.insert_node(
                    {
                        "uuid_": node_uuid,
                        "graph_id": graph_id,
                        "name": name,
                        "labels": labels,
                        "summary": summary,
                        "attributes": attributes,
                    }
                )
                name_to_uuid[name.lower()] = node_uuid

        for edge in raw_edges:
            if not isinstance(edge, dict):
                continue
            source_name = str(edge.get("source") or "").strip().lower()
            target_name = str(edge.get("target") or "").strip().lower()
            source_uuid = name_to_uuid.get(source_name) or self._resolve_existing(graph_id, source_name)
            target_uuid = name_to_uuid.get(target_name) or self._resolve_existing(graph_id, target_name)
            if not source_uuid or not target_uuid or source_uuid == target_uuid:
                continue
            edge_name = str(edge.get("type") or "RELATED_TO").strip() or "RELATED_TO"
            fact = str(edge.get("fact") or "").strip()
            if self._store.find_edge(graph_id, source_uuid, target_uuid, edge_name):
                continue
            self._store.insert_edge(
                {
                    "uuid_": str(_uuid.uuid4()),
                    "graph_id": graph_id,
                    "name": edge_name,
                    "fact": fact or edge_name,
                    "source_node_uuid": source_uuid,
                    "target_node_uuid": target_uuid,
                    "episodes": [episode_uuid],
                    "scope": "global",
                }
            )

    def _resolve_existing(self, graph_id: str, name: str) -> str | None:
        if not name:
            return None
        row = self._store.find_node_by_name(graph_id, name)
        return row["uuid_"] if row else None


# ---------------------------------------------------------------------------
# 检索
# ---------------------------------------------------------------------------


class _Searcher:
    """字符级 TF-IDF 相似度检索（对中文友好，无需 embedding 服务）。"""

    def __init__(self, store: _Store) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = {}

    @staticmethod
    def _vectorizer():
        from sklearn.feature_extraction.text import TfidfVectorizer

        return TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 3), min_df=1, sublinear_tf=True)

    def _index(self, graph_id: str):
        nodes = self._store.list_nodes(graph_id)
        edges = self._store.list_edges(graph_id)
        signature = len(nodes) * 1_000_000 + len(edges)
        with self._lock:
            cached = self._cache.get(graph_id)
            if cached and cached["signature"] == signature:
                return cached["matrix"], cached["vectorizer"], nodes, edges
        node_docs = [
            f"{row['name']} {' '.join(_loads(row['labels_json'], []) or [])} {row['summary'] or ''}"
            for row in nodes
        ]
        edge_docs = [f"{row['name']} {row['fact'] or ''}" for row in edges]
        corpus = node_docs + edge_docs
        if not corpus:
            return None, None, nodes, edges
        vectorizer = self._vectorizer()
        matrix = vectorizer.fit_transform(corpus)
        with self._lock:
            self._cache[graph_id] = {
                "signature": signature,
                "matrix": matrix,
                "vectorizer": vectorizer,
            }
        return matrix, vectorizer, nodes, edges

    def search(
        self, graph_id: str, query: str, limit: int, scope: str | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        matrix, vectorizer, nodes, edges = self._index(graph_id)
        scope = (scope or "").strip().lower()
        if scope == "nodes":
            want_nodes, want_edges = True, False
        elif scope in ("edges", "episodes", "observations", "thread_summaries"):
            want_nodes, want_edges = False, True
        else:
            want_nodes, want_edges = True, True
        if vectorizer is None or matrix is None:
            return (nodes[:limit] if want_nodes else []), (edges[:limit] if want_edges else [])

        from sklearn.metrics.pairwise import cosine_similarity

        scores = cosine_similarity(vectorizer.transform([query or ""]), matrix).ravel()
        node_scores = scores[: len(nodes)]
        edge_scores = scores[len(nodes) :]

        node_hits: list[dict[str, Any]] = []
        edge_hits: list[dict[str, Any]] = []
        if want_nodes and len(node_scores):
            for index in node_scores.argsort()[::-1]:
                if node_scores[index] <= 0 or len(node_hits) >= limit:
                    break
                item = dict(nodes[int(index)])
                item["_score"] = float(node_scores[index])
                node_hits.append(item)
        if want_edges and len(edge_scores):
            for index in edge_scores.argsort()[::-1]:
                if edge_scores[index] <= 0 or len(edge_hits) >= limit:
                    break
                item = dict(edges[int(index)])
                item["_score"] = float(edge_scores[index])
                edge_hits.append(item)
        return node_hits, edge_hits


# ---------------------------------------------------------------------------
# 对象构造
# ---------------------------------------------------------------------------


def _node_obj(row: dict[str, Any], *, relevance: float | None = None) -> _Obj:
    return _Obj(
        uuid_=row["uuid_"],
        name=row.get("name"),
        labels=_loads(row.get("labels_json"), []) or [],
        summary=row.get("summary"),
        attributes=_loads(row.get("attributes_json"), {}) or {},
        created_at=row.get("created_at"),
        relevance=relevance,
        score=relevance,
        selection_rank=None,
    )


def _edge_obj(row: dict[str, Any], *, relevance: float | None = None) -> _Obj:
    return _Obj(
        uuid_=row["uuid_"],
        name=row.get("name"),
        fact=row.get("fact"),
        source_node_uuid=row.get("source_node_uuid"),
        target_node_uuid=row.get("target_node_uuid"),
        attributes=_loads(row.get("attributes_json"), {}) or {},
        created_at=row.get("created_at"),
        valid_at=row.get("valid_at"),
        invalid_at=row.get("invalid_at"),
        expired_at=row.get("expired_at"),
        episodes=_loads(row.get("episodes_json"), []) or [],
        scope=row.get("scope"),
        relevance=relevance,
        score=relevance,
        selection_rank=None,
    )


def _episode_obj(row: dict[str, Any], *, relevance: float | None = None) -> _Obj:
    return _Obj(
        uuid_=row["uuid_"],
        content=row.get("content"),
        created_at=row.get("created_at"),
        metadata=_loads(row.get("metadata_json"), {}) or {},
        processed=bool(row.get("processed")),
        role=row.get("role"),
        role_type=row.get("role_type"),
        source=row.get("source"),
        source_description=row.get("source_description"),
        task_id=row.get("task_id"),
        thread_id=row.get("thread_id"),
        relevance=relevance,
        score=relevance,
        selection_rank=None,
    )


def _graph_obj(row: dict[str, Any]) -> _Obj:
    return _Obj(
        uuid_=row["graph_id"],
        graph_id=row["graph_id"],
        name=row.get("name"),
        description=row.get("description"),
        created_at=row.get("created_at"),
        ontology=_loads(row.get("ontology_json"), {}) or {},
    )


# ---------------------------------------------------------------------------
# graph.* 子命名空间
# ---------------------------------------------------------------------------


class _RawNodeAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client

    def get_by_graph_id(
        self,
        graph_id: str,
        limit: int = _NODE_PAGE_SIZE,
        cursor: str | None = None,
        **kwargs: Any,
    ) -> _RawResponse:
        offset = int(cursor or 0)
        rows = self._client._store.list_nodes(graph_id)
        page = rows[offset : offset + max(1, int(limit))]
        next_cursor = str(offset + len(page)) if offset + len(page) < len(rows) else None
        return _RawResponse([_node_obj(row) for row in page], next_cursor)


class _RawEdgeAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client

    def get_by_graph_id(
        self,
        graph_id: str,
        limit: int = _NODE_PAGE_SIZE,
        cursor: str | None = None,
        **kwargs: Any,
    ) -> _RawResponse:
        offset = int(cursor or 0)
        rows = self._client._store.list_edges(graph_id)
        page = rows[offset : offset + max(1, int(limit))]
        next_cursor = str(offset + len(page)) if offset + len(page) < len(rows) else None
        return _RawResponse([_edge_obj(row) for row in page], next_cursor)


class _NodeAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client
        self.with_raw_response = _RawNodeAPI(client)

    def get(self, uuid_: str, **kwargs: Any) -> _Obj:
        row = self._client._store.get_node(uuid_)
        if not row:
            raise NotFoundError(f"node {uuid_} not found")
        return _node_obj(row)

    def get_edges(self, node_uuid: str | None = None, uuid_: str | None = None, **kwargs: Any) -> list[_Obj]:
        target = node_uuid or uuid_
        if not target:
            raise ValueError("node_uuid is required")
        return [_edge_obj(row) for row in self._client._store.edges_of_node(target)]


class _EdgeAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client
        self.with_raw_response = _RawEdgeAPI(client)

    def get(self, uuid_: str, **kwargs: Any) -> _Obj:
        row = self._client._store.get_edge(uuid_)
        if not row:
            raise NotFoundError(f"edge {uuid_} not found")
        return _edge_obj(row)


class _EpisodeAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client

    def get(self, uuid_: str, **kwargs: Any) -> _Obj:
        row = self._client._store.get_episode(uuid_)
        if not row:
            raise NotFoundError(f"episode {uuid_} not found")
        return _episode_obj(row)


class _GraphAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client
        self.node = _NodeAPI(client)
        self.edge = _EdgeAPI(client)
        self.episode = _EpisodeAPI(client)

    # -- CRUD --------------------------------------------------------------
    def create(self, graph_id: str, name: str | None = None, description: str | None = None, **kwargs: Any) -> _Obj:
        store = self._client._store
        if not store.graph_exists(graph_id):
            store.create_graph(graph_id, name, description)
        return _graph_obj(store.get_graph_row(graph_id))

    def get(self, graph_id: str, **kwargs: Any) -> _Obj:
        row = self._client._store.get_graph_row(graph_id)
        if not row:
            raise NotFoundError(f"graph {graph_id} not found")
        return _graph_obj(row)

    def delete(self, graph_id: str, **kwargs: Any) -> None:
        self._client._store.delete_graph(graph_id)

    def set_ontology(
        self,
        graph_ids: list[str] | None = None,
        entities: dict[str, Any] | None = None,
        edges: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        ontology = {
            "entities": self._describe_entities(entities or {}),
            "edges": self._describe_edges(edges or {}),
        }
        for graph_id in graph_ids or []:
            if self._client._store.graph_exists(graph_id):
                self._client._store.set_ontology(graph_id, ontology)

    @staticmethod
    def _field_names(model_class: Any) -> dict[str, str]:
        fields = getattr(model_class, "model_fields", None) or getattr(model_class, "__fields__", None) or {}
        described: dict[str, str] = {}
        for name, info in fields.items():
            annotation = getattr(info, "annotation", None)
            described[str(name)] = str(getattr(annotation, "__name__", annotation) or "text")
        return described

    def _describe_entities(self, entities: dict[str, Any]) -> dict[str, Any]:
        described: dict[str, Any] = {}
        for name, model_class in entities.items():
            described[str(name)] = {
                "description": (getattr(model_class, "__doc__", None) or "").strip()[:500],
                "attributes": self._field_names(model_class),
            }
        return described

    def _describe_edges(self, edges: dict[str, Any]) -> dict[str, Any]:
        described: dict[str, Any] = {}
        for name, definition in edges.items():
            model_class = definition
            source_targets: list[dict[str, str]] = []
            if isinstance(definition, tuple) and definition:
                model_class = definition[0]
                if len(definition) > 1 and definition[1]:
                    for source_target in definition[1]:
                        source_targets.append(
                            {
                                "source": str(getattr(source_target, "source", "") or ""),
                                "target": str(getattr(source_target, "target", "") or ""),
                            }
                        )
            described[str(name)] = {
                "description": (getattr(model_class, "__doc__", None) or "").strip()[:500],
                "attributes": self._field_names(model_class),
                "source_targets": source_targets,
            }
        return described

    # -- ingestion ---------------------------------------------------------
    def add(
        self,
        graph_id: str,
        type: str = "text",
        data: Any = None,
        created_at: str | None = None,
        source_description: str | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> _Obj:
        client = self._client
        store = client._store
        if not store.graph_exists(graph_id):
            store.create_graph(graph_id, None, None)
        content = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
        episode_uuid = str(_uuid.uuid4())
        store.insert_episode(
            {
                "uuid_": episode_uuid,
                "graph_id": graph_id,
                "content": content,
                "created_at": created_at,
                "source_description": source_description,
                "metadata": metadata,
            }
        )
        client.submit_episode(graph_id, episode_uuid, content)
        return _episode_obj(store.get_episode(episode_uuid))

    # -- search ------------------------------------------------------------
    def search(
        self,
        graph_id: str,
        query: str = "",
        limit: int = 10,
        scope: str | None = None,
        reranker: str | None = None,
        **kwargs: Any,
    ) -> _Obj:
        if not self._client._store.graph_exists(graph_id):
            raise NotFoundError(f"graph {graph_id} not found")
        node_hits, edge_hits = self._client._searcher.search(
            graph_id, query, max(1, int(limit or 10)), scope
        )
        nodes = [_node_obj(row, relevance=row.get("_score")) for row in node_hits]
        edges = [_edge_obj(row, relevance=row.get("_score")) for row in edge_hits]
        context_lines = [f"- {edge.fact}" for edge in edges if edge.fact]
        context_lines += [f"- {node.name}：{node.summary}" for node in nodes if node.summary]
        return _Obj(
            nodes=nodes or None,
            edges=edges or None,
            episodes=None,
            observations=None,
            thread_summaries=None,
            response=None,
            context="\n".join(context_lines) or None,
        )


# ---------------------------------------------------------------------------
# batch.* 子命名空间
# ---------------------------------------------------------------------------


class _BatchAPI:
    def __init__(self, client: "LocalZepClient") -> None:
        self._client = client

    def create(self, metadata: dict[str, Any] | None = None, **kwargs: Any) -> _Obj:
        batch_id = f"batch_{_uuid.uuid4().hex[:16]}"
        self._client._store.insert_batch(batch_id, metadata or {}, 0)
        row = self._client._store.get_batch(batch_id)
        return _Obj(
            batch_id=batch_id,
            status="draft",
            metadata=metadata or {},
            created_at=row["created_at"] if row else _now_iso(),
        )

    def add(self, batch_id: str, items: list[dict[str, Any]] | None = None, **kwargs: Any) -> list[_Obj]:
        store = self._client._store
        batch = store.get_batch(batch_id)
        if not batch:
            raise NotFoundError(f"batch {batch_id} not found")
        batch_metadata = _loads(batch["metadata_json"], {}) or {}
        graph_id = batch_metadata.get("graph_id")
        created: list[_Obj] = []
        start_index = store.count_batch_items(batch_id)
        for offset, raw_item in enumerate(items or []):
            item = _coerce_batch_item(raw_item)
            item_metadata = item.get("metadata") or {}
            item_graph_id = item.get("graph_id") or graph_id
            if item_graph_id and not store.graph_exists(item_graph_id):
                store.create_graph(item_graph_id, None, None)
            episode_uuid = str(_uuid.uuid4())
            content = item.get("data")
            if not isinstance(content, str):
                try:
                    content = json.dumps(content, ensure_ascii=False)
                except (TypeError, ValueError):
                    content = str(content)
            store.insert_episode(
                {
                    "uuid_": episode_uuid,
                    "graph_id": item_graph_id,
                    "content": content,
                    "source_description": item.get("source_description")
                    or batch_metadata.get("source_description"),
                    "metadata": item_metadata,
                }
            )
            sequence_index = start_index + offset
            store.insert_batch_item(
                {
                    "batch_id": batch_id,
                    "sequence_index": sequence_index,
                    "type": item.get("type") or "text",
                    "data": content,
                    "metadata": item_metadata,
                    "episode_uuid": episode_uuid,
                    "status": "pending",
                }
            )
            created.append(
                _Obj(
                    sequence_index=sequence_index,
                    episode_uuid=episode_uuid,
                    status="pending",
                    type=item.get("type") or "text",
                    data=content,
                    metadata=item_metadata,
                )
            )
        store.set_batch_total(batch_id, store.count_batch_items(batch_id))
        return created

    def process(self, batch_id: str, **kwargs: Any) -> None:
        store = self._client._store
        if not store.get_batch(batch_id):
            raise NotFoundError(f"batch {batch_id} not found")
        store.update_batch_status(batch_id, "processing")
        self._client.submit_batch(batch_id)

    def get(self, batch_id: str, **kwargs: Any) -> _Obj:
        row = self._client._store.get_batch(batch_id)
        if not row:
            raise NotFoundError(f"batch {batch_id} not found")
        return self._batch_summary(row)

    def list(self, limit: int = 100, cursor: str | None = None, **kwargs: Any) -> _Obj:
        store = self._client._store
        offset = int(cursor or 0)
        rows = store.list_batches(max(1, int(limit)), offset)
        next_cursor = str(offset + len(rows)) if offset + len(rows) < store.count_batches() else None
        return _Obj(batches=[self._batch_summary(row) for row in rows], next_cursor=next_cursor)

    def list_items(self, batch_id: str, limit: int = 100, cursor: str | None = None, **kwargs: Any) -> _Obj:
        store = self._client._store
        offset = int(cursor or 0)
        rows = store.list_batch_items(batch_id, max(1, int(limit)), offset)
        next_cursor = str(offset + len(rows)) if offset + len(rows) < store.count_batch_items(batch_id) else None
        items = [
            _Obj(
                sequence_index=row["sequence_index"],
                episode_uuid=row["episode_uuid"],
                status=row["status"],
                type=row["type"],
                data=row["data"],
                metadata=_loads(row["metadata_json"], {}) or {},
                error=row["error"],
            )
            for row in rows
        ]
        return _Obj(items=items, next_cursor=next_cursor)

    def _batch_summary(self, row: dict[str, Any]) -> _Obj:
        total = int(row.get("total_items") or 0)
        succeeded = int(row.get("succeeded_items") or 0)
        failed = int(row.get("failed_items") or 0)
        percent = 100.0 if total == 0 else round(min(succeeded + failed, total) / total * 100, 2)
        return _Obj(
            batch_id=row["batch_id"],
            status=row.get("status"),
            metadata=_loads(row.get("metadata_json"), {}) or {},
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            progress=_Obj(
                total_items=total,
                succeeded_items=succeeded,
                failed_items=failed,
                percent_complete=percent,
            ),
        )

    # -- 内部处理 ----------------------------------------------------------
    def run_batch(self, batch_id: str) -> None:
        store = self._client._store
        row = store.get_batch(batch_id)
        if not row:
            return
        metadata = _loads(row["metadata_json"], {}) or {}
        graph_id = metadata.get("graph_id")
        items = store.list_batch_items(batch_id, 1_000_000, 0)
        store.update_batch_progress(batch_id, 0, 0)
        succeeded = 0
        failed = 0
        for item in items:
            try:
                if not item.get("episode_uuid"):
                    raise RuntimeError("episode missing")
                self._client._extractor.process_episode(
                    graph_id, item["episode_uuid"], item.get("data") or ""
                )
                store.update_batch_item_status(batch_id, item["sequence_index"], "succeeded")
                succeeded += 1
            except Exception as error:  # noqa: BLE001 - 单项失败不影响整批
                store.update_batch_item_status(
                    batch_id, item["sequence_index"], "failed", str(error)[:500]
                )
                failed += 1
            store.update_batch_progress(batch_id, succeeded, failed)

        store.update_batch_status(batch_id, "succeeded" if failed == 0 else "partial")


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------


class LocalZepClient:
    """自建（本地）Zep 客户端，接口形态对齐 zep_cloud.Zep。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._store = _Store(db_path)
        self._extractor = _Extractor(self._store)
        self._searcher = _Searcher(self._store)
        self._pool = ThreadPoolExecutor(max_workers=_EXTRACT_WORKERS, thread_name_prefix="local-zep")
        self.graph = _GraphAPI(self)
        self.batch = _BatchAPI(self)

    # -- 线程池任务 --------------------------------------------------------
    def submit_episode(self, graph_id: str, episode_uuid: str, content: str) -> None:
        self._pool.submit(self._safe_process, graph_id, episode_uuid, content)

    def _safe_process(self, graph_id: str, episode_uuid: str, content: str) -> None:
        try:
            self._extractor.process_episode(graph_id, episode_uuid, content)
        except Exception:  # noqa: BLE001 - 抽取失败不应打断主流程
            self._store.mark_episode_processed(episode_uuid)

    def submit_batch(self, batch_id: str) -> None:
        self._pool.submit(self.batch.run_batch, batch_id)

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)


_LOCAL_CLIENT_LOCK = threading.Lock()
_LOCAL_CLIENT: LocalZepClient | None = None


def get_local_zep_client() -> LocalZepClient:
    """返回进程内共享的本地客户端（数据库路径取自 Config）。"""

    global _LOCAL_CLIENT
    with _LOCAL_CLIENT_LOCK:
        if _LOCAL_CLIENT is None:
            _LOCAL_CLIENT = LocalZepClient(Config.ZEP_LOCAL_DB_PATH)
        return _LOCAL_CLIENT


def clear_local_zep_client() -> None:
    global _LOCAL_CLIENT
    with _LOCAL_CLIENT_LOCK:
        if _LOCAL_CLIENT is not None:
            _LOCAL_CLIENT.close()
        _LOCAL_CLIENT = None
