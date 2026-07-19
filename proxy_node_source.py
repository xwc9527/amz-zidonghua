"""
proxy_node_source.py — 活动订阅定位、节点读取、IPRoyal 排除、指纹计算。

Clash 控制 API（9097）仅作可选信息源，不参与关键路径。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from typing import Any

import yaml

from config import (
    CLASH_PROFILES_DIR,
    CLASH_PROFILES_META,
    PROXY_CDN_SERVERS,
    PROXY_IPROYAL_MARKERS,
    PROXY_MAX_NODES,
    PROXY_SKIP_PROTOCOLS,
)

log = logging.getLogger("proxy_node_source")

_METADATA_NAME_RE = re.compile(
    r"(剩余流量|套餐到期|到期时间|距离下次重置|官网|流量重置|更新订阅|"
    r"^PASS$|^REJECT|^DIRECT$|^COMPATIBLE$|^GLOBAL$)",
    re.IGNORECASE,
)
_METADATA_TYPES = frozenset({
    "selector", "urltest", "loadbalance", "fallback", "relay", "direct", "reject",
})


@dataclass
class ProfileFingerprint:
    uid: str
    path: str
    sha256: str
    updated_at: str
    name: str = ""
    entry_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NodeLoadStats:
    subscription_entries: int = 0
    metadata_excluded: int = 0
    iproyal_excluded: int = 0
    protocol_excluded: int = 0
    cdn_excluded: int = 0
    candidates: int = 0
    reasons: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NodeLoadResult:
    ok: bool
    fingerprint: ProfileFingerprint | None
    nodes: list[dict]
    stats: NodeLoadStats
    error: str = ""
    error_code: str = ""

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "fingerprint": self.fingerprint.to_dict() if self.fingerprint else None,
            "nodes": [{"name": n.get("name"), "type": n.get("type"), "server": n.get("server")} for n in self.nodes],
            "candidate_count": len(self.nodes),
            "stats": self.stats.to_dict(),
            "error": self.error,
            "error_code": self.error_code,
        }


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _looks_iproyal(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (dict, list)):
        blob = json.dumps(value, ensure_ascii=False).lower()
    else:
        blob = str(value).lower()
    return any(marker in blob for marker in PROXY_IPROYAL_MARKERS)


def is_iproyal_node(node: dict) -> bool:
    """任意字段命中 IPRoyal 标记即排除。"""
    if not isinstance(node, dict):
        return False
    for key in ("name", "server", "provider", "source", "remark", "remarks", "extra"):
        if key in node and _looks_iproyal(node.get(key)):
            return True
    return _looks_iproyal(node)


def is_metadata_node(node: dict) -> bool:
    name = str(node.get("name") or "").strip()
    ntype = str(node.get("type") or "").strip().lower()
    if ntype in _METADATA_TYPES:
        return True
    if not name:
        return True
    if _METADATA_NAME_RE.search(name):
        return True
    return False


def resolve_active_profile(
    profiles_meta: str | None = None,
    profiles_dir: str | None = None,
) -> ProfileFingerprint:
    meta_path = profiles_meta or CLASH_PROFILES_META
    dir_path = profiles_dir or CLASH_PROFILES_DIR
    if not meta_path or not os.path.isfile(meta_path):
        raise FileNotFoundError(f"profiles.yaml 不存在: {meta_path}")
    if not dir_path or not os.path.isdir(dir_path):
        raise FileNotFoundError(f"profiles 目录不存在: {dir_path}")

    with open(meta_path, encoding="utf-8") as f:
        meta = yaml.safe_load(f) or {}
    uid = meta.get("current")
    if not uid:
        raise RuntimeError("profiles.yaml 缺少 current 字段")

    items = meta.get("items") or []
    item = next((x for x in items if isinstance(x, dict) and x.get("uid") == uid), None)
    file_name = (item or {}).get("file") or f"{uid}.yaml"
    path = os.path.join(dir_path, file_name)
    if not os.path.isfile(path):
        alt = os.path.join(dir_path, f"{uid}.yaml")
        if os.path.isfile(alt):
            path = alt
        else:
            raise FileNotFoundError(f"当前活动订阅文件不存在: {path}")

    updated = ""
    if item and item.get("updated") is not None:
        updated = str(item.get("updated"))
    else:
        updated = str(int(os.path.getmtime(path)))

    return ProfileFingerprint(
        uid=str(uid),
        path=path,
        sha256=_sha256_file(path),
        updated_at=updated,
        name=str((item or {}).get("name") or ""),
    )


def load_candidate_nodes(
    max_n: int | None = None,
    profiles_meta: str | None = None,
    profiles_dir: str | None = None,
) -> NodeLoadResult:
    """
    只读取当前活动机场订阅，排除元数据 / IPRoyal / 无效协议 / CDN 共享出口。
    """
    stats = NodeLoadStats()
    limit = PROXY_MAX_NODES if max_n is None else max_n
    try:
        fp = resolve_active_profile(profiles_meta, profiles_dir)
    except Exception as e:
        return NodeLoadResult(
            ok=False, fingerprint=None, nodes=[], stats=stats,
            error=str(e), error_code="PROFILE_RESOLVE_FAILED",
        )

    try:
        with open(fp.path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        return NodeLoadResult(
            ok=False, fingerprint=fp, nodes=[], stats=stats,
            error=f"订阅文件解析失败: {e}", error_code="PROFILE_PARSE_FAILED",
        )

    proxies = data.get("proxies")
    if not isinstance(proxies, list):
        return NodeLoadResult(
            ok=False, fingerprint=fp, nodes=[], stats=stats,
            error="订阅缺少 proxies 列表", error_code="PROFILE_SCHEMA_INVALID",
        )

    stats.subscription_entries = len(proxies)
    fp.entry_count = len(proxies)
    nodes: list[dict] = []

    for raw in proxies:
        if not isinstance(raw, dict):
            stats.metadata_excluded += 1
            stats.reasons["non_dict"] = stats.reasons.get("non_dict", 0) + 1
            continue
        if is_metadata_node(raw):
            stats.metadata_excluded += 1
            stats.reasons["metadata"] = stats.reasons.get("metadata", 0) + 1
            continue
        if is_iproyal_node(raw):
            stats.iproyal_excluded += 1
            stats.reasons["iproyal"] = stats.reasons.get("iproyal", 0) + 1
            log.info("[source] 排除 IPRoyal 节点: %s", raw.get("name"))
            continue
        ntype = str(raw.get("type") or "").lower()
        if ntype in PROXY_SKIP_PROTOCOLS:
            stats.protocol_excluded += 1
            stats.reasons[f"proto:{ntype}"] = stats.reasons.get(f"proto:{ntype}", 0) + 1
            continue
        server = str(raw.get("server") or "").strip().lower()
        if server in PROXY_CDN_SERVERS:
            stats.cdn_excluded += 1
            stats.reasons["cdn"] = stats.reasons.get("cdn", 0) + 1
            continue
        nodes.append(dict(raw))
        if limit > 0 and len(nodes) >= limit:
            break

    stats.candidates = len(nodes)
    if not nodes:
        return NodeLoadResult(
            ok=False, fingerprint=fp, nodes=[], stats=stats,
            error="过滤后无候选节点", error_code="NO_CANDIDATES",
        )
    return NodeLoadResult(ok=True, fingerprint=fp, nodes=nodes, stats=stats)

