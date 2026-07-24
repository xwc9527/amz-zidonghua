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
    # 多订阅汇总时的逐订阅明细（uid/name/entries/candidates/ok/error），供排障与
    # 前端展示"当前汇总了哪些机场"；单订阅场景下同样填充，保持信息一致。
    per_profile: list = field(default_factory=list)

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


# 订阅型 profile 的合法 type：显式 remote/local，或缺省（兼容手工精简配置/旧测试）。
# merge/script/rules/proxies/groups 等派生文件（Clash Verge 拆分导出的辅助文件）不在此列。
_SUBSCRIPTION_PROFILE_TYPES = frozenset({"", "remote", "local"})


def resolve_remote_profiles(
    profiles_meta: str | None = None,
    profiles_dir: str | None = None,
) -> list[ProfileFingerprint]:
    """列出 profiles.yaml 里所有"订阅型"配置（非 current 独占），作为多机场汇总
    与"嗅探新增订阅"的共同入口：新订阅只要出现在 items 里就会被发现，不需要
    用户手动把它切成 Clash 的"当前选中"。"""
    meta_path = profiles_meta or CLASH_PROFILES_META
    dir_path = profiles_dir or CLASH_PROFILES_DIR
    if not meta_path or not os.path.isfile(meta_path):
        raise FileNotFoundError(f"profiles.yaml 不存在: {meta_path}")
    if not dir_path or not os.path.isdir(dir_path):
        raise FileNotFoundError(f"profiles 目录不存在: {dir_path}")

    with open(meta_path, encoding="utf-8") as f:
        meta = yaml.safe_load(f) or {}
    items = meta.get("items") or []

    fingerprints: list[ProfileFingerprint] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ptype = str(item.get("type") or "").strip().lower()
        if ptype not in _SUBSCRIPTION_PROFILE_TYPES:
            continue
        uid = item.get("uid")
        if not uid:
            continue
        file_name = item.get("file") or f"{uid}.yaml"
        path = os.path.join(dir_path, file_name)
        if not os.path.isfile(path):
            log.warning("[source] 订阅文件缺失，跳过: uid=%s path=%s", uid, path)
            continue
        if item.get("updated") is not None:
            updated = str(item.get("updated"))
        else:
            try:
                updated = str(int(os.path.getmtime(path)))
            except OSError:
                updated = ""
        try:
            sha = _sha256_file(path)
        except OSError as e:
            log.warning("[source] 订阅文件读取失败，跳过: uid=%s error=%s", uid, e)
            continue
        fingerprints.append(ProfileFingerprint(
            uid=str(uid), path=path, sha256=sha, updated_at=updated,
            name=str(item.get("name") or ""),
        ))
    return fingerprints


def _combine_fingerprints(fps: list[ProfileFingerprint]) -> ProfileFingerprint:
    """把多个订阅的指纹合成一个：任意一个订阅内容变化，或新增/移除订阅，
    合成后的 sha256 都会变化，从而复用现有的"指纹变化 -> 重新计算候选节点差量"
    逻辑（daemon._maybe_reload_subscription），不需要额外改动守护进程。"""
    if len(fps) == 1:
        return fps[0]

    def _as_int(v: str) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    ordered = sorted(fps, key=lambda f: f.uid)
    sha_src = "|".join(f"{f.uid}:{f.sha256}" for f in ordered)
    combined_sha = hashlib.sha256(sha_src.encode("utf-8")).hexdigest()
    return ProfileFingerprint(
        uid="+".join(f.uid for f in ordered),
        path="; ".join(f.path for f in ordered),
        sha256=combined_sha,
        updated_at=str(max((_as_int(f.updated_at) for f in ordered), default=0)),
        name=" + ".join((f.name or f.uid) for f in ordered),
        entry_count=sum(f.entry_count for f in ordered),
    )


def _parse_profile_nodes(fp: ProfileFingerprint) -> tuple[list[dict], NodeLoadStats, str]:
    """解析单个订阅文件：排除元数据 / IPRoyal / 无效协议 / CDN 共享出口。
    返回 (存活节点, 该订阅自身统计, 错误信息；非空表示整个订阅被跳过)。
    存活节点会打上 _profile_uid/_profile_name（下划线前缀，Mihomo 配置生成时
    会自动剔除，见 proxy_runtime.build_mihomo_config），供排障与去重溯源。"""
    stats = NodeLoadStats()
    try:
        with open(fp.path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        return [], stats, f"订阅文件解析失败: {e}"

    proxies = data.get("proxies")
    if not isinstance(proxies, list):
        return [], stats, "订阅缺少 proxies 列表"

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
        node = dict(raw)
        node["_profile_uid"] = fp.uid
        node["_profile_name"] = fp.name or fp.uid
        nodes.append(node)

    stats.candidates = len(nodes)
    return nodes, stats, ""


def load_candidate_nodes(
    max_n: int | None = None,
    profiles_meta: str | None = None,
    profiles_dir: str | None = None,
) -> NodeLoadResult:
    """
    汇总 profiles.yaml 里所有订阅型配置的节点（不再局限于 Clash 的"当前选中"），
    排除元数据 / IPRoyal / 无效协议 / CDN 共享出口后合并为统一候选池。

    新增一个机场订阅只需让它出现在 profiles.yaml 的 items 里（Clash Verge 添加
    订阅链接后自动写入，不需要手动切换"当前选中"）；调用方（常驻守护进程）按
    既有的订阅轮询周期重新调用本函数即可自动发现并汇入，这就是"嗅探新增订阅"
    的落地方式——不需要额外起轮询线程或手动按钮。
    """
    limit = PROXY_MAX_NODES if max_n is None else max_n
    try:
        profile_fps = resolve_remote_profiles(profiles_meta, profiles_dir)
    except Exception as e:
        return NodeLoadResult(
            ok=False, fingerprint=None, nodes=[], stats=NodeLoadStats(),
            error=str(e), error_code="PROFILE_RESOLVE_FAILED",
        )
    if not profile_fps:
        return NodeLoadResult(
            ok=False, fingerprint=None, nodes=[], stats=NodeLoadStats(),
            error="profiles.yaml 中未找到任何订阅型配置", error_code="NO_PROFILES",
        )

    total_stats = NodeLoadStats()
    nodes_by_profile: list[list[dict]] = []
    ok_fps: list[ProfileFingerprint] = []
    for fp in profile_fps:
        nodes, stats, err = _parse_profile_nodes(fp)
        if err:
            log.warning("[source] 订阅 %s(%s) 跳过: %s", fp.name or fp.uid, fp.uid, err)
            total_stats.per_profile.append({
                "uid": fp.uid, "name": fp.name, "ok": False, "error": err,
            })
            continue
        ok_fps.append(fp)
        nodes_by_profile.append(nodes)
        total_stats.subscription_entries += stats.subscription_entries
        total_stats.metadata_excluded += stats.metadata_excluded
        total_stats.iproyal_excluded += stats.iproyal_excluded
        total_stats.protocol_excluded += stats.protocol_excluded
        total_stats.cdn_excluded += stats.cdn_excluded
        for k, v in stats.reasons.items():
            total_stats.reasons[k] = total_stats.reasons.get(k, 0) + v
        total_stats.per_profile.append({
            "uid": fp.uid, "name": fp.name, "ok": True,
            "entries": stats.subscription_entries, "candidates": stats.candidates,
        })

    if not ok_fps:
        return NodeLoadResult(
            ok=False, fingerprint=None, nodes=[], stats=total_stats,
            error="全部订阅解析失败", error_code="ALL_PROFILES_FAILED",
        )

    # 始终按订阅轮流取（round-robin）合并，而非新订阅整体排在旧订阅之后：
    # 守护进程的探测本身是限速/节流的（尤其空闲模式），新增订阅若排在队尾，
    # 会在很长一段时间内一个节点都探测不到；轮转合并让新订阅从一开始就能
    # 和旧订阅公平地穿插进探测队列。有全局上限时同时提供"不被挤没"的效果。
    merged: list[dict] = []
    offsets = [0] * len(nodes_by_profile)
    while any(offsets[i] < len(nodes_by_profile[i]) for i in range(len(nodes_by_profile))):
        for i, nodes in enumerate(nodes_by_profile):
            if offsets[i] < len(nodes):
                merged.append(nodes[offsets[i]])
                offsets[i] += 1
                if limit > 0 and len(merged) >= limit:
                    break
        if limit > 0 and len(merged) >= limit:
            break

    total_stats.candidates = len(merged)
    fp_combined = _combine_fingerprints(ok_fps)
    if not merged:
        return NodeLoadResult(
            ok=False, fingerprint=fp_combined, nodes=[], stats=total_stats,
            error="过滤后无候选节点", error_code="NO_CANDIDATES",
        )
    return NodeLoadResult(ok=True, fingerprint=fp_combined, nodes=merged, stats=total_stats)

