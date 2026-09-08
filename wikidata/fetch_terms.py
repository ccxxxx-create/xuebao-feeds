# -*- coding: utf-8 -*-
"""Wikidata 军事术语抓取（EN↔ZH）——独立于 feeds 抓取管线，勿混用输出目录。

只跑在 GitHub Actions 上（本机网络 wikidata.org 不可达）：
    python wikidata/fetch_terms.py
产出 wikidata/terms-enzh.json + wikidata/state.json（断点续抓），commit 回仓库。

流程：类目树 BFS（P279 直接子类）→ 每类收集实例（P31，keyset 分页）→
wbgetentities 批量取 en/zh 标签与别名 → 过滤留 EN+ZH 双语词条。
合规：UA 标识、请求间隔 ≥1s、单类/总量上限、失败重试后跳过并如实记录。
出站安全：仅 https + 域名白名单 + 解析 IP 禁私网/环回/链路本地（Mimosa SSRF 约束）。
"""
from __future__ import annotations

import ipaddress
import json
import socket
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATE_PATH = HERE / "state.json"
OUT_PATH = HERE / "terms-enzh.json"

UA = "SENTRA-AI-TermsHarvester/1.0 (SENTRA journal project; GitHub Actions run)"
SPARQL_EP = "https://query.wikidata.org/sparql?format=json&query="
API_EP = "https://www.wikidata.org/w/api.php?"
ALLOWED_HOSTS = {"query.wikidata.org", "www.wikidata.org"}

# 军事领域根类目（按英文标签在运行时解析 QID，不硬编码）。
# 注意：不收 "military person" 等个体人名类——数百万人名实例既非术语，
# 其 P31 全量查询必然撞 Wikidata 60s 超时（首跑失败原因）。
ROOT_LABELS = [
    "military unit", "weapon", "military equipment", "military organization",
    "military exercise", "military rank", "military operation", "military aircraft",
    "missile", "armored fighting vehicle", "warship", "military installation",
    "military technology",
]
MAX_DEPTH = 4                      # 类目树深度
MAX_CLASSES = 3000                 # 最多访问的类目数
MAX_INSTANCES_PER_CLASS = 3000     # 每类最多实例数（keyset 分页 3 页）
MAX_TOTAL_ENTITIES = 80000         # 实体总量上限（抓多少如实统计，宁缺毋滥造假）
BATCH = 50                         # wbgetentities 每批 ID 数
SLEEP = 1.05                       # 请求间隔（合规 ≤1 req/s）


def check_url(url: str) -> None:
    """出站校验：仅 https、域名白名单、解析 IP 非私网/环回/链路本地。"""
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or host not in ALLOWED_HOSTS:
        raise ValueError(f"blocked outbound url: {url[:100]}")
    for fam, _, _, _, sockaddr in socket.getaddrinfo(host, 443):
        ip = ipaddress.ip_address(sockaddr[0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError(f"blocked resolved private/reserved ip for {host}")


def get_json(url: str, retries: int = 2) -> dict:
    headers = {"User-Agent": UA, "Accept": "application/sparql-results+json" if "sparql" in url else "application/json"}
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            check_url(url)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.load(resp)
        except Exception as exc:  # 网络抖动重试，最终失败抛出
            last_exc = exc
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"GET failed: {url[:120]} ({last_exc})")


def sparql(query: str) -> list[dict]:
    data = get_json(SPARQL_EP + urllib.parse.quote(query))
    time.sleep(SLEEP)
    return data["results"]["bindings"]


def qid_of(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def resolve_roots() -> dict[str, str]:
    """英文标签 → QID（可能多义，取第一个命中，多义项由 P31 查询自然漏斗）。"""
    roots: dict[str, str] = {}
    for label in ROOT_LABELS:
        rows = sparql(f'SELECT ?c WHERE {{ ?c rdfs:label "{label}"@en }} LIMIT 8')
        if rows:
            roots[label] = qid_of(rows[0]["c"]["value"])
            print(f"root {label} -> {roots[label]}", flush=True)
        else:
            print(f"root {label} -> 未解析到（如实记录）", flush=True)
    return roots


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def bfs_classes(roots: dict[str, str]) -> list[str]:
    """P279 直接子类 BFS，深度与总量受限，已访问即跳过（state 可续）。

    单类子类查询失败（超时/限流）：跳过该类的子树扩展但保留已访问标记，不炸全程。
    """
    state = load_state()
    visited: set[str] = set(state.get("done", []))
    errors: dict[str, str] = state.setdefault("errors", {})
    queue: list[tuple[str, int]] = [(qid, 0) for qid in roots.values() if qid not in visited]
    order: list[str] = []
    while queue and len(visited) < MAX_CLASSES:
        qid, depth = queue.pop(0)
        if qid in visited:
            continue
        visited.add(qid)
        order.append(qid)
        if depth < MAX_DEPTH:
            try:
                rows = sparql(f"SELECT ?sub WHERE {{ ?sub wdt:P279 wd:{qid} }} LIMIT 2000")
            except Exception as exc:
                errors[qid] = f"bfs: {exc}"
                print(f"bfs {qid} failed, skip children: {exc}", flush=True)
                rows = []
            for r in rows:
                sub = qid_of(r["sub"]["value"])
                if sub not in visited:
                    queue.append((sub, depth + 1))
        if len(order) % 50 == 0:
            print(f"BFS {len(order)} classes…", flush=True)
            state["done"] = sorted(visited)
            state["errors"] = errors
            save_state(state)
    state["done"] = sorted(visited)
    state["errors"] = errors
    save_state(state)
    return order


def collect_instances(class_ids: list[str]) -> dict[str, list[str]]:
    """每类 P31 实例，keyset 分页（避免 ORDER BY 超时），断点续抓。

    超大类的单页查询可能 60s 超时：失败记入 errors 并继续，规模如实统计。
    """
    state = load_state()
    ents: dict[str, list] = state.setdefault("entities", {})
    done: dict[str, bool] = state.setdefault("class_done", {})
    errors: dict[str, str] = state.setdefault("errors", {})
    total = 0
    for idx, cls in enumerate(class_ids):
        if done.get(cls):
            continue
        got: list[str] = []
        cursor = ""
        for _page in range(MAX_INSTANCES_PER_CLASS // 1000):
            gt = f'FILTER(STR(?item) > "{cursor}")' if cursor else ""
            try:
                rows = sparql(
                    f"SELECT ?item WHERE {{ ?item wdt:P31 wd:{cls} . {gt} }} LIMIT 1000"
                )
            except Exception as exc:
                errors[cls] = f"instances: {exc}"
                print(f"instances {cls} page failed, give up this class: {exc}", flush=True)
                break
            if not rows:
                break
            got.extend(qid_of(r["item"]["value"]) for r in rows)
            cursor = rows[-1]["item"]["value"]
            if len(rows) < 1000:
                break
        if got:
            ents[cls] = sorted(set(got))
        done[cls] = True
        total += len(got)
        if (idx + 1) % 25 == 0:
            print(f"instances {idx + 1}/{len(class_ids)} classes, +{total} last batch", flush=True)
            save_state(state)
            if sum(len(v) for v in ents.values()) > MAX_TOTAL_ENTITIES * 2:
                print("实体池超预算，停止类目扫描", flush=True)
                break
    state["errors"] = errors
    save_state(state)
    return ents


def fetch_labels(entity_ids: list[str]) -> list[dict]:
    """wbgetentities 批量取 en/zh 标签+别名，留 EN+ZH 双语词条。"""
    state = load_state()
    fetched: int = state.get("labels_fetched", 0)
    terms: list[dict] = state.get("terms", [])
    seen: set[str] = {t["qid"] for t in terms}
    langs = "en|zh|zh-cn|zh-hans|zh-hant"
    for i in range(fetched, len(entity_ids), BATCH):
        batch = entity_ids[i : i + BATCH]
        url = (
            API_EP
            + urllib.parse.urlencode({
                "action": "wbgetentities", "ids": "|".join(batch),
                "props": "labels|aliases", "languages": langs,
                "format": "json",
            })
        )
        try:
            data = get_json(url)
        except Exception as exc:
            print(f"labels batch {i} failed, skip: {exc}", flush=True)
            data = {"entities": {}}
        time.sleep(SLEEP)
        for qid, ent in data.get("entities", {}).items():
            if qid in seen or ent.get("missing") is not None:
                continue
            labels = {k: v["value"] for k, v in ent.get("labels", {}).items()}
            en = labels.get("en")
            zh = labels.get("zh-cn") or labels.get("zh-hans") or labels.get("zh")
            if not en or not zh:
                continue
            aliases = ent.get("aliases", {})
            terms.append({
                "qid": qid,
                "en": en,
                "zh": zh,
                "en_aliases": sorted({a["value"] for a in aliases.get("en", [])})[:8],
                "zh_variants": sorted({
                    a["value"] for k in ("zh-cn", "zh-hans", "zh", "zh-hant")
                    for a in aliases.get(k, [])
                    if a["value"] != zh
                })[:8],
            })
            seen.add(qid)
        state["labels_fetched"] = i + len(batch)
        state["terms"] = terms
        if (i // BATCH) % 20 == 0:
            print(f"labels {i + len(batch)}/{len(entity_ids)}, terms={len(terms)}", flush=True)
            save_state(state)
    save_state(state)
    return terms


def main() -> None:
    t0 = time.time()
    roots = resolve_roots()
    classes = bfs_classes(roots)
    print(f"classes visited: {len(classes)}", flush=True)
    ents = collect_instances(classes)
    all_ids = sorted({q for lst in ents.values() for q in lst})[:MAX_TOTAL_ENTITIES]
    print(f"entities collected: {len(all_ids)}", flush=True)
    terms = fetch_labels(all_ids)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ua": UA,
        "roots": roots,
        "classes_visited": len(classes),
        "entities_seen": len(all_ids),
        "term_count": len(terms),
        "elapsed_min": round((time.time() - t0) / 60, 1),
        "note": "Wikidata zh 标签=条目名，军事译名质量中上，须经三源投票仲裁后方可进 L0（方案 §5.2）",
        "terms": terms,
    }
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"DONE terms={len(terms)} in {out['elapsed_min']}min -> {OUT_PATH.name}", flush=True)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    main()
