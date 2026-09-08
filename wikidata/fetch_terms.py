# -*- coding: utf-8 -*-
"""Wikidata 军事术语抓取（EN↔ZH）——QLever 单端点版（v2，本地可直接运行）。

端点：https://qlever.dev/api/wikidata（弗莱堡大学 QLever 全量 Wikidata 镜像，
公开学术服务；官方 query.wikidata.org 处于故障限流期且本机不可达，见 docs/decisions.md D-005）。
流程：根类目解析（字面量+P279 约束）→ P279 子类树 BFS → 每类一条
"双语实例直取"查询（P31 + EN/ZH 标签 join，keyset 分页）→ terms JSON。
断点续抓：state.json（类级 done/entities/errors）。
合规：UA 标识、请求间隔 ≥0.5s、单类/总量上限、单类失败记录后继续。
出站安全：仅 https + 域名白名单 + 解析 IP 禁私网/环回（Mimosa SSRF 约束）。
用法：python wikidata/fetch_terms.py   （Actions 上也可用同一路径运行）
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

UA = "SENTRA-AI-TermsHarvester/2.0 (SENTRA journal project; terms ingestion)"
ENDPOINTS = ("https://qlever.dev/api/wikidata", "https://qlever.cs.uni-freiburg.de/api/wikidata")
ALLOWED_HOSTS = {"qlever.dev", "qlever.cs.uni-freiburg.de"}
PREFIXES = (
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
    "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
    "PREFIX wd: <http://www.wikidata.org/entity/>\n"
)
Q_PREFIX = "http://www.wikidata.org/entity/Q"

ROOT_LABELS = [
    "military unit", "weapon", "military equipment", "military organization",
    "military exercise", "military rank", "military operation", "military aircraft",
    "missile", "armored fighting vehicle", "warship", "military installation",
    "military technology",
]
# 已人工核验的补充根类目（QLever 单实体反查验证：Q728 = weapon）
EXTRA_ROOTS = {"weapon": ["Q728"]}
MAX_DEPTH = 4
MAX_CLASSES = 3000
MAX_PAGES_PER_CLASS = 3        # 每类最多 3 页 × 1000
SLEEP = 0.55


def check_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or host not in ALLOWED_HOSTS:
        raise ValueError(f"blocked outbound url: {url[:100]}")
    for _fam, _typ, _proto, _canon, sockaddr in socket.getaddrinfo(host, 443):
        ip = ipaddress.ip_address(sockaddr[0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError(f"blocked resolved private/reserved ip for {host}")


def get_json(url: str, retries: int = 3) -> dict:
    headers = {"User-Agent": UA, "Accept": "application/sparql-results+json", "Accept-Encoding": "identity"}
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            check_url(url)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.load(resp)
        except Exception as exc:
            last_exc = exc
            wait = 3 * (attempt + 1)
            print(f"  request failed ({exc}), retry in {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"GET failed: {url[:120]} ({last_exc})")


def sparql(query: str) -> list[dict]:
    url = ENDPOINTS[0] + "?query=" + urllib.parse.quote(PREFIXES + query)
    data = get_json(url)
    time.sleep(SLEEP)
    if data.get("status") == "ERROR" or "exception" in data:
        raise RuntimeError(f"SPARQL error: {str(data.get('exception'))[:160]}")
    return data["results"]["bindings"]


def qid_of(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def resolve_roots() -> dict[str, list[str]]:
    roots: dict[str, list[str]] = {}
    for label in ROOT_LABELS:
        qids: list[str] = list(EXTRA_ROOTS.get(label, []))
        try:
            rows = sparql(
                'SELECT DISTINCT ?c WHERE { ?c rdfs:label "%s"@en . ?c wdt:P279 ?sup . '
                'FILTER(STRSTARTS(STR(?c), "%s")) } LIMIT 6' % (label, Q_PREFIX)
            )
            for r in rows:
                q = qid_of(r["c"]["value"])
                if q not in qids:
                    qids.append(q)
        except Exception as exc:
            print(f"root {label} 解析失败: {exc}", flush=True)
        if qids:
            roots[label] = qids[:4]
            print(f"root {label} -> {'/'.join(roots[label])}", flush=True)
        else:
            print(f"root {label} -> 未解析到（如实记录）", flush=True)
        time.sleep(SLEEP)
    return roots


def load_state() -> dict:
    if STATE_PATH.is_file():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def bfs_classes(roots: dict[str, list[str]]) -> list[str]:
    """P279 直接子类 BFS（Q 实体过滤），深度/总量受限，state 续抓，单类失败不炸。"""
    state = load_state()
    visited: set[str] = set(state.get("done", []))
    errors: dict[str, str] = state.setdefault("errors", {})
    queue: list[tuple[str, int]] = [(q, 0) for qs in roots.values() for q in qs if q not in visited]
    order: list[str] = []
    while queue and len(visited) < MAX_CLASSES:
        qid, depth = queue.pop(0)
        if qid in visited:
            continue
        visited.add(qid)
        order.append(qid)
        if depth < MAX_DEPTH:
            try:
                rows = sparql(
                    'SELECT DISTINCT ?sub WHERE { ?sub wdt:P279 wd:%s . '
                    'FILTER(STRSTARTS(STR(?sub), "%s")) } LIMIT 2000' % (qid, Q_PREFIX)
                )
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


def fetch_class_terms(cls: str) -> list[dict]:
    """单类双语实例直取：P31 + EN/ZH 标签 join，keyset 分页。只收双语句。"""
    terms: list[dict] = []
    cursor = ""
    for _page in range(MAX_PAGES_PER_CLASS):
        gt = f'FILTER(STR(?item) > "{cursor}")' if cursor else ""
        rows = sparql(
            "SELECT ?item ?en ?zh WHERE {"
            " ?item wdt:P31 wd:%s ."
            " ?item rdfs:label ?en . FILTER(LANG(?en) = \"en\")"
            " ?item rdfs:label ?zh . FILTER(LANG(?zh) = \"zh\")"
            " %s } LIMIT 1000" % (cls, gt)
        )
        if not rows:
            break
        for r in rows:
            terms.append({
                "qid": qid_of(r["item"]["value"]),
                "en": r["en"]["value"],
                "zh": r["zh"]["value"],
                "en_aliases": [],
                "zh_variants": [],
            })
        cursor = rows[-1]["item"]["value"]
        if len(rows) < 1000:
            break
    return terms


def main() -> None:
    t0 = time.time()
    roots = resolve_roots()
    state = load_state()
    term_by_qid: dict[str, dict] = {t["qid"]: t for t in state.get("terms", [])}
    classes = bfs_classes(roots)
    print(f"classes to scan: {len(classes)}", flush=True)
    done: dict[str, bool] = state.setdefault("class_done", {})
    errors: dict[str, str] = state.setdefault("errors", {})
    for i, cls in enumerate(classes):
        if done.get(cls):
            continue
        try:
            got = fetch_class_terms(cls)
        except Exception as exc:
            errors[cls] = f"terms: {exc}"
            print(f"class {cls} failed, record & continue: {exc}", flush=True)
            got = []
        for t in got:
            term_by_qid.setdefault(t["qid"], t)
        done[cls] = True
        if (i + 1) % 25 == 0:
            print(f"{i + 1}/{len(classes)} classes, terms={len(term_by_qid)}", flush=True)
            state["terms"] = list(term_by_qid.values())
            state["class_done"] = done
            state["errors"] = errors
            save_state(state)
    state["terms"] = list(term_by_qid.values())
    state["class_done"] = done
    state["errors"] = errors
    save_state(state)

    terms = list(term_by_qid.values())
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ua": UA,
        "endpoint": ENDPOINTS[0],
        "roots": roots,
        "classes_visited": len(classes),
        "term_count": len(terms),
        "elapsed_min": round((time.time() - t0) / 60, 1),
        "note": "QLever 镜像直取双语标签；zh 繁简混杂如实收录，质量由三源投票+人工仲裁兜底（方案 §5.2）",
        "terms": terms,
    }
    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"DONE terms={len(terms)} in {out['elapsed_min']}min -> {OUT_PATH.name}", flush=True)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    main()
