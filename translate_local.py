#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""《英语学报》本地翻译管线（hy-mt2 · 2026-09-28 起试用）

流程：拉线上 latest.json（云端 06:00 抓好的英文新文）→ hy-mt2 逐条翻标题+摘要
→ 校验/清洗 → 推回 xuebao-feeds 仓库 → 用户端 09:00 刷新即见中文。

运行方式：Windows 任务计划程序每日 06:30 触发 run_translate_local.bat
前提：本机 Ollama（模型库 F:/ollama/models，模型 hy-mt2:latest）；
      服务未运行时脚本自动拉起并等待就绪。
术语：内置 GLOSSARY 军事缩写表（与云端 DeepSeek 同配置，保证对比公平）；
      用户大术语库暂不接入（2026-09-28 拍板：先裸翻观察一周再定）。

日志：logs/translate_local_YYYYMMDD.log（每条译文留档，供观察期错译复盘）
"""
import base64
import ipaddress
import json
import os
import pathlib
import re
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parent
REPO = "ccxxxx-create/xuebao-feeds"
# 公网拉取端点白名单：模块常量、https、域名逐一显式列出
FEEDS_URLS = [
    "https://raw.githubusercontent.com/ccxxxx-create/xuebao-feeds/main/feeds/latest.json",
    "https://cdn.jsdelivr.net/gh/ccxxxx-create/xuebao-feeds@main/feeds/latest.json",
]
PUBLIC_HOSTS = {"raw.githubusercontent.com", "cdn.jsdelivr.net"}
# 本机 Ollama 固定回环端点：常量+断言，拒绝任何改写（唯一允许的私有地址）
OLLAMA = r"C:/Users/ASUS/AppData/Local/Programs/Ollama/ollama.exe"
OLLAMA_MODELS = "F:/ollama/models"
OLLAMA_CHAT_URL = "http://127.0.0.1:11434/v1/chat/completions"
OLLAMA_TAGS_URL = "http://127.0.0.1:11434/api/tags"
MODEL = "hy-mt2:latest"
TITLE_BATCH_DELAY = 0.5     # 逐条间隔（秒）
LEN_RATIO = (0.05, 4.0)     # 译文/原文长度比（标题短，下限放宽）
MAX_ITEMS = 200             # 单次运行上限护栏
KEEP_ALIVE = "10m"          # 模型驻留：一批增量翻完自动卸载

# 内置军事缩写术语表（与 translate_feeds.py 的 GLOSSARY 保持同步；新增两边一起加）
GLOSSARY = [
    ("NORAD", "北美防空司令部"), ("NATO", "北约"), ("AUKUS", "奥库斯联盟"),
    ("Pentagon", "五角大楼"), ("INDOPACOM", "美军印太司令部"), ("CENTCOM", "美军中央司令部"),
    ("EUCOM", "美军欧洲司令部"), ("NORTHCOM", "美军北方司令部"), ("STRATCOM", "美军战略司令部"),
    ("SOCOM", "美军特种作战司令部"), ("DARPA", "美国国防高级研究计划局"),
    ("DIU", "国防创新单元"), ("DVIDS", "国防视觉信息分发服务"), ("NDAA", "国防授权法案"),
    ("Carrier Strike Group", "航母打击群"), ("Amphibious Ready Group", "两栖戒备群"),
    ("Marine Expeditionary Unit", "海军陆战队远征分队"), ("Air Tasking Order", "空中任务指令"),
    ("freedom of navigation operation", "航行自由行动"), ("freedom of navigation", "航行自由"),
    ("anti-access/area denial", "反介入/区域拒止"), ("counterinsurgency", "反叛乱"),
    ("deterrence", "威慑"), ("readiness", "战备"), ("sortie", "架次"),
    ("flight deck", "飞行甲板"), ("rules of engagement", "交战规则"),
    ("ballistic missile", "弹道导弹"), ("cruise missile", "巡航导弹"),
    ("hypersonic", "高超声速"), ("electronic warfare", "电子战"),
    ("joint exercise", "联合演习"),
]
GLOSSARY_LINE = "；".join("%s=%s" % (en, zh) for en, zh in GLOSSARY)

LOG_DIR = ROOT / "logs"


def check_public_url(url):
    """出站校验：仅 https + 白名单域名 + DNS 解析不得落在私有/保留网段。"""
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme != "https":
        raise ValueError("scheme not allowed: %s" % p.scheme)
    host = (p.hostname or "").strip().lower().rstrip(".")
    if host not in PUBLIC_HOSTS:
        raise ValueError("host not in allowlist: %s" % host)
    for info in socket.getaddrinfo(host, None):
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("host resolves to reserved address: %s" % ip)
    return host


def _assert_loopback(url):
    """本机 Ollama 端点断言：必须恰为 127.0.0.1:11434 回环地址（防改写）。"""
    p = urllib.parse.urlsplit(str(url or ""))
    ip = ipaddress.ip_address((p.hostname or "").strip("[]"))
    if p.scheme != "http" or ip != ipaddress.ip_address("127.0.0.1") or p.port != 11434:
        raise ValueError("only the fixed local ollama endpoint is allowed")
    return url


def log(msg):
    line = "[%s] %s" % (datetime.now().strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    try:
        LOG_DIR.mkdir(exist_ok=True)
        with open(LOG_DIR / ("translate_local_%s.log" % datetime.now().strftime("%Y%m%d")), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def http_get_json(url, timeout=60):
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "xuebao-local/1.0"}),
        timeout=timeout,
    ) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_latest():
    """按优先级拉线上 latest.json，取第一个成功的。每个 URL 过白名单校验。"""
    last = None
    for u in FEEDS_URLS:
        try:
            check_public_url(u)
            d = http_get_json(u)
            if isinstance(d.get("items"), list):
                return d
        except Exception as e:  # noqa: BLE001
            last = e
    raise RuntimeError("all feeds endpoints failed: %s" % last)


def ollama_ready():
    try:
        _assert_loopback(OLLAMA_TAGS_URL)
        with urllib.request.urlopen(OLLAMA_TAGS_URL, timeout=5) as r:
            names = [m.get("name", "") for m in json.loads(r.read().decode("utf-8")).get("models", [])]
            return MODEL.split(":")[0] in [n.split(":")[0] for n in names]
    except Exception:  # noqa: BLE001
        return False


def ensure_ollama():
    if ollama_ready():
        return
    log("ollama not ready, starting serve ...")
    env = dict(os.environ, OLLAMA_MODELS=OLLAMA_MODELS)
    subprocess.Popen([OLLAMA, "serve"], env=env,
                     stdout=open("F:/AI/_ollama_serve.log", "ab"),
                     stderr=subprocess.STDOUT)
    for _ in range(30):
        time.sleep(2)
        if ollama_ready():
            log("ollama ready")
            return
    raise RuntimeError("ollama failed to become ready in 60s")


def chat_hy(prompt, num_predict=1024, timeout=240):
    """调 hy-mt2（固定回环端点，请求前断言）。"""
    _assert_loopback(OLLAMA_CHAT_URL)
    body = json.dumps({
        "model": MODEL, "messages": [{"role": "user", "content": prompt}],
        "stream": False, "temperature": 0.2, "keep_alive": KEEP_ALIVE,
        "options": {"num_predict": num_predict},
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(OLLAMA_CHAT_URL, data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode("utf-8"))
    return (d.get("choices") or [{}])[0].get("message", {}).get("content") or ""


def clean_zh(s):
    """清洗模型输出残留：'第一行：' 前缀、包裹引号、编号头。"""
    s = str(s or "").strip()
    s = re.sub(r"^(?:第一行|标题)\s*[：:]\s*", "", s)
    s = re.sub(r"^(?:摘要|第二行)\s*[：:]\s*", "", s)
    s = re.sub(r"^[\[［(（]?\d{1,2}[\]］)）]?\s*[：:.、]?\s*", "", s)
    return s.strip().strip('"“”「」').strip()


def ratio_ok(en, zh):
    if not zh:
        return False
    r = len(zh) / max(len(en), 1)
    return LEN_RATIO[0] <= r <= LEN_RATIO[1]


def parse_single(raw):
    """解析单条输出：优先 JSON 单对象 {"t":...,"s":...}，失败降级两行文本。"""
    m = re.search(r"\{[\s\S]*\}", str(raw or ""))
    if m:
        try:
            obj = json.loads(m.group(0))
            t, s = clean_zh(obj.get("t")), clean_zh(obj.get("s"))
            if t and s:
                return t, s
        except Exception:  # noqa: BLE001
            pass
    lines = [ln.strip() for ln in str(raw or "").splitlines() if ln.strip()]
    if len(lines) >= 2:
        return clean_zh(lines[0]), clean_zh(" ".join(lines[1:]))
    if len(lines) == 1:
        return clean_zh(lines[0]), ""
    return "", ""


def translate_one(title, summary):
    """单条翻译（语境式，2026-09-28 用户思路实测）：摘要先翻建立机构/缩写语境，标题最后翻。
    根因：hy-mt2 对超短标题的军事缩写无上下文会乱抓近似词（CNRC/NAVFAC→"北美防空司令部"），
    摘要先行后难点条目 3/3 全对。JSON 单对象 {"s","t"} 优先、两行文本兜底。"""
    prompt = (
        "Translate this English military news into Simplified Chinese. The full summary is given "
        "for context — read it first, translate the summary, and translate the title LAST using "
        "that context (institution abbreviations must follow military conventions).\n"
        "Military terms glossary (must follow): %s\n"
        "Output JSON object only: {\"s\":\"摘要中文\",\"t\":\"标题中文\"} — no explanations.\n\n"
        "Title: %s\nSummary: %s" % (GLOSSARY_LINE, title, summary)
    )
    raw = chat_hy(prompt)
    t, s = parse_single(raw)
    if t and not ratio_ok(title, t):
        t = ""
    if s and not ratio_ok(summary, s):
        s = ""
    return t, s, raw


def gh_api(args, inp=None):
    r = subprocess.run(["gh"] + args, input=inp, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=90)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "")[:300])
    return r.stdout.strip()


def push_latest(data):
    """Contents API 推回 latest.json（带远端 sha；冲突时报错由下次运行重试）。"""
    path = "feeds/latest.json"
    old = gh_api(["api", "repos/%s/contents/%s" % (REPO, path), "--jq", ".sha"])
    payload = {
        "message": "translate: local hy-mt2 titles+summary (%s)" % datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "content": base64.b64encode(json.dumps(data, ensure_ascii=False, indent=1).encode("utf-8")).decode(),
    }
    if old:
        payload["sha"] = old
    out = gh_api(["api", "--method", "PUT", "repos/%s/contents/%s" % (REPO, path), "--input", "-"],
                 json.dumps(payload))
    return json.loads(out)["commit"]["sha"][:7]


def main():
    t0 = time.time()
    log("=== local hy translate start ===")
    data = fetch_latest()
    items = data.get("items") or []
    todo = [it for it in items
            if not (it.get("titleZh") or "").strip() and (it.get("title") or "").strip()]
    log("items=%d pending=%d" % (len(items), len(todo)))
    if not todo:
        log("nothing to translate, exit")
        return 0
    ensure_ollama()
    todo = todo[:MAX_ITEMS]
    ok = fail = 0
    for i, it in enumerate(todo):
        title = (it.get("title") or "").strip()
        summary = (it.get("summary") or "").strip()[:600]
        try:
            t, s, raw = translate_one(title, summary)
            if t:
                it["titleZh"] = t
                it["summaryZh"] = s or it.get("summaryZh", "")
                ok += 1
                log("[%d/%d] OK %s => %s" % (i + 1, len(todo), title[:40], t[:40]))
            else:
                fail += 1
                log("[%d/%d] FAIL %s | raw: %s" % (i + 1, len(todo), title[:40], re.sub(r"\s+", " ", raw)[:100]))
        except Exception as e:  # noqa: BLE001 单条失败不拖垮
            fail += 1
            log("[%d/%d] ERROR %s: %s" % (i + 1, len(todo), title[:40], e))
        time.sleep(TITLE_BATCH_DELAY)
    log("translate done: ok=%d fail=%d in %.0fs" % (ok, fail, time.time() - t0))
    if ok:
        sha = push_latest(data)
        log("pushed to %s -> %s" % (REPO, sha))
    log("=== done in %.0fs ===" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
