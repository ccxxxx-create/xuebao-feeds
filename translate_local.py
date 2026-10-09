#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""《英语学报》本地翻译管线 v2 · 全文（hy-mt2 · 2026-09-28）

流程：拉线上 latest.json（云端 06:00 抓好的英文新文）→ 每篇文章按
「正文逐段 → 摘要 → 标题」顺序一次性翻完（语境逐级放大：正文段落带文章级语境，
摘要带正文首段译文，标题带摘要+正文首段译文）→ 推回仓库 → 用户 09:00 刷新即见。

存量回填（2026-09-28 拍板）：增量翻完后用剩余预算翻无译文的老文章，每晚自然补完。
护栏：单次运行 MAX_ARTICLES 篇 / MAX_PARAS 段；失败段留空（zhState=failed），下次运行只补空段。

运行方式：Windows 任务计划程序每日 06:30 触发 run_translate_local.bat
前提：本机 Ollama（模型库 F:/ollama/models，模型 hy-mt2:latest 7.5B）；
      服务未运行时脚本自动拉起并等待就绪。
术语：内置 GLOSSARY 军事缩写表；用户大术语库（RAG 词面/语义检索）待观察期后接入。
日志：logs/translate_local_YYYYMMDD.log
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# —— 翻译后端切换（2026-10-09 用户指令：当前用云端 deepseek-v4.1-flash，本机 hy-mt2 暂歇）——
# "opencode" = OpenCode Go 套餐端点（DeepSeek V4.1 Flash）；"local" = 本机 Ollama hy-mt2
TRANSLATE_BACKEND = "opencode"
OPENCODE_CHAT_URL = "https://opencode.ai/zen/go/v1/chat/completions"
OPENCODE_HOST = "opencode.ai"
OPENCODE_MODEL = "deepseek-v4.1-flash"
OPENCODE_KEY_FILE = pathlib.Path("C:/Users/ASUS/Desktop/收纳盒/api.txt")  # 运行时读取：key 不落盘、不回显、不进日志
OPENCODE_TIMEOUT = 90
OPENCODE_CONCURRENCY = 4    # 云端后端并发路数（无硬件压力；段序按 index 回填，段间接龙语境降级为仅标题+摘要）
PARA_DELAY = 0.2            # 段间间隔（秒）
LEN_RATIO = (0.05, 5.0)     # 译文/原文长度比
MAX_ARTICLES = 60           # 单次运行篇数护栏
MAX_PARAS = 1500            # 单次运行总段数护栏（约 45 分钟上限）
KEEP_ALIVE = "30m"          # 翻译期间模型常驻
PARA_SPLIT_RE = re.compile(r"\n{2,}")

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

# ---- 用户术语库接口（2026-09-28 拍板：留接口不落数据，数据到位即自动生效）----
# 约定：F:/AI/terms/glossary.csv（推荐，Excel「另存为 CSV UTF-8」即得）或 glossary.tsv，
#       两列（英文,中文），首行表头自动跳过，# 开头为注释行，UTF-8。分享/导出即标准表格文件。
# 翻译每条文本前做词面命中检索（1~4 词窗口查字典，无第三方依赖），命中 0~8 条注入该条提示词。
# 文件不存在/格式错 → 空表，管线回退内置 GLOSSARY，行为与现在完全一致。
USER_GLOSSARY_PATHS = [
    pathlib.Path("F:/AI/terms/glossary.csv"),
    pathlib.Path("F:/AI/terms/glossary.tsv"),
]
USER_GLOSSARY_MAX = 500000

_user_glossary_cache = {"loaded": False, "map": {}}


def load_user_glossary():
    if _user_glossary_cache["loaded"]:
        return _user_glossary_cache["map"]
    g = {}
    path = next((p for p in USER_GLOSSARY_PATHS if p.exists()), None)
    if path:
        try:
            raw = path.read_text(encoding="utf-8-sig", errors="ignore")  # utf-8-sig 吃掉 Excel 的 BOM
            sep = "\t" if path.suffix == ".tsv" else ","
            for ln in raw.splitlines():
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                parts = ln.split(sep, 1)
                if len(parts) < 2:
                    continue
                en, zh = parts[0].strip().lower(), parts[1].strip()
                if en in ("english", "en", "术语", "原文", "term"):  # 表头行
                    continue
                if en and zh and len(en) <= 80 and len(zh) <= 200:
                    g[en] = zh
                    if len(g) >= USER_GLOSSARY_MAX:
                        break
            log("user glossary loaded from %s: %d entries" % (path.name, len(g)))
        except OSError:
            pass  # 接口空载：无术语文件是常态
    _user_glossary_cache["loaded"] = True
    _user_glossary_cache["map"] = g
    return g


def glossary_hits(text, user_map, max_hits=8):
    """词面命中检索：英文按非字母数字切词后拼 1~4 词窗口查用户术语字典。"""
    if not user_map:
        return []
    words = re.split(r"[^A-Za-z0-9\-]+", str(text or ""))
    hits, seen = [], set()
    for i in range(len(words)):
        for n in (1, 2, 3, 4):
            if i + n > len(words):
                break
            phrase = " ".join(w for w in words[i:i + n]).strip(".-").lower()
            if phrase and phrase in user_map and phrase not in seen:
                seen.add(phrase)
                hits.append("%s=%s" % (phrase, user_map[phrase]))
                if len(hits) >= max_hits:
                    return hits
    return hits

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
    """按优先级拉线上 latest.json，取第一个成功的。每个 URL 过白名单校验。
    workflow 内运行时（XUEBAO_FEEDS_FILE 指向本地文件）直接读文件，不再出网。"""
    local = os.environ.get("XUEBAO_FEEDS_FILE", "").strip()
    if local:
        p = pathlib.Path(local)
        if not p.is_file():
            raise RuntimeError("XUEBAO_FEEDS_FILE not found: %s" % p)
        d = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(d.get("items"), list):
            return d
        raise RuntimeError("local feeds file has no items")
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


def chat_hy(prompt, num_predict=2048, timeout=300):
    """按 TRANSLATE_BACKEND 分发：云端 DeepSeek（opencodego）或本机 hy-mt2。"""
    if TRANSLATE_BACKEND == "opencode":
        return chat_opencode(prompt, max_tokens=num_predict, timeout=OPENCODE_TIMEOUT)
    return chat_hy_local(prompt, num_predict=num_predict, timeout=timeout)


def _assert_public_https(url, allow_host):
    """出站校验：仅 https + 指定域名 + DNS 解析不得落在私有/保留网段。"""
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme != "https":
        raise ValueError("scheme not allowed: %s" % p.scheme)
    host = (p.hostname or "").strip().lower().rstrip(".")
    if host != allow_host:
        raise ValueError("host not allowed: %s" % host)
    for info in socket.getaddrinfo(host, None):
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("host resolves to reserved address: %s" % ip)
    return host


_opencode_key_cache = {"loaded": False, "key": ""}


def _load_opencode_key():
    if _opencode_key_cache["loaded"]:
        return _opencode_key_cache["key"]
    key = os.environ.get("OPENCODEGO_API_KEY", "").strip()  # workflow 用 Secret 注入，优先
    if not key:
        try:
            for ln in OPENCODE_KEY_FILE.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
                ln = ln.strip()
                if ln.startswith("oc_sk_"):
                    key = ln
                    break
        except OSError:
            pass
    _opencode_key_cache["loaded"] = True
    _opencode_key_cache["key"] = key
    if not key:
        raise RuntimeError("opencode key not found (env OPENCODEGO_API_KEY empty and no oc_sk_ line in key file)")
    return key


_OPENCODE_SESSION = None


def chat_opencode(prompt, max_tokens=2048, timeout=OPENCODE_TIMEOUT):
    """云端 DeepSeek V4.1 Flash（OpenCode Go 套餐）：OpenAI 兼容 + 必需 x-opencode-session 头。"""
    global _OPENCODE_SESSION
    _assert_public_https(OPENCODE_CHAT_URL, OPENCODE_HOST)
    if _OPENCODE_SESSION is None:
        _OPENCODE_SESSION = "%s-%s" % (datetime.now().strftime("%Y%m%d"), os.urandom(8).hex())
    body = json.dumps({
        "model": OPENCODE_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False, "temperature": 0.2, "max_tokens": max_tokens,
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(OPENCODE_CHAT_URL, data=body, headers={
        "Authorization": "Bearer " + _load_opencode_key(),
        "Content-Type": "application/json",
        "User-Agent": "xuebao-local/1.0",
        "x-opencode-session": _OPENCODE_SESSION,
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode("utf-8"))
    return (d.get("choices") or [{}])[0].get("message", {}).get("content") or ""


def chat_hy_local(prompt, num_predict=2048, timeout=300):
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


def clean_zh(s, strip_num=True):
    """清洗模型输出残留：'译文/标题/摘要：' 前缀、包裹引号。
    strip_num 仅剥【带括号】的编号头（[1]/（2））——裸数字开头是日期/列表常态
    （实测 "10月10日" 被剥成 "月10日"、"2026年" 被剥成 "26年"），正文段一律不剥。"""
    s = str(s or "").strip()
    s = re.sub(r"^(?:第[一二三四五六七八九十]+行|译文|标题|摘要)\s*[：:]\s*", "", s)
    if strip_num:
        s = re.sub(r"^[\[［(（]\s*\d{1,2}\s*[\]］)）]\s*", "", s)
    return s.strip().strip('"“”「」').strip()


def ratio_ok(en, zh):
    if not zh:
        return False
    return LEN_RATIO[0] <= len(zh) / max(len(en), 1) <= LEN_RATIO[1]


def split_paras(body):
    """与 webapp paras()/云端 split_paras 完全一致：连续空行分段。"""
    return [p.strip() for p in PARA_SPLIT_RE.split(str(body or "")) if p.strip()]


def parse_field(raw, key, strip_num=True):
    """从模型输出提取指定字段：优先 JSON，失败取'key：值'行，
    最后兜底纯文本（实测 hy-mt2 偶发直接输出译文不含 JSON——译文本身是对的，不能丢）。"""
    raw = str(raw or "")
    m = re.search(r"\{[\s\S]*\}", raw)
    if m:
        try:
            obj = json.loads(m.group(0))
            v = clean_zh(obj.get(key) or "", strip_num)
            if v:
                return v
        except Exception:  # noqa: BLE001
            # JSON 损坏修复（2026-10-08 实测）：引语段含内层双引号时模型不转义，
            # json.loads 必失败——按 "key":" 截取、取最后一个 } 前的内容、还原转义。
            m2 = re.search(r'"?\s*%s\s*"?\s*:\s*"' % key, m.group(0))
            if m2:
                frag = m.group(0)[m2.end():]
                end = frag.rfind("}")
                if end != -1:
                    frag = frag[:end]
                frag = frag.replace('\\"', '"').replace("\\n", "\n").replace("\\t", " ")
                frag = frag.strip()
                if frag.endswith('"'):
                    frag = frag[:-1]
                v = clean_zh(frag, strip_num)
                if v:
                    return v
    for ln in raw.splitlines():
        m2 = re.match(r"^\s*[\"“]?\s*%s[\"”]?\s*[：:]\s*(.+)$" % key, ln.strip())
        if m2:
            v = clean_zh(m2.group(1), strip_num)
            if v:
                return v
    # 纯文本兜底：无任何结构标记时，整段输出即译文（p 取全文；t/s 取首行）
    if "{" not in raw and "}" not in raw:
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if lines:
            return clean_zh(raw.strip() if key == "p" else lines[0], strip_num)
    return ""


def translate_para(context, para, user_hits=None):
    """翻一个正文段（语境 = 文章标题+摘要 + 前一段译文 + 用户术语命中注入）。"""
    user_line = ("User glossary hits (must follow): " + "；".join(user_hits) + "\n") if user_hits else ""
    prompt = (
        "Translate one paragraph of an English military news article into Simplified Chinese.\n"
        "Article context (for reference only, do NOT translate): %s\n"
        "Previous paragraph translation (for coherence): %s\n"
        "Military terms glossary (must follow): %s\n"
        "%s"
        "Output JSON only: {\"p\":\"本段译文\"} — no explanations.\n\n"
        "Paragraph: %s" % (context["head"], context["prev"], GLOSSARY_LINE, user_line, para)
    )
    raw = chat_hy(prompt)
    return parse_field(raw, "p", strip_num=False)


def translate_summary(context):
    """翻摘要（语境 = 正文首段译文）。"""
    prompt = (
        "Translate this English news summary into Simplified Chinese, using the article's first "
        "translated paragraph as context.\n"
        "First paragraph (translated): %s\n"
        "Military terms glossary (must follow): %s\n"
        "Output JSON only: {\"s\":\"摘要中文\"} — no explanations.\n\n"
        "Summary: %s" % (context["first_zh"], GLOSSARY_LINE, context["summary"])
    )
    raw = chat_hy(prompt)
    return parse_field(raw, "s")


def translate_title(context):
    """翻标题（语境 = 摘要译文 + 正文首段译文，最后翻——语境最强）。"""
    prompt = (
        "Translate this English news title into Simplified Chinese. The summary and first paragraph "
        "translations are given as context — institution abbreviations must follow military conventions.\n"
        "Summary (translated): %s\n"
        "First paragraph (translated): %s\n"
        "Military terms glossary (must follow): %s\n"
        "Output JSON only: {\"t\":\"标题中文\"} — no explanations.\n\n"
        "Title: %s" % (context["sum_zh"], context["first_zh"], GLOSSARY_LINE, context["title"])
    )
    raw = chat_hy(prompt)
    return parse_field(raw, "t")


def translate_article(it, budget, user_map):
    """一篇完整翻译：正文逐段 → 摘要 → 标题（2026-09-28 用户拍板的语境放大顺序）。
    返回消耗的段数。已有译文的段自动跳过（失败重跑只补空段）。"""
    paras_en = split_paras(it.get("body"))
    if not paras_en:
        return 0
    prev_zh = it.get("zhParas") if isinstance(it.get("zhParas"), list) else []
    zh = [prev_zh[i] if i < len(prev_zh) and isinstance(prev_zh[i], str) and prev_zh[i].strip() else ""
          for i in range(len(paras_en))]
    context = {
        "head": ((it.get("title") or "")[:120] + " | " + (it.get("summary") or "")[:300]),
        "title": (it.get("title") or "").strip(),
        "summary": (it.get("summary") or "").strip()[:600],
        "prev": "",
    }
    # —— 1) 正文逐段 ——
    used = 0
    if TRANSLATE_BACKEND == "opencode":
        # 云端并发：按 index 提交、按 index 回填保段序；强模型用标题+摘要语境即可，
        # 段间接龙语境（prev）在并发下不可得，降级为空
        idx_todo = [i for i in range(min(len(paras_en), max(0, budget))) if not zh[i]]
        if idx_todo:
            with ThreadPoolExecutor(max_workers=OPENCODE_CONCURRENCY) as ex:
                futs = {ex.submit(translate_para, context, paras_en[i], glossary_hits(paras_en[i], user_map)): i
                        for i in idx_todo}
                for fu in as_completed(futs):
                    i = futs[fu]
                    try:
                        v = fu.result()
                        if v and ratio_ok(paras_en[i], v):
                            zh[i] = v
                        else:
                            log("    para %d ratio/parse fail" % (i + 1))
                    except Exception as e:  # noqa: BLE001 单段失败不拖垮整篇
                        log("    para %d ERROR: %s" % (i + 1, e))
                    used += 1
    else:
        for i, p in enumerate(paras_en):
            if used >= budget:
                break
            if zh[i]:
                context["prev"] = zh[i][:200]
                continue
            try:
                v = translate_para(context, p, glossary_hits(p, user_map))
                if v and ratio_ok(p, v):
                    zh[i] = v
                else:
                    log("    para %d ratio/parse fail" % (i + 1))
            except Exception as e:  # noqa: BLE001 单段失败不拖垮整篇
                log("    para %d ERROR: %s" % (i + 1, e))
            context["prev"] = zh[i][:200] if zh[i] else ""
            used += 1
            time.sleep(PARA_DELAY)
    context["first_zh"] = next((v for v in zh if v.strip()), "")
    # —— 2) 摘要（已有译文则跳过）——
    if context["summary"] and not (it.get("summaryZh") or "").strip():
        try:
            s = translate_summary(context)
            if s and ratio_ok(context["summary"], s):
                it["summaryZh"] = s
        except Exception as e:  # noqa: BLE001
            log("    summary ERROR: %s" % e)
    context["sum_zh"] = (it.get("summaryZh") or "").strip()
    # —— 3) 标题（最后翻，语境最强；已有/锁定不覆盖）——
    if context["title"] and not (it.get("titleZh") or "").strip() and not it.get("titleZhLocked"):
        try:
            t = translate_title(context)
            if t and ratio_ok(context["title"], t):
                it["titleZh"] = t
                it["titleTrans"] = "ok"
        except Exception as e:  # noqa: BLE001
            log("    title ERROR: %s" % e)
    it["zhParas"] = zh
    it["zhFull"] = "\n\n".join(zh)
    it["zhDone"] = sum(1 for v in zh if v.strip())
    it["zhChunks"] = len(paras_en)
    it["zhState"] = "ok" if all(v.strip() for v in zh) else "failed"
    return used


def gh_api(args, inp=None):
    r = subprocess.run(["gh"] + args, input=inp, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=90)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or "")[:300])
    return r.stdout.strip()


def push_latest(data):
    """Contents API 推回 latest.json（带远端 sha；冲突时报错由下次运行重试）。
    workflow 内运行时（XUEBAO_NO_PUSH=1）只写本地 feeds/latest.json，由 workflow 的 Commit 步统一提交。"""
    if os.environ.get("XUEBAO_NO_PUSH", "").strip() == "1":
        out = ROOT / "feeds" / "latest.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        log("no-push mode: wrote %s" % out)
        return "local"
    path = "feeds/latest.json"
    old = gh_api(["api", "repos/%s/contents/%s" % (REPO, path), "--jq", ".sha"])
    payload = {
        "message": "translate: local hy-mt2 full pipeline (%s)" % datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "content": base64.b64encode(json.dumps(data, ensure_ascii=False, indent=1).encode("utf-8")).decode(),
    }
    if old:
        payload["sha"] = old
    out = gh_api(["api", "--method", "PUT", "repos/%s/contents/%s" % (REPO, path), "--input", "-"],
                 json.dumps(payload))
    return json.loads(out)["commit"]["sha"][:7]


def main():
    t0 = time.time()
    log("=== local hy full translate start ===")
    data = fetch_latest()
    items = data.get("items") or []
    # 待翻：zhState != ok 且有正文（新文优先按 pubDate 降序，存量回填自然靠后）
    todo = [it for it in items if it.get("zhState") != "ok" and (it.get("body") or "").strip()]
    todo.sort(key=lambda x: x.get("pubDate") or "", reverse=True)
    log("items=%d pending(full)=%d" % (len(items), len(todo)))
    if not todo:
        log("nothing to translate, exit")
        return 0
    if TRANSLATE_BACKEND == "local":
        ensure_ollama()
    else:
        log("backend: opencode/%s" % OPENCODE_MODEL)
    user_map = load_user_glossary()
    ok_cnt = part_cnt = 0
    paras_total = 0
    for n, it in enumerate(todo[:MAX_ARTICLES]):
        if paras_total >= MAX_PARAS:
            log("para budget guard hit, %d articles deferred" % (len(todo) - n))
            break
        title = (it.get("title") or "")[:44]
        used = translate_article(it, MAX_PARAS - paras_total, user_map)
        paras_total += used
        state = it.get("zhState")
        if state == "ok":
            ok_cnt += 1
        else:
            part_cnt += 1
        log("[%d/%d] %s (%d paras used) %s" % (n + 1, min(len(todo), MAX_ARTICLES), state, used, title))
    log("translate done: ok=%d partial=%d paras=%d in %.0fs" % (ok_cnt, part_cnt, paras_total, time.time() - t0))
    if paras_total:
        sha = push_latest(data)
        log("pushed to %s -> %s" % (REPO, sha))
    log("=== done in %.0fs ===" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
