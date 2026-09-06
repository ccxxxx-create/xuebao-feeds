#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 2026-09-04 手动触发：推送本改动以恢复定时抓取（云端 IP 才能正常访问 rand 等反爬源）
"""《英语学报》信源镜像：抓取 6 个官方直连源 → feeds/latest.json

与个人工作台 arxiv-mirror 同模式：GitHub Actions 每天定时运行本脚本，
把各官方 RSS/Atom 的最新条目（含正文全文）汇总为一个 JSON 提交回仓库，
静态网页前端从 raw/jsdelivr 读取（绕过浏览器跨域）。

输出（feeds/latest.json）：
{
  "updatedAt": "ISO8601",
  "meta": { "<channel>": {"name":..., "status":"ok|error", "count":N, "error":null|"...", "fetchedAt":...} },
  "items": [ {"url","channel","channelName","title","author","pubDate","summary","body"} ]
}
"""
import ipaddress
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
import concurrent.futures as cf
from concurrent.futures import ThreadPoolExecutor

import feedparser
from bs4 import BeautifulSoup

UA = "xuebao-mirror/1.0 (official rss reader; personal archive only)"
# 正文页抓取用浏览器 UA：rand/af.mil 等源会拦截非浏览器 UA（实测浏览器 UA 即 200，爬虫 UA 403）
PAGE_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
THROTTLE = 2.0          # 频道间请求间隔（秒）
BODY_THROTTLE = 2.5     # 正文页请求间隔（秒）
MAX_PER_CHANNEL = 20    # 每频道每轮上限
LOOKBACK_DAYS = 7
BODY_MAX_CHARS = 200000    # 正文上限（按字符计：字母/空格/标点各算1）。实测正文多在3k-19k字，个别超长分析文可达数万字，4万不够，提到20万
KEEP_TOP = 280000       # 保留字段总上限（防止单文件过大）

# ---- 出站 URL 安全校验：仅 http/https，拒绝 localhost/环回/私有/保留地址（防 feed 内恶意/失效链接把请求打进内网）----
BAD_HOST_RE = re.compile(r"^(localhost|.*\.local|.*\.internal|.*\.localhost)$", re.I)


def check_url(url):
    """校验 URL 可否出站访问；不合法抛 ValueError。返回解析出的 host。"""
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme not in ("http", "https"):
        raise ValueError("scheme not allowed: %s" % p.scheme)
    host = (p.hostname or "").strip().lower().rstrip(".")
    if not host or BAD_HOST_RE.match(host):
        raise ValueError("host not allowed: %s" % host)
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception as e:  # noqa: BLE001 —— DNS 解析失败按原有错误路径处理
        raise ValueError("dns fail: %s" % e)
    for info in infos:
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("host resolves to reserved address: %s" % ip)
    return host


class _ValidatingRedirectHandler(urllib.request.HTTPRedirectHandler):
    """重定向每一跳都重新过 check_url（scheme/host/私网校验），防绕道内网。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        check_url(newurl)
        return urllib.request.HTTPRedirectHandler.redirect_request(self, req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_ValidatingRedirectHandler())


def http_get(url, timeout=30, retries=2, ua=None):
    last = None
    for i in range(retries + 1):
        try:
            check_url(url)  # 首跳校验（重定向各跳由 _ValidatingRedirectHandler 校验）
            req = urllib.request.Request(url, headers={"User-Agent": ua or UA})
            with _OPENER.open(req, timeout=timeout) as resp:
                raw = resp.read()
            ctype = resp.headers.get("Content-Type", "") or ""
            return raw.decode("utf-8", "ignore") if "utf-8" in ctype or not ctype else raw.decode("utf-8", "ignore")
        except Exception as e:  # noqa: BLE001
            last = e
            if i < retries:
                time.sleep(1.5 * (i + 1))
    raise last

CHANNELS = [
    {
        "id": "defensenews", "name": "Defense News",
        "feeds": ["https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"],
        "full": "page",
        "selectors": ["div.layout-section", "div.article-body"],
    },
    {
        "id": "airandspaceforces", "name": "Air & Space Forces",
        "feeds": ["https://www.airandspaceforces.com/feed/"],
        "full": "page",
        "selectors": ["article", "div.entry-content", "div.cms-content"],
    },
    {
        "id": "govuk_mod", "name": "英国国防部",
        "feeds": ["https://www.gov.uk/government/organisations/ministry-of-defence.atom"],
        "full": "page", "selectors": ["div.govspeak"],
    },
    {
        "id": "afresearchlab", "name": "AFRL",
        "feeds": ["https://afresearchlab.com/feed/"],
        "full": "content",
        "lookback": 40,  # 期刊类，更新频率低
    },
    {
        "id": "westpoint", "name": "西点军校",
        "feeds": ["https://www.westpoint.edu/rss.xml"],
        "full": "page",
        "selectors": ["div.field--name-body", "div.node__content", "article", "main"],
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    },
    {
        "id": "rand", "name": "兰德",
        "feeds": [
            "https://www.rand.org/pubs/new.xml",
            "https://www.rand.org/pubs/commentary.xml",
            "https://www.rand.org/pubs/articles.xml",
        ],
        "full": "page",
        "selectors": ["div.body-text", "article.blog", "div.product-main", "div.abstract", "article", "main"],
        "lookback": 30,
    },
    {
        "id": "us_dod", "name": "美国国防部",
        "feeds": [
            "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=808&max=20",
            "https://www.dvidshub.net/rss/department-of-defense",
        ],
        "full": "page",
        "selectors": ["div.news-body", "div.news-item-body", "div.field--name-body", "div.body-content", "div.article-body", "div.news-story"],
        "lookback": 10,
    },
    {
        "id": "us_marines", "name": "美国海军陆战队",
        "feeds": [
            "https://www.marines.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=20",
            "https://www.dvidshub.net/rss/marines",
        ],
        "full": "page",
        "selectors": ["div.news-body", "div.news-article-body", "div.field--name-body", "div.body-content", "div.article-body"],
        "lookback": 10,
    },
    {
        "id": "us_airforce", "name": "美国空军",
        "feeds": [
            "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=20",
            "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1",
        ],
        "full": "page",
        "selectors": ["div.field--name-body", "div.field--name-field-story-body", "div.article-body", "div.body-content", "div#dnn_NewsArticleContent", "div.news-body", "article", "main"],
        "lookback": 10,
    },
]

STRIP_TAGS = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")

# DVIDS 图集标题后缀（同一次活动每个镜头一条 feed 条目，标题带 [Image N of M]）
GALLERY_RE = re.compile(r"\s*[\[(]\s*Image\s+\d+\s+of\s+\d+\s*[\])]\s*", re.I)


def strip_gallery_suffix(title):
    return GALLERY_RE.sub("", str(title or "")).strip()


# 行级噪音：DVIDS/军方媒体页的元信息块（IMAGE INFO 表、版权行、下载按钮、相关推荐等）。
# 只删除「整行命中」的行，正文里偶尔出现的普通词不受影响。
JUNK_LINE_RE = re.compile(
    r"^\s*(?:"
    r"(?:IMAGE|AUDIO|VIDEO|GRAPHIC)\s+INFO\s*"
    r"|(?:Date Taken|Date Posted|Photo ID|VIRIN|Resolution|Size|Location|Web Views|Downloads|High-Res\.?\s*Downloads|Category|Filename|Length|Originator|Content ?type|Status|Not Released)\s*[:：].*"
    r"|\d{1,2}\.\d{1,2}\.\d{4}\s*"
    r"|PUBLIC DOMAIN\s*"
    r"|Photo ?(?:by|By)\s*[:：]?\s*.*"
    r"|(?:Video|Audio|Film|Graphic)\s+?by\s*[:：]?\s*.*"
    r"|Courtesy\s+(?:Audio|Video|Photo|Image|Graphic)s?\s*[:：]?\s*.*"
    r"|This work, .*must comply with.*"
    r"|MORE LIKE THIS\s*|CONTROLLED VOCABULARY KEYWORDS\s*|TAGS\s*|OPTIONS\s*|RELATED\s+(?:LINKS|PRODUCTS|ARTICLES|IMAGES|VIDEOS|NEWS)\s*|NEWS INFO\s*"
    r"|Register/Login to Download.*|Register to Download.*|Validate Your Account.*|Connect (?:My )?Placements.*"
    r"|Add to (?:My )?Albums.*|Download (?:Audio|Video|Image|High-Res|Closed Caption|Transcript|Photo|Assets|Now).*|Close Download Panel.*"
    r"|VIEW (?:IMAGE|VIDEO|AUDIO) PAGE\s*|VIEW ORIGINAL\s*"
    r"|DOWNLOAD PUBLICATION\s*|Download the publication.*"
    r"|READ MORE\s*|SEE LESS\s*|Share this.*|PRINT\s*|EMAIL\s*|SUBSCRIBE\s*"
    r"|Click (?:here|photo) .*|View the full story.*|See the full story.*"
    r"|\d+\s*/\s*\d+\s*"
    r")\s*$",
    re.I,
)
# DVIDS 资产页元信息：整行大写的地点行（如 MARINE CORPS BASE HAWAII, HAWAII, UNITED STATES）
ALLCAPS_LOC_RE = re.compile(r"^[A-Z0-9 .,'&()/-]{4,80},\s*UNITED STATES\.?\s*$")


def filter_junk_lines(text):
    """按空行分段后逐段过滤元信息/页面皮肤行，只留正文。"""
    out = []
    for p in str(text or "").split("\n\n"):
        p = p.strip()
        if not p:
            continue
        # 单行段落整行判定；多行段落仅剔除其中命中的行
        lines = p.split("\n")
        kept = [ln for ln in lines if not JUNK_LINE_RE.match(ln.strip()) and not ALLCAPS_LOC_RE.match(ln.strip())]
        kept = [ln for ln in kept if ln.strip()]
        if kept:
            out.append("\n".join(kept))
    return "\n\n".join(out)


def norm_title(text):
    """标题规范化：去大小写/空白/标点，用于跨源同题去重"""
    return re.sub(r"[\W_]+", "", str(text or "").lower())


def http_get(url, timeout=30, retries=2, ua=None):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": ua or UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            ctype = resp.headers.get("Content-Type", "") or ""
            return raw.decode("utf-8", "ignore") if "utf-8" in ctype or not ctype else raw.decode("utf-8", "ignore")
        except Exception as e:  # noqa: BLE001
            last = e
            if i < retries:
                time.sleep(1.5 * (i + 1))
    raise last


def norm_date(value):
    if not value:
        return None
    v = str(value).strip()
    try:
        dt = parsedate_to_datetime(v)
    except Exception:  # noqa: BLE001
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:  # noqa: BLE001
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def clean_html_to_paragraphs(html):
    """把 HTML 片段转成正文段落文本（\n\n 分隔），尽力去导航、去图片。
    只保留纯文字：图片/表格多媒体一律丢弃。广泛纳入 p/标题/列表/引用/表格单元格等标准标签，
    并额外用「可视化分段容器（div/section/article/li）」兜底，避免正文用裸 div 承载时漏抓。"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    # 去导航/脚本/样式/图片（用户明确只要文字，不抓图）
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "aside", "form",
                              "iframe", "noscript", "img", "picture", "figure", "video", "audio",
                              "svg", "canvas", "source"]):
        tag.decompose()
    seen = set()
    paras = []
    # 1) 标准块级标签（最可靠）
    for p in soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "blockquote",
                            "td", "th", "dd", "dt", "figcaption", "pre"]):
        t = WS.sub(" ", p.get_text(" ", strip=True)).strip()
        key = t
        if len(t) >= 2 and key not in seen:
            seen.add(key)
            paras.append(t)
    # 2) 可视化分段容器兜底：很多站正文直接塞在裸 div 里（无 <p>）。
    #    始终扫描「内容叶子容器」：不包含任何块级子标签(p/h*/li/table/blockquote/div/section/article)的才是纯文本块，
    #    这样既补上裸 div 正文，又不会与上面标准标签重复叠加。
    containers = soup.find_all(["div", "section", "article"])
    for c in containers:
        if c.find(["div", "section", "article", "p", "li", "table", "ul", "ol",
                   "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre"]):
            continue
        t = WS.sub(" ", c.get_text(" ", strip=True)).strip()
        if len(t) >= 2 and t not in seen:
            seen.add(t)
            paras.append(t)
    # 3) 仅当标准标签与叶子容器全部落空时，才退回节点整体文本按句粗分。
    #    注意不能放宽到"少于3段"：lxml 会把纯文本（如 DVIDS 图说）包成单个 <p>，
    #    此时按句切分会把完整图说重复地切成碎片。
    if not paras:
        whole = WS.sub(" ", soup.get_text(" ", strip=True)).strip()
        if whole:
            chunks = re.split(r"(?<=[。！？.!?])\s*|\n+", whole)
            for c in chunks:
                c = c.strip()
                if len(c) >= 20 and c not in seen:
                    seen.add(c)
                    paras.append(c)
    return filter_junk_lines("\n\n".join(paras))[:BODY_MAX_CHARS]


def extract_og(soup, prop):
    """读取 <meta property/name=...> 的 content（og:description 常是站点自带的干净首段/图说）"""
    tag = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
    if tag:
        return WS.sub(" ", (tag.get("content") or "")).strip()
    return ""


def dvids_og_caption(url):
    """DVIDS 图/音/视频资产页的唯一"正文"= 官方图说/简介，站点已把它放进 og:description
    （页面其余全是元信息表：IMAGE INFO/VIRIN/下载按钮/相关推荐）。"""
    html = http_get(url, timeout=40)
    soup = BeautifulSoup(html, "lxml")
    og = extract_og(soup, "og:description")
    if og:
        return filter_junk_lines(og)
    # og 缺失时退回 asset 描述容器（share 按钮块后面的 <p>）
    for p in soup.find_all("p"):
        t = WS.sub(" ", p.get_text(" ", strip=True)).strip()
        if len(t) >= 60:
            return t
    return ""


def extract_page(url, selectors, ua=None):
    """按候选选择器抽取正文段落。
    选择器命中但只抓到开头几段（正文容器选小了）是历史主诉——为此：
    1) 所有候选（精确选择器/最密段落容器/og:description/长段落兜底）统一走噪音过滤后比长度；
    2) 精确选择器结果过短（<1200 字符）时，允许「最密段落容器」接管，避免拦腰截断；
    3) 全部落空时退回 og:description / 官方摘要。
    只保留文字：导航/页眉页脚/图片/元信息表一律丢弃。"""
    html = http_get(url, timeout=40, ua=(ua or PAGE_UA))
    soup = BeautifulSoup(html, "lxml")

    def ptext_total(el):
        return sum(len(WS.sub(" ", p.get_text(" ", strip=True)).strip()) for p in el.find_all("p"))

    # 1) 标签级噪音：脚本/样式/图片/表单控件等直接删。
    #    nav/header/footer/aside 若包含大量段落文本则改用 unwrap——部分站点 HTML 畸形
    #    （如 westpoint.edu 正文被浏览器解析进 <nav> 里），直接删会把整篇正文一起删掉。
    for tag in soup.find_all(["script", "style", "iframe", "noscript",
                              "img", "picture", "video", "audio", "svg", "canvas", "source", "button", "input", "select", "textarea"]):
        tag.decompose()
    for tag in soup.find_all(["nav", "header", "footer", "aside", "form"]):
        if ptext_total(tag) >= 400:
            tag.unwrap()
        else:
            tag.decompose()
    # 2) 类名/id 命中"页面皮肤"关键字的容器（DVIDS 的纯菜单 dvids_main_nav/页脚/隐藏嵌入弹窗 uk-modal、
    #    gov.uk 的 cookie 横幅、各类 related/share/subscribe/offcanvas 等）：
    #    避免把 导航菜单/隐藏弹窗/嵌入代码说明/相关推荐/分享条 等非正文页面皮肤混进正文。
    #    保护规则：容器内含有足量段落文本（>=400 字符）时不删——那是内容包装层，不是皮肤。
    chrome_re = re.compile(
        r"(^|[-_ ])(menu|nav|footer|modal|offcanvas|cookie|share|subscribe|embed|pagination|masthead|logo|breadcrumb)([-_ ]|$)", re.I)
    to_remove = []
    for el in soup.find_all(True):
        if el.name in ("body", "html"):
            continue   # 绝不动 body/html，避免 WordPress 站（asf/rand 等的 banner/related/sidebar 词在正文包装上）被整页误删
        attrs = getattr(el, "attrs", None) or {}
        cls = attrs.get("class") or []
        cid = attrs.get("id")
        if not isinstance(cls, list):
            cls = list(cls) if cls else []
        ident = (" ".join(map(str, cls)) + (" " + str(cid) if cid else "")).strip()
        if ident and chrome_re.search(ident):
            if ptext_total(el) >= 400:
                continue
            to_remove.append(el)
    for el in to_remove:
        el.decompose()
    best = ""
    for sel in selectors or []:
        try:
            node = soup.select_one(sel)
        except Exception:  # noqa: BLE001
            continue
        if not node:
            continue
        text = clean_html_to_paragraphs(str(node))
        # 取最长候选：某些选择器只覆盖正文前半，取最全的
        if len(text) > len(best):
            best = text
    # 兜底A：正文段落最密集的容器（始终计算）。精确选择器抓到的正文过短（<1200 字符，疑似只抓到
    # 导语/前几段）且容器正文更长时用容器接管——修复"原文十几段只抓到 1-2 段"的截断。
    dense = ""
    psums = []
    for node in soup.find_all(["div", "article", "section", "main"]):
        ps = node.find_all("p")
        if not ps:
            continue
        total = sum(len(WS.sub(" ", p.get_text(" ", strip=True)).strip()) for p in ps)
        if total >= 200:
            psums.append((total, node))
    if psums:
        psums.sort(key=lambda x: x[0], reverse=True)
        cand = clean_html_to_paragraphs(str(psums[0][1]))
        if cand:
            dense = cand
    if len(best) < 1200 and len(dense) > len(best):
        best = dense
    # 兜底B：站点自带的 og:description（干净、无页面皮肤），正文仍过短时补上
    if len(best) < 300:
        og = extract_og(soup, "og:description") or extract_og(soup, "description")
        if len(og) > len(best):
            best = og
    # 兜底C：全页较长段落（导航/页眉页脚已在上一步清除）
    if len(best) < 200:
        paras = []
        for p in soup.find_all(["p", "h1", "h2", "h3", "li", "blockquote", "td"]):
            t = WS.sub(" ", p.get_text(" ", strip=True)).strip()
            if len(t) >= 60:  # 短句多为导航/链接
                paras.append(t)
        cand = "\n\n".join(paras)[:BODY_MAX_CHARS]
        if len(cand) > len(best):
            best = cand
    return best


def feed_entries(channel):
    """拉取一个频道所有 feed 的原始条目，返回 [{...}]。单 feed 失败不拖垮整频道。
    DVIDS 图集同一活动会按镜头拆成多条（标题带 [Image N of M]、图说相同）：
    标题去后缀后按基础标题去重（保留图说最长的一条），避免一图一条刷屏。"""
    entries = []
    seen = set()
    seen_base = {}
    errors = []
    for feed_url in channel["feeds"]:
        try:
            raw = http_get(feed_url, timeout=40, ua=channel.get("ua"))
            parsed = feedparser.parse(raw)
            if parsed.bozo and not parsed.entries:
                raise ValueError(str(parsed.get("bozo_exception") or "parse error"))
        except Exception as e:  # noqa: BLE001
            errors.append("%s -> %s" % (feed_url, e))
            continue
        for e in parsed.entries:
            url = (e.get("link") or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            title = WS.sub(" ", strip_gallery_suffix(e.get("title"))).strip()
            author = ""
            if e.get("author"):
                author = WS.sub(" ", str(e["author"]).strip())
            elif e.get("authors"):
                author = WS.sub(" ", "、".join(a.get("name", "") for a in e["authors"] if a.get("name")))
            pub = norm_date(e.get("published") or e.get("updated") or e.get("pubDate") or "")
            # feed 内全文或摘要（保留原始 HTML 摘要：DVIDS 图说的干净来源，正文抓取失败时的兜底）
            raw_summary = ""
            if channel["full"] == "content":
                if e.get("content"):
                    raw_summary = e.content[0].get("value", "")
                elif e.get("summary"):
                    raw_summary = e.get("summary")
            else:
                raw_summary = e.get("summary") or ""
            body = clean_html_to_paragraphs(raw_summary) if (channel["full"] == "content" and raw_summary) else ""
            summary = WS.sub(" ", STRIP_TAGS.sub(" ", (body or raw_summary or "")[:600])).strip()
            entry = {
                "url": url, "title": title, "author": author, "pubDate": pub,
                "summary": summary[:500], "body": body,
                "_rawSummary": raw_summary,
            }
            # 图集去重：同基础标题（如 [Image 2 of 5] 剥离后相同）只留图说最全的一条
            base = norm_title(title)
            if base:
                prev = seen_base.get(base)
                if prev is not None:
                    old = entries[prev]
                    if len(clean_html_to_paragraphs(raw_summary)) > len(old.get("_rawSummary") or ""):
                        entries[prev] = entry
                    continue
                seen_base[base] = len(entries)
            entries.append(entry)
    if not entries and errors:
        raise RuntimeError("all feeds failed: %s" % "; ".join(errors)[:400])
    return entries


def process_channel(ch, now):
    """并行处理单个信源：拉取 feed → 排序筛选 → (若为 page 型)源内并行抓正文 → 返回 (id, meta, items)。
    单源失败不影响其它源（局部容错）。"""
    lookback = ch.get("lookback", LOOKBACK_DAYS)
    cutoff = now - timedelta(days=lookback)
    meta = {"name": ch["name"], "status": "error", "count": 0, "error": None, "fetchedAt": now.isoformat()}
    try:
        entries = feed_entries(ch)
        entries.sort(key=lambda x: x["pubDate"] or "", reverse=True)
        ch_items = [e for e in entries if (not e["pubDate"]) or e["pubDate"] >= cutoff.isoformat()][:MAX_PER_CHANNEL]
        # page 型：源内多篇正文并行抓取（限并发，避免同一源站被限流/反爬）
        if ch["full"] == "page" and ch_items:
            def grab(e):
                try:
                    raw = e.get("_rawSummary", "") or ""
                    url = e["url"] or ""
                    # DVIDS 图/音/视频资产页：页面主体是元信息表（IMAGE INFO/VIRIN/下载按钮等），
                    # 唯一"正文"就是官方图说/简介 —— 优先 RSS description，为空则取页面 og:description。
                    if "dvidshub.net" in url and "/news/" not in url:
                        cap = clean_html_to_paragraphs(raw) or dvids_og_caption(url)
                        if cap:
                            e["body"] = cap
                            if not e["summary"]:
                                e["summary"] = WS.sub(" ", cap[:400]).strip()[:400]
                            print("[%s] caption-from-rss %s chars: %s" % (ch["id"], len(cap), url[:70]), flush=True)
                            return e
                    body = extract_page(url, ch.get("selectors"), ua=ch.get("ua"))
                    if body:
                        # 首行与标题相同（dense 容器把 h1 一起带进来）时去掉，避免标题混进正文
                        parts_ = body.split("\n\n", 1)
                        if len(parts_) == 2 and norm_title(parts_[0]) == norm_title(e["title"] or ""):
                            body = parts_[1]
                        e["body"] = body
                        if not e["summary"]:
                            e["summary"] = WS.sub(" ", body[:400]).strip()[:400]
                        print("[%s] body %s chars: %s" % (ch["id"], len(body), url[:70]), flush=True)
                    else:
                        # 页面正文未取到（站点反爬/JS 渲染）：退回该源官方摘要全文（去标签、不再截短到 500），
                        # 至少不给用户一个正文空白的断章条目，并可点原文链接核对完整内容
                        full_summary = clean_html_to_paragraphs(raw)
                        if full_summary:
                            e["body"] = full_summary
                            print("[%s] body fallback-rss-summary %s chars: %s" % (ch["id"], len(full_summary), url[:70]), flush=True)
                        else:
                            print("[%s] body empty: %s" % (ch["id"], url[:70]), flush=True)
                except Exception as ex:  # noqa: BLE001
                    print("[%s] body-fail %s: %s" % (ch["id"], e["url"][:70], ex), flush=True)
                    # 抓取失败同样退回源官方摘要，避免正文空白
                    if e.get("_rawSummary") and not e.get("body"):
                        e["body"] = clean_html_to_paragraphs(e.get("_rawSummary"))
                return e
            with ThreadPoolExecutor(max_workers=4) as ex:
                ch_items = list(ex.map(grab, ch_items))
        for e in ch_items:
            e.pop("_rawSummary", None)
            e["channel"] = ch["id"]
            e["channelName"] = ch["name"]
        meta = {"name": ch["name"], "status": "ok", "count": len(ch_items), "error": None, "fetchedAt": now.isoformat()}
        print("[%s] ok=%d" % (ch["id"], len(ch_items)), flush=True)
        return ch["id"], meta, ch_items
    except Exception as e:  # noqa: BLE001
        meta["error"] = str(e)[:300]
        print("[%s] ERROR: %s" % (ch["id"], e), flush=True)
        return ch["id"], meta, []


def main():
    now = datetime.now(timezone.utc)
    meta = {}
    items = []
    seen_titles = set()  # 跨源同题去重（规范化标题，保留先完成的通道）
    errors = []

    # 多信源并行：线程池同时抓取所有 CHANNELS，任一失败不拖垮整体
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(process_channel, ch, now): ch["id"] for ch in CHANNELS}
        for f in cf.as_completed(futs):
            cid, m, ch_items = f.result()
            meta[cid] = m
            if m["status"] != "ok":
                errors.append(cid)
            keep = []
            for e in ch_items:
                key = norm_title(e.get("title") or "")
                if key and key in seen_titles:
                    continue
                if key:
                    seen_titles.add(key)
                keep.append(e)
            items.extend(keep)

    items.sort(key=lambda x: x["pubDate"] or "", reverse=True)

    data = {"updatedAt": now.isoformat(), "meta": meta, "items": items}
    # 输出固定为脚本同目录下的 feeds/latest.json（字面量相对路径，无任何拼接/穿越可能）
    import pathlib
    out = pathlib.Path(__file__).resolve().parent / "feeds" / "latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    print("done items=%d errors=%s" % (len(items), errors or "none"), flush=True)
    if not items:
        sys.exit(1)


if __name__ == "__main__":
    main()
