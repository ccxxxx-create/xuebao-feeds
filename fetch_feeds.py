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
import json
import os
import re
import sys
import time
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
BODY_MAX_CHARS = 40000     # 正文上限：放宽，避免长文后半段丢失
KEEP_TOP = 60000        # 保留字段总上限（防止单文件过大）

CHANNELS = [
    {
        "id": "defensenews", "name": "Defense News",
        "feeds": ["https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"],
        "full": "content",
    },
    {
        "id": "airandspaceforces", "name": "Air & Space Forces",
        "feeds": ["https://www.airandspaceforces.com/feed/"],
        "full": "content",
    },
    {
        "id": "govuk_mod", "name": "英国国防部",
        "feeds": ["https://www.gov.uk/government/organisations/ministry-of-defence.atom"],
        "full": "page", "selectors": ["div.govspeak", "article", "main"],
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
        "selectors": ["div.news-body", "div.news-item-body", "div.field--name-body", "div.body-content", "div.article-body", "div.news-story", "article", "main"],
        "lookback": 10,
    },
    {
        "id": "us_marines", "name": "美国海军陆战队",
        "feeds": [
            "https://www.marines.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=20",
            "https://www.dvidshub.net/rss/marines",
        ],
        "full": "page",
        "selectors": ["div.news-body", "div.news-article-body", "div.field--name-body", "div.body-content", "div.article-body", "article", "main"],
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
    # 3) 若仍太少，退回节点整体文本（按句号/换行粗分），确保有内容
    if len(paras) < 3:
        whole = WS.sub(" ", soup.get_text(" ", strip=True)).strip()
        if whole:
            chunks = re.split(r"(?<=[。！？.!?])\s*|\n+", whole)
            for c in chunks:
                c = c.strip()
                if len(c) >= 20 and c not in seen:
                    seen.add(c)
                    paras.append(c)
    return "\n\n".join(paras)[:BODY_MAX_CHARS]


def extract_page(url, selectors, ua=None):
    """按候选选择器抽取正文段落；取【最长】候选为主。
    选择器覆盖不全时，用「正文 <p> 最密集的容器」兜底（跨站通用，不再只靠硬编码选择器），
    避免 DVIDS/rand/af.mil 等站正文容器写错就被拦腰截断。只保留文字：导航/页眉页脚/图片一律丢弃。"""
    html = http_get(url, timeout=40, ua=(ua or PAGE_UA))
    soup = BeautifulSoup(html, "lxml")
    # 去脚本/样式/导航/页眉页脚/图片等噪音（正文容器内的纯文字保留）
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "aside", "form",
                              "iframe", "noscript", "img", "picture", "video", "audio", "svg", "canvas", "source"]):
        tag.decompose()
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
    # 兜底：正文段落最密集的容器（很多站结构不一，单纯选择器易漏；按 <p> 文本总量挑正文所在容器）
    if len(best) < 500:
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
            if len(cand) > len(best):
                best = cand
    # 最终退回：全页较长段落（导航/页眉页脚已在上一步清除）
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
    """拉取一个频道所有 feed 的原始条目，返回 [{...}]。单 feed 失败不拖垮整频道。"""
    entries = []
    seen = set()
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
            title = WS.sub(" ", (e.get("title") or "").strip())
            author = ""
            if e.get("author"):
                author = WS.sub(" ", str(e["author"]).strip())
            elif e.get("authors"):
                author = WS.sub(" ", "、".join(a.get("name", "") for a in e["authors"] if a.get("name")))
            pub = norm_date(e.get("published") or e.get("updated") or e.get("pubDate") or "")
            # feed 内全文或摘要
            content_html = ""
            if channel["full"] == "content":
                if e.get("content"):
                    content_html = e.content[0].get("value", "")
                elif e.get("summary"):
                    content_html = e.get("summary")
            body = clean_html_to_paragraphs(content_html) if content_html else ""
            summary = WS.sub(" ", re.sub(r"\s+", " ", STRIP_TAGS.sub(" ", (body or e.get("summary") or "")[:600]))).strip()
            entries.append({
                "url": url, "title": title, "author": author, "pubDate": pub,
                "summary": summary[:500], "body": body,
            })
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
                    body = extract_page(e["url"], ch.get("selectors"), ua=ch.get("ua"))
                    if body:
                        e["body"] = body
                        if not e["summary"]:
                            e["summary"] = WS.sub(" ", body[:400]).strip()[:400]
                    print("[%s] body %s chars: %s" % (ch["id"], len(body), e["url"][:70]), flush=True)
                except Exception as ex:  # noqa: BLE001
                    print("[%s] body-fail %s: %s" % (ch["id"], e["url"][:70], ex), flush=True)
                return e
            with ThreadPoolExecutor(max_workers=4) as ex:
                ch_items = list(ex.map(grab, ch_items))
        for e in ch_items:
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
    os.makedirs("feeds", exist_ok=True)
    with open("feeds/latest.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print("done items=%d errors=%s" % (len(items), errors or "none"), flush=True)
    if not items:
        sys.exit(1)


if __name__ == "__main__":
    main()
