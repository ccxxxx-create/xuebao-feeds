#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

import feedparser
from bs4 import BeautifulSoup

UA = "xuebao-mirror/1.0 (official rss reader; personal archive only)"
THROTTLE = 2.0          # 频道间请求间隔（秒）
BODY_THROTTLE = 2.5     # 正文页请求间隔（秒）
MAX_PER_CHANNEL = 20    # 每频道每轮上限
LOOKBACK_DAYS = 7
BODY_MAX_CHARS = 15000
KEEP_TOP = 20000        # 保留字段总上限（防止单文件过大）

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
        "selectors": ["div.col-sm-9", "div#main", "article", "main"],
        "lookback": 30,
    },
    {
        "id": "us_dod", "name": "美国国防部",
        "feeds": [
            "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=808&max=20",
            "https://www.dvidshub.net/rss/department-of-defense",
        ],
        "full": "page",
        "selectors": ["div.news-item-body", "div.body-content", "div.article-body", "article", "main"],
        "lookback": 10,
    },
    {
        "id": "us_marines", "name": "美国海军陆战队",
        "feeds": [
            "https://www.marines.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=20",
            "https://www.dvidshub.net/rss/marines",
        ],
        "full": "page",
        "selectors": ["div.news-article-body", "div.body-content", "div.article-body", "article", "main"],
        "lookback": 10,
    },
    {
        "id": "us_airforce", "name": "美国空军",
        "feeds": [
            "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=20",
            "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1",
        ],
        "full": "page",
        "selectors": ["div#dnn_NewsArticleContent", "div.news-body", "div.article-body", "article", "main"],
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
    """把 HTML 片段转成正文段落文本（\n\n 分隔），尽力去导航。"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "noscript"]):
        tag.decompose()
    paras = []
    for p in soup.find_all(["p", "h1", "h2", "h3", "li"]):
        t = WS.sub(" ", p.get_text(" ", strip=True)).strip()
        if len(t) >= 2:
            paras.append(t)
    return "\n\n".join(paras)[:BODY_MAX_CHARS]


def extract_page(url, selectors, ua=None):
    """按候选选择器抽取正文段落；都不足则退回全页 <p>。"""
    html = http_get(url, timeout=40, ua=ua)
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "nav", "form", "iframe", "noscript"]):
        tag.decompose()
    best = ""
    for sel in selectors or []:
        node = soup.select_one(sel)
        if not node:
            continue
        text = clean_html_to_paragraphs(str(node))
        if len(text) >= 200:
            best = text
            break
    if not best:
        paras = []
        for p in soup.find_all("p"):
            t = WS.sub(" ", p.get_text(" ", strip=True)).strip()
            if len(t) >= 60:  # 短句多为导航/链接
                paras.append(t)
        best = "\n\n".join(paras)[:BODY_MAX_CHARS]
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


def main():
    now = datetime.now(timezone.utc)
    items = []
    meta = {}
    last_err = None
    seen_titles = set()  # 跨通道同题去重（按规范化标题，保留先到的通道）

    for ch in CHANNELS:
        lookback = ch.get("lookback", LOOKBACK_DAYS)
        cutoff = now - timedelta(days=lookback)
        ch_items = []
        try:
            entries = feed_entries(ch)
            entries.sort(key=lambda x: x["pubDate"] or "", reverse=True)
            # 无日期条目（部分官方 feed 缺失）也保留，避免静默归零
            ch_items = [e for e in entries if (not e["pubDate"]) or e["pubDate"] >= cutoff.isoformat()][:MAX_PER_CHANNEL]
            keep = []
            for e in ch_items:
                key = norm_title(e.get("title"))
                if key and key in seen_titles:
                    continue
                if key:
                    seen_titles.add(key)
                keep.append(e)
            ch_items = keep
            meta[ch["id"]] = {"name": ch["name"], "status": "ok", "count": len(ch_items), "error": None, "fetchedAt": now.isoformat()}
        except Exception as e:  # noqa: BLE001
            last_err = e
            meta[ch["id"]] = {"name": ch["name"], "status": "error", "count": 0, "error": str(e)[:300], "fetchedAt": now.isoformat()}
            print("[%s] ERROR: %s" % (ch["id"], e), flush=True)
            continue

        # 摘要型频道：逐个抓正文页
        if ch["full"] == "page":
            for e in ch_items:
                try:
                    body = extract_page(e["url"], ch.get("selectors"), ua=ch.get("ua"))
                    if body:
                        e["body"] = body
                        if not e["summary"]:
                            e["summary"] = WS.sub(" ", body[:400]).strip()[:400]
                    print("[%s] body %s chars: %s" % (ch["id"], len(body), e["url"][:80]), flush=True)
                except Exception as ex:  # noqa: BLE001
                    print("[%s] body-fail %s: %s" % (ch["id"], e["url"][:80], ex), flush=True)
                time.sleep(BODY_THROTTLE)

        for e in ch_items:
            e["channel"] = ch["id"]
            e["channelName"] = ch["name"]
        items.extend(ch_items)
        time.sleep(THROTTLE)
        print("[%s] ok=%d" % (ch["id"], len(ch_items)), flush=True)

    # 控制单文件体积（截断超长 body 取前段+尾部说明由客户端处理）
    if len(json.dumps(items, ensure_ascii=False)) > KEEP_TOP * 2:
        # 保守：单条目 body 上限已由 BODY_MAX_CHARS 控制，这里仅全量兜底
        pass

    data = {"updatedAt": now.isoformat(), "meta": meta, "items": items}
    os.makedirs("feeds", exist_ok=True)
    with open("feeds/latest.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    total = len(items)
    errors = [k for k, v in meta.items() if v["status"] == "error"]
    print("done items=%d errors=%s" % (total, errors or "none"))
    if last_err and total == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
