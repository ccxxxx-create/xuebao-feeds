#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""《英语学报》服务端预翻译管线：feeds/latest.json 未翻条目 → DeepSeek 批量翻译 → 写回 zh 字段

用法（GitHub Actions / 本机通用）：
  python translate_feeds.py              # 增量翻译所有 zhState != ok 的条目
  python translate_feeds.py --limit 5    # 本次最多翻 5 篇（试跑/控预算）
  python translate_feeds.py --dry-run    # 不调 API：打印将翻篇数与首批请求体后退出

环境变量：
  DEEPSEEK_API_KEY    必需。缺失时打印 skip 并以退出码 0 结束——翻译缺失不阻塞英文版照常投送
  DEEPSEEK_BASE_URL   可选，默认 https://api.deepseek.com
  DEEPSEEK_MODEL      可选，默认 deepseek-flash

每条 item 写入（与 webapp 阅读页 zhParas 对照格式兼容，字段语义同 mirror.js）：
  zhParas  译分数组，与 body 按 \\n\\n 切分的段一一对应（失败段为空串）
  zhFull   译文全文（\\n\\n join）
  zhState  ok | failed（仅全部段成功才 ok；failed 时 webapp 可自配模型续翻）
  zhDone / zhChunks  成功段数 / 总段数
  zhAt     翻译时间（ISO8601）
meta.translate 汇总：{model, okCount, failCount, requests, tokensIn, tokensOut, seconds, deferred}
"""
import argparse
import ipaddress
import json
import os
import pathlib
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

MAX_REQUESTS = 400          # 单次运行请求预算护栏：超限剩余篇目标 deferred 留到下轮
BATCH = 8                   # 每批段数（编号保序+校验，缺号/错序降级单段）
NEW_ARTICLE_DAYS = 3        # 仅翻「每日新增」：pubDate 在近 3 天内的条目；窗口内存量旧文不回翻（2026-09-23 用户拍板）
TIMEOUT = 180               # 单次 API 调用超时（秒）
RETRY_BACKOFF = (2, 4, 8)   # 429/5xx/超时的重试间隔（秒）
LEN_RATIO = (0.1, 4.0)      # 译文/原文长度比合理区间（防截断/复读）
PARA_SPLIT_RE = re.compile(r"\n{2,}")

# ---- 出站 URL 安全校验（与 fetch_feeds.check_url 同规则；API 端点同样不得绕过）----
BAD_HOST_RE = re.compile(r"^(localhost|.*\.local|.*\.internal|.*\.localhost)$", re.I)


def check_url(url):
    """校验 URL 可否出站访问：仅 https、拒绝保留主机名、DNS 解析含私网/环回/保留地址即拒绝。"""
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme != "https":
        raise ValueError("scheme not allowed: %s" % p.scheme)
    host = (p.hostname or "").strip().lower().rstrip(".")
    if not host or BAD_HOST_RE.match(host):
        raise ValueError("host not allowed: %s" % host)
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception as e:  # noqa: BLE001 —— 解析失败按错误路径处理
        raise ValueError("dns fail: %s" % e)
    for info in infos:
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("host resolves to reserved address: %s" % ip)
    return host


# 内置精选军事术语表（控制注入体积；新增词条直接追加）
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
    ("deterrence", "威慑"), ("readiness", "战备"), ("munition(s)", "弹药"),
    ("sortie", "架次"), ("flight deck", "飞行甲板"), ("flight surgeon", "航空军医"),
    ("boots on the ground", "地面部队"), ("rules of engagement", "交战规则"),
    ("situation report", "态势报告"), ("after-action review", "行动后复盘"),
    ("chain of command", "指挥链"), ("joint exercise", "联合演习"),
    ("ballistic missile", "弹道导弹"), ("cruise missile", "巡航导弹"),
    ("hypersonic", "高超声速"), ("unmanned aerial system", "无人机系统"),
    ("electronic warfare", "电子战"), ("cyber command", "网络司令部"),
    ("territorial defense", "领土防御"), ("escalation", "局势升级"),
]

SYSTEM_PROMPT = (
    "你是国防/军事新闻专业译者，把英文新闻段落译成简体中文。要求：\n"
    "1) 忠实原意，用词准确、语句通顺，符合中文新闻语感；不增不减，不加注释。\n"
    "2) 武器装备型号（F-35、B-21、M1A2 等）、军衔缩写、度量单位保留英文；人名首译在中文后括注原文。\n"
    "3) 术语表（英文=中文）：\n"
    + "\n".join("%s=%s" % (en, zh) for en, zh in GLOSSARY)
)


def parse_args():
    ap = argparse.ArgumentParser(description="translate feeds/latest.json via DeepSeek")
    ap.add_argument("--limit", type=int, default=0, help="本次最多翻译篇数（0=不限）")
    ap.add_argument("--dry-run", action="store_true", help="不调用 API，只打印计划与首批请求体")
    return ap.parse_args()


def split_paras(body):
    """与 webapp 阅读页 paras()/mirror.js splitParas 完全一致：连续空行分段。"""
    return [p.strip() for p in PARA_SPLIT_RE.split(str(body or "")) if p.strip()]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # API 调用不跟随重定向（302 落入 HTTPError 分支），杜绝跳转绕过 check_url


_OPENER = urllib.request.build_opener(_NoRedirect())


def post_json(url, payload, key, timeout=TIMEOUT):
    """带出站校验与退避重试的 JSON POST。返回 (response_dict, usage_tuple)。"""
    check_url(url)  # scheme/host/私网解析校验，拒绝内网/保留地址
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json", "Authorization": "Bearer %s" % key},
    )
    last = None
    waits = (0,) + RETRY_BACKOFF
    for attempt, wait in enumerate(waits):
        if wait:
            time.sleep(wait)
        try:
            with _OPENER.open(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data, (int((data.get("usage") or {}).get("prompt_tokens") or 0),
                              int((data.get("usage") or {}).get("completion_tokens") or 0))
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500 and e.code != 429:  # key 错/参数错重试无意义
                raise
            last = e
            print("  api http %d" % e.code, flush=True)
        except Exception as e:  # noqa: BLE001 —— 网络/超时类同样退避
            last = e
            print("  api error: %s" % e, flush=True)
    raise last


def chat(messages, key, base, model):
    data, usage = post_json(
        "%s/chat/completions" % base.rstrip("/"),
        {"model": model, "messages": messages, "temperature": 0.2, "stream": False,
         "max_tokens": 4096},
        key,
    )
    choice = (data.get("choices") or [{}])[0]
    if choice.get("finish_reason") == "length":
        # 输出在 max_tokens 处被截断：残缺译文不能标 ok，按失败走重试/兜底
        raise RuntimeError("output truncated (finish_reason=length)")
    text = (choice.get("message") or {}).get("content") or ""
    return text, usage


def parse_json_arr(text, n):
    """解析 JSON 字符串数组输出；元素数==n 且都非空才返回列表，否则 None。
    （编号行格式实测会被模型在长句中间重新断句导致段间错位，JSON 数组的位置对应是结构强约束）"""
    m = re.search(r"\[[\s\S]*\]", str(text or ""))
    if not m:
        return None
    try:
        arr = json.loads(m.group(0))
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(arr, list) or len(arr) != n:
        return None
    vals = [str(x).strip() for x in arr]
    return vals if all(vals) else None


def ratio_ok(en, zh):
    if not zh:
        return False
    r = len(zh) / max(len(en), 1)
    return LEN_RATIO[0] <= r <= LEN_RATIO[1]


def translate_batch(paras_en, idxs, key, base, model, stats):
    """翻译一批（idxs 为段落下标）。返回 {下标: 译文}；整批解析失败则逐段兜底。"""
    n = len(idxs)
    user = ("把以下 %d 个编号英文段落译成简体中文。只输出一个 JSON 字符串数组（恰好 %d 个元素，"
            "第 i 个元素 = 第 i 段的完整译文，段落之间不得串写、合并或拆分），不要输出任何其他内容：\n\n%s") % (
        n, n, "\n\n".join("[%d] %s" % (j + 1, paras_en[i]) for j, i in enumerate(idxs)))
    stats["requests"] += 1
    text, usage = chat([{"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user}], key, base, model)
    stats["tokensIn"] += usage[0]
    stats["tokensOut"] += usage[1]
    if not str(text).strip():
        # 偶发空响应（API 侧行为，实测约 1/5 批）：先重试一次批量再降级单段
        print("  empty content, retry batch once", flush=True)
        stats["requests"] += 1
        text, usage = chat([{"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user}], key, base, model)
        stats["tokensIn"] += usage[0]
        stats["tokensOut"] += usage[1]
    vals = parse_json_arr(text, n)
    out = {}
    if vals is not None and all(ratio_ok(paras_en[i], v) for i, v in zip(idxs, vals)):
        return {i: v for i, v in zip(idxs, vals)}
    # 整批校验不过 → 逐段重试（失败段留空由 zhState=failed 表达）；留原始输出片段便于云端排查
    print("  batch parse/ratio failed (%d paras), raw head: %s" % (n, re.sub(r"\s+", " ", str(text))[:150]), flush=True)
    for i in idxs:
        got = translate_one(paras_en[i], key, base, model, stats)
        if got is not None:
            out[i] = got
    return out


def translate_one(text_en, key, base, model, stats, tries=2):
    user = "把下面的英文段落译成简体中文，只输出译文本身：\n\n%s" % text_en
    for _ in range(tries):
        stats["requests"] += 1
        text, usage = chat([{"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user}], key, base, model)
        stats["tokensIn"] += usage[0]
        stats["tokensOut"] += usage[1]
        v = re.sub(r"^\s*\[[^\]]*\]\s*", "", str(text).strip(), count=1)
        if ratio_ok(text_en, v):
            return v
    return None


def translate_item(it, key, base, model, stats):
    """翻一篇：跳过已有非空译文段（失败重跑只补空段）。写回 zh 字段族。"""
    paras_en = split_paras(it.get("body"))
    if not paras_en:
        return "skip-empty"
    prev = it.get("zhParas") if isinstance(it.get("zhParas"), list) else []
    zh = [prev[i] if i < len(prev) and isinstance(prev[i], str) and prev[i].strip() else ""
          for i in range(len(paras_en))]
    todo = [i for i, v in enumerate(zh) if not v]
    for b in range(0, len(todo), BATCH):
        out = translate_batch(paras_en, todo[b:b + BATCH], key, base, model, stats)
        for i, v in out.items():
            zh[i] = v
    failed = [i for i, v in enumerate(zh) if not v.strip()]
    it["zhParas"] = zh
    it["zhFull"] = "\n\n".join(zh)
    it["zhDone"] = len(paras_en) - len(failed)
    it["zhChunks"] = len(paras_en)
    it["zhState"] = "ok" if not failed else "failed"
    it["zhAt"] = datetime.now(timezone.utc).isoformat()
    if failed:
        print("  %d/%d paras failed: %s" % (len(failed), len(paras_en), it.get("url", "")[:70]), flush=True)
    return it["zhState"]


def main():
    args = parse_args()
    # 数据文件固定为脚本同目录 feeds/latest.json（与 fetch_feeds.py 相同的 pathlib 字面量拼接，无任何路径拼接参数）
    out = pathlib.Path(__file__).resolve().parent / "feeds" / "latest.json"
    data = json.loads(out.read_text(encoding="utf-8"))
    items = data.get("items") or []
    # 「仅每日新增」：zhState != ok 且（pubDate 在近 NEW_ARTICLE_DAYS 天内 或 pubDate 缺失的保守纳入）
    cutoff = (datetime.now(timezone.utc) - timedelta(days=NEW_ARTICLE_DAYS)).isoformat()
    targets = []
    for it in items:
        if it.get("zhState") == "ok" or not (it.get("body") or "").strip():
            continue
        pub = it.get("pubDate") or ""
        if pub and str(pub) < cutoff:
            continue
        targets.append(it)
    base = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-flash").strip()
    key = os.environ.get("DEEPSEEK_API_KEY", "").strip()

    if args.dry_run:
        print("dry-run: %d/%d items pending translate (model=%s)" % (len(targets), len(items), model))
        if targets:
            demo = split_paras(targets[0].get("body"))[:BATCH]
            print("first batch would be %d paras of: %s" % (len(demo), targets[0].get("title", "")[:60]))
            for j, p in enumerate(demo):
                print("  [%d] %.60s..." % (j + 1, p))
        return 0

    if not key:
        print("translate skip: DEEPSEEK_API_KEY not set (english-only delivery continues)")
        return 0

    if args.limit and len(targets) > args.limit:
        targets = targets[:args.limit]

    stats = {"model": model, "okCount": 0, "failCount": 0, "requests": 0,
             "tokensIn": 0, "tokensOut": 0, "deferred": 0}
    t0 = time.time()
    for n, it in enumerate(targets):
        if stats["requests"] >= MAX_REQUESTS:
            stats["deferred"] = len(targets) - n
            print("budget guard: %d items deferred to next run" % stats["deferred"], flush=True)
            break
        title = (it.get("title") or "")[:50]
        try:
            state = translate_item(it, key, base, model, stats)
            stats["okCount" if state == "ok" else "failCount"] += 1
            print("[%d/%d] %s %s" % (n + 1, len(targets), state, title), flush=True)
        except Exception as e:  # noqa: BLE001 —— 单篇失败不拖垮整批
            stats["failCount"] += 1
            it["zhState"] = "failed"
            print("[%d/%d] ERROR %s: %s" % (n + 1, len(targets), title, e), flush=True)
    stats["seconds"] = round(time.time() - t0, 1)
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta["translate"] = stats
    data["meta"] = meta
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    print("translate done: ok=%d failed=%d requests=%d tokens=%d+%d in %.1fs deferred=%d" % (
        stats["okCount"], stats["failCount"], stats["requests"],
        stats["tokensIn"], stats["tokensOut"], stats["seconds"], stats["deferred"]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
