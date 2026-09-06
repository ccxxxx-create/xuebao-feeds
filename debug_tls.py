#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一次性诊断（跑完即删）：军方站文章页对 GitHub Actions 出口 IP 的可达性测试。
分别用 urllib（Python TLS 指纹）与 curl_cffi（模拟 Chrome TLS 指纹）抓取
af.mil / defense.gov / marines.mil 的 RSS 内文章页，报告状态码与字节数。"""
import ipaddress
import pathlib
import re
import socket
import urllib.parse
import urllib.request
import urllib.error

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
REPORT = []
ALLOW_HOSTS = ("af.mil", "defense.gov", "marines.mil", "westpoint.edu", "dvidshub.net")


def check_url(url):
    """仅允许 http/https 且 host 属于本诊断的官方信源域名；解析 IP 不得为私网/环回/保留段。"""
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme not in ("http", "https"):
        raise ValueError("scheme not allowed: %s" % p.scheme)
    host = (p.hostname or "").strip().lower().rstrip(".")
    if not host or not any(host == h or host.endswith("." + h) for h in ALLOW_HOSTS):
        raise ValueError("host not in allowlist: %s" % host)
    for info in socket.getaddrinfo(host, None):
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("host resolves to reserved address: %s" % ip)


def note(s):
    REPORT.append(s)
    print(s, flush=True)


def via_urllib(url):
    check_url(url)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=45) as r:
        return "OK %d bytes" % len(r.read())


def via_curl_cffi(url):
    check_url(url)
    try:
        from curl_cffi import requests as cr
    except ImportError:
        return "curl_cffi 未安装"
    r = cr.get(url, impersonate="chrome", timeout=45)
    return "%d %d bytes" % (r.status_code, len(r.text))


RSS = {
    "af.mil": "https://www.af.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=5",
    "defense.gov": "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=808&max=5",
    "marines.mil": "https://www.marines.mil/DesktopModules/ArticleCS/RSS.ashx?ContentType=1&Site=1&max=5",
    "westpoint": "https://www.westpoint.edu/rss.xml",
}

for name, rss in RSS.items():
    try:
        check_url(rss)
        raw = urllib.request.urlopen(urllib.request.Request(rss, headers={"User-Agent": UA}), timeout=45).read().decode("utf-8", "ignore")
        links = re.findall(r"<link>(https?://[^<]+)</link>", raw)
        links = [l.strip() for l in links if "RSS" not in l]
        if not links:
            note("%s RSS 解析不到文章链接（%d bytes）" % (name, len(raw)))
            continue
        url = links[0]
        note("%s 测试页: %s" % (name, url))
        for label, fn in (("urllib", via_urllib), ("curl_cffi", via_curl_cffi)):
            try:
                note("  %-9s -> %s" % (label, fn(url)))
            except urllib.error.HTTPError as e:
                note("  %-9s -> HTTP %d" % (label, e.code))
            except Exception as e:  # noqa: BLE001
                note("  %-9s -> %s: %s" % (label, type(e).__name__, str(e)[:100]))
    except urllib.error.HTTPError as e:
        note("%s RSS -> HTTP %d" % (name, e.code))
    except Exception as e:  # noqa: BLE001
        note("%s RSS -> %s: %s" % (name, type(e).__name__, str(e)[:100]))

out = pathlib.Path(__file__).resolve().parent / "feeds" / "debug_tls.txt"
out.write_text("\n".join(REPORT) + "\n", encoding="utf-8")
print("done", flush=True)
