#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一次性诊断第二轮（跑完即删）：用真实文章页 URL 测 curl_cffi 各指纹对军方站文章页的可达性。"""
import ipaddress
import pathlib
import socket
import urllib.parse

REPORT = []
ALLOW_HOSTS = ("af.mil", "defense.gov", "marines.mil", "westpoint.edu", "dvidshub.net")


def check_url(url):
    p = urllib.parse.urlsplit(str(url or ""))
    if p.scheme not in ("http", "https"):
        raise ValueError("scheme not allowed")
    host = (p.hostname or "").strip().lower().rstrip(".")
    if not host or not any(host == h or host.endswith("." + h) for h in ALLOW_HOSTS):
        raise ValueError("host not in allowlist: %s" % host)
    for info in socket.getaddrinfo(host, None):
        ip = ipaddress.ip_address(str(info[4][0]))
        if (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local
                or ip.is_multicast or ip.is_unspecified):
            raise ValueError("reserved ip")


def note(s):
    REPORT.append(s)
    print(s, flush=True)


URLS = [
    # 真实文章页（来自镜像 RSS 的近期文章）
    ("af.mil 文章页", "https://www.af.mil/News/Article-Display/Article/4590404/senior-afgsc-leaders-see-sentinel-preparations-during-hill-afb-immersion/"),
    ("af.mil 首页", "https://www.af.mil/"),
    ("defense.gov 文章页", "https://www.defense.gov/News/News- Releases/Release/Article/3950175/joint-statement-from-secretary-of-defense-pete-hegseth-and-uk-secretary-of-state-for-defence-john-healey/"),
    ("marines.mil 文章页", "https://www.marines.mil/News/News-Display/Article/4590067/"),
]

TARGETS = [
    ("chrome", dict(impersonate="chrome")),
    ("chrome131", dict(impersonate="chrome131")),
    ("safari184", dict(impersonate="safari184")),
]

from curl_cffi import requests as cr

for label, url in URLS:
    try:
        check_url(url)
    except Exception as e:  # noqa: BLE001
        note("%s URL 不合法 %s" % (label, e))
        continue
    for tname, kw in TARGETS:
        try:
            r = cr.get(url, timeout=45, allow_redirects=True, **kw)
            note("%-18s %-10s -> %d %d bytes" % (label, tname, r.status_code, len(r.content)))
        except Exception as e:  # noqa: BLE001
            note("%-18s %-10s -> EXC %s" % (label, tname, str(e)[:80]))

out = pathlib.Path(__file__).resolve().parent / "feeds" / "debug_tls.txt"
out.write_text("\n".join(REPORT) + "\n", encoding="utf-8")
print("done", flush=True)
