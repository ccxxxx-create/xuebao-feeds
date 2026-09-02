# xuebao-feeds · 英语学报信源镜像

与个人工作台 `arxiv-mirror` 同模式：GitHub Actions 每天 09:00（北京时间）自动抓取
6 个官方直连信源的 RSS/Atom（含正文全文），汇总到 `feeds/latest.json` 提交回本仓库；
《英语学报》静态网页从 jsdelivr / raw.githubusercontent 读取该 JSON（绕过浏览器跨域）。

- 抓取脚本：`_mirror/fetch_feeds.py`
- 定时任务：`.github/workflows/feeds.yml`（也可在 Actions 页手动 Run workflow 触发）

## 频道

defensenews（Defense News）、airandspaceforces（Air & Space Forces）、govuk_mod（英国防部）、
afresearchlab（AFRL）、westpoint（西点军校）、rand（兰德，合并 new/commentary/articles/press 四 feed）。

## 输出格式

```json
{
  "updatedAt": "ISO8601",
  "meta": { "<channel>": { "name", "status", "count", "error", "fetchedAt" } },
  "items": [ { "url", "channel", "channelName", "title", "author", "pubDate", "summary", "body" } ]
}
```

## 说明

- 内容版权归各原发布方所有，本镜像仅作为个人阅读归档的 RSS 代理，请勿公开再分发全文。
- 抓取遵循各站 robots 许可的低频单轮策略（间隔 ≥2s、每频道每轮 ≤10 条）。
