@echo off
rem 英语学报 · 本地 hy-mt2 标题摘要翻译（每日 06:30 由任务计划程序触发）
rem 翻译增量条目并推回 xuebao-feeds 仓库；日志见 _mirror\logs\
cd /d "F:\英语学报\sentra\_mirror"
"F:\英语学报\sentra\_mirror\.venv-probe\Scripts\python.exe" -X utf8 translate_local.py >> "F:\英语学报\sentra\_mirror\logs\task_run.log" 2>&1
