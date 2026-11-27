# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""访问统计：只记「接口 json 被拉了多少次」

从 server.py 拆出来的独立模块，不依赖 HTTP 层——Handler 只管调 hit()。
统计数据 15 秒落盘一次（write-behind），崩溃最多丢十几秒计数。
"""

import json
import os
import threading
import time

import state as ST


BUCKET_ALIAS = {"cfg:tvbox": "cfg:tvbox", "cfg:full": "cfg:tvbox",
                "cfg:all": "cfg:all", "cfg:libretv": "cfg:ltv",
                "cfg:direct": "cfg:direct",
                "live:main": "live:main", "live:cctv": "live:main",
                "live:local": "live:local", "live:other": "live:local",
                "login": "login"}
BUCKET_LABEL = {"cfg:tvbox": "/tvbox.json（默认档）",
                "cfg:all": "/all.json（全能档）",
                "cfg:ltv": "/ltv.json（LibreTV）",
                "cfg:direct": "/tvbox-direct.json（直连）",
                "live:main": "/live/cctv.m3u（央视+卫视）",
                "live:local": "/live/local.m3u（地方台）",
                "login": "管理页登录"}

ACCESS = {}                  # bucket -> {n, bytes, first, last, ua{}, hour{}, ip{}}
ACCESS_PATH = None
ACCESS_START = int(time.time())
_DIRTY = False
_LOCK = threading.Lock()


def _merge_alias(doc):
    """把旧桶名（cfg:full / live:cctv …）并到新桶里，面板上就一行"""
    out = {}
    for k, v in (doc or {}).items():
        key = BUCKET_ALIAS.get(k, k)
        if k.startswith("cms:"):
            continue
        cur = out.setdefault(key, {"n": 0, "bytes": 0, "first": v.get("first") or 0,
                                   "last": v.get("last") or 0, "ua": {}, "hour": {},
                                   "ip": {}})
        cur["n"] += int(v.get("n") or 0)
        cur["bytes"] += int(v.get("bytes") or 0)
        cur["first"] = min([x for x in (cur["first"], v.get("first")) if x] or [0])
        cur["last"] = max(cur["last"], v.get("last") or 0)
        for f in ("ua", "hour", "ip"):
            for kk, vv in (v.get(f) or {}).items():
                cur[f][kk] = cur[f].get(kk, 0) + vv
    return out


def hit(bucket, nbytes=0, ua="", ip=""):
    global _DIRTY
    bucket = BUCKET_ALIAS.get(bucket, bucket)
    now = int(time.time())
    with _LOCK:
        a = ACCESS.setdefault(bucket, {"n": 0, "bytes": 0, "first": now,
                                       "last": now, "ua": {}, "hour": {},
                                       "ip": {}})
        a["n"] += 1
        a["bytes"] += nbytes
        a["last"] = now
        if ua:
            u = ua[:36]
            a["ua"][u] = a["ua"].get(u, 0) + 1
        if ip:
            a.setdefault("ip", {})[ip] = a.setdefault("ip", {}).get(ip, 0) + 1
        h = str(now // 3600 * 3600)
        a["hour"][h] = a["hour"].get(h, 0) + 1
        if len(a["hour"]) > 48:                     # 只留最近 48 小时
            for k in sorted(a["hour"])[:-48]:
                a["hour"].pop(k, None)
        _DIRTY = True


def load(path):
    global ACCESS, ACCESS_PATH, ACCESS_START
    ACCESS_PATH = path
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        ACCESS = _merge_alias(doc.get("buckets") or {})
        ACCESS_START = int(doc.get("started") or time.time())
    except Exception:
        ACCESS = {}


def flush(force=False):
    global _DIRTY
    if not ACCESS_PATH or (not _DIRTY and not force):
        return
    with _LOCK:
        doc = {"started": ACCESS_START,
               "saved": time.strftime("%Y-%m-%d %H:%M:%S"),
               "buckets": ACCESS}
        _DIRTY = False
    try:
        ST.save_file(ACCESS_PATH, doc)
    except Exception:
        pass


def today(bucket):
    a = ACCESS.get(bucket) or {}
    today = time.strftime("%Y-%m-%d")
    return sum(v for k, v in (a.get("hour") or {}).items()
               if time.strftime("%Y-%m-%d", time.localtime(int(k))) == today)


def summary(limit=40):
    """给面板用：按请求数排序。只统计接口 json 的拉取，名字用友好标签。"""

    rows = []
    for bucket, a in ACCESS.items():
        rows.append({
            "bucket": bucket,
            "label": BUCKET_LABEL.get(bucket, bucket),
            "n": a.get("n", 0),
            "today": today(bucket),
            "bytes": a.get("bytes", 0),
            "first": a.get("first", 0),
            "last": a.get("last", 0),
            "ua": sorted((a.get("ua") or {}).items(), key=lambda kv: -kv[1])[:3],
            "ip": sorted((a.get("ip") or {}).items(), key=lambda kv: -kv[1])[:3],
            "unik": len(a.get("ip") or {}),
            "avg": int((a.get("bytes") or 0) / max(1, a.get("n") or 1)),
        })
    rows.sort(key=lambda r: -r["n"])
    return {"started": ACCESS_START, "total": sum(r["n"] for r in rows),
            "rows": rows[:limit]}


# ---------------------------------------------------------------- 管理密码
#
# 原来是一串 TOKEN，要手填到请求头里——用起来别扭。现在改成「登录管理页要密码」：
#   GET /login  输密码 → 种一个 HttpOnly Cookie → 之后 /status 和 /api/* 都认这个 Cookie
# Cookie 里存的是 sha256(密码哈希+"cookie") 的前 32 位，改密码会让旧 Cookie 全部失效。
# 也保留 ?token=xxx 的用法，方便 curl / cron。


def clear_legacy():
    """去掉旧口径（按站点记的 cms:*）留下的桶"""
    with _LOCK:
        for k in list(ACCESS.keys()):
            if k.startswith("cms:"):
                ACCESS.pop(k, None)
