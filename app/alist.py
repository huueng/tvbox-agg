#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""Alist / OpenList 网盘浏览

把网盘目录树伪装成一个「苹果CMS v10」采集站，TVBox 系客户端当普通站点用：
    /alist?ac=list                   → 分类（网盘里的目录）
    /alist?ac=videolist&t=<tid>      → 该目录下的条目（子目录 / 视频文件）
    /alist?ac=detail&ids=<tid>       → 详情：目录则展开成选集，文件则单集

这样做的好处：不需要 jar、不需要 py 脚本、客户端零改动。

约定
  * 分类、条目的 id 都是 base64(path)，省得 URL 里出现斜杠和中文
  * 视频按扩展名认，音频单独归到「音频」分类，其它文件不显示
  * 播放地址实时向 Alist 要（/api/fs/get 的 raw_url，带签名），缓存 5 分钟
"""

import base64
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request as urlreq
import concurrent.futures as cf

VIDEO_EXT = {".mp4", ".mkv", ".avi", ".ts", ".m2ts", ".mov", ".wmv", ".flv",
             ".rmvb", ".webm", ".m4v", ".mpg", ".mpeg", ".iso"}
AUDIO_EXT = {".mp3", ".flac", ".m4a", ".aac", ".wav", ".ape", ".ogg", ".wma",
             ".opus", ".dsf", ".dff"}
MAX_ITEMS = 60          # 详情页最多给几集，太多客户端会卡
RAW_TTL = 300           # 播放直链缓存秒数

_RAW = {}               # path -> (expire_ts, url)
_RAW_LOCK = threading.Lock()
_CFG_CACHE = {"ts": 0.0, "doc": None}


def cfg_path(data_dir):
    return os.path.join(data_dir, "alist.json")


def load_cfg(data_dir):
    """读 data/alist.json；60 秒缓存，改完不用重启"""
    now = time.time()
    if _CFG_CACHE["doc"] is not None and now - _CFG_CACHE["ts"] < 60:
        return _CFG_CACHE["doc"]
    try:
        with open(cfg_path(data_dir), encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        doc = {}
    doc.setdefault("url", "")
    doc.setdefault("token", "")
    doc.setdefault("roots", [])
    doc.setdefault("as_home", 0)
    doc["url"] = str(doc.get("url") or "").rstrip("/")
    doc["roots"] = [str(x).strip() for x in (doc.get("roots") or []) if str(x).strip()]
    _CFG_CACHE.update(ts=now, doc=doc)
    return doc


def save_cfg(data_dir, doc):
    import state as ST
    doc = dict(doc or {})
    doc["url"] = str(doc.get("url") or "").strip().rstrip("/")
    doc["roots"] = [str(x).strip() for x in (doc.get("roots") or []) if str(x).strip()]
    doc["as_home"] = 1 if doc.get("as_home") else 0
    ST.save_file(cfg_path(data_dir), doc)
    _CFG_CACHE.update(ts=0.0, doc=None)
    return doc


def enabled(data_dir):
    c = load_cfg(data_dir)
    return bool(c.get("url") and c.get("roots"))


# ------------------------------------------------------------------ 与 Alist 通信

def _call(cfg, path, body, timeout=12):
    """POST 到 Alist；返回 (ok, data_or_error)"""
    url = cfg["url"] + path
    data = json.dumps(body or {}).encode()
    hdr = {"Content-Type": "application/json"}
    tok = (cfg.get("token") or "").strip()
    if tok:
        hdr["Authorization"] = tok
    req = urlreq.Request(url, data=data, headers=hdr)
    try:
        raw = urlreq.urlopen(req, timeout=timeout).read()
        doc = json.loads(raw)
    except urllib.error.HTTPError as e:
        return False, "HTTP %s" % e.code
    except Exception as e:                                  # noqa: BLE001
        return False, "%s: %s" % (type(e).__name__, e)
    if doc.get("code") not in (200, None):
        return False, str(doc.get("message") or "接口返回错误")
    return True, doc.get("data") or {}


def ls(cfg, path, page=1, per_page=200):
    ok, d = _call(cfg, "/api/fs/list", {
        "path": path, "password": "", "page": page,
        "per_page": per_page, "refresh": False})
    if not ok:
        return False, d
    return True, d.get("content") or []


def raw_url(cfg, path):
    """取播放直链（带签名），带 5 分钟缓存"""
    now = time.time()
    with _RAW_LOCK:
        hit = _RAW.get(path)
        if hit and hit[0] > now:
            return hit[1]
    ok, d = _call(cfg, "/api/fs/get", {"path": path, "password": ""})
    if not ok:
        return ""
    url = str(d.get("raw_url") or "")
    if not url and d.get("is_dir") is not True and d.get("name"):
        url = "%s/d%s" % (cfg["url"], urllib.parse.quote(path))
    if url:
        with _RAW_LOCK:
            if len(_RAW) > 2000:
                _RAW.clear()
            _RAW[path] = (now + RAW_TTL, url)
    return url


# ------------------------------------------------------------------ 路径 <-> id

def tid(path):
    return base64.urlsafe_b64encode(str(path).encode()).decode().rstrip("=")


def pid(s):
    s = str(s or "")
    try:
        return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()
    except Exception:
        return ""


def _ext(name):
    return os.path.splitext(str(name).lower())[1]


def _ispath_video(name):
    return _ext(name) in VIDEO_EXT


def _ispath_audio(name):
    return _ext(name) in AUDIO_EXT


def _natkey(name):
    """自然排序：第2集 排在 第10集 前面"""
    return [int(t) if t.isdigit() else t.lower()
            for t in re.split(r"(\d+)", str(name))]


def _human(n):
    try:
        n = float(n or 0)
    except Exception:
        return ""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return ("%.0f%s" % (n, unit)) if unit in ("B", "KB") else ("%.1f%s" % (n, unit))
        n /= 1024.0
    return ""


# ------------------------------------------------------------------ 分类

def _classes(cfg):
    """一级分类 = 每个 root 下的子目录；root 本身是内容目录时也算一个分类

    返回 [{"type_id": tid, "type_name": 名字, "type_pid": 0}, ...]
    """
    out = []
    seen = set()
    for root in cfg.get("roots") or []:
        ok, items = ls(cfg, root)
        if not ok:
            continue
        dirs = [x for x in items if x.get("is_dir")]
        files = [x for x in items if not x.get("is_dir")
                 and (_ispath_video(x.get("name")) or _ispath_audio(x.get("name")))]
        if dirs:
            for x in dirs:
                p = root.rstrip("/") + "/" + x["name"]
                if p in seen:
                    continue
                seen.add(p)
                out.append({"type_id": tid(p), "type_name": x["name"], "type_pid": 0})
        elif files:
            # root 直接就是一堆片子（比如 /电影），整体做一个分类
            nm = os.path.basename(root.rstrip("/")) or "网盘"
            if root not in seen:
                seen.add(root)
                out.append({"type_id": tid(root), "type_name": nm, "type_pid": 0})
    return out


# ------------------------------------------------------------------ 条目与详情

def _entries(cfg, path):
    """目录下的条目：子目录 + 视频/音频文件，各自成条"""
    ok, items = ls(cfg, path)
    if not ok:
        return [], str(items)
    out = []
    for x in sorted(items, key=lambda y: _natkey(y.get("name"))):
        nm = str(x.get("name") or "")
        p = path.rstrip("/") + "/" + nm
        if x.get("is_dir"):
            out.append({"id": tid(p), "name": nm, "kind": "dir", "size": 0})
        elif _ispath_video(nm) or _ispath_audio(nm):
            out.append({"id": tid(p), "name": os.path.splitext(nm)[0],
                        "kind": "file", "size": x.get("size") or 0})
    return out, ""


def _leaf_files(cfg, path, depth=2):
    """把一个目录展开成叶子文件（最多两层的目录视为「一季/一部」）"""
    ok, items = ls(cfg, path)
    if not ok:
        return []
    vids, subs = [], []
    for x in items:
        nm = str(x.get("name") or "")
        p = path.rstrip("/") + "/" + nm
        if x.get("is_dir"):
            if depth > 0:
                subs.append((nm, p))
        elif _ispath_video(nm) or _ispath_audio(nm):
            vids.append((nm, p))
    out = [(os.path.splitext(n)[0], p) for n, p in sorted(vids, key=lambda z: _natkey(z[0]))]
    for nm, p in sorted(subs, key=lambda z: _natkey(z[0])):
        for n2, p2 in _leaf_files(cfg, p, depth - 1):
            # 多季的情况：加上目录名区分
            out.append(("%s %s" % (nm, n2), p2))
    return out


def _play_url(cfg, files, limit=MAX_ITEMS):
    """并发取直链，拼成 vod_play_url"""
    files = [f for f in files if f[1]][:limit]
    if not files:
        return ""
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        urls = list(ex.map(lambda f: raw_url(cfg, f[1]), files))
    parts = []
    for (nm, _p), u in zip(files, urls):
        if not u:
            continue
        # 集名/地址里的 $ # 必须换掉，否则会串台
        parts.append("%s$%s" % (str(nm).replace("$", "＄").replace("#", "＃"),
                                u.replace("#", "%23")))
    return "#".join(parts)


# ------------------------------------------------------------------ CMS 协议

def cms(data_dir, params):
    """按 ac= 分发，返回苹果CMS风格的 dict"""
    cfg = load_cfg(data_dir)
    if not (cfg.get("url") and cfg.get("roots")):
        return {"class": [], "list": [], "page": 1, "pagecount": 0, "total": 0,
                "msg": "还没有配置 Alist（面板 → 设置 → 网盘）"}

    ac = (params.get("ac") or ["list"])[0]
    t = (params.get("t") or [""])[0]
    ids = (params.get("ids") or params.get("id") or [""])[0]
    pg = int((params.get("pg") or ["1"])[0] or 1)

    if ac == "detail" or ids:
        path = pid(ids)
        if not path:
            return {"list": []}
        # 判断"这个 id 是文件还是目录"要看扩展名，不能看"目录里有没有子目录"——
        # 乡村爱情 S01 下面全是 01.mp4 这种文件，没有子目录，之前就被误判成单文件了
        if _ext(path) in (VIDEO_EXT | AUDIO_EXT):
            name = os.path.splitext(os.path.basename(path))[0]
            u = raw_url(cfg, path)
            return {"list": [{"vod_id": ids, "vod_name": name, "vod_pic": "",
                              "vod_play_from": "网盘",
                              "vod_play_url": ("正片$" + u) if u else ""}]}
        files = _leaf_files(cfg, path)
        purl = _play_url(cfg, files)
        return {"list": [{"vod_id": ids,
                          "vod_name": os.path.basename(path.rstrip("/")),
                          "vod_pic": "", "vod_play_from": "网盘",
                          "vod_play_url": purl,
                          "vod_remarks": "%d 个文件" % len(files)}]}

    if t:
        path = pid(t)
        entries, err = _entries(cfg, path)
        if err:
            return {"list": [], "page": 1, "pagecount": 0, "total": 0, "msg": err}
        per = 40
        start = (pg - 1) * per
        page_items = entries[start:start + per]
        return {"list": [{"vod_id": e["id"], "vod_name": e["name"], "vod_pic": "",
                          "vod_remarks": ("目录" if e["kind"] == "dir"
                                          else _human(e["size"]))}
                         for e in page_items],
                "page": pg,
                "pagecount": max(1, (len(entries) + per - 1) // per),
                "total": len(entries), "limit": str(per)}

    return {"class": _classes(cfg)}


# ------------------------------------------------------------------ 给输出配置注入站点

def site_entry(data_dir, base):
    """聚合时调用：把网盘做成一个 type=1 的站点

    base 是本服务的对外地址（http://ip:port），客户端直接请求 /alist
    """
    if not enabled(data_dir):
        return None
    cfg = load_cfg(data_dir)
    try:
        cls = _classes(cfg)
    except Exception:
        cls = []
    return {"key": "alist_netdisk", "name": "网盘",
            "type": 1, "api": (base or "").rstrip("/") + "/alist",
            "searchable": 0, "quickSearch": 0,
            "ext": (cfg["url"] or ""), "_classes": len(cls)}
