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


def _norm_roots(v):
    """把各种写法的 roots 统一成 ["/a", "/b"]

    接受 list、多行字符串；顺带自愈一种坏数据：
    之前有人把整个 list 又 str() 了一遍，存成 ["['/a', '/b']"]，这里能拆回来。
    """
    if isinstance(v, (list, tuple)):
        items = [str(x) for x in v]
    else:
        items = str(v or "").replace(",", "\n").splitlines()
    out = []
    for x in items:
        t = str(x).strip().strip('"').strip("'").strip()
        if not t:
            continue
        if t.startswith("[") and t.endswith("]"):
            try:
                import ast
                inner = ast.literal_eval(t)
                if isinstance(inner, (list, tuple)):
                    out.extend(str(y).strip() for y in inner if str(y).strip())
                    continue
            except Exception:
                pass
        out.append(t)
    return out


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
    doc.setdefault("public_url", "")     # Alist 的对外地址（反代域名），留空用原地址
    doc.setdefault("relay", 0)           # 1 = 播放走本容器转发
    doc["url"] = str(doc.get("url") or "").rstrip("/")
    doc["roots"] = _norm_roots(doc.get("roots"))
    _CFG_CACHE.update(ts=now, doc=doc)
    return doc


def save_cfg(data_dir, doc):
    import state as ST
    doc = dict(doc or {})
    doc["url"] = str(doc.get("url") or "").strip().rstrip("/")
    doc["roots"] = _norm_roots(doc.get("roots"))
    doc["public_url"] = str(doc.get("public_url") or "").strip().rstrip("/")
    doc["relay"] = 1 if doc.get("relay") else 0
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

# 有些客户端（影视仓等）会把 type_id / vod_id 当数字处理，
# 所以对外一律用纯数字 id，内部再映射回路径。映射存盘，重启后不变。
_IDS = {"next": 1, "map": {}}       # map: "1" -> "/ssd128/mp4/电视剧"
_IDS_LOCK = threading.Lock()


def _ids_path(data_dir):
    return os.path.join(data_dir, "alist_ids.json")


def _ids_load(data_dir):
    global _IDS
    if _IDS.get("map") or _IDS.get("next", 1) > 1:
        return _IDS
    try:
        with open(_ids_path(data_dir), encoding="utf-8") as f:
            doc = json.load(f)
        _IDS = {"next": int(doc.get("next") or 1),
                "map": {str(k): str(v) for k, v in (doc.get("map") or {}).items()}}
    except Exception:
        _IDS = {"next": 1, "map": {}}
    return _IDS


def _ids_save(data_dir):
    try:
        import state as ST
        ST.save_file(_ids_path(data_dir), _IDS)
    except Exception:
        pass


def tid(path, data_dir=None):
    """路径 -> 数字 id（同一个路径永远同一个 id）"""
    path = str(path)
    if not data_dir:
        # 没有数据目录时退回 base64（只用于测试）
        return base64.urlsafe_b64encode(path.encode()).decode().rstrip("=")
    with _IDS_LOCK:
        ids = _ids_load(data_dir)
        for k, v in ids["map"].items():
            if v == path:
                return int(k)
        nid = int(ids["next"])
        ids["map"][str(nid)] = path
        ids["next"] = nid + 1
        _ids_save(data_dir)
        return nid


def pid(s, data_dir=None):
    """数字 id -> 路径；也兼容老的 base64 id"""
    t = str(s or "").strip()
    if not t:
        return ""
    if data_dir and t.isdigit():
        ids = _ids_load(data_dir)
        return ids["map"].get(t, "")
    try:
        return base64.urlsafe_b64decode(t + "=" * (-len(t) % 4)).decode()
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

def _classes(cfg, data_dir=None):
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
                out.append({"type_id": tid(p, data_dir), "type_name": x["name"],
                            "type_pid": 0})
        elif files:
            # root 直接就是一堆片子（比如 /电影），整体做一个分类
            nm = os.path.basename(root.rstrip("/")) or "网盘"
            if root not in seen:
                seen.add(root)
                out.append({"type_id": tid(root, data_dir), "type_name": nm,
                            "type_pid": 0})
    return out


# ------------------------------------------------------------------ 条目与详情

def _entries(cfg, path, data_dir=None):
    """目录下的条目：子目录 + 视频/音频文件，各自成条"""
    ok, items = ls(cfg, path)
    if not ok:
        return [], str(items)
    out = []
    for x in sorted(items, key=lambda y: _natkey(y.get("name"))):
        nm = str(x.get("name") or "")
        p = path.rstrip("/") + "/" + nm
        if x.get("is_dir"):
            out.append({"id": tid(p, data_dir), "name": nm, "kind": "dir",
                        "size": 0})
        elif _ispath_video(nm) or _ispath_audio(nm):
            out.append({"id": tid(p, data_dir),
                        "name": os.path.splitext(nm)[0],
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


def play_link(cfg, path, base=""):
    """给客户端用的播放地址

    三种情况：
      1) relay=1 且知道本服务的对外地址 → 走本容器转发 /alist/p（客户端只需要能访问我们）
      2) 配了 public_url（Alist 自己的反代域名）→ 把内网前缀换成它
      3) 都没有 → 原样返回，只能局域网播
    """
    if int(cfg.get("relay") or 0) and base:
        raw = base64.urlsafe_b64encode(str(path).encode()).decode().rstrip("=")
        return "%s/alist/p?p=%s" % (base.rstrip("/"), raw)
    u = raw_url(cfg, path)
    pub = cfg.get("public_url") or ""
    if u and pub and cfg.get("url") and u.startswith(cfg["url"]):
        return pub + u[len(cfg["url"]):]
    return u


def _play_url(cfg, files, limit=MAX_ITEMS, base=""):
    """并发取直链，拼成 vod_play_url"""
    files = [f for f in files if f[1]][:limit]
    if not files:
        return ""
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        urls = list(ex.map(lambda f: play_link(cfg, f[1], base), files))
    parts = []
    for (nm, _p), u in zip(files, urls):
        if not u:
            continue
        # 集名/地址里的 $ # 必须换掉，否则会串台
        parts.append("%s$%s" % (str(nm).replace("$", "＄").replace("#", "＃"),
                                u.replace("#", "%23")))
    return "#".join(parts)


# ------------------------------------------------------------------ CMS 协议

def cms(data_dir, params, base=""):
    """按 ac= 分发，返回苹果CMS风格的 dict"""
    cfg = load_cfg(data_dir)
    if not (cfg.get("url") and cfg.get("roots")):
        return {"class": [], "list": [], "page": 1, "pagecount": 0, "total": 0,
                "msg": "还没有配置 Alist（面板 → 设置 → 网盘）"}

    ac = (params.get("ac") or ["list"])[0]
    t = (params.get("t") or [""])[0]
    ids = (params.get("ids") or params.get("id") or [""])[0]
    pg = int((params.get("pg") or ["1"])[0] or 1)

    # 有的客户端（影视仓）用 ac=detail&t=<分类> 取列表，ids 为空；
    # 所以判断顺序必须是：「有 ids 才当详情」，否则交给下面的 t 分支。
    if ids:
        path = pid(ids, data_dir)
        if not path:
            return {"list": []}
        # 判断"这个 id 是文件还是目录"要看扩展名，不能看"目录里有没有子目录"——
        # 乡村爱情 S01 下面全是 01.mp4 这种文件，没有子目录，之前就被误判成单文件了
        if _ext(path) in (VIDEO_EXT | AUDIO_EXT):
            name = os.path.splitext(os.path.basename(path))[0]
            u = play_link(cfg, path, base=base)
            return {"list": [{"vod_id": ids, "vod_name": name, "vod_pic": "",
                              "vod_play_from": "网盘",
                              "vod_play_url": ("正片$" + u) if u else ""}]}
        files = _leaf_files(cfg, path)
        purl = _play_url(cfg, files, base=base)
        return {"list": [{"vod_id": ids,
                          "vod_name": os.path.basename(path.rstrip("/")),
                          "vod_pic": "", "vod_play_from": "网盘",
                          "vod_play_url": purl,
                          "vod_remarks": "%d 个文件" % len(files)}]}

    if t:
        path = pid(t, data_dir)
        entries, err = _entries(cfg, path, data_dir)
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

    return {"class": _classes(cfg, data_dir)}


# ------------------------------------------------------------------ 给输出配置注入站点

def site_entry(data_dir, base):
    """聚合时调用：把网盘做成一个 type=1 的站点

    base 是本服务的对外地址（http://ip:port），客户端直接请求 /alist
    """
    if not enabled(data_dir):
        return None
    cfg = load_cfg(data_dir)
    try:
        cls = _classes(cfg, data_dir)
    except Exception:
        cls = []
    return {"key": "alist_netdisk", "name": "网盘",
            "type": 1, "api": (base or "").rstrip("/") + "/alist",
            "searchable": 0, "quickSearch": 0,
            "ext": (cfg["url"] or ""), "_classes": len(cls)}


def test_page(params, data_dir=None):
    """网页播放测试页

    直接把播放地址塞进 <video>：浏览器能播，说明流没问题，锅在客户端；
    播不了，就是转发出问题。排查用，不做播放器。
    """
    p0 = (params.get("p") or [""])[0]
    url = ("/alist/p?p=" + urllib.parse.quote(p0)) if p0 else ""
    try:
        n = pid(p0, data_dir) if p0 else ""
    except Exception:
        n = ""
    name = os.path.basename(n) if n else "（没给 p 参数）"
    return """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<title>网盘播放测试</title>
<style>body{background:#141414;color:#ddd;font:14px system-ui;margin:0;padding:20px}
video{width:100%%;max-height:70vh;background:#000;border-radius:6px}
.m{color:#8b949e;font-size:12px;word-break:break-all;margin:8px 0}
code{background:#222;padding:1px 4px;border-radius:3px}</style></head><body>
<h3>网盘播放测试</h3>
<div class="m">文件：%s</div>
<div class="m">地址：<code>%s</code></div>
<div class="m">浏览器能播 → 流没问题，问题在客户端解码/网络；播不了 → 转发出问题。</div>
<video src="%s" controls autoplay playsinline></video>
<div class="m">提示：如果一直转圈，打开浏览器 F12 → Network 看 <code>/alist/p</code> 的状态码和大小。</div>
</body></html>""" % (name, url or "（无）", url)
