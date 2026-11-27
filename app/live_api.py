# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""直播索引：缓存、后台测速、按需刷新

从 server.py 拆出来，不再直接读全局 CFG —— 数据目录和设置都当参数传进来，
这样单测里可以直接喂临时目录跑。
"""

import json
import os
import threading
import time

import live as L


CACHE = {"doc": None, "building": False}



def items(data_dir):
    p = os.path.join(data_dir, "lives.json")
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f).get("items") or []
    except Exception:
        return []


def load_disk(data_dir):
    p = os.path.join(data_dir, "live_tested.json")
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def doc(data_dir, force=False, log=print, settings=None):
    """直播索引：内存 → 磁盘缓存 → 现测

    缓存在 30 分钟内直接用；过期了也先把旧的给客户端，再后台刷新——
    测几百条流要两分钟，不能让客户端等着。
    """
    if CACHE.get("doc") and not force:
        doc = CACHE["doc"]
        if time.time() - float(doc.get("ts") or 0) < 1800:
            return doc
        if not CACHE.get("building"):
            CACHE["building"] = True
            threading.Thread(target=bg, args=(data_dir, settings), daemon=True).start()
        return doc
    if CACHE.get("building") and not force:
        # 正在后台测：有旧结果就先给旧的，别让面板/客户端吃 503
        return CACHE.get("doc") or load_disk(data_dir) or {}
    path = os.path.join(data_dir, "live_tested.json")
    if not force:
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
            CACHE["doc"] = doc
            if time.time() - float(doc.get("ts") or 0) > 1800:
                CACHE["building"] = True
                threading.Thread(target=bg, args=(data_dir, settings), daemon=True).start()
            return doc
        except Exception:
            pass
    CACHE["building"] = True
    try:
        stg = settings or {}
        doc = L.build_index(items(data_dir), data_dir,
                            log=log, force=force,
                            max_test=int(stg.get("live_max_test") or 1500))
        CACHE["doc"] = doc
        return doc
    finally:
        CACHE["building"] = False


def bg(data_dir, settings=None):
    try:
        stg = settings or {}
        doc = L.build_index(items(data_dir), data_dir,
                            log=print, force=True,
                            max_test=int(stg.get("live_max_test") or 1500))
        CACHE["doc"] = doc
    except Exception as e:
        print("直播测速失败：%s" % e)
    finally:
        CACHE["building"] = False
