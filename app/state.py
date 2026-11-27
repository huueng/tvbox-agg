# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""data/*.json 的统一读写入口

以前每个模块各写一遍 `open(tmp)` + `json.dump` + `os.replace`，有三个问题：

1. **tmp 文件名固定** —— 两个写入方同时跑（比如连点两次「解析」）会抢同一个 tmp，
   轻则内容交错，重则 replace 到一个半截文件。
2. **没有 fsync** —— 断电/容器被 kill 时可能留下 0 字节的配置。
3. 谁写了哪些文件、要不要备份，全散在代码里，看不出来。

这里统一成：唯一 tmp 名（pid+随机）→ 写完 fsync → 原子 replace → 可选留 .bak。
"""

import json
import os
import shutil
import tempfile
import time


def path(data_dir, name):
    return os.path.join(data_dir, name)


def load(data_dir, name, default=None):
    """读 json；读不到或坏了返回 default（默认 {}）"""
    try:
        with open(path(data_dir, name), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {} if default is None else default


def save(data_dir, name, obj, backup=False, indent=1):
    """原子写：临时文件 → fsync → replace

    backup=True 时把上一版留成 <name>.bak（出事了能手工捞回来）
    """
    p = path(data_dir, name)
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    if backup and os.path.exists(p):
        try:
            shutil.copy2(p, p + ".bak")
        except Exception:
            pass
    fd, tmp = tempfile.mkstemp(dir=d or ".", prefix=".w-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise
    return p


def save_file(path, obj, indent=1):
    """按完整路径原子写（调用方自己拼路径时用这个）"""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d or ".", prefix=".w-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise
    return path


def append_jsonl(data_dir, name, row):
    """追加一行（历史轮次这种只增不改的数据）"""
    p = path(data_dir, name)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    return p


def load_jsonl(data_dir, name, limit=None):
    p = path(data_dir, name)
    rows = []
    try:
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    return rows[-limit:] if limit else rows


def touch_stamp(data_dir, name, obj=None):
    """写一个带时间戳的小文件（缓存类，允许丢）"""
    obj = dict(obj or {})
    obj["ts"] = time.time()
    try:
        save(data_dir, name, obj)
    except Exception:
        pass
    return obj
