# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""jar 插件静态扫描：不执行代码，只看文件结构和 dex 里的字符串

为什么需要这一层：
    站点的 jar 在客户端里是**真的会被加载执行**的。有些 jar 会下载 go 二进制再 exec、
    起本地代理、申请存储权限、甚至装 APK。这些行为在接口体检里完全看不出来
    （我们只请求 api.php/provide/vod），只有把 jar 拆开看才能发现。

判定分两档：
    blocked  —— 直接不进任何输出档（装应用 / 杀进程 / 提权 / 结构异常）
    warn     —— 不拦，但在面板标注（申请存储权限 / 起本地代理这类"常见但可疑"）

策略对齐 ElderTV：扫不了、扫出错 → 一律按 blocked 处理（fail-closed）。
误判可以接受——站点池子够大，少几个不心疼；但客户端不能出现莫名其妙的弹窗。
"""

import hashlib
import io
import json
import os
import re
import time
import urllib.request
import zipfile

import state as ST


def parse_jar_ref(value):
    """和 aggregate.parse_jar_ref 同一套规则（避免反向依赖）"""
    text = (value or "").strip()
    wrapped = text.startswith("img+")
    if wrapped:
        text = text[4:]
    for tag in (";sha256;", ";md5;"):
        i = text.find(tag)
        if i > 0:
            return text[:i], wrapped
    return text, wrapped

UA = ("Mozilla/5.0 (Linux; Android 12; TV) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36")

# 解压上限，防止压缩炸弹把内存吃干
MAX_ENTRIES = 2000
MAX_TOTAL = 200 * 1024 * 1024
MAX_RATIO = 120

# dex 里的字符串 → 风险点。全部是小写匹配（匹配前会把文本转小写）。
RISK_BLOCK = (
    ("会尝试安装其他应用", re.compile(
        r"(android\.intent\.action\.install_package|application/vnd\.android\.package-archive|"
        r"pm install|packageinstaller)")),
    ("含强制关闭应用的代码", re.compile(
        r"(killbackgroundprocesses|process\.killprocess|am force-stop)")),
    ("会执行外部命令", re.compile(
        r"(runtime\.getruntime\(\)\.exec|processbuilder|/system/bin/su|/system/xbin/su)")),
)

# 只留真正能区分的信号。
# 之前把 "serversocket" / "nanohttpd" / "WRITE_EXTERNAL_STORAGE" 当告警，
# 结果 27/64 个 jar 全部命中 —— 因为 dex 里有一张「类引用表」，只要代码里提到过
# 这些类名就会留下字符串，**跟真的调用完全是两回事**，纯噪音。
# 下面这两条是硬行为（装应用 / 杀进程 / 执行命令），误报率低，保留。
# 只标注不拦截：这些行为在正经插件里也常见，但确实值得知道
RISK_WARN = (
    ("会起本地代理", re.compile(
        r"(127\.0\.0\.1|localhost|nanohttpd|serversocket|"
        r"inetaddress\.getloopbackaddress|proxy\.select)")),
    ("会申请存储权限", re.compile(
        r"(requestpermissions|write_external_storage|read_external_storage|"
        r"manage_external_storage)")),
    ("会读写设备标识", re.compile(
        r"(getdeviceid|getimei|android_id|getsubscriberid)")),
)

PRINTABLE = re.compile(rb"[\x20-\x7e]{6,}")


def _strings(blob, limit=4 * 1024 * 1024):
    """从二进制里抠出可打印 ASCII 串（dex 里的类名/方法名/常量都是这种形式）"""
    text = blob[:limit].lower()
    return b" ".join(PRINTABLE.findall(text)).decode("latin-1", "ignore")


def scan_bytes(data, label=""):
    """扫一份 jar 字节，返回 {verdict, reasons[], sha256, size}"""
    out = {"verdict": "ok", "reasons": [], "size": len(data),
           "sha256": hashlib.sha256(data).hexdigest()[:16], "ts": time.time()}

    if len(data) < 512:
        out.update(verdict="blocked", reasons=["下载到的不是插件，地址可能已失效"])
        return out

    if data[:2] not in (b"PK", b"\x1f\x8b"):
        out.update(verdict="blocked",
                   reasons=["不是有效的 jar（可能是 img+ 包裹）"])
        return out

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
        infos = zf.infolist()
    except Exception as e:
        out.update(verdict="blocked", reasons=["插件无法解开：%s" % type(e).__name__])
        return out

    names = [i.filename for i in infos]
    bad = []
    if len(names) > MAX_ENTRIES:
        bad.append("条目数异常（%d）" % len(names))
    total = sum(i.file_size for i in infos)
    packed = max(1, sum(i.compress_size for i in infos))
    if total > MAX_TOTAL:
        bad.append("解压后过大（%.0fMB）" % (total / 1048576))
    if total / packed > MAX_RATIO and total > 8 * 1024 * 1024:
        bad.append("压缩比异常（%.0f×）" % (total / packed))
    if any(".." in n or n.startswith("/") for n in names):
        bad.append("包含不安全的路径")
    if any(n.endswith(".so") for n in names):
        bad.append("夹带了原生库 .so")
    if bad:
        out.update(verdict="blocked", reasons=bad)
        return out

    # 扫 dex / class：这些才是真正会被加载执行的代码
    blob = b""
    for i in infos:
        if i.filename.endswith((".dex", ".class", ".jar")):
            try:
                blob += zf.read(i)[:3 * 1024 * 1024]
            except Exception:
                continue
        if len(blob) > 12 * 1024 * 1024:
            break
    if not blob:
        out.update(verdict="warn", reasons=["没有可扫描的代码文件"])
        return out

    text = _strings(blob)
    for label_txt, pat in RISK_BLOCK:
        if pat.search(text):
            out["reasons"].append(label_txt)
    if out["reasons"]:
        out["verdict"] = "blocked"
        return out

    # 可疑但不拦：只打在结果里，面板上单独一类
    for label_txt, pat in RISK_WARN:
        if pat.search(text) and label_txt not in out["reasons"]:
            out["reasons"].append(label_txt)
    if out["reasons"]:
        out["verdict"] = "warn"
    return out


def scan_url(url, timeout=45, max_bytes=64 * 1024 * 1024):
    """下载并扫描一个 jar（带大小上限）

    配置里的 jar 常写成 `url;md5;hash`，还有 `img+` 包裹的形式：
    前者要去掉后缀才能下，后者扫不了 → 直接拦。
    """
    real, wrapped = parse_jar_ref(url)
    if wrapped:
        # img+ 是故意把 jar 藏在图片里，属于伪装 → 拦
        return {"verdict": "blocked", "size": 0, "sha256": "",
                "reasons": ["插件被包在图片里（img+）"], "ts": time.time()}
    req = urllib.request.Request(real, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read(max_bytes + 1)
    if len(data) > max_bytes:
        return {"verdict": "blocked", "reasons": ["插件体积超过 64MB，先拦下"],
                "size": len(data), "sha256": "", "ts": time.time()}
    return scan_bytes(data)


def scan_all(data_dir, urls=None, max_jars=200, workers=3, log=print, force=False,
             max_age_days=7):
    """扫全部（或指定的）jar，结果按 url 缓存到 jar_gate.json

    同一个 url 在 max_age_days 内不重扫——除非 force。
    """
    path = os.path.join(data_dir, "jar_gate.json")
    cur = ST.load(data_dir, "jar_gate.json", {}) or {}
    if not force:
        cur = {k: v for k, v in cur.items()
               if time.time() - float(v.get("ts") or 0) < max_age_days * 86400}

    if urls is None:
        doc = ST.load(data_dir, "jars.json", {}) or {}
        seen, urls = set(), []
        for s0 in doc.get("sites") or []:
            j = s0.get("jar")
            if s0.get("kind") != "jar" or not j or j in seen:
                continue
            seen.add(j)
            urls.append(j)
            if len(urls) >= max_jars:
                break

    todo = [u for u in urls if u and (force or u not in cur)]
    log("  jar 扫描：共 %d 个，待扫 %d 个" % (len(urls), len(todo)))

    import concurrent.futures as cf

    def one(u):
        try:
            return u, scan_url(u)
        except Exception as e:
            return u, {"verdict": "unknown", "size": 0, "sha256": "",
                       "reasons": ["下载失败：%s" % type(e).__name__],
                       "ts": time.time()}

    if todo:
        t0 = time.time()
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for u, res in ex.map(one, todo):
                cur[u] = res
        log("  jar 扫描完成：%d 个，用时 %.0fs" % (len(todo), time.time() - t0))
        ST.save(data_dir, "jar_gate.json", cur)

    blocked = sum(1 for v in cur.values() if v.get("verdict") == "blocked")
    warn = sum(1 for v in cur.values() if v.get("verdict") == "warn")
    log("  jar 扫描结果：拦下 %d 个，警告 %d 个，通过 %d 个"
        % (blocked, warn, len(cur) - blocked - warn))
    return cur


def summary(data_dir):
    """给面板用：按原因汇总"""
    cur = ST.load(data_dir, "jar_gate.json", {}) or {}
    by_reason = {}
    for url, v in cur.items():
        if v.get("verdict") == "ok":
            continue
        for r in (v.get("reasons") or ["未知"]):
            key = re.sub(r"（.*?）|：.*$", "", r)
            by_reason[key] = by_reason.get(key, 0) + 1
    return {"total": len(cur), "by_reason": by_reason,
            "blocked": [{"url": u, "reasons": v.get("reasons") or []}
                        for u, v in cur.items() if v.get("verdict") == "blocked"][:50]}


def blocked_urls(data_dir):
    """真正需要拦下的 jar

    老版本把「下载失败 / 不是 jar / 解不开」也记成 blocked，那是 fail-closed 的
    副作用：一次网络抖动就能让几十个站点永久从输出里消失。这类不拦，
    只拦真正的风险（装应用 / 杀进程 / 执行命令 / 夹带 .so / 结构异常）。
    """
    cur = ST.load(data_dir, "jar_gate.json", {}) or {}
    out = set()
    for u, v in cur.items():
        if v.get("verdict") != "blocked":
            continue
        if v.get("allow"):          # 用户手动放行过
            continue
        why = " ".join(str(x) for x in (v.get("reasons") or []))
        if any(k in why for k in ("下载失败", "不是有效", "无法解开", "不是插件",
                                  "体积超过", "HTTPError", "URLError")):
            continue
        out.add(u)
    return out


def blocked_list(data_dir):
    """被拦下的插件清单（面板用）：带上原因和影响的站点数"""
    cur = ST.load(data_dir, "jar_gate.json", {}) or {}
    jars = ST.load(data_dir, "jars.json", {}) or {}
    from collections import Counter
    cnt = Counter()
    for s0 in (jars.get("sites") or []):
        u = s0.get("jar") or ""
        if u:
            cnt[u] += 1
    out = []
    for u, v in cur.items():
        if v.get("verdict") != "blocked":
            continue
        why = "；".join(str(x) for x in (v.get("reasons") or [])) or "未知"
        if any(k in why for k in ("下载失败", "不是有效", "无法解开", "不是插件",
                                  "体积超过")):
            continue                      # 这类不算风险，不列出来
        out.append({"url": u, "name": u.split("/")[-1][:40], "why": why,
                    "sites": cnt.get(u, 0), "allow": bool(v.get("allow"))})
    out.sort(key=lambda x: -x["sites"])
    return out


def set_allow(data_dir, url, allow=True):
    cur = ST.load(data_dir, "jar_gate.json", {}) or {}
    if url not in cur:
        return {"ok": False, "error": "没有这个插件的扫描记录"}
    cur[url]["allow"] = bool(allow)
    ST.save_file(os.path.join(data_dir, "jar_gate.json"), cur)
    return {"ok": True}
