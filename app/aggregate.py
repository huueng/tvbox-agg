#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""TVBox 源聚合器 —— 最小可用版

链路：sources.json（种子） → 真实体检 → 合并去重 → 同时输出两份 profile
      out/tvbox.json       TVBox 单仓 JSON（对外地址 /tvbox）
      out/all.json         全能档（对外 /tvbox-all）
      out/ltv.json         LibreTV-SourceList v2（对外 /libretv）
      out/tvbox-direct.json 只 CMS 不走代理（内部用，不再对外）
      out/report.json / out/report.md  变更与过滤报告

设计约束（见交接文档 §10）：
  * 服务端不「转换」源类型，只做筛选 + 裁剪 + 打包
  * 健康检测必须真拉一次列表，拿到播放地址才算活
  * 单文件、标准库、无第三方依赖

用法：
  python aggregate.py                     # 常规聚合
  python aggregate.py --out out           # 指定输出目录
  python aggregate.py --workers 6         # 并发数
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import ssl
import sys

import state as ST
import time
import urllib.error
import urllib.parse
import urllib.request
import concurrent.futures as cf
from urllib.parse import urlsplit, urlunsplit

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

HERE = os.path.dirname(os.path.abspath(__file__))

# 生成 site.key 时要剥掉的常见子域前缀
HOST_PREFIXES = ("www.", "api.", "caiji.", "cj.", "m.", "v.", "vod.", "json.")
# 内网/本机地址直接拒绝（LibreTV 侧本来也会过滤，这里提前挡掉）
PRIVATE_RE = re.compile(
    r"^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|0\.|169\.254\.)", re.I)


# ---------------------------------------------------------------- 基础工具

def norm(url):
    """补协议头、中文域名转 punycode、路径百分号编码"""
    url = (url or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "http://" + url
    # 有些配置里「代理前缀 + 完整 URL」被写成了单斜杠（https:/raw...），顺手修掉；
    # 不修的话 gh-proxy 这类地址会 404，站点整条报废
    url = re.sub(r"(?<=/)(https?):/(?![/])", r"\1://", url)
    p = urlsplit(url)
    host = p.hostname or ""
    try:
        host = host if host.isascii() else host.encode("idna").decode()
    except Exception:
        pass
    netloc = host + (":%d" % p.port if p.port else "")
    return urlunsplit((p.scheme, netloc,
                       urllib.parse.quote(p.path, safe="/%:@&=+$,;~()[]!*'"),
                       urllib.parse.quote(p.query, safe="=&%:@+,;/?:~()[]!*'"), ""))


def with_query(base, query):
    """把 ac=xxx&pg=1 拼到接口上（接口本身可能已带 query）"""
    return base + ("&" if "?" in base else "?") + query


def fetch(url, timeout=15, cap=3000000, retries=2, done=None, deadline=None,
          ua=None):
    """带退避重试 + 分块读取 + 整体时限

    * 公开采集站常见 SSL 握手超时 → 单次超时不能判死，要重试
    * 有的站能连但传输极慢、响应体很大（>400KB）→ 分块读，并用 done(buf)
      判断「已收完」，收完就停，不靠猜长度
    * 每次 read 的超时只管单次读，慢站会一点点滴数据把总时长拖到几分钟
      → 这里加一个整体 deadline（默认 2.5×timeout）兜底
    * 中途卡住时保留已读数据，由调用方判断截断是否仍算活
    """
    deadline = deadline if deadline is not None else timeout * 2.5
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(norm(url), headers={
                "User-Agent": ua or UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "close",
            })
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                buf = bytearray()
                while len(buf) < cap:
                    if time.time() - t0 > deadline:
                        break                      # 整体超时：用已读到的数据
                    try:
                        chunk = r.read(min(65536, cap - len(buf)))
                    except (socket.timeout, TimeoutError):
                        break                      # 传一半卡住：保留已有数据
                    if not chunk:
                        break
                    buf += chunk
                    if done and done(bytes(buf)):
                        break
            if buf:
                return bytes(buf)
            last = IOError("空响应")
        except urllib.error.HTTPError:
            raise                      # 4xx/5xx 是确定答案，不重试
        except Exception as e:
            last = e
        if i < retries:
            time.sleep(1.0 + i)
    raise last


def prefix_items(txt, limit=50):
    """从被限速截断的 JSON 里，把已完整的 list 元素抠出来

    慢站/共享代理常在 64KB 处卡死，此时整体 JSON 不合法，但前几条数据是完整的，
    足够判断「接口是否真的返回可播内容」。
    """
    m = re.search(r'"list"\s*:\s*\[', txt)
    if not m:
        return []
    i, n, depth, start, out = m.end(), len(txt), 0, None, []
    in_str = esc = False
    while i < n and len(out) < limit:
        c = txt[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "{":
            if depth == 0:
                start = i
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    out.append(json.loads(txt[start:i + 1], strict=False))
                except Exception:
                    pass
                start = None
        elif c == "]" and depth == 0:
            break
        i += 1
    return out


def _done_doc(b):
    """判断响应是否已完整：JSON 能解析 或 XML 已到 </rss>"""
    s = b.lstrip()
    if not s:
        return False
    if s[0:1] == b"{":
        if b.count(b"{") != b.count(b"}") or not b.rstrip().endswith(b"}"):
            return False
        return loads_loose(s.decode("utf-8", "replace")) is not None
    if s[0:1] == b"<":
        return b.rstrip().endswith(b"</rss>")
    return False


def strip_json_comments(text):
    """去掉 // 和 /* */ 注释，但**不碰字符串里的内容**

    以前用 `re.sub(r"//[^\n]*", "")` 一把梭，会把 "http://..." 里的 // 也剁掉，
    整份配置直接变非法 JSON —— cttv.vip 那种带注释的配置就是这么被判成「解析不出」的。
    """
    out, i, n = [], 0, len(text)
    in_str = esc = False
    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            i += 1
            continue
        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue
        # 行首的 # 也是注释（Gson 宽松模式认，实测 hgyx.json 里有 "## json" 这种）
        if ch == "#":
            ls = text.rfind("\n", 0, i) + 1
            if text[ls:i].strip() == "":
                j = text.find("\n", i)
                i = n if j < 0 else j
                continue
        if ch == "/" and i + 1 < n:
            nxt = text[i + 1]
            if nxt == "/":
                j = text.find("\n", i)
                i = n if j < 0 else j
                continue
            if nxt == "*":
                j = text.find("*/", i + 2)
                i = n if j < 0 else j + 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def loads_loose(text):
    """容忍 BOM / // 注释 / 尾逗号 / 字符串内裸控制字符"""
    clean = strip_json_comments(text)
    cands = [text, text.lstrip("\ufeff"), clean, clean.lstrip("\ufeff")]
    cands.append(re.sub(r",\s*([}\]])", r"\1", text))
    cands.append(re.sub(r",\s*([}\]])", r"\1", clean))
    for s in cands:
        try:
            return json.loads(s, strict=False)
        except Exception:
            continue
    return None


def load_config_urls(data_dir):
    """配置接口分成启用 / 备选两组 url"""
    doc = {}
    try:
        with open(os.path.join(data_dir, "configs.json"), encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        doc = {}
    active, bench = set(), set()
    for c in doc.get("configs") or []:
        u = (c.get("url") or "").strip()
        if not u:
            continue
        (bench if c.get("tier") == "bench" else active).add(u)
    return active, bench


def src_allowed(entry, active):
    """这个站点该不该参与聚合

    只聚合「启用的配置接口」解析出来的站点；备选配置接口解析出来的站点只做数量统计，
    不进池子。手工加的（from 为空或非 http）一律保留。
    """
    if entry.get("orphan"):
        # 离源：原配置接口已经删了，但它带进来的站点还在用着，不该跟着丢
        return True
    f = str(entry.get("from") or "")
    if f.startswith("config:"):
        # 老格式只记了名字，判断不出来源 —— 重建时一并清掉，让启用的配置接口重拉
        return False
    if not f or not f.startswith("http"):
        return True          # 手工加的
    return f in active


def orphan_of_config(data_dir, url, name=""):
    """配置接口被删掉时，把它带进来的站点/扩展/直播源标记为「离源」

    离源条目不依赖原接口，继续参与聚合；但它们自己仍然走「连续失败就移除」，
    被移除就是真移除，以后原接口再带回来算新来源。
    """
    url = str(url or "").strip()
    if not url:
        return 0
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    tag = {"orphan": 1, "orphaned_from": url, "orphaned_name": name or "",
           "orphaned_at": now}
    n = 0
    sp = os.path.join(data_dir, "sources.json")
    try:
        doc = load_sources_doc(sp)
        hit = 0
        for x in doc.get("sources") or []:
            if str(x.get("from") or "") == url:
                x.update(tag)
                hit += 1
        if hit:
            dump_sources_doc(sp, doc)
            n += hit
    except Exception:
        pass
    for fn, key in (("jars.json", "sites"), ("lives.json", "items")):
        fp = os.path.join(data_dir, fn)
        try:
            with open(fp, encoding="utf-8") as f:
                d2 = json.load(f)
            hit = 0
            for x in d2.get(key) or []:
                if str(x.get("from") or "") == url:
                    x.update(tag)
                    hit += 1
            if hit:
                ST.save_file(fp, d2)
                n += hit
        except Exception:
            pass
    return n


def migrate_source_origin(data_dir):
    """把老格式的 from="config:名字" 改写成配置接口的 url

    以前 _merge_into_sources 记的是名字，按来源过滤时判断不出来源，
    结果「配置解析出来的 CMS 全被当成野站过滤掉」。这里按名字反查 configs.json 修回来。
    """
    try:
        with open(os.path.join(data_dir, "configs.json"), encoding="utf-8") as f:
            reg = json.load(f)
    except Exception:
        return 0
    by_name = {}
    for c in reg.get("configs") or []:
        u = (c.get("url") or "").strip()
        if u:
            by_name[(c.get("name") or "").strip()] = u
    sp = os.path.join(data_dir, "sources.json")
    doc = load_sources_doc(sp)
    n = 0
    for x in doc.get("sources") or []:
        f = str(x.get("from") or "")
        if not f.startswith("config:"):
            continue
        nm = f[len("config:"):].strip()
        url = by_name.get(nm)
        if url:
            x["from"] = url
            x["src_name"] = nm
            n += 1
    if n:
        ST.save_file(sp, doc)
    return n


def purge_unselected(data_dir):
    """把备选配置接口解析出来的站点从池子里清掉（重建用）"""
    active, bench = load_config_urls(data_dir)
    removed = {"jars": 0, "sources": 0}
    doc = {}
    try:
        with open(os.path.join(data_dir, "jars.json"), encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        doc = {}
    if doc.get("sites"):
        keep = [x for x in doc["sites"] if src_allowed(x, active)]
        removed["jars"] = len(doc["sites"]) - len(keep)
        doc["sites"] = keep
        ST.save_file(os.path.join(data_dir, "jars.json"), doc)
    sp = os.path.join(data_dir, "sources.json")
    sdoc = load_sources_doc(sp)
    if sdoc.get("sources"):
        keep = [x for x in sdoc["sources"] if src_allowed(x, active)]
        removed["sources"] = len(sdoc["sources"]) - len(keep)
        sdoc["sources"] = keep
        ST.save_file(sp, sdoc)
    return removed


def site_key(api):
    """由域名生成稳定、唯一、纯 ASCII 的 key（TVBox 会拿它当 URL 参数）"""
    host = (urlsplit(norm(api)).hostname or "").lower()
    port = urlsplit(norm(api)).port
    for pre in HOST_PREFIXES:
        if host.startswith(pre):
            host = host[len(pre):]
    if re.fullmatch(r"[\d.]+", host):          # 纯 IP
        base = host.replace(".", "-")
        if port:
            base += "-%d" % port
    else:
        base = host.split(".")[0] or host
    base = re.sub(r"[^a-z0-9\-]", "-", base).strip("-")
    return base or "site"


def is_private(api):
    host = (urlsplit(norm(api)).hostname or "").lower()
    return bool(PRIVATE_RE.match(host)) or host in ("", "::1")


def clean_name(name):
    name = (name or "").strip().strip("❤♥★☆ ").strip()
    return name or "未命名"


# 站点名清洗：参考 ElderTV 的 NameRules
# 去掉「」[] 标签、emoji/符号、首尾分隔符，但**保留 ┃**（很多站用它当词间分隔）
_NAME_TAGS = re.compile(r"「[^」]*」|\[[^\]]*]|［[^］]*］|【[^】]*】")
_NAME_EMOJI = re.compile(
    "[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF\u2190-\u21FF"
    "\u3297\u3299\u303D\uFE0F\uFE0E\u200D\u20E3\U000E0020-\U000E007F]")
_NAME_EDGES = re.compile(r"^[\s|｜/／\-—·•、,，]+|[\s|｜/／\-—·•、,，]+$")
_NAME_SPACES = re.compile(r"\s{2,}")
_NAME_RUNS = re.compile(r"[|｜/／\-—·]{2,}")


def clean_site_name(name, keep_emoji=False):
    v = (name or "").strip()
    for r in (RULES.get("name_rules") or []):
        f = r.get("find")
        if f:
            try:
                v = re.sub(f, r.get("replace") or "", v)
            except re.error:
                v = v.replace(f, r.get("replace") or "")
    if not keep_emoji:
        v = _NAME_EMOJI.sub("", v)
    v = _NAME_TAGS.sub("", v)
    v = _NAME_RUNS.sub("·", v)
    v = _NAME_EDGES.sub("", v)
    v = _NAME_SPACES.sub(" ", v).strip()
    return v or clean_name(name)


# ---------------------------------------------------------------- 体检

def detect_param(api, tid):
    """探出这个站「分类浏览」认哪个参数名：t 还是 type_id

    实测：采集站（苹果CMS 精简版）多数只认 type_id，收到 t= 会返回空列表，
    于是 TVBox 点分类一片空白。返回 None 表示两种都不认（只能靠搜索）。
    """
    for p in ("t", "type_id"):
        try:
            b = fetch(with_query(api, "ac=videolist&%s=%s&pg=1" % (p, tid)),
                      timeout=12, done=_done_doc, retries=1)
            txt = b.decode("utf-8", "replace").lstrip("\ufeff")
            d = loads_loose(txt)
            n = len((d or {}).get("list") or []) if isinstance(d, dict) \
                else len(prefix_items(txt))
            if n > 0:
                return p
        except Exception:
            continue
    return None


def _count_items(txt):
    """数一个响应里有多少条内容（JSON 看 list，XML 看 <video>）"""
    txt = txt or ""
    if txt.lstrip()[:1] == "<":
        return len(re.findall(r"<video[ >]", txt))
    d = loads_loose(txt)
    if isinstance(d, dict) and isinstance(d.get("list"), list):
        return len(d["list"])
    return len(prefix_items(txt))


def search_ok(api, timeout=12):
    """真搜一个不可能命中的词：返回 0 条才算「支持搜索」

    实测坑：有的采集站完全忽略 wd，搜什么都吐同一批默认列表
    （例如 jacloudapi.php 那家），在客户端里就是「搜任何词都混进 20 条无关结果」。
    这种站要标 searchable=0，让客户端别拿它搜。
    """
    kw = "zqxjvv%d" % int(time.time() % 10000)
    try:
        b = fetch(with_query(api, "wd=%s" % kw), timeout=timeout,
                  retries=0, done=None)
        return _count_items(b.decode("utf-8", "replace").lstrip("\ufeff")) == 0
    except Exception:
        return True          # 搜索接口报错不代表「搜不准」，别误杀


def probe(seed, timeout=18, retries=1, with_classes=True):
    """体检 + 搜索能力判定（结果按 api 缓存在 state 里，不用每轮重测）"""
    out = _probe_core(seed, timeout, retries, with_classes)
    cached = seed.get("_search")
    if out["alive"]:
        if cached is None and with_classes:
            cached = 1 if search_ok(out["api"], timeout=min(timeout, 15)) else 0
        out["searchable"] = 1 if cached is None else int(cached)
    else:
        out["searchable"] = int(cached) if cached is not None else 1
    return out


def _probe_core(seed, timeout=18, retries=1, with_classes=True):
    """真拉一次 ac=videolist：必须有条目、且条目里带可播地址，才算活"""
    api = norm(seed["api"])
    out = {"name": clean_name(seed.get("name")), "api": api,
           "from": seed.get("from", ""), "alive": False, "type": None,
           "msg": "", "items": 0, "playable": 0, "total": 0,
           "classes": [], "class_ids": [], "param": seed.get("_param"),
           "sample": "", "recheck": False, "partial": False, "latency": 0,
           "searchable": 1}
    t0 = time.time()
    try:
        body = fetch(with_query(api, "ac=videolist&pg=1"), timeout=timeout,
                     retries=retries, done=_done_doc)
    except Exception as e:
        out["msg"] = "抓取失败：%s" % type(e).__name__
        return out

    out["latency"] = int((time.time() - t0) * 1000)
    txt = body.decode("utf-8", "replace").lstrip("\ufeff").lstrip()
    if not txt:
        out["msg"] = "空响应"
        return out

    # --- JSON（苹果 CMS v10，type=1）
    if txt[0] in "{[":
        d = loads_loose(txt)
        if not isinstance(d, dict):
            # 传输被限速截断：从已完整的前缀条目里判断是否真的可播
            objs = prefix_items(txt)
            playable = [o for o in objs
                        if "://" in str(o.get("vod_play_url") or "")]
            mt = re.search(r'"total"\s*:\s*"?(\d+)', txt)
            total = int(mt.group(1)) if mt else 0
            if playable:
                out.update(type=1, alive=True, partial=True, items=len(objs),
                           playable=len(playable), total=total,
                           sample=str(playable[0].get("vod_name") or "")[:20],
                           msg="OK（限速截断，前缀 %d 条全可播）" % len(playable))
                return out
            out["msg"] = "JSON 解析失败（收到 %dKB：%s）" % (
                len(body) // 1024, "前缀 %d 条无可播地址" % len(objs)
                if objs else "无完整条目")
            return out
        lst = d.get("list")
        if not isinstance(lst, list) or not lst:
            out["msg"] = "无 list：%s" % str(d)[:50]
            return out
        items = [x for x in lst if isinstance(x, dict)]
        playable = [x for x in items
                    if "://" in str(x.get("vod_play_url") or "")]
        out.update(type=1, items=len(items), playable=len(playable))
        try:
            out["total"] = int(d.get("total") or 0)
        except Exception:
            out["total"] = 0
        if not playable:
            out["msg"] = "%d 条但无播放地址" % len(items)
            return out
        out["alive"] = True
        out["msg"] = "OK (%d/%d 条可播)" % (len(playable), len(items))
        out["sample"] = str(playable[0].get("vod_name") or "")[:20]
        # 顺手取分类，并探出「分类浏览用哪个参数名」
        # 很多采集站只认 type_id，收到 t= 会直接返回空列表 → TVBox 点分类就是空白
        if with_classes:
            try:
                b2 = fetch(with_query(api, "ac=list&pg=1"), timeout=12,
                           cap=200000, done=None, retries=2)
                d2 = loads_loose(b2.decode("utf-8", "replace").lstrip("\ufeff"))
                if isinstance(d2, dict) and isinstance(d2.get("class"), list):
                    pairs = [(c.get("type_id"), c.get("type_name"))
                             for c in d2["class"]
                             if isinstance(c, dict) and c.get("type_name")]
                    out["classes"] = [n for _, n in pairs]
                    out["class_ids"] = [i for i, _ in pairs]
                    # 已知参数名就沿用（state 缓存），省掉 1~2 次探测请求
                    if pairs and pairs[0][0] not in (None, "") and not out["param"]:
                        out["param"] = detect_param(api, pairs[0][0])
            except Exception:
                pass
        return out

    # --- XML（type=0）
    if txt[0] == "<":
        vids = len(re.findall(r"<video>", txt)) or len(re.findall(r"<vod>", txt))
        plays = len(re.findall(r"<dd[^>]*>\s*(?:<!\[CDATA\[)?[^<]*?\$https?://", txt))
        out.update(type=0, items=vids, playable=plays)
        if not vids:
            out["msg"] = "XML 无 <video> 节点"
            return out
        if not plays:
            out["msg"] = "%d 个节点但无可播地址" % vids
            return out
        out["alive"] = True
        out["msg"] = "OK (XML %d 条 / %d 可播)" % (vids, plays)
        m = re.search(r"<name>(?:<!\[CDATA\[)?([^<\]]+)", txt)
        out["sample"] = (m.group(1).strip() if m else "")[:20]
        return out

    out["msg"] = "非预期内容：%s" % txt[:40].replace("\n", " ")
    return out


# ---------------------------------------------------------------- 合并

def merge(rows):
    """按域名去重（同域只留体检成绩最好的那条），再补唯一 key / 唯一 name"""
    by_host, order = {}, []
    for r in rows:
        host = (urlsplit(r["api"]).hostname or "").lower()
        if host not in by_host:
            by_host[host] = r
            order.append(host)
        else:
            cur = by_host[host]
            if (r["alive"], r["playable"], r["total"]) > \
               (cur["alive"], cur["playable"], cur["total"]):
                by_host[host] = r
    kept = [by_host[h] for h in order]

    used_keys, used_names = set(), set()
    for r in kept:
        k = site_key(r["api"])
        base, n = k, 2
        while k in used_keys:
            k = "%s-%d" % (base, n)
            n += 1
        used_keys.add(k)
        r["key"] = k

        name, n = r["name"], 2
        while name in used_names:
            name = "%s·%d" % (r["name"], n)
            n += 1
        used_names.add(name)
        r["name"] = name
    return kept


def _cat_names(row, limit=12):
    """把探测到的分类名整理成 TVBox 的 categories 字段

    分类是站点自带的（我们只是透传），几十个铺满一排没意义，
    只留前 limit 个；客户端真要别的分类还能通过 ac=list 拿到全量。
    """
    out = []
    for c in row.get("classes") or []:
        if isinstance(c, dict):
            n = str(c.get("type_name") or "").strip()
        else:
            n = str(c or "").strip()
        if n and n not in out:
            out.append(n)
    return out[:limit]


DOUBAN_SITE = {
    "key": "douban",
    "type": 3,
    "api": "csp_Douban",
    "searchable": 0, "quickSearch": 0, "filterable": 0,
    "changeable": 0, "playerType": 2,
    "builtin": True,          # 统计时不计入「站点总数」
}


# csp_Douban 是「从已加载的 spider jar 里找类」，配置里没有 spider 字段就实例化不出来
# （客户端表现：站点名显示，但一直转圈）。
#
# 硬编码一个 jar 不行：那个地址挂了就永久失效。这里改成**候选池 + 自动校验**：
#   按顺序试，第一个「能下载 且 dex 里确实有 Douban 类」的胜出；
#   结果缓存 7 天（jar_probe.json），全都不行就不加豆瓣首站。
DOUBAN_JAR_POOL = (
    "candymuj",            # 实测含 Douban 且没有广告文案
    "vox.jar",             # 含 Douban，无广告文案
    "HeChengChaXiu",       # 含 Douban
    "tgyg/spider.jar",     # 含 Douban，但带"永久免费/谨防倒卖"弹窗
    "custom_spider",
    "spider.jar",
)


def _jar_has_douban(url, timeout=25, cache_dir=None):
    """下载 jar 看 dex 里有没有 Douban 类（结果缓存 7 天，避免每轮都下 2MB）"""
    import io
    import zipfile
    cache_path = os.path.join(cache_dir or HERE, "jar_probe.json")
    try:
        with open(cache_path, encoding="utf-8") as f:
            cache = json.load(f)
    except Exception:
        cache = {}
    rec = cache.get(url) or {}
    if rec.get("ts") and time.time() - float(rec["ts"]) < 7 * 86400:
        return bool(rec.get("douban"))
    ok = False
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "okhttp/3.15"})
        raw = urllib.request.urlopen(req, timeout=timeout).read(30 * 1024 * 1024)
        zf = zipfile.ZipFile(io.BytesIO(raw))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".dex"))
        ok = b"Douban" in blob or b"douban" in blob
    except Exception:
        ok = False
    cache[url] = {"douban": ok, "ts": time.time()}
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except Exception:
        pass
    return ok


def _douban_pool(cache_dir=None):
    """候选顺序可以从 data/douban_jars.json 覆盖，不用改代码

    文件格式：{"hints": ["candymuj", "vox.jar", ...]}
    """
    try:
        with open(os.path.join(cache_dir or HERE, "douban_jars.json"),
                  encoding="utf-8") as f:
            hints = (json.load(f) or {}).get("hints") or []
        hints = [str(h).strip() for h in hints if str(h).strip()]
        if hints:
            return tuple(hints)
    except Exception:
        pass
    return DOUBAN_JAR_POOL


def pick_douban_jar(sites, log=print, cache_dir=None):
    """挑一个真能跑 csp_Douban 的 jar；挑不到返回空（宁可不加豆瓣）"""
    jars = [s.get("jar") for s in sites if s.get("jar")]
    if not jars:
        return ""
    seen = []
    for hint in _douban_pool(cache_dir):
        for j in jars:
            if hint in j and j not in seen:
                seen.append(j)
    for j in seen:
        if _jar_has_douban(j, cache_dir=cache_dir):
            return j
    return ""


def insert_douban(doc, name="豆瓣"):
    """把内置豆瓣塞到第一个位置

    豆瓣只有影视墙、没有搜索，但它响应快、海报好看，适合当门面。
    标记 builtin=True，输出统计里不算它一个站点。
    """
    sites = (doc or {}).get("sites") or []
    if any(s.get("key") == "douban" for s in sites):
        return 0
    item = dict(DOUBAN_SITE, name=name)
    sites.insert(0, item)
    doc["sites"] = sites
    return 1


def stamp_first_site(doc, stamp):
    """首个站点名前面加聚合时间：客户端左上角就能看到「这份配置是几点生成的」

    排查「是不是客户端还在用旧配置」时，比翻设置快得多。
    """
    sites = (doc or {}).get("sites") or []
    if stamp and sites:
        nm = str(sites[0].get("name") or "")
        if not nm.startswith(stamp):
            sites[0]["name"] = ("%s  %s" % (stamp, nm)).strip()


def embed_lives(doc, data_dir, max_channels=80, lines_per=4):
    """把测过的央视+卫视频道塞进配置的 lives 字段

    TVBox 系客户端的「直播」页读的就是配置里的 lives；不写进去的话，用户还得自己
    去「直播地址」里填 m3u，实测影视仓那个对话框还经常加不进去。
    """
    try:
        with open(os.path.join(data_dir, "live_tested.json"), encoding="utf-8") as f:
            t = json.load(f)
    except Exception:
        return doc
    out = []
    for grp in ("cctv", "sat"):
        for name, lines in sorted((t.get(grp) or {}).items()):
            urls = [{"name": "%sP %dms" % (v.get("height") or "直播", v.get("ms") or 0),
                     "url": v["url"]}
                    for v in lines[:max(1, lines_per)] if v.get("url")]
            if urls:
                out.append({"name": name, "urls": urls})
            if len(out) >= max_channels:
                break
        if len(out) >= max_channels:
            break
    if out:
        doc["lives"] = out
    return doc


def build_tvbox(cfg_name, sites, proxy_base=None, max_categories=12):
    """纯 CMS 档（不带任何 spider）

    以前这里是另一套实现，和 build_shell_profile 有 80% 重复，改一处要改两遍。
    现在只是「kinds 为空」的壳子 profile，site 字段完全一致。
    """
    return build_shell_profile(cfg_name, sites, {}, proxy_base, kinds=(),
                               label="CMS", include_pan=False,
                               max_categories=max_categories, log=lambda *a: None)


# ---------------------------------------------------------------- 全量 profile（壳子用）
#
# 影视仓 / Fongmi 这类客户端自己会加载 jar 跑 spider，所以 type=3 的站点交给它们就行。
# 我们只负责：收集 → 去重 → jar 可达性过滤 → 名字清洗 → 配额 → 合并 parses/flags。
# 注意：hosts / proxy 字段一律不带（一个订阅不该能改全局流量走向）。

JAR_MIN_BYTES = 1024


def check_jar(url, timeout=15):
    """只取头部判断「这确实是个 jar」：下载量极小，能挡住 404 和 HTML 页面"""
    try:
        req = urllib.request.Request(norm(url), headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            head = r.read(4096)
        if not head:
            return False, 0
        # zip 头 PK\x03\x04；gzip 头 \x1f\x8b（有的站对 jar 做了 gzip）
        ok = head[:2] == b"PK" or head[:2] == b"\x1f\x8b"
        return ok, len(head)
    except Exception:
        return False, 0


def load_block(path=None):
    """屏蔽名单：名字关键字 + 资源地址关键字

    默认**空**：装好什么样就是什么样，不替用户做决定。
    想挡掉某些站（比如哔哩/B 站这类 UGC，客厅场景多半用不上），在面板「站点屏蔽」里加，
    或者写 data/block.json：
        {"names": ["哔哩", "B站", "bili"], "assets": []}
    """
    default = {"names": [], "assets": [], "pin": []}
    if not path:
        path = os.path.join(HERE, "block.json")
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        return {"names": [str(x) for x in doc.get("names", [])],
                "assets": [str(x) for x in doc.get("assets", [])]}
    except Exception:
        return default


# 明确属于「网盘 / 盘搜」的 spider（api 前缀 + jar 里的类名）
# 加进来的站点不会进任何点播档，只在面板「网盘站点」里列出，方便以后单独接入。
# 判断依据：这些 spider 取的是阿里/夸克/天翼等网盘分享，客户端要额外解析才有播放地址。
PAN_SPIDERS = (
    "panweb", "pancms", "pansearch", "pansou", "alist", "xyqhikeral",
    "mogg", "yiso", "upyun", "quark", "tianyi", "aipan", "kkpan",
    "wobg", "wogg",           # 玩偶 / 木偶（网盘聚合）
    "appget",                  # 仓鼠/蔬菜/梨瓜 这类「XX • APP」网盘聚合
    "fourkhdr",                # 4KHDR
)
PAN_API = re.compile(
    r"(?i)("
    r"panweb|alist|pansou|/pan\b|pan\.|panapi|cloudshare|quark|aliyundrive|"
    r"115\.com|uc\.cn|tianyi|thunder|"
    + "|".join("csp_" + x for x in PAN_SPIDERS) +
    r")")
PAN_NAME = re.compile(
    r"(网盘|盘搜|搜盘|云盘|夸克|阿里|天翼|迅雷|115|小雅|AList|网分享|网飞|"
    r"4k搜|盘4k|🔹盘|◈盘|盘\s*4k|网盘搜)", re.I)


def is_pan(site):
    """是不是「网盘搜索/网盘浏览」类站点

    这类站点返回的是网盘分享链接，需要登录态或转存才能播，和直链采集站不是一回事。
    按用户要求：只进全能 profile，不进 CMS 档和 jar 档。
    """
    blob = " ".join(str(site.get(k) or "") for k in ("name", "api", "jar", "ext"))
    return bool(PAN_API.search(blob) or PAN_NAME.search(str(site.get("name") or "")))


def is_blocked(site, block):
    name = str(site.get("name") or "")
    asset = str(site.get("jar") or site.get("api") or "")
    for kw in block.get("names", []):
        if kw and kw.lower() in name.lower():
            return True
    for kw in block.get("assets", []):
        if kw and kw.lower() in asset.lower():
            return True
    return False


def parse_jar_ref(value):
    """拆开 jar 引用，返回 (真实地址, 是否被 img+ 包裹)

    配置里常写成 `https://x/spider.jar;md5;abcd...`，直接拿整串去下载必然失败——
    我们之前就是这样，把一批好 jar 判成了「不可达」，站点跟着被丢。
    TVBox 客户端自己认这个后缀，所以输出时保留原样，只在**我们下载时**去掉。
    """
    text = (value or "").strip()
    wrapped = text.startswith("img+")
    if wrapped:
        text = text[4:]
    for tag in (";sha256;", ";md5;"):
        i = text.find(tag)
        if i > 0:
            return text[:i], wrapped
    return text, wrapped


def check_asset(url, kind, timeout=15):
    """确认这个资源真的存在：jar 认 PK/gzip 魔数，js/py 至少不能是 HTML 错误页"""
    real, wrapped = parse_jar_ref(url)
    if wrapped:
        return False          # 包在图片里的 jar 没法校验内容
    try:
        req = urllib.request.Request(norm(real), headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            head = r.read(2048)
        if not head:
            return False
        if kind == "jar":
            return head[:2] == b"PK" or head[:2] == b"\x1f\x8b"
        low = head.lstrip()[:16].lower()
        return not low.startswith(b"<!doctype") and not low.startswith(b"<html")
    except Exception:
        return False


# ---------------------------------------------------------------- 规则（外置，可改）
#
# 这些规则以前全写死在代码里，改一条就得发版。现在 data/rules.json 可以覆盖任意一条：
#     {"pan_spiders": [...], "notice_names": [...], "name_rules": [{"find":"..","replace":""}]}
# 镜像里带一份默认值，文件不存在就用默认。

RULES_DEFAULT = {
    "pan_spiders": ["panweb", "pancms", "pansearch", "pansou", "alist", "xyqhikeral",
                    "mogg", "yiso", "upyun", "quark", "tianyi", "aipan", "kkpan",
                    "wobg", "wogg", "appget", "fourkhdr"],
    "pan_names": ["网盘", "盘搜", "搜盘", "云盘", "夸克", "阿里", "天翼", "迅雷",
                  "115", "小雅", "AList", "网分享", "网飞", "4k搜", "盘4k"],
    "notice_names": ["推广", "公众号", "加群", "QQ群", "微信群", "防失联", "发布页",
                     "免费接口", "备用地址", "请关注", "扫码", "赞助", "打赏",
                     "下载APP", "软件下载", "收费接口"],
    "notice_keep": [],
    "credential_fields": ["token", "cookie", "authorization", "api_key", "apikey",
                          "password", "passwd", "账号", "密码"],
    "local_hosts": [r"127\.0\.0\.1", r"localhost", r"0\.0\.0\.0", r"\[::1\]",
                    r"192\.168\.\d+\.\d+", r"10\.\d+\.\d+\.\d+",
                    r"172\.(1[6-9]|2\d|3[01])\.\d+\.\d+"],
    "name_rules": [],
}

RULES = dict(RULES_DEFAULT)


def load_rules(data_dir=None):
    """读 data/rules.json（可只写想改的字段），和默认值合并"""
    path = os.path.join(data_dir, "rules.json") if data_dir else \
        os.path.join(HERE, "rules.json")
    doc = dict(RULES_DEFAULT)
    try:
        with open(path, encoding="utf-8") as f:
            got = json.load(f)
        for k, v in got.items():
            if k in doc and isinstance(v, list):
                doc[k] = v
    except Exception:
        pass
    return doc


def apply_rules(rules):
    """把规则编译进模块级正则（进程内生效，改完调用一次即可）"""
    global RULES, PAN_API, PAN_NAME, NOTICE_NAMES, NOTICE_KEEP, CRED_FIELDS, LOCAL_HOSTS
    RULES = dict(RULES_DEFAULT)
    RULES.update(rules or {})
    PAN_API = re.compile(
        r"(?i)(" + "|".join("csp_" + x for x in RULES["pan_spiders"]) +
        r"|panweb|alist|pansou|/pan\b|quark|aliyundrive|115\.com|uc\.cn)")
    PAN_NAME = re.compile("|".join(RULES["pan_names"]), re.I)
    NOTICE_NAMES = re.compile("|".join(RULES["notice_names"]))
    NOTICE_KEEP = tuple(RULES["notice_keep"])
    CRED_FIELDS = tuple(RULES["credential_fields"])
    LOCAL_HOSTS = re.compile(r"(?i)(" + "|".join(RULES["local_hosts"]) + r")")
    return RULES


# ---------------------------------------------------------------- 站点判定（配置层）
#
# 参照 ElderTV 的 SiteFilter：只读配置，不联网、不跑 jar 代码。
# 这三类站到电视上必然是坏体验，早拦掉比让用户点进去看弹窗强。

REASON_INVALID = "配置无效"
REASON_LOCAL = "依赖本机地址"
REASON_PAN = "网盘或需要登录"
REASON_NOTICE = "公告或推广"

LOCAL_HOSTS = re.compile(
    r"(?i)(127\.0\.0\.1|localhost|0\.0\.0\.0|\[::1\]|"
    r"192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+\.\d+)")

# 名字里出现这些基本就不是影视站
# 只用「强特征」词：光一个「公告」会误伤「公告影院」这种正经站
NOTICE_NAMES = re.compile(
    r"(推广|公众号|加群|QQ群|微信群|防失联|发布页|免费接口|备用地址|"
    r"请关注|扫码|赞助|打赏|下载APP|软件下载|收费接口)")
# 白名单：名字里带这些词就不当公告站（比如「公告影院」这种正经站）
NOTICE_KEEP = ()

# ext 里出现这些字段说明要账号/token（网盘或自建）
CRED_FIELDS = ("token", "cookie", "authorization", "api_key", "apikey",
               "password", "passwd", "账号", "密码")


# 让默认规则成为唯一真相（server 会再用 data/rules.json 覆盖一次）
apply_rules(RULES_DEFAULT)


def site_reason(site):
    """按配置判断这个站该不该丢，返回原因字符串；没问题返回 None"""
    name = str(site.get("name") or "")
    api = str(site.get("api") or "")
    jar = str(site.get("jar") or "")
    ext = site.get("ext")
    ext_text = json.dumps(ext, ensure_ascii=False) if ext is not None else ""

    if not api:
        return REASON_INVALID
    # 指向本机/内网的站，在服务端和别人的电视上都用不了
    if LOCAL_HOSTS.search(api) or LOCAL_HOSTS.search(ext_text) or LOCAL_HOSTS.search(jar):
        return REASON_LOCAL
    low = ext_text.lower()
    if "token.json" in low or "token_json" in low:
        return REASON_PAN
    if any(f in low for f in CRED_FIELDS):
        return REASON_PAN
    if is_pan(site):
        return REASON_PAN
    if NOTICE_NAMES.search(name) and not any(k in name for k in NOTICE_KEEP):
        return REASON_NOTICE
    return None


def ext_key(site):
    """扩展站点的稳定 key：api + ext 一起算

    同一批配置里常有几十个站点共用同一个 api（csp_xxx）但 ext 不同，
    只按 api 当身份会把它们混成一个 —— 面板「被使用」曾经因此把 894 个站点全标成在用。
    """
    return "et_" + hashlib.sha1(
        ("%s|%s" % (site.get("api"), site.get("ext") or "")).encode("utf-8")
    ).hexdigest()[:16]


def build_shell_profile(cfg_name, cms_sites, assets_doc, proxy_base=None,
                        kinds=("jar",), max_assets=25, max_sites=120,
                        max_per_asset=10, max_parses=20, max_flags=100,
                        asset_ok=None, block=None, label="全量", log=print,
                        include_pan=False, max_categories=12, blocked_jars=None,
                        sort_mode="latency", speed=None):
    """给「壳子类客户端」的 profile：CMS + 指定类型的扩展站点（jar / js / py）

    * CMS 部分照旧：只带本轮在线的，走 /cms 代理，保留 searchable
    * 扩展站点：按 (api+ext) 去重 → 屏蔽名单 → 资源可达性检查 → 每个资源均摊
    * 不带 hosts / proxy / lives：一个订阅不该能改全局流量走向
    """
    base = (proxy_base or "").rstrip("/")
    asset_ok = asset_ok if asset_ok is not None else {}
    block = block or {"names": [], "assets": []}
    if not include_pan:
        cms_sites = [s for s in cms_sites if not is_pan(s)]

    sites = [{
        "key": s["key"],
        "name": clean_site_name(s["name"]),
        "type": s["type"],
        "api": ("%s/cms/%s" % (base, s["key"])) if base else s["api"],
        "searchable": int(s.get("searchable", 1)),
        "quickSearch": int(s.get("searchable", 1)), "filterable": 1,
        "changeable": 1, "playerType": 2,
        "categories": _cat_names(s, max_categories),
    } for s in cms_sites]

    raw = [s for s in (assets_doc.get("sites") or [])
           if (s.get("kind") or "jar") in kinds]
    # 一期判定：本机地址 / 需要登录 / 公告推广 —— 和 ElderTV 的配置层过滤对齐
    dropped = {}
    kept = []
    for s0 in raw:
        why = site_reason(s0)
        if why:
            dropped[why] = dropped.get(why, 0) + 1
            continue
        kept.append(s0)
    raw = kept
    if dropped:
        log("  跳过 %s" % "，".join("%s %d" % kv for kv in dropped.items()))
    if blocked_jars:
        before = len(raw)
        raw = [s for s in raw if (s.get("jar") or "") not in blocked_jars]
        if before != len(raw):
            log("  跳过 %d 个站点：其 jar 未通过安全扫描" % (before - len(raw)))
    if not include_pan:
        raw = [s for s in raw if not is_pan(s)]
    pins = {str(x) for x in (block.get("pin") or [])}
    if pins:
        raw.sort(key=lambda s0: 0 if (s0.get("name") in pins) else 1)
    blocked = sum(1 for s in raw if is_blocked(s, block))
    raw = [s for s in raw if not is_blocked(s, block)]

    by_asset = {}
    for s in raw:
        by_asset.setdefault((s.get("kind") or "jar"), {}).setdefault(
            s.get("jar") or s.get("api"), []).append(s)

    # 按类型配额分配资源名额：一眼看上去 jar 组最大，不配额的话 JS/Py 一个都进不来
    share = {"jar": 0.6, "js": 0.3, "py": 0.2}
    present = [k for k in ("jar", "js", "py") if by_asset.get(k)]
    quota = {k: max(1, int(round(max_assets * share.get(k, 0.15))))
             for k in present}
    picked, picked_sites, dead = [], [], 0

    def ok_asset(kind, asset):
        nonlocal dead
        ok = asset_ok.get(asset)
        if ok is None:
            ok = check_asset(asset, kind, timeout=12)
            asset_ok[asset] = ok
        if not ok:
            dead += 1
            return False
        return True

    # 先按类型配额挑出「要带哪些 jar/js/py」
    queues = []
    for kind in present:
        assets = sorted(by_asset[kind].items(), key=lambda kv: -len(kv[1]))
        taken = 0
        for asset, group in assets:
            if len(picked) >= max_assets or taken >= quota[kind]:
                break
            if ok_asset(kind, asset):
                picked.append(asset)
                queues.append((kind, asset, sorted(group, key=lambda x: x.get("name") or "")))
                taken += 1

    # 有空位继续补，还是按组大小
    if len(picked) < max_assets:
        rest = []
        for kind in present:
            for asset, group in sorted(by_asset[kind].items(), key=lambda kv: -len(kv[1])):
                if asset not in picked:
                    rest.append((len(group), kind, asset, group))
        for _, kind, asset, group in sorted(rest, key=lambda x: -x[0]):
            if len(picked) >= max_assets:
                break
            if ok_asset(kind, asset):
                picked.append(asset)
                queues.append((kind, asset, sorted(group, key=lambda x: x.get("name") or "")))

    # 轮流取：每个资源每次只出一个站点，避免少数几个大 jar 把列表占满
    # （实测有配置把 100 多个站点塞在同一个 jar 里，全按组大小排就只剩那几个 jar 的站）
    used = {a: 0 for _, a, _ in queues}
    while len(picked_sites) < max_sites:
        progressed = False
        for kind, asset, group in queues:
            if used[asset] >= min(max_per_asset, len(group)):
                continue
            picked_sites.append(group[used[asset]])
            used[asset] += 1
            progressed = True
            if len(picked_sites) >= max_sites:
                break
        if not progressed:
            break

    # 输出顺序：电视上看到的就是这个顺序，排前面的会被优先点开
    speed = speed or {}
    kind_order = {"jar": 0, "js": 1, "py": 2}

    def _lat(x):
        sp = speed.get(x.get("jar") or x.get("api") or "") or {}
        return sp.get("ttfb") or 999999

    if sort_mode == "name":
        picked_sites.sort(key=lambda x: (x.get("kind") or "jar", x.get("name") or ""))
    elif sort_mode == "kind":
        picked_sites.sort(key=lambda x: (kind_order.get(x.get("kind") or "jar", 9), _lat(x)))
    else:                                  # latency（默认）
        picked_sites.sort(key=lambda x: _lat(x))

    for s in picked_sites:
        key = ext_key(s)
        item = {"key": key, "name": clean_site_name(s.get("name")),
                "type": 3, "api": s.get("api"),
                "searchable": 1, "quickSearch": 1, "filterable": 1,
                "changeable": 1, "playerType": 2}
        if s.get("jar"):
            item["jar"] = s["jar"]
        if s.get("ext"):
            item["ext"] = s["ext"]
        sites.append(item)

    parses, seen = [], set()
    for p in (assets_doc.get("parses") or []):
        if not p.get("url"):
            continue
        k = "%s|%s" % (p.get("type"), p["url"])
        if k in seen:
            continue
        seen.add(k)
        parses.append(p)
        if len(parses) >= max_parses:
            break

    by_kind = {}
    for s in picked_sites:
        by_kind[s.get("kind") or "jar"] = by_kind.get(s.get("kind") or "jar", 0) + 1
    log("  %s profile：CMS %d + 扩展 %d（%s；%d 个资源，跳过 %d 不可达、屏蔽 %d）"
        "，parses %d" % (label, len(cms_sites), len(picked_sites),
                         " ".join("%s=%d" % kv for kv in sorted(by_kind.items())) or "-",
                         len(picked), dead, blocked, len(parses)))

    return {"name": cfg_name, "sites": sites, "lives": [], "parses": parses,
            "flags": list(assets_doc.get("flags") or [])[:max_flags]}


def build_libretv(cfg_name, sites, max_sources=100):
    """LibreTV-SourceList v2：只吃 type=1，不吃 spider/XML"""
    picked = [s for s in sites if s["type"] == 1][:max_sources]
    return {
        "name": cfg_name,
        "version": 2,
        "sources": [{
            "name": s["name"],
            "url": s["api"],
            "detail": "%s://%s/" % (urlsplit(s["api"]).scheme,
                                    urlsplit(s["api"]).netloc),
            "isAdult": False,
        } for s in picked],
        "liveSources": [],
    }


# ---------------------------------------------------------------- 状态

def load_state(path):
    """state.json：每个接口的连续失败次数与最后成功时间

    公开采集站会反复抖动，单次失败就摘除会让源列表天天变。用 fail_streak 做滞回：
    连续失败到阈值才摘，恢复一次就立即回归。
    """
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_state(path, state):
    ST.save_file(path, state)


def apply_state(results, state, drop_after=3):
    """把本轮体检结果并入 state，并标出「暂留」的源"""
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    for r in results:
        st = state.setdefault(r["api"], {})
        if r["alive"]:
            st["fail_streak"] = 0
            st["ok_streak"] = int(st.get("ok_streak") or 0) + 1
            st["last_ok"] = now
            st["type"] = r["type"]
            st["name"] = r["name"]
            if r.get("param"):
                st["param"] = r["param"]        # 分类参数方言，下次直接复用
            st["searchable"] = int(r.get("searchable", 1))
        else:
            st["fail_streak"] = int(st.get("fail_streak") or 0) + 1
            st["ok_streak"] = 0
        st["last_check"] = now
        r["fail_streak"] = st["fail_streak"]
        r["last_ok"] = st.get("last_ok")
        r["grace"] = False
        if not r["alive"] and st.get("last_ok") and \
           st["fail_streak"] < drop_after and st.get("type") in (0, 1):
            # 历史上活过、还没到摘除阈值 → 暂留，用上次的类型继续输出
            r["type"] = st["type"]
            r["grace"] = True
            r["msg"] += "（暂留 %d/%d，上次成功 %s）" % (
                st["fail_streak"], drop_after, st["last_ok"])
    return state


# ---------------------------------------------------------------- 报告

def write_report(out_dir, rows, kept, included, tvbox, libretv, elapsed,
                 drop_after=3, bench=None, probed=None, skipped=None):
    # 口径统一在 kept（去重后）上，否则 35 体检 / 25 去重 / 23 在线 三个数对不上：
    #   seeds = deduped + alive + grace + dead   （+ queued 若本轮没全探）
    alive = [r for r in kept if r["alive"]]
    grace = [r for r in kept if r.get("grace") and not r["alive"]]
    dropped = [r for r in kept if not r["alive"] and not r.get("grace")]
    dropped_from_libretv = [r for r in included if r["type"] != 1]

    rep = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_sec": round(elapsed, 1),
        "seeds": len(rows),
        # 体检预算轮转时，一部分源本轮不探 —— 不写出来，面板上
        # 「35 个源 / 在线 24 / 暂留 1」就对不上账（差的 10 个去哪了）
        "probed": len(rows) if probed is None else int(probed),
        "queued": (len(rows) - int(probed)) if probed is not None and skipped is None
                  else int(skipped or 0),
        "merged": len(kept),
        # 体检完多少条被 merge() 当成重复合并掉了 —— 不写出来，面板上
        # 「35 体检 / 23 在线 / 2 暂留」就永远差 10
        "deduped": max(0, len(rows) - len(kept)),
        "alive": len(alive),
        "grace": len(grace),
        "active": len(included),
        "bench_n": len(bench or []),
        "dead": len(dropped),
        "tvbox_sites": len([x for x in (tvbox.get("sites") or [])
                            if not x.get("builtin")]),
        "libretv_sources": len(libretv["sources"]),
        "dropped": [{"api": r["api"], "name": r["name"], "msg": r["msg"],
                     "fail_streak": r.get("fail_streak")} for r in dropped],
        "libretv_skipped": [{"api": r["api"], "name": r["name"],
                             "why": "type=%s，LibreTV 只吃 type=1" % r["type"]}
                            for r in dropped_from_libretv],
        "bench": [{"key": r.get("key"), "name": r["name"], "api": r["api"],
                   "latency": r.get("latency") or 0, "msg": r.get("msg")}
                  for r in (bench or [])],
        "sources": [{k: r.get(k) for k in ("key", "name", "api", "type", "alive",
                                           "grace", "fail_streak", "last_ok",
                                           "msg", "items", "playable", "total",
                                           "sample", "param", "searchable")}
                    | {"nclass": len(r.get("classes") or []),
                       "latency": r.get("latency") or 0,
                       "pan": is_pan(r)}
                    for r in included],
    }
    with open(os.path.join(out_dir, "report.json"), "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=1)

    L = ["# 聚合报告", "",
         "- 生成时间：%s（耗时 %.1fs）" % (rep["generated"], rep["elapsed_sec"]),
         "- 种子源：%d → 按域名去重后 %d → 在线 **%d**，暂留 %d，摘除 %d" %
         (rep["seeds"], rep["merged"], rep["alive"], rep["grace"], rep["dead"]),
         "- 输出：TVBox 站点 **%d** 个；LibreTV 源 **%d** 个｜主站 %d，备选池 %d" %
         (rep["tvbox_sites"], rep["libretv_sources"], len(included), len(bench or [])),
         "", "## 输出中的源", "",
         "| key | 名称 | type | 状态 | 首屏可播 | 总量 | 示例 |",
         "|---|---|---|---|---|---|---|"]
    for r in included:
        if r["items"] and r["items"] > 0:
            ratio = "%d/%d" % (r["playable"], r["items"])
        else:
            ratio = "前缀可播"
        state = "在线" if r["alive"] else "暂留 %d/%d" % (r["fail_streak"], drop_after)
        L.append("| `%s` | %s | %s | %s | %s | %s | %s |" %
                 (r["key"], r["name"], r["type"], state, ratio,
                  r["total"] or "-", r["sample"] or "-"))
    if dropped:
        L += ["", "## 已摘除（连续失败达阈值）", "",
              "| 名称 | 接口 | 连续失败 | 原因 |", "|---|---|---|---|"]
        for r in dropped:
            L.append("| %s | `%s` | %s | %s |" %
                     (r["name"], r["api"], r.get("fail_streak", "-"), r["msg"]))
    if dropped_from_libretv:
        L += ["", "## LibreTV 未收录（类型不兼容）", "",
              "| 名称 | 接口 | 原因 |", "|---|---|---|"]
        for r in dropped_from_libretv:
            L.append("| %s | `%s` | type=%s，LibreTV 只吃 type=1 |" %
                     (r["name"], r["api"], r["type"]))
    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L) + "\n")
    return rep


# ---------------------------------------------------------------- 主流程

def load_sources_doc(path):
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        doc.setdefault("sources", [])
        return doc
    except Exception:
        return {"name": "自用聚合源", "sources": []}


def dump_sources_doc(path, doc):
    ST.save_file(path, doc)


SETTING_DEFAULTS = {
    "active_max": 12,        # 主站上限
    "min_active": 8,         # 主站下限
    "bench_latency": 4000,   # 超过这个延迟且主站够用时进备选
    "bench_every": 4,        # 备选池每 N 轮补探一次
    "config_days": 5,          # 启用接口的拉取间隔（天）
    "bench_days": 2,           # 备选接口的初筛间隔（天），只浅解析不探活        # 配置源多久重新拉一次（天）
    "config_max_fail": 3,    # 连续拉不到几次就删掉这个配置源
    "auto_floor": 4,         # 一轮产出的站点少于这个数，判定异常
    "max_seeds": 120,        # 待聚合源上限（配置源合并时不能无限涨）
    "probe_budget": 40,      # 每轮最多体检多少个（按最久没探的轮转）
    "max_categories": 12,    # 每个站点最多铺几个分类 Tab（站点自带几十个，多了没用）
    "sort_mode": "kind",      # 输出顺序：kind=按资源类型(默认) / latency=按响应时间 / name=按名称
    # 首站：douban=内置豆瓣 / alist=网盘 / none=都不用（三者互斥）
    "home_site": "douban",
    "douban_first": 1,        # 旧键，仅为读老配置保留
    "stamp_suffix": "豆瓣",    # 时间后面的文字，用户可改
    "stamp_name": 1,         # 首个站点名前加「聚合时间」，方便判断客户端是不是用的旧配置
    "live_tomb_days": 15,        # 屏蔽/删除的源多久复检一次（天）
    "live_tomb_max_fail": 5,     # 连续这么多次复检不通就真删除
    "live_drop_seg_bad": 1,   # 分片取不到的线路不输出（关掉则保留并排最后）
    "live_active_n": 10,      # 直播优选源个数（其余做备选，不参与常规解析）
    "live_auto_promote": 1,   # 直播源自动补位：备选价值超过门槛就顶进优选
    "live_bench_days": 2,     # 备选源初筛间隔（天）
    "live_big_days": 3,       # 大聚合间隔（天）：重新展开 TXT/M3U
    "live_small_days": 1,     # 小聚合间隔（天）：只测线路
    "live_max_test": 1500,   # 直播每轮最多测多少条线路
    "live_lines_per": 5,     # 每个台输出几条线路（取质量最好的几条）
    "merge_room": 40,        # 单个配置接口一次最多并进多少条 CMS（多轮累积）
    "ext_default": 120,      # 默认档带多少个扩展站点（最终输出 = 这个数 + 采集站点数）
    "ext_all": 120,          # 全能档带多少个扩展站点
    "ltv_cms": 100,          # LibreTV 输出多少个 CMS 接口
    "embed_lives": 1,        # 把央视+卫视写进 tvbox.json 的 lives（客户端直播页直接可用）
    "update_mirror": "",     # 检查更新用的镜像/加速地址，留空只查官方 GHCR
    "interval_mode": "daily",  # 聚合间隔：hours=每N小时 / daily=每天某点
    "interval_hours": 12,
    "daily_at": "03:30",
    "speed_hours": 12,       # 扩展资源测速多久重测一次
    "max_per_asset": 30,     # 同一个 jar/js/py 最多带几个站点（配置里常见几十个站共用一个 jar）
    "max_configs": 10,       # 最多同时解析几条配置线路，其余进备选库（省时间）
    "auto_promote": 1,       # 主线路不够时自动从备选库补一条
    "zero_bench": 2,         # 连续几次解析出 0 个站点就自动下沉到备选库
    "drop_bench_sites": 0,   # 下沉时是否把已并入的站点一并剔除（默认不剔，删了纯亏）
}


def load_settings(path=None):
    """运行参数：面板可改，改完下一次聚合生效（不用重启容器）"""
    if not path:
        path = os.path.join(HERE, "settings.json")
    doc = dict(SETTING_DEFAULTS)
    try:
        with open(path, encoding="utf-8") as f:
            got = json.load(f)
        for k in list(doc):
            if k not in got or got[k] in (None, ""):
                continue
            # 字符串型设置（interval_mode / daily_at）不能 int()，
            # 之前无脑 int 会让一条坏值把整份设置读挂，全部回落到默认。
            if isinstance(doc[k], str):
                doc[k] = str(got[k]).strip()
            else:
                try:
                    doc[k] = int(got[k])
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return doc


def save_settings(doc, path=None):
    path = path or os.path.join(HERE, "settings.json")
    out = dict(SETTING_DEFAULTS)
    for k in out:
        if k not in doc or doc[k] in (None, ""):
            continue
        # 字符串型（interval_mode / daily_at）不能 int()，否则保存时被丢掉，
        # 面板再读回来就是默认值 —— 之前「改成 daily 保存后又变回 hours」就是这个。
        if isinstance(out[k], str):
            out[k] = str(doc[k]).strip() or out[k]
            continue
        try:
            out[k] = int(doc[k])
        except (TypeError, ValueError):
            pass
    ST.save_file(path, out)
    return out


def load_overrides(path=None):
    """人工干预清单：禁用 / 置顶 / 备选

    data/overrides.json:
        {"disabled": ["key或接口"], "pinned": ["..."], "bench": ["..."]}
    面板上的按钮就是改这个文件——不碰 sources.json，删错了还能改回来。
    """
    if not path:
        path = os.path.join(HERE, "overrides.json")
    doc = {"disabled": [], "pinned": [], "bench": []}
    try:
        with open(path, encoding="utf-8") as f:
            got = json.load(f)
        for k in doc:
            doc[k] = [str(x) for x in (got.get(k) or [])]
    except Exception:
        pass
    return doc


def save_overrides(doc, path=None):
    path = path or os.path.join(HERE, "overrides.json")
    ST.save_file(path, doc)
    return doc


def _ov_match(ov, api, key=None):
    """overrides 里既支持写 key，也支持直接写接口地址（人肉添加时更直观）"""
    n = norm(api)
    cands = {n, n.rstrip("/"), key or ""}
    return bool(cands & set(ov or []))


LASTGOOD_FILES = ("tvbox.json", "tvbox-direct.json", "all.json",
                  "ltv.json", "report.json")


def save_lastgood(out_dir, data_dir, meta):
    """把这一轮的产物留一份，聚合出问题时能退回去"""
    dst = os.path.join(data_dir, "lastgood")
    os.makedirs(dst, exist_ok=True)
    for fn in LASTGOOD_FILES:
        src = os.path.join(out_dir, fn)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(dst, fn))
    with open(os.path.join(dst, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    return dst


def load_lastgood(data_dir):
    """读回上一次成功的产物；没有就返回 None"""
    src = os.path.join(data_dir, "lastgood")
    docs = {}
    for fn in LASTGOOD_FILES:
        p = os.path.join(src, fn)
        try:
            with open(p, encoding="utf-8") as f:
                docs[fn] = json.load(f)
        except Exception:
            if fn in ("tvbox.json", "libretv.json"):
                return None
    if not docs.get("tvbox.json"):
        return None
    try:
        with open(os.path.join(src, "meta.json"), encoding="utf-8") as f:
            docs["meta"] = json.load(f)
    except Exception:
        docs["meta"] = {}
    return docs


def restore_lastgood(data_dir, out_dir, log=print):
    """把 lastgood 写回 out 目录（手动/自动退回都走这里）"""
    docs = load_lastgood(data_dir)
    if not docs:
        return None
    os.makedirs(out_dir, exist_ok=True)
    for fn in LASTGOOD_FILES:
        if fn in docs:
            with open(os.path.join(out_dir, fn), "w", encoding="utf-8") as f:
                json.dump(docs[fn], f, ensure_ascii=False, indent=1)
    log("[ROLLBACK] 已退回上一次成功的结果：%s（%s 个站点）"
        % (docs["meta"].get("t", "?"), len(docs["tvbox.json"].get("sites") or [])))
    return docs


# ---------------------------------------------------------------- 配置源（定期拉取）
#
# 面板上「添加配置源」加的是**配置地址**（多仓/单仓 json），不是采集接口。
# 每隔 config_days 天拉一次，把里面的 CMS 并进 sources.json、jar/js/py 并进 jars.json；
# 连续拉不到 config_max_fail 次就把它删掉，免得一直拖时间。

def _cfg_score(st):
    """线路贡献度：产出越多、延迟越低越该留在主线路"""
    st = st or {}
    s0 = st.get("stats") or {}
    gain = sum(int(s0.get(k) or 0) for k in ("cms", "jar", "js", "py"))
    lat = int(st.get("latency") or 99999)
    return (-gain, lat)


def _tier_map(entries):
    """给配置列表补上 tier 字段（老数据默认 active）"""
    for e in entries:
        if isinstance(e, dict):
            e.setdefault("tier", "active")
    return entries


def balance_configs(entries, state, settings, log=print):
    """整理主线路数量：超出上限的按贡献度下沉，不够时从备选库补

    **只改每个条目的 tier 字段，最后返回原列表本身**。
    之前用 `active + bench` 拼新列表，结果「下沉忘了 append 回 bench」「提升忘了 append 回 active」
    各丢过一次数据（59 条掉到 9 条）。现在从结构上不可能丢：返回的就是传进来的那份。
    """
    limit = max(1, int(settings.get("max_configs") or 10))
    active = [e for e in entries if e.get("tier") != "bench"]
    bench = [e for e in entries if e.get("tier") == "bench"]
    total = len(entries)

    active.sort(key=lambda e: _cfg_score(state.get(e.get("url"))))
    demoted = []
    while len(active) > limit:
        e = active.pop()
        e["tier"] = "bench"
        bench.append(e)
        demoted.append(e)

    promoted = []
    if int(settings.get("auto_promote", 1)):
        bench.sort(key=lambda e: _cfg_score(state.get(e.get("url"))))
        while len(active) < limit and bench:
            e = bench.pop(0)
            e["tier"] = "active"
            active.append(e)
            promoted.append(e)

    assert len(active) + len(bench) == total, "线路整理丢失条目"
    if demoted:
        log("  线路超限（上限 %d）：%d 条下沉到备选库" % (limit, len(demoted)))
    if promoted:
        log("  主线路不足：从备选库补 %d 条" % len(promoted))
    return entries, promoted, demoted


def drop_sites_of(data_dir, url, name=""):
    """把某条线路并入的站点从池子里剔除（默认不调用，留给 drop_bench_sites 开关）"""
    removed = 0
    jp = os.path.join(data_dir, "jars.json")
    try:
        with open(jp, encoding="utf-8") as f:
            doc = json.load(f)
        before = len(doc.get("sites") or [])
        doc["sites"] = [x for x in (doc.get("sites") or []) if x.get("from") != url]
        removed += before - len(doc["sites"])
        ST.save_file(jp, doc)
    except Exception:
        pass
    return removed


def measure_asset(url, kind="jar", timeout=15, sample=128 * 1024):
    """给一个 jar/js/py 资源测速

    只读前 512KB：既能看到首字节延迟和吞吐，又不会把几十 MB 的 jar 整个拖下来。
    体积优先用 Content-Length，没有再按实读算。
    """
    import urllib.error
    t0 = time.time()
    out = {"ok": False, "ttfb": 0, "bytes": 0, "speed": 0, "err": "", "tested": True}
    # 配置里常带 `;md5;xxx` 后缀或 `img+` 前缀，直接拿去下载必然失败
    real, wrapped = parse_jar_ref(url)
    if wrapped:
        out["err"] = "img+ 包裹，无法测速"
        return out
    # 中文路径要先百分号编码，否则 Request 直接抛 UnicodeEncodeError
    real = norm(real)
    try:
        req = urllib.request.Request(real, headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36",
            "Range": "bytes=0-%d" % (sample - 1)})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out["ttfb"] = int((time.time() - t0) * 1000)
            body = r.read(sample)
            # 用了 Range，Content-Length 只是这一段的长度；真实体积在 Content-Range 里
            cr = r.headers.get("Content-Range") or ""
            m = re.search(r"/(\d+)$", cr)
            if m:
                out["bytes"] = int(m.group(1))
            else:
                out["bytes"] = int(r.headers.get("Content-Length") or 0) or len(body)
        dt = max(time.time() - t0, 0.05)
        out["speed"] = int(len(body) / dt / 1024)          # KB/s
        head = body[:8]
        if kind == "jar":
            out["ok"] = head[:2] == b"PK" or head[:2] == b"\x1f\x8b" or len(body) > 4096
        else:
            out["ok"] = len(body) > 200
    except Exception as e:
        out["err"] = "%s" % type(e).__name__
    return out


def speed_test_assets(data_dir, kinds=("jar", "js", "py"), limit=0,
                      workers=8, log=print, force=False, urls=None,
                      timeout=10):
    """把所有扩展资源测一遍速度，结果写 data/asset_speed.json"""
    path = os.path.join(data_dir, "asset_speed.json")
    if not force:
        try:
            with open(path, encoding="utf-8") as f:
                cur = json.load(f)
            stg = load_settings(os.path.join(data_dir, "settings.json"))
            if time.time() - float(cur.get("ts") or 0) < int(stg.get("speed_hours") or 12) * 3600:
                return cur
        except Exception:
            pass
    try:
        with open(os.path.join(data_dir, "jars.json"), encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        return {}

    assets = {}
    for s0 in doc.get("sites") or []:
        u = s0.get("jar") or s0.get("api") or ""
        if not u:
            continue
        k = s0.get("kind") or "jar"
        if k not in kinds:
            continue
        a = assets.setdefault(u, {"kind": k, "sites": 0})
        a["sites"] += 1
    if urls:
        # 只测指定的资源（面板「重测筛选」用）
        seen, items = set(), []
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            items.append((u, assets.get(u) or {"kind": "jar", "sites": 0}))
    else:
        items = sorted(assets.items(), key=lambda kv: -kv[1]["sites"])
        if limit and limit > 0:
            items = items[:limit]      # limit=0 表示全测
    log("  资源测速：共 %d 个资源，本轮测 %d 个…" % (len(assets), len(items)))

    def one(kv):
        u, meta = kv
        r = measure_asset(u, meta["kind"], timeout=timeout)
        return u, dict(meta, **r)

    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        res = dict(ex.map(one, items))
    doc = {"ts": time.time(), "cost": round(time.time() - t0, 1), "items": res}
    ST.save_file(path, doc)
    ok = sum(1 for v in res.values() if v.get("ok"))
    log("  资源测速完成：%d 个可用 / %d 个（%.0fs）" % (ok, len(res), doc["cost"]))
    return doc


def _count_sources(seeds_path):
    try:
        return len(load_sources_doc(seeds_path).get("sources") or [])
    except Exception:
        return 0


def _cfg_state_path(data_dir):
    return os.path.join(data_dir, "configs_state.json")


def load_cfg_state(data_dir):
    try:
        with open(_cfg_state_path(data_dir), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cfg_state(data_dir, doc):
    p = _cfg_state_path(data_dir)
    ST.save_file(p, doc)


def _merge_into_sources(seeds_path, cands, tag, log=print, room=None, src_name=""):
    """把一批 CMS 候选并进待聚合源（按域名+接口去重），标 pending 待聚合

    room 是这轮还能收多少个（受 max_seeds 和每源上限约束），防止一次灌爆。
    """
    from urllib.parse import urlsplit
    doc = load_sources_doc(seeds_path)
    _dd0 = os.path.dirname(os.path.abspath(seeds_path))
    _act, _bnc = load_config_urls(_dd0)
    if _act or _bnc:
        _keep0 = [x for x in doc.get("sources") or [] if src_allowed(x, _act)]
        if len(_keep0) != len(doc.get("sources") or []):
            log("  只聚合启用配置接口的采集站点：%d → %d"
                % (len(doc.get("sources") or []), len(_keep0)))
        doc["sources"] = _keep0
    have_api = {norm(x.get("api", "")) for x in doc["sources"] if x.get("api")}
    # 以前按域名去重，同域只留一个 —— 但配置里同一台机器常同时提供 json 和 xml 两个接口，
    # 全被砍掉后站点数直接腰斩（实测 96 个候选只剩 18）。改成：按完整地址去重，
    # 同一域名最多留 3 条（json/xml/at 这种变体）。
    from collections import Counter
    per_host = Counter((urlsplit(norm(x.get("api", ""))).hostname or "").lower()
                       for x in doc["sources"] if x.get("api"))
    MAX_PER_HOST = 3
    added = 0
    changed = False
    limit = len(doc["sources"]) + (room if room is not None else 15)
    for c in cands:
        if len(doc["sources"]) >= limit:
            log("    （已达本轮收录上限，剩余 %d 个留到下轮）"
                % (len(cands) - added))
            break
        api = norm(c.get("api") or "")
        if api in have_api:
            # 原接口又回来了：把离源条目重新挂回它（否则会一直显示"离源"）
            for x in doc["sources"]:
                if norm(x.get("api") or "") == api and x.get("orphan"):
                    x["from"] = tag
                    x["src_name"] = src_name
                    x.pop("orphan", None)
                    x.pop("orphaned_from", None)
                    x.pop("orphaned_name", None)
                    x.pop("orphaned_at", None)
                    changed = True
                    break
            continue
        if not api:
            continue
        host = (urlsplit(api).hostname or "").lower()
        if host and per_host[host] >= MAX_PER_HOST:
            continue
        doc["sources"].append({"name": c.get("name") or site_key(api),
                               "api": api, "from": tag, "src_name": src_name,
                               "pending": True})
        have_api.add(api)
        if host:
            per_host[host] += 1
        added += 1
    if added or changed:
        dump_sources_doc(seeds_path, doc)
    return added


def _merge_into_jars(data_dir, assets_doc, log=print, src=""):
    """把 jar/js/py 站点与 parses/flags 并进 jars.json"""
    path = os.path.join(data_dir, "jars.json")
    try:
        with open(path, encoding="utf-8") as f:
            cur = json.load(f)
    except Exception:
        cur = {"sites": [], "parses": [], "flags": [], "jars": [], "configs": 0}
    seen = {"%s|%s" % (x.get("api"), x.get("ext") or "")
            for x in cur.get("sites") or []}
    pseen = {"%s|%s" % (x.get("type"), x.get("url"))
             for x in cur.get("parses") or []}
    added = 0
    for x in assets_doc.get("sites") or []:
        k = "%s|%s" % (x.get("api"), x.get("ext") or "")
        if k in seen:
            continue
        seen.add(k)
        cur.setdefault("sites", []).append(x)
        added += 1
    for x in assets_doc.get("parses") or []:
        k = "%s|%s" % (x.get("type"), x.get("url"))
        if k in pseen:
            continue
        pseen.add(k)
        cur.setdefault("parses", []).append(x)
    for f0 in assets_doc.get("flags") or []:
        if f0 not in cur.setdefault("flags", []):
            cur["flags"].append(f0)
    cur["generated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    ST.save_file(path, cur)
    return added


def _merge_lives(data_dir, lives, src, log=print):
    """把配置里的直播源收进 data/lives.json（先只解析+汇总，暂不进聚合输出）"""
    path = os.path.join(data_dir, "lives.json")
    try:
        with open(path, encoding="utf-8") as f:
            cur = json.load(f)
    except Exception:
        cur = {"updated": "", "items": []}
    seen = {x.get("url") for x in cur.get("items") or []}
    # 墓碑：屏蔽的不要再收；删掉的按「新源」收进备选并标记待解析
    try:
        import live as _L
        tomb = _L.load_tomb(data_dir)
    except Exception:
        _L, tomb = None, {}
    added = 0
    for lv in lives or []:
        u = str(lv.get("url") or "").strip()
        if not u or u in seen:
            continue
        rec = tomb.get(u) or {}
        if rec.get("kind") == "blocked":
            continue                                  # 还在屏蔽期
        seen.add(u)
        item = {"name": lv.get("name") or u, "url": u,
                "ua": lv.get("ua") or "", "epg": lv.get("epg") or "",
                "from": src}
        if rec.get("kind") == "deleted":
            # 删过又被配置带回来 → 当新源：进备选，解析一次再看值不值得补位
            item.update({"tier": "bench", "needs_parse": 1})
            tomb.pop(u, None)
            try:
                _L.save_tomb(data_dir, tomb)
            except Exception:
                pass
        cur.setdefault("items", []).append(item)
        added += 1
    if added:
        cur["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
        ST.save_file(path, cur)
    return added


def sync_config_sources(data_dir, seeds_path=None, settings=None,
                        force=False, only=None, log=print):
    """拉取到期的配置源；返回每个源的结果列表"""
    import discover as D          # 延迟导入：discover 依赖本模块

    settings = settings or load_settings(os.path.join(data_dir, "settings.json"))
    try:
        # 手改 mirrors.json 也能在下一轮立即生效，不必重启
        D.load_mirrors(data_dir)
    except Exception:
        pass
    days = max(1, int(settings.get("config_days") or 5))
    max_fail = max(1, int(settings.get("config_max_fail") or 3))
    seeds_path = seeds_path or os.path.join(data_dir, "sources.json")
    cfg_path = os.path.join(data_dir, "configs.json")
    try:
        with open(cfg_path, encoding="utf-8") as f:
            reg = json.load(f)
    except Exception:
        return []
    entries = _tier_map(reg.get("configs") or [])
    state = load_cfg_state(data_dir)
    now = time.time()
    results = []

    _before = len(entries)
    entries, promoted, demoted = balance_configs(entries, state, settings, log)
    if len(entries) != _before:
        log("  !! 整理线路时条目数变了（%d → %d），本轮不写回，保留原文件"
            % (_before, len(entries)))
        return []
    active_urls = {e.get("url") for e in entries if e.get("tier") != "bench"}
    if int(settings.get("drop_bench_sites") or 0):
        for e in demoted:
            n = drop_sites_of(data_dir, e.get("url"))
            if n:
                log("  下沉线路剔除 %d 个已并入站点：%s" % (n, e.get("url")))

    for ent in list(entries):
        url = ent.get("url") if isinstance(ent, dict) else ent
        if not url:
            continue
        if only and url != only:
            continue
        if not only and url not in active_urls:
            continue                      # 备选库不解析，省时间
        st = state.get(url) or {}
        due = force or only or (now - float(st.get("last") or 0)) >= days * 86400
        if not due:
            continue
        name = ent.get("name") if isinstance(ent, dict) else url
        t0 = time.time()
        try:
            cands = D.scan(url, log=lambda *a: None)              # CMS（type 0/1）
            assets = D.collect_profiles([url], workers=1,
                                        log=lambda *a: None)     # jar/js/py
            latency = int((time.time() - t0) * 1000)
            # 网络没通和「通了但确实 0 站点」是两码事：
            # 前者要记失败（不能算产出 0 去下沉），后者才是真没货。
            if not D.fetch_ok(url):
                raise RuntimeError(D.last_error(url) or "拉取失败（直连与镜像都不通）")
            if not cands and not (assets.get("sites") or []) and D.last_error(url):
                raise RuntimeError(D.last_error(url))
            kinds = {}
            for x in assets.get("sites") or []:
                kinds[x.get("kind") or "jar"] = kinds.get(x.get("kind") or "jar", 0) + 1
            # 只有「启用的配置接口」才把站点并进池子；
            # 备选配置接口解析只用于看产出数量（初筛），不污染聚合池。
            _is_active = (ent.get("tier") != "bench")
            if _is_active:
                _merge_lives(data_dir, assets.get("lives") or [], url, log)
            max_seeds = int(settings.get("max_seeds") or 120)
            _room_cap = int(settings.get("merge_room") or 40)
            room = max(0, min(_room_cap, max_seeds - _count_sources(seeds_path)))
            if _is_active:
                added_cms = _merge_into_sources(seeds_path, cands, url,
                                                log, room=room, src_name=name or "")
                added_ext = _merge_into_jars(data_dir, assets, log, src=url)
            else:
                added_cms = added_ext = 0
            st.update(last=now, latency=latency, fail=0, ok=True,
                      stats={"cms": len(cands), "jar": kinds.get("jar", 0),
                             "js": kinds.get("js", 0), "py": kinds.get("py", 0),
                             "lives": len(assets.get("lives") or [])},
                      added_cms=int(st.get("added_cms") or 0) + added_cms,
                      added_ext=int(st.get("added_ext") or 0) + added_ext,
                      error="")
            log("  [CFG] %-44s %d ms ｜ CMS %d（新 %d）jar %d js %d py %d（新 %d）"
                % ((name or url)[:44], latency, len(cands), added_cms,
                   kinds.get("jar", 0), kinds.get("js", 0), kinds.get("py", 0),
                   added_ext))
            total_gain = (len(cands) + kinds.get("jar", 0) + kinds.get("js", 0)
                          + kinds.get("py", 0))
            if total_gain == 0:
                st["zero"] = int(st.get("zero") or 0) + 1
                if st["zero"] >= int(settings.get("zero_bench") or 2) \
                        and isinstance(ent, dict) and ent.get("tier") != "bench":
                    ent["tier"] = "bench"
                    log("  [CFG] 连续 %d 次解析出 0 个站点，下沉到备选库：%s"
                        % (st["zero"], url))
            else:
                st["zero"] = 0
        except Exception as e:
            st["fail"] = int(st.get("fail") or 0) + 1
            st["ok"] = False
            st["error"] = "%s: %s" % (type(e).__name__, e)
            st["last"] = now
            log("  [CFG] %-44s 拉取失败（%d/%d）：%s"
                % ((name or url)[:44], st["fail"], max_fail, st["error"]))
            if st["fail"] >= max_fail:
                entries = [e2 for e2 in entries
                           if (e2.get("url") if isinstance(e2, dict) else e2) != url]
                _n = orphan_of_config(data_dir, url, name or "")
                log("  [CFG] 连续 %d 次拉不到，已从源库删除：%s（离源保留 %d 站点）"
                    % (st["fail"], url, _n))
                st["removed"] = True
        state[url] = st
        save_cfg_state(data_dir, state)     # 每处理完一个就落盘，中途崩了也不丢
        results.append({"url": url, "name": name, **st})

    # ---- 备选接口初筛：只浅解析（不探活），给「自动补位」一个判断依据
    # 之前备选完全不解析（`url not in active_urls: continue`），
    # 补位时拿到的 score 全是 0 —— 等于闭着眼睛往里拉。
    bench_days = max(1, int(settings.get("bench_days") or 2))
    screened = 0
    for ent in list(entries):
        if not isinstance(ent, dict) or ent.get("tier") != "bench":
            continue
        url = ent.get("url")
        if not url or (only and url != only):
            continue
        st = state.get(url) or {}
        if now - float(st.get("last_screen") or 0) < bench_days * 86400:
            continue
        try:
            docs = D.load_config(url, log=lambda *a: None)
            cnt = {"cms": 0, "jar": 0, "js": 0, "py": 0, "lives": 0}
            for _, doc0 in docs:
                for s0 in doc0.get("sites") or []:
                    t0 = s0.get("type")
                    if t0 in (0, 1):
                        cnt["cms"] += 1
                    elif t0 == 3:
                        k0 = s0.get("kind") or "jar"
                        cnt[k0] = cnt.get(k0, 0) + 1
                cnt["lives"] += len(doc0.get("lives") or [])
            st.update(last_screen=now, stats=cnt, screen_only=True,
                      ok=bool(sum(cnt[k] for k in ("cms", "jar", "js", "py"))),
                      error="")
            screened += 1
            log("  [备选] %-40s 初筛 CMS %d ｜ jar %d js %d py %d"
                % ((ent.get("name") or url)[:40], cnt["cms"], cnt["jar"],
                   cnt["js"], cnt["py"]))
        except Exception as e:
            st.update(last_screen=now, ok=False, screen_only=True,
                      error="%s: %s" % (type(e).__name__, e))
            log("  [备选] %-40s 初筛失败：%s" % ((ent.get("name") or url)[:40], e))
        state[url] = st
        save_cfg_state(data_dir, state)
    if screened:
        # 初筛后重新整理一次：有货的备选才有资格被自动补位
        entries, promoted2, _dem2 = balance_configs(entries, state, settings, log)
        if promoted2:
            log("  初筛后自动补位 %d 个：%s" % (
                len(promoted2),
                "、".join((e.get("name") or e.get("url") or "")[:12] for e in promoted2)))

    reg["configs"] = entries
    ST.save_file(cfg_path, reg)
    save_cfg_state(data_dir, state)
    return results


def clear_pending(seeds_path, apis, log=print):
    """已在输出里的源，去掉「待聚合」标记"""
    doc = load_sources_doc(seeds_path)
    n = 0
    for x in doc["sources"]:
        if x.get("pending") and norm(x.get("api", "")) in apis:
            x.pop("pending", None)
            n += 1
    if n:
        dump_sources_doc(seeds_path, doc)
    return n


def prune_sources(seeds_path, state, prune_after=10, log=print):
    """把连续失败很多轮的种子从 sources.json 里摘掉

    不做这件事，日积月累会攒一堆死链，每轮体检都在浪费 40 秒超时。
    阈值默认 10 轮（6 小时一轮 ≈ 2.5 天），足够区分「暂时挂了」和「真死了」。
    """
    try:
        with open(seeds_path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception:
        return []
    keep, removed = [], []
    for s in doc.get("sources", []):
        st = state.get(norm(s.get("api", ""))) or {}
        fs = int(st.get("fail_streak") or 0)
        never_ok = not st.get("last_ok")
        # 配置源批量带进来的「待聚合」源：从没成功过、连续 2 次不通就立刻清掉，
        # 不然一次合并能塞进来几十个死链，白占体检预算
        if fs >= prune_after or (s.get("pending") and never_ok and fs >= 2):
            removed.append(s)
        else:
            keep.append(s)
    if not removed:
        return []
    doc["sources"] = keep
    ST.save_file(seeds_path, doc)
    for s in removed:
        log("  [PRUNE] 连续失败达 %d 轮，移除：%s" % (prune_after, s.get("api")))
    return removed


def run_pipeline(seeds_path=None, out_dir=None, workers=3, timeout=20,
                 state_path=None, drop_after=3, recheck=True, log=print,
                 proxy_base=None):
    """完整链路：体检 → 并状态 → 去重合并 → 双格式输出

    proxy_base 非空时，TVBox 输出的 site.api 指向 <proxy_base>/cms/<key>（本服务的
    透明代理），用于修正「采集站不认 t= 参数导致分类空白」。LibreTV 输出保持直连。
    返回 {"tvbox", "libretv", "report", "sources"}，供 CLI 与 server 共用。
    """
    seeds_path = seeds_path or os.path.join(HERE, "sources.json")
    out_dir = out_dir or os.path.join(HERE, "out")
    state_path = state_path or os.path.join(HERE, "state.json")
    t0 = time.time()

    try:
        with open(seeds_path, encoding="utf-8") as _sf:
            seed_doc = json.load(_sf)
    except Exception:
        # 全新部署时 data/ 是空的，没有 sources.json 很正常，当空池处理
        seed_doc = {"name": "自用聚合源", "sources": []}
    cfg_name = seed_doc.get("name") or "自用聚合源"
    overrides = load_overrides(os.path.join(
        os.path.dirname(os.path.abspath(seeds_path)), "overrides.json"))
    # 只聚合「启用的配置接口」解析出来的采集站点（备选配置的只做数量统计）
    _active_cfg, _bench_cfg = load_config_urls(
        os.path.dirname(os.path.abspath(seeds_path)))
    if _active_cfg or _bench_cfg:
        _all0 = seed_doc.get("sources", [])
        _keep0 = [x for x in _all0 if src_allowed(x, _active_cfg)]
        if len(_keep0) != len(_all0):
            log("  只聚合启用配置接口的采集站点：%d → %d"
                % (len(_all0), len(_keep0)))
        seed_doc["sources"] = _keep0
    seeds, disabled = [], 0
    for s in seed_doc.get("sources", []):
        if not s.get("api"):
            continue
        if is_private(s["api"]):
            log("  [SKIP] 内网地址：%s" % s["api"])
            continue
        if _ov_match(overrides["disabled"], s["api"], s.get("key")):
            disabled += 1
            continue
        if _ov_match(overrides["pinned"], s["api"], s.get("key")):
            s["_pin"] = 1
        if _ov_match(overrides["bench"], s["api"], s.get("key")):
            s["_bench"] = 1
        seeds.append(s)
    if disabled:
        log("  人工禁用 %d 个，本轮跳过" % disabled)
    # 先把上次的状态读出来：已知的分类参数名直接复用，不用再探
    state = load_state(state_path)
    for s in seeds:
        st0 = state.get(norm(s["api"])) or {}
        p = st0.get("param")
        if p:
            s["_param"] = p
        if "searchable" in st0:
            s["_search"] = int(st0["searchable"])
    # 备选池策略：主力站点够用时，慢的/手工标备选的这一轮不探（省一半以上时间），
    # 每 bench_every 轮补探一次；主站数量不足时当轮立刻全探。
    data_dir = os.path.dirname(os.path.abspath(seeds_path))
    stg = load_settings(os.path.join(data_dir, "settings.json"))
    active_max = int(os.environ.get("ACTIVE_MAX") or stg["active_max"])
    bench_every = int(os.environ.get("BENCH_EVERY") or stg["bench_every"])
    bench_latency = int(os.environ.get("BENCH_LATENCY") or stg["bench_latency"])
    probe_budget = int(os.environ.get("PROBE_BUDGET") or stg.get("probe_budget") or 40)
    round_no = int(state.get("_round") or 0) + 1
    state["_round"] = round_no
    good = 0
    for s in seeds:
        st = state.get(norm(s["api"])) or {}
        if int(st.get("fail_streak") or 0) == 0 and st.get("last_ok") \
                and (st.get("latency") or 0) <= bench_latency:
            good += 1
    skip_bench = good >= active_max and (round_no % bench_every) != 1

    def _last_probe(x):
        return (state.get(norm(x["api"])) or {}).get("last_probe") or 0

    pins = [s for s in seeds if s.get("_pin")]
    rest = [s for s in seeds if not s.get("_pin")]
    # 备选跳过：只跳「成功过」的，从没成功过的必须探一次
    keep, bench_skipped = [], []
    for s in rest:
        st = state.get(norm(s["api"])) or {}
        lat = st.get("latency") or 0
        if not st.get("last_ok"):
            keep.append(s)                      # 没成功过的必须探
        elif s.get("_bench") or (skip_bench and lat > bench_latency):
            bench_skipped.append(s)             # 只跳慢的尾部 / 手标备选
        else:
            keep.append(s)
    # 预算轮转：最久没探的优先（新源 last_probe=0 排最前），保证新加的很快被验证
    keep.sort(key=_last_probe)
    room = max(0, probe_budget - len(pins))
    todo = pins + keep[:room]
    probed_n = len(todo)      # 后面复核会复用 todo 这个名字，先存下来
    skipped = bench_skipped + keep[room:]
    if bench_skipped:
        log("  备选池：%d 个高延迟/手标站点本轮不探（第 %d 轮，主力 %d 个）"
            % (len(bench_skipped), round_no, good))
    if keep[room:]:
        log("  体检预算 %d 个：%d 个排队等下一轮（按最久没探的轮转）"
            % (probe_budget, len(keep[room:])))
    log("种子源 %d 条（本轮体检 %d），开始体检（并发 %d）…"
        % (len(seeds), len(todo), workers))

    results = [None] * len(seeds)
    idx = {id(x): i for i, x in enumerate(seeds)}
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(probe, s, timeout): idx[id(s)] for s in todo}
        for f in cf.as_completed(futs):
            i = futs[f]
            r = f.result()
            results[i] = r
            log("  [%s] type=%-4s %-46s %s" %
                ("UP  " if r["alive"] else "DOWN",
                 r["type"] if r["type"] is not None else "-",
                 r["api"][:46], r["msg"]))

    # 二次复核只挑「复核真正有价值」的少数时机：
    #   a) 首次见到就失败（否则直接摘除，没有豁免）
    #   b) 已经连续失败 drop_after-1 轮（再失败一次就被摘除）
    # 其余失败走「暂留」继续输出，下一轮自然恢复——之前对所有失败源都串行复核，
    # 29 个源里有 10 个死源，一轮能被拖到 16 分钟。
    dead = [i for i, r in enumerate(results) if r and not r["alive"]]
    if dead and recheck:
        recheck_list = []
        for i in dead:
            st = state.get(norm(results[i]["api"])) or {}
            fs = int(st.get("fail_streak") or 0)
            need = (not st.get("last_ok")) or fs >= drop_after - 1
            if not need:
                log("  [SKIP] 连续失败 %s 轮，走暂留，不复核：%s" %
                    (fs, results[i]["api"][:44]))
                continue
            recheck_list.append(i)
        if len(recheck_list) > 6:
            log("  需复核 %d 个，只做前 6 个" % len(recheck_list))
            recheck_list = recheck_list[:6]
        log("  二次串行复核 %d 个（超时 %ds）…" % (len(recheck_list), timeout * 2))
        for i in recheck_list:
            r2 = probe(seeds[i], timeout=timeout * 2, retries=0, with_classes=False)
            r2["recheck"] = True
            if r2["alive"]:
                results[i] = r2
                log("  [RECOVER] %-46s %s" % (r2["api"][:46], r2["msg"]))
            else:
                log("  [STILL-DOWN] %-43s %s" % (r2["api"][:43], r2["msg"]))

    for i, sd in enumerate(seeds):
        if results[i] is not None:
            continue
        st = state.get(norm(sd["api"])) or {}
        results[i] = {
            "name": clean_name(sd.get("name")), "api": norm(sd["api"]),
            "from": sd.get("from", ""), "alive": bool(st.get("last_ok")),
            "type": st.get("type"), "msg": "备选（本轮未探）",
            "items": 0, "playable": 0, "total": 0, "classes": [],
            "class_ids": [], "param": st.get("param"), "sample": "",
            "recheck": False, "partial": False, "skipped": True,
            "latency": st.get("latency") or 0,
            "searchable": int(st.get("searchable", 1)),
        }

    rows = [r for r in results if r]
    apply_state(rows, state, drop_after)
    save_state(state_path, state)

    kept = merge(rows)
    cand = [r for r in kept if r["alive"] or r.get("grace")]
    min_active = int(os.environ.get("MIN_ACTIVE", "8"))
    pins = [r for r in cand if _ov_match(overrides["pinned"], r["api"], r.get("key"))]
    for r in pins:
        r["_pin"] = 1
    rest = [r for r in cand if r not in pins]
    # 不可搜的站（假搜索/接口报错）排在最后、不占主站名额：
    # 它们在客户端里点搜索会污染结果，只适合当「能浏览的兜底」
    def is_ok(r):
        return 1 if int(r.get("searchable", 1)) == 1 else 0
    rest.sort(key=lambda r: (-is_ok(r), 1 if r.get("skipped") else 0,
                             r.get("latency") or 999999))
    slots = max(active_max - len(pins), min_active)
    can_search = [r for r in rest if is_ok(r)]
    no_search = [r for r in rest if not is_ok(r)]
    taken = can_search[:slots]
    # 可搜的站占不满名额时，才让不可搜的补最后的位子
    fill = max(0, min(slots - len(taken), len(no_search)))
    included = pins + taken + no_search[:fill]
    bench = [r for r in rest if r not in included]
    if bench:
        log("  主站 %d 个；%d 个进备选池（延迟高或手工标备选）"
            % (len(included), len(bench)))
    # 进了输出的新源，去掉「待聚合」标记
    try:
        clear_pending(seeds_path, {r["api"] for r in included if r["alive"]}, log)
    except Exception:
        pass
    # 客户端默认选中第一个站点，所以排最前的很重要
    sort_mode = str(stg.get("sort_mode") or "kind")
    if sort_mode == "name":
        included.sort(key=lambda r: (0 if r.get("_pin") else 1,
                                     0 if r["alive"] else 1,
                                     r.get("name") or ""))
    else:                                  # latency / kind 都用延迟（CMS 只有一种类型）
        included.sort(key=lambda r: (0 if r.get("_pin") else 1,
                                     0 if r["alive"] else 1,
                                     r.get("latency") or 999999))
    os.makedirs(out_dir, exist_ok=True)
    max_cat = int(os.environ.get("MAX_CATEGORIES") or stg.get("max_categories") or 12)
    ltv = build_libretv(cfg_name, included,
                        max_sources=int(stg.get("ltv_cms") or 100))          # LibreTV 只吃 CMS，直连上游
    # 默认档给影视仓：实测它支持 type0/1 + jar + py，不支持 js
    tvbox = build_shell_profile(cfg_name, included, {}, proxy_base,
                                kinds=("jar", "py"), label="默认",
                                include_pan=False, max_categories=max_cat)
    direct = build_tvbox(cfg_name, included, None, max_categories=max_cat)

    # 全能档：jar + js + py（网盘类仍然不进来）
    allp = build_shell_profile(cfg_name, included, {}, proxy_base,
                               kinds=("jar", "js", "py"), label="全能",
                               include_pan=False, max_categories=max_cat)

    # 有扩展资源池时补上真实站点；没有就保持纯 CMS
    full = None
    assets_path = os.path.join(os.path.dirname(os.path.abspath(seeds_path)),
                               "jars.json")
    if os.path.exists(assets_path):
        try:
            with open(assets_path, encoding="utf-8") as f:
                assets_doc = json.load(f)
            # 只聚合启用配置接口的站点：备选配置解析出来的只做数量统计，不进池子
            _dd = os.path.dirname(os.path.abspath(seeds_path))
            _active, _bench = load_config_urls(_dd)
            if _active or _bench:
                _all = assets_doc.get("sites") or []
                _keep = [x for x in _all if src_allowed(x, _active)]
                if len(_keep) != len(_all):
                    log("  只聚合启用配置接口的站点：扩展站点 %d → %d（剔除备选配置来的 %d 个）"
                        % (len(_all), len(_keep), len(_all) - len(_keep)))
                assets_doc["sites"] = _keep
            asset_ok_path = os.path.join(
                os.path.dirname(os.path.abspath(state_path)), "jar_state.json")
            try:
                with open(asset_ok_path, encoding="utf-8") as f:
                    asset_ok = json.load(f)
            except Exception:
                asset_ok = {}
            block = load_block(os.path.join(
                os.path.dirname(os.path.abspath(seeds_path)), "block.json"))
            alive = [r for r in included if r["alive"]]
            common = dict(proxy_base=proxy_base,
                          max_assets=int(os.environ.get("MAX_ASSETS", "25")),
                          max_per_asset=int(os.environ.get("MAX_PER_ASSET")
                                            or stg.get("max_per_asset") or 30),
                          max_categories=max_cat,
                          asset_ok=asset_ok, block=block, log=log)
            _spd = {}
            try:
                with open(os.path.join(os.path.dirname(os.path.abspath(seeds_path)),
                                       "asset_speed.json"), encoding="utf-8") as f:
                    _spd = (json.load(f).get("items") or {})
            except Exception:
                _spd = {}
            kw_default = {k: v for k, v in common.items() if k != "proxy_base"}
            kw_default["sort_mode"] = str(stg.get("sort_mode") or "kind")
            kw_default["speed"] = _spd
            kw_default["max_sites"] = int(stg.get("ext_default") or 120)
            tvbox = build_shell_profile(cfg_name, alive, assets_doc, proxy_base,
                                        kinds=("jar", "py"), label="默认",
                                        include_pan=False, **kw_default)
            kw_all = dict(kw_default)
            kw_all["max_sites"] = int(stg.get("ext_all") or 120)
            allp = build_shell_profile(cfg_name, alive, assets_doc, proxy_base,
                                       kinds=("jar", "js", "py"), label="全能",
                                       include_pan=False, **kw_all)
            full = tvbox                      # 兼容旧字段
            with open(asset_ok_path, "w", encoding="utf-8") as f:
                json.dump(asset_ok, f, ensure_ascii=False, indent=1)
        except Exception as e:
            log("  !! 壳子 profile 生成失败：%s: %s" % (type(e).__name__, e))
    else:
        log("  （没有 jars.json，跳过扩展站点；跑 discover.py --collect-jars 生成）")

    try:
        import jargate as JG
        _blocked_jars = JG.blocked_urls(data_dir)
    except Exception:
        _blocked_jars = set()
    if _blocked_jars:
        for _doc in (tvbox, allp):
            _keep = [x for x in (_doc.get("sites") or [])
                     if (x.get("jar") or "") not in _blocked_jars]
            if len(_keep) != len(_doc.get("sites") or []):
                log("  安全扫描：从输出里移除 %d 个站点"
                    % (len(_doc.get("sites") or []) - len(_keep)))
                _doc["sites"] = _keep
    if int(stg.get("embed_lives", 1)):
        for _doc in (tvbox, allp):
            embed_lives(_doc, data_dir, lines_per=int(stg.get("live_lines_per") or 4))
    # 网盘（Alist）：做成一个普通 CMS 站点；排第几由 home_site 决定
    _asite = None
    try:
        import alist as _AL
        _asite = _AL.site_entry(data_dir, proxy_base or "")
    except Exception as e:                              # noqa: BLE001
        log("  跳过网盘站点：%s" % e)
    if _asite:
        _asite = {k: v for k, v in _asite.items() if not str(k).startswith("_")}

    _home = str(stg.get("home_site") or "").strip()
    if not _home:                       # 老配置没有这个键，按 douban_first 推断
        _home = "douban" if int(stg.get("douban_first", 1)) else "none"
    _suffix = str(stg.get("stamp_suffix") or "").strip()

    if _home == "douban":
        for _doc in (tvbox, allp):
            _jar = pick_douban_jar(_doc.get("sites") or [], log=log,
                                   cache_dir=data_dir)
            if not _jar:
                log("  跳过豆瓣首站：这份配置里没有可用的 spider jar")
                continue
            # csp_Douban 要靠配置级 spider 才能加载，必须写上
            _doc["spider"] = _jar
            if insert_douban(_doc, _suffix or "豆瓣"):
                log("  首个站点：内置豆瓣（spider=%s）" % _jar[-34:])
    if _asite:
        # 首站是网盘时，名字跟着「时间后面的文字」走；没填就用「网盘」
        if _home == "alist":
            _asite["name"] = (_suffix if _suffix and _suffix != "豆瓣" else "网盘")
        for _doc in (tvbox, direct, allp):
            _sites = [x for x in (_doc.get("sites") or [])
                      if x.get("key") != _asite["key"]]
            if _home == "alist":
                _sites.insert(0, dict(_asite))
            elif _home == "douban":
                pos = 1 if _sites and _sites[0].get("key") == "douban" else 0
                _sites.insert(pos, dict(_asite))
            else:
                _sites.append(dict(_asite))
            _doc["sites"] = _sites
        log("  网盘站点：%s" % ({"alist": "作为首站", "douban": "排在豆瓣之后"}
                              .get(_home, "排在最后")))

    stamp = (time.strftime("%Y-%m-%d  %H:%M")
             if int(stg.get("stamp_name", 1)) else "")
    for _doc in (tvbox, direct, allp):
        stamp_first_site(_doc, stamp)

    # 异常兜底：这一轮产出的站点太少（上游集体抖动/配置写坏）就退回上一次成功的
    rolled = False
    if len(tvbox["sites"]) < int(stg["auto_floor"]):
        back = restore_lastgood(data_dir, out_dir, log=log)
        if back:
            tvbox = back["tvbox.json"]
            ltv = back.get("ltv.json") or ltv
            allp = back.get("all.json") or allp
            direct = back.get("tvbox-direct.json") or direct
            rolled = True
            included = [{"key": s0.get("key"), "name": s0.get("name"),
                         "api": s0.get("api"), "type": s0.get("type"),
                         "alive": True, "grace": False, "fail_streak": 0,
                         "last_ok": None, "msg": "退回快照",
                         "items": 0, "playable": 0, "total": 0, "sample": "",
                         "param": None, "searchable": 1, "latency": 0,
                         "classes": []}
                        for s0 in tvbox.get("sites") or []]

    # 全部处理完再落盘：三份配置 + LibreTV
    for fn, doc in (("tvbox.json", tvbox), ("tvbox-direct.json", direct),
                    ("all.json", allp), ("ltv.json", ltv)):
        with open(os.path.join(out_dir, fn), "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=1)

    rep = write_report(out_dir, rows, kept, included, tvbox, ltv,
                       time.time() - t0, drop_after, bench=bench,
                       probed=probed_n, skipped=len(seeds) - probed_n)
    if rolled:
        rep["rolled_back"] = True
        with open(os.path.join(out_dir, "report.json"), "w", encoding="utf-8") as f:
            json.dump(rep, f, ensure_ascii=False, indent=1)
    elif len(tvbox["sites"]) >= int(stg["auto_floor"]):
        # 正常的一轮才更新快照，免得把坏结果存成"上一次成功"
        try:
            save_lastgood(out_dir, data_dir, {
                "t": rep["generated"], "sites": len(tvbox["sites"]),
                "alive": rep["alive"], "active": rep["active"],
                "bench": rep["bench_n"]})
        except Exception as e:
            log("  !! 保存快照失败：%s" % e)
    if full:
        rep["full_sites"] = len(full["sites"])
        rep["full_parses"] = len(full["parses"])
    if allp:
        rep["all_sites"] = len(allp["sites"])
    # 历史趋势：每轮一行，面板画折线、判断源在不在衰减
    try:
        hist_path = os.path.join(os.path.dirname(os.path.abspath(seeds_path)),
                                 "history.jsonl")
        lat = [r.get("latency") or 0 for r in included if r["alive"]]
        with open(hist_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "t": rep["generated"], "alive": rep["alive"],
                "grace": rep["grace"], "dead": rep["dead"],
                "active": len(included), "bench": len(bench or []),
                "sites": rep["tvbox_sites"],
                "avg_latency": int(sum(lat) / len(lat)) if lat else 0,
            }, ensure_ascii=False) + "\n")
        if os.path.getsize(hist_path) > 200_000:      # 留最近约 800 轮
            with open(hist_path, encoding="utf-8") as f:
                lines = f.readlines()[-800:]
            with open(hist_path, "w", encoding="utf-8") as f:
                f.writelines(lines)
    except Exception:
        pass

    # 收尾：把长期失效的种子从 sources.json 里清掉，避免越攒越多
    prune_sources(seeds_path, state, prune_after=max(drop_after * 3, 10), log=log)
    return {"tvbox": tvbox, "libretv": ltv, "report": rep,
            "sources": included, "full": tvbox, "all": allp, "direct": direct}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default=os.path.join(HERE, "sources.json"))
    ap.add_argument("--out", default=os.path.join(HERE, "out"))
    ap.add_argument("--state", default=os.path.join(HERE, "state.json"))
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--drop-after", type=int, default=3,
                    help="连续失败几次才摘除（默认 3）")
    ap.add_argument("--no-recheck", action="store_true",
                    help="跳过失败源的二次串行复核")
    a = ap.parse_args()

    res = run_pipeline(a.seeds, a.out, a.workers, a.timeout, a.state,
                       a.drop_after, not a.no_recheck)
    rep = res["report"]
    print("\n==== 结果 ====")
    print("  在线 %d / 暂留 %d / 摘除 %d / 种子 %d" %
          (rep["alive"], rep["grace"], rep["dead"], rep["seeds"]))
    print("  TVBox  → %d 个站点  %s" % (rep["tvbox_sites"], os.path.join(a.out, "tvbox.json")))
    print("  LibreTV→ %d 个源    %s" % (rep["libretv_sources"], os.path.join(a.out, "libretv.json")))
    if rep["libretv_skipped"]:
        print("  LibreTV 跳过 %d 个非 type=1 源：%s" %
              (len(rep["libretv_skipped"]),
               ", ".join(x["name"] for x in rep["libretv_skipped"])))
    if rep["dropped"]:
        print("  摘除 %d 个失效源（详见 report.md）" % len(rep["dropped"]))
    print("  耗时 %.1fs" % rep["elapsed_sec"])
    return 0 if rep["tvbox_sites"] else 2


if __name__ == "__main__":
    sys.exit(main())
