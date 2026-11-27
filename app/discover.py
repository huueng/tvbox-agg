#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""扫源：从多仓 / 单仓配置里抽出可用的 CMS 接口，探活后并入 sources.json

为什么需要它：公开采集站生命周期只有 6~8 个月，12 个种子撑不住长期运行。
现成的配置站（尤其用户自己的多仓）里躺着几十上百个站点，但 90% 是
spider/jar 型（type=3）或已失效，必须"抽出来 → 逐个探活 → 只留能直连的"。

流程：
  load_config(多仓)  → 递归子仓 → 抽 sites[].api
                     → 加密配置用 tvboxcfg.decode 解
  candidates         → probe 探活（必须真拉到可播数据）
  --merge            → 按域名去重后追加到 sources.json

用法：
  python discover.py --from http://192.168.1.10:6/
  python discover.py --from http://192.168.1.10:6/ --check --merge
  python discover.py --from http://192.168.1.10:6/ --dry-run      # 只看抽到多少
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
import concurrent.futures as cf
from urllib.parse import urlsplit

import aggregate as A
import tvboxcfg

HERE = os.path.dirname(os.path.abspath(__file__))

# 采集接口的常见形态
API_RE = re.compile(
    r"https?://[^\s\"'<>,\\）]+?/api\.php/provide/vod"
    r"(?:/(?:at|from)/[^\s\"'<>,\\）]*)?", re.I)

# 配置站几乎都按 TVBox 的 UA 放行，用 Chrome UA 会被 403
CFG_UA = "okhttp/3.15"


_CFG_CACHE = {}          # url -> (ts, [(name, doc)])，同一次解析里只下载一次


# GitHub 原始地址在国内经常「连上了但返回空」，超时能拖到 6 分钟。
# 镜像模板可改：data/mirrors.json 里写一个字符串数组，支持两种占位符
#   {url}                                   整条原始地址（前缀型镜像，如 gh-proxy）
#   {user} {repo} {branch} {path}           仅当原地址是 GitHub raw 时可用（如 jsdelivr）
MIRROR_DEFAULT = [
    "https://gh-proxy.com/{url}",
    "https://ghfast.top/{url}",
    "https://cdn.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}",
]
MIRRORS = list(MIRROR_DEFAULT)


def load_mirrors(data_dir=None):
    """读 data/mirrors.json；读不到或写坏了就用默认，不让配置拖死网络"""
    global MIRRORS
    path = os.path.join(data_dir, "mirrors.json") if data_dir else \
        os.path.join(HERE, "mirrors.json")
    try:
        with open(path, encoding="utf-8") as f:
            got = json.load(f)
        items = got.get("mirrors") if isinstance(got, dict) else got
        items = [str(x).strip() for x in (items or []) if str(x).strip()]
        MIRRORS = items or list(MIRROR_DEFAULT)
    except Exception:
        MIRRORS = list(MIRROR_DEFAULT)
    return MIRRORS


def set_mirrors(items):
    global MIRRORS
    items = [str(x).strip() for x in (items or []) if str(x).strip()]
    MIRRORS = items or list(MIRROR_DEFAULT)
    return MIRRORS


def github_mirrors(url):
    m = re.match(r"https?://raw\.githubusercontent\.com/([^/]+)/([^/]+)/"
                 r"(?:refs/heads/)?([^/]+)/(.+)", url or "")
    out = []
    # 镜像只对 GitHub 地址有意义。以前这里对**所有** url 都套 {url} 模板，
    # 结果 https://api.hgyx.vip/hgyx.json 被拼成 gh-proxy 前缀，拿到一张错误页，
    # 解析成「不是 JSON」→ 0 产出。这个门必须留着。
    is_gh = ("githubusercontent.com" in (url or "")) or ("github.com" in (url or ""))
    if not is_gh:
        return []
    # 已经是镜像地址就别再套一层（gh-proxy.com/https://gh-proxy.com/... 会白等一轮）
    for host in ("gh-proxy.com", "ghfast.top", "cdn.jsdelivr.net", "ghproxy.net"):
        if host in (url or ""):
            return []
    for tpl in MIRRORS:
        try:
            if "{user}" in tpl:
                if not m:
                    continue           # 不是 raw 地址，这条模板用不上
                user, repo, branch, path = m.groups()
                out.append(tpl.format(user=user, repo=repo, branch=branch,
                                      path=path, url=url))
            else:
                out.append(tpl.replace("{url}", url))
        except Exception:
            continue
    # 去重 + 去掉等于原始地址的
    seen, res = set(), []
    for u in out:
        if u and u != url and u not in seen:
            seen.add(u)
            res.append(u)
    return res


def _fetch_hard(url, timeout=25):
    """带硬超时的抓取

    A.fetch 的 timeout 只管 socket 读，遇到「连上了但一直不吐数据」会挂到系统超时
    （实测 raw.githubusercontent 能耗 6 分钟）。这里用线程 + 队列兜底，
    到点直接放弃，剩下的线程自己烂在后台。
    """
    import queue
    import threading

    q = queue.Queue()

    def _work():
        try:
            q.put((True, A.fetch(url, timeout=timeout, done=None, retries=0, ua=CFG_UA)))
        except Exception as e:                    # noqa: BLE001
            q.put((False, e))

    threading.Thread(target=_work, daemon=True).start()
    try:
        ok, val = q.get(timeout=timeout + 3)
    except queue.Empty:
        raise TimeoutError("超过 %ds 没有响应" % (timeout + 3))
    if not ok:
        raise val
    return val


def _fetch_config(url, log=print, timeout=25):
    """抓配置：GitHub 地址**直接走镜像**，其它地址先直连

    为什么 GitHub 不先试直连：raw.githubusercontent 在部分网络下是
    「连得上、返回空、但要等到 6 分钟才断」——试过 25 秒超时也没用，
    socket 层面不返回。与其干等 6 分钟，不如先用镜像（1~3 秒就有结果）。
    """
    errs = []
    mirrors = github_mirrors(url)
    order = mirrors + [url] if mirrors else [url]
    for alt in order:
        host = alt.split("/")[2] if "://" in alt else alt
        try:
            if alt != url:
                log("  ↻ 走镜像：%s" % host)
            raw = _fetch_hard(alt, timeout=timeout)
            if raw:
                return raw
            errs.append("%s(空响应)" % host)
        except Exception as e:
            errs.append("%s(%s)" % (host, type(e).__name__))
    raise OSError("直连和镜像都失败：" + "，".join(errs))


_FETCH_STATE = {}          # url -> True(拿到内容) / False(网络失败)
_LAST_ERR = {}             # url -> 人话原因（给面板显示）


def last_error(url):
    return _LAST_ERR.get(url, "")


def fetch_ok(url):
    """上一次 load_config 这条 url 到底有没有抓到内容"""
    return _FETCH_STATE.get(url, True)


def load_config(url, depth=0, seen=None, log=print):
    """读一个配置（可能是多仓、可能是加密的），返回 [(配置名, dict)]"""
    hit = _CFG_CACHE.get(url)
    if hit and time.time() - hit[0] < 120:
        return hit[1]
    seen = seen if seen is not None else set()
    if depth > 2 or url in seen:
        return []
    seen.add(url)
    try:
        # 有些配置仓慢得离谱（实测 szyyds 要 50 秒）；GitHub 原始地址还会「空响应」，
        # 所以先短超时直连，失败自动换镜像（见 _fetch_config）
        raw = _fetch_config(url, log)
    except Exception as e:
        log("  !! 读取失败 %s（%s）" % (url, e))
        _FETCH_STATE[url] = False
        _LAST_ERR[url] = "抓取失败：%s" % e
        return []
    _FETCH_STATE[url] = True
    _LAST_ERR.pop(url, None)

    # 有些配置带 UTF-8 BOM（\ufeff），str.strip() 不认它，
    # 结果既不是 JSON 也不是加密串，整份配置被当空——实测 szyyds 就是这种。
    text = raw.decode("utf-8", "replace").lstrip("\ufeff").strip()
    # 先按 JSON 解析（容忍注释/BOM/尾逗号），失败才当加密处理。
    # 反过来会把「以 // 注释开头的 JSON」误送进解密器，报
    # "Invalid control character" 之类看不懂的错——实测南风线路就是这样。
    doc = A.loads_loose(text)
    if doc is None:
        try:
            text2, key, msg = tvboxcfg.decode(raw)
            log("     %s" % msg)
            doc = A.loads_loose(text2)
            if doc is not None:
                text = text2
        except Exception as e:
            log("  !! 解密失败 %s（%s）" % (url, e))
            _LAST_ERR[url] = "解密失败：%s" % e
            return []
    if not isinstance(doc, dict):
        log("  !! 不是 JSON 对象：%s" % url)
        _LAST_ERR[url] = "返回的不是配置（可能是网页/图片/错误页）"
        return []

    if depth == 0:
        pass      # 顶层结果在函数末尾统一缓存

    # 2) 多仓：{"urls":[{url,name}...]}
    if isinstance(doc.get("urls"), list):
        out = []
        for item in doc["urls"]:
            if isinstance(item, str):
                sub, name = item, ""
            elif isinstance(item, dict):
                sub, name = item.get("url"), item.get("name") or ""
            else:
                continue
            if not sub:
                continue
            log("   子仓 %s" % (name or sub))
            for n, d in load_config(A.norm(sub), depth + 1, seen, log):
                out.append((name or n, d))
        _CFG_CACHE[url] = (time.time(), out)
        return out
    out = [("", doc)]
    _CFG_CACHE[url] = (time.time(), out)
    return out


def clean_api(api):
    """把 api 里预烘焙的动作参数去掉

    配置里常见 `.../provide/vod?ac=list`（作者直接粘了列表地址），
    留着会导致我们拼出 `?ac=list&ac=videolist&pg=1` 这种废 URL。
    只保留非动作参数（有的站要 ?key=xxx）。
    """
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    p = urlsplit(api)
    drop = {"ac", "pg", "t", "ids", "wd", "type_id", "limit"}
    kept = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in drop]
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(kept), ""))


def extract(doc, src_name):
    """从一个配置里抽 type 0/1 的 CMS 接口"""
    got = []
    for s in (doc.get("sites") or []):
        if not isinstance(s, dict):
            continue
        if s.get("type") not in (0, 1):
            continue
        api = str(s.get("api") or "").strip()
        if not api:
            continue
        name = str(s.get("name") or "").strip()
        got.append({"name": name or A.site_key(api), "api": clean_api(A.norm(api)),
                    "type": s["type"], "from": src_name})
    return got


def scan(url, log=print):
    """扫一个配置，返回去重后的候选（内部按域名去重）"""
    cands, seen_api, seen_host = [], set(), set()
    for name, doc in load_config(url, log=log):
        for c in extract(doc, name or url):
            n = c["api"]
            host = (urlsplit(n).hostname or "").lower()
            if n in seen_api or host in seen_host or A.is_private(n):
                continue
            seen_api.add(n)
            seen_host.add(host)
            cands.append(c)
    return cands


def scan_many(urls, workers=4, log=print):
    """并发扫多个配置，全局按 api/域名去重"""
    out, seen_api, seen_host = [], set(), set()
    total = len(urls)

    def job(i_u):
        i, u = i_u
        got = scan(u)
        return i, u, got

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for i, u, got in ex.map(job, enumerate(urls)):
            tag = "抽到 %d" % len(got) if got else "无 type0/1"
            log("  [%2d/%2d] %-52s %s" % (i + 1, total, u[:52], tag))
            for c in got:
                h = (urlsplit(c["api"]).hostname or "").lower()
                if c["api"] in seen_api or h in seen_host:
                    continue
                seen_api.add(c["api"])
                seen_host.add(h)
                out.append(c)
    return out


def load_registry(path):
    """读源库：{"configs":[{name,url}]} 或 ["url", ...]"""
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    if isinstance(doc, dict):
        items = doc.get("configs") or doc.get("urls") or []
    else:
        items = doc
    urls = []
    for it in items:
        if isinstance(it, str):
            urls.append(it)
        elif isinstance(it, dict) and it.get("url"):
            urls.append(it["url"])
    return urls


def load_sources(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"name": "自用聚合源", "sources": []}


# ---------------------------------------------------------------- 全量 profile 的原料
#
# 壳子类客户端（影视仓 / Fongmi）自己能跑 jar，所以 type=3 的站点对我们不是废物：
# 我们只负责「收集 + 去重 + 过滤 + 清洗 + 排序」，跑代码是客户端的事。
# 实测一个典型配置：246 个站点里 213 个是 type=3，只搬 CMS 等于丢掉 5/6。

def _abs(url, base):
    """把相对路径拼成绝对地址

    不能用 urljoin：它会把 `https://gh-proxy.com/https://raw...` 规范化成
    `https://gh-proxy.com/https:/raw...`（双斜杠塌成单斜杠），gh-proxy 这类
    「代理前缀 + 完整 URL」的地址就全废了。这里手工拼，只处理 ./ 和 ../。
    """
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "csp_")):
        return url
    if not base:
        return url
    b = base.split("?")[0].split("#")[0]
    i = b.find("//")
    tail = b[i + 2:] if i >= 0 else b
    d = b[:b.rfind("/") + 1] if "/" in tail else b + "/"
    path = url
    while path.startswith("./"):
        path = path[2:]
    while path.startswith("../"):
        path = path[3:]
        d = d[:-1]
        d = d[:d.rfind("/") + 1] if "/" in d else d
    return d + path


def collect_profiles(urls, workers=4, log=print):
    """扫配置，收集 type=3 站点 / parses / flags / lives（只读配置，不加载任何代码）"""
    sites, parses, flags, lives = [], [], [], []
    seen_site, seen_parse, seen_flag, seen_live = set(), set(), set(), set()
    jars = {}

    def site_id(s):
        raw = "%s|%s" % (s.get("api") or "", json.dumps(s.get("ext") or "",
                                                     ensure_ascii=False, sort_keys=True))
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def job(u):
        return u, load_config(u)

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for u, docs in ex.map(job, urls):
            got = 0
            for _, doc in docs:
                # 配置级全局 spider：很多配置把 jar 放在顶层，站点自己只写 csp_XXX。
                # 不认这个字段的话，整份配置的站点会全部丢掉（实测 116 个只剩 2 个 py）。
                gspider = str(doc.get("spider") or "").split(";")[0].strip()
                for s in (doc.get("sites") or []):
                    if not isinstance(s, dict) or s.get("type") != 3:
                        continue
                    api = _abs(s.get("api"), u)
                    jar = _abs(s.get("jar"), u)
                    if not api:
                        continue
                    # app 内置的功能站点（豆瓣首页/配置中心/本地文件），不是内容源
                    if api in ("csp_Config", "csp_LocalFile", "csp_Live") or \
                            any(k in (s.get("name") or "") for k in
                                ("配置", "公告", "教程", "声明", "更新日期", "更新时间")):
                        continue
                    if not jar and gspider and api.startswith("csp_"):
                        jar = _abs(gspider, u)
                    # 三种扩展型站点：jar spider / JS spider（drpy）/ Python spider
                    low = api.split("?")[0].lower()
                    if jar:
                        kind = "jar"
                    elif low.endswith(".js"):
                        kind = "js"
                    elif low.endswith(".py"):
                        kind = "py"
                    else:
                        continue          # type4 的 php 播放器我们暂不支持
                    sid = site_id({"api": api, "ext": s.get("ext")})
                    if sid in seen_site:
                        continue
                    seen_site.add(sid)
                    name = str(s.get("name") or "").strip() or api
                    sites.append({"name": name, "api": api, "jar": jar,
                                  "type": 3, "ext": s.get("ext") or "",
                                  "kind": kind, "from": u})
                    asset = jar or api
                    jars[asset] = jars.get(asset, 0) + 1
                    got += 1
                for p in (doc.get("parses") or []):
                    if not isinstance(p, dict):
                        continue
                    key = "%s|%s" % (p.get("type"), p.get("url") or p.get("name"))
                    if key in seen_parse:
                        continue
                    seen_parse.add(key)
                    parses.append({"name": p.get("name") or "",
                                   "type": int(p.get("type") or 0),
                                   "url": _abs(p.get("url"), u),
                                   "ext": p.get("ext") or {}})
                for f in (doc.get("flags") or []):
                    k = json.dumps(f, ensure_ascii=False, sort_keys=True)
                    if k in seen_flag:
                        continue
                    seen_flag.add(k)
                    flags.append(f)
                for lv in (doc.get("lives") or []):
                    if not isinstance(lv, dict):
                        continue
                    lu = _abs(lv.get("url"), u)
                    if not lu or lu in seen_live:
                        continue
                    seen_live.add(lu)
                    lives.append({"name": lv.get("name") or lu, "url": lu,
                                  "ua": lv.get("ua") or "", "epg": lv.get("epg") or ""})
            log("  %-52s jar 站点 +%d" % (u[:52], got))

    doc = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "configs": len(urls),
        "sites": sites,
        "parses": parses,
        "flags": flags,
        "lives": lives,
        "jars": [{"url": k, "sites": v} for k, v in
                 sorted(jars.items(), key=lambda kv: -kv[1])],
    }
    return doc


def merge_into(path, alive, log=print):
    doc = load_sources(path)
    have_api = {A.norm(s["api"]) for s in doc["sources"] if s.get("api")}
    have_host = {(urlsplit(A.norm(s["api"])).hostname or "").lower()
                 for s in doc["sources"] if s.get("api")}
    added = []
    for c in alive:
        h = (urlsplit(c["api"]).hostname or "").lower()
        if c["api"] in have_api or h in have_host:
            continue
        doc["sources"].append({"name": c["name"], "api": c["api"],
                               "from": c.get("from") or "扫源"})
        have_api.add(c["api"])
        have_host.add(h)
        added.append(c)
    if added:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    return added, len(doc["sources"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", action="append", default=[],
                    help="配置地址，可多次给")
    ap.add_argument("--from-file", action="append", default=[],
                    help="源库文件（configs.json），默认读本目录的 configs.json")
    ap.add_argument("--sources", default=os.path.join(HERE, "sources.json"))
    ap.add_argument("--out", default=os.path.join(HERE, "candidates.json"))
    ap.add_argument("--check", action="store_true", help="逐个探活（慢，但只留能用的）")
    ap.add_argument("--merge", action="store_true", help="把探活的源并入 sources.json")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="最多探多少个")
    ap.add_argument("--only-new", action="store_true",
                    help="只探 sources.json 里还没有的（跳过已收录）")
    ap.add_argument("--collect-jars", action="store_true",
                    help="只收集 jar 站点 / parses / flags / lives 到 jars.json（不探活）")
    ap.add_argument("--jars-out", default=os.path.join(HERE, "jars.json"))
    a = ap.parse_args()

    t0 = time.time()
    urls = list(a.src)
    for f in (a.from_file or [os.path.join(HERE, "configs.json")]):
        if os.path.exists(f):
            got = load_registry(f)
            urls.extend(got)
            print("源库 %s：%d 条配置" % (os.path.basename(f), len(got)))
    urls = list(dict.fromkeys(urls))
    if not urls:
        print("没有可扫的配置（--from 或 --from-file）")
        return 1

    if a.collect_jars:
        print("== 收集 jar 站点（%d 个配置，并发 %d）" % (len(urls), a.workers))
        doc = collect_profiles(urls, workers=a.workers)
        with open(a.jars_out, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=1)
        import collections
        kinds = collections.Counter(x["kind"] for x in doc["sites"])
        print("\n扩展站点 %d 个（%s）/ 不同资源 %d 个 / parses %d / flags %d / lives %d"
              % (len(doc["sites"]), dict(kinds), len(doc["jars"]),
                 len(doc["parses"]), len(doc["flags"]), len(doc["lives"])))
        print("已写入 %s（%.1fs）" % (a.jars_out, time.time() - t0))
        return 0

    print("== 扫 %d 个配置（并发 %d）" % (len(urls), a.workers))
    all_c = scan_many(urls, workers=a.workers)
    print("\n合计去重后 %d 个候选（%.1fs）" % (len(all_c), time.time() - t0))

    if a.only_new:
        have = {(urlsplit(A.norm(s["api"])).hostname or "").lower()
                for s in load_sources(a.sources).get("sources", []) if s.get("api")}
        before = len(all_c)
        all_c = [c for c in all_c
                 if (urlsplit(c["api"]).hostname or "").lower() not in have]
        print("其中已收录 %d 个，剩 %d 个待探" % (before - len(all_c), len(all_c)))

    if a.limit:
        all_c = all_c[:a.limit]

    result = {"generated": time.strftime("%Y-%m-%d %H:%M:%S"),
              "sources_scanned": a.src, "candidates": all_c, "alive": []}
    if a.check and all_c:
        print("开始探活（并发 %d，只留真能拉到可播数据的）…" % a.workers)
        rows = [None] * len(all_c)

        def job(i):
            c = all_c[i]
            r = A.probe({"name": c["name"], "api": c["api"], "from": c["from"]},
                        timeout=20, retries=1, with_classes=False)
            rows[i] = r
            mark = "UP  " if r["alive"] else "DOWN"
            print("  [%s] %-46s %s" % (mark, c["api"][:46], r["msg"]))
            return r

        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            list(ex.map(job, range(len(all_c))))
        alive, dead = [], []
        for c, r in zip(all_c, rows):
            if r and r["alive"]:
                alive.append({**c, "type": r["type"], "sample": r.get("sample"),
                              "total": r.get("total")})
            else:
                dead.append({**c, "msg": (r or {}).get("msg")})
        result["alive"] = alive
        result["dead"] = dead
        print("\n探活结果：%d 活 / %d 死" % (len(alive), len(dead)))

    if not a.dry_run:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=1)
        print("候选已写入 %s" % a.out)

    if a.merge:
        if not result["alive"]:
            print("没有探活的源，跳过合并（要合并请加 --check）")
        else:
            added, total = merge_into(a.sources, result["alive"])
            print("并入 %d 个新源，sources.json 现在共 %d 条" % (len(added), total))
            for c in added:
                print("  + %-14s %s" % (c["name"][:14], c["api"]))
    print("耗时 %.1fs" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
