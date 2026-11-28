#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""TVBox 聚合器 —— 常驻服务

端点
  GET  /tvbox           TVBox 单仓 JSON（默认档，客户端填这个）
  GET  /tvbox-all       全能档（含 js）
  GET  /libretv         LibreTV-SourceList v2（纯 CMS）
  GET  /cctv.m3u        直播主（央视+卫视）
  GET  /other.m3u       直播副（央视+卫视+港澳台+地方台）
  GET  /report.md       体检报告（人看）
  GET  /report.json     体检报告（机读）
  GET  /status          状态面板（HTML），带手动刷新按钮
  GET  /refresh?token=  触发一次重新聚合（异步，立刻返回）
  GET  /healthz         健康检查

特性
  * 启动先读磁盘缓存，客户端零等待；随后后台自动聚合一次
  * 后台按间隔定时聚合（默认 6 小时），失败 5 分钟后重试
  * 聚合过程是串行单飞：正在跑时再触发会直接返回「已在运行」
  * 配置类响应一律 no-store，客户端每次拿到的是最新一次结果

用法
  python server.py                          # 0.0.0.0:5680，间隔 6h
  python server.py --bind 192.0.2.10       # 只监听局域网地址
  python server.py --interval 2 --token xx  # 2 小时一次，刷新要 token
  环境变量：PORT / BIND / INTERVAL / TOKEN / BASE_URL / OUT_DIR / SEEDS
"""

import argparse
import collections
import hashlib
import json
import os
import re
import sys

import state as ST

# 版本号：发版时只改这里（面板、HTTP 头、更新检查都读它）
VERSION = "0.9.9"
REGISTRY = "ghcr.io"
IMAGE_REPO = "huueng/tvbox-agg"
import threading
import concurrent.futures as cf
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import aggregate as A
import alist as AL
import live as L

HERE = os.path.dirname(os.path.abspath(__file__))

CACHE = {
    "tvbox": {"name": "初始化中", "sites": []},
    "libretv": {"name": "初始化中", "version": 2, "sources": [], "liveSources": []},
    "report": {},
    "last_run": None,
    "last_error": None,
    "running": False,
}
_LOCK = threading.Lock()
_CMS_CACHE = {}          # (key, query) -> (expire_ts, ctype, body)
_CMS_TTL = 60
_CMS_TTL_LIST = 300      # 分类列表缓存 5 分钟：客户端开机就拉它，不能让它等上游
_CMS_MAX = 400
_PARAM_LEARNED = {}      # key -> "t" | "type_id"（运行时学到的方言）

CFG = {
    # 只有在反向代理后面才开启：TRUST_PROXY=1，之后才采信 X-Forwarded-For。
    # 默认关闭 —— 这个头谁都能写，无脑采信等于让人随便换 IP 绕过限流。
    "trust_proxy": bool(int(os.environ.get("TRUST_PROXY") or 0)),
    "seeds": os.path.join(HERE, "sources.json"),
    "out": os.path.join(HERE, "out"),
    "state": os.path.join(HERE, "state.json"),
    "workers": 3,
    "timeout": 20,
    "drop_after": 3,
    "interval": 6 * 3600,
    "token": "",
    "base_url": "",
    "proxy_base": "",      # 非空 → TVBox 输出走 /cms/<key> 透明代理
}


def guess_lan_ip():
    """猜本机在局域网的地址

    开 /cms 代理时要把站点地址写进配置，如果写成 127.0.0.1，
    电视拿到配置后就去连它自己了 —— 必须给一个电视能访问的地址。
    用 UDP 连一下公网地址，看内核选了哪张网卡（不会真发包）。
    """
    import socket
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("223.5.5.5", 80))
        ip = s.getsockname()[0]
        return "" if ip.startswith("127.") else ip
    except Exception:
        return ""
    finally:
        try:
            s.close()
        except Exception:
            pass


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------- 聚合调度

def load_cache():
    """启动时先把上次结果读进内存，客户端立刻可用"""
    for key, fn in (("tvbox", "tvbox.json"), ("report", "report.json"),
                    ("all", "all.json"), ("ltv", "ltv.json"),
                    ("direct", "tvbox-direct.json")):
        p = os.path.join(CFG["out"], fn)
        try:
            with open(p, encoding="utf-8") as f:
                CACHE[key] = json.load(f)
            CACHE["last_run"] = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(os.path.getmtime(p)))
        except Exception:
            pass


def run_once():
    """跑一次完整聚合；已有任务在跑时直接返回 False"""
    if LIVE_RUN["on"]:
        log("直播聚合进行中，这轮点播跳过")
        return False
    with _LOCK:
        if CACHE["running"]:
            return False
        CACHE["running"] = True
    try:
        try:
            # 每轮都重读规则 —— 改了 rules.json 不用重启
            A.apply_rules(A.load_rules(os.path.dirname(os.path.abspath(CFG["seeds"]))))
        except Exception:
            pass
        log("开始聚合…")
        res = A.run_pipeline(
            seeds_path=CFG["seeds"], out_dir=CFG["out"], workers=CFG["workers"],
            timeout=CFG["timeout"], state_path=CFG["state"],
            drop_after=CFG["drop_after"], recheck=True, log=log,
            proxy_base=CFG["proxy_base"] or None)
        rep = res["report"]
        CACHE.update(tvbox=res["tvbox"], libretv=res["libretv"],
                     ltv=res.get("ltv") or res["libretv"], report=rep,
                     full=res.get("full") or CACHE.get("full"),
                     all=res.get("all") or CACHE.get("all"),
                     direct=res.get("direct") or CACHE.get("direct"),
                     last_run=time.strftime("%Y-%m-%d %H:%M:%S"),
                     last_error=None)
        threading.Thread(target=warm_dialects, args=(res,), daemon=True).start()
        threading.Thread(target=lambda: JG.scan_all(
            os.path.dirname(os.path.abspath(CFG["seeds"])), log=log),
            daemon=True).start()
        threading.Thread(target=lambda: A.speed_test_assets(
            os.path.dirname(os.path.abspath(CFG["seeds"])), log=log),
            daemon=True).start()
        log("聚合完成：在线 %d / 暂留 %d / 摘除 %d；TVBox %d 站，LibreTV %d 源（%.1fs）" %
            (rep["alive"], rep["grace"], rep["dead"], rep["tvbox_sites"],
             rep["libretv_sources"], rep["elapsed_sec"]))
        return True
    except Exception as e:
        CACHE["last_error"] = "%s: %s" % (type(e).__name__, e)
        log("聚合失败：%s" % CACHE["last_error"])
        # 光有异常名没法定位，把栈也打出来
        try:
            import traceback
            log(traceback.format_exc())
        except Exception:
            pass
        return False
    finally:
        CACHE["running"] = False


def run_async(delay=0):
    def _job():
        if delay:
            time.sleep(delay)
        run_once()
    t = threading.Thread(target=_job, name="aggregate", daemon=True)
    t.start()
    return t


def _next_run_ts(stg, last_ts):
    """下一次该什么时候跑

    两种模式：daily = 每天固定点（如 03:30）；hours = 每 N 小时。
    设置改了下一次循环就生效，不用重启。
    """
    now = time.time()
    mode = (stg.get("interval_mode") or "hours").lower()
    if mode == "daily":
        raw = str(stg.get("daily_at") or "03:30")
        try:
            hh, mm = [int(x) for x in raw.split(":")[:2]]
        except Exception:
            hh, mm = 3, 30
        lt = time.localtime(now)
        target = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, hh, mm, 0, 0, 0, -1))
        if target <= now:
            target += 86400
        return target
    raw = stg.get("interval_hours")
    hours = float(6 if raw in (None, "") else raw)
    if hours <= 0:
        return 0                       # 0 = 不自动跑
    return max(last_ts + hours * 3600, now)


def scheduler():
    """每 30 秒看一眼：到点就聚合，设置改了立刻按新设置算下一次"""
    settings_path = os.path.join(os.path.dirname(os.path.abspath(CFG["seeds"])),
                                 "settings.json")
    last = time.time()
    nxt = _next_run_ts(A.load_settings(settings_path), last)
    while True:
        time.sleep(30)
        try:
            stg = A.load_settings(settings_path)
        except Exception:
            stg = {}
        if not nxt:
            nxt = _next_run_ts(stg, last)
            if not nxt:
                continue               # 关掉了自动聚合
        if time.time() >= nxt:
            mode = (stg.get("interval_mode") or "hours").lower()
            log("定时聚合触发（%s）" % ("每天 " + str(stg.get("daily_at") or "03:30")
                                    if mode == "daily"
                                    else "每 %s 小时" % (stg.get("interval_hours") or 6)))
            ok = run_once()
            last = time.time()
            nxt = _next_run_ts(stg, last)
            if not ok:
                log("本轮失败，5 分钟后重试")
                time.sleep(300)
                run_once()
                last = time.time()
                nxt = _next_run_ts(stg, last)
        else:
            # 设置被改过就重算，不用等到下一次
            nxt2 = _next_run_ts(stg, last)
            if nxt2 and abs(nxt2 - nxt) > 60:
                nxt = nxt2


# ---------------------------------------------------------------- 访问统计
#
# 一个线路「被拉了多少次」是判断它该不该换地址的唯一硬指标：
# 次数高说明客户端真的在用它，扛不住就该换；次数长期为 0 说明客户端根本没选它。
# 统计口径：/cms/<key> 的每次请求记到这个站点名下；配置文件按 profile 单独记。




CMS_LOG = collections.deque(maxlen=300)
CFG_SYNC_LOCK = threading.Lock()     # 同一时间只允许一条线路在解析
# 直播索引的实现搬到了 live_api.py
import jargate as JG
import live_api as LA                   # noqa: E402


def _data_dir0():
    return os.path.dirname(os.path.abspath(CFG["seeds"]))


def live_doc(force=False, log=print):
    d = _data_dir0()
    stg = A.load_settings(os.path.join(d, "settings.json"))
    return LA.doc(d, force=force, log=log, settings=stg)


def _live_bg():
    d = _data_dir0()
    stg = A.load_settings(os.path.join(d, "settings.json"))
    LA.bg(d, settings=stg)


def warm_dialects(res, log=None):
    """聚合完就把各站分类列表拉一遍塞进缓存

    客户端开机第一件事就是拉 ac=list，而我们这层代理要等上游（个别站 30 秒以上），
    客户端超时就表现为「站点没分类、影视墙空白」。预热后客户端拿到的是命中缓存，
    几十毫秒就返回。
    """
    sites = (res.get("tvbox") or {}).get("sites") or []
    def one(item):
        key = item.get("key")
        if not key:
            return
        try:
            cms_fetch(key, [("ac", "list"), ("pg", "1")])
        except Exception:
            pass
    try:
        with cf.ThreadPoolExecutor(max_workers=3) as ex:
            list(ex.map(one, sites))
    except Exception:
        pass


# 访问统计的实现搬到了 access.py，这里保留同名别名，调用点不用改
import access as AC                     # noqa: E402

access_hit = AC.hit
access_summary = AC.summary
access_today = AC.today
access_load = AC.load
access_flush = AC.flush


import hashlib

AUTH_PATH = None


def _h(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def auth_load():
    try:
        with open(AUTH_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ---------------------------------------------------------------- 检查更新
#
# 只"查"，不自动升级：镜像不可变 + 手动 pull 才能回滚，这是故意的。
# 查询顺序：官方 GHCR → 设置里的镜像/加速地址（国内经常只有镜像站通）。

_UPDATE_CACHE = {"ts": 0.0, "data": None}


def _semver(tag):
    m = re.match(r"^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?$", str(tag or "").strip())
    return tuple(int(x or 0) for x in m.groups()) if m else None


def _registry_tags(host, repo, timeout=6):
    """读镜像仓库的 tag 列表；拿不到返回 None（不通 / 仓库私有 / 格式不对）"""
    base = "https://" + host
    repo = (repo or IMAGE_REPO).strip("/")
    for with_token in (False, True):
        hdr = {}
        if with_token:
            try:
                tk_url = "%s/token?scope=repository:%s:pull&service=%s" % (
                    base, repo, host)
                tk = json.loads(urllib.request.urlopen(tk_url, timeout=timeout).read())
                if not tk.get("token"):
                    continue
                hdr["Authorization"] = "Bearer " + tk["token"]
            except Exception:
                continue
        try:
            req = urllib.request.Request(base + "/v2/%s/tags/list" % repo,
                                         headers=hdr)
            data = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
            tags = [t for t in (data.get("tags") or []) if _semver(t)]
            if tags:
                return tags
        except Exception:
            continue
    return None


def check_update(mirror=""):
    """当前版本 vs 镜像仓库里的最新版本（结果缓存 10 分钟）"""
    now = time.time()
    if _UPDATE_CACHE["data"] and now - _UPDATE_CACHE["ts"] < 600:
        return _UPDATE_CACHE["data"]

    sources = [("官方 GHCR", REGISTRY, IMAGE_REPO)]
    raw = (mirror or "").strip()
    if raw:
        raw = raw.split("://", 1)[-1].rstrip("/")
        if "/" in raw:
            h, r = raw.split("/", 1)
            sources.append(("镜像地址", h, r))
        else:
            sources.append(("镜像地址", raw, IMAGE_REPO))

    tried = []
    for label, host, repo in sources:
        tags = _registry_tags(host, repo)
        tried.append("%s %s：%s" % (label, host, "通" if tags else "不通"))
        if not tags:
            continue
        best = max(tags, key=lambda t: _semver(t))
        cur = _semver(VERSION) or (0, 0, 0)
        out = {"ok": True, "current": VERSION, "latest": best.lstrip("v"),
               "newer": _semver(best) > cur, "source": label, "host": host,
               "tried": tried, "tags": sorted(set(tags), key=_semver)[-8:]}
        _UPDATE_CACHE.update(ts=now, data=out)
        return out

    out = {"ok": False, "current": VERSION, "tried": tried,
           "error": "镜像仓库都连不上，查不到最新版本",
           "hint": "网络原因可以用镜像加速：在设置里填「镜像/加速地址」，"
                   "例如 ghcr.nju.edu.cn/huueng/tvbox-agg"}
    _UPDATE_CACHE.update(ts=now, data=out)
    return out


def auth_password_ok(pw):
    """校验密码：优先用 data/auth.json 里改过的，否则用环境变量 TOKEN"""
    doc = auth_load()
    if doc.get("hash"):
        return _h(doc.get("salt", "") + pw) == doc["hash"]
    return bool(CFG["token"]) and pw == CFG["token"]


def auth_ready():
    """有没有设置过密码（没设置就完全不拦，方便内网裸跑）"""
    doc = auth_load()
    return bool(doc.get("hash") or CFG["token"])


def auth_set_password(pw):
    salt = os.urandom(8).hex()
    doc = {"salt": salt, "hash": _h(salt + pw),
           "changed": time.strftime("%Y-%m-%d %H:%M:%S")}
    ST.save_file(AUTH_PATH, doc)
    return doc


def auth_cookie_value():
    """当前密码对应的 Cookie 值；改密码后自动变"""
    doc = auth_load()
    base = doc.get("hash") or _h(CFG["token"] or "")
    return _h(base + "cookie")[:32]


def auth_login_html(err="", prefix=""):
    return """<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>登录 · TVBox 聚合器</title><style>
body{font:14px/1.6 system-ui,"Microsoft YaHei",sans-serif;background:#f6f7f9;
margin:0;display:flex;align-items:center;justify-content:center;height:100vh}
.box{background:#fff;border:1px solid #e4e6eb;border-radius:12px;padding:28px 26px;
width:320px;box-shadow:0 2px 12px rgba(0,0,0,.06)}
h1{font-size:17px;margin:0 0 4px;text-align:center}
p{color:#656d76;font-size:12px;text-align:center;margin:0 0 18px}
input{width:100%%;box-sizing:border-box;font:inherit;padding:9px 11px;border:1px solid #d0d7de;
border-radius:8px;margin-bottom:12px}
button{width:100%%;font:inherit;padding:9px;border:0;border-radius:8px;background:#1f883d;
color:#fff;cursor:pointer}button:hover{background:#1a7f37}
.err{color:#cf222e;font-size:12px;text-align:center;margin-bottom:10px}
</style></head><body><form class="box" method="post" action="login">
<h1>TVBox 聚合器</h1><p>请输入管理密码</p>
%s<input type="password" name="pw" placeholder="管理密码" autofocus autocomplete="current-password">
<button type="submit">登 录</button>%s</form></body></html>""" % (
        ('<div class="err">%s</div>' % err) if err else "",
        # 从没在面板改过密码时，密码就是 docker-compose 里的 TOKEN，这里提示一下
        ('' if auth_load().get("hash") else
         '<p style="margin:12px 0 0;font-size:11px">还没设置过密码：当前使用 '
         '<code>docker-compose.yml</code> 里的 <code>TOKEN</code> 登录，'
         '登录后可在「设置」里改。</p>'))

# ---------------------------------------------------------------- CMS 代理

# ---------------------------------------------------------------- 留言板
#
# 匿名可看可发，但要有底线：
#   · 文本只存纯文本，前端 esc() 输出（不渲染 HTML）
#   · 单条 ≤500 字，昵称 ≤20 字，body 上限 8KB
#   · 同一 IP 30 秒 1 条、24 小时 30 条
#   · 总数上限 500，超了删最旧的非置顶
#   · 回复/置顶/删除需要管理员登录（走面板 Cookie）

MSG_MAX_ITEMS = 500
MSG_TEXT_MAX = 500
MSG_NICK_MAX = 20
MSG_RATE = {}          # ip -> [ts, ...]
MSG_LOCK = threading.Lock()   # 留言是"读整个文件 → 改 → 写回"，必须串行
MSG_GAP = 30           # 秒
MSG_DAY_MAX = 30


def _msg_path(data_dir):
    return os.path.join(data_dir, "messages.json")


def msg_load(data_dir):
    try:
        with open(_msg_path(data_dir), encoding="utf-8") as f:
            return json.load(f).get("items") or []
    except Exception:
        return []


def msg_save(data_dir, items):
    items = sorted(items, key=lambda x: (0 if x.get("pinned") else 1,
                                         -(x.get("ts") or 0)))
    ST.save_file(_msg_path(data_dir), {"items": items[:MSG_MAX_ITEMS]})


def _msg_clean(t, limit):
    """去掉控制字符、压缩空行，按字数截断"""
    t = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", str(t or ""))
    t = t.replace("\r\n", "\n").strip()
    return t[:limit]


def msg_rate_ok(ip):
    now = time.time()
    if len(MSG_RATE) > 4000:                  # 被刷时不至于把内存撑爆
        for k in [k for k, v in MSG_RATE.items() if not v or now - v[-1] > 86400]:
            MSG_RATE.pop(k, None)
    hist = [t for t in MSG_RATE.get(ip, []) if now - t < 86400]
    if hist and now - hist[-1] < MSG_GAP:
        return False, "发太快了，%d 秒后再试" % int(MSG_GAP - (now - hist[-1]))
    if len(hist) >= MSG_DAY_MAX:
        return False, "今天发得够多了，明天再来"
    hist.append(now)
    MSG_RATE[ip] = hist
    return True, ""


# 直播聚合的互斥：同一时间只允许一个直播任务
# （小聚合跑着又点大聚合，两个一起测会把带宽打满、结果也互相污染）
LIVE_LOCK = threading.Lock()
LIVE_RUN = {"on": False, "mode": "", "since": 0.0, "note": ""}


def live_busy():
    """直播是否在跑；顺便返回描述"""
    return LIVE_RUN["on"], LIVE_RUN["mode"]


def _live_state(data_dir):
    p = os.path.join(data_dir, "live_state.json")
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _live_tick(data_dir, force_big=False, force_small=False, force_screen=False,
               log=print):
    """直播的大聚合 / 小聚合 / 备选初筛

    大聚合：重新展开优选源的 TXT/M3U（频道增减），3 天一次
    小聚合：用上次的频道清单只测线路，1 天一次
    备选初筛：只解析频道数不测活，2 天一次，决定要不要顶进优选
    """
    import live as L
    stg = A.load_settings(os.path.join(data_dir, "settings.json"))
    n = int(stg.get("live_active_n") or 10)
    auto = bool(int(stg.get("live_auto_promote") or 0))
    big_d = int(stg.get("live_big_days") or 3)
    small_d = int(stg.get("live_small_days") or 1)
    bench_d = int(stg.get("live_bench_days") or 2)
    now = time.time()
    st = _live_state(data_dir)

    doc = L.load_lives(data_dir)
    items = doc.get("items") or []
    if not items:
        return
    L.apply_tiers(items, n, auto)
    L.save_lives(data_dir, doc)

    def due(k, days):
        return now - float(st.get(k) or 0) >= days * 86400

    # 墓碑复检：到期的试一次，通了就回备选
    try:
        revive = L.tomb_check(data_dir, log=log,
                              days=int(stg.get("live_tomb_days") or 15),
                              max_fail=int(stg.get("live_tomb_max_fail") or 5))
        for u, nm in revive or []:
            L.revive_source(data_dir, u, nm)
        if revive:
            st["last_tomb"] = now
    except Exception as e:                        # noqa: BLE001
        log("  墓碑复检失败：%s" % e)
    # 新进来的源（复活 / 删除后重新加）各解析一次
    try:
        if L.parse_pending(data_dir, log=log):
            doc_r = L.load_lives(data_dir)
            L.apply_tiers(doc_r.get("items") or [], n, auto)
            L.save_lives(data_dir, doc_r)
    except Exception as e:                        # noqa: BLE001
        log("  待解析源处理失败：%s" % e)

    did = []
    if force_big or due("last_big", big_d):
        doc2 = L.load_lives(data_dir)
        L.big_refresh(data_dir, doc2.get("items") or [], active_n=n, log=log,
                      max_test=int(stg.get("live_max_test") or 1500))
        st["last_big"] = st["last_small"] = now
        did.append("大聚合")
    elif force_small or due("last_small", small_d):
        L.small_test(data_dir, log=log,
                     max_test=int(stg.get("live_max_test") or 1500))
        st["last_small"] = now
        did.append("小聚合")

    if force_screen or due("last_bench", bench_d):
        doc3 = L.load_lives(data_dir)
        L.screen_bench(data_dir, doc3.get("items") or [], log=log)
        doc3 = L.load_lives(data_dir)
        L.apply_tiers(doc3.get("items") or [], n, auto)
        L.save_lives(data_dir, doc3)
        st["last_bench"] = now
        did.append("备选初筛")

    st["checked"] = now
    ST.save_file(os.path.join(data_dir, "live_state.json"), st)
    if did:
        log("  直播调度：%s（优选 %d 个）" % ("+".join(did), n))


def _mirror_list():
    try:
        import discover as D
        return list(D.MIRRORS)
    except Exception:
        return []


def site_table():
    """key -> (上游 api, 分类参数名)"""
    t = {}
    for s in (CACHE.get("report") or {}).get("sources", []):
        k = s.get("key")
        if k and s.get("api"):
            t[k] = (s["api"], s.get("param"))
    return t


def _cms_count(body):
    """数一下响应里有几条内容（JSON 看 list，XML 看 <video>）"""
    try:
        head = body.lstrip()[:1]
        if head == b"<":
            return body.count(b"<video>")
        d = A.loads_loose(body.decode("utf-8", "replace"))
        return len((d or {}).get("list") or []) if isinstance(d, dict) else 0
    except Exception:
        return 0


def cms_fetch(key, query_pairs):
    """透明代理：把客户端发来的参数翻成上游认的名字，再原样返回

    关键点：采集站多数不认 t=，收到就返回空列表 → TVBox 点分类空白。
    1) 已知方言（体检时探到的）直接用
    2) 未知方言：先按 t 发，空了就换 type_id 再发一次，谁有数据记住谁
    这样即使体检时 ac=list 超时漏探，第一次点分类也能自动纠正。
    """
    info = site_table().get(key)
    if not info:
        return None
    api, param = info
    param = param or _PARAM_LEARNED.get(key)

    def build(p):
        pairs = []
        for k, v in query_pairs:
            if k == "t" and p == "type_id":
                k = "type_id"
            pairs.append((k, v))
        return pairs

    def call(pairs):
        url = api + ("&" if "?" in api else "?") + urllib.parse.urlencode(pairs)
        try:
            return A.fetch(url, timeout=30, done=None, retries=1)
        except Exception:
            return None

    pairs = build(param)
    cache_key = (key, tuple(sorted(pairs)))
    now = time.time()
    hit = _CMS_CACHE.get(cache_key)
    if hit and hit[0] > now:
        return hit[1], hit[2], True

    body = call(pairs)
    if body is not None and not _cms_count(body) and any(k == "t" or k == "type_id"
                                                         for k, _ in pairs):
        alt = "type_id" if param != "type_id" else "t"
        body2 = call(build(alt))
        if body2 is not None and _cms_count(body2) > 0:
            body = body2
            _PARAM_LEARNED[key] = alt
            log("  学到方言：%s 认 %s=" % (key, alt))
    if body is None:
        return None, "抓取失败", False

    head = body.lstrip()[:1]
    ctype = ("application/xml; charset=utf-8" if head == b"<"
             else "application/json; charset=utf-8")
    if len(_CMS_CACHE) > _CMS_MAX:
        _CMS_CACHE.clear()
    ttl = _CMS_TTL_LIST if any(k == "ac" and v == "list" for k, v in pairs) \
        else _CMS_TTL
    _CMS_CACHE[cache_key] = (now + ttl, ctype, body)
    return ctype, body, False


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    # 慢连接保护：客户端一直不发数据/不读响应时，别把线程永远占着
    timeout = 30
    server_version = "TVBoxAgg/" + VERSION
    protocol_version = "HTTP/1.1"

    # ---- 工具
    def _send(self, code, body, ctype="application/json; charset=utf-8",
              extra=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False, indent=1)
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, must-revalidate")
        self.send_header("Access-Control-Allow-Origin", "*")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # 浏览器刷新 / 电视切换频道时会直接断开，这不是错误，
                # 不吞掉会在日志里刷一屏 traceback
                return

    def _cookie_ok(self):
        raw = self.headers.get("Cookie") or ""
        want = auth_cookie_value()
        for part in raw.split(";"):
            k, _, v = part.strip().partition("=")
            if k == "agg_auth" and v and v == want:
                return True
        return False

    def _auth_ok(self, q, allow_query=False):
        """通过条件：已登录 Cookie；或者（仅 /refresh）带正确 ?token=

        ?token= 只留给 cron 这类机器调用，不能拿来读管理接口——
        否则改了密码，compose 里那串旧 TOKEN 依然是个后门。
        """
        if not auth_ready():
            return True
        if self._cookie_ok():
            return True
        if allow_query and q.get("token", [""])[0] and q["token"][0] == CFG["token"]:
            return True
        return self.headers.get("Authorization") == "Bearer " + CFG["token"]

    def _devtest(self, kind, idx=1):
        """只放一个指定类型的站点，用来验证某个客户端到底支不支持这类源

        kind=jar / js / py / cms。客户端拿这个配置时只会看到那一个站点，
        能不能出内容一目了然。
        """
        docs = [CACHE.get("full") or {}, CACHE.get("all") or {}, CACHE.get("tvbox") or {}]
        found = []
        for doc in docs:
            for s0 in doc.get("sites") or []:
                if s0.get("type") != 3:
                    continue
                api = str(s0.get("api") or "")
                has_jar = bool(s0.get("jar"))
                ok = ((kind == "jar" and has_jar) or
                      (kind == "js" and not has_jar and api.endswith(".js")) or
                      (kind == "py" and not has_jar and api.endswith(".py")))
                if ok and s0 not in found:
                    found.append(s0)
        pick = found[max(0, min(idx - 1, len(found) - 1))] if found else None
        if kind == "cms" or not pick:
            for s0 in (CACHE.get("tvbox") or {}).get("sites") or []:
                if s0.get("type") in (0, 1):
                    pick = dict(s0)
                    break
            if kind != "cms" and pick:
                pick["name"] = "（没找到 %s 类型，退回 CMS）%s" % (kind, pick.get("name"))
        if not pick:
            return {"name": "空", "sites": [], "lives": [], "parses": [], "flags": []}
        out = {"name": "devtest-%s" % kind, "sites": [dict(pick)],
               "lives": [], "parses": [], "flags": []}
        return out

    def _client_ip(self):
        """真实客户端 IP

        默认只用 TCP 对端地址：X-Forwarded-For 是谁都能写的，
        无条件采信等于让人随便换 IP 绕过留言限流。
        放在反向代理后面时，用环境变量 TRUST_PROXY=1 显式开启。
        """
        try:
            peer = self.client_address[0]
        except Exception:
            peer = ""
        if CFG.get("trust_proxy"):
            fwd = (self.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
            if fwd:
                return fwd
            real = (self.headers.get("X-Real-IP") or "").strip()
            if real:
                return real
        return peer

    def _req_base(self):
        """这次请求是从哪个地址打进来的

        用反向代理（比如 https://tvbox.xxx.io:812/）访问时，配置里的站点 api
        必须跟着变成同样的 https 地址，否则 Android 会按「明文流量」直接拦掉：
        表现就是能加载站点列表，但每个站点没有分类、没有影视墙、搜索无结果。
        """
        proto = (self.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
        host = (self.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
        if not host:
            host = self.headers.get("Host") or ""
        if not proto:
            proto = "https" if (self.headers.get("X-Forwarded-Ssl") or
                                self.headers.get("X-Forwarded-Scheme") == "https") else ""
        if not proto:
            # 代理没透传协议时靠 Host 猜：域名走 https，内网 IP 走 http。
            # 猜错会有代价（https 站点里请求 http 会被 Android 拦），所以宁可保守。
            hostname = host.split(":")[0]
            is_ip = bool(hostname) and hostname.replace(".", "").isdigit()
            proto = "http" if (is_ip or hostname.endswith(".local")) else "https"
        return ("%s://%s" % (proto, host)).rstrip("/") if host else ""

    def _cfg_text(self, key):
        """配置里的站点地址跟随访问地址改写（只改 <base>/cms/ 前缀）"""
        doc = CACHE.get(key) or {}
        text = json.dumps(doc, ensure_ascii=False, indent=1)
        cfg_base = (CFG["proxy_base"] or CFG["base_url"] or "").rstrip("/")
        req_base = self._req_base()
        if cfg_base and req_base and req_base != cfg_base:
            text = text.replace(cfg_base + "/cms/", req_base + "/cms/")
        return text

    def _base(self):
        if CFG["base_url"]:
            return CFG["base_url"].rstrip("/")
        host = self.headers.get("Host") or ("%s:%s" % self.server.server_address)
        return "http://" + host

    def _loc(self, target):
        """跳转用相对地址

        反代可能是「前端 / → 后端 /status」这种（浏览器看不到 /status），
        也可能是「保留 /status」。发绝对地址 `Location: /status` 在第一种下会变成
        /status/status。相对地址由浏览器按它自己看到的 URL 解析，两种都对。
        """
        if target in ("/login", "/logout"):
            return "login" if target == "/login" else "login"
        if target == "/status":
            return "./"
        return target

    # ---- 路由
    # 反向代理如果配了「保留 /status 前缀」，我们收到的就是 /status/login 这种。
    # 这里把前缀剥掉再路由，跳转时再补回去 —— 否则面板在反代下面完全点不动。
    def _split_path(self):
        u = urllib.parse.urlsplit(self.path)
        raw = (u.path or "/")
        prefix = ""
        for pre in ("/status", "/tvbox"):
            # 只有「前缀 + 后面还有内容」才剥；单独的 /status 就是面板本身
            if raw.startswith(pre + "/"):
                prefix = pre
                rest = raw[len(pre):].rstrip("/")
                # /status/（反代把前端根拼成 base + "/"）也要当面板，不能归一成 "/"
                raw = rest or pre
                break
        return (raw.rstrip("/") or "/"), urllib.parse.parse_qs(u.query), prefix

    def do_GET(self):
        path, q, prefix = self._split_path()
        self._prefix = prefix

        # 浏览器打开根路径是想看面板；TVBox 客户端（okhttp）要的是配置本身。
        # 按 Accept 头区分，两边都不打扰。
        if path == "/" and "text/html" in (self.headers.get("Accept") or "") \
                and "okhttp" not in (self.headers.get("User-Agent") or "").lower():
            # 注意这里不能用 _loc("/status")：它返回 "./"，
            # 从根路径跳过去会解析回 "/"，又回到配置 JSON。
            return self._send(302, "", "text/html; charset=utf-8",
                              {"Location": (self._prefix or "") + "/status"})
        # 留言板是公开页面：不用登录就能看和发（放这里是为了绕过下面的鉴权）
        if path == "/board":
            try:
                with open(os.path.join(HERE, "board.html"), encoding="utf-8") as bf:
                    _html = bf.read()
            except Exception as e:
                return self._send(500, {"error": "留言板页面读取失败：%s" % e})
            return self._send(200, _html, "text/html; charset=utf-8")

        # === 输出地址（没有别名，没有重定向，写错就是 404）===
        #   /tvbox        默认档（CMS + jar + py）
        #   /tvbox-all    全能档（CMS + jar + js + py）
        #   /libretv      LibreTV 源（纯 CMS 列表）
        #   /cctv.m3u     直播主（央视 + 卫视）
        #   /other.m3u    直播副（央视 + 卫视 + 港澳台 + 地方台）
        _CFG = {"/tvbox": "tvbox", "/tvbox-all": "all"}
        if path in _CFG:
            key = _CFG[path]
            if not CACHE.get(key):
                return self._send(404, {"error": "%s 档还没生成" % key,
                                        "hint": "先跑一轮点播聚合"})
            _txt = self._cfg_text(key)
            access_hit("cfg:" + key, len(_txt.encode("utf-8")),
                       self.headers.get("User-Agent", "")[:36], self._client_ip())
            return self._send(200, _txt)
        if path == "/libretv":
            access_hit("cfg:ltv", 0, self.headers.get("User-Agent", "")[:36],
                       self._client_ip())
            return self._send(200, CACHE.get("ltv") or CACHE.get("libretv") or {})
        if path in ("/export.txt", "/export"):
            if not self._auth_ok(q):
                return self._send(401, {"error": "需要登录"},
                                  extra={"Location": self._loc("/login")})
            access_hit("export", 0, self.headers.get("User-Agent", "")[:36],
                       self._client_ip())
            return self._send(200, self._export_txt(), "text/plain; charset=utf-8",
                              {"Content-Disposition":
                               'attachment; filename="%s"' % self.EXPORT_NAME})
        # 报告里有每个站的评分、被移除原因，属于"内部判断"，不公开
        if path in ("/report.json", "/report.md", "/devtest") \
                and not self._auth_ok(q):
            return self._send(401, {"error": "需要登录", "login": "/login"})
        if path == "/report.json":
            return self._send(200, CACHE["report"])
        if path == "/report.md":
            p = os.path.join(CFG["out"], "report.md")
            try:
                with open(p, encoding="utf-8") as f:
                    return self._send(200, f.read(),
                                      "text/markdown; charset=utf-8")
            except Exception:
                return self._send(404, {"error": "还没有报告"})
        if path == "/login":
            if self._auth_ok(q):
                return self._send(302, "", "text/html; charset=utf-8",
                                  {"Location": self._loc("/status")})
            return self._send(200, auth_login_html(prefix=self._prefix),
                              "text/html; charset=utf-8")
        if path == "/logout":
            return self._send(302, "", "text/html; charset=utf-8",
                              {"Location": self._loc("/login"),
                               "Set-Cookie": "agg_auth=; Path=/; Max-Age=0"})
        # 留言板是公开页面：列表谁都能看（admin 字段会告诉他们能不能管理）
        _public = path in ("/api/msg/list",)
        if (path == "/status" or path.startswith("/api/")) and not _public \
                and not self._auth_ok(q):
            if path.startswith("/api/"):
                return self._send(401, {"error": "需要登录", "login": "/login"})
            return self._send(302, "", "text/html; charset=utf-8",
                              {"Location": self._loc("/login")})
        if path in ("/cctv.m3u", "/other.m3u"):
            try:
                doc = live_doc(log=lambda *a: None)
            except Exception:
                doc = LA.CACHE.get("doc") or {}
            if not doc:
                return self._send(503, {"error": "直播测速正在后台进行，稍后再试"})
            stg = A.load_settings(os.path.join(
                os.path.dirname(os.path.abspath(CFG["seeds"])), "settings.json"))
            per = int(stg.get("live_lines_per") or 5)
            drop_bad = bool(int(stg.get("live_drop_seg_bad", 1)))
            if path == "/cctv.m3u":
                access_hit("live:main", 0, self.headers.get("User-Agent", "")[:36],
                           self._client_ip())
                return self._send(200, L.to_m3u(doc, "main", per, drop_bad),
                                  "audio/x-mpegurl; charset=utf-8")
            # 副接口 = 全量（央视+卫视+港澳台+地方台），地方台每台 2 条
            if path == "/other.m3u":
                access_hit("live:local", 0, self.headers.get("User-Agent", "")[:36],
                           self._client_ip())
                return self._send(200, L.to_m3u(doc, None, per, drop_bad,
                                               local_lines=2),
                                  "audio/x-mpegurl; charset=utf-8")
            return self._send(404, {"error": "没有这个地址",
                                    "hint": "直播只有 /cctv.m3u 和 /other.m3u"})
        if path == "/api/update/check":
            if not self._auth_ok(q):
                return self._send(401, {"error": "需要登录"})
            try:
                with open(os.path.join(self._data_dir(), "settings.json"),
                          encoding="utf-8") as f:
                    _s = json.load(f)
            except Exception:
                _s = {}
            return self._send(200, check_update(str(_s.get("update_mirror") or "")))
        if path == "/api/alist":          # 读网盘配置（含令牌，必须登录）
            return self._send(200, AL.load_cfg(self._data_dir()))
        if path == "/api/msg/list":
            items = msg_load(self._data_dir())
            items = sorted(items, key=lambda x: (0 if x.get("pinned") else 1,
                                                 -(x.get("ts") or 0)))
            # 白名单字段：绝不能把 ip / ip_h 这类东西吐给公开接口
            _PUB = ("id", "ts", "nick", "text", "reply", "reply_ts", "pinned")
            items = [{k: it.get(k) for k in _PUB} for it in items]
            return self._send(200, {
                "items": items[:200],
                "admin": bool(self._auth_ok({})),
                "total": len(items)})
        if path == "/api/access":
            return self._send(200, access_summary())
        if path == "/api/overview":
            return self._send(200, self._overview())
        if path == "/devtest":
            kind = (q.get("kind") or ["jar"])[0]
            idx = int((q.get("n") or ["1"])[0])
            return self._send(200, self._devtest(kind, idx))
        if path == "/api/cmslog":
            return self._send(200, {"rows": list(CMS_LOG)})
        if path == "/api/history":
            hp = os.path.join(os.path.dirname(os.path.abspath(CFG["seeds"])),
                              "history.jsonl")
            out = []
            try:
                with open(hp, encoding="utf-8") as f:
                    for line in f.readlines()[-200:]:
                        try:
                            out.append(json.loads(line))
                        except Exception:
                            pass
            except Exception:
                pass
            return self._send(200, {"rows": out})
        if path == "/status":
            return self._send(200, self._status_html(), "text/html; charset=utf-8")
        if path == "/healthz":
            return self._send(200, {"ok": True, "running": CACHE["running"],
                                    "last_run": CACHE["last_run"]})
        if path == "/refresh":
            if not self._auth_ok(q, allow_query=True):
                return self._send(401, {"error": "token 不对"})
            if CACHE["running"]:
                return self._send(202, {"started": False, "reason": "点播聚合已在跑"})
            if LIVE_RUN["on"]:
                return self._send(202, {"started": False,
                                        "reason": "直播聚合进行中（%s），等它结束" %
                                                  (LIVE_RUN["mode"] or "未知")})
            run_async()
            return self._send(202, {"started": True})
        # 网盘（Alist）：伪装成苹果CMS接口，客户端当普通采集站用
        if path in ("/alist", "/alist/"):
            _pr = urllib.parse.parse_qs(
                urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
            try:
                return self._send(200, AL.cms(self._data_dir(), _pr))
            except Exception as e:                        # noqa: BLE001
                return self._send(500, {"class": [], "list": [],
                                        "msg": "网盘接口出错：%s" % e})
        if path.startswith("/cms/"):
            key = path[5:]
            pairs = urllib.parse.parse_qsl(
                urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
            res = cms_fetch(key, pairs)
            if res is None:
                return self._send(404, {"error": "未知站点 key", "key": key})
            ctype, payload, cached = res
            try:
                CMS_LOG.append({
                    "t": time.strftime("%H:%M:%S"), "key": key,
                    "q": urllib.parse.urlsplit(self.path).query[:80],
                    "cache": bool(cached),
                    "size": len(payload) if not isinstance(payload, str) else 0,
                    "ua": (self.headers.get("User-Agent") or "")[:28],
                    "err": payload if isinstance(payload, str) else ""})
            except Exception:
                pass
            if isinstance(payload, str):
                return self._send(502, {"error": payload, "key": key})
            return self._send(200, payload, ctype,
                              {"X-CMS-Cache": "hit" if cached else "miss"})
        return self._send(404, {"error": "not found", "path": path,
                                "hint": "/tvbox /tvbox-all /libretv /cctv.m3u /other.m3u /status /refresh /cms/<key>"})

    def do_HEAD(self):
        return self.do_GET()

    def do_POST(self):
        path, _q2, prefix = self._split_path()
        self._prefix = prefix
        u = urllib.parse.urlsplit(self.path)
        if not path.startswith("/api/") and path != "/login":
            return self._send(404, {"error": "only /api/* accepts POST"})
        q = urllib.parse.parse_qs(u.query)
        n = int(self.headers.get("Content-Length") or 0)
        if n > 2 * 1024 * 1024:      # 正常请求最大也就几十 KB，超了直接拒绝
            return self._send(413, {"error": "请求体太大"})
        raw = self.rfile.read(n) if n else b""

        if path == "/login":
            form = urllib.parse.parse_qs(raw.decode("utf-8", "replace"))
            pw = (form.get("pw") or [""])[0]
            if auth_password_ok(pw):
                access_hit("login", 0, self.headers.get("User-Agent", "")[:36],
                           self._client_ip())
                # 登录页在 /login，_loc("/status") 会算成 "./"（= 根路径），
                # 而根路径现在不再输出配置了，必须给绝对地址
                return self._send(302, "", "text/html; charset=utf-8",
                                  {"Location": (self._prefix or "") + "/status",
                                   "Set-Cookie": "agg_auth=%s; Path=/; HttpOnly; "
                                                 "SameSite=Lax; Max-Age=604800"
                                                 % auth_cookie_value()})
            return self._send(200, auth_login_html("密码不对", self._prefix),
                              "text/html; charset=utf-8")

        # 留言板：匿名可发，所以放在鉴权之前，但自带限流
        if path == "/api/msg/add":
            if len(raw) > 8192:
                return self._send(413, {"ok": False, "error": "内容太长了"})
            try:
                mb = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return self._send(400, {"ok": False, "error": "body 不是合法 JSON"})
            ip = self._client_ip()
            ok, why = msg_rate_ok(ip)
            if not ok:
                return self._send(429, {"ok": False, "error": why})
            text = _msg_clean(mb.get("text"), MSG_TEXT_MAX)
            if not text:
                return self._send(400, {"ok": False, "error": "写点什么再发"})
            nick = _msg_clean(mb.get("nick"), MSG_NICK_MAX) or "匿名"
            d = self._data_dir()
            with MSG_LOCK:
                items = msg_load(d)
                items.append({"id": int(time.time() * 1000), "ts": int(time.time()),
                              "nick": nick, "text": text,
                              # 只留短哈希：够定位刷屏来源，又不至于存访客 IP
                              "ip_h": hashlib.sha256(ip.encode()).hexdigest()[:12],
                              "reply": "", "reply_ts": 0, "pinned": 0})
                msg_save(d, items)
            return self._send(200, {"ok": True})

        if not self._auth_ok(q):
            return self._send(401, {"error": "需要登录", "login": "/login"})
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return self._send(400, {"error": "body 不是合法 JSON"})
        try:
            if path == "/api/source/add":
                return self._send(200, self._source_add(body))
            if path == "/api/source/remove":
                return self._send(200, self._source_remove(body))
            if path == "/api/override":
                return self._send(200, self._override(body))
            if path == "/api/config/scan-bench":
                def _scan_bench():
                    try:
                        d = self._data_dir()
                        _act, bench = A.load_config_urls(d)
                        log("开始解析 %d 个备选配置接口（只统计，不并池）" % len(bench))
                        for u in sorted(bench):
                            try:
                                A.sync_config_sources(d, CFG["seeds"], force=True,
                                                      only=u, log=print)
                            except Exception as e:
                                log("  %s 解析失败：%s" % (u[-40:], e))
                        log("备选配置接口解析完成")
                    except Exception as e:
                        log("备选解析失败：%s" % e)
                threading.Thread(target=_scan_bench, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path == "/api/purge/rebuild":
                def _rebuild():
                    try:
                        d = self._data_dir()
                        rm = A.purge_unselected(d)
                        log("清理未启用配置的站点：扩展 %d / 采集 %d" % (rm["jars"], rm["sources"]))
                        _act, _b = A.load_config_urls(d)
                        for u in sorted(_act):
                            try:
                                A.sync_config_sources(d, CFG["seeds"], force=True,
                                                      only=u, log=print)
                            except Exception as e:
                                log("  %s 重建失败：%s" % (u[-40:], e))
                        log("站点池重建完成，开始聚合")
                        run_once()
                    except Exception as e:
                        log("重建失败：%s" % e)
                threading.Thread(target=_rebuild, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path in ("/api/alist", "/api/alist/test"):
                d = self._data_dir()
                if path == "/api/alist/test":
                    cand = {"url": str(body.get("url") or "").strip(),
                            "token": str(body.get("token") or "").strip(),
                            "roots": [str(x).strip() for x in
                                      str(body.get("roots") or "").splitlines()
                                      if str(x).strip()]}
                    if not cand["url"]:
                        return self._send(400, {"ok": False, "error": "先填地址"})
                    try:
                        cls = AL._classes(cand)
                    except Exception as e:                # noqa: BLE001
                        return self._send(200, {"ok": False,
                                                "error": "连不上：%s" % e})
                    if not cls:
                        return self._send(200, {
                            "ok": False,
                            "error": "连上了，但这些目录里没有子目录或视频文件"})
                    return self._send(200, {"ok": True, "classes": [
                        {"name": c["type_name"]} for c in cls]})
                if body.get("url") is None:
                    return self._send(200, AL.load_cfg(d))
                doc = AL.save_cfg(d, {
                    "url": body.get("url"),
                    "token": body.get("token"),
                    "roots": str(body.get("roots") or "").splitlines(),
                    "as_home": body.get("as_home")})
                return self._send(200, {"ok": True, "alist": doc})
            if path == "/api/mirrors":
                import discover as D
                if body.get("mirrors") is None:
                    return self._send(200, {"mirrors": D.MIRRORS,
                                            "default": D.MIRROR_DEFAULT})
                items = [str(x).strip() for x in (body.get("mirrors") or [])
                         if str(x).strip()]
                D.set_mirrors(items)
                ST.save_file(os.path.join(self._data_dir(), "mirrors.json"),
                             {"mirrors": D.MIRRORS})
                return self._send(200, {"ok": True, "mirrors": D.MIRRORS})
            if path == "/api/rules":
                return self._send(200, {"rules": A.RULES,
                                        "defaults": A.RULES_DEFAULT})
            if path == "/api/site/pin":
                name = str(body.get("name") or "").strip()
                if not name:
                    return self._send(400, {"ok": False, "error": "缺少站点名"})
                d = self._data_dir()
                blk = ST.load(d, "block.json", {"names": [], "assets": [], "pin": []})
                pins = [x for x in (blk.get("pin") or []) if x]
                if body.get("pinned", True):
                    if name not in pins:
                        pins.append(name)
                else:
                    pins = [x for x in pins if x != name]
                blk["pin"] = pins
                ST.save(d, "block.json", blk)
                return self._send(200, {"ok": True, "pin": pins})
            if path == "/api/site/block":
                names_in = [str(x).strip() for x in (body.get("names") or []) if str(x).strip()]
                name = str(body.get("name") or "").strip()
                if names_in:
                    d = self._data_dir()
                    blk = ST.load(d, "block.json", {"names": [], "assets": [], "pin": []})
                    names = [x for x in (blk.get("names") or []) if x]
                    low = {x.lower() for x in names}
                    for n in names_in:
                        if n.lower() not in low:
                            names.append(n)
                            low.add(n.lower())
                    blk["names"] = names
                    ST.save(d, "block.json", blk)
                    return self._send(200, {"ok": True, "added": len(names_in), "block": blk})
                if not name:
                    return self._send(400, {"ok": False, "error": "缺少站点名"})
                d = self._data_dir()
                blk = ST.load(d, "block.json", {"names": [], "assets": []})
                names = [x for x in (blk.get("names") or []) if x]
                low = [x.lower() for x in names]
                if body.get("blocked", True):
                    if name.lower() not in low:
                        names.append(name)
                else:
                    names = [x for x in names if x.lower() != name.lower()]
                blk["names"] = names
                ST.save(d, "block.json", blk)
                return self._send(200, {"ok": True, "block": blk})
            if path == "/api/block":
                return self._send(200, self._block(body))
            if path == "/api/probe":
                return self._send(200, self._probe_one(body))
            if path == "/api/password":
                old = str(body.get("old") or "")
                new = str(body.get("new") or "")
                # 还没设过密码（TOKEN 也留空）时，没有"原密码"可验，
                # 直接允许设置 —— 否则第一次在面板里设密码永远失败。
                if auth_ready() and not auth_password_ok(old):
                    return self._send(200, {"ok": False, "error": "原密码不对"})
                if len(new) < 6:
                    return self._send(200, {"ok": False, "error": "新密码至少 6 位"})
                auth_set_password(new)
                access_flush(force=True)
                return self._send(200, {"ok": True,
                                        "Set-Cookie_hint": "重新登录后生效"})
            if path == "/api/jargate/scan":
                def _bg3():
                    try:
                        JG.scan_all(self._data_dir(), force=True, log=print)
                    except Exception as e:
                        print("jar 扫描失败：%s" % e)
                threading.Thread(target=_bg3, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path == "/api/asset/speed":
                _urls = [str(u) for u in (body.get("urls") or []) if u]
                _names = {str(x) for x in (body.get("names") or []) if x}
                if _names:
                    _doc = ST.load(self._data_dir(), "jars.json", {})
                    _urls = [s0.get("jar") for s0 in (_doc.get("sites") or [])
                             if s0.get("name") in _names and s0.get("jar")]
                    _urls = list(dict.fromkeys(_urls))
                def _bg2():
                    try:
                        A.speed_test_assets(self._data_dir(), force=True,
                                            urls=_urls or None)
                    except Exception as e:
                        print("资源测速失败：%s" % e)
                threading.Thread(target=_bg2, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path in ("/api/live/big", "/api/live/small", "/api/live/both",
                        "/api/live/screen"):
                _d = self._data_dir()
                _mode = path.rsplit("/", 1)[-1]

                if CACHE["running"]:
                    return self._send(409, {
                        "ok": False, "error": "点播聚合正在跑，等它结束再试"})
                if not LIVE_LOCK.acquire(blocking=False):
                    return self._send(202, {
                        "ok": False, "running": True, "mode": LIVE_RUN["mode"],
                        "error": "直播聚合已经在跑了（%s），等它结束" % (
                            LIVE_RUN["mode"] or "未知")})

                def _run():
                    LIVE_RUN.update(on=True, mode=_mode, since=time.time())
                    try:
                        _live_tick(_d,
                                   force_big=_mode in ("big", "both"),
                                   force_small=_mode in ("small", "both"),
                                   force_screen=_mode == "screen",
                                   log=log)
                    except Exception as e:            # noqa: BLE001
                        log("直播调度失败：%s" % e)
                    finally:
                        LIVE_RUN.update(on=False, mode="", since=0.0)
                        LIVE_LOCK.release()
                threading.Thread(target=_run, daemon=True).start()
                return self._send(200, {"ok": True, "started": _mode})
            if path == "/api/live/parse":
                _u = str(body.get("url") or "").strip()
                if not _u:
                    return self._send(400, {"error": "缺少 url"})

                def _one():
                    try:
                        import live as L
                        d = self._data_dir()
                        with open(os.path.join(d, "lives.json"), encoding="utf-8") as f:
                            items = (json.load(f).get("items") or [])
                        one = [x for x in items if (x.get("url") or "").strip() == _u]
                        if not one:
                            log("直播源不存在：%s" % _u[:60])
                            return
                        ch, stats = L.expand_lives_stats(one, max_sources=0,
                                                         timeout=15, log=log)
                        L._save_source_stats(d, stats)
                        r = stats.get(_u) or {}
                        log("直播源解析完成 %s：%d 频道（央视 %d / 卫视 %d）%s"
                            % ((one[0].get("name") or "")[:20], r.get("n", 0),
                               r.get("cctv", 0), r.get("sat", 0),
                               ("｜" + r["err"]) if r.get("err") else ""))
                    except Exception as e:            # noqa: BLE001
                        log("直播源解析失败：%s" % e)
                threading.Thread(target=_one, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path == "/api/live/block":
                _u = str(body.get("url") or "").strip()
                _on = 1 if body.get("on") else 0
                d = self._data_dir()
                p2 = os.path.join(d, "lives.json")
                try:
                    with open(p2, encoding="utf-8") as f:
                        doc = json.load(f)
                    nm = ""
                    for it in doc.get("items") or []:
                        if (it.get("url") or "").strip() == _u:
                            it["blocked"] = _on
                            nm = it.get("name") or ""
                    ST.save_file(p2, doc)
                    import live as _L
                    if _on:
                        _L.tomb_add(self._data_dir(), _u, nm, "blocked")
                    else:
                        _L.tomb_drop(self._data_dir(), _u)
                    return self._send(200, {"ok": True, "blocked": _on})
                except Exception as e:                # noqa: BLE001
                    return self._send(500, {"error": str(e)})
            if path == "/api/live/delete":
                _u = str(body.get("url") or "").strip()
                d = self._data_dir()
                p2 = os.path.join(d, "lives.json")
                try:
                    with open(p2, encoding="utf-8") as f:
                        doc = json.load(f)
                    before = len(doc.get("items") or [])
                    _nm = ""
                    for x in doc.get("items") or []:
                        if (x.get("url") or "").strip() == _u:
                            _nm = x.get("name") or ""
                    doc["items"] = [x for x in (doc.get("items") or [])
                                    if (x.get("url") or "").strip() != _u]
                    ST.save_file(p2, doc)
                    import live as _L
                    _L.tomb_add(self._data_dir(), _u, _nm, "deleted")
                    return self._send(200, {"ok": True,
                                            "removed": before - len(doc["items"])})
                except Exception as e:                # noqa: BLE001
                    return self._send(500, {"error": str(e)})
            if path == "/api/live/refresh":
                def _bg():
                    try:
                        live_doc(force=True, log=print)
                    except Exception as e:
                        print("直播测速失败：%s" % e)
                threading.Thread(target=_bg, daemon=True).start()
                return self._send(200, {"ok": True, "started": True})
            if path == "/api/settings":
                return self._send(200, self._settings(body))
            if path == "/api/config/add":
                return self._send(200, self._config_add(body))
            if path == "/api/msg/reply":
                d = self._data_dir()
                with MSG_LOCK:
                    return self._send(200, self._msg_reply(d, body))
            if path == "/api/msg/pin":
                d = self._data_dir()
                with MSG_LOCK:
                    return self._send(200, self._msg_pin(d, body))
            if path == "/api/msg/del":
                d = self._data_dir()
                with MSG_LOCK:
                    return self._send(200, self._msg_del(d, body))
            if path == "/api/config/import":
                return self._send(200, self._config_import(body))
            if path == "/api/config/sync":
                return self._send(200, self._config_sync(body))
            if path == "/api/config/promote":
                return self._send(200, self._tier(body, "active"))
            if path == "/api/config/demote":
                return self._send(200, self._tier(body, "bench"))
            if path == "/api/config/remove":
                return self._send(200, self._config_remove(body))
            if path == "/api/rollback":
                return self._send(200, self._rollback(body))
        except Exception as e:
            return self._send(500, {"error": "%s: %s" % (type(e).__name__, e)})
        return self._send(404, {"error": "unknown api", "path": path})

    # ---- 管理接口（都要求 token）
    def _data_dir(self):
        return os.path.dirname(os.path.abspath(CFG["seeds"]))

    def _key_api(self):
        return {s.get("key"): s.get("api")
                for s in (CACHE.get("report") or {}).get("sources", [])}

    def _overview(self):
        rep = CACHE.get("report") or {}
        full, allp = CACHE.get("full") or {}, CACHE.get("all") or {}
        def stat(doc):
            sites = doc.get("sites") or []
            ext = [x for x in sites if x.get("type") == 3]
            return {"total": len(sites), "cms": len(sites) - len(ext),
                    "ext": len(ext), "parses": len(doc.get("parses") or []),
                    "sources": len(doc.get("sources") or [])}
        return {
            "last_run": CACHE.get("last_run"),
            "running": CACHE["running"],
            "error": CACHE.get("last_error"),
            "elapsed": rep.get("elapsed_sec"),
            "counts": {"alive": rep.get("alive"), "grace": rep.get("grace"),
                       "active": rep.get("active"), "bench": rep.get("bench_n"),
                       "dead": rep.get("dead"), "seeds": rep.get("seeds"),
                       "probed": rep.get("probed"), "queued": rep.get("queued"),
                       "merged": rep.get("merged"), "deduped": rep.get("deduped"),
                       "orphan": getattr(self, "_orphan_n", 0),
                       "tvbox": rep.get("tvbox_sites"),
                       "libretv": rep.get("libretv_sources")},
            "profiles": {"tvbox": stat(CACHE.get("tvbox") or {}),
                         "ltv": stat(CACHE.get("ltv") or {}),
                         "all": stat(allp)},
            "sources": self._with_usage(rep.get("sources") or []),
            "live": self._live_overview(),
            "bench": rep.get("bench", []),
            "dropped": rep.get("dropped", []),
            "settings": A.load_settings(os.path.join(self._data_dir(),
                                                     "settings.json")),
            "pending": [A.norm(x.get("api", ""))
                        for x in A.load_sources_doc(CFG["seeds"]).get("sources", [])
                        if x.get("pending")],
            "lastgood": self._lastgood_meta(),
            "configs": self._configs_list(),
            "config_limit": int((A.load_settings(os.path.join(
                self._data_dir(), "settings.json")).get("max_configs") or 10)),
            "lives": self._lives_summary(),
            "assets": self._assets_list(),
            "ext": self._ext_stats(),
            "ext_sites": self._ext_sites(),
            "reasons": self._reasons(),
            "gate": JG.summary(self._data_dir()),
            "rules": {"rules": A.RULES},
            "mirrors": _mirror_list(),
            "overrides": A.load_overrides(os.path.join(self._data_dir(),
                                                       "overrides.json")),
            "block": A.load_block(os.path.join(self._data_dir(), "block.json")),
            "access": access_summary(60),
        }

    def _lastgood_meta(self):
        path = os.path.join(self._data_dir(), "lastgood", "meta.json")
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _configs_list(self):
        d = self._data_dir()
        try:
            with open(os.path.join(d, "configs.json"), encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            reg = {"configs": []}
        st = A.load_cfg_state(d)
        out = []
        for ent in reg.get("configs") or []:
            url = ent.get("url") if isinstance(ent, dict) else ent
            if not url:
                continue
            s0 = st.get(url) or {}
            out.append({"url": url,
                        "tier": (ent.get("tier") if isinstance(ent, dict) else "active") or "active",
                        "name": (ent.get("name") if isinstance(ent, dict) else "") or url,
                        "last": s0.get("last") or 0,
                        "latency": s0.get("latency") or 0,
                        "fail": s0.get("fail") or 0,
                        "ok": s0.get("ok"),
                        "stats": s0.get("stats") or {},
                        "added_cms": s0.get("added_cms") or 0,
                        "added_ext": s0.get("added_ext") or 0,
                        "error": s0.get("error") or ""})
        return out

    def _with_usage(self, rows):
        usage = self._usage_map()
        orphan = set()
        n_orphan = 0
        try:
            with open(os.path.join(self._data_dir(), "sources.json"),
                      encoding="utf-8") as f:
                for x in (json.load(f).get("sources") or []):
                    if x.get("orphan"):
                        orphan.add(x.get("api") or "")
                        n_orphan += 1
        except Exception:
            pass
        self._orphan_n = n_orphan
        out = []
        for r0 in rows:
            r = dict(r0)
            r["used"] = sorted(usage.get(("key", r0.get("key")), set()))
            if (r0.get("api") or "") in orphan:
                r["orphan"] = 1
            out.append(r)
        return out

    def _usage_map(self):
        """每个站点被哪些输出档用了：("key", k) / ("api", u) -> {"默认","全能",...}

        直接从生成好的 out/*.json 反查，比在内存里推演配额更准。
        """
        m = {}
        for label, fn in (("默认", "tvbox.json"), ("全能", "all.json")):
            doc = ST.load(CFG["out"], fn, {})
            for s0 in doc.get("sites") or []:
                if s0.get("key"):
                    m.setdefault(("key", s0["key"]), set()).add(label)
                if s0.get("api"):
                    m.setdefault(("api", s0["api"]), set()).add(label)
        # cms（纯 CMS 输出）不能直接拿 site.api 比：开了 /cms 代理后，
        # 输出里的 api 已经是 <base>/cms/<key>，和 LibreTV 里的上游地址对不上。
        # 所以从 site_table 反查 key -> 上游 api，再和 ltv.json 的 url 比对。
        try:
            doc = ST.load(CFG["out"], "ltv.json", {})
            ltv_urls = {str(x.get("url") or "").strip()
                        for x in (doc.get("sources") or [])}
            for k, (api, _p) in site_table().items():
                if api and api in ltv_urls:
                    m.setdefault(("key", k), set()).add("cms")
                    m.setdefault(("api", api), set()).add("cms")
        except Exception:
            pass
        return m

    def _ext_sites(self):
        """扩展站点逐个列出（含网盘），带「被哪些档使用」"""
        d = self._data_dir()
        doc = ST.load(d, "jars.json", {})
        if not doc:
            return {"items": [], "pan": []}
        usage = self._usage_map()
        block = ST.load(d, "block.json", {"names": [], "assets": []})
        bnames = [str(x).lower() for x in (block.get("names") or [])]
        items, pan = [], []
        for s0 in doc.get("sites") or []:
            name = s0.get("name") or ""
            api = s0.get("api") or ""
            key = A.ext_key(s0)
            row = {"name": name, "kind": s0.get("kind") or "jar", "api": api,
                   "jar": s0.get("jar") or "",
                   "reason": A.site_reason(s0),
                   "gate": (self._gate_map().get(s0.get("jar") or "") or {}),
                   "used": sorted(usage.get(("key", key), set())),
                   "blocked": any(b in name.lower() for b in bnames),
                   "pinned": name in {str(x) for x in (block.get("pin") or [])}}
            (pan if A.is_pan(s0) else items).append(row)
        items.sort(key=lambda r: (r["kind"], r["name"]))
        pan.sort(key=lambda r: r["name"])
        return {"items": items, "pan": pan}

    def _gate_map(self):
        return ST.load(self._data_dir(), "jar_gate.json", {}) or {}

    def _reasons(self):
        """按原因汇总被拦下的站点数（对齐 ElderTV 的「需要留意」）"""
        d = self._data_dir()
        doc = ST.load(d, "jars.json", {})
        cnt = {}
        for s0 in doc.get("sites") or []:
            why = A.site_reason(s0)
            if why:
                cnt[why] = cnt.get(why, 0) + 1
        return {"counts": cnt, "total": sum(cnt.values())}

    def _ext_stats(self):
        """扩展站点按类型统计（剔除网盘类），给面板显示占比用"""
        d = self._data_dir()
        try:
            with open(os.path.join(d, "jars.json"), encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            return {"jar": 0, "js": 0, "py": 0, "pan": 0, "total": 0}
        out = {"jar": 0, "js": 0, "py": 0, "pan": 0}
        for s0 in doc.get("sites") or []:
            if A.is_pan(s0):
                out["pan"] += 1
                continue
            k = s0.get("kind") or "jar"
            out[k] = out.get(k, 0) + 1
        out["total"] = out["jar"] + out["js"] + out["py"]
        return out

    def _assets_list(self):
        """jar/js/py 资源清单：类型、承载站点数、是否可达"""
        d = self._data_dir()
        try:
            with open(os.path.join(d, "jars.json"), encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            return []
        try:
            with open(os.path.join(d, "jar_state.json"), encoding="utf-8") as f:
                ok_map = json.load(f)
        except Exception:
            ok_map = {}
        agg = {}
        for s0 in doc.get("sites") or []:
            asset = s0.get("jar") or s0.get("api") or ""
            if not asset:
                continue
            a = agg.setdefault(asset, {"asset": asset, "kind": s0.get("kind") or "jar",
                                       "sites": 0, "sample": s0.get("name")})
            a["sites"] += 1
        try:
            with open(os.path.join(d, "asset_speed.json"), encoding="utf-8") as f:
                speeds = (json.load(f).get("items") or {})
        except Exception:
            speeds = {}
        rows = list(agg.values())
        for r in rows:
            r["reachable"] = ok_map.get(r["asset"])
            sp = speeds.get(r["asset"]) or {}
            r.update(ttfb=sp.get("ttfb") or 0, speed=sp.get("speed") or 0,
                     size=sp.get("bytes") or 0, tested=bool(sp))
        rows.sort(key=lambda x: -x["sites"])
        return rows[:200]

    def _live_overview(self):
        """直播侧状态：正在跑什么、上次大/小聚合是什么时候"""
        try:
            st = _live_state(self._data_dir())
        except Exception:
            st = {}

        def fmt(ts):
            try:
                return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(ts)))
            except Exception:
                return None

        mode_name = {"big": "大聚合", "small": "小聚合", "both": "大+小",
                     "screen": "全量复检"}
        return {"running": LIVE_RUN["on"],
                "mode": mode_name.get(LIVE_RUN["mode"], LIVE_RUN["mode"] or ""),
                "since": int(time.time() - LIVE_RUN["since"]) if LIVE_RUN["since"] else 0,
                "last_big": fmt(st.get("last_big")),
                "last_small": fmt(st.get("last_small")),
                "last_bench": fmt(st.get("last_bench"))}

    def _lives_summary(self):
        try:
            with open(os.path.join(self._data_dir(), "lives.json"),
                      encoding="utf-8") as f:
                doc = json.load(f)
        except Exception:
            return {"total": 0, "updated": "", "sample": []}
        items = doc.get("items") or []
        tested = None
        try:
            with open(os.path.join(self._data_dir(), "live_tested.json"),
                      encoding="utf-8") as f:
                t = json.load(f)
            tested = {"cctv": len(t.get("cctv") or {}),
                      "sat": len(t.get("sat") or {}),
                      "hmt": len(t.get("hmt") or {}),
                      "local": len(t.get("local") or {}),
                      "dropped": len(t.get("other") or {}),
                      "ok": t.get("ok") or 0,
                      "lines": sum(len(v) for k in ("cctv", "sat", "hmt", "local")
                                   for v in (t.get(k) or {}).values())}
        except Exception:
            pass
        seg_bad = low_bps = _tomb_n = 0
        try:
            import live as _L0
            _tomb_n = len(_L0.load_tomb(self._data_dir()))
        except Exception:
            pass
        try:
            with open(os.path.join(self._data_dir(), "live_url_cache.json"),
                      encoding="utf-8") as f:
                _c = json.load(f)
            for v in _c.values():
                if not v.get("ok"):
                    continue
                if v.get("seg_ok") is False:
                    seg_bad += 1
                elif (v.get("bps") or 0) and v["bps"] < 200:
                    low_bps += 1
        except Exception:
            pass

        def _row(x):
            return {"name": x.get("name"), "url": x.get("url"),
                    "tier": x.get("tier") or "bench",
                    "from": x.get("from"), "blocked": int(x.get("blocked") or 0),
                    "channels": x.get("channels_n"), "cctv": x.get("cctv"),
                    "sat": x.get("sat"), "local": x.get("local"),
                    "parse_ms": x.get("parse_ms"), "parse_err": x.get("parse_err"),
                    "parsed_at": x.get("parsed_at")}
        rows = [_row(x) for x in items]
        def _score(x):
            return (x.get("cctv") or 0) + (x.get("sat") or 0)
        rows.sort(key=lambda r: (-_score(r), (r.get("name") or "")))
        try:
            stg = A.load_settings(os.path.join(self._data_dir(), "settings.json"))
        except Exception:
            stg = {}
        n = int(stg.get("live_active_n") or 10)
        act = [x for x in rows if x.get("tier") == "active"]
        bnc = [x for x in rows if x.get("tier") == "bench" and not x.get("blocked")]
        free = max(0, n - len(act))
        return {"total": len(items), "updated": doc.get("updated") or "",
                "tested": tested, "items": rows, "sample": rows[:30],
                "active_n": n, "active_count": len(act), "free_slots": free,
                "bench_count": len(bnc),
                "bench_best": max([_score(x) for x in bnc] or [0]),
                "weakest": min([_score(x) for x in act] or [0]),
                "seg_bad": seg_bad, "low_bps": low_bps, "tomb": _tomb_n}

    def _tier(self, body, tier):
        """把一条线路设为主线路 / 备选"""
        url = str(body.get("url") or "").strip()
        d = self._data_dir()
        path = os.path.join(d, "configs.json")
        try:
            with open(path, encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            return {"ok": False, "error": "源库读不到"}
        entries = A._tier_map(reg.get("configs") or [])
        limit = int(A.load_settings(os.path.join(d, "settings.json"))
                    .get("max_configs") or 10)
        if tier == "active":
            act = [e for e in entries if isinstance(e, dict) and e.get("tier") != "bench"]
            if url not in {e.get("url") for e in act} and len(act) >= limit:
                return {"ok": False,
                        "error": "已到最大解析线路数（%d），先把别的线路放进备选库" % limit}
        hit = False
        for e in entries:
            if isinstance(e, dict) and e.get("url") == url:
                e["tier"] = tier
                hit = True
        if not hit:
            return {"ok": False, "error": "源库里没有这条线路"}
        reg["configs"] = entries
        ST.save_file(path, reg)
        return {"ok": True, "tier": tier}

    def _settings(self, body):
        path = os.path.join(self._data_dir(), "settings.json")
        cur = A.load_settings(path)
        cur.update({k: v for k, v in (body or {}).items() if k in cur})
        saved = A.save_settings(cur, path)
        return {"ok": True, "settings": saved}

    ADD_LIMIT = 10          # 一次最多加几个配置接口

    def _config_add(self, body):
        """添加配置接口。支持一次贴多个（换行/空格/逗号分隔），上限 10 个

        超过 10 个不报错，只取前 10 个，并明确告诉用户被截断了多少。
        """
        raw = body.get("urls")
        if raw is None:
            raw = body.get("url") or ""
        if isinstance(raw, (list, tuple)):
            raw = "\n".join(str(x) for x in raw)
        items = [u.strip() for u in re.split(r"[\s,;]+", str(raw)) if u.strip()]

        if not items:
            return {"ok": False, "error": "要填配置地址（http/https 开头）"}

        total = len(items)
        truncated = max(0, total - self.ADD_LIMIT)
        items = items[:self.ADD_LIMIT]

        name = str(body.get("name") or "").strip()
        d = self._data_dir()
        path = os.path.join(d, "configs.json")
        try:
            with open(path, encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            reg = {"configs": []}
        have = {(x.get("url") if isinstance(x, dict) else x) for x in reg["configs"]}

        added, skipped = [], []
        for u in items:
            if not u.lower().startswith(("http://", "https://")):
                skipped.append({"url": u, "why": "不是 http/https 地址"})
                continue
            if A.is_private(u):
                skipped.append({"url": u, "why": "内网地址不收"})
                continue
            if u in have:
                skipped.append({"url": u, "why": "已在源库里"})
                continue
            have.add(u)
            # 多个一起加时名称自动带上序号，只有一个就用用户填的名字
            nm = name if (name and len(items) == 1) else ""
            reg["configs"].append({"name": nm, "url": u, "note": ""})
            added.append({"url": u, "name": nm})

        if not added:
            return {"ok": False, "error": "没有可添加的地址", "skipped": skipped,
                    "truncated": truncated, "limit": self.ADD_LIMIT}

        ST.save_file(path, reg)

        # 解析放到后台：10 个地址串行拉要好几分钟，不能让请求挂着
        def _sync_new(urls):
            for u in urls:
                if not CFG_SYNC_LOCK.acquire(blocking=False):
                    log("  [CFG] 有线路在解析，%s 稍后由定时任务补上" % u[-36:])
                    continue
                try:
                    res = A.sync_config_sources(d, CFG["seeds"], force=True, only=u)
                    r0 = (res or [{}])[0]
                    log("  [CFG] 新增 %s → CMS %d"
                        % (u[-36:], len((r0.get("stats") or {}).get("cms", [])
                                        if isinstance((r0.get("stats") or {}).get("cms"), list)
                                        else []) or (r0.get("stats") or {}).get("cms", 0)))
                except Exception as e:            # noqa: BLE001
                    log("  [CFG] 新增 %s 解析失败：%s" % (u[-36:], e))
                finally:
                    CFG_SYNC_LOCK.release()
        threading.Thread(target=_sync_new,
                         args=([x["url"] for x in added],), daemon=True).start()

        return {"ok": True, "added": added, "skipped": skipped,
                "truncated": truncated, "limit": self.ADD_LIMIT,
                "total": total, "syncing": True}

    def _config_sync(self, body):
        url = str(body.get("url") or "").strip() or None
        if not CFG_SYNC_LOCK.acquire(blocking=False):
            return {"ok": False, "error": "已经有一条线路在解析，等它跑完再点（一条最多 60 秒）"}
        try:
            res = A.sync_config_sources(self._data_dir(), CFG["seeds"],
                                        force=True, only=url)
            return {"ok": True, "synced": res}
        finally:
            CFG_SYNC_LOCK.release()

    EXPORT_NAME = "tvbox-agg-export.txt"

    def _export_txt(self):
        """导出人类可读的 txt：配置接口 + 设置

        只导这两样是有意的：采集站/扩展站/直播源都能从接口重新拉，
        导出去再导回来只会和池子里现有的重复。接口才是「根」。
        """
        d = self._data_dir()
        try:
            with open(os.path.join(d, "configs.json"), encoding="utf-8") as f:
                cfgs = json.load(f).get("configs") or []
        except Exception:
            cfgs = []
        act = [c for c in cfgs if isinstance(c, dict) and c.get("tier") != "bench"]
        bnc = [c for c in cfgs if isinstance(c, dict) and c.get("tier") == "bench"]
        try:
            stg = A.load_settings(os.path.join(d, "settings.json"))
        except Exception:
            stg = {}

        t = time.strftime("%Y-%m-%d %H:%M:%S")
        out = [
            "# TVBox 聚合器 · 配置导出",
            "# 时间：%s ｜ 版本：0.9" % t,
            "# 接口：%d 个（主用 %d ｜ 备选 %d）" % (len(cfgs), len(act), len(bnc)),
            "#",
            "# 只导出「配置接口 + 设置」。采集站/扩展站/直播源都能从接口重新拉取，",
            "# 导出它们再导回来只会和池子里现有的重复，所以不导。",
            "#",
            "# 接口行格式：档位<TAB>名称<TAB>地址（可直接手改，导入时按地址去重）",
            "# 想全部改成备选，把「主用」改成「备选」即可。",
            "",
            "[接口]",
        ]
        for c, tier in [(x, "主用") for x in act] + [(x, "备选") for x in bnc]:
            out.append("%s\t%s\t%s" % (tier, c.get("name") or "", c.get("url") or ""))
        out += ["", "# 设置项：key=value，导入时可选是否覆盖当前设置", "[设置]"]
        for k in sorted(stg):
            out.append("%s=%s" % (k, stg[k]))

        # 屏蔽名单：导入时合并去重，不会覆盖你现有的
        try:
            blk = A.load_block(os.path.join(d, "block.json"))
        except Exception:
            blk = {"names": [], "assets": []}
        if blk.get("names") or blk.get("assets"):
            out += ["", "# 站点屏蔽：名字关键字 / 资源地址关键字，导入时合并去重",
                    "[屏蔽名单]"]
            for x in blk.get("names") or []:
                out.append("名字\t%s" % x)
            for x in blk.get("assets") or []:
                out.append("资源\t%s" % x)

        # GitHub 镜像（加速前缀），导入时合并去重
        try:
            import discover as _D
            mirs = _D.load_mirrors(d)
        except Exception:
            mirs = []
        if mirs:
            out += ["", "# GitHub 镜像前缀，一行一个，导入时合并去重", "[GitHub镜像]"]
            out += list(mirs)

        out.append("")
        return "\n".join(out)

    @staticmethod
    def _parse_export(text):
        """解析导出文本

        返回 {"configs": [(tier,name,url)], "settings": {}, "block": {names,assets},
              "mirrors": [...]}
        """
        urls, stg, sec = [], {}, ""
        block = {"names": [], "assets": []}
        mirrors = []
        for raw in (text or "").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                sec = line[1:-1].strip()
                continue
            if sec == "设置":
                if "=" in line and not line.startswith("["):
                    k, v = line.split("=", 1)
                    stg[k.strip()] = v.strip()
                continue
            if sec == "屏蔽名单":
                parts = [x.strip() for x in line.split("\t")]
                if len(parts) >= 2 and parts[0] in ("名字", "资源") and parts[1]:
                    block["names" if parts[0] == "名字" else "assets"].append(parts[1])
                elif line:
                    block["names"].append(line)     # 只写了关键字，当名字处理
                continue
            if sec == "GitHub镜像":
                if line and not line.startswith("#"):
                    mirrors.append(line)
                continue
            if sec != "接口":
                continue
            # 不管用 TAB、竖线还是空格分隔，都先从行里把 URL 抠出来，
            # 剩下的部分再判断档位和名称 —— 这样用户手改格式也不会解析漏。
            m = re.search(r"https?://[^\s|]+", line, re.I)
            if not m:
                continue
            url = m.group(0).rstrip(",;、。")
            rest = line.replace(url, "").strip(" \t|")
            parts = [x.strip() for x in re.split(r"[\t|]+", rest) if x.strip()]
            tier = "active" if parts and parts[0] in ("主用", "启用", "active") else "bench"
            name = " ".join(parts[1:]) if len(parts) > 1 else ""
            urls.append((tier, name, url))
        return {"configs": urls, "settings": stg,
                "block": block, "mirrors": mirrors}

    @staticmethod
    def _uniq(items):
        """按去空白 + 忽略大小写去重，保留原顺序"""
        seen, out = set(), []
        for x in items or []:
            t = str(x or "").strip()
            k = t.lower()
            if t and k not in seen:
                seen.add(k)
                out.append(t)
        return out

    def _msg_find(self, d, mid):
        items = msg_load(d)
        for it in items:
            if int(it.get("id") or 0) == mid:
                return items, it
        return items, None

    def _msg_reply(self, d, body):
        items, it = self._msg_find(d, int(body.get("id") or 0))
        if it is None:
            return {"ok": False, "error": "这条留言不在了"}
        it["reply"] = _msg_clean(body.get("reply"), MSG_TEXT_MAX)
        it["reply_ts"] = int(time.time())
        msg_save(d, items)
        return {"ok": True}

    def _msg_pin(self, d, body):
        items, it = self._msg_find(d, int(body.get("id") or 0))
        if it is None:
            return {"ok": False, "error": "这条留言不在了"}
        it["pinned"] = 1 if body.get("pinned") else 0
        msg_save(d, items)
        return {"ok": True}

    def _msg_del(self, d, body):
        mid = int(body.get("id") or 0)
        items = [x for x in msg_load(d) if int(x.get("id") or 0) != mid]
        msg_save(d, items)
        return {"ok": True}

    def _config_import(self, body):
        """导入 txt：接口/屏蔽/镜像都是合并去重，设置项可选覆盖"""
        parsed = self._parse_export(str(body.get("text") or ""))
        urls = parsed["configs"]
        stg = parsed["settings"]
        blk_in = parsed["block"]
        mirs_in = parsed["mirrors"]
        if not (urls or stg or blk_in["names"] or blk_in["assets"] or mirs_in):
            return {"ok": False, "error": "没解析到内容，确认贴的是导出的 txt"}
        d = self._data_dir()
        path = os.path.join(d, "configs.json")
        try:
            with open(path, encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            reg = {"configs": []}
        have = {(x.get("url") if isinstance(x, dict) else x) for x in reg["configs"]}
        added, skipped = 0, 0
        for tier, name, u in urls:
            if u in have:
                skipped += 1
                continue
            have.add(u)
            reg["configs"].append({"name": name, "url": u, "note": "",
                                   **({"tier": "bench"} if tier == "bench" else {})})
            added += 1
        if added:
            ST.save_file(path, reg)

        applied = 0
        if stg and int(body.get("apply_settings") or 0):
            sp = os.path.join(d, "settings.json")
            try:
                with open(sp, encoding="utf-8") as f:
                    cur = json.load(f)
            except Exception:
                cur = {}
            for k, v in stg.items():
                if k not in A.SETTING_DEFAULTS:
                    continue
                dv = A.SETTING_DEFAULTS[k]
                try:
                    cur[k] = int(v) if isinstance(dv, int) else v
                    applied += 1
                except Exception:
                    continue
            ST.save_file(sp, cur)

        # 屏蔽名单：合并去重（不删你现有的）
        blk_added = 0
        if blk_in["names"] or blk_in["assets"]:
            bp = os.path.join(d, "block.json")
            cur_blk = ST.load(d, "block.json", {"names": [], "assets": [], "pin": []})
            before = set(self._uniq(cur_blk.get("names"))) | set(self._uniq(cur_blk.get("assets")))
            names = self._uniq((cur_blk.get("names") or []) + blk_in["names"])
            assets = self._uniq((cur_blk.get("assets") or []) + blk_in["assets"])
            after = set(names) | set(assets)
            blk_added = len(after - before)
            cur_blk["names"], cur_blk["assets"] = names, assets
            cur_blk.setdefault("pin", [])
            ST.save_file(bp, cur_blk)

        # GitHub 镜像：合并去重
        mir_added = 0
        if mirs_in:
            try:
                import discover as _D
                cur_m = _D.load_mirrors(d)
            except Exception:
                cur_m = []
            merged = self._uniq(cur_m + mirs_in)
            mir_added = len(merged) - len(self._uniq(cur_m))
            try:
                import discover as _D
                _D.set_mirrors(merged)
                ST.save_file(os.path.join(d, "mirrors.json"),
                             {"mirrors": _D.MIRRORS})
            except Exception:
                mir_added = 0

        return {"ok": True, "added": added, "skipped": skipped,
                "settings_applied": applied, "settings_seen": len(stg),
                "block_added": blk_added, "mirror_added": mir_added}

    def _config_remove(self, body):
        url = str(body.get("url") or "").strip()
        d = self._data_dir()
        path = os.path.join(d, "configs.json")
        try:
            with open(path, encoding="utf-8") as f:
                reg = json.load(f)
        except Exception:
            return {"ok": False, "error": "源库读不到"}
        before = len(reg["configs"])
        name = ""
        for x in reg["configs"]:
            if isinstance(x, dict) and x.get("url") == url:
                name = x.get("name") or ""
        reg["configs"] = [x for x in reg["configs"]
                          if (x.get("url") if isinstance(x, dict) else x) != url]
        ST.save_file(path, reg)
        # 离源：这个接口带进来的站点/扩展/直播源留下来继续用
        n = A.orphan_of_config(d, url, name)
        return {"ok": True, "removed": before - len(reg["configs"]),
                "orphaned": n}

    def _rollback(self, body):
        docs = A.restore_lastgood(self._data_dir(), CFG["out"])
        if not docs:
            return {"ok": False, "error": "还没有快照可用"}
        load_cache()
        return {"ok": True, "meta": docs.get("meta") or {},
                "sites": len((docs.get("tvbox.json") or {}).get("sites") or [])}

    def _probe_seed(self, url, name=""):
        r = A.probe({"name": name or A.site_key(url), "api": url, "from": "manual"},
                    timeout=20, retries=1, with_classes=True)
        return {"alive": r["alive"], "type": r["type"], "msg": r["msg"],
                "latency": r.get("latency") or 0,
                "searchable": r.get("searchable", 1),
                "param": r.get("param"), "nclass": len(r.get("classes") or []),
                "sample": r.get("sample"), "items": r.get("items"),
                "api": r["api"], "name": r["name"]}

    def _source_add(self, body):
        url = A.norm(str(body.get("url") or ""))
        if not url:
            return {"ok": False, "error": "缺 url"}
        if A.is_private(url):
            return {"ok": False, "error": "内网地址不收"}
        name = str(body.get("name") or "").strip() or A.site_key(url)
        info = self._probe_seed(url, name)

        # 探测结果写进 state，下一轮聚合直接复用（不用重新测方言/搜索）
        state = A.load_state(CFG["state"])
        st = state.setdefault(url, {})
        if info["alive"]:
            st.update(type=info["type"], name=name, last_ok=int(time.time()),
                      fail_streak=0, ok_streak=int(st.get("ok_streak") or 0) + 1)
            if info.get("param"):
                st["param"] = info["param"]
            st["searchable"] = int(info.get("searchable", 1))
        A.save_state(CFG["state"], state)

        path = CFG["seeds"]
        doc = A.load_sources_doc(path)
        have = {A.norm(x["api"]) for x in doc["sources"] if x.get("api")}
        added = False
        if url not in have:
            doc["sources"].append({"name": name, "api": url, "from": "manual"})
            A.dump_sources_doc(path, doc)
            added = True
        return {"ok": info["alive"], "added": added, "result": info}

    def _source_remove(self, body):
        url = A.norm(str(body.get("url") or ""))
        if not url:
            key = str(body.get("key") or "")
            url = A.norm(self._key_api().get(key) or "")
        if not url:
            return {"ok": False, "error": "缺 url 或 key"}
        path = CFG["seeds"]
        doc = A.load_sources_doc(path)
        before = len(doc["sources"])
        doc["sources"] = [x for x in doc["sources"]
                          if A.norm(x.get("api", "")) != url]
        A.dump_sources_doc(path, doc)
        return {"ok": True, "removed": before - len(doc["sources"]), "url": url}

    def _override(self, body):
        action = str(body.get("action") or "")
        url = A.norm(str(body.get("url") or "")) or \
            A.norm(self._key_api().get(str(body.get("key") or "")) or "")
        if not url:
            return {"ok": False, "error": "缺 url 或 key"}
        path = os.path.join(self._data_dir(), "overrides.json")
        ov = A.load_overrides(path)
        field = {"disable": "disabled", "enable": "disabled",
                 "pin": "pinned", "unpin": "pinned",
                 "bench": "bench", "unbench": "bench"}.get(action)
        if not field:
            return {"ok": False, "error": "action 只支持 disable/enable/pin/unpin/bench/unbench"}
        lst = [x for x in ov[field] if A.norm(x) != url]
        if action in ("disable", "pin", "bench"):
            lst.append(url)
        ov[field] = lst
        # 一个源不该同时处于互斥状态：禁用了就退出置顶/备选
        if action == "disable":
            ov["pinned"] = [x for x in ov["pinned"] if A.norm(x) != url]
            ov["bench"] = [x for x in ov["bench"] if A.norm(x) != url]
        if action in ("pin",):
            ov["bench"] = [x for x in ov["bench"] if A.norm(x) != url]
        A.save_overrides(ov, path)
        return {"ok": True, "overrides": ov}

    def _block(self, body):
        action = str(body.get("action") or "add")
        field = str(body.get("field") or "names")
        raw = body.get("value")
        if field not in ("names", "assets"):
            return {"ok": False, "error": "field 只支持 names / assets"}
        path = os.path.join(self._data_dir(), "block.json")
        blk = A.load_block(path)
        if action in ("set", "setList"):
            vals = raw if isinstance(raw, list) else str(raw or "").splitlines()
            lst = [str(x).strip() for x in vals if str(x).strip()]
        else:
            value = (raw[0] if isinstance(raw, list) and raw else str(raw or "")).strip()
            lst = [x for x in blk.get(field, []) if x != value]
            if action == "add" and value:
                lst.append(value)
        blk[field] = lst
        ST.save_file(path, blk)
        return {"ok": True, "block": blk}

    def _probe_one(self, body):
        url = A.norm(str(body.get("url") or "")) or \
            A.norm(self._key_api().get(str(body.get("key") or "")) or "")
        if not url:
            return {"ok": False, "error": "缺 url 或 key"}
        info = self._probe_seed(url, str(body.get("name") or ""))
        return dict(ok=True, result=info)

    # ---- 状态面板（页面本体在 admin.html，这里只注入初始数据）
    def _status_html(self):
        return self._render_admin()

    def _render_admin(self):
        try:
            pth = os.path.join(HERE, "admin.html")
            with open(pth, encoding="utf-8") as f:
                tpl = f.read()
            try:
                # 显示「最新一次改动」的时间（admin.html 和所有 .py 里最新的那个），
                # 这样后端改了也能一眼看出是不是新版本
                newest = os.path.getmtime(pth)
                for fn in os.listdir(HERE):
                    if fn.endswith(".py"):
                        newest = max(newest, os.path.getmtime(os.path.join(HERE, fn)))
                stamp = time.strftime("%m-%d %H:%M", time.localtime(newest))
            except Exception:
                stamp = "?"
            tpl = tpl.replace("__BUILD__", "v%s · 构建 %s" % (VERSION, stamp))
            tpl = tpl.replace("__PREFIX__", getattr(self, "_prefix", "") or "")
            return tpl.replace("__BOOT__",
                               json.dumps(self._overview(), ensure_ascii=False))
        except Exception as e:
            return ("<h1>管理页缺失</h1><p>%s</p>" % e)

    # 访问日志只留错误，避免刷屏
    def log_message(self, fmt, *args):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", default=os.environ.get("BIND", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "5680")))
    ap.add_argument("--interval", type=float,
                    default=float(os.environ.get("INTERVAL", "6")),
                    help="定时聚合间隔（小时），0 = 不自动跑")
    ap.add_argument("--token", default=os.environ.get("TOKEN", ""),
                    help="/refresh 的访问令牌，留空则不需鉴权")
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL", ""),
                    help="写进面板/多仓的对外地址，如 http://192.0.2.10:5680")
    ap.add_argument("--seeds", default=os.environ.get("SEEDS",
                    os.path.join(HERE, "sources.json")))
    ap.add_argument("--out", default=os.environ.get("OUT_DIR",
                    os.path.join(HERE, "out")))
    ap.add_argument("--state", default=os.environ.get("STATE",
                    os.path.join(HERE, "state.json")))
    ap.add_argument("--workers", type=int, default=int(os.environ.get("WORKERS", "3")))
    ap.add_argument("--timeout", type=int, default=int(os.environ.get("TIMEOUT", "20")))
    ap.add_argument("--drop-after", type=int, default=int(os.environ.get("DROP_AFTER", "3")))
    ap.add_argument("--proxy", action="store_true",
                    default=os.environ.get("PROXY", "") in ("1", "true", "yes"),
                    help="TVBox 输出改走 /cms/<key> 透明代理（修正采集站 t= 参数问题）")
    ap.add_argument("--no-startup-run", action="store_true")
    a = ap.parse_args()

    # 既没有 TOKEN、也没有在面板里改过密码，还监听 0.0.0.0 = 管理面板裸奔。
    # 内网自用没问题，但一旦映射到公网就是任何人可改配置，这里必须提醒。
    # （老数据里通常已经有 auth.json，那种情况不算裸奔，别误报）
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(a.seeds)),
                               "auth.json"), encoding="utf-8") as _af:
            _has_pw = bool(json.load(_af).get("hash"))
    except Exception:
        _has_pw = False
    if not a.token and a.bind == "0.0.0.0" and not _has_pw:
        print("!! 警告：既没有 TOKEN 也没有设置面板密码，却监听 0.0.0.0 —— "
              "管理面板与写接口无鉴权。\n"
              "!! 内网自用可忽略；要暴露到公网请设置 TOKEN（如 openssl rand -hex 12）。")

    # 全新部署时 data/ 可能是空的：先建目录、补一个空源库，
    # 否则第一轮聚合会直接 FileNotFoundError（面板上显示「上次出错」）
    try:
        os.makedirs(os.path.dirname(os.path.abspath(a.seeds)), exist_ok=True)
        os.makedirs(a.out, exist_ok=True)
        if not os.path.exists(a.seeds):
            A.dump_sources_doc(a.seeds, {"name": "自用聚合源", "sources": []})
    except Exception as _e:
        print("!! 初始化数据目录失败：%s" % _e)

    CFG.update(seeds=a.seeds, out=a.out, state=a.state, workers=a.workers,
               timeout=a.timeout, drop_after=a.drop_after, token=a.token,
               base_url=a.base_url, interval=max(a.interval, 0) * 3600)
    # 对外地址：BASE_URL 优先；没填就猜局域网 IP（写 127.0.0.1 的话电视会连自己）
    base = a.base_url.rstrip("/")
    _auto = False
    if not base:
        host = a.bind if a.bind not in ("0.0.0.0", "::") else (guess_lan_ip() or "")
        if host:
            base = "http://%s:%s" % (host, a.port)
            _auto = True
        else:
            base = "http://127.0.0.1:%s" % a.port
            print("!! 猜不到局域网地址，配置里会写成 %s —— 电视将无法访问，"
                  "请设 BASE_URL=http://<本机IP>:%s" % (base, a.port))
    if a.proxy:
        CFG["proxy_base"] = base
        if _auto:
            print("  对外地址  %s（自动探测；多网卡或反代请设 BASE_URL）" % base)
    os.makedirs(CFG["out"], exist_ok=True)
    load_cache()
    global AUTH_PATH
    _dd = os.path.dirname(os.path.abspath(CFG["seeds"]))
    AUTH_PATH = os.path.join(_dd, "auth.json")
    # 外置规则：data/rules.json 存在就用它覆盖默认值
    try:
        rules_path = os.path.join(_dd, "rules.json")
        if not os.path.exists(rules_path):
            ST.save_file(rules_path, A.RULES_DEFAULT)
        A.apply_rules(A.load_rules(_dd))
        log("规则已载入（%s）" % rules_path)
        try:
            _mv = A.migrate_source_origin(_dd)
            if _mv:
                log("来源标记迁移：%d 条 sources 的 from 从名字改成配置地址" % _mv)
        except Exception as e:
            log("来源标记迁移失败：%s" % e)
        try:
            import discover as D
            mpath = os.path.join(_dd, "mirrors.json")
            if not os.path.exists(mpath):
                ST.save_file(mpath, {"mirrors": D.MIRROR_DEFAULT})
            _m = D.load_mirrors(_dd)
            log("GitHub 镜像 %d 个（%s）" % (len(_m), mpath))
        except Exception as e:
            log("镜像配置载入失败，用默认：%s" % e)
    except Exception as e:
        log("规则载入失败，用默认值：%s" % e)
    access_load(os.path.join(_dd, "access.json"))

    def _flush_loop():
        while True:
            time.sleep(15)
            access_flush()
    threading.Thread(target=_flush_loop, daemon=True).start()

    def _config_loop():
        while True:
            time.sleep(1800)                      # 每 30 分钟看一次有没有到期的
            try:
                res = A.sync_config_sources(
                    os.path.dirname(os.path.abspath(CFG["seeds"])), CFG["seeds"])
                if res:
                    log("配置源刷新完成：%d 个（有变化的下轮聚合生效）" % len(res))
            except Exception as e:
                log("配置源刷新失败：%s: %s" % (type(e).__name__, e))
    threading.Thread(target=_config_loop, daemon=True).start()

    def _live_loop():
        # 启动后先等 3 分钟（让点播聚合先跑），之后每小时看一次直播有没有到期
        time.sleep(180)
        while True:
            try:
                _live_tick(_dd, log=log)
            except Exception as e:                # noqa: BLE001
                log("直播调度失败：%s: %s" % (type(e).__name__, e))
            time.sleep(3600)
    threading.Thread(target=_live_loop, daemon=True).start()

    AC.clear_legacy()
    CACHE.setdefault("libretv", CACHE.get("ltv") or {})
    n_sites = len(CACHE["tvbox"].get("sites", []))
    n_src = len((CACHE.get("ltv") or {}).get("sources", []))

    print("TVBox 聚合器服务启动", flush=True)
    print("  监听      %s:%s" % (a.bind, a.port), flush=True)
    _all = CACHE.get("all") or {}
    print("  默认档    %s/tvbox            （%d 站：CMS+jar+py，影视仓用）"
          % (base, n_sites), flush=True)
    print("  全能档    %s/tvbox-all        （%d 站：+js）"
          % (base, len(_all.get("sites") or [])), flush=True)
    print("  纯 CMS    %s/libretv          （%d 源，LibreTV 用）" % (base, n_src), flush=True)
    print("  直播主    %s/cctv.m3u          （央视+卫视）" % base, flush=True)
    print("  直播副    %s/other.m3u         （央视+卫视+港澳台+地方台）" % base, flush=True)
    print("  面板      %s/status" % base, flush=True)
    print("  定时      %s" % ("关闭" if CFG["interval"] == 0
                              else "每 %.0f 小时" % (CFG["interval"] / 3600)),
          flush=True)

    if not a.no_startup_run:
        run_async(delay=0.5)
    if CFG["interval"] > 0:
        threading.Thread(target=scheduler, name="scheduler", daemon=True).start()

    srv = ThreadingHTTPServer((a.bind, a.port), Handler)
    srv.daemon_threads = True
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n收到 Ctrl-C，退出", flush=True)
    finally:
        srv.server_close()


if __name__ == "__main__":
    sys.exit(main())
