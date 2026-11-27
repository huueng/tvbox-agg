# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""直播源：解析、测活、测速、按台排名，输出 m3u / TVBox lives

只做「直播源」这一层，不碰点播聚合。两个产出：
  /cctv.m3u    央视 + 卫视，每台多条线路按（清晰度, 延迟）排序
  /other.m3u   全量：央视 + 卫视 + 港澳台 + 地方台
"""

import concurrent.futures as cf
import json
import os
import re
import socket
import ssl
import time
import urllib.request

UA = ("Mozilla/5.0 (Linux; Android 12; TV) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36")
# 直播源里有相当一部分只有 IPv6（或 v6 更快）。
# 策略：先整批用 IPv4 测，失败的再用 IPv6 补测一轮——两边都试过才算真的不通。
_GAI_ORIG = socket.getaddrinfo
_FAMILY = {"v": 0}


def _apply_family(fam):
    """fam 传 socket.AF_INET / AF_INET6 即可锁定协议栈；0 表示系统默认"""
    _FAMILY["v"] = fam

    def _gai(host, port, family=0, type=0, proto=0, flags=0):
        return _GAI_ORIG(host, port, fam or family, type, proto, flags)

    socket.getaddrinfo = _gai


_HAS_V6 = {"v": None}


def has_ipv6():
    """能否真的建立一个 IPv6 连接（有地址不代表能出网）

    之前拿阿里 DNS 的 80 端口探，本来就不通，所以永远返回 False，
    IPv6 补测那一轮等于没跑。改成探一个真的开 443 的双栈网站。
    """
    if _HAS_V6["v"] is not None:
        return _HAS_V6["v"]
    ok = False
    for host in ("ipv6.baidu.com", "www.taobao.com", "ipv6.taobao.com"):
        try:
            infos = _GAI_ORIG(host, 443, socket.AF_INET6, socket.SOCK_STREAM)
            if not infos:
                continue
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            s.settimeout(4)
            s.connect(infos[0][4][:2])
            s.close()
            ok = True
            break
        except Exception:
            continue
    _HAS_V6["v"] = ok
    return ok


if os.environ.get("LIVE_IPV4") == "1":
    _apply_family(socket.AF_INET)            # 兼容旧开关：显式要求只用 v4

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

# 央视只认 CCTV1~17、CCTV5+、CCTV4K、CGTN。
# 用白名单而不是正则：垃圾源里满是「CCTV20 / CCTV95 / CCTV99」这种伪造台名。
CCTV_NUMS = {str(i) for i in range(1, 18)}
SAT_PROVINCES = (
    "北京 上海 东方 湖南 浙江 江苏 广东 深圳 安徽 山东 天津 重庆 四川 湖北 河南 "
    "河北 山西 陕西 辽宁 吉林 黑龙江 江西 福建 广西 云南 贵州 甘肃 青海 宁夏 "
    "新疆 内蒙古 西藏 海南 三沙 东南 兵团 农林 厦门 延边 南方 大湾区 康巴 海峡"
).split()

RE_CCTV = re.compile(r"(?i)(cctv[\s\-_]?(\d{1,2}|4k|5\+)|cgtn|中央|央视)")
RE_SAT = re.compile("|".join(SAT_PROVINCES) + r"|卫视")


def is_cctv(name):
    """严格的央视判定：CCTV1..17 / CCTV5+ / CCTV4K / CGTN"""
    n = (name or "").upper()
    m = re.search(r"CCTV[\s\-_]?(\d{1,2}|4K|5\+)", n)
    if m:
        v = m.group(1)
        if v in ("5+", "4K"):
            return True
        return v.lstrip("0") in CCTV_NUMS
    if "CGTN" in n:
        return True
    # 央视台球 / 央视风云音乐 / CCTV央视文化精品 这类，没有数字编号但也算央视
    if re.search(r"(央视|CCTV)", n) and CCTV_EXTRA.search(n):
        return True
    return False


PLACES = (
    "北京 上海 天津 重庆 河北 山西 辽宁 吉林 黑龙江 江苏 浙江 安徽 福建 江西 山东 "
    "河南 湖北 湖南 广东 广西 海南 四川 贵州 云南 陕西 甘肃 青海 宁夏 新疆 西藏 "
    "内蒙古 深圳 广州 杭州 南京 苏州 无锡 宁波 温州 成都 武汉 西安 青岛 大连 厦门 "
    "长沙 郑州 济南 合肥 南昌 太原 石家庄 沈阳 长春 哈尔滨 兰州 贵阳 南宁 海口 "
    "银川 西宁 乌鲁木齐 呼和浩特 拉萨 福州 昆明 东莞 佛山 珠海 汕头 泉州 烟台 洛阳"
).split()

# 名字里像「节目」而不是「频道」的，直接丢
JUNK_NAME = re.compile(r"(《|》|电影|电视剧|短剧|MV|舞曲|合集|解说|直播回看|测试|公告|广告)")


# 央视的付费/专业频道（CCTV风云系列等），没有数字编号但确实是央视
CCTV_EXTRA = re.compile(
    r"(台球|风云|高尔夫|足球|网球|纪录|戏曲|音乐|综艺|财经|社会|文化|怀旧|"
    r"女性|剧场|足球|排球|乒乓球|羽毛球|拳击|武术|第一剧场|怀旧剧场)")

CHANNEL_SUFFIX = re.compile(
    r"(台|频道|综合|新闻|影视|经济|都市|公共|文艺|体育|少儿|生活|科教|法制|"
    r"纪录|纪实|戏剧|娱乐|电视剧|电影|购物|交通|农业|卫视|法制|民生|"
    r"珠江|南方|金鹰|戏曲|人物|故事|生活|新闻综合)$")


def _strip_quality(n):
    """去掉画面质量后缀，方便判断「像不像频道名」"""
    return re.sub(r"(高清|超清|蓝光|标清|4K|8K|HD|FHD|UHD|IPV6|IPV4|线路\d+|备用\d*)$",
                  "", (n or "").strip(), flags=re.I).strip()


# 港澳台必须是**白名单**：早先用「凤凰」「非凡」「澳门」这种宽泛关键词，
# 结果「凤凰传奇奢香夫人」「非凡虎女王(上)」「澳门风云1」这些歌名/剧名全被当成频道，
# 69 个「港澳台台」里一多半是垃圾。
HMT_KEYS = re.compile(
    r"(凤凰卫视|凤凰中文|凤凰资讯|凤凰香港|鳳凰|翡翠台|翡翠|明珠台|明珠|无线新闻|"
    r"TVB|亚视|澳视|莲花卫视|澳门莲花|香港开电视|有线新闻|有线财经|NOW新闻|"
    r"东森新闻|东森财经|东森超视|东森美洲|东森中國|东森中国|中天新闻|中天综合|中天亚洲|"
    r"三立新闻|三立台湾|民视新闻|民视综艺|民视|台视新闻|华视新闻|中视新闻|"
    r"TVBS|纬来|靖天|龙华戏剧|龙华偶像|龙华电影|龙华洋片|八大|"
    r"非凡新闻|年代新闻|公视|寰宇新闻|客家电视)", re.I)

# 剧名/歌名特征：书名号、上下集、明显不是频道名的
JUNK_CH = re.compile(r"(《|》|\(上\)|\(下\)|\(中\)|\d{4}$)")


def is_hmt(name):
    """港澳台频道：默认丢在 other 里，能测通就会并进主输出的「卫视」组"""
    n = _strip_quality(name or "")
    # 不用 JUNK_NAME：它把「电影」也当垃圾词，但「龙华电影」是正经港台频道
    if not n or len(n) > 16 or JUNK_CH.search(n):
        return False
    if is_cctv(n):
        return False
    return bool(HMT_KEYS.search(n))


def is_local(name):
    """地方台：命中城市/省份名，且不是央视/卫视"""
    raw = name or ""
    if not raw or JUNK_NAME.search(raw):
        return False
    if is_cctv(raw) or is_sat(raw):
        return False
    n = _strip_quality(raw)
    if not n or len(n) > 12:
        return False
    place = [k for k in PLACES if k in n]
    if not place:
        return False
    # 光有地名不够（「袖中忽见洮河北」这种剧名会中招），还得像频道名：
    # 要么本身就是地名（河北 / 上海），要么以「台/频道/综合/新闻…」收尾
    if n in PLACES or n.replace(" ", "") in PLACES:
        return True
    if CHANNEL_SUFFIX.search(n):
        return True
    return False


def is_sat(name):
    """省级卫视：名字里必须出现「卫视」

    之前只要含省份名就算（「上海十三太保」这种电影名、「东南西北风」这种歌名全中招），
    现在只认明确写着「卫视」的。
    """
    n = name or ""
    if "卫视" not in n:
        return False
    if re.search(r"(TVBS|凤凰|翡翠|明珠|澳视|莲花|无线|NOW|HBO|CNN|BBC|"
                 r"东森|中天|三立|年代|人间|大爱|华藏|佛卫|生命|龙华|净空)", n, re.I):
        return False
    if len(n) > 16 or "《" in n or "》" in n:
        return False
    return True
# 名字里的清晰度线索
RE_RES = re.compile(r"(?i)(\d{3,4})\s*[pPiI]|(\d{3,4})P|(\d{3,4})·")

CHANNEL_CLEAN = re.compile(r"(?i)[\s\-_]*(cctv[\s\-_]?(\d{1,2}|4k|5\+)|卫视|高清|超清|蓝光|1080p?|720p?|4k|hd|fhd|ipv4|ipv6|线路\d*)[\s\-_]*")


# 画质/编码后缀，归一化时去掉（"云南卫视高清" 和 "云南卫视" 是同一个台）
QUALITY_SUFFIX = re.compile(
    r"(超高清|高清|超清|蓝光|标清|4K|8K|UHD|FHD|HD|2160P?|1080P?|720P?|"
    r"IPV[46]|HEVC|H265|H264|HDR)", re.I)

# 常见繁体写法（港澳台源的台名大量用繁体，不统一就分不成一个台）
TRAD = str.maketrans("廣東東南寧蘇浙閩贛魯豫鄂湘粵瓊川貴雲陝甘青遼吉黑蒙藏維寧夏臺灣灣"
                     "衛視電臺頻道新聞經濟影視綜藝紀錄戲劇娛樂體育少兒生活公共",
                     "广东东南宁苏浙闽赣鲁豫鄂湘粤琼川贵云陕甘青辽吉黑蒙藏维宁夏台湾湾"
                     "卫视电台频道新闻经济影视综艺纪录戏剧娱乐体育少儿生活公共")


def _base_name(n):
    """去掉画质后缀、频道/台尾巴、空格和符号，用来判「是不是同一个台」"""
    t = QUALITY_SUFFIX.sub("", n or "").translate(TRAD)
    t = re.sub(r"(电视台|频道|台)$", "", t.strip())
    return re.sub(r"[\s\-_·•|]+", "", t)


def norm_channel(name):
    """把「CCTV-1 综合 高清」「CCTV1高清」统一成 CCTV1，用来判断是不是同一个台"""
    n = (name or "").strip()
    if is_cctv(n):
        m = re.search(r"(?i)(cctv[\s\-_]?(\d{1,2}|4k|5\+)|cgtn)", n)
        if m:
            k = m.group(0).upper().replace(" ", "").replace("_", "").replace("-", "")
            if not k.startswith("CCTV"):
                return "CGTN"
            v = k[4:]
            if v.isdigit():
                return "CCTV" + str(int(v))      # CCTV01 / CCTV-01 → CCTV1
            return k
        return _base_name(n) or "CCTV"
    # 卫视 / 地方台也要归一化，否则「云南卫视」和「云南卫视高清」会算成两个台，
    # 每个台就只剩一条线路了。
    return _base_name(n) or n


def parse_playlist(text, base=""):
    """解析 m3u / 频道 txt，返回 [{name, url, group}]"""
    out = []
    text = text.replace("\r", "")
    if "#EXTM3U" in text.upper():
        cur = {}
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("#EXTINF"):
                nm = line.split(",")[-1].strip()
                g = re.search(r'group-title="([^"]*)"', line)
                cur = {"name": nm, "group": g.group(1) if g else ""}
            elif line.startswith("#"):
                continue
            elif cur.get("name"):
                out.append({"name": cur["name"], "url": line,
                            "group": cur.get("group", "")})
                cur = {}
        return out
    # txt：台名,url  /  #genre# 分组
    group = ""
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if "genre#" in line:                     # 央视,#genre# —— 分组行
            group = line.split(",")[0].replace("#genre#", "").strip()
            continue
        if line.startswith("#"):
            continue
        parts = line.split(",")
        if len(parts) < 2:
            continue
        nm, u = parts[0].strip(), parts[1].strip()
        if not u.lower().startswith(("http", "rtmp", "rtsp")):
            continue
        out.append({"name": nm, "url": u, "group": group})
    return out


def _fetch(url, timeout=8, limit=200_000):
    """抓直播源文件；GitHub 类地址失败时自动走镜像

    和 discover._fetch_config 一个道理：raw.githubusercontent 会「连上不吐数据」，
    不换镜像的话这条源永远解析不到（实测南风直播源就是这样）。
    """
    cands = [url]
    try:
        import discover as _D
        cands += _D.github_mirrors(url)
    except Exception:
        pass
    last = None
    for u in cands:
        try:
            req = urllib.request.Request(u, headers={"User-Agent": UA})
            t0 = time.time()
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                data = r.read(limit)
            return data, (time.time() - t0) * 1000
        except Exception as e:                     # noqa: BLE001
            last = e
    raise last or RuntimeError("fetch failed")


LOW_BPS = 200


def height_from_bps(bps):
    """按码率反推清晰度：很多单码率 m3u8 不写 RESOLUTION，但码率能说明问题"""
    if not bps:
        return 0
    # HLS 直播的经验值：1080p 一般 3~6Mbps，4K 要 15Mbps 以上。
    # 之前拿 5Mbps 就判 4K，一口气标出 145 个「4K」——比不标还糟。
    if bps >= 12000:
        return 2160
    if bps >= 4500:
        return 1080
    if bps >= 2000:
        return 720
    if bps >= 900:
        return 480
    return 0        # 低于这个码率（kbps）基本是黑屏/静帧，HD 频道正常都在 1500+


def _seg_probe(url, timeout=6):
    """取第一个分片：返回 (总大小, 是否拿得到)

    比 HEAD 靠谱得多：很多直播服务器不认 HEAD，还有的 Range 也不支持，
    干脆读前 64KB——拿到数据说明分片真能下，拿不到基本就是黑屏/内网地址。
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                   "Range": "bytes=0-65535"})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            data = r.read(65536)
            if not data:
                return 0, False
            cr = r.headers.get("Content-Range") or ""
            m = re.search(r"/(\d+)", cr)
            if m:
                return int(m.group(1)), True
            n = int(r.headers.get("Content-Length") or 0)
            return n or len(data), True
    except Exception:
        return 0, False


def _head_size(url, timeout=6):
    """拿分片大小：优先 HEAD 的 Content-Length，不行再用 Range 读 1 字节"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA}, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            n = int(r.headers.get("Content-Length") or 0)
            if n:
                return n
    except Exception:
        pass
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                   "Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
            cr = r.headers.get("Content-Range") or ""
            m = re.search(r"/(\d+)", cr)
            if m:
                return int(m.group(1))
            n = int(r.headers.get("Content-Length") or 0)
            return n if n > 1 else 0
    except Exception:
        return 0


def split_v6(chans):
    """把每台的线路按 IPv4 / IPv6 拆开

    IPv6 线路单独成组：有些电视只走 v4，混在一起反而会选了打不开的那条。
    """
    v4, v6 = {}, {}
    for k, arr in (chans or {}).items():
        a4 = [x for x in arr if not x.get("v6")]
        a6 = [x for x in arr if x.get("v6")]
        if a4:
            v4[k] = a4
        if a6:
            v6[k] = a6
    return v4, v6


def pick_lines(arr, n, drop_bad=True):
    """取前 n 条可播线路

    分片取不到的（内网地址/超时/防盗链）**直接不输出** —— 实测 CCTV1 的
    bfgd 那两条就是分片主机超时，电视上打开是黑屏。只有该台一条好的都没有时，
    才退回输出它们（总比整个台消失好）。
    """
    good = [v for v in arr if v.get("seg_ok") is not False] if drop_bad else arr
    use = good or arr
    return sorted(use, key=line_sort_key)[:max(1, n)]


def line_sort_key(v):
    """线路排序：分片取不到 / 码率过低的排最后，再看分辨率、延迟"""
    seg_bad = 1 if v.get("seg_ok") is False else 0
    bps = v.get("bps") or 0
    low = 1 if (bps and bps < LOW_BPS) else 0
    return (seg_bad, low, -(v.get("height") or 0), v.get("ms") or 999999)


def probe_bps(media_body, base_url, timeout=6):
    """从 media 列表估码率：第一个分片大小 / EXTINF 时长

    轻量做法，不解码：黑屏/静帧的分片压得极小，码率会明显低于正常频道；
    分片根本取不到（内网地址/防盗链）也直接算可疑。
    返回 (kbps, 字节数, 分片是否可取)
    """
    m = re.search(r"#EXTINF:([\d.]+)[^\n]*\n(?:#[^\n]*\n)*\s*([^\s#][^\n]*)",
                  media_body or "")
    if not m:
        return 0, 0, True
    try:
        dur = float(m.group(1))
    except Exception:
        dur = 0
    seg = _abs(m.group(2).strip(), base_url)
    if "$" in seg:                     # 带 $Number$ 模板的没法直接取
        return 0, 0, True
    size, got = _seg_probe(seg, timeout)
    if not got:
        # 分片拿不到：多半是内网地址/防盗链，客户端打开就是黑屏
        return 0, 0, False
    if not size or dur <= 0:
        return 0, size, True
    return int(size * 8 / dur / 1000), size, True


def test_stream(url, timeout=7):
    """测一条直播线路：有效？TTFB？清晰度？

    m3u8 主列表里有 RESOLUTION 就取最高码率那条再测一次；
    拿不到分辨率就看 URL / 内容里有没有 1080/720 的线索。
    """
    res = {"ok": False, "ms": 0, "height": 0, "h_src": "", "why": "",
           "bps": 0, "seg": 0, "seg_ok": True}
    try:
        data, ms = _fetch(url, timeout=timeout, limit=300_000)
    except Exception as e:
        res["why"] = type(e).__name__
        return res
    body = data.decode("utf-8", "replace")

    if "#EXTM3U" not in body.upper():
        res["ok"] = bool(body.strip())
        res["ms"] = int(ms)
        res["why"] = "" if res["ok"] else "空响应"
        return res

    # 主列表：挑分辨率最高的子列表
    variants = re.findall(r"#EXT-X-STREAM-INF:([^\n]*)\n([^\s#][^\n]*)", body)
    if variants:
        best, best_h, best_url = None, 0, ""
        for attrs, sub in variants:
            m = re.search(r"RESOLUTION=(\d+)x(\d+)", attrs)
            h = int(m.group(2)) if m else 0
            if h > best_h or best is None:
                best_h, best_url = h, sub.strip()
        sub_url = _abs(best_url, url)
        try:
            data2, ms2 = _fetch(sub_url, timeout=timeout, limit=120_000)
            body2 = data2.decode("utf-8", "replace")
            res["ok"] = "#EXTINF" in body2 or "#EXT-X-TARGETDURATION" in body2
            res["ms"] = int(ms + ms2)
            res["height"] = best_h
            res["h_src"] = "res" if best_h else ""
            if res["ok"] and not best_h:
                m2 = re.search(r"RESOLUTION=(\d+)x(\d+)", body2)
                res["height"] = int(m2.group(2)) if m2 else 0
                res["h_src"] = "res" if res["height"] else ""
            if res["ok"]:
                res["bps"], res["seg"], res["seg_ok"] = probe_bps(body2, sub_url, timeout)
                if not res["height"]:
                    res["height"] = height_from_bps(res["bps"])
                    res["h_src"] = "bps" if res["height"] else ""
            return res
        except Exception as e:
            res["why"] = type(e).__name__
            return res

    # 单码率列表：本身能读到就认为有效
    res["ok"] = "#EXTINF" in body or "#EXT-X-TARGETDURATION" in body
    res["ms"] = int(ms)
    if res["ok"]:
        res["bps"], res["seg"], res["seg_ok"] = probe_bps(body, url, timeout)
        if not res["height"]:
            res["height"] = height_from_bps(res["bps"])
            res["h_src"] = "bps" if res["height"] else ""
    m = re.search(r"RESOLUTION=(\d+)x(\d+)", body)
    if m:
        res["height"] = int(m.group(2))
        res["h_src"] = "res"
    if not res["ok"]:
        res["why"] = "不是合法播放列表"
    return res


def _abs(url, base):
    if url.startswith(("http://", "https://")):
        return url
    i = base.rfind("/")
    return base[:i + 1] + url.lstrip("./")


def _guess_height(name, url):
    for s in (name, url):
        m = re.search(r"(?i)(2160|4k|1080|720|576|480)", s or "")
        if m:
            v = m.group(1).lower()
            return {"4k": 2160, "2160": 2160, "1080": 1080,
                    "720": 720, "576": 576, "480": 480}[v]
    return 0


def expand_lives_stats(sources, max_sources=0, timeout=10, log=print):
    """展开全部直播源，并返回每个源的解析统计

    max_sources=0 表示不限制（以前默认 40，导致第 41 个之后的源永远是 0 频道）。
    返回 (channels, stats)，stats[url] = {n, cctv, sat, local, other, ms, err}
    """
    channels, seen, stats = [], set(), {}
    todo = sources if not max_sources else sources[:max_sources]
    for item in todo:
        u = (item.get("url") or "").strip()
        if not u:
            continue
        rec = {"n": 0, "cctv": 0, "sat": 0, "local": 0, "other": 0,
               "ms": 0, "err": "", "parsed_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        try:
            data, ms = _fetch(u, timeout=timeout, limit=3_000_000)
            rec["ms"] = int(ms)
            text = data.decode("utf-8", "replace")
        except Exception as e:                     # noqa: BLE001
            rec["err"] = type(e).__name__
            stats[u] = rec
            continue
        parsed = parse_playlist(text)
        if not parsed:
            rec["err"] = "没有可用频道"
            stats[u] = rec
            continue
        rec["n"] = len(parsed)
        for ch in parsed:
            nm = ch["name"]
            if is_cctv(nm):
                rec["cctv"] += 1
            elif is_sat(nm):
                rec["sat"] += 1
            elif is_local(nm):
                rec["local"] += 1
            else:
                rec["other"] += 1
            key = (norm_channel(nm), ch["url"])
            if key in seen:
                continue
            seen.add(key)
            ch["from"] = item.get("name") or item.get("from") or ""
            if not ch.get("group"):
                ch["group"] = item.get("group") or ""
            channels.append(ch)
        stats[u] = rec
    log("  直播源展开：%d 个地址 → %d 个频道" % (len(todo), len(channels)))
    return channels, stats


def expand_lives(sources, max_sources=0, timeout=10, log=print):
    """把「直播源地址」展开成「具体频道」

    data/lives.json 里存的是配置里的 lives 数组，通常是 m3u/txt 播放列表的地址，
    一个地址里可能有一两百个台。这里把它们全拉下来解析。
    """
    channels, seen = [], set()
    for item in (sources if not max_sources else sources[:max_sources]):
        u = (item.get("url") or "").strip()
        if not u:
            continue
        try:
            data, _ = _fetch(u, timeout=timeout, limit=3_000_000)
            text = data.decode("utf-8", "replace")
        except Exception:
            continue
        parsed = parse_playlist(text)
        if not parsed:
            continue
        for ch in parsed:
            key = (norm_channel(ch["name"]), ch["url"])
            if key in seen:
                continue
            seen.add(key)
            ch["from"] = item.get("name") or item.get("from") or ""
            if not ch.get("group"):
                ch["group"] = item.get("group") or ""
            channels.append(ch)
    log("  直播源展开：%d 个地址 → %d 个频道" % (min(len(sources), max_sources), len(channels)))
    return channels


def _save_source_stats(data_dir, stats, blocked=None, deleted=None):
    """把每个源的解析结果写回 lives.json（卡片要显示延迟/央视卫视含量/上次解析）"""
    import json as _json
    path = os.path.join(data_dir, "lives.json")
    try:
        with open(path, encoding="utf-8") as f:
            doc = _json.load(f)
    except Exception:
        return 0
    items = doc.get("items") or []
    n = 0
    for it in items:
        u = (it.get("url") or "").strip()
        rec = stats.get(u)
        if not rec:
            continue
        it.update({"channels_n": rec["n"], "cctv": rec["cctv"], "sat": rec["sat"],
                   "local": rec["local"], "other": rec["other"],
                   "parse_ms": rec["ms"], "parse_err": rec["err"],
                   "parsed_at": rec["parsed_at"]})
        n += 1
    doc["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump(doc, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
    except Exception:
        pass
    return n


def _jdump(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _jload(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


# ---------------------------------------------------------------- 优选 / 备选

def score_of(x):
    """源的价值：央视+卫视台数（地方台不算，因为不进主输出）"""
    return int(x.get("cctv") or 0) + int(x.get("sat") or 0)


def rank_sources(items, active_n=10):
    """按价值给每个源打档位：active(优选) / bench(备选) / blocked(屏蔽)

    名额不足就留空，不拿 0 频道的源凑数 —— 面板上会说明「备选源不满足优选条件」。
    """
    cand = [x for x in items if not x.get("blocked")]
    cand.sort(key=lambda x: (-score_of(x), str(x.get("name") or "")))
    act_ids = {id(x) for x in cand[:active_n]}
    active, bench = [], []
    for x in items:
        if x.get("blocked"):
            x["tier"] = "blocked"
        elif id(x) in act_ids:
            x["tier"] = "active"
            active.append(x)
        else:
            x["tier"] = "bench"
            bench.append(x)
    return active, bench


def apply_tiers(items, active_n=10, auto=True):
    """写回档位；auto=0 时只补空位，不替换已有优选"""
    if auto:
        return rank_sources(items, active_n)
    cur = [x for x in items if x.get("tier") == "active" and not x.get("blocked")]
    free = max(0, active_n - len(cur))
    if free:
        others = [x for x in items
                  if x.get("tier") != "active" and not x.get("blocked")]
        others.sort(key=lambda x: -score_of(x))
        for x in others[:free]:
            if score_of(x) > 0:
                x["tier"] = "active"
    for x in items:
        if x.get("blocked"):
            x["tier"] = "blocked"
        elif x.get("tier") not in ("active", "bench"):
            x["tier"] = "bench"
    return ([x for x in items if x.get("tier") == "active"],
            [x for x in items if x.get("tier") == "bench"])


# ---------------------------------------------------------------- 墓碑（屏蔽/删除）
#
# 屏蔽或删除之后要记住这件事，否则下一轮从配置里又解析到同一个地址，它就当新源回来了。
# 复检规则（用户定的）：每 live_tomb_days(15) 天试一次，
#   通了 → 解除，只进备选 + 自动解析一次
#   连续 live_tomb_max_fail(5) 次不通 → 真删除（记录也删掉）

def _tomb_path(data_dir):
    return os.path.join(data_dir, "live_tomb.json")


def load_tomb(data_dir):
    d = _jload(_tomb_path(data_dir), {})
    return d.get("urls") or {}


def save_tomb(data_dir, urls):
    _jdump(_tomb_path(data_dir), {"updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                                  "urls": urls})


def tomb_add(data_dir, url, name, kind="blocked"):
    url = (url or "").strip()
    if not url:
        return
    t = load_tomb(data_dir)
    old = t.get(url) or {}
    t[url] = {"name": name or url, "kind": kind, "at": time.time(),
              "checks": int(old.get("checks") or 0),
              # 刚屏蔽/删除时不要立刻复检，否则下一次调度（1 小时内）就自己解除了
              "last_check": float(old.get("last_check") or time.time())}
    save_tomb(data_dir, t)


def tomb_drop(data_dir, url):
    t = load_tomb(data_dir)
    if t.pop((url or "").strip(), None) is not None:
        save_tomb(data_dir, t)


def check_source(url, timeout=10):
    """复检一个直播源：能不能拉到、分片能不能取

    返回 (ok, why)
    """
    try:
        data, _ = _fetch(url, timeout=timeout, limit=500_000)
        body = data.decode("utf-8", "replace")
    except Exception as e:                          # noqa: BLE001
        return False, type(e).__name__
    if "#EXTM3U" in body.upper():
        _bps, _seg, seg_ok = probe_bps(body, url, timeout)
        return (True, "") if seg_ok else (False, "分片不可达")
    if re.search(r"^[^#\n]+,\s*https?://", body, re.M):
        return True, ""                             # txt 台单
    return False, "内容无法识别"


def tomb_check(data_dir, log=print, days=15, max_fail=5):
    """到期的墓碑做一次复检，返回需要「复活」的 [(url, name)]"""
    t = load_tomb(data_dir)
    if not t:
        return []
    now = time.time()
    revive, changed = [], False
    for url, rec in list(t.items()):
        if now - float(rec.get("last_check") or 0) < days * 86400:
            continue
        ok, why = check_source(url)
        rec["last_check"] = now
        changed = True
        if ok:
            revive.append((url, rec.get("name") or url))
            t.pop(url, None)
            log("  墓碑复检通过：%s（%s）→ 回备选" %
                ((rec.get("name") or url)[:24], rec.get("kind")))
        else:
            rec["checks"] = int(rec.get("checks") or 0) + 1
            if rec["checks"] >= max_fail:
                t.pop(url, None)
                log("  墓碑复检连续 %d 次不通，真删除：%s" %
                    (max_fail, (rec.get("name") or url)[:24]))
            else:
                log("  墓碑复检失败（%d/%d，%s）：%s" %
                    (rec["checks"], max_fail, why,
                     (rec.get("name") or url)[:24]))
    if changed:
        save_tomb(data_dir, t)
    return revive


def revive_source(data_dir, url, name=""):
    """复活一个源：进备选 + 标记待解析（按新源对待）"""
    doc = load_lives(data_dir)
    items = doc.get("items") or []
    url = (url or "").strip()
    hit = None
    for x in items:
        if (x.get("url") or "").strip() == url:
            hit = x
            break
    if hit is None:
        hit = {"name": name or url, "url": url, "ua": "", "epg": "", "from": ""}
        items.append(hit)
    hit.update({"tier": "bench", "blocked": 0, "needs_parse": 1,
                "revived_at": time.strftime("%Y-%m-%d %H:%M:%S")})
    doc["items"] = items
    save_lives(data_dir, doc)
    return hit


def parse_pending(data_dir, log=print):
    """把 needs_parse 的源各解析一次（复活的新源）"""
    doc = load_lives(data_dir)
    items = [x for x in (doc.get("items") or []) if x.get("needs_parse")]
    if not items:
        return 0
    stats = {}
    for it in items:
        _ch, st = expand_lives_stats([it], max_sources=1, timeout=15,
                                     log=lambda *a: None)
        stats.update(st)
        r = st.get(it.get("url")) or {}
        log("  新源解析：%s → %d 频道（央视 %d / 卫视 %d）%s"
            % ((it.get("name") or "")[:20], r.get("n", 0), r.get("cctv", 0),
               r.get("sat", 0), ("｜" + r["err"]) if r.get("err") else ""))
    _save_source_stats(data_dir, stats)
    doc2 = load_lives(data_dir)
    for x in doc2.get("items") or []:
        x.pop("needs_parse", None)
    save_lives(data_dir, doc2)
    return len(items)


def load_lives(data_dir):
    return _jload(os.path.join(data_dir, "lives.json"), {"items": []})


def save_lives(data_dir, doc):
    doc["updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _jdump(os.path.join(data_dir, "lives.json"), doc)


def bench_room(active, items, active_n=10):
    """优选还差几个名额 + 当前备选里最高的价值（面板用来解释为什么留空）"""
    best = max([score_of(x) for x in items
                if x.get("tier") == "bench" and not x.get("blocked")] or [0])
    return max(0, active_n - len(active)), best


# ---------------------------------------------------------------- 测活

def _test_channels(channels, data_dir, workers=12, timeout=7, max_test=1500,
                   log=print, use_cache=True):
    """从候选里采样测活，结果进 url 缓存，再按台归并成 doc

    用 url 缓存是为了「多次小聚合累积线路」：每一轮只测没测过 / 很久没测的，
    已经通过的线路继续留在结果里，卫视就不会永远只有 1 条。
    """
    cache_path = os.path.join(data_dir, "live_url_cache.json")
    cache = _jload(cache_path, {}) if use_cache else {}
    if not isinstance(cache, dict):
        cache = {}

    groups = {}
    for c in channels:
        key = norm_channel(c.get("name")) or c.get("name") or ""
        groups.setdefault(key, []).append(c)

    keep = {k: v for k, v in groups.items()
            if is_cctv(k) or is_sat(k) or is_hmt(k) or is_local(k)}

    def quota_for(k):
        if is_cctv(k):
            return 10
        if is_sat(k):
            return 8
        if is_hmt(k):
            return 6
        return 2

    def pick(arr, n):
        """同台候选按主机分散，优先挑没测过的"""
        from urllib.parse import urlsplit as _us
        def keyof(c):
            rec = cache.get(c.get("url"))
            if rec is None:
                return (0, 0)                    # 从没测过，最先测
            if "seg_ok" not in rec:
                return (1, rec.get("ts") or 0)   # 老缓存缺分片判定，补测一次
            return (3 if rec.get("ok") else 2, rec.get("ts") or 0)
        seen_host, out = set(), []
        for c in sorted(arr, key=keyof):
            h = (_us(c.get("url") or "").hostname or "").lower()
            if h and h in seen_host:
                continue
            if h:
                seen_host.add(h)
            out.append(c)
            if len(out) >= n:
                break
        return out or arr[:n]

    def prio(k):
        return 0 if is_cctv(k) else (1 if is_sat(k) else 2)

    picked = []
    for k, arr in sorted(keep.items(), key=lambda kv: prio(kv[0])):
        picked.extend(pick(arr, quota_for(k)))
    if len(picked) > max_test:
        picked = picked[:max_test]
    log("  频道 %d 条 → %d 个台（目标 %d 个），本轮测 %d 条（缓存已有 %d 条）"
        % (len(channels), len(groups), len(keep), len(picked), len(cache)))

    def _v6_literal(u):
        return "[" in (u or "")

    def one(item):
        r = test_stream(item["url"], timeout=timeout)
        name = item.get("name") or ""
        h = r.get("height") or _guess_height(name, item["url"])
        hsrc = r.get("h_src") or ("guess" if h else "")
        return {"name": name, "url": item["url"], "group": item.get("group") or "",
                "from": item.get("from") or "", "ok": r["ok"], "ms": r["ms"],
                "height": h, "h_src": hsrc,
                "bps": r.get("bps") or 0, "seg": r.get("seg") or 0,
                "seg_ok": r.get("seg_ok", True), "v6": _v6_literal(item["url"]),
                "why": r.get("why", "")}

    t0 = time.time()
    v6_ok = has_ipv6()
    _apply_family(socket.AF_INET)
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        tested = list(ex.map(one, picked))
    _apply_family(0)
    if v6_ok:
        bad = [i for i, r in enumerate(tested) if not r["ok"]]
        rescued = 0
        if bad:
            try:
                _apply_family(socket.AF_INET6)
                with cf.ThreadPoolExecutor(max_workers=workers) as ex:
                    retry = list(ex.map(lambda i: one(picked[i]), bad))
                for i, r in zip(bad, retry):
                    if r["ok"]:
                        r["v6"] = True          # 只有走 IPv6 才通 → 标记出来
                        tested[i] = r
                        rescued += 1
            except Exception:
                pass
            _apply_family(0)
            if rescued:
                log("  IPv6 补测：%d 条失败中救回 %d 条" % (len(bad), rescued))

    now = time.time()
    for r in tested:
        cache[r["url"]] = {"name": r["name"], "group": r["group"],
                           "from": r.get("from") or "", "ok": r["ok"],
                           "ms": r["ms"], "height": r["height"],
                           "bps": r.get("bps") or 0, "seg": r.get("seg") or 0,
                           "seg_ok": r.get("seg_ok", True),
                           "h_src": r.get("h_src") or "",
                           "v6": bool(r.get("v6")),
                           "ts": now, "why": r.get("why", "")}
    _jdump(cache_path, cache)

    # 结果从「缓存」里取：这样多轮采样能累积，不会每轮从零开始
    fresh = now - 7 * 86400
    by_ch = {}
    for url, rec in cache.items():
        if not rec.get("ok") or (rec.get("ts") or 0) < fresh:
            continue
        k = norm_channel(rec.get("name")) or rec.get("name") or ""
        if not (is_cctv(k) or is_sat(k) or is_hmt(k) or is_local(k)):
            continue
        _h = rec.get("height") or 0
        if rec.get("h_src") == "bps":          # 用当前阈值重算，避免旧缓存留错
            _h = height_from_bps(rec.get("bps") or 0)
        by_ch.setdefault(k, []).append({
            "name": rec.get("name"), "url": url, "group": rec.get("group") or "",
            "from": rec.get("from") or "", "ok": True, "ms": rec.get("ms") or 0,
            "height": _h, "bps": rec.get("bps") or 0,
            "seg_ok": rec.get("seg_ok", True), "v6": bool(rec.get("v6")),
            "why": ""})
    for arr in by_ch.values():
        arr.sort(key=line_sort_key)
        for i, v in enumerate(arr):
            v["rank"] = i + 1

    cctv, sat, local, hmt, other = {}, {}, {}, {}, {}
    for k, v in by_ch.items():
        if is_cctv(k):
            cctv[k] = v
        elif is_sat(k):
            sat[k] = v
        elif is_hmt(k):
            hmt[k] = v            # 港澳台：单独统计，输出时并进卫视组
        elif is_local(k):
            local[k] = v
        else:
            other[k] = v

    doc = {"ts": time.time(), "cost": round(time.time() - t0, 1),
           "total": len(cache), "ok": sum(1 for v in cache.values() if v.get("ok")),
           "tested_now": len(tested),
           "cctv": cctv, "sat": sat, "hmt": hmt, "local": local, "other": other,
           "items": tested}
    _jdump(os.path.join(data_dir, "live_tested.json"), doc)
    log("  直播体检：本轮测 %d 条 → 缓存累计有效 %d｜央视 %d｜卫视 %d｜港澳台 %d｜地方 %d（%.1fs）"
        % (len(tested), doc["ok"], len(cctv), len(sat), len(hmt), len(local),
           doc["cost"]))
    return doc


def rebuild_from_cache(data_dir, log=print):
    """只按当前分类规则重建 doc，不重新测活

    改了 is_cctv / is_sat / is_hmt 这类判定后，缓存里的结果要重新归类，
    但没必要再测一遍——直接重算分组就行。
    """
    cache = _jload(os.path.join(data_dir, "live_url_cache.json"), {})
    doc = _jload(os.path.join(data_dir, "live_tested.json"), {})
    tested = doc.get("items") or []
    fresh = time.time() - 7 * 86400
    by_ch = {}
    for url, rec in cache.items():
        if not rec.get("ok") or (rec.get("ts") or 0) < fresh:
            continue
        k = norm_channel(rec.get("name")) or rec.get("name") or ""
        if not (is_cctv(k) or is_sat(k) or is_hmt(k) or is_local(k)):
            continue
        _h = rec.get("height") or 0
        if rec.get("h_src") == "bps":          # 用当前阈值重算，避免旧缓存留错
            _h = height_from_bps(rec.get("bps") or 0)
        by_ch.setdefault(k, []).append({
            "name": rec.get("name"), "url": url, "group": rec.get("group") or "",
            "from": rec.get("from") or "", "ok": True, "ms": rec.get("ms") or 0,
            "height": _h, "bps": rec.get("bps") or 0,
            "seg_ok": rec.get("seg_ok", True), "v6": bool(rec.get("v6")),
            "why": ""})
    for arr in by_ch.values():
        arr.sort(key=line_sort_key)
        for i, v in enumerate(arr):
            v["rank"] = i + 1
    cctv, sat, local, hmt, other = {}, {}, {}, {}, {}
    for k, v in by_ch.items():
        if is_cctv(k):
            cctv[k] = v
        elif is_sat(k):
            sat[k] = v
        elif is_hmt(k):
            hmt[k] = v
        elif is_local(k):
            local[k] = v
        else:
            other[k] = v
    doc.update({"ts": time.time(), "cctv": cctv, "sat": sat, "hmt": hmt,
                "local": local, "other": other,
                "total": len(cache),
                "ok": sum(1 for v in cache.values() if v.get("ok"))})
    _jdump(os.path.join(data_dir, "live_tested.json"), doc)
    log("  按新规则重建：央视 %d ｜ 卫视 %d ｜ 港澳台 %d ｜ 地方 %d"
        % (len(cctv), len(sat), len(hmt), len(local)))
    return doc


def save_channels(data_dir, channels):
    _jdump(os.path.join(data_dir, "live_channels.json"),
           {"ts": time.time(), "channels": channels})
    return len(channels)


def load_channels(data_dir):
    d = _jload(os.path.join(data_dir, "live_channels.json"), {})
    return d.get("channels") or []


def big_refresh(data_dir, items, active_n=10, workers=12, timeout=7,
                max_test=1500, log=print):
    """大聚合：把优选源的 TXT/M3U 全展开，存频道清单，再测一轮"""
    active, bench = rank_sources(items, active_n)
    log("  大聚合：优选 %d 个源展开（备选 %d 个不解析）" % (len(active), len(bench)))
    ch, stats = expand_lives_stats(active, max_sources=0, timeout=15, log=log)
    _save_source_stats(data_dir, stats)
    save_channels(data_dir, ch)
    return _test_channels(ch, data_dir, workers=workers, timeout=timeout,
                          max_test=max_test, log=log)


def small_test(data_dir, workers=12, timeout=7, max_test=1500, log=print):
    """小聚合：用上次大聚合存下的频道清单，只做线路测活与优选"""
    ch = load_channels(data_dir)
    if not ch:
        log("  还没有频道清单，先跑大聚合")
        return {}
    log("  小聚合：用已存频道清单 %d 条" % len(ch))
    return _test_channels(ch, data_dir, workers=workers, timeout=timeout,
                          max_test=max_test, log=log)


def screen_bench(data_dir, items, log=print):
    """备选源初筛：只解析频道数（不测活），用来判断值不值得进优选"""
    bench = [x for x in items if x.get("tier") == "bench" and not x.get("blocked")]
    if not bench:
        return {}
    log("  备选初筛：%d 个源（只解析，不测活）" % len(bench))
    ch, stats = expand_lives_stats(bench, max_sources=0, timeout=15,
                                   log=lambda *a: None)
    _save_source_stats(data_dir, stats)
    return stats


def build_index(lives, data_dir, workers=12, timeout=7, log=print, force=False,
                max_test=1500):
    """兼容入口：展开 + 测活一次跑完（live_api 用）"""
    ch, stats = expand_lives_stats(lives, max_sources=0, log=log)
    _save_source_stats(data_dir, stats)
    save_channels(data_dir, ch)
    return _test_channels(ch, data_dir, workers=workers, timeout=timeout,
                          max_test=max_test, log=log)


def channel_sort_key(name, lines=None):
    """台名排序：央视按号码，卫视按线路数，地方台按名字

    以前直接 sorted() 按字符排，CCTV1 后面跟的是 CCTV10（字典序），
    用户看到的顺序是 1,10,11,12,...,2,3 —— 显然不对。
    """
    n = _strip_quality(name or "")
    if is_cctv(n):
        m = re.search(r"(?i)cctv[\s\-_]*(\d{1,2})(\+|plus)?", n)
        if m:
            num = int(m.group(1))
            sub = 0.5 if m.group(2) else 0      # CCTV5+ 排在 CCTV5 之后
            return (0, num + sub, "")
        if "cgtn" in n.lower():
            return (0, 20, "")
        return (1, 0, n)                        # 第一剧场/风云足球 这类排最后
    if (lines or 0) > 0:
        return (2, -lines, n)
    return (2, 0, n)


def _ordered(chans, kind="cctv"):
    def key(ch):
        if kind == "cctv":
            return channel_sort_key(ch)
        if kind == "sat":
            return channel_sort_key(ch, len(chans[ch]))
        return ch
    return sorted(chans, key=key)


def to_m3u(doc, group_filter=None, lines_per=4, drop_bad=True,
           local_lines=None):
    """输出 m3u：每台取质量最好的若干条线路

    group_filter: "main" = 央视+卫视（主接口）｜"local" = 仅地方台
                  None/"all" = 全量（央视+卫视+港澳台+地方台，副接口用这个）
    local_lines: 地方台单独给几条（不给就和 lines_per 一样）
    """
    lines = ["#EXTM3U"]
    groups = []
    if group_filter in (None, "main"):
        _cctv4, _cctv6 = split_v6(doc.get("cctv") or {})
        _sat = dict(doc.get("sat") or {})
        _sat.update(doc.get("hmt") or {})       # 港澳台能测通就并进卫视
        _sat4, _sat6 = split_v6(_sat)
        groups.append(("央视", _cctv4, "cctv"))
        groups.append(("卫视", _sat4, "sat"))
        _v6 = dict(_cctv6)
        _v6.update(_sat6)
        if _v6:
            groups.append(("IPV6央视", _v6, "cctv"))
    if group_filter in (None, "local", "other"):
        groups.append(("地方台", doc.get("local") or {}, "local"))
    for gname, chans, kind in groups:
        lp = local_lines if (kind == "local" and local_lines) else lines_per
        for ch in _ordered(chans, kind):
            for v in pick_lines(chans[ch], lp, drop_bad):
                h = v["height"] or 0
                tag = ("4K" if h >= 2160 else "1080P" if h >= 1080 else
                       "720P" if h >= 720 else "高清" if h >= 480 else "")
                low = v.get("bps") and v["bps"] < LOW_BPS
                bad = v.get("seg_ok") is False
                mark = " · 分片不可达" if bad else (" · 低码率" if low else "")
                label = "%s · %s%s · %dms" % (ch, tag or "直播", mark, v["ms"])
                lines.append('#EXTINF:-1 tvg-name="%s" group-title="%s",%s'
                             % (ch, gname, label))
                lines.append(v["url"])
    return "\n".join(lines) + "\n"


def to_tvbox_lives(doc, group_filter=None, lines_per=4, drop_bad=True):
    """TVBox 的 lives 结构：{"lives":[{"name":"台","urls":[{"name":"线路n","url":...}]}]}"""
    groups = []
    if group_filter in (None, "main"):
        _c4, _c6 = split_v6(doc.get("cctv") or {})
        _s2 = dict(doc.get("sat") or {})
        _s2.update(doc.get("hmt") or {})
        _s4, _s6 = split_v6(_s2)
        groups.append((_c4, "cctv"))
        groups.append((_s4, "sat"))
        _m6 = dict(_c6)
        _m6.update(_s6)
        if _m6:
            groups.append((_m6, "sat"))
    if group_filter in (None, "local", "other"):
        groups.append((doc.get("local") or {}, "local"))
    out = []
    for chans, kind in groups:
        for ch in _ordered(chans, kind):
            urls = []
            for v in pick_lines(chans[ch], lines_per, drop_bad):
                urls.append({"name": "%s %s %dms" % (
                    v["height"] or "live", "rank%d" % v["rank"], v["ms"]),
                    "url": v["url"]})
            out.append({"name": ch, "urls": urls})
    return {"lives": out, "name": "聚合直播"}
