# -*- coding: utf-8 -*-
"""核心纯函数的回归测试（标准库 unittest，不依赖 pytest）

这些函数都踩过坑：
  * balance_configs 连丢两次数据（下沉忘放回、提升忘加回）
  * 频道名归一化缺失 → 每个台只剩一条线路
  * 配置带 BOM → 整份配置被判空
  * load_settings 无脑 int() → 加字符串设置后全部回落默认

跑法：
    cd agg && python -m unittest discover -s tests -v
"""

import base64
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aggregate as A          # noqa: E402
import discover as D           # noqa: E402
import jargate as JG           # noqa: E402
import live as L               # noqa: E402
import tvboxcfg as T           # noqa: E402


class TestBalanceConfigs(unittest.TestCase):
    """线路分层：无论如何都不能丢条目（丢过两次，59→9）"""

    def _mk(self, n, tiers):
        return [{"url": "u%d" % i, "tier": t} for i, t in enumerate(tiers[:n])]

    def _run(self, entries, limit, promote=1):
        out, promoted, demoted = A.balance_configs(
            entries, {}, {"max_configs": limit, "auto_promote": promote},
            log=lambda *a: None)
        return out, promoted, demoted

    def test_count_conserved_when_all_active(self):
        e = self._mk(9, ["active"] * 9)
        out, _, _ = self._run(e, 3)
        self.assertEqual(len(out), 9)

    def test_count_conserved_when_all_bench(self):
        # 就是那次 59 条掉到 9 条的场景：全部在备选，全部被提升
        e = self._mk(9, ["bench"] * 9)
        out, promoted, _ = self._run(e, 5)
        self.assertEqual(len(out), 9)
        self.assertEqual(len(promoted), 5)

    def test_count_conserved_when_mixed(self):
        e = self._mk(9, ["active", "bench"] * 4 + ["active"])
        out, _, _ = self._run(e, 2)
        self.assertEqual(len(out), 9)

    def test_active_never_exceeds_limit(self):
        e = self._mk(10, ["active"] * 10)
        out, _, _ = self._run(e, 4)
        self.assertEqual(len([x for x in out if x["tier"] == "active"]), 4)

    def test_promote_fills_to_limit(self):
        e = self._mk(6, ["active"] + ["bench"] * 5)
        out, promoted, _ = self._run(e, 4)
        self.assertEqual(len([x for x in out if x["tier"] == "active"]), 4)
        self.assertEqual(len(promoted), 3)

    def test_promote_disabled_keeps_bench(self):
        e = self._mk(6, ["active"] + ["bench"] * 5)
        out, promoted, _ = self._run(e, 4, promote=0)
        self.assertEqual(promoted, [])
        self.assertEqual(len([x for x in out if x["tier"] == "active"]), 1)

    def test_tier_field_actually_mutated(self):
        e = [{"url": "a", "tier": "active"}, {"url": "b", "tier": "active"}]
        self._run(e, 1)
        self.assertEqual(sum(1 for x in e if x["tier"] == "bench"), 1)


class TestChannelNorm(unittest.TestCase):
    """频道名归一化：不同画质/繁简必须归到同一个台"""

    def test_quality_suffix_merged(self):
        for name in ("云南卫视", "云南卫视高清", "云南卫视HD", "云南卫视 4K"):
            self.assertEqual(L.norm_channel(name), "云南卫视", name)

    def test_cctv_merged(self):
        for name in ("CCTV1", "CCTV-1", "CCTV1高清", "CCTV-01"):
            self.assertEqual(L.norm_channel(name), "CCTV1", name)

    def test_traditional_merged(self):
        self.assertEqual(L.norm_channel("廣東衛視"), L.norm_channel("广东卫视"))

    def test_channel_suffix_stripped(self):
        self.assertEqual(L.norm_channel("吉林影视频道"), L.norm_channel("吉林影视"))

    def test_cctv_whitelist(self):
        for name in ("CCTV1", "CCTV5+", "CCTV4K", "CCTV17", "CGTN", "央视台球"):
            self.assertTrue(L.is_cctv(name), name)

    def test_fake_cctv_rejected(self):
        for name in ("CCTV95", "CCTV20", "CCTV20241", "CCTV9576"):
            self.assertFalse(L.is_cctv(name), name)

    def test_sat_requires_weishi(self):
        self.assertTrue(L.is_sat("湖南卫视"))
        self.assertTrue(L.is_sat("北京卫视高清"))
        # 电影名里带省份，不是卫视
        self.assertFalse(L.is_sat("上海十三太保"))
        self.assertFalse(L.is_sat("DJ劲爆舞曲《东南西北风》"))

    def test_overseas_rejected(self):
        for name in ("TVBS新闻", "凤凰卫视", "翡翠台", "东森美洲卫视", "人间卫视"):
            self.assertFalse(L.is_sat(name), name)
            self.assertFalse(L.is_local(name), name)

    def test_local_channel(self):
        for name in ("上海新闻综合", "吉林影视频道", "北京卡酷少儿", "河北", "河北4K"):
            self.assertTrue(L.is_local(name), name)

    def test_movie_name_not_local(self):
        self.assertFalse(L.is_local("袖中忽见洮河北"))
        self.assertFalse(L.is_local("追剧大湾区"))


class TestDecode(unittest.TestCase):
    """配置解码：明文 / BOM / base64 三种形态"""

    def test_plain_json(self):
        txt, key, msg = T.decode(b'{"sites": []}')
        self.assertIn("明文", msg)
        self.assertEqual(json.loads(txt)["sites"], [])

    def test_json_with_bom(self):
        # szyyds 那种：带 UTF-8 BOM，str.strip() 不认，曾经被判成非 JSON
        txt, _, msg = T.decode(b'\xef\xbb\xbf{"sites": [1]}')
        self.assertEqual(json.loads(txt)["sites"], [1])

    def test_base64_form(self):
        payload = json.dumps({"sites": [1, 2]}).encode()
        raw = b"SWKYkz1W**" + base64.b64encode(payload)
        txt, _, msg = T.decode(raw)
        self.assertIn("base64", msg)
        self.assertEqual(json.loads(txt)["sites"], [1, 2])

    def test_marker_form_reports_error(self):
        # 有 $#..#$ 标记但内容不是合法密文：应该报错而不是抛奇怪的异常
        with self.assertRaises(Exception):
            T.decode(b"$#abc#$00")

    def test_garbage_reports_error(self):
        with self.assertRaises(Exception):
            T.decode(b"<html>not a config</html>")


class TestSettings(unittest.TestCase):
    """设置读写：字符串型和新键不能把整份设置读挂"""

    def test_string_and_int_mixed(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "settings.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"interval_mode": "daily", "daily_at": "03:30",
                       "interval_hours": 12, "max_configs": 7}, f)
        s = A.load_settings(p)
        self.assertEqual(s["interval_mode"], "daily")
        self.assertEqual(s["daily_at"], "03:30")
        self.assertEqual(s["interval_hours"], 12)
        self.assertEqual(s["max_configs"], 7)

    def test_bad_value_falls_back(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "settings.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"max_configs": "not-a-number"}, f)
        s = A.load_settings(p)
        self.assertIsInstance(s["max_configs"], int)

    def test_defaults_contain_new_keys(self):
        s = A.load_settings(os.path.join(tempfile.mkdtemp(), "none.json"))
        for k in ("interval_mode", "daily_at", "live_lines_per", "embed_lives",
                  "max_configs", "live_max_test"):
            self.assertIn(k, s)


class TestCatNames(unittest.TestCase):
    def test_limit(self):
        row = {"classes": [{"type_name": "分类%d" % i} for i in range(40)]}
        self.assertEqual(len(A._cat_names(row, 12)), 12)

    def test_dedup_and_blank(self):
        row = {"classes": [{"type_name": "电影"}, {"type_name": "电影"},
                           {"type_name": ""}, {"type_name": "剧集"}]}
        self.assertEqual(A._cat_names(row, 12), ["电影", "剧集"])


class TestPan(unittest.TestCase):
    def test_pan_detected(self):
        for name, api in (("123🔹盘4k", "csp_Mogg"), ("☁️易搜┃4K搜", "csp_YiSo"),
                          ("某网盘", "csp_AList"), ("小雅", "csp_PanWebShare")):
            self.assertTrue(A.is_pan({"name": name, "api": api}), name)

    def test_normal_site_not_pan(self):
        # csp_WoGG 是玩偶（网盘聚合），已归网盘；这里用真正的内容站验证不误判
        for name, api in (("猫眼┃资源", "http://x/api.php"), ("多多影音", "csp_DouDou")):
            self.assertFalse(A.is_pan({"name": name, "api": api}), name)

    def test_4k_pan_family(self):
        # 这些是用户反馈"应该是网盘"的那批，spider 已加入 PAN_SPIDERS
        for name, api in (("卡卡 • 4K", "csp_KKPan"), ("玩偶 • 4K", "csp_Wogg"),
                          ("弹幕 | 4KHDR", "csp_FourKHDR"), ("仓鼠｜APP", "csp_AppGet2")):
            self.assertTrue(A.is_pan({"name": name, "api": api}), name)


class TestPlaylistParse(unittest.TestCase):
    def test_m3u(self):
        text = ('#EXTM3U\n#EXTINF:-1 tvg-name="CCTV1" group-title="央视",CCTV1 高清\n'
                'http://a/1.m3u8\n#EXTINF:-1,CCTV2\nhttp://a/2.m3u8\n')
        got = L.parse_playlist(text)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0]["name"], "CCTV1 高清")
        self.assertEqual(got[0]["group"], "央视")
        self.assertEqual(got[1]["url"], "http://a/2.m3u8")

    def test_txt(self):
        text = "央视,#genre#\nCCTV1,http://a/1.m3u8\nCCTV2,http://a/2.m3u8\n"
        got = L.parse_playlist(text)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0]["group"], "央视")


class TestM3uOutput(unittest.TestCase):
    def _doc(self):
        return {"cctv": {"CCTV1": [{"url": "http://a", "height": 1080, "ms": 100, "rank": 1},
                                   {"url": "http://b", "height": 720, "ms": 200, "rank": 2},
                                   {"url": "http://c", "height": 0, "ms": 300, "rank": 3}]},
                "sat": {"湖南卫视": [{"url": "http://d", "height": 0, "ms": 50, "rank": 1}]},
                "local": {"上海新闻": [{"url": "http://e", "height": 0, "ms": 60, "rank": 1}]}}

    def test_single_header(self):
        out = L.to_m3u(self._doc(), "main")
        self.assertEqual(out.count("#EXTM3U"), 1)

    def test_main_only_cctv_and_sat(self):
        out = L.to_m3u(self._doc(), "main")
        self.assertIn("CCTV1", out)
        self.assertIn("湖南卫视", out)
        self.assertNotIn("上海新闻", out)

    def test_local_only(self):
        out = L.to_m3u(self._doc(), "local")
        self.assertIn("上海新闻", out)
        self.assertNotIn("CCTV1", out)

    def test_lines_per_channel(self):
        out = L.to_m3u(self._doc(), "main", lines_per=1)
        # 主接口 3 台（1 央视 + 1 卫视… 实际 cctv 1 台 + sat 1 台），每台 1 条线路
        self.assertEqual(len([x for x in out.split("\n") if x.startswith("http")]), 2)
        self.assertEqual(out.count("#EXTINF"), 2)

    def test_tvbox_lives_shape(self):
        d = L.to_tvbox_lives(self._doc(), "main")
        self.assertIn("lives", d)
        self.assertEqual(len(d["lives"]), 2)
        self.assertIn("urls", d["lives"][0])


class TestScheduler(unittest.TestCase):
    """聚合间隔：两种模式 + 0 表示关闭"""

    def test_hours_mode(self):
        import time
        now = time.time()
        nxt = A_next_run({"interval_mode": "hours", "interval_hours": 6}, now)
        self.assertAlmostEqual(nxt - now, 6 * 3600, delta=5)

    def test_daily_mode(self):
        import time
        nxt = A_next_run({"interval_mode": "daily", "daily_at": "03:30"}, time.time())
        lt = time.localtime(nxt)
        self.assertEqual((lt.tm_hour, lt.tm_min), (3, 30))
        self.assertGreater(nxt, time.time())

    def test_zero_disables(self):
        import time
        self.assertEqual(A_next_run({"interval_mode": "hours",
                                     "interval_hours": 0}, time.time()), 0)


class TestSiteReason(unittest.TestCase):
    """一期：配置层三类判定（对齐 ElderTV SiteFilter）"""

    def test_local_address(self):
        for api, ext in (("http://127.0.0.1:9978/x", None),
                         ("http://localhost/api", None),
                         ("http://x/api", "http://10.0.0.2/token.txt"),
                         ("http://x/api", "http://10.0.0.2/t")):
            self.assertEqual(A.site_reason({"name": "站", "api": api, "ext": ext}),
                             A.REASON_LOCAL, api)

    def test_credential_ext(self):
        self.assertEqual(
            A.site_reason({"name": "站", "api": "csp_X",
                           "ext": {"cookie": "abc"}}), A.REASON_PAN)
        self.assertEqual(
            A.site_reason({"name": "站", "api": "csp_X",
                           "ext": "http://x/token.txt"}), A.REASON_PAN)

    def test_notice_name(self):
        for n in ("关注微信公众号：xx", "🌈公众号【宝盒】", "加群防失联"):
            self.assertEqual(A.site_reason({"name": n, "api": "http://x/api"}),
                             A.REASON_NOTICE, n)

    def test_normal_site_passes(self):
        for n in ("猫眼┃资源", "弹幕影院", "公告影院"):
            self.assertIsNone(A.site_reason({"name": n, "api": "http://x/api"}), n)

    def test_invalid(self):
        self.assertEqual(A.site_reason({"name": "x", "api": ""}), A.REASON_INVALID)


class TestLoadsLoose(unittest.TestCase):
    """带 // 注释的配置（TVBox 允许，标准 JSON 不允许）"""

    def test_comments_with_urls(self):
        # URL 里的 // 不能被当成注释剁掉，否则整份配置变非法 JSON
        text = '{"sites":[{"api":"http://a.com/api.php/provide/vod"}, // 说明\n                        {"api":"https://b.com/x"}]}'
        d = A.loads_loose(text)
        self.assertIsNotNone(d)
        self.assertEqual(d["sites"][0]["api"], "http://a.com/api.php/provide/vod")
        self.assertEqual(len(d["sites"]), 2)

    def test_block_comment_and_trailing_comma(self):
        text = '{"a":[1,2,], /* 块注释 */ "b":"http://c/d",}'
        d = A.loads_loose(text)
        self.assertEqual(d["a"], [1, 2])
        self.assertEqual(d["b"], "http://c/d")

    def test_plain_json_untouched(self):
        text = '{"a":"http://x//y"}'
        self.assertEqual(A.loads_loose(text)["a"], "http://x//y")


class TestMirrors(unittest.TestCase):
    """GitHub 镜像模板：可配、可回落默认"""

    def tearDown(self):
        import discover as DD
        DD.set_mirrors([])

    def test_default_order(self):
        import discover as DD
        DD.set_mirrors([])
        self.assertIn("gh-proxy", DD.MIRRORS[0])

    def test_url_placeholder(self):
        import discover as DD
        DD.set_mirrors(["https://m.example/{url}"])
        got = DD.github_mirrors("https://raw.githubusercontent.com/a/b/main/c.json")
        self.assertEqual(got, ["https://m.example/https://raw.githubusercontent.com/a/b/main/c.json"])

    def test_jsdelivr_style(self):
        import discover as DD
        DD.set_mirrors(["https://cdn.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}"])
        got = DD.github_mirrors("https://raw.githubusercontent.com/guot55/yg/refs/heads/main/x.json")
        self.assertEqual(got, ["https://cdn.jsdelivr.net/gh/guot55/yg@main/x.json"])

    def test_user_template_skipped_for_non_github(self):
        import discover as DD
        DD.set_mirrors(["https://cdn.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}"])
        self.assertEqual(DD.github_mirrors("https://example.com/a.json"), [])

    def test_empty_falls_back(self):
        import discover as DD
        self.assertEqual(len(DD.set_mirrors([])), 3)


class TestReportInvariants(unittest.TestCase):
    """报表三个加法必须自洽

    seeds = probed + queued
    probed = merged + deduped
    merged = alive + grace + dead
    之前就是因为少写「去重」这一步，面板上 35/23/2 永远对不上。
    """

    def _row(self, name, alive, grace=False):
        return {"name": name, "api": "http://x/" + name, "alive": alive,
                "grace": grace, "msg": "", "type": 1, "items": 1,
                "playable": 1, "total": 1, "fail_streak": 0,
                "latency": 100, "searchable": 1, "key": name,
                "sample": "", "classes": [], "partial": False,
                "class_ids": [], "param": None, "skipped": False}

    def test_counts_add_up(self):
        import json as J
        import tempfile
        rows = [self._row("a", True), self._row("b", False, True),
                self._row("c", False), self._row("d", True)]
        kept = rows[:3]                     # 4 条体检 → 去重后 3
        tv = {"sites": [{"key": "k", "name": "n", "type": 1}]}
        ltv = {"sources": []}
        d = tempfile.mkdtemp()
        rep = A.write_report(d, rows, kept, kept, tv, ltv, 1.0, 3,
                             bench=[], probed=4, skipped=0)
        self.assertEqual(rep["seeds"], rep["probed"] + rep["queued"])
        self.assertEqual(rep["probed"], rep["merged"] + rep["deduped"])
        self.assertEqual(rep["merged"],
                         rep["alive"] + rep["grace"] + rep["dead"])
        self.assertEqual(rep["deduped"], 1)


class TestLiveSort(unittest.TestCase):
    """央视按号码排，不能按字典序（CCTV1 后面不该是 CCTV10）"""

    def test_cctv_numeric_order(self):
        import live as L
        names = ["CCTV10", "CCTV1", "CCTV17", "CCTV2", "CCTV5", "CCTV5+"]
        got = sorted(names, key=L.channel_sort_key)
        self.assertEqual(got, ["CCTV1", "CCTV2", "CCTV5", "CCTV5+",
                               "CCTV10", "CCTV17"])

    def test_cctv_specials_last(self):
        import live as L
        names = ["CCTV第一剧场", "CCTV1", "CGTN", "CCTV风云足球"]
        got = sorted(names, key=L.channel_sort_key)
        self.assertEqual(got[0], "CCTV1")
        self.assertEqual(got[1], "CGTN")
        self.assertIn(got[-1], ("CCTV第一剧场", "CCTV风云足球"))

    def test_sat_by_line_count(self):
        import live as L
        self.assertLess(L.channel_sort_key("湖南卫视", 5),
                        L.channel_sort_key("浙江卫视", 1))


class TestLiveClassify(unittest.TestCase):
    """港澳台 / 地方台不要漏，也不要误吞央视卫视"""

    def test_hmt(self):
        import live as L
        for n in ("凤凰中文", "TVB翡翠台", "东森新闻", "澳视澳门"):
            self.assertTrue(L.is_hmt(n), n)
        self.assertFalse(L.is_hmt("CCTV1"))
        self.assertFalse(L.is_hmt("湖南卫视"))

    def test_local(self):
        import live as L
        for n in ("广东珠江", "深圳都市", "北京纪实"):
            self.assertTrue(L.is_local(n), n)
        self.assertFalse(L.is_local("CCTV5"))
        self.assertFalse(L.is_local("浙江卫视"))


class TestLiveTomb(unittest.TestCase):
    """屏蔽/删除要留记录，否则下一轮又从配置里冒出来"""

    def setUp(self):
        import tempfile
        self.d = tempfile.mkdtemp()

    def test_block_then_merge_skips(self):
        import live as L
        L.tomb_add(self.d, "http://x/a.txt", "某源", "blocked")
        t = L.load_tomb(self.d)
        self.assertEqual(t["http://x/a.txt"]["kind"], "blocked")

    def test_delete_then_merge_readds_as_bench(self):
        import aggregate as A
        import live as L
        L.tomb_add(self.d, "http://x/b.txt", "旧源", "deleted")
        A._merge_lives(self.d, [{"name": "旧源", "url": "http://x/b.txt"}],
                       "cfg", log=lambda *a: None)
        doc = json.load(open(self.d + "/lives.json", encoding="utf-8"))
        it = [x for x in doc["items"] if x["url"] == "http://x/b.txt"][0]
        self.assertEqual(it.get("tier"), "bench")
        self.assertTrue(it.get("needs_parse"))
        self.assertNotIn("http://x/b.txt", L.load_tomb(self.d))

    def test_blocked_merge_ignored(self):
        import aggregate as A
        import live as L
        L.tomb_add(self.d, "http://x/c.txt", "坏源", "blocked")
        A._merge_lives(self.d, [{"name": "坏源", "url": "http://x/c.txt"}],
                       "cfg", log=lambda *a: None)
        import os as _os
        fp = self.d + "/lives.json"
        items = (json.load(open(fp, encoding="utf-8")).get("items") or []) \
            if _os.path.exists(fp) else []
        self.assertEqual([x for x in items if x["url"] == "http://x/c.txt"], [])

    def test_max_fail_removes_record(self):
        import live as L
        L.tomb_add(self.d, "http://x/d.txt", "死源", "deleted")
        t = L.load_tomb(self.d)
        t["http://x/d.txt"]["checks"] = 4
        t["http://x/d.txt"]["last_check"] = 0
        L.save_tomb(self.d, t)
        # 打桩：check_source 永远失败
        old = L.check_source
        L.check_source = lambda *a, **k: (False, "timeout")
        try:
            L.tomb_check(self.d, log=lambda *a: None, days=15, max_fail=5)
            self.assertNotIn("http://x/d.txt", L.load_tomb(self.d))
        finally:
            L.check_source = old


class TestOrphan(unittest.TestCase):
    """离源：配置接口删了，它带进来的站点要留下来继续用"""

    def setUp(self):
        import tempfile
        self.d = tempfile.mkdtemp()

    def _write(self, name, obj):
        import json as J
        import os as O
        with open(O.path.join(self.d, name), "w", encoding="utf-8") as f:
            J.dump(obj, f, ensure_ascii=False)

    def test_orphan_marks_and_passes_filter(self):
        import aggregate as A
        self._write("sources.json", {"sources": [
            {"name": "A", "api": "http://a/api.php", "from": "http://cfg/1.json"},
            {"name": "B", "api": "http://b/api.php", "from": "http://cfg/2.json"}]})
        n = A.orphan_of_config(self.d, "http://cfg/1.json", "某配置")
        self.assertEqual(n, 1)
        doc = json.load(open(self.d + "/sources.json", encoding="utf-8"))
        a = doc["sources"][0]
        self.assertEqual(a.get("orphan"), 1)
        self.assertEqual(a.get("orphaned_name"), "某配置")
        # 只启用 2 号配置时，离源条目仍然要放行
        self.assertTrue(A.src_allowed(a, {"http://cfg/2.json"}))
        self.assertTrue(A.src_allowed(doc["sources"][1], {"http://cfg/2.json"}))
        self.assertFalse(A.src_allowed(
            {"from": "http://cfg/3.json"}, {"http://cfg/2.json"}))

    def test_reattach_when_config_back(self):
        import aggregate as A
        self._write("sources.json", {"sources": [
            {"name": "A", "api": "http://a/api.php", "from": "http://cfg/1.json",
             "orphan": 1, "orphaned_from": "http://cfg/1.json"}]})
        A._merge_into_sources(self.d + "/sources.json",
                              [{"name": "A", "api": "http://a/api.php"}],
                              "http://cfg/1.json", log=lambda *a: None,
                              src_name="某配置")
        doc = json.load(open(self.d + "/sources.json", encoding="utf-8"))
        self.assertFalse(doc["sources"][0].get("orphan"))
        self.assertEqual(doc["sources"][0]["from"], "http://cfg/1.json")


class TestDoubanJar(unittest.TestCase):
    """豆瓣首站要能自己换 jar，不能硬编码一个地址"""

    def test_pool_order_and_skip_broken(self):
        import aggregate as A
        sites = [{"jar": "http://x/broken.jar"},
                 {"jar": "http://x/candymuj.jar"},
                 {"jar": "http://x/vox.jar"}]
        old = A._jar_has_douban
        A._jar_has_douban = lambda u, timeout=25, cache_dir=None: "candymuj" in u
        try:
            self.assertTrue(A.pick_douban_jar(sites).endswith("candymuj.jar"))
        finally:
            A._jar_has_douban = old

    def test_none_valid_returns_empty(self):
        import aggregate as A
        old = A._jar_has_douban
        A._jar_has_douban = lambda u, timeout=25, cache_dir=None: False
        try:
            self.assertEqual(
                A.pick_douban_jar([{"jar": "http://x/spider.jar"}]), "")
        finally:
            A._jar_has_douban = old


class TestExportParse(unittest.TestCase):
    """导出 txt 的解析要能容忍手改（TAB / 竖线 / 纯空格）"""

    SAMPLE = (
        "# 注释行\n"
        "[接口]\n"
        "主用\t某接口\thttps://a.com/1.json\n"
        "备选\t\t\thttps://b.com/2.json\n"
        "主用|竖线分隔\thttps://c.com/3.json\n"
        "https://d.com/4.json\n"
        "[设置]\n"
        "config_days=5\n"
        "interval_mode=daily\n"
        "[屏蔽名单]\n"
        "名字\t哔哩\n"
        "资源\tbilibili.com\n"
        "只写关键字\n"
        "[GitHub镜像]\n"
        "https://gh-proxy.com/{url}\n"
    )

    def _p(self, text):
        import server
        return server.Handler._parse_export(text)

    def test_parse_formats(self):
        got = self._p(self.SAMPLE)["configs"]
        self.assertEqual(got[0], ("active", "某接口", "https://a.com/1.json"))
        self.assertEqual(got[1], ("bench", "", "https://b.com/2.json"))
        self.assertEqual(got[2], ("active", "竖线分隔", "https://c.com/3.json"))
        self.assertEqual(got[3][2], "https://d.com/4.json")   # 没写档位默认备选

    def test_parse_settings(self):
        stg = self._p(self.SAMPLE)["settings"]
        self.assertEqual(stg.get("config_days"), "5")
        self.assertEqual(stg.get("interval_mode"), "daily")

    def test_garbage_ignored(self):
        r = self._p("随便写点什么\n[接口]\n不是地址的一行\n")
        self.assertEqual(r["configs"], [])
        self.assertEqual(r["settings"], {})

    def test_parse_block_and_mirrors(self):
        r = self._p(self.SAMPLE)
        self.assertEqual(r["block"]["names"], ["哔哩", "只写关键字"])
        self.assertEqual(r["block"]["assets"], ["bilibili.com"])
        self.assertEqual(r["mirrors"], ["https://gh-proxy.com/{url}"])

    def test_uniq_ignores_case_and_space(self):
        import server
        got = server.Handler._uniq(["bili", " BILI ", "哔哩", "", None, "bili"])
        self.assertEqual(got, ["bili", "哔哩"])


class TestSiteKey(unittest.TestCase):
    """两个 key 函数不能互相覆盖（曾经因为同名，聚合直接崩）"""

    def test_cms_key_accepts_url(self):
        self.assertIsInstance(A.site_key("http://x/api.php/provide/vod"), str)

    def test_ext_key_accepts_site(self):
        k = A.ext_key({"api": "csp_T4", "ext": "http://a"})
        self.assertTrue(k.startswith("et_"))
        self.assertNotEqual(k, A.ext_key({"api": "csp_T4", "ext": "http://b"}))

    def test_same_api_different_ext(self):
        a = A.ext_key({"api": "csp_T4", "ext": "x"})
        b = A.ext_key({"api": "csp_T4", "ext": "y"})
        self.assertNotEqual(a, b)


class TestBuildProfileSmoke(unittest.TestCase):
    """build_shell_profile 端到端跑一遍

    曾经因为里面引用了不存在的 `timeout` 变量，整个壳子档生成抛 NameError，
    输出只剩 CMS —— 而单测全绿（没覆盖到这条路径）。
    """

    def test_unreachable_asset_does_not_crash(self):
        doc = {"sites": [{"name": "测试站", "api": "csp_T", "kind": "jar",
                          "jar": "http://nonexistent-xyz-123.example/none.jar",
                          "type": 3, "from": "manual"}]}
        out = A.build_shell_profile("t", [], doc, None, kinds=("jar",), label="t",
                                    asset_ok={}, log=lambda *a: None,
                                    block={"names": [], "assets": [], "pin": []})
        self.assertIn("sites", out)

    def test_cms_only_profile(self):
        cms = [{"key": "k1", "name": "站A", "type": 1, "api": "http://x/api.php",
                "searchable": 1, "classes": [{"type_name": "电影"}]}]
        out = A.build_shell_profile("t", cms, {"sites": []}, None, kinds=(),
                                    label="t", asset_ok={}, log=lambda *a: None,
                                    block={"names": [], "assets": [], "pin": []})
        self.assertEqual(len(out["sites"]), 1)


class TestSourceOrigin(unittest.TestCase):
    """配置来源标记：必须记 url，不能记名字"""

    def test_config_prefix_is_not_allowed(self):
        self.assertFalse(A.src_allowed({"from": "config:某站"}, {"http://a/x.json"}))

    def test_active_url_allowed(self):
        self.assertTrue(A.src_allowed({"from": "http://a/x.json"}, {"http://a/x.json"}))

    def test_manual_kept(self):
        self.assertTrue(A.src_allowed({"from": ""}, set()))
        self.assertTrue(A.src_allowed({"from": "manual"}, set()))


class TestJarRef(unittest.TestCase):
    """配置里 jar 的 ;md5; 后缀与 img+ 包裹"""

    def test_md5_suffix(self):
        self.assertEqual(A.parse_jar_ref("http://x/a.jar;md5;abc")[0], "http://x/a.jar")

    def test_sha256_suffix(self):
        self.assertEqual(A.parse_jar_ref("http://x/a.jar;sha256;def")[0], "http://x/a.jar")

    def test_plain(self):
        self.assertEqual(A.parse_jar_ref("http://x/a.jar"), ("http://x/a.jar", False))

    def test_img_wrapped(self):
        url, wrapped = A.parse_jar_ref("img+http://x/b.png")
        self.assertTrue(wrapped)
        self.assertEqual(url, "http://x/b.png")

    def test_gate_and_aggregate_agree(self):
        v = "http://x/a.jar;md5;abc"
        self.assertEqual(A.parse_jar_ref(v), JG.parse_jar_ref(v))


class TestJarGate(unittest.TestCase):
    """二期：jar 静态扫描。宁杀错不放过 —— 扫不了就拦。"""

    def test_html_not_jar(self):
        r = JG.scan_bytes(b"<html>404</html>" * 100)
        self.assertEqual(r["verdict"], "blocked")

    def test_broken_zip(self):
        r = JG.scan_bytes(b"PK\x03\x04" + b"\x00" * 3000)
        self.assertEqual(r["verdict"], "blocked")

    def test_small_file(self):
        r = JG.scan_bytes(b"PK")
        self.assertEqual(r["verdict"], "blocked")

    def _zip(self, files):
        import io, zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for name, data in files.items():
                z.writestr(name, data)
        return buf.getvalue()

    def test_clean_jar_passes(self):
        data = self._zip({"classes.dex": b"normal code " * 200})
        self.assertEqual(JG.scan_bytes(data)["verdict"], "ok")

    def test_install_apk_blocked(self):
        data = self._zip({"classes.dex":
                          b"x" + b"android.intent.action.INSTALL_PACKAGE" + b"y" * 600})
        r = JG.scan_bytes(data)
        self.assertEqual(r["verdict"], "blocked")
        self.assertIn("会尝试安装其他应用", r["reasons"])

    def test_exec_blocked(self):
        data = self._zip({"classes.dex":
                          b"a" + b"Runtime.getRuntime().exec" + b"b" * 600})
        self.assertEqual(JG.scan_bytes(data)["verdict"], "blocked")

    def test_path_traversal_blocked(self):
        data = self._zip({"../../etc/passwd": b"x" * 800})
        r = JG.scan_bytes(data)
        self.assertEqual(r["verdict"], "blocked")
        self.assertTrue(any("路径" in x for x in r["reasons"]))

    def test_native_so_blocked(self):
        data = self._zip({"lib/arm64/libx.so": b"\x7fELF" + b"x" * 400})
        r = JG.scan_bytes(data)
        self.assertEqual(r["verdict"], "blocked")

    def test_storage_permission_is_warn_only(self):
        # 之前把 WRITE_EXTERNAL_STORAGE / ServerSocket 当告警，结果 27/64 个 jar 全命中——
        # dex 里的「类引用表」只要提到类名就留字符串，跟真的调用无关，全是噪音。
        data = self._zip({"classes.dex":
                          b"z" + b"WRITE_EXTERNAL_STORAGE" + b"z" * 600})
        r = JG.scan_bytes(data)
        self.assertEqual(r["verdict"], "warn")

    def test_server_socket_reference_is_warn_only(self):
        data = self._zip({"classes.dex":
                          b"lfi/iki/elonen/nanohttpd;ljava/net/serversocket;" + b"z" * 600})
        self.assertEqual(JG.scan_bytes(data)["verdict"], "warn")


class TestState(unittest.TestCase):
    """统一读写层：原子、并发安全、jsonl"""

    def setUp(self):
        self.d = tempfile.mkdtemp()

    def test_roundtrip(self):
        import state as ST
        ST.save(self.d, "a.json", {"x": 1})
        self.assertEqual(ST.load(self.d, "a.json"), {"x": 1})

    def test_missing_returns_default(self):
        import state as ST
        self.assertEqual(ST.load(self.d, "none.json"), {})
        self.assertEqual(ST.load(self.d, "none.json", []), [])

    def test_concurrent_writes_stay_parsable(self):
        import state as ST, threading, json, os
        def w(i):
            for k in range(30):
                ST.save(self.d, "b.json", {"who": i, "n": k})
        ts = [threading.Thread(target=w, args=(i,)) for i in range(5)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        with open(os.path.join(self.d, "b.json"), encoding="utf-8") as f:
            json.load(f)          # 不能有半截文件
        self.assertEqual([x for x in os.listdir(self.d) if x.startswith(".w-")], [])

    def test_jsonl(self):
        import state as ST
        ST.append_jsonl(self.d, "h.jsonl", {"t": 1})
        ST.append_jsonl(self.d, "h.jsonl", {"t": 2})
        self.assertEqual([r["t"] for r in ST.load_jsonl(self.d, "h.jsonl")], [1, 2])

    def test_backup_keeps_previous(self):
        import state as ST, os
        ST.save(self.d, "c.json", {"v": 1})
        ST.save(self.d, "c.json", {"v": 2}, backup=True)
        self.assertTrue(os.path.exists(os.path.join(self.d, "c.json.bak")))


class TestAccessStats(unittest.TestCase):
    """访问统计：只记接口、同名桶合并"""

    def setUp(self):
        import access as AC
        AC.ACCESS.clear()

    def test_alias_merge(self):
        import access as AC
        AC.hit("cfg:tvbox", 100)
        AC.hit("cfg:full", 100)          # 旧别名，应并到同一行
        rows = AC.summary()["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["n"], 2)

    def test_bytes_and_avg(self):
        import access as AC
        AC.hit("cfg:all", 1000)
        AC.hit("cfg:all", 3000)
        r = AC.summary()["rows"][0]
        self.assertEqual(r["bytes"], 4000)
        self.assertEqual(r["avg"], 2000)

    def test_clear_legacy(self):
        import access as AC
        AC.ACCESS["cms:abc"] = {"n": 5}
        AC.clear_legacy()
        self.assertNotIn("cms:abc", AC.ACCESS)


def A_next_run(stg, last_ts):
    """把 server 里的调度函数引进来（避免顶层 import 触发副作用）"""
    import server as S
    return S._next_run_ts(stg, last_ts)


if __name__ == "__main__":
    unittest.main(verbosity=2)
