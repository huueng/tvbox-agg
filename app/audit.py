# -*- coding: utf-8 -*-
# Copyright (c) 2026 TVBox Aggregator 作者. All rights reserved.
# 源码可见 · 非开源：禁止商业使用与二次分发，详见仓库根目录 LICENSE。
"""把配置接口逐个真解析一遍，给每条一个明确的原因标签"""
import json
import sys
import time

sys.path.insert(0, "/app")
import aggregate as A
import discover as D

DATA = "/app/data"
reg = json.load(open(DATA + "/configs.json", encoding="utf-8"))
rows = []
for ent in reg.get("configs") or []:
    url = ent.get("url") if isinstance(ent, dict) else ent
    name = (ent.get("name") if isinstance(ent, dict) else "") or url
    st = ent.get("stats") or {}
    pass   # 全量审计，不再跳过
    t0 = time.time()
    rec = {"url": url, "name": name, "tier": ent.get("tier")}
    try:
        docs = D.load_config(url, log=lambda *a: None)
        rec["cost"] = round(time.time() - t0, 1)
        if not D.fetch_ok(url):
            rec["verdict"] = "抓不到（网络/域名/空响应）"
        elif not docs:
            rec["verdict"] = "抓到了但不是有效配置"
        else:
            sites = sum(len(d.get("sites") or []) for _, d in docs)
            lives = sum(len(d.get("lives") or []) for _, d in docs)
            rec["sites"], rec["lives"] = sites, lives
            rec["verdict"] = ("有内容 %d 站点" % sites) if (sites or lives) else "配置是空的"
    except Exception as e:
        rec["cost"] = round(time.time() - t0, 1)
        rec["verdict"] = "异常 %s" % type(e).__name__
        rec["err"] = str(e)[:120]
    rows.append(rec)
    print("%-22s %-26s %5.1fs  %s" % (name[:22], rec["verdict"][:26], rec.get("cost", 0),
                                      url[:52]), flush=True)

json.dump(rows, open(DATA + "/cfg_audit2.json", "w", encoding="utf-8"),
          ensure_ascii=False, indent=1)
print("\n汇总：")
from collections import Counter
for k, v in Counter(r["verdict"].split(" ")[0] for r in rows).most_common():
    print("  %-16s %d" % (k, v))
