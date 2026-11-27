# 安装与部署

## 目录

- [系统要求](#系统要求)
- [docker compose 部署](#docker-compose-部署)
- [一行命令部署](#一行命令部署)
- [参数说明](#参数说明)
- [反向代理与 HTTPS](#反向代理与-https)
- [升级](#升级)
- [备份与迁移](#备份与迁移)
- [资源占用](#资源占用)

---

## 系统要求

| 项 | 要求 |
|---|---|
| Docker | 20.10+（支持 `--network host`） |
| 架构 | amd64 / arm64（纯 Python 标准库，无编译产物） |
| 内存 | 常驻 150~300MB，聚合峰值约 500MB |
| 磁盘 | 镜像约 50MB，数据目录几十 MB |
| 网络 | **建议有 IPv6**：不少直播源是纯 v6 地址，没有 v6 会少一部分可用线路 |

---

## docker compose 部署

```bash
git clone <仓库地址> tvbox-agg
cd tvbox-agg
mkdir -p data

cat > .env <<'EOF'
# 客户端能访问到的地址（不是 127.0.0.1！）
BASE_URL=http://192.168.1.10:5680
# 管理令牌，随便生成一串：openssl rand -hex 12
TOKEN=请替换成随机字符串
EOF

docker compose up -d --build
docker compose logs -f      # 看第一次聚合的进度
```

第一次聚合大概 3~10 分钟。看到 `聚合完成：...` 就可以用了。

---

## 一行命令部署

```bash
docker run -d --name tvbox-agg \
  --network host \
  --restart unless-stopped \
  -e TZ=Asia/Shanghai \
  -e BASE_URL=http://192.168.1.10:5680 \
  -e TOKEN=$(openssl rand -hex 12) \
  -e INTERVAL=6 \
  -v /你的路径/tvbox-data:/app/data \
  ghcr.io/huueng/tvbox-agg:0.9
```

> 用 `--network host` 而不是 `-p 5680:5680`：
> bridge 网络里拿不到 IPv6，纯 v6 的直播线路会被误判成死的。

---

## 参数说明

全部通过环境变量传入。

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `5680` | 监听端口 |
| `BIND` | `0.0.0.0` | 监听地址 |
| `BASE_URL` | 空 | **写进输出配置的对外地址**。留空则用请求的 Host 推断，反代场景建议显式设置 |
| `TOKEN` | 空 | 面板密码 / 写接口令牌。**留空且监听 0.0.0.0 = 无鉴权**，启动时会警告 |
| `INTERVAL` | `6` | 自动聚合间隔（小时），`0` = 只手动 |
| `WORKERS` | `3` | 单轮并发。采集站对并发握手敏感，3~4 比较稳 |
| `TIMEOUT` | `20` | 单个采集站超时（秒） |
| `DROP_AFTER` | `3` | 连续失败多少次摘除 |
| `PROXY` | `0` | `1` = 站点 API 走 `/cms/<key>` 代理（自动修正 `t=` 分类参数，兼容性更好） |
| `MAX_ASSETS` | `25` | 全能档带几个扩展资源 |
| `MAX_JAR_SITES` | `120` | 扩展站点总数上限 |

改完重启容器生效；面板里的设置项改完是**下一次聚合生效**，不用重启。

---

## 反向代理与 HTTPS

TVBox 客户端对 HTTPS 支持一般，**能用 HTTP 就别上 HTTPS**。
如果一定要走反代（比如外网访问），注意三点：

1. `BASE_URL` 必须写**反代后的公网地址**，否则配置里还是内网 IP
2. 不要改写路径前缀（`/tvbox.json`、`/live/...` 都是根路径）
3. 关掉缓冲，配置是流式返回的

Nginx 示例：

```nginx
location / {
    proxy_pass http://127.0.0.1:5680;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_buffering off;
    proxy_read_timeout 300s;
}
```

Caddy 示例：

```caddy
agg.example.com {
    reverse_proxy 127.0.0.1:5680
}
```

---

## 升级

```bash
docker compose pull && docker compose up -d
# 或本地构建：
git pull && docker compose up -d --build
```

`data/` 目录不受影响。**升级后第一次聚合会用新逻辑重跑一遍**，输出的站点顺序可能变化。

---

## 备份与迁移

```bash
# 备份
tar czf tvbox-agg-$(date +%F).tgz data/

# 迁移到另一台机器
scp tvbox-agg-*.tgz new-host:/path/
tar xzf tvbox-agg-*.tgz   # 放在 compose 同级的 data/
```

只备份 `configs.json / sources.json / jars.json / lives.json` 也行，其余会自动重建。

---

## 资源占用

实测（N5105 / 4 核）：

| 场景 | CPU | 内存 | 耗时 |
|---|---|---|---|
| 空闲 | <1% | 150MB | - |
| 点播聚合一轮 | 30~60% | 350~500MB | 3~8 分钟 |
| 直播大聚合 | 20~40% | 300MB | 8~12 分钟 |
| 直播小聚合 | 15~30% | 250MB | 5~8 分钟 |

定时聚合是错开跑的：点播按 `INTERVAL`，直播大聚合 3 天一次、小聚合 1 天一次。
