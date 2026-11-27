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

## 直接使用官方镜像

```bash
docker pull ghcr.io/huueng/tvbox-agg:0.9
```

多架构（amd64 / arm64），由 GitHub Actions 自动构建。
不想自己构建的话，下面 compose 里的 `build: .` 可以直接删掉。

## docker compose 部署（推荐）

**复制一段 compose 就能跑**，不用克隆仓库、不用建 .env。

```bash
mkdir -p tvbox-agg && cd tvbox-agg
curl -fsSLO https://raw.githubusercontent.com/huueng/tvbox-agg/main/docker-compose.yml
vi docker-compose.yml      # 只改 TOKEN 那一行
docker compose up -d
docker compose logs -f     # 看第一次聚合，约 3~10 分钟
```

要改的只有这一行：

```yaml
      - TOKEN=${TOKEN:-}        # 改成  - TOKEN=你的密码
```

其他变量都有默认值（`${VAR:-默认}`），按需覆盖即可，也可以写进同目录的 `.env`：

```bash
cp .env.example .env     # 可选，喜欢用 .env 就用
```

### 必须用端口映射的环境

默认 compose 用 `host` 网络（能测纯 IPv6 的直播源）。如果环境不允许 host 网络：

```bash
docker compose -f docker-compose.bridge.yml up -d
```

代价：容器里没有 IPv6，纯 v6 的直播线路会被判成不可用、从输出里消失。
另外 NAS 面板里 host 网络**不显示端口**，这是飞牛/群晖的显示限制，不影响使用。

### 从源码构建

```bash
git clone https://github.com/huueng/tvbox-agg.git
cd tvbox-agg
# 在 docker-compose.yml 的 image 行下面加一行：build: .
docker compose up -d --build
```

---

## 一行命令部署

```bash
docker run -d --name tvbox-agg \
  --network host \
  --restart unless-stopped \
  -e TZ=Asia/Shanghai \
  -e TOKEN=$(openssl rand -hex 12) \
  -e INTERVAL=6 \
  -v /你的路径/tvbox-data:/app/data \
  ghcr.io/huueng/tvbox-agg:0.9
```

> 用 `--network host` 而不是 `-p 5680:5680`：
> bridge 网络里拿不到 IPv6，纯 v6 的直播线路会被误判成死的。
>
> 生成的配置里地址不对（例如走了反代、或机器有多网卡）时，再加
> `-e BASE_URL=http://你的设备地址:5680` 覆盖。

---

## 参数说明

全部通过环境变量传入。

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | `5680` | 监听端口 |
| `BIND` | `0.0.0.0` | 监听地址 |
| `BASE_URL` | 空 | 写进输出配置的对外地址。**一般留空**（自动用访问时的 Host 推断）；反代或多网卡时再显式设置 |
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

1. `BASE_URL` 要写**反代后的公网地址**（留空会被推断成内网 IP）
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

面板「设置 → 版本与更新」里可以**检查有没有新版本**（只查不升）。
国内连不上 `ghcr.io` 时，在那一栏填个镜像/加速地址，例如
`ghcr.nju.edu.cn/huueng/tvbox-agg`，检查会改从这里查——
注意这只影响「检查」，真正 `pull` 还是按 compose 里写的地址走。

`data/` 目录不受影响。**升级后第一次聚合会用新逻辑重跑一遍**，输出的站点顺序可能变化。

---

## 从旧版本（自建镜像）迁移到官方镜像

数据格式向后兼容：**新容器挂同一个 `data/` 目录，设置、源库、直播线路全部照旧**。
只多出一些新的设置项，会走默认值。

```bash
# 1. 备份（一定要做）
cd /你的部署目录
tar czf backup/pre-migrate-$(date +%F-%H%M).tgz data/

# 2. 停掉旧的
docker compose down

# 3. 换成官方镜像的 compose（TOKEN 保留原值，data 挂载路径不变）
docker compose up -d

# 4. 确认数据还在
curl -s http://127.0.0.1:5680/healthz
```

迁移后建议检查三处：

| 检查 | 期望 |
|---|---|
| 面板 → 采集站点统计 | 站点数、直播源数和迁移前一致（±几个属正常浮动） |
| `/live/cctv.m3u` 行数 | 和迁移前一致 |
| 面板 → 设置 | 原来的设置项还在，新增项为默认值 |

**回滚**：`docker compose down`，把备份的 `data/` 解回去，再启动旧镜像即可。

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
