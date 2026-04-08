# 项目数据库说明

本文档记录 `fly2026` 当前案例归因相关的数据库、连接方式、启动顺序和使用边界。

## 1. 总览

当前项目里有两个核心 PostgreSQL 数据库：

- `event_news`
  - 用途：保存新闻、知识星球、华尔街见闻等事件流数据
  - 典型查询：股票相关消息检索、主题催化检索、时间窗证据补充
- `event_quant`
  - 用途：保存 `tushare` 同步下来的个股、概念、市场等量价数据
  - 典型查询：日线、资金、涨停、概念相关性、市场基准

两者当前共享同一个 PostgreSQL 实例，对外都通过本机 `5432` 端口访问。

## 2. 当前实例信息

当前机器已切换为 **Homebrew 管理的本机 PostgreSQL 16**，不再依赖 `colima + docker` 作为默认运行口径。

- 服务名：`postgresql@16`
- 数据目录：`/opt/homebrew/var/postgresql@16`
- 主机端口：`5432`
- 当前默认系统用户：`zhengshenghua`
- 为兼容历史 dump，已补建角色：`postgres`
- 当前可用本地连接示例：
  - `postgresql://zhengshenghua@localhost:5432/event_news`
  - `postgresql://zhengshenghua@localhost:5432/event_quant`
- 若业务脚本或旧文档仍显式使用 `postgres:postgres`，需先确认本机 `pg_hba.conf` 与角色密码是否按该口径配置；本机恢复默认优先使用系统用户直连。

## 3. 启动顺序

本项目当前默认依赖 **Homebrew 安装的本机 PostgreSQL**。

首次安装：

```bash
brew install postgresql@16
```

日常启动与停止：

```bash
brew services start postgresql@16
brew services stop postgresql@16
brew services restart postgresql@16
```

检查状态：

```bash
brew services list | grep postgresql@16
/opt/homebrew/opt/postgresql@16/bin/pg_ctl -D /opt/homebrew/var/postgresql@16 status
```

如果 `Connection refused`，默认先检查：

1. `postgresql@16` 服务是否已启动
2. `5432` 端口是否被当前实例占用
3. 目标数据库 `event_news / event_quant` 是否已恢复

不要在未检查本机 PostgreSQL 前直接跳回本地文件或第三方接口。

### 3.2 归因服务的数据库预检

归因 service 现在会在真正执行 `/tasks/{task_id}/run` 前先做数据库预检：

- `event_news` 是否可连接，且 `event_metadata` 是否存在
- `event_quant` 是否可连接，且 `raw_stock_daily_qfq` 是否存在

也可以单独调用：

```bash
curl http://127.0.0.1:8000/health/datastores
```

如果这里已经返回 `ok=false`，就先修 PostgreSQL 和数据库恢复，不要继续发正式归因任务。

### 3.1 兼容旧环境的 Docker 方案

如果后续换到旧机器，仍然可以继续沿用 Docker 方案；但它不再是当前项目文档的默认口径。

```bash
docker start event-news-pg || docker run -d \
  --name event-news-pg \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=event_news \
  -p 5432:5432 \
  postgres:16
```

## 4. 连接校验

### 4.1 检查实例中有哪些数据库

```bash
python3 - <<'PY'
import psycopg
conn = psycopg.connect('postgresql://zhengshenghua@localhost:5432/postgres', connect_timeout=5)
cur = conn.cursor()
cur.execute("select datname from pg_database where datistemplate = false order by datname")
print([row[0] for row in cur.fetchall()])
conn.close()
PY
```

### 4.2 校验 `event_news`

```bash
python3 - <<'PY'
import psycopg
conn = psycopg.connect('postgresql://zhengshenghua@localhost:5432/event_news', connect_timeout=5)
cur = conn.cursor()
cur.execute('select current_database(), current_user')
print(cur.fetchone())
conn.close()
PY
```

### 4.3 校验 `event_quant`

```bash
python3 - <<'PY'
import psycopg
conn = psycopg.connect('postgresql://zhengshenghua@localhost:5432/event_quant', connect_timeout=5)
cur = conn.cursor()
cur.execute('select current_database(), current_user')
print(cur.fetchone())
conn.close()
PY
```

## 5. 数据库职责

### 5.1 `event_news`

主要用于消息面证据：

- 股票名或代码命中的直接消息
- 同时间窗内的主题消息
- 来源优先级：
  - `zsxq_zhuwang`
  - `zsxq_damao`

在 `stock-wave-attribution` 流程里，它的职责是：

- 补起涨时间点
- 校正 ChatGPT 把催化看得过晚的问题
- 提供库内命中与否的证据

### 5.2 `event_quant`

主要用于量价与概念验证。

当前最常用表：

- `raw_stock_daily_qfq`
- `raw_daily_basic`
- `raw_moneyflow`
- `raw_limit_list_d`
- `raw_index_daily`
- `raw_ths_concept_daily`
- `raw_ths_member`
- `ana_stock_concept_map`
- `ana_concept_day`

在 `stock-wave-attribution` 流程里，它的职责是：

- 提供个股量价事实
- 提供概念指数和市场基准
- 支撑个股 vs 概念相关性验证

## 6. 当前表快照（2026-03-18）

### 6.1 `event_quant`

| 表名 | 行数 | 主要信息 |
|---|---:|---|
| `raw_stock_daily_qfq` | `1643357` | 个股前复权日线 |
| `raw_daily_basic` | `1643357` | 换手率、市值、估值 |
| `raw_moneyflow` | `1559925` | 大单/超大单/净流入 |
| `raw_limit_list_d` | `32867` | 涨停、开板、封单强度 |
| `raw_index_daily` | `18` | 上证/深成/创业板基准 |
| `raw_ths_member` | `80938` | 概念 -> 成分股映射 |
| `raw_ths_concept_daily` | `194947` | 概念指数原始日线 |
| `ana_stock_concept_map` | `74015` | 股票 -> 概念映射 |
| `ana_concept_day` | `194947` | 分析友好的概念指数日线 |
| `sync_job_state` | `22` | 同步任务状态与断点续传 |

说明：

- 全市场个股量价：已补到 `2025-01-02` 至 `2026-04-07`
- 全量概念指数：已补到 `2025-01-02` 至 `2026-04-07`
- 全量概念成员映射：已按 `2026-04-07` 快照补齐
- 主营行业临时映射：仍可补出 `map / frequency`，但尚未升级成正式 `industry reps`

### 6.2 `event_news`

| 表名 | 行数 | 主要信息 |
|---|---:|---|
| `event_metadata` | `89264` | 新闻/知识星球/华尔街见闻原始事件 |
| `event_dedup` | `89264` | 去重关系与版本追踪 |

## 7. 新机器恢复建议

如果迁移到新机器，最小恢复顺序建议是：

1. 恢复 Python 环境
   - 默认使用 conda `base`
   - 安装核心依赖：
     - `pandas`
     - `openpyxl`
     - `psycopg`
     - `tushare==1.4.24`
     - `matplotlib`
2. 恢复本地 PostgreSQL
   - 如未安装，先执行 `brew install postgresql@16`
   - 启动 `brew services start postgresql@16`
   - 确认本机 `5432` 可用
3. 恢复数据库
   - 先恢复 `event_news`
   - 再恢复 `event_quant`
4. 校验数据库连接
   - 校验数据库列表
   - 校验 `event_news`
   - 校验 `event_quant`
5. 校验关键业务资产
   - `stock.xlsx`
   - `data/wencai_top200_20250910_20260309.csv`
   - `data/wencai_top400_20250910_20260309.csv`
   - `data/top400_industry_map.csv`
   - `data/top400_industry_frequency.csv`
   - `docs/analysis/`
6. 如需继续同步
   - 个股量价：`event_quant_sync.py sync-stock-file`
   - 全市场个股量价：`event_quant_sync.py sync-all-stocks`
   - 全量概念指数：`event_quant_sync.py sync-all-concepts`
   - 全量概念成员映射：`event_quant_sync.py sync-all-concept-members`
   - 概念映射 + 概念指数：`event_quant_sync.py sync-stock-file-concepts`

### 7.1 从项目内 dump 恢复数据库

如果项目目录里已经有 `data/db_backups/<timestamp>/`，推荐按下面顺序恢复：

```bash
export PG_BIN=/opt/homebrew/opt/postgresql@16/bin
export BACKUP_DIR=/path/to/case_data/data/db_backups/20260318_153319
```

如本机还没有兼容旧 dump 的 `postgres` 角色，可先补建：

```bash
$PG_BIN/psql -d postgres -c "CREATE ROLE postgres LOGIN SUPERUSER;"
```

重建数据库：

```bash
$PG_BIN/dropdb --if-exists event_news
$PG_BIN/dropdb --if-exists event_quant
$PG_BIN/createdb event_news
$PG_BIN/createdb event_quant
```

恢复 dump：

```bash
$PG_BIN/pg_restore --no-owner --dbname=event_news "$BACKUP_DIR/event_news.dump"
$PG_BIN/pg_restore --no-owner --dbname=event_quant "$BACKUP_DIR/event_quant.dump"
```

若只想保留当前系统用户 ownership，优先继续使用 `--no-owner`；
若后续需要严格复刻旧库 owner，再额外处理角色和对象 owner。

### 7.2 恢复后的快速校验

```bash
$PG_BIN/psql -d postgres -c "select datname from pg_database where datistemplate = false order by datname;"
$PG_BIN/psql -d event_news -c "select count(*) as event_metadata_count from event_metadata;"
$PG_BIN/psql -d event_news -c "select count(*) as event_dedup_count from event_dedup;"
$PG_BIN/psql -d event_quant -c "select count(*) as raw_stock_daily_qfq_count from raw_stock_daily_qfq;"
$PG_BIN/psql -d event_quant -c "select count(*) as raw_ths_concept_daily_count from raw_ths_concept_daily;"
```

## 8. 项目内备份方式

当前项目默认把数据库备份到项目目录里，而不是备份到系统临时目录或用户家目录。

备份脚本：

- [backup_postgres.py](../scripts/backup_postgres.py)

默认输出目录：

- [data/db_backups](../data/db_backups)

默认会备份两个库：

- `event_news`
- `event_quant`

运行方式：

```bash
python3 scripts/backup_postgres.py
```

输出内容：

- `event_news.dump`
- `event_quant.dump`
- `manifest.json`

最近一次项目内备份目录示例：

- `data/db_backups/20260318_153319`

### 8.1 当前脚本口径说明（重要）

当前仓库里的部分脚本还保留旧环境口径，接手时需要注意：

- `scripts/event_quant_sync.py`
  - 当前已改为优先读取 `config/project.yaml`
  - 命令行参数 `--db-dsn` / `--token` / `--http-url` 仅作为本次调用的显式覆盖
  - 默认不再依赖环境变量，也不再依赖脚本内硬编码项目级默认值
- `scripts/backup_postgres.py`
  - 当前已支持 `local` / `docker` 两种备份模式
  - 默认模式由 `config/project.yaml` 中的 `backup.mode` 决定
  - `local` 模式使用本机 `pg_dump`，`docker` 模式保留旧的 `docker exec ... pg_dump` 行为

## 8. 当前工作假设

- `ths_member` 当前映射先作为候选概念集合使用
- 第一版不追求严格历史概念成分回溯
- 当 `event_quant` 缺概念日线时，可临时回退 `tushare ths_daily`
- 但在回退前，必须先确认 PostgreSQL 实例是否已正常启动

## 9. 对 skill 的约束

对 `stock-wave-attribution` 这类归因工作流，默认执行顺序应是：

1. 先检查 `postgresql@16` 服务是否已启动
2. 再检查 `event_news / event_quant` 是否可连
3. 确认数据库不可用后，才允许回退到 `tushare` 或本地文件

也就是说：

- `Connection refused` 不是“直接回退”的信号
- 它首先是“需要恢复 PostgreSQL 实例”的信号
