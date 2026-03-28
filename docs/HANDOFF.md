# case_data Handoff

## 目标与结论

- [已验证] `case_data` 当前不是单纯的 Excel 资料目录，而是一个围绕案例归因分析组织的数据与工作流项目
- [已验证] 当前仓库已经形成三层主线：原始案例资产、量价/概念数据同步、项目内归因 skill
- [已验证] 项目内 skill 采用“项目优先”口径，`chatgpt-plus-browser` 与 `stock-wave-attribution` 的正式入口都在 `case_data/skills/`
- [未验证] 本轮没有实机验证 PostgreSQL、ChatGPT 自动化 Chrome、外部 API 在线状态；本文件中的在线/停止判断以仓库结构和现有文档为准，不代表服务此刻一定可用

## 当前架构

当前真实链路可按五层理解：

1. 原始资产层
   - `stock.xlsx`
   - `marco.xlsx`
   - `字段说明书_正例库V1.md`
2. 项目脚本层
   - `scripts/event_quant_sync.py`
   - `scripts/wave_segmentation.py`
   - `scripts/wave_plotting.py`
   - `scripts/attribution_data.py`
3. 数据库层
   - `event_news`：消息面证据库
   - `event_quant`：量价、概念、市场基准库
4. 项目内 skill 层
   - `skills/chatgpt-plus-browser/`：ChatGPT 网页自动化运行时
   - `skills/stock-wave-attribution/`：单票波段归因 orchestrator 与报告契约
5. 产出层
   - `docs/analysis/`：正式分析报告
   - `data/plots/`：K 线与波段配图
   - `data/`：榜单、中间产物、缓存与数据库备份

## 当前运行状态

- [已验证] 仓库内存在项目内 skill、数据库说明文档、正式分析报告和项目脚本，文档入口完整
- [已验证] 根 README 已调整为入口文档；完整交接正文在 `docs/HANDOFF.md`
- [已验证] `chatgpt-plus-browser` 不是常驻服务；默认按需手动启动 Chrome 自动化窗口，任务完成后会自动关闭对应任务页
- [已验证] PostgreSQL 当前文档口径是本机 Homebrew 管理的 `postgresql@16`；恢复路径默认按需执行 `brew services start postgresql@16`
- [未验证] `event_news` 与 `event_quant` 当前是否在线
- [未验证] ChatGPT 自动化 Chrome 当前是否已登录且可用
- [未验证] `tushare.token` 当前机器是否已完成有效注入

## 关键文件与职责

- `README.md`
  - 项目入口文档，只保留入口、关键路径和最短恢复
- `docs/HANDOFF.md`
  - 项目交接正文
- `docs/project-datastores.md`
  - PostgreSQL 启动、连接、校验和恢复口径
- `docs/top400-layer-taxonomy.md`
  - `Top400` 三层口径正式定义
- `skills/README.md`
  - 项目内 skills 总入口
- `skills/chatgpt-plus-browser/SKILL.md`
  - ChatGPT 网页自动化运行时的项目内说明
- `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
  - CDP 驱动的 ChatGPT 任务提交、轮询、结果提取与任务页关闭逻辑
- `skills/stock-wave-attribution/SKILL.md`
  - 波段归因 skill 的项目内边界说明
- `skills/stock-wave-attribution/scripts/orchestrator.py`
  - 波段归因编排层
- `scripts/event_quant_sync.py`
  - `event_quant` 数据同步主脚本
- `scripts/wave_segmentation.py`
  - 候选波段切分
- `scripts/wave_plotting.py`
  - K 线波段绘图
- `scripts/attribution_data.py`
  - 归因数据访问与标准化工具层

## 安装与初始化

- 默认 Python 环境为 conda `base`
- 核心依赖至少包括：
  - `pandas`
  - `openpyxl`
  - `psycopg`
  - `tushare==1.4.24`
  - `matplotlib`
- 如需使用 ChatGPT 网页自动化，还需要：
  - 本机可启动的 Chrome
  - 已登录 ChatGPT 的专用 profile
  - 可访问的 CDP 调试端口 `9222`

## 配置与环境变量

- 统一配置入口：
  - `config/project.yaml`
- 文档中已经明确的配置点：
  - PostgreSQL 连接口径
  - Tushare token 占位
  - 项目路径与备份模式
- 敏感信息边界：
  - 仓库中不应提交真实 token、密码或浏览器登录态
  - `config/project.yaml` 中的 `tushare.token` 应保持占位或通过外部方式注入

## 数据库与存储

- 当前默认数据库口径：
  - 本机 Homebrew 管理的 PostgreSQL 16
  - 端口 `5432`
  - 数据库：`event_news`、`event_quant`
- 当前默认连接示例见 [project-datastores.md](./project-datastores.md)
- 数据库备份目录：
  - `data/db_backups/`
- 当前最常用的数据表：
  - `event_news.event_metadata`
  - `event_news.event_dedup`
  - `event_quant.raw_stock_daily_qfq`
  - `event_quant.raw_daily_basic`
  - `event_quant.raw_moneyflow`
  - `event_quant.raw_limit_list_d`
  - `event_quant.raw_index_daily`
  - `event_quant.raw_ths_concept_daily`
  - `event_quant.raw_ths_member`
  - `event_quant.ana_stock_concept_map`
  - `event_quant.ana_concept_day`

## 外部依赖与第三方系统

- `tushare`
  - 提供个股、概念和市场量价数据
- ChatGPT 网页版
  - 通过项目内 `chatgpt-plus-browser` skill 以 Chrome CDP 方式驱动
- 问财榜单文件
  - 作为 `Top200 / Top400` 候选池上游资产
- 本地 PostgreSQL
  - 承载 `event_news` 与 `event_quant`

## 安全与敏感信息边界

- 不在仓库内提交真实 `tushare.token`
- 不在文档中写明数据库密码、网页登录态或浏览器 Cookie
- ChatGPT 自动化只应使用专用 profile，不应污染日常浏览器上下文
- 项目内 skill 已切到项目路径，不应再把 `~/.codex/skills` 当作本项目运行事实源

## 已知限制

- [已验证] `Top200` 已具备第一优先级量价数据，适合直接进入单票归因
- [已验证] `Top201-400` 尚未补完整个股第一优先级量价细项，更适合先做概念级分析
- [已验证] `industry` 层当前仍带有临时映射性质，不是正式行业体系 reps
- [已验证] `theme_concept` / `subchain_tag` 口径仍在继续收敛，详细定义见 `docs/top400-layer-taxonomy.md`
- [已验证] `chatgpt-plus-browser` 当前已经项目内化，但仍依赖真实网页 DOM、登录态和 Chrome CDP，可用性受页面变化影响
- [已验证] `chatgpt-plus-browser` 当前会自动关闭任务完成后的 ChatGPT 任务页，但不会主动退出整个 Chrome 进程
- [未验证] 当前项目是否已经具备稳定的 end-to-end 样本回归流程

## 恢复运行步骤

最短恢复顺序如下：

1. 启动 Python 环境

```bash
conda activate base
```

2. 如未安装 PostgreSQL 16，先安装

```bash
brew install postgresql@16
```

3. 启动 PostgreSQL 16

```bash
brew services start postgresql@16
```

4. 检查 PostgreSQL 状态

```bash
brew services list | grep postgresql@16
/opt/homebrew/opt/postgresql@16/bin/pg_ctl -D /opt/homebrew/var/postgresql@16 status
```

5. 校验数据库列表

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

6. 校验 `event_news`

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

7. 校验 `event_quant`

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

8. 校验项目内 skill 基线

```bash
node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs
python -m unittest discover -s skills/chatgpt-plus-browser/tests -p 'test_*.py'
python -m unittest discover -s skills/stock-wave-attribution/tests -p 'test_*.py'
```

9. 如需继续归因，先看以下入口

```bash
python3 skills/stock-wave-attribution/scripts/project_skill.py summary --json
python3 skills/stock-wave-attribution/scripts/orchestrator.py deps
```

10. 如需使用 ChatGPT 浏览器链路，先看以下入口

```bash
./skills/chatgpt-plus-browser/scripts/start_chrome.sh
node skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs status
```

## 故障排查优先级

1. 先查数据库
   - `postgresql@16` 是否运行
   - `5432` 是否可用
   - `event_news / event_quant` 是否存在且可连
2. 再查项目配置
   - `config/project.yaml` 路径与 token 注入方式是否适配当前机器
3. 再查 skill 入口
   - 是否误用 `~/.codex/skills`
   - 是否从项目内 `skills/` 启动
4. 再查 ChatGPT 自动化
   - Chrome 是否已启动
   - 调试端口 `9222` 是否可达
   - 专用 profile 是否仍保持登录态
5. 最后才考虑数据缺失
   - 不要把“数据库未启动”误判成“没有数据”

## 后续待办

- 优先把 README 中已拆出的交接正文持续维护在 `docs/HANDOFF.md`
- 继续把 `stock-wave-attribution` 的本地 news / 概念数据读取正式接到数据库层
- 为 `chatgpt-plus-browser` 增加更稳定的页面结构断言和真实样本回归
- 继续收敛 `Top400` 的行业层正式 reps 与概念层分析产物

## 补充记录

- 2026-03-19 起，项目内 skill 已明确采用“项目优先”口径，`case_data/skills/` 为本项目事实来源
- 本轮补充了根 README 入口化改造，并新增本文件承接交接正文
- 本轮没有实机验证数据库在线状态、ChatGPT 登录态或外部 API 可用性，相关状态仍应在接手时重新确认
