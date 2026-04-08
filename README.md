# case_data

`case_data` 是一个面向交易案例归因分析的项目工作目录，当前重点覆盖三条主线：

1. 维护案例库原始资产
2. 同步和整理量价/概念数据
3. 输出单票波段归因与概念级分析文档

## 文档入口

- [docs/HANDOFF.md](./docs/HANDOFF.md)
  - 项目交接正文，包含架构、运行状态、恢复步骤、已知限制和后续待办
- [docs/project-datastores.md](./docs/project-datastores.md)
  - PostgreSQL 实例、连接方式、恢复流程和表快照
- [docs/top400-layer-taxonomy.md](./docs/top400-layer-taxonomy.md)
  - `industry / theme_concept / subchain_tag` 三层口径定义
- [skills/README.md](./skills/README.md)
  - 项目内 skills 总入口

## 快速事实

- 当前数据库主线为本机 PostgreSQL 16，承载 `event_news` 和 `event_quant`
- 正式分析输出在 [docs/analysis](./docs/analysis/)
- 项目内 skills 采用“项目优先”口径
- 单票波段归因主入口是 [skills/stock-wave-attribution/SKILL.md](./skills/stock-wave-attribution/SKILL.md)
- `stock-wave-attribution` 默认配置文件是 [skills/stock-wave-attribution/stock-wave-attribution.yaml](./skills/stock-wave-attribution/stock-wave-attribution.yaml)
- ChatGPT 浏览器运行时主入口是 [skills/chatgpt-plus-browser/SKILL.md](./skills/chatgpt-plus-browser/SKILL.md)
- 本机归因服务说明见 [service/README.md](./service/README.md)

## 关键路径

- 原始资产：
  - [stock.xlsx](./stock.xlsx)
  - [marco.xlsx](./marco.xlsx)
  - [字段说明书_正例库V1.md](./字段说明书_正例库V1.md)
- 量价与数据脚本：
  - [scripts/event_quant_sync.py](./scripts/event_quant_sync.py)
  - [scripts/wave_segmentation.py](./scripts/wave_segmentation.py)
  - [scripts/wave_plotting.py](./scripts/wave_plotting.py)
  - [scripts/attribution_data.py](./scripts/attribution_data.py)
  - 其中 `event_quant_sync.py sync-all-stocks` 用于按交易日批量同步全市场个股量价包
  - 其中 `event_quant_sync.py sync-all-concepts` 用于批量同步全量同花顺概念指数
  - 其中 `event_quant_sync.py sync-all-concept-members` 用于批量同步全量同花顺概念成员映射
- 项目内 skills：
  - [skills/chatgpt-plus-browser](./skills/chatgpt-plus-browser/)
  - [skills/stock-wave-attribution](./skills/stock-wave-attribution/)
- 本机归因服务：
  - [service](./service/)
- 正式分析结果：
  - [docs/analysis](./docs/analysis/)

## 最短恢复

1. 使用 conda `base`，确认核心依赖已安装
2. 按 [docs/project-datastores.md](./docs/project-datastores.md) 启动并校验本机 PostgreSQL 16
3. 检查 [config/project.yaml](./config/project.yaml) 是否符合当前机器路径与凭据注入方式
4. 如需继续归因，先看 [skills/stock-wave-attribution/SKILL.md](./skills/stock-wave-attribution/SKILL.md)
5. 如需使用 ChatGPT 网页自动化，先看 [skills/chatgpt-plus-browser/SKILL.md](./skills/chatgpt-plus-browser/SKILL.md)

详细交接信息、限制与恢复命令见 [docs/HANDOFF.md](./docs/HANDOFF.md)。
