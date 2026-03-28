# stock-wave-attribution

## 目标

把股票波段归因能力收拢成可迁移的自包含 skill，复制到其他环境后只需要修改 `stock-wave-attribution.yaml` 就能接入当前数据库和 `tushare` 配置。

## 当前阶段

本轮已经落下第一版正式 orchestrator、skill 内 runtime 和默认配置文件，并把详细表格版输出契约固化到项目内。

三层边界仍保持不变：

1. `skills/stock-wave-attribution/runtime/`
   - 放 skill 自带的配置、数据读取、波段切分、绘图模块
2. `skills/stock-wave-attribution/scripts/`
   - 放编排层、入口脚本

## 最小入口

```bash
python3 skills/stock-wave-attribution/scripts/orchestrator.py deps
python3 skills/stock-wave-attribution/scripts/orchestrator.py contract-path
```

默认配置入口：

```bash
skills/stock-wave-attribution/stock-wave-attribution.yaml
```

迁移元数据入口仍保留：

```bash
python3 skills/stock-wave-attribution/scripts/project_skill.py summary --json
```

## 核心迁移口径

- 波段切分、绘图、量价验证辅助函数一起进入 skill runtime
- 波段审查与归因编排全部收口为本地数据库与规则链路
- 报告继续输出到 `docs/analysis/`
- 配图继续输出到 `data/plots/`
- PostgreSQL 与 `tushare` 配置默认从 `stock-wave-attribution.yaml` 读取
- 详细表格合同固定在：
  - `skills/stock-wave-attribution/templates/detailed_report_contract.md`

## 特别规则

- 数据源恢复、news 检索、概念验证优先复用 skill runtime 与宿主数据库资产
- `本地 news 库证据` 章节默认直接写数据库原文，不再写“完整摘要/正文要点”
- `本地 news 库证据` 章节固定拆成两段：
  - 先给元信息表：`序号 / 时间 / 来源 / 标题 / 链接`
  - 再给逐条 `证据原文`
- `证据原文` 必须用 fenced code block 原样放数据库全文，避免 Markdown 渲染器把多行原文打穿
- 当前正式报告主因、备选、时间线和结论，统一由本地 `event_news / event_quant / tushare / 概念联动验证` 收口
- 默认不要在 skill 主流程里发起 ChatGPT 联网搜索或浏览器自动化
- 如果后续要恢复外部搜索能力，应作为独立补强链路接回，不再写入本 skill 的默认执行步骤

## 当前真实调用链

1. `runtime/wave_segmentation.py`
2. `runtime/wave_plotting.py`
3. `runtime/attribution_data.py`

当前 orchestrator 文件：

- `skills/stock-wave-attribution/scripts/orchestrator.py`

历史报告批量迁移脚本：

- `scripts/migrate_news_raw_text_reports.py`

当前默认配置文件：

- `skills/stock-wave-attribution/stock-wave-attribution.yaml`

## 下一步

1. 把本地 news / 概念数据读取正式接到数据库层
2. 再做真实样本端到端回归
