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
python3 skills/stock-wave-attribution/scripts/orchestrator.py run \
  --stock-name 五洲新春 \
  --ts-code 603667.SH \
  --start-date 2025-11-05 \
  --end-date 2026-01-22 \
  --sample-label 机器人概念
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
- 波段审查与归因编排默认收口为本地数据库与规则链路
- 报告继续输出到 `docs/analysis/`
- 配图继续输出到 `data/plots/`
- PostgreSQL 与 `tushare` 配置默认从 `stock-wave-attribution.yaml` 读取
- 详细表格合同固定在：
  - `skills/stock-wave-attribution/templates/detailed_report_contract.md`

## 特别规则

- 数据源恢复、news 检索、概念验证优先复用 skill runtime 与宿主数据库资产
- `本地 news 库证据` 先做候选召回，再做精排，不允许把命中的全部 news 直接写进报告
- news 候选来源至少覆盖：
  - `zsxq_zhuwang`
  - `zsxq_damao`
  - `zsxq_saidao_touyan`
  - `wscn_live`
- 精排默认优先：
  - 离波段启动日更近的消息
  - 直接提到标的的消息
  - 与样本标签/核心概念强相关的标题与正文
  - 去重后的高价值消息
- `本地 news 库证据` 章节固定拆成两段：
  - 先给元信息表：`序号 / 时间 / 来源 / 标题 / 链接`
  - 再给逐条 `证据原文`
- `事件时间线表` 只放精选后的摘要，不放整段原文全文
- `证据原文` 必须用 fenced code block 原样放数据库全文，避免 Markdown 渲染器把多行原文打穿
- 正式报告必须包含：
  - `一句话逻辑`
  - `综合裁决`
- 默认本地归因链也必须直接给出：
  - `主因`
  - `备选`
  - `最终判定`
  不能再写“待结合本地证据裁决”这类占位文案
- 当前正式报告主因、备选、时间线和结论，默认由本地 `event_news / event_quant / tushare / 概念联动验证` 收口
- ChatGPT 链路暂时不删除，但默认关闭
- 如需恢复 ChatGPT 补强链路，优先通过 `stock-wave-attribution.yaml` 里的 `chatgpt.enabled` 显式开启
- ChatGPT 开启后，才把 `chatgpt-plus-browser` 视为当前执行链的一部分

## 当前真实调用链

1. `runtime/wave_segmentation.py`
2. `runtime/wave_plotting.py`
3. `runtime/attribution_data.py`
4. `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
   - 仅当 `chatgpt.enabled = true` 时启用

当前 orchestrator 文件：

- `skills/stock-wave-attribution/scripts/orchestrator.py`

历史报告批量迁移脚本：

- `scripts/migrate_news_raw_text_reports.py`

当前默认配置文件：

- `skills/stock-wave-attribution/stock-wave-attribution.yaml`

## 下一步

1. 继续做真实样本端到端回归
2. 再补量化数据 provider 灾备链
