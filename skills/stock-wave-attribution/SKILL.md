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
- 量价数据必须 `数据库优先`
- 当 `event_quant` 量价窗口不足时，必须先用 `Akshare` 补齐缺口并回写数据库，再重新从数据库读取，不允许直接拿 Akshare 结果跳过数据库落报告
- 概念映射与概念日线也必须 `数据库优先`
- 当 `ana_stock_concept_map / ana_concept_day` 对目标股票为空时，必须先用 `Tushare 代理` 回填数据库，再重新从数据库读取，不允许直接拿代理结果跳过数据库落报告
- 服务模式下，`本地 news 库证据` 必须走两阶段：
  - 先做候选召回与标题去重
  - 再按 `100选3-5` 分 chunk 让 Codex 直接粗排
  - 再从粗排并集里直接精选最终 10 条
  - 粗排和精选都不做逐条打分
- 本地 `run` 入口仍保留旧规则链，方便兼容回归，但正式服务默认不走 `run_chatgpt_browser`
- 正式报告默认只分析 `涨幅 Top2` 的波段
- news 候选来源至少覆盖：
  - `zsxq_zhuwang`
  - `zsxq_damao`
  - `zsxq_saidao_touyan`
- 服务模式粗排窗口当前保持 `波段 start_date - 60 天 -> peak_date`
- 正式报告按波段展开：
  - 一级标题固定为 `波段 Wn`
  - 每个波段下固定包含 `证据原文 / 量价验证表 / 概念联动验证表 / 结论与置信度表 / 综合裁决`
- 波段概览必须包含 `粗排新闻来源分布`
- `证据原文` 必须按发布时间升序展示，并用 fenced code block 原样放数据库全文，避免 Markdown 渲染器把多行原文打穿
- 正式报告必须包含：
  - `报告时间`
  - `一句话逻辑`
  - `综合裁决`
- 默认本地归因链也必须直接给出：
  - `主因`
  - `备选`
  - `最终判定`
  不能再写“待结合本地证据裁决”这类占位文案
- 当前正式报告主因、备选、时间线和结论，默认由本地 `event_news / event_quant / tushare / 概念联动验证` 收口
- 历史 ChatGPT 链路暂时不删除，但默认关闭，也不属于正式服务链

## 当前真实调用链

1. `runtime/wave_segmentation.py`
2. `runtime/wave_plotting.py`
3. `runtime/attribution_data.py`
4. `scripts/orchestrator.py prepare-agent-rerank`
5. `Codex service agent rerank`
6. `scripts/orchestrator.py finalize-agent-rerank`
7. `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
   - 仅保留给历史兼容路径，当前正式服务默认不启用

当前 orchestrator 文件：

- `skills/stock-wave-attribution/scripts/orchestrator.py`

历史报告批量迁移脚本：

- `scripts/migrate_news_raw_text_reports.py`

当前默认配置文件：

- `skills/stock-wave-attribution/stock-wave-attribution.yaml`

## 下一步

1. 继续做真实样本端到端回归
2. 继续回归数据库优先、量价 Akshare / 概念 Tushare 代理补库兜底链路
