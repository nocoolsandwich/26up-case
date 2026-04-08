# stock-wave-attribution 可迁移说明

## 当前目标

把 `stock-wave-attribution` 做成可复制到其他环境直接使用的 skill。使用方默认只需要修改 `stock-wave-attribution.yaml` 里的数据库连接、`tushare token` 和路径配置。

## 当前归位方案

- 自带 runtime 进入 `skills/stock-wave-attribution/runtime/`
- 编排层进入 `skills/stock-wave-attribution/scripts/`
- 默认配置文件是 `skills/stock-wave-attribution/stock-wave-attribution.yaml`
- 正式服务链默认走 `prepare-agent-rerank -> Codex 100选3-5 粗排 -> finalize-agent-rerank`
- 历史 ChatGPT 浏览器链路仍保留在项目内，但当前正式服务默认不使用

## 当前产物

- `scripts/orchestrator.py`：正式编排入口
  - `run`：保留本地规则归因入口
  - `prepare-agent-rerank / finalize-agent-rerank`：正式服务链的两阶段入口
  - 同时保留历史 `build_wave_attribution_search_prompt()`，仅供兼容性实验或人工排查使用
- `runtime/config.py`：统一配置加载
- `runtime/attribution_data.py`：量价、news、概念验证辅助函数；其中 `news` 先候选召回
- 量价窗口默认从 `event_quant` 读取；若窗口不足，编排层会先用 `Akshare` 补库，再回读数据库继续归因
- 概念映射与概念日线默认也从 `event_quant.ana_stock_concept_map / ana_concept_day` 读取；若为空，编排层会先用 `Tushare 代理` 回填后再回读数据库
- `runtime/wave_segmentation.py`：波段切分
- `runtime/wave_plotting.py`：K 线绘图
- `stock-wave-attribution.yaml`：默认配置模板
- `templates/detailed_report_contract.md`：详细表格版输出合同
- 正式报告默认会给出：
  - `报告时间`
  - `一句话逻辑`
  - `综合裁决`
  - 本地证据收口后的 `主因 / 备选 / 最终判定`
  - 并且只分析 `涨幅 Top2` 波段，每个波段独立输出一套归因
- `tests/test_orchestrator.py`：orchestrator 回归测试
- `scripts/migrate_news_raw_text_reports.py`：历史报告 news 原文迁移脚本
  - 当前统一按 `波段 Wn` 展开正文
  - 每个波段固定包含：`证据原文 / 量价验证表 / 概念联动验证表 / 结论与置信度表 / 综合裁决`
  - 波段概览会补充 `粗排新闻来源分布`
  - `证据原文` 按发布时间升序展示，并用 fenced code block 原样放数据库全文
  - 服务模式下，news 正式链路是：
    - 先按波段窗口召回粗排候选
    - 再按标题去重
    - 再按 `100选3-5` 分 chunk 交给 Codex 直接入围
    - 最后从粗排并集里直接精选最终 10 条，不做逐条打分
  - 当前默认来源包含：`zsxq_zhuwang / zsxq_damao / zsxq_saidao_touyan`

- `SKILL.md`：项目内 skill 使用口径
- `skill_manifest.json`：依赖、边界、输出目标
- `scripts/project_skill.py`：最小可运行入口
- `tests/test_project_skill.py`：入口回归测试

## 使用方式

1. 复制整个 `skills/stock-wave-attribution/` 目录
2. 修改 `stock-wave-attribution.yaml`
3. 确保目标环境能访问同结构的 `event_news / event_quant`
4. 单票本地直达命令入口：

```bash
python skills/stock-wave-attribution/scripts/orchestrator.py run \
  --stock-name 五洲新春 \
  --ts-code 603667.SH \
  --start-date 2025-11-05 \
  --end-date 2026-01-22 \
  --sample-label 机器人概念
```

5. 服务模式默认不走 `run_chatgpt_browser`，而是：

```bash
python skills/stock-wave-attribution/scripts/orchestrator.py prepare-agent-rerank \
  --stock-name 五洲新春 \
  --ts-code 603667.SH \
  --start-date 2025-11-05 \
  --end-date 2026-01-22 \
  --sample-label 机器人概念 \
  --task-id attr-demo

python skills/stock-wave-attribution/scripts/orchestrator.py finalize-agent-rerank \
  --stock-name 五洲新春 \
  --ts-code 603667.SH \
  --start-date 2025-11-05 \
  --end-date 2026-01-22 \
  --sample-label 机器人概念 \
  --task-id attr-demo \
  --selection-path data/service_tasks/attr-demo/agent_rerank/final_selection.json
```

## Prompt 标准化

历史联网归因 prompt 仍统一从：

`skills/stock-wave-attribution/scripts/orchestrator.py`

里的 `build_wave_attribution_search_prompt()` 生成。

当前固定字段顺序是：

1. `标的`
2. `波段`
3. `波段涨幅`
4. `样本标签`
5. 固定输出结构
6. 固定四条要求

其中第 `2` 条要求允许按个股注入 `candidate_mainline`，第 `3` 条要求允许按个股注入 `cross_themes`，其余部分保持固定，避免不同案例之间 prompt 漂移。当前正式服务链默认不走这条路径。
