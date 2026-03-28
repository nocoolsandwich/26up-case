# stock-wave-attribution 可迁移说明

## 当前目标

把 `stock-wave-attribution` 做成可复制到其他环境直接使用的 skill。使用方默认只需要修改 `stock-wave-attribution.yaml` 里的数据库连接、`tushare token` 和路径配置。

## 当前归位方案

- 自带 runtime 进入 `skills/stock-wave-attribution/runtime/`
- 编排层进入 `skills/stock-wave-attribution/scripts/`
- 默认配置文件是 `skills/stock-wave-attribution/stock-wave-attribution.yaml`
- ChatGPT 浏览器链路由项目内 `skills/chatgpt-plus-browser/` 提供

## 当前产物

- `scripts/orchestrator.py`：正式编排入口
  - 内含标准化 ChatGPT 联网归因 prompt 生成函数 `build_wave_attribution_search_prompt()`
- `runtime/config.py`：统一配置加载
- `runtime/attribution_data.py`：量价、news、概念验证辅助函数；其中 `news` 证据统一输出数据库原文
- `runtime/wave_segmentation.py`：波段切分
- `runtime/wave_plotting.py`：K 线绘图
- `stock-wave-attribution.yaml`：默认配置模板
- `templates/detailed_report_contract.md`：详细表格版输出合同
- `tests/test_orchestrator.py`：orchestrator 回归测试
- `scripts/migrate_news_raw_text_reports.py`：历史报告 news 原文迁移脚本
  - 当前统一把 `本地 news 库证据` 渲染成两段：
  - 上半段是元信息表：`序号 / 时间 / 来源 / 标题 / 链接`
  - 下半段是逐条 `证据原文`，用 fenced code block 原样放数据库全文

- `SKILL.md`：项目内 skill 使用口径
- `skill_manifest.json`：依赖、边界、输出目标
- `scripts/project_skill.py`：最小可运行入口
- `tests/test_project_skill.py`：入口回归测试

## 使用方式

1. 复制整个 `skills/stock-wave-attribution/` 目录
2. 修改 `stock-wave-attribution.yaml`
3. 确保目标环境能访问同结构的 `event_news / event_quant`
4. 如需归因搜索，确保 `chatgpt-plus-browser` 对应脚本路径可用
5. 如同一轮要发多只股票，默认串行提交 ChatGPT 搜索任务，不并发提交
6. 如果 ChatGPT 搜索结果在报告生成时还没写入，先把 `未写入 + task id + prompt + 结果文件路径` 写进报告，后续优先查结果文件补写
7. 回填时优先读取 `.state/<task-id>.json`；当 `status=done` 且 `resultText` 非空时，直接视为结果可用

## Prompt 标准化

联网归因 prompt 现在统一从：

`skills/stock-wave-attribution/scripts/orchestrator.py`

里的 `build_wave_attribution_search_prompt()` 生成。

当前固定字段顺序是：

1. `标的`
2. `波段`
3. `波段涨幅`
4. `样本标签`
5. 固定输出结构
6. 固定四条要求

其中第 `2` 条要求允许按个股注入 `candidate_mainline`，第 `3` 条要求允许按个股注入 `cross_themes`，其余部分保持固定，避免不同案例之间 prompt 漂移。
