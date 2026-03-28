# Top400 三层口径规范

本文档用于把 `Top400` 候选池与归因口径正式固定为三层：

- `industry`
- `theme_concept`
- `subchain_tag`

从本文档生效开始，不再把 `thematic concept` 作为正式层级名称使用；历史文件或历史报告里出现该词时，一律按 `theme_concept` 理解。

## 1. 适用范围

本规范适用于以下场景：

- `Top400` 候选池分层
- 概念/主题/行业代表股抽样
- 策略卡样本挑选
- 概念级与子链级归因报告
- 后续 CSV、脚本参数、文档命名

本规范暂不直接修改底层行情抓取服务的原始板块枚举。像 [board_top_service.py](/Users/zhengshenghua/Library/Mobile Documents/com~apple~CloudDocs/work/my/case_data/board_top_service.py) 中的 `industry / concept / sector` 仍属于上游数据源板块类型；在 `Top400` 候选池语义层，`concept` 对应收敛为 `theme_concept`。

## 2. 三层正式定义

### 2.1 `industry`

定义：
按主营业务、生产经营属性划分的行业层分类。

判断问题：
这家公司主要是做什么生意的，属于哪个产业部门。

边界：

- 优先表达稳定经营属性，不表达市场短期题材炒作。
- 一家公司通常应有一个主 `industry`，必要时可以保留少量次级补充，但候选池代表股产物以主行业为主。
- 可对接现有行业板块、申万/同花顺行业、或项目后续统一的行业映射表。

示例：

- 半导体
- 汽车零部件
- 工程机械
- 消费电子

### 2.2 `theme_concept`

定义：
由市场叙事、技术主题、政策主题、需求主题或投资主线形成的跨行业主题概念层。

判断问题：
市场为什么会把这些分属不同行业的股票放进同一个题材篮子。

边界：

- 允许跨行业。
- 可直接继承当前 `ana_stock_concept_map`、同花顺概念指数、概念成分等现有“概念”数据源。
- 是本项目中原 `concept` / `thematic concept` / “主题概念”混称的唯一正式替代词。
- 后续文件、脚本、文档一律使用 `theme_concept`，不再新增 `thematic_concept` 命名。

示例：

- 机器人概念
- 商业航天
- 芯片概念
- AI 手机

### 2.3 `subchain_tag`

定义：
在某个 `theme_concept` 内，为了归因和样本分层而维护的更细颗粒度标签，用于表达产业链位置、部件环节、能力分工或应用段落。

判断问题：
在同一主题里，这只股票究竟属于哪一段链条、哪类零件、哪种能力。

边界：

- `subchain_tag` 必须从属于某个 `theme_concept` 语境，不能脱离主题单独理解。
- 它是分析标签，不要求对应第三方现成指数或板块。
- 允许一只股票在同一主题下命中多个 `subchain_tag`。
- 允许人工维护，只要来源、口径、归属主题写清楚。

示例：

- 在 `机器人概念` 下：
  - 减速器
  - 丝杠
  - 伺服系统
  - 机器视觉
- 在 `商业航天` 下：
  - 卫星制造
  - 火箭总装
  - 地面设备

## 3. 层级关系与判定顺序

建议按以下顺序判定：

1. 先确定 `industry`
2. 再确定 `theme_concept`
3. 最后在具体主题下打 `subchain_tag`

原因：

- `industry` 更稳定，适合做底座分类。
- `theme_concept` 承担市场叙事与横向聚类。
- `subchain_tag` 负责把同一主题内部的异质性拆开。

## 4. 历史文件兼容策略

### 4.1 `top400_thematic_concept_reps.csv`

处理原则：

- 当前文件保留，不做破坏性重命名。
- 从语义上将其定义为 `theme_concept` 代表股样本的历史兼容文件。
- 新增文档、README、后续新脚本输出时，正式文件名应使用 `top400_theme_concept_reps.csv`。

兼容映射：

- `top400_thematic_concept_reps.csv`
  - 历史兼容名
- `top400_theme_concept_reps.csv`
  - 后续正式产物名

字段兼容解释：

- 历史字段 `concept_code` 等价于 `theme_concept_code`
- 历史字段 `concept_name` 等价于 `theme_concept_name`

### 4.2 `top400_concept_frequency.csv`

处理原则：

- 当前文件保留。
- 在语义上将其解释为 `theme_concept` 频次统计的历史兼容文件。
- 如果后续需要新产物，可新增 `top400_theme_concept_frequency.csv`，但在未生成前不强制替换现有链路。

## 5. 后续产物命名规则

Top400 三层正式产物名固定如下：

- `top400_industry_reps.csv`
- `top400_theme_concept_reps.csv`
- `top400_subchain_reps.csv`

命名约束：

- 层级名必须使用 `industry / theme_concept / subchain`
- 禁止新建包含 `thematic_concept` 的正式产物名
- 原则上 `reps` 表示“代表股样本表”，不是全量映射表

## 6. 输出字段规则

为了兼容现有 `theme_concept` 代表股表，三层 `reps` 文件统一优先使用以下宽表字段：

- `layer_code`
- `layer_name`
- `stock_count`
- `avg_rank`
- `leader_code`
- `leader_name`
- `leader_rank`
- `leader_gain_pct`
- `median_code`
- `median_name`
- `median_rank`
- `median_gain_pct`
- `edge_code`
- `edge_name`
- `edge_rank`
- `edge_gain_pct`

层级映射说明：

- `top400_industry_reps.csv`
  - `layer_code` / `layer_name` 表示行业
- `top400_theme_concept_reps.csv`
  - `layer_code` / `layer_name` 表示主题概念
- `top400_subchain_reps.csv`
  - `layer_code` / `layer_name` 表示子链标签
  - 建议额外增加 `parent_theme_concept_code` 与 `parent_theme_concept_name`

若出于兼容原因继续沿用历史字段名，则必须在 README 或 manifest 中明确映射关系，避免“concept 到底指主题还是子链”再次混淆。

## 7. 数据边界与落地建议

现阶段数据就绪度：

- `industry`
  - 需要补一版稳定的行业归属映射与代表股抽样
- `theme_concept`
  - 已有现成概念映射、概念频次和历史代表股样本，可率先规范化
- `subchain_tag`
  - 仍需要基于具体主题建立人工/规则化标签体系

因此最短落地顺序是：

1. 先把 `theme_concept` 命名和兼容关系定死
2. 再补 `industry` 代表股样本
3. 最后围绕重点主题构建 `subchain_tag` 体系与样本
