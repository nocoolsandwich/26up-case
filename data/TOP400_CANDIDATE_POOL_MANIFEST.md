# Top400 Candidate Pool Manifest

本文件定义 `Top400` 三层候选池正式产物、输入来源和输出字段约束。

## 1. 正式产物

当前项目后续应分别产出：

- `top400_industry_reps.csv`
- `top400_theme_concept_reps.csv`
- `top400_subchain_reps.csv`

其中：

- `top400_theme_concept_reps.csv` 是正式产物名
- `top400_thematic_concept_reps.csv` 是历史兼容文件名，当前继续保留，供旧链路引用

## 2. 各产物输入来源

### 2.1 `top400_industry_reps.csv`

目标：
从 `Top400` 股票池中，按行业层抽取代表股样本。

最小输入：

- `data/wencai_top400_*.csv`
  - 提供 `Top400` 股票名单、区间排名、区间涨幅
- 行业映射来源
  - 可来自现有数据库映射表、行业板块成分映射、或后续维护的行业归属表

生成规则：

- 先按股票映射到主 `industry`
- 再按每个行业的命中股票生成 `stock_count` 与 `avg_rank`
- 之后分别挑选 `leader / median / edge` 三类代表股

### 2.2 `top400_theme_concept_reps.csv`

目标：
从 `Top400` 股票池中，按主题概念层抽取代表股样本。

最小输入：

- `data/wencai_top400_*.csv`
- `event_quant.ana_stock_concept_map` 或等价的概念映射
- `data/top400_concept_frequency.csv`
  - 当前可作为频次统计的兼容输入
- `data/top400_thematic_concept_reps.csv`
  - 当前可作为历史基线样本

生成规则：

- 以 `theme_concept` 为分组键
- 概念命中股票数写入 `stock_count`
- `avg_rank` 为概念内股票在 `Top400` 里的平均排名
- `leader` 取该主题中排名最靠前的强势样本
- `median` 取排名居中的代表样本
- `edge` 取仍属于该主题、但位于主题样本尾层的代表样本

### 2.3 `top400_subchain_reps.csv`

目标：
在重点 `theme_concept` 内继续细分子链标签代表股样本。

最小输入：

- `data/wencai_top400_*.csv`
- `top400_theme_concept_reps.csv` 或其历史兼容文件
- 面向具体主题维护的 `subchain_tag` 映射表
  - 可人工维护
  - 可规则生成
  - 但必须标明 `parent_theme_concept`

生成规则：

- 以 `parent_theme_concept + subchain_tag` 为分组键
- 同一只股票允许落入多个 `subchain_tag`
- `leader / median / edge` 的挑选逻辑与前两层一致

## 3. 统一输出字段

三份 `reps` 文件统一使用以下字段：

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

`top400_subchain_reps.csv` 额外要求：

- `parent_theme_concept_code`
- `parent_theme_concept_name`

## 4. 历史字段兼容

如果短期内仍复用旧版 `top400_thematic_concept_reps.csv`，则按下述方式解释：

- `concept_code` => `layer_code`
- `concept_name` => `layer_name`
- 其余 `leader / median / edge` 字段不变

这意味着旧文件目前仍可继续被策略卡和分析稿引用，但新脚本、新文档、新数据出口不应再写出 `thematic_concept` 这个名字。

## 5. 当前落地状态

截至当前版本：

- `theme_concept`
  - 已有历史样本和频次表，可立即进入正式命名收敛
- `industry`
  - 尚未产出正式 `reps` 文件
  - 但已补出 `top400_industry_map.csv` 与 `top400_industry_frequency.csv` 两个临时产物，可先支撑分布盘点
  - 当前行业口径来自 `tushare stock_basic.industry`，仍不是申万/同花顺正式行业体系
- `subchain_tag`
  - 尚未产出正式 `reps` 文件，需要先确定父主题与标签映射

因此，本轮先完成“规范、命名、兼容契约”落地，不强行伪造尚未验证的 `industry` / `subchain_tag` CSV。

补充说明：

- `data/top400_theme_concept_top15_random3.csv`
  - 当前是去噪后 Top15 `theme_concept` 的固定种子随机样本池
  - 仅作为待办与归因选样辅助文件
  - 不视为正式 `reps` 产物
