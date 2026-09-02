# 本地定制化与上游补丁记录 (Local Customizations & Patches)

本文档是本仓库（Fork）对比上游官方仓库（`ZhuLinsen/daily_stock_analysis`）所做的**全部本地优化、绕开方案（Workarounds）与定制功能**的唯一持久化索引。

> [!IMPORTANT]
> **开发与合并准则**：
> 1. 在开启新会话、进行代码重构或同步上游（Upstream Merge）时，**必须严格对照本文档**，严禁误删或随意精简本地定制逻辑。
> 2. 当上游合并发生代码冲突时，以本文档中的“设计初衷与上游追踪”为准进行冲突判定与决策。
> 3. 若上游后续原生支持了某个临时绕开方案的功能，需对照“移除/下线条件”进行完整验证后再做清理。

---

## 目录索引

- [1. 临时绕开方案 (Upstream Workarounds)](#1-临时绕开方案-upstream-workarounds)
  - [1.1 非 A 股（美股/港股）资金流与风控校准跳过](#11-非-a-股美股港股资金流与风控校准跳过)
  - [1.2 yfinance 日期索引安全提取](#12-yfinance-日期索引安全提取)
  - [1.3 美股大盘复盘统一语言配置](#13-美股大盘复盘统一语言配置)
- [2. 业务策略与分析引擎增强 (Engine & Strategies)](#2-业务策略与分析引擎增强-engine--strategies)
  - [2.1 仓位策略三种风险偏好（稳健/激进/保守）差异化推导与 Schema 支持](#21-仓位策略三种风险偏好稳健激进保守差异化推导与-schema-支持)
  - [2.2 大盘环境防护栏“软化”与徽章提示机制](#22-大盘环境防护栏软化与徽章提示机制)
- [3. 静态看板与数据持久化 (Dashboard & Historical Data)](#3-静态看板与数据持久化-dashboard--historical-data)
  - [3.1 独立静态 Web 看板 (Vue 3 + Tailwind CSS)](#31-独立静态-web-看板-vue-3--tailwind-css)
  - [3.2 非线性多维度综合评分算法](#32-非线性多维度综合评分算法)
  - [3.3 历史评分走势图表与 60 天数据持久化](#33-历史评分走势图表与-60-天数据持久化)
- [4. CI/CD 与自动化部署 (Workflows & Cloudflare Pages)](#4-cicd-与自动化部署-workflows--cloudflare-pages)
  - [4.1 GitHub Actions 交易日跳过标记 (`GITHUB_OUTPUT`)](#41-github-actions-交易日跳过标记-github_output)
  - [4.2 数据库脱敏导出与 Cloudflare Pages 自动部署](#42-数据库脱敏导出与-cloudflare-pages-自动部署)

---

## 1. 临时绕开方案 (Upstream Workarounds)

### 1.1 非 A 股（美股/港股）资金流与风控校准跳过
* **核心文件**：[`src/analyzer.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/analyzer.py) (`stabilize_decision_with_structure` 函数)
* **类型**：`临时绕开`
* **问题背景**：非 A 股市场（美股 / 港股）无 A 股特有的逐笔资金流与大单博弈数据。上游主干在调用风控校准时，会因缺失资金流而将正常的看多/持有决策错误降级。
* **本地实现**：
  ```python
  from src.market_context import detect_market
  if detect_market(result.code) != "cn":
      dashboard["decision_stability"] = {
          "applied": False,
          "reason": "非 A 股市场暂不启用资金流及价格区间风控校准",
          "capital_flow_status": "not_supported",
          ...
      }
      return
  ```
* **上游追踪 / 移除条件**：若上游未来在 `stabilize_decision_with_structure` 中原生支持了海外市场的非资金流降级规则或多市场风控分流，方可评估移除此绕开。

### 1.2 yfinance 日期索引安全提取
* **核心文件**：[`data_provider/yfinance_fetcher.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/data_provider/yfinance_fetcher.py) (`_normalize_data` 函数)
* **类型**：`兼容性修复`
* **问题背景**：新旧版本 `yfinance`（尤其是 0.2.54+）以及不同 pandas 版本下，返回的 DataFrame 索引名称可能是 `Date`、`Datetime`、`None` 或 MultiIndex 列，常规 `df.reset_index()` 加重命名容易导致 `date` 列缺失。
* **本地实现**：
  ```python
  if not isinstance(df.index, pd.RangeIndex):
      df['date'] = df.index
  df = df.reset_index(drop=True)
  ```
* **上游追踪 / 移除条件**：作为长期的防御性代码永久保留，防止三方库升级带来的日线解析故障。

### 1.3 美股大盘复盘统一语言配置
* **核心文件**：[`src/market_analyzer.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/market_analyzer.py)
* **类型**：`配置遵循修复`
* **问题背景**：上游早期代码硬编码了 `if self.region == "us": return "en"`，导致配置为中文的用户在分析美股大盘时依然被强制输出英文报告。
* **本地实现**：移除该硬编码判断，统一通过全局 `REPORT_LANGUAGE` 配置决定复盘语言。
* **上游追踪 / 移除条件**：永久生效。

---

## 2. 业务策略与分析引擎增强 (Engine & Strategies)

### 2.1 仓位策略三种风险偏好（稳健/激进/保守）差异化推导与 Schema 支持
* **核心文件**：
  - [`src/agent/risk_override.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/agent/risk_override.py) (`build_risk_style_options`)
  - [`src/analyzer.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/analyzer.py) (Prompt 模板与兜底挂载)
  - [`src/schemas/report_schema.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/schemas/report_schema.py) (`RiskStyleItem` 与 `PositionStrategy.styles`)
  - [`tests/test_report_schema.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/tests/test_report_schema.py)
* **类型**：`核心功能增强`
* **设计初衷**：单一的仓位建议无法满足不同交易风格的用户需求。本优化实现了：
  1. **Prompt 引导**：在 Gemini/LLM 分析模板中增加 `styles: {balanced, aggressive, conservative}` 输出字段规范。
  2. **规则引擎兜底**：当模型未输出 styles 结构时，`build_risk_style_options` 会结合个股多空趋势（得分、买卖建议）、关键阻力突破位及止损线，自动演算出三类风险偏好的建议仓位、进场条件、容忍止损点与风控纪律。
  3. **数据契约保障**：Pydantic Schema 完整支持并校验该字段。

### 2.2 大盘环境防护栏“软化”与徽章提示机制
* **核心文件**：[`src/daily_market_context_guardrail.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/src/daily_market_context_guardrail.py)
* **类型**：`风控体验优化`
* **设计初衷**：
  - 上游原始防护栏在大盘偏弱时会一刀切重写个股的核心结论和计划，丢失了个股独立的 Alpha 研判逻辑。
  - 本地优化后：**保留 LLM 原有个股深度研判与策略**，仅对极端追高进行软化约束，并在 dashboard 中显式注入 `market_guardrail_applied = True`，同时保护 `position_strategy.styles` 结构不被冲掉。

---

## 3. 静态看板与数据持久化 (Dashboard & Historical Data)

### 3.1 独立静态 Web 看板 (Vue 3 + Tailwind CSS)
* **核心文件**：[`public/index.html`](file:///c:/Users/Admin/Project/daily_stock_analysis/public/index.html)
* **类型**：`前端应用`
* **设计初衷**：提供一个无需后端复杂服务即可单文件部署、在任意静态托管（如 GitHub Pages / Cloudflare Pages）上运行的精美股票日报看板。
* **特性**：
  - 响应式侧边栏与大盘复盘全景；
  - 股票卡片支持多维度指标展开、阻力支撑区间仪表盘；
  - 风险警报红点与高亮；
  - 交互式切换“稳健 / 激进 / 保守”三种风险偏好策略视图；
  - 大盘防护栏生效时的“防护栏软化”状态徽章。

### 3.2 非线性多维度综合评分算法
* **核心文件**：[`public/index.html`](file:///c:/Users/Admin/Project/daily_stock_analysis/public/index.html) & [`export_to_json.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/export_to_json.py)
* **类型**：`算法与排序`
* **设计初衷**：单纯的加权平均无法体现技术形态的决定性作用。本地实现了非线性阶梯综合评分：
  - **空头排列（趋势 ≤ 30）**：趋势一票否决，大幅压低情绪权重（`Trend*0.8 + Sentiment*0.2`），严防抄底被套。
  - **多头排列（趋势 ≥ 60）**：释放情绪弹性（`Trend*0.45 + Sentiment*0.55`），放大催化剂乘数效应，对盘后爆雷高敏。
  - **震荡修复**：平衡分配权重（`Trend*0.55 + Sentiment*0.45`）。
  - 看板默认按该综合得分降序排列，快速聚焦高胜率标的。

### 3.3 历史评分走势图表与 60 天数据持久化
* **核心文件**：[`export_to_json.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/export_to_json.py), [`public/score_history.json`](file:///c:/Users/Admin/Project/daily_stock_analysis/public/score_history.json), [`public/index.html`](file:///c:/Users/Admin/Project/daily_stock_analysis/public/index.html)
* **类型**：`数据服务与可视化`
* **设计初衷**：
  - 每次分析完成后，`export_to_json.py` 自动提炼历史数据，滚动维护每只股票过去 60 天的趋势分、情绪分与综合分；
  - 看板内置 Canvas 图表引擎，点击任意个股即可展示近期评分变动趋势，直观追踪多空形态演变。

---

## 4. CI/CD 与自动化部署 (Workflows & Cloudflare Pages)

### 4.1 GitHub Actions 交易日跳过标记 (`GITHUB_OUTPUT`)
* **核心文件**：
  - [`main.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/main.py)
  - [`.github/workflows/00-daily-analysis.yml`](file:///c:/Users/Admin/Project/daily_stock_analysis/.github/workflows/00-daily-analysis.yml)
* **类型**：`自动化流控`
* **设计初衷**：
  - 当非交易日运行分析跳过时，`main.py` 向 `$GITHUB_OUTPUT` 写入 `skipped=true`（正常执行写入 `skipped=false`）；
  - Workflow 根据 `steps.analysis.outputs.skipped` 决定是否跳过后续的通知推送与 Cloudflare Pages 部署步骤，避免在休市日推送重复或空白报告。

### 4.2 数据库脱敏导出与 Cloudflare Pages 自动部署
* **核心文件**：
  - [`export_to_json.py`](file:///c:/Users/Admin/Project/daily_stock_analysis/export_to_json.py)
  - [`.github/workflows/00-daily-analysis.yml`](file:///c:/Users/Admin/Project/daily_stock_analysis/.github/workflows/00-daily-analysis.yml)
* **类型**：`自动化发布`
* **设计初衷**：
  - 分析结束后运行 `export_to_json.py`，从 SQLite 数据库提取最新 7 天内各标的最新分析结果，剥离 `raw_result`、`news_content` 等冗余文本，输出精简高效的 `public/daily_report.json`；
  - 配合 Cloudflare Pages Action，自动将 `public/` 目录同步部署至 CDN 全球加速节点。

---

## 5. 本地定制全景清单速查表

| 序号 | 功能 / 补丁名称 | 所属模块 / 文件 | 改动类型 | 是否为临时 Workaround |
| :--- | :--- | :--- | :--- | :--- |
| **1** | 非 A 股跳过资金流风控校准 | `src/analyzer.py` | 算法风控 | 是（待上游完善海外风控） |
| **2** | yfinance 多格式日期索引提取 | `data_provider/yfinance_fetcher.py` | 数据源兼容 | 否（防御性代码长期保留） |
| **3** | 美股大盘复盘遵循中文配置 | `src/market_analyzer.py` | 语言与国际化 | 否（长期保留） |
| **4** | 三种风险风格策略全链路演算 | `src/agent/risk_override.py`, `src/schemas/report_schema.py`, `src/analyzer.py` | 策略与 Schema | 否（核心定制特性） |
| **5** | 大盘环境防护栏软化与徽章 | `src/daily_market_context_guardrail.py` | 风控与交互 | 否（核心定制特性） |
| **6** | Vue 3 + Tailwind 独立静态看板 | `public/index.html` | 前端看板 | 否（独立应用） |
| **7** | 非线性多维度综合评分 | `public/index.html`, `export_to_json.py` | 排序算法 | 否（核心定制特性） |
| **8** | 60 天评分走势持久化与图表 | `export_to_json.py`, `public/score_history.json` | 数据与可视化 | 否（核心定制特性） |
| **9** | 交易日流控与 Cloudflare 部署 | `main.py`, `export_to_json.py`, `.github/workflows/00-daily-analysis.yml` | CI/CD 与部署 | 否（基础设施扩展） |
