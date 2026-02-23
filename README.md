# EmailsInfoExtraction

从多种来源文件（邮件/Excel/图片/文本）中抽取结构化信息，输出统一的 JSON，并可按模板自动填充 Excel（例如社保增员/减员表）。

> 面向第三者的阅读顺序：先看“怎么用”，再看“怎么实现”。

![architecture](architecture.png)

---

## 用法（先上手）

### 你能得到什么

- **一份 JSON 结果**：汇总了输入文件的抽取结果、错误与告警、以及填充产物路径。
- **若配置了模板**：会在输出目录生成填充后的 `.xlsx` 文件（增员/减员或 profile 中定义的多任务模板）。

### 运行前准备

#### 1) 安装依赖

建议使用虚拟环境：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### 2) 配置 LLM（必需）

项目默认通过环境变量或 `.env` 读取配置，**至少需要**：

```bash
export OPENAI_API_KEY="你的key"
```

可选配置（例如私有网关/模型名）：

```bash
export OPENAI_BASE_URL="https://api.openai.com/v1"
export OPENAI_MODEL="gpt-4o-mini"
export OPENAI_VISION_MODEL="gpt-4o-mini"
```

#### 3) 准备提示词文件 `prompt.md`（必需，除非用 `PROMPT_FILE` 指定）

默认读取项目根目录的 `prompt.md`；你也可以用 `PROMPT_FILE` 指向其它路径。该文件需要包含以下二级标题段落（`##`）：

- `## EXCEL_SCHEMA_INFER_PROMPT`
- `## EMAIL_TO_JSON_PROMPT`
- `## EML_BODY_TO_JSON_PROMPT`

如果缺失会报错提示创建。

#### 4) 准备模板（可选，但填表场景通常需要）

默认从 `app/templates/` 查找以下两个文件名：

- `目标1模版：社保增员表.xlsx`
- `目标2模版：社保减员表.xlsx`

也可以用环境变量覆盖：

- `TEMPLATE_ADD_PATH` / `TEMPLATE_REMOVE_PATH`：直接指定模板路径
- `TEMPLATE_ADD_NAME` / `TEMPLATE_REMOVE_NAME`：覆盖默认文件名（仍从 `app/templates/` 下解析）

#### 5) OCR（可选）

图片抽取会尝试 OCR；如果系统未安装 Tesseract，OCR 会跳过并记录 warning（不会中断流程）。

macOS（Homebrew）示例：

```bash
brew install tesseract
```

---

## 命令行（CLI）

从仓库根目录运行：

```bash
python -m app.cli --inputs /path/to/files /path/to/dir --output-dir ./output
```

常用参数：

- `--inputs`：输入文件/目录（目录会递归收集所有文件）
- `--output-dir`：输出目录（默认 `.`）
- `--require-llm`：在模板填充规划阶段强制使用 LLM（默认会尽量走规则/确定性路径）
- `--profile-path`：指定一个 profile YAML（见下文“Profile”）
- `--company`：内置预设 profile（`顺丰/天草/楚天龙` 或 `shunfeng/tiancao/chutianlong`）
- `--output-json-name`：指定输出 JSON 文件名
- `--output-json-timestamp`：输出 JSON 文件名追加时间戳

示例：使用公司预设 profile

```bash
python -m app.cli --inputs ./data --output-dir ./output --company shunfeng
```

示例：使用自定义 profile 文件

```bash
python -m app.cli --inputs ./data --output-dir ./output --profile-path ./profiles/default.yaml
```

---

## HTTP API（FastAPI）

启动服务：

```bash
uvicorn app.api:app --host 0.0.0.0 --port 8000
```

### 1) 通过 JSON 传入文件路径

请求体示例：

```json
{
  "paths": ["/abs/path/to/a.eml", "/abs/path/to/b.xlsx"],
  "require_llm": false,
  "profile_path": "/abs/path/to/profile.yaml"
}
```

也可用 `company` 选择预设 profile：

```json
{
  "paths": ["/abs/path/to/a.eml"],
  "company": "shunfeng"
}
```

### 2) 通过 multipart 上传文件

向 `POST /process` 以 `multipart/form-data` 上传 `files[]` 即可（服务端会保存到临时目录处理）。

### 3) 下载产物

`/process` 的返回值中包含 `downloads` 字段（含 `download_url`）。调用：

- `GET /download/{file_id}`

即可下载对应文件（填充后的 xlsx 或 result.json）。

---

## 输出内容说明

CLI 与 API 最终都会产出一个结构化结果（并写入 JSON）。核心字段示意：

- `meta`：处理时间、输入列表、输出目录
- `summary`：按 source_type 统计、错误列表
- `fills`：每个模板任务的输出路径、写入单元格数量、warnings 等
- `sources`：每个来源文件的 `source_id` / `source_type` / `extracted`（或错误信息）

---

## Profile（可选但推荐）

Profile 是一个 YAML，用于把“客户/业务差异”外置配置：模板任务列表、关键词、表头强制映射、路由与 Excel 选表策略等。

- **示例**：`profiles/default.yaml`（带中文注释）
- **使用方式**：CLI `--profile-path` 或 API JSON body 的 `profile_path`

---

## 配置项速查（环境变量）

### LLM

- `OPENAI_API_KEY`（必需）
- `OPENAI_BASE_URL`
- `OPENAI_MODEL`
- `OPENAI_VISION_MODEL`
- `TEMPERATURE`
- `REQUEST_TIMEOUT`

### 提示词

- `PROMPT_FILE`：提示词 markdown 文件路径（默认 `./prompt.md`）

### 模板

- `TEMPLATE_ADD_PATH` / `TEMPLATE_REMOVE_PATH`
- `TEMPLATE_ADD_NAME` / `TEMPLATE_REMOVE_NAME`

### 输出

- `OUTPUT_JSON_NAME`
- `OUTPUT_JSON_TIMESTAMP`（`1/true/yes/on`）

---

## 常见问题（Troubleshooting）

- **报错 `OPENAI_API_KEY is not set or empty`**：配置 `.env` 或导出 `OPENAI_API_KEY`。
- **报错 `Prompt file not found: .../prompt.md`**：创建 `prompt.md`（包含三个 `##` 段落），或设置 `PROMPT_FILE` 指向正确位置。
- **图片没有 OCR 结果**：确认系统安装了 Tesseract（`tesseract -v`）；未安装时不会中断流程，只会降级为纯视觉模型/或跳过 OCR。

---

## 技术与实现（再看原理）

### 总体流程

核心流水线是“提取 → 规范化 → 填充”：

- **路由**：`core/router.py` 将输入文件按扩展名路由到不同 `SourceDoc` 类型。
- **队列抽取（BFS）**：`core/extract/queue_runner.py` 以队列方式处理衍生文件（如邮件附件导出后的二次抽取）。
- **ExtractorRegistry**：`core/extract/registry.py` 管理各类 extractor（Excel/Email/Image/Text/Other）。
- **IR（中间表示）**：`core/ir.py` 统一承载不同来源的 blocks 与 extracted 结果。
- **模板填充**：`core/template/*` 解析模板结构，`core/fill.py` 负责计划与写入。

### 路由策略

- 基于扩展名白名单路由：Excel / Email / Image / Text / Other。
- 对未知扩展名降级为 `other`，避免二进制误解析。
- 每个文件生成唯一 `source_id`，并记录父子关系（用于附件与派生文件追踪）。

### 邮件抽取策略

- 支持 `.eml` / `.txt` / `.docx` 三类邮件来源，按类型使用不同解析器。
- `.eml` 解析 headers 与 text/html bodies，并对 HTML 做去标签与空白归一化。
- 邮件线程截断：识别“原始邮件/Forward/On ... wrote”等标记，仅保留最新一段正文。
- 文本长度上限控制（默认约 15k 字符量级），用于限制 LLM 上下文消耗。
- 附件导出采用 sha256 去重，并记录元数据，进入 BFS 队列继续抽取。

### Excel 抽取：采样 + 表头推断

- 避免全量读表：只抽取前若干行与均匀分布样本行，降低 LLM 成本与延迟。
- 表头推断（Header Inference）：用“连续非空/非空密度”判定 header 行，支持双层 header，并形成 `header_path` 增强语义。
- 列摘要与样本值：每列提取典型样本值，用于语义理解与字段对齐。

### 图片抽取：Vision + OCR 融合

- OCR 通道：使用 Tesseract（`pytesseract`）提取文本作为证据层。
- Vision 通道：视觉模型输出 JSON（`summary/extracted_fields/tables/numbers/warnings`）。
- 融合策略：以 Vision 为主，OCR 用于补充字段与纠错（例如表格行数不足时用 OCR 修复）。

### 模板填充：从“规划”到“写入”

- 解析模板 schema → 构造 payload → 生成 fill plan → 写入并产出调试信息（写入列、单元格计数、warnings）。
- Profile 支持：限制可填列（`fill_columns`）、强制字段映射（`special_field_to_column`）、以及业务关键词与表头语义强制映射等。

---

## 测试（可选）

```bash
pytest -q
```
