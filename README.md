1
## 2. 核心模块与技术实现细节### 2.1 路由策略
- 基于扩展名白名单进行路由：Excel / Email / Image / Text / Other。
- 对未知扩展名降级为 `other`，避免二进制误解析。
- 每个文件生成唯一 `source_id` 与绝对路径，形成统一的 `SourceDoc` 输入对象。

### 2.2 邮件的提取策略
- 支持 `.eml` / `.txt` / `.docx` 三类邮件来源，按类型使用不同解析器。
- `.eml` 解析 headers 与 text/html bodies，并对 HTML 做去标签与空白归一化。
- 邮件线程截断：识别“原始邮件/Forward/On ... wrote”等标记，仅保留最新一段正文。
- 文本长度上限控制（默认 15k 字符），用于限制 LLM 上下文消耗。
- 附件导出采用 sha256 去重，基于 content-type 与文件名推断扩展名并记录元数据。
- 正文抽取由 LLM 输出固定 JSON 结构，内置纠错与 warnings 聚合。

### 2.3 抽取引擎：从“采样”到“语义理解”
Excel 抽取逻辑：采样 + 表头推断  
避免全量读表：系统仅抽取前若干行和均匀分布的样本行，减少 LLM 上下文消耗。  
表头推断（Header Inference）：  
通过“连续非空”与“非空密度”判定可能的 header 行；  
支持双层 header（header1/header2），用于增强语义上下文；  
形成“header_path”（如 “部门 / 子部门”）用于增强语义识别。  
列摘要与样本值：每列提取典型样本值，作为 LLM 语义理解输入。  
大表策略：对于数据行极多的 Excel，仅抽取代表性样本；同时限制 CSV 字符量上限。  
多模态融合（Vision + Text）  
OCR 通道：使用 Tesseract 获取原始文本作为证据层。  
Vision LLM 通道：  
让视觉模型输出 JSON（summary / extracted_fields / tables / numbers）  
表格强约束：强制按“行”提取整表，避免传统 OCR 的单元格错位问题  
融合策略：  
Vision 输出为主  
OCR 输出用于补充字段与纠错（例如 Vision 表格行数不足时启用 OCR 修复）

## Backend Usage (CLI / API)

### Templates
- Place the templates in `app/templates/` with these filenames:
  - `目标1模版：社保增员表.xlsx`
  - `目标2模版：社保减员表.xlsx`
- Or override by environment variables:
  - `TEMPLATE_ADD_PATH`
  - `TEMPLATE_REMOVE_PATH`
  - `TEMPLATE_ADD_NAME`
  - `TEMPLATE_REMOVE_NAME`
- Prompts file can be overridden by `PROMPT_FILE` (default: `prompt.md` at project root).

### CLI
Run from repo root:
```
python -m app.cli --inputs /path/to/files /path/to/dir --output-dir .
```

Optional output JSON controls:
```
python -m app.cli --inputs /path/to/files --output-dir . --output-json-name custom.json
python -m app.cli --inputs /path/to/files --output-dir . --output-json-timestamp
```

### API
Start server:
```
uvicorn app.api:app --host 0.0.0.0 --port 8000
```

Process via JSON body (paths):
```
POST /process
{
  "paths": ["/path/to/file1.eml", "/path/to/file2.xlsx"],
  "require_llm": false
}
```

Process via multipart upload:
```
POST /process (multipart/form-data, files=[])
```
