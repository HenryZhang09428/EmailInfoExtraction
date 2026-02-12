# 大文件处理说明

## 问题描述

当上传行数较多的Excel文件时，可能会遇到"No extracted data available"的错误。这通常是因为：

1. **CSV文本过长**：Excel文件被转换为CSV文本后，如果行数太多，文本会非常长
2. **LLM Token限制**：过长的文本可能超过LLM的token限制，导致API调用失败
3. **内存问题**：处理大文件时可能消耗过多内存
4. **超时问题**：处理大文件需要更长时间，可能超过请求超时时间

## 解决方案

### 1. 自动截断和采样

系统现在会自动处理大文件：

- **行数限制**：默认最多处理1000行数据（不包括表头）
- **字符数限制**：CSV文本最多100K字符
- **智能采样**：如果文件超过500行，会使用采样策略（每N行取1行）
- **保留表头**：始终保留前两行作为表头，确保数据结构完整

### 2. 警告信息

如果数据被截断，系统会在提取结果中添加警告信息：

```json
{
  "warnings": [
    "文件包含 5000 行数据，仅处理前 1002 行（包括表头）"
  ],
  "metadata": {
    "total_rows": 5000,
    "processed_rows": 1002,
    "truncated": true
  }
}
```

### 3. 错误处理改进

即使提取失败，系统也会返回包含错误信息的JSON，而不是显示"No extracted data available"：

```json
{
  "error": "Failed to extract JSON from Excel via LLM: ...",
  "warnings": ["LLM提取失败: ..."],
  "metadata": {
    "total_rows": 5000,
    "processed_rows": 1002,
    "truncated": true
  },
  "csv_preview": "..."
}
```

## 配置参数

如果需要调整处理大文件的参数，可以修改 `core/extractors/excel_extractor.py` 中的常量：

```python
MAX_ROWS_TO_PROCESS = 1000  # 最多处理的行数（不包括表头）
MAX_CSV_CHARS = 100000      # CSV文本最大字符数（约100K）
MIN_ROWS_FOR_SAMPLING = 500 # 超过这个行数时使用采样策略
```

### 调整建议

- **如果文件通常小于500行**：可以增加 `MAX_ROWS_TO_PROCESS` 到 2000 或更多
- **如果LLM支持更大的token限制**：可以增加 `MAX_CSV_CHARS` 到 200000 或更多
- **如果希望处理更多数据**：可以同时增加两个参数

**注意**：增加这些参数可能会：
- 增加API调用成本（更多token）
- 增加处理时间
- 增加内存使用
- 可能仍然会遇到LLM的token限制

## 最佳实践

### 1. 预处理大文件

如果文件非常大（超过1000行），建议：

1. **拆分文件**：将大文件拆分成多个小文件
2. **筛选数据**：只保留需要处理的数据行
3. **使用数据透视表**：如果可能，先汇总数据再处理

### 2. 检查提取结果

处理大文件后，检查 `metadata` 字段：

```python
if extracted_json.get("metadata", {}).get("truncated"):
    print("警告：数据被截断，可能不完整")
    print(f"总行数: {extracted_json['metadata']['total_rows']}")
    print(f"已处理: {extracted_json['metadata']['processed_rows']}")
```

### 3. 使用警告信息

系统会在 `warnings` 字段中提供详细信息，建议：

- 检查是否有警告信息
- 根据警告信息决定是否需要重新处理文件
- 如果数据被截断，考虑拆分文件或调整参数

## 故障排查

### 问题：仍然显示"No extracted data available"

**可能原因**：
1. 文件读取失败（格式问题、损坏等）
2. LLM API调用失败（网络问题、API密钥问题等）
3. 其他未捕获的异常

**解决方法**：
1. 检查控制台日志，查看具体错误信息
2. 检查 `extracted_json` 中的 `error` 字段
3. 确认文件格式正确（.xlsx 或 .xls）
4. 检查网络连接和API配置

### 问题：数据被截断，需要处理完整数据

**解决方法**：
1. 增加 `MAX_ROWS_TO_PROCESS` 参数
2. 增加 `MAX_CSV_CHARS` 参数
3. 将文件拆分成多个小文件分别处理
4. 使用采样策略处理代表性数据

### 问题：处理速度慢

**可能原因**：
- 文件太大
- LLM API响应慢
- 网络延迟

**解决方法**：
1. 减少 `MAX_ROWS_TO_PROCESS` 参数
2. 使用采样策略
3. 检查网络连接
4. 考虑使用更快的LLM模型

## 技术细节

### Excel提取流程

1. **读取Excel**：使用 `pandas.read_excel()` 读取第一个工作表
2. **转换为CSV**：将DataFrame转换为CSV格式的文本
3. **长度检查**：检查行数和字符数，决定是否需要截断或采样
4. **调用LLM**：将CSV文本发送给LLM进行结构化提取
5. **返回结果**：返回包含提取数据和元信息的JSON

### 采样策略

当文件超过 `MIN_ROWS_FOR_SAMPLING` 行时：

1. 计算采样步长：`step = (total_rows - 2) // MAX_ROWS_TO_PROCESS`
2. 按步长采样行：`sampled_indices = range(2, total_rows, step)`
3. 限制采样数量：最多 `MAX_ROWS_TO_PROCESS` 行
4. 确保表头完整：始终包含前两行

这样可以：
- 保持数据的代表性
- 控制处理的数据量
- 减少API调用成本

## 更新日志

- **2026-01-26**：添加大文件处理支持
  - 自动截断和采样
  - 改进错误处理
  - 添加警告和元数据信息
