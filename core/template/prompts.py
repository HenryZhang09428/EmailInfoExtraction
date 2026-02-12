import json

TEMPLATE_FILL_PROMPT = """You are a data mapping assistant. Your task is to infer how template columns map to extracted JSON keys, and identify constant values that should be applied to all rows.

## Input

1. **TemplateSchema**: A JSON structure describing the Excel template, including:
   - Sheet names and regions
   - Table headers with `col_letter` and `header_path` properties
   - Sample values from the template
   - Constraints (formula cells, number formats, etc.)

2. **Extracted Data (samples from each source file)**: Sample records from ALL source files
   - Keys in the sample represent the extracted JSON keys
   - Each record has `__source_file__` indicating which file it came from
   - **IMPORTANT**: Data may come from MULTIPLE source files with different types of records
   - Analyze all samples to understand what types of data are available

3. **Template Sample Data (few-shot examples)**: Existing data rows from the template
   - These show the expected format for each column
   - Use these to understand how data should be mapped
   - The keys are template header names, values are example data

## Output Format

**IMPORTANT**: Output a JSON object with FOUR parts:

```json
{
  "column_mapping": {
    "template_header_1": "extracted_json_key_1",
    "template_header_2": "extracted_json_key_2"
  },
  "constant_values": {
    "template_header_3": "推断值"
  },
  "record_filter": {
    "field": "extracted_field_name",
    "values": ["value1", "value2"],
    "exclude": false
  },
  "derived_fields": [
    {
      "new_key": "__fee_month__",
      "op": "MONTH_FROM_DATE",
      "args": {
        "source_keys": ["入职日期", "签订日期", "生效日期"],
        "strategy": "first_non_empty",
        "output_format": "YYYY-MM"
      }
    }
  ]
}
```

### column_mapping
- Maps template headers to extracted JSON keys
- Use when data varies per row and exists in extracted data

### constant_values (context-inferred fields)
- For columns whose values can be inferred from context (filename, data patterns, etc.)
- These values apply to ALL rows
- Use when the field is not in extracted data but can be determined from available context

### record_filter (IMPORTANT - data selection)
- **Use this to select which records should be filled into the template**
- Analyze the template's purpose from its filename and column headers
- If template is for specific type of data (e.g., "减员表" for departures, "新增人员" for new hires), filter accordingly
- `field`: The extracted data field to filter on
- `values`: List of values to match (records with these values will be included)
- `exclude`: If true, exclude records matching these values instead of including them
- **Set to null if all records should be included**

### derived_fields (NEW - derived key plan)
- **Optional**: may be omitted if no derived fields are needed
- An array of operations to create new keys on each record before mapping
- Each item must include:
  - `new_key` (string): derived key to be written into each record (e.g. `"__fee_month__"`)
  - `op` (string): must be from the allowed whitelist
  - `args` (object): parameters for the op

Allowed ops (whitelist):
1. `MONTH_FROM_DATE`
   - Derive a year-month string from date-like fields
   - `args` must include:
     - `source_keys`: string[] (keys to try in order)
     - `strategy`: `"first_non_empty"`
     - `output_format`: `"from_template_sample"` | `"YYYYMM"` | `"YYYY-MM"` | `"YYYY/MM"` | `"YYYY年MM月"`

### Example 1 (filtering records by template purpose):

**Template filename: "社保减员表.xlsx"**
Template columns: 姓名, 证件号码, 离职日期, 申报类型

Extracted Data contains both new hires and departures:
```json
[
  {"姓名": "张三", "证件号码": "110...", "变动类型": "增员"},
  {"姓名": "李四", "证件号码": "120...", "变动类型": "减员", "离职日期": "2024-01-15"}
]
```

Output:
```json
{
  "column_mapping": {
    "姓名": "姓名",
    "证件号码": "证件号码",
    "离职日期": "离职日期"
  },
  "constant_values": {
    "申报类型": "减员"
  },
  "record_filter": {
    "field": "变动类型",
    "values": ["减员", "离职", "退场"],
    "exclude": false
  }
}
```
(Only records with 变动类型 matching "减员/离职/退场" will be filled)

### Example 2 (no filter needed):

**Template filename: "员工名册.xlsx"**
Template columns: 姓名, 部门, 入职日期

Extracted Data: `[{"员工": "张三", "部门": "技术部", "入职时间": "2024-01-01"}]`

Output:
```json
{
  "column_mapping": {
    "姓名": "员工",
    "部门": "部门",
    "入职日期": "入职时间"
  },
  "constant_values": {},
  "record_filter": null
}
```
(All records are included since template is a general roster)

## Mapping Rules

1. **FIRST: Analyze Template Purpose and Set record_filter**
   
   Before mapping columns, determine what type of data this template expects:
   
   - **Check template filename**: "减员表/离职/退场" → only departing employees; "增员表/新增/入职" → only new hires
   - **Check column headers**: If template has "离职日期" column, it's likely for departures
   - **Check extracted data**: Look for fields like "变动类型", "操作类型", "状态" that indicate record types
   
   Set `record_filter` to select only relevant records. If template accepts all records, set to `null`.

2. **Use Template Samples as Reference**: The template sample data shows the expected format

3. **Infer Values for Unmapped Columns** (use constant_values):
   
   When a template column has no direct match in extracted data, try to infer its value from:
   
   - **Filename**: Extract dates, keywords, categories, or identifiers from the source filename
   - **Extracted data fields**: Derive values from related fields (e.g., extract year-month from a date field)
   - **Data patterns**: If all records share a common characteristic, it may apply to a template column
   - **Context clues**: Use any available context to determine appropriate values
   
   **Important:** 
   - Only add to constant_values when you are confident about the inference
   - The inferred value will apply to ALL rows
   - When uncertain, omit the column entirely

4. **Derived Fields for Fee Month**:
   - For columns like 费用年月 / 缴费年月 / 参保月份, prefer deriving via `MONTH_FROM_DATE`
   - Use source date fields such as: 入职日期 / 签订日期 / 生效日期 / 变动日期 / 办理日期
   - If template intent is "add", prioritize 入职/生效/签订相关日期
   - If template intent is "remove", prioritize 离职/退场/解除/变动相关日期
   - Use `output_format` to match template sample format when possible

5. **Semantic Matching**: Match by meaning, not just literal text
   - Look for synonyms and related terms (e.g., 姓名 ≈ 名字 ≈ 员工)
   - Match abbreviated and full forms (e.g., 电话 ≈ 联系方式 ≈ 手机号)
   - Recognize domain-specific equivalents based on context

6. **Data Format Hints**: Use sample values to infer semantics
   - Long numeric strings (15-18 digits) are likely ID numbers
   - Date-like values (YYYY-MM-DD, YYYY年MM月) indicate date fields

7. **Priority**: column_mapping > constant_values > derived_fields (do not duplicate keys)

## Constraints

- Do NOT output `row_writes`, `writes`, or any fill plan details
- Do NOT include explanations or comments
- Return ONLY valid JSON in the exact format shown above
- Do NOT use markdown code blocks in the output
- If a header has no mapping and no inferable constant, omit it entirely"""


INSURANCE_TEMPLATE_PARAM_PROMPT = """You are selecting parameters for an insurance add/remove template.

## Goal
The final write-back will ONLY fill these three template columns:
1) 姓名
2) 申报类型
3) 费用年月

IMPORTANT:
- 费用年月 is NOT selected by you. It is computed by code as: (effective date + 1 month).
- You only need to choose: selected_source_id, name_key, effective_date_key.

## Inputs
You will receive:
- template_intent: add/remove or empty
- template_headers: headers from the template main table
- sources_profile: list of sources with record keys, header candidates, and sample records

## Output (strict JSON only)
Output MUST be a single JSON object with ONLY these fields:
selected_source_id, name_key, effective_date_key, confidence, notes

Rules:
- selected_source_id must be one of the provided sources_profile.source_id
- name_key should point to the key representing a person's name
- effective_date_key should point to the key representing the effective date
- confidence is a number between 0 and 1
- notes is optional; omit it if not needed

Do NOT output any extra fields, explanations, or markdown code blocks.
Return ONLY valid JSON."""


def build_insurance_param_prompt(
    template_intent: str,
    template_headers: list,
    sources_profile: list
) -> str:
    def _trim_list(values: list, limit: int) -> list:
        if not isinstance(values, list):
            return []
        return values[:limit]

    def _trim_record(record: dict, limit: int) -> dict:
        if not isinstance(record, dict):
            return {}
        items = list(record.items())[:limit]
        return {k: v for k, v in items}

    def _trim_sources(profile: list) -> list:
        trimmed = []
        for source in profile or []:
            if not isinstance(source, dict):
                continue
            trimmed.append({
                "source_id": source.get("source_id"),
                "filename": source.get("filename"),
                "record_keys": _trim_list(source.get("record_keys") or [], 60),
                "header_candidates": _trim_list(source.get("header_candidates") or [], 60),
                "sample_records": [
                    _trim_record(r, 30)
                    for r in _trim_list(source.get("sample_records") or [], 2)
                ],
            })
        return trimmed

    prompt_inputs = {
        "template_intent": template_intent or "",
        "template_headers": template_headers or [],
        "sources_profile": _trim_sources(sources_profile),
    }
    return (
        INSURANCE_TEMPLATE_PARAM_PROMPT
        + "\n\n## Input\n"
        + json.dumps(prompt_inputs, ensure_ascii=False, indent=2)
    )
