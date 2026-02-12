## EXCEL_SCHEMA_INFER_PROMPT
You infer a column-level schema from an Excel table summary. Return ONLY valid JSON (no markdown, no explanation).

Input is provided as JSON in the prompt under `INPUT_JSON`, with fields:
- total_rows, data_rows, columns
- header_row_1, header_row_2 (arrays)
- column_summaries: array of objects with:
  - column_index (1-based)
  - header_path (string, derived from header rows)
  - non_empty_ratio (0-1)
  - unique_count (optional)
  - samples (array of sample values)

Output JSON schema:
```json
{
  "semantic_key_by_header": {
    "<header_path>": "<semantic_key>"
  },
  "row_filter": {
    "min_nonempty_ratio": 0.2,
    "exclude_if_contains_any": ["合计", "备注", "总计", "合并"],
    "required_fields_any": ["name", "id"]
  },
  "normalization": {
    "date_fields": [],
    "id_fields": [],
    "phone_fields": []
  }
}
```

Rules:
- `semantic_key_by_header` keys MUST be existing `header_path` values from INPUT_JSON.
- `semantic_key` must be a normalized semantic label in snake_case (e.g. name, id_number, employee_id, start_date, end_date, phone, address, amount).
- Be generic and avoid business-specific hardcoding.
- If uncertain, return an empty string for that header_path (do NOT invent new header paths).
- `row_filter` and `normalization` are optional; include only when helpful.

## EMAIL_TO_JSON_PROMPT
Extract structured information from the email content below. Identify key fields such as sender, recipient, subject, date, body content, and any structured data. Return the result as a JSON object.

## EML_BODY_TO_JSON_PROMPT
You are extracting structured records from an email BODY text (already normalized and truncated).

Return ONLY valid JSON (no markdown, no explanation) with this EXACT schema:
```json
{
  "data": [
    { "field": "value" }
  ],
  "metadata": {},
  "warnings": []
}
```

Rules:
- Output MUST be a JSON OBJECT (not an array).
- `data` MUST be a JSON ARRAY. Each item is one record (object).
- `metadata` MUST be a JSON OBJECT (can be empty).
- `warnings` MUST be a JSON ARRAY of strings (can be empty).
- All record field values MUST be strings. Use empty string "" for missing/unknown values. Do NOT output null.

Extraction guidance (generic, no hardcoded "增员/减员"):
- If the body contains personnel changes / roster changes / employee movements, extract each person as one record.
- If the body only says "see attachment" and contains no explicit person list or fields, `data` is allowed to be empty.
- Prefer extracting fields that commonly appear in HR / social insurance / onboarding/offboarding emails, such as:
  - 姓名 / 员工姓名 / 姓名拼音
  - 证件号码 / 身份证号 / 护照号
  - 变动类型 / 办理事项 / 操作类型 / 申报类型
  - 入职日期 / 离职日期 / 生效日期 / 变动日期 / 办理日期
  - 公司 / 部门 / 岗位 / 城市 / 缴费地 / 参保地
  - 联系方式

Robustness:
- If you are unsure about a field, still include the record with what you know.
- Put ambiguity notes into `warnings`.

## ATTRIBUTE_SET_PROMPT
Based on the header rows and sample data provided, generate an attribute set definition. Each attribute should include: name, type (string/number/date), and whether it's required. Return a JSON array of attribute definitions.

## FINAL_MAPPING_PROMPT
Map the extracted information to the target schema based on the attribute set provided. Match fields from the raw data and extracted info to the target attributes. Return a JSON object with the mapped fields.

## EMAIL_LEAVE_LINES_TO_JSON_PROMPT
You are extracting leave/resignation records from email body lines.

Return ONLY valid JSON (no markdown, no explanation) with this EXACT schema:
```json
{
  "data": [
    {"name": "", "employee_id": "", "leave_date_text": "", "intent": "remove", "note": ""}
  ],
  "metadata": {"source": "email_body_leave_lines"},
  "warnings": []
}
```

Field definitions:
- `name`: Employee name (姓名). Required if available.
- `employee_id`: Employee ID or ID number (工号/身份证号). Use empty string "" if not found.
- `leave_date_text`: The raw date text as it appears in the email (e.g. "11月1日", "2024年3月15日", "3/15"). Keep original text even without year; year will be inferred by code. Use empty string "" if not found.
- `intent`: MUST always be "remove". Do NOT output any other value.
- `note`: Any additional context or remarks. Use empty string "" if none.

Rules:
- Output MUST be a JSON OBJECT (not an array).
- `data` MUST be a JSON ARRAY. Each item is one leave/resignation record.
- `metadata` MUST contain `{"source": "email_body_leave_lines"}`.
- `warnings` MUST be a JSON ARRAY of strings (can be empty).
- All field values MUST be strings. Use empty string "" for missing/unknown values. Do NOT output null.
- Do NOT add any extra fields beyond: name, employee_id, leave_date_text, intent, note.
- `intent` is ALWAYS "remove" — do not infer or change this value.
