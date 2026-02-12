from core.template.fill_planner import _apply_record_type_filter


def _build_extracted(records):
    return {
        "sources": [
            {
                "filename": "source.xlsx",
                "source_type": "excel",
                "extracted": {"data": records},
            }
        ],
        "merged": {}
    }


def test_remove_template_filters_add_records():
    records = [
        {"name": "A", "termination_reason": "离职", "__sheet_name__": "减员"},
        {"name": "B", "start_date": "2025-01-01", "__sheet_name__": "增员"},
    ]
    extracted = _build_extracted(records)
    filtered = _apply_record_type_filter(extracted, "remove")
    filtered_records = filtered["sources"][0]["extracted"]["data"]
    names = [r.get("name") for r in filtered_records]
    assert "A" in names
    assert "B" not in names


def test_add_template_filters_remove_records():
    records = [
        {"name": "A", "termination_date": "2025-01-01", "__sheet_name__": "减员"},
        {"name": "B", "start_date": "2025-01-01", "__sheet_name__": "增员"},
    ]
    extracted = _build_extracted(records)
    filtered = _apply_record_type_filter(extracted, "add")
    filtered_records = filtered["sources"][0]["extracted"]["data"]
    names = [r.get("name") for r in filtered_records]
    assert "B" in names
    assert "A" not in names


def test_sheet_name_fallback_when_no_semantic_keys():
    records = [
        {"name": "A", "__sheet_name__": "示例减员"},
        {"name": "B", "__sheet_name__": "示例增员"},
    ]
    extracted = _build_extracted(records)
    filtered = _apply_record_type_filter(extracted, "remove")
    filtered_records = filtered["sources"][0]["extracted"]["data"]
    names = [r.get("name") for r in filtered_records]
    assert names == ["A"]
