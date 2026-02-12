from app.backend.process import apply_fill_plan_overrides


def test_apply_fill_plan_overrides_filters_and_overrides():
    fill_plan = {
        "row_writes": [
            {
                "start_cell": "A2",
                "rows": [{"a": 1}],
                "column_mapping": {"姓名": "B", "证件号码": "D", "手机号": "E"},
            }
        ]
    }
    result = apply_fill_plan_overrides(
        fill_plan,
        fill_columns=["B", "C", "F"],
        special_field_to_column={"证件号码": "C", "手机号": "F"},
    )
    mapping = result["row_writes"][0]["column_mapping"]
    assert mapping["姓名"] == "B"
    assert mapping["证件号码"] == "C"
    assert mapping["手机号"] == "F"
    assert set(mapping.values()) == {"B", "C", "F"}
