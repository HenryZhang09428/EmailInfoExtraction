from typing import Union, List

def validate(final_json: Union[dict, list], attribute_set: List[dict]) -> dict:
    cells = []
    
    if isinstance(final_json, list):
        if len(final_json) == 0:
            return {
                "cells": [],
                "constraint_pass_rate": 0.0,
                "needs_human_review": True
            }
        final_json = final_json[0]
    
    if not isinstance(final_json, dict):
        return {
            "cells": [],
            "constraint_pass_rate": 0.0,
            "needs_human_review": True
        }
    
    attribute_map = {}
    for attr in attribute_set:
        if isinstance(attr, dict) and "name" in attr:
            attr_name = attr["name"]
            attribute_map[attr_name] = attr
    
    for key, value in final_json.items():
        ok = True
        reason = None
        
        if key in attribute_map:
            attr_def = attribute_map[key]
            
            if "required" in attr_def and attr_def["required"]:
                if value is None or value == "":
                    ok = False
                    reason = "Required field is missing or empty"
            
            if "type" in attr_def and value is not None:
                expected_type = attr_def["type"]
                if expected_type == "string" and not isinstance(value, str):
                    ok = False
                    reason = f"Expected string, got {type(value).__name__}"
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        ok = False
                        reason = f"Expected number, got {type(value).__name__}"
                elif expected_type == "date" and not isinstance(value, str):
                    ok = False
                    reason = f"Expected date string, got {type(value).__name__}"
        
        cells.append({
            "name": key,
            "value": value,
            "ok": ok,
            "reason": reason
        })
    
    total_cells = len(cells)
    if total_cells == 0:
        constraint_pass_rate = 0.0
    else:
        passed_cells = sum(1 for cell in cells if cell["ok"])
        constraint_pass_rate = passed_cells / total_cells
    
    needs_human_review = constraint_pass_rate < 0.8 or any(not cell["ok"] for cell in cells)
    
    return {
        "cells": cells,
        "constraint_pass_rate": constraint_pass_rate,
        "needs_human_review": needs_human_review
    }
