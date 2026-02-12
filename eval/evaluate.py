import json
import csv
from pathlib import Path
from core.pipeline import run_pipeline, update_ir_scores
from core.mapping.attribute_set import build_attribute_set
from core.mapping.mapper import map_to_schema
from core.config import get_settings
from core.llm import LLMClient
from core.prompts_loader import get_prompts

def deep_diff(pred: any, gt: any, path: str = "") -> list[dict]:
    diffs = []
    
    if isinstance(pred, dict) and isinstance(gt, dict):
        all_keys = set(pred.keys()) | set(gt.keys())
        for key in all_keys:
            new_path = f"{path}.{key}" if path else key
            if key not in pred:
                diffs.append({
                    "path": new_path,
                    "type": "missing_in_pred",
                    "predicted": None,
                    "groundtruth": gt[key]
                })
            elif key not in gt:
                diffs.append({
                    "path": new_path,
                    "type": "extra_in_pred",
                    "predicted": pred[key],
                    "groundtruth": None
                })
            else:
                diffs.extend(deep_diff(pred[key], gt[key], new_path))
    elif isinstance(pred, list) and isinstance(gt, list):
        max_len = max(len(pred), len(gt))
        for i in range(max_len):
            new_path = f"{path}[{i}]"
            if i >= len(pred):
                diffs.append({
                    "path": new_path,
                    "type": "missing_in_pred",
                    "predicted": None,
                    "groundtruth": gt[i]
                })
            elif i >= len(gt):
                diffs.append({
                    "path": new_path,
                    "type": "extra_in_pred",
                    "predicted": pred[i],
                    "groundtruth": None
                })
            else:
                diffs.extend(deep_diff(pred[i], gt[i], new_path))
    else:
        if type(pred) != type(gt):
            diffs.append({
                "path": path,
                "type": "type_mismatch",
                "predicted": pred,
                "groundtruth": gt
            })
        elif pred != gt:
            diffs.append({
                "path": path,
                "type": "value_mismatch",
                "predicted": pred,
                "groundtruth": gt
            })
    
    return diffs

def run_evaluation():
    eval_dir = Path(__file__).parent
    fixtures_dir = eval_dir / "fixtures"
    
    sample_xlsx = fixtures_dir / "sample.xlsx"
    sample_email_docx = fixtures_dir / "sample_email.docx"
    groundtruth_json = fixtures_dir / "groundtruth.json"
    target_schema_csv = fixtures_dir / "target_schema_sample.csv"
    
    file_paths = [str(sample_xlsx), str(sample_email_docx)]
    
    settings = get_settings()
    llm = LLMClient()
    prompts = get_prompts()
    
    print("Running pipeline...")
    ir = run_pipeline(file_paths)
    
    print("Building attribute set...")
    header_rows = []
    sample_rows = []
    
    with open(target_schema_csv, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)
        if len(rows) >= 2:
            header_rows = [rows[0], rows[1]]
        if len(rows) >= 4:
            sample_rows = [rows[2], rows[3]]
    
    attribute_set = build_attribute_set(header_rows, sample_rows, llm, prompts)
    
    print("Mapping to schema...")
    final_json = map_to_schema(ir, attribute_set, None, llm, prompts)
    
    print("Validating...")
    update_ir_scores(ir, final_json, attribute_set)
    
    print("Loading groundtruth...")
    with open(groundtruth_json, 'r', encoding='utf-8') as f:
        groundtruth = json.load(f)
    
    print("Computing diff...")
    diffs = deep_diff(final_json, groundtruth)
    
    report = {
        "predicted": final_json,
        "groundtruth": groundtruth,
        "diff": diffs,
        "scores": ir.scores
    }
    
    report_path = eval_dir / "report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"Report saved to {report_path}")
    
    issues = []
    issues.append("# Auto-detected Issues\n\n")
    
    if not diffs:
        issues.append("✅ No differences found between predicted and groundtruth.\n")
    else:
        issues.append(f"Found {len(diffs)} difference(s):\n\n")
        for diff in diffs:
            path = diff["path"]
            diff_type = diff["type"]
            pred = diff["predicted"]
            gt = diff["groundtruth"]
            
            if diff_type in ["value_mismatch", "type_mismatch"]:
                issues.append(f"## Field: `{path}`\n")
                issues.append(f"- **Predicted**: `{json.dumps(pred, ensure_ascii=False)}`\n")
                issues.append(f"- **Groundtruth**: `{json.dumps(gt, ensure_ascii=False)}`\n\n")
            elif diff_type == "missing_in_pred":
                issues.append(f"## Field: `{path}` (Missing in prediction)\n")
                issues.append(f"- **Groundtruth**: `{json.dumps(gt, ensure_ascii=False)}`\n\n")
            elif diff_type == "extra_in_pred":
                issues.append(f"## Field: `{path}` (Extra in prediction)\n")
                issues.append(f"- **Predicted**: `{json.dumps(pred, ensure_ascii=False)}`\n\n")
    
    issues_path = eval_dir / "auto_issues.md"
    with open(issues_path, 'w', encoding='utf-8') as f:
        f.write("".join(issues))
    print(f"Issues saved to {issues_path}")
    
    if settings.OPENAI_API_KEY:
        print("Generating correction suggestions...")
        diff_summary = "\n".join([
            f"- {d['path']}: predicted={json.dumps(d['predicted'], ensure_ascii=False)}, groundtruth={json.dumps(d['groundtruth'], ensure_ascii=False)}"
            for d in diffs[:20]
        ])
        
        suggestion_prompt = f"""Based on the following differences between predicted and groundtruth data, provide correction suggestions including:
1. Synonym mappings (e.g., "姓名" -> "Name")
2. Enumeration normalization (e.g., standardizing date formats, currency formats)
3. Field mapping suggestions

Differences:
{diff_summary}

Provide a structured list of correction suggestions."""
        
        try:
            suggestions = llm.chat_text(suggestion_prompt, system=None)
            
            suggestions_path = eval_dir / "correction_suggestions.md"
            with open(suggestions_path, 'w', encoding='utf-8') as f:
                f.write("# Correction Suggestions\n\n")
                f.write(suggestions)
            print(f"Correction suggestions saved to {suggestions_path}")
        except Exception as e:
            print(f"Failed to generate suggestions: {e}")
    
    return report

if __name__ == "__main__":
    run_evaluation()
