import os
import tempfile

import pandas as pd

from core.extractors.excel_extractor import ExcelExtractor, FAST_ROWS_THRESHOLD


class FakeLLM:
    def __init__(self) -> None:
        self.prompts = []

    def chat_json(self, prompt: str, system=None, temperature=None, step=None):
        self.prompts.append(prompt)
        return {
            "column_semantics": {
                "姓名 / name": "name",
                "身份证 / id": "id",
                "入职日期 / date": "start_date"
            },
            "row_filter": {
                "min_nonempty_ratio": 0.2
            },
            "normalization": {
                "date_fields": ["start_date"],
                "id_fields": ["id"],
                "phone_fields": []
            }
        }


def _build_dataframe(data_rows: int) -> pd.DataFrame:
    rows = [
        ["姓名", "身份证", "入职日期"],
        ["name", "id", "date"]
    ]
    for i in range(data_rows):
        rows.append([
            f"张三{i}",
            f"11010119900101{i:04d}",
            f"2024-01-{(i % 28) + 1:02d}"
        ])
    return pd.DataFrame(rows)


def main() -> None:
    data_rows = FAST_ROWS_THRESHOLD + 5
    df = _build_dataframe(data_rows)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "sample.xlsx")
        df.to_excel(path, index=False, header=False)

        llm = FakeLLM()
        prompts = {
            "EXCEL_SCHEMA_INFER_PROMPT": "SCHEMA_INFER",
        }
        extractor = ExcelExtractor(llm, prompts)
        source_doc = extractor.extract(path)

    extracted = source_doc.extracted
    assert extracted["metadata"]["mode"] == "schema_infer"
    assert len(extracted["data"]) == data_rows
    assert extracted["data"][0]["name"].startswith("张三")
    assert extracted["data"][0]["__source_file__"] == "sample.xlsx"
    assert any("SCHEMA_INFER" in prompt for prompt in llm.prompts)
    print("FAST mode smoke test passed.")


if __name__ == "__main__":
    main()
