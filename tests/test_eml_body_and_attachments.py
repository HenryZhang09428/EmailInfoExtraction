import os
import uuid
from pathlib import Path
from email.message import EmailMessage

from core.extractors.email_extractor import EmailExtractor


def test_eml_exports_attachments_and_extracts_body_records(tmp_path):
    # Use an existing valid xlsx fixture as attachment bytes (avoid importing openpyxl/numpy)
    project_root = Path(__file__).resolve().parents[1]
    fixture_xlsx = project_root / "eval" / "fixtures" / "sample.xlsx"
    xlsx_bytes = fixture_xlsx.read_bytes()

    # Build a simple .eml with text/plain body + one xlsx attachment
    msg = EmailMessage()
    msg["From"] = "sender@example.com"
    msg["To"] = "receiver@example.com"
    msg["Subject"] = "人员信息"
    msg.set_content(
        "本邮件包含人员变动信息：\n"
        "1) 姓名：张三 证件号码：110101199001011234 变动类型：增员\n"
        "2) 姓名：李四 证件号码：44030019900101123X 变动类型：减员\n"
    )
    msg.add_attachment(
        xlsx_bytes,
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="changes.xlsx",
    )

    eml_path = tmp_path / "sample.eml"
    eml_path.write_bytes(msg.as_bytes())

    class DummyLLM:
        def chat_json(self, prompt: str, system=None, temperature=None, step=None):
            # Return a deterministic record without external calls
            return {
                "data": [
                    {"姓名": "张三", "证件号码": "110101199001011234", "变动类型": "增员"}
                ],
                "metadata": {"stub": "1"},
                "warnings": [],
            }

    prompts = {
        "EML_BODY_TO_JSON_PROMPT": (
            "Return ONLY JSON: {\"data\":[],\"metadata\":{},\"warnings\":[]}"
        )
    }
    source_id = f"pytest-{uuid.uuid4()}"
    extractor = EmailExtractor(DummyLLM(), prompts, source_id=source_id)
    doc = extractor.extract(str(eml_path))

    derived_files = extractor.get_derived_files()
    assert any(p.lower().endswith(".xlsx") for p in derived_files)
    assert all(os.path.exists(p) for p in derived_files)

    assert isinstance(doc.extracted, dict)
    assert "body_extracted" in doc.extracted
    assert "data" in doc.extracted["body_extracted"]
    assert doc.extracted["body_extracted"]["data"], "expected non-empty extracted records"

    first = doc.extracted["body_extracted"]["data"][0]
    assert first.get("__source_file__") == "sample.eml"

